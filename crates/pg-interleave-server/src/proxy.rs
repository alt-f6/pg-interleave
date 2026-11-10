use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, lookup_host};
use tracing::{debug, error, info, warn};

use crate::barrier::{BarrierAction, ConnFuzzer, SharedState, extract_session_tag};
use crate::protocol::{
    PgMessage, SpecialStartup, decode_regular_message, decode_startup_message,
    extract_backend_key_data, extract_simple_query, parse_extended_bind, parse_extended_execute,
    parse_extended_parse, peek_startup_type,
};

#[derive(Clone)]
struct PortalData {
    stmt_name: String,
    params: Vec<Option<String>>,
}

struct ConnectionState {
    statements: HashMap<String, String>,
    portals: HashMap<String, PortalData>,
    last_tx_status: u8,
    pending_pause_after_rule: Option<String>,
    pending_drop_on_commit: bool,
    response_buffer: Vec<u8>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            statements: HashMap::new(),
            portals: HashMap::new(),
            last_tx_status: b'I',
            pending_pause_after_rule: None,
            pending_drop_on_commit: false,
            response_buffer: Vec::new(),
        }
    }
}

pub async fn run_proxy(
    listen_addr: SocketAddr,
    target_addr: String,
    state: SharedState,
    barrier_ttl_ms: u64,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!(
        "Proxy listening for incoming connections on {}",
        listen_addr
    );

    let mut connection_id: u64 = 0;

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        client_stream.set_nodelay(true)?;
        connection_id += 1;
        let conn_id = connection_id;
        let conn_state = state.clone();
        let target = target_addr.clone();

        tokio::spawn(async move {
            info!(conn_id, %client_addr, "New client connection");
            if let Err(err) =
                handle_connection(client_stream, &target, conn_id, conn_state, barrier_ttl_ms).await
            {
                error!(conn_id, "Error handling connection: {:?}", err);
            }
            info!(conn_id, "Session closed");
        });
    }
}

async fn handle_connection(
    mut client: TcpStream,
    target_addr: &str,
    conn_id: u64,
    state: SharedState,
    barrier_ttl_ms: u64,
) -> anyhow::Result<()> {
    let mut conn_fuzzer = state.fuzzer().await.map(|f| f.for_connection(conn_id));

    let mut resolved = lookup_host(target_addr).await?;
    let target_sock = resolved
        .next()
        .ok_or_else(|| anyhow::anyhow!("Unable to resolve target address: {}", target_addr))?;

    let mut server = TcpStream::connect(target_sock).await?;
    server.set_nodelay(true)?;
    info!(conn_id, "Established connection to upstream database");

    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();

    let mut client_buf = BytesMut::with_capacity(8192);
    let mut server_buf = BytesMut::with_capacity(8192);

    let mut client_started = false;
    let mut authenticated = false;
    let mut server_pid: Option<u32> = None;
    let mut conn_scope = ConnectionState::new();
    let mut client_batch: Vec<PgMessage> = Vec::new();

    let res: anyhow::Result<()> = async {
        loop {
            tokio::select! {
                read_res = client_read.read_buf(&mut client_buf) => {
                    let n = read_res?;
                    if n == 0 {
                        break;
                    }

                    while !client_buf.is_empty() {
                        if !client_started {
                            if let Some(special) = peek_startup_type(&client_buf)? {
                                match special {
                                    SpecialStartup::SslRequest => {
                                        client_buf.advance(8);
                                        debug!(conn_id, "Replying 'N' directly to SSLRequest");
                                        client_write.write_all(b"N").await?;
                                        continue;
                                    }
                                    SpecialStartup::GssEncRequest => {
                                        client_buf.advance(8);
                                        debug!(conn_id, "Replying 'N' directly to GSSENCRequest");
                                        client_write.write_all(b"N").await?;
                                        continue;
                                    }
                                    SpecialStartup::CancelRequest { .. } => {
                                        let packet = client_buf.split_to(16).to_vec();
                                        server_write.write_all(&packet).await?;
                                        return Ok(());
                                    }
                                    SpecialStartup::RegularStartup => {}
                                }
                            }

                            if let Some(msg) = decode_startup_message(&mut client_buf)? {
                                client_started = true;
                                server_write.write_all(&msg.to_bytes()).await?;
                            }
                            break;
                        }

                        match decode_regular_message(&mut client_buf)? {
                            Some(m) => {
                                if !authenticated {
                                    server_write.write_all(&m.to_bytes()).await?;
                                    continue;
                                }

                                let tag = m.tag;
                                if tag == Some(b'X') {
                                    server_write.write_all(&m.to_bytes()).await?;
                                    return Ok(());
                                }

                                client_batch.push(m);

                                if tag == Some(b'S') || tag == Some(b'Q') {
                                    let batch = std::mem::take(&mut client_batch);
                                    process_client_batch(
                                        batch,
                                        &mut conn_scope,
                                        &state,
                                        server_pid,
                                        conn_id,
                                        barrier_ttl_ms,
                                        &mut client_write,
                                        &mut server_write,
                                        conn_fuzzer.as_mut(),
                                    ).await?;
                                }
                            }
                            None => break,
                        }
                    }
                }

                read_res = server_read.read_buf(&mut server_buf) => {
                    let n = read_res?;
                    if n == 0 {
                        break;
                    }

                    while !server_buf.is_empty() {
                        match decode_regular_message(&mut server_buf)? {
                            Some(m) => {
                                if let Some((pid, _key)) = extract_backend_key_data(&m) {
                                    server_pid = Some(pid);
                                    state.register_pid(pid, conn_id).await;
                                    info!(conn_id, server_pid = pid, "Bound BackendKeyData");
                                }

                                if m.tag == Some(b'Z') {
                                    authenticated = true;
                                    if !m.payload.is_empty() {
                                        conn_scope.last_tx_status = m.payload[0];
                                    }
                                }

                                if m.tag == Some(b'C') {
                                    let payload_str = String::from_utf8_lossy(&m.payload);
                                    let is_commit_tag = payload_str.starts_with("COMMIT");
                                    let is_autocommit = conn_scope.last_tx_status != b'T';

                                    if (conn_scope.pending_drop_on_commit && (is_commit_tag || is_autocommit))
                                        || (is_commit_tag && state.should_drop_on_commit(server_pid, None, &payload_str).await)
                                    {
                                        conn_scope.pending_drop_on_commit = false;
                                        warn!(conn_id, "CHAOS: Forcing TCP connection drop right after database COMMIT!");
                                        return Ok(());
                                    }
                                }

                                if let Some(rule_id) = conn_scope.pending_pause_after_rule.clone() {
                                    conn_scope.response_buffer.extend_from_slice(&m.to_bytes());
                                    if m.tag == Some(b'Z') {
                                        conn_scope.pending_pause_after_rule = None;
                                        let (barrier_id, rx) = state.create_manual_barrier_with_rule(Some(rule_id)).await;
                                        info!(conn_id, barrier_id = %barrier_id, "PauseAfterResponse reached ReadyForQuery, holding response buffer...");

                                        let cleanup_state = state.clone();
                                        let b_id = barrier_id.clone();
                                        let wait_result = tokio::time::timeout(Duration::from_millis(barrier_ttl_ms), rx).await;
                                        if wait_result.is_err() {
                                            cleanup_state.record_timeout(&b_id).await;
                                            warn!(conn_id, barrier_id = %barrier_id, "PauseAfterResponse barrier timed out! Emitting 57014");

                                            conn_scope.response_buffer.clear();
                                            let err_msg = PgMessage::build_error_response(
                                                "57014",
                                                "canceling statement due to pg-interleave barrier timeout",
                                            );
                                            let ready_status = match conn_scope.last_tx_status {
                                                b'T' => b'E',
                                                _ => b'I',
                                            };
                                            let ready_msg = PgMessage::build_ready_for_query(ready_status);

                                            client_write.write_all(&err_msg.to_bytes()).await?;
                                            client_write.write_all(&ready_msg.to_bytes()).await?;
                                        } else {
                                            let buffered = std::mem::take(&mut conn_scope.response_buffer);
                                            client_write.write_all(&buffered).await?;
                                        }
                                    }
                                } else {
                                    client_write.write_all(&m.to_bytes()).await?;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }
        Ok(())
    }.await;

    state.cleanup_connection(conn_id, server_pid).await;
    res
}

#[allow(clippy::too_many_arguments)]
async fn process_client_batch<W1, W2>(
    batch: Vec<PgMessage>,
    conn_scope: &mut ConnectionState,
    state: &SharedState,
    server_pid: Option<u32>,
    conn_id: u64,
    barrier_ttl_ms: u64,
    client_write: &mut W1,
    server_write: &mut W2,
    mut fuzzer: Option<&mut ConnFuzzer>,
) -> anyhow::Result<()>
where
    W1: AsyncWriteExt + Unpin,
    W2: AsyncWriteExt + Unpin,
{
    let mut executed_sqls: Vec<(String, Vec<Option<String>>)> = Vec::new();

    for m in &batch {
        if let Some((stmt_name, sql)) = parse_extended_parse(m) {
            conn_scope.statements.insert(stmt_name.clone(), sql.clone());
            state.store_statement(conn_id, stmt_name, sql).await;
        }

        if let Some(bound) = parse_extended_bind(m) {
            conn_scope.portals.insert(
                bound.portal_name,
                PortalData {
                    stmt_name: bound.stmt_name,
                    params: bound.params,
                },
            );
        }

        if let Some(portal_name) = parse_extended_execute(m)
            && let Some(portal) = conn_scope.portals.get(&portal_name)
            && let Some(sql) = conn_scope.statements.get(&portal.stmt_name)
        {
            executed_sqls.push((sql.clone(), portal.params.clone()));
        }

        if let Some(sql) = extract_simple_query(m) {
            executed_sqls.push((sql, Vec::new()));
        }

        if m.tag == Some(b'C') && m.payload.len() >= 2 {
            let close_type = m.payload[0];
            let name_bytes = &m.payload[1..];
            let name = String::from_utf8_lossy(name_bytes)
                .trim_matches('\0')
                .to_string();
            if close_type == b'S' {
                conn_scope.statements.remove(&name);
            } else if close_type == b'P' {
                conn_scope.portals.remove(&name);
            }
        }
    }

    for (sql, params) in &executed_sqls {
        let is_internal_query = sql.contains("pg_backend_pid");

        if !is_internal_query {
            state.wait_for_replay_turn(conn_id, sql).await;
        }

        let session_tag = extract_session_tag(sql);

        if !is_internal_query && let Some(fuzz) = fuzzer.as_deref_mut() {
            let delay = fuzz.next_delay();
            if delay > 0 {
                debug!(conn_id, delay_ms = delay, "Deterministic jitter injected");
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }

        if !is_internal_query {
            state.record_step(conn_id, sql, params).await;
        }

        info!(conn_id, query = %sql, ?params, "-> Inspecting batch query");

        if state
            .check_deadlock_match(server_pid, session_tag.as_deref(), sql, params)
            .await
        {
            warn!(conn_id, query = %sql, "CHAOS: Injecting synthetic 40P01 Deadlock, dropping batch");

            let err_msg = PgMessage::build_error_response(
                "40P01",
                "deadlock detected (synthetic injection by pg-interleave)",
            );
            let ready_status = match conn_scope.last_tx_status {
                b'T' => b'E',
                _ => b'I',
            };
            let ready_msg = PgMessage::build_ready_for_query(ready_status);

            client_write.write_all(&err_msg.to_bytes()).await?;
            client_write.write_all(&ready_msg.to_bytes()).await?;
            return Ok(());
        }

        if let Some(rule) = state
            .check_action_match(
                server_pid,
                session_tag.as_deref(),
                sql,
                BarrierAction::PauseAfterResponse,
                params,
            )
            .await
        {
            conn_scope.pending_pause_after_rule = Some(rule.id);
        }

        if let Some(_rule) = state
            .check_action_match(
                server_pid,
                session_tag.as_deref(),
                sql,
                BarrierAction::DropConnectionOnCommit,
                params,
            )
            .await
        {
            conn_scope.pending_drop_on_commit = true;
        }

        if let Some((barrier_id, receiver)) = state
            .check_and_create_barrier(server_pid, session_tag.as_deref(), sql, params)
            .await
        {
            info!(conn_id, barrier_id = %barrier_id, ttl_ms = barrier_ttl_ms, "Waiting for resume command before sending batch...");
            let cleanup_state = state.clone();
            let b_id = barrier_id.clone();

            let wait_result =
                tokio::time::timeout(Duration::from_millis(barrier_ttl_ms), receiver).await;
            if wait_result.is_err() {
                cleanup_state.record_timeout(&b_id).await;
                warn!(conn_id, barrier_id = %barrier_id, "Barrier timed out! Canceling batch with 57014");

                let err_msg = PgMessage::build_error_response(
                    "57014",
                    "canceling statement due to pg-interleave barrier timeout",
                );
                let ready_status = match conn_scope.last_tx_status {
                    b'T' => b'E',
                    _ => b'I',
                };
                let ready_msg = PgMessage::build_ready_for_query(ready_status);

                client_write.write_all(&err_msg.to_bytes()).await?;
                client_write.write_all(&ready_msg.to_bytes()).await?;
                return Ok(());
            }
            info!(conn_id, barrier_id = %barrier_id, "Resuming batch forward to database");
        }
    }

    for m in &batch {
        server_write.write_all(&m.to_bytes()).await?;
    }

    for (sql, _) in &executed_sqls {
        if !sql.contains("pg_backend_pid") {
            state.advance_replay_step();
        }
    }

    Ok(())
}
