use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::barrier::{BarrierAction, SharedState};
use crate::protocol::{
    PgMessage, SpecialStartup, decode_regular_message, decode_startup_message,
    extract_backend_key_data, extract_simple_query, parse_extended_bind, parse_extended_execute,
    parse_extended_parse, peek_startup_type,
};

struct ConnectionState {
    statements: HashMap<String, String>,
    portals: HashMap<String, String>,
    last_tx_status: u8,
    pending_pause_after: bool,
    response_buffer: Vec<u8>,
}

impl ConnectionState {
    fn new() -> Self {
        Self {
            statements: HashMap::new(),
            portals: HashMap::new(),
            last_tx_status: b'I',
            pending_pause_after: false,
            response_buffer: Vec::new(),
        }
    }
}

pub async fn run_proxy(
    listen_addr: SocketAddr,
    target_addr: SocketAddr,
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
        connection_id += 1;
        let conn_id = connection_id;
        let conn_state = state.clone();

        tokio::spawn(async move {
            info!(conn_id, %client_addr, "New client connection");
            if let Err(err) = handle_connection(
                client_stream,
                target_addr,
                conn_id,
                conn_state,
                barrier_ttl_ms,
            )
            .await
            {
                error!(conn_id, "Error handling connection: {:?}", err);
            }
            info!(conn_id, "Session closed");
        });
    }
}

async fn handle_connection(
    mut client: TcpStream,
    target_addr: SocketAddr,
    conn_id: u64,
    state: SharedState,
    barrier_ttl_ms: u64,
) -> anyhow::Result<()> {
    let mut server = TcpStream::connect(target_addr).await?;
    info!(conn_id, "Established connection to upstream database");

    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();

    let mut client_buf = BytesMut::with_capacity(8192);
    let mut server_buf = BytesMut::with_capacity(8192);

    let mut client_started = false;
    let mut server_pid: Option<u32> = None;
    let mut conn_scope = ConnectionState::new();

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
                            let mut query_opt = extract_simple_query(&m);

                            if let Some((stmt_name, sql)) = parse_extended_parse(&m) {
                                conn_scope.statements.insert(stmt_name.clone(), sql.clone());
                                state.store_statement(conn_id, stmt_name, sql).await;
                            }

                            if let Some((portal_name, stmt_name)) = parse_extended_bind(&m) {
                                conn_scope.portals.insert(portal_name, stmt_name);
                            }

                            if let Some(portal_name) = parse_extended_execute(&m)
                                && let Some(stmt_name) = conn_scope.portals.get(&portal_name)
                                && let Some(sql) = conn_scope.statements.get(stmt_name)
                            {
                                query_opt = Some(sql.clone())
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

                            if let Some(ref sql) = query_opt {
                                info!(conn_id, query = %sql, "-> Client query");

                                if state.check_deadlock_match(server_pid, sql).await {
                                    warn!(conn_id, query = %sql, "CHAOS: Injecting synthetic 40P01 Deadlock error!");

                                    if m.tag != Some(b'Q') {
                                        while !client_buf.is_empty() {
                                            if let Some(next_msg) = decode_regular_message(&mut client_buf)? {
                                                if next_msg.tag == Some(b'S') {
                                                    break;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    }

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
                                    continue;
                                }

                                if let Some(rule) = state.check_action_match(server_pid, sql, BarrierAction::PauseAfterResponse).await {
                                    let _ = rule;
                                    conn_scope.pending_pause_after = true;
                                }

                                if let Some((barrier_id, receiver)) = state.check_and_create_barrier(server_pid, sql).await {
                                    info!(conn_id, barrier_id = %barrier_id, ttl_ms = barrier_ttl_ms, "Waiting for resume command...");
                                    let cleanup_state = state.clone();
                                    let b_id = barrier_id.clone();

                                    let wait_result = tokio::time::timeout(Duration::from_millis(barrier_ttl_ms), receiver).await;
                                    if wait_result.is_err() {
                                        cleanup_state.record_timeout(&b_id).await;
                                        warn!(conn_id, barrier_id = %barrier_id, "Barrier timed out! Canceling query with 57014");

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
                                        continue;
                                    }
                                    info!(conn_id, barrier_id = %barrier_id, "Resuming packet forward to database");
                                }
                            }

                            server_write.write_all(&m.to_bytes()).await?;
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

                            if m.tag == Some(b'Z') && !m.payload.is_empty() {
                                conn_scope.last_tx_status = m.payload[0];
                            }

                            if m.tag == Some(b'C') {
                                let payload_str = String::from_utf8_lossy(&m.payload);
                                if payload_str.starts_with("COMMIT")
                                    && state.should_drop_on_commit(server_pid).await
                                {
                                    warn!(conn_id, "CHAOS: Forcing TCP connection drop right after database COMMIT!");
                                    return Ok(());
                                }
                            }

                            if conn_scope.pending_pause_after {
                                conn_scope.response_buffer.extend_from_slice(&m.to_bytes());
                                if m.tag == Some(b'Z') {
                                    conn_scope.pending_pause_after = false;
                                    let (barrier_id, rx) = state.create_manual_barrier().await;
                                    info!(conn_id, barrier_id = %barrier_id, "PauseAfterResponse reached ReadyForQuery, holding response buffer...");

                                    let cleanup_state = state.clone();
                                    let b_id = barrier_id.clone();
                                    let wait_result = tokio::time::timeout(Duration::from_millis(barrier_ttl_ms), rx).await;
                                    if wait_result.is_err() {
                                        cleanup_state.record_timeout(&b_id).await;
                                        warn!(conn_id, barrier_id = %barrier_id, "PauseAfterResponse barrier timed out!");
                                    }

                                    let buffered = std::mem::take(&mut conn_scope.response_buffer);
                                    client_write.write_all(&buffered).await?;
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
}
