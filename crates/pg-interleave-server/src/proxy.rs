use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::barrier::SharedState;
use crate::protocol::{
    decode_regular_message, decode_startup_message, extract_backend_key_data, extract_query_text,
};

pub async fn run_proxy(
    listen_addr: SocketAddr,
    target_addr: SocketAddr,
    state: SharedState,
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
            if let Err(err) =
                handle_connection(client_stream, target_addr, conn_id, conn_state).await
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
) -> anyhow::Result<()> {
    let mut server = TcpStream::connect(target_addr).await?;
    info!(conn_id, "Established connection to upstream database");

    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();

    let mut client_buf = BytesMut::with_capacity(8192);
    let mut server_buf = BytesMut::with_capacity(8192);

    let mut client_started = false;
    let mut server_pid: Option<u32> = None;

    loop {
        tokio::select! {
            read_res = client_read.read_buf(&mut client_buf) => {
                let n = read_res?;
                if n == 0 {
                    break;
                }

                while !client_buf.is_empty() {
                    let msg = if !client_started {
                        if let Some(msg) = decode_startup_message(&mut client_buf)? {
                            if msg.payload.len() == 4 && msg.payload == 80877103_i32.to_be_bytes() {
                                debug!(conn_id, "Received SSLRequest");
                            } else {
                                client_started = true;
                            }
                            Some(msg)
                        } else {
                            None
                        }
                    } else {
                        decode_regular_message(&mut client_buf)?
                    };

                    match msg {
                        Some(m) => {
                            if let Some(sql) = extract_query_text(&m) {
                                info!(conn_id, query = %sql, "-> Client query");

                                // Check intercept barriers
                                if let Some((barrier_id, receiver)) = state.check_and_create_barrier(server_pid, &sql).await {
                                    info!(conn_id, barrier_id = %barrier_id, "Waiting for resume command from controller...");
                                    let _ = receiver.await;
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

                if !server_buf.is_empty() && (server_buf[0] == b'N' || server_buf[0] == b'S') && server_buf.len() == 1 {
                    let byte = server_buf.split_to(1);
                    client_write.write_all(&byte).await?;
                    continue;
                }

                while !server_buf.is_empty() {
                    match decode_regular_message(&mut server_buf)? {
                        Some(m) => {
                            if let Some((pid, _key)) = extract_backend_key_data(&m) {
                                server_pid = Some(pid);
                                state.register_pid(pid, conn_id).await;
                                info!(conn_id, server_pid = pid, "Bound BackendKeyData");
                            }
                            client_write.write_all(&m.to_bytes()).await?;
                        }
                        None => break,
                    }
                }
            }
        }
    }

    Ok(())
}
