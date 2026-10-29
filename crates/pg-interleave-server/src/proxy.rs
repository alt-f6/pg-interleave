use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::protocol::{
    decode_regular_message, decode_startup_message, extract_backend_key_data, extract_query_text,
};

pub async fn run_proxy(listen_addr: SocketAddr, target_addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!("Прокси слушает входящие соединения на {}", listen_addr);

    let mut connection_id: u64 = 0;

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        connection_id += 1;
        let conn_id = connection_id;

        tokio::spawn(async move {
            info!(conn_id, %client_addr, "Новое подключение клиента");
            if let Err(err) = handle_connection(client_stream, target_addr, conn_id).await {
                error!(conn_id, "Ошибка обработки соединения: {:?}", err);
            }
            info!(conn_id, "Сессия закрыта");
        });
    }
}

async fn handle_connection(
    mut client: TcpStream,
    target_addr: SocketAddr,
    conn_id: u64,
) -> anyhow::Result<()> {
    let mut server = TcpStream::connect(target_addr).await?;
    info!(conn_id, "Соединение с базой установлено");

    let (mut client_read, mut client_write) = client.split();
    let (mut server_read, mut server_write) = server.split();

    let mut client_buf = BytesMut::with_capacity(8192);
    let mut server_buf = BytesMut::with_capacity(8192);

    let mut client_started = false;

    // Цикл прокачки с селектом по обоим сокетам
    loop {
        tokio::select! {
            // Чтение данных от клиента -> пересылка в сервер
            read_res = client_read.read_buf(&mut client_buf) => {
                let n = read_res?;
                if n == 0 {
                    break;
                }

                while !client_buf.is_empty() {
                    let msg = if !client_started {
                        if let Some(msg) = decode_startup_message(&mut client_buf)? {
                            // Проверка на SSLRequest: код 80877103 (0x04D2162F)
                            if msg.payload.len() == 4 && msg.payload == 80877103_i32.to_be_bytes() {
                                debug!(conn_id, "Получен SSLRequest от клиента");
                            } else {
                                client_started = true;
                                debug!(conn_id, "Получен StartupMessage");
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
                            let tag_repr = m.tag_char();
                            if let Some(sql) = extract_query_text(&m) {
                                info!(conn_id, tag = %tag_repr, query = %sql, "-> Запрос клиента");
                            } else {
                                debug!(conn_id, tag = %tag_repr, len = m.payload.len(), "-> Пакет клиента");
                            }
                            server_write.write_all(&m.to_bytes()).await?;
                        }
                        None => break, // Недостаточно байт для полного сообщения
                    }
                }
            }

            // Чтение данных от сервера -> пересылка клиенту
            read_res = server_read.read_buf(&mut server_buf) => {
                let n = read_res?;
                if n == 0 {
                    break;
                }

                // Ответ на SSLRequest от сервера — это один байт ('N' или 'S')
                if !server_buf.is_empty() && (server_buf[0] == b'N' || server_buf[0] == b'S') && server_buf.len() == 1 {
                    let byte = server_buf.split_to(1);
                    debug!(conn_id, response = %(byte[0] as char), "<- Ответ сервера на SSLRequest");
                    client_write.write_all(&byte).await?;
                    continue;
                }

                while !server_buf.is_empty() {
                    match decode_regular_message(&mut server_buf)? {
                        Some(m) => {
                            if let Some((pid, _key)) = extract_backend_key_data(&m) {
                                info!(conn_id, server_pid = pid, "Привязан BackendKeyData от Postgres");
                            } else {
                                debug!(conn_id, tag = %m.tag_char(), len = m.payload.len(), "<- Пакет сервера");
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
