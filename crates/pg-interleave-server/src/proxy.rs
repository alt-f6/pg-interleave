use std::net::SocketAddr;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub async fn run_proxy(listen_addr: SocketAddr, target_addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!("Proxy listens for incoming connections on {}", listen_addr);

    let mut connection_id: u64 = 0;

    loop {
        let (client_stream, client_addr) = listener.accept().await?;
        connection_id += 1;
        let conn_id = connection_id;

        tokio::spawn(async move {
            info!(conn_id, %client_addr, "New client connection");
            if let Err(err) = handle_connection(client_stream, target_addr, conn_id).await {
                error!(conn_id, "Connection processing error: {:?}", err);
            }
            info!(conn_id, "The session is closed");
        });
    }
}

async fn handle_connection(
    mut client_stream: TcpStream,
    target_addr: SocketAddr,
    conn_id: u64,
) -> anyhow::Result<()> {
    let mut server_stream = TcpStream::connect(target_addr).await?;
    info!(conn_id, "A bridge to PostgreSQL has been installed on {}", target_addr);

    let (bytes_client_to_server, bytes_server_to_client) =
        copy_bidirectional(&mut client_stream, &mut server_stream).await?;

    info!(
        conn_id,
        bytes_sent = bytes_client_to_server,
        bytes_received = bytes_server_to_client,
        "The traffic was transmitted successfully"
    );

    Ok(())
}