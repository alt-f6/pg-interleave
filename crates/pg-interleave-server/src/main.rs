use clap::Parser;
use std::net::SocketAddr;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

mod proxy;

#[derive(Parser, Debug)]
#[command(name = "pg-interleave", about = "Deterministic Postgres Proxy for integration tests")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5432")]
    target_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    info!(
        "Запуск pg-interleave на {} -> PostgreSQL на {}",
        args.listen_addr, args.target_addr
    );

    proxy::run_proxy(args.listen_addr, args.target_addr).await
}