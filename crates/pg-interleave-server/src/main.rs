use clap::Parser;
use std::net::SocketAddr;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

mod barrier;
mod control;
mod protocol;
mod proxy;

#[derive(Parser, Debug)]
#[command(
    name = "pg-interleave",
    about = "Deterministic Postgres Proxy for integration tests"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5432")]
    target_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:9090")]
    control_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();
    let state = barrier::SharedState::new();

    let proxy_handle = {
        let state = state.clone();
        tokio::spawn(
            async move { proxy::run_proxy(args.listen_addr, args.target_addr, state).await },
        )
    };

    let control_handle = {
        let state = state.clone();
        tokio::spawn(async move { control::run_control_server(args.control_addr, state).await })
    };

    info!(
        "pg-interleave started. Proxy: {}, Control: {}",
        args.listen_addr, args.control_addr
    );
    let (proxy_res, control_res) = tokio::try_join!(proxy_handle, control_handle)?;
    proxy_res?;
    control_res?;

    Ok(())
}
