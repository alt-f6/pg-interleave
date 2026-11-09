use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

mod barrier;
mod control;
mod protocol;
mod proxy;
mod trace;

#[derive(Parser, Debug)]
#[command(
    name = "pg-interleave",
    about = "Deterministic Postgres Proxy for integration tests"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:5433")]
    listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5432")]
    target_addr: String,

    #[arg(long, default_value = "127.0.0.1:9090")]
    control_addr: SocketAddr,

    #[arg(long, default_value = "5000")]
    barrier_ttl_ms: u64,

    #[arg(long)]
    fuzz: bool,

    #[arg(long)]
    seed: Option<u64>,

    #[arg(long, default_value = "50")]
    fuzz_max_delay_ms: u64,

    #[arg(long, conflicts_with = "replay")]
    record: Option<PathBuf>,

    #[arg(long, conflicts_with = "record")]
    replay: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    let fuzz_seed = if args.fuzz {
        let seed = args.seed.unwrap_or_else(|| fastrand::u64(..));
        info!(
            "Schedule fuzzer ENABLED. Seed: {} (use --seed {} to reproduce)",
            seed, seed
        );
        Some(seed)
    } else {
        None
    };

    let recorder = args.record.map(trace::TraceRecorder::new);
    let replayer = match args.replay {
        Some(path) => {
            let r = trace::TraceReplayer::from_file(&path)?;
            info!(path = ?path, "Schedule replay ENABLED");
            Some(Arc::new(r))
        }
        None => None,
    };

    let state = barrier::SharedState::new(
        fuzz_seed,
        args.fuzz_max_delay_ms,
        recorder.clone(),
        replayer,
    );

    let proxy_handle = {
        let state = state.clone();
        let target = args.target_addr.clone();
        tokio::spawn(async move {
            proxy::run_proxy(args.listen_addr, target, state, args.barrier_ttl_ms).await
        })
    };

    let control_handle = {
        let state = state.clone();
        tokio::spawn(async move { control::run_control_server(args.control_addr, state).await })
    };

    info!(
        "pg-interleave started. Proxy: {}, Control: {}, TTL: {}ms",
        args.listen_addr, args.control_addr, args.barrier_ttl_ms
    );

    tokio::select! {
        res = async {
            let (proxy_res, control_res) = tokio::try_join!(proxy_handle, control_handle)?;
            proxy_res?;
            control_res?;
            Ok::<(), anyhow::Error>(())
        } => {
            if let Err(e) = res {
                error!("Server exited with error: {:?}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal (Ctrl+C)");
        }
    }

    if let Some(rec) = recorder
        && let Err(err) = rec.flush().await
    {
        error!("Failed to flush trace recorder: {:?}", err);
    }

    Ok(())
}
