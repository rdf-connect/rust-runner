use anyhow::Result;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <orchestrator-addr> <runner-uri>", args[0]);
        std::process::exit(1);
    }

    let addr = args[1].clone();
    let uri = args[2].clone();

    info!("RDF-Connect Rust runner starting");
    rdfc_rust_runner::run(addr, uri).await
}
