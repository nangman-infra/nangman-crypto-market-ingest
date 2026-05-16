use market_ingest_app::log_stream;
use market_ingest_app::supervisor::{parse_args, print_help, run_supervisor};
use std::env;
use std::process;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        log_stream::error("crypto_market_ingest_supervisor_error", &error.to_string());
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    run_supervisor(args).await
}
