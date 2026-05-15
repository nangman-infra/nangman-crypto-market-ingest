use market_ingest_app::log_stream;
use market_ingest_app::normalize::args::{parse_args, print_help, unix_timestamp_millis};
use market_ingest_app::normalize::run::run_normalize;
use std::env;
use std::process;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        log_stream::error("market_normalize_error", &error.to_string());
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    let now_ms = unix_timestamp_millis();
    run_normalize(args, now_ms).await
}
