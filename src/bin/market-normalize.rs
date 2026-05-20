use market_ingest_app::clock;
use market_ingest_app::log_stream;
use market_ingest_app::normalize::args::{parse_args, print_help};
use market_ingest_app::normalize::run::run_normalize;
use serde_json::json;
use std::env;
use std::process;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        let _ = log_stream::error(
            "market_normalize_error",
            json!({ "message": error.to_string() }),
        );
        process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    let now_ms = clock::now_ms();
    run_normalize(args, now_ms).await
}
