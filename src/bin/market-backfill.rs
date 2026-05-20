use market_ingest_app::backfill::{parse_args, print_help, run_backfill};
use market_ingest_app::log_stream;
use serde_json::json;
use std::env;
use std::process;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        let _ = log_stream::error(
            "market_backfill_error",
            json!({ "message": error.to_string() }),
        );
        process::exit(1);
    }
}

async fn run() -> Result<(), market_ingest_app::backfill::BackfillError> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    run_backfill(args).await
}
