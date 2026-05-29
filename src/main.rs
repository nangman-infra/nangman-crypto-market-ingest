use market_ingest_app::log_stream;
use serde_json::json;
use std::process;

mod maintenance;
mod runtime;
mod venue_runner;

#[tokio::main]
async fn main() {
    if let Err(error) = runtime::run().await {
        let _ = log_stream::error(
            "market_ingest_error",
            json!({ "message": error.to_string() }),
        );
        process::exit(1);
    }
}
