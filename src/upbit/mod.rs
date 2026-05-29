mod config;
mod error;
mod events;
mod logging;
mod report;
mod stats;
mod storage_output;
mod universe;
mod url;
mod ws;

pub use self::config::UpbitRunConfig;
use self::config::validate_config;
pub use self::error::UpbitIngestError;
use self::logging::print_upbit_ingest_log;
use self::report::{UpbitReportInput, build_report, validate_report};
use self::storage_output::finalize_storage;
pub use self::universe::{UpbitMarket, fetch_top_krw_markets};
use crate::log_stream;
use crate::storage::L0StorageSink;
use std::time::Duration;

pub async fn run_upbit_l0_smoke(config: UpbitRunConfig) -> Result<(), UpbitIngestError> {
    validate_config(&config)?;

    let http = reqwest::Client::new();
    let markets = fetch_top_krw_markets(
        &http,
        &config.rest_base_url,
        &config.quote_currency,
        config.expect_symbol_count,
    )
    .await?;
    let planned_stream_count = markets.len() * 3;

    log_stream::info(
        "market_ingest_start",
        serde_json::json!({
            "venue": "upbit",
            "source_role": "execution",
            "symbol_count": markets.len(),
            "planned_stream_count": planned_stream_count,
            "duration_seconds": config.duration_seconds,
            "stream_kinds": ["ticker", "trade", "orderbook"],
            "orderbook_unit": config.orderbook_unit
        }),
    )?;

    let mut storage_sink = match config.l0_storage.clone() {
        Some(storage_config) => Some(
            L0StorageSink::new(storage_config)
                .await
                .map_err(|error| UpbitIngestError::Storage(error.to_string()))?,
        ),
        None => None,
    };

    let stats = ws::watch_upbit_ingest_streams(
        &config.websocket_url,
        &markets,
        config.orderbook_unit,
        Duration::from_secs(config.duration_seconds),
        Duration::from_secs(config.log_interval_seconds),
        storage_sink.as_mut(),
        print_upbit_ingest_log,
    )
    .await?;
    if let Some(sink) = storage_sink.as_mut() {
        finalize_storage(sink, &stats).await?;
    }
    let storage = storage_sink.as_ref().map(L0StorageSink::report);

    let allow_partial_symbol_coverage = config.allow_partial_symbol_coverage;
    let report = build_report(UpbitReportInput {
        config,
        markets,
        planned_stream_count,
        storage,
        stats,
    });

    log_stream::info("market_ingest_report", &report)?;
    validate_report(&report, allow_partial_symbol_coverage)
}
