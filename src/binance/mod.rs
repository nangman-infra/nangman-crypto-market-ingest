mod config;
mod derivatives;
mod error;
mod events;
mod logging;
mod report;
mod rest;
mod stats;
mod storage_output;
mod ws;

pub use self::config::{BinanceMarket, BinanceRunConfig};
pub use self::error::BinanceIngestError;

use self::report::{BinanceReportInput, build_report, validate_report};
use self::storage_output::{append_depth_snapshots, finalize_storage};
use crate::clock;
use crate::log_stream;
use crate::storage::L0StorageSink;
use std::time::Duration;

pub async fn run_binance_l0_smoke(config: BinanceRunConfig) -> Result<(), BinanceIngestError> {
    let planned_stream_count = config
        .stream_config
        .combined_stream_names_for_kinds(&config.stream_kinds)?
        .len();

    log_stream::info(
        "market_ingest_start",
        serde_json::json!({
            "venue": "binance",
            "source_role": "reference",
            "symbol_count": config.markets.len(),
            "planned_stream_count": planned_stream_count,
            "duration_seconds": config.duration_seconds,
            "depth_snapshot_limit": config.depth_snapshot_limit,
            "stream_kinds": config
                .stream_kinds
                .iter()
                .map(|kind| kind.name())
                .collect::<Vec<_>>()
        }),
    )?;

    let mut storage_sink = match config.l0_storage.clone() {
        Some(storage_config) => Some(
            L0StorageSink::new(storage_config)
                .await
                .map_err(|error| BinanceIngestError::Storage(error.to_string()))?,
        ),
        None => None,
    };

    let rest_depth_snapshot_records = if let Some(sink) = storage_sink.as_mut() {
        append_depth_snapshots(&config, sink).await?
    } else {
        0
    };
    let derivative_snapshot_report = if let Some(sink) = storage_sink.as_mut() {
        let report = derivatives::append_derivative_snapshots(
            &config.futures_rest_base_url,
            &config.markets,
            clock::now_ms(),
            sink,
        )
        .await?;
        sink.flush_all()
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
        report
    } else {
        derivatives::BinanceDerivativeSnapshotReport::default()
    };

    let mut stats = ws::watch_binance_l0_streams(
        &config.stream_config,
        &config.markets,
        &config.stream_kinds,
        ws::BinanceL0WatchConfig {
            duration: Duration::from_secs(config.duration_seconds),
            log_interval: Duration::from_secs(config.log_interval_seconds),
            derivative_snapshot_interval: Duration::from_secs(
                config.derivative_snapshot_interval_seconds,
            ),
            futures_rest_base_url: &config.futures_rest_base_url,
        },
        storage_sink.as_mut(),
        logging::print_binance_ingest_log,
    )
    .await?;
    stats.depth_snapshot_messages += rest_depth_snapshot_records;
    if let Some(sink) = storage_sink.as_mut() {
        finalize_storage(sink, &stats).await?;
    }
    let storage = storage_sink.as_ref().map(L0StorageSink::report);
    let allow_partial_symbol_coverage = config.allow_partial_symbol_coverage;
    let report = build_report(BinanceReportInput {
        config,
        planned_stream_count,
        rest_depth_snapshot_records,
        derivative_snapshot_report,
        storage,
        stats,
    });

    log_stream::info("market_ingest_report", &report)?;
    validate_report(&report, allow_partial_symbol_coverage)
}
