use super::derivatives::BinanceDerivativeSnapshotReport;
use super::stats::BinanceL0WatchStats;
use super::{BinanceIngestError, BinanceRunConfig};
use crate::storage::StorageReport;
use crate::storage::smoke_validation;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(super) struct BinanceL0SmokeReport {
    venue: String,
    source_role: String,
    config_dir: String,
    duration_seconds: u64,
    log_interval_seconds: u64,
    depth_snapshot_limit: u16,
    rest_depth_snapshot_records: u64,
    funding_rate_snapshot_records: u64,
    open_interest_snapshot_records: u64,
    derivative_snapshot_records: u64,
    derivative_snapshot_unsupported_symbol_count: u64,
    derivative_snapshot_failure_count: u64,
    symbol_count: usize,
    expected_symbol_count: usize,
    planned_stream_count: usize,
    stream_kinds: Vec<&'static str>,
    symbols_seen_count: usize,
    missing_symbols: Vec<String>,
    coverage_ok: bool,
    received_messages: u64,
    parsed_messages: u64,
    malformed_messages: u64,
    source_health_status: String,
    ticker_messages: u64,
    trade_messages: u64,
    book_ticker_messages: u64,
    depth_delta_messages: u64,
    depth_snapshot_messages: u64,
    gap_alert_count: u64,
    close_messages: u64,
    storage: Option<StorageReport>,
    stats: BinanceL0WatchStats,
}

pub(super) struct BinanceReportInput {
    pub(super) config: BinanceRunConfig,
    pub(super) planned_stream_count: usize,
    pub(super) rest_depth_snapshot_records: u64,
    pub(super) derivative_snapshot_report: BinanceDerivativeSnapshotReport,
    pub(super) storage: Option<StorageReport>,
    pub(super) stats: BinanceL0WatchStats,
}

pub(super) fn build_report(input: BinanceReportInput) -> BinanceL0SmokeReport {
    let BinanceReportInput {
        config,
        planned_stream_count,
        rest_depth_snapshot_records,
        derivative_snapshot_report,
        storage,
        stats,
    } = input;
    let expected_symbols = config
        .markets
        .iter()
        .map(|market| market.raw_symbol.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let missing_symbols = expected_symbols
        .iter()
        .filter(|symbol| !stats.symbol_counts.contains_key(symbol.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let coverage_ok = missing_symbols.is_empty();

    BinanceL0SmokeReport {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        config_dir: config.config_dir,
        duration_seconds: config.duration_seconds,
        log_interval_seconds: config.log_interval_seconds,
        depth_snapshot_limit: config.depth_snapshot_limit,
        rest_depth_snapshot_records,
        funding_rate_snapshot_records: derivative_snapshot_report.funding_rate_snapshot_records,
        open_interest_snapshot_records: derivative_snapshot_report.open_interest_snapshot_records,
        derivative_snapshot_records: derivative_snapshot_report.total_records(),
        derivative_snapshot_unsupported_symbol_count: derivative_snapshot_report
            .unsupported_futures_symbol_count,
        derivative_snapshot_failure_count: derivative_snapshot_report.failure_count,
        symbol_count: config.markets.len(),
        expected_symbol_count: config.expect_symbol_count,
        planned_stream_count,
        stream_kinds: config.stream_kinds.iter().map(|kind| kind.name()).collect(),
        symbols_seen_count: stats.symbol_counts.len(),
        missing_symbols,
        coverage_ok,
        received_messages: stats.received_messages,
        parsed_messages: stats.parsed_messages,
        malformed_messages: stats.malformed_messages,
        source_health_status: stats.source_health_status.clone(),
        ticker_messages: stats.ticker_messages,
        trade_messages: stats.trade_messages,
        book_ticker_messages: stats.book_ticker_messages,
        depth_delta_messages: stats.depth_delta_messages,
        depth_snapshot_messages: stats.depth_snapshot_messages,
        gap_alert_count: stats.gap_alert_count,
        close_messages: stats.close_messages,
        storage,
        stats,
    }
}

pub(super) fn validate_report(
    report: &BinanceL0SmokeReport,
    allow_partial_symbol_coverage: bool,
) -> Result<(), BinanceIngestError> {
    validate_message_report(report, allow_partial_symbol_coverage)?;
    if let Some(storage) = &report.storage {
        validate_storage_report(report, storage)?;
    }
    Ok(())
}

fn validate_message_report(
    report: &BinanceL0SmokeReport,
    allow_partial_symbol_coverage: bool,
) -> Result<(), BinanceIngestError> {
    if report.received_messages == 0 {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance ingest smoke received zero messages".to_owned(),
        ));
    }
    if !report.coverage_ok && !allow_partial_symbol_coverage {
        return Err(BinanceIngestError::InvalidMessage(format!(
            "Binance symbol coverage incomplete: {} missing symbols",
            report.missing_symbols.len()
        )));
    }
    if report.malformed_messages > 0 {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance ingest smoke observed malformed messages".to_owned(),
        ));
    }
    Ok(())
}

fn validate_storage_report(
    report: &BinanceL0SmokeReport,
    storage: &StorageReport,
) -> Result<(), BinanceIngestError> {
    smoke_validation::validate_common_storage_report(storage, "Binance")
        .map_err(BinanceIngestError::InvalidMessage)?;
    if report.gap_alert_count > 0 && !smoke_validation::storage_has_family(storage, "gap_alert") {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance L0 storage observed gaps but uploaded no gap_alert".to_owned(),
        ));
    }
    if report.depth_snapshot_messages == 0 {
        return Err(BinanceIngestError::InvalidMessage(
            "Binance L0 storage did not record REST depth_snapshot events".to_owned(),
        ));
    }
    Ok(())
}
