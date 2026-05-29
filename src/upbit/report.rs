use super::stats::UpbitIngestWatchStats;
use super::{UpbitIngestError, UpbitMarket, UpbitRunConfig};
use crate::storage::StorageReport;
use crate::storage::smoke_validation;
use serde::Serialize;
use std::collections::HashSet;

#[derive(Debug, Serialize)]
pub(super) struct UpbitL0SmokeReport {
    venue: String,
    source_role: String,
    rest_base_url: String,
    websocket_url: String,
    quote_currency: String,
    duration_seconds: u64,
    log_interval_seconds: u64,
    orderbook_unit: u8,
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
    orderbook_messages: u64,
    derived_book_tickers: u64,
    gap_alert_count: u64,
    close_messages: u64,
    storage: Option<StorageReport>,
    stats: UpbitIngestWatchStats,
}

pub(super) struct UpbitReportInput {
    pub(super) config: UpbitRunConfig,
    pub(super) markets: Vec<UpbitMarket>,
    pub(super) planned_stream_count: usize,
    pub(super) storage: Option<StorageReport>,
    pub(super) stats: UpbitIngestWatchStats,
}

pub(super) fn build_report(input: UpbitReportInput) -> UpbitL0SmokeReport {
    let UpbitReportInput {
        config,
        markets,
        planned_stream_count,
        storage,
        stats,
    } = input;
    let expected_symbols = markets
        .iter()
        .map(|market| market.market.clone())
        .collect::<Vec<_>>();
    let expected_set = expected_symbols.iter().cloned().collect::<HashSet<_>>();
    let missing_symbols = expected_symbols
        .iter()
        .filter(|symbol| !stats.symbol_counts.contains_key(symbol.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let coverage_ok = missing_symbols.is_empty();

    UpbitL0SmokeReport {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        rest_base_url: config.rest_base_url,
        websocket_url: config.websocket_url,
        quote_currency: config.quote_currency,
        duration_seconds: config.duration_seconds,
        log_interval_seconds: config.log_interval_seconds,
        orderbook_unit: config.orderbook_unit,
        symbol_count: markets.len(),
        expected_symbol_count: expected_set.len(),
        planned_stream_count,
        stream_kinds: vec!["ticker", "trade", "orderbook"],
        symbols_seen_count: stats.symbol_counts.len(),
        missing_symbols,
        coverage_ok,
        received_messages: stats.received_messages,
        parsed_messages: stats.parsed_messages,
        malformed_messages: stats.malformed_messages,
        source_health_status: stats.source_health_status.clone(),
        ticker_messages: stats.ticker_messages,
        trade_messages: stats.trade_messages,
        orderbook_messages: stats.orderbook_messages,
        derived_book_tickers: stats.derived_book_tickers,
        gap_alert_count: stats.gap_alert_count,
        close_messages: stats.close_messages,
        storage,
        stats,
    }
}

pub(super) fn validate_report(
    report: &UpbitL0SmokeReport,
    allow_partial_symbol_coverage: bool,
) -> Result<(), UpbitIngestError> {
    validate_message_report(report, allow_partial_symbol_coverage)?;
    if let Some(storage) = &report.storage {
        validate_storage_report(report, storage)?;
    }
    Ok(())
}

fn validate_message_report(
    report: &UpbitL0SmokeReport,
    allow_partial_symbol_coverage: bool,
) -> Result<(), UpbitIngestError> {
    if report.received_messages == 0 {
        return Err(UpbitIngestError::InvalidMessage(
            "Upbit ingest smoke received zero messages".to_owned(),
        ));
    }
    if !report.coverage_ok && !allow_partial_symbol_coverage {
        return Err(UpbitIngestError::InvalidMessage(format!(
            "Upbit symbol coverage incomplete: {} missing symbols",
            report.missing_symbols.len()
        )));
    }
    if report.malformed_messages > 0 {
        return Err(UpbitIngestError::InvalidMessage(
            "Upbit ingest smoke observed malformed messages".to_owned(),
        ));
    }
    if report.orderbook_messages > 0 && report.derived_book_tickers == 0 {
        return Err(UpbitIngestError::InvalidMessage(
            "Upbit ingest smoke derived zero book_ticker events".to_owned(),
        ));
    }
    Ok(())
}

fn validate_storage_report(
    report: &UpbitL0SmokeReport,
    storage: &StorageReport,
) -> Result<(), UpbitIngestError> {
    smoke_validation::validate_common_storage_report(storage, "Upbit")
        .map_err(UpbitIngestError::InvalidMessage)?;
    if report.gap_alert_count > 0 && !smoke_validation::storage_has_family(storage, "gap_alert") {
        return Err(UpbitIngestError::InvalidMessage(
            "Upbit L0 storage observed gaps but uploaded no gap_alert".to_owned(),
        ));
    }
    Ok(())
}
