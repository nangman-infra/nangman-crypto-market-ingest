mod events;
mod stats;
mod universe;
mod ws;

use self::stats::UpbitIngestWatchStats;
pub use self::universe::{UpbitMarket, fetch_top_krw_markets};
use crate::clock;
use crate::log_stream;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::smoke_validation;
use crate::storage::symbol_health::SymbolHealthDraft;
use crate::storage::{L0StorageConfig, L0StorageSink, StorageReport};
use serde::Serialize;
use std::collections::HashSet;
use std::fmt;
use std::str::Utf8Error;
use std::time::Duration;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Clone)]
pub struct UpbitRunConfig {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub quote_currency: String,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub orderbook_unit: u8,
    pub l0_storage: Option<L0StorageConfig>,
}

#[derive(Debug, Serialize)]
struct UpbitL0SmokeReport {
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

#[derive(Debug)]
pub enum UpbitIngestError {
    Http(reqwest::Error),
    Json(serde_json::Error),
    WebSocket(tungstenite::Error),
    Utf8(Utf8Error),
    Storage(String),
    InvalidConfig(String),
    InvalidMessage(String),
}

impl fmt::Display for UpbitIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => write!(f, "upbit http error: {error}"),
            Self::Json(error) => write!(f, "upbit json error: {error}"),
            Self::WebSocket(error) => write!(f, "upbit websocket error: {error}"),
            Self::Utf8(error) => write!(f, "upbit utf-8 error: {error}"),
            Self::Storage(error) => write!(f, "upbit storage error: {error}"),
            Self::InvalidConfig(message) => write!(f, "upbit invalid config: {message}"),
            Self::InvalidMessage(message) => write!(f, "upbit invalid message: {message}"),
        }
    }
}

impl std::error::Error for UpbitIngestError {}

impl From<reqwest::Error> for UpbitIngestError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for UpbitIngestError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<tungstenite::Error> for UpbitIngestError {
    fn from(value: tungstenite::Error) -> Self {
        Self::WebSocket(value)
    }
}

impl From<Utf8Error> for UpbitIngestError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

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

    let report = UpbitL0SmokeReport {
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
    };

    log_stream::info("market_ingest_report", &report)?;
    validate_report(&report, config.allow_partial_symbol_coverage)
}

async fn finalize_storage(
    sink: &mut L0StorageSink,
    stats: &UpbitIngestWatchStats,
) -> Result<(), UpbitIngestError> {
    sink.append_source_health(source_health_draft(stats))
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    for draft in symbol_health_drafts(stats) {
        sink.append_symbol_health(draft)
            .await
            .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    }
    for alert in &stats.gap_alerts {
        sink.append_gap_alert(gap_alert_draft(alert))
            .await
            .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    }
    sink.flush_all()
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    sink.upload_manifest()
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))
}

fn validate_config(config: &UpbitRunConfig) -> Result<(), UpbitIngestError> {
    if config.quote_currency != "KRW" {
        return Err(UpbitIngestError::InvalidConfig(
            "initial Upbit L0 ingest only supports KRW quote markets".to_owned(),
        ));
    }
    if !matches!(config.orderbook_unit, 1 | 5 | 15 | 30) {
        return Err(UpbitIngestError::InvalidConfig(
            "upbit orderbook unit must be one of 1, 5, 15, or 30".to_owned(),
        ));
    }
    Ok(())
}

fn validate_report(
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

fn print_upbit_ingest_log(stats: &UpbitIngestWatchStats) {
    let _ = log_stream::debug(
        "market_ingest_progress",
        serde_json::json!({
            "venue": "upbit",
            "source_role": "execution",
            "health": stats.source_health_status,
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "ticker_messages": stats.ticker_messages,
            "trade_messages": stats.trade_messages,
            "orderbook_messages": stats.orderbook_messages,
            "derived_book_tickers": stats.derived_book_tickers,
            "gap_alert_count": stats.gap_alert_count,
            "malformed_messages": stats.malformed_messages,
            "close_messages": stats.close_messages
        }),
    );
}

fn source_health_draft(stats: &UpbitIngestWatchStats) -> SourceHealthDraft {
    let observed_at_ms = clock::now_ms();
    SourceHealthDraft {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        observed_at_ms,
        connection_status: stats.source_health_status.clone(),
        heartbeat_delay_ms: stats.heartbeat_delay_ms_at(observed_at_ms),
        stream_lag_ms: stats.latest_stream_lag_ms,
        recent_gap_count: stats.gap_alert_count,
        book_rebuild_count: 0,
        health_level: health_level(stats),
        payload_json: serde_json::json!({
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "malformed_messages": stats.malformed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "ticker_messages": stats.ticker_messages,
            "trade_messages": stats.trade_messages,
            "orderbook_messages": stats.orderbook_messages,
            "derived_book_tickers": stats.derived_book_tickers,
            "gap_alert_count": stats.gap_alert_count,
            "last_exchange_timestamp_ms": stats.last_exchange_timestamp_ms,
            "last_ingest_timestamp_ms": stats.last_ingest_timestamp_ms,
            "stream_lag_ms": stats.latest_stream_lag_ms,
            "symbol_health": symbol_health_payload(stats, observed_at_ms)
        })
        .to_string(),
    }
}

fn symbol_health_payload(
    stats: &UpbitIngestWatchStats,
    observed_at_ms: i64,
) -> Vec<serde_json::Value> {
    stats
        .symbol_counts
        .keys()
        .map(|symbol| {
            let last_event_time_ms = stats.symbol_last_event_time_ms.get(symbol).copied();
            let last_received_time_ms = stats.symbol_last_ingest_time_ms.get(symbol).copied();
            let latency_ms = last_event_time_ms
                .zip(last_received_time_ms)
                .map(|(event, received)| received.saturating_sub(event).max(0));
            let stale_ms = last_received_time_ms
                .map(|received| observed_at_ms.saturating_sub(received).max(0))
                .unwrap_or(0);
            serde_json::json!({
                "symbol_native": symbol,
                "last_event_time_ms": last_event_time_ms,
                "last_received_time_ms": last_received_time_ms,
                "latency_ms": latency_ms,
                "stale_ms": stale_ms,
                "is_tradeable": stale_ms < 60_000 && stats.gap_alert_count == 0,
                "reason_codes": if stale_ms >= 60_000 {
                    vec!["source_stale"]
                } else {
                    Vec::<&str>::new()
                }
            })
        })
        .collect()
}

fn symbol_health_drafts(stats: &UpbitIngestWatchStats) -> Vec<SymbolHealthDraft> {
    let observed_at_ms = clock::now_ms();
    stats
        .symbol_counts
        .keys()
        .map(|symbol| {
            let last_event_time_ms = stats
                .symbol_last_event_time_ms
                .get(symbol)
                .copied()
                .unwrap_or(observed_at_ms);
            let last_received_time_ms = stats
                .symbol_last_ingest_time_ms
                .get(symbol)
                .copied()
                .unwrap_or(observed_at_ms);
            let stale_ms = observed_at_ms.saturating_sub(last_received_time_ms).max(0);
            let mut reason_codes = Vec::new();
            if stale_ms >= 60_000 {
                reason_codes.push("source_stale".to_owned());
            }
            if stats.gap_alert_count > 0 {
                reason_codes.push("source_gap_detected".to_owned());
            }
            SymbolHealthDraft {
                venue: "upbit".to_owned(),
                symbol_native: symbol.clone(),
                observed_at_ms,
                last_event_time_ms,
                latency_ms: last_received_time_ms
                    .saturating_sub(last_event_time_ms)
                    .max(0),
                is_tradeable: reason_codes.is_empty(),
                reason_codes,
            }
        })
        .collect()
}

fn gap_alert_draft(alert: &stats::UpbitGapAlert) -> GapAlertDraft {
    GapAlertDraft {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        symbol_native: alert.symbol.clone(),
        gap_type: alert.gap_type.clone(),
        detected_at_ms: alert.detected_at_ms,
        expected_sequence_id: alert.expected_sequence_id,
        observed_sequence_id: alert.observed_sequence_id,
        heal_action: alert.heal_action.clone(),
        heal_status: alert.heal_status.clone(),
        payload_json: serde_json::to_string(alert).unwrap_or_else(|_| "{}".to_owned()),
    }
}

fn health_level(stats: &UpbitIngestWatchStats) -> String {
    if stats.source_health_status == "connected"
        && stats.malformed_messages == 0
        && stats.gap_alert_count == 0
    {
        "healthy".to_owned()
    } else if stats.received_messages == 0 {
        "critical".to_owned()
    } else {
        "degraded".to_owned()
    }
}
