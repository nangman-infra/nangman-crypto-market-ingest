mod derivatives;
mod events;
mod rest;
mod stats;
mod ws;
use self::stats::BinanceL0WatchStats;
use crate::log_stream;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::symbol_health::SymbolHealthDraft;
use crate::storage::{L0StorageConfig, L0StorageSink, StorageReport};
use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind, MarketDataError};
use serde::Serialize;
use std::fmt;
use std::str::Utf8Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite;
#[derive(Debug, Clone)]
pub struct BinanceMarket {
    pub raw_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}
#[derive(Debug, Clone)]
pub struct BinanceRunConfig {
    pub config_dir: String,
    pub rest_base_url: String,
    pub futures_rest_base_url: String,
    pub stream_config: BinanceStreamConfig,
    pub markets: Vec<BinanceMarket>,
    pub duration_seconds: u64,
    pub log_interval_seconds: u64,
    pub depth_snapshot_limit: u16,
    pub expect_symbol_count: usize,
    pub allow_partial_symbol_coverage: bool,
    pub stream_kinds: Vec<BinanceStreamKind>,
    pub l0_storage: Option<L0StorageConfig>,
}
#[derive(Debug, Serialize)]
struct BinanceL0SmokeReport {
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
#[derive(Debug)]
pub enum BinanceIngestError {
    Http(reqwest::Error),
    MarketData(MarketDataError),
    Json(serde_json::Error),
    WebSocket(tungstenite::Error),
    Utf8(Utf8Error),
    Storage(String),
    InvalidMessage(String),
}

impl fmt::Display for BinanceIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MarketData(error) => write!(f, "binance market data error: {error}"),
            Self::Http(error) => write!(f, "binance http error: {error}"),
            Self::Json(error) => write!(f, "binance json error: {error}"),
            Self::WebSocket(error) => write!(f, "binance websocket error: {error}"),
            Self::Utf8(error) => write!(f, "binance utf-8 error: {error}"),
            Self::Storage(error) => write!(f, "binance storage error: {error}"),
            Self::InvalidMessage(message) => write!(f, "binance invalid message: {message}"),
        }
    }
}

impl std::error::Error for BinanceIngestError {}

impl From<MarketDataError> for BinanceIngestError {
    fn from(value: MarketDataError) -> Self {
        Self::MarketData(value)
    }
}

impl From<reqwest::Error> for BinanceIngestError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for BinanceIngestError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<tungstenite::Error> for BinanceIngestError {
    fn from(value: tungstenite::Error) -> Self {
        Self::WebSocket(value)
    }
}

impl From<Utf8Error> for BinanceIngestError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

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
        derivatives::append_derivative_snapshots(
            &config.futures_rest_base_url,
            &config.markets,
            unix_timestamp_ms(),
            sink,
        )
        .await?
    } else {
        derivatives::BinanceDerivativeSnapshotReport::default()
    };

    let mut stats = ws::watch_binance_l0_streams(
        &config.stream_config,
        &config.markets,
        &config.stream_kinds,
        Duration::from_secs(config.duration_seconds),
        Duration::from_secs(config.log_interval_seconds),
        storage_sink.as_mut(),
        print_binance_ingest_log,
    )
    .await?;
    stats.depth_snapshot_messages += rest_depth_snapshot_records;
    if let Some(sink) = storage_sink.as_mut() {
        finalize_storage(sink, &stats).await?;
    }
    let storage = storage_sink.as_ref().map(L0StorageSink::report);

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

    let report = BinanceL0SmokeReport {
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
    };

    log_stream::info("market_ingest_report", &report)?;
    validate_report(&report, config.allow_partial_symbol_coverage)
}

async fn append_depth_snapshots(
    config: &BinanceRunConfig,
    sink: &mut L0StorageSink,
) -> Result<u64, BinanceIngestError> {
    let client = reqwest::Client::new();
    let mut appended = 0;
    for market in &config.markets {
        let draft = rest::fetch_depth_snapshot_draft(
            &client,
            &config.rest_base_url,
            market,
            config.depth_snapshot_limit,
            unix_timestamp_ms(),
        )
        .await?;
        sink.append_raw_market_event(draft)
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
        appended += 1;
    }
    Ok(appended)
}

async fn finalize_storage(
    sink: &mut L0StorageSink,
    stats: &BinanceL0WatchStats,
) -> Result<(), BinanceIngestError> {
    sink.append_source_health(source_health_draft(stats))
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    for draft in symbol_health_drafts(stats) {
        sink.append_symbol_health(draft)
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    }
    for alert in &stats.gap_alerts {
        sink.append_gap_alert(gap_alert_draft(alert))
            .await
            .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    }
    sink.flush_all()
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))?;
    sink.upload_manifest()
        .await
        .map_err(|error| BinanceIngestError::Storage(error.to_string()))
}

fn validate_report(
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
    validate_common_storage_report(storage, "Binance")?;
    if report.gap_alert_count > 0 && !storage_has_family(storage, "gap_alert") {
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

fn validate_common_storage_report(
    storage: &StorageReport,
    venue: &str,
) -> Result<(), BinanceIngestError> {
    if storage.failed_upload_count > 0 {
        return Err(BinanceIngestError::InvalidMessage(format!(
            "{venue} L0 storage exhausted retries for {} uploads",
            storage.failed_upload_count
        )));
    }
    if storage.record_count == 0 || storage.uploaded_object_count == 0 {
        return Err(BinanceIngestError::InvalidMessage(format!(
            "{venue} L0 storage produced no objects"
        )));
    }
    if storage.manifest_key.is_none() {
        return Err(BinanceIngestError::InvalidMessage(format!(
            "{venue} L0 storage did not upload manifest.json"
        )));
    }
    require_storage_family(storage, venue, "source_health")?;
    require_storage_family(storage, venue, "symbol_health")
}

fn require_storage_family(
    storage: &StorageReport,
    venue: &str,
    object_family: &str,
) -> Result<(), BinanceIngestError> {
    if storage_has_family(storage, object_family) {
        return Ok(());
    }
    Err(BinanceIngestError::InvalidMessage(format!(
        "{venue} L0 storage did not upload {object_family}"
    )))
}

fn storage_has_family(storage: &StorageReport, object_family: &str) -> bool {
    storage
        .uploaded_objects
        .iter()
        .any(|object| object.object_family == object_family)
}

fn print_binance_ingest_log(stats: &BinanceL0WatchStats) {
    let _ = log_stream::debug(
        "market_ingest_progress",
        serde_json::json!({
            "venue": "binance",
            "source_role": "reference",
            "health": stats.source_health_status,
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "trade_messages": stats.trade_messages,
            "book_ticker_messages": stats.book_ticker_messages,
            "ticker_messages": stats.ticker_messages,
            "depth_delta_messages": stats.depth_delta_messages,
            "depth_snapshot_messages": stats.depth_snapshot_messages,
            "gap_alert_count": stats.gap_alert_count,
            "malformed_messages": stats.malformed_messages,
            "close_messages": stats.close_messages
        }),
    );
}

fn source_health_draft(stats: &BinanceL0WatchStats) -> SourceHealthDraft {
    let observed_at_ms = unix_timestamp_ms();
    SourceHealthDraft {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
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
            "book_ticker_messages": stats.book_ticker_messages,
            "depth_delta_messages": stats.depth_delta_messages,
            "depth_snapshot_messages": stats.depth_snapshot_messages,
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
    stats: &BinanceL0WatchStats,
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

fn symbol_health_drafts(stats: &BinanceL0WatchStats) -> Vec<SymbolHealthDraft> {
    let observed_at_ms = unix_timestamp_ms();
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
                venue: "binance".to_owned(),
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

fn gap_alert_draft(alert: &stats::BinanceL0GapAlert) -> GapAlertDraft {
    GapAlertDraft {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
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

fn health_level(stats: &BinanceL0WatchStats) -> String {
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

fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
