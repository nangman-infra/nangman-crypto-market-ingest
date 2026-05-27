pub mod args;
mod binance;
mod upbit;

use crate::clock;
use crate::log_stream;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::symbol_health::SymbolHealthDraft;
use crate::storage::{
    L0StorageConfig, L0StorageSink, S3RetentionConfig, StorageReport,
    default_l0_retention_prefixes, run_s3_retention_once,
};
use serde::Serialize;
use serde_json::json;
use std::fmt;

pub use args::{BackfillArgs, Venue, parse_args, print_help};

#[derive(Debug)]
pub enum BackfillError {
    Http(reqwest::Error),
    Json(serde_json::Error),
    Storage(String),
    InvalidArgs(String),
    InvalidConfig(String),
}

impl fmt::Display for BackfillError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http(error) => write!(f, "market backfill http error: {error}"),
            Self::Json(error) => write!(f, "market backfill json error: {error}"),
            Self::Storage(error) => write!(f, "market backfill storage error: {error}"),
            Self::InvalidArgs(error) => write!(f, "market backfill invalid args: {error}"),
            Self::InvalidConfig(error) => write!(f, "market backfill invalid config: {error}"),
        }
    }
}

impl std::error::Error for BackfillError {}

impl From<reqwest::Error> for BackfillError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<serde_json::Error> for BackfillError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolBackfillReport {
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub record_count: u64,
    pub first_event_time_ms: Option<i64>,
    pub last_event_time_ms: Option<i64>,
    pub gap_alert_count: u64,
    pub status: String,
}

impl SymbolBackfillReport {
    fn empty(symbol_native: impl Into<String>, symbol_canonical: impl Into<String>) -> Self {
        Self {
            symbol_native: symbol_native.into(),
            symbol_canonical: symbol_canonical.into(),
            record_count: 0,
            first_event_time_ms: None,
            last_event_time_ms: None,
            gap_alert_count: 0,
            status: "empty".to_owned(),
        }
    }

    fn observe(&mut self, exchange_timestamp_ms: i64) {
        self.record_count += 1;
        self.first_event_time_ms = Some(
            self.first_event_time_ms
                .map(|current| current.min(exchange_timestamp_ms))
                .unwrap_or(exchange_timestamp_ms),
        );
        self.last_event_time_ms = Some(
            self.last_event_time_ms
                .map(|current| current.max(exchange_timestamp_ms))
                .unwrap_or(exchange_timestamp_ms),
        );
        self.status = "ok".to_owned();
    }

    fn note_gap(&mut self) {
        self.gap_alert_count += 1;
        if self.record_count > 0 {
            self.status = "partial".to_owned();
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BackfillRunReport {
    pub venue: String,
    pub source_role: String,
    pub input_start_ms: i64,
    pub input_end_ms: i64,
    pub requested_symbol_count: usize,
    pub processed_symbol_count: usize,
    pub total_record_count: u64,
    pub total_gap_alert_count: u64,
    pub symbols: Vec<SymbolBackfillReport>,
    pub storage: StorageReport,
}

pub async fn run_backfill(args: BackfillArgs) -> Result<(), BackfillError> {
    let venue = args.venue.as_str().to_owned();
    let mut sink = L0StorageSink::new(storage_config(&args))
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    let mut report = match args.venue {
        Venue::Binance => binance::run(&args, &mut sink).await?,
        Venue::Upbit => upbit::run(&args, &mut sink).await?,
    };

    sink.flush_all()
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    sink.upload_manifest()
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    report.storage = sink.report();

    log_stream::info("market_backfill_report", &report).map_err(BackfillError::Json)?;
    log_stream::info(
        "market_backfill_done",
        serde_json::json!({
            "venue": venue,
            "run_id": report.storage.run_id,
            "record_count": report.total_record_count,
            "uploaded_object_count": report.storage.uploaded_object_count,
            "manifest_key": report.storage.manifest_key,
        }),
    )
    .map_err(BackfillError::Json)?;
    log_l0_retention_cleanup(&args).await?;
    Ok(())
}

async fn log_l0_retention_cleanup(args: &BackfillArgs) -> Result<(), BackfillError> {
    if !args.s3_retention_enabled {
        return Ok(());
    }
    let config = S3RetentionConfig {
        bucket: args.l0_s3_bucket.clone(),
        region: args.aws_region.clone(),
        profile: args.aws_profile.clone(),
        prefixes: default_l0_retention_prefixes(),
        protected_prefixes: Vec::new(),
        retention_secs: args.s3_retention_days.saturating_mul(86_400),
        max_deletes_per_run: args.s3_retention_max_deletes_per_run,
    };
    match run_s3_retention_once(&config, clock::now_ms()).await {
        Ok(stats) => log_stream::info(
            "market_backfill_s3_retention_run",
            serde_json::json!({
                "bucket": &config.bucket,
                "retention_secs": config.retention_secs,
                "max_deletes_per_run": config.max_deletes_per_run,
                "scanned_object_count": stats.scanned_object_count,
                "expired_object_count": stats.expired_object_count,
                "deleted_object_count": stats.deleted_object_count,
                "failed_delete_count": stats.failed_delete_count,
                "deleted_bytes": stats.deleted_bytes,
                "stopped_at_delete_limit": stats.stopped_at_delete_limit
            }),
        )
        .map_err(BackfillError::Json),
        Err(error) => log_stream::warn(
            "market_backfill_s3_retention_error",
            serde_json::json!({
                "bucket": &config.bucket,
                "error": error.to_string()
            }),
        )
        .map_err(BackfillError::Json),
    }
}

fn storage_config(args: &BackfillArgs) -> L0StorageConfig {
    L0StorageConfig {
        bucket: args.l0_s3_bucket.clone(),
        region: args.aws_region.clone(),
        profile: args.aws_profile.clone(),
        spool_root: args.l0_spool_root.clone(),
        run_id: format!(
            "market-backfill-{}-{}",
            args.venue.as_str(),
            clock::now_secs()
        ),
        flush_records: args.l0_flush_records,
        shard_count: args.l0_shard_count,
        live_nats: None,
    }
}

pub(crate) async fn append_empty_gap_alert(
    sink: &mut L0StorageSink,
    venue: &str,
    source_role: &str,
    symbol_native: &str,
    input_start_ms: i64,
    input_end_ms: i64,
    reason: &str,
) -> Result<(), BackfillError> {
    sink.append_gap_alert(GapAlertDraft {
        venue: venue.to_owned(),
        source_role: source_role.to_owned(),
        symbol_native: symbol_native.to_owned(),
        gap_type: "historical_range_empty".to_owned(),
        detected_at_ms: clock::now_ms(),
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "review_range_or_source".to_owned(),
        heal_status: "open".to_owned(),
        payload_json: serde_json::to_string(&json!({
            "input_start_ms": input_start_ms,
            "input_end_ms": input_end_ms,
            "reason": reason
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}

pub(crate) async fn append_symbol_health_for(
    sink: &mut L0StorageSink,
    venue: &str,
    symbols: &[SymbolBackfillReport],
    observed_at_ms: i64,
) -> Result<(), BackfillError> {
    for symbol in symbols {
        let last_event_time_ms = symbol.last_event_time_ms.unwrap_or(0);
        sink.append_symbol_health(SymbolHealthDraft {
            venue: venue.to_owned(),
            symbol_native: symbol.symbol_native.clone(),
            observed_at_ms,
            last_event_time_ms,
            latency_ms: observed_at_ms.saturating_sub(last_event_time_ms).max(0),
            is_tradeable: symbol.record_count > 0,
            reason_codes: if symbol.record_count > 0 {
                Vec::new()
            } else {
                vec!["no_historical_trades".to_owned()]
            },
        })
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    }
    Ok(())
}

pub(crate) struct SourceHealthSummary<'a> {
    pub venue: &'a str,
    pub source_role: &'a str,
    pub mode: &'a str,
    pub observed_at_ms: i64,
    pub args: &'a BackfillArgs,
    pub symbol_count: usize,
    pub total_record_count: u64,
    pub total_gap_alert_count: u64,
}

pub(crate) async fn append_source_health_for(
    sink: &mut L0StorageSink,
    summary: SourceHealthSummary<'_>,
) -> Result<(), BackfillError> {
    sink.append_source_health(SourceHealthDraft {
        venue: summary.venue.to_owned(),
        source_role: summary.source_role.to_owned(),
        observed_at_ms: summary.observed_at_ms,
        connection_status: "historical_backfill_completed".to_owned(),
        heartbeat_delay_ms: 0,
        stream_lag_ms: summary
            .observed_at_ms
            .saturating_sub(summary.args.input_end_ms)
            .max(0),
        recent_gap_count: summary.total_gap_alert_count,
        book_rebuild_count: 0,
        health_level: if summary.total_gap_alert_count == 0 {
            "ok"
        } else {
            "warn"
        }
        .to_owned(),
        payload_json: serde_json::to_string(&json!({
            "mode": summary.mode,
            "symbol_count": summary.symbol_count,
            "input_start_ms": summary.args.input_start_ms,
            "input_end_ms": summary.args.input_end_ms,
            "record_count": summary.total_record_count,
            "gap_alert_count": summary.total_gap_alert_count
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}

pub(crate) fn empty_storage_report() -> StorageReport {
    StorageReport {
        bucket: String::new(),
        run_id: String::new(),
        record_count: 0,
        uploaded_object_count: 0,
        uploaded_object_retained_count: 0,
        uploaded_object_dropped_count: 0,
        uploaded_objects: Vec::new(),
        failed_upload_count: 0,
        failed_upload_retained_count: 0,
        failed_upload_dropped_count: 0,
        failed_uploads: Vec::new(),
        manifest_key: None,
    }
}
