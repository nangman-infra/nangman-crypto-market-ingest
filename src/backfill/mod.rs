pub mod args;
mod binance;
mod upbit;

use crate::log_stream;
use crate::storage::{L0StorageConfig, L0StorageSink, StorageReport};
use serde::Serialize;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

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
    Ok(())
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
            unix_timestamp_seconds()
        ),
        flush_records: args.l0_flush_records,
        shard_count: args.l0_shard_count,
    }
}

pub(crate) fn unix_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn unix_timestamp_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}
