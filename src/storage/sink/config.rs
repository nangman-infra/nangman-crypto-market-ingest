use crate::live::{LiveMarketNatsConfig, LiveMarketPublisher};
use crate::log_stream;
use crate::storage::StorageError;
use serde::Serialize;
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize)]
pub struct L0StorageConfig {
    pub bucket: String,
    pub region: String,
    pub profile: Option<String>,
    pub spool_root: PathBuf,
    pub run_id: String,
    pub flush_records: usize,
    pub shard_count: u16,
    pub live_nats: Option<LiveMarketNatsConfig>,
}

pub(super) fn validate_config(config: &L0StorageConfig) -> Result<(), StorageError> {
    if config.bucket.is_empty() {
        return Err(StorageError::InvalidConfig(
            "l0 storage bucket is required".to_owned(),
        ));
    }
    if config.flush_records == 0 {
        return Err(StorageError::InvalidConfig(
            "l0 flush records must be positive".to_owned(),
        ));
    }
    if config.shard_count == 0 {
        return Err(StorageError::InvalidConfig(
            "l0 shard count must be positive".to_owned(),
        ));
    }
    if let Some(live_nats) = &config.live_nats {
        if live_nats.url.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS URL must not be empty when configured".to_owned(),
            ));
        }
        if live_nats.stream.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS stream must not be empty when configured".to_owned(),
            ));
        }
        if live_nats.subject_prefix.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS subject prefix must not be empty when configured".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(super) async fn connect_live_publisher(
    config: &L0StorageConfig,
) -> Result<Option<LiveMarketPublisher>, StorageError> {
    let Some(live_nats) = &config.live_nats else {
        return Ok(None);
    };
    match LiveMarketPublisher::connect(live_nats).await {
        Ok(publisher) => Ok(Some(publisher)),
        Err(error) if live_nats.required => Err(StorageError::Nats(format!(
            "connect market live publisher {}: {error}",
            live_nats.url
        ))),
        Err(error) => {
            let _ = log_stream::warn(
                "market_live_tick_publisher_disabled",
                json!({
                    "url": live_nats.url,
                    "stream": live_nats.stream,
                    "subject_prefix": live_nats.subject_prefix,
                    "error": error.to_string(),
                    "required": false
                }),
            );
            Ok(None)
        }
    }
}

pub(super) fn live_nats_required(config: &L0StorageConfig) -> bool {
    config
        .live_nats
        .as_ref()
        .map(|live_nats| live_nats.required)
        .unwrap_or(false)
}
