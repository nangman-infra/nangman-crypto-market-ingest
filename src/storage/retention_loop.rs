use super::StorageError;
use super::retention::{S3RetentionConfig, S3RetentionStats, run_s3_retention_once};
use crate::log_stream;
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3RetentionLoopEvents {
    pub run_event: &'static str,
    pub error_event: &'static str,
}

pub fn spawn_s3_retention_loop(
    layer: &'static str,
    config: S3RetentionConfig,
    interval_secs: u64,
    events: S3RetentionLoopEvents,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_s3_retention_loop(layer, config, interval_secs, events).await;
    })
}

pub async fn abort_s3_retention_handles(handles: Vec<JoinHandle<()>>) {
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
}

async fn run_s3_retention_loop(
    layer: &'static str,
    config: S3RetentionConfig,
    interval_secs: u64,
    events: S3RetentionLoopEvents,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let now_ms = unix_timestamp_millis();
        match run_s3_retention_once(&config, now_ms).await {
            Ok(stats) => log_s3_retention_run(events.run_event, layer, &config, &stats),
            Err(error) => log_s3_retention_error(events.error_event, layer, &config, error),
        }
    }
}

fn log_s3_retention_run(
    event: &'static str,
    layer: &'static str,
    config: &S3RetentionConfig,
    stats: &S3RetentionStats,
) {
    let _ = log_stream::info(
        event,
        json!({
            "layer": layer,
            "bucket": &config.bucket,
            "retention_secs": config.retention_secs,
            "max_deletes_per_run": config.max_deletes_per_run,
            "scanned_object_count": stats.scanned_object_count,
            "expired_object_count": stats.expired_object_count,
            "deleted_object_count": stats.deleted_object_count,
            "failed_delete_count": stats.failed_delete_count,
            "deleted_bytes": stats.deleted_bytes,
            "max_deleted_age_secs": stats.max_deleted_age_secs,
            "stopped_at_delete_limit": stats.stopped_at_delete_limit
        }),
    );
}

fn log_s3_retention_error(
    event: &'static str,
    layer: &'static str,
    config: &S3RetentionConfig,
    error: StorageError,
) {
    let _ = log_stream::warn(
        event,
        json!({
            "layer": layer,
            "bucket": &config.bucket,
            "error": error.to_string()
        }),
    );
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
