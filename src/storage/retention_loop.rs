use super::StorageError;
use super::retention::{
    S3RetentionConfig, S3RetentionStats, l0_s3_retention_config, l1_s3_retention_config,
    run_s3_retention_once,
};
use crate::clock;
use crate::log_stream;
use serde_json::json;
use std::time::Duration;
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

pub struct DualBucketRetention {
    pub l0_bucket: String,
    pub l1_bucket: String,
    pub aws_region: String,
    pub aws_profile: Option<String>,
    pub l0_retention_days: i64,
    pub l1_retention_days: i64,
    pub max_deletes_per_run: usize,
    pub interval_secs: u64,
    pub events: S3RetentionLoopEvents,
}

pub fn spawn_l0_l1_retention_loops(config: DualBucketRetention) -> Vec<JoinHandle<()>> {
    [
        (
            "l0",
            l0_s3_retention_config(
                config.l0_bucket,
                config.aws_region.clone(),
                config.aws_profile.clone(),
                config.l0_retention_days,
                config.max_deletes_per_run,
            ),
        ),
        (
            "l1",
            l1_s3_retention_config(
                config.l1_bucket,
                config.aws_region,
                config.aws_profile,
                config.l1_retention_days,
                config.max_deletes_per_run,
            ),
        ),
    ]
    .into_iter()
    .map(|(layer, retention_config)| {
        spawn_s3_retention_loop(layer, retention_config, config.interval_secs, config.events)
    })
    .collect()
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
        let now_ms = clock::now_ms();
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
