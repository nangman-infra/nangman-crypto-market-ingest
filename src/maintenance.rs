use market_ingest_app::args::Args;
use market_ingest_app::clock;
use market_ingest_app::log_stream;
use market_ingest_app::storage::{
    EvictionConfig, S3RetentionLoopEvents, UnsealedOrphanConfig, cleanup_invalid_unsealed_once,
    disk_used_pct, evict_once, l0_s3_retention_config, spawn_s3_retention_loop,
};
use serde_json::json;
use std::path::Path;
use std::time::Duration;
use tokio::task::JoinHandle;

pub(crate) fn spawn_eviction_loop(args: &Args) -> Option<JoinHandle<()>> {
    let config = EvictionConfig {
        spool_root: args.l0_spool_root.clone(),
        high_water_pct: args.local_disk_high_water_pct,
        emergency_pct: args.local_disk_emergency_pct,
        safety_floor_secs: args.safety_floor_hours.saturating_mul(3_600),
    };
    let interval_secs = args.eviction_check_interval_secs;
    Some(tokio::spawn(async move {
        run_eviction_loop(config, interval_secs).await;
    }))
}

pub(crate) fn spawn_l0_s3_retention_loop(args: &Args) -> Option<JoinHandle<()>> {
    if !args.s3_retention_enabled {
        return None;
    }
    let bucket = args.l0_s3_bucket.clone()?;
    let config = l0_s3_retention_config(
        bucket,
        args.aws_region.clone(),
        args.aws_profile.clone(),
        args.s3_retention_days,
        args.s3_retention_max_deletes_per_run,
    );
    let interval_secs = args.s3_retention_check_interval_secs;
    Some(spawn_s3_retention_loop(
        "l0",
        config,
        interval_secs,
        S3RetentionLoopEvents {
            run_event: "market_ingest_s3_retention_run",
            error_event: "market_ingest_s3_retention_error",
        },
    ))
}

pub(crate) fn log_unsealed_orphan_cleanup(args: &Args) {
    let config = UnsealedOrphanConfig {
        spool_root: args.l0_spool_root.clone(),
        safety_floor_secs: args.safety_floor_hours.saturating_mul(3_600),
    };
    match cleanup_invalid_unsealed_once(&config, clock::now_ms()) {
        Ok(stats) => {
            let fields = json!({
                "spool_root": config.spool_root.display().to_string(),
                "scanned_unsealed_count": stats.scanned_unsealed_count,
                "recent_unsealed_count": stats.recent_unsealed_count,
                "valid_unsealed_count": stats.valid_unsealed_count,
                "invalid_unsealed_count": stats.invalid_unsealed_count,
                "quarantined_count": stats.quarantined_count,
                "quarantined_bytes": stats.quarantined_bytes,
                "quarantine_root": stats.quarantine_root
                    .map(|path| path.display().to_string())
            });
            let result = if stats.invalid_unsealed_count > 0 || stats.quarantined_count > 0 {
                log_stream::warn("market_ingest_unsealed_orphan_cleanup", fields)
            } else {
                log_stream::debug("market_ingest_unsealed_orphan_cleanup", fields)
            };
            let _ = result;
        }
        Err(error) => {
            let _ = log_stream::warn(
                "market_ingest_unsealed_orphan_cleanup_failed",
                json!({ "error": error.to_string() }),
            );
        }
    }
}

async fn run_eviction_loop(config: EvictionConfig, interval_secs: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let now_ms = clock::now_ms();
        let probe_root = config.spool_root.clone();
        let probe = move || disk_probe(&probe_root);
        match evict_once(&config, now_ms, probe) {
            Ok(stats) => {
                let _ = log_stream::debug(
                    "market_ingest_eviction_heartbeat",
                    json!({
                        "spool_root": config.spool_root.display().to_string(),
                        "disk_used_pct_before": stats.disk_used_pct_before,
                        "disk_used_pct_after": stats.disk_used_pct_after,
                        "high_water_pct": config.high_water_pct,
                        "emergency_pct": config.emergency_pct,
                        "triggered": stats.triggered,
                        "candidate_count": stats.candidate_count
                    }),
                );
                if !stats.triggered {
                    continue;
                }
                let _ = log_stream::info(
                    "market_ingest_eviction_run",
                    json!({
                        "disk_used_pct_before": stats.disk_used_pct_before,
                        "disk_used_pct_after": stats.disk_used_pct_after,
                        "evicted_count": stats.evicted_count,
                        "evicted_bytes": stats.evicted_bytes,
                        "candidate_count": stats.candidate_count,
                        "emergency": stats.emergency
                    }),
                );
            }
            Err(error) => {
                let _ = log_stream::error(
                    "market_ingest_eviction_error",
                    json!({ "message": error.to_string() }),
                );
            }
        }
    }
}

fn disk_probe(spool_root: &Path) -> Result<u8, market_ingest_app::storage::StorageError> {
    disk_used_pct(spool_root).map_err(market_ingest_app::storage::StorageError::Io)
}
