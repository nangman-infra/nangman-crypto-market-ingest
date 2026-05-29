use crate::backfill::{BackfillArgs, BackfillError, Venue, binance, upbit};
use crate::clock;
use crate::log_stream;
use crate::storage::{
    L0StorageConfig, L0StorageSink, S3RetentionConfig, default_l0_retention_prefixes,
    run_s3_retention_once,
};

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
