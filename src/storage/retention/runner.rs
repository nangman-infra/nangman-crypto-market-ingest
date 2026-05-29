use super::super::StorageError;
use super::super::s3_upload::S3Uploader;
use super::config::{S3RetentionConfig, S3RetentionStats, validate_config};
use super::planner::observe_retention_object;

pub async fn run_s3_retention_once(
    config: &S3RetentionConfig,
    now_ms: i64,
) -> Result<S3RetentionStats, StorageError> {
    validate_config(config)?;
    let uploader = S3Uploader::new(
        config.bucket.clone(),
        config.region.clone(),
        config.profile.clone(),
    )
    .await?;
    run_s3_retention_once_with_uploader(config, now_ms, &uploader).await
}

async fn run_s3_retention_once_with_uploader(
    config: &S3RetentionConfig,
    now_ms: i64,
    uploader: &S3Uploader,
) -> Result<S3RetentionStats, StorageError> {
    let mut stats = S3RetentionStats::default();
    let mut delete_candidates = Vec::new();

    for prefix in &config.prefixes {
        uploader
            .for_each_object_summary(prefix, |object| {
                observe_retention_object(
                    config,
                    now_ms,
                    object,
                    &mut stats,
                    &mut delete_candidates,
                );
            })
            .await?;
    }

    for candidate in delete_candidates {
        match uploader.delete_object(&candidate.key).await {
            Ok(()) => {
                stats.deleted_object_count += 1;
                stats.deleted_bytes = stats.deleted_bytes.saturating_add(candidate.size_bytes);
                stats.max_deleted_age_secs = stats.max_deleted_age_secs.max(candidate.age_secs);
            }
            Err(_) => stats.failed_delete_count += 1,
        }
    }

    Ok(stats)
}
