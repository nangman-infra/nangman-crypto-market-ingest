use super::StorageError;
use super::s3_upload::{S3ObjectSummary, S3Uploader};
use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct S3RetentionConfig {
    pub bucket: String,
    pub region: String,
    pub profile: Option<String>,
    pub prefixes: Vec<String>,
    pub protected_prefixes: Vec<String>,
    pub retention_secs: i64,
    pub max_deletes_per_run: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct S3RetentionStats {
    pub scanned_object_count: usize,
    pub missing_last_modified_count: usize,
    pub protected_object_count: usize,
    pub expired_object_count: usize,
    pub deleted_object_count: usize,
    pub failed_delete_count: usize,
    pub deleted_bytes: u64,
    pub max_deleted_age_secs: i64,
    pub stopped_at_delete_limit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RetentionDecision {
    Keep,
    MissingLastModified,
    Protected,
    Delete { age_secs: i64 },
}

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
    let mut seen = BTreeSet::new();

    for prefix in &config.prefixes {
        for object in uploader.list_object_summaries(prefix).await? {
            if !seen.insert(object.key.clone()) {
                continue;
            }
            stats.scanned_object_count += 1;
            match retention_decision(config, now_ms, &object) {
                RetentionDecision::Keep => {}
                RetentionDecision::MissingLastModified => stats.missing_last_modified_count += 1,
                RetentionDecision::Protected => stats.protected_object_count += 1,
                RetentionDecision::Delete { age_secs } => {
                    stats.expired_object_count += 1;
                    if stats.deleted_object_count >= config.max_deletes_per_run {
                        stats.stopped_at_delete_limit = true;
                        continue;
                    }
                    match uploader.delete_object(&object.key).await {
                        Ok(()) => {
                            stats.deleted_object_count += 1;
                            stats.deleted_bytes =
                                stats.deleted_bytes.saturating_add(object.size_bytes);
                            stats.max_deleted_age_secs = stats.max_deleted_age_secs.max(age_secs);
                        }
                        Err(_) => stats.failed_delete_count += 1,
                    }
                }
            }
        }
    }

    Ok(stats)
}

fn retention_decision(
    config: &S3RetentionConfig,
    now_ms: i64,
    object: &S3ObjectSummary,
) -> RetentionDecision {
    if is_protected_key(&object.key, &config.protected_prefixes) {
        return RetentionDecision::Protected;
    }
    let Some(last_modified_ms) = object.last_modified_ms else {
        return RetentionDecision::MissingLastModified;
    };
    let age_ms = now_ms.saturating_sub(last_modified_ms);
    let retention_ms = config.retention_secs.saturating_mul(1000);
    if age_ms < retention_ms {
        return RetentionDecision::Keep;
    }
    RetentionDecision::Delete {
        age_secs: age_ms / 1000,
    }
}

fn is_protected_key(key: &str, protected_prefixes: &[String]) -> bool {
    protected_prefixes
        .iter()
        .any(|prefix| !prefix.is_empty() && key.starts_with(prefix))
}

fn validate_config(config: &S3RetentionConfig) -> Result<(), StorageError> {
    if config.bucket.trim().is_empty() {
        return Err(StorageError::InvalidConfig(
            "s3 retention bucket is required".to_owned(),
        ));
    }
    if config.prefixes.is_empty() {
        return Err(StorageError::InvalidConfig(
            "s3 retention prefixes are required".to_owned(),
        ));
    }
    if config.retention_secs <= 0 {
        return Err(StorageError::InvalidConfig(
            "s3 retention seconds must be positive".to_owned(),
        ));
    }
    if config.max_deletes_per_run == 0 {
        return Err(StorageError::InvalidConfig(
            "s3 retention max deletes per run must be positive".to_owned(),
        ));
    }
    Ok(())
}

pub fn default_l0_retention_prefixes() -> Vec<String> {
    [
        "raw_market_event/",
        "source_health/",
        "symbol_health/",
        "gap_alert/",
        "runs/",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub fn default_l1_retention_prefixes() -> Vec<String> {
    [
        "normalized_market_slice/",
        "normalization_report/",
        "market_data_quality_summary/",
        "market_feature_delta/",
        "market_feature_delta_summary/",
        "market_regime_context/",
        "symbol_universe_snapshot/",
        "runs/",
        "l1_index/",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_decision_deletes_objects_past_retention() {
        let config = config();
        let object = S3ObjectSummary {
            key: "raw_market_event/old.parquet".to_owned(),
            last_modified_ms: Some(1_000),
            size_bytes: 10,
        };

        let decision = retention_decision(&config, 11_000, &object);

        assert_eq!(decision, RetentionDecision::Delete { age_secs: 10 });
    }

    #[test]
    fn retention_decision_keeps_recent_objects() {
        let config = config();
        let object = S3ObjectSummary {
            key: "raw_market_event/recent.parquet".to_owned(),
            last_modified_ms: Some(8_000),
            size_bytes: 10,
        };

        assert_eq!(
            retention_decision(&config, 11_000, &object),
            RetentionDecision::Keep
        );
    }

    #[test]
    fn retention_decision_never_deletes_protected_objects() {
        let mut config = config();
        config.protected_prefixes = vec!["current/".to_owned()];
        let object = S3ObjectSummary {
            key: "current/manifest.json".to_owned(),
            last_modified_ms: Some(1_000),
            size_bytes: 10,
        };

        assert_eq!(
            retention_decision(&config, 11_000, &object),
            RetentionDecision::Protected
        );
    }

    #[test]
    fn retention_decision_keeps_objects_without_creation_time() {
        let config = config();
        let object = S3ObjectSummary {
            key: "raw_market_event/no-time.parquet".to_owned(),
            last_modified_ms: None,
            size_bytes: 10,
        };

        assert_eq!(
            retention_decision(&config, 11_000, &object),
            RetentionDecision::MissingLastModified
        );
    }

    #[test]
    fn default_prefixes_cover_l0_and_l1_families() {
        assert!(default_l0_retention_prefixes().contains(&"raw_market_event/".to_owned()));
        assert!(default_l0_retention_prefixes().contains(&"runs/".to_owned()));
        assert!(default_l1_retention_prefixes().contains(&"normalized_market_slice/".to_owned()));
        assert!(default_l1_retention_prefixes().contains(&"l1_index/".to_owned()));
    }

    fn config() -> S3RetentionConfig {
        S3RetentionConfig {
            bucket: "bucket".to_owned(),
            region: "ap-northeast-2".to_owned(),
            profile: None,
            prefixes: vec!["raw_market_event/".to_owned()],
            protected_prefixes: Vec::new(),
            retention_secs: 5,
            max_deletes_per_run: 100,
        }
    }
}
