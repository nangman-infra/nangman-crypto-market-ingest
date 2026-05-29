use super::super::StorageError;
use serde::Serialize;

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

pub fn l0_s3_retention_config(
    bucket: String,
    region: String,
    profile: Option<String>,
    retention_days: i64,
    max_deletes_per_run: usize,
) -> S3RetentionConfig {
    S3RetentionConfig {
        bucket,
        region,
        profile,
        prefixes: default_l0_retention_prefixes(),
        protected_prefixes: Vec::new(),
        retention_secs: retention_days.saturating_mul(86_400),
        max_deletes_per_run,
    }
}

pub fn l1_s3_retention_config(
    bucket: String,
    region: String,
    profile: Option<String>,
    retention_days: i64,
    max_deletes_per_run: usize,
) -> S3RetentionConfig {
    S3RetentionConfig {
        bucket,
        region,
        profile,
        prefixes: default_l1_retention_prefixes(),
        protected_prefixes: Vec::new(),
        retention_secs: retention_days.saturating_mul(86_400),
        max_deletes_per_run,
    }
}

pub(super) fn validate_config(config: &S3RetentionConfig) -> Result<(), StorageError> {
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
