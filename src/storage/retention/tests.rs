use super::config::validate_config;
use super::planner::{
    RetentionDecision, RetentionDeleteCandidate, plan_retention_deletes, retention_decision,
};
use super::*;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3ObjectSummary;

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

#[test]
fn config_builders_apply_layer_prefixes_and_day_retention() {
    let l0 = l0_s3_retention_config(
        "l0-bucket".to_owned(),
        "ap-northeast-2".to_owned(),
        Some("dev".to_owned()),
        240,
        1_000,
    );
    assert_eq!(l0.bucket, "l0-bucket");
    assert_eq!(l0.profile.as_deref(), Some("dev"));
    assert_eq!(l0.retention_secs, 20_736_000);
    assert_eq!(l0.max_deletes_per_run, 1_000);
    assert!(l0.prefixes.contains(&"raw_market_event/".to_owned()));

    let l1 = l1_s3_retention_config(
        "l1-bucket".to_owned(),
        "ap-northeast-2".to_owned(),
        None,
        240,
        500,
    );
    assert_eq!(l1.bucket, "l1-bucket");
    assert_eq!(l1.profile, None);
    assert_eq!(l1.retention_secs, 20_736_000);
    assert_eq!(l1.max_deletes_per_run, 500);
    assert!(l1.prefixes.contains(&"normalized_market_slice/".to_owned()));
}

#[test]
fn plan_retention_deletes_deduplicates_and_classifies_objects() {
    let mut config = config();
    config.protected_prefixes = vec!["raw_market_event/protected/".to_owned()];
    let plan = plan_retention_deletes(
        &config,
        11_000,
        vec![
            object("raw_market_event/old.parquet", Some(1_000), 10),
            object("raw_market_event/old.parquet", Some(1_000), 10),
            object("raw_market_event/recent.parquet", Some(9_000), 20),
            object("raw_market_event/no-time.parquet", None, 30),
            object("raw_market_event/protected/old.parquet", Some(1_000), 40),
        ],
    );

    assert_eq!(
        plan.delete_candidates,
        vec![RetentionDeleteCandidate {
            key: "raw_market_event/old.parquet".to_owned(),
            size_bytes: 10,
            age_secs: 10,
        }]
    );
    assert_eq!(
        plan.stats,
        S3RetentionStats {
            scanned_object_count: 4,
            missing_last_modified_count: 1,
            protected_object_count: 1,
            expired_object_count: 1,
            deleted_object_count: 0,
            failed_delete_count: 0,
            deleted_bytes: 0,
            max_deleted_age_secs: 0,
            stopped_at_delete_limit: false,
        }
    );
}

#[test]
fn plan_retention_deletes_stops_at_delete_limit_but_counts_expired_objects() {
    let mut config = config();
    config.max_deletes_per_run = 1;

    let plan = plan_retention_deletes(
        &config,
        11_000,
        vec![
            object("raw_market_event/old-1.parquet", Some(1_000), 10),
            object("raw_market_event/old-2.parquet", Some(2_000), 20),
        ],
    );

    assert_eq!(plan.delete_candidates.len(), 1);
    assert_eq!(plan.stats.scanned_object_count, 2);
    assert_eq!(plan.stats.expired_object_count, 2);
    assert!(plan.stats.stopped_at_delete_limit);
}

#[test]
fn validate_config_rejects_unsafe_cleanup_settings() {
    let mut blank_bucket = config();
    blank_bucket.bucket = "  ".to_owned();
    assert!(matches!(
        validate_config(&blank_bucket),
        Err(StorageError::InvalidConfig(message)) if message.contains("bucket")
    ));

    let mut no_prefixes = config();
    no_prefixes.prefixes.clear();
    assert!(matches!(
        validate_config(&no_prefixes),
        Err(StorageError::InvalidConfig(message)) if message.contains("prefixes")
    ));

    let mut no_retention = config();
    no_retention.retention_secs = 0;
    assert!(matches!(
        validate_config(&no_retention),
        Err(StorageError::InvalidConfig(message)) if message.contains("seconds")
    ));

    let mut no_delete_budget = config();
    no_delete_budget.max_deletes_per_run = 0;
    assert!(matches!(
        validate_config(&no_delete_budget),
        Err(StorageError::InvalidConfig(message)) if message.contains("max deletes")
    ));
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

fn object(key: &str, last_modified_ms: Option<i64>, size_bytes: u64) -> S3ObjectSummary {
    S3ObjectSummary {
        key: key.to_owned(),
        last_modified_ms,
        size_bytes,
    }
}
