use super::*;

#[test]
fn parses_minimum_required_flags() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.l0_s3_bucket, "l0");
    assert_eq!(parsed.l1_s3_bucket, "l1");
    assert_eq!(parsed.l0_s3_retention_days, 45);
    assert_eq!(parsed.l1_s3_retention_days, 240);
    assert_eq!(parsed.s3_retention_check_interval_secs, 21_600);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 1_000);
    assert_eq!(parsed.l1_index_upload_concurrency, 1);
}

#[test]
fn requires_input_start_and_end_together() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--input-start-ms".to_owned(),
        "1000".to_owned(),
    ];
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("must be provided together"));
}

#[test]
fn rejects_unknown_flag() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--unknown".to_owned(),
    ];
    assert!(parse_args(raw.into_iter()).is_err());
}

#[test]
fn parses_preflight_flag() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--preflight".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert!(parsed.preflight);
}

#[test]
fn parses_l1_index_audit_range() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--audit-l1-index-start-ms".to_owned(),
        "1000".to_owned(),
        "--audit-l1-index-end-ms".to_owned(),
        "2000".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.audit_l1_index_start_ms, Some(1000));
    assert_eq!(parsed.audit_l1_index_end_ms, Some(2000));
}

#[test]
fn requires_l1_index_audit_range_pair() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--audit-l1-index-start-ms".to_owned(),
        "1000".to_owned(),
    ];

    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("must be provided together"));
}

#[test]
fn parses_max_windows_per_tick() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--max-windows-per-tick".to_owned(),
        "12".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.max_windows_per_tick, 12);
}

#[test]
fn parses_max_latency_ms() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--max-latency-ms".to_owned(),
        "2500".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.max_latency_ms, 2500);
}

#[test]
fn parses_l0_run_key_overlap_ms() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--l0-run-key-overlap-ms".to_owned(),
        "420000".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.l0_run_key_overlap_ms, 420000);
}

#[test]
fn accepts_legacy_max_windows_per_invocation_alias() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--max-windows-per-invocation".to_owned(),
        "12".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.max_windows_per_tick, 12);
}

#[test]
fn parses_live_priority_knobs() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--live-priority".to_owned(),
        "--live-priority-only".to_owned(),
        "--live-priority-lag-threshold-ms".to_owned(),
        "1800000".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert!(parsed.live_priority);
    assert!(parsed.live_priority_only);
    assert_eq!(parsed.live_priority_lag_threshold_ms, 1_800_000);
}

#[test]
fn parses_s3_retention_knobs() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--s3-retention-days".to_owned(),
        "365".to_owned(),
        "--s3-retention-check-interval-secs".to_owned(),
        "3600".to_owned(),
        "--s3-retention-max-deletes-per-run".to_owned(),
        "50".to_owned(),
        "--disable-s3-retention".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.l0_s3_retention_days, 365);
    assert_eq!(parsed.l1_s3_retention_days, 365);
    assert_eq!(parsed.s3_retention_check_interval_secs, 3600);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 50);
    assert!(!parsed.s3_retention_enabled);
}

#[test]
fn parses_l1_index_upload_concurrency() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--l1-index-upload-concurrency".to_owned(),
        "32".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.l1_index_upload_concurrency, 32);
}

#[test]
fn parses_layered_s3_retention_days() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "l0".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
        "--l0-s3-retention-days".to_owned(),
        "30".to_owned(),
        "--l1-s3-retention-days".to_owned(),
        "365".to_owned(),
    ];
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.l0_s3_retention_days, 30);
    assert_eq!(parsed.l1_s3_retention_days, 365);
}

#[test]
fn rejects_public_doc_bucket_placeholder() {
    let raw = vec![
        "--l0-s3-bucket".to_owned(),
        "nangman-crypto-dev-market-ingest-l0-<account-suffix>".to_owned(),
        "--l1-s3-bucket".to_owned(),
        "l1".to_owned(),
    ];
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("public-doc placeholder"));
}
