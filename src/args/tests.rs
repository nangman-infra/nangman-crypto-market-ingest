use super::*;

fn base_args() -> Vec<String> {
    vec!["--venue".to_owned(), "binance".to_owned()]
}

#[test]
fn defaults_eviction_knobs() {
    let parsed = parse_args(base_args().into_iter()).unwrap().unwrap();
    assert_eq!(parsed.local_disk_high_water_pct, 70);
    assert_eq!(parsed.local_disk_emergency_pct, 90);
    assert_eq!(parsed.safety_floor_hours, 2);
    assert_eq!(parsed.eviction_check_interval_secs, 600);
    assert_eq!(parsed.s3_retention_days, 45);
    assert_eq!(parsed.s3_retention_check_interval_secs, 21_600);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 1_000);
    assert!(parsed.s3_retention_enabled);
    assert_eq!(
        parsed.binance_futures_rest_base_url,
        "https://fapi.binance.com"
    );
    assert_eq!(parsed.binance_derivatives_snapshot_interval_seconds, 300);
}

#[test]
fn rejects_emergency_below_high_water() {
    let mut raw = base_args();
    raw.push("--local-disk-high-water-pct".to_owned());
    raw.push("80".to_owned());
    raw.push("--local-disk-emergency-pct".to_owned());
    raw.push("70".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains(">="));
}

#[test]
fn rejects_pct_above_100() {
    let mut raw = base_args();
    raw.push("--local-disk-high-water-pct".to_owned());
    raw.push("150".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("1..100"));
}

#[test]
fn parses_eviction_knobs_when_provided() {
    let mut raw = base_args();
    raw.push("--local-disk-high-water-pct".to_owned());
    raw.push("60".to_owned());
    raw.push("--local-disk-emergency-pct".to_owned());
    raw.push("85".to_owned());
    raw.push("--safety-floor-hours".to_owned());
    raw.push("4".to_owned());
    raw.push("--eviction-check-interval-secs".to_owned());
    raw.push("300".to_owned());
    raw.push("--s3-retention-days".to_owned());
    raw.push("365".to_owned());
    raw.push("--s3-retention-check-interval-secs".to_owned());
    raw.push("3600".to_owned());
    raw.push("--s3-retention-max-deletes-per-run".to_owned());
    raw.push("50".to_owned());
    raw.push("--disable-s3-retention".to_owned());
    raw.push("--binance-derivatives-snapshot-interval-seconds".to_owned());
    raw.push("120".to_owned());
    let parsed = parse_args(raw.into_iter()).unwrap().unwrap();
    assert_eq!(parsed.local_disk_high_water_pct, 60);
    assert_eq!(parsed.local_disk_emergency_pct, 85);
    assert_eq!(parsed.safety_floor_hours, 4);
    assert_eq!(parsed.eviction_check_interval_secs, 300);
    assert_eq!(parsed.s3_retention_days, 365);
    assert_eq!(parsed.s3_retention_check_interval_secs, 3600);
    assert_eq!(parsed.s3_retention_max_deletes_per_run, 50);
    assert!(!parsed.s3_retention_enabled);
    assert_eq!(parsed.binance_derivatives_snapshot_interval_seconds, 120);
}

#[test]
fn rejects_public_doc_bucket_placeholder() {
    let mut raw = base_args();
    raw.push("--l0-s3-bucket".to_owned());
    raw.push("nangman-crypto-dev-market-ingest-l0-<account-suffix>".to_owned());
    let err = parse_args(raw.into_iter()).err().unwrap();
    assert!(err.to_string().contains("public-doc placeholder"));
}
