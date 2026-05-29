use super::json_doc::{
    input_time_range_end_ms, json_status_success, pointer_manifest_key, temporary_json_path,
};
use super::pointer::{json_keys_desc, l1_pointer_hour_prefix};
use super::time::parse_event_date_hour_ms;
use serde_json::json;
use std::path::Path;

#[test]
fn parses_event_date_hour_into_epoch_ms() {
    let key = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-05/hour=13/shard=00/run_id=run-1-part-000001.parquet";
    let parsed = parse_event_date_hour_ms(key).unwrap();

    assert_eq!(parsed, 1_777_986_000_000);
}

#[test]
fn returns_none_for_invalid_key() {
    assert!(parse_event_date_hour_ms("totally/wrong/key.parquet").is_none());
}

#[test]
fn builds_l1_pointer_hour_prefix_from_utc_hour() {
    assert_eq!(
        l1_pointer_hour_prefix(1_000, 1_777_986_000_000),
        "l1_index/window_ms=1000/event_date=2026-05-05/hour=13/"
    );
}

#[test]
fn keeps_json_keys_in_descending_order() {
    let keys = json_keys_desc(vec![
        "l1_index/a/window_start_ms=1000.json".to_owned(),
        "l1_index/a/window_start_ms=0900.txt".to_owned(),
        "l1_index/a/window_start_ms=2000.json".to_owned(),
    ]);

    assert_eq!(
        keys,
        vec![
            "l1_index/a/window_start_ms=2000.json",
            "l1_index/a/window_start_ms=1000.json"
        ]
    );
}

#[test]
fn recognizes_success_status_only() {
    assert!(json_status_success(&json!({ "status": "success" })));
    assert!(!json_status_success(&json!({ "status": "failed" })));
    assert!(!json_status_success(&json!({})));
}

#[test]
fn reads_current_and_legacy_pointer_manifest_keys() {
    assert_eq!(
        pointer_manifest_key(&json!({ "canonical_manifest_key": "runs/run_id=a/manifest.json" })),
        Some("runs/run_id=a/manifest.json")
    );
    assert_eq!(
        pointer_manifest_key(&json!({ "manifest_key": "runs/run_id=b/manifest.json" })),
        Some("runs/run_id=b/manifest.json")
    );
}

#[test]
fn reads_pointer_input_end_ms() {
    assert_eq!(
        input_time_range_end_ms(&json!({ "input_time_range_end_ms": 900_000 })),
        Some(900_000)
    );
    assert_eq!(input_time_range_end_ms(&json!({})), None);
}

#[test]
fn temporary_json_path_uses_configured_tmp_root() {
    let path = temporary_json_path(Path::new("/opt/nangman-crypto/tmp"), "pointer");

    assert!(path.starts_with("/opt/nangman-crypto/tmp/_l1_pointer_lookup"));
    assert!(
        path.file_name()
            .and_then(|value| value.to_str())
            .is_some_and(|value| value.starts_with("market-normalize-pointer-"))
    );
}
