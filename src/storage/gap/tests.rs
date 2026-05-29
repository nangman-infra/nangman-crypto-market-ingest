use super::*;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn from_draft_formats_optional_sequences_and_hashes_payload() {
    let record = GapAlertRecord::from_draft(
        GapAlertDraft {
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            gap_type: "sequence_gap".to_owned(),
            detected_at_ms: 10,
            expected_sequence_id: Some(41),
            observed_sequence_id: None,
            heal_action: "refetch_snapshot".to_owned(),
            heal_status: "detected".to_owned(),
            payload_json: r#"{"gap":true}"#.to_owned(),
        },
        "run-1",
        7,
    );

    assert_eq!(record.gap_id, "gap_binance_sequence_gap_10_7");
    assert_eq!(record.expected_sequence_id, "41");
    assert_eq!(record.observed_sequence_id, "");
    assert_eq!(record.payload_sha256.len(), 64);
    assert_eq!(record.schema_version, "gap_alert_v1");
}

#[test]
fn writes_gap_alert_parquet_file() {
    let path = temp_parquet_path("gap-alert");
    let record = GapAlertRecord::from_draft(
        GapAlertDraft {
            venue: "upbit".to_owned(),
            source_role: "execution".to_owned(),
            symbol_native: "KRW-BTC".to_owned(),
            gap_type: "upbit_error".to_owned(),
            detected_at_ms: 10,
            expected_sequence_id: None,
            observed_sequence_id: None,
            heal_action: "inspect_error".to_owned(),
            heal_status: "detected".to_owned(),
            payload_json: r#"{"error":"too_many_requests"}"#.to_owned(),
        },
        "run-2",
        1,
    );

    write_gap_alert_parquet(&path, &[record]).unwrap();

    assert!(fs::metadata(&path).unwrap().len() > 0);
    let _ = fs::remove_file(path);
}

fn temp_parquet_path(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "market-ingest-{label}-{}-{nonce}.parquet",
        std::process::id()
    ))
}
