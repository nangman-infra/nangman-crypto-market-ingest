use super::*;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn from_draft_caps_large_counts_and_hashes_payload() {
    let record = SourceHealthRecord::from_draft(
        SourceHealthDraft {
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            observed_at_ms: 20,
            connection_status: "connected".to_owned(),
            heartbeat_delay_ms: 30,
            stream_lag_ms: 40,
            recent_gap_count: u64::MAX,
            book_rebuild_count: 3,
            health_level: "degraded".to_owned(),
            payload_json: r#"{"status":"degraded"}"#.to_owned(),
        },
        "run-1",
        2,
    );

    assert_eq!(record.health_event_id, "health_binance_20_2");
    assert_eq!(record.recent_gap_count, i64::MAX);
    assert_eq!(record.book_rebuild_count, 3);
    assert_eq!(record.payload_sha256.len(), 64);
    assert_eq!(record.schema_version, "source_health_v2");
}

#[test]
fn writes_source_health_parquet_file() {
    let path = temp_parquet_path("source-health");
    let record = SourceHealthRecord::from_draft(
        SourceHealthDraft {
            venue: "upbit".to_owned(),
            source_role: "execution".to_owned(),
            observed_at_ms: 20,
            connection_status: "waiting".to_owned(),
            heartbeat_delay_ms: 0,
            stream_lag_ms: 0,
            recent_gap_count: 0,
            book_rebuild_count: 0,
            health_level: "waiting_for_messages".to_owned(),
            payload_json: r#"{"received":0}"#.to_owned(),
        },
        "run-2",
        1,
    );

    write_source_health_parquet(&path, &[record]).unwrap();

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
