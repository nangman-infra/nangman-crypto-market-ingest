use super::*;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn from_draft_joins_reasons_and_builds_stable_identity() {
    let record = SymbolHealthRecord::from_draft(
        SymbolHealthDraft {
            venue: "binance".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            observed_at_ms: 30,
            last_event_time_ms: 20,
            latency_ms: 10,
            is_tradeable: false,
            reason_codes: vec!["stale".to_owned(), "gap".to_owned()],
        },
        "run-1",
        3,
    );

    assert_eq!(
        record.symbol_health_event_id,
        "symbol_health_binance_BTCUSDT_30_3"
    );
    assert_eq!(record.reason_codes, "stale;gap");
    assert!(!record.is_tradeable);
    assert_eq!(record.payload_sha256.len(), 64);
    assert_eq!(record.schema_version, "symbol_health_v1");
}

#[test]
fn writes_symbol_health_parquet_file() {
    let path = temp_parquet_path("symbol-health");
    let record = SymbolHealthRecord::from_draft(
        SymbolHealthDraft {
            venue: "upbit".to_owned(),
            symbol_native: "KRW-BTC".to_owned(),
            observed_at_ms: 30,
            last_event_time_ms: 30,
            latency_ms: 0,
            is_tradeable: true,
            reason_codes: Vec::new(),
        },
        "run-2",
        1,
    );

    write_symbol_health_parquet(&path, &[record]).unwrap();

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
