use super::*;
use crate::storage::record::{RawMarketEventDraft, RawMarketEventRecord};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn writes_raw_market_event_parquet_file_with_nullable_sequences() {
    let path = temp_parquet_path("raw-market-event");
    let record = RawMarketEventRecord::from_draft(
        RawMarketEventDraft {
            event_type: "depth_delta".to_owned(),
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            market_type: "spot".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            symbol_canonical: "BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
            exchange_timestamp_ms: 100,
            ingest_timestamp_ms: 110,
            sequence_id: "binance:depth_delta:42".to_owned(),
            sequence_tag: String::new(),
            exchange_sequence: Some(42),
            diff_first_update_id: Some(40),
            diff_final_update_id: Some(42),
            is_snapshot: false,
            stream_type: "REALTIME".to_owned(),
            stream_phase: "realtime".to_owned(),
            payload_json: r#"{"b":[],"a":[]}"#.to_owned(),
        },
        "run-1",
        1,
    );

    write_raw_market_event_parquet(&path, &[record]).unwrap();

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
