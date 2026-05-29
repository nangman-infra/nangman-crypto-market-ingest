use super::*;
use crate::normalize::args::parse_args;
use crate::normalize::model::RawInputEvent;

fn args() -> NormalizeArgs {
    parse_args(
        vec![
            "--l0-s3-bucket".to_owned(),
            "l0-test".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1-test".to_owned(),
            "--l0-local-root".to_owned(),
            "/tmp/nangman-crypto-test/l0".to_owned(),
            "--spool-root".to_owned(),
            "/tmp/nangman-crypto-test/l1".to_owned(),
            "--catchup-tmp-root".to_owned(),
            "/tmp/nangman-crypto-test/catchup".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap()
    .unwrap()
}

fn empty_inputs(input_s3_object_count: usize) -> NormalizeInputs {
    NormalizeInputs {
        raw_events: Vec::new(),
        symbol_health: Vec::new(),
        source_health: Vec::new(),
        gap_alerts: Vec::new(),
        run_mode: "catchup".to_owned(),
        fallback_alert: input_s3_object_count > 0,
        input_local_object_count: 0,
        input_s3_object_count,
        input_object_keys: (0..input_s3_object_count)
            .map(|index| format!("raw/test-{index}.jsonl"))
            .collect(),
    }
}

#[test]
fn empty_build_is_not_successful_l1_output() {
    let args = args();
    let result = build_slices(
        &args,
        InputRange {
            start_ms: 1_000,
            end_ms: 2_000,
        },
        InputRange {
            start_ms: 0,
            end_ms: 3_000,
        },
        empty_inputs(1),
        3_000,
    );

    assert_eq!(result.status, "empty");
    assert_eq!(result.failure_reason, Some("no_l1_slices".to_owned()));
    assert!(result.slices.is_empty());
}

#[test]
fn payload_hash_mismatch_blocks_before_empty_status() {
    let args = args();
    let mut inputs = empty_inputs(1);
    inputs.raw_events.push(RawInputEvent {
        event_id: "bad-hash".to_owned(),
        producer_run_id: "run".to_owned(),
        venue: "binance".to_owned(),
        source_role: "primary".to_owned(),
        market_type: "spot".to_owned(),
        event_type: "trade".to_owned(),
        symbol_native: "BTCUSDT".to_owned(),
        symbol_canonical: "BTC-USDT".to_owned(),
        base_asset: "BTC".to_owned(),
        quote_asset: "USDT".to_owned(),
        exchange_timestamp_ms: 1_500,
        ingest_timestamp_ms: 1_500,
        exchange_sequence: None,
        payload_json: "{}".to_owned(),
        payload_sha256: "not-the-payload-hash".to_owned(),
        schema_version: "raw_market_event_v2".to_owned(),
    });

    let result = build_slices(
        &args,
        InputRange {
            start_ms: 1_000,
            end_ms: 2_000,
        },
        InputRange {
            start_ms: 0,
            end_ms: 3_000,
        },
        inputs,
        3_000,
    );

    assert_eq!(result.status, "blocked");
    assert_eq!(
        result.failure_reason,
        Some("payload_hash_mismatch".to_owned())
    );
    assert_eq!(result.payload_hash_mismatch_count, 1);
}
