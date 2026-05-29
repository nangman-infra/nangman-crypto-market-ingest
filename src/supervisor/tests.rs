use super::bootstrap::bootstrap_chunks;
use super::*;

fn default_args() -> SupervisorArgs {
    parse_args(
        vec![
            "--l0-s3-bucket".to_owned(),
            "test-market-l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "test-market-l1".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap()
    .unwrap()
}

#[test]
fn requires_real_s3_buckets_for_all_in_one_contract() {
    let err = parse_args(Vec::<String>::new().into_iter())
        .unwrap_err()
        .to_string();

    assert!(err.contains("--l0-s3-bucket"));
}

#[test]
fn explicit_args_enable_all_in_one_contract() {
    let parsed = default_args();
    assert!(parsed.bootstrap_enabled);
    assert_eq!(parsed.bootstrap_lookback_days, 210);
    assert_eq!(parsed.realtime_venue, "binance");
    assert_eq!(parsed.l0_s3_bucket, "test-market-l0");
    assert_eq!(parsed.l1_s3_bucket, "test-market-l1");
    assert_eq!(
        parsed.l0_run_key_overlap_ms,
        args::DEFAULT_L0_RUN_KEY_OVERLAP_MS
    );
}

#[test]
fn parses_bootstrap_symbols_and_knobs() {
    let parsed = parse_args(
        vec![
            "--l0-s3-bucket".to_owned(),
            "test-market-l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "test-market-l1".to_owned(),
            "--bootstrap-symbols".to_owned(),
            "btcusdt, ethusdt".to_owned(),
            "--bootstrap-lookback-days".to_owned(),
            "2".to_owned(),
            "--bootstrap-chunk-hours".to_owned(),
            "6".to_owned(),
            "--l0-run-key-overlap-ms".to_owned(),
            "720000".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        parsed.bootstrap_symbols,
        Some(vec!["BTCUSDT".to_owned(), "ETHUSDT".to_owned()])
    );
    assert_eq!(parsed.bootstrap_lookback_days, 2);
    assert_eq!(parsed.bootstrap_chunk_hours, 6);
    assert_eq!(parsed.l0_run_key_overlap_ms, 720_000);
}

#[test]
fn bootstrap_chunks_are_oldest_first_and_hour_aligned() {
    let mut args = default_args();
    args.bootstrap_lookback_days = 1;
    args.bootstrap_chunk_hours = 6;
    let now = 1778947200123;
    let chunks = bootstrap_chunks(&args, now);
    assert_eq!(chunks.len(), 4);
    assert!(
        chunks
            .windows(2)
            .all(|pair| pair[0].end_ms == pair[1].start_ms)
    );
    assert!(chunks.iter().all(|chunk| chunk.start_ms % 3_600_000 == 0));
    assert!(chunks.iter().all(|chunk| chunk.end_ms % 3_600_000 == 0));
    assert!(chunks.iter().all(|chunk| chunk.start_ms % 21_600_000 == 0));
    assert!(chunks.iter().all(|chunk| chunk.end_ms % 21_600_000 == 0));
}

#[test]
fn bootstrap_chunks_do_not_shift_within_same_chunk_boundary() {
    let mut args = default_args();
    args.bootstrap_lookback_days = 2;
    args.bootstrap_chunk_hours = 24;
    let chunks_before = bootstrap_chunks(&args, 1778979600123);
    let chunks_after = bootstrap_chunks(&args, 1779004800123);

    assert_eq!(chunks_before, chunks_after);
    assert_eq!(chunks_before.len(), 2);
    assert!(
        chunks_before
            .windows(2)
            .all(|pair| pair[0].end_ms == pair[1].start_ms)
    );
    assert!(
        chunks_before
            .iter()
            .all(|chunk| chunk.start_ms % 86_400_000 == 0 && chunk.end_ms % 86_400_000 == 0)
    );
}

#[test]
fn rejects_unstable_bootstrap_chunk_hours() {
    let err = parse_args(
        vec![
            "--l0-s3-bucket".to_owned(),
            "test-market-l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "test-market-l1".to_owned(),
            "--bootstrap-chunk-hours".to_owned(),
            "7".to_owned(),
        ]
        .into_iter(),
    )
    .unwrap_err()
    .to_string();

    assert!(err.contains("evenly divide 24"));
}

#[test]
fn backfill_args_include_symbol_filter_when_configured() {
    let mut args = default_args();
    args.bootstrap_symbols = Some(vec!["BTCUSDT".to_owned()]);
    let values = backfill_args(
        &args,
        &BootstrapChunk {
            start_ms: 1,
            end_ms: 2,
        },
    );
    assert!(
        values
            .windows(2)
            .any(|pair| pair[0] == "--symbols" && pair[1] == "BTCUSDT")
    );
}

#[test]
fn bootstrap_l1_normalize_subchunks_use_schedule_interval() {
    let mut args = default_args();
    args.normalize_schedule_interval_ms = 900_000;

    let chunks = normalize_subchunks(
        &args,
        BootstrapChunk {
            start_ms: 0,
            end_ms: 3_600_000,
        },
    );

    assert_eq!(chunks.len(), 4);
    assert_eq!(
        chunks.first(),
        Some(&BootstrapChunk {
            start_ms: 0,
            end_ms: 900_000
        })
    );
    assert_eq!(
        chunks.last(),
        Some(&BootstrapChunk {
            start_ms: 2_700_000,
            end_ms: 3_600_000
        })
    );
}

#[test]
fn normalize_backfill_args_add_explicit_input_range() {
    let args = default_args();
    let values = normalize_backfill_args(
        &args,
        &BootstrapChunk {
            start_ms: 900_000,
            end_ms: 1_800_000,
        },
    );

    assert!(
        values
            .windows(2)
            .any(|pair| pair[0] == "--input-start-ms" && pair[1] == "900000")
    );
    assert!(
        values
            .windows(2)
            .any(|pair| pair[0] == "--input-end-ms" && pair[1] == "1800000")
    );
}

#[test]
fn normalize_args_use_dedicated_run_key_overlap() {
    let mut args = default_args();
    args.realtime_duration_seconds = 31_536_000;
    args.l0_run_key_overlap_ms = 720_000;
    let values = normalize_args(&args);
    let overlap = values
        .windows(2)
        .find(|pair| pair[0] == "--l0-run-key-overlap-ms")
        .map(|pair| pair[1].clone());
    assert_eq!(overlap, Some("720000".to_owned()));
}

#[test]
fn live_priority_normalize_args_limit_work_to_one_window() {
    let mut args = default_args();
    args.normalize_max_windows_per_tick = 192;

    let values = live_priority_normalize_args(&args);

    assert!(values.iter().any(|value| value == "--live-priority"));
    assert!(values.iter().any(|value| value == "--live-priority-only"));
    assert_eq!(
        values
            .windows(2)
            .filter(|pair| pair[0] == "--max-windows-per-tick")
            .map(|pair| pair[1].as_str())
            .next_back(),
        Some("1")
    );
}
