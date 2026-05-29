use super::RAW_EVENT_TYPES;
use super::entry::{InputEntry, InputEntrySource, merge_entries};
use super::local::{key_from_local_path, key_matches_parts};
use super::s3::select_s3_keys;
use super::time::hourly_parts;
use crate::normalize::args::InputRange;
use crate::normalize::mode::RunMode;
use std::path::PathBuf;

#[test]
fn hourly_parts_are_utc_and_cover_range() {
    let parts = hourly_parts(3_599_999, 3_600_001);

    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].event_date, "1970-01-01");
    assert_eq!(parts[0].hour, 0);
    assert_eq!(parts[1].hour, 1);
}

#[test]
fn raw_event_types_include_derivatives_snapshots_for_s3_discovery() {
    assert!(RAW_EVENT_TYPES.contains(&"funding_rate_snapshot"));
    assert!(RAW_EVENT_TYPES.contains(&"open_interest_snapshot"));
}

#[test]
fn extracts_object_key_from_local_run_path() {
    let root = PathBuf::from("/opt/nangman-crypto/data/spool/market-ingest/l0");
    let path = root.join(
        "run-1/raw_market_event/venue=upbit/event_type=trade/event_date=1970-01-01/hour=00/shard=00/run_id=run-1-part-000001.parquet",
    );
    assert_eq!(
        key_from_local_path(&root, &path).unwrap(),
        "raw_market_event/venue=upbit/event_type=trade/event_date=1970-01-01/hour=00/shard=00/run_id=run-1-part-000001.parquet"
    );
}

#[test]
fn merge_keeps_local_entry_and_adds_missing_s3_keys() {
    let local_key = "raw_market_event/venue=binance/event_type=trade/event_date=1970-01-01/hour=00/shard=00/run_id=local.parquet".to_owned();
    let s3_only_key =
        "source_health/venue=binance/event_date=1970-01-01/hour=00/shard=00/run_id=s3.parquet"
            .to_owned();
    let local = vec![InputEntry {
        key: local_key.clone(),
        path: Some(PathBuf::from("/tmp/local.parquet")),
        source: InputEntrySource::Local,
    }];
    let merged = merge_entries(local, vec![local_key.clone(), s3_only_key.clone()]);

    assert_eq!(merged.len(), 2);
    assert!(merged.iter().any(|entry| {
        entry.key == local_key && matches!(entry.source, InputEntrySource::Local)
    }));
    assert!(
        merged.iter().any(|entry| {
            entry.key == s3_only_key && matches!(entry.source, InputEntrySource::S3)
        })
    );
}

#[test]
fn long_running_market_ingest_run_ids_are_kept_by_hour_partition() {
    let key = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=17/shard=00/run_id=market-ingest-binance-1779471443-part-000101.parquet";
    let parts = hourly_parts(1_779_471_000_000, 1_779_471_900_000);

    assert!(key_matches_parts(key, &parts));
}

#[test]
fn live_s3_selection_keeps_long_running_run_that_overlaps_range() {
    let old_run = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779471443-part-000001.parquet".to_owned();
    let overlap_run = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779473155-part-000001.parquet".to_owned();
    let active_run = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779474484-part-000001.parquet".to_owned();
    let future_run = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779476052-part-000001.parquet".to_owned();

    let selected = select_s3_keys(
        vec![
            old_run.clone(),
            overlap_run.clone(),
            active_run.clone(),
            future_run.clone(),
        ],
        InputRange {
            start_ms: 1_779_474_600_000,
            end_ms: 1_779_475_800_000,
        },
        RunMode::Live,
        360_000,
    );

    assert!(selected.contains(&overlap_run));
    assert!(selected.contains(&active_run));
    assert!(!selected.contains(&old_run));
    assert!(!selected.contains(&future_run));
}

#[test]
fn live_s3_selection_clamps_negative_overlap_to_zero() {
    let overlapping_run = "raw_market_event/venue=binance/event_type=trade/event_date=1970-01-01/hour=00/shard=00/run_id=market-ingest-binance-1-part-000001.parquet".to_owned();
    let next_run = "raw_market_event/venue=binance/event_type=trade/event_date=1970-01-01/hour=00/shard=00/run_id=market-ingest-binance-2-part-000001.parquet".to_owned();

    let selected = select_s3_keys(
        vec![overlapping_run.clone(), next_run.clone()],
        InputRange {
            start_ms: 1_500,
            end_ms: 2_500,
        },
        RunMode::Live,
        -1_000,
    );

    assert!(selected.contains(&overlapping_run));
    assert!(selected.contains(&next_run));
}

#[test]
fn backfill_s3_selection_keeps_all_parquet_keys() {
    let parquet = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779471443-part-000001.parquet".to_owned();
    let non_parquet = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-22/hour=18/shard=00/run_id=market-ingest-binance-1779471443-part-000001.txt".to_owned();

    let selected = select_s3_keys(
        vec![parquet.clone(), non_parquet],
        InputRange {
            start_ms: 1_779_474_600_000,
            end_ms: 1_779_475_800_000,
        },
        RunMode::Backfill,
        360_000,
    );

    assert_eq!(selected, vec![parquet]);
}
