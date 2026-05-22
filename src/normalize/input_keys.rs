use super::args::InputRange;
use super::mode::RunMode;
use crate::log_stream;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use chrono::{DateTime, Timelike, Utc};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub(crate) const VENUES: &[&str] = &["upbit", "binance"];
pub(crate) const RAW_EVENT_TYPES: &[&str] = &[
    "trade",
    "book_ticker",
    "depth_delta",
    "depth_snapshot",
    "ticker",
    "funding_rate_snapshot",
    "open_interest_snapshot",
];
const GAP_ALERT_TYPES: &[&str] = &["depth_update_id_gap", "ordering_violation", "upbit_error"];

#[derive(Debug, Clone)]
pub(crate) struct InputEntry {
    pub(crate) key: String,
    pub(crate) path: Option<PathBuf>,
    pub(crate) source: InputEntrySource,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InputEntrySource {
    Local,
    S3,
}

pub(crate) async fn collect_input_entries(
    s3: &S3Uploader,
    l0_local_root: &Path,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<Vec<InputEntry>, StorageError> {
    let local_entries = if matches!(run_mode, RunMode::Live) {
        local_input_entries(l0_local_root, range)?
    } else {
        Vec::new()
    };
    let s3_keys = s3_input_keys(s3, range, run_mode, l0_run_key_overlap_ms).await?;
    Ok(merge_entries(local_entries, s3_keys))
}

fn merge_entries(local_entries: Vec<InputEntry>, s3_keys: Vec<String>) -> Vec<InputEntry> {
    let mut entries = BTreeMap::new();
    for entry in local_entries {
        entries.insert(entry.key.clone(), entry);
    }
    for key in s3_keys {
        entries.entry(key.clone()).or_insert(InputEntry {
            key,
            path: None,
            source: InputEntrySource::S3,
        });
    }
    entries.into_values().collect()
}

async fn s3_input_keys(
    s3: &S3Uploader,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<Vec<String>, StorageError> {
    let mut keys = BTreeSet::new();
    let parts = hourly_parts(range.start_ms, range.end_ms);
    for part in &parts {
        for venue in VENUES {
            for event_type in RAW_EVENT_TYPES {
                list_into(
                    s3,
                    &format!(
                        "raw_market_event/venue={venue}/event_type={event_type}/event_date={}/hour={:02}/",
                        part.event_date, part.hour
                    ),
                    &mut keys,
                    range,
                    run_mode,
                    l0_run_key_overlap_ms,
                )
                .await?;
            }
            list_into(
                s3,
                &format!(
                    "symbol_health/venue={venue}/event_date={}/hour={:02}/",
                    part.event_date, part.hour
                ),
                &mut keys,
                range,
                run_mode,
                l0_run_key_overlap_ms,
            )
            .await?;
            list_into(
                s3,
                &format!(
                    "source_health/venue={venue}/event_date={}/hour={:02}/",
                    part.event_date, part.hour
                ),
                &mut keys,
                range,
                run_mode,
                l0_run_key_overlap_ms,
            )
            .await?;
            for gap_type in GAP_ALERT_TYPES {
                list_into(
                    s3,
                    &format!(
                        "gap_alert/venue={venue}/gap_type={gap_type}/event_date={}/hour={:02}/",
                        part.event_date, part.hour
                    ),
                    &mut keys,
                    range,
                    run_mode,
                    l0_run_key_overlap_ms,
                )
                .await?;
            }
        }
    }

    Ok(keys.into_iter().collect())
}

fn local_input_entries(root: &Path, range: InputRange) -> Result<Vec<InputEntry>, StorageError> {
    let parts = hourly_parts(range.start_ms, range.end_ms);
    let mut entries = BTreeMap::new();
    for path in parquet_files_under(root)? {
        let Some(key) = key_from_local_path(root, &path) else {
            continue;
        };
        if key_matches_parts(&key, &parts) {
            entries.insert(
                key.clone(),
                InputEntry {
                    key,
                    path: Some(path),
                    source: InputEntrySource::Local,
                },
            );
        }
    }
    Ok(entries.into_values().collect())
}

async fn list_into(
    s3: &S3Uploader,
    prefix: &str,
    keys: &mut BTreeSet<String>,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<(), StorageError> {
    let _ = log_stream::debug(
        "market_normalize_listing_inputs",
        json!({
            "phase": "start",
            "prefix": prefix
        }),
    );
    let listed_keys = s3.list_keys(prefix).await?;
    let listed_key_count = listed_keys.len();
    let parquet_key_count = listed_keys
        .iter()
        .filter(|key| key.ends_with(".parquet"))
        .count();
    let mut selected_key_count = 0_usize;
    for key in select_s3_keys(listed_keys, range, run_mode, l0_run_key_overlap_ms) {
        if key.ends_with(".parquet") {
            selected_key_count += 1;
            keys.insert(key);
        }
    }
    let _ = log_stream::debug(
        "market_normalize_listing_inputs",
        json!({
            "phase": "finished",
            "prefix": prefix,
            "listed_key_count": listed_key_count,
            "parquet_key_count": parquet_key_count,
            "selected_key_count": selected_key_count
        }),
    );
    Ok(())
}

fn select_s3_keys(
    listed_keys: Vec<String>,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Vec<String> {
    let parquet_keys = listed_keys
        .into_iter()
        .filter(|key| key.ends_with(".parquet"))
        .collect::<Vec<_>>();
    if !matches!(run_mode, RunMode::Live) {
        return parquet_keys;
    }

    let mut run_starts = parquet_keys
        .iter()
        .filter_map(|key| run_start_ms_from_key(key))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    run_starts.sort_unstable();

    parquet_keys
        .into_iter()
        .filter(|key| {
            let Some(run_start_ms) = run_start_ms_from_key(key) else {
                return true;
            };
            let run_end_ms = next_run_start_ms(&run_starts, run_start_ms).unwrap_or(i64::MAX);
            let range_start_ms = range.start_ms.saturating_sub(l0_run_key_overlap_ms);
            run_start_ms < range.end_ms && run_end_ms > range_start_ms
        })
        .collect()
}

fn run_start_ms_from_key(key: &str) -> Option<i64> {
    let marker = "run_id=";
    let start = key.find(marker)? + marker.len();
    let tail = key.get(start..)?;
    let end = tail.find('/').unwrap_or(tail.len());
    let run_id = tail.get(..end)?;
    let run_id = run_id
        .split_once("-part-")
        .map_or(run_id, |(prefix, _)| prefix);
    let seconds = run_id.rsplit('-').next()?.parse::<i64>().ok()?;
    seconds.checked_mul(1_000)
}

fn next_run_start_ms(run_starts: &[i64], current: i64) -> Option<i64> {
    run_starts
        .iter()
        .copied()
        .find(|candidate| *candidate > current)
}

fn hourly_parts(start_ms: i64, end_ms: i64) -> Vec<HourPart> {
    let mut parts = Vec::new();
    let mut current = floor_hour_ms(start_ms);
    while current < end_ms {
        let timestamp =
            DateTime::<Utc>::from_timestamp_millis(current).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        parts.push(HourPart {
            event_date: timestamp.format("%Y-%m-%d").to_string(),
            hour: timestamp.hour(),
        });
        current = current.saturating_add(3_600_000);
    }
    parts
}

fn floor_hour_ms(value: i64) -> i64 {
    value.div_euclid(3_600_000) * 3_600_000
}

fn key_matches_parts(key: &str, parts: &[HourPart]) -> bool {
    let recognized = key.starts_with("raw_market_event/")
        || key.starts_with("symbol_health/")
        || key.starts_with("source_health/")
        || key.starts_with("gap_alert/");
    recognized
        && parts.iter().any(|part| {
            key.contains(&format!(
                "/event_date={}/hour={:02}/",
                part.event_date, part.hour
            ))
        })
}

fn key_from_local_path(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let normalized = relative.to_string_lossy().replace('\\', "/");
    for marker in [
        "raw_market_event/",
        "symbol_health/",
        "source_health/",
        "gap_alert/",
    ] {
        if let Some(index) = normalized.find(marker) {
            return Some(normalized[index..].to_owned());
        }
    }
    None
}

fn parquet_files_under(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    collect_parquet_files(root, &mut files)?;
    Ok(files)
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), StorageError> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, files)?;
        } else if path.extension().is_some_and(|value| value == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}

struct HourPart {
    event_date: String,
    hour: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(merged.iter().any(|entry| {
            entry.key == s3_only_key && matches!(entry.source, InputEntrySource::S3)
        }));
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
}
