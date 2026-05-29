use crate::normalize::args::InputRange;
use crate::normalize::mode::RunMode;
use std::collections::BTreeSet;

pub(in crate::normalize::input_keys) fn select_s3_keys(
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

    let overlap_ms = l0_run_key_overlap_ms.max(0);
    parquet_keys
        .into_iter()
        .filter(|key| {
            let Some(run_start_ms) = run_start_ms_from_key(key) else {
                return true;
            };
            let run_end_ms = next_run_start_ms(&run_starts, run_start_ms).unwrap_or(i64::MAX);
            let range_start_ms = range.start_ms.saturating_sub(overlap_ms);
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
