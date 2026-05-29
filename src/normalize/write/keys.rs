use chrono::{DateTime, Timelike, Utc};
use std::path::{Path, PathBuf};

pub fn slice_object_key(venue: &str, window_start_ms: i64, window_ms: i64, run_id: &str) -> String {
    let part = time_part(window_start_ms);
    format!(
        "normalized_market_slice/venue={venue}/event_date={}/hour={:02}/window_ms={window_ms}/shard=00/run_id={run_id}-part-000001.parquet",
        part.event_date, part.hour
    )
}

pub fn report_object_key(run_id: &str) -> String {
    format!("normalization_report/run_id={run_id}/report.json")
}

pub fn manifest_object_key(run_id: &str) -> String {
    format!("runs/run_id={run_id}/manifest.json")
}

pub fn market_data_quality_summary_object_key(run_id: &str) -> String {
    format!("market_data_quality_summary/run_id={run_id}/summary.json")
}

pub fn market_feature_delta_object_key(run_id: &str) -> String {
    format!("market_feature_delta/run_id={run_id}/delta.json")
}

pub fn market_feature_delta_summary_object_key(run_id: &str) -> String {
    format!("market_feature_delta_summary/run_id={run_id}/summary.json")
}

pub fn market_regime_context_object_key(run_id: &str) -> String {
    format!("market_regime_context/run_id={run_id}/context.json")
}

pub fn symbol_universe_snapshot_object_key(run_id: &str) -> String {
    format!("symbol_universe_snapshot/run_id={run_id}/snapshot.json")
}

pub fn symbol_universe_bootstrap_rollup_object_key(day_start_ms: i64) -> String {
    let part = time_part(day_start_ms);
    format!(
        "symbol_universe_snapshot/bootstrap_rollup/event_date={}/latest.json",
        part.event_date
    )
}

pub fn index_pointer_key(window_ms: i64, window_start_ms: i64) -> String {
    let part = time_part(window_start_ms);
    format!(
        "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/window_start_ms={window_start_ms}.json",
        part.event_date, part.hour
    )
}

pub fn local_output_path(spool_root: &Path, run_id: &str, key: &str) -> PathBuf {
    spool_root.join("output").join(run_id).join(key)
}

struct TimePart {
    event_date: String,
    hour: u32,
}

fn time_part(timestamp_ms: i64) -> TimePart {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    TimePart {
        event_date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_15_minute_index_pointer_key() {
        assert_eq!(
            index_pointer_key(1_000, 0),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=0.json"
        );
    }
}
