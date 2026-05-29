use serde_json::json;

use crate::log_stream;

use super::types::BootstrapRollupReadResult;

pub(super) fn read_recent_start(
    l1_run_id: &str,
    expected_rollup_day_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::debug(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "read_recent_start",
            "l1_run_id": l1_run_id,
            "expected_rollup_day_count": expected_rollup_day_count
        }),
    )?)
}

pub(super) fn read_recent_finished(
    l1_run_id: &str,
    read_result: &BootstrapRollupReadResult,
    expected_rollup_day_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::debug(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "read_recent_finished",
            "l1_run_id": l1_run_id,
            "loaded_rollup_count": read_result.rollups.len(),
            "missing_rollup_count": read_result.missing_count,
            "invalid_rollup_count": read_result.invalid_count,
            "expected_rollup_day_count": expected_rollup_day_count
        }),
    )?)
}

pub(super) fn current_empty(
    l1_run_id: &str,
    slice_count_total: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::warn(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "current_empty",
            "l1_run_id": l1_run_id,
            "slice_count_total": slice_count_total
        }),
    )?)
}

pub(super) fn upload_current(
    l1_run_id: &str,
    key: &str,
    symbol_count: usize,
    source_window_count: usize,
    bytes_len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::debug(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "upload_current",
            "l1_run_id": l1_run_id,
            "key": key,
            "symbol_count": symbol_count,
            "source_window_count": source_window_count,
            "bytes": bytes_len
        }),
    )?)
}

pub(super) fn finished(
    l1_run_id: &str,
    read_result: &BootstrapRollupReadResult,
    loaded_rollup_count: usize,
    current_rollup_count: usize,
    published_rollup_keys: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::info(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "finished",
            "l1_run_id": l1_run_id,
            "loaded_rollup_count": loaded_rollup_count,
            "missing_rollup_count": read_result.missing_count,
            "invalid_rollup_count": read_result.invalid_count,
            "current_rollup_count": current_rollup_count,
            "published_rollup_count": published_rollup_keys.len(),
            "published_rollup_keys": published_rollup_keys
        }),
    )?)
}

pub(super) fn upload_symbol_universe_snapshot(
    l1_run_id: &str,
    key: &str,
    included_count: usize,
    excluded_count: usize,
    bytes_len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_symbol_universe_snapshot",
            "l1_run_id": l1_run_id,
            "key": key,
            "included_count": included_count,
            "excluded_count": excluded_count,
            "bytes": bytes_len
        }),
    )?)
}

pub(super) fn read_recent_invalid_json(l1_run_id: &str, key: &str, error: &serde_json::Error) {
    let _ = log_stream::warn(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "read_recent_invalid_json",
            "l1_run_id": l1_run_id,
            "key": key,
            "error": error.to_string()
        }),
    );
}
