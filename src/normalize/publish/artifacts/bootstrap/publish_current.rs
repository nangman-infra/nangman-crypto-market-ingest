use std::error::Error;

use crate::normalize::model::SymbolUniverseBootstrapRollup;
use crate::normalize::projection::merge_symbol_universe_bootstrap_rollup;
use crate::normalize::write::symbol_universe_bootstrap_rollup_object_key;
use crate::storage::s3_upload::S3Uploader;

use super::logs;
use super::types::BootstrapRollupReadResult;

pub(super) async fn publish_current_rollups(
    uploader: &S3Uploader,
    l1_run_id: &str,
    bootstrap_rollup_read: &mut BootstrapRollupReadResult,
    current_rollups: Vec<SymbolUniverseBootstrapRollup>,
    slice_count_total: usize,
) -> Result<Vec<String>, Box<dyn Error>> {
    if current_rollups.is_empty() {
        logs::current_empty(l1_run_id, slice_count_total)?;
    }
    let mut published_rollup_keys = Vec::new();
    for current_rollup in current_rollups {
        let key = symbol_universe_bootstrap_rollup_object_key(current_rollup.day_start_ms);
        let existing = take_existing_rollup(bootstrap_rollup_read, current_rollup.day_start_ms);
        let merged_rollup = merge_symbol_universe_bootstrap_rollup(existing, current_rollup);
        let symbol_count = merged_rollup.symbols.len();
        let source_window_count = merged_rollup.source_windows.len();
        let bytes = serde_json::to_vec(&merged_rollup)?;
        logs::upload_current(
            l1_run_id,
            &key,
            symbol_count,
            source_window_count,
            bytes.len(),
        )?;
        uploader.upload_json(&key, bytes).await?;
        bootstrap_rollup_read.rollups.push(merged_rollup);
        published_rollup_keys.push(key);
    }
    published_rollup_keys.sort();
    Ok(published_rollup_keys)
}

fn take_existing_rollup(
    bootstrap_rollup_read: &mut BootstrapRollupReadResult,
    day_start_ms: i64,
) -> Option<SymbolUniverseBootstrapRollup> {
    bootstrap_rollup_read
        .rollups
        .iter()
        .position(|rollup| rollup.day_start_ms == day_start_ms)
        .map(|index| bootstrap_rollup_read.rollups.remove(index))
}
