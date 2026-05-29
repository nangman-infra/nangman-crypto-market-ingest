use crate::normalize::args::InputRange;
use crate::normalize::model::SymbolUniverseBootstrapRollup;
use crate::normalize::projection::bootstrap_rollup_day_starts;
use crate::normalize::write::symbol_universe_bootstrap_rollup_object_key;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;

use super::logs;
use super::types::BootstrapRollupReadResult;

pub(super) async fn read_recent_symbol_universe_bootstrap_rollups(
    uploader: &S3Uploader,
    input_range: InputRange,
    l1_run_id: &str,
) -> Result<BootstrapRollupReadResult, StorageError> {
    let mut rollups = Vec::new();
    let mut missing_count = 0usize;
    let mut invalid_count = 0usize;
    for day_start_ms in bootstrap_rollup_day_starts(input_range) {
        let key = symbol_universe_bootstrap_rollup_object_key(day_start_ms);
        match uploader
            .download_json_optional::<SymbolUniverseBootstrapRollup>(&key)
            .await
        {
            Ok(Some(rollup)) => rollups.push(rollup),
            Ok(None) => missing_count += 1,
            Err(StorageError::Json(error)) => {
                invalid_count += 1;
                logs::read_recent_invalid_json(l1_run_id, &key, &error);
            }
            Err(error) => return Err(error),
        }
    }
    rollups.sort_by_key(|rollup| rollup.day_start_ms);
    Ok(BootstrapRollupReadResult {
        rollups,
        missing_count,
        invalid_count,
    })
}
