use std::error::Error;

use crate::normalize::args::InputRange;
use crate::normalize::build::BuildResult;
use crate::normalize::projection::build_symbol_universe_snapshot_from_bootstrap;
use crate::normalize::write::symbol_universe_snapshot_object_key;
use crate::storage::s3_upload::S3Uploader;

use super::super::super::PublishedOutputKeys;
use super::logs;
use super::types::BootstrapRollupReadResult;

pub(super) async fn publish_universe_snapshot(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &BuildResult,
    bootstrap_rollup_read: &BootstrapRollupReadResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let universe_key = symbol_universe_snapshot_object_key(l1_run_id);
    let universe_snapshot = build_symbol_universe_snapshot_from_bootstrap(
        l1_run_id,
        input_range,
        finished_at_ms,
        &build.slices,
        &bootstrap_rollup_read.rollups,
    );
    let included_count = universe_snapshot.included_symbols.len();
    let excluded_count = universe_snapshot.excluded_symbols.len();
    let universe_bytes = serde_json::to_vec(&universe_snapshot)?;
    logs::upload_symbol_universe_snapshot(
        l1_run_id,
        &universe_key,
        included_count,
        excluded_count,
        universe_bytes.len(),
    )?;
    uploader.upload_json(&universe_key, universe_bytes).await?;
    published_keys.symbol_universe_snapshot_key = Some(universe_key);
    Ok(())
}
