mod logs;
mod publish_current;
mod read;
mod snapshot;
mod types;

use std::error::Error;

use crate::normalize::args::InputRange;
use crate::normalize::build::BuildResult;
use crate::normalize::projection::{
    bootstrap_rollup_day_starts, build_symbol_universe_bootstrap_rollups,
};
use crate::storage::s3_upload::S3Uploader;

use super::super::PublishedOutputKeys;
use publish_current::publish_current_rollups;
use read::read_recent_symbol_universe_bootstrap_rollups;
use snapshot::publish_universe_snapshot;

pub(in crate::normalize::publish) async fn publish_bootstrap_rollup_and_universe(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let expected_rollup_day_count = bootstrap_rollup_day_starts(input_range).len();
    logs::read_recent_start(l1_run_id, expected_rollup_day_count)?;
    let mut bootstrap_rollup_read =
        read_recent_symbol_universe_bootstrap_rollups(uploader, input_range, l1_run_id).await?;
    let loaded_rollup_count = bootstrap_rollup_read.rollups.len();
    logs::read_recent_finished(l1_run_id, &bootstrap_rollup_read, expected_rollup_day_count)?;

    let current_rollups = build_symbol_universe_bootstrap_rollups(
        l1_run_id,
        input_range,
        finished_at_ms,
        &build.slices,
    );
    let current_rollup_count = current_rollups.len();
    let published_rollup_keys = publish_current_rollups(
        uploader,
        l1_run_id,
        &mut bootstrap_rollup_read,
        current_rollups,
        build.slices.len(),
    )
    .await?;
    published_keys.symbol_universe_bootstrap_rollup_key = published_rollup_keys.first().cloned();
    logs::finished(
        l1_run_id,
        &bootstrap_rollup_read,
        loaded_rollup_count,
        current_rollup_count,
        &published_rollup_keys,
    )?;

    publish_universe_snapshot(
        uploader,
        l1_run_id,
        input_range,
        finished_at_ms,
        build,
        &bootstrap_rollup_read,
        published_keys,
    )
    .await
}
