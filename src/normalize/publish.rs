use super::args::{InputRange, NormalizeArgs};
use super::build::BuildResult;
use super::model::{
    L1Manifest, MANIFEST_SCHEMA_VERSION, NormalizationReport, REPORT_SCHEMA_VERSION,
    SLICE_SCHEMA_VERSION, SliceRow,
};
use super::write::{
    index_pointer_key, local_output_path, manifest_object_key, report_object_key, slice_object_key,
};
use crate::log_stream;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

mod artifacts;
mod manifest_index;

use artifacts::*;
use manifest_index::*;

pub async fn publish_outputs(
    args: &NormalizeArgs,
    l1_run_id: &str,
    input_range: InputRange,
    mut build: BuildResult,
    started_at_ms: i64,
    finished_at_ms: i64,
) -> Result<(), Box<dyn Error>> {
    let uploader = S3Uploader::new(
        args.l1_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;
    let mut published_keys = PublishedOutputKeys::default();

    if build.status == "success" {
        published_keys.slice_output_keys =
            publish_slice_parquets(&uploader, args, l1_run_id, &build.slices).await?;
        publish_quality_summary(
            &uploader,
            l1_run_id,
            input_range,
            finished_at_ms,
            &build.slices,
            &mut published_keys,
        )
        .await?;
        publish_feature_deltas(
            &uploader,
            l1_run_id,
            input_range,
            finished_at_ms,
            &build,
            &mut published_keys,
        )
        .await?;
        publish_regime_contexts(
            &uploader,
            l1_run_id,
            input_range,
            finished_at_ms,
            &mut build,
            &mut published_keys,
        )
        .await?;
        publish_bootstrap_rollup_and_universe(
            &uploader,
            l1_run_id,
            input_range,
            finished_at_ms,
            &build,
            &mut published_keys,
        )
        .await?;
    }

    let timing = RunTiming {
        started_at_ms,
        finished_at_ms,
    };
    publish_manifest_and_index(
        &uploader,
        args,
        l1_run_id,
        input_range,
        build,
        timing,
        published_keys,
    )
    .await
}

#[derive(Debug, Clone, Default)]
struct PublishedOutputKeys {
    slice_output_keys: Vec<String>,
    market_data_quality_summary_key: Option<String>,
    market_feature_delta_key: Option<String>,
    market_feature_delta_summary_key: Option<String>,
    market_regime_context_key: Option<String>,
    symbol_universe_snapshot_key: Option<String>,
    symbol_universe_bootstrap_rollup_key: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct RunTiming {
    started_at_ms: i64,
    finished_at_ms: i64,
}

fn group_slices(run_id: &str, window_ms: i64, slices: &[SliceRow]) -> BTreeMap<String, Vec<usize>> {
    let mut grouped = BTreeMap::<String, Vec<usize>>::new();
    for (index, slice) in slices.iter().enumerate() {
        let key = slice_object_key(&slice.venue, slice.window_start_ms, window_ms, run_id);
        grouped.entry(key).or_default().push(index);
    }
    grouped
}

async fn remove_file_best_effort(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

async fn file_size_best_effort(path: &Path) -> Option<u64> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .map(|metadata| metadata.len())
}
