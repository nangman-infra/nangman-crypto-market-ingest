use super::args::{InputRange, NormalizeArgs};
use super::build::BuildResult;
use super::model::{
    L1Manifest, MANIFEST_SCHEMA_VERSION, NormalizationReport, REPORT_SCHEMA_VERSION,
    SLICE_SCHEMA_VERSION, SliceRow, SymbolUniverseBootstrapRollup,
};
use super::projection::{
    bootstrap_rollup_day_starts, build_market_data_quality_summary,
    build_market_feature_delta_summary, build_market_feature_deltas, build_market_regime_contexts,
    build_symbol_universe_bootstrap_rollups, build_symbol_universe_snapshot_from_bootstrap,
    merge_symbol_universe_bootstrap_rollup,
};
use super::write::{
    index_pointer_key, local_output_path, manifest_object_key,
    market_data_quality_summary_object_key, market_feature_delta_object_key,
    market_feature_delta_summary_object_key, market_regime_context_object_key, report_object_key,
    slice_object_key, symbol_universe_bootstrap_rollup_object_key,
    symbol_universe_snapshot_object_key, write_slice_parquet_refs,
};
use crate::log_stream;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use futures_util::{StreamExt, stream};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;

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

async fn publish_slice_parquets(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    l1_run_id: &str,
    slices: &[SliceRow],
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut output_keys = Vec::new();
    for (key, row_indices) in group_slices(l1_run_id, args.window_ms, slices) {
        let rows = row_indices
            .iter()
            .map(|index| &slices[*index])
            .collect::<Vec<_>>();
        let path = local_output_path(&args.spool_root, l1_run_id, &key);
        write_slice_parquet_refs(&path, &rows)?;
        let bytes = file_size_best_effort(&path).await.unwrap_or(0);
        log_stream::debug(
            "market_normalize_publishing",
            json!({
                "phase": "upload_parquet",
                "l1_run_id": l1_run_id,
                "key": key,
                "bytes": bytes,
                "row_count": rows.len()
            }),
        )?;
        uploader.upload_file(&key, &path).await?;
        output_keys.push(key);
        remove_file_best_effort(&path).await;
    }
    output_keys.sort();
    Ok(output_keys)
}

async fn publish_quality_summary(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    slices: &[SliceRow],
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let quality_key = market_data_quality_summary_object_key(l1_run_id);
    let quality_summary =
        build_market_data_quality_summary(l1_run_id, input_range, finished_at_ms, slices);
    let bytes = serde_json::to_vec(&quality_summary)?;
    uploader.upload_json(&quality_key, bytes).await?;
    published_keys.market_data_quality_summary_key = Some(quality_key);
    Ok(())
}

async fn publish_feature_deltas(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let feature_delta_key = market_feature_delta_object_key(l1_run_id);
    let feature_delta_summary_key = market_feature_delta_summary_object_key(l1_run_id);
    let feature_delta_count;
    let feature_delta_bytes;
    let feature_delta_summary_count;
    let feature_delta_summary_bytes;
    {
        let feature_deltas = build_market_feature_deltas(
            l1_run_id,
            input_range,
            finished_at_ms,
            &build.projection_slices,
            &build.projection_derivative_metrics,
        );
        feature_delta_count = feature_deltas.len();
        let feature_delta_summary = build_market_feature_delta_summary(
            l1_run_id,
            input_range,
            finished_at_ms,
            &feature_delta_key,
            &feature_deltas,
        );
        feature_delta_summary_count = feature_delta_summary.summary_row_count;
        feature_delta_summary_bytes = serde_json::to_vec(&feature_delta_summary)?;
        feature_delta_bytes = serde_json::to_vec(&feature_deltas)?;
        drop(feature_deltas);
    }
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_feature_delta_summary",
            "l1_run_id": l1_run_id,
            "key": &feature_delta_summary_key,
            "summary_row_count": feature_delta_summary_count,
            "detail_record_count": feature_delta_count,
            "bytes": feature_delta_summary_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&feature_delta_summary_key, feature_delta_summary_bytes)
        .await?;
    published_keys.market_feature_delta_summary_key = Some(feature_delta_summary_key);
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_feature_delta",
            "l1_run_id": l1_run_id,
            "key": &feature_delta_key,
            "record_count": feature_delta_count,
            "bytes": feature_delta_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&feature_delta_key, feature_delta_bytes)
        .await?;
    published_keys.market_feature_delta_key = Some(feature_delta_key);
    Ok(())
}

async fn publish_regime_contexts(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &mut BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let regime_context_key = market_regime_context_object_key(l1_run_id);
    let regime_context_count;
    let regime_context_bytes;
    {
        let regime_contexts = build_market_regime_contexts(
            l1_run_id,
            input_range,
            finished_at_ms,
            &build.projection_slices,
        );
        regime_context_count = regime_contexts.len();
        regime_context_bytes = serde_json::to_vec(&regime_contexts)?;
        drop(regime_contexts);
    }
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_market_regime_context",
            "l1_run_id": l1_run_id,
            "key": &regime_context_key,
            "record_count": regime_context_count,
            "bytes": regime_context_bytes.len()
        }),
    )?;
    uploader
        .upload_json(&regime_context_key, regime_context_bytes)
        .await?;
    published_keys.market_regime_context_key = Some(regime_context_key);
    // projection inputs are large and no longer needed once delta+regime are published.
    build.projection_slices.clear();
    build.projection_slices.shrink_to_fit();
    build.projection_derivative_metrics.clear();
    build.projection_derivative_metrics.shrink_to_fit();
    Ok(())
}

async fn publish_bootstrap_rollup_and_universe(
    uploader: &S3Uploader,
    l1_run_id: &str,
    input_range: InputRange,
    finished_at_ms: i64,
    build: &BuildResult,
    published_keys: &mut PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let expected_rollup_day_count = bootstrap_rollup_day_starts(input_range).len();
    log_stream::debug(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "read_recent_start",
            "l1_run_id": l1_run_id,
            "expected_rollup_day_count": expected_rollup_day_count
        }),
    )?;
    let mut bootstrap_rollup_read =
        read_recent_symbol_universe_bootstrap_rollups(uploader, input_range, l1_run_id).await?;
    let loaded_rollup_count = bootstrap_rollup_read.rollups.len();
    log_stream::debug(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "read_recent_finished",
            "l1_run_id": l1_run_id,
            "loaded_rollup_count": loaded_rollup_count,
            "missing_rollup_count": bootstrap_rollup_read.missing_count,
            "invalid_rollup_count": bootstrap_rollup_read.invalid_count,
            "expected_rollup_day_count": expected_rollup_day_count
        }),
    )?;
    let current_rollups = build_symbol_universe_bootstrap_rollups(
        l1_run_id,
        input_range,
        finished_at_ms,
        &build.slices,
    );
    let current_rollup_count = current_rollups.len();
    if current_rollup_count == 0 {
        log_stream::warn(
            "market_normalize_bootstrap_rollup",
            json!({
                "phase": "current_empty",
                "l1_run_id": l1_run_id,
                "slice_count_total": build.slices.len()
            }),
        )?;
    }
    let mut published_rollup_keys = Vec::new();
    for current_rollup in current_rollups {
        let key = symbol_universe_bootstrap_rollup_object_key(current_rollup.day_start_ms);
        let existing = bootstrap_rollup_read
            .rollups
            .iter()
            .position(|rollup| rollup.day_start_ms == current_rollup.day_start_ms)
            .map(|index| bootstrap_rollup_read.rollups.remove(index));
        let merged_rollup = merge_symbol_universe_bootstrap_rollup(existing, current_rollup);
        let symbol_count = merged_rollup.symbols.len();
        let source_window_count = merged_rollup.source_windows.len();
        let bytes = serde_json::to_vec(&merged_rollup)?;
        log_stream::debug(
            "market_normalize_bootstrap_rollup",
            json!({
                "phase": "upload_current",
                "l1_run_id": l1_run_id,
                "key": &key,
                "symbol_count": symbol_count,
                "source_window_count": source_window_count,
                "bytes": bytes.len()
            }),
        )?;
        uploader.upload_json(&key, bytes).await?;
        bootstrap_rollup_read.rollups.push(merged_rollup);
        published_rollup_keys.push(key);
    }
    published_rollup_keys.sort();
    published_keys.symbol_universe_bootstrap_rollup_key = published_rollup_keys.first().cloned();
    log_stream::info(
        "market_normalize_bootstrap_rollup",
        json!({
            "phase": "finished",
            "l1_run_id": l1_run_id,
            "loaded_rollup_count": loaded_rollup_count,
            "missing_rollup_count": bootstrap_rollup_read.missing_count,
            "invalid_rollup_count": bootstrap_rollup_read.invalid_count,
            "current_rollup_count": current_rollup_count,
            "published_rollup_count": published_rollup_keys.len(),
            "published_rollup_keys": published_rollup_keys
        }),
    )?;

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
    drop(universe_snapshot);
    drop(bootstrap_rollup_read);
    log_stream::debug(
        "market_normalize_publishing",
        json!({
            "phase": "upload_symbol_universe_snapshot",
            "l1_run_id": l1_run_id,
            "key": &universe_key,
            "included_count": included_count,
            "excluded_count": excluded_count,
            "bytes": universe_bytes.len()
        }),
    )?;
    uploader.upload_json(&universe_key, universe_bytes).await?;
    published_keys.symbol_universe_snapshot_key = Some(universe_key);
    Ok(())
}

async fn publish_manifest_and_index(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    l1_run_id: &str,
    input_range: InputRange,
    build: BuildResult,
    timing: RunTiming,
    published_keys: PublishedOutputKeys,
) -> Result<(), Box<dyn Error>> {
    let report_key = report_object_key(l1_run_id);
    let manifest_key = manifest_object_key(l1_run_id);
    let report = report(
        build,
        l1_run_id,
        input_range,
        timing,
        manifest_key.clone(),
        published_keys.clone(),
    );
    uploader
        .upload_json(&report_key, serde_json::to_vec_pretty(&report)?)
        .await?;

    let manifest = manifest(
        &report,
        l1_run_id,
        input_range,
        report_key,
        published_keys.clone(),
        timing,
    );
    uploader
        .upload_json(&manifest_key, serde_json::to_vec_pretty(&manifest)?)
        .await?;
    verify_manifest(uploader, &args.spool_root, l1_run_id, &manifest_key).await?;

    let index_pointer_count = if should_publish_index_pointers(report.status.as_str()) {
        publish_index_pointers(
            uploader,
            args,
            &manifest_key,
            l1_run_id,
            report.status.as_str(),
            timing.finished_at_ms,
            input_range,
        )
        .await?
    } else {
        0
    };

    log_stream::info(
        "market_normalize_index_published",
        json!({
            "l1_run_id": l1_run_id,
            "status": report.status,
            "window_ms": args.window_ms,
            "input_time_range_start_ms": input_range.start_ms,
            "input_time_range_end_ms": input_range.end_ms,
            "index_pointer_count": index_pointer_count
        }),
    )?;

    log_stream::info(
        "market_normalize_finished",
        json!({
            "l1_run_id": l1_run_id,
            "status": report.status,
            "slice_count_total": report.slice_count_total,
            "output_object_count": published_keys.slice_output_keys.len()
        }),
    )?;
    Ok(())
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

struct BootstrapRollupReadResult {
    rollups: Vec<SymbolUniverseBootstrapRollup>,
    missing_count: usize,
    invalid_count: usize,
}

#[derive(Debug, Clone, Copy)]
struct RunTiming {
    started_at_ms: i64,
    finished_at_ms: i64,
}

fn index_pointer_json(
    manifest_key: &str,
    l1_run_id: &str,
    status: &str,
    finished_at_ms: i64,
    input_range: InputRange,
    indexed_window_start_ms: i64,
    window_ms: i64,
) -> serde_json::Value {
    json!({
        "schema_version": "l1_index_pointer_v1",
        "canonical_manifest_key": manifest_key,
        "l1_run_id": l1_run_id,
        "status": status,
        "finished_at_ms": finished_at_ms,
        "input_time_range_start_ms": input_range.start_ms,
        "input_time_range_end_ms": input_range.end_ms,
        "indexed_window_start_ms": indexed_window_start_ms,
        "indexed_window_end_ms": indexed_window_start_ms.saturating_add(window_ms),
        "schema_version_emitted": SLICE_SCHEMA_VERSION
    })
}

async fn read_recent_symbol_universe_bootstrap_rollups(
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

fn index_window_starts(input_range: InputRange, window_ms: i64) -> Vec<i64> {
    if window_ms <= 0 || input_range.end_ms <= input_range.start_ms {
        return Vec::new();
    }

    let mut starts = Vec::new();
    let mut current = input_range.start_ms;
    while current < input_range.end_ms {
        starts.push(current);
        let Some(next) = current.checked_add(window_ms) else {
            break;
        };
        if next <= current {
            break;
        }
        current = next;
    }
    starts
}

fn should_publish_index_pointers(status: &str) -> bool {
    matches!(status, "success" | "empty")
}

async fn publish_index_pointers(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    manifest_key: &str,
    l1_run_id: &str,
    status: &str,
    finished_at_ms: i64,
    input_range: InputRange,
) -> Result<usize, Box<dyn Error>> {
    let window_starts = index_window_starts(input_range, args.window_ms);
    let pointer_count = window_starts.len();
    let concurrency = args.l1_index_upload_concurrency.max(1);
    let manifest_key = manifest_key.to_owned();
    let l1_run_id = l1_run_id.to_owned();
    let status = status.to_owned();

    let results = stream::iter(window_starts.into_iter().map(|window_start_ms| {
        let manifest_key = manifest_key.clone();
        let l1_run_id = l1_run_id.clone();
        let status = status.clone();
        async move {
            let pointer_key = index_pointer_key(args.window_ms, window_start_ms);
            let pointer = index_pointer_json(
                &manifest_key,
                &l1_run_id,
                status.as_str(),
                finished_at_ms,
                input_range,
                window_start_ms,
                args.window_ms,
            );
            uploader
                .upload_json_if_pointer_current(&pointer_key, serde_json::to_vec_pretty(&pointer)?)
                .await?;
            Ok::<(), Box<dyn Error>>(())
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    for result in results {
        result?;
    }

    Ok(pointer_count)
}

fn report(
    build: BuildResult,
    l1_run_id: &str,
    input_range: InputRange,
    timing: RunTiming,
    manifest_key: String,
    published_keys: PublishedOutputKeys,
) -> NormalizationReport {
    let mut complete = 0;
    let mut partial = 0;
    let mut incomplete = 0;
    let mut reference_only = 0;
    for slice in &build.slices {
        match slice.slice_completeness.as_str() {
            "complete" => complete += 1,
            "partial" => partial += 1,
            "reference_only" => reference_only += 1,
            _ => incomplete += 1,
        }
    }
    NormalizationReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        l1_run_id: l1_run_id.to_owned(),
        input_time_range_start_ms: input_range.start_ms,
        input_time_range_end_ms: input_range.end_ms,
        run_mode: build.run_mode,
        fallback_alert: build.fallback_alert,
        input_schema_versions: build.input_schema_versions,
        input_local_object_count: build.input_local_object_count,
        input_s3_object_count: build.input_s3_object_count,
        input_object_keys: build.input_object_keys,
        input_record_count: build.input_record_count,
        duplicate_event_count: build.duplicate_event_count,
        invalid_event_count: build.invalid_event_count,
        payload_hash_mismatch_count: build.payload_hash_mismatch_count,
        slice_count_total: build.slices.len(),
        slice_count_complete: complete,
        slice_count_partial: partial,
        slice_count_incomplete: incomplete,
        slice_count_reference_only: reference_only,
        output_object_keys: published_keys.slice_output_keys,
        market_data_quality_summary_key: published_keys.market_data_quality_summary_key,
        market_feature_delta_key: published_keys.market_feature_delta_key,
        market_feature_delta_summary_key: published_keys.market_feature_delta_summary_key,
        market_regime_context_key: published_keys.market_regime_context_key,
        symbol_universe_snapshot_key: published_keys.symbol_universe_snapshot_key,
        symbol_universe_bootstrap_rollup_key: published_keys.symbol_universe_bootstrap_rollup_key,
        status: build.status,
        failure_reason: build.failure_reason,
        manifest_key,
        started_at_ms: timing.started_at_ms,
        finished_at_ms: timing.finished_at_ms,
        runner_git_sha: option_env!("NANGMAN_GIT_SHA")
            .unwrap_or("unknown")
            .to_owned(),
        runner_git_dirty: option_env!("NANGMAN_GIT_DIRTY").is_some_and(|value| value == "true"),
        runner_build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
        .to_owned(),
        schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
    }
}

fn manifest(
    report: &NormalizationReport,
    l1_run_id: &str,
    input_range: InputRange,
    report_key: String,
    published_keys: PublishedOutputKeys,
    timing: RunTiming,
) -> L1Manifest {
    L1Manifest {
        schema_version: MANIFEST_SCHEMA_VERSION.to_owned(),
        l1_run_id: l1_run_id.to_owned(),
        status: report.status.clone(),
        input_time_range_start_ms: input_range.start_ms,
        input_time_range_end_ms: input_range.end_ms,
        schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
        report_key,
        output_object_keys: published_keys.slice_output_keys,
        market_data_quality_summary_key: published_keys.market_data_quality_summary_key,
        market_feature_delta_key: published_keys.market_feature_delta_key,
        market_feature_delta_summary_key: published_keys.market_feature_delta_summary_key,
        market_regime_context_key: published_keys.market_regime_context_key,
        symbol_universe_snapshot_key: published_keys.symbol_universe_snapshot_key,
        symbol_universe_bootstrap_rollup_key: published_keys.symbol_universe_bootstrap_rollup_key,
        output_record_count: report.slice_count_total,
        slice_count_total: report.slice_count_total,
        finished_at_ms: timing.finished_at_ms,
    }
}

async fn verify_manifest(
    uploader: &S3Uploader,
    spool_root: &Path,
    l1_run_id: &str,
    manifest_key: &str,
) -> Result<(), StorageError> {
    let path = local_output_path(spool_root, l1_run_id, manifest_key).with_extension("verify.json");
    uploader.download_file(manifest_key, &path).await?;
    let bytes = tokio::fs::read(&path).await?;
    let manifest = serde_json::from_slice::<L1Manifest>(&bytes)?;
    remove_file_best_effort(&path).await;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(StorageError::InvalidConfig(
            "manifest verification failed".to_owned(),
        ));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_window_starts_cover_every_window_in_run() {
        let starts = index_window_starts(
            InputRange {
                start_ms: 1_000,
                end_ms: 4_000,
            },
            1_000,
        );

        assert_eq!(starts, vec![1_000, 2_000, 3_000]);
    }

    #[test]
    fn index_window_starts_keep_existing_schedule_interval_shape() {
        let starts = index_window_starts(
            InputRange {
                start_ms: 0,
                end_ms: 900_000,
            },
            1_000,
        );

        assert_eq!(starts.len(), 900);
        assert_eq!(starts.first(), Some(&0));
        assert_eq!(starts.last(), Some(&899_000));
    }

    #[test]
    fn index_window_starts_reject_empty_or_invalid_ranges() {
        assert!(
            index_window_starts(
                InputRange {
                    start_ms: 1,
                    end_ms: 1
                },
                1_000
            )
            .is_empty()
        );
        assert!(
            index_window_starts(
                InputRange {
                    start_ms: 2,
                    end_ms: 1
                },
                1_000
            )
            .is_empty()
        );
        assert!(
            index_window_starts(
                InputRange {
                    start_ms: 1,
                    end_ms: 2
                },
                0
            )
            .is_empty()
        );
    }

    #[test]
    fn publishes_index_pointers_for_terminal_empty_outputs() {
        assert!(should_publish_index_pointers("success"));
        assert!(should_publish_index_pointers("empty"));
        assert!(!should_publish_index_pointers("blocked"));
    }

    #[test]
    fn index_pointer_records_run_range_and_indexed_window() {
        let pointer = index_pointer_json(
            "runs/run_id=r/manifest.json",
            "r",
            "success",
            10,
            InputRange {
                start_ms: 0,
                end_ms: 900_000,
            },
            42_000,
            1_000,
        );

        assert_eq!(pointer["input_time_range_start_ms"], 0);
        assert_eq!(pointer["input_time_range_end_ms"], 900_000);
        assert_eq!(pointer["indexed_window_start_ms"], 42_000);
        assert_eq!(pointer["indexed_window_end_ms"], 43_000);
    }
}
