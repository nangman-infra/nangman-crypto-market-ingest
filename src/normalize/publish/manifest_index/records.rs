use super::*;

pub(super) fn report(
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

pub(super) fn manifest(
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
