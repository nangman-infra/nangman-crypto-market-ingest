use super::*;
use crate::normalize::args::InputRange;
use crate::normalize::model::{
    L1Manifest, MANIFEST_SCHEMA_VERSION, NormalizationReport, REPORT_SCHEMA_VERSION,
    SLICE_SCHEMA_VERSION,
};

const MANIFEST_KEY: &str = "runs/run_id=l1_0_900000_1/manifest.json";

#[test]
fn builds_read_plan_for_success_pointer_manifest_and_report() {
    let request = request();
    let pointer = pointer();
    let manifest = manifest();
    let report = report();

    let plan = build_read_plan(&pointer, &manifest, &report, MANIFEST_KEY, &request).unwrap();

    assert_eq!(plan.l1_run_id, "l1_0_900000_1");
    assert_eq!(
        plan.output_object_keys,
        vec!["normalized_market_slice/a.parquet"]
    );
    assert_eq!(
        plan.market_data_quality_summary_key,
        Some("market_data_quality_summary/run_id=l1_0_900000_1/summary.json".to_owned())
    );
    assert_eq!(
        plan.market_feature_delta_key,
        Some("market_feature_delta/run_id=l1_0_900000_1/delta.json".to_owned())
    );
    assert_eq!(
        plan.market_feature_delta_summary_key,
        Some("market_feature_delta_summary/run_id=l1_0_900000_1/summary.json".to_owned())
    );
    assert_eq!(
        plan.market_regime_context_key,
        Some("market_regime_context/run_id=l1_0_900000_1/context.json".to_owned())
    );
    assert_eq!(
        plan.symbol_universe_snapshot_key,
        Some("symbol_universe_snapshot/run_id=l1_0_900000_1/snapshot.json".to_owned())
    );
}

#[test]
fn rejects_non_success_pointer_before_downstream_read() {
    let mut pointer = pointer();
    pointer.status = "partial".to_owned();

    let error = validate_pointer(&pointer, &request()).unwrap_err();

    assert_eq!(
        error,
        L1AdmissibilityError::PointerNotSuccess {
            status: "partial".to_owned()
        }
    );
}

#[test]
fn rejects_manifest_with_wrong_window() {
    let mut manifest = manifest();
    manifest.input_time_range_end_ms = 500;

    let error =
        build_read_plan(&pointer(), &manifest, &report(), MANIFEST_KEY, &request()).unwrap_err();

    assert!(matches!(
        error,
        L1AdmissibilityError::ManifestTimeRangeMismatch { .. }
    ));
}

#[test]
fn accepts_run_range_that_contains_requested_window() {
    let request = L1ReadRequest::normalized_market_slice(InputRange {
        start_ms: 1_000,
        end_ms: 2_000,
    });

    let plan = build_read_plan(&pointer(), &manifest(), &report(), MANIFEST_KEY, &request).unwrap();

    assert_eq!(plan.manifest_key, MANIFEST_KEY);
}

#[test]
fn rejects_report_manifest_output_drift() {
    let mut report = report();
    report
        .output_object_keys
        .push("unexpected.parquet".to_owned());

    let error =
        build_read_plan(&pointer(), &manifest(), &report, MANIFEST_KEY, &request()).unwrap_err();

    assert_eq!(error, L1AdmissibilityError::ReportOutputObjectsMismatch);
}

#[test]
fn rejects_report_manifest_projection_drift() {
    let mut report = report();
    report.symbol_universe_snapshot_key =
        Some("symbol_universe_snapshot/run_id=l1_0_900000_1/other.json".to_owned());

    let error =
        build_read_plan(&pointer(), &manifest(), &report, MANIFEST_KEY, &request()).unwrap_err();

    assert_eq!(error, L1AdmissibilityError::ReportProjectionObjectsMismatch);
}

fn request() -> L1ReadRequest {
    L1ReadRequest::normalized_market_slice(InputRange {
        start_ms: 0,
        end_ms: 900_000,
    })
}

fn pointer() -> L1IndexPointer {
    L1IndexPointer {
        schema_version: POINTER_SCHEMA_VERSION.to_owned(),
        canonical_manifest_key: MANIFEST_KEY.to_owned(),
        l1_run_id: "l1_0_900000_1".to_owned(),
        status: "success".to_owned(),
        finished_at_ms: 1,
        input_time_range_start_ms: 0,
        input_time_range_end_ms: 900_000,
        indexed_window_start_ms: Some(0),
        indexed_window_end_ms: Some(1_000),
        schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
    }
}

fn manifest() -> L1Manifest {
    L1Manifest {
        schema_version: MANIFEST_SCHEMA_VERSION.to_owned(),
        l1_run_id: "l1_0_900000_1".to_owned(),
        status: "success".to_owned(),
        input_time_range_start_ms: 0,
        input_time_range_end_ms: 900_000,
        schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
        report_key: "normalization_report/run_id=l1_0_900000_1/report.json".to_owned(),
        output_object_keys: vec!["normalized_market_slice/a.parquet".to_owned()],
        market_data_quality_summary_key: Some(
            "market_data_quality_summary/run_id=l1_0_900000_1/summary.json".to_owned(),
        ),
        market_feature_delta_key: Some(
            "market_feature_delta/run_id=l1_0_900000_1/delta.json".to_owned(),
        ),
        market_feature_delta_summary_key: Some(
            "market_feature_delta_summary/run_id=l1_0_900000_1/summary.json".to_owned(),
        ),
        market_regime_context_key: Some(
            "market_regime_context/run_id=l1_0_900000_1/context.json".to_owned(),
        ),
        symbol_universe_snapshot_key: Some(
            "symbol_universe_snapshot/run_id=l1_0_900000_1/snapshot.json".to_owned(),
        ),
        symbol_universe_bootstrap_rollup_key: Some(
            "symbol_universe_snapshot/bootstrap_rollup/event_date=1970-01-01/latest.json"
                .to_owned(),
        ),
        output_record_count: 1,
        slice_count_total: 1,
        finished_at_ms: 1,
    }
}

fn report() -> NormalizationReport {
    NormalizationReport {
        schema_version: REPORT_SCHEMA_VERSION.to_owned(),
        l1_run_id: "l1_0_900000_1".to_owned(),
        input_time_range_start_ms: 0,
        input_time_range_end_ms: 900_000,
        run_mode: "BACKFILL".to_owned(),
        fallback_alert: false,
        input_schema_versions: vec!["raw_market_event_v2".to_owned()],
        input_local_object_count: 0,
        input_s3_object_count: 1,
        input_object_keys: vec!["raw_market_event/a.parquet".to_owned()],
        input_record_count: 1,
        duplicate_event_count: 0,
        invalid_event_count: 0,
        payload_hash_mismatch_count: 0,
        slice_count_total: 1,
        slice_count_complete: 1,
        slice_count_partial: 0,
        slice_count_incomplete: 0,
        slice_count_reference_only: 0,
        output_object_keys: vec!["normalized_market_slice/a.parquet".to_owned()],
        market_data_quality_summary_key: Some(
            "market_data_quality_summary/run_id=l1_0_900000_1/summary.json".to_owned(),
        ),
        market_feature_delta_key: Some(
            "market_feature_delta/run_id=l1_0_900000_1/delta.json".to_owned(),
        ),
        market_feature_delta_summary_key: Some(
            "market_feature_delta_summary/run_id=l1_0_900000_1/summary.json".to_owned(),
        ),
        market_regime_context_key: Some(
            "market_regime_context/run_id=l1_0_900000_1/context.json".to_owned(),
        ),
        symbol_universe_snapshot_key: Some(
            "symbol_universe_snapshot/run_id=l1_0_900000_1/snapshot.json".to_owned(),
        ),
        symbol_universe_bootstrap_rollup_key: Some(
            "symbol_universe_snapshot/bootstrap_rollup/event_date=1970-01-01/latest.json"
                .to_owned(),
        ),
        status: "success".to_owned(),
        failure_reason: None,
        manifest_key: MANIFEST_KEY.to_owned(),
        started_at_ms: 0,
        finished_at_ms: 1,
        runner_git_sha: "test".to_owned(),
        runner_git_dirty: false,
        runner_build_profile: "debug".to_owned(),
        schema_version_emitted: SLICE_SCHEMA_VERSION.to_owned(),
    }
}
