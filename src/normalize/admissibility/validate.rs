use super::error::L1AdmissibilityError;
use super::types::{L1IndexPointer, L1ReadPlan, L1ReadRequest, POINTER_SCHEMA_VERSION};
use crate::normalize::args::InputRange;
use crate::normalize::model::{
    L1Manifest, MANIFEST_SCHEMA_VERSION, NormalizationReport, REPORT_SCHEMA_VERSION,
};

pub fn validate_pointer(
    pointer: &L1IndexPointer,
    request: &L1ReadRequest,
) -> Result<(), L1AdmissibilityError> {
    if pointer.schema_version != POINTER_SCHEMA_VERSION {
        return Err(L1AdmissibilityError::PointerSchemaMismatch {
            actual: pointer.schema_version.clone(),
        });
    }
    if pointer.status != "success" {
        return Err(L1AdmissibilityError::PointerNotSuccess {
            status: pointer.status.clone(),
        });
    }
    if pointer.schema_version_emitted != request.schema_version_emitted {
        return Err(L1AdmissibilityError::PointerSchemaEmittedMismatch {
            actual: pointer.schema_version_emitted.clone(),
            expected: request.schema_version_emitted.clone(),
        });
    }
    validate_time_range(
        pointer.input_time_range_start_ms,
        pointer.input_time_range_end_ms,
        request.input_range,
    )
    .map_err(
        |(actual_start_ms, actual_end_ms, expected_start_ms, expected_end_ms)| {
            L1AdmissibilityError::PointerTimeRangeMismatch {
                actual_start_ms,
                actual_end_ms,
                expected_start_ms,
                expected_end_ms,
            }
        },
    )?;
    Ok(())
}

pub fn build_read_plan(
    pointer: &L1IndexPointer,
    manifest: &L1Manifest,
    report: &NormalizationReport,
    manifest_key: &str,
    request: &L1ReadRequest,
) -> Result<L1ReadPlan, L1AdmissibilityError> {
    validate_pointer(pointer, request)?;
    validate_manifest(pointer, manifest, request)?;
    validate_report(report, manifest, manifest_key)?;
    Ok(L1ReadPlan {
        l1_run_id: manifest.l1_run_id.clone(),
        manifest_key: manifest_key.to_owned(),
        report_key: manifest.report_key.clone(),
        output_object_keys: manifest.output_object_keys.clone(),
        market_data_quality_summary_key: manifest.market_data_quality_summary_key.clone(),
        market_feature_delta_key: manifest.market_feature_delta_key.clone(),
        market_feature_delta_summary_key: manifest.market_feature_delta_summary_key.clone(),
        market_regime_context_key: manifest.market_regime_context_key.clone(),
        symbol_universe_snapshot_key: manifest.symbol_universe_snapshot_key.clone(),
    })
}

fn validate_manifest(
    pointer: &L1IndexPointer,
    manifest: &L1Manifest,
    request: &L1ReadRequest,
) -> Result<(), L1AdmissibilityError> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(L1AdmissibilityError::ManifestSchemaMismatch {
            actual: manifest.schema_version.clone(),
        });
    }
    if manifest.l1_run_id != pointer.l1_run_id {
        return Err(L1AdmissibilityError::ManifestRunIdMismatch {
            pointer_run_id: pointer.l1_run_id.clone(),
            manifest_run_id: manifest.l1_run_id.clone(),
        });
    }
    if manifest.status != "success" {
        return Err(L1AdmissibilityError::ManifestNotSuccess {
            status: manifest.status.clone(),
        });
    }
    if manifest.schema_version_emitted != request.schema_version_emitted {
        return Err(L1AdmissibilityError::ManifestSchemaEmittedMismatch {
            actual: manifest.schema_version_emitted.clone(),
            expected: request.schema_version_emitted.clone(),
        });
    }
    validate_time_range(
        manifest.input_time_range_start_ms,
        manifest.input_time_range_end_ms,
        request.input_range,
    )
    .map_err(
        |(actual_start_ms, actual_end_ms, expected_start_ms, expected_end_ms)| {
            L1AdmissibilityError::ManifestTimeRangeMismatch {
                actual_start_ms,
                actual_end_ms,
                expected_start_ms,
                expected_end_ms,
            }
        },
    )?;
    if manifest.output_object_keys.is_empty() {
        return Err(L1AdmissibilityError::ManifestHasNoOutputObjects);
    }
    if manifest.output_record_count != manifest.slice_count_total {
        return Err(L1AdmissibilityError::ManifestRecordCountMismatch {
            output_record_count: manifest.output_record_count,
            slice_count_total: manifest.slice_count_total,
        });
    }
    Ok(())
}

fn validate_report(
    report: &NormalizationReport,
    manifest: &L1Manifest,
    manifest_key: &str,
) -> Result<(), L1AdmissibilityError> {
    if report.schema_version != REPORT_SCHEMA_VERSION {
        return Err(L1AdmissibilityError::ReportSchemaMismatch {
            actual: report.schema_version.clone(),
        });
    }
    if report.l1_run_id != manifest.l1_run_id {
        return Err(L1AdmissibilityError::ReportRunIdMismatch {
            report_run_id: report.l1_run_id.clone(),
            manifest_run_id: manifest.l1_run_id.clone(),
        });
    }
    if report.status != manifest.status {
        return Err(L1AdmissibilityError::ReportStatusMismatch {
            report_status: report.status.clone(),
            manifest_status: manifest.status.clone(),
        });
    }
    if report.input_time_range_start_ms != manifest.input_time_range_start_ms
        || report.input_time_range_end_ms != manifest.input_time_range_end_ms
    {
        return Err(L1AdmissibilityError::ReportTimeRangeMismatch {
            report_start_ms: report.input_time_range_start_ms,
            report_end_ms: report.input_time_range_end_ms,
            manifest_start_ms: manifest.input_time_range_start_ms,
            manifest_end_ms: manifest.input_time_range_end_ms,
        });
    }
    if report.schema_version_emitted != manifest.schema_version_emitted {
        return Err(L1AdmissibilityError::ReportSchemaEmittedMismatch {
            report_schema: report.schema_version_emitted.clone(),
            manifest_schema: manifest.schema_version_emitted.clone(),
        });
    }
    if report.manifest_key != manifest_key {
        return Err(L1AdmissibilityError::ReportManifestKeyMismatch {
            report_manifest_key: report.manifest_key.clone(),
            manifest_key: manifest_key.to_owned(),
        });
    }
    if report.output_object_keys != manifest.output_object_keys {
        return Err(L1AdmissibilityError::ReportOutputObjectsMismatch);
    }
    if report.market_data_quality_summary_key != manifest.market_data_quality_summary_key
        || report.market_feature_delta_key != manifest.market_feature_delta_key
        || report.market_feature_delta_summary_key != manifest.market_feature_delta_summary_key
        || report.market_regime_context_key != manifest.market_regime_context_key
        || report.symbol_universe_snapshot_key != manifest.symbol_universe_snapshot_key
        || report.symbol_universe_bootstrap_rollup_key
            != manifest.symbol_universe_bootstrap_rollup_key
    {
        return Err(L1AdmissibilityError::ReportProjectionObjectsMismatch);
    }
    Ok(())
}

fn validate_time_range(
    actual_start_ms: i64,
    actual_end_ms: i64,
    expected: InputRange,
) -> Result<(), (i64, i64, i64, i64)> {
    if actual_start_ms <= expected.start_ms && actual_end_ms >= expected.end_ms {
        Ok(())
    } else {
        Err((
            actual_start_ms,
            actual_end_ms,
            expected.start_ms,
            expected.end_ms,
        ))
    }
}
