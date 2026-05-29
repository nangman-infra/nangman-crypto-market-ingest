use crate::normalize::admissibility::error::L1AdmissibilityError;
use crate::normalize::model::{L1Manifest, NormalizationReport, REPORT_SCHEMA_VERSION};

pub(super) fn validate_report(
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
