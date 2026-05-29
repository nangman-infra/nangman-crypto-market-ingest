use super::manifest::validate_manifest;
use super::pointer::validate_pointer;
use super::report::validate_report;
use crate::normalize::admissibility::error::L1AdmissibilityError;
use crate::normalize::admissibility::types::{L1IndexPointer, L1ReadPlan, L1ReadRequest};
use crate::normalize::model::{L1Manifest, NormalizationReport};

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
