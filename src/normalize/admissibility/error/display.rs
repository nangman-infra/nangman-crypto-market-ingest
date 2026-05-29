use std::fmt;

use super::L1AdmissibilityError;

pub(super) fn format_admissibility_error(
    error: &L1AdmissibilityError,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match error {
        L1AdmissibilityError::PointerSchemaMismatch { actual } => {
            write!(f, "l1 pointer schema mismatch: {actual}")
        }
        L1AdmissibilityError::PointerNotSuccess { status } => {
            write!(f, "l1 pointer is not success: {status}")
        }
        L1AdmissibilityError::PointerSchemaEmittedMismatch { actual, expected } => write!(
            f,
            "l1 pointer emitted schema mismatch: actual={actual} expected={expected}"
        ),
        L1AdmissibilityError::PointerTimeRangeMismatch {
            actual_start_ms,
            actual_end_ms,
            expected_start_ms,
            expected_end_ms,
        } => write!(
            f,
            "l1 pointer time range mismatch: actual=[{actual_start_ms},{actual_end_ms}) expected=[{expected_start_ms},{expected_end_ms})"
        ),
        L1AdmissibilityError::ManifestSchemaMismatch { actual } => {
            write!(f, "l1 manifest schema mismatch: {actual}")
        }
        L1AdmissibilityError::ManifestRunIdMismatch {
            pointer_run_id,
            manifest_run_id,
        } => write!(
            f,
            "l1 manifest run id mismatch: pointer={pointer_run_id} manifest={manifest_run_id}"
        ),
        L1AdmissibilityError::ManifestNotSuccess { status } => {
            write!(f, "l1 manifest is not success: {status}")
        }
        L1AdmissibilityError::ManifestSchemaEmittedMismatch { actual, expected } => write!(
            f,
            "l1 manifest emitted schema mismatch: actual={actual} expected={expected}"
        ),
        L1AdmissibilityError::ManifestTimeRangeMismatch {
            actual_start_ms,
            actual_end_ms,
            expected_start_ms,
            expected_end_ms,
        } => write!(
            f,
            "l1 manifest time range mismatch: actual=[{actual_start_ms},{actual_end_ms}) expected=[{expected_start_ms},{expected_end_ms})"
        ),
        L1AdmissibilityError::ManifestHasNoOutputObjects => {
            write!(f, "l1 manifest has no output object keys")
        }
        L1AdmissibilityError::ManifestRecordCountMismatch {
            output_record_count,
            slice_count_total,
        } => write!(
            f,
            "l1 manifest count mismatch: output_record_count={output_record_count} slice_count_total={slice_count_total}"
        ),
        L1AdmissibilityError::ReportSchemaMismatch { actual } => {
            write!(f, "l1 report schema mismatch: {actual}")
        }
        L1AdmissibilityError::ReportRunIdMismatch {
            report_run_id,
            manifest_run_id,
        } => write!(
            f,
            "l1 report run id mismatch: report={report_run_id} manifest={manifest_run_id}"
        ),
        L1AdmissibilityError::ReportStatusMismatch {
            report_status,
            manifest_status,
        } => write!(
            f,
            "l1 report status mismatch: report={report_status} manifest={manifest_status}"
        ),
        L1AdmissibilityError::ReportTimeRangeMismatch {
            report_start_ms,
            report_end_ms,
            manifest_start_ms,
            manifest_end_ms,
        } => write!(
            f,
            "l1 report time range mismatch: report=[{report_start_ms},{report_end_ms}) manifest=[{manifest_start_ms},{manifest_end_ms})"
        ),
        L1AdmissibilityError::ReportSchemaEmittedMismatch {
            report_schema,
            manifest_schema,
        } => write!(
            f,
            "l1 report emitted schema mismatch: report={report_schema} manifest={manifest_schema}"
        ),
        L1AdmissibilityError::ReportManifestKeyMismatch {
            report_manifest_key,
            manifest_key,
        } => write!(
            f,
            "l1 report manifest key mismatch: report={report_manifest_key} manifest={manifest_key}"
        ),
        L1AdmissibilityError::ReportOutputObjectsMismatch => {
            write!(f, "l1 report output objects differ from manifest")
        }
        L1AdmissibilityError::ReportProjectionObjectsMismatch => {
            write!(f, "l1 report projection object keys differ from manifest")
        }
    }
}
