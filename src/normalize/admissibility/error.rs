use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L1AdmissibilityError {
    PointerSchemaMismatch {
        actual: String,
    },
    PointerNotSuccess {
        status: String,
    },
    PointerSchemaEmittedMismatch {
        actual: String,
        expected: String,
    },
    PointerTimeRangeMismatch {
        actual_start_ms: i64,
        actual_end_ms: i64,
        expected_start_ms: i64,
        expected_end_ms: i64,
    },
    ManifestSchemaMismatch {
        actual: String,
    },
    ManifestRunIdMismatch {
        pointer_run_id: String,
        manifest_run_id: String,
    },
    ManifestNotSuccess {
        status: String,
    },
    ManifestSchemaEmittedMismatch {
        actual: String,
        expected: String,
    },
    ManifestTimeRangeMismatch {
        actual_start_ms: i64,
        actual_end_ms: i64,
        expected_start_ms: i64,
        expected_end_ms: i64,
    },
    ManifestHasNoOutputObjects,
    ManifestRecordCountMismatch {
        output_record_count: usize,
        slice_count_total: usize,
    },
    ReportSchemaMismatch {
        actual: String,
    },
    ReportRunIdMismatch {
        report_run_id: String,
        manifest_run_id: String,
    },
    ReportStatusMismatch {
        report_status: String,
        manifest_status: String,
    },
    ReportTimeRangeMismatch {
        report_start_ms: i64,
        report_end_ms: i64,
        manifest_start_ms: i64,
        manifest_end_ms: i64,
    },
    ReportSchemaEmittedMismatch {
        report_schema: String,
        manifest_schema: String,
    },
    ReportManifestKeyMismatch {
        report_manifest_key: String,
        manifest_key: String,
    },
    ReportOutputObjectsMismatch,
    ReportProjectionObjectsMismatch,
}

impl fmt::Display for L1AdmissibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PointerSchemaMismatch { actual } => {
                write!(f, "l1 pointer schema mismatch: {actual}")
            }
            Self::PointerNotSuccess { status } => {
                write!(f, "l1 pointer is not success: {status}")
            }
            Self::PointerSchemaEmittedMismatch { actual, expected } => write!(
                f,
                "l1 pointer emitted schema mismatch: actual={actual} expected={expected}"
            ),
            Self::PointerTimeRangeMismatch {
                actual_start_ms,
                actual_end_ms,
                expected_start_ms,
                expected_end_ms,
            } => write!(
                f,
                "l1 pointer time range mismatch: actual=[{actual_start_ms},{actual_end_ms}) expected=[{expected_start_ms},{expected_end_ms})"
            ),
            Self::ManifestSchemaMismatch { actual } => {
                write!(f, "l1 manifest schema mismatch: {actual}")
            }
            Self::ManifestRunIdMismatch {
                pointer_run_id,
                manifest_run_id,
            } => write!(
                f,
                "l1 manifest run id mismatch: pointer={pointer_run_id} manifest={manifest_run_id}"
            ),
            Self::ManifestNotSuccess { status } => {
                write!(f, "l1 manifest is not success: {status}")
            }
            Self::ManifestSchemaEmittedMismatch { actual, expected } => write!(
                f,
                "l1 manifest emitted schema mismatch: actual={actual} expected={expected}"
            ),
            Self::ManifestTimeRangeMismatch {
                actual_start_ms,
                actual_end_ms,
                expected_start_ms,
                expected_end_ms,
            } => write!(
                f,
                "l1 manifest time range mismatch: actual=[{actual_start_ms},{actual_end_ms}) expected=[{expected_start_ms},{expected_end_ms})"
            ),
            Self::ManifestHasNoOutputObjects => {
                write!(f, "l1 manifest has no output object keys")
            }
            Self::ManifestRecordCountMismatch {
                output_record_count,
                slice_count_total,
            } => write!(
                f,
                "l1 manifest count mismatch: output_record_count={output_record_count} slice_count_total={slice_count_total}"
            ),
            Self::ReportSchemaMismatch { actual } => {
                write!(f, "l1 report schema mismatch: {actual}")
            }
            Self::ReportRunIdMismatch {
                report_run_id,
                manifest_run_id,
            } => write!(
                f,
                "l1 report run id mismatch: report={report_run_id} manifest={manifest_run_id}"
            ),
            Self::ReportStatusMismatch {
                report_status,
                manifest_status,
            } => write!(
                f,
                "l1 report status mismatch: report={report_status} manifest={manifest_status}"
            ),
            Self::ReportTimeRangeMismatch {
                report_start_ms,
                report_end_ms,
                manifest_start_ms,
                manifest_end_ms,
            } => write!(
                f,
                "l1 report time range mismatch: report=[{report_start_ms},{report_end_ms}) manifest=[{manifest_start_ms},{manifest_end_ms})"
            ),
            Self::ReportSchemaEmittedMismatch {
                report_schema,
                manifest_schema,
            } => write!(
                f,
                "l1 report emitted schema mismatch: report={report_schema} manifest={manifest_schema}"
            ),
            Self::ReportManifestKeyMismatch {
                report_manifest_key,
                manifest_key,
            } => write!(
                f,
                "l1 report manifest key mismatch: report={report_manifest_key} manifest={manifest_key}"
            ),
            Self::ReportOutputObjectsMismatch => {
                write!(f, "l1 report output objects differ from manifest")
            }
            Self::ReportProjectionObjectsMismatch => {
                write!(f, "l1 report projection object keys differ from manifest")
            }
        }
    }
}

impl Error for L1AdmissibilityError {}
