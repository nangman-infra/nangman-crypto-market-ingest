use std::error::Error;
use std::fmt;

mod display;

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
        display::format_admissibility_error(self, f)
    }
}

impl Error for L1AdmissibilityError {}
