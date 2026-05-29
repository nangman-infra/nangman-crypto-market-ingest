use super::time_range::validate_time_range;
use crate::normalize::admissibility::error::L1AdmissibilityError;
use crate::normalize::admissibility::types::{L1IndexPointer, L1ReadRequest};
use crate::normalize::model::{L1Manifest, MANIFEST_SCHEMA_VERSION};

pub(super) fn validate_manifest(
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
