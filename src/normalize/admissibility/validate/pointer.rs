use super::time_range::validate_time_range;
use crate::normalize::admissibility::error::L1AdmissibilityError;
use crate::normalize::admissibility::types::{
    L1IndexPointer, L1ReadRequest, POINTER_SCHEMA_VERSION,
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
