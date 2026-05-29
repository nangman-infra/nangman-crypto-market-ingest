use super::super::model::{DerivativeMetricObservation, SliceRow};

pub struct BuildResult {
    pub slices: Vec<SliceRow>,
    pub projection_slices: Vec<SliceRow>,
    pub projection_derivative_metrics: Vec<DerivativeMetricObservation>,
    pub input_object_keys: Vec<String>,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_record_count: usize,
    pub duplicate_event_count: usize,
    pub invalid_event_count: usize,
    pub payload_hash_mismatch_count: usize,
    pub input_schema_versions: Vec<String>,
    pub status: String,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BuildInputMetadata {
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
}
