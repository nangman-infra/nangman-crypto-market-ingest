use super::super::quality::{apply_health_and_gaps, finalize_slices};
use super::accumulator::BuildAccumulator;
use super::result::BuildResult;

impl BuildAccumulator {
    pub fn finish(mut self) -> BuildResult {
        apply_health_and_gaps(
            &self.symbol_health,
            &self.source_health,
            &self.gap_alerts,
            self.rows.values_mut(),
            self.input_range,
        );
        let slices = finalize_slices(self.rows.into_values());

        apply_health_and_gaps(
            &self.symbol_health,
            &self.source_health,
            &self.gap_alerts,
            self.projection_rows.values_mut(),
            self.projection_range,
        );
        let projection_slices = finalize_slices(self.projection_rows.into_values());

        let (status, failure_reason) = if self.stats.payload_hash_mismatch_count > 0 {
            ("blocked", Some("payload_hash_mismatch".to_owned()))
        } else if slices.is_empty() {
            ("empty", Some("no_l1_slices".to_owned()))
        } else {
            ("success", None)
        };
        BuildResult {
            slices,
            projection_slices,
            projection_derivative_metrics: self.projection_derivative_metrics,
            input_object_keys: self.metadata.input_object_keys,
            run_mode: self.metadata.run_mode,
            fallback_alert: self.metadata.fallback_alert,
            input_local_object_count: self.metadata.input_local_object_count,
            input_s3_object_count: self.metadata.input_s3_object_count,
            input_record_count: self.stats.input_record_count,
            duplicate_event_count: self.stats.duplicate_event_count,
            invalid_event_count: self.stats.invalid_event_count,
            payload_hash_mismatch_count: self.stats.payload_hash_mismatch_count,
            input_schema_versions: self.schema_versions.into_iter().collect(),
            status: status.to_owned(),
            failure_reason,
        }
    }
}
