#[derive(Default)]
pub(in crate::normalize::build) struct BuildStats {
    pub(in crate::normalize::build) input_record_count: usize,
    pub(in crate::normalize::build) duplicate_event_count: usize,
    pub(in crate::normalize::build) invalid_event_count: usize,
    pub(in crate::normalize::build) payload_hash_mismatch_count: usize,
}
