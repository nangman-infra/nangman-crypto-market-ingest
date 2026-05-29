use serde::Serialize;
use std::collections::VecDeque;

pub(super) const MAX_REPORTED_OBJECTS: usize = 1_000;
pub(super) const MAX_REPORTED_FAILURES: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct StorageReport {
    pub bucket: String,
    pub run_id: String,
    pub record_count: u64,
    pub uploaded_object_count: usize,
    pub uploaded_object_retained_count: usize,
    pub uploaded_object_dropped_count: usize,
    pub uploaded_objects: Vec<UploadedObject>,
    pub failed_upload_count: usize,
    pub failed_upload_retained_count: usize,
    pub failed_upload_dropped_count: usize,
    pub failed_uploads: Vec<FailedUploadObject>,
    pub manifest_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UploadedObject {
    pub object_family: String,
    pub key: String,
    pub local_path: String,
    pub record_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedUploadObject {
    pub object_family: String,
    pub key: String,
    pub discarded_local_path: String,
    pub record_count: usize,
    pub error: String,
}

pub(super) fn append_capped<T>(
    values: &mut Vec<T>,
    value: T,
    max_len: usize,
    dropped_count: &mut usize,
) {
    values.push(value);
    if values.len() > max_len {
        values.remove(0);
        *dropped_count += 1;
    }
}

pub(super) fn push_capped_deque<T>(
    values: &mut VecDeque<T>,
    value: T,
    max_len: usize,
    dropped_count: &mut usize,
) {
    values.push_back(value);
    if values.len() > max_len {
        values.pop_front();
        *dropped_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FailedUploadObject, MAX_REPORTED_FAILURES, MAX_REPORTED_OBJECTS, UploadedObject,
        append_capped,
    };

    #[test]
    fn caps_uploaded_object_report_entries() {
        let mut values = Vec::new();
        let mut dropped_count = 0;

        for index in 0..=MAX_REPORTED_OBJECTS {
            append_capped(
                &mut values,
                UploadedObject {
                    object_family: "raw_market_event".to_owned(),
                    key: format!("key-{index}"),
                    local_path: format!("path-{index}"),
                    record_count: 1,
                },
                MAX_REPORTED_OBJECTS,
                &mut dropped_count,
            );
        }

        assert_eq!(values.len(), MAX_REPORTED_OBJECTS);
        assert_eq!(dropped_count, 1);
        assert_eq!(values.first().unwrap().key, "key-1");
    }

    #[test]
    fn caps_failed_upload_report_entries() {
        let mut values = Vec::new();
        let mut dropped_count = 0;

        for index in 0..=MAX_REPORTED_FAILURES {
            append_capped(
                &mut values,
                FailedUploadObject {
                    object_family: "raw_market_event".to_owned(),
                    key: format!("key-{index}"),
                    discarded_local_path: format!("path-{index}"),
                    record_count: 1,
                    error: "upload failed".to_owned(),
                },
                MAX_REPORTED_FAILURES,
                &mut dropped_count,
            );
        }

        assert_eq!(values.len(), MAX_REPORTED_FAILURES);
        assert_eq!(dropped_count, 1);
        assert_eq!(values.first().unwrap().key, "key-1");
    }
}
