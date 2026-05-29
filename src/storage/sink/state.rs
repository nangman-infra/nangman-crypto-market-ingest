use super::super::eviction::sealed_marker_path;
use super::report::{
    FailedUploadObject, MAX_REPORTED_FAILURES, MAX_REPORTED_OBJECTS, StorageReport, UploadedObject,
    append_capped, push_capped_deque,
};
use super::{L0StorageSink, StorageError};
use std::path::PathBuf;

impl L0StorageSink {
    pub async fn upload_manifest(&mut self) -> Result<(), StorageError> {
        let key = format!("runs/run_id={}/manifest.json", self.config.run_id);
        self.manifest_key = Some(key.clone());
        let manifest_object = UploadedObject {
            object_family: "manifest".to_owned(),
            key: key.clone(),
            local_path: format!("s3://{}/{}", self.config.bucket, key),
            record_count: 1,
        };
        let mut report = self.report();
        append_capped(
            &mut report.uploaded_objects,
            manifest_object.clone(),
            MAX_REPORTED_OBJECTS,
            &mut report.uploaded_object_dropped_count,
        );
        report.uploaded_object_count += 1;
        report.uploaded_object_retained_count = report.uploaded_objects.len();
        let bytes = serde_json::to_vec_pretty(&report)?;
        self.uploader.upload_json(&key, bytes).await?;
        self.record_uploaded_object(manifest_object);
        Ok(())
    }

    pub fn report(&self) -> StorageReport {
        StorageReport {
            bucket: self.config.bucket.clone(),
            run_id: self.config.run_id.clone(),
            record_count: self.next_ordinal.saturating_sub(1),
            uploaded_object_count: self.uploaded_object_count,
            uploaded_object_retained_count: self.uploaded_objects.len(),
            uploaded_object_dropped_count: self.uploaded_object_dropped_count,
            uploaded_objects: self.uploaded_objects.iter().cloned().collect(),
            failed_upload_count: self.failed_upload_count,
            failed_upload_retained_count: self.failed_uploads.len(),
            failed_upload_dropped_count: self.failed_upload_dropped_count,
            failed_uploads: self.failed_uploads.iter().cloned().collect(),
            manifest_key: self.manifest_key.clone(),
        }
    }

    pub(super) async fn upload(
        &mut self,
        object_family: &str,
        key: String,
        local_path: PathBuf,
        record_count: usize,
    ) -> Result<(), StorageError> {
        match self.uploader.upload_file(&key, &local_path).await {
            Ok(()) => {
                let sealed = sealed_marker_path(&local_path);
                let _ = tokio::fs::write(&sealed, b"").await;
                self.record_uploaded_object(UploadedObject {
                    object_family: object_family.to_owned(),
                    key,
                    local_path: local_path.display().to_string(),
                    record_count,
                });
            }
            Err(error) => {
                let error = error.to_string();
                self.record_failed_upload(FailedUploadObject {
                    object_family: object_family.to_owned(),
                    key,
                    discarded_local_path: local_path.display().to_string(),
                    record_count,
                    error,
                });
            }
        }
        Ok(())
    }

    fn record_uploaded_object(&mut self, object: UploadedObject) {
        self.uploaded_object_count += 1;
        push_capped_deque(
            &mut self.uploaded_objects,
            object,
            MAX_REPORTED_OBJECTS,
            &mut self.uploaded_object_dropped_count,
        );
    }

    fn record_failed_upload(&mut self, failure: FailedUploadObject) {
        self.failed_upload_count += 1;
        push_capped_deque(
            &mut self.failed_uploads,
            failure,
            MAX_REPORTED_FAILURES,
            &mut self.failed_upload_dropped_count,
        );
    }

    pub(super) fn local_path(&self, key: &str) -> Result<PathBuf, StorageError> {
        let local_path = self.config.spool_root.join(&self.config.run_id).join(key);
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(local_path)
    }

    pub(super) fn take_ordinal(&mut self) -> u64 {
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        ordinal
    }
}
