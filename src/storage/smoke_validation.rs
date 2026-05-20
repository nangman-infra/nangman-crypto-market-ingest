use super::sink::StorageReport;

pub fn storage_has_family(storage: &StorageReport, object_family: &str) -> bool {
    storage
        .uploaded_objects
        .iter()
        .any(|object| object.object_family == object_family)
}

pub fn require_storage_family(
    storage: &StorageReport,
    venue: &str,
    object_family: &str,
) -> Result<(), String> {
    if storage_has_family(storage, object_family) {
        return Ok(());
    }
    Err(format!("{venue} L0 storage did not upload {object_family}"))
}

pub fn validate_common_storage_report(storage: &StorageReport, venue: &str) -> Result<(), String> {
    if storage.failed_upload_count > 0 {
        return Err(format!(
            "{venue} L0 storage exhausted retries for {} uploads",
            storage.failed_upload_count
        ));
    }
    if storage.record_count == 0 || storage.uploaded_object_count == 0 {
        return Err(format!("{venue} L0 storage produced no objects"));
    }
    if storage.manifest_key.is_none() {
        return Err(format!("{venue} L0 storage did not upload manifest.json"));
    }
    require_storage_family(storage, venue, "source_health")?;
    require_storage_family(storage, venue, "symbol_health")
}
