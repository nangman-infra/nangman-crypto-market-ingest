use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub(super) async fn download_json_value(
    uploader: &S3Uploader,
    tmp_root: &Path,
    key: &str,
    label: &str,
) -> Result<Value, StorageError> {
    let tmp_path = temporary_json_path(tmp_root, label);
    if let Some(parent) = tmp_path.parent() {
        std::fs::create_dir_all(parent).map_err(StorageError::Io)?;
    }
    uploader.download_file(key, &tmp_path).await?;
    let bytes = match std::fs::read(&tmp_path) {
        Ok(value) => value,
        Err(error) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(StorageError::Io(error));
        }
    };
    let _ = std::fs::remove_file(&tmp_path);
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) fn json_status_success(value: &Value) -> bool {
    value
        .get("status")
        .and_then(|status| status.as_str())
        .is_some_and(|status| status == "success")
}

pub(super) fn input_time_range_end_ms(value: &Value) -> Option<i64> {
    value
        .get("input_time_range_end_ms")
        .and_then(|field| field.as_i64())
}

pub(super) fn pointer_manifest_key(value: &Value) -> Option<&str> {
    value
        .get("canonical_manifest_key")
        .or_else(|| value.get("manifest_key"))
        .and_then(|field| field.as_str())
}

pub(super) fn temporary_json_path(tmp_root: &Path, label: &str) -> PathBuf {
    tmp_root.join("_l1_pointer_lookup").join(format!(
        "market-normalize-{label}-{}-{}",
        std::process::id(),
        nanos_now()
    ))
}

fn nanos_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}
