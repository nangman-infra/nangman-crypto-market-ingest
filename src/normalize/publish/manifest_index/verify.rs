use super::*;

pub(super) async fn verify_manifest(
    uploader: &S3Uploader,
    spool_root: &Path,
    l1_run_id: &str,
    manifest_key: &str,
) -> Result<(), StorageError> {
    let path = local_output_path(spool_root, l1_run_id, manifest_key).with_extension("verify.json");
    uploader.download_file(manifest_key, &path).await?;
    let bytes = tokio::fs::read(&path).await?;
    let manifest = serde_json::from_slice::<L1Manifest>(&bytes)?;
    remove_file_best_effort(&path).await;
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
        return Err(StorageError::InvalidConfig(
            "manifest verification failed".to_owned(),
        ));
    }
    Ok(())
}
