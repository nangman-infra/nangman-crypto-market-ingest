use super::InputEntry;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use std::path::{Path, PathBuf};

pub(super) struct MaterializedInputEntry {
    pub(super) entry: InputEntry,
    pub(super) local_path: PathBuf,
    pub(super) remove_after_read: bool,
    pub(super) downloaded_bytes: u64,
}

pub(super) async fn materialize_entry(
    s3: S3Uploader,
    catchup_session_root: PathBuf,
    index: usize,
    entry: InputEntry,
) -> Result<(usize, MaterializedInputEntry), StorageError> {
    if let Some(path) = entry.path.clone() {
        return Ok((
            index,
            MaterializedInputEntry {
                entry,
                local_path: path,
                remove_after_read: false,
                downloaded_bytes: 0,
            },
        ));
    }
    // Catch-up / fallback tmp layout: catchup_tmp_root/{session_id}/{key}
    // The whole session dir is removed by cleanup_session_tmp at run end.
    let spool_path = catchup_session_root.join(&entry.key);
    s3.download_file(&entry.key, &spool_path).await?;
    let downloaded_bytes = file_size_best_effort(&spool_path).await.unwrap_or(0);
    Ok((
        index,
        MaterializedInputEntry {
            entry,
            local_path: spool_path,
            remove_after_read: true,
            downloaded_bytes,
        },
    ))
}

pub(super) async fn remove_file_best_effort(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

async fn file_size_best_effort(path: &Path) -> Option<u64> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .map(|metadata| metadata.len())
}
