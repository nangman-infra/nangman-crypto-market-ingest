use super::candidate::EvictableFile;
use super::sealed_marker_path;
use crate::storage::StorageError;
use std::path::Path;
use std::time::SystemTime;

pub(super) fn collect_files_on_disk(root: &Path) -> Result<Vec<EvictableFile>, StorageError> {
    let mut result = Vec::new();
    if !root.exists() {
        return Ok(result);
    }
    walk_parquet(root, &mut |path| {
        let sealed_path = sealed_marker_path(path);
        let sealed_meta = match std::fs::metadata(&sealed_path) {
            Ok(meta) => meta,
            Err(_) => return Ok(()),
        };
        let sealed_at_ms = mtime_ms(&sealed_meta);
        let parquet_meta = std::fs::metadata(path)?;
        result.push(EvictableFile {
            parquet_path: path.to_path_buf(),
            sealed_path,
            sealed_at_ms,
            parquet_size: parquet_meta.len(),
        });
        Ok(())
    })?;
    Ok(result)
}

fn walk_parquet<F>(path: &Path, visit: &mut F) -> Result<(), StorageError>
where
    F: FnMut(&Path) -> Result<(), StorageError>,
{
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        let entry_path = entry.path();
        if entry_type.is_dir() {
            walk_parquet(&entry_path, visit)?;
        } else if entry_type.is_file() && entry_path.extension().is_some_and(|ext| ext == "parquet")
        {
            visit(&entry_path)?;
        }
    }
    Ok(())
}

fn mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}
