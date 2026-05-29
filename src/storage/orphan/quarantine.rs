use super::super::StorageError;
use super::files::nanos_now;
use std::path::{Path, PathBuf};

pub(super) fn quarantine_root(spool_root: &Path) -> PathBuf {
    spool_root
        .parent()
        .map(|parent| parent.join("orphaned-unsealed"))
        .unwrap_or_else(|| spool_root.join("orphaned-unsealed"))
}

pub(super) fn quarantine_destination(
    spool_root: &Path,
    quarantine_root: &Path,
    path: &Path,
) -> Result<PathBuf, StorageError> {
    let relative = path
        .strip_prefix(spool_root)
        .map_err(|error| StorageError::InvalidConfig(error.to_string()))?;
    let candidate = quarantine_root.join(relative);
    Ok(unique_path(candidate))
}

fn unique_path(path: PathBuf) -> PathBuf {
    if !path.exists() {
        return path;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("file.parquet");
    for index in 1..1_000 {
        let candidate = path.with_file_name(format!("{file_name}.{index}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    path.with_file_name(format!("{file_name}.{}", nanos_now()))
}
