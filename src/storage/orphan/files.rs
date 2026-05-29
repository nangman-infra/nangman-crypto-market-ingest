use super::super::StorageError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(super) fn parquet_footer_is_valid(path: &Path) -> Result<bool, StorageError> {
    let file = fs::File::open(path)?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(file).is_ok())
}

pub(super) fn parquet_files_under(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut files = Vec::new();
    collect_parquet_files(root, &mut files)?;
    Ok(files)
}

pub(super) fn mtime_ms(meta: &fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub(super) fn nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), StorageError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files(&path, files)?;
        } else if path.extension().is_some_and(|value| value == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}
