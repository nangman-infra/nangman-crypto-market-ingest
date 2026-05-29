use super::entry::{InputEntry, InputEntrySource};
use super::time::{HourPart, hourly_parts};
use crate::normalize::args::InputRange;
use crate::storage::StorageError;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(super) fn local_input_entries(
    root: &Path,
    range: InputRange,
) -> Result<Vec<InputEntry>, StorageError> {
    let parts = hourly_parts(range.start_ms, range.end_ms);
    let mut entries = BTreeMap::new();
    for path in parquet_files_under(root)? {
        let Some(key) = key_from_local_path(root, &path) else {
            continue;
        };
        if key_matches_parts(&key, &parts) {
            entries.insert(
                key.clone(),
                InputEntry {
                    key,
                    path: Some(path),
                    source: InputEntrySource::Local,
                },
            );
        }
    }
    Ok(entries.into_values().collect())
}

pub(super) fn key_matches_parts(key: &str, parts: &[HourPart]) -> bool {
    let recognized = key.starts_with("raw_market_event/")
        || key.starts_with("symbol_health/")
        || key.starts_with("source_health/")
        || key.starts_with("gap_alert/");
    recognized
        && parts.iter().any(|part| {
            key.contains(&format!(
                "/event_date={}/hour={:02}/",
                part.event_date, part.hour
            ))
        })
}

pub(super) fn key_from_local_path(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let normalized = relative.to_string_lossy().replace('\\', "/");
    for marker in [
        "raw_market_event/",
        "symbol_health/",
        "source_health/",
        "gap_alert/",
    ] {
        if let Some(index) = normalized.find(marker) {
            return Some(normalized[index..].to_owned());
        }
    }
    None
}

fn parquet_files_under(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    collect_parquet_files(root, &mut files)?;
    Ok(files)
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<(), StorageError> {
    for entry in std::fs::read_dir(path)? {
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
