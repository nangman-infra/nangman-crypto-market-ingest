use super::local::local_input_entries;
use super::s3::s3_input_keys;
use crate::normalize::args::InputRange;
use crate::normalize::mode::RunMode;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub(crate) struct InputEntry {
    pub(crate) key: String,
    pub(crate) path: Option<PathBuf>,
    pub(crate) source: InputEntrySource,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum InputEntrySource {
    Local,
    S3,
}

pub(crate) async fn collect_input_entries(
    s3: &S3Uploader,
    l0_local_root: &Path,
    range: InputRange,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
) -> Result<Vec<InputEntry>, StorageError> {
    let local_entries = if matches!(run_mode, RunMode::Live) {
        local_input_entries(l0_local_root, range)?
    } else {
        Vec::new()
    };
    let s3_keys = s3_input_keys(s3, range, run_mode, l0_run_key_overlap_ms).await?;
    Ok(merge_entries(local_entries, s3_keys))
}

pub(super) fn merge_entries(
    local_entries: Vec<InputEntry>,
    s3_keys: Vec<String>,
) -> Vec<InputEntry> {
    let mut entries = BTreeMap::new();
    for entry in local_entries {
        entries.insert(entry.key.clone(), entry);
    }
    for key in s3_keys {
        entries.entry(key.clone()).or_insert(InputEntry {
            key,
            path: None,
            source: InputEntrySource::S3,
        });
    }
    entries.into_values().collect()
}
