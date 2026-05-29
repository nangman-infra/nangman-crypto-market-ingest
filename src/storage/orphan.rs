mod files;
mod quarantine;
#[cfg(test)]
mod tests;

use super::StorageError;
use super::eviction::sealed_marker_path;
use std::fs;
use std::path::PathBuf;

use self::files::{mtime_ms, parquet_files_under, parquet_footer_is_valid};
use self::quarantine::{quarantine_destination, quarantine_root};

#[derive(Debug, Clone)]
pub struct UnsealedOrphanConfig {
    pub spool_root: PathBuf,
    pub safety_floor_secs: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnsealedOrphanStats {
    pub scanned_unsealed_count: usize,
    pub recent_unsealed_count: usize,
    pub valid_unsealed_count: usize,
    pub invalid_unsealed_count: usize,
    pub quarantined_count: usize,
    pub quarantined_bytes: u64,
    pub quarantine_root: Option<PathBuf>,
}

pub fn cleanup_invalid_unsealed_once(
    config: &UnsealedOrphanConfig,
    now_ms: i64,
) -> Result<UnsealedOrphanStats, StorageError> {
    let mut stats = UnsealedOrphanStats::default();
    if !config.spool_root.exists() {
        return Ok(stats);
    }

    let safety_cutoff_ms = now_ms.saturating_sub(config.safety_floor_secs.saturating_mul(1000));
    let quarantine_root = quarantine_root(&config.spool_root);
    let files = parquet_files_under(&config.spool_root)?;

    for path in files {
        if sealed_marker_path(&path).exists() {
            continue;
        }
        stats.scanned_unsealed_count += 1;

        let meta = fs::metadata(&path)?;
        if mtime_ms(&meta) > safety_cutoff_ms {
            stats.recent_unsealed_count += 1;
            continue;
        }

        if parquet_footer_is_valid(&path)? {
            stats.valid_unsealed_count += 1;
            continue;
        }

        stats.invalid_unsealed_count += 1;
        let destination = quarantine_destination(&config.spool_root, &quarantine_root, &path)?;
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(&path, &destination)?;
        stats.quarantined_count += 1;
        stats.quarantined_bytes += meta.len();
    }

    if stats.quarantined_count > 0 {
        stats.quarantine_root = Some(quarantine_root);
    }
    Ok(stats)
}
