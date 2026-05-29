use super::StorageError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

mod candidate;
mod collect;
mod config;
mod plan;

#[cfg(test)]
mod tests;

pub use config::{EvictionConfig, EvictionStats};

use collect::collect_files_on_disk;
use plan::{rank_evictable, safety_cutoff_ms};

const SEALED_EXTENSION: &str = "sealed";
const TARGET_OFFSET_PCT: u8 = 5;

pub fn sealed_marker_path(parquet_path: &Path) -> PathBuf {
    let mut bytes: OsString = parquet_path.as_os_str().to_owned();
    bytes.push(".");
    bytes.push(SEALED_EXTENSION);
    PathBuf::from(bytes)
}

pub fn evict_once<F>(
    config: &EvictionConfig,
    now_ms: i64,
    mut probe_disk_pct: F,
) -> Result<EvictionStats, StorageError>
where
    F: FnMut() -> Result<u8, StorageError>,
{
    let mut stats = EvictionStats::default();
    let initial = probe_disk_pct()?;
    stats.disk_used_pct_before = initial;
    stats.disk_used_pct_after = initial;
    if initial < config.high_water_pct {
        return Ok(stats);
    }
    stats.triggered = true;
    stats.emergency = initial >= config.emergency_pct;

    let on_disk = collect_files_on_disk(&config.spool_root)?;
    let safety_cutoff_ms = safety_cutoff_ms(now_ms, config.safety_floor_secs);
    let candidates = rank_evictable(on_disk, safety_cutoff_ms, &config.spool_root);
    stats.candidate_count = candidates.len();

    let target = config.high_water_pct.saturating_sub(TARGET_OFFSET_PCT);
    for candidate in candidates {
        let current = probe_disk_pct()?;
        if current < target {
            break;
        }
        if std::fs::remove_file(&candidate.parquet_path).is_ok() {
            let _ = std::fs::remove_file(&candidate.sealed_path);
            stats.evicted_count += 1;
            stats.evicted_bytes += candidate.parquet_size;
        }
    }

    stats.disk_used_pct_after = probe_disk_pct()?;
    Ok(stats)
}
