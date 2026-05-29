use super::candidate::EvictableFile;
use std::path::Path;

const PROTECTED_MARKERS: &[&str] = &["symbol_health/", "source_health/", "gap_alert/"];

pub(super) fn safety_cutoff_ms(now_ms: i64, safety_floor_secs: i64) -> i64 {
    now_ms.saturating_sub(safety_floor_secs.saturating_mul(1000))
}

pub(super) fn rank_evictable(
    files: Vec<EvictableFile>,
    safety_cutoff_ms: i64,
    root: &Path,
) -> Vec<EvictableFile> {
    let mut filtered: Vec<EvictableFile> = files
        .into_iter()
        .filter(|file| !is_protected_path(&file.parquet_path, root))
        .filter(|file| file.sealed_at_ms <= safety_cutoff_ms)
        .collect();
    filtered.sort_by_key(|file| file.sealed_at_ms);
    filtered
}

fn is_protected_path(path: &Path, root: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let normalized = relative.to_string_lossy().replace('\\', "/");
    PROTECTED_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}
