use super::StorageError;
use super::eviction::sealed_marker_path;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

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

fn parquet_footer_is_valid(path: &Path) -> Result<bool, StorageError> {
    let file = fs::File::open(path)?;
    Ok(ParquetRecordBatchReaderBuilder::try_new(file).is_ok())
}

fn quarantine_root(spool_root: &Path) -> PathBuf {
    spool_root
        .parent()
        .map(|parent| parent.join("orphaned-unsealed"))
        .unwrap_or_else(|| spool_root.join("orphaned-unsealed"))
}

fn quarantine_destination(
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

fn parquet_files_under(root: &Path) -> Result<Vec<PathBuf>, StorageError> {
    let mut files = Vec::new();
    collect_parquet_files(root, &mut files)?;
    Ok(files)
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

fn mtime_ms(meta: &fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAR_FUTURE_MS: i64 = 4_000_000_000_000;

    fn unique_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("market-ingest-orphan-{name}-{}", nanos_now()))
    }

    #[test]
    fn old_invalid_unsealed_parquet_is_quarantined() {
        let root = unique_root("invalid");
        let l0_root = root.join("l0");
        let path = l0_root.join("run-1/raw_market_event/bad.parquet");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not parquet").unwrap();

        let cfg = UnsealedOrphanConfig {
            spool_root: l0_root.clone(),
            safety_floor_secs: 7_200,
        };
        let stats = cleanup_invalid_unsealed_once(&cfg, FAR_FUTURE_MS).unwrap();

        assert_eq!(stats.scanned_unsealed_count, 1);
        assert_eq!(stats.invalid_unsealed_count, 1);
        assert_eq!(stats.quarantined_count, 1);
        assert!(!path.exists());
        assert!(
            root.join("orphaned-unsealed/run-1/raw_market_event/bad.parquet")
                .exists()
        );
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn recent_unsealed_parquet_is_left_in_place() {
        let root = unique_root("recent");
        let l0_root = root.join("l0");
        let path = l0_root.join("run-1/raw_market_event/recent.parquet");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"not parquet").unwrap();

        let cfg = UnsealedOrphanConfig {
            spool_root: l0_root.clone(),
            safety_floor_secs: 7_200,
        };
        let stats = cleanup_invalid_unsealed_once(&cfg, 0).unwrap();

        assert_eq!(stats.scanned_unsealed_count, 1);
        assert_eq!(stats.recent_unsealed_count, 1);
        assert_eq!(stats.quarantined_count, 0);
        assert!(path.exists());
        fs::remove_dir_all(&root).ok();
    }
}
