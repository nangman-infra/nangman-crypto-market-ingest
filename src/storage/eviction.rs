use super::StorageError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

const PROTECTED_MARKERS: &[&str] = &["symbol_health/", "source_health/", "gap_alert/"];

const SEALED_EXTENSION: &str = "sealed";
const TARGET_OFFSET_PCT: u8 = 5;

#[derive(Debug, Clone)]
pub struct EvictionConfig {
    pub spool_root: PathBuf,
    pub high_water_pct: u8,
    pub emergency_pct: u8,
    pub safety_floor_secs: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvictionStats {
    pub disk_used_pct_before: u8,
    pub disk_used_pct_after: u8,
    pub triggered: bool,
    pub emergency: bool,
    pub evicted_count: usize,
    pub evicted_bytes: u64,
    pub candidate_count: usize,
}

#[derive(Debug, Clone)]
struct EvictableFile {
    parquet_path: PathBuf,
    sealed_path: PathBuf,
    sealed_at_ms: i64,
    parquet_size: u64,
}

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

fn safety_cutoff_ms(now_ms: i64, safety_floor_secs: i64) -> i64 {
    now_ms.saturating_sub(safety_floor_secs.saturating_mul(1000))
}

fn rank_evictable(
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

fn collect_files_on_disk(root: &Path) -> Result<Vec<EvictableFile>, StorageError> {
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
        let entry_path = entry.path();
        if entry_path.is_dir() {
            walk_parquet(&entry_path, visit)?;
        } else if entry_path.extension().is_some_and(|ext| ext == "parquet") {
            visit(&entry_path)?;
        }
    }
    Ok(())
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

fn mtime_ms(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::fs;
    use std::time::SystemTime;

    fn unique_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "market-ingest-eviction-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    fn fake_evictable(path: &Path, sealed_at_ms: i64) -> EvictableFile {
        EvictableFile {
            parquet_path: path.to_path_buf(),
            sealed_path: sealed_marker_path(path),
            sealed_at_ms,
            parquet_size: 0,
        }
    }

    #[test]
    fn sealed_marker_path_appends_extension() {
        let parquet = Path::new("/tmp/run-1/raw_market_event/file.parquet");
        let sealed = sealed_marker_path(parquet);
        assert_eq!(
            sealed.to_string_lossy(),
            "/tmp/run-1/raw_market_event/file.parquet.sealed"
        );
    }

    #[test]
    fn rank_drops_protected_paths() {
        let root = Path::new("/spool");
        let raw = root.join("run-1/raw_market_event/venue=binance/file.parquet");
        let symbol_health = root.join("run-1/symbol_health/venue=binance/file.parquet");
        let source_health = root.join("run-1/source_health/venue=binance/file.parquet");
        let gap_alert = root.join("run-1/gap_alert/venue=binance/file.parquet");
        let files = vec![
            fake_evictable(&raw, 100),
            fake_evictable(&symbol_health, 100),
            fake_evictable(&source_health, 100),
            fake_evictable(&gap_alert, 100),
        ];
        let ranked = rank_evictable(files, 1_000, root);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].parquet_path, raw);
    }

    #[test]
    fn rank_drops_files_inside_safety_floor() {
        let root = Path::new("/spool");
        let recent = root.join("run-1/raw_market_event/recent.parquet");
        let old = root.join("run-1/raw_market_event/old.parquet");
        let files = vec![fake_evictable(&recent, 2_000), fake_evictable(&old, 500)];
        let ranked = rank_evictable(files, 1_000, root);
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].parquet_path, old);
    }

    #[test]
    fn rank_sorts_oldest_first() {
        let root = Path::new("/spool");
        let a = root.join("run-1/raw_market_event/a.parquet");
        let b = root.join("run-1/raw_market_event/b.parquet");
        let c = root.join("run-1/raw_market_event/c.parquet");
        let files = vec![
            fake_evictable(&b, 200),
            fake_evictable(&a, 100),
            fake_evictable(&c, 300),
        ];
        let ranked = rank_evictable(files, 1_000, root);
        assert_eq!(ranked[0].parquet_path, a);
        assert_eq!(ranked[1].parquet_path, b);
        assert_eq!(ranked[2].parquet_path, c);
    }

    #[test]
    fn safety_cutoff_subtracts_floor() {
        let now_ms = 1_700_000_000_000_i64;
        let cutoff = safety_cutoff_ms(now_ms, 7200);
        assert_eq!(cutoff, now_ms - 7_200_000);
    }

    #[test]
    fn safety_cutoff_handles_underflow() {
        let cutoff = safety_cutoff_ms(0, 7200);
        assert_eq!(cutoff, -7_200_000);
    }

    #[test]
    fn evict_skips_when_below_high_water() {
        let root = unique_root("below");
        fs::create_dir_all(&root).unwrap();
        let cfg = EvictionConfig {
            spool_root: root.clone(),
            high_water_pct: 70,
            emergency_pct: 90,
            safety_floor_secs: 7200,
        };
        let probe = || Ok(50_u8);
        let stats = evict_once(&cfg, 1_700_000_000_000, probe).unwrap();
        assert!(!stats.triggered);
        assert_eq!(stats.evicted_count, 0);
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn evict_flags_emergency() {
        let root = unique_root("emergency");
        fs::create_dir_all(&root).unwrap();
        let cfg = EvictionConfig {
            spool_root: root.clone(),
            high_water_pct: 70,
            emergency_pct: 90,
            safety_floor_secs: 7200,
        };
        let probe = || Ok(95_u8);
        let stats = evict_once(&cfg, 1, probe).unwrap();
        assert!(stats.triggered);
        assert!(stats.emergency);
        fs::remove_dir_all(&root).ok();
    }

    /// Far-future now_ms so any sealed-at-real-wall-clock file passes the safety cutoff.
    const FAR_FUTURE_MS: i64 = 4_000_000_000_000;

    #[test]
    fn evict_deletes_only_files_with_sealed_sidecar() {
        let root = unique_root("sealed");
        let unsealed =
            root.join("run-1/raw_market_event/venue=binance/event_type=trade/unsealed.parquet");
        let sealed =
            root.join("run-1/raw_market_event/venue=binance/event_type=trade/sealed.parquet");
        fs::create_dir_all(unsealed.parent().unwrap()).unwrap();
        fs::write(&unsealed, b"u").unwrap();
        fs::write(&sealed, b"s").unwrap();
        fs::write(sealed_marker_path(&sealed), b"").unwrap();
        let cfg = EvictionConfig {
            spool_root: root.clone(),
            high_water_pct: 70,
            emergency_pct: 90,
            safety_floor_secs: 7200,
        };
        // 80 -> trigger, 73 -> evict 1st, 60 -> stop, final 60
        let calls = RefCell::new(vec![80_u8, 73, 60, 60]);
        let probe = || {
            let mut calls = calls.borrow_mut();
            Ok(if calls.is_empty() {
                60
            } else {
                calls.remove(0)
            })
        };
        let stats = evict_once(&cfg, FAR_FUTURE_MS, probe).unwrap();
        assert!(stats.triggered);
        assert_eq!(stats.evicted_count, 1);
        assert!(unsealed.exists()); // unsealed file untouched
        assert!(!sealed.exists());
        assert!(!sealed_marker_path(&sealed).exists());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn evict_stops_when_target_reached() {
        let root = unique_root("stop");
        let dir = root.join("run-1/raw_market_event/venue=binance/event_type=trade");
        fs::create_dir_all(&dir).unwrap();
        let a = dir.join("a.parquet");
        let b = dir.join("b.parquet");
        let c = dir.join("c.parquet");
        for path in [&a, &b, &c] {
            fs::write(path, b"x").unwrap();
            fs::write(sealed_marker_path(path), b"").unwrap();
        }
        let cfg = EvictionConfig {
            spool_root: root.clone(),
            high_water_pct: 70,
            emergency_pct: 90,
            safety_floor_secs: 7200,
        };
        // initial 80, iter1 73 -> delete, iter2 68 -> delete, iter3 60 -> break, final 60
        let calls = RefCell::new(vec![80_u8, 73, 68, 60, 60]);
        let probe = || {
            let mut calls = calls.borrow_mut();
            Ok(if calls.is_empty() {
                60
            } else {
                calls.remove(0)
            })
        };
        let stats = evict_once(&cfg, FAR_FUTURE_MS, probe).unwrap();
        assert!(stats.triggered);
        assert_eq!(stats.evicted_count, 2);
        let remaining = [&a, &b, &c].iter().filter(|p| p.exists()).count();
        assert_eq!(remaining, 1);
        fs::remove_dir_all(&root).ok();
    }
}
