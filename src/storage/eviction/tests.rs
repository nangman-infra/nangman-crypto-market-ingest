use super::candidate::EvictableFile;
use super::collect::collect_files_on_disk;
use super::plan::{rank_evictable, safety_cutoff_ms};
use super::{EvictionConfig, evict_once, sealed_marker_path};
use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
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

#[cfg(unix)]
#[test]
fn collect_skips_symlinked_directories() {
    use std::os::unix::fs::symlink;

    let root = unique_root("symlink-root");
    let outside = unique_root("symlink-outside");
    let link_parent = root.join("run-1/raw_market_event");
    let outside_dir = outside.join("raw_market_event");
    fs::create_dir_all(&link_parent).unwrap();
    fs::create_dir_all(&outside_dir).unwrap();
    let outside_parquet = outside_dir.join("outside.parquet");
    fs::write(&outside_parquet, b"x").unwrap();
    fs::write(sealed_marker_path(&outside_parquet), b"").unwrap();
    symlink(&outside_dir, link_parent.join("linked")).unwrap();

    let files = collect_files_on_disk(&root).unwrap();

    assert!(files.is_empty());
    fs::remove_dir_all(&root).ok();
    fs::remove_dir_all(&outside).ok();
}

/// Far-future now_ms so any sealed-at-real-wall-clock file passes the safety cutoff.
const FAR_FUTURE_MS: i64 = 4_000_000_000_000;

#[test]
fn evict_deletes_only_files_with_sealed_sidecar() {
    let root = unique_root("sealed");
    let unsealed =
        root.join("run-1/raw_market_event/venue=binance/event_type=trade/unsealed.parquet");
    let sealed = root.join("run-1/raw_market_event/venue=binance/event_type=trade/sealed.parquet");
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
