use super::files::nanos_now;
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
