use super::*;
use std::path::PathBuf;

fn unique_root(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "market-normalize-read-{}-{}-{}",
        name,
        std::process::id(),
        nanos
    ))
}

#[tokio::test]
async fn cleanup_session_tmp_removes_session_dir_only() {
    let root = unique_root("cleanup");
    let session = "l1_run_session_test";
    let session_dir = root.join(session);
    let other_dir = root.join("other_session");
    std::fs::create_dir_all(session_dir.join("raw_market_event/venue=binance")).unwrap();
    std::fs::create_dir_all(other_dir.join("raw_market_event")).unwrap();
    std::fs::write(
        session_dir.join("raw_market_event/venue=binance/a.parquet"),
        b"",
    )
    .unwrap();
    std::fs::write(other_dir.join("raw_market_event/keep.parquet"), b"").unwrap();

    cleanup_session_tmp(&root, session).await;

    assert!(!session_dir.exists());
    assert!(other_dir.exists());
    assert!(other_dir.join("raw_market_event/keep.parquet").exists());
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn cleanup_session_tmp_is_no_op_when_session_missing() {
    let root = unique_root("cleanup-missing");
    cleanup_session_tmp(&root, "never-existed").await;
    // No panic, no error. Best-effort.
}
