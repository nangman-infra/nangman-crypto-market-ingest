use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub(super) async fn wait_until_shutdown(shutdown_requested: &Arc<AtomicBool>) {
    while !shutdown_requested.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(super) async fn sleep_or_shutdown(shutdown_requested: &Arc<AtomicBool>, seconds: u64) {
    for _ in 0..seconds {
        if shutdown_requested.load(Ordering::SeqCst) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
