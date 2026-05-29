use super::super::UpbitIngestError;
use super::super::stats::{UpbitGapAlert, UpbitIngestWatchStats};
use crate::clock;
use crate::shutdown::ShutdownListener;
use std::cmp;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

pub(super) fn spawn_shutdown_flag() -> Result<Arc<AtomicBool>, UpbitIngestError> {
    let flag = Arc::new(AtomicBool::new(false));
    let mut listener = ShutdownListener::new().map_err(|error| {
        UpbitIngestError::InvalidMessage(format!("failed to install shutdown listener: {error}"))
    })?;
    let signal_flag = Arc::clone(&flag);
    tokio::spawn(async move {
        listener.wait().await;
        signal_flag.store(true, Ordering::SeqCst);
    });
    Ok(flag)
}

pub(super) async fn sleep_or_shutdown(shutdown_flag: &Arc<AtomicBool>, duration: Duration) {
    let target = Instant::now() + duration;
    while Instant::now() < target {
        if shutdown_flag.load(Ordering::SeqCst) {
            return;
        }
        let remaining = target.saturating_duration_since(Instant::now());
        let step = cmp::min(remaining, Duration::from_millis(250));
        if step.is_zero() {
            return;
        }
        tokio::time::sleep(step).await;
    }
}

pub(super) fn record_reconnect(stats: &mut UpbitIngestWatchStats, reason: &'static str) {
    let now_ms = clock::now_ms();
    stats.reconnect_count += 1;
    stats.last_reconnect_at_ms = Some(now_ms);
    stats.source_health_status = "reconnecting".to_owned();
    stats.source_health_events += 1;
    stats.record_gap_alert(UpbitGapAlert {
        gap_type: "ws_reconnect".to_owned(),
        symbol: String::new(),
        detected_at_ms: now_ms,
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "reconnect_with_backoff".to_owned(),
        heal_status: reason.to_owned(),
    });
}
