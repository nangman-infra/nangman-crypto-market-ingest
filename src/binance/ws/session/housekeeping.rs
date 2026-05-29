use super::super::types::{SessionEnd, SessionTimers, Websocket};
use super::snapshots::append_and_flush_derivative_snapshots;
use super::{BinanceL0WatchStats, BinanceMarket};
use crate::storage::L0StorageSink;
use futures_util::SinkExt;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;

pub(super) async fn run_housekeeping(
    websocket: &mut Websocket,
    stats: &mut BinanceL0WatchStats,
    storage: Option<&mut L0StorageSink>,
    markets: &[BinanceMarket],
    futures_rest_base_url: &str,
    timers: &mut SessionTimers,
    log_callback: &impl Fn(&BinanceL0WatchStats),
) -> Option<SessionEnd> {
    let now = Instant::now();
    if now >= timers.last_message_at + timers.stale_timeout {
        return Some(SessionEnd::Disconnected("stale_timeout"));
    }
    if now >= timers.next_ping_at && send_ping(websocket).await.is_err() {
        return Some(SessionEnd::Disconnected("ping_failed"));
    }
    if now >= timers.next_ping_at {
        timers.next_ping_at += Duration::from_secs(30);
    }
    if now >= timers.next_log_at {
        stats.update_health();
        log_callback(stats);
        timers.next_log_at += timers.log_interval;
    }
    if now >= timers.next_derivative_snapshot_at {
        if let Some(sink) = storage {
            append_and_flush_derivative_snapshots(futures_rest_base_url, markets, sink).await;
        }
        timers.next_derivative_snapshot_at += timers.derivative_snapshot_interval;
        while timers.next_derivative_snapshot_at <= Instant::now() {
            timers.next_derivative_snapshot_at += timers.derivative_snapshot_interval;
        }
    }
    None
}

async fn send_ping(websocket: &mut Websocket) -> Result<(), tungstenite::Error> {
    websocket
        .send(tungstenite::Message::Ping(Vec::new().into()))
        .await
}
