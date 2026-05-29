use super::super::{BinanceIngestError, BinanceMarket, stats::BinanceL0WatchStats};
use super::message::handle_message;
use super::types::{SessionEnd, SessionPoll, SessionTimers, Websocket};
use crate::storage::L0StorageSink;
use housekeeping::run_housekeeping;
use polling::{next_session_tick, poll_session};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

mod housekeeping;
mod polling;
mod snapshots;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_session(
    mut websocket: Websocket,
    stats: &mut BinanceL0WatchStats,
    mut storage: Option<&mut L0StorageSink>,
    markets_by_raw: &HashMap<String, BinanceMarket>,
    markets: &[BinanceMarket],
    deadline: Instant,
    log_interval: Duration,
    derivative_snapshot_interval: Duration,
    futures_rest_base_url: &str,
    stale_timeout: Duration,
    log_callback: &impl Fn(&BinanceL0WatchStats),
    shutdown_flag: &Arc<AtomicBool>,
) -> Result<SessionEnd, BinanceIngestError> {
    let now = Instant::now();
    let mut timers = SessionTimers {
        next_log_at: now + log_interval,
        next_ping_at: now + Duration::from_secs(30),
        last_message_at: now,
        log_interval,
        stale_timeout,
        derivative_snapshot_interval,
        next_derivative_snapshot_at: now + derivative_snapshot_interval,
    };

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            return Ok(SessionEnd::ShutdownRequested);
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(SessionEnd::DeadlineReached);
        }
        let next_tick = next_session_tick(deadline, &timers, now);

        match poll_session(&mut websocket, next_tick).await? {
            SessionPoll::Message(message) => {
                timers.last_message_at = Instant::now();
                if let Err(error) =
                    handle_message(message, stats, storage.as_deref_mut(), markets_by_raw).await
                {
                    let _ = error;
                    return Ok(SessionEnd::Disconnected("message_error"));
                }
            }
            SessionPoll::Disconnected(reason) => {
                stats.malformed_messages += u64::from(reason == "websocket_error");
                return Ok(SessionEnd::Disconnected(reason));
            }
            SessionPoll::Tick => {}
        }

        if let Some(end) = run_housekeeping(
            &mut websocket,
            stats,
            storage.as_deref_mut(),
            markets,
            futures_rest_base_url,
            &mut timers,
            log_callback,
        )
        .await
        {
            return Ok(end);
        }
    }
}
