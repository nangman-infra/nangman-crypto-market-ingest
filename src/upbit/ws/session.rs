use super::super::UpbitIngestError;
use super::super::stats::UpbitIngestWatchStats;
use super::super::universe::UpbitMarket;
use super::message::handle_message;
use super::types::{SessionEnd, SessionPoll, SessionTimers, Websocket};
use crate::storage::L0StorageSink;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::tungstenite;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_session(
    mut websocket: Websocket,
    stats: &mut UpbitIngestWatchStats,
    mut storage: Option<&mut L0StorageSink>,
    markets_by_code: &HashMap<String, UpbitMarket>,
    deadline: Instant,
    log_interval: Duration,
    stale_timeout: Duration,
    log_callback: &impl Fn(&UpbitIngestWatchStats),
    shutdown_flag: &Arc<AtomicBool>,
) -> Result<SessionEnd, UpbitIngestError> {
    let now = Instant::now();
    let mut timers = SessionTimers {
        next_log_at: now + log_interval,
        next_ping_at: now + Duration::from_secs(30),
        last_message_at: now,
        log_interval,
        stale_timeout,
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
                    handle_message(message, stats, storage.as_deref_mut(), markets_by_code).await
                {
                    stats.malformed_messages += 1;
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

        if let Some(end) = run_housekeeping(&mut websocket, stats, &mut timers, log_callback).await
        {
            return Ok(end);
        }
    }
}

fn next_session_tick(deadline: Instant, timers: &SessionTimers, now: Instant) -> Instant {
    let stale_at = timers.last_message_at + timers.stale_timeout;
    let poll_tick = now + Duration::from_millis(250);
    [
        deadline,
        timers.next_log_at,
        timers.next_ping_at,
        stale_at,
        poll_tick,
    ]
    .into_iter()
    .min()
    .unwrap_or(deadline)
}

async fn poll_session(
    websocket: &mut Websocket,
    next_tick: Instant,
) -> Result<SessionPoll, UpbitIngestError> {
    match timeout_at(next_tick, websocket.next()).await {
        Ok(Some(Ok(message))) => Ok(SessionPoll::Message(message)),
        Ok(Some(Err(_))) => Ok(SessionPoll::Disconnected("websocket_error")),
        Ok(None) => Ok(SessionPoll::Disconnected("ended")),
        Err(_) => Ok(SessionPoll::Tick),
    }
}

async fn run_housekeeping(
    websocket: &mut Websocket,
    stats: &mut UpbitIngestWatchStats,
    timers: &mut SessionTimers,
    log_callback: &impl Fn(&UpbitIngestWatchStats),
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
    None
}

async fn send_ping(websocket: &mut Websocket) -> Result<(), tungstenite::Error> {
    websocket
        .send(tungstenite::Message::Ping(Vec::new().into()))
        .await
}
