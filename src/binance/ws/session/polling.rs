use super::super::types::{SessionPoll, SessionTimers, Websocket};
use super::BinanceIngestError;
use futures_util::StreamExt;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

pub(super) fn next_session_tick(
    deadline: Instant,
    timers: &SessionTimers,
    now: Instant,
) -> Instant {
    let stale_at = timers.last_message_at + timers.stale_timeout;
    let poll_tick = now + Duration::from_millis(250);
    [
        deadline,
        timers.next_log_at,
        timers.next_ping_at,
        timers.next_derivative_snapshot_at,
        stale_at,
        poll_tick,
    ]
    .into_iter()
    .min()
    .unwrap_or(deadline)
}

pub(super) async fn poll_session(
    websocket: &mut Websocket,
    next_tick: Instant,
) -> Result<SessionPoll, BinanceIngestError> {
    match timeout_at(next_tick, websocket.next()).await {
        Ok(Some(Ok(message))) => Ok(SessionPoll::Message(message)),
        Ok(Some(Err(_))) => Ok(SessionPoll::Disconnected("websocket_error")),
        Ok(None) => Ok(SessionPoll::Disconnected("ended")),
        Err(_) => Ok(SessionPoll::Tick),
    }
}
