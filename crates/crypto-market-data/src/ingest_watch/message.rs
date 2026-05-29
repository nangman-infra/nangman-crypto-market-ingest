use crate::error::MarketDataError;
use crate::stats::BinanceIngestWatchStats;
use std::str;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite;

pub(super) enum IngestWebSocketMessage {
    Payload(String),
    Pong(tungstenite::Bytes),
    Closed,
    Control,
}

pub(super) fn classify_websocket_message(
    message: tungstenite::Message,
    stats: &mut BinanceIngestWatchStats,
    track_source_health: bool,
) -> Result<IngestWebSocketMessage, MarketDataError> {
    match message {
        tungstenite::Message::Text(text) => Ok(IngestWebSocketMessage::Payload(text.to_string())),
        tungstenite::Message::Binary(bytes) => {
            let text = str::from_utf8(bytes.as_ref()).map_err(|error| {
                MarketDataError::InvalidMessage(format!(
                    "binary websocket payload is not utf-8: {error}"
                ))
            })?;
            Ok(IngestWebSocketMessage::Payload(text.to_owned()))
        }
        tungstenite::Message::Ping(payload) => {
            stats.control_messages += 1;
            stats.pings_received += 1;
            if track_source_health {
                stats.source_health_status = "connected".to_owned();
            }
            Ok(IngestWebSocketMessage::Pong(payload))
        }
        tungstenite::Message::Pong(_) => {
            stats.control_messages += 1;
            stats.pongs_received += 1;
            Ok(IngestWebSocketMessage::Control)
        }
        tungstenite::Message::Close(_) => {
            stats.control_messages += 1;
            stats.close_messages += 1;
            if track_source_health {
                stats.source_health_status = "closed".to_owned();
                stats.source_health_events += 1;
            }
            Ok(IngestWebSocketMessage::Closed)
        }
        tungstenite::Message::Frame(_) => {
            stats.control_messages += 1;
            Ok(IngestWebSocketMessage::Control)
        }
    }
}

pub(super) fn advance_log_if_due(
    next_log: &mut Instant,
    log_interval: Duration,
    mut on_due: impl FnMut(),
) {
    let now = Instant::now();
    if now < *next_log {
        return;
    }
    on_due();
    if log_interval.is_zero() {
        *next_log = now;
        return;
    }
    while *next_log <= now {
        *next_log += log_interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_log_if_due_handles_zero_interval_without_spinning() {
        let mut next_log = Instant::now() - Duration::from_millis(1);
        let mut calls = 0;

        advance_log_if_due(&mut next_log, Duration::ZERO, || {
            calls += 1;
        });

        assert_eq!(calls, 1);
    }
}
