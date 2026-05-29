use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};

pub(super) type Websocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) enum SessionEnd {
    ShutdownRequested,
    DeadlineReached,
    Disconnected(&'static str),
}

pub(super) enum SessionPoll {
    Message(tungstenite::Message),
    Disconnected(&'static str),
    Tick,
}

pub(super) struct SessionTimers {
    pub(super) next_log_at: Instant,
    pub(super) next_ping_at: Instant,
    pub(super) last_message_at: Instant,
    pub(super) log_interval: Duration,
    pub(super) stale_timeout: Duration,
}
