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
    pub(super) derivative_snapshot_interval: Duration,
    pub(super) next_derivative_snapshot_at: Instant,
}

pub(in crate::binance) struct BinanceL0WatchConfig<'a> {
    pub(in crate::binance) duration: Duration,
    pub(in crate::binance) log_interval: Duration,
    pub(in crate::binance) derivative_snapshot_interval: Duration,
    pub(in crate::binance) futures_rest_base_url: &'a str,
}
