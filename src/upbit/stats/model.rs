use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, Serialize)]
pub struct UpbitIngestWatchStats {
    pub stream_url: String,
    pub planned_stream_count: usize,
    pub received_messages: u64,
    pub parsed_messages: u64,
    pub malformed_messages: u64,
    pub control_messages: u64,
    pub pings_received: u64,
    pub pongs_received: u64,
    pub close_messages: u64,
    pub status_messages: u64,
    pub kind_counts: BTreeMap<String, u64>,
    pub symbol_counts: BTreeMap<String, u64>,
    pub ticker_messages: u64,
    pub trade_messages: u64,
    pub orderbook_messages: u64,
    pub derived_book_tickers: u64,
    pub sequence_anomalies: u64,
    pub source_health_status: String,
    pub source_health_events: u64,
    pub reconnect_count: u64,
    pub last_reconnect_at_ms: Option<i64>,
    pub gap_alert_count: u64,
    pub recent_gap_alerts: VecDeque<UpbitGapAlert>,
    pub last_exchange_timestamp_ms: Option<i64>,
    pub last_ingest_timestamp_ms: Option<i64>,
    pub latest_stream_lag_ms: i64,
    pub symbol_last_event_time_ms: BTreeMap<String, i64>,
    pub symbol_last_ingest_time_ms: BTreeMap<String, i64>,
    #[serde(skip)]
    pub gap_alerts: VecDeque<UpbitGapAlert>,
    pub last_best_quotes: BTreeMap<String, UpbitBestQuote>,
    #[serde(skip)]
    pub(super) last_trade_sequence_by_symbol: BTreeMap<String, i64>,
    #[serde(skip)]
    pub(super) last_orderbook_timestamp_by_symbol: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpbitBestQuote {
    pub symbol: String,
    pub best_bid: f64,
    pub best_bid_size: f64,
    pub best_ask: f64,
    pub best_ask_size: f64,
    pub exchange_timestamp_ms: i64,
    pub stream_type: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpbitGapAlert {
    pub gap_type: String,
    pub symbol: String,
    pub detected_at_ms: i64,
    pub expected_sequence_id: Option<i64>,
    pub observed_sequence_id: Option<i64>,
    pub heal_action: String,
    pub heal_status: String,
}

impl UpbitIngestWatchStats {
    pub fn new(stream_url: String, planned_stream_count: usize) -> Self {
        Self {
            stream_url,
            planned_stream_count,
            received_messages: 0,
            parsed_messages: 0,
            malformed_messages: 0,
            control_messages: 0,
            pings_received: 0,
            pongs_received: 0,
            close_messages: 0,
            status_messages: 0,
            kind_counts: BTreeMap::new(),
            symbol_counts: BTreeMap::new(),
            ticker_messages: 0,
            trade_messages: 0,
            orderbook_messages: 0,
            derived_book_tickers: 0,
            sequence_anomalies: 0,
            source_health_status: "connected".to_owned(),
            source_health_events: 1,
            reconnect_count: 0,
            last_reconnect_at_ms: None,
            gap_alert_count: 0,
            recent_gap_alerts: VecDeque::new(),
            last_exchange_timestamp_ms: None,
            last_ingest_timestamp_ms: None,
            latest_stream_lag_ms: 0,
            symbol_last_event_time_ms: BTreeMap::new(),
            symbol_last_ingest_time_ms: BTreeMap::new(),
            gap_alerts: VecDeque::new(),
            last_best_quotes: BTreeMap::new(),
            last_trade_sequence_by_symbol: BTreeMap::new(),
            last_orderbook_timestamp_by_symbol: BTreeMap::new(),
        }
    }
}
