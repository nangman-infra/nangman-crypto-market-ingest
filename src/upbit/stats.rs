use super::events::{UpbitOrderbookMessage, UpbitParsedEvent, UpbitTradeMessage};
use serde::Serialize;
use std::collections::BTreeMap;

const MAX_STORED_GAP_ALERTS: usize = 1_000;
const MAX_RECENT_GAP_ALERTS: usize = 20;

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
    pub gap_alert_count: u64,
    pub recent_gap_alerts: Vec<UpbitGapAlert>,
    pub last_exchange_timestamp_ms: Option<i64>,
    pub last_ingest_timestamp_ms: Option<i64>,
    pub latest_stream_lag_ms: i64,
    pub symbol_last_event_time_ms: BTreeMap<String, i64>,
    pub symbol_last_ingest_time_ms: BTreeMap<String, i64>,
    #[serde(skip)]
    pub gap_alerts: Vec<UpbitGapAlert>,
    pub last_best_quotes: BTreeMap<String, UpbitBestQuote>,
    #[serde(skip)]
    last_trade_sequence_by_symbol: BTreeMap<String, i64>,
    #[serde(skip)]
    last_orderbook_timestamp_by_symbol: BTreeMap<String, i64>,
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
            gap_alert_count: 0,
            recent_gap_alerts: Vec::new(),
            last_exchange_timestamp_ms: None,
            last_ingest_timestamp_ms: None,
            latest_stream_lag_ms: 0,
            symbol_last_event_time_ms: BTreeMap::new(),
            symbol_last_ingest_time_ms: BTreeMap::new(),
            gap_alerts: Vec::new(),
            last_best_quotes: BTreeMap::new(),
            last_trade_sequence_by_symbol: BTreeMap::new(),
            last_orderbook_timestamp_by_symbol: BTreeMap::new(),
        }
    }

    pub fn record_event(&mut self, event: UpbitParsedEvent, detected_at_ms: i64) {
        self.parsed_messages += 1;
        if let Some(exchange_timestamp_ms) = event.exchange_timestamp_ms() {
            self.record_timing(exchange_timestamp_ms, detected_at_ms);
            if let Some(symbol) = event.symbol() {
                self.symbol_last_event_time_ms
                    .insert(symbol.to_owned(), exchange_timestamp_ms);
                self.symbol_last_ingest_time_ms
                    .insert(symbol.to_owned(), detected_at_ms);
            }
        }
        *self
            .kind_counts
            .entry(event.kind_name().to_owned())
            .or_insert(0) += 1;
        if let Some(symbol) = event.symbol() {
            *self.symbol_counts.entry(symbol.to_owned()).or_insert(0) += 1;
        }

        match event {
            UpbitParsedEvent::Ticker(_) => self.ticker_messages += 1,
            UpbitParsedEvent::Trade(message) => self.record_trade(message, detected_at_ms),
            UpbitParsedEvent::Orderbook(message) => self.record_orderbook(message, detected_at_ms),
            UpbitParsedEvent::Status(_) => self.status_messages += 1,
            UpbitParsedEvent::Error {
                name: _,
                message: _,
            } => {
                self.record_gap_alert(UpbitGapAlert {
                    gap_type: "upbit_error".to_owned(),
                    symbol: "venue".to_owned(),
                    detected_at_ms,
                    expected_sequence_id: None,
                    observed_sequence_id: None,
                    heal_action: "inspect_error".to_owned(),
                    heal_status: "detected".to_owned(),
                });
            }
            UpbitParsedEvent::Unknown(_) => self.malformed_messages += 1,
        }
    }

    fn record_trade(&mut self, message: UpbitTradeMessage, detected_at_ms: i64) {
        self.trade_messages += 1;
        if let Some(last_sequence) = self
            .last_trade_sequence_by_symbol
            .insert(message.code.clone(), message.sequential_id)
            && message.sequential_id < last_sequence
        {
            self.sequence_anomalies += 1;
            self.record_gap_alert(UpbitGapAlert {
                gap_type: "ordering_violation".to_owned(),
                symbol: message.code,
                detected_at_ms,
                expected_sequence_id: Some(last_sequence),
                observed_sequence_id: Some(message.sequential_id),
                heal_action: "mark_incomplete_and_continue".to_owned(),
                heal_status: "detected".to_owned(),
            });
        }
    }

    fn record_orderbook(&mut self, message: UpbitOrderbookMessage, detected_at_ms: i64) {
        self.orderbook_messages += 1;
        if let Some(last_timestamp) = self
            .last_orderbook_timestamp_by_symbol
            .insert(message.code.clone(), message.timestamp)
            && message.timestamp < last_timestamp
        {
            self.record_gap_alert(UpbitGapAlert {
                gap_type: "ordering_violation".to_owned(),
                symbol: message.code.clone(),
                detected_at_ms,
                expected_sequence_id: Some(last_timestamp),
                observed_sequence_id: Some(message.timestamp),
                heal_action: "refetch_snapshot".to_owned(),
                heal_status: "detected".to_owned(),
            });
        }

        if let Some(unit) = message.orderbook_units.first() {
            self.derived_book_tickers += 1;
            self.last_best_quotes.insert(
                message.code.clone(),
                UpbitBestQuote {
                    symbol: message.code,
                    best_bid: unit.bid_price,
                    best_bid_size: unit.bid_size,
                    best_ask: unit.ask_price,
                    best_ask_size: unit.ask_size,
                    exchange_timestamp_ms: message.timestamp,
                    stream_type: message.stream_type.unwrap_or_else(|| "UNKNOWN".to_owned()),
                },
            );
        }
    }

    fn record_gap_alert(&mut self, alert: UpbitGapAlert) {
        self.gap_alert_count += 1;
        self.gap_alerts.push(alert.clone());
        if self.gap_alerts.len() > MAX_STORED_GAP_ALERTS {
            self.gap_alerts.remove(0);
        }
        self.recent_gap_alerts.push(alert);
        if self.recent_gap_alerts.len() > MAX_RECENT_GAP_ALERTS {
            self.recent_gap_alerts.remove(0);
        }
    }

    pub fn update_health(&mut self) {
        self.source_health_status = if self.received_messages == 0 {
            "waiting_for_messages".to_owned()
        } else if self.malformed_messages > 0 || self.gap_alert_count > 0 {
            "degraded".to_owned()
        } else {
            "connected".to_owned()
        };
        self.source_health_events += 1;
    }

    pub fn heartbeat_delay_ms_at(&self, observed_at_ms: i64) -> i64 {
        self.last_ingest_timestamp_ms
            .map(|last| observed_at_ms.saturating_sub(last).max(0))
            .unwrap_or(0)
    }

    fn record_timing(&mut self, exchange_timestamp_ms: i64, ingest_timestamp_ms: i64) {
        self.last_exchange_timestamp_ms = Some(exchange_timestamp_ms);
        self.last_ingest_timestamp_ms = Some(ingest_timestamp_ms);
        self.latest_stream_lag_ms = ingest_timestamp_ms
            .saturating_sub(exchange_timestamp_ms)
            .max(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upbit::events::{UpbitOrderbookUnit, UpbitTickerMessage};

    #[test]
    fn records_trade_ordering_violation_as_gap_alert() {
        let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
        stats.received_messages = 2;
        stats.record_event(trade("KRW-BTC", 100, 20), 120);
        stats.record_event(trade("KRW-BTC", 90, 19), 130);
        stats.update_health();

        assert_eq!(stats.trade_messages, 2);
        assert_eq!(stats.sequence_anomalies, 1);
        assert_eq!(stats.gap_alert_count, 1);
        assert_eq!(stats.source_health_status, "degraded");
        assert_eq!(
            stats.recent_gap_alerts[0].heal_action,
            "mark_incomplete_and_continue"
        );
    }

    #[test]
    fn records_orderbook_best_quote_and_ticker_counts() {
        let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
        stats.received_messages = 2;
        stats.record_event(
            UpbitParsedEvent::Ticker(UpbitTickerMessage {
                event_type: "ticker".to_owned(),
                code: "KRW-BTC".to_owned(),
                timestamp: 100,
                trade_timestamp: Some(90),
                trade_price: Some(100.0),
                acc_trade_price_24h: Some(1_000.0),
                acc_trade_volume_24h: Some(10.0),
                stream_type: Some("REALTIME".to_owned()),
            }),
            110,
        );
        stats.record_event(
            UpbitParsedEvent::Orderbook(UpbitOrderbookMessage {
                event_type: "orderbook".to_owned(),
                code: "KRW-BTC".to_owned(),
                timestamp: 120,
                total_ask_size: 1.0,
                total_bid_size: 1.0,
                orderbook_units: vec![UpbitOrderbookUnit {
                    ask_price: 101.0,
                    bid_price: 100.0,
                    ask_size: 1.5,
                    bid_size: 2.5,
                }],
                stream_type: Some("SNAPSHOT".to_owned()),
                level: Some(5.0),
            }),
            130,
        );

        assert_eq!(stats.ticker_messages, 1);
        assert_eq!(stats.orderbook_messages, 1);
        assert_eq!(stats.derived_book_tickers, 1);
        assert_eq!(stats.kind_counts["ticker"], 1);
        assert_eq!(stats.symbol_counts["KRW-BTC"], 2);
        assert_eq!(stats.last_best_quotes["KRW-BTC"].best_bid, 100.0);
        assert_eq!(stats.heartbeat_delay_ms_at(150), 20);
    }

    #[test]
    fn status_error_and_unknown_update_health_inputs() {
        let mut stats = UpbitIngestWatchStats::new("wss://example.test".to_owned(), 3);
        stats.record_event(UpbitParsedEvent::Status("UP".to_owned()), 100);
        stats.record_event(
            UpbitParsedEvent::Error {
                name: "BAD".to_owned(),
                message: "bad".to_owned(),
            },
            101,
        );
        stats.record_event(UpbitParsedEvent::Unknown(serde_json::json!({"x":1})), 102);
        stats.update_health();

        assert_eq!(stats.status_messages, 1);
        assert_eq!(stats.malformed_messages, 1);
        assert_eq!(stats.gap_alert_count, 1);
        assert_eq!(stats.source_health_status, "waiting_for_messages");
    }

    fn trade(code: &str, trade_timestamp: i64, sequential_id: i64) -> UpbitParsedEvent {
        UpbitParsedEvent::Trade(UpbitTradeMessage {
            event_type: "trade".to_owned(),
            code: code.to_owned(),
            timestamp: trade_timestamp,
            trade_timestamp,
            trade_price: 100.0,
            trade_volume: 0.1,
            ask_bid: "BID".to_owned(),
            sequential_id,
            best_ask_price: None,
            best_ask_size: None,
            best_bid_price: None,
            best_bid_size: None,
            stream_type: Some("REALTIME".to_owned()),
        })
    }
}
