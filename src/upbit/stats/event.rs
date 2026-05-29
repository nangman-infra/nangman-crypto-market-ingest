use super::model::{UpbitBestQuote, UpbitGapAlert, UpbitIngestWatchStats};
use super::{MAX_RECENT_GAP_ALERTS, MAX_STORED_GAP_ALERTS};
use crate::upbit::events::{UpbitOrderbookMessage, UpbitParsedEvent, UpbitTradeMessage};

impl UpbitIngestWatchStats {
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

    pub(in crate::upbit) fn record_gap_alert(&mut self, alert: UpbitGapAlert) {
        self.gap_alert_count += 1;
        self.gap_alerts.push_back(alert.clone());
        if self.gap_alerts.len() > MAX_STORED_GAP_ALERTS {
            self.gap_alerts.pop_front();
        }
        self.recent_gap_alerts.push_back(alert);
        if self.recent_gap_alerts.len() > MAX_RECENT_GAP_ALERTS {
            self.recent_gap_alerts.pop_front();
        }
    }
}
