use super::events::{BinanceParsedEnvelope, BinanceParsedEvent};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};

const MAX_STORED_GAP_ALERTS: usize = 1_000;
const MAX_RECENT_GAP_ALERTS: usize = 20;

#[derive(Debug, Clone, Serialize)]
pub struct BinanceL0WatchStats {
    pub stream_url: String,
    pub planned_stream_count: usize,
    pub received_messages: u64,
    pub parsed_messages: u64,
    pub malformed_messages: u64,
    pub control_messages: u64,
    pub pings_received: u64,
    pub pongs_received: u64,
    pub close_messages: u64,
    pub kind_counts: BTreeMap<String, u64>,
    pub symbol_counts: BTreeMap<String, u64>,
    pub ticker_messages: u64,
    pub trade_messages: u64,
    pub book_ticker_messages: u64,
    pub depth_delta_messages: u64,
    pub depth_snapshot_messages: u64,
    pub sequence_anomalies: u64,
    pub source_health_status: String,
    pub source_health_events: u64,
    pub reconnect_count: u64,
    pub last_reconnect_at_ms: Option<i64>,
    pub gap_alert_count: u64,
    pub recent_gap_alerts: VecDeque<BinanceL0GapAlert>,
    #[serde(skip)]
    pub gap_alerts: VecDeque<BinanceL0GapAlert>,
    pub last_exchange_timestamp_ms: Option<i64>,
    pub last_ingest_timestamp_ms: Option<i64>,
    pub latest_stream_lag_ms: i64,
    pub symbol_last_event_time_ms: BTreeMap<String, i64>,
    pub symbol_last_ingest_time_ms: BTreeMap<String, i64>,
    #[serde(skip)]
    last_sequence_by_stream: BTreeMap<String, i64>,
    #[serde(skip)]
    last_depth_final_update_by_symbol: BTreeMap<String, i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceL0GapAlert {
    pub gap_type: String,
    pub symbol: String,
    pub detected_at_ms: i64,
    pub expected_sequence_id: Option<i64>,
    pub observed_sequence_id: Option<i64>,
    pub heal_action: String,
    pub heal_status: String,
}

impl BinanceL0WatchStats {
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
            kind_counts: BTreeMap::new(),
            symbol_counts: BTreeMap::new(),
            ticker_messages: 0,
            trade_messages: 0,
            book_ticker_messages: 0,
            depth_delta_messages: 0,
            depth_snapshot_messages: 0,
            sequence_anomalies: 0,
            source_health_status: "connected".to_owned(),
            source_health_events: 1,
            reconnect_count: 0,
            last_reconnect_at_ms: None,
            gap_alert_count: 0,
            recent_gap_alerts: VecDeque::new(),
            gap_alerts: VecDeque::new(),
            last_exchange_timestamp_ms: None,
            last_ingest_timestamp_ms: None,
            latest_stream_lag_ms: 0,
            symbol_last_event_time_ms: BTreeMap::new(),
            symbol_last_ingest_time_ms: BTreeMap::new(),
            last_sequence_by_stream: BTreeMap::new(),
            last_depth_final_update_by_symbol: BTreeMap::new(),
        }
    }

    pub fn record_event(&mut self, envelope: BinanceParsedEnvelope, detected_at_ms: i64) {
        self.parsed_messages += 1;
        let symbol = envelope.event.symbol().to_ascii_uppercase();
        let exchange_timestamp_ms = envelope.event.exchange_timestamp_ms(detected_at_ms);
        self.record_timing(exchange_timestamp_ms, detected_at_ms);
        self.symbol_last_event_time_ms
            .insert(symbol.clone(), exchange_timestamp_ms);
        self.symbol_last_ingest_time_ms
            .insert(symbol.clone(), detected_at_ms);

        *self
            .kind_counts
            .entry(envelope.event.kind_name().to_owned())
            .or_insert(0) += 1;
        *self.symbol_counts.entry(symbol.clone()).or_insert(0) += 1;

        match &envelope.event {
            BinanceParsedEvent::Trade(_) => {
                self.trade_messages += 1;
                self.record_trade_ordering_gap(
                    &symbol,
                    &envelope.stream,
                    envelope.event.numeric_sequence(),
                    detected_at_ms,
                );
            }
            BinanceParsedEvent::Ticker(_) => self.ticker_messages += 1,
            BinanceParsedEvent::BookTicker(_) => self.book_ticker_messages += 1,
            BinanceParsedEvent::DiffDepth(message) => {
                self.depth_delta_messages += 1;
                self.record_depth_update_id_gap(
                    &symbol,
                    message.first_update_id,
                    message.final_update_id,
                    detected_at_ms,
                );
            }
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

    fn record_trade_ordering_gap(
        &mut self,
        symbol: &str,
        stream: &str,
        sequence: i64,
        detected_at_ms: i64,
    ) {
        let previous_sequence = self
            .last_sequence_by_stream
            .insert(stream.to_owned(), sequence);
        if let Some(previous) = previous_sequence
            && sequence < previous
        {
            self.sequence_anomalies += 1;
            self.record_gap_alert(BinanceL0GapAlert {
                gap_type: "ordering_violation".to_owned(),
                symbol: symbol.to_owned(),
                detected_at_ms,
                expected_sequence_id: Some(previous.saturating_add(1)),
                observed_sequence_id: Some(sequence),
                heal_action: "mark_incomplete_and_continue".to_owned(),
                heal_status: "detected".to_owned(),
            });
        }
    }

    fn record_timing(&mut self, exchange_timestamp_ms: i64, ingest_timestamp_ms: i64) {
        self.last_exchange_timestamp_ms = Some(exchange_timestamp_ms);
        self.last_ingest_timestamp_ms = Some(ingest_timestamp_ms);
        self.latest_stream_lag_ms = ingest_timestamp_ms
            .saturating_sub(exchange_timestamp_ms)
            .max(0);
    }

    fn record_depth_update_id_gap(
        &mut self,
        symbol: &str,
        first_update_id: i64,
        final_update_id: i64,
        detected_at_ms: i64,
    ) {
        if let Some(previous_final_update_id) = self
            .last_depth_final_update_by_symbol
            .insert(symbol.to_owned(), final_update_id)
        {
            let expected = previous_final_update_id.saturating_add(1);
            if first_update_id != expected {
                self.sequence_anomalies += 1;
                self.record_gap_alert(BinanceL0GapAlert {
                    gap_type: "depth_update_id_gap".to_owned(),
                    symbol: symbol.to_owned(),
                    detected_at_ms,
                    expected_sequence_id: Some(expected),
                    observed_sequence_id: Some(first_update_id),
                    heal_action: "refetch_snapshot".to_owned(),
                    heal_status: "detected".to_owned(),
                });
            }
        }
    }

    pub(super) fn record_gap_alert(&mut self, alert: BinanceL0GapAlert) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binance::events::{
        BinanceDiffDepthMessage, BinanceParsedEnvelope, BinanceParsedEvent,
    };
    use crate::clock;

    #[test]
    fn records_depth_update_id_gap_alert() {
        let mut stats = BinanceL0WatchStats::new("wss://example.test".to_owned(), 1);
        stats.record_event(depth_envelope(41, 42), clock::now_ms());
        stats.record_event(depth_envelope(44, 45), clock::now_ms());

        assert_eq!(stats.gap_alert_count, 1);
        assert_eq!(stats.gap_alerts[0].gap_type, "depth_update_id_gap");
        assert_eq!(stats.gap_alerts[0].expected_sequence_id, Some(43));
        assert_eq!(stats.gap_alerts[0].observed_sequence_id, Some(44));
    }

    fn depth_envelope(first: i64, final_id: i64) -> BinanceParsedEnvelope {
        BinanceParsedEnvelope {
            stream: "btcusdt@depth@100ms".to_owned(),
            event: BinanceParsedEvent::DiffDepth(BinanceDiffDepthMessage {
                event_time_ms: 1,
                symbol: "BTCUSDT".to_owned(),
                first_update_id: first,
                final_update_id: final_id,
            }),
            payload_json: "{}".to_owned(),
        }
    }
}
