use super::model::{BinanceL0GapAlert, BinanceL0WatchStats};
use super::{MAX_RECENT_GAP_ALERTS, MAX_STORED_GAP_ALERTS};
use crate::binance::events::{BinanceParsedEnvelope, BinanceParsedEvent};

impl BinanceL0WatchStats {
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

    pub(in crate::binance) fn record_gap_alert(&mut self, alert: BinanceL0GapAlert) {
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
