use super::model::BinanceL0WatchStats;

impl BinanceL0WatchStats {
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

    pub(super) fn record_timing(&mut self, exchange_timestamp_ms: i64, ingest_timestamp_ms: i64) {
        self.last_exchange_timestamp_ms = Some(exchange_timestamp_ms);
        self.last_ingest_timestamp_ms = Some(ingest_timestamp_ms);
        self.latest_stream_lag_ms = ingest_timestamp_ms
            .saturating_sub(exchange_timestamp_ms)
            .max(0);
    }
}
