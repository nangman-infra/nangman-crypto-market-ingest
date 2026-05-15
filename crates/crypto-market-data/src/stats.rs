use crate::depth_sync::{BinanceGapAlert, BinanceLocalOrderBook};
use crate::stream_config::BinanceStreamKind;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct MarketStreamStats {
    pub received_messages: u64,
    pub sent_snapshots: u64,
    pub overflowed_snapshots: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceIngestWatchStats {
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
    pub normalized_market_snapshots: u64,
    pub normalized_depth_snapshots: u64,
    pub normalization_errors: u64,
    pub sequence_anomalies: u64,
    pub source_health_status: String,
    pub source_health_events: u64,
    pub depth_delta_messages: u64,
    pub depth_snapshot_requests: u64,
    pub depth_snapshot_successes: u64,
    pub depth_snapshot_failures: u64,
    pub depth_books_synced: usize,
    pub depth_books_buffering: usize,
    pub gap_alert_count: u64,
    pub recent_gap_alerts: Vec<BinanceGapAlert>,
}

impl BinanceIngestWatchStats {
    pub(crate) fn new(stream_url: String, planned_stream_count: usize) -> Self {
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
            normalized_market_snapshots: 0,
            normalized_depth_snapshots: 0,
            normalization_errors: 0,
            sequence_anomalies: 0,
            source_health_status: "connected".to_owned(),
            source_health_events: 1,
            depth_delta_messages: 0,
            depth_snapshot_requests: 0,
            depth_snapshot_successes: 0,
            depth_snapshot_failures: 0,
            depth_books_synced: 0,
            depth_books_buffering: 0,
            gap_alert_count: 0,
            recent_gap_alerts: Vec::new(),
        }
    }

    pub(crate) fn increment_kind(&mut self, kind: BinanceStreamKind) {
        *self.kind_counts.entry(kind.name().to_owned()).or_insert(0) += 1;
    }

    pub(crate) fn increment_symbol(&mut self, raw_symbol: &str) {
        *self
            .symbol_counts
            .entry(raw_symbol.to_ascii_uppercase())
            .or_insert(0) += 1;
    }

    pub(crate) fn update_depth_book_counts(
        &mut self,
        books: &BTreeMap<String, BinanceLocalOrderBook>,
    ) {
        self.depth_books_synced = books.values().filter(|book| book.is_synced()).count();
        self.depth_books_buffering = books.len().saturating_sub(self.depth_books_synced);
    }

    pub(crate) fn record_gap_alert(&mut self, alert: BinanceGapAlert) {
        self.gap_alert_count += 1;
        self.recent_gap_alerts.push(alert);
        if self.recent_gap_alerts.len() > 20 {
            self.recent_gap_alerts.remove(0);
        }
    }
}
