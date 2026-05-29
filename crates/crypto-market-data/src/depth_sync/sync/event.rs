use super::super::book::apply_depth_delta;
use super::super::types::{BinanceGapAlert, BinanceLocalOrderBook};
use super::alerts::{record_delta_parse_gap, record_sequence_gap};
use crate::messages::BinanceDiffDepthMessage;
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::TimestampMs;
use std::collections::HashSet;

pub(super) fn handle_synced_depth_event(
    book: &mut BinanceLocalOrderBook,
    event: BinanceDiffDepthMessage,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) {
    let Some(last_update_id) = book.last_update_id else {
        return;
    };
    if event.final_update_id <= last_update_id {
        return;
    }
    if event.first_update_id > last_update_id + 1 {
        record_sequence_gap(
            stats,
            raw_symbol,
            received_time_ms,
            last_update_id + 1,
            event.first_update_id,
        );
        book.reset_for_resync(event);
        snapshot_attempted.remove(raw_symbol);
        return;
    }
    if let Err(alert) = apply_depth_delta(book, &event) {
        record_delta_parse_gap(stats, raw_symbol, received_time_ms, *alert);
        book.reset_after_overflow();
        snapshot_attempted.remove(raw_symbol);
    }
}

pub(super) fn buffer_unsynced_depth_event(
    book: &mut BinanceLocalOrderBook,
    event: BinanceDiffDepthMessage,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) {
    if book.buffered_at_capacity() {
        let dropped_count = book.buffered_events.len();
        book.reset_after_overflow();
        snapshot_attempted.remove(raw_symbol);
        stats.buffer_overflow_count += 1;
        stats.record_gap_alert(BinanceGapAlert {
            gap_type: "buffered_overflow".to_owned(),
            symbol: raw_symbol.to_owned(),
            detected_at_ms: received_time_ms,
            expected_sequence_id: None,
            observed_sequence_id: Some(event.first_update_id),
            heal_action: "refetch_snapshot".to_owned(),
            heal_status: format!("dropped_count={dropped_count}"),
        });
    }
    book.buffered_events.push(event);
}
