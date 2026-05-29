use super::super::types::BinanceGapAlert;
use crate::error::MarketDataError;
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::{Sequence, TimestampMs};

pub(super) fn record_sequence_gap(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    expected_sequence_id: Sequence,
    observed_sequence_id: Sequence,
) {
    stats.record_gap_alert(BinanceGapAlert {
        gap_type: "sequence_gap".to_owned(),
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        expected_sequence_id: Some(expected_sequence_id),
        observed_sequence_id: Some(observed_sequence_id),
        heal_action: "refetch_snapshot".to_owned(),
        heal_status: "resync_requested".to_owned(),
    });
}

pub(super) fn record_delta_parse_gap(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    alert: BinanceGapAlert,
) {
    stats.record_gap_alert(BinanceGapAlert {
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        ..alert
    });
}

pub(super) fn record_snapshot_fetch_failure(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    error: MarketDataError,
) {
    stats.depth_snapshot_failures += 1;
    stats.record_gap_alert(BinanceGapAlert {
        gap_type: "snapshot_fetch_failed".to_owned(),
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "retry_snapshot".to_owned(),
        heal_status: format!("failed: {error}"),
    });
}
