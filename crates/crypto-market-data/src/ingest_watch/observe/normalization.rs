use crate::normalize::normalize_binance_stream_message;
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig};
use crypto_domain::{TimestampMs, TraceId};

pub(super) fn record_normalization_outcome(
    config: &BinanceStreamConfig,
    raw_payload: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
    stats: &mut BinanceIngestWatchStats,
) {
    match normalize_binance_stream_message(config, raw_payload, received_time_ms, decision_trace_id)
    {
        Ok(BinanceNormalizedMarketEvent::Market(_)) => {
            stats.normalized_market_snapshots += 1;
        }
        Ok(BinanceNormalizedMarketEvent::Depth(_)) => {
            stats.normalized_depth_snapshots += 1;
        }
        Err(_) => {
            stats.normalization_errors += 1;
        }
    }
}
