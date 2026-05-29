use super::stats::BinanceL0WatchStats;
use crate::log_stream;

pub(super) fn print_binance_ingest_log(stats: &BinanceL0WatchStats) {
    let _ = log_stream::debug(
        "market_ingest_progress",
        serde_json::json!({
            "venue": "binance",
            "source_role": "reference",
            "health": stats.source_health_status,
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "trade_messages": stats.trade_messages,
            "book_ticker_messages": stats.book_ticker_messages,
            "ticker_messages": stats.ticker_messages,
            "depth_delta_messages": stats.depth_delta_messages,
            "depth_snapshot_messages": stats.depth_snapshot_messages,
            "gap_alert_count": stats.gap_alert_count,
            "malformed_messages": stats.malformed_messages,
            "close_messages": stats.close_messages
        }),
    );
}
