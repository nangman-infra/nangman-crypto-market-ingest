use super::stats::UpbitIngestWatchStats;
use crate::log_stream;

pub(super) fn print_upbit_ingest_log(stats: &UpbitIngestWatchStats) {
    let _ = log_stream::debug(
        "market_ingest_progress",
        serde_json::json!({
            "venue": "upbit",
            "source_role": "execution",
            "health": stats.source_health_status,
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "ticker_messages": stats.ticker_messages,
            "trade_messages": stats.trade_messages,
            "orderbook_messages": stats.orderbook_messages,
            "derived_book_tickers": stats.derived_book_tickers,
            "gap_alert_count": stats.gap_alert_count,
            "malformed_messages": stats.malformed_messages,
            "close_messages": stats.close_messages
        }),
    );
}
