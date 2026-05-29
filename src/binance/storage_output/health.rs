use super::super::stats::BinanceL0WatchStats;
use crate::clock;
use crate::storage::health::SourceHealthDraft;
use crate::storage::symbol_health::SymbolHealthDraft;

pub(super) fn source_health_draft(stats: &BinanceL0WatchStats) -> SourceHealthDraft {
    let observed_at_ms = clock::now_ms();
    SourceHealthDraft {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        observed_at_ms,
        connection_status: stats.source_health_status.clone(),
        heartbeat_delay_ms: stats.heartbeat_delay_ms_at(observed_at_ms),
        stream_lag_ms: stats.latest_stream_lag_ms,
        recent_gap_count: stats.gap_alert_count,
        book_rebuild_count: 0,
        health_level: health_level(stats),
        payload_json: serde_json::json!({
            "received_messages": stats.received_messages,
            "parsed_messages": stats.parsed_messages,
            "malformed_messages": stats.malformed_messages,
            "symbols_seen": stats.symbol_counts.len(),
            "ticker_messages": stats.ticker_messages,
            "trade_messages": stats.trade_messages,
            "book_ticker_messages": stats.book_ticker_messages,
            "depth_delta_messages": stats.depth_delta_messages,
            "depth_snapshot_messages": stats.depth_snapshot_messages,
            "gap_alert_count": stats.gap_alert_count,
            "last_exchange_timestamp_ms": stats.last_exchange_timestamp_ms,
            "last_ingest_timestamp_ms": stats.last_ingest_timestamp_ms,
            "stream_lag_ms": stats.latest_stream_lag_ms,
            "symbol_health": symbol_health_payload(stats, observed_at_ms)
        })
        .to_string(),
    }
}

fn symbol_health_payload(
    stats: &BinanceL0WatchStats,
    observed_at_ms: i64,
) -> Vec<serde_json::Value> {
    stats
        .symbol_counts
        .keys()
        .map(|symbol| {
            let last_event_time_ms = stats.symbol_last_event_time_ms.get(symbol).copied();
            let last_received_time_ms = stats.symbol_last_ingest_time_ms.get(symbol).copied();
            let latency_ms = last_event_time_ms
                .zip(last_received_time_ms)
                .map(|(event, received)| received.saturating_sub(event).max(0));
            let stale_ms = last_received_time_ms
                .map(|received| observed_at_ms.saturating_sub(received).max(0))
                .unwrap_or(0);
            serde_json::json!({
                "symbol_native": symbol,
                "last_event_time_ms": last_event_time_ms,
                "last_received_time_ms": last_received_time_ms,
                "latency_ms": latency_ms,
                "stale_ms": stale_ms,
                "is_tradeable": stale_ms < 60_000 && stats.gap_alert_count == 0,
                "reason_codes": if stale_ms >= 60_000 {
                    vec!["source_stale"]
                } else {
                    Vec::<&str>::new()
                }
            })
        })
        .collect()
}

pub(super) fn symbol_health_drafts(stats: &BinanceL0WatchStats) -> Vec<SymbolHealthDraft> {
    let observed_at_ms = clock::now_ms();
    stats
        .symbol_counts
        .keys()
        .map(|symbol| {
            let last_event_time_ms = stats
                .symbol_last_event_time_ms
                .get(symbol)
                .copied()
                .unwrap_or(observed_at_ms);
            let last_received_time_ms = stats
                .symbol_last_ingest_time_ms
                .get(symbol)
                .copied()
                .unwrap_or(observed_at_ms);
            let stale_ms = observed_at_ms.saturating_sub(last_received_time_ms).max(0);
            let mut reason_codes = Vec::new();
            if stale_ms >= 60_000 {
                reason_codes.push("source_stale".to_owned());
            }
            if stats.gap_alert_count > 0 {
                reason_codes.push("source_gap_detected".to_owned());
            }
            SymbolHealthDraft {
                venue: "binance".to_owned(),
                symbol_native: symbol.clone(),
                observed_at_ms,
                last_event_time_ms,
                latency_ms: last_received_time_ms
                    .saturating_sub(last_event_time_ms)
                    .max(0),
                is_tradeable: reason_codes.is_empty(),
                reason_codes,
            }
        })
        .collect()
}

fn health_level(stats: &BinanceL0WatchStats) -> String {
    if stats.source_health_status == "connected"
        && stats.malformed_messages == 0
        && stats.gap_alert_count == 0
    {
        "healthy".to_owned()
    } else if stats.received_messages == 0 {
        "critical".to_owned()
    } else {
        "degraded".to_owned()
    }
}
