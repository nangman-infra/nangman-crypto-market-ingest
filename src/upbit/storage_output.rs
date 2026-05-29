use super::UpbitIngestError;
use super::stats::{UpbitGapAlert, UpbitIngestWatchStats};
use crate::clock;
use crate::storage::L0StorageSink;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::symbol_health::SymbolHealthDraft;

pub(super) async fn finalize_storage(
    sink: &mut L0StorageSink,
    stats: &UpbitIngestWatchStats,
) -> Result<(), UpbitIngestError> {
    sink.append_source_health(source_health_draft(stats))
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    for draft in symbol_health_drafts(stats) {
        sink.append_symbol_health(draft)
            .await
            .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    }
    for alert in &stats.gap_alerts {
        sink.append_gap_alert(gap_alert_draft(alert))
            .await
            .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    }
    sink.flush_all()
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))?;
    sink.upload_manifest()
        .await
        .map_err(|error| UpbitIngestError::Storage(error.to_string()))
}

fn source_health_draft(stats: &UpbitIngestWatchStats) -> SourceHealthDraft {
    let observed_at_ms = clock::now_ms();
    SourceHealthDraft {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
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
            "orderbook_messages": stats.orderbook_messages,
            "derived_book_tickers": stats.derived_book_tickers,
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
    stats: &UpbitIngestWatchStats,
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

fn symbol_health_drafts(stats: &UpbitIngestWatchStats) -> Vec<SymbolHealthDraft> {
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
                venue: "upbit".to_owned(),
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

fn gap_alert_draft(alert: &UpbitGapAlert) -> GapAlertDraft {
    GapAlertDraft {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        symbol_native: alert.symbol.clone(),
        gap_type: alert.gap_type.clone(),
        detected_at_ms: alert.detected_at_ms,
        expected_sequence_id: alert.expected_sequence_id,
        observed_sequence_id: alert.observed_sequence_id,
        heal_action: alert.heal_action.clone(),
        heal_status: alert.heal_status.clone(),
        payload_json: serde_json::to_string(alert).unwrap_or_else(|_| "{}".to_owned()),
    }
}

fn health_level(stats: &UpbitIngestWatchStats) -> String {
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
