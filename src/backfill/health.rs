use crate::backfill::{BackfillArgs, BackfillError, SymbolBackfillReport};
use crate::clock;
use crate::storage::L0StorageSink;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::symbol_health::SymbolHealthDraft;
use serde_json::json;

pub(crate) async fn append_empty_gap_alert(
    sink: &mut L0StorageSink,
    venue: &str,
    source_role: &str,
    symbol_native: &str,
    input_start_ms: i64,
    input_end_ms: i64,
    reason: &str,
) -> Result<(), BackfillError> {
    sink.append_gap_alert(GapAlertDraft {
        venue: venue.to_owned(),
        source_role: source_role.to_owned(),
        symbol_native: symbol_native.to_owned(),
        gap_type: "historical_range_empty".to_owned(),
        detected_at_ms: clock::now_ms(),
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "review_range_or_source".to_owned(),
        heal_status: "open".to_owned(),
        payload_json: serde_json::to_string(&json!({
            "input_start_ms": input_start_ms,
            "input_end_ms": input_end_ms,
            "reason": reason
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}

pub(crate) async fn append_symbol_health_for(
    sink: &mut L0StorageSink,
    venue: &str,
    symbols: &[SymbolBackfillReport],
    observed_at_ms: i64,
) -> Result<(), BackfillError> {
    for symbol in symbols {
        let last_event_time_ms = symbol.last_event_time_ms.unwrap_or(0);
        sink.append_symbol_health(SymbolHealthDraft {
            venue: venue.to_owned(),
            symbol_native: symbol.symbol_native.clone(),
            observed_at_ms,
            last_event_time_ms,
            latency_ms: observed_at_ms.saturating_sub(last_event_time_ms).max(0),
            is_tradeable: symbol.record_count > 0,
            reason_codes: if symbol.record_count > 0 {
                Vec::new()
            } else {
                vec!["no_historical_trades".to_owned()]
            },
        })
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    }
    Ok(())
}

pub(crate) struct SourceHealthSummary<'a> {
    pub venue: &'a str,
    pub source_role: &'a str,
    pub mode: &'a str,
    pub observed_at_ms: i64,
    pub args: &'a BackfillArgs,
    pub symbol_count: usize,
    pub total_record_count: u64,
    pub total_gap_alert_count: u64,
}

pub(crate) async fn append_source_health_for(
    sink: &mut L0StorageSink,
    summary: SourceHealthSummary<'_>,
) -> Result<(), BackfillError> {
    sink.append_source_health(SourceHealthDraft {
        venue: summary.venue.to_owned(),
        source_role: summary.source_role.to_owned(),
        observed_at_ms: summary.observed_at_ms,
        connection_status: "historical_backfill_completed".to_owned(),
        heartbeat_delay_ms: 0,
        stream_lag_ms: summary
            .observed_at_ms
            .saturating_sub(summary.args.input_end_ms)
            .max(0),
        recent_gap_count: summary.total_gap_alert_count,
        book_rebuild_count: 0,
        health_level: if summary.total_gap_alert_count == 0 {
            "ok"
        } else {
            "warn"
        }
        .to_owned(),
        payload_json: serde_json::to_string(&json!({
            "mode": summary.mode,
            "symbol_count": summary.symbol_count,
            "input_start_ms": summary.args.input_start_ms,
            "input_end_ms": summary.args.input_end_ms,
            "record_count": summary.total_record_count,
            "gap_alert_count": summary.total_gap_alert_count
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}
