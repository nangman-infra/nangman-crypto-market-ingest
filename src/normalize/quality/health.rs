use super::super::model::{
    SliceRow, SourceHealthInput, SourceHealthSnapshot, SymbolHealthInput, SymbolHealthSnapshot,
};

pub(super) fn latest_symbol_health(
    rows: &[SymbolHealthInput],
    slice: &SliceRow,
) -> Option<SymbolHealthSnapshot> {
    rows.iter()
        .filter(|row| {
            row.venue == slice.venue
                && row.symbol_native == slice.symbol_native
                && row.observed_at_ms <= slice.window_end_ms
        })
        .max_by_key(|row| row.observed_at_ms)
        .map(|row| SymbolHealthSnapshot {
            observed_at_ms: row.observed_at_ms,
            last_event_time_ms: row.last_event_time_ms,
            last_received_time_ms: row.last_event_time_ms.saturating_add(row.latency_ms),
            latency_ms: row.latency_ms,
            is_tradeable: row.is_tradeable,
            reason_codes: row
                .reason_codes
                .split(';')
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
        })
}

pub(super) fn latest_source_health(
    rows: &[SourceHealthInput],
    slice: &SliceRow,
) -> Option<SourceHealthSnapshot> {
    rows.iter()
        .filter(|row| row.venue == slice.venue && row.observed_at_ms <= slice.window_end_ms)
        .max_by_key(|row| row.observed_at_ms)
        .map(|row| SourceHealthSnapshot {
            observed_at_ms: row.observed_at_ms,
            connection_status: row.connection_status.clone(),
            health_level: row.health_level.clone(),
            heartbeat_delay_ms: row.heartbeat_delay_ms,
            stream_lag_ms: row.stream_lag_ms,
            recent_gap_count: row.recent_gap_count,
            book_rebuild_count: row.book_rebuild_count,
        })
}

pub(super) fn is_healthy_level(value: &str) -> bool {
    matches!(value, "ok" | "healthy" | "nominal")
}
