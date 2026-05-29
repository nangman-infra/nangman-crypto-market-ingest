use super::columns::{bool_value, int64_optional, int64_value, string_value};
use crate::normalize::model::{GapAlertInput, RawInputEvent, SourceHealthInput, SymbolHealthInput};
use crate::storage::StorageError;
use arrow_array::RecordBatch;

pub(super) fn raw_event_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<RawInputEvent, StorageError> {
    Ok(RawInputEvent {
        event_id: string_value(batch, "event_id", row)?,
        producer_run_id: string_value(batch, "producer_run_id", row)?,
        venue: string_value(batch, "venue", row)?,
        source_role: string_value(batch, "source_role", row)?,
        market_type: string_value(batch, "market_type", row)?,
        event_type: string_value(batch, "event_type", row)?,
        symbol_native: string_value(batch, "symbol_native", row)?,
        symbol_canonical: string_value(batch, "symbol_canonical", row)?,
        base_asset: string_value(batch, "base_asset", row)?,
        quote_asset: string_value(batch, "quote_asset", row)?,
        exchange_timestamp_ms: int64_value(batch, "exchange_timestamp_ms", row)?,
        ingest_timestamp_ms: int64_value(batch, "ingest_timestamp_ms", row)?,
        exchange_sequence: int64_optional(batch, "exchange_sequence", row)?,
        payload_json: string_value(batch, "payload_json", row)?,
        payload_sha256: string_value(batch, "payload_sha256", row)?,
        schema_version: string_value(batch, "schema_version", row)?,
    })
}

pub(super) fn symbol_health_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<SymbolHealthInput, StorageError> {
    Ok(SymbolHealthInput {
        venue: string_value(batch, "venue", row)?,
        symbol_native: string_value(batch, "symbol_native", row)?,
        observed_at_ms: int64_value(batch, "observed_at_ms", row)?,
        last_event_time_ms: int64_value(batch, "last_event_time_ms", row)?,
        latency_ms: int64_value(batch, "latency_ms", row)?,
        is_tradeable: bool_value(batch, "is_tradeable", row)?,
        reason_codes: string_value(batch, "reason_codes", row)?,
        payload_sha256: string_value(batch, "payload_sha256", row)?,
        schema_version: string_value(batch, "schema_version", row)?,
    })
}

pub(super) fn source_health_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<SourceHealthInput, StorageError> {
    Ok(SourceHealthInput {
        venue: string_value(batch, "venue", row)?,
        observed_at_ms: int64_value(batch, "observed_at_ms", row)?,
        connection_status: string_value(batch, "connection_status", row)?,
        heartbeat_delay_ms: int64_value(batch, "heartbeat_delay_ms", row)?,
        stream_lag_ms: int64_value(batch, "stream_lag_ms", row)?,
        recent_gap_count: int64_value(batch, "recent_gap_count", row)?,
        book_rebuild_count: int64_value(batch, "book_rebuild_count", row)?,
        health_level: string_value(batch, "health_level", row)?,
        payload_json: string_value(batch, "payload_json", row)?,
        payload_sha256: string_value(batch, "payload_sha256", row)?,
        schema_version: string_value(batch, "schema_version", row)?,
    })
}

pub(super) fn gap_alert_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<GapAlertInput, StorageError> {
    Ok(GapAlertInput {
        venue: string_value(batch, "venue", row)?,
        symbol_native: string_value(batch, "symbol_native", row)?,
        gap_type: string_value(batch, "gap_type", row)?,
        detected_at_ms: int64_value(batch, "detected_at_ms", row)?,
        payload_json: string_value(batch, "payload_json", row)?,
        payload_sha256: string_value(batch, "payload_sha256", row)?,
        schema_version: string_value(batch, "schema_version", row)?,
    })
}
