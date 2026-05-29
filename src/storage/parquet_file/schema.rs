use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub(super) fn raw_market_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("event_id", DataType::Utf8),
        field("producer_run_id", DataType::Utf8),
        field("venue", DataType::Utf8),
        field("source_role", DataType::Utf8),
        field("market_type", DataType::Utf8),
        field("event_type", DataType::Utf8),
        field("symbol_native", DataType::Utf8),
        field("symbol_canonical", DataType::Utf8),
        field("base_asset", DataType::Utf8),
        field("quote_asset", DataType::Utf8),
        field("exchange_timestamp_ms", DataType::Int64),
        field("ingest_timestamp_ms", DataType::Int64),
        field("sequence_id", DataType::Utf8),
        field("sequence_tag", DataType::Utf8),
        nullable_field("exchange_sequence", DataType::Int64),
        nullable_field("diff_first_update_id", DataType::Int64),
        nullable_field("diff_final_update_id", DataType::Int64),
        field("is_snapshot", DataType::Boolean),
        field("stream_type", DataType::Utf8),
        field("stream_phase", DataType::Utf8),
        field("payload_json", DataType::Utf8),
        field("payload_sha256", DataType::Utf8),
        field("schema_version", DataType::Utf8),
    ]))
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn nullable_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}
