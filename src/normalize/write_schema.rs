use arrow_schema::{DataType, Field, Fields, Schema};
use std::sync::Arc;

pub(super) fn slice_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("schema_version", DataType::Utf8),
        field("slice_id", DataType::Utf8),
        field("venue", DataType::Utf8),
        field("source_role", DataType::Utf8),
        field("symbol_native", DataType::Utf8),
        field("symbol_canonical", DataType::Utf8),
        field("base_asset", DataType::Utf8),
        field("quote_asset", DataType::Utf8),
        field("market_type", DataType::Utf8),
        field("window_ms", DataType::Int64),
        field("window_start_ms", DataType::Int64),
        field("window_end_ms", DataType::Int64),
        field("slice_completeness", DataType::Utf8),
        field("missing_reasons", string_list_type()),
        field("quality_ok", DataType::Int64),
        field("quality_delayed", DataType::Int64),
        field("quality_stale", DataType::Int64),
        field("quality_gap", DataType::Int64),
        field("quality_invalid", DataType::Int64),
        field("trade_count", DataType::Int64),
        field("trade_volume", DataType::Float64),
        nullable_field("last_trade_price", DataType::Float64),
        nullable_field("last_trade_size", DataType::Float64),
        nullable_field("best_bid", DataType::Float64),
        nullable_field("best_ask", DataType::Float64),
        nullable_field("mid_price", DataType::Float64),
        nullable_field("spread_bps", DataType::Float64),
        field("book_ticker_count", DataType::Int64),
        field("depth_event_count", DataType::Int64),
        field("depth_book_rebuilt", DataType::Boolean),
        field("trade_events", list_struct_type(trade_fields())),
        field("book_ticker_events", list_struct_type(book_fields())),
        field("depth_events", list_struct_type(compact_fields())),
        field("ticker_events", list_struct_type(compact_fields())),
        nullable_field(
            "symbol_health_snapshot",
            DataType::Struct(symbol_health_fields()),
        ),
        nullable_field(
            "source_health_snapshot",
            DataType::Struct(source_health_fields()),
        ),
        field("parent_event_ids", string_list_type()),
        field("parent_run_ids", string_list_type()),
    ]))
}

pub(super) fn trade_fields() -> Fields {
    Fields::from(vec![
        field("exchange_timestamp_ms", DataType::Int64),
        field("ingest_timestamp_ms", DataType::Int64),
        field("price", DataType::Float64),
        field("quantity", DataType::Float64),
        field("side", DataType::Utf8),
        nullable_field("exchange_sequence", DataType::Int64),
        field("parent_event_id", DataType::Utf8),
    ])
}

pub(super) fn book_fields() -> Fields {
    Fields::from(vec![
        field("exchange_timestamp_ms", DataType::Int64),
        field("ingest_timestamp_ms", DataType::Int64),
        field("best_bid", DataType::Float64),
        field("best_bid_qty", DataType::Float64),
        field("best_ask", DataType::Float64),
        field("best_ask_qty", DataType::Float64),
        nullable_field("exchange_sequence", DataType::Int64),
        field("parent_event_id", DataType::Utf8),
    ])
}

pub(super) fn compact_fields() -> Fields {
    Fields::from(vec![
        field("exchange_timestamp_ms", DataType::Int64),
        field("ingest_timestamp_ms", DataType::Int64),
        field("event_type", DataType::Utf8),
        field("parent_event_id", DataType::Utf8),
    ])
}

pub(super) fn symbol_health_fields() -> Fields {
    Fields::from(vec![
        field("observed_at_ms", DataType::Int64),
        field("last_event_time_ms", DataType::Int64),
        field("last_received_time_ms", DataType::Int64),
        field("latency_ms", DataType::Int64),
        field("is_tradeable", DataType::Boolean),
        field("reason_codes", string_list_type()),
    ])
}

pub(super) fn source_health_fields() -> Fields {
    Fields::from(vec![
        field("observed_at_ms", DataType::Int64),
        field("connection_status", DataType::Utf8),
        field("health_level", DataType::Utf8),
        field("heartbeat_delay_ms", DataType::Int64),
        field("stream_lag_ms", DataType::Int64),
        field("recent_gap_count", DataType::Int64),
        field("book_rebuild_count", DataType::Int64),
    ])
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn nullable_field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
}

fn string_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

fn list_struct_type(fields: Fields) -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true)))
}
