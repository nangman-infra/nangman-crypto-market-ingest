use super::model::{CompactEventRef, SliceRow};
use super::write_schema::{
    book_fields, compact_fields, slice_schema, source_health_fields, symbol_health_fields,
    trade_fields,
};
use crate::storage::StorageError;
use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use chrono::{DateTime, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn write_slice_parquet(path: &Path, rows: &[SliceRow]) -> Result<(), StorageError> {
    let row_refs = rows.iter().collect::<Vec<_>>();
    write_slice_parquet_refs(path, &row_refs)
}

pub fn write_slice_parquet_refs(path: &Path, rows: &[&SliceRow]) -> Result<(), StorageError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let schema = slice_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            string_const_col(rows.len(), "normalized_market_slice_v1"),
            string_col(rows, |row| &row.slice_id),
            string_col(rows, |row| &row.venue),
            string_col(rows, |row| &row.source_role),
            string_col(rows, |row| &row.symbol_native),
            string_col(rows, |row| &row.symbol_canonical),
            string_col(rows, |row| &row.base_asset),
            string_col(rows, |row| &row.quote_asset),
            string_col(rows, |row| &row.market_type),
            int64_col(rows, |row| row.window_ms),
            int64_col(rows, |row| row.window_start_ms),
            int64_col(rows, |row| row.window_end_ms),
            string_col(rows, |row| &row.slice_completeness),
            string_list_col(rows, |row| &row.missing_reasons),
            int64_col(rows, |row| row.quality_ok),
            int64_col(rows, |row| row.quality_delayed),
            int64_col(rows, |row| row.quality_stale),
            int64_col(rows, |row| row.quality_gap),
            int64_col(rows, |row| row.quality_invalid),
            int64_col(rows, |row| row.trade_count),
            float64_col(rows, |row| Some(row.trade_volume)),
            float64_col(rows, |row| row.last_trade_price),
            float64_col(rows, |row| row.last_trade_size),
            float64_col(rows, |row| row.best_bid),
            float64_col(rows, |row| row.best_ask),
            float64_col(rows, |row| row.mid_price),
            float64_col(rows, |row| row.spread_bps),
            int64_col(rows, |row| row.book_ticker_count),
            int64_col(rows, |row| row.depth_event_count),
            bool_col(rows, |row| row.depth_book_rebuilt),
            trade_events_col(rows),
            book_ticker_events_col(rows),
            compact_events_col(rows, |row| &row.depth_events),
            compact_events_col(rows, |row| &row.ticker_events),
            symbol_health_col(rows),
            source_health_col(rows),
            string_list_col(rows, |row| &row.parent_event_ids),
            string_list_col(rows, |row| &row.parent_run_ids),
        ],
    )?;
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn slice_object_key(venue: &str, window_start_ms: i64, window_ms: i64, run_id: &str) -> String {
    let part = time_part(window_start_ms);
    format!(
        "normalized_market_slice/venue={venue}/event_date={}/hour={:02}/window_ms={window_ms}/shard=00/run_id={run_id}-part-000001.parquet",
        part.event_date, part.hour
    )
}

pub fn report_object_key(run_id: &str) -> String {
    format!("normalization_report/run_id={run_id}/report.json")
}

pub fn manifest_object_key(run_id: &str) -> String {
    format!("runs/run_id={run_id}/manifest.json")
}

pub fn market_data_quality_summary_object_key(run_id: &str) -> String {
    format!("market_data_quality_summary/run_id={run_id}/summary.json")
}

pub fn market_feature_delta_object_key(run_id: &str) -> String {
    format!("market_feature_delta/run_id={run_id}/delta.json")
}

pub fn market_feature_delta_summary_object_key(run_id: &str) -> String {
    format!("market_feature_delta_summary/run_id={run_id}/summary.json")
}

pub fn market_regime_context_object_key(run_id: &str) -> String {
    format!("market_regime_context/run_id={run_id}/context.json")
}

pub fn symbol_universe_snapshot_object_key(run_id: &str) -> String {
    format!("symbol_universe_snapshot/run_id={run_id}/snapshot.json")
}

pub fn symbol_universe_bootstrap_rollup_object_key(day_start_ms: i64) -> String {
    let part = time_part(day_start_ms);
    format!(
        "symbol_universe_snapshot/bootstrap_rollup/event_date={}/latest.json",
        part.event_date
    )
}

pub fn index_pointer_key(window_ms: i64, window_start_ms: i64) -> String {
    let part = time_part(window_start_ms);
    format!(
        "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/window_start_ms={window_start_ms}.json",
        part.event_date, part.hour
    )
}

pub fn local_output_path(spool_root: &Path, run_id: &str, key: &str) -> PathBuf {
    spool_root.join("output").join(run_id).join(key)
}

fn string_const_col(len: usize, value: &str) -> ArrayRef {
    Arc::new(StringArray::from_iter_values((0..len).map(|_| value)))
}

fn string_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> &String) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| value(row)),
    ))
}

fn int64_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> i64) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| value(row)),
    ))
}

fn float64_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> Option<f64>) -> ArrayRef {
    Arc::new(Float64Array::from_iter(rows.iter().map(|row| value(row))))
}

fn bool_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> bool) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(
        rows.iter().map(|row| Some(value(row))),
    ))
}

fn string_list_col(rows: &[&SliceRow], value: impl Fn(&SliceRow) -> &Vec<String>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for item in value(row) {
            builder.values().append_value(item);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn trade_events_col(rows: &[&SliceRow]) -> ArrayRef {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(trade_fields(), 0));
    for row in rows {
        for trade in &row.trade_events {
            let values = builder.values();
            values
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(trade.exchange_timestamp_ms);
            values
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(trade.ingest_timestamp_ms);
            values
                .field_builder::<Float64Builder>(2)
                .unwrap()
                .append_value(trade.price);
            values
                .field_builder::<Float64Builder>(3)
                .unwrap()
                .append_value(trade.quantity);
            values
                .field_builder::<StringBuilder>(4)
                .unwrap()
                .append_value(&trade.side);
            append_i64_optional(values, 5, trade.exchange_sequence);
            values
                .field_builder::<StringBuilder>(6)
                .unwrap()
                .append_value(&trade.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn book_ticker_events_col(rows: &[&SliceRow]) -> ArrayRef {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(book_fields(), 0));
    for row in rows {
        for book in &row.book_ticker_events {
            let values = builder.values();
            values
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(book.exchange_timestamp_ms);
            values
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(book.ingest_timestamp_ms);
            values
                .field_builder::<Float64Builder>(2)
                .unwrap()
                .append_value(book.best_bid);
            values
                .field_builder::<Float64Builder>(3)
                .unwrap()
                .append_value(book.best_bid_qty);
            values
                .field_builder::<Float64Builder>(4)
                .unwrap()
                .append_value(book.best_ask);
            values
                .field_builder::<Float64Builder>(5)
                .unwrap()
                .append_value(book.best_ask_qty);
            append_i64_optional(values, 6, book.exchange_sequence);
            values
                .field_builder::<StringBuilder>(7)
                .unwrap()
                .append_value(&book.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn compact_events_col(
    rows: &[&SliceRow],
    value: impl Fn(&SliceRow) -> &Vec<CompactEventRef>,
) -> ArrayRef {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(compact_fields(), 0));
    for row in rows {
        for event in value(row) {
            let values = builder.values();
            values
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(event.exchange_timestamp_ms);
            values
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(event.ingest_timestamp_ms);
            values
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_value(&event.event_type);
            values
                .field_builder::<StringBuilder>(3)
                .unwrap()
                .append_value(&event.parent_event_id);
            values.append(true);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn symbol_health_col(rows: &[&SliceRow]) -> ArrayRef {
    let mut builder = symbol_health_builder();
    for row in rows {
        if let Some(snapshot) = &row.symbol_health_snapshot {
            builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(snapshot.observed_at_ms);
            builder
                .field_builder::<Int64Builder>(1)
                .unwrap()
                .append_value(snapshot.last_event_time_ms);
            builder
                .field_builder::<Int64Builder>(2)
                .unwrap()
                .append_value(snapshot.last_received_time_ms);
            builder
                .field_builder::<Int64Builder>(3)
                .unwrap()
                .append_value(snapshot.latency_ms);
            builder
                .field_builder::<BooleanBuilder>(4)
                .unwrap()
                .append_value(snapshot.is_tradeable);
            append_struct_string_list(&mut builder, 5, &snapshot.reason_codes);
            builder.append(true);
        } else {
            append_null_symbol_health(&mut builder);
        }
    }
    Arc::new(builder.finish())
}

fn source_health_col(rows: &[&SliceRow]) -> ArrayRef {
    let mut builder = source_health_builder();
    for row in rows {
        if let Some(snapshot) = &row.source_health_snapshot {
            builder
                .field_builder::<Int64Builder>(0)
                .unwrap()
                .append_value(snapshot.observed_at_ms);
            builder
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(&snapshot.connection_status);
            builder
                .field_builder::<StringBuilder>(2)
                .unwrap()
                .append_value(&snapshot.health_level);
            builder
                .field_builder::<Int64Builder>(3)
                .unwrap()
                .append_value(snapshot.heartbeat_delay_ms);
            builder
                .field_builder::<Int64Builder>(4)
                .unwrap()
                .append_value(snapshot.stream_lag_ms);
            builder
                .field_builder::<Int64Builder>(5)
                .unwrap()
                .append_value(snapshot.recent_gap_count);
            builder
                .field_builder::<Int64Builder>(6)
                .unwrap()
                .append_value(snapshot.book_rebuild_count);
            builder.append(true);
        } else {
            append_null_source_health(&mut builder);
        }
    }
    Arc::new(builder.finish())
}

fn append_i64_optional(builder: &mut StructBuilder, field_index: usize, value: Option<i64>) {
    let field = builder.field_builder::<Int64Builder>(field_index).unwrap();
    if let Some(value) = value {
        field.append_value(value);
    } else {
        field.append_null();
    }
}

fn symbol_health_builder() -> StructBuilder {
    StructBuilder::new(
        symbol_health_fields(),
        vec![
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(BooleanBuilder::new()),
            Box::new(ListBuilder::new(StringBuilder::new())),
        ],
    )
}

fn source_health_builder() -> StructBuilder {
    StructBuilder::new(
        source_health_fields(),
        vec![
            Box::new(Int64Builder::new()),
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
            Box::new(Int64Builder::new()),
        ],
    )
}

fn append_struct_string_list(builder: &mut StructBuilder, field_index: usize, values: &[String]) {
    let list = builder
        .field_builder::<ListBuilder<StringBuilder>>(field_index)
        .unwrap();
    for value in values {
        list.values().append_value(value);
    }
    list.append(true);
}

fn append_null_symbol_health(builder: &mut StructBuilder) {
    builder
        .field_builder::<Int64Builder>(0)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(1)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(2)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(3)
        .unwrap()
        .append_null();
    builder
        .field_builder::<BooleanBuilder>(4)
        .unwrap()
        .append_null();
    builder
        .field_builder::<ListBuilder<StringBuilder>>(5)
        .unwrap()
        .append(false);
    builder.append(false);
}

fn append_null_source_health(builder: &mut StructBuilder) {
    builder
        .field_builder::<Int64Builder>(0)
        .unwrap()
        .append_null();
    builder
        .field_builder::<StringBuilder>(1)
        .unwrap()
        .append_null();
    builder
        .field_builder::<StringBuilder>(2)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(3)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(4)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(5)
        .unwrap()
        .append_null();
    builder
        .field_builder::<Int64Builder>(6)
        .unwrap()
        .append_null();
    builder.append(false);
}

struct TimePart {
    event_date: String,
    hour: u32,
}

fn time_part(timestamp_ms: i64) -> TimePart {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    TimePart {
        event_date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_15_minute_index_pointer_key() {
        assert_eq!(
            index_pointer_key(1_000, 0),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=0.json"
        );
    }
}
