use super::events::{book_ticker_events_col, compact_events_col, trade_events_col};
use super::health::{source_health_col, symbol_health_col};
use super::primitive::{
    bool_col, float64_col, int64_col, string_col, string_const_col, string_list_col,
};
use crate::normalize::model::SliceRow;
use crate::normalize::write_schema::slice_schema;
use crate::storage::StorageError;
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;

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
            trade_events_col(rows)?,
            book_ticker_events_col(rows)?,
            compact_events_col(rows, |row| &row.depth_events)?,
            compact_events_col(rows, |row| &row.ticker_events)?,
            symbol_health_col(rows)?,
            source_health_col(rows)?,
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
