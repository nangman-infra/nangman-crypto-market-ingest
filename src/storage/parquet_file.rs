use super::StorageError;
use super::record::RawMarketEventRecord;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub fn write_raw_market_event_parquet(
    path: &Path,
    records: &[RawMarketEventRecord],
) -> Result<(), StorageError> {
    let schema = raw_market_event_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            string_col(records, |record| &record.event_id),
            string_col(records, |record| &record.producer_run_id),
            string_col(records, |record| &record.venue),
            string_col(records, |record| &record.source_role),
            string_col(records, |record| &record.market_type),
            string_col(records, |record| &record.event_type),
            string_col(records, |record| &record.symbol_native),
            string_col(records, |record| &record.symbol_canonical),
            string_col(records, |record| &record.base_asset),
            string_col(records, |record| &record.quote_asset),
            int64_col(records, |record| record.exchange_timestamp_ms),
            int64_col(records, |record| record.ingest_timestamp_ms),
            string_col(records, |record| &record.sequence_id),
            string_col(records, |record| &record.sequence_tag),
            int64_opt_col(records, |record| record.exchange_sequence),
            int64_opt_col(records, |record| record.diff_first_update_id),
            int64_opt_col(records, |record| record.diff_final_update_id),
            bool_col(records, |record| record.is_snapshot),
            string_col(records, |record| &record.stream_type),
            string_col(records, |record| &record.stream_phase),
            string_col(records, |record| &record.payload_json),
            string_col(records, |record| &record.payload_sha256),
            string_col(records, |record| &record.schema_version),
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

fn raw_market_event_schema() -> Arc<Schema> {
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

fn string_col(
    records: &[RawMarketEventRecord],
    value: impl Fn(&RawMarketEventRecord) -> &String,
) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(records.iter().map(value)))
}

fn int64_col(
    records: &[RawMarketEventRecord],
    value: impl Fn(&RawMarketEventRecord) -> i64,
) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(records.iter().map(value)))
}

fn int64_opt_col(
    records: &[RawMarketEventRecord],
    value: impl Fn(&RawMarketEventRecord) -> Option<i64>,
) -> ArrayRef {
    Arc::new(Int64Array::from_iter(records.iter().map(value)))
}

fn bool_col(
    records: &[RawMarketEventRecord],
    value: impl Fn(&RawMarketEventRecord) -> bool,
) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(
        records.iter().map(|record| Some(value(record))),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{RawMarketEventDraft, RawMarketEventRecord};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn writes_raw_market_event_parquet_file_with_nullable_sequences() {
        let path = temp_parquet_path("raw-market-event");
        let record = RawMarketEventRecord::from_draft(
            RawMarketEventDraft {
                event_type: "depth_delta".to_owned(),
                venue: "binance".to_owned(),
                source_role: "reference".to_owned(),
                market_type: "spot".to_owned(),
                symbol_native: "BTCUSDT".to_owned(),
                symbol_canonical: "BTC".to_owned(),
                base_asset: "BTC".to_owned(),
                quote_asset: "USDT".to_owned(),
                exchange_timestamp_ms: 100,
                ingest_timestamp_ms: 110,
                sequence_id: "binance:depth_delta:42".to_owned(),
                sequence_tag: String::new(),
                exchange_sequence: Some(42),
                diff_first_update_id: Some(40),
                diff_final_update_id: Some(42),
                is_snapshot: false,
                stream_type: "REALTIME".to_owned(),
                stream_phase: "realtime".to_owned(),
                payload_json: r#"{"b":[],"a":[]}"#.to_owned(),
            },
            "run-1",
            1,
        );

        write_raw_market_event_parquet(&path, &[record]).unwrap();

        assert!(fs::metadata(&path).unwrap().len() > 0);
        let _ = fs::remove_file(path);
    }

    fn temp_parquet_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "market-ingest-{label}-{}-{nonce}.parquet",
            std::process::id()
        ))
    }
}
