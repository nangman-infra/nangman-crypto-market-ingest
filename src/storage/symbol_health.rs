use super::StorageError;
use super::record::sha256_hex;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct SymbolHealthDraft {
    pub venue: String,
    pub symbol_native: String,
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolHealthRecord {
    pub symbol_health_event_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub symbol_native: String,
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

impl SymbolHealthRecord {
    pub fn from_draft(draft: SymbolHealthDraft, producer_run_id: &str, ordinal: u64) -> Self {
        let reason_codes = draft.reason_codes.join(";");
        let payload = format!(
            "{}:{}:{}:{}",
            draft.venue, draft.symbol_native, draft.observed_at_ms, reason_codes
        );
        Self {
            symbol_health_event_id: format!(
                "symbol_health_{}_{}_{}_{}",
                draft.venue, draft.symbol_native, draft.observed_at_ms, ordinal
            ),
            producer_run_id: producer_run_id.to_owned(),
            venue: draft.venue,
            symbol_native: draft.symbol_native,
            observed_at_ms: draft.observed_at_ms,
            last_event_time_ms: draft.last_event_time_ms,
            latency_ms: draft.latency_ms,
            is_tradeable: draft.is_tradeable,
            reason_codes,
            payload_sha256: sha256_hex(payload.as_bytes()),
            schema_version: "symbol_health_v1".to_owned(),
        }
    }
}

pub fn write_symbol_health_parquet(
    path: &Path,
    records: &[SymbolHealthRecord],
) -> Result<(), StorageError> {
    let schema = symbol_health_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            string_col(records, |record| &record.symbol_health_event_id),
            string_col(records, |record| &record.producer_run_id),
            string_col(records, |record| &record.venue),
            string_col(records, |record| &record.symbol_native),
            int64_col(records, |record| record.observed_at_ms),
            int64_col(records, |record| record.last_event_time_ms),
            int64_col(records, |record| record.latency_ms),
            bool_col(records, |record| record.is_tradeable),
            string_col(records, |record| &record.reason_codes),
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

fn symbol_health_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("symbol_health_event_id", DataType::Utf8),
        field("producer_run_id", DataType::Utf8),
        field("venue", DataType::Utf8),
        field("symbol_native", DataType::Utf8),
        field("observed_at_ms", DataType::Int64),
        field("last_event_time_ms", DataType::Int64),
        field("latency_ms", DataType::Int64),
        field("is_tradeable", DataType::Boolean),
        field("reason_codes", DataType::Utf8),
        field("payload_sha256", DataType::Utf8),
        field("schema_version", DataType::Utf8),
    ]))
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn string_col(
    records: &[SymbolHealthRecord],
    value: impl Fn(&SymbolHealthRecord) -> &String,
) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(records.iter().map(value)))
}

fn int64_col(
    records: &[SymbolHealthRecord],
    value: impl Fn(&SymbolHealthRecord) -> i64,
) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(records.iter().map(value)))
}

fn bool_col(
    records: &[SymbolHealthRecord],
    value: impl Fn(&SymbolHealthRecord) -> bool,
) -> ArrayRef {
    Arc::new(BooleanArray::from_iter(
        records.iter().map(|record| Some(value(record))),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn from_draft_joins_reasons_and_builds_stable_identity() {
        let record = SymbolHealthRecord::from_draft(
            SymbolHealthDraft {
                venue: "binance".to_owned(),
                symbol_native: "BTCUSDT".to_owned(),
                observed_at_ms: 30,
                last_event_time_ms: 20,
                latency_ms: 10,
                is_tradeable: false,
                reason_codes: vec!["stale".to_owned(), "gap".to_owned()],
            },
            "run-1",
            3,
        );

        assert_eq!(
            record.symbol_health_event_id,
            "symbol_health_binance_BTCUSDT_30_3"
        );
        assert_eq!(record.reason_codes, "stale;gap");
        assert!(!record.is_tradeable);
        assert_eq!(record.payload_sha256.len(), 64);
        assert_eq!(record.schema_version, "symbol_health_v1");
    }

    #[test]
    fn writes_symbol_health_parquet_file() {
        let path = temp_parquet_path("symbol-health");
        let record = SymbolHealthRecord::from_draft(
            SymbolHealthDraft {
                venue: "upbit".to_owned(),
                symbol_native: "KRW-BTC".to_owned(),
                observed_at_ms: 30,
                last_event_time_ms: 30,
                latency_ms: 0,
                is_tradeable: true,
                reason_codes: Vec::new(),
            },
            "run-2",
            1,
        );

        write_symbol_health_parquet(&path, &[record]).unwrap();

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
