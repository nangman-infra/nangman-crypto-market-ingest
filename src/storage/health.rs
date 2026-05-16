use super::StorageError;
use super::record::sha256_hex;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize)]
pub struct SourceHealthDraft {
    pub venue: String,
    pub source_role: String,
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: u64,
    pub book_rebuild_count: u64,
    pub health_level: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceHealthRecord {
    pub health_event_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub source_role: String,
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: i64,
    pub book_rebuild_count: i64,
    pub health_level: String,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

impl SourceHealthRecord {
    pub fn from_draft(draft: SourceHealthDraft, producer_run_id: &str, ordinal: u64) -> Self {
        let payload_sha256 = sha256_hex(draft.payload_json.as_bytes());
        Self {
            health_event_id: format!(
                "health_{}_{}_{}",
                draft.venue, draft.observed_at_ms, ordinal
            ),
            producer_run_id: producer_run_id.to_owned(),
            venue: draft.venue,
            source_role: draft.source_role,
            observed_at_ms: draft.observed_at_ms,
            connection_status: draft.connection_status,
            heartbeat_delay_ms: draft.heartbeat_delay_ms,
            stream_lag_ms: draft.stream_lag_ms,
            recent_gap_count: i64::try_from(draft.recent_gap_count).unwrap_or(i64::MAX),
            book_rebuild_count: i64::try_from(draft.book_rebuild_count).unwrap_or(i64::MAX),
            health_level: draft.health_level,
            payload_json: draft.payload_json,
            payload_sha256,
            schema_version: "source_health_v2".to_owned(),
        }
    }
}

pub fn write_source_health_parquet(
    path: &Path,
    records: &[SourceHealthRecord],
) -> Result<(), StorageError> {
    let schema = source_health_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            string_col(records, |record| &record.health_event_id),
            string_col(records, |record| &record.producer_run_id),
            string_col(records, |record| &record.venue),
            string_col(records, |record| &record.source_role),
            int64_col(records, |record| record.observed_at_ms),
            string_col(records, |record| &record.connection_status),
            int64_col(records, |record| record.heartbeat_delay_ms),
            int64_col(records, |record| record.stream_lag_ms),
            int64_col(records, |record| record.recent_gap_count),
            int64_col(records, |record| record.book_rebuild_count),
            string_col(records, |record| &record.health_level),
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

fn source_health_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("health_event_id", DataType::Utf8),
        field("producer_run_id", DataType::Utf8),
        field("venue", DataType::Utf8),
        field("source_role", DataType::Utf8),
        field("observed_at_ms", DataType::Int64),
        field("connection_status", DataType::Utf8),
        field("heartbeat_delay_ms", DataType::Int64),
        field("stream_lag_ms", DataType::Int64),
        field("recent_gap_count", DataType::Int64),
        field("book_rebuild_count", DataType::Int64),
        field("health_level", DataType::Utf8),
        field("payload_json", DataType::Utf8),
        field("payload_sha256", DataType::Utf8),
        field("schema_version", DataType::Utf8),
    ]))
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn string_col(
    records: &[SourceHealthRecord],
    value: impl Fn(&SourceHealthRecord) -> &String,
) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(records.iter().map(value)))
}

fn int64_col(
    records: &[SourceHealthRecord],
    value: impl Fn(&SourceHealthRecord) -> i64,
) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(records.iter().map(value)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn from_draft_caps_large_counts_and_hashes_payload() {
        let record = SourceHealthRecord::from_draft(
            SourceHealthDraft {
                venue: "binance".to_owned(),
                source_role: "reference".to_owned(),
                observed_at_ms: 20,
                connection_status: "connected".to_owned(),
                heartbeat_delay_ms: 30,
                stream_lag_ms: 40,
                recent_gap_count: u64::MAX,
                book_rebuild_count: 3,
                health_level: "degraded".to_owned(),
                payload_json: r#"{"status":"degraded"}"#.to_owned(),
            },
            "run-1",
            2,
        );

        assert_eq!(record.health_event_id, "health_binance_20_2");
        assert_eq!(record.recent_gap_count, i64::MAX);
        assert_eq!(record.book_rebuild_count, 3);
        assert_eq!(record.payload_sha256.len(), 64);
        assert_eq!(record.schema_version, "source_health_v2");
    }

    #[test]
    fn writes_source_health_parquet_file() {
        let path = temp_parquet_path("source-health");
        let record = SourceHealthRecord::from_draft(
            SourceHealthDraft {
                venue: "upbit".to_owned(),
                source_role: "execution".to_owned(),
                observed_at_ms: 20,
                connection_status: "waiting".to_owned(),
                heartbeat_delay_ms: 0,
                stream_lag_ms: 0,
                recent_gap_count: 0,
                book_rebuild_count: 0,
                health_level: "waiting_for_messages".to_owned(),
                payload_json: r#"{"received":0}"#.to_owned(),
            },
            "run-2",
            1,
        );

        write_source_health_parquet(&path, &[record]).unwrap();

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
