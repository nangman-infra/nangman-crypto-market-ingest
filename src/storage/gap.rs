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
pub struct GapAlertDraft {
    pub venue: String,
    pub source_role: String,
    pub symbol_native: String,
    pub gap_type: String,
    pub detected_at_ms: i64,
    pub expected_sequence_id: Option<i64>,
    pub observed_sequence_id: Option<i64>,
    pub heal_action: String,
    pub heal_status: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GapAlertRecord {
    pub gap_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub source_role: String,
    pub symbol_native: String,
    pub gap_type: String,
    pub detected_at_ms: i64,
    pub expected_sequence_id: String,
    pub observed_sequence_id: String,
    pub heal_action: String,
    pub heal_status: String,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

impl GapAlertRecord {
    pub fn from_draft(draft: GapAlertDraft, producer_run_id: &str, ordinal: u64) -> Self {
        let payload_sha256 = sha256_hex(draft.payload_json.as_bytes());
        Self {
            gap_id: format!(
                "gap_{}_{}_{}_{}",
                draft.venue, draft.gap_type, draft.detected_at_ms, ordinal
            ),
            producer_run_id: producer_run_id.to_owned(),
            venue: draft.venue,
            source_role: draft.source_role,
            symbol_native: draft.symbol_native,
            gap_type: draft.gap_type,
            detected_at_ms: draft.detected_at_ms,
            expected_sequence_id: optional_i64(draft.expected_sequence_id),
            observed_sequence_id: optional_i64(draft.observed_sequence_id),
            heal_action: draft.heal_action,
            heal_status: draft.heal_status,
            payload_json: draft.payload_json,
            payload_sha256,
            schema_version: "gap_alert_v1".to_owned(),
        }
    }
}

pub fn write_gap_alert_parquet(
    path: &Path,
    records: &[GapAlertRecord],
) -> Result<(), StorageError> {
    let schema = gap_alert_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            string_col(records, |record| &record.gap_id),
            string_col(records, |record| &record.producer_run_id),
            string_col(records, |record| &record.venue),
            string_col(records, |record| &record.source_role),
            string_col(records, |record| &record.symbol_native),
            string_col(records, |record| &record.gap_type),
            int64_col(records, |record| record.detected_at_ms),
            string_col(records, |record| &record.expected_sequence_id),
            string_col(records, |record| &record.observed_sequence_id),
            string_col(records, |record| &record.heal_action),
            string_col(records, |record| &record.heal_status),
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

fn gap_alert_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        field("gap_id", DataType::Utf8),
        field("producer_run_id", DataType::Utf8),
        field("venue", DataType::Utf8),
        field("source_role", DataType::Utf8),
        field("symbol_native", DataType::Utf8),
        field("gap_type", DataType::Utf8),
        field("detected_at_ms", DataType::Int64),
        field("expected_sequence_id", DataType::Utf8),
        field("observed_sequence_id", DataType::Utf8),
        field("heal_action", DataType::Utf8),
        field("heal_status", DataType::Utf8),
        field("payload_json", DataType::Utf8),
        field("payload_sha256", DataType::Utf8),
        field("schema_version", DataType::Utf8),
    ]))
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(|inner| inner.to_string()).unwrap_or_default()
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

fn string_col(records: &[GapAlertRecord], value: impl Fn(&GapAlertRecord) -> &String) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(records.iter().map(value)))
}

fn int64_col(records: &[GapAlertRecord], value: impl Fn(&GapAlertRecord) -> i64) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(records.iter().map(value)))
}

#[cfg(test)]
mod tests;
