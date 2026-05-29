use super::RawMarketEventRecord;
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{ArrowError, Schema};
use std::sync::Arc;

pub(super) fn raw_market_event_batch(
    schema: Arc<Schema>,
    records: &[RawMarketEventRecord],
) -> Result<RecordBatch, ArrowError> {
    RecordBatch::try_new(
        schema,
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
    )
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
