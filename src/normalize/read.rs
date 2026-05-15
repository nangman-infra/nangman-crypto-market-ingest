use super::args::{InputRange, NormalizeArgs};
use super::build::{BuildAccumulator, BuildInputMetadata, BuildResult};
use super::input_keys::{InputEntry, InputEntrySource, collect_input_entries};
use super::mode::RunMode;
use super::model::{
    GapAlertInput, NormalizeInputs, RawInputEvent, SourceHealthInput, SymbolHealthInput,
};
use crate::log_stream;
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use futures_util::stream::{self, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use std::fs::File;
use std::path::{Path, PathBuf};
use tokio::time::{Duration, Instant};

const DOWNLOAD_CONCURRENCY: usize = 16;

pub async fn read_inputs(
    args: &NormalizeArgs,
    range: InputRange,
    run_mode: RunMode,
    session_id: &str,
) -> Result<NormalizeInputs, StorageError> {
    let reader = L0InputReader::new(args, run_mode, session_id).await?;
    reader.read_range(range).await
}

pub async fn read_and_build_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    scan_range: InputRange,
    read_range: InputRange,
    run_mode: RunMode,
    session_id: &str,
) -> Result<BuildResult, StorageError> {
    let reader = L0InputReader::new(args, run_mode, session_id).await?;
    reader
        .fold_range(args, input_range, scan_range, read_range)
        .await
}

/// Best-effort cleanup of the catch-up tmp directory for a run. Called by
/// the orchestrator after publish succeeds (or fails) to enforce the
/// `catchup_tmp_lifecycle` invariant: the session dir must not survive the run.
pub async fn cleanup_session_tmp(catchup_tmp_root: &Path, session_id: &str) {
    let session_dir = catchup_tmp_root.join(session_id);
    let _ = tokio::fs::remove_dir_all(&session_dir).await;
}

struct L0InputReader {
    s3: S3Uploader,
    l0_local_root: PathBuf,
    catchup_session_root: PathBuf,
    run_mode: RunMode,
    l0_run_key_overlap_ms: i64,
}

impl L0InputReader {
    async fn new(
        args: &NormalizeArgs,
        run_mode: RunMode,
        session_id: &str,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            s3: S3Uploader::new(
                args.l0_s3_bucket.clone(),
                args.aws_region.clone(),
                args.aws_profile.clone(),
            )
            .await?,
            l0_local_root: args.l0_local_root.clone(),
            catchup_session_root: args.catchup_tmp_root.join(session_id),
            run_mode,
            l0_run_key_overlap_ms: args.l0_run_key_overlap_ms,
        })
    }

    async fn read_range(&self, range: InputRange) -> Result<NormalizeInputs, StorageError> {
        let entries = self.input_entries(range).await?;
        let keys = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        let input_local_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::Local))
            .count();
        let input_s3_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::S3))
            .count();
        // LIVE mode is supposed to read local only. Any S3 hit means L0 ingest is missing data
        // for this range and we had to recover via fallback — flag it for control-plane.
        let fallback_alert = matches!(self.run_mode, RunMode::Live) && input_s3_object_count > 0;
        let mut inputs = NormalizeInputs {
            raw_events: Vec::new(),
            symbol_health: Vec::new(),
            source_health: Vec::new(),
            gap_alerts: Vec::new(),
            run_mode: self.run_mode.as_str().to_owned(),
            fallback_alert,
            input_local_object_count,
            input_s3_object_count,
            input_object_keys: keys.clone(),
        };

        if input_s3_object_count > 0 {
            let _ = log_stream::debug(
                "market_normalize_downloading",
                json!({
                    "s3_object_count": input_s3_object_count,
                    "local_object_count": input_local_object_count,
                    "total_object_count": keys.len(),
                    "download_concurrency": DOWNLOAD_CONCURRENCY
                }),
            );
        }
        let materialized_entries = self
            .materialize_entries(entries, input_s3_object_count)
            .await?;
        for materialized in materialized_entries {
            append_batches(
                &materialized.entry.key,
                &materialized.local_path,
                &mut inputs,
            )?;
            if materialized.remove_after_read {
                remove_file_best_effort(&materialized.local_path).await;
            }
        }

        Ok(inputs)
    }

    async fn fold_range(
        &self,
        args: &NormalizeArgs,
        input_range: InputRange,
        scan_range: InputRange,
        read_range: InputRange,
    ) -> Result<BuildResult, StorageError> {
        let entries = self.input_entries(read_range).await?;
        let keys = entries
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        let input_local_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::Local))
            .count();
        let input_s3_object_count = entries
            .iter()
            .filter(|entry| matches!(entry.source, InputEntrySource::S3))
            .count();
        let fallback_alert = matches!(self.run_mode, RunMode::Live) && input_s3_object_count > 0;
        if input_s3_object_count > 0 {
            let _ = log_stream::debug(
                "market_normalize_downloading",
                json!({
                    "s3_object_count": input_s3_object_count,
                    "local_object_count": input_local_object_count,
                    "total_object_count": keys.len(),
                    "download_concurrency": DOWNLOAD_CONCURRENCY
                }),
            );
        }

        let metadata = BuildInputMetadata {
            run_mode: self.run_mode.as_str().to_owned(),
            fallback_alert,
            input_local_object_count,
            input_s3_object_count,
            input_object_keys: keys,
        };
        let mut accumulator = BuildAccumulator::new(args, input_range, scan_range, metadata);
        let downloads = stream::iter(entries.into_iter().enumerate().map(|(index, entry)| {
            let s3 = self.s3.clone();
            let catchup_session_root = self.catchup_session_root.clone();
            async move { materialize_entry(s3, catchup_session_root, index, entry).await }
        }))
        .buffer_unordered(DOWNLOAD_CONCURRENCY);

        tokio::pin!(downloads);
        let mut downloaded_s3_object_count = 0_usize;
        let mut downloaded_s3_bytes = 0_u64;
        let mut next_download_progress_at = Instant::now() + Duration::from_secs(10);
        while let Some(result) = downloads.next().await {
            let (_, materialized) = result?;
            if materialized.remove_after_read {
                downloaded_s3_object_count += 1;
                downloaded_s3_bytes =
                    downloaded_s3_bytes.saturating_add(materialized.downloaded_bytes);
                if downloaded_s3_object_count == input_s3_object_count
                    || downloaded_s3_object_count.is_multiple_of(10)
                    || Instant::now() >= next_download_progress_at
                {
                    let _ = log_stream::debug(
                        "market_normalize_download_progress",
                        json!({
                            "downloaded_files": downloaded_s3_object_count,
                            "total_files": input_s3_object_count,
                            "downloaded_bytes": downloaded_s3_bytes,
                            "download_concurrency": DOWNLOAD_CONCURRENCY
                        }),
                    );
                    next_download_progress_at = Instant::now() + Duration::from_secs(10);
                }
            }
            append_batches_to_accumulator(
                &materialized.entry.key,
                &materialized.local_path,
                args,
                &mut accumulator,
            )?;
            if materialized.remove_after_read {
                remove_file_best_effort(&materialized.local_path).await;
            }
        }

        Ok(accumulator.finish())
    }

    async fn input_entries(&self, range: InputRange) -> Result<Vec<InputEntry>, StorageError> {
        collect_input_entries(
            &self.s3,
            &self.l0_local_root,
            range,
            self.run_mode,
            self.l0_run_key_overlap_ms,
        )
        .await
    }

    async fn materialize_entries(
        &self,
        entries: Vec<InputEntry>,
        input_s3_object_count: usize,
    ) -> Result<Vec<MaterializedInputEntry>, StorageError> {
        let mut materialized_entries = Vec::with_capacity(entries.len());
        for _ in 0..entries.len() {
            materialized_entries.push(None);
        }

        let downloads = stream::iter(entries.into_iter().enumerate().map(|(index, entry)| {
            let s3 = self.s3.clone();
            let catchup_session_root = self.catchup_session_root.clone();
            async move { materialize_entry(s3, catchup_session_root, index, entry).await }
        }))
        .buffer_unordered(DOWNLOAD_CONCURRENCY);

        tokio::pin!(downloads);
        let mut downloaded_s3_object_count = 0_usize;
        let mut downloaded_s3_bytes = 0_u64;
        let mut next_download_progress_at = Instant::now() + Duration::from_secs(10);
        while let Some(result) = downloads.next().await {
            let (index, materialized) = result?;
            if materialized.remove_after_read {
                downloaded_s3_object_count += 1;
                downloaded_s3_bytes =
                    downloaded_s3_bytes.saturating_add(materialized.downloaded_bytes);
                if downloaded_s3_object_count == input_s3_object_count
                    || downloaded_s3_object_count.is_multiple_of(10)
                    || Instant::now() >= next_download_progress_at
                {
                    let _ = log_stream::debug(
                        "market_normalize_download_progress",
                        json!({
                            "downloaded_files": downloaded_s3_object_count,
                            "total_files": input_s3_object_count,
                            "downloaded_bytes": downloaded_s3_bytes,
                            "download_concurrency": DOWNLOAD_CONCURRENCY
                        }),
                    );
                    next_download_progress_at = Instant::now() + Duration::from_secs(10);
                }
            }
            materialized_entries[index] = Some(materialized);
        }

        Ok(materialized_entries
            .into_iter()
            .map(|entry| entry.expect("materialized entry must be filled"))
            .collect())
    }
}

struct MaterializedInputEntry {
    entry: InputEntry,
    local_path: PathBuf,
    remove_after_read: bool,
    downloaded_bytes: u64,
}

async fn materialize_entry(
    s3: S3Uploader,
    catchup_session_root: PathBuf,
    index: usize,
    entry: InputEntry,
) -> Result<(usize, MaterializedInputEntry), StorageError> {
    if let Some(path) = entry.path.clone() {
        return Ok((
            index,
            MaterializedInputEntry {
                entry,
                local_path: path,
                remove_after_read: false,
                downloaded_bytes: 0,
            },
        ));
    }
    // Catch-up / fallback tmp layout: catchup_tmp_root/{session_id}/{key}
    // The whole session dir is removed by cleanup_session_tmp at run end.
    let spool_path = catchup_session_root.join(&entry.key);
    s3.download_file(&entry.key, &spool_path).await?;
    let downloaded_bytes = file_size_best_effort(&spool_path).await.unwrap_or(0);
    Ok((
        index,
        MaterializedInputEntry {
            entry,
            local_path: spool_path,
            remove_after_read: true,
            downloaded_bytes,
        },
    ))
}

fn append_batches(
    key: &str,
    path: &Path,
    inputs: &mut NormalizeInputs,
) -> Result<(), StorageError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    for batch in reader {
        let batch = batch?;
        if key.starts_with("raw_market_event/") {
            append_raw_events(&mut inputs.raw_events, &batch)?;
        } else if key.starts_with("symbol_health/") {
            append_symbol_health(&mut inputs.symbol_health, &batch)?;
        } else if key.starts_with("source_health/") {
            append_source_health(&mut inputs.source_health, &batch)?;
        } else if key.starts_with("gap_alert/") {
            append_gap_alerts(&mut inputs.gap_alerts, &batch)?;
        }
    }
    Ok(())
}

fn append_batches_to_accumulator(
    key: &str,
    path: &Path,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let Some(family) = InputObjectFamily::from_key(key) else {
        return Ok(());
    };
    for batch in reader {
        let batch = batch?;
        append_batch_to_accumulator(family, &batch, args, accumulator)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum InputObjectFamily {
    RawMarketEvent,
    SymbolHealth,
    SourceHealth,
    GapAlert,
}

impl InputObjectFamily {
    fn from_key(key: &str) -> Option<Self> {
        if key.starts_with("raw_market_event/") {
            Some(Self::RawMarketEvent)
        } else if key.starts_with("symbol_health/") {
            Some(Self::SymbolHealth)
        } else if key.starts_with("source_health/") {
            Some(Self::SourceHealth)
        } else if key.starts_with("gap_alert/") {
            Some(Self::GapAlert)
        } else {
            None
        }
    }
}

fn append_batch_to_accumulator(
    family: InputObjectFamily,
    batch: &RecordBatch,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        append_accumulator_row(family, batch, row, args, accumulator)?;
    }
    Ok(())
}

fn append_accumulator_row(
    family: InputObjectFamily,
    batch: &RecordBatch,
    row: usize,
    args: &NormalizeArgs,
    accumulator: &mut BuildAccumulator,
) -> Result<(), StorageError> {
    match family {
        InputObjectFamily::RawMarketEvent => {
            accumulator.ingest_raw_event(args, raw_event_from_batch(batch, row)?);
        }
        InputObjectFamily::SymbolHealth => {
            accumulator.ingest_symbol_health(symbol_health_from_batch(batch, row)?);
        }
        InputObjectFamily::SourceHealth => {
            accumulator.ingest_source_health(source_health_from_batch(batch, row)?);
        }
        InputObjectFamily::GapAlert => {
            accumulator.ingest_gap_alert(gap_alert_from_batch(batch, row)?);
        }
    }
    Ok(())
}

fn append_raw_events(
    output: &mut Vec<RawInputEvent>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(raw_event_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_symbol_health(
    output: &mut Vec<SymbolHealthInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(symbol_health_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_source_health(
    output: &mut Vec<SourceHealthInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(source_health_from_batch(batch, row)?);
    }
    Ok(())
}

fn append_gap_alerts(
    output: &mut Vec<GapAlertInput>,
    batch: &RecordBatch,
) -> Result<(), StorageError> {
    for row in 0..batch.num_rows() {
        output.push(gap_alert_from_batch(batch, row)?);
    }
    Ok(())
}

fn raw_event_from_batch(batch: &RecordBatch, row: usize) -> Result<RawInputEvent, StorageError> {
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

fn symbol_health_from_batch(
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

fn source_health_from_batch(
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

fn gap_alert_from_batch(batch: &RecordBatch, row: usize) -> Result<GapAlertInput, StorageError> {
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

fn string_value(batch: &RecordBatch, name: &str, row: usize) -> Result<String, StorageError> {
    let array = downcast_column::<StringArray>(batch, name)?;
    Ok(if array.is_null(row) {
        String::new()
    } else {
        array.value(row).to_owned()
    })
}

fn int64_value(batch: &RecordBatch, name: &str, row: usize) -> Result<i64, StorageError> {
    let array = downcast_column::<Int64Array>(batch, name)?;
    Ok(if array.is_null(row) {
        0
    } else {
        array.value(row)
    })
}

fn int64_optional(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<i64>, StorageError> {
    let array = downcast_column::<Int64Array>(batch, name)?;
    Ok(if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    })
}

fn bool_value(batch: &RecordBatch, name: &str, row: usize) -> Result<bool, StorageError> {
    let array = downcast_column::<BooleanArray>(batch, name)?;
    Ok(!array.is_null(row) && array.value(row))
}

fn downcast_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, StorageError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|error| StorageError::InvalidConfig(error.to_string()))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| StorageError::InvalidConfig(format!("column {name} has unexpected type")))
}

async fn remove_file_best_effort(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}

async fn file_size_best_effort(path: &Path) -> Option<u64> {
    tokio::fs::metadata(path)
        .await
        .ok()
        .map(|metadata| metadata.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_root(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "market-normalize-read-{}-{}-{}",
            name,
            std::process::id(),
            nanos
        ))
    }

    #[tokio::test]
    async fn cleanup_session_tmp_removes_session_dir_only() {
        let root = unique_root("cleanup");
        let session = "l1_run_session_test";
        let session_dir = root.join(session);
        let other_dir = root.join("other_session");
        std::fs::create_dir_all(session_dir.join("raw_market_event/venue=binance")).unwrap();
        std::fs::create_dir_all(other_dir.join("raw_market_event")).unwrap();
        std::fs::write(
            session_dir.join("raw_market_event/venue=binance/a.parquet"),
            b"",
        )
        .unwrap();
        std::fs::write(other_dir.join("raw_market_event/keep.parquet"), b"").unwrap();

        cleanup_session_tmp(&root, session).await;

        assert!(!session_dir.exists());
        assert!(other_dir.exists());
        assert!(other_dir.join("raw_market_event/keep.parquet").exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn cleanup_session_tmp_is_no_op_when_session_missing() {
        let root = unique_root("cleanup-missing");
        cleanup_session_tmp(&root, "never-existed").await;
        // No panic, no error. Best-effort.
    }
}
