use super::StorageError;
use super::eviction::sealed_marker_path;
use super::gap::{GapAlertDraft, GapAlertRecord, write_gap_alert_parquet};
use super::health::{SourceHealthDraft, SourceHealthRecord, write_source_health_parquet};
use super::parquet_file::write_raw_market_event_parquet;
use super::partition::{
    GapPartitionKey, HealthPartitionKey, RawPartitionKey, SymbolHealthPartitionKey, gap_object_key,
    gap_partition_for, health_object_key, health_partition_for, next_part_number, raw_object_key,
    raw_partition_for, symbol_health_object_key, symbol_health_partition_for,
};
use super::record::{RawMarketEventDraft, RawMarketEventRecord};
use super::s3_upload::S3Uploader;
use super::symbol_health::{SymbolHealthDraft, SymbolHealthRecord, write_symbol_health_parquet};
use crate::live::{LiveMarketNatsConfig, LiveMarketPublisher, MarketLiveTick};
use crate::log_stream;
use serde::Serialize;
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;

const MAX_REPORTED_OBJECTS: usize = 1_000;
const MAX_REPORTED_FAILURES: usize = 200;

#[derive(Debug, Clone, Serialize)]
pub struct L0StorageConfig {
    pub bucket: String,
    pub region: String,
    pub profile: Option<String>,
    pub spool_root: PathBuf,
    pub run_id: String,
    pub flush_records: usize,
    pub shard_count: u16,
    pub live_nats: Option<LiveMarketNatsConfig>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageReport {
    pub bucket: String,
    pub run_id: String,
    pub record_count: u64,
    pub uploaded_object_count: usize,
    pub uploaded_object_retained_count: usize,
    pub uploaded_object_dropped_count: usize,
    pub uploaded_objects: Vec<UploadedObject>,
    pub failed_upload_count: usize,
    pub failed_upload_retained_count: usize,
    pub failed_upload_dropped_count: usize,
    pub failed_uploads: Vec<FailedUploadObject>,
    pub manifest_key: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UploadedObject {
    pub object_family: String,
    pub key: String,
    pub local_path: String,
    pub record_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedUploadObject {
    pub object_family: String,
    pub key: String,
    pub discarded_local_path: String,
    pub record_count: usize,
    pub error: String,
}

pub struct L0StorageSink {
    config: L0StorageConfig,
    uploader: S3Uploader,
    live_publisher: Option<LiveMarketPublisher>,
    raw_buffers: BTreeMap<RawPartitionKey, Vec<RawMarketEventRecord>>,
    health_buffers: BTreeMap<HealthPartitionKey, Vec<SourceHealthRecord>>,
    symbol_health_buffers: BTreeMap<SymbolHealthPartitionKey, Vec<SymbolHealthRecord>>,
    gap_buffers: BTreeMap<GapPartitionKey, Vec<GapAlertRecord>>,
    raw_part_numbers: BTreeMap<RawPartitionKey, u64>,
    health_part_numbers: BTreeMap<HealthPartitionKey, u64>,
    symbol_health_part_numbers: BTreeMap<SymbolHealthPartitionKey, u64>,
    gap_part_numbers: BTreeMap<GapPartitionKey, u64>,
    uploaded_objects: VecDeque<UploadedObject>,
    uploaded_object_count: usize,
    uploaded_object_dropped_count: usize,
    failed_uploads: VecDeque<FailedUploadObject>,
    failed_upload_count: usize,
    failed_upload_dropped_count: usize,
    next_ordinal: u64,
    manifest_key: Option<String>,
}

impl L0StorageSink {
    pub async fn new(config: L0StorageConfig) -> Result<Self, StorageError> {
        validate_config(&config)?;
        let uploader = S3Uploader::new(
            config.bucket.clone(),
            config.region.clone(),
            config.profile.clone(),
        )
        .await?;
        let live_publisher = connect_live_publisher(&config).await?;
        Ok(Self {
            config,
            uploader,
            live_publisher,
            raw_buffers: BTreeMap::new(),
            health_buffers: BTreeMap::new(),
            symbol_health_buffers: BTreeMap::new(),
            gap_buffers: BTreeMap::new(),
            raw_part_numbers: BTreeMap::new(),
            health_part_numbers: BTreeMap::new(),
            symbol_health_part_numbers: BTreeMap::new(),
            gap_part_numbers: BTreeMap::new(),
            uploaded_objects: VecDeque::new(),
            uploaded_object_count: 0,
            uploaded_object_dropped_count: 0,
            failed_uploads: VecDeque::new(),
            failed_upload_count: 0,
            failed_upload_dropped_count: 0,
            next_ordinal: 1,
            manifest_key: None,
        })
    }

    pub async fn append_raw_market_event(
        &mut self,
        draft: RawMarketEventDraft,
    ) -> Result<(), StorageError> {
        let ordinal = self.take_ordinal();
        let record = RawMarketEventRecord::from_draft(draft, &self.config.run_id, ordinal);
        self.publish_live_tick(&record).await?;
        let partition = raw_partition_for(&record, self.config.shard_count);
        let should_flush = {
            let buffer = self.raw_buffers.entry(partition.clone()).or_default();
            buffer.push(record);
            buffer.len() >= self.config.flush_records
        };
        if should_flush {
            self.flush_raw_partition(&partition).await?;
        }
        Ok(())
    }

    async fn publish_live_tick(&self, record: &RawMarketEventRecord) -> Result<(), StorageError> {
        let Some(publisher) = &self.live_publisher else {
            return Ok(());
        };
        let tick = MarketLiveTick::from_raw_market_event(record);
        if !tick.has_mark_price() {
            return Ok(());
        }
        match publisher.publish_tick(&tick).await {
            Ok(()) => Ok(()),
            Err(error) if live_nats_required(&self.config) => Err(StorageError::Nats(format!(
                "publish market live tick {}: {error}",
                tick.event_id
            ))),
            Err(error) => {
                let _ = log_stream::warn(
                    "market_live_tick_publish_failed",
                    json!({
                        "event_id": tick.event_id,
                        "venue": tick.venue,
                        "symbol": tick.symbol_canonical,
                        "error": error.to_string(),
                        "required": false
                    }),
                );
                Ok(())
            }
        }
    }

    pub async fn append_source_health(
        &mut self,
        draft: SourceHealthDraft,
    ) -> Result<(), StorageError> {
        let ordinal = self.take_ordinal();
        let record = SourceHealthRecord::from_draft(draft, &self.config.run_id, ordinal);
        let partition = health_partition_for(&record, self.config.shard_count);
        let should_flush = {
            let buffer = self.health_buffers.entry(partition.clone()).or_default();
            buffer.push(record);
            buffer.len() >= self.config.flush_records
        };
        if should_flush {
            self.flush_health_partition(&partition).await?;
        }
        Ok(())
    }

    pub async fn append_symbol_health(
        &mut self,
        draft: SymbolHealthDraft,
    ) -> Result<(), StorageError> {
        let ordinal = self.take_ordinal();
        let record = SymbolHealthRecord::from_draft(draft, &self.config.run_id, ordinal);
        let partition = symbol_health_partition_for(&record, self.config.shard_count);
        let should_flush = {
            let buffer = self
                .symbol_health_buffers
                .entry(partition.clone())
                .or_default();
            buffer.push(record);
            buffer.len() >= self.config.flush_records
        };
        if should_flush {
            self.flush_symbol_health_partition(&partition).await?;
        }
        Ok(())
    }

    pub async fn append_gap_alert(&mut self, draft: GapAlertDraft) -> Result<(), StorageError> {
        let ordinal = self.take_ordinal();
        let record = GapAlertRecord::from_draft(draft, &self.config.run_id, ordinal);
        let partition = gap_partition_for(&record, self.config.shard_count);
        let should_flush = {
            let buffer = self.gap_buffers.entry(partition.clone()).or_default();
            buffer.push(record);
            buffer.len() >= self.config.flush_records
        };
        if should_flush {
            self.flush_gap_partition(&partition).await?;
        }
        Ok(())
    }

    pub async fn flush_all(&mut self) -> Result<(), StorageError> {
        for partition in self.raw_buffers.keys().cloned().collect::<Vec<_>>() {
            self.flush_raw_partition(&partition).await?;
        }
        for partition in self.health_buffers.keys().cloned().collect::<Vec<_>>() {
            self.flush_health_partition(&partition).await?;
        }
        for partition in self
            .symbol_health_buffers
            .keys()
            .cloned()
            .collect::<Vec<_>>()
        {
            self.flush_symbol_health_partition(&partition).await?;
        }
        for partition in self.gap_buffers.keys().cloned().collect::<Vec<_>>() {
            self.flush_gap_partition(&partition).await?;
        }
        if let Some(publisher) = &self.live_publisher {
            publisher.flush().await.map_err(|error| {
                StorageError::Nats(format!("flush market live publisher: {error}"))
            })?;
        }
        Ok(())
    }

    pub async fn upload_manifest(&mut self) -> Result<(), StorageError> {
        let key = format!("runs/run_id={}/manifest.json", self.config.run_id);
        self.manifest_key = Some(key.clone());
        let manifest_object = UploadedObject {
            object_family: "manifest".to_owned(),
            key: key.clone(),
            local_path: format!("s3://{}/{}", self.config.bucket, key),
            record_count: 1,
        };
        let mut report = self.report();
        append_capped(
            &mut report.uploaded_objects,
            manifest_object.clone(),
            MAX_REPORTED_OBJECTS,
            &mut report.uploaded_object_dropped_count,
        );
        report.uploaded_object_count += 1;
        report.uploaded_object_retained_count = report.uploaded_objects.len();
        let bytes = serde_json::to_vec_pretty(&report)?;
        self.uploader.upload_json(&key, bytes).await?;
        self.record_uploaded_object(manifest_object);
        Ok(())
    }

    pub fn report(&self) -> StorageReport {
        StorageReport {
            bucket: self.config.bucket.clone(),
            run_id: self.config.run_id.clone(),
            record_count: self.next_ordinal.saturating_sub(1),
            uploaded_object_count: self.uploaded_object_count,
            uploaded_object_retained_count: self.uploaded_objects.len(),
            uploaded_object_dropped_count: self.uploaded_object_dropped_count,
            uploaded_objects: self.uploaded_objects.iter().cloned().collect(),
            failed_upload_count: self.failed_upload_count,
            failed_upload_retained_count: self.failed_uploads.len(),
            failed_upload_dropped_count: self.failed_upload_dropped_count,
            failed_uploads: self.failed_uploads.iter().cloned().collect(),
            manifest_key: self.manifest_key.clone(),
        }
    }

    async fn flush_raw_partition(
        &mut self,
        partition: &RawPartitionKey,
    ) -> Result<(), StorageError> {
        let Some(records) = self.raw_buffers.remove(partition) else {
            return Ok(());
        };
        if records.is_empty() {
            return Ok(());
        }
        let part_number = next_part_number(&mut self.raw_part_numbers, partition);
        let key = raw_object_key(partition, &self.config.run_id, part_number);
        let local_path = self.local_path(&key)?;
        write_raw_market_event_parquet(&local_path, &records)?;
        self.upload("raw_market_event", key, local_path, records.len())
            .await
    }

    async fn flush_health_partition(
        &mut self,
        partition: &HealthPartitionKey,
    ) -> Result<(), StorageError> {
        let Some(records) = self.health_buffers.remove(partition) else {
            return Ok(());
        };
        if records.is_empty() {
            return Ok(());
        }
        let part_number = next_part_number(&mut self.health_part_numbers, partition);
        let key = health_object_key(partition, &self.config.run_id, part_number);
        let local_path = self.local_path(&key)?;
        write_source_health_parquet(&local_path, &records)?;
        self.upload("source_health", key, local_path, records.len())
            .await
    }

    async fn flush_symbol_health_partition(
        &mut self,
        partition: &SymbolHealthPartitionKey,
    ) -> Result<(), StorageError> {
        let Some(records) = self.symbol_health_buffers.remove(partition) else {
            return Ok(());
        };
        if records.is_empty() {
            return Ok(());
        }
        let part_number = next_part_number(&mut self.symbol_health_part_numbers, partition);
        let key = symbol_health_object_key(partition, &self.config.run_id, part_number);
        let local_path = self.local_path(&key)?;
        write_symbol_health_parquet(&local_path, &records)?;
        self.upload("symbol_health", key, local_path, records.len())
            .await
    }

    async fn flush_gap_partition(
        &mut self,
        partition: &GapPartitionKey,
    ) -> Result<(), StorageError> {
        let Some(records) = self.gap_buffers.remove(partition) else {
            return Ok(());
        };
        if records.is_empty() {
            return Ok(());
        }
        let part_number = next_part_number(&mut self.gap_part_numbers, partition);
        let key = gap_object_key(partition, &self.config.run_id, part_number);
        let local_path = self.local_path(&key)?;
        write_gap_alert_parquet(&local_path, &records)?;
        self.upload("gap_alert", key, local_path, records.len())
            .await
    }

    async fn upload(
        &mut self,
        object_family: &str,
        key: String,
        local_path: PathBuf,
        record_count: usize,
    ) -> Result<(), StorageError> {
        match self.uploader.upload_file(&key, &local_path).await {
            Ok(()) => {
                let sealed = sealed_marker_path(&local_path);
                let _ = tokio::fs::write(&sealed, b"").await;
                self.record_uploaded_object(UploadedObject {
                    object_family: object_family.to_owned(),
                    key,
                    local_path: local_path.display().to_string(),
                    record_count,
                });
            }
            Err(error) => {
                let error = error.to_string();
                self.record_failed_upload(FailedUploadObject {
                    object_family: object_family.to_owned(),
                    key,
                    discarded_local_path: local_path.display().to_string(),
                    record_count,
                    error,
                });
            }
        }
        Ok(())
    }

    fn record_uploaded_object(&mut self, object: UploadedObject) {
        self.uploaded_object_count += 1;
        push_capped_deque(
            &mut self.uploaded_objects,
            object,
            MAX_REPORTED_OBJECTS,
            &mut self.uploaded_object_dropped_count,
        );
    }

    fn record_failed_upload(&mut self, failure: FailedUploadObject) {
        self.failed_upload_count += 1;
        push_capped_deque(
            &mut self.failed_uploads,
            failure,
            MAX_REPORTED_FAILURES,
            &mut self.failed_upload_dropped_count,
        );
    }

    fn local_path(&self, key: &str) -> Result<PathBuf, StorageError> {
        let local_path = self.config.spool_root.join(&self.config.run_id).join(key);
        if let Some(parent) = local_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(local_path)
    }

    fn take_ordinal(&mut self) -> u64 {
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        ordinal
    }
}

fn validate_config(config: &L0StorageConfig) -> Result<(), StorageError> {
    if config.bucket.is_empty() {
        return Err(StorageError::InvalidConfig(
            "l0 storage bucket is required".to_owned(),
        ));
    }
    if config.flush_records == 0 {
        return Err(StorageError::InvalidConfig(
            "l0 flush records must be positive".to_owned(),
        ));
    }
    if config.shard_count == 0 {
        return Err(StorageError::InvalidConfig(
            "l0 shard count must be positive".to_owned(),
        ));
    }
    if let Some(live_nats) = &config.live_nats {
        if live_nats.url.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS URL must not be empty when configured".to_owned(),
            ));
        }
        if live_nats.stream.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS stream must not be empty when configured".to_owned(),
            ));
        }
        if live_nats.subject_prefix.trim().is_empty() {
            return Err(StorageError::InvalidConfig(
                "live NATS subject prefix must not be empty when configured".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn connect_live_publisher(
    config: &L0StorageConfig,
) -> Result<Option<LiveMarketPublisher>, StorageError> {
    let Some(live_nats) = &config.live_nats else {
        return Ok(None);
    };
    match LiveMarketPublisher::connect(live_nats).await {
        Ok(publisher) => Ok(Some(publisher)),
        Err(error) if live_nats.required => Err(StorageError::Nats(format!(
            "connect market live publisher {}: {error}",
            live_nats.url
        ))),
        Err(error) => {
            let _ = log_stream::warn(
                "market_live_tick_publisher_disabled",
                json!({
                    "url": live_nats.url,
                    "stream": live_nats.stream,
                    "subject_prefix": live_nats.subject_prefix,
                    "error": error.to_string(),
                    "required": false
                }),
            );
            Ok(None)
        }
    }
}

fn live_nats_required(config: &L0StorageConfig) -> bool {
    config
        .live_nats
        .as_ref()
        .map(|live_nats| live_nats.required)
        .unwrap_or(false)
}

fn append_capped<T>(values: &mut Vec<T>, value: T, max_len: usize, dropped_count: &mut usize) {
    values.push(value);
    if values.len() > max_len {
        values.remove(0);
        *dropped_count += 1;
    }
}

fn push_capped_deque<T>(
    values: &mut VecDeque<T>,
    value: T,
    max_len: usize,
    dropped_count: &mut usize,
) {
    values.push_back(value);
    if values.len() > max_len {
        values.pop_front();
        *dropped_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FailedUploadObject, MAX_REPORTED_FAILURES, MAX_REPORTED_OBJECTS, UploadedObject,
        append_capped,
    };

    #[test]
    fn caps_uploaded_object_report_entries() {
        let mut values = Vec::new();
        let mut dropped_count = 0;

        for index in 0..=MAX_REPORTED_OBJECTS {
            append_capped(
                &mut values,
                UploadedObject {
                    object_family: "raw_market_event".to_owned(),
                    key: format!("key-{index}"),
                    local_path: format!("path-{index}"),
                    record_count: 1,
                },
                MAX_REPORTED_OBJECTS,
                &mut dropped_count,
            );
        }

        assert_eq!(values.len(), MAX_REPORTED_OBJECTS);
        assert_eq!(dropped_count, 1);
        assert_eq!(values.first().unwrap().key, "key-1");
    }

    #[test]
    fn caps_failed_upload_report_entries() {
        let mut values = Vec::new();
        let mut dropped_count = 0;

        for index in 0..=MAX_REPORTED_FAILURES {
            append_capped(
                &mut values,
                FailedUploadObject {
                    object_family: "raw_market_event".to_owned(),
                    key: format!("key-{index}"),
                    discarded_local_path: format!("path-{index}"),
                    record_count: 1,
                    error: "upload failed".to_owned(),
                },
                MAX_REPORTED_FAILURES,
                &mut dropped_count,
            );
        }

        assert_eq!(values.len(), MAX_REPORTED_FAILURES);
        assert_eq!(dropped_count, 1);
        assert_eq!(values.first().unwrap().key, "key-1");
    }
}
