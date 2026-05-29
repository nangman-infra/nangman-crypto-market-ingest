use super::StorageError;
use super::gap::{GapAlertDraft, GapAlertRecord};
use super::health::{SourceHealthDraft, SourceHealthRecord};
use super::partition::{
    GapPartitionKey, HealthPartitionKey, RawPartitionKey, SymbolHealthPartitionKey,
    gap_partition_for, health_partition_for, raw_partition_for, symbol_health_partition_for,
};
use super::record::{RawMarketEventDraft, RawMarketEventRecord};
use super::s3_upload::S3Uploader;
use super::symbol_health::{SymbolHealthDraft, SymbolHealthRecord};
use crate::live::LiveMarketPublisher;
use std::collections::{BTreeMap, VecDeque};

mod config;
mod flush;
mod live;
mod report;
mod state;

pub use config::L0StorageConfig;
pub use report::StorageReport;

use config::{connect_live_publisher, validate_config};
use report::{FailedUploadObject, UploadedObject};

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
}
