use super::super::gap::write_gap_alert_parquet;
use super::super::health::write_source_health_parquet;
use super::super::parquet_file::write_raw_market_event_parquet;
use super::super::partition::{
    GapPartitionKey, HealthPartitionKey, RawPartitionKey, SymbolHealthPartitionKey, gap_object_key,
    health_object_key, next_part_number, raw_object_key, symbol_health_object_key,
};
use super::super::symbol_health::write_symbol_health_parquet;
use super::{L0StorageSink, StorageError};

impl L0StorageSink {
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

    pub(super) async fn flush_raw_partition(
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

    pub(super) async fn flush_health_partition(
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

    pub(super) async fn flush_symbol_health_partition(
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

    pub(super) async fn flush_gap_partition(
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
}
