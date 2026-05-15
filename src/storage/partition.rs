use super::gap::GapAlertRecord;
use super::health::SourceHealthRecord;
use super::record::RawMarketEventRecord;
use super::symbol_health::SymbolHealthRecord;
use chrono::{DateTime, Timelike, Utc};
use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RawPartitionKey {
    pub venue: String,
    pub event_type: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct HealthPartitionKey {
    pub venue: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}

pub(super) type SymbolHealthPartitionKey = HealthPartitionKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct GapPartitionKey {
    pub venue: String,
    pub gap_type: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}

pub(super) fn raw_partition_for(
    record: &RawMarketEventRecord,
    shard_count: u16,
) -> RawPartitionKey {
    let parts = time_parts(record.ingest_timestamp_ms);
    RawPartitionKey {
        venue: record.venue.clone(),
        event_type: record.event_type.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.symbol_native, shard_count),
    }
}

pub(super) fn health_partition_for(
    record: &SourceHealthRecord,
    shard_count: u16,
) -> HealthPartitionKey {
    let parts = time_parts(record.observed_at_ms);
    HealthPartitionKey {
        venue: record.venue.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.venue, shard_count),
    }
}

pub(super) fn symbol_health_partition_for(
    record: &SymbolHealthRecord,
    shard_count: u16,
) -> SymbolHealthPartitionKey {
    let parts = time_parts(record.observed_at_ms);
    HealthPartitionKey {
        venue: record.venue.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.symbol_native, shard_count),
    }
}

pub(super) fn gap_partition_for(record: &GapAlertRecord, shard_count: u16) -> GapPartitionKey {
    let parts = time_parts(record.detected_at_ms);
    GapPartitionKey {
        venue: record.venue.clone(),
        gap_type: record.gap_type.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.symbol_native, shard_count),
    }
}

pub(super) fn next_part_number<K: Ord + Clone>(
    part_numbers: &mut BTreeMap<K, u64>,
    partition: &K,
) -> u64 {
    let entry = part_numbers.entry(partition.clone()).or_insert(0);
    *entry += 1;
    *entry
}

pub(super) fn raw_object_key(
    partition: &RawPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "raw_market_event/venue={}/event_type={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue,
        partition.event_type,
        partition.event_date,
        partition.hour,
        partition.shard,
        run_id,
        part_number
    )
}

pub(super) fn health_object_key(
    partition: &HealthPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "source_health/venue={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue, partition.event_date, partition.hour, partition.shard, run_id, part_number
    )
}

pub(super) fn symbol_health_object_key(
    partition: &SymbolHealthPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "symbol_health/venue={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue, partition.event_date, partition.hour, partition.shard, run_id, part_number
    )
}

pub(super) fn gap_object_key(
    partition: &GapPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "gap_alert/venue={}/gap_type={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue,
        partition.gap_type,
        partition.event_date,
        partition.hour,
        partition.shard,
        run_id,
        part_number
    )
}

struct TimeParts {
    date: String,
    hour: u32,
}

fn time_parts(timestamp_ms: i64) -> TimeParts {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    TimeParts {
        date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

fn shard_for(value: &str, shard_count: u16) -> u16 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    (hasher.finish() % u64::from(shard_count)) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_l0_object_keys() {
        let raw = RawPartitionKey {
            venue: "upbit".to_owned(),
            event_type: "trade".to_owned(),
            event_date: "2026-05-05".to_owned(),
            hour: 2,
            shard: 0,
        };
        assert_eq!(
            raw_object_key(&raw, "run-1", 1),
            "raw_market_event/venue=upbit/event_type=trade/event_date=2026-05-05/hour=02/shard=00/run_id=run-1-part-000001.parquet"
        );

        let health = HealthPartitionKey {
            venue: "binance".to_owned(),
            event_date: "2026-05-05".to_owned(),
            hour: 2,
            shard: 0,
        };
        assert_eq!(
            health_object_key(&health, "run-1", 1),
            "source_health/venue=binance/event_date=2026-05-05/hour=02/shard=00/run_id=run-1-part-000001.parquet"
        );
        assert_eq!(
            symbol_health_object_key(&health, "run-1", 1),
            "symbol_health/venue=binance/event_date=2026-05-05/hour=02/shard=00/run_id=run-1-part-000001.parquet"
        );
    }
}
