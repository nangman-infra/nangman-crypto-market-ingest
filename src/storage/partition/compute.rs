use super::model::{
    GapPartitionKey, HealthPartitionKey, RawPartitionKey, SymbolHealthPartitionKey,
};
use crate::storage::gap::GapAlertRecord;
use crate::storage::health::SourceHealthRecord;
use crate::storage::record::RawMarketEventRecord;
use crate::storage::symbol_health::SymbolHealthRecord;
use chrono::{DateTime, Timelike, Utc};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub(in crate::storage) fn raw_partition_for(
    record: &RawMarketEventRecord,
    shard_count: u16,
) -> RawPartitionKey {
    let parts = time_parts(record.exchange_timestamp_ms);
    RawPartitionKey {
        venue: record.venue.clone(),
        event_type: record.event_type.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.symbol_native, shard_count),
    }
}

pub(in crate::storage) fn health_partition_for(
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

pub(in crate::storage) fn symbol_health_partition_for(
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

pub(in crate::storage) fn gap_partition_for(
    record: &GapAlertRecord,
    shard_count: u16,
) -> GapPartitionKey {
    let parts = time_parts(record.detected_at_ms);
    GapPartitionKey {
        venue: record.venue.clone(),
        gap_type: record.gap_type.clone(),
        event_date: parts.date,
        hour: parts.hour,
        shard: shard_for(&record.symbol_native, shard_count),
    }
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
