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
    use crate::storage::gap::{GapAlertDraft, GapAlertRecord};
    use crate::storage::health::{SourceHealthDraft, SourceHealthRecord};
    use crate::storage::record::{RawMarketEventDraft, RawMarketEventRecord};
    use crate::storage::symbol_health::{SymbolHealthDraft, SymbolHealthRecord};
    use std::collections::BTreeMap;

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

    #[test]
    fn partitions_records_by_event_time_and_stable_shards() {
        let raw = RawMarketEventRecord::from_draft(raw_draft(), "run-1", 1);
        let source_health = SourceHealthRecord::from_draft(source_health_draft(), "run-1", 1);
        let symbol_health = SymbolHealthRecord::from_draft(symbol_health_draft(), "run-1", 1);
        let gap = GapAlertRecord::from_draft(gap_draft(), "run-1", 1);

        let raw_partition = raw_partition_for(&raw, 16);
        let source_health_partition = health_partition_for(&source_health, 16);
        let symbol_health_partition = symbol_health_partition_for(&symbol_health, 16);
        let gap_partition = gap_partition_for(&gap, 16);

        assert_eq!(raw_partition.event_date, "2026-05-16");
        assert_eq!(raw_partition.hour, 1);
        assert_eq!(raw_partition.event_type, "trade");
        assert!(raw_partition.shard < 16);
        assert_eq!(source_health_partition.event_date, "2026-05-16");
        assert_eq!(source_health_partition.hour, 1);
        assert!(source_health_partition.shard < 16);
        assert_eq!(symbol_health_partition.venue, "upbit");
        assert!(symbol_health_partition.shard < 16);
        assert_eq!(gap_partition.gap_type, "sequence_gap");
        assert!(gap_partition.shard < 16);
    }

    #[test]
    fn part_numbers_increment_per_partition() {
        let mut parts = BTreeMap::new();
        let first = RawPartitionKey {
            venue: "binance".to_owned(),
            event_type: "trade".to_owned(),
            event_date: "2026-05-16".to_owned(),
            hour: 1,
            shard: 1,
        };
        let second = RawPartitionKey {
            venue: "binance".to_owned(),
            event_type: "ticker".to_owned(),
            event_date: "2026-05-16".to_owned(),
            hour: 1,
            shard: 1,
        };

        assert_eq!(next_part_number(&mut parts, &first), 1);
        assert_eq!(next_part_number(&mut parts, &first), 2);
        assert_eq!(next_part_number(&mut parts, &second), 1);
    }

    #[test]
    fn builds_gap_object_keys() {
        let gap = GapPartitionKey {
            venue: "binance".to_owned(),
            gap_type: "sequence_gap".to_owned(),
            event_date: "2026-05-16".to_owned(),
            hour: 1,
            shard: 3,
        };

        assert_eq!(
            gap_object_key(&gap, "run-1", 7),
            "gap_alert/venue=binance/gap_type=sequence_gap/event_date=2026-05-16/hour=01/shard=03/run_id=run-1-part-000007.parquet"
        );
    }

    fn raw_draft() -> RawMarketEventDraft {
        RawMarketEventDraft {
            event_type: "trade".to_owned(),
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            market_type: "spot".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            symbol_canonical: "BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
            exchange_timestamp_ms: 1_778_893_200_000,
            ingest_timestamp_ms: 1_778_893_200_000,
            sequence_id: "binance:trade:1".to_owned(),
            sequence_tag: "binance:trade:1".to_owned(),
            exchange_sequence: Some(1),
            diff_first_update_id: None,
            diff_final_update_id: None,
            is_snapshot: false,
            stream_type: "REALTIME".to_owned(),
            stream_phase: "realtime".to_owned(),
            payload_json: "{}".to_owned(),
        }
    }

    fn source_health_draft() -> SourceHealthDraft {
        SourceHealthDraft {
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            observed_at_ms: 1_778_893_200_000,
            connection_status: "connected".to_owned(),
            heartbeat_delay_ms: 0,
            stream_lag_ms: 0,
            recent_gap_count: 0,
            book_rebuild_count: 0,
            health_level: "connected".to_owned(),
            payload_json: "{}".to_owned(),
        }
    }

    fn symbol_health_draft() -> SymbolHealthDraft {
        SymbolHealthDraft {
            venue: "upbit".to_owned(),
            symbol_native: "KRW-BTC".to_owned(),
            observed_at_ms: 1_778_893_200_000,
            last_event_time_ms: 1_778_893_200_000,
            latency_ms: 0,
            is_tradeable: true,
            reason_codes: Vec::new(),
        }
    }

    fn gap_draft() -> GapAlertDraft {
        GapAlertDraft {
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            gap_type: "sequence_gap".to_owned(),
            detected_at_ms: 1_778_893_200_000,
            expected_sequence_id: Some(1),
            observed_sequence_id: Some(3),
            heal_action: "refetch_snapshot".to_owned(),
            heal_status: "detected".to_owned(),
            payload_json: "{}".to_owned(),
        }
    }
}
