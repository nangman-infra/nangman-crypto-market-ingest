use super::model::{
    GapPartitionKey, HealthPartitionKey, RawPartitionKey, SymbolHealthPartitionKey,
};
use std::collections::BTreeMap;

pub(in crate::storage) fn next_part_number<K: Ord + Clone>(
    part_numbers: &mut BTreeMap<K, u64>,
    partition: &K,
) -> u64 {
    let entry = part_numbers.entry(partition.clone()).or_insert(0);
    *entry += 1;
    *entry
}

pub(in crate::storage) fn raw_object_key(
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

pub(in crate::storage) fn health_object_key(
    partition: &HealthPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "source_health/venue={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue, partition.event_date, partition.hour, partition.shard, run_id, part_number
    )
}

pub(in crate::storage) fn symbol_health_object_key(
    partition: &SymbolHealthPartitionKey,
    run_id: &str,
    part_number: u64,
) -> String {
    format!(
        "symbol_health/venue={}/event_date={}/hour={:02}/shard={:02}/run_id={}-part-{:06}.parquet",
        partition.venue, partition.event_date, partition.hour, partition.shard, run_id, part_number
    )
}

pub(in crate::storage) fn gap_object_key(
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
