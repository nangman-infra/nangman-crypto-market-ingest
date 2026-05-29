#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::storage) struct RawPartitionKey {
    pub venue: String,
    pub event_type: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::storage) struct HealthPartitionKey {
    pub venue: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}

pub(in crate::storage) type SymbolHealthPartitionKey = HealthPartitionKey;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::storage) struct GapPartitionKey {
    pub venue: String,
    pub gap_type: String,
    pub event_date: String,
    pub hour: u32,
    pub shard: u16,
}
