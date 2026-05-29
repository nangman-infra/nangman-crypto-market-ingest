use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct EvictionConfig {
    pub spool_root: PathBuf,
    pub high_water_pct: u8,
    pub emergency_pct: u8,
    pub safety_floor_secs: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvictionStats {
    pub disk_used_pct_before: u8,
    pub disk_used_pct_after: u8,
    pub triggered: bool,
    pub emergency: bool,
    pub evicted_count: usize,
    pub evicted_bytes: u64,
    pub candidate_count: usize,
}
