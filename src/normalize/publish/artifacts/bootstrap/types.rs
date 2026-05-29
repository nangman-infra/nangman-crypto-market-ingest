use crate::normalize::model::SymbolUniverseBootstrapRollup;

pub(super) struct BootstrapRollupReadResult {
    pub(super) rollups: Vec<SymbolUniverseBootstrapRollup>,
    pub(super) missing_count: usize,
    pub(super) invalid_count: usize,
}
