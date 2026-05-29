use crate::storage::record::RawMarketEventDraft;
use std::collections::BTreeSet;

#[derive(Debug)]
pub struct FundingRateSnapshotBatch {
    pub drafts: Vec<RawMarketEventDraft>,
    pub supported_symbols: BTreeSet<String>,
}
