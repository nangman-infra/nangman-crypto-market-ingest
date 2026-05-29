use crate::messages::BinanceDiffDepthMessage;
use crypto_domain::{FixedDecimal, Sequence, TimestampMs};
use serde::Serialize;
use std::collections::BTreeMap;

// Upper bound on events held while waiting for a snapshot to arrive.
// 50 symbols * ~10 depth msgs/sec * 200s safety window ~= 100k worst case
// across symbols; per-book 1_000 protects a single symbol whose snapshot
// fetch is repeatedly failing without starving the rest.
pub(crate) const MAX_BUFFERED_EVENTS: usize = 1_000;

#[derive(Debug, Clone, Serialize)]
pub struct BinanceDepthSyncSettings {
    pub rest_base_url: String,
    pub snapshot_limit: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceGapAlert {
    pub gap_type: String,
    pub symbol: String,
    pub detected_at_ms: TimestampMs,
    pub expected_sequence_id: Option<Sequence>,
    pub observed_sequence_id: Option<Sequence>,
    pub heal_action: String,
    pub heal_status: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BinanceLocalOrderBook {
    pub(crate) last_update_id: Option<Sequence>,
    /// Bid levels keyed by `FixedDecimal` price so iteration is price-sorted
    /// and zero-quantity entries can be detected via `FixedDecimal::is_positive`.
    pub(crate) bids: BTreeMap<FixedDecimal, FixedDecimal>,
    pub(crate) asks: BTreeMap<FixedDecimal, FixedDecimal>,
    pub(crate) buffered_events: Vec<BinanceDiffDepthMessage>,
}

impl BinanceLocalOrderBook {
    pub(crate) fn is_synced(&self) -> bool {
        self.last_update_id.is_some()
    }

    pub(crate) fn reset_for_resync(&mut self, event: BinanceDiffDepthMessage) {
        self.last_update_id = None;
        self.bids.clear();
        self.asks.clear();
        self.buffered_events.clear();
        self.buffered_events.push(event);
    }

    pub(crate) fn reset_after_overflow(&mut self) {
        self.last_update_id = None;
        self.bids.clear();
        self.asks.clear();
        self.buffered_events.clear();
    }

    pub(crate) fn buffered_at_capacity(&self) -> bool {
        self.buffered_events.len() >= MAX_BUFFERED_EVENTS
    }
}
