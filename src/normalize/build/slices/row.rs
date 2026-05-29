use std::collections::BTreeMap;

use crate::normalize::args::{InputRange, NormalizeArgs};
use crate::normalize::model::{SLICE_SCHEMA_VERSION, SliceRow};
use crate::storage::record::sha256_hex;

use super::identity::{Identity, SliceKey};

pub(in crate::normalize::build) fn seed_identity_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    identity: &Identity,
    rows: &mut BTreeMap<SliceKey, SliceRow>,
) {
    let mut window_start_ms = input_range.start_ms;
    while window_start_ms < input_range.end_ms {
        let key = SliceKey {
            venue: identity.venue.clone(),
            symbol_canonical: identity.symbol_canonical.clone(),
            window_start_ms,
        };
        rows.entry(key)
            .or_insert_with(|| empty_slice(args, identity, window_start_ms));
        window_start_ms = window_start_ms.saturating_add(args.window_ms);
    }
}

fn empty_slice(args: &NormalizeArgs, identity: &Identity, window_start_ms: i64) -> SliceRow {
    let slice_key = format!(
        "{}|{}|{}|{}|{}",
        identity.venue,
        identity.symbol_canonical,
        window_start_ms,
        args.window_ms,
        SLICE_SCHEMA_VERSION
    );
    SliceRow {
        slice_id: sha256_hex(slice_key.as_bytes()),
        venue: identity.venue.clone(),
        source_role: identity.source_role.clone(),
        symbol_native: identity.symbol_native.clone(),
        symbol_canonical: identity.symbol_canonical.clone(),
        base_asset: identity.base_asset.clone(),
        quote_asset: identity.quote_asset.clone(),
        market_type: identity.market_type.clone(),
        window_ms: args.window_ms,
        window_start_ms,
        window_end_ms: window_start_ms.saturating_add(args.window_ms),
        slice_completeness: String::new(),
        missing_reasons: Vec::new(),
        quality_ok: 0,
        quality_delayed: 0,
        quality_stale: 0,
        quality_gap: 0,
        quality_invalid: 0,
        trade_count: 0,
        trade_volume: 0.0,
        last_trade_price: None,
        last_trade_size: None,
        best_bid: None,
        best_ask: None,
        mid_price: None,
        spread_bps: None,
        book_ticker_count: 0,
        depth_event_count: 0,
        depth_book_rebuilt: false,
        trade_events: Vec::new(),
        book_ticker_events: Vec::new(),
        depth_events: Vec::new(),
        ticker_events: Vec::new(),
        symbol_health_snapshot: None,
        source_health_snapshot: None,
        parent_event_ids: Vec::new(),
        parent_run_ids: Vec::new(),
    }
}
