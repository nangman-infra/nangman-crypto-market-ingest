use crate::normalize::model::SliceRow;
use std::collections::BTreeMap;

pub(in crate::normalize::projection) fn group_slices_by_symbol(
    slices: &[SliceRow],
) -> BTreeMap<SymbolWindowKey, Vec<&SliceRow>> {
    let mut grouped = BTreeMap::<SymbolWindowKey, Vec<&SliceRow>>::new();
    for row in slices {
        grouped
            .entry(SymbolWindowKey {
                venue: row.venue.clone(),
                symbol_native: row.symbol_native.clone(),
                symbol_canonical: row.symbol_canonical.clone(),
                market_type: row.market_type.clone(),
            })
            .or_default()
            .push(row);
    }
    for rows in grouped.values_mut() {
        rows.sort_by_key(|row| row.window_start_ms);
    }
    grouped
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::normalize::projection) struct SymbolWindowKey {
    venue: String,
    symbol_native: String,
    symbol_canonical: String,
    market_type: String,
}

pub(in crate::normalize::projection) fn price(row: &SliceRow) -> Option<f64> {
    row.mid_price
        .or(row.last_trade_price)
        .filter(|value| value.is_finite() && *value > 0.0)
}

pub(in crate::normalize::projection) fn volume(row: &SliceRow) -> Option<f64> {
    Some(row.trade_volume).filter(|value| value.is_finite() && *value >= 0.0)
}

pub(in crate::normalize::projection) fn value_at_or_before(
    rows: &[&SliceRow],
    target_window_start_ms: i64,
    value: impl Fn(&SliceRow) -> Option<f64>,
) -> Option<f64> {
    let idx = rows.partition_point(|row| row.window_start_ms <= target_window_start_ms);
    if idx == 0 {
        return None;
    }
    value(rows[idx - 1])
}
