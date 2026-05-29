use super::native::assign_native_symbols;
use crate::normalize::model::{SliceRow, SymbolUniverseBootstrapSymbolStats};

#[derive(Debug, Clone)]
pub(in crate::normalize::projection::symbol_universe) struct SymbolStats {
    pub(in crate::normalize::projection::symbol_universe) symbol_canonical: String,
    pub(in crate::normalize::projection::symbol_universe) execution_symbol_native: Option<String>,
    pub(in crate::normalize::projection::symbol_universe) reference_symbol_native: Option<String>,
    pub(in crate::normalize::projection::symbol_universe) bootstrap_days_available: i64,
    pub(in crate::normalize::projection::symbol_universe) median_spread_bps: Option<f64>,
    pub(in crate::normalize::projection::symbol_universe) median_traded_notional: Option<f64>,
    pub(in crate::normalize::projection::symbol_universe) gap_rate: Option<f64>,
    pub(in crate::normalize::projection::symbol_universe) mapping_confidence: String,
    pub(super) observed_traded_notional: f64,
}

impl SymbolStats {
    pub(super) fn from_slice(row: &SliceRow) -> Self {
        Self {
            symbol_canonical: row.symbol_canonical.clone(),
            execution_symbol_native: None,
            reference_symbol_native: None,
            observed_traded_notional: 0.0,
            bootstrap_days_available: 0,
            median_spread_bps: None,
            median_traded_notional: None,
            gap_rate: None,
            mapping_confidence: "moderate".to_owned(),
        }
    }

    pub(super) fn from_bootstrap(symbol: &SymbolUniverseBootstrapSymbolStats) -> Self {
        Self {
            symbol_canonical: symbol.symbol_canonical.clone(),
            execution_symbol_native: symbol.execution_symbol_native.clone(),
            reference_symbol_native: symbol.reference_symbol_native.clone(),
            observed_traded_notional: 0.0,
            bootstrap_days_available: 0,
            median_spread_bps: None,
            median_traded_notional: None,
            gap_rate: None,
            mapping_confidence: symbol.mapping_confidence.clone(),
        }
    }

    pub(super) fn observe_native_symbol(&mut self, row: &SliceRow) {
        assign_native_symbols(
            row,
            &mut self.execution_symbol_native,
            &mut self.reference_symbol_native,
        );
    }
}
