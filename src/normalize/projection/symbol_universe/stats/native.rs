use crate::normalize::model::SliceRow;

pub(super) fn assign_native_symbols(
    row: &SliceRow,
    execution_symbol_native: &mut Option<String>,
    reference_symbol_native: &mut Option<String>,
) {
    if row.venue == "upbit" && execution_symbol_native.is_none() {
        *execution_symbol_native = Some(row.symbol_native.clone());
    }
    if row.venue == "binance" && reference_symbol_native.is_none() {
        *reference_symbol_native = Some(row.symbol_native.clone());
    }
    execution_symbol_native.get_or_insert_with(|| row.symbol_native.clone());
    reference_symbol_native.get_or_insert_with(|| row.symbol_native.clone());
}
