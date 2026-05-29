use super::super::types::ResolvedBinanceBackfill;
use crate::backfill::{BackfillArgs, BackfillError};
use crate::log_stream;
use serde_json::json;

pub(super) fn log_start(
    args: &BackfillArgs,
    resolved: &ResolvedBinanceBackfill,
) -> Result<(), BackfillError> {
    log_stream::info(
        "market_backfill_start",
        json!({
            "venue": "binance",
            "source_role": "reference",
            "symbol_count": resolved.markets.len(),
            "input_start_ms": args.input_start_ms,
            "input_end_ms": args.input_end_ms,
            "rest_base_url": resolved.rest_base_url,
            "mode": "historical_trade_backfill"
        }),
    )
    .map_err(BackfillError::Json)
}
