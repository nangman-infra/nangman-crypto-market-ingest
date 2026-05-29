use super::super::types::ResolvedBinanceBackfill;
use crate::backfill::{
    BackfillArgs, BackfillError, BackfillRunReport, SourceHealthSummary, SymbolBackfillReport,
    append_source_health_for, append_symbol_health_for, empty_storage_report,
};
use crate::clock;
use crate::storage::L0StorageSink;

pub(super) async fn write_health_and_report(
    args: &BackfillArgs,
    sink: &mut L0StorageSink,
    resolved: &ResolvedBinanceBackfill,
    symbols: Vec<SymbolBackfillReport>,
) -> Result<BackfillRunReport, BackfillError> {
    let observed_at_ms = clock::now_ms();
    let total_record_count = symbols
        .iter()
        .map(|symbol| symbol.record_count)
        .sum::<u64>();
    let total_gap_alert_count = symbols
        .iter()
        .map(|symbol| symbol.gap_alert_count)
        .sum::<u64>();

    append_symbol_health_for(sink, "binance", &symbols, observed_at_ms).await?;
    append_source_health_for(
        sink,
        SourceHealthSummary {
            venue: "binance",
            source_role: "reference",
            mode: "historical_trade_backfill",
            observed_at_ms,
            args,
            symbol_count: resolved.markets.len(),
            total_record_count,
            total_gap_alert_count,
        },
    )
    .await?;

    Ok(BackfillRunReport {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        input_start_ms: args.input_start_ms,
        input_end_ms: args.input_end_ms,
        requested_symbol_count: args
            .symbols
            .as_ref()
            .map(|symbols| symbols.len())
            .unwrap_or(args.expect_symbol_count),
        processed_symbol_count: symbols.len(),
        total_record_count,
        total_gap_alert_count,
        symbols,
        storage: empty_storage_report(),
    })
}
