use super::cursor::{advance_cursor, initial_cursor, validate_recent_window};
use super::fetch::{UPBIT_PAGE_LIMIT, fetch_trade_page};
use super::markets::{resolve_markets, resolve_rest_base_url};
use super::trade::append_page_trades;
use super::types::UpbitBackfillMarket;
use crate::backfill::{
    BackfillArgs, BackfillError, BackfillRunReport, SourceHealthSummary, SymbolBackfillReport,
    append_empty_gap_alert, append_source_health_for, append_symbol_health_for,
    empty_storage_report,
};
use crate::clock;
use crate::log_stream;
use crate::storage::L0StorageSink;
use serde_json::json;
use std::time::Duration;

pub(in crate::backfill) async fn run(
    args: &BackfillArgs,
    sink: &mut L0StorageSink,
) -> Result<BackfillRunReport, BackfillError> {
    validate_recent_window(args.input_start_ms, args.input_end_ms)?;
    let resolved = resolve_markets(args).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    log_stream::info(
        "market_backfill_start",
        json!({
            "venue": "upbit",
            "source_role": "execution",
            "symbol_count": resolved.len(),
            "input_start_ms": args.input_start_ms,
            "input_end_ms": args.input_end_ms,
            "quote_currency": args.upbit_quote_currency,
            "mode": "recent_trade_repair"
        }),
    )
    .map_err(BackfillError::Json)?;

    let rest_base_url = resolve_rest_base_url(args).await?;
    let mut symbols = Vec::with_capacity(resolved.len());
    for market in &resolved {
        symbols.push(
            backfill_symbol(
                &client,
                &rest_base_url,
                market,
                args.input_start_ms,
                args.input_end_ms,
                sink,
            )
            .await?,
        );
    }

    let observed_at_ms = clock::now_ms();
    let total_record_count = symbols
        .iter()
        .map(|symbol| symbol.record_count)
        .sum::<u64>();
    let total_gap_alert_count = symbols
        .iter()
        .map(|symbol| symbol.gap_alert_count)
        .sum::<u64>();
    append_symbol_health_for(sink, "upbit", &symbols, observed_at_ms).await?;
    append_source_health_for(
        sink,
        SourceHealthSummary {
            venue: "upbit",
            source_role: "execution",
            mode: "recent_trade_repair",
            observed_at_ms,
            args,
            symbol_count: resolved.len(),
            total_record_count,
            total_gap_alert_count,
        },
    )
    .await?;

    Ok(BackfillRunReport {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
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

async fn backfill_symbol(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &UpbitBackfillMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
) -> Result<SymbolBackfillReport, BackfillError> {
    let mut report = SymbolBackfillReport::empty(&market.market, &market.base_asset);
    let initial_cursor = initial_cursor(input_end_ms)?;
    let mut next_cursor = None;
    let mut first_page = true;

    loop {
        let page = fetch_trade_page(
            client,
            rest_base_url,
            &market.market,
            &initial_cursor,
            next_cursor,
            first_page,
        )
        .await?;
        if page.is_empty() {
            break;
        }

        if append_page_trades(
            &page,
            market,
            input_start_ms,
            input_end_ms,
            sink,
            &mut report,
        )
        .await?
        {
            break;
        }

        next_cursor = advance_cursor(&page, next_cursor, &market.market)?;
        first_page = false;
        if page.len() < UPBIT_PAGE_LIMIT {
            break;
        }
    }

    if report.record_count == 0 {
        report.note_gap();
        append_empty_gap_alert(
            sink,
            "upbit",
            "execution",
            &market.market,
            input_start_ms,
            input_end_ms,
            "no_trade_records_returned",
        )
        .await?;
    }
    Ok(report)
}
