use super::super::fetch::{fetch_agg_trades_by_time, fetch_agg_trades_from_id};
use super::super::trade::raw_trade_draft;
use super::super::types::{AggTrade, BINANCE_PAGE_LIMIT};
use crate::backfill::{BackfillError, SymbolBackfillReport, append_empty_gap_alert};
use crate::binance::BinanceMarket;
use crate::storage::L0StorageSink;

pub(super) async fn backfill_symbol(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &BinanceMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
) -> Result<SymbolBackfillReport, BackfillError> {
    let mut report = SymbolBackfillReport::empty(&market.raw_symbol, &market.base_asset);
    let first_page = fetch_agg_trades_by_time(
        client,
        rest_base_url,
        &market.raw_symbol,
        input_start_ms,
        input_end_ms,
    )
    .await?;
    if first_page.is_empty() {
        append_no_trade_gap(sink, &mut report, market, input_start_ms, input_end_ms).await?;
        return Ok(report);
    }

    let mut stop = append_page(
        sink,
        market,
        input_start_ms,
        input_end_ms,
        &first_page,
        &mut report,
    )
    .await?;
    let mut next_from_id = first_page
        .last()
        .map(|trade| trade.aggregate_trade_id + 1)
        .unwrap_or_default();
    let mut last_page_was_full = first_page.len() == BINANCE_PAGE_LIMIT;

    while !stop && last_page_was_full {
        let page =
            fetch_agg_trades_from_id(client, rest_base_url, &market.raw_symbol, next_from_id)
                .await?;
        if page.is_empty() {
            break;
        }
        stop = append_page(
            sink,
            market,
            input_start_ms,
            input_end_ms,
            &page,
            &mut report,
        )
        .await?;
        let next_candidate = page
            .last()
            .map(|trade| trade.aggregate_trade_id + 1)
            .unwrap_or(next_from_id);
        if next_candidate <= next_from_id {
            return Err(BackfillError::InvalidConfig(format!(
                "Binance aggTrades cursor did not advance for {}",
                market.raw_symbol
            )));
        }
        next_from_id = next_candidate;
        last_page_was_full = page.len() == BINANCE_PAGE_LIMIT;
    }

    if report.record_count == 0 {
        append_no_trade_gap(sink, &mut report, market, input_start_ms, input_end_ms).await?;
    }
    Ok(report)
}

async fn append_no_trade_gap(
    sink: &mut L0StorageSink,
    report: &mut SymbolBackfillReport,
    market: &BinanceMarket,
    input_start_ms: i64,
    input_end_ms: i64,
) -> Result<(), BackfillError> {
    report.note_gap();
    append_empty_gap_alert(
        sink,
        "binance",
        "reference",
        &market.raw_symbol,
        input_start_ms,
        input_end_ms,
        "no_trade_records_returned",
    )
    .await
}

async fn append_page(
    sink: &mut L0StorageSink,
    market: &BinanceMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    page: &[AggTrade],
    report: &mut SymbolBackfillReport,
) -> Result<bool, BackfillError> {
    for trade in page {
        if trade.trade_timestamp_ms > input_end_ms {
            return Ok(true);
        }
        if trade.trade_timestamp_ms < input_start_ms {
            continue;
        }
        sink.append_raw_market_event(raw_trade_draft(market, trade)?)
            .await
            .map_err(|error| BackfillError::Storage(error.to_string()))?;
        report.observe(trade.trade_timestamp_ms);
    }
    Ok(false)
}
