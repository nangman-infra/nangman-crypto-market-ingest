use super::types::{UpbitBackfillMarket, UpbitTrade};
use crate::backfill::{BackfillError, SymbolBackfillReport};
use crate::clock;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use serde_json::json;

pub(super) async fn append_page_trades(
    page: &[UpbitTrade],
    market: &UpbitBackfillMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
    report: &mut SymbolBackfillReport,
) -> Result<bool, BackfillError> {
    let mut reached_start = false;
    for trade in page {
        if trade.timestamp > input_end_ms {
            continue;
        }
        if trade.timestamp < input_start_ms {
            reached_start = true;
            continue;
        }
        sink.append_raw_market_event(raw_trade_draft(market, trade)?)
            .await
            .map_err(|error| BackfillError::Storage(error.to_string()))?;
        report.observe(trade.timestamp);
    }
    Ok(reached_start)
}

pub(super) fn raw_trade_draft(
    market: &UpbitBackfillMarket,
    trade: &UpbitTrade,
) -> Result<RawMarketEventDraft, BackfillError> {
    let payload = json!({
        "type": "trade",
        "code": trade.market,
        "timestamp": trade.timestamp,
        "trade_timestamp": trade.timestamp,
        "trade_price": trade.trade_price,
        "trade_volume": trade.trade_volume,
        "ask_bid": trade.ask_bid,
        "sequential_id": trade.sequential_id,
        "stream_type": "BACKFILL"
    });
    let sequence_tag = format!("upbit:trade:{}", trade.sequential_id);
    Ok(RawMarketEventDraft {
        event_type: "trade".to_owned(),
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.market.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: trade.timestamp,
        ingest_timestamp_ms: clock::now_ms(),
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: Some(trade.sequential_id),
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: false,
        stream_type: "HISTORICAL_REST".to_owned(),
        stream_phase: "backfill".to_owned(),
        payload_json: serde_json::to_string(&payload)?,
    })
}
