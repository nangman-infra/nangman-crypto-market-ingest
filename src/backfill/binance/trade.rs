use super::types::AggTrade;
use crate::backfill::BackfillError;
use crate::binance::BinanceMarket;
use crate::clock;
use crate::storage::record::RawMarketEventDraft;
use serde_json::json;

pub(super) fn raw_trade_draft(
    market: &BinanceMarket,
    trade: &AggTrade,
) -> Result<RawMarketEventDraft, BackfillError> {
    let payload = json!({
        "data": {
            "e": "trade",
            "E": trade.trade_timestamp_ms,
            "s": market.raw_symbol,
            "a": trade.aggregate_trade_id,
            "f": trade.first_trade_id,
            "l": trade.last_trade_id,
            "T": trade.trade_timestamp_ms,
            "p": trade.price,
            "q": trade.quantity,
            "m": trade.is_buyer_maker,
            "M": trade.is_best_match,
            "source": "aggTrades"
        }
    });
    let sequence_tag = format!("binance:trade:{}", trade.aggregate_trade_id);
    Ok(RawMarketEventDraft {
        event_type: "trade".to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: trade.trade_timestamp_ms,
        ingest_timestamp_ms: clock::now_ms(),
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: Some(trade.aggregate_trade_id),
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: false,
        stream_type: "HISTORICAL_REST".to_owned(),
        stream_phase: "backfill".to_owned(),
        payload_json: serde_json::to_string(&payload)?,
    })
}
