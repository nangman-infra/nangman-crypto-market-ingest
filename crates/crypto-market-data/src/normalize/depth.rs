use crate::error::MarketDataError;
use crate::messages::BinancePartialDepthMessage;
use crate::normalize::math::{
    calculate_depth_imbalance_bps, calculate_spread_bps, classify_quality, midpoint_price,
};
use crate::stream_config::BinanceStreamConfig;
use crypto_domain::{
    EventQuality, FixedDecimal, MarketDepthSnapshot, OrderBookLevel, TimestampMs, TraceId,
};

pub(super) fn normalize_binance_partial_depth(
    config: &BinanceStreamConfig,
    stream: &str,
    message: BinancePartialDepthMessage,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketDepthSnapshot, MarketDataError> {
    let raw_symbol = partial_depth_symbol_from_stream(stream)?;
    let symbol = config.symbol(raw_symbol)?;
    let bids = parse_depth_levels(message.bids)?;
    let asks = parse_depth_levels(message.asks)?;
    let best_bid = bids
        .first()
        .ok_or_else(|| MarketDataError::InvalidMessage("depth bids are empty".to_owned()))?
        .price;
    let best_ask = asks
        .first()
        .ok_or_else(|| MarketDataError::InvalidMessage("depth asks are empty".to_owned()))?
        .price;
    let bid_depth_qty = sum_level_quantities(&bids)?;
    let ask_depth_qty = sum_level_quantities(&asks)?;
    let depth_imbalance_bps = calculate_depth_imbalance_bps(bid_depth_qty, ask_depth_qty)?;
    let reference_price = midpoint_price(best_bid, best_ask)?;
    let spread_bps = calculate_spread_bps(best_bid, best_ask, reference_price)?;
    let quality = classify_quality(received_time_ms, received_time_ms, config.max_latency_ms);

    let snapshot = MarketDepthSnapshot {
        decision_trace_id,
        exchange: symbol.exchange.clone(),
        symbol,
        event_time_ms: received_time_ms,
        received_time_ms,
        sequence: message.last_update_id,
        quality,
        level_count: bids.len().max(asks.len()),
        bids,
        asks,
        bid_depth_qty,
        ask_depth_qty,
        depth_imbalance_bps,
        spread_bps,
    };
    if snapshot.validate().is_err() {
        return Ok(MarketDepthSnapshot {
            quality: EventQuality::Invalid,
            ..snapshot
        });
    }
    Ok(snapshot)
}

fn partial_depth_symbol_from_stream(stream: &str) -> Result<&str, MarketDataError> {
    let mut parts = stream.split('@');
    let symbol = parts.next().unwrap_or_default();
    let suffix = parts.next().unwrap_or_default();
    if symbol.trim().is_empty() || !matches!(suffix, "depth5" | "depth10" | "depth20") {
        return Err(MarketDataError::InvalidMessage(format!(
            "unsupported partial depth stream: {stream}"
        )));
    }
    Ok(symbol)
}

fn parse_depth_levels(
    raw_levels: Vec<[String; 2]>,
) -> Result<Vec<OrderBookLevel>, MarketDataError> {
    raw_levels
        .into_iter()
        .map(|raw_level| {
            let [price, quantity] = raw_level;
            Ok(OrderBookLevel {
                price: FixedDecimal::parse_unsigned(&price)?,
                quantity: FixedDecimal::parse_unsigned(&quantity)?,
            })
        })
        .collect()
}

fn sum_level_quantities(levels: &[OrderBookLevel]) -> Result<FixedDecimal, MarketDataError> {
    let mut total = FixedDecimal::zero();
    for level in levels {
        total = total.checked_add(level.quantity)?;
    }
    Ok(total)
}
