use crate::error::MarketDataError;
use crate::messages::BinanceTickerMessage;
use crate::normalize::math::{calculate_spread_bps, classify_quality, normalize_received_time};
use crate::stream_config::BinanceStreamConfig;
use crypto_domain::{EventQuality, FixedDecimal, MarketSnapshot, TimestampMs, TraceId};

pub(super) fn normalize_binance_ticker(
    config: &BinanceStreamConfig,
    message: BinanceTickerMessage,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketSnapshot, MarketDataError> {
    if message.event_type != "24hrTicker" {
        return Err(MarketDataError::InvalidMessage(format!(
            "unsupported event type: {}",
            message.event_type
        )));
    }

    let symbol = config.symbol(&message.symbol)?;
    let last_price = FixedDecimal::parse_unsigned(&message.last_price)?;
    let best_bid = FixedDecimal::parse_unsigned(&message.best_bid)?;
    let best_ask = FixedDecimal::parse_unsigned(&message.best_ask)?;
    let best_bid_qty = FixedDecimal::parse_unsigned(&message.best_bid_qty)?;
    let best_ask_qty = FixedDecimal::parse_unsigned(&message.best_ask_qty)?;
    let spread_bps = calculate_spread_bps(best_bid, best_ask, last_price)?;
    let received_time_ms = normalize_received_time(message.event_time_ms, received_time_ms);
    let quality = classify_quality(
        message.event_time_ms,
        received_time_ms,
        config.max_latency_ms,
    );

    let snapshot = MarketSnapshot {
        decision_trace_id,
        exchange: symbol.exchange.clone(),
        symbol,
        event_time_ms: message.event_time_ms,
        received_time_ms,
        sequence: message.last_trade_id,
        quality,
        last_price,
        best_bid,
        best_ask,
        best_bid_qty,
        best_ask_qty,
        spread_bps,
    };
    if snapshot.validate().is_err() {
        return Ok(MarketSnapshot {
            quality: EventQuality::Invalid,
            ..snapshot
        });
    }
    Ok(snapshot)
}
