use crate::MAX_CLOCK_SKEW_MS;
use crate::error::MarketDataError;
use crate::messages::{
    BinanceCombinedMessage, BinancePartialDepthMessage, BinancePayload, BinanceTickerMessage,
};
use crate::stream_config::{BinanceNormalizedMarketEvent, BinanceStreamConfig, BinanceStreamKind};
use crypto_domain::{
    Bps, DomainError, EventQuality, FixedDecimal, MarketDepthSnapshot, MarketSnapshot,
    OrderBookLevel, Sequence, TimestampMs, TraceId,
};
use serde_json::Value;

pub fn normalize_binance_ticker_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketSnapshot, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload = unwrap_combined_payload(value)?;
    let message: BinanceTickerMessage = serde_json::from_value(payload)?;
    normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
}

pub fn normalize_binance_partial_depth_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<MarketDepthSnapshot, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let (stream, payload) = unwrap_combined_stream_payload(value)?;
    let message: BinancePartialDepthMessage = serde_json::from_value(payload)?;
    normalize_binance_partial_depth(
        config,
        &stream,
        message,
        received_time_ms,
        decision_trace_id,
    )
}

pub fn normalize_binance_stream_message(
    config: &BinanceStreamConfig,
    raw_json: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
) -> Result<BinanceNormalizedMarketEvent, MarketDataError> {
    let value: Value = serde_json::from_str(raw_json)?;
    let payload = unwrap_binance_payload(value)?;
    let Some(stream) = payload.stream else {
        let message: BinanceTickerMessage = serde_json::from_value(payload.data)?;
        return normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
            .map(BinanceNormalizedMarketEvent::Market);
    };
    let stream_kind = stream_kind_from_stream(&stream)?;
    match stream_kind {
        BinanceStreamKind::Ticker => {
            let message: BinanceTickerMessage = serde_json::from_value(payload.data)?;
            normalize_binance_ticker(config, message, received_time_ms, decision_trace_id)
                .map(BinanceNormalizedMarketEvent::Market)
        }
        BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => {
            let message: BinancePartialDepthMessage = serde_json::from_value(payload.data)?;
            normalize_binance_partial_depth(
                config,
                &stream,
                message,
                received_time_ms,
                decision_trace_id,
            )
            .map(BinanceNormalizedMarketEvent::Depth)
        }
        BinanceStreamKind::Trade
        | BinanceStreamKind::BookTicker
        | BinanceStreamKind::DiffDepth100ms => Err(MarketDataError::InvalidMessage(format!(
            "{} stream is raw-ingest only and is not supported for replay normalization",
            stream_kind.name()
        ))),
    }
}

pub(crate) fn sequence_from_binance_payload(
    kind: BinanceStreamKind,
    payload: &Value,
) -> Option<Sequence> {
    let field = match kind {
        BinanceStreamKind::Trade => "t",
        BinanceStreamKind::Ticker => "L",
        BinanceStreamKind::BookTicker => "u",
        BinanceStreamKind::DiffDepth100ms => "u",
        BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => "lastUpdateId",
    };
    payload.get(field)?.as_u64()
}

pub(crate) fn stream_symbol(stream: &str) -> Option<&str> {
    let symbol = stream.split('@').next()?.trim();
    (!symbol.is_empty()).then_some(symbol)
}

pub(crate) fn unwrap_binance_payload(value: Value) -> Result<BinancePayload, MarketDataError> {
    if value.get("stream").is_some() && value.get("data").is_some() {
        let combined: BinanceCombinedMessage = serde_json::from_value(value)?;
        if combined.stream.trim().is_empty() {
            return Err(MarketDataError::InvalidMessage(
                "combined stream name is empty".to_owned(),
            ));
        }
        return Ok(BinancePayload {
            stream: Some(combined.stream),
            data: combined.data,
        });
    }
    Ok(BinancePayload {
        stream: None,
        data: value,
    })
}

pub(crate) fn stream_kind_from_stream(stream: &str) -> Result<BinanceStreamKind, MarketDataError> {
    let suffix = stream
        .split_once('@')
        .map(|(_, suffix)| suffix)
        .ok_or_else(|| MarketDataError::InvalidMessage(format!("unsupported stream: {stream}")))?;
    match suffix {
        "trade" => Ok(BinanceStreamKind::Trade),
        "ticker" => Ok(BinanceStreamKind::Ticker),
        "depth@100ms" => Ok(BinanceStreamKind::DiffDepth100ms),
        "depth5" => Ok(BinanceStreamKind::PartialDepth5),
        "depth10" => Ok(BinanceStreamKind::PartialDepth10),
        "depth20" => Ok(BinanceStreamKind::PartialDepth20),
        "bookTicker" => Ok(BinanceStreamKind::BookTicker),
        _ => Err(MarketDataError::InvalidMessage(format!(
            "unsupported stream: {stream}"
        ))),
    }
}

fn unwrap_combined_payload(value: Value) -> Result<Value, MarketDataError> {
    Ok(unwrap_binance_payload(value)?.data)
}

fn unwrap_combined_stream_payload(value: Value) -> Result<(String, Value), MarketDataError> {
    let payload = unwrap_binance_payload(value)?;
    let stream = payload.stream.ok_or_else(|| {
        MarketDataError::InvalidMessage(
            "combined stream wrapper is required for partial depth payload".to_owned(),
        )
    })?;
    Ok((stream, payload.data))
}

fn normalize_binance_ticker(
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

fn normalize_binance_partial_depth(
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

fn calculate_depth_imbalance_bps(
    bid_depth_qty: FixedDecimal,
    ask_depth_qty: FixedDecimal,
) -> Result<Bps, MarketDataError> {
    let total_qty = bid_depth_qty.checked_add(ask_depth_qty)?;
    if total_qty.value == 0 {
        return Ok(Bps::new(0));
    }
    let delta_qty = bid_depth_qty.checked_sub(ask_depth_qty)?;
    let ratio = delta_qty.div_to_scale(total_qty, 8)?;
    let value = ratio
        .value
        .checked_mul(10_000)
        .ok_or(DomainError::ScaleOverflow)?
        / 100_000_000_i128;
    let value = i64::try_from(value).map_err(|_| DomainError::ScaleOverflow)?;
    Ok(Bps::new(value))
}

fn classify_quality(
    event_time_ms: TimestampMs,
    received_time_ms: TimestampMs,
    max_latency_ms: i64,
) -> EventQuality {
    if event_time_ms <= 0 || received_time_ms <= 0 || received_time_ms < event_time_ms {
        return EventQuality::Invalid;
    }
    if received_time_ms - event_time_ms > max_latency_ms {
        return EventQuality::Delayed;
    }
    EventQuality::Ok
}

fn normalize_received_time(
    event_time_ms: TimestampMs,
    received_time_ms: TimestampMs,
) -> TimestampMs {
    if event_time_ms > 0
        && received_time_ms > 0
        && received_time_ms < event_time_ms
        && event_time_ms - received_time_ms <= MAX_CLOCK_SKEW_MS
    {
        event_time_ms
    } else {
        received_time_ms
    }
}

fn midpoint_price(
    best_bid: FixedDecimal,
    best_ask: FixedDecimal,
) -> Result<FixedDecimal, MarketDataError> {
    let total = best_bid.checked_add(best_ask)?;
    Ok(FixedDecimal::new(total.value / 2, total.scale))
}

fn calculate_spread_bps(
    best_bid: FixedDecimal,
    best_ask: FixedDecimal,
    reference_price: FixedDecimal,
) -> Result<Bps, MarketDataError> {
    if best_ask.checked_lt(best_bid)? {
        return Ok(Bps::new(0));
    }
    let spread = best_ask.checked_sub(best_bid)?;
    let spread_ratio = spread.div_to_scale(reference_price, 8)?;
    let numerator = spread_ratio
        .value
        .checked_mul(10_000)
        .ok_or(DomainError::ScaleOverflow)?
        .max(0);
    let denominator = 100_000_000_i128;
    let value = if numerator == 0 {
        0
    } else {
        (numerator + denominator - 1) / denominator
    };
    let value = i64::try_from(value).map_err(|_| DomainError::ScaleOverflow)?;
    Ok(Bps::new(value))
}
