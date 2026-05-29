use super::types::{BinanceGapAlert, BinanceLocalOrderBook};
use crate::messages::{BinanceDiffDepthMessage, BinanceOrderBookSnapshot};
use crypto_domain::{FixedDecimal, Sequence};
use std::collections::BTreeMap;

pub(crate) fn sync_depth_book_from_snapshot(
    book: &mut BinanceLocalOrderBook,
    snapshot: BinanceOrderBookSnapshot,
) -> Result<(), Box<BinanceGapAlert>> {
    book.last_update_id = Some(snapshot.last_update_id);
    book.bids = parse_snapshot_levels(&snapshot.bids).map_err(snapshot_parse_alert)?;
    book.asks = parse_snapshot_levels(&snapshot.asks).map_err(snapshot_parse_alert)?;
    book.buffered_events
        .retain(|event| event.final_update_id > snapshot.last_update_id);

    let Some(first_event) = book.buffered_events.first() else {
        return Ok(());
    };
    if first_event.first_update_id > snapshot.last_update_id + 1 {
        return Err(Box::new(BinanceGapAlert {
            gap_type: "snapshot_alignment_gap".to_owned(),
            symbol: String::new(),
            detected_at_ms: 0,
            expected_sequence_id: Some(snapshot.last_update_id + 1),
            observed_sequence_id: Some(first_event.first_update_id),
            heal_action: "refetch_snapshot".to_owned(),
            heal_status: "pending".to_owned(),
        }));
    }

    let buffered_events = std::mem::take(&mut book.buffered_events);
    for event in buffered_events {
        if let Some(last_update_id) = book.last_update_id {
            if event.final_update_id <= last_update_id {
                continue;
            }
            if event.first_update_id > last_update_id + 1 {
                book.buffered_events.push(event.clone());
                return Err(Box::new(BinanceGapAlert {
                    gap_type: "buffered_sequence_gap".to_owned(),
                    symbol: String::new(),
                    detected_at_ms: 0,
                    expected_sequence_id: Some(last_update_id + 1),
                    observed_sequence_id: Some(event.first_update_id),
                    heal_action: "refetch_snapshot".to_owned(),
                    heal_status: "pending".to_owned(),
                }));
            }
        }
        if let Err(alert) = apply_depth_delta(book, &event) {
            book.reset_after_overflow();
            return Err(alert);
        }
    }
    Ok(())
}

pub(super) fn apply_depth_delta(
    book: &mut BinanceLocalOrderBook,
    event: &BinanceDiffDepthMessage,
) -> Result<(), Box<BinanceGapAlert>> {
    let bid_updates = parse_level_updates(&event.bids, "bid", event)?;
    let ask_updates = parse_level_updates(&event.asks, "ask", event)?;
    apply_parsed_level_updates(&mut book.bids, bid_updates);
    apply_parsed_level_updates(&mut book.asks, ask_updates);
    book.last_update_id = Some(event.final_update_id);
    Ok(())
}

fn apply_parsed_level_updates(
    book_side: &mut BTreeMap<FixedDecimal, FixedDecimal>,
    levels: Vec<(FixedDecimal, FixedDecimal)>,
) {
    for (price, quantity) in levels {
        if quantity.is_positive() {
            book_side.insert(price, quantity);
        } else {
            book_side.remove(&price);
        }
    }
}

fn parse_level_updates(
    levels: &[[String; 2]],
    side: &str,
    event: &BinanceDiffDepthMessage,
) -> Result<Vec<(FixedDecimal, FixedDecimal)>, Box<BinanceGapAlert>> {
    let mut parsed = Vec::with_capacity(levels.len());
    for [price_str, quantity_str] in levels {
        let price = FixedDecimal::parse_unsigned(price_str).map_err(|error| {
            depth_level_parse_alert(
                side,
                "price",
                price_str,
                error.to_string(),
                event.first_update_id,
            )
        })?;
        let quantity = FixedDecimal::parse_unsigned(quantity_str).map_err(|error| {
            depth_level_parse_alert(
                side,
                "quantity",
                quantity_str,
                error.to_string(),
                event.first_update_id,
            )
        })?;
        parsed.push((price, quantity));
    }
    Ok(parsed)
}

fn depth_level_parse_alert(
    side: &str,
    field: &str,
    value: &str,
    reason: String,
    observed_sequence_id: Sequence,
) -> Box<BinanceGapAlert> {
    Box::new(BinanceGapAlert {
        gap_type: "depth_level_parse_failed".to_owned(),
        symbol: String::new(),
        detected_at_ms: 0,
        expected_sequence_id: None,
        observed_sequence_id: Some(observed_sequence_id),
        heal_action: "refetch_snapshot".to_owned(),
        heal_status: format!("side={side} field={field} value={value} reason={reason}"),
    })
}

fn parse_snapshot_levels(
    levels: &[[String; 2]],
) -> Result<BTreeMap<FixedDecimal, FixedDecimal>, String> {
    let mut parsed = BTreeMap::new();
    for [price_str, quantity_str] in levels {
        let price = FixedDecimal::parse_unsigned(price_str)
            .map_err(|error| format!("invalid snapshot price {price_str}: {error}"))?;
        let quantity = FixedDecimal::parse_unsigned(quantity_str)
            .map_err(|error| format!("invalid snapshot quantity {quantity_str}: {error}"))?;
        if quantity.is_positive() {
            parsed.insert(price, quantity);
        }
    }
    Ok(parsed)
}

fn snapshot_parse_alert(reason: String) -> Box<BinanceGapAlert> {
    Box::new(BinanceGapAlert {
        gap_type: "snapshot_parse_failed".to_owned(),
        symbol: String::new(),
        detected_at_ms: 0,
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "refetch_snapshot".to_owned(),
        heal_status: reason,
    })
}
