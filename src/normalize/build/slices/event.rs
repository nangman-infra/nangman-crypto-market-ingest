use std::collections::BTreeMap;

use crate::normalize::args::{InputRange, NormalizeArgs};
use crate::normalize::model::{RawInputEvent, SliceRow};
use crate::normalize::payload::{compact_ref, parse_book_ticker, parse_trade};
use crate::storage::record::sha256_hex;

use super::identity::SliceKey;
use super::stats::BuildStats;

pub(in crate::normalize::build) fn is_derivative_market_event(event: &RawInputEvent) -> bool {
    matches!(
        (event.venue.as_str(), event.event_type.as_str()),
        (
            "binance",
            "funding_rate_snapshot" | "open_interest_snapshot"
        )
    )
}

pub(in crate::normalize::build) fn apply_event(
    args: &NormalizeArgs,
    input_range: InputRange,
    event: &RawInputEvent,
    rows: &mut BTreeMap<SliceKey, SliceRow>,
    stats: &mut BuildStats,
) {
    if event.exchange_timestamp_ms < input_range.start_ms
        || event.exchange_timestamp_ms >= input_range.end_ms
    {
        return;
    }
    let window_start_ms = event.exchange_timestamp_ms.div_euclid(args.window_ms) * args.window_ms;
    let key = SliceKey {
        venue: event.venue.clone(),
        symbol_canonical: event.symbol_canonical.clone(),
        window_start_ms,
    };
    let Some(row) = rows.get_mut(&key) else {
        return;
    };

    match event.event_type.as_str() {
        "trade" => apply_trade_event(row, args, event, stats),
        "book_ticker" | "depth_snapshot" => apply_book_event(row, args, event, stats),
        "depth_delta" => apply_compact_depth_event(row, args, event),
        "ticker" => apply_ticker_event(row, args, event),
        _ => mark_invalid(row, stats, "unknown_event_type"),
    }
}

pub(in crate::normalize::build) fn payload_hash(payload: &str) -> String {
    sha256_hex(payload.as_bytes())
}

fn apply_trade_event(
    row: &mut SliceRow,
    args: &NormalizeArgs,
    event: &RawInputEvent,
    stats: &mut BuildStats,
) {
    match parse_trade(event) {
        Some(trade) => {
            row.trade_count += 1;
            row.trade_volume += trade.quantity;
            row.last_trade_price = Some(trade.price);
            row.last_trade_size = Some(trade.quantity);
            row.trade_events.push(trade);
            mark_event_quality(row, args, event);
            push_parent(row, event);
        }
        None => mark_invalid(row, stats, "parse_trade_failed"),
    }
}

fn apply_book_event(
    row: &mut SliceRow,
    args: &NormalizeArgs,
    event: &RawInputEvent,
    stats: &mut BuildStats,
) {
    if let Some(book) = parse_book_ticker(event) {
        row.book_ticker_count += 1;
        row.best_bid = Some(book.best_bid);
        row.best_ask = Some(book.best_ask);
        row.book_ticker_events.push(book);
        mark_event_quality(row, args, event);
        push_parent(row, event);
    } else if event.event_type == "book_ticker" {
        mark_invalid(row, stats, "parse_book_ticker_failed");
    }
    if event.event_type == "depth_snapshot" {
        row.depth_event_count += 1;
        row.depth_events.push(compact_ref(event));
        push_parent(row, event);
    }
}

fn apply_compact_depth_event(row: &mut SliceRow, args: &NormalizeArgs, event: &RawInputEvent) {
    row.depth_event_count += 1;
    row.depth_events.push(compact_ref(event));
    mark_event_quality(row, args, event);
    push_parent(row, event);
}

fn apply_ticker_event(row: &mut SliceRow, args: &NormalizeArgs, event: &RawInputEvent) {
    row.ticker_events.push(compact_ref(event));
    mark_event_quality(row, args, event);
    push_parent(row, event);
}

fn push_parent(row: &mut SliceRow, event: &RawInputEvent) {
    row.parent_event_ids.push(event.event_id.clone());
    row.parent_run_ids.push(event.producer_run_id.clone());
}

fn mark_event_quality(row: &mut SliceRow, args: &NormalizeArgs, event: &RawInputEvent) {
    if event
        .ingest_timestamp_ms
        .saturating_sub(event.exchange_timestamp_ms)
        > args.max_latency_ms
    {
        row.quality_delayed += 1;
    } else {
        row.quality_ok += 1;
    }
}

fn mark_invalid(row: &mut SliceRow, stats: &mut BuildStats, reason: &str) {
    stats.invalid_event_count += 1;
    row.quality_invalid += 1;
    push_missing(row, reason);
}

fn push_missing(row: &mut SliceRow, reason: &str) {
    row.missing_reasons.push(reason.to_owned());
}
