use super::args::InputRange;
use super::model::{
    GapAlertInput, SliceRow, SourceHealthInput, SourceHealthSnapshot, SymbolHealthInput,
    SymbolHealthSnapshot,
};

pub fn apply_health_and_gaps<'a>(
    symbol_health: &[SymbolHealthInput],
    source_health: &[SourceHealthInput],
    gap_alerts: &[GapAlertInput],
    rows: impl Iterator<Item = &'a mut SliceRow>,
    input_range: InputRange,
) {
    for row in rows {
        row.symbol_health_snapshot = latest_symbol_health(symbol_health, row);
        row.source_health_snapshot = latest_source_health(source_health, row);
        if row.symbol_health_snapshot.is_none() {
            push_missing(row, "symbol_health_missing");
        }
        if row.source_health_snapshot.is_none() {
            push_missing(row, "source_health_missing");
        }
        if let Some(snapshot) = &row.symbol_health_snapshot {
            let is_tradeable = snapshot.is_tradeable;
            let is_stale = snapshot.last_received_time_ms < row.window_start_ms;
            if !is_tradeable {
                push_missing(row, "venue_unavailable");
            }
            if is_stale {
                row.quality_stale += 1;
                push_missing(row, "stale");
            }
        }
        if let Some(snapshot) = &row.source_health_snapshot
            && (snapshot.connection_status != "connected"
                || !is_healthy_level(&snapshot.health_level))
        {
            push_missing(row, "source_stale");
        }
        for gap in gap_alerts {
            if gap.venue == row.venue
                && gap.symbol_native == row.symbol_native
                && gap.detected_at_ms >= row.window_start_ms
                && gap.detected_at_ms < row.window_end_ms
                && gap.detected_at_ms >= input_range.start_ms
                && gap.detected_at_ms < input_range.end_ms
            {
                row.quality_gap += 1;
                push_missing(row, &format!("gap_alert.{}", gap.gap_type));
            }
        }
    }
}

pub fn finalize_slices(rows: impl Iterator<Item = SliceRow>) -> Vec<SliceRow> {
    rows.map(|mut row| {
        normalize_event_order(&mut row);
        if row.trade_count == 0
            && row.book_ticker_count == 0
            && row.depth_event_count == 0
            && row.ticker_events.is_empty()
        {
            if row.quality_invalid > 0 {
                row.missing_reasons.push("all_invalid".to_owned());
            } else {
                row.missing_reasons.push("trade_silence".to_owned());
            }
        }
        if row.best_bid.is_none() && row.best_ask.is_none() {
            row.missing_reasons.push("depth_top_missing".to_owned());
        }
        row.parent_event_ids.sort();
        row.parent_event_ids.dedup();
        row.parent_run_ids.sort();
        row.parent_run_ids.dedup();
        row.missing_reasons.sort();
        row.missing_reasons.dedup();
        if let (Some(bid), Some(ask)) = (row.best_bid, row.best_ask) {
            row.mid_price = Some((bid + ask) / 2.0);
            row.spread_bps = row
                .mid_price
                .and_then(|mid| (mid > 0.0).then(|| (ask - bid) / mid * 10_000.0));
        }
        row.slice_completeness = completeness(&row);
        row
    })
    .collect()
}

fn normalize_event_order(row: &mut SliceRow) {
    row.trade_events.sort_by(|left, right| {
        (
            left.exchange_timestamp_ms,
            left.exchange_sequence.unwrap_or(i64::MAX),
            left.ingest_timestamp_ms,
            &left.parent_event_id,
        )
            .cmp(&(
                right.exchange_timestamp_ms,
                right.exchange_sequence.unwrap_or(i64::MAX),
                right.ingest_timestamp_ms,
                &right.parent_event_id,
            ))
    });
    if let Some(last_trade) = row.trade_events.last() {
        row.last_trade_price = Some(last_trade.price);
        row.last_trade_size = Some(last_trade.quantity);
    }

    row.book_ticker_events.sort_by(|left, right| {
        (
            left.exchange_timestamp_ms,
            left.exchange_sequence.unwrap_or(i64::MAX),
            left.ingest_timestamp_ms,
            &left.parent_event_id,
        )
            .cmp(&(
                right.exchange_timestamp_ms,
                right.exchange_sequence.unwrap_or(i64::MAX),
                right.ingest_timestamp_ms,
                &right.parent_event_id,
            ))
    });
    if let Some(last_book) = row.book_ticker_events.last() {
        row.best_bid = Some(last_book.best_bid);
        row.best_ask = Some(last_book.best_ask);
    }

    row.depth_events.sort_by(|left, right| {
        (
            left.exchange_timestamp_ms,
            left.ingest_timestamp_ms,
            &left.parent_event_id,
        )
            .cmp(&(
                right.exchange_timestamp_ms,
                right.ingest_timestamp_ms,
                &right.parent_event_id,
            ))
    });
    row.ticker_events.sort_by(|left, right| {
        (
            left.exchange_timestamp_ms,
            left.ingest_timestamp_ms,
            &left.parent_event_id,
        )
            .cmp(&(
                right.exchange_timestamp_ms,
                right.ingest_timestamp_ms,
                &right.parent_event_id,
            ))
    });
}

fn latest_symbol_health(
    rows: &[SymbolHealthInput],
    slice: &SliceRow,
) -> Option<SymbolHealthSnapshot> {
    rows.iter()
        .filter(|row| {
            row.venue == slice.venue
                && row.symbol_native == slice.symbol_native
                && row.observed_at_ms <= slice.window_end_ms
        })
        .max_by_key(|row| row.observed_at_ms)
        .map(|row| SymbolHealthSnapshot {
            observed_at_ms: row.observed_at_ms,
            last_event_time_ms: row.last_event_time_ms,
            last_received_time_ms: row.last_event_time_ms.saturating_add(row.latency_ms),
            latency_ms: row.latency_ms,
            is_tradeable: row.is_tradeable,
            reason_codes: row
                .reason_codes
                .split(';')
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
        })
}

fn latest_source_health(
    rows: &[SourceHealthInput],
    slice: &SliceRow,
) -> Option<SourceHealthSnapshot> {
    rows.iter()
        .filter(|row| row.venue == slice.venue && row.observed_at_ms <= slice.window_end_ms)
        .max_by_key(|row| row.observed_at_ms)
        .map(|row| SourceHealthSnapshot {
            observed_at_ms: row.observed_at_ms,
            connection_status: row.connection_status.clone(),
            health_level: row.health_level.clone(),
            heartbeat_delay_ms: row.heartbeat_delay_ms,
            stream_lag_ms: row.stream_lag_ms,
            recent_gap_count: row.recent_gap_count,
            book_rebuild_count: row.book_rebuild_count,
        })
}

fn completeness(row: &SliceRow) -> String {
    if row
        .missing_reasons
        .iter()
        .any(|value| value == "venue_unavailable")
    {
        return "incomplete".to_owned();
    }
    if row.trade_count == 0
        && row.book_ticker_count == 0
        && row.depth_event_count == 0
        && row.ticker_events.is_empty()
    {
        return "incomplete".to_owned();
    }
    if row.quality_invalid > 0 && row.quality_ok == 0 {
        return "incomplete".to_owned();
    }
    if !row.missing_reasons.is_empty()
        || row.quality_delayed > 0
        || row.quality_stale > 0
        || row.quality_gap > 0
        || row.quality_invalid > 0
    {
        return "partial".to_owned();
    }
    if row.source_role == "reference" {
        "reference_only".to_owned()
    } else {
        "complete".to_owned()
    }
}

fn push_missing(row: &mut SliceRow, reason: &str) {
    row.missing_reasons.push(reason.to_owned());
}

fn is_healthy_level(value: &str) -> bool {
    matches!(value, "ok" | "healthy" | "nominal")
}

#[cfg(test)]
mod tests {
    use super::super::model::{BookTickerNormalized, TradeNormalized};
    use super::*;

    #[test]
    fn finalizes_depth_top_missing_when_top_book_absent() {
        let finalized = finalize_slices(vec![slice_row()].into_iter());
        let row = finalized.first().unwrap();
        assert!(
            row.missing_reasons
                .iter()
                .any(|reason| reason == "depth_top_missing")
        );
    }

    #[test]
    fn prefixes_gap_alert_missing_reason() {
        let mut row = slice_row();
        let gaps = vec![GapAlertInput {
            venue: "binance".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            gap_type: "ordering_violation".to_owned(),
            detected_at_ms: 1_000,
            payload_json: "{}".to_owned(),
            payload_sha256: "unused".to_owned(),
            schema_version: "gap_alert_v1".to_owned(),
        }];

        apply_health_and_gaps(
            &[],
            &[],
            &gaps,
            std::iter::once(&mut row),
            InputRange {
                start_ms: 1_000,
                end_ms: 2_000,
            },
        );

        assert_eq!(row.quality_gap, 1);
        assert!(
            row.missing_reasons
                .iter()
                .any(|reason| reason == "gap_alert.ordering_violation")
        );
    }

    #[test]
    fn finalizes_event_order_before_last_value_fields() {
        let mut row = slice_row();
        row.trade_count = 2;
        row.trade_volume = 3.0;
        row.trade_events = vec![
            trade("late", 2_000, 12.0, 2.0),
            trade("early", 1_000, 10.0, 1.0),
        ];
        row.book_ticker_count = 2;
        row.book_ticker_events = vec![
            book("late-book", 2_000, 99.0, 101.0),
            book("early-book", 1_000, 90.0, 110.0),
        ];

        let finalized = finalize_slices(vec![row].into_iter());
        let row = finalized.first().unwrap();

        assert_eq!(row.trade_events[0].parent_event_id, "early");
        assert_eq!(row.trade_events[1].parent_event_id, "late");
        assert_eq!(row.last_trade_price, Some(12.0));
        assert_eq!(row.last_trade_size, Some(2.0));
        assert_eq!(row.book_ticker_events[0].parent_event_id, "early-book");
        assert_eq!(row.book_ticker_events[1].parent_event_id, "late-book");
        assert_eq!(row.best_bid, Some(99.0));
        assert_eq!(row.best_ask, Some(101.0));
    }

    fn slice_row() -> SliceRow {
        SliceRow {
            slice_id: "slice-1".to_owned(),
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            symbol_canonical: "BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
            market_type: "spot".to_owned(),
            window_ms: 1_000,
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            slice_completeness: String::new(),
            missing_reasons: Vec::new(),
            quality_ok: 0,
            quality_delayed: 0,
            quality_stale: 0,
            quality_gap: 0,
            quality_invalid: 0,
            trade_count: 0,
            trade_volume: 0.0,
            last_trade_price: None,
            last_trade_size: None,
            best_bid: None,
            best_ask: None,
            mid_price: None,
            spread_bps: None,
            book_ticker_count: 0,
            depth_event_count: 0,
            depth_book_rebuilt: false,
            trade_events: Vec::new(),
            book_ticker_events: Vec::new(),
            depth_events: Vec::new(),
            ticker_events: Vec::new(),
            symbol_health_snapshot: None,
            source_health_snapshot: None,
            parent_event_ids: Vec::new(),
            parent_run_ids: Vec::new(),
        }
    }

    fn trade(id: &str, timestamp_ms: i64, price: f64, quantity: f64) -> TradeNormalized {
        TradeNormalized {
            exchange_timestamp_ms: timestamp_ms,
            ingest_timestamp_ms: timestamp_ms,
            price,
            quantity,
            side: "unknown".to_owned(),
            exchange_sequence: None,
            parent_event_id: id.to_owned(),
        }
    }

    fn book(id: &str, timestamp_ms: i64, best_bid: f64, best_ask: f64) -> BookTickerNormalized {
        BookTickerNormalized {
            exchange_timestamp_ms: timestamp_ms,
            ingest_timestamp_ms: timestamp_ms,
            best_bid,
            best_bid_qty: 1.0,
            best_ask,
            best_ask_qty: 1.0,
            exchange_sequence: None,
            parent_event_id: id.to_owned(),
        }
    }
}
