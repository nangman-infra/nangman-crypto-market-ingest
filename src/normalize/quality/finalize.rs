use super::super::model::SliceRow;

pub(crate) fn finalize_slices(rows: impl Iterator<Item = SliceRow>) -> Vec<SliceRow> {
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
