use crate::error::MarketDataError;
use crate::messages::{BinanceDiffDepthMessage, BinanceOrderBookSnapshot};
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::{FixedDecimal, Sequence, TimestampMs};
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};

// Upper bound on events held while waiting for a snapshot to arrive.
// 50 symbols * ~10 depth msgs/sec * 200s safety window ~= 100k worst case
// across symbols; per-book 1_000 protects a single symbol whose snapshot
// fetch is repeatedly failing without starving the rest.
pub(crate) const MAX_BUFFERED_EVENTS: usize = 1_000;

#[derive(Debug, Clone, Serialize)]
pub struct BinanceDepthSyncSettings {
    pub rest_base_url: String,
    pub snapshot_limit: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinanceGapAlert {
    pub gap_type: String,
    pub symbol: String,
    pub detected_at_ms: TimestampMs,
    pub expected_sequence_id: Option<Sequence>,
    pub observed_sequence_id: Option<Sequence>,
    pub heal_action: String,
    pub heal_status: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct BinanceLocalOrderBook {
    pub(crate) last_update_id: Option<Sequence>,
    /// Bid levels keyed by `FixedDecimal` price so iteration is price-sorted
    /// and zero-quantity entries can be detected via `FixedDecimal::is_positive`.
    pub(crate) bids: BTreeMap<FixedDecimal, FixedDecimal>,
    pub(crate) asks: BTreeMap<FixedDecimal, FixedDecimal>,
    pub(crate) buffered_events: Vec<BinanceDiffDepthMessage>,
}

impl BinanceLocalOrderBook {
    pub(crate) fn is_synced(&self) -> bool {
        self.last_update_id.is_some()
    }

    pub(crate) fn reset_for_resync(&mut self, event: BinanceDiffDepthMessage) {
        self.last_update_id = None;
        self.bids.clear();
        self.asks.clear();
        self.buffered_events.clear();
        self.buffered_events.push(event);
    }

    pub(crate) fn reset_after_overflow(&mut self) {
        self.last_update_id = None;
        self.bids.clear();
        self.asks.clear();
        self.buffered_events.clear();
    }

    pub(crate) fn buffered_at_capacity(&self) -> bool {
        self.buffered_events.len() >= MAX_BUFFERED_EVENTS
    }
}

pub(crate) async fn handle_diff_depth_event(
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    event: BinanceDiffDepthMessage,
    received_time_ms: TimestampMs,
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) -> Result<(), MarketDataError> {
    let raw_symbol = event.symbol.to_ascii_uppercase();
    let book = books.entry(raw_symbol.clone()).or_default();
    if book.is_synced() {
        handle_synced_depth_event(
            book,
            event,
            &raw_symbol,
            received_time_ms,
            snapshot_attempted,
            stats,
        );
    } else {
        buffer_unsynced_depth_event(
            book,
            event,
            &raw_symbol,
            received_time_ms,
            snapshot_attempted,
            stats,
        );
    }

    if should_fetch_snapshot(books, snapshot_attempted, &raw_symbol) {
        fetch_and_sync_snapshot(
            depth_sync,
            http_client,
            books,
            snapshot_attempted,
            stats,
            &raw_symbol,
            received_time_ms,
        )
        .await?;
    }
    Ok(())
}

fn handle_synced_depth_event(
    book: &mut BinanceLocalOrderBook,
    event: BinanceDiffDepthMessage,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) {
    let Some(last_update_id) = book.last_update_id else {
        return;
    };
    if event.final_update_id <= last_update_id {
        return;
    }
    if event.first_update_id > last_update_id + 1 {
        record_sequence_gap(
            stats,
            raw_symbol,
            received_time_ms,
            last_update_id + 1,
            event.first_update_id,
        );
        book.reset_for_resync(event);
        snapshot_attempted.remove(raw_symbol);
        return;
    }
    if let Err(alert) = apply_depth_delta(book, &event) {
        record_delta_parse_gap(stats, raw_symbol, received_time_ms, *alert);
        book.reset_after_overflow();
        snapshot_attempted.remove(raw_symbol);
    }
}

fn buffer_unsynced_depth_event(
    book: &mut BinanceLocalOrderBook,
    event: BinanceDiffDepthMessage,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
) {
    if book.buffered_at_capacity() {
        let dropped_count = book.buffered_events.len();
        book.reset_after_overflow();
        snapshot_attempted.remove(raw_symbol);
        stats.buffer_overflow_count += 1;
        stats.record_gap_alert(BinanceGapAlert {
            gap_type: "buffered_overflow".to_owned(),
            symbol: raw_symbol.to_owned(),
            detected_at_ms: received_time_ms,
            expected_sequence_id: None,
            observed_sequence_id: Some(event.first_update_id),
            heal_action: "refetch_snapshot".to_owned(),
            heal_status: format!("dropped_count={dropped_count}"),
        });
    }
    book.buffered_events.push(event);
}

fn should_fetch_snapshot(
    books: &BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    raw_symbol: &str,
) -> bool {
    books.get(raw_symbol).is_some_and(|book| !book.is_synced())
        && snapshot_attempted.insert(raw_symbol.to_owned())
}

async fn fetch_and_sync_snapshot(
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
) -> Result<(), MarketDataError> {
    stats.depth_snapshot_requests += 1;
    match fetch_binance_depth_snapshot(http_client, depth_sync, raw_symbol).await {
        Ok(snapshot) => sync_fetched_snapshot(
            books,
            snapshot_attempted,
            stats,
            raw_symbol,
            received_time_ms,
            snapshot,
        ),
        Err(error) => record_snapshot_fetch_failure(stats, raw_symbol, received_time_ms, error),
    }
    Ok(())
}

fn sync_fetched_snapshot(
    books: &mut BTreeMap<String, BinanceLocalOrderBook>,
    snapshot_attempted: &mut HashSet<String>,
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    snapshot: BinanceOrderBookSnapshot,
) {
    stats.depth_snapshot_successes += 1;
    let Some(book) = books.get_mut(raw_symbol) else {
        return;
    };
    if let Err(alert) = sync_depth_book_from_snapshot(book, snapshot) {
        record_delta_parse_gap(stats, raw_symbol, received_time_ms, *alert);
        snapshot_attempted.remove(raw_symbol);
    }
}

fn record_sequence_gap(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    expected_sequence_id: Sequence,
    observed_sequence_id: Sequence,
) {
    stats.record_gap_alert(BinanceGapAlert {
        gap_type: "sequence_gap".to_owned(),
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        expected_sequence_id: Some(expected_sequence_id),
        observed_sequence_id: Some(observed_sequence_id),
        heal_action: "refetch_snapshot".to_owned(),
        heal_status: "resync_requested".to_owned(),
    });
}

fn record_delta_parse_gap(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    alert: BinanceGapAlert,
) {
    stats.record_gap_alert(BinanceGapAlert {
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        ..alert
    });
}

fn record_snapshot_fetch_failure(
    stats: &mut BinanceIngestWatchStats,
    raw_symbol: &str,
    received_time_ms: TimestampMs,
    error: MarketDataError,
) {
    stats.depth_snapshot_failures += 1;
    stats.record_gap_alert(BinanceGapAlert {
        gap_type: "snapshot_fetch_failed".to_owned(),
        symbol: raw_symbol.to_owned(),
        detected_at_ms: received_time_ms,
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "retry_snapshot".to_owned(),
        heal_status: format!("failed: {error}"),
    });
}

async fn fetch_binance_depth_snapshot(
    client: &reqwest::Client,
    depth_sync: &BinanceDepthSyncSettings,
    raw_symbol: &str,
) -> Result<BinanceOrderBookSnapshot, MarketDataError> {
    let url = format!(
        "{}/api/v3/depth?symbol={raw_symbol}&limit={}",
        depth_sync.rest_base_url.trim_end_matches('/'),
        depth_sync.snapshot_limit
    );
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<BinanceOrderBookSnapshot>()
        .await?)
}

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

fn apply_depth_delta(
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
