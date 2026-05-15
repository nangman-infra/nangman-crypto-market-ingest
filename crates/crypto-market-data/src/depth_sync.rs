use crate::error::MarketDataError;
use crate::messages::{BinanceDiffDepthMessage, BinanceOrderBookSnapshot};
use crate::stats::BinanceIngestWatchStats;
use crypto_domain::{Sequence, TimestampMs};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};

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
    pub(crate) bids: HashMap<String, String>,
    pub(crate) asks: HashMap<String, String>,
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
    if let Some(last_update_id) = book.last_update_id {
        if event.final_update_id <= last_update_id {
            return Ok(());
        }
        if event.first_update_id > last_update_id + 1 {
            stats.record_gap_alert(BinanceGapAlert {
                gap_type: "sequence_gap".to_owned(),
                symbol: raw_symbol.clone(),
                detected_at_ms: received_time_ms,
                expected_sequence_id: Some(last_update_id + 1),
                observed_sequence_id: Some(event.first_update_id),
                heal_action: "refetch_snapshot".to_owned(),
                heal_status: "resync_requested".to_owned(),
            });
            book.reset_for_resync(event);
            snapshot_attempted.remove(&raw_symbol);
        } else {
            apply_depth_delta(book, &event);
        }
    } else {
        book.buffered_events.push(event);
    }

    if books.get(&raw_symbol).is_some_and(|book| !book.is_synced())
        && snapshot_attempted.insert(raw_symbol.clone())
    {
        stats.depth_snapshot_requests += 1;
        let snapshot = fetch_binance_depth_snapshot(http_client, depth_sync, &raw_symbol).await;
        match snapshot {
            Ok(snapshot) => {
                stats.depth_snapshot_successes += 1;
                let Some(book) = books.get_mut(&raw_symbol) else {
                    return Ok(());
                };
                match sync_depth_book_from_snapshot(book, snapshot) {
                    Ok(()) => {}
                    Err(alert) => {
                        let alert = *alert;
                        stats.record_gap_alert(BinanceGapAlert {
                            symbol: raw_symbol.clone(),
                            detected_at_ms: received_time_ms,
                            ..alert
                        });
                        snapshot_attempted.remove(&raw_symbol);
                    }
                }
            }
            Err(error) => {
                stats.depth_snapshot_failures += 1;
                stats.record_gap_alert(BinanceGapAlert {
                    gap_type: "snapshot_fetch_failed".to_owned(),
                    symbol: raw_symbol,
                    detected_at_ms: received_time_ms,
                    expected_sequence_id: None,
                    observed_sequence_id: None,
                    heal_action: "retry_snapshot".to_owned(),
                    heal_status: format!("failed: {error}"),
                });
            }
        }
    }
    Ok(())
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
    book.bids = snapshot
        .bids
        .into_iter()
        .map(|[price, quantity]| (price, quantity))
        .collect();
    book.asks = snapshot
        .asks
        .into_iter()
        .map(|[price, quantity]| (price, quantity))
        .collect();
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
        apply_depth_delta(book, &event);
    }
    Ok(())
}

fn apply_depth_delta(book: &mut BinanceLocalOrderBook, event: &BinanceDiffDepthMessage) {
    apply_depth_level_updates(&mut book.bids, &event.bids);
    apply_depth_level_updates(&mut book.asks, &event.asks);
    book.last_update_id = Some(event.final_update_id);
}

fn apply_depth_level_updates(book_side: &mut HashMap<String, String>, levels: &[[String; 2]]) {
    for level in levels {
        let [price, quantity] = level;
        if quantity
            .trim_start_matches('0')
            .trim_start_matches('.')
            .trim_end_matches('0')
            .is_empty()
            || quantity == "0"
            || quantity == "0.0"
            || quantity == "0.00000000"
        {
            book_side.remove(price);
        } else {
            book_side.insert(price.clone(), quantity.clone());
        }
    }
}
