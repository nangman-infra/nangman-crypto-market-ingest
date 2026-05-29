use crate::depth_sync::{BinanceDepthSyncSettings, BinanceLocalOrderBook, handle_diff_depth_event};
use crate::error::MarketDataError;
use crate::messages::BinanceDiffDepthMessage;
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::{BinanceStreamConfig, BinanceStreamKind};
use crypto_domain::{Sequence, TimestampMs, TraceId};
use std::collections::{BTreeMap, HashSet};

mod normalization;
mod parsed;
mod sequence;

use normalization::record_normalization_outcome;
use parsed::observe_parsed_binance_payload;

pub(crate) fn observe_binance_ingest_payload(
    config: &BinanceStreamConfig,
    raw_payload: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
    last_sequence_by_stream: &mut BTreeMap<String, Sequence>,
    stats: &mut BinanceIngestWatchStats,
) {
    stats.received_messages += 1;
    let Some(observation) =
        observe_parsed_binance_payload(raw_payload, last_sequence_by_stream, stats)
    else {
        return;
    };

    match observation.kind {
        BinanceStreamKind::Ticker
        | BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => record_normalization_outcome(
            config,
            raw_payload,
            received_time_ms,
            decision_trace_id,
            stats,
        ),
        BinanceStreamKind::Trade
        | BinanceStreamKind::BookTicker
        | BinanceStreamKind::DiffDepth100ms => {}
    }
}

pub(super) struct DepthObserveState<'a> {
    pub(super) last_sequence_by_stream: &'a mut BTreeMap<String, Sequence>,
    pub(super) books: &'a mut BTreeMap<String, BinanceLocalOrderBook>,
    pub(super) snapshot_attempted: &'a mut HashSet<String>,
    pub(super) stats: &'a mut BinanceIngestWatchStats,
}

pub(super) async fn observe_binance_ingest_payload_with_depth_sync(
    config: &BinanceStreamConfig,
    depth_sync: &BinanceDepthSyncSettings,
    http_client: &reqwest::Client,
    raw_payload: &str,
    received_time_ms: TimestampMs,
    decision_trace_id: TraceId,
    state: DepthObserveState<'_>,
) -> Result<(), MarketDataError> {
    let DepthObserveState {
        last_sequence_by_stream,
        books,
        snapshot_attempted,
        stats,
    } = state;

    stats.received_messages += 1;
    let Some(observation) =
        observe_parsed_binance_payload(raw_payload, last_sequence_by_stream, stats)
    else {
        return Ok(());
    };

    match observation.kind {
        BinanceStreamKind::DiffDepth100ms => {
            stats.depth_delta_messages += 1;
            let event: BinanceDiffDepthMessage = serde_json::from_value(observation.data)?;
            handle_diff_depth_event(
                depth_sync,
                http_client,
                event,
                received_time_ms,
                books,
                snapshot_attempted,
                stats,
            )
            .await?;
        }
        BinanceStreamKind::Ticker
        | BinanceStreamKind::PartialDepth5
        | BinanceStreamKind::PartialDepth10
        | BinanceStreamKind::PartialDepth20 => record_normalization_outcome(
            config,
            raw_payload,
            received_time_ms,
            decision_trace_id,
            stats,
        ),
        BinanceStreamKind::Trade | BinanceStreamKind::BookTicker => {}
    }
    Ok(())
}
