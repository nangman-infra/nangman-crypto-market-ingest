use super::sequence::record_sequence_observation;
use crate::normalize::{stream_kind_from_stream, stream_symbol, unwrap_binance_payload};
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::BinanceStreamKind;
use crypto_domain::Sequence;
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) struct ParsedBinancePayload {
    pub(super) kind: BinanceStreamKind,
    pub(super) data: Value,
}

pub(super) fn observe_parsed_binance_payload(
    raw_payload: &str,
    last_sequence_by_stream: &mut BTreeMap<String, Sequence>,
    stats: &mut BinanceIngestWatchStats,
) -> Option<ParsedBinancePayload> {
    let Ok(value) = serde_json::from_str::<Value>(raw_payload) else {
        stats.malformed_messages += 1;
        return None;
    };
    let Ok(payload) = unwrap_binance_payload(value) else {
        stats.malformed_messages += 1;
        return None;
    };
    let Some(stream) = payload.stream.as_deref() else {
        stats.malformed_messages += 1;
        return None;
    };
    let Ok(kind) = stream_kind_from_stream(stream) else {
        stats.malformed_messages += 1;
        return None;
    };
    let Some(raw_symbol) = stream_symbol(stream) else {
        stats.malformed_messages += 1;
        return None;
    };

    stats.parsed_messages += 1;
    stats.increment_kind(kind);
    stats.increment_symbol(raw_symbol);
    record_sequence_observation(kind, &payload.data, stream, last_sequence_by_stream, stats);

    Some(ParsedBinancePayload {
        kind,
        data: payload.data,
    })
}
