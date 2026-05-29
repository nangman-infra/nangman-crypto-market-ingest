use crate::normalize::sequence_from_binance_payload;
use crate::stats::BinanceIngestWatchStats;
use crate::stream_config::BinanceStreamKind;
use crypto_domain::Sequence;
use serde_json::Value;
use std::collections::BTreeMap;

pub(super) fn record_sequence_observation(
    kind: BinanceStreamKind,
    payload: &Value,
    stream: &str,
    last_sequence_by_stream: &mut BTreeMap<String, Sequence>,
    stats: &mut BinanceIngestWatchStats,
) {
    if let Some(sequence) = sequence_from_binance_payload(kind, payload)
        && last_sequence_by_stream
            .insert(stream.to_owned(), sequence)
            .is_some_and(|previous| sequence <= previous)
    {
        stats.sequence_anomalies += 1;
    }
}
