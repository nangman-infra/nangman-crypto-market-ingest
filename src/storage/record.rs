use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize)]
pub struct RawMarketEventDraft {
    pub event_type: String,
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub sequence_id: String,
    pub sequence_tag: String,
    pub exchange_sequence: Option<i64>,
    pub diff_first_update_id: Option<i64>,
    pub diff_final_update_id: Option<i64>,
    pub is_snapshot: bool,
    pub stream_type: String,
    pub stream_phase: String,
    pub payload_json: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawMarketEventRecord {
    pub event_id: String,
    pub producer_run_id: String,
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub event_type: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub sequence_id: String,
    pub sequence_tag: String,
    pub exchange_sequence: Option<i64>,
    pub diff_first_update_id: Option<i64>,
    pub diff_final_update_id: Option<i64>,
    pub is_snapshot: bool,
    pub stream_type: String,
    pub stream_phase: String,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

impl RawMarketEventRecord {
    pub fn from_draft(draft: RawMarketEventDraft, producer_run_id: &str, ordinal: u64) -> Self {
        let payload_sha256 = sha256_hex(draft.payload_json.as_bytes());
        let event_id = format!(
            "evt_{}_{}_{}_{}",
            draft.venue, draft.event_type, draft.ingest_timestamp_ms, ordinal
        );
        let sequence_tag = if draft.sequence_tag.is_empty() {
            draft.sequence_id.clone()
        } else {
            draft.sequence_tag
        };
        Self {
            event_id,
            producer_run_id: producer_run_id.to_owned(),
            venue: draft.venue,
            source_role: draft.source_role,
            market_type: draft.market_type,
            event_type: draft.event_type,
            symbol_native: draft.symbol_native,
            symbol_canonical: draft.symbol_canonical,
            base_asset: draft.base_asset,
            quote_asset: draft.quote_asset,
            exchange_timestamp_ms: draft.exchange_timestamp_ms,
            ingest_timestamp_ms: draft.ingest_timestamp_ms,
            sequence_id: draft.sequence_id,
            sequence_tag,
            exchange_sequence: draft.exchange_sequence,
            diff_first_update_id: draft.diff_first_update_id,
            diff_final_update_id: draft.diff_final_update_id,
            is_snapshot: draft.is_snapshot,
            stream_type: draft.stream_type,
            stream_phase: draft.stream_phase,
            payload_json: draft.payload_json,
            payload_sha256,
            schema_version: "raw_market_event_v2".to_owned(),
        }
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_draft_uses_ordinal_for_unique_event_id() {
        let draft = RawMarketEventDraft {
            event_type: "trade".to_owned(),
            venue: "binance".to_owned(),
            source_role: "reference".to_owned(),
            market_type: "spot".to_owned(),
            symbol_native: "BTCUSDT".to_owned(),
            symbol_canonical: "BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
            exchange_timestamp_ms: 1,
            ingest_timestamp_ms: 2,
            sequence_id: "binance:trade:42".to_owned(),
            sequence_tag: "binance:trade:42".to_owned(),
            exchange_sequence: Some(42),
            diff_first_update_id: None,
            diff_final_update_id: None,
            is_snapshot: false,
            stream_type: "REALTIME".to_owned(),
            stream_phase: "realtime".to_owned(),
            payload_json: "{}".to_owned(),
        };

        let first = RawMarketEventRecord::from_draft(draft.clone(), "run-1", 1);
        let second = RawMarketEventRecord::from_draft(draft, "run-1", 2);

        assert_ne!(first.event_id, second.event_id);
        assert_eq!(first.schema_version, "raw_market_event_v2");
        assert_eq!(first.sequence_tag, "binance:trade:42");
    }
}
