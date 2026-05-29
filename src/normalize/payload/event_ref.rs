use crate::normalize::model::{CompactEventRef, RawInputEvent};

pub fn compact_ref(event: &RawInputEvent) -> CompactEventRef {
    CompactEventRef {
        exchange_timestamp_ms: event.exchange_timestamp_ms,
        ingest_timestamp_ms: event.ingest_timestamp_ms,
        event_type: event.event_type.clone(),
        parent_event_id: event.event_id.clone(),
    }
}
