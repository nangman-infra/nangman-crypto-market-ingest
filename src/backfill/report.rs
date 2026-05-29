use crate::storage::StorageReport;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SymbolBackfillReport {
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub record_count: u64,
    pub first_event_time_ms: Option<i64>,
    pub last_event_time_ms: Option<i64>,
    pub gap_alert_count: u64,
    pub status: String,
}

impl SymbolBackfillReport {
    pub(in crate::backfill) fn empty(
        symbol_native: impl Into<String>,
        symbol_canonical: impl Into<String>,
    ) -> Self {
        Self {
            symbol_native: symbol_native.into(),
            symbol_canonical: symbol_canonical.into(),
            record_count: 0,
            first_event_time_ms: None,
            last_event_time_ms: None,
            gap_alert_count: 0,
            status: "empty".to_owned(),
        }
    }

    pub(in crate::backfill) fn observe(&mut self, exchange_timestamp_ms: i64) {
        self.record_count += 1;
        self.first_event_time_ms = Some(
            self.first_event_time_ms
                .map(|current| current.min(exchange_timestamp_ms))
                .unwrap_or(exchange_timestamp_ms),
        );
        self.last_event_time_ms = Some(
            self.last_event_time_ms
                .map(|current| current.max(exchange_timestamp_ms))
                .unwrap_or(exchange_timestamp_ms),
        );
        self.status = "ok".to_owned();
    }

    pub(in crate::backfill) fn note_gap(&mut self) {
        self.gap_alert_count += 1;
        if self.record_count > 0 {
            self.status = "partial".to_owned();
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BackfillRunReport {
    pub venue: String,
    pub source_role: String,
    pub input_start_ms: i64,
    pub input_end_ms: i64,
    pub requested_symbol_count: usize,
    pub processed_symbol_count: usize,
    pub total_record_count: u64,
    pub total_gap_alert_count: u64,
    pub symbols: Vec<SymbolBackfillReport>,
    pub storage: StorageReport,
}

pub(crate) fn empty_storage_report() -> StorageReport {
    StorageReport {
        bucket: String::new(),
        run_id: String::new(),
        record_count: 0,
        uploaded_object_count: 0,
        uploaded_object_retained_count: 0,
        uploaded_object_dropped_count: 0,
        uploaded_objects: Vec::new(),
        failed_upload_count: 0,
        failed_upload_retained_count: 0,
        failed_upload_dropped_count: 0,
        failed_uploads: Vec::new(),
        manifest_key: None,
    }
}
