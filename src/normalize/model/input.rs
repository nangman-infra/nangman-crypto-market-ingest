#[derive(Debug, Clone)]
pub struct RawInputEvent {
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
    pub exchange_sequence: Option<i64>,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct SymbolHealthInput {
    pub venue: String,
    pub symbol_native: String,
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct SourceHealthInput {
    pub venue: String,
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: i64,
    pub book_rebuild_count: i64,
    pub health_level: String,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct GapAlertInput {
    pub venue: String,
    pub symbol_native: String,
    pub gap_type: String,
    pub detected_at_ms: i64,
    pub payload_json: String,
    pub payload_sha256: String,
    pub schema_version: String,
}

#[derive(Debug, Clone)]
pub struct NormalizeInputs {
    pub raw_events: Vec<RawInputEvent>,
    pub symbol_health: Vec<SymbolHealthInput>,
    pub source_health: Vec<SourceHealthInput>,
    pub gap_alerts: Vec<GapAlertInput>,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
}
