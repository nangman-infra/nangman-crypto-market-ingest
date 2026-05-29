use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TradeNormalized {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub price: f64,
    pub quantity: f64,
    pub side: String,
    pub exchange_sequence: Option<i64>,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BookTickerNormalized {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub best_bid: f64,
    pub best_bid_qty: f64,
    pub best_ask: f64,
    pub best_ask_qty: f64,
    pub exchange_sequence: Option<i64>,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompactEventRef {
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub event_type: String,
    pub parent_event_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DerivativeMetricObservation {
    pub venue: String,
    pub source_role: String,
    pub market_type: String,
    pub metric_name: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub value: f64,
    pub unit: String,
    pub exchange_timestamp_ms: i64,
    pub ingest_timestamp_ms: i64,
    pub parent_event_id: String,
    pub parent_run_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SymbolHealthSnapshot {
    pub observed_at_ms: i64,
    pub last_event_time_ms: i64,
    pub last_received_time_ms: i64,
    pub latency_ms: i64,
    pub is_tradeable: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceHealthSnapshot {
    pub observed_at_ms: i64,
    pub connection_status: String,
    pub health_level: String,
    pub heartbeat_delay_ms: i64,
    pub stream_lag_ms: i64,
    pub recent_gap_count: i64,
    pub book_rebuild_count: i64,
}

#[derive(Debug, Clone)]
pub struct SliceRow {
    pub slice_id: String,
    pub venue: String,
    pub source_role: String,
    pub symbol_native: String,
    pub symbol_canonical: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub market_type: String,
    pub window_ms: i64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub slice_completeness: String,
    pub missing_reasons: Vec<String>,
    pub quality_ok: i64,
    pub quality_delayed: i64,
    pub quality_stale: i64,
    pub quality_gap: i64,
    pub quality_invalid: i64,
    pub trade_count: i64,
    pub trade_volume: f64,
    pub last_trade_price: Option<f64>,
    pub last_trade_size: Option<f64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid_price: Option<f64>,
    pub spread_bps: Option<f64>,
    pub book_ticker_count: i64,
    pub depth_event_count: i64,
    pub depth_book_rebuilt: bool,
    pub trade_events: Vec<TradeNormalized>,
    pub book_ticker_events: Vec<BookTickerNormalized>,
    pub depth_events: Vec<CompactEventRef>,
    pub ticker_events: Vec<CompactEventRef>,
    pub symbol_health_snapshot: Option<SymbolHealthSnapshot>,
    pub source_health_snapshot: Option<SourceHealthSnapshot>,
    pub parent_event_ids: Vec<String>,
    pub parent_run_ids: Vec<String>,
}
