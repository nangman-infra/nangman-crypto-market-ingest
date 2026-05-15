use super::args::{InputRange, NormalizeArgs};
use super::model::{
    DerivativeMetricObservation, NormalizeInputs, RawInputEvent, SLICE_SCHEMA_VERSION, SliceRow,
    SourceHealthInput, SymbolHealthInput,
};
use super::payload::{compact_ref, parse_book_ticker, parse_derivative_metric, parse_trade};
use super::quality::{apply_health_and_gaps, finalize_slices};
use crate::storage::record::sha256_hex;
use std::collections::{BTreeMap, BTreeSet};

pub struct BuildResult {
    pub slices: Vec<SliceRow>,
    pub projection_slices: Vec<SliceRow>,
    pub projection_derivative_metrics: Vec<DerivativeMetricObservation>,
    pub input_object_keys: Vec<String>,
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_record_count: usize,
    pub duplicate_event_count: usize,
    pub invalid_event_count: usize,
    pub payload_hash_mismatch_count: usize,
    pub input_schema_versions: Vec<String>,
    pub status: String,
    pub failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BuildInputMetadata {
    pub run_mode: String,
    pub fallback_alert: bool,
    pub input_local_object_count: usize,
    pub input_s3_object_count: usize,
    pub input_object_keys: Vec<String>,
}

pub struct BuildAccumulator {
    input_range: InputRange,
    scan_range: InputRange,
    projection_range: InputRange,
    input_identities: BTreeMap<IdentityKey, Identity>,
    projection_identities: BTreeMap<IdentityKey, Identity>,
    rows: BTreeMap<SliceKey, SliceRow>,
    projection_rows: BTreeMap<SliceKey, SliceRow>,
    projection_derivative_metrics: Vec<DerivativeMetricObservation>,
    symbol_health: Vec<SymbolHealthInput>,
    source_health: Vec<SourceHealthInput>,
    gap_alerts: Vec<super::model::GapAlertInput>,
    stats: BuildStats,
    projection_stats: BuildStats,
    schema_versions: BTreeSet<String>,
    seen_event_ids: BTreeSet<String>,
    metadata: BuildInputMetadata,
}

impl BuildAccumulator {
    pub fn new(
        args: &NormalizeArgs,
        input_range: InputRange,
        scan_range: InputRange,
        metadata: BuildInputMetadata,
    ) -> Self {
        let projection_range = InputRange {
            start_ms: input_range
                .start_ms
                .saturating_sub(args.projection_lookback_ms),
            end_ms: input_range.end_ms,
        };
        Self {
            input_range,
            scan_range,
            projection_range,
            input_identities: BTreeMap::new(),
            projection_identities: BTreeMap::new(),
            rows: BTreeMap::new(),
            projection_rows: BTreeMap::new(),
            projection_derivative_metrics: Vec::new(),
            symbol_health: Vec::new(),
            source_health: Vec::new(),
            gap_alerts: Vec::new(),
            stats: BuildStats::default(),
            projection_stats: BuildStats::default(),
            schema_versions: BTreeSet::new(),
            seen_event_ids: BTreeSet::new(),
            metadata,
        }
    }

    pub fn ingest_raw_event(&mut self, args: &NormalizeArgs, event: RawInputEvent) {
        self.stats.input_record_count += 1;
        self.schema_versions.insert(event.schema_version.clone());
        if event.schema_version != "raw_market_event_v2" {
            self.stats.invalid_event_count += 1;
            return;
        }
        if payload_hash(&event.payload_json) != event.payload_sha256 {
            self.stats.payload_hash_mismatch_count += 1;
            self.stats.invalid_event_count += 1;
            return;
        }
        if event.exchange_timestamp_ms <= 0
            || event.exchange_timestamp_ms
                >= self
                    .scan_range
                    .end_ms
                    .saturating_add(args.clock_skew_margin_ms)
        {
            self.stats.invalid_event_count += 1;
            return;
        }
        if !self.seen_event_ids.insert(event.event_id.clone()) {
            self.stats.duplicate_event_count += 1;
            return;
        }
        if is_derivative_market_event(&event) {
            self.ingest_derivative_event(event);
            return;
        }
        self.ingest_spot_event(args, &event);
    }

    pub fn ingest_symbol_health(&mut self, row: SymbolHealthInput) {
        self.stats.input_record_count += 1;
        self.schema_versions.insert(row.schema_version.clone());
        let payload = format!(
            "{}:{}:{}:{}",
            row.venue, row.symbol_native, row.observed_at_ms, row.reason_codes
        );
        if row.schema_version != "symbol_health_v1" {
            self.stats.invalid_event_count += 1;
            return;
        }
        if payload_hash(&payload) != row.payload_sha256 {
            self.stats.invalid_event_count += 1;
            self.stats.payload_hash_mismatch_count += 1;
            return;
        }
        self.symbol_health.push(row);
    }

    pub fn ingest_source_health(&mut self, row: SourceHealthInput) {
        self.stats.input_record_count += 1;
        self.schema_versions.insert(row.schema_version.clone());
        if row.schema_version != "source_health_v2"
            || payload_hash(&row.payload_json) != row.payload_sha256
        {
            self.stats.invalid_event_count += 1;
            if row.schema_version == "source_health_v2" {
                self.stats.payload_hash_mismatch_count += 1;
            }
            return;
        }
        self.source_health.push(row);
    }

    pub fn ingest_gap_alert(&mut self, row: super::model::GapAlertInput) {
        self.stats.input_record_count += 1;
        self.schema_versions.insert(row.schema_version.clone());
        if row.schema_version != "gap_alert_v1"
            || payload_hash(&row.payload_json) != row.payload_sha256
        {
            self.stats.invalid_event_count += 1;
            if row.schema_version == "gap_alert_v1" {
                self.stats.payload_hash_mismatch_count += 1;
            }
            return;
        }
        self.gap_alerts.push(row);
    }

    pub fn finish(mut self) -> BuildResult {
        apply_health_and_gaps(
            &self.symbol_health,
            &self.source_health,
            &self.gap_alerts,
            self.rows.values_mut(),
            self.input_range,
        );
        let slices = finalize_slices(self.rows.into_values());

        apply_health_and_gaps(
            &self.symbol_health,
            &self.source_health,
            &self.gap_alerts,
            self.projection_rows.values_mut(),
            self.projection_range,
        );
        let projection_slices = finalize_slices(self.projection_rows.into_values());

        let status = if self.stats.payload_hash_mismatch_count > 0 {
            "blocked"
        } else {
            "success"
        };
        BuildResult {
            slices,
            projection_slices,
            projection_derivative_metrics: self.projection_derivative_metrics,
            input_object_keys: self.metadata.input_object_keys,
            run_mode: self.metadata.run_mode,
            fallback_alert: self.metadata.fallback_alert,
            input_local_object_count: self.metadata.input_local_object_count,
            input_s3_object_count: self.metadata.input_s3_object_count,
            input_record_count: self.stats.input_record_count,
            duplicate_event_count: self.stats.duplicate_event_count,
            invalid_event_count: self.stats.invalid_event_count,
            payload_hash_mismatch_count: self.stats.payload_hash_mismatch_count,
            input_schema_versions: self.schema_versions.into_iter().collect(),
            status: status.to_owned(),
            failure_reason: (status == "blocked").then(|| "payload_hash_mismatch".to_owned()),
        }
    }

    fn ingest_derivative_event(&mut self, event: RawInputEvent) {
        let in_projection = event.exchange_timestamp_ms >= self.projection_range.start_ms
            && event.exchange_timestamp_ms < self.projection_range.end_ms;
        if !in_projection {
            return;
        }
        let Some(observation) = parse_derivative_metric(&event) else {
            self.stats.invalid_event_count += 1;
            return;
        };
        self.projection_derivative_metrics.push(observation);
    }

    fn ingest_spot_event(&mut self, args: &NormalizeArgs, event: &RawInputEvent) {
        if event.exchange_timestamp_ms >= self.projection_range.start_ms
            && event.exchange_timestamp_ms < self.projection_range.end_ms
        {
            self.ensure_projection_identity(args, event);
            apply_event(
                args,
                self.projection_range,
                event,
                &mut self.projection_rows,
                &mut self.projection_stats,
            );
        }
        if event.exchange_timestamp_ms >= self.input_range.start_ms
            && event.exchange_timestamp_ms < self.input_range.end_ms
        {
            self.ensure_input_identity(args, event);
            apply_event(
                args,
                self.input_range,
                event,
                &mut self.rows,
                &mut self.stats,
            );
        }
    }

    fn ensure_input_identity(&mut self, args: &NormalizeArgs, event: &RawInputEvent) {
        let key = IdentityKey::from_event(event);
        if self.input_identities.contains_key(&key) {
            return;
        }
        let identity = Identity::from_event(event);
        self.input_identities.insert(key, identity.clone());
        seed_identity_slices(args, self.input_range, &identity, &mut self.rows);
    }

    fn ensure_projection_identity(&mut self, args: &NormalizeArgs, event: &RawInputEvent) {
        let key = IdentityKey::from_event(event);
        if self.projection_identities.contains_key(&key) {
            return;
        }
        let identity = Identity::from_event(event);
        self.projection_identities.insert(key, identity.clone());
        seed_identity_slices(
            args,
            self.projection_range,
            &identity,
            &mut self.projection_rows,
        );
    }
}

pub fn build_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    scan_range: InputRange,
    inputs: NormalizeInputs,
    _started_at_ms: i64,
) -> BuildResult {
    let metadata = BuildInputMetadata {
        run_mode: inputs.run_mode,
        fallback_alert: inputs.fallback_alert,
        input_local_object_count: inputs.input_local_object_count,
        input_s3_object_count: inputs.input_s3_object_count,
        input_object_keys: inputs.input_object_keys,
    };
    let mut accumulator = BuildAccumulator::new(args, input_range, scan_range, metadata);
    for event in inputs.raw_events {
        accumulator.ingest_raw_event(args, event);
    }
    for row in inputs.symbol_health {
        accumulator.ingest_symbol_health(row);
    }
    for row in inputs.source_health {
        accumulator.ingest_source_health(row);
    }
    for row in inputs.gap_alerts {
        accumulator.ingest_gap_alert(row);
    }
    accumulator.finish()
}

fn is_derivative_market_event(event: &RawInputEvent) -> bool {
    matches!(
        (event.venue.as_str(), event.event_type.as_str()),
        (
            "binance",
            "funding_rate_snapshot" | "open_interest_snapshot"
        )
    )
}

#[derive(Default)]
struct BuildStats {
    input_record_count: usize,
    duplicate_event_count: usize,
    invalid_event_count: usize,
    payload_hash_mismatch_count: usize,
}

fn empty_slice(args: &NormalizeArgs, identity: &Identity, window_start_ms: i64) -> SliceRow {
    let slice_key = format!(
        "{}|{}|{}|{}|{}",
        identity.venue,
        identity.symbol_canonical,
        window_start_ms,
        args.window_ms,
        SLICE_SCHEMA_VERSION
    );
    SliceRow {
        slice_id: sha256_hex(slice_key.as_bytes()),
        venue: identity.venue.clone(),
        source_role: identity.source_role.clone(),
        symbol_native: identity.symbol_native.clone(),
        symbol_canonical: identity.symbol_canonical.clone(),
        base_asset: identity.base_asset.clone(),
        quote_asset: identity.quote_asset.clone(),
        market_type: identity.market_type.clone(),
        window_ms: args.window_ms,
        window_start_ms,
        window_end_ms: window_start_ms.saturating_add(args.window_ms),
        slice_completeness: String::new(),
        missing_reasons: Vec::new(),
        quality_ok: 0,
        quality_delayed: 0,
        quality_stale: 0,
        quality_gap: 0,
        quality_invalid: 0,
        trade_count: 0,
        trade_volume: 0.0,
        last_trade_price: None,
        last_trade_size: None,
        best_bid: None,
        best_ask: None,
        mid_price: None,
        spread_bps: None,
        book_ticker_count: 0,
        depth_event_count: 0,
        depth_book_rebuilt: false,
        trade_events: Vec::new(),
        book_ticker_events: Vec::new(),
        depth_events: Vec::new(),
        ticker_events: Vec::new(),
        symbol_health_snapshot: None,
        source_health_snapshot: None,
        parent_event_ids: Vec::new(),
        parent_run_ids: Vec::new(),
    }
}

fn seed_identity_slices(
    args: &NormalizeArgs,
    input_range: InputRange,
    identity: &Identity,
    rows: &mut BTreeMap<SliceKey, SliceRow>,
) {
    let mut window_start_ms = input_range.start_ms;
    while window_start_ms < input_range.end_ms {
        let key = SliceKey {
            venue: identity.venue.clone(),
            symbol_canonical: identity.symbol_canonical.clone(),
            window_start_ms,
        };
        rows.entry(key)
            .or_insert_with(|| empty_slice(args, identity, window_start_ms));
        window_start_ms = window_start_ms.saturating_add(args.window_ms);
    }
}

fn apply_event(
    args: &NormalizeArgs,
    input_range: InputRange,
    event: &RawInputEvent,
    rows: &mut BTreeMap<SliceKey, SliceRow>,
    stats: &mut BuildStats,
) {
    if event.exchange_timestamp_ms < input_range.start_ms
        || event.exchange_timestamp_ms >= input_range.end_ms
    {
        return;
    }
    let window_start_ms = event.exchange_timestamp_ms.div_euclid(args.window_ms) * args.window_ms;
    let key = SliceKey {
        venue: event.venue.clone(),
        symbol_canonical: event.symbol_canonical.clone(),
        window_start_ms,
    };
    let Some(row) = rows.get_mut(&key) else {
        return;
    };

    match event.event_type.as_str() {
        "trade" => match parse_trade(event) {
            Some(trade) => {
                row.trade_count += 1;
                row.trade_volume += trade.quantity;
                row.last_trade_price = Some(trade.price);
                row.last_trade_size = Some(trade.quantity);
                row.trade_events.push(trade);
                mark_event_quality(row, args, event);
                push_parent(row, event);
            }
            None => mark_invalid(row, stats, "parse_trade_failed"),
        },
        "book_ticker" | "depth_snapshot" => {
            if let Some(book) = parse_book_ticker(event) {
                row.book_ticker_count += 1;
                row.best_bid = Some(book.best_bid);
                row.best_ask = Some(book.best_ask);
                row.book_ticker_events.push(book);
                mark_event_quality(row, args, event);
                push_parent(row, event);
            } else if event.event_type == "book_ticker" {
                mark_invalid(row, stats, "parse_book_ticker_failed");
            }
            if event.event_type == "depth_snapshot" {
                row.depth_event_count += 1;
                row.depth_events.push(compact_ref(event));
                push_parent(row, event);
            }
        }
        "depth_delta" => {
            row.depth_event_count += 1;
            row.depth_events.push(compact_ref(event));
            mark_event_quality(row, args, event);
            push_parent(row, event);
        }
        "ticker" => {
            row.ticker_events.push(compact_ref(event));
            mark_event_quality(row, args, event);
            push_parent(row, event);
        }
        _ => mark_invalid(row, stats, "unknown_event_type"),
    }
}

fn payload_hash(payload: &str) -> String {
    sha256_hex(payload.as_bytes())
}

fn push_parent(row: &mut SliceRow, event: &RawInputEvent) {
    row.parent_event_ids.push(event.event_id.clone());
    row.parent_run_ids.push(event.producer_run_id.clone());
}

fn mark_event_quality(row: &mut SliceRow, args: &NormalizeArgs, event: &RawInputEvent) {
    if event
        .ingest_timestamp_ms
        .saturating_sub(event.exchange_timestamp_ms)
        > args.max_latency_ms
    {
        row.quality_delayed += 1;
    } else {
        row.quality_ok += 1;
    }
}

fn mark_invalid(row: &mut SliceRow, stats: &mut BuildStats, reason: &str) {
    stats.invalid_event_count += 1;
    row.quality_invalid += 1;
    push_missing(row, reason);
}

fn push_missing(row: &mut SliceRow, reason: &str) {
    row.missing_reasons.push(reason.to_owned());
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SliceKey {
    venue: String,
    symbol_canonical: String,
    window_start_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IdentityKey {
    venue: String,
    symbol_canonical: String,
}

impl IdentityKey {
    fn from_event(event: &RawInputEvent) -> Self {
        Self {
            venue: event.venue.clone(),
            symbol_canonical: event.symbol_canonical.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Identity {
    venue: String,
    source_role: String,
    symbol_native: String,
    symbol_canonical: String,
    base_asset: String,
    quote_asset: String,
    market_type: String,
}

impl Identity {
    fn from_event(event: &RawInputEvent) -> Self {
        Self {
            venue: event.venue.clone(),
            source_role: event.source_role.clone(),
            symbol_native: event.symbol_native.clone(),
            symbol_canonical: event.symbol_canonical.clone(),
            base_asset: event.base_asset.clone(),
            quote_asset: event.quote_asset.clone(),
            market_type: event.market_type.clone(),
        }
    }
}
