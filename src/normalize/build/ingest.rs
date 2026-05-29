use super::super::args::NormalizeArgs;
use super::super::model::{GapAlertInput, RawInputEvent, SourceHealthInput, SymbolHealthInput};
use super::super::payload::parse_derivative_metric;
use super::accumulator::BuildAccumulator;
use super::slices::{
    Identity, IdentityKey, apply_event, is_derivative_market_event, payload_hash,
    seed_identity_slices,
};

impl BuildAccumulator {
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

    pub fn ingest_gap_alert(&mut self, row: GapAlertInput) {
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
