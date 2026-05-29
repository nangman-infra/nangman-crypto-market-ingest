use super::super::args::{InputRange, NormalizeArgs};
use super::super::model::{
    DerivativeMetricObservation, GapAlertInput, SliceRow, SourceHealthInput, SymbolHealthInput,
};
use super::result::BuildInputMetadata;
use super::slices::{BuildStats, Identity, IdentityKey, SliceKey};
use std::collections::{BTreeMap, BTreeSet};

pub struct BuildAccumulator {
    pub(super) input_range: InputRange,
    pub(super) scan_range: InputRange,
    pub(super) projection_range: InputRange,
    pub(super) input_identities: BTreeMap<IdentityKey, Identity>,
    pub(super) projection_identities: BTreeMap<IdentityKey, Identity>,
    pub(super) rows: BTreeMap<SliceKey, SliceRow>,
    pub(super) projection_rows: BTreeMap<SliceKey, SliceRow>,
    pub(super) projection_derivative_metrics: Vec<DerivativeMetricObservation>,
    pub(super) symbol_health: Vec<SymbolHealthInput>,
    pub(super) source_health: Vec<SourceHealthInput>,
    pub(super) gap_alerts: Vec<GapAlertInput>,
    pub(super) stats: BuildStats,
    pub(super) projection_stats: BuildStats,
    pub(super) schema_versions: BTreeSet<String>,
    pub(super) seen_event_ids: BTreeSet<String>,
    pub(super) metadata: BuildInputMetadata,
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
}
