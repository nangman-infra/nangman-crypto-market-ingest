use super::super::args::InputRange;
use super::super::model::{
    MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION, MarketDataQualitySummary, SliceRow,
};
use super::common::stable_id;
use std::collections::BTreeSet;

pub fn build_market_data_quality_summary(
    l1_run_id: &str,
    input_range: InputRange,
    known_as_of_ms: i64,
    slices: &[SliceRow],
) -> MarketDataQualitySummary {
    let coverage_ratio = if slices.is_empty() {
        0.0
    } else {
        let covered = slices
            .iter()
            .filter(|row| {
                matches!(
                    row.slice_completeness.as_str(),
                    "complete" | "partial" | "reference_only"
                )
            })
            .count();
        covered as f64 / slices.len() as f64
    };
    let gap_count = slices.iter().map(|row| row.quality_gap).sum();
    let stale_sources = sources_by_quality(slices, |row| row.quality_stale > 0);
    let delayed_sources = sources_by_quality(slices, |row| row.quality_delayed > 0);

    MarketDataQualitySummary {
        schema_version: MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION.to_owned(),
        quality_summary_id: stable_id(&[
            l1_run_id,
            &input_range.start_ms.to_string(),
            &input_range.end_ms.to_string(),
            MARKET_DATA_QUALITY_SUMMARY_SCHEMA_VERSION,
        ]),
        l1_run_id: l1_run_id.to_owned(),
        coverage_ratio,
        gap_count,
        stale_sources,
        delayed_sources,
        missing_venues: missing_venues(slices),
        source_health_status: source_health_status(slices),
        symbol_health_status: symbol_health_status(slices),
        quality_window_start_ms: input_range.start_ms,
        quality_window_end_ms: input_range.end_ms,
        known_as_of_ms,
    }
}

fn sources_by_quality(slices: &[SliceRow], is_flagged: impl Fn(&SliceRow) -> bool) -> Vec<String> {
    slices
        .iter()
        .filter(|row| is_flagged(row))
        .map(|row| row.venue.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn missing_venues(slices: &[SliceRow]) -> Vec<String> {
    let mut venues = BTreeSet::new();
    for row in slices {
        if row.slice_completeness == "incomplete"
            || row.missing_reasons.iter().any(|reason| {
                reason.contains("missing") || reason.contains("gap") || reason.contains("stale")
            })
        {
            venues.insert(row.venue.clone());
        }
    }
    venues.into_iter().collect()
}

fn source_health_status(slices: &[SliceRow]) -> String {
    let mut has_unknown = false;
    for row in slices {
        let Some(snapshot) = row.source_health_snapshot.as_ref() else {
            has_unknown = true;
            continue;
        };
        if snapshot.health_level != "healthy" && snapshot.health_level != "ok" {
            return "degraded".to_owned();
        }
    }
    if has_unknown {
        "unknown".to_owned()
    } else {
        "healthy".to_owned()
    }
}

fn symbol_health_status(slices: &[SliceRow]) -> String {
    let mut has_unknown = false;
    for row in slices {
        let Some(snapshot) = row.symbol_health_snapshot.as_ref() else {
            has_unknown = true;
            continue;
        };
        if !snapshot.is_tradeable || row.quality_gap > 0 || row.quality_stale > 0 {
            return "degraded".to_owned();
        }
    }
    if has_unknown {
        "unknown".to_owned()
    } else {
        "healthy".to_owned()
    }
}
