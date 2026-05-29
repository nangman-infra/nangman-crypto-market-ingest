use super::super::args::InputRange;
use super::super::model::{GapAlertInput, SliceRow, SourceHealthInput, SymbolHealthInput};
use super::health::{is_healthy_level, latest_source_health, latest_symbol_health};

pub(crate) fn apply_health_and_gaps<'a>(
    symbol_health: &[SymbolHealthInput],
    source_health: &[SourceHealthInput],
    gap_alerts: &[GapAlertInput],
    rows: impl Iterator<Item = &'a mut SliceRow>,
    input_range: InputRange,
) {
    for row in rows {
        apply_health_and_gaps_to_row(symbol_health, source_health, gap_alerts, row, input_range);
    }
}

fn apply_health_and_gaps_to_row(
    symbol_health: &[SymbolHealthInput],
    source_health: &[SourceHealthInput],
    gap_alerts: &[GapAlertInput],
    row: &mut SliceRow,
    input_range: InputRange,
) {
    row.symbol_health_snapshot = latest_symbol_health(symbol_health, row);
    row.source_health_snapshot = latest_source_health(source_health, row);
    apply_symbol_health_snapshot(row);
    apply_source_health_snapshot(row);
    apply_gap_alerts(gap_alerts, row, input_range);
}

fn apply_symbol_health_snapshot(row: &mut SliceRow) {
    let Some(snapshot) = &row.symbol_health_snapshot else {
        push_missing(row, "symbol_health_missing");
        return;
    };
    let is_tradeable = snapshot.is_tradeable;
    let last_received_time_ms = snapshot.last_received_time_ms;
    if !is_tradeable {
        push_missing(row, "venue_unavailable");
    }
    if last_received_time_ms < row.window_start_ms {
        row.quality_stale += 1;
        push_missing(row, "stale");
    }
}

fn apply_source_health_snapshot(row: &mut SliceRow) {
    let Some(snapshot) = &row.source_health_snapshot else {
        push_missing(row, "source_health_missing");
        return;
    };
    if snapshot.connection_status != "connected" || !is_healthy_level(&snapshot.health_level) {
        push_missing(row, "source_stale");
    }
}

fn apply_gap_alerts(gap_alerts: &[GapAlertInput], row: &mut SliceRow, input_range: InputRange) {
    let reasons = gap_alerts
        .iter()
        .filter(|gap| gap_applies_to_row(gap, row, input_range))
        .map(|gap| format!("gap_alert.{}", gap.gap_type))
        .collect::<Vec<_>>();
    for reason in reasons {
        row.quality_gap += 1;
        push_missing(row, &reason);
    }
}

fn gap_applies_to_row(gap: &GapAlertInput, row: &SliceRow, input_range: InputRange) -> bool {
    gap.venue == row.venue
        && gap.symbol_native == row.symbol_native
        && gap.detected_at_ms >= row.window_start_ms
        && gap.detected_at_ms < row.window_end_ms
        && gap.detected_at_ms >= input_range.start_ms
        && gap.detected_at_ms < input_range.end_ms
}

fn push_missing(row: &mut SliceRow, reason: &str) {
    row.missing_reasons.push(reason.to_owned());
}
