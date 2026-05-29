use super::super::stats::BinanceL0GapAlert;
use crate::storage::gap::GapAlertDraft;

pub(super) fn gap_alert_draft(alert: &BinanceL0GapAlert) -> GapAlertDraft {
    GapAlertDraft {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        symbol_native: alert.symbol.clone(),
        gap_type: alert.gap_type.clone(),
        detected_at_ms: alert.detected_at_ms,
        expected_sequence_id: alert.expected_sequence_id,
        observed_sequence_id: alert.observed_sequence_id,
        heal_action: alert.heal_action.clone(),
        heal_status: alert.heal_status.clone(),
        payload_json: serde_json::to_string(alert).unwrap_or_else(|_| "{}".to_owned()),
    }
}
