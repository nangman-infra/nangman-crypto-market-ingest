use super::super::time::HourPart;
use super::super::{RAW_EVENT_TYPES, VENUES};

const GAP_ALERT_TYPES: &[&str] = &["depth_update_id_gap", "ordering_violation", "upbit_error"];

pub(super) fn input_prefixes_for_part(part: &HourPart) -> Vec<String> {
    let mut prefixes = Vec::new();
    for venue in VENUES {
        for event_type in RAW_EVENT_TYPES {
            prefixes.push(format!(
                "raw_market_event/venue={venue}/event_type={event_type}/event_date={}/hour={:02}/",
                part.event_date, part.hour
            ));
        }
        prefixes.push(format!(
            "symbol_health/venue={venue}/event_date={}/hour={:02}/",
            part.event_date, part.hour
        ));
        prefixes.push(format!(
            "source_health/venue={venue}/event_date={}/hour={:02}/",
            part.event_date, part.hour
        ));
        for gap_type in GAP_ALERT_TYPES {
            prefixes.push(format!(
                "gap_alert/venue={venue}/gap_type={gap_type}/event_date={}/hour={:02}/",
                part.event_date, part.hour
            ));
        }
    }
    prefixes
}
