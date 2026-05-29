use chrono::{DateTime, Timelike, Utc};

pub(super) struct HourPart {
    pub(super) event_date: String,
    pub(super) hour: u32,
}

pub(super) fn hourly_parts(start_ms: i64, end_ms: i64) -> Vec<HourPart> {
    let mut parts = Vec::new();
    let mut current = floor_hour_ms(start_ms);
    while current < end_ms {
        let timestamp =
            DateTime::<Utc>::from_timestamp_millis(current).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        parts.push(HourPart {
            event_date: timestamp.format("%Y-%m-%d").to_string(),
            hour: timestamp.hour(),
        });
        current = current.saturating_add(3_600_000);
    }
    parts
}

fn floor_hour_ms(value: i64) -> i64 {
    value.div_euclid(3_600_000) * 3_600_000
}
