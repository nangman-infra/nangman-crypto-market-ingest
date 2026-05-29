use chrono::{DateTime, Timelike, Utc};

pub(super) const HOUR_MS: i64 = 3_600_000;

pub(super) fn parse_event_date_hour_ms(key: &str) -> Option<i64> {
    let date_marker = "event_date=";
    let hour_marker = "/hour=";
    let date_start = key.find(date_marker)? + date_marker.len();
    let date_end = date_start + 10;
    let date_str = key.get(date_start..date_end)?;
    let hour_start = key.find(hour_marker)? + hour_marker.len();
    let hour_end = hour_start + 2;
    let hour_str = key.get(hour_start..hour_end)?;
    let hour: u32 = hour_str.parse().ok()?;
    let formatted = format!("{date_str}T{hour:02}:00:00Z");
    let parsed = DateTime::parse_from_rfc3339(&formatted).ok()?;
    Some(parsed.with_timezone(&Utc).timestamp_millis())
}

pub(super) fn time_part(timestamp_ms: i64) -> HourPart {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    HourPart {
        event_date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

pub(super) fn floor_hour_ms(value: i64) -> i64 {
    value.div_euclid(HOUR_MS) * HOUR_MS
}

pub(super) struct HourPart {
    pub(super) event_date: String,
    pub(super) hour: u32,
}
