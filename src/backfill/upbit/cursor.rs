use super::types::{UpbitInitialCursor, UpbitTrade};
use crate::backfill::BackfillError;
use chrono::{DateTime, Duration as ChronoDuration, Utc};

pub(super) const UPBIT_RECENT_WINDOW_DAYS: i64 = 7;

pub(super) fn initial_cursor(input_end_ms: i64) -> Result<UpbitInitialCursor, BackfillError> {
    let end = datetime_from_ms(input_end_ms)?;
    let now = Utc::now();
    let days_ago = now
        .date_naive()
        .signed_duration_since(end.date_naive())
        .num_days();
    if !(0..=UPBIT_RECENT_WINDOW_DAYS).contains(&days_ago) {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit end range must be within the most recent {} UTC days",
            UPBIT_RECENT_WINDOW_DAYS
        )));
    }
    Ok(UpbitInitialCursor {
        to: end.format("%H:%M:%S").to_string(),
        days_ago: if days_ago == 0 { None } else { Some(days_ago) },
    })
}

pub(super) fn validate_recent_window(
    input_start_ms: i64,
    input_end_ms: i64,
) -> Result<(), BackfillError> {
    let now = Utc::now();
    let start = datetime_from_ms(input_start_ms)?;
    let end = datetime_from_ms(input_end_ms)?;
    if end > now {
        return Err(BackfillError::InvalidArgs(
            "Upbit end range must not be in the future".to_owned(),
        ));
    }
    if start < now - ChronoDuration::days(UPBIT_RECENT_WINDOW_DAYS) {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit recent trade backfill only supports the most recent {} days",
            UPBIT_RECENT_WINDOW_DAYS
        )));
    }
    if start >= end {
        return Err(BackfillError::InvalidArgs(
            "Upbit start range must be earlier than end range".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn advance_cursor(
    page: &[UpbitTrade],
    current_cursor: Option<i64>,
    market: &str,
) -> Result<Option<i64>, BackfillError> {
    let Some(last_trade) = page.last() else {
        return Ok(current_cursor);
    };
    let next_cursor = last_trade.sequential_id;
    if current_cursor == Some(next_cursor) {
        return Err(BackfillError::InvalidConfig(format!(
            "Upbit cursor did not advance for {market}"
        )));
    }
    Ok(Some(next_cursor))
}

fn datetime_from_ms(value: i64) -> Result<DateTime<Utc>, BackfillError> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| {
        BackfillError::InvalidArgs(format!("timestamp {value} is outside supported range"))
    })
}
