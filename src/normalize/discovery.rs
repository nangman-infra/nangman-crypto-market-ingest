use super::args::{NormalizeArgs, unix_timestamp_millis};
use super::input_keys::{RAW_EVENT_TYPES, VENUES};
use crate::storage::StorageError;
use crate::storage::s3_upload::S3Uploader;
use chrono::{DateTime, Timelike, Utc};
use serde_json::Value;
use std::path::PathBuf;

const HOUR_MS: i64 = 3_600_000;
const MAX_L1_POINTER_LOOKBACK_HOURS: i64 = 24 * 90;
const L1_POINTER_KEYS_PER_HOUR: i32 = 1_000;
const L0_BOOTSTRAP_KEYS_PER_PREFIX: i32 = 1;

/// Best-effort lookup for the most recent successful L1 manifest's
/// `input_time_range_end_ms`. Returns Ok(None) if no success exists yet.
pub(super) async fn resolve_last_l1_success_end_ms(
    args: &NormalizeArgs,
) -> Result<Option<i64>, StorageError> {
    let uploader = S3Uploader::new(
        args.l1_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let latest_hour_start_ms = floor_hour_ms(unix_timestamp_millis());
    for offset in 0..MAX_L1_POINTER_LOOKBACK_HOURS {
        let hour_start_ms = latest_hour_start_ms.saturating_sub(offset.saturating_mul(HOUR_MS));
        let prefix = l1_pointer_hour_prefix(args.window_ms, hour_start_ms);
        let pointer_keys = json_keys_desc(
            uploader
                .list_keys_page(&prefix, L1_POINTER_KEYS_PER_HOUR)
                .await?,
        );

        for pointer_key in pointer_keys {
            if let Some(end_ms) = success_end_ms_from_pointer(&uploader, &pointer_key).await? {
                return Ok(Some(end_ms));
            }
        }
    }

    Ok(None)
}

/// Best-effort lookup for the oldest L0 raw_market_event object's start-of-hour
/// timestamp (UTC ms). Used only when no L1 success history exists.
pub(super) async fn resolve_oldest_l0_object_ms(
    args: &NormalizeArgs,
) -> Result<Option<i64>, StorageError> {
    let uploader = S3Uploader::new(
        args.l0_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let mut oldest: Option<i64> = None;
    for venue in VENUES {
        for event_type in RAW_EVENT_TYPES {
            let prefix = format!("raw_market_event/venue={venue}/event_type={event_type}/");
            let keys = uploader
                .list_keys_page(&prefix, L0_BOOTSTRAP_KEYS_PER_PREFIX)
                .await?;
            let Some(key) = keys.into_iter().find(|key| key.ends_with(".parquet")) else {
                continue;
            };
            let Some(candidate) = parse_event_date_hour_ms(&key) else {
                continue;
            };
            oldest = Some(oldest.map_or(candidate, |current| current.min(candidate)));
        }
    }

    Ok(oldest)
}

async fn success_end_ms_from_pointer(
    uploader: &S3Uploader,
    pointer_key: &str,
) -> Result<Option<i64>, StorageError> {
    let pointer = download_json_value(uploader, pointer_key, "pointer").await?;
    if !json_status_success(&pointer) {
        return Ok(None);
    }

    if let Some(end_ms) = input_time_range_end_ms(&pointer) {
        return Ok(Some(end_ms));
    }

    let Some(manifest_key) = pointer_manifest_key(&pointer) else {
        return Ok(None);
    };

    let manifest = download_json_value(uploader, manifest_key, "manifest").await?;
    if !json_status_success(&manifest) {
        return Ok(None);
    }

    Ok(input_time_range_end_ms(&manifest))
}

async fn download_json_value(
    uploader: &S3Uploader,
    key: &str,
    label: &str,
) -> Result<Value, StorageError> {
    let tmp_path = temporary_json_path(label);
    uploader.download_file(key, &tmp_path).await?;
    let bytes = match std::fs::read(&tmp_path) {
        Ok(value) => value,
        Err(error) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(StorageError::Io(error));
        }
    };
    let _ = std::fs::remove_file(&tmp_path);
    Ok(serde_json::from_slice(&bytes)?)
}

fn json_status_success(value: &Value) -> bool {
    value
        .get("status")
        .and_then(|status| status.as_str())
        .is_some_and(|status| status == "success")
}

fn input_time_range_end_ms(value: &Value) -> Option<i64> {
    value
        .get("input_time_range_end_ms")
        .and_then(|field| field.as_i64())
}

fn pointer_manifest_key(value: &Value) -> Option<&str> {
    value
        .get("canonical_manifest_key")
        .or_else(|| value.get("manifest_key"))
        .and_then(|field| field.as_str())
}

fn l1_pointer_hour_prefix(window_ms: i64, hour_start_ms: i64) -> String {
    let part = time_part(hour_start_ms);
    format!(
        "l1_index/window_ms={window_ms}/event_date={}/hour={:02}/",
        part.event_date, part.hour
    )
}

fn json_keys_desc(mut keys: Vec<String>) -> Vec<String> {
    keys.retain(|key| key.ends_with(".json"));
    keys.sort_by(|left, right| right.cmp(left));
    keys
}

fn parse_event_date_hour_ms(key: &str) -> Option<i64> {
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

fn time_part(timestamp_ms: i64) -> HourPart {
    let timestamp =
        DateTime::<Utc>::from_timestamp_millis(timestamp_ms).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    HourPart {
        event_date: timestamp.format("%Y-%m-%d").to_string(),
        hour: timestamp.hour(),
    }
}

fn floor_hour_ms(value: i64) -> i64 {
    value.div_euclid(HOUR_MS) * HOUR_MS
}

fn temporary_json_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "market-normalize-{label}-{}-{}",
        std::process::id(),
        nanos_now()
    ))
}

fn nanos_now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}

struct HourPart {
    event_date: String,
    hour: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_event_date_hour_into_epoch_ms() {
        let key = "raw_market_event/venue=binance/event_type=trade/event_date=2026-05-05/hour=13/shard=00/run_id=run-1-part-000001.parquet";
        let parsed = parse_event_date_hour_ms(key).unwrap();

        assert_eq!(parsed, 1_777_986_000_000);
    }

    #[test]
    fn returns_none_for_invalid_key() {
        assert!(parse_event_date_hour_ms("totally/wrong/key.parquet").is_none());
    }

    #[test]
    fn builds_l1_pointer_hour_prefix_from_utc_hour() {
        assert_eq!(
            l1_pointer_hour_prefix(1_000, 1_777_986_000_000),
            "l1_index/window_ms=1000/event_date=2026-05-05/hour=13/"
        );
    }

    #[test]
    fn keeps_json_keys_in_descending_order() {
        let keys = json_keys_desc(vec![
            "l1_index/a/window_start_ms=1000.json".to_owned(),
            "l1_index/a/window_start_ms=0900.txt".to_owned(),
            "l1_index/a/window_start_ms=2000.json".to_owned(),
        ]);

        assert_eq!(
            keys,
            vec![
                "l1_index/a/window_start_ms=2000.json",
                "l1_index/a/window_start_ms=1000.json"
            ]
        );
    }

    #[test]
    fn recognizes_success_status_only() {
        assert!(json_status_success(&json!({ "status": "success" })));
        assert!(!json_status_success(&json!({ "status": "failed" })));
        assert!(!json_status_success(&json!({})));
    }

    #[test]
    fn reads_current_and_legacy_pointer_manifest_keys() {
        assert_eq!(
            pointer_manifest_key(
                &json!({ "canonical_manifest_key": "runs/run_id=a/manifest.json" })
            ),
            Some("runs/run_id=a/manifest.json")
        );
        assert_eq!(
            pointer_manifest_key(&json!({ "manifest_key": "runs/run_id=b/manifest.json" })),
            Some("runs/run_id=b/manifest.json")
        );
    }

    #[test]
    fn reads_pointer_input_end_ms() {
        assert_eq!(
            input_time_range_end_ms(&json!({ "input_time_range_end_ms": 900_000 })),
            Some(900_000)
        );
        assert_eq!(input_time_range_end_ms(&json!({})), None);
    }
}
