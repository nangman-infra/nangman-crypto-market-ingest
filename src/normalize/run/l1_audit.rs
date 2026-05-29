use crate::log_stream;
use crate::normalize::args::{InputRange, NormalizeArgs};
use crate::normalize::write::index_pointer_key;
use crate::storage::s3_upload::S3Uploader;
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;

const MAX_AUDIT_WINDOWS: usize = 100_000;
const MAX_AUDIT_MISSING_SAMPLES: usize = 20;

pub(super) async fn run_l1_index_audit(
    args: &NormalizeArgs,
    range: InputRange,
) -> Result<(), Box<dyn Error>> {
    if range.end_ms <= range.start_ms {
        return Err("audit range must be positive and non-empty".into());
    }
    if range.start_ms % args.window_ms != 0 || range.end_ms % args.window_ms != 0 {
        return Err("audit range must align to window_ms".into());
    }
    let expected_keys = audit_expected_keys(range, args.window_ms)?;
    let expected_count = expected_keys.len();
    let uploader = S3Uploader::new(
        args.l1_s3_bucket.clone(),
        args.aws_region.clone(),
        args.aws_profile.clone(),
    )
    .await?;

    let mut existing_keys = BTreeSet::new();
    for prefix in audit_hour_prefixes(&expected_keys) {
        for key in uploader.list_keys(&prefix).await? {
            existing_keys.insert(key);
        }
    }

    let mut missing = Vec::new();
    for key in &expected_keys {
        if !existing_keys.contains(key) {
            missing.push(key.clone());
        }
    }

    let missing_count = missing.len();
    let missing_samples = missing
        .iter()
        .take(MAX_AUDIT_MISSING_SAMPLES)
        .cloned()
        .collect::<Vec<_>>();
    log_stream::info(
        "market_normalize_l1_index_audit",
        json!({
            "l1_s3_bucket": args.l1_s3_bucket.as_str(),
            "window_ms": args.window_ms,
            "input_time_range_start_ms": range.start_ms,
            "input_time_range_end_ms": range.end_ms,
            "expected_index_pointer_count": expected_count,
            "missing_index_pointer_count": missing_count,
            "missing_index_pointer_samples": missing_samples
        }),
    )?;

    if missing_count == 0 {
        Ok(())
    } else {
        Err(
            format!("l1 index audit failed: missing {missing_count}/{expected_count} pointers")
                .into(),
        )
    }
}

fn audit_expected_keys(range: InputRange, window_ms: i64) -> Result<Vec<String>, Box<dyn Error>> {
    if window_ms <= 0 {
        return Err("window_ms must be positive".into());
    }
    let mut keys = Vec::new();
    let mut current = range.start_ms;
    while current < range.end_ms {
        if keys.len() >= MAX_AUDIT_WINDOWS {
            return Err(format!("audit range exceeds {MAX_AUDIT_WINDOWS} windows").into());
        }
        keys.push(index_pointer_key(window_ms, current));
        let Some(next) = current.checked_add(window_ms) else {
            return Err("audit range overflow".into());
        };
        if next <= current {
            return Err("audit range did not advance".into());
        }
        current = next;
    }
    Ok(keys)
}

fn audit_hour_prefixes(keys: &[String]) -> Vec<String> {
    let mut prefixes = BTreeMap::<String, ()>::new();
    for key in keys {
        if let Some((prefix, _)) = key.rsplit_once("window_start_ms=") {
            prefixes.insert(prefix.to_owned(), ());
        }
    }
    prefixes.into_keys().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_expected_keys_cover_every_window() {
        let keys = audit_expected_keys(
            InputRange {
                start_ms: 0,
                end_ms: 3_000,
            },
            1_000,
        )
        .unwrap();

        assert_eq!(keys.len(), 3);
        assert!(keys[0].ends_with("window_start_ms=0.json"));
        assert!(keys[1].ends_with("window_start_ms=1000.json"));
        assert!(keys[2].ends_with("window_start_ms=2000.json"));
    }

    #[test]
    fn audit_hour_prefixes_deduplicate_sorted_prefixes() {
        let keys = vec![
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/window_start_ms=3600000.json"
                .to_owned(),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=0.json"
                .to_owned(),
            "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/window_start_ms=1000.json"
                .to_owned(),
        ];

        let prefixes = audit_hour_prefixes(&keys);

        assert_eq!(
            prefixes,
            vec![
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=00/".to_owned(),
                "l1_index/window_ms=1000/event_date=1970-01-01/hour=01/".to_owned(),
            ]
        );
    }
}
