use super::*;

pub(super) fn index_pointer_json(
    manifest_key: &str,
    l1_run_id: &str,
    status: &str,
    finished_at_ms: i64,
    input_range: InputRange,
    indexed_window_start_ms: i64,
    window_ms: i64,
) -> serde_json::Value {
    json!({
        "schema_version": "l1_index_pointer_v1",
        "canonical_manifest_key": manifest_key,
        "l1_run_id": l1_run_id,
        "status": status,
        "finished_at_ms": finished_at_ms,
        "input_time_range_start_ms": input_range.start_ms,
        "input_time_range_end_ms": input_range.end_ms,
        "indexed_window_start_ms": indexed_window_start_ms,
        "indexed_window_end_ms": indexed_window_start_ms.saturating_add(window_ms),
        "schema_version_emitted": SLICE_SCHEMA_VERSION
    })
}

pub(super) fn index_window_starts(input_range: InputRange, window_ms: i64) -> Vec<i64> {
    if window_ms <= 0 || input_range.end_ms <= input_range.start_ms {
        return Vec::new();
    }

    let mut starts = Vec::new();
    let mut current = input_range.start_ms;
    while current < input_range.end_ms {
        starts.push(current);
        let Some(next) = current.checked_add(window_ms) else {
            break;
        };
        if next <= current {
            break;
        }
        current = next;
    }
    starts
}

pub(super) fn should_publish_index_pointers(status: &str) -> bool {
    matches!(status, "success" | "empty")
}

pub(super) async fn publish_index_pointers(
    uploader: &S3Uploader,
    args: &NormalizeArgs,
    manifest_key: &str,
    l1_run_id: &str,
    status: &str,
    finished_at_ms: i64,
    input_range: InputRange,
) -> Result<usize, Box<dyn Error>> {
    let window_starts = index_window_starts(input_range, args.window_ms);
    let pointer_count = window_starts.len();
    let concurrency = args.l1_index_upload_concurrency.max(1);
    let manifest_key = manifest_key.to_owned();
    let l1_run_id = l1_run_id.to_owned();
    let status = status.to_owned();

    let results = stream::iter(window_starts.into_iter().map(|window_start_ms| {
        let manifest_key = manifest_key.clone();
        let l1_run_id = l1_run_id.clone();
        let status = status.clone();
        async move {
            let pointer_key = index_pointer_key(args.window_ms, window_start_ms);
            let pointer = index_pointer_json(
                &manifest_key,
                &l1_run_id,
                status.as_str(),
                finished_at_ms,
                input_range,
                window_start_ms,
                args.window_ms,
            );
            uploader
                .upload_json_if_pointer_current(&pointer_key, serde_json::to_vec_pretty(&pointer)?)
                .await?;
            Ok::<(), Box<dyn Error>>(())
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;

    for result in results {
        result?;
    }

    Ok(pointer_count)
}
