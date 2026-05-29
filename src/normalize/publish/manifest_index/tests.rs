use super::index::{index_pointer_json, index_window_starts, should_publish_index_pointers};
use super::*;

#[test]
fn index_window_starts_cover_every_window_in_run() {
    let starts = index_window_starts(
        InputRange {
            start_ms: 1_000,
            end_ms: 4_000,
        },
        1_000,
    );

    assert_eq!(starts, vec![1_000, 2_000, 3_000]);
}

#[test]
fn index_window_starts_keep_existing_schedule_interval_shape() {
    let starts = index_window_starts(
        InputRange {
            start_ms: 0,
            end_ms: 900_000,
        },
        1_000,
    );

    assert_eq!(starts.len(), 900);
    assert_eq!(starts.first(), Some(&0));
    assert_eq!(starts.last(), Some(&899_000));
}

#[test]
fn index_window_starts_reject_empty_or_invalid_ranges() {
    assert!(
        index_window_starts(
            InputRange {
                start_ms: 1,
                end_ms: 1
            },
            1_000
        )
        .is_empty()
    );
    assert!(
        index_window_starts(
            InputRange {
                start_ms: 2,
                end_ms: 1
            },
            1_000
        )
        .is_empty()
    );
    assert!(
        index_window_starts(
            InputRange {
                start_ms: 1,
                end_ms: 2
            },
            0
        )
        .is_empty()
    );
}

#[test]
fn publishes_index_pointers_for_terminal_empty_outputs() {
    assert!(should_publish_index_pointers("success"));
    assert!(should_publish_index_pointers("empty"));
    assert!(!should_publish_index_pointers("blocked"));
}

#[test]
fn index_pointer_records_run_range_and_indexed_window() {
    let pointer = index_pointer_json(
        "runs/run_id=r/manifest.json",
        "r",
        "success",
        10,
        InputRange {
            start_ms: 0,
            end_ms: 900_000,
        },
        42_000,
        1_000,
    );

    assert_eq!(pointer["input_time_range_start_ms"], 0);
    assert_eq!(pointer["input_time_range_end_ms"], 900_000);
    assert_eq!(pointer["indexed_window_start_ms"], 42_000);
    assert_eq!(pointer["indexed_window_end_ms"], 43_000);
}
