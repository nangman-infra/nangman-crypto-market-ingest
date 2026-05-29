use super::shutdown::ShutdownHandle;
use crate::clock;
use crate::log_stream;
use crate::normalize::args::NormalizeArgs;
use crate::normalize::run::window::{
    resolve_initial_l0_l1_state, run_live_priority_decision, run_next_ready_decision,
};
use serde_json::json;
use std::error::Error;

pub(super) async fn drain_ready_windows(
    args: &NormalizeArgs,
    initial_now_ms: i64,
    shutdown: Option<&ShutdownHandle>,
) -> Result<(), Box<dyn Error>> {
    let (mut last_l1_success_end_ms, mut oldest_l0_object_ms) =
        resolve_initial_l0_l1_state(args).await?;
    let mut processed_windows = 0_usize;
    let mut now_ms = initial_now_ms;
    let mut live_priority_completed_range = None;
    if let Some(completed_range) =
        run_live_priority_decision(args, now_ms, &mut last_l1_success_end_ms).await?
    {
        processed_windows += 1;
        live_priority_completed_range = Some(completed_range);
        oldest_l0_object_ms = None;
        now_ms = clock::now_ms();
    }
    if args.live_priority_only {
        log_stream::debug(
            "market_normalize_live_priority_only_tick_finished",
            json!({
                "processed_windows": processed_windows,
                "last_l1_success_end_ms": last_l1_success_end_ms
            }),
        )?;
        return Ok(());
    }

    loop {
        if shutdown.is_some_and(ShutdownHandle::is_requested) {
            log_stream::info(
                "market_normalize_shutdown_complete",
                json!({
                    "processed_windows": processed_windows,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }

        if processed_windows >= args.max_windows_per_tick {
            log_stream::warn(
                "market_normalize_max_windows_reached",
                json!({
                    "processed_windows": processed_windows,
                    "max_windows_per_tick": args.max_windows_per_tick,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }

        let Some(completed_end_ms) = run_next_ready_decision(
            args,
            now_ms,
            last_l1_success_end_ms,
            oldest_l0_object_ms,
            live_priority_completed_range,
            processed_windows,
        )
        .await?
        else {
            return Ok(());
        };
        processed_windows += 1;
        last_l1_success_end_ms = Some(completed_end_ms);
        oldest_l0_object_ms = None;
        if shutdown.is_some_and(ShutdownHandle::is_requested) {
            log_stream::info(
                "market_normalize_shutdown_after_window",
                json!({
                    "processed_windows": processed_windows,
                    "last_l1_success_end_ms": last_l1_success_end_ms
                }),
            )?;
            return Ok(());
        }
        now_ms = clock::now_ms();
    }
}
