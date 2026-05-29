use super::args::{InputRange, NormalizeArgs};
use super::mode::{RunDecision, decide_mode};
use l1_audit::run_l1_index_audit;
use preflight::run_preflight;
use std::error::Error;
use worker::run_normalize_worker;

mod decision;
mod env;
mod l1_audit;
mod preflight;
mod window;
mod worker;

use decision::run_decision;
use env::env_flag;

const PREFLIGHT_ENV: &str = "MARKET_NORMALIZE_PREFLIGHT";

pub async fn run_normalize(args: NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    if args.preflight || env_flag(PREFLIGHT_ENV) {
        run_preflight(&args, now_ms).await?;
        return Ok(());
    }

    if let (Some(start_ms), Some(end_ms)) =
        (args.audit_l1_index_start_ms, args.audit_l1_index_end_ms)
    {
        run_l1_index_audit(&args, InputRange { start_ms, end_ms }).await?;
        return Ok(());
    }

    if args.input_start_ms.is_some() {
        return run_backfill_once(&args, now_ms).await;
    }

    run_normalize_worker(args, now_ms).await
}

async fn run_backfill_once(args: &NormalizeArgs, now_ms: i64) -> Result<(), Box<dyn Error>> {
    let Some(decision) = decide_mode(args, now_ms, None, None)? else {
        return Ok(());
    };
    run_decision(args, decision, now_ms).await
}
