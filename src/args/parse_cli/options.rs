mod exchange;
mod live;
mod retention;
mod runtime;
mod storage;

use super::super::Args;
use std::error::Error;

pub(super) fn apply_option(
    arg: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> Result<(), Box<dyn Error>> {
    match arg {
        "--venue"
        | "--config"
        | "--duration-seconds"
        | "--log-interval-seconds"
        | "--depth-snapshot-limit"
        | "--expect-symbol-count"
        | "--allow-partial-symbol-coverage" => runtime::apply_option(arg, args, parsed),
        "--binance-futures-rest-base-url"
        | "--binance-derivatives-snapshot-interval-seconds"
        | "--upbit-rest-base-url"
        | "--upbit-websocket-url"
        | "--upbit-quote-currency"
        | "--upbit-orderbook-unit" => exchange::apply_option(arg, args, parsed),
        "--l0-s3-bucket"
        | "--aws-profile"
        | "--aws-region"
        | "--l0-spool-root"
        | "--l0-flush-records"
        | "--l0-shard-count"
        | "--local-disk-high-water-pct"
        | "--local-disk-emergency-pct"
        | "--safety-floor-hours"
        | "--eviction-check-interval-secs" => storage::apply_option(arg, args, parsed),
        "--disable-s3-retention"
        | "--s3-retention-days"
        | "--s3-retention-check-interval-secs"
        | "--s3-retention-max-deletes-per-run" => retention::apply_option(arg, args, parsed),
        "--live-nats-url"
        | "--live-nats-stream"
        | "--live-nats-subject-prefix"
        | "--live-nats-required" => live::apply_option(arg, args, parsed),
        _ => Err(format!("unknown argument: {arg}").into()),
    }
}
