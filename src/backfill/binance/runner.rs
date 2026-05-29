mod client;
mod logging;
mod reporting;
mod symbol;

use super::markets::resolve_markets;
use crate::backfill::{BackfillArgs, BackfillError, BackfillRunReport};
use crate::storage::L0StorageSink;

use self::client::build_client;
use self::logging::log_start;
use self::reporting::write_health_and_report;
use self::symbol::backfill_symbol;

pub(in crate::backfill) async fn run(
    args: &BackfillArgs,
    sink: &mut L0StorageSink,
) -> Result<BackfillRunReport, BackfillError> {
    let resolved = resolve_markets(args)?;
    let client = build_client()?;
    log_start(args, &resolved)?;

    let mut symbols = Vec::with_capacity(resolved.markets.len());
    for market in &resolved.markets {
        symbols.push(
            backfill_symbol(
                &client,
                &resolved.rest_base_url,
                market,
                args.input_start_ms,
                args.input_end_ms,
                sink,
            )
            .await?,
        );
    }

    write_health_and_report(args, sink, &resolved, symbols).await
}
