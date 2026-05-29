use super::super::super::Args;
use super::super::super::parse::{parse_positive_u64, parse_upbit_orderbook_unit};
use super::super::required_arg;
use std::error::Error;

pub(super) fn apply_option(
    arg: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> Result<(), Box<dyn Error>> {
    match arg {
        "--binance-futures-rest-base-url" => {
            parsed.binance_futures_rest_base_url = required_arg(
                args,
                "--binance-futures-rest-base-url requires an absolute HTTPS URL",
            )?;
        }
        "--binance-derivatives-snapshot-interval-seconds" => {
            parsed.binance_derivatives_snapshot_interval_seconds = parse_positive_u64(
                required_arg(
                    args,
                    "--binance-derivatives-snapshot-interval-seconds requires a positive integer",
                )?,
                "--binance-derivatives-snapshot-interval-seconds",
            )?;
        }
        "--upbit-rest-base-url" => {
            parsed.upbit_rest_base_url = Some(required_arg(
                args,
                "--upbit-rest-base-url requires an absolute HTTPS URL",
            )?);
        }
        "--upbit-websocket-url" => {
            parsed.upbit_websocket_url = Some(required_arg(
                args,
                "--upbit-websocket-url requires an absolute WSS URL",
            )?);
        }
        "--upbit-quote-currency" => {
            parsed.upbit_quote_currency =
                required_arg(args, "--upbit-quote-currency requires a quote currency")?;
        }
        "--upbit-orderbook-unit" => {
            parsed.upbit_orderbook_unit = parse_upbit_orderbook_unit(args.next())?;
        }
        _ => unreachable!("exchange option dispatch mismatch: {arg}"),
    }
    Ok(())
}
