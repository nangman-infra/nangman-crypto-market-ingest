use super::types::ResolvedBinanceBackfill;
use crate::backfill::{BackfillArgs, BackfillError};
use crate::binance::BinanceMarket;
use crate::config::load_market_ingest_config;
use std::collections::BTreeMap;

pub(super) fn resolve_markets(
    args: &BackfillArgs,
) -> Result<ResolvedBinanceBackfill, BackfillError> {
    let config = load_market_ingest_config(&args.config_dir)
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    let exchange = config
        .enabled_exchange("binance")
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    let mut markets = config
        .enabled_symbols_for_exchange("binance")
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?
        .into_iter()
        .map(|symbol| BinanceMarket {
            raw_symbol: symbol.raw,
            base_asset: symbol.base,
            quote_asset: symbol.quote,
        })
        .collect::<Vec<_>>();

    if let Some(symbols) = &args.symbols {
        let mut by_raw = markets
            .into_iter()
            .map(|market| (market.raw_symbol.clone(), market))
            .collect::<BTreeMap<_, _>>();
        let mut filtered = Vec::with_capacity(symbols.len());
        for symbol in symbols {
            let Some(market) = by_raw.remove(symbol) else {
                return Err(BackfillError::InvalidArgs(format!(
                    "unknown Binance symbol in --symbols: {symbol}"
                )));
            };
            filtered.push(market);
        }
        markets = filtered;
    } else if markets.len() != args.expect_symbol_count {
        return Err(BackfillError::InvalidConfig(format!(
            "expected {} enabled Binance symbols, found {}",
            args.expect_symbol_count,
            markets.len()
        )));
    }

    let rest_base_url = args
        .rest_base_url
        .clone()
        .unwrap_or_else(|| exchange.rest_base_url.clone());
    Ok(ResolvedBinanceBackfill {
        rest_base_url,
        markets,
    })
}
