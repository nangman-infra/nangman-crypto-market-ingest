use super::{
    BackfillArgs, BackfillError, BackfillRunReport, SymbolBackfillReport, unix_timestamp_ms,
};
use crate::binance::BinanceMarket;
use crate::config::load_market_ingest_config;
use crate::log_stream;
use crate::storage::gap::GapAlertDraft;
use crate::storage::health::SourceHealthDraft;
use crate::storage::record::RawMarketEventDraft;
use crate::storage::symbol_health::SymbolHealthDraft;
use crate::storage::{L0StorageSink, StorageReport};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;

const BINANCE_PAGE_LIMIT: usize = 1_000;

#[derive(Debug, Deserialize, Clone)]
struct AggTrade {
    #[serde(rename = "a")]
    aggregate_trade_id: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "f")]
    first_trade_id: i64,
    #[serde(rename = "l")]
    last_trade_id: i64,
    #[serde(rename = "T")]
    trade_timestamp_ms: i64,
    #[serde(rename = "m")]
    is_buyer_maker: bool,
    #[serde(rename = "M")]
    is_best_match: bool,
}

pub async fn run(
    args: &BackfillArgs,
    sink: &mut L0StorageSink,
) -> Result<BackfillRunReport, BackfillError> {
    let resolved = resolve_markets(args)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    log_stream::info(
        "market_backfill_start",
        json!({
            "venue": "binance",
            "source_role": "reference",
            "symbol_count": resolved.markets.len(),
            "input_start_ms": args.input_start_ms,
            "input_end_ms": args.input_end_ms,
            "rest_base_url": resolved.rest_base_url,
            "mode": "historical_trade_backfill"
        }),
    )
    .map_err(BackfillError::Json)?;

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

    let observed_at_ms = unix_timestamp_ms();
    let total_record_count = symbols
        .iter()
        .map(|symbol| symbol.record_count)
        .sum::<u64>();
    let total_gap_alert_count = symbols
        .iter()
        .map(|symbol| symbol.gap_alert_count)
        .sum::<u64>();
    append_symbol_health(sink, &symbols, observed_at_ms).await?;
    append_source_health(
        sink,
        observed_at_ms,
        args,
        resolved.markets.len(),
        total_record_count,
        total_gap_alert_count,
    )
    .await?;

    Ok(BackfillRunReport {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        input_start_ms: args.input_start_ms,
        input_end_ms: args.input_end_ms,
        requested_symbol_count: args
            .symbols
            .as_ref()
            .map(|symbols| symbols.len())
            .unwrap_or(args.expect_symbol_count),
        processed_symbol_count: symbols.len(),
        total_record_count,
        total_gap_alert_count,
        symbols,
        storage: empty_storage_report(),
    })
}

struct ResolvedBinanceBackfill {
    rest_base_url: String,
    markets: Vec<BinanceMarket>,
}

fn resolve_markets(args: &BackfillArgs) -> Result<ResolvedBinanceBackfill, BackfillError> {
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

async fn backfill_symbol(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &BinanceMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
) -> Result<SymbolBackfillReport, BackfillError> {
    let mut report = SymbolBackfillReport::empty(&market.raw_symbol, &market.base_asset);
    let first_page = fetch_agg_trades_by_time(
        client,
        rest_base_url,
        &market.raw_symbol,
        input_start_ms,
        input_end_ms,
    )
    .await?;
    if first_page.is_empty() {
        report.note_gap();
        append_empty_gap_alert(
            sink,
            "binance",
            "reference",
            &market.raw_symbol,
            input_start_ms,
            input_end_ms,
        )
        .await?;
        return Ok(report);
    }

    let mut stop = append_page(
        sink,
        market,
        input_start_ms,
        input_end_ms,
        &first_page,
        &mut report,
    )
    .await?;
    let mut next_from_id = first_page
        .last()
        .map(|trade| trade.aggregate_trade_id + 1)
        .unwrap_or_default();
    let mut last_page_was_full = first_page.len() == BINANCE_PAGE_LIMIT;

    while !stop && last_page_was_full {
        let page =
            fetch_agg_trades_from_id(client, rest_base_url, &market.raw_symbol, next_from_id)
                .await?;
        if page.is_empty() {
            break;
        }
        stop = append_page(
            sink,
            market,
            input_start_ms,
            input_end_ms,
            &page,
            &mut report,
        )
        .await?;
        let next_candidate = page
            .last()
            .map(|trade| trade.aggregate_trade_id + 1)
            .unwrap_or(next_from_id);
        if next_candidate <= next_from_id {
            return Err(BackfillError::InvalidConfig(format!(
                "Binance aggTrades cursor did not advance for {}",
                market.raw_symbol
            )));
        }
        next_from_id = next_candidate;
        last_page_was_full = page.len() == BINANCE_PAGE_LIMIT;
    }

    if report.record_count == 0 {
        report.note_gap();
        append_empty_gap_alert(
            sink,
            "binance",
            "reference",
            &market.raw_symbol,
            input_start_ms,
            input_end_ms,
        )
        .await?;
    }
    Ok(report)
}

async fn append_page(
    sink: &mut L0StorageSink,
    market: &BinanceMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    page: &[AggTrade],
    report: &mut SymbolBackfillReport,
) -> Result<bool, BackfillError> {
    for trade in page {
        if trade.trade_timestamp_ms > input_end_ms {
            return Ok(true);
        }
        if trade.trade_timestamp_ms < input_start_ms {
            continue;
        }
        sink.append_raw_market_event(raw_trade_draft(market, trade)?)
            .await
            .map_err(|error| BackfillError::Storage(error.to_string()))?;
        report.observe(trade.trade_timestamp_ms);
    }
    Ok(false)
}

async fn fetch_agg_trades_by_time(
    client: &reqwest::Client,
    rest_base_url: &str,
    symbol: &str,
    input_start_ms: i64,
    input_end_ms: i64,
) -> Result<Vec<AggTrade>, BackfillError> {
    client
        .get(format!(
            "{}/api/v3/aggTrades",
            rest_base_url.trim_end_matches('/')
        ))
        .query(&[
            ("symbol", symbol.to_owned()),
            ("startTime", input_start_ms.to_string()),
            ("endTime", input_end_ms.to_string()),
            ("limit", BINANCE_PAGE_LIMIT.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<AggTrade>>()
        .await
        .map_err(BackfillError::from)
}

async fn fetch_agg_trades_from_id(
    client: &reqwest::Client,
    rest_base_url: &str,
    symbol: &str,
    from_id: i64,
) -> Result<Vec<AggTrade>, BackfillError> {
    client
        .get(format!(
            "{}/api/v3/aggTrades",
            rest_base_url.trim_end_matches('/')
        ))
        .query(&[
            ("symbol", symbol.to_owned()),
            ("fromId", from_id.to_string()),
            ("limit", BINANCE_PAGE_LIMIT.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<AggTrade>>()
        .await
        .map_err(BackfillError::from)
}

fn raw_trade_draft(
    market: &BinanceMarket,
    trade: &AggTrade,
) -> Result<RawMarketEventDraft, BackfillError> {
    let payload = json!({
        "data": {
            "e": "trade",
            "E": trade.trade_timestamp_ms,
            "s": market.raw_symbol,
            "a": trade.aggregate_trade_id,
            "f": trade.first_trade_id,
            "l": trade.last_trade_id,
            "T": trade.trade_timestamp_ms,
            "p": trade.price,
            "q": trade.quantity,
            "m": trade.is_buyer_maker,
            "M": trade.is_best_match,
            "source": "aggTrades"
        }
    });
    let sequence_tag = format!("binance:trade:{}", trade.aggregate_trade_id);
    Ok(RawMarketEventDraft {
        event_type: "trade".to_owned(),
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.raw_symbol.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: trade.trade_timestamp_ms,
        ingest_timestamp_ms: unix_timestamp_ms(),
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: Some(trade.aggregate_trade_id),
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: false,
        stream_type: "HISTORICAL_REST".to_owned(),
        stream_phase: "backfill".to_owned(),
        payload_json: serde_json::to_string(&payload)?,
    })
}

async fn append_empty_gap_alert(
    sink: &mut L0StorageSink,
    venue: &str,
    source_role: &str,
    symbol_native: &str,
    input_start_ms: i64,
    input_end_ms: i64,
) -> Result<(), BackfillError> {
    sink.append_gap_alert(GapAlertDraft {
        venue: venue.to_owned(),
        source_role: source_role.to_owned(),
        symbol_native: symbol_native.to_owned(),
        gap_type: "historical_range_empty".to_owned(),
        detected_at_ms: unix_timestamp_ms(),
        expected_sequence_id: None,
        observed_sequence_id: None,
        heal_action: "review_range_or_source".to_owned(),
        heal_status: "open".to_owned(),
        payload_json: serde_json::to_string(&json!({
            "input_start_ms": input_start_ms,
            "input_end_ms": input_end_ms,
            "reason": "no_trade_records_returned"
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}

async fn append_symbol_health(
    sink: &mut L0StorageSink,
    symbols: &[SymbolBackfillReport],
    observed_at_ms: i64,
) -> Result<(), BackfillError> {
    for symbol in symbols {
        let last_event_time_ms = symbol.last_event_time_ms.unwrap_or(0);
        sink.append_symbol_health(SymbolHealthDraft {
            venue: "binance".to_owned(),
            symbol_native: symbol.symbol_native.clone(),
            observed_at_ms,
            last_event_time_ms,
            latency_ms: observed_at_ms.saturating_sub(last_event_time_ms).max(0),
            is_tradeable: symbol.record_count > 0,
            reason_codes: if symbol.record_count > 0 {
                Vec::new()
            } else {
                vec!["no_historical_trades".to_owned()]
            },
        })
        .await
        .map_err(|error| BackfillError::Storage(error.to_string()))?;
    }
    Ok(())
}

async fn append_source_health(
    sink: &mut L0StorageSink,
    observed_at_ms: i64,
    args: &BackfillArgs,
    symbol_count: usize,
    total_record_count: u64,
    total_gap_alert_count: u64,
) -> Result<(), BackfillError> {
    sink.append_source_health(SourceHealthDraft {
        venue: "binance".to_owned(),
        source_role: "reference".to_owned(),
        observed_at_ms,
        connection_status: "historical_backfill_completed".to_owned(),
        heartbeat_delay_ms: 0,
        stream_lag_ms: observed_at_ms.saturating_sub(args.input_end_ms).max(0),
        recent_gap_count: total_gap_alert_count,
        book_rebuild_count: 0,
        health_level: if total_gap_alert_count == 0 {
            "ok"
        } else {
            "warn"
        }
        .to_owned(),
        payload_json: serde_json::to_string(&json!({
            "mode": "historical_trade_backfill",
            "symbol_count": symbol_count,
            "input_start_ms": args.input_start_ms,
            "input_end_ms": args.input_end_ms,
            "record_count": total_record_count,
            "gap_alert_count": total_gap_alert_count
        }))?,
    })
    .await
    .map_err(|error| BackfillError::Storage(error.to_string()))
}

fn empty_storage_report() -> StorageReport {
    StorageReport {
        bucket: String::new(),
        run_id: String::new(),
        record_count: 0,
        uploaded_object_count: 0,
        uploaded_object_retained_count: 0,
        uploaded_object_dropped_count: 0,
        uploaded_objects: Vec::new(),
        failed_upload_count: 0,
        failed_upload_retained_count: 0,
        failed_upload_dropped_count: 0,
        failed_uploads: Vec::new(),
        manifest_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_market() -> BinanceMarket {
        BinanceMarket {
            raw_symbol: "BTCUSDT".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "USDT".to_owned(),
        }
    }

    #[test]
    fn raw_trade_payload_matches_normalizer_shape() {
        let trade = AggTrade {
            aggregate_trade_id: 42,
            price: "100.25".to_owned(),
            quantity: "0.50".to_owned(),
            first_trade_id: 7,
            last_trade_id: 8,
            trade_timestamp_ms: 1234,
            is_buyer_maker: true,
            is_best_match: true,
        };
        let draft = raw_trade_draft(&sample_market(), &trade).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&draft.payload_json).unwrap();
        assert_eq!(payload["data"]["p"], "100.25");
        assert_eq!(payload["data"]["q"], "0.50");
        assert_eq!(payload["data"]["m"], true);
        assert_eq!(draft.stream_type, "HISTORICAL_REST");
    }
}
