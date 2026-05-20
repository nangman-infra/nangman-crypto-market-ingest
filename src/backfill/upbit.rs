use super::{
    BackfillArgs, BackfillError, BackfillRunReport, SourceHealthSummary, SymbolBackfillReport,
    append_empty_gap_alert, append_source_health_for, append_symbol_health_for,
    empty_storage_report,
};
use crate::clock;
use crate::log_stream;
use crate::storage::L0StorageSink;
use crate::storage::record::RawMarketEventDraft;
use crate::upbit::fetch_top_krw_markets;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

const UPBIT_PAGE_LIMIT: usize = 200;
const UPBIT_RECENT_WINDOW_DAYS: i64 = 7;

#[derive(Debug, Clone)]
struct UpbitBackfillMarket {
    market: String,
    base_asset: String,
    quote_asset: String,
}

#[derive(Debug, Deserialize, Clone)]
struct UpbitTrade {
    market: String,
    timestamp: i64,
    trade_price: f64,
    trade_volume: f64,
    ask_bid: String,
    sequential_id: i64,
}

#[derive(Debug, Clone)]
struct UpbitInitialCursor {
    to: String,
    days_ago: Option<i64>,
}

pub async fn run(
    args: &BackfillArgs,
    sink: &mut L0StorageSink,
) -> Result<BackfillRunReport, BackfillError> {
    validate_recent_window(args.input_start_ms, args.input_end_ms)?;
    let resolved = resolve_markets(args).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    log_stream::info(
        "market_backfill_start",
        json!({
            "venue": "upbit",
            "source_role": "execution",
            "symbol_count": resolved.len(),
            "input_start_ms": args.input_start_ms,
            "input_end_ms": args.input_end_ms,
            "quote_currency": args.upbit_quote_currency,
            "mode": "recent_trade_repair"
        }),
    )
    .map_err(BackfillError::Json)?;

    let rest_base_url = resolve_rest_base_url(args).await?;
    let mut symbols = Vec::with_capacity(resolved.len());
    for market in &resolved {
        symbols.push(
            backfill_symbol(
                &client,
                &rest_base_url,
                market,
                args.input_start_ms,
                args.input_end_ms,
                sink,
            )
            .await?,
        );
    }

    let observed_at_ms = clock::now_ms();
    let total_record_count = symbols
        .iter()
        .map(|symbol| symbol.record_count)
        .sum::<u64>();
    let total_gap_alert_count = symbols
        .iter()
        .map(|symbol| symbol.gap_alert_count)
        .sum::<u64>();
    append_symbol_health_for(sink, "upbit", &symbols, observed_at_ms).await?;
    append_source_health_for(
        sink,
        SourceHealthSummary {
            venue: "upbit",
            source_role: "execution",
            mode: "recent_trade_repair",
            observed_at_ms,
            args,
            symbol_count: resolved.len(),
            total_record_count,
            total_gap_alert_count,
        },
    )
    .await?;

    Ok(BackfillRunReport {
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
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

async fn resolve_rest_base_url(args: &BackfillArgs) -> Result<String, BackfillError> {
    if let Some(url) = &args.rest_base_url {
        return Ok(url.clone());
    }
    let config = crate::config::load_market_ingest_config(&args.config_dir)
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    let exchange = config
        .enabled_exchange("upbit")
        .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    Ok(exchange.rest_base_url.clone())
}

async fn resolve_markets(args: &BackfillArgs) -> Result<Vec<UpbitBackfillMarket>, BackfillError> {
    if args.upbit_quote_currency != "KRW" {
        return Err(BackfillError::InvalidArgs(
            "Upbit historical backfill only supports KRW quote markets".to_owned(),
        ));
    }
    if let Some(symbols) = &args.symbols {
        return symbols
            .iter()
            .map(|symbol| parse_explicit_market(symbol, &args.upbit_quote_currency))
            .collect();
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let rest_base_url = resolve_rest_base_url(args).await?;
    let markets = fetch_top_krw_markets(
        &client,
        &rest_base_url,
        &args.upbit_quote_currency,
        args.expect_symbol_count,
    )
    .await
    .map_err(|error| BackfillError::InvalidConfig(error.to_string()))?;
    Ok(markets
        .into_iter()
        .map(|market| UpbitBackfillMarket {
            market: market.market,
            base_asset: market.base_asset,
            quote_asset: market.quote_asset,
        })
        .collect())
}

fn parse_explicit_market(
    value: &str,
    expected_quote: &str,
) -> Result<UpbitBackfillMarket, BackfillError> {
    let Some((quote, base)) = value.split_once('-') else {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit symbol must look like KRW-BTC, got {value}"
        )));
    };
    if quote != expected_quote {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit symbol {value} must use {expected_quote} quote"
        )));
    }
    Ok(UpbitBackfillMarket {
        market: value.to_owned(),
        base_asset: base.to_owned(),
        quote_asset: quote.to_owned(),
    })
}

async fn backfill_symbol(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &UpbitBackfillMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
) -> Result<SymbolBackfillReport, BackfillError> {
    let mut report = SymbolBackfillReport::empty(&market.market, &market.base_asset);
    let initial_cursor = initial_cursor(input_end_ms)?;
    let mut next_cursor = None;
    let mut first_page = true;

    loop {
        let page = fetch_trade_page(
            client,
            rest_base_url,
            &market.market,
            &initial_cursor,
            next_cursor,
            first_page,
        )
        .await?;
        if page.is_empty() {
            break;
        }

        if append_page_trades(
            &page,
            market,
            input_start_ms,
            input_end_ms,
            sink,
            &mut report,
        )
        .await?
        {
            break;
        }

        next_cursor = advance_cursor(&page, next_cursor, &market.market)?;
        first_page = false;
        if page.len() < UPBIT_PAGE_LIMIT {
            break;
        }
    }

    if report.record_count == 0 {
        report.note_gap();
        append_empty_gap_alert(
            sink,
            "upbit",
            "execution",
            &market.market,
            input_start_ms,
            input_end_ms,
            "no_trade_records_returned",
        )
        .await?;
    }
    Ok(report)
}

async fn append_page_trades(
    page: &[UpbitTrade],
    market: &UpbitBackfillMarket,
    input_start_ms: i64,
    input_end_ms: i64,
    sink: &mut L0StorageSink,
    report: &mut SymbolBackfillReport,
) -> Result<bool, BackfillError> {
    let mut reached_start = false;
    for trade in page {
        if trade.timestamp > input_end_ms {
            continue;
        }
        if trade.timestamp < input_start_ms {
            reached_start = true;
            continue;
        }
        sink.append_raw_market_event(raw_trade_draft(market, trade)?)
            .await
            .map_err(|error| BackfillError::Storage(error.to_string()))?;
        report.observe(trade.timestamp);
    }
    Ok(reached_start)
}

fn advance_cursor(
    page: &[UpbitTrade],
    current_cursor: Option<i64>,
    market: &str,
) -> Result<Option<i64>, BackfillError> {
    let Some(last_trade) = page.last() else {
        return Ok(current_cursor);
    };
    let next_cursor = last_trade.sequential_id;
    if current_cursor == Some(next_cursor) {
        return Err(BackfillError::InvalidConfig(format!(
            "Upbit cursor did not advance for {market}"
        )));
    }
    Ok(Some(next_cursor))
}

async fn fetch_trade_page(
    client: &reqwest::Client,
    rest_base_url: &str,
    market: &str,
    initial_cursor: &UpbitInitialCursor,
    cursor: Option<i64>,
    first_page: bool,
) -> Result<Vec<UpbitTrade>, BackfillError> {
    let mut request = client
        .get(format!(
            "{}/v1/trades/ticks",
            rest_base_url.trim_end_matches('/')
        ))
        .query(&[
            ("market", market.to_owned()),
            ("count", UPBIT_PAGE_LIMIT.to_string()),
        ]);
    if first_page {
        request = request.query(&[("to", initial_cursor.to.clone())]);
        if let Some(days_ago) = initial_cursor.days_ago {
            request = request.query(&[("days_ago", days_ago.to_string())]);
        }
    }
    if let Some(cursor) = cursor {
        request = request.query(&[("cursor", cursor.to_string())]);
    }
    request
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<UpbitTrade>>()
        .await
        .map_err(BackfillError::from)
}

fn initial_cursor(input_end_ms: i64) -> Result<UpbitInitialCursor, BackfillError> {
    let end = datetime_from_ms(input_end_ms)?;
    let now = Utc::now();
    let days_ago = now
        .date_naive()
        .signed_duration_since(end.date_naive())
        .num_days();
    if !(0..=UPBIT_RECENT_WINDOW_DAYS).contains(&days_ago) {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit end range must be within the most recent {} UTC days",
            UPBIT_RECENT_WINDOW_DAYS
        )));
    }
    Ok(UpbitInitialCursor {
        to: end.format("%H:%M:%S").to_string(),
        days_ago: if days_ago == 0 { None } else { Some(days_ago) },
    })
}

fn validate_recent_window(input_start_ms: i64, input_end_ms: i64) -> Result<(), BackfillError> {
    let now = Utc::now();
    let start = datetime_from_ms(input_start_ms)?;
    let end = datetime_from_ms(input_end_ms)?;
    if end > now {
        return Err(BackfillError::InvalidArgs(
            "Upbit end range must not be in the future".to_owned(),
        ));
    }
    if start < now - ChronoDuration::days(UPBIT_RECENT_WINDOW_DAYS) {
        return Err(BackfillError::InvalidArgs(format!(
            "Upbit recent trade backfill only supports the most recent {} days",
            UPBIT_RECENT_WINDOW_DAYS
        )));
    }
    if start >= end {
        return Err(BackfillError::InvalidArgs(
            "Upbit start range must be earlier than end range".to_owned(),
        ));
    }
    Ok(())
}

fn datetime_from_ms(value: i64) -> Result<DateTime<Utc>, BackfillError> {
    DateTime::from_timestamp_millis(value).ok_or_else(|| {
        BackfillError::InvalidArgs(format!("timestamp {value} is outside supported range"))
    })
}

fn raw_trade_draft(
    market: &UpbitBackfillMarket,
    trade: &UpbitTrade,
) -> Result<RawMarketEventDraft, BackfillError> {
    let payload = json!({
        "type": "trade",
        "code": trade.market,
        "timestamp": trade.timestamp,
        "trade_timestamp": trade.timestamp,
        "trade_price": trade.trade_price,
        "trade_volume": trade.trade_volume,
        "ask_bid": trade.ask_bid,
        "sequential_id": trade.sequential_id,
        "stream_type": "BACKFILL"
    });
    let sequence_tag = format!("upbit:trade:{}", trade.sequential_id);
    Ok(RawMarketEventDraft {
        event_type: "trade".to_owned(),
        venue: "upbit".to_owned(),
        source_role: "execution".to_owned(),
        market_type: "spot".to_owned(),
        symbol_native: market.market.clone(),
        symbol_canonical: market.base_asset.clone(),
        base_asset: market.base_asset.clone(),
        quote_asset: market.quote_asset.clone(),
        exchange_timestamp_ms: trade.timestamp,
        ingest_timestamp_ms: clock::now_ms(),
        sequence_id: sequence_tag.clone(),
        sequence_tag,
        exchange_sequence: Some(trade.sequential_id),
        diff_first_update_id: None,
        diff_final_update_id: None,
        is_snapshot: false,
        stream_type: "HISTORICAL_REST".to_owned(),
        stream_phase: "backfill".to_owned(),
        payload_json: serde_json::to_string(&payload)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn initial_cursor_uses_days_ago_for_prior_day() {
        let now = Utc::now();
        let end = (now - ChronoDuration::days(1))
            .with_hour(8)
            .unwrap()
            .with_minute(10)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        let cursor = initial_cursor(end.timestamp_millis()).unwrap();
        assert_eq!(cursor.to, "08:10:00");
        assert_eq!(cursor.days_ago, Some(1));
    }

    #[test]
    fn rejects_range_older_than_recent_window() {
        let now = Utc::now();
        let start = (now - ChronoDuration::days(8)).timestamp_millis();
        let end = (now - ChronoDuration::days(7)).timestamp_millis();
        let err = validate_recent_window(start, end).err().unwrap();
        assert!(err.to_string().contains("most recent 7 days"));
    }

    #[test]
    fn raw_trade_payload_matches_normalizer_shape() {
        let market = UpbitBackfillMarket {
            market: "KRW-BTC".to_owned(),
            base_asset: "BTC".to_owned(),
            quote_asset: "KRW".to_owned(),
        };
        let trade = UpbitTrade {
            market: "KRW-BTC".to_owned(),
            timestamp: 1234,
            trade_price: 100.0,
            trade_volume: 0.25,
            ask_bid: "BID".to_owned(),
            sequential_id: 99,
        };
        let draft = raw_trade_draft(&market, &trade).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&draft.payload_json).unwrap();
        assert_eq!(payload["trade_price"], 100.0);
        assert_eq!(payload["trade_volume"], 0.25);
        assert_eq!(payload["ask_bid"], "BID");
    }
}
