use super::super::{BinanceIngestError, BinanceMarket, stats::BinanceL0WatchStats};
use super::control::{record_reconnect, sleep_or_shutdown, spawn_shutdown_flag};
use super::session::run_session;
use super::types::{BinanceL0WatchConfig, SessionEnd};
use crate::reconnect::ReconnectPolicy;
use crate::storage::L0StorageSink;
use crypto_market_data::{BinanceStreamConfig, BinanceStreamKind};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;

pub(in crate::binance) async fn watch_binance_l0_streams(
    stream_config: &BinanceStreamConfig,
    markets: &[BinanceMarket],
    stream_kinds: &[BinanceStreamKind],
    runtime: BinanceL0WatchConfig<'_>,
    mut storage: Option<&mut L0StorageSink>,
    log_callback: impl Fn(&BinanceL0WatchStats),
) -> Result<BinanceL0WatchStats, BinanceIngestError> {
    install_rustls_crypto_provider();

    let url = stream_config.combined_stream_url_for_kinds(stream_kinds)?;
    let planned_stream_count = stream_config
        .combined_stream_names_for_kinds(stream_kinds)?
        .len();
    let markets_by_raw = markets
        .iter()
        .map(|market| (market.raw_symbol.to_ascii_uppercase(), market.clone()))
        .collect::<HashMap<_, _>>();
    let mut stats = BinanceL0WatchStats::new(url.clone(), planned_stream_count);
    let shutdown_flag = spawn_shutdown_flag()?;

    let policy = ReconnectPolicy::default_24x7();
    let mut current_backoff = policy.initial_backoff;
    let deadline = Instant::now() + runtime.duration;

    loop {
        if shutdown_flag.load(Ordering::SeqCst) {
            stats.source_health_status = "shutdown".to_owned();
            stats.source_health_events += 1;
            break;
        }
        if Instant::now() >= deadline {
            stats.source_health_status = "deadline_reached".to_owned();
            stats.source_health_events += 1;
            break;
        }

        let websocket = match connect_async(&url).await {
            Ok((ws, _)) => ws,
            Err(_) => {
                record_reconnect(&mut stats, "connect_failed");
                sleep_or_shutdown(&shutdown_flag, current_backoff).await;
                current_backoff = policy.next_backoff(current_backoff);
                continue;
            }
        };
        // Backoff resets only after a healthy session (stats event observed).
        let session_outcome = run_session(
            websocket,
            &mut stats,
            storage.as_deref_mut(),
            &markets_by_raw,
            markets,
            deadline,
            runtime.log_interval,
            runtime.derivative_snapshot_interval,
            runtime.futures_rest_base_url,
            policy.stale_message_timeout,
            &log_callback,
            &shutdown_flag,
        )
        .await?;

        match session_outcome {
            SessionEnd::ShutdownRequested => {
                stats.source_health_status = "shutdown".to_owned();
                stats.source_health_events += 1;
                break;
            }
            SessionEnd::DeadlineReached => {
                stats.source_health_status = "deadline_reached".to_owned();
                stats.source_health_events += 1;
                break;
            }
            SessionEnd::Disconnected(reason) => {
                record_reconnect(&mut stats, reason);
                sleep_or_shutdown(&shutdown_flag, current_backoff).await;
                current_backoff = policy.next_backoff(current_backoff);
            }
        }
    }

    stats.update_health();
    Ok(stats)
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
