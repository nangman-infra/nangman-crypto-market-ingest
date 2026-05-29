use super::super::UpbitIngestError;
use super::super::stats::UpbitIngestWatchStats;
use super::super::universe::UpbitMarket;
use super::control::{record_reconnect, sleep_or_shutdown, spawn_shutdown_flag};
use super::session::run_session;
use super::subscription::subscription_message;
use super::types::SessionEnd;
use crate::reconnect::ReconnectPolicy;
use crate::storage::L0StorageSink;
use futures_util::SinkExt;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite};

pub(in crate::upbit) async fn watch_upbit_ingest_streams(
    websocket_url: &str,
    markets: &[UpbitMarket],
    orderbook_unit: u8,
    duration: Duration,
    log_interval: Duration,
    mut storage: Option<&mut L0StorageSink>,
    log_callback: impl Fn(&UpbitIngestWatchStats),
) -> Result<UpbitIngestWatchStats, UpbitIngestError> {
    install_rustls_crypto_provider();

    let planned_stream_count = markets.len() * 3;
    let markets_by_code = markets
        .iter()
        .map(|market| (market.market.clone(), market.clone()))
        .collect::<HashMap<_, _>>();
    let mut stats = UpbitIngestWatchStats::new(websocket_url.to_owned(), planned_stream_count);
    let shutdown_flag = spawn_shutdown_flag()?;

    let policy = ReconnectPolicy::default_24x7();
    let mut current_backoff = policy.initial_backoff;
    let deadline = Instant::now() + duration;

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

        let mut websocket = match connect_async(websocket_url).await {
            Ok((ws, _)) => ws,
            Err(_) => {
                record_reconnect(&mut stats, "connect_failed");
                sleep_or_shutdown(&shutdown_flag, current_backoff).await;
                current_backoff = policy.next_backoff(current_backoff);
                continue;
            }
        };

        if let Err(_error) = websocket
            .send(tungstenite::Message::Text(
                subscription_message(markets, orderbook_unit).into(),
            ))
            .await
        {
            record_reconnect(&mut stats, "subscribe_failed");
            sleep_or_shutdown(&shutdown_flag, current_backoff).await;
            current_backoff = policy.next_backoff(current_backoff);
            continue;
        }

        let session_outcome = run_session(
            websocket,
            &mut stats,
            storage.as_deref_mut(),
            &markets_by_code,
            deadline,
            log_interval,
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
