use super::super::env::{env_bool, env_string};
use super::super::{
    Args, DEFAULT_AWS_REGION, DEFAULT_BINANCE_DERIVATIVES_SNAPSHOT_INTERVAL_SECONDS,
    DEFAULT_BINANCE_FUTURES_REST_BASE_URL, DEFAULT_CONFIG_DIR, DEFAULT_EMERGENCY_PCT,
    DEFAULT_EVICTION_CHECK_INTERVAL_SECS, DEFAULT_HIGH_WATER_PCT, DEFAULT_L0_SPOOL_ROOT,
    DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS, DEFAULT_S3_RETENTION_DAYS,
    DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN, DEFAULT_SAFETY_FLOOR_HOURS, Venue,
};
use crate::live::{DEFAULT_MARKET_LIVE_NATS_STREAM, DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX};
use std::path::PathBuf;

pub(super) fn default_args() -> Args {
    Args {
        venue: Venue::Binance,
        config_dir: PathBuf::from(DEFAULT_CONFIG_DIR),
        duration_seconds: 15,
        log_interval_seconds: 5,
        depth_snapshot_limit: 100,
        expect_symbol_count: 50,
        allow_partial_symbol_coverage: false,
        binance_futures_rest_base_url: DEFAULT_BINANCE_FUTURES_REST_BASE_URL.to_owned(),
        binance_derivatives_snapshot_interval_seconds:
            DEFAULT_BINANCE_DERIVATIVES_SNAPSHOT_INTERVAL_SECONDS,
        upbit_rest_base_url: None,
        upbit_websocket_url: None,
        upbit_quote_currency: "KRW".to_owned(),
        upbit_orderbook_unit: 5,
        l0_s3_bucket: None,
        aws_profile: None,
        aws_region: DEFAULT_AWS_REGION.to_owned(),
        l0_spool_root: PathBuf::from(DEFAULT_L0_SPOOL_ROOT),
        l0_flush_records: 1_000,
        l0_shard_count: 1,
        local_disk_high_water_pct: DEFAULT_HIGH_WATER_PCT,
        local_disk_emergency_pct: DEFAULT_EMERGENCY_PCT,
        safety_floor_hours: DEFAULT_SAFETY_FLOOR_HOURS,
        eviction_check_interval_secs: DEFAULT_EVICTION_CHECK_INTERVAL_SECS,
        s3_retention_enabled: true,
        s3_retention_days: DEFAULT_S3_RETENTION_DAYS,
        s3_retention_check_interval_secs: DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS,
        s3_retention_max_deletes_per_run: DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN,
        live_nats_url: env_string("MARKET_LIVE_NATS_URL"),
        live_nats_stream: env_string("MARKET_LIVE_NATS_STREAM")
            .unwrap_or_else(|| DEFAULT_MARKET_LIVE_NATS_STREAM.to_owned()),
        live_nats_subject_prefix: env_string("MARKET_LIVE_NATS_SUBJECT_PREFIX")
            .unwrap_or_else(|| DEFAULT_MARKET_LIVE_NATS_SUBJECT_PREFIX.to_owned()),
        live_nats_required: env_bool("MARKET_LIVE_NATS_REQUIRED"),
    }
}
