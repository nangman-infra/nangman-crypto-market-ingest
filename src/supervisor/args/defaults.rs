pub(super) const DEFAULT_CONFIG_DIR: &str =
    "/opt/nangman-crypto/strategies/crypto/rust-engine/config";
pub(super) const DEFAULT_L0_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l0";
pub(super) const DEFAULT_L1_SPOOL_ROOT: &str = "/opt/nangman-crypto/data/spool/market-ingest/l1";
pub(super) const DEFAULT_CATCHUP_TMP_ROOT: &str =
    "/opt/nangman-crypto/data/spool/market-normalize/catchup";
pub(super) const DEFAULT_REALTIME_BIN: &str = "/usr/local/bin/market-ingest-app";
pub(super) const DEFAULT_BACKFILL_BIN: &str = "/usr/local/bin/market-backfill";
pub(super) const DEFAULT_NORMALIZE_BIN: &str = "/usr/local/bin/market-normalize";
pub(super) const DEFAULT_AWS_REGION: &str = "ap-northeast-2";
pub(super) const DEFAULT_L0_S3_BUCKET: &str =
    "nangman-crypto-dev-market-ingest-l0-<account-suffix>";
pub(super) const DEFAULT_L1_S3_BUCKET: &str =
    "nangman-crypto-dev-market-ingest-l1-<account-suffix>";
pub(super) const DEFAULT_RESTART_DELAY_SECS: u64 = 15;
pub(super) const DEFAULT_BOOTSTRAP_LOOKBACK_DAYS: i64 = 210;
pub(super) const DEFAULT_BOOTSTRAP_CHUNK_HOURS: i64 = 24;
pub(super) const DEFAULT_BOOTSTRAP_INTERVAL_SECS: u64 = 60;
pub(super) const DEFAULT_REALTIME_DURATION_SECONDS: u64 = 31_536_000;
pub(super) const DEFAULT_L0_RUN_KEY_OVERLAP_MS: i64 = 360_000;
pub(super) const DEFAULT_L0_S3_RETENTION_DAYS: i64 = 45;
pub(super) const DEFAULT_L1_S3_RETENTION_DAYS: i64 = 240;
pub(super) const DEFAULT_S3_RETENTION_CHECK_INTERVAL_SECS: u64 = 21_600;
pub(super) const DEFAULT_S3_RETENTION_MAX_DELETES_PER_RUN: usize = 1_000;
