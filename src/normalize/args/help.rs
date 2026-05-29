pub(super) fn print_help() {
    println!(
        r#"market-normalize
Usage:
  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix> \
    --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
    --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
    --aws-profile market-ingest-roles-anywhere

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix> \
    --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
    --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
    --input-start-ms 1778042400000 \
    --input-end-ms 1778043300000

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix> \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
    --preflight

  market-normalize \
    --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix> \
    --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
    --audit-l1-index-start-ms 1778042400000 \
    --audit-l1-index-end-ms 1778043300000

Without an explicit input range, market-normalize runs as a long-lived worker.
Each tick decides LIVE / CATCH-UP from the most recent successful L1 manifest
and the watermark, then processes up to --max-windows-per-tick contiguous
windows before sleeping for --schedule-interval-ms. Events with
ingest_timestamp_ms - exchange_timestamp_ms greater than --max-latency-ms are
counted as delayed. L0 object candidates are selected from event_date/hour
partitions; row exchange_timestamp_ms performs the exact time filter. run_id is
a producer execution id and is not used as an object coverage interval.
--l0-run-key-overlap-ms is accepted for compatibility but ignored by current
input discovery. --live-priority processes the latest closed watermark window
first when sequential catch-up lags by at least
--live-priority-lag-threshold-ms, then continues the same tick with contiguous
catch-up work. --live-priority-only is for the supervisor bootstrap hot path:
it seeds at most the latest closed watermark window and never consumes
historical catch-up work. With an explicit range, BACKFILL mode is one-shot.
--preflight and --audit-l1-index-* are also one-shot. S3 retention cleanup is
app-owned for both L0 and L1 buckets in long-lived worker mode. L0 defaults to
45 days; L1 defaults to 240 days. Bucket lifecycle remains only a fallback
safety net. --l1-index-upload-concurrency controls only L1 index pointer
publishing and defaults to 1."#
    );
}
