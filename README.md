# market-ingest-app

Public market data receive smoke for the on-prem runtime spine.

This app only reads public market streams:

- Binance reference truth
  - `trade`
  - `bookTicker`
  - `ticker`
  - `depth@100ms`
- Upbit execution truth
  - `ticker`
  - `trade`
  - `orderbook`
  - `book_ticker` is derived later from the first orderbook unit; L0 stores the
    original orderbook payload once.

Binance fetches REST order book snapshots from `/api/v3/depth` to align diff depth
updates into local order books. Upbit derives Top50 KRW symbols from `/v1/market/all`
and `/v1/ticker/all?quote_currencies=KRW`, then receives all Top50 symbols in one
public WebSocket subscription.

Binance Top50 uses the checked-in
`/opt/nangman-crypto/strategies/crypto/rust-engine/config/universe.major-50.toml`
receive universe. It is generated from Binance public `/api/v3/exchangeInfo` and
`/api/v3/ticker/24hr` data by selecting USDT spot symbols with
`status=TRADING` and `isSpotTradingAllowed=true`, sorting by 24h `quoteVolume`
descending, and taking the first 50. Binance does not expose a separate warning
flag in this public symbol response; warning/monitoring-tag symbols must be
manually disabled in the universe file when needed.

It does not use private APIs, credentials, AI hot-path decisions, order placement, or live trading.

## Binance

```bash
cargo run \
  --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  -- \
  --venue binance \
  --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --depth-snapshot-limit 100
```

## Binance L0 S3 storage

```bash
cargo run \
  --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  -- \
  --venue binance \
  --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --expect-symbol-count 50 \
  --allow-partial-symbol-coverage \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
  --aws-region ap-northeast-2 \
  --l0-flush-records 1000
```

Binance public streams do not guarantee that every subscribed symbol emits within
a short smoke window. Use `--allow-partial-symbol-coverage` for short storage
verification runs.

## Upbit

```bash
cargo run \
  --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  -- \
  --venue upbit \
  --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --expect-symbol-count 50 \
  --upbit-orderbook-unit 5
```

## Upbit L0 S3 storage

```bash
cargo run \
  --manifest-path /opt/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  -- \
  --venue upbit \
  --config /opt/nangman-crypto/strategies/crypto/rust-engine/config \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --expect-symbol-count 50 \
  --upbit-orderbook-unit 5 \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
  --aws-region ap-northeast-2 \
  --l0-flush-records 1000
```

When `--l0-s3-bucket` is set, the app writes `raw_market_event_v2` Parquet
objects and a run manifest:

```text
raw_market_event/venue=binance/event_type=ticker/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=binance/event_type=trade/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=binance/event_type=book_ticker/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=binance/event_type=depth_delta/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=binance/event_type=depth_snapshot/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=upbit/event_type=ticker/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=upbit/event_type=trade/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
raw_market_event/venue=upbit/event_type=depth_snapshot/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
source_health/venue=binance/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
source_health/venue=upbit/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
symbol_health/venue=binance/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
symbol_health/venue=upbit/event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
gap_alert/venue=binance/gap_type=.../event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
gap_alert/venue=upbit/gap_type=.../event_date=YYYY-MM-DD/hour=HH/shard=00/*.parquet
runs/run_id=market-ingest-upbit-*/manifest.json
runs/run_id=market-ingest-binance-*/manifest.json
```

Parquet files are retained in the local L0 spool as the LIVE L1 input cache.
S3 upload remains the durable recovery/backfill source. Each S3 `PutObject` uses
bounded retry with exponential backoff and stable jitter. Upload failure does
not delete the local parquet file; the failed object is recorded in the run
report, and the next recovery/backfill pass can retry from local data.

## L1 normalization

`market-normalize` is a separate long-lived worker binary in this app image. It
reads L0 objects by operating mode, builds 1-second
`normalized_market_slice_v1` rows by `exchange_timestamp_ms`, and publishes Parquet data,
`normalization_report_v1`, `l1_manifest_v1`, and the success-only `l1_index`
pointer fanout. A successful 15-minute run writes one pointer per 1-second
window, while each pointer still references the canonical run manifest/report.
This lets downstream apps recover market context from an arbitrary event
timestamp without listing L1 prefixes.

S3 recovery/backfill must discover every raw market event type that contributes
to L1 projections, including Binance derivatives snapshots
`funding_rate_snapshot` and `open_interest_snapshot`. If those L0 objects are not
listed during normalization, `market_feature_delta` cannot contain
`funding_rate` or `open_interest`, and derivatives candidates remain blocked
before research.

Downstream apps must not read L1 Parquet by prefix listing. They must resolve
the success-only reader path:

```text
l1_index pointer -> l1_manifest_v1 -> normalization_report_v1 -> output_object_keys
```

The reader rejects `blocked`, `partial`, and `failed` runs, schema mismatches,
requested windows outside the run time range, and report/manifest drift.
Existing uploaded objects are kept as immutable run evidence; schema or
quality-rule changes are handled by targeted backfill and a new success pointer,
not by deleting old objects.

L0 S3 is the durable truth source. The local L0 spool is an evictable hot cache:
LIVE mode uses local entries first and downloads only missing keys from S3 into
`catchup_tmp_root`; CATCH-UP and manual BACKFILL stream from S3 into that tmp
directory and remove the session directory when the run finishes.

Audit L1 index coverage after deployment:

```bash
cargo run \
  --manifest-path /home/seongwon/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  --bin market-normalize -- \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
  --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
  --aws-profile market-ingest-roles-anywhere \
  --aws-region ap-northeast-2 \
  --audit-l1-index-start-ms 1778042400000 \
  --audit-l1-index-end-ms 1778043300000
```

Default mode is a compose-managed worker loop. Every `--schedule-interval-ms`
tick it processes contiguous 15-minute UTC windows until it is caught up or
`--max-windows-per-tick` is reached. It never skips intermediate windows:
if only the latest ready window is pending the current window is LIVE; if older
windows are still missing it is CATCH-UP. On SIGTERM, the worker finishes the
current window and exits before starting another window or sleep cycle.
`MARKET_NORMALIZE_MAX_LATENCY_MS` controls when valid events are counted as
`quality_delayed` instead of `quality_ok`.
`MARKET_NORMALIZE_L0_RUN_KEY_OVERLAP_MS` controls the conservative L0 run-id
timestamp filter used after hourly S3 listing; keep it greater than or equal to
the L0 ingest duration so boundary files are not dropped.

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  up -d market-normalize
```

## L0 historical backfill

`market-backfill` is the L0 historical trade worker in this app image. It writes
only `raw_market_event_v2`, `source_health_v2`, `symbol_health_v1`,
`gap_alert_v1`, and the run manifest into the L0 bucket. It does not write L1
directly.

Binance uses public `/api/v3/aggTrades` to backfill long-range spot trades for
the checked-in major-50 universe or an explicit `--symbols` subset.

```bash
cargo run \
  --manifest-path /home/seongwon/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  --bin market-backfill \
  -- \
  --venue binance \
  --config /home/seongwon/nangman-crypto/strategies/crypto/rust-engine/config \
  --input-start-ms 1778042400000 \
  --input-end-ms 1778043300000 \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214
```

Upbit uses public recent trade history only. It is for recent repair/backfill,
not 210-day full bootstrap, and it rejects ranges outside the recent window.

```bash
cargo run \
  --manifest-path /home/seongwon/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  --bin market-backfill \
  -- \
  --venue upbit \
  --input-start-ms 1778572800000 \
  --input-end-ms 1778573400000 \
  --symbols KRW-BTC,KRW-ETH \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214
```

Manual backfill pins the exact input range:

```bash
cargo run \
  --manifest-path /home/seongwon/nangman-crypto/apps/market-ingest-app/Cargo.toml \
  --bin market-normalize \
  -- \
  --l0-s3-bucket nangman-crypto-dev-market-ingest-l0-962214 \
  --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
  --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-962214 \
  --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
  --input-start-ms 1778042400000 \
  --input-end-ms 1778043300000
```

## L0 DoD

This app is complete for L0 ingest when:

- Binance and Upbit raw public events are written as Parquet.
- Binance writes REST `/api/v3/depth` snapshots as `depth_snapshot` before WS diff
  depth deltas are finalized for the run.
- Each run writes at least one `source_health` object per venue.
- Each run writes at least one `symbol_health` object per venue.
- Detected gap alerts are written as `gap_alert` Parquet objects.
- S3 retry exhaustion count is zero.
- The run manifest contains uploaded object keys and record counts.
- `cargo fmt`, `cargo check`, and `cargo test` pass.
- Every code file stays under 500 lines.

## Docker Compose

The compose unit runs Binance, Upbit, and `market-normalize` as separate
services using the same image. L0 venue services are duration-based, so compose
runs each service for a finite window and restarts it after a normal exit. L1 is
a long-lived worker service and owns its own sleep/drain loop.

The Linux checkout path is `/home/seongwon/nangman-crypto`. The container app
root remains `/opt/nangman-crypto`, and host-local runtime state such as
Roles Anywhere helper files, PKI material, and spool data also stays under
`/opt/nangman-crypto`.

### Initial host setup (once per host)

The setup script prepares `/opt` host directories, installs the IAM Roles
Anywhere credential helper, creates `config.container`, and creates the
checkout-local `apps/market-ingest-app/.env` file from `.env.example`. It is
idempotent and should be run on each on-prem Linux host.

The workload certificate and private key are not generated by this script. They
must come from the signing host and live under the host PKI directory:

```text
/opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.791444962214.dev.pem
/opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.791444962214.dev.key
```

Use scp from the signing host, then rerun setup if the first run stopped with
exit code 2:

```bash
scp signing-host:/secure/path/nangman-crypto-market-ingest.791444962214.dev.pem \
  /opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.791444962214.dev.pem
scp signing-host:/secure/path/nangman-crypto-market-ingest.791444962214.dev.key \
  /opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.791444962214.dev.key
```

```bash
cd /home/seongwon/nangman-crypto/apps/market-ingest-app
./scripts/setup-host.sh
```

### Redeploy (after git pull)

After pulling new code, rebuild, run preflight, and recreate all compose
services with:

```bash
cd /home/seongwon/nangman-crypto/apps/market-ingest-app
./scripts/deploy.sh
```

The deploy script verifies host clock sync, runs `docker compose config`,
builds the image, runs container-side AWS/S3 preflight with the same mounted
`AWS_CONFIG_FILE` and `AWS_PROFILE` that runtime uses, recreates Binance,
Upbit, and `market-normalize`, and prints service status with the checkout-local
`.env` and compose file paths.

The AWS profile itself is created on the deployment host. `deploy.sh` only
checks that the configured runtime profile can list both buckets and
write/read/delete a `_preflight/market-ingest-app/...` smoke object in both
buckets. If `timedatectl` reports `NTPSynchronized=no`, deploy stops before
starting services because watermark decisions depend on host wall-clock time.

### Manual stop

Stop it with:

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  down
```

## Structured Logs

The app writes JSON Lines to stdout/stderr with schema
`market_ingest_log_v1`. Production compose defaults to
`MARKET_INGEST_LOG_LEVEL=info`.

Log levels:

- `error`: unrecoverable process failure.
- `warn`: degraded operation or operator action needed.
- `info`: lifecycle, reports, and successful publish summaries.
- `debug`: high-frequency progress, heartbeats, not-ready polling, and per-window start details.

Events:

- `market_ingest_start`
- `market_ingest_report`
- `market_ingest_eviction_run`
- `market_normalize_preflight_ok`
- `market_normalize_worker_started`
- `market_normalize_worker_stopped`
- `market_normalize_index_published`
- `market_normalize_l1_index_audit`
- `market_normalize_finished`
- `market_normalize_fallback_alert`
- `market_ingest_error`

Debug-only events in the default production image:

- `market_ingest_progress`
- `market_ingest_unsealed_orphan_cleanup` when no invalid orphan is found
- `market_ingest_eviction_heartbeat`
- `market_normalize_started`
- `market_normalize_not_ready`
- `market_normalize_worker_sleep`

Example:

```json
{"schema_version":"market_ingest_log_v1","app":"market-ingest-app","level":"info","event":"market_normalize_finished","timestamp_ms":1777976991000,"fields":{"l1_run_id":"l1_1777975200000_1777976100000_1777976991000","status":"success","slice_count_total":90000,"output_object_count":2}}
```

Follow logs on Linux:

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  logs -f --no-log-prefix
```

Filter only structured ingest logs:

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  logs --no-log-prefix | jq -c 'select(.schema_version == "market_ingest_log_v1")'
```

Temporarily enable verbose operational diagnostics:

```bash
sudo env MARKET_INGEST_LOG_LEVEL=debug docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  up -d --force-recreate
```

Container DoD:

- `docker compose config` renders without errors.
- `MARKET_INGEST_REPO_ROOT` in `/home/seongwon/nangman-crypto/apps/market-ingest-app/.env`
  points to the actual clone path on the host.
- Both `binance` and `upbit` services start from compose.
- IAM Roles Anywhere config and certificate material are mounted read-only from ignored app infra.
- Local L0 data is retained under `/opt/nangman-crypto/data/spool/market-ingest/l0`
  as the LIVE L1 input cache. S3 is the recovery/backfill truth source, and
  fallback downloads go to `/opt/nangman-crypto/data/spool/market-normalize/catchup`.
- S3 output goes to `nangman-crypto-dev-market-ingest-l0-962214` unless `MARKET_L0_BUCKET` is overridden.
- L1 normalize output goes to `nangman-crypto-dev-market-ingest-l1-962214`
  unless `MARKET_L1_BUCKET` is overridden.
- L1 universe bootstrap writes and reads
  `symbol_universe_snapshot/bootstrap_rollup/*`; this existing universe prefix is
  required for 30-day point-in-time universe approval.
- The runtime role can `ListBucket`, `GetObject`, and `PutObject` on the L0/L1
  buckets, and can `DeleteObject` only for `_preflight/market-ingest-app/*`.
- If L0/L1 buckets use SSE-KMS, the runtime role also needs the matching KMS
  `GenerateDataKey` and `Decrypt` permissions.
