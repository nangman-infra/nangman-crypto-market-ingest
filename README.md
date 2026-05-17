# market-ingest-app

All-in-one public market data ingest package for the AI-DLC Alpha Discovery
runtime.

## Production Contract

`nangman-crypto-market-ingest` is the canonical deterministic market-truth
ingress app for Nangman Crypto. Production runs one Docker image, one ECS
service, and one supervisor entrypoint:

```text
cluster:  ecs-nangman-dev-invest-apn2
service:  svc-nangman-dev-crypto-market-ingest
compute:  Fargate Spot, ARM64
task:     td-nangman-dev-crypto-market-ingest
image:    ecr-nangman-dev-crypto-market-ingest-apn2
logs:     /ecs/nangman/dev/crypto-market-ingest
```

The current dev buckets are:

```text
L0: nangman-crypto-dev-market-ingest-l0-962214
L1: nangman-crypto-dev-market-ingest-l1-962214
```

The app is intentionally stateless at the compute layer. ECS task replacement,
Fargate Spot interruption, or deploy restart must be safe because progress is
derived from S3 markers and L1 success pointers, not from local process memory.
The local filesystem is only an in-task hot spool.

The production container entrypoint is `crypto-market-ingest-supervisor`. One
ECS service runs one task and the supervisor starts three internal workers:

- `market-ingest-app`: realtime WebSocket L0 writer.
- `market-backfill`: Binance historical L0 bootstrap scheduler.
- `market-normalize`: L0-to-L1 normalization loop.

Default production behavior is automatic. On task start, realtime Binance L0
ingest begins immediately and Binance historical bootstrap fills the missing
210-day L0 range in stable UTC-day chunks. Long-lived L1 normalization is
deferred while bootstrap is incomplete; the supervisor runs one-shot 15-minute
L1 normalize subchunks after each L0 bootstrap chunk, then starts the long-lived
normalize worker after all bootstrap chunks are complete.

Bootstrap markers are written under the L1 bucket:

```text
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/success.json
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/complete.json
```

`success.json` means the L0 backfill chunk completed. `complete.json` means the
same chunk has completed both L0 backfill and L1 normalization. `complete.json`
is the idempotency contract used to skip completed bootstrap chunks after task
restart. If a task stops mid-chunk, the missing L0 or L1 segment is retried on
the next task start instead of mutating partial files in place.

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

## Runtime triggers

- Task start: supervisor starts realtime L0 and the bootstrap scheduler.
- Realtime worker exit: supervisor exits non-zero so ECS restarts the task.
- Normalize worker exit: supervisor restarts only the normalize worker after a
  bounded delay.
- Bootstrap enabled: supervisor defers the long-lived normalize worker until
  bootstrap completes, avoiding concurrent normalizers in one task.
- Bootstrap L0 success: supervisor writes `success.json`.
- Bootstrap L1 success: supervisor writes `complete.json` and moves to the next
  missing chunk after `--bootstrap-interval-secs`.
- Bootstrap failure: supervisor leaves the relevant marker absent, waits, and
  retries the same missing work later.
- SIGTERM: supervisor stops children and exits cleanly.

The default L0 app-owned retention is 45 days. The default L1 app-owned
retention is 240 days. S3 lifecycle remains a secondary cleanup guard, not the
primary data-management owner.

## ECS Operations

Production deploys should keep the app as one ECS service. Do not split
realtime, backfill, and normalize into separate ECS services unless the
supervisor contract is intentionally replaced.

Recommended dev shape:

- Capacity provider: `FARGATE_SPOT` with `weight=1`, `base=0`.
- Runtime platform: `LINUX/ARM64`.
- Task size: start with `2 vCPU / 4 GB` while 210-day bootstrap is running.
- CloudWatch log retention: 3 days.
- ECR lifecycle: retain the latest 5 pushed images.
- Container image: distroless runtime, non-root user.

Useful read-only checks:

```bash
aws ecs describe-services \
  --profile AdministratorAccess-791444962214 \
  --region ap-northeast-2 \
  --cluster ecs-nangman-dev-invest-apn2 \
  --services svc-nangman-dev-crypto-market-ingest \
  --query 'services[0].{desired:desiredCount,running:runningCount,pending:pendingCount,capacityProviderStrategy:capacityProviderStrategy,rollout:deployments[0].rolloutState,taskDefinition:taskDefinition}'
```

```bash
aws logs filter-log-events \
  --profile AdministratorAccess-791444962214 \
  --region ap-northeast-2 \
  --log-group-name /ecs/nangman/dev/crypto-market-ingest \
  --filter-pattern 'market_backfill_done || market_normalize_finished || crypto_market_ingest_bootstrap_chunk_done || error || panic || SIGKILL || OutOfMemory'
```

```bash
aws s3api list-objects-v2 \
  --profile AdministratorAccess-791444962214 \
  --region ap-northeast-2 \
  --bucket nangman-crypto-dev-market-ingest-l1-962214 \
  --prefix supervisor/bootstrap/ \
  --query 'sort_by(Contents || `[]`, &LastModified)[-10:].{key:Key,size:Size,lastModified:LastModified}'
```

## S3 Prefix Contract

L0 stores immutable raw public market truth and run evidence:

```text
raw_market_event/venue={venue}/event_type={event_type}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
source_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
symbol_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
gap_alert/venue={venue}/gap_type={gap_type}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
runs/run_id={run_id}/manifest.json
```

L1 stores normalized market slices, success-only index pointers, manifests,
reports, universe bootstrap rollups, and supervisor markers:

```text
normalized_market_slice/window_ms=1000/event_date=YYYY-MM-DD/hour=HH/run_id={l1_run_id}-part-NNNNNN.parquet
l1_index/window_ms=1000/event_date=YYYY-MM-DD/hour=HH/window_start_ms={ms}.json
normalization_report/run_id={l1_run_id}.json
runs/run_id={l1_run_id}/manifest.json
symbol_universe_snapshot/bootstrap_rollup/event_date=YYYY-MM-DD/latest.json
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/{success,complete}.json
```

Consumers must read L1 through the success pointer path:

```text
l1_index pointer -> l1_manifest_v1 -> normalization_report_v1 -> output_object_keys
```

They must not treat arbitrary L1 prefix listing as canonical state.

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

`market-normalize` is a long-lived worker binary in this app image. In
production it is started by `crypto-market-ingest-supervisor`; it can still be
run directly for manual audit and repair. It
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

Default mode is a supervisor-managed worker loop. Every `--schedule-interval-ms`
tick it processes contiguous 15-minute UTC windows until it is caught up or
`--max-windows-per-tick` is reached. It never skips intermediate windows:
if only the latest ready window is pending the current window is LIVE; if older
windows are still missing it is CATCH-UP. On SIGTERM, the worker finishes the
current window and exits before starting another window or sleep cycle.
`MARKET_NORMALIZE_MAX_LATENCY_MS` controls when valid events are counted as
`quality_delayed` instead of `quality_ok`.
`--l0-run-key-overlap-ms` controls the conservative L0 run-id timestamp filter
used after hourly S3 listing. The supervisor default is 360000 ms, separate from
the realtime worker lifetime, so a long-running task does not force L1 to scan a
year of L0 run prefixes every tick.

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

The compose unit is a local verification harness and may run Binance, Upbit, and
`market-normalize` as separate services using the same image. ECS production is
not split into separate services; it runs the all-in-one supervisor as the only
container entrypoint.

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

- `crypto_market_ingest_supervisor_start`
- `crypto_market_ingest_normalize_deferred`
- `crypto_market_ingest_bootstrap_chunk_start`
- `crypto_market_ingest_bootstrap_l0_done`
- `crypto_market_ingest_bootstrap_l1_done`
- `crypto_market_ingest_bootstrap_chunk_done`
- `crypto_market_ingest_bootstrap_complete`
- `crypto_market_ingest_s3_retention_run`
- `market_ingest_start`
- `market_ingest_report`
- `market_ingest_eviction_run`
- `market_backfill_done`
- `market_backfill_s3_retention_run`
- `market_normalize_preflight_ok`
- `market_normalize_worker_started`
- `market_normalize_worker_stopped`
- `market_normalize_index_published`
- `market_normalize_l1_index_audit`
- `market_normalize_finished`
- `market_normalize_s3_retention_run`
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
- S3 retention is app-owned: L0 defaults to 45 days, L1 defaults to 240 days,
  and the shared cleanup loop checks every 6 hours. Bucket lifecycle is only the
  fallback safety net: L0 expires after 60 days, L1 expires after 300 days, and
  `normalized_market_slice/` transitions to Standard-IA after 30 days.
- L1 universe bootstrap writes and reads
  `symbol_universe_snapshot/bootstrap_rollup/*`; this existing universe prefix is
  required for 30-day point-in-time universe approval.
- The runtime role can `ListBucket`, `GetObject`, and `PutObject` on the L0/L1
  buckets, and can `DeleteObject` for `_preflight/market-ingest-app/*` plus the
  app-owned market-ingest retention prefixes.
- If L0/L1 buckets use SSE-KMS, the runtime role also needs the matching KMS
  `GenerateDataKey` and `Decrypt` permissions.
