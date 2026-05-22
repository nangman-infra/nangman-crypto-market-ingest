# Market Ingest App Contract

`market-ingest-app` is the public market-data source of truth for Nangman Crypto.
It captures exchange public data into L0, normalizes point-in-time L1 market
context, and preserves enough evidence for downstream intel, candidate,
research, and replay apps.

## Boundaries

The app may:

- read public exchange market data
- write market L0 and L1 artifacts to the configured S3 buckets
- resume bootstrap and normalization from S3 markers and L1 success pointers
- emit structured logs for runtime verification

The app must not:

- call private account APIs
- use exchange credentials
- place orders
- make alpha, strategy, paper, or live-trading decisions
- delete S3 artifacts unless an operator explicitly enables and approves that path

## Event Bus Boundary

S3 is the canonical durable store for market data. This app currently does not
publish NATS subjects. Its downstream handoff contract is the success-only
`l1_index` pointer in the configured L1 bucket.

```text
NATS subject emitted by market-ingest-app: none
downstream payload pointer: MARKET_L1_BUCKET/l1_index/
canonical payload storage: MARKET_L1_BUCKET/normalized_market_slice/
```

If a future pointer publisher is added, that change must define the subject,
payload schema version, idempotency key, ack/retry behavior, and replay semantics
before deployment.

## Required Runtime Inputs

The supervisor must receive real bucket names at runtime:

```text
--l0-s3-bucket nangman-crypto-dev-market-ingest-l0-<account-suffix>
--l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix>
```

The placeholder values shown above are public documentation placeholders. The
binary rejects placeholder bucket names at runtime.

## L0 Artifact Families

L0 artifacts are written under the configured L0 bucket.

```text
raw_market_event/venue={venue}/event_type={event_type}/event_date=YYYY-MM-DD/hour=HH/shard=NN/run_id={run_id}-part-NNNNNN.parquet
source_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=NN/run_id={run_id}-part-NNNNNN.parquet
symbol_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=NN/run_id={run_id}-part-NNNNNN.parquet
gap_alert/venue={venue}/gap_type={gap_type}/event_date=YYYY-MM-DD/hour=HH/shard=NN/run_id={run_id}-part-NNNNNN.parquet
runs/run_id={run_id}/manifest.json
```

L0 preserves raw evidence. It does not perform alpha or order decisions.

## L1 Artifact Families

L1 artifacts are written under the configured L1 bucket.

```text
normalized_market_slice/venue={venue}/event_date=YYYY-MM-DD/hour=HH/window_ms={window_ms}/shard=00/run_id={l1_run_id}-part-000001.parquet
l1_index/window_ms={window_ms}/event_date=YYYY-MM-DD/hour=HH/window_start_ms={window_start_ms}.json
normalization_report/run_id={l1_run_id}/report.json
runs/run_id={l1_run_id}/manifest.json
market_data_quality_summary/run_id={l1_run_id}/summary.json
market_feature_delta/run_id={l1_run_id}/delta.json
market_feature_delta_summary/run_id={l1_run_id}/summary.json
market_regime_context/run_id={l1_run_id}/context.json
symbol_universe_snapshot/run_id={l1_run_id}/snapshot.json
symbol_universe_snapshot/bootstrap_rollup/event_date=YYYY-MM-DD/latest.json
```

Success-only `l1_index` pointers are the downstream read contract.

## Schema Versions

Current L1 schema versions:

```text
normalized_market_slice_v1
normalization_report_v1
l1_manifest_v1
market_data_quality_summary_v1
market_feature_delta_v1
market_feature_delta_summary_v1
market_regime_context_v1
symbol_universe_bootstrap_rollup_v1
symbol_universe_snapshot_v1
```

New artifact families must include a schema version, producer identity, stable
ID or deterministic object key, creation or decision time, and enough metadata
to replay the result.

## Build Provenance

L1 normalization reports must include compile-time build provenance:

```text
runner_git_sha
runner_git_dirty
runner_build_profile
```

Container builds must pass `NANGMAN_GIT_SHA` and `NANGMAN_GIT_DIRTY` as Docker
build args before compiling the Rust binaries. If the source tree is unavailable
or cannot be inspected, `runner_git_sha` must be `unknown` and
`runner_git_dirty` must be `true`, so downstream audit never mistakes an
unverifiable image for a clean source revision.

## Bootstrap and Live Priority

During historical bootstrap, the supervisor runs:

```text
realtime L0 worker
historical bootstrap scheduler
live-priority L1 normalize worker
```

The live-priority worker seeds only the latest closed L1 window per tick. It
keeps downstream intel from blocking on stale market context while the historical
bootstrap scheduler fills older L0/L1 windows.

After bootstrap completes, the supervisor stops the live-priority worker and
starts the full long-lived normalize worker.

## Verification

Local verification requires:

```text
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
docker buildx build --platform linux/arm64
```

Runtime verification must check service stability, CloudWatch log retention set
to 3 days, recent structured logs, fresh S3 L0/L1 outputs under recent UTC hour
prefixes, no panic/OOM/SIGKILL loop, meaningful sample artifacts, and downstream
readability through the L1 success index.

The local repository contract gate is:

```text
scripts/check-repository-contract.py
```

It must be runnable both locally and in GitHub Actions. It validates required
files, executable verification scripts, ECS example hardening, required contract
phrases, placeholder-only public docs, and absence of public account/profile/IP
leakage. CI must call this script instead of maintaining a separate inline copy
of the same repository contract.

The local release readiness gate is:

```text
scripts/check-release-readiness.sh
```

It must run script validation, verifier self-tests, repository contract checks,
Compose config rendering, Rust fmt/clippy/test, linux/arm64 Docker build, and a
container smoke check. It must not push images, register ECS task definitions,
update ECS services, write S3 objects, or claim runtime freshness. Runtime DoD
still requires live AWS/S3/CloudWatch verification after an approved deploy.

The read-only release artifact preparer is:

```text
scripts/prepare-release-artifacts.sh
```

It may call AWS read APIs for STS and ECS only. It must produce a release
manifest and hardened `register-task-definition` JSON under an operator-provided
local output directory. It must not push images, register task definitions,
update ECS services, write S3 objects, or delete S3 objects. If the git worktree
is dirty, the manifest must set `release_ready=false` and record a blocker
instead of presenting the artifacts as deployable.

The read-only runtime verifier is:

```text
scripts/check-runtime.sh
```

It may call AWS read APIs for ECS, CloudWatch Logs, CloudWatch metrics, STS, and
S3. The S3 check must read one `l1_index` pointer, its manifest, its report, and
head the sampled normalized slice object. The resource check must inspect ECS
CPU/Memory utilization thresholds for the runtime window.
It must not update ECS services, restart tasks, push images, or delete S3 objects. The verifier
should continue across independent read-only checks and summarize all observed
failures at the end, so `RUNNING` service state cannot hide stale S3 output,
resource pressure, or task-definition drift.

The read-only L1 staleness diagnosis tool is:

```text
scripts/diagnose-l1-staleness.sh
```

It may call AWS read APIs for ECS, CloudWatch Logs, STS, and S3. It must report
the current deployed task image, task hardening shape, recent lifecycle counts,
latest matching lifecycle samples, and recent L0/L1 S3 prefix samples. It must
not update ECS services, restart tasks, push images, mutate repositories, or
write/delete S3 objects. Its purpose is diagnosis only: it does not replace the
runtime verifier and cannot mark the service healthy when fresh L1 output is
missing.

ECS production examples are:

```text
ecs/task-definition.example.json
ecs/service.example.json
ecs/task-role-policy.example.json
```

The service example must keep the single supervisor service on `FARGATE_SPOT`
with public IP assignment disabled and only placeholder subnet/security group
values. The task definition example must use ARM64/FARGATE, one supervisor
container, `readonlyRootFilesystem=true`, explicit non-root user, and
`capabilities.drop=["ALL"]`.

The read-only hardening renderer is:

```text
scripts/render-ecs-task-definition.sh
```

It may call AWS read APIs for ECS only. It must output a
`register-task-definition` JSON document and must not register the task
definition, update the service, push images, or mutate AWS state. The output
must preserve the single-supervisor ARM64/FARGATE shape and enforce
`readonlyRootFilesystem=true`, explicit non-root user, and `drop=["ALL"]`.

The read-only ECR scan verifier is:

```text
scripts/check-ecr-scan.sh
```

It must inspect the linux/ARM64 image manifest. If the provided tag points to a
multi-arch image index, the verifier must resolve the linux/arm64 child digest
and inspect scan findings for that digest. It must fail on blocking ECR scan
severities and must not push images, mutate repositories, or change lifecycle
policies.
