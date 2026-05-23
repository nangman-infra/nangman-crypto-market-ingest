# market-ingest-app

> Nangman Crypto의 deterministic L0/L1 market data ingestor.
> 한 image, 한 ECS service, 한 supervisor가 세 worker를 띄워서
> Binance/Upbit public market stream을 받아 S3에 immutable parquet으로 적재한다.

- **L0**: raw market truth + health + gap. immutable append-only.
- **L1**: 1초 단위 `normalized_market_slice` + success-only index pointer.
- private API, account credential, AI hot-path 결정, 주문 경로는 사용하지 않는다.

상세 계약은 [`docs/contracts/market-ingest-app-contract.md`](docs/contracts/market-ingest-app-contract.md)가 source of truth다.

---

## 목차

- [TL;DR](#tldr)
- [Architecture](#architecture)
- [Workers](#workers)
  - [market-ingest-app — realtime L0](#worker-1-market-ingest-app--realtime-l0)
  - [market-backfill — historical L0](#worker-2-market-backfill--historical-l0)
  - [market-normalize — L0 → L1](#worker-3-market-normalize--l0--l1)
- [Data Contract (요약)](#data-contract-요약)
- [Running](#running)
  - [ECS production](#ecs-production)
  - [Local Compose](#local-compose)
  - [Manual smoke / audit](#manual-smoke--audit)
- [Monitoring](#monitoring)
- [DoD](#dod)

---

## TL;DR

```text
production:  ECS service 1개 = task 1개 = supervisor 1개 = worker 3개
cluster:     ecs-nangman-dev-invest-apn2
service:     svc-nangman-dev-crypto-market-ingest
task:        td-nangman-dev-crypto-market-ingest (Fargate Spot, ARM64)
image:       ecr-nangman-dev-crypto-market-ingest-apn2
logs:        /ecs/nangman/dev/crypto-market-ingest

dev buckets:
  L0 = nangman-crypto-dev-market-ingest-l0-<account-suffix>  (retention 45d)
  L1 = nangman-crypto-dev-market-ingest-l1-<account-suffix>  (retention 240d)
```

- compute layer는 stateless. ECS 재시작·Fargate Spot interruption·deploy restart 모두 안전.
  진행 상황은 S3 marker와 L1 success pointer에서 복구한다.
- market-ingest-app은 현재 NATS subject를 직접 publish하지 않는다.
  downstream handoff는 S3 `l1_index` success pointer이며, NATS pointer publisher가
  명시적으로 추가되기 전까지 이 앱의 NATS subject는 `none`이다.
- bootstrap: realtime L0와 동시에 Binance 210일 historical L0를 UTC-day 청크로 채운다.
- bootstrap 동안 live-priority-only normalize worker가 최신 닫힌 윈도우 1개를 계속 seed한다.
  이 경로는 downstream intel이 current `l1_index`를 기다리며 멈추지 않게 하기 위한 hot path다.
  historical catch-up은 이 worker가 소비하지 않고 bootstrap scheduler가 담당한다.
- 청크가 끝날 때마다 일회성 L1 normalize를 돌려 historical L1을 채운다.
- 모든 bootstrap 청크가 끝나면 live-priority worker를 종료하고 full long-lived normalize worker로 전환한다.

---

## Architecture

```text
            ┌─────────────────────────────────────┐
            │  crypto-market-ingest-supervisor    │  (ECS entrypoint)
            └───────────────┬─────────────────────┘
                            │ spawn / kill / restart
        ┌───────────────────┼───────────────────────────┐
        ▼                   ▼                           ▼
 market-ingest-app   market-backfill               market-normalize
 (realtime WS L0)    (historical REST L0)          (L0 → L1 loop)
        │                   │                           │
        └─→ MARKET_L0_BUCKET ←┘                         └─→ MARKET_L1_BUCKET
            (raw_market_event,                            (normalized_market_slice,
             source_health,                                l1_index, manifest)
             symbol_health,
             gap_alert,
             manifest)
```

**Production runtime triggers**:

```text
task start              → realtime + bootstrap scheduler 시작
bootstrap active        → live-priority-only normalize worker 시작 (latest closed window, max 1)
realtime worker exit    → supervisor가 ECS 재시작 유도 (non-zero exit)
normalize worker exit   → supervisor가 bounded delay 후 재기동
bootstrap L0 success    → success.json marker
bootstrap L1 success    → complete.json marker → 다음 청크
bootstrap 전체 완료     → full long-lived normalize worker로 전환
SIGTERM                 → graceful shutdown (모든 buffer flush + manifest upload)
```

Bootstrap marker는 L1 bucket에 저장된다.

```text
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/success.json
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/complete.json
```

`complete.json`이 idempotency contract다. 청크 도중 task가 죽으면 다음 task 시작 시 누락분만 재시도한다. 부분 파일을 그 자리에서 mutate하지 않는다.

---

## Workers

### Worker 1: `market-ingest-app` — realtime L0

Binance reference + Upbit execution public stream을 in-process로 24x7 수집한다.

**입력 stream**:

| venue   | role      | streams |
|---------|-----------|---------|
| Binance | reference | `trade`, `bookTicker`, `ticker`, `depth@100ms` |
| Upbit   | execution | `ticker`, `trade`, `orderbook` |

- Binance: REST `/api/v3/depth` snapshot으로 diff depth를 정렬한다.
- Upbit: Top50 KRW 심볼을 `/v1/market/all` + `/v1/ticker/all?quote_currencies=KRW`에서 도출 후 단일 WS 구독.
- Upbit `book_ticker`는 L0에 저장하지 않고 L1에서 `orderbook_units[0]`로 파생한다.
- Binance Top50 universe는 checked-in `config/universe.major-50.toml`.

**Resilience (운영 모드)**:

```text
in-process WS reconnect:
  initial_backoff       = 1s
  max_backoff           = 60s
  multiplier            = 2
  stale_message_timeout = 30s   (전체 venue 침묵 기준)
  max_consecutive_failures = 무한 (process exit 하지 않음)

depth book buffered_events 상한:
  symbol당 최대 1_000개. 초과 시 silent drop 금지.
  book reset + gap_alert(gap_type="buffered_overflow", heal_status="dropped_count={N}")
  + source_health.buffer_overflow_count++
```

매 재연결마다 `gap_alert(gap_type="ws_reconnect")` 1회와 `source_health.reconnect_count++`로 표면화한다. 재연결 후 sequence tracker / depth book / `last_update_id`는 reset되고, 다음 depth_delta 수신 시 새 REST snapshot이 자동 fetch된다.

**In-memory order book 표현**:

- L0 storage(`payload_json`)는 raw string 그대로 유지한다.
- in-process book의 `bids` / `asks`는 `BTreeMap<FixedDecimal, FixedDecimal>` (가격 → 수량).
- 가격 정렬은 lexicographic이 아닌 numeric. `tests/fixtures/binance_depth_delta/sample.parquet` golden fixture가 round-trip을 CI에서 검증한다.

### Worker 2: `market-backfill` — historical L0

`/api/v3/aggTrades` (Binance) 또는 recent trade history (Upbit)로 missing window를 채운다. L0만 쓰며 L1은 건드리지 않는다. supervisor가 210일 lookback을 UTC-day 청크로 자동 호출한다.

### Worker 3: `market-normalize` — L0 → L1

L0 Parquet을 읽어 1초 단위 `normalized_market_slice_v1`을 만들고, 다음을 publish한다.

- `normalization_report_v1`
- `l1_manifest_v1`
- 성공 시에만 1초 윈도우당 1개씩 `l1_index` pointer
- `symbol_universe_bootstrap_rollup`, `symbol_universe_snapshot`

운영 모드는 `--schedule-interval-ms`마다 15분 윈도우를 연속 처리한다. 결정 트리거:

```text
LIVE     - 최신 ready 윈도우 1개 처리
CATCH-UP - 더 오래된 미처리 윈도우 우선
BACKFILL - --input-start-ms / --input-end-ms 명시 (one-shot)
REPAIR   - audit 또는 missing pointer 보강
REPORT   - 모든 모드에서 manifest/report 출력
```

`MARKET_NORMALIZE_MAX_LATENCY_MS`로 valid 이벤트의 `quality_delayed` 컷오프를 조정한다. L1 입력 선택은 L0 `event_date/hour` partition을 기준으로 후보 Parquet을 모으고, 실제 포함 여부는 각 row의 `exchange_timestamp_ms`로 결정한다. `run_id`는 producer 실행 식별자라서 장기 실행 worker의 시간 범위로 해석하지 않는다.

**Downstream consumer 규칙**: arbitrary L1 prefix listing 금지. 항상 success pointer 경로로 read.

```text
l1_index pointer → l1_manifest_v1 → normalization_report_v1 → output_object_keys
```

reader는 `blocked` / `partial` / `failed` run, schema mismatch, time range outside, manifest drift를 거부한다. 기존 객체는 immutable run evidence로 보존. schema/quality rule 변경은 새 success pointer로 표시한다.

L0 S3가 durable truth source다. local L0 spool은 evictable hot cache. LIVE는 local 우선 + 누락만 S3에서 다운로드(`catchup_tmp_root`). CATCH-UP/BACKFILL은 S3에서 tmp로 stream하고 세션이 끝나면 디렉터리를 제거한다.

---

## Data Contract (요약)

### L0 prefix

```text
raw_market_event/venue={venue}/event_type={event_type}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
source_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
symbol_health/venue={venue}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
gap_alert/venue={venue}/gap_type={gap_type}/event_date=YYYY-MM-DD/hour=HH/shard=00/run_id={run_id}-part-NNNNNN.parquet
runs/run_id={run_id}/manifest.json
```

### L1 prefix

```text
normalized_market_slice/venue={venue}/event_date=YYYY-MM-DD/hour=HH/window_ms=1000/shard=00/run_id={l1_run_id}-part-NNNNNN.parquet
l1_index/window_ms=1000/event_date=YYYY-MM-DD/hour=HH/window_start_ms={ms}.json
normalization_report/run_id={l1_run_id}/report.json
runs/run_id={l1_run_id}/manifest.json
symbol_universe_snapshot/bootstrap_rollup/event_date=YYYY-MM-DD/latest.json
supervisor/bootstrap/venue=binance/start_ms={start_ms}/end_ms={end_ms}/{success,complete}.json
```

### 신규 health signal (A1/A2)

```text
source_health.reconnect_count        : 누적 in-process WS 재연결 횟수
source_health.last_reconnect_at_ms   : 마지막 재연결 시각 (null = 한 번도 없음)
source_health.buffer_overflow_count  : depth book buffered_events 상한 초과 누적
gap_alert.gap_type = ws_reconnect    : 매 재연결 시 1회
gap_alert.gap_type = buffered_overflow: heal_status="dropped_count={N}"
gap_alert.gap_type = snapshot_parse_failed: FixedDecimal 변환 실패
```

전체 schema는 contract 문서 참조.

---

## Running

### ECS production

배포 자체는 IaC pipeline 책임. 운영자는 다음만 알면 된다.

```bash
# 서비스 상태
aws ecs describe-services \
  --profile "${AWS_PROFILE}" \
  --region ap-northeast-2 \
  --cluster ecs-nangman-dev-invest-apn2 \
  --services svc-nangman-dev-crypto-market-ingest \
  --query 'services[0].{desired:desiredCount,running:runningCount,pending:pendingCount,rollout:deployments[0].rolloutState,taskDefinition:taskDefinition}'

# 핵심 이벤트 필터링
aws logs filter-log-events \
  --profile "${AWS_PROFILE}" \
  --region ap-northeast-2 \
  --log-group-name /ecs/nangman/dev/crypto-market-ingest \
  --filter-pattern 'market_backfill_done || market_normalize_finished || crypto_market_ingest_bootstrap_chunk_done || error || panic || SIGKILL || OutOfMemory'

# 가장 최근 bootstrap marker 10개
aws s3api list-objects-v2 \
  --profile "${AWS_PROFILE}" \
  --region ap-northeast-2 \
  --bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
  --prefix supervisor/bootstrap/ \
  --query 'sort_by(Contents || `[]`, &LastModified)[-10:].{key:Key,size:Size,lastModified:LastModified}'
```

**읽기 전용 runtime check**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_L0_BUCKET="nangman-crypto-dev-market-ingest-l0-<account-suffix>" \
MARKET_L1_BUCKET="nangman-crypto-dev-market-ingest-l1-<account-suffix>" \
./scripts/check-runtime.sh
```

이 스크립트는 AWS/ECS/CloudWatch/S3를 읽기만 한다. service stable, FARGATE_SPOT,
ARM64 task, CloudWatch log retention 3일, 최근 lifecycle log, 최근 error 부재,
CloudWatch ECS CPU/Memory utilization threshold, 최근 UTC hour의 L0/L1 artifact, 그리고 `l1_index` pointer에서
manifest/report/sample slice까지 읽히는지 한 번에 확인한다. S3 object 삭제,
service update, task restart는 수행하지 않는다. 첫 실패에서 멈추지 않고 가능한
읽기 전용 항목을 계속 검사한 뒤 마지막에 실패 목록을 요약한다.

**읽기 전용 L1 staleness diagnosis**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_L0_BUCKET="nangman-crypto-dev-market-ingest-l0-<account-suffix>" \
MARKET_L1_BUCKET="nangman-crypto-dev-market-ingest-l1-<account-suffix>" \
./scripts/diagnose-l1-staleness.sh
```

이 스크립트는 runtime check가 `RUNNING` service와 stale L1 output을 동시에
보여줄 때 원인을 좁히는 읽기 전용 진단 도구다. 현재 task image, task hardening
shape, 최근 CloudWatch lifecycle count, 최신 lifecycle sample, 최근 L0/L1 S3
prefix sample을 같이 출력한다. ECS update, task restart, ECR push, S3 object
삭제나 쓰기는 수행하지 않는다.

**읽기 전용 universe readiness check**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_L1_BUCKET="nangman-crypto-dev-market-ingest-l1-<account-suffix>" \
./scripts/check-universe-readiness.sh
```

이 스크립트는 `symbol_universe_snapshot_v1`과 최근
`symbol_universe_bootstrap_rollup_v1`만 읽어서 major-50 universe가 어느 단계에서
막혔는지 분리한다. 특히 `configured_major50_observed`, `major50_observed`,
`major50_approved`, `missing_configured_symbols`,
`bootstrap_spread_samples_present`, `bootstrap_symbol_coverage_incomplete`를 구분한다.
서비스 업데이트, normalize 실행, S3 쓰기, S3 삭제는 수행하지 않는다.

**로컬 bootstrap admission preview**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_DIR="/tmp/nangman-crypto/market-universe-bootstrap/symbol_universe_snapshot/bootstrap_rollup" \
./scripts/preview-universe-bootstrap-admission.sh
```

로컬 seed와 이미 별도로 내려받은 live rollup을 겹쳐 보고 싶으면 overlay directory를
추가한다. 같은 `event_date`가 있으면 overlay rollup이 우선한다.

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_DIR="/tmp/nangman-crypto/market-universe-bootstrap/symbol_universe_snapshot/bootstrap_rollup" \
MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_OVERLAY_DIR="/tmp/nangman-crypto/live-rollups/symbol_universe_snapshot/bootstrap_rollup" \
./scripts/preview-universe-bootstrap-admission.sh
```

이 스크립트는 bootstrap seed를 S3에 올리기 전에 해당 seed가 30일 universe
approval을 몇 개 심볼까지 열 수 있는지 미리 계산한다.
`would_open_current_approved_subset`과 `would_open_full_major50_approval`을 분리해서
보여준다. AWS 호출, S3 업로드, S3 삭제, normalize 실행은 수행하지 않는다.

**읽기 전용 ECR scan check**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_INGEST_ECR_REPOSITORY="ecr-nangman-dev-crypto-market-ingest-apn2" \
MARKET_INGEST_ECR_IMAGE_TAG="git-<short-sha>-arm64" \
./scripts/check-ecr-scan.sh
```

ECR Basic Scanning은 multi-arch image index tag와 child image manifest digest의 결과가
다를 수 있다. 이 검증은 기본적으로 `arm64`가 포함된 tag를 요구하며, tag가 OCI/Docker
multi-arch index를 가리키면 linux/arm64 child digest를 따라가 scan finding을 읽는다.
`CRITICAL`/`HIGH` finding이 있으면 실패한다. ECR image나 repository는 수정하지 않는다.

**읽기 전용 hardened task definition render**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_INGEST_RENDER_OUTPUT="/tmp/market-ingest-task-definition.hardened.json" \
./scripts/render-ecs-task-definition.sh
```

이 스크립트는 현재 ECS task definition을 읽어 `register-task-definition`에 넣을 수
있는 JSON만 렌더링한다. AWS에는 아무 것도 쓰지 않는다. 출력 JSON에는
`readonlyRootFilesystem=true`, `user=0:0`,
`capabilities.drop=ALL`이 강제된다. Fargate writable bind mount는 기본
root-owned라서 이 조합을 사용한다. 새 image URI를 미리 반영해야 하면
`MARKET_INGEST_ECR_IMAGE_URI`를 지정한다.

**읽기 전용 release artifact preparation**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
AWS_PROFILE="${AWS_PROFILE}" \
MARKET_INGEST_RELEASE_OUTPUT_DIR="/tmp/market-ingest-release-$(git rev-parse --short=12 HEAD)" \
./scripts/prepare-release-artifacts.sh
```

이 스크립트는 현재 git sha 기준 `git-<short-sha>-arm64` image tag와 hardened
`register-task-definition` JSON, release manifest를 `/tmp/...`에 만든다. AWS에는
STS/ECS read API만 사용하고 ECR push, ECS register/update, S3 write/delete는 하지
않는다. worktree가 dirty이면 manifest의 `release_ready=false`와 blocker reason으로
기록한다.

**Build provenance**:

L1 normalization report는 `runner_git_sha`, `runner_git_dirty`,
`runner_build_profile`을 포함한다. Docker build는 Rust compile 전에
`NANGMAN_GIT_SHA`와 `NANGMAN_GIT_DIRTY` build arg를 주입해야 한다. `deploy.sh`는
local compose build 전에 현재 git 상태를 읽어 이 값을 자동 export한다. git 상태를
확인할 수 없으면 `NANGMAN_GIT_SHA=unknown`, `NANGMAN_GIT_DIRTY=true`로 빌드해
증거 artifact가 clean revision처럼 보이지 않게 한다.

**Local repository contract gate**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
./scripts/check-repository-contract.py
```

이 gate는 필수 파일, 실행 권한, ECS 예제 hardening, public 문서 placeholder,
계정/profile/IP 누수 금지, README/contract 핵심 문구를 한 번에 확인한다. GitHub
Actions도 같은 스크립트를 호출하므로 push 전에 로컬에서 같은 기준을 확인할 수 있다.

**Local release readiness gate**:

```bash
cd /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app
./scripts/check-release-readiness.sh
```

이 gate는 shell/Python script validation, self-test, repository contract, compose
config, Rust fmt/clippy/test, linux/arm64 Docker build, image smoke를 한 번에
실행한다. AWS/ECR/ECS/S3에는 쓰지 않는다. 배포 승인 전에 로컬 release candidate가
기본 DoD를 만족하는지 확인하기 위한 gate이며, runtime freshness 증명을 대체하지
않는다.

**권장 task shape (dev)**:

```text
capacity:     FARGATE_SPOT (weight=1, base=0)
platform:     LINUX/ARM64
size:         2 vCPU / 8 GB  (210일 bootstrap 진행 중일 때)
log retention: 3 days
ECR lifecycle: 최신 5개 image 보존
image:        distroless runtime
task hardening: readonlyRootFilesystem=true, user=0:0, capabilities.drop=ALL
```

production은 항상 하나의 ECS service. realtime/backfill/normalize를 별도 service로 쪼개지 말 것 (supervisor contract 침범).
Placeholder 기반 production 예제는 `ecs/task-definition.example.json`,
`ecs/service.example.json`, `ecs/task-role-policy.example.json`에 둔다.

### Local Compose

Compose는 local verification harness다. Binance, Upbit, `market-normalize`를 같은 image에서 별도 service로 띄운다. **production은 이 구성을 사용하지 않는다** (all-in-one supervisor 한 컨테이너).

Linux checkout: `/home/seongwon/nangman-crypto`. 컨테이너 app root: `/opt/nangman-crypto`. host runtime state(Roles Anywhere helper, PKI, spool)는 `/opt/nangman-crypto`.

**초기 host setup (host당 1회)**:

setup 스크립트는 `/opt` 호스트 디렉터리 준비, IAM Roles Anywhere credential helper 설치, `config.container` 생성, `apps/market-ingest-app/.env` 생성 (`.env.example` 복사)을 수행한다. idempotent.
생성된 `.env`의 `MARKET_L0_BUCKET` / `MARKET_L1_BUCKET`은 deploy 전에 실제 bucket 이름으로 바꿔야 한다.

PKI material은 별도. 서명 호스트에서 받아 다음 경로에 둔다.

```text
/opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.<account-id>.dev.pem
/opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.<account-id>.dev.key
```

```bash
scp signing-host:/secure/path/nangman-crypto-market-ingest.<account-id>.dev.pem \
  /opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.<account-id>.dev.pem
scp signing-host:/secure/path/nangman-crypto-market-ingest.<account-id>.dev.key \
  /opt/nangman-crypto/infra/pki/nangman-crypto-market-ingest.<account-id>.dev.key

cd /home/seongwon/nangman-crypto/apps/market-ingest-app
./scripts/setup-host.sh
```

setup이 exit 2로 멈추면 PKI 배치 후 재실행.

**Redeploy (git pull 후)**:

```bash
cd /home/seongwon/nangman-crypto/apps/market-ingest-app
./scripts/deploy.sh
```

deploy.sh는 host clock sync(`timedatectl NTPSynchronized=yes`) 검증, `docker compose config` 검증, image build, container-side AWS/S3 preflight, Binance·Upbit·normalize service 재기동, 상태 출력을 한 번에 한다. NTP unsync면 watermark 결정이 흔들리므로 service 시작을 막는다.

**수동 stop**:

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  down
```

**Normalize만 띄우기**:

```bash
sudo docker compose \
  -f /home/seongwon/nangman-crypto/apps/market-ingest-app/compose.yml \
  --env-file /home/seongwon/nangman-crypto/apps/market-ingest-app/.env \
  up -d market-normalize
```

### Manual smoke / audit

`cargo run`으로 직접 워커를 띄우는 절차. 운영에는 사용하지 않고 short smoke 검증·debug·audit에만 쓴다.

```bash
# 공통 prefix
ROOT=/opt/nangman-crypto/apps/market-ingest-app
CFG=/opt/nangman-crypto/strategies/crypto/rust-engine/config
BUCKET=nangman-crypto-dev-market-ingest-l0-<account-suffix>
```

**realtime smoke (in-memory only)**:

```bash
cargo run --manifest-path $ROOT/Cargo.toml -- \
  --venue binance \
  --config $CFG \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --depth-snapshot-limit 100
```

`--venue upbit`로 venue 전환. Upbit는 `--upbit-orderbook-unit 5` 추가.

**realtime smoke + L0 S3 storage**:

```bash
cargo run --manifest-path $ROOT/Cargo.toml -- \
  --venue binance \
  --config $CFG \
  --duration-seconds 15 \
  --log-interval-seconds 5 \
  --expect-symbol-count 50 \
  --allow-partial-symbol-coverage \
  --l0-s3-bucket $BUCKET \
  --aws-region ap-northeast-2 \
  --l0-flush-records 1000
```

Binance public stream은 짧은 smoke 윈도우 안에서 모든 구독 심볼이 발화한다는 보장이 없다. 짧은 storage 검증에는 `--allow-partial-symbol-coverage`를 사용한다.

**historical backfill (one-shot)**:

```bash
cargo run --manifest-path $ROOT/Cargo.toml --bin market-backfill -- \
  --venue binance \
  --config $CFG \
  --input-start-ms 1778042400000 \
  --input-end-ms 1778043300000 \
  --l0-s3-bucket $BUCKET
```

Upbit backfill은 recent trade history만 지원하며 `--symbols KRW-BTC,KRW-ETH` 같이 명시. 오래된 범위는 거부한다.

**manual normalize (one-shot)**:

```bash
cargo run --manifest-path $ROOT/Cargo.toml --bin market-normalize -- \
  --l0-s3-bucket $BUCKET \
  --l0-local-root /opt/nangman-crypto/data/spool/market-ingest/l0 \
  --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
  --catchup-tmp-root /opt/nangman-crypto/data/spool/market-normalize/catchup \
  --input-start-ms 1778042400000 \
  --input-end-ms 1778043300000
```

**L1 index coverage audit**:

```bash
cargo run --manifest-path $ROOT/Cargo.toml --bin market-normalize -- \
  --l0-s3-bucket $BUCKET \
  --l1-s3-bucket nangman-crypto-dev-market-ingest-l1-<account-suffix> \
  --aws-profile market-ingest-roles-anywhere \
  --aws-region ap-northeast-2 \
  --audit-l1-index-start-ms 1778042400000 \
  --audit-l1-index-end-ms 1778043300000
```

---

## Monitoring

### Structured logs

```text
schema: market_ingest_log_v1
format: JSON Lines (stdout=info+, stderr=warn/error)
common fields: app, level, event, timestamp_ms, fields
default level: MARKET_INGEST_LOG_LEVEL=info
```

Level별 의미:

```text
error - 복구 불가, process 종료 가능
warn  - degraded 또는 운영자 조치 필요
info  - lifecycle, report, 성공 publish 요약
debug - 고빈도 progress, heartbeat, not-ready polling, 윈도우별 시작
```

**Lifecycle events (info)**:

- `crypto_market_ingest_supervisor_start`
- `crypto_market_ingest_live_priority_normalize_start`
- `crypto_market_ingest_bootstrap_chunk_start` / `_l0_done` / `_l1_done` / `_chunk_done` / `_complete`
- `crypto_market_ingest_s3_retention_run`
- `market_ingest_start` / `market_ingest_report`
- `market_ingest_eviction_run`
- `market_backfill_done` / `market_backfill_s3_retention_run`
- `market_normalize_preflight_ok` / `market_normalize_worker_started` / `market_normalize_worker_stopped`
- `market_normalize_index_published` / `market_normalize_l1_index_audit`
- `market_normalize_finished` / `market_normalize_s3_retention_run`
- `market_normalize_fallback_alert`

**Errors (stderr)**:

- `market_ingest_error`
- `market_normalize_error`
- `market_backfill_error`
- `crypto_market_ingest_supervisor_error`

**Debug-only**:

- `market_ingest_progress`
- `market_ingest_unsealed_orphan_cleanup` (invalid orphan 없을 때)
- `market_ingest_eviction_heartbeat`
- `market_normalize_started` / `_not_ready` / `_worker_sleep`

**예시**:

```json
{"schema_version":"market_ingest_log_v1","app":"market-ingest-app","level":"info","event":"market_normalize_finished","timestamp_ms":1777976991000,"fields":{"l1_run_id":"l1_1777975200000_1777976100000_1777976991000","status":"success","slice_count_total":90000,"output_object_count":2}}
```

**유용한 명령어**:

```bash
# follow
sudo docker compose -f $ROOT/compose.yml --env-file $ROOT/.env logs -f --no-log-prefix

# 구조 로그만
sudo docker compose -f $ROOT/compose.yml --env-file $ROOT/.env logs --no-log-prefix \
  | jq -c 'select(.schema_version == "market_ingest_log_v1")'

# verbose 일시 활성화
sudo env MARKET_INGEST_LOG_LEVEL=debug docker compose \
  -f $ROOT/compose.yml --env-file $ROOT/.env \
  up -d --force-recreate
```

### Health signals 모니터링

운영에서 주기적으로 봐야 할 핵심 지표.

```text
source_health.reconnect_count        - 0에 가깝게 유지. spike는 WS 인프라 이슈.
source_health.buffer_overflow_count  - 0에 가깝게 유지. spike는 snapshot 경로 장애.
gap_alert(gap_type=ws_reconnect)     - heal_status에서 원인 분류 (stale_timeout 등)
gap_alert(gap_type=buffered_overflow) - dropped_count={N}으로 손실 규모 확인
gap_alert(gap_type=depth_update_id_gap) - Binance depth sequence 단절
market_ingest_progress.health        - degraded/critical 지속 여부
```

---

## DoD

### L0 ingest DoD

- Binance와 Upbit 모두 public market stream만 사용.
- private API, account credential, order placement, live trading 경로 없음.
- Binance는 `reference`, Upbit는 `execution`으로 저장.
- `raw_market_event_v2` Parquet이 L0 bucket에 저장됨.
- `source_health_v2`가 run당 venue별 최소 1개.
- `symbol_health_v1`가 run당 venue별 최소 1개.
- gap이 감지되면 `gap_alert_v1`이 별도 prefix에 저장됨.
- `runs/run_id={run_id}/manifest.json`에 업로드된 객체 키와 record count 포함.
- 모든 객체가 `event_date`, `hour`, `shard` partition을 가짐.
- run 종료 전 모든 buffer flush.
- `cargo fmt`, `cargo check`, `cargo test` 통과.
- short smoke에서 S3 listing으로 `raw_market_event`, `source_health`, `symbol_health`, `manifest` 존재 확인.
- 비활성 심볼 때문에 coverage 부족 가능성이 있는 venue는 `allow_partial_symbol_coverage` 명시.

### 운영 모드 DoD (추가)

- 무기한 실행 모드 존재.
- in-process WS reconnect (initial=1s, max=60s, mult=2, stale_timeout=30s, max_failures=무한).
- 매 reconnect마다 `gap_alert(ws_reconnect)` + `source_health.reconnect_count++`.
- depth book buffered_events 상한 1_000. silent drop 금지. 초과 시 `gap_alert(buffered_overflow)` + `source_health.buffer_overflow_count++`.
- Binance in-memory order book은 `BTreeMap<FixedDecimal, FixedDecimal>`. golden fixture round-trip 테스트가 CI에서 통과.
- 종료 신호 수신 시 graceful shutdown으로 buffer flush + manifest upload 보장.
- S3 upload 지연 시 bounded buffer/backpressure 정책 명확.
- 장시간 run에서 `source_health` 주기 저장.

### Container DoD

- `docker compose config` 무오류 렌더링.
- `MARKET_INGEST_REPO_ROOT`가 실제 host clone 경로를 가리킴.
- Docker build가 `NANGMAN_GIT_SHA` / `NANGMAN_GIT_DIRTY`를 compile-time build arg로 주입.
- Binance와 Upbit service 모두 compose에서 시작.
- IAM Roles Anywhere config와 PKI material은 ignored app infra에서 read-only mount.
- local L0 데이터는 `/opt/nangman-crypto/data/spool/market-ingest/l0`에 LIVE L1 input cache로 보존.
- fallback 다운로드는 `/opt/nangman-crypto/data/spool/market-normalize/catchup`.
- L0 S3 출력은 `nangman-crypto-dev-market-ingest-l0-<account-suffix>` (override: `MARKET_L0_BUCKET`).
- L1 normalize 출력은 `nangman-crypto-dev-market-ingest-l1-<account-suffix>` (override: `MARKET_L1_BUCKET`).
- app-owned retention: L0 = 45일, L1 = 240일, cleanup loop 주기 = 6시간.
- bucket lifecycle은 fallback safety net: L0 = 60일, L1 = 300일, `normalized_market_slice/` = 30일 후 Standard-IA.
- L1 universe bootstrap이 `symbol_universe_snapshot/bootstrap_rollup/*` 쓰고 읽음 (30-day point-in-time universe approval).
- 런타임 role: L0/L1 bucket에 `ListBucket`, `GetObject`, `PutObject`, `_preflight/market-ingest-app/*`와 retention prefix에는 `DeleteObject`.
- SSE-KMS 사용 시 매칭 KMS `GenerateDataKey`, `Decrypt` 권한.

---

_Last updated: 2026-05-23 KST (runtime proof and build provenance reflected)_
