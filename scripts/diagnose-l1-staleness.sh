#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
ENV_FILE="${MARKET_INGEST_ENV_FILE:-$APP_DIR/.env}"

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'L1 staleness diagnosis failed: %s\n' "$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "missing required command: $1"
  fi
}

load_env_file() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
  fi
}

require_real_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" || "$value" == *"<"* || "$value" == *">"* ]]; then
    die "$name must be set to a real value before diagnosis"
  fi
}

aws_cli() {
  local args=(--region "$AWS_REGION")
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    args+=(--profile "$AWS_PROFILE")
  fi
  aws "${args[@]}" "$@"
}

start_time_ms() {
  python3 - "$MARKET_INGEST_DIAGNOSE_LOOKBACK_MINUTES" <<'PY'
import sys
import time

minutes = int(sys.argv[1])
print(int((time.time() - minutes * 60) * 1000))
PY
}

log_count() {
  local pattern="$1"
  local start_ms="$2"
  aws_cli logs filter-log-events \
    --log-group-name "$MARKET_INGEST_LOG_GROUP" \
    --start-time "$start_ms" \
    --filter-pattern "$pattern" \
    --query 'events' \
    --output json | python3 -c 'import json, sys; print(len(json.load(sys.stdin)))'
}

latest_log_messages() {
  local pattern="$1"
  local start_ms="$2"
  local limit="$3"
  local events_json
  events_json="$(aws_cli logs filter-log-events \
    --log-group-name "$MARKET_INGEST_LOG_GROUP" \
    --start-time "$start_ms" \
    --filter-pattern "$pattern" \
    --limit "$limit" \
    --query 'events' \
    --output json)"
  EVENTS_JSON="$events_json" python3 - "$limit" <<'PY'
import json
import os
import sys

limit = int(sys.argv[1])
events = json.loads(os.environ["EVENTS_JSON"])
for event in events[-limit:]:
    message = event.get("message", "").strip().replace("\n", " ")
    if message:
        print(message)
PY
}

summarize_task_definition() {
  local task_json="$1"
  TASK_JSON="$task_json" python3 <<'PY'
import json
import os

task = json.loads(os.environ["TASK_JSON"]).get("taskDefinition", {})
containers = task.get("containerDefinitions", [])
container = containers[0] if containers else {}
command = container.get("command") or []
print(f"taskDefinition={task.get('taskDefinitionArn')}")
print(f"cpu={task.get('cpu')} memory={task.get('memory')} arch={task.get('runtimePlatform', {}).get('cpuArchitecture')}")
print(f"image={container.get('image')}")
print(f"readonlyRootFilesystem={container.get('readonlyRootFilesystem')} user={container.get('user')}")
print(f"command_has_buckets={'--l0-s3-bucket' in command and '--l1-s3-bucket' in command}")
PY
}

recent_hour_rows() {
  python3 - "$MARKET_INGEST_S3_LOOKBACK_HOURS" <<'PY'
from datetime import datetime, timezone, timedelta
import sys

lookback_hours = int(sys.argv[1])
now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
for offset in range(lookback_hours):
    current = now - timedelta(hours=offset)
    print(f"{current:%Y-%m-%d}\t{current:%H}")
PY
}

first_s3_object_json() {
  local bucket="$1"
  local prefix="$2"
  aws_cli s3api list-objects-v2 \
    --bucket "$bucket" \
    --prefix "$prefix" \
    --max-items 1 \
    --query '(Contents || `[]`)[0].{key:Key,size:Size,lastModified:LastModified}' \
    --output json
}

object_has_key() {
  local value="$1"
  OBJECT_JSON="$value" python3 - <<'PY'
import json
import os
import sys

try:
    value = json.loads(os.environ["OBJECT_JSON"])
except json.JSONDecodeError:
    sys.exit(1)
sys.exit(0 if value and value.get("key") else 1)
PY
}

print_object_or_missing() {
  local label="$1"
  local bucket="$2"
  local prefix="$3"
  local latest
  latest="$(first_s3_object_json "$bucket" "$prefix")"
  LATEST_JSON="$latest" python3 - "$label" "$prefix" <<'PY'
import json
import os
import sys

label = sys.argv[1]
prefix = sys.argv[2]
value = json.loads(os.environ["LATEST_JSON"])
if value and value.get("key"):
    print(f"{label}: {value['key']} size={value.get('size')} lastModified={value.get('lastModified')}")
else:
    print(f"{label}: missing under {prefix}")
PY
}

print_recent_s3_samples() {
  local event_date
  local hour
  local event_type
  local checked_l1_index=0
  local checked_l1_slice=0

  while IFS=$'\t' read -r event_date hour; do
    for event_type in trade book_ticker depth_delta; do
      print_object_or_missing \
        "L0 raw ${event_type} ${event_date}T${hour}Z" \
        "$MARKET_L0_BUCKET" \
        "raw_market_event/venue=$MARKET_INGEST_VENUE/event_type=$event_type/event_date=$event_date/hour=$hour/"
    done
    if [[ "$checked_l1_index" == "0" ]]; then
      print_object_or_missing \
        "L1 index ${event_date}T${hour}Z" \
        "$MARKET_L1_BUCKET" \
        "l1_index/window_ms=$MARKET_INGEST_WINDOW_MS/event_date=$event_date/hour=$hour/"
      checked_l1_index=1
    fi
    if [[ "$checked_l1_slice" == "0" ]]; then
      print_object_or_missing \
        "L1 slice ${event_date}T${hour}Z" \
        "$MARKET_L1_BUCKET" \
        "normalized_market_slice/venue=$MARKET_INGEST_VENUE/event_date=$event_date/hour=$hour/window_ms=$MARKET_INGEST_WINDOW_MS/"
      checked_l1_slice=1
    fi
  done < <(recent_hour_rows)
}

self_test() {
  local summary
  summary="$(summarize_task_definition '{
    "taskDefinition": {
      "taskDefinitionArn": "arn:aws:ecs:ap-northeast-2:<aws-account-id>:task-definition/mock:16",
      "cpu": "2048",
      "memory": "4096",
      "runtimePlatform": {"cpuArchitecture": "ARM64"},
      "containerDefinitions": [{
        "image": "example-image",
        "readonlyRootFilesystem": true,
        "user": "nonroot:nonroot",
        "command": ["--l0-s3-bucket", "mock-l0", "--l1-s3-bucket", "mock-l1"]
      }]
    }
  }')"
  [[ "$summary" == *"arch=ARM64"* ]] || die "self-test expected ARM64 summary"
  [[ "$summary" == *"readonlyRootFilesystem=True"* ]] || die "self-test expected readonly summary"
  [[ "$summary" == *"command_has_buckets=True"* ]] || die "self-test expected bucket command summary"
  log "diagnose-l1-staleness self-test passed"
}

main() {
  require_command aws
  require_command python3
  load_env_file

  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  MARKET_INGEST_CLUSTER="${MARKET_INGEST_CLUSTER:-ecs-nangman-dev-invest-apn2}"
  MARKET_INGEST_SERVICE="${MARKET_INGEST_SERVICE:-svc-nangman-dev-crypto-market-ingest}"
  MARKET_INGEST_LOG_GROUP="${MARKET_INGEST_LOG_GROUP:-/ecs/nangman/dev/crypto-market-ingest}"
  MARKET_INGEST_DIAGNOSE_LOOKBACK_MINUTES="${MARKET_INGEST_DIAGNOSE_LOOKBACK_MINUTES:-60}"
  MARKET_INGEST_S3_LOOKBACK_HOURS="${MARKET_INGEST_S3_LOOKBACK_HOURS:-6}"
  MARKET_INGEST_VENUE="${MARKET_INGEST_VENUE:-binance}"
  MARKET_INGEST_WINDOW_MS="${MARKET_INGEST_WINDOW_MS:-1000}"
  MARKET_L0_BUCKET="${MARKET_L0_BUCKET:-${L0_S3_BUCKET:-}}"
  MARKET_L1_BUCKET="${MARKET_L1_BUCKET:-${L1_S3_BUCKET:-}}"

  require_real_value AWS_REGION "$AWS_REGION"
  require_real_value MARKET_INGEST_CLUSTER "$MARKET_INGEST_CLUSTER"
  require_real_value MARKET_INGEST_SERVICE "$MARKET_INGEST_SERVICE"
  require_real_value MARKET_INGEST_LOG_GROUP "$MARKET_INGEST_LOG_GROUP"
  require_real_value MARKET_L0_BUCKET "$MARKET_L0_BUCKET"
  require_real_value MARKET_L1_BUCKET "$MARKET_L1_BUCKET"

  local start_ms
  local task_definition_arn
  local task_json
  local live_priority_count
  local index_count
  local finished_count
  local bootstrap_l1_count

  start_ms="$(start_time_ms)"

  log "[1/4] ECS task/image"
  task_definition_arn="$(aws_cli ecs describe-services \
    --cluster "$MARKET_INGEST_CLUSTER" \
    --services "$MARKET_INGEST_SERVICE" \
    --query 'services[0].taskDefinition' \
    --output text)"
  task_json="$(aws_cli ecs describe-task-definition \
    --task-definition "$task_definition_arn" \
    --output json)"
  summarize_task_definition "$task_json"

  log "[2/4] CloudWatch lifecycle counts (${MARKET_INGEST_DIAGNOSE_LOOKBACK_MINUTES}m)"
  live_priority_count="$(log_count "crypto_market_ingest_live_priority_normalize_start" "$start_ms")"
  index_count="$(log_count "market_normalize_index_published" "$start_ms")"
  finished_count="$(log_count "market_normalize_finished" "$start_ms")"
  bootstrap_l1_count="$(log_count "crypto_market_ingest_bootstrap_l1_start" "$start_ms")"
  printf 'crypto_market_ingest_live_priority_normalize_start=%s\n' "$live_priority_count"
  printf 'market_normalize_index_published=%s\n' "$index_count"
  printf 'market_normalize_finished=%s\n' "$finished_count"
  printf 'crypto_market_ingest_bootstrap_l1_start=%s\n' "$bootstrap_l1_count"
  printf 'market_normalize_error=%s\n' "$(log_count "market_normalize_error" "$start_ms")"
  printf 'crypto_market_ingest_supervisor_error=%s\n' "$(log_count "crypto_market_ingest_supervisor_error" "$start_ms")"

  log "[3/4] Latest matching lifecycle samples"
  latest_log_messages "crypto_market_ingest_bootstrap_l1_start" "$start_ms" 5 || true
  latest_log_messages "market_normalize_index_published" "$start_ms" 5 || true
  latest_log_messages "crypto_market_ingest_live_priority_normalize_start" "$start_ms" 5 || true

  log "[4/4] Recent S3 samples (${MARKET_INGEST_S3_LOOKBACK_HOURS}h)"
  print_recent_s3_samples

  log "diagnosis_hints"
  if [[ "$live_priority_count" == "0" && "$bootstrap_l1_count" != "0" ]]; then
    log "- live-priority normalize was not observed while bootstrap L1 was active; current deployed image may predate the live-priority fix or the path is not starting."
  fi
  if [[ "$index_count" == "0" && "$finished_count" == "0" ]]; then
    log "- no recent normalize success/index logs were observed; L1 freshness cannot be proven from logs."
  fi
  log "- this script is read-only: no ECS update, task restart, image push, or S3 mutation was performed."
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ "${MARKET_INGEST_DIAGNOSE_L1_SELF_TEST:-}" == "1" ]]; then
    require_command python3
    self_test
  else
    main "$@"
  fi
fi
