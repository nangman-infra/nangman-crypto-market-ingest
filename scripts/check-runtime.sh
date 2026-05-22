#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
ENV_FILE="${MARKET_INGEST_ENV_FILE:-$APP_DIR/.env}"

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'runtime check failed: %s\n' "$*" >&2
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
    die "$name must be set to a real value before runtime verification"
  fi
}

run_runtime_check() {
  local label="$1"
  shift
  local output
  log "$label"
  if output="$("$@" 2>&1)"; then
    if [[ -n "$output" ]]; then
      printf '%s\n' "$output"
    fi
  else
    local compact
    if [[ -n "$output" ]]; then
      printf '%s\n' "$output" >&2
      compact="${output//$'\n'/ | }"
    else
      compact="no diagnostic output"
    fi
    RUNTIME_CHECK_FAILURES+=("$label: $compact")
  fi
}

aws_cli() {
  local args=(--region "$AWS_REGION")
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    args+=(--profile "$AWS_PROFILE")
  fi
  aws "${args[@]}" "$@"
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

require_s3_object() {
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
if not value or not value.get("key"):
    print(f"{label}: missing object under {prefix}", file=sys.stderr)
    sys.exit(1)
print(f"{label}: {value['key']} size={value.get('size')} lastModified={value.get('lastModified')}")
PY
}

require_recent_s3_object() {
  local label="$1"
  local bucket="$2"
  local latest
  local prefix
  while IFS= read -r prefix; do
    if [[ -z "$prefix" ]]; then
      continue
    fi
    latest="$(first_s3_object_json "$bucket" "$prefix")"
    if object_has_key "$latest"; then
      FOUND_S3_KEY="$(object_key_from_json "$latest")"
      LATEST_JSON="$latest" python3 - "$label" <<'PY'
import json
import os
import sys

label = sys.argv[1]
value = json.loads(os.environ["LATEST_JSON"])
print(f"{label}: {value['key']} size={value.get('size')} lastModified={value.get('lastModified')}")
PY
      return 0
    fi
  done
  die "$label missing recent object in the last ${MARKET_INGEST_S3_LOOKBACK_HOURS} UTC hours"
}

find_recent_s3_object_json() {
  local bucket="$1"
  local latest
  local prefix
  while IFS= read -r prefix; do
    if [[ -z "$prefix" ]]; then
      continue
    fi
    latest="$(first_s3_object_json "$bucket" "$prefix")"
    if object_has_key "$latest"; then
      printf '%s\n' "$latest"
      return 0
    fi
  done
  return 1
}

print_s3_object_json() {
  local label="$1"
  local latest="$2"
  LATEST_JSON="$latest" python3 - "$label" <<'PY'
import json
import os
import sys

label = sys.argv[1]
value = json.loads(os.environ["LATEST_JSON"])
print(f"{label}: {value['key']} size={value.get('size')} lastModified={value.get('lastModified')}")
PY
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

object_key_from_json() {
  local value="$1"
  OBJECT_JSON="$value" python3 - <<'PY'
import json
import os

print(json.loads(os.environ["OBJECT_JSON"])["key"])
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

recent_l0_raw_prefixes() {
  local event_date
  local hour
  local event_type
  while IFS=$'\t' read -r event_date hour; do
    for event_type in $MARKET_INGEST_RAW_EVENT_TYPES; do
      printf 'raw_market_event/venue=%s/event_type=%s/event_date=%s/hour=%s/\n' \
        "$MARKET_INGEST_VENUE" "$event_type" "$event_date" "$hour"
    done
  done < <(recent_hour_rows)
}

recent_l0_family_prefixes() {
  local family="$1"
  local event_date
  local hour
  while IFS=$'\t' read -r event_date hour; do
    printf '%s/venue=%s/event_date=%s/hour=%s/\n' \
      "$family" "$MARKET_INGEST_VENUE" "$event_date" "$hour"
  done < <(recent_hour_rows)
}

recent_l1_index_prefixes() {
  local event_date
  local hour
  while IFS=$'\t' read -r event_date hour; do
    printf 'l1_index/window_ms=%s/event_date=%s/hour=%s/\n' \
      "$MARKET_INGEST_WINDOW_MS" "$event_date" "$hour"
  done < <(recent_hour_rows)
}

recent_l1_slice_prefixes() {
  local event_date
  local hour
  while IFS=$'\t' read -r event_date hour; do
    printf 'normalized_market_slice/venue=%s/event_date=%s/hour=%s/window_ms=%s/\n' \
      "$MARKET_INGEST_VENUE" "$event_date" "$hour" "$MARKET_INGEST_WINDOW_MS"
  done < <(recent_hour_rows)
}

validate_l1_pointer_readability() {
  local pointer_key="$1"
  local tmp_dir="$RUNTIME_CHECK_TMP_DIR/l1-pointer"
  local pointer_path="$tmp_dir/pointer.json"
  local manifest_path="$tmp_dir/manifest.json"
  local report_path="$tmp_dir/report.json"
  local manifest_key
  local report_key
  local output_key

  mkdir -p "$tmp_dir"
  aws_cli s3api get-object \
    --bucket "$MARKET_L1_BUCKET" \
    --key "$pointer_key" \
    "$pointer_path" >/dev/null

  manifest_key="$(python3 - "$pointer_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    pointer = json.load(handle)
if pointer.get("schema_version") != "l1_index_pointer_v1":
    raise SystemExit(f"unexpected pointer schema_version: {pointer.get('schema_version')}")
if pointer.get("status") != "success":
    raise SystemExit(f"l1 pointer status is not success: {pointer.get('status')}")
if pointer.get("schema_version_emitted") != "normalized_market_slice_v1":
    raise SystemExit(
        f"unexpected emitted schema: {pointer.get('schema_version_emitted')}"
    )
manifest_key = pointer.get("canonical_manifest_key")
if not manifest_key:
    raise SystemExit("l1 pointer missing canonical_manifest_key")
print(manifest_key)
PY
)"

  aws_cli s3api get-object \
    --bucket "$MARKET_L1_BUCKET" \
    --key "$manifest_key" \
    "$manifest_path" >/dev/null

  python3 - "$manifest_path" > "$tmp_dir/manifest-fields.txt" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    manifest = json.load(handle)
if manifest.get("schema_version") != "l1_manifest_v1":
    raise SystemExit(f"unexpected manifest schema_version: {manifest.get('schema_version')}")
if manifest.get("status") != "success":
    raise SystemExit(f"l1 manifest status is not success: {manifest.get('status')}")
if manifest.get("schema_version_emitted") != "normalized_market_slice_v1":
    raise SystemExit(
        f"unexpected manifest emitted schema: {manifest.get('schema_version_emitted')}"
    )
if int(manifest.get("output_record_count", 0)) <= 0:
    raise SystemExit("l1 manifest output_record_count must be positive")
output_keys = manifest.get("output_object_keys") or []
if not output_keys:
    raise SystemExit("l1 manifest output_object_keys must not be empty")
report_key = manifest.get("report_key")
if not report_key:
    raise SystemExit("l1 manifest missing report_key")
print(report_key)
print(output_keys[0])
PY
  report_key="$(sed -n '1p' "$tmp_dir/manifest-fields.txt")"
  output_key="$(sed -n '2p' "$tmp_dir/manifest-fields.txt")"

  aws_cli s3api get-object \
    --bucket "$MARKET_L1_BUCKET" \
    --key "$report_key" \
    "$report_path" >/dev/null

  python3 - "$report_path" "$manifest_key" "$output_key" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
manifest_key = sys.argv[2]
output_key = sys.argv[3]
if report.get("schema_version") != "normalization_report_v1":
    raise SystemExit(f"unexpected report schema_version: {report.get('schema_version')}")
if report.get("status") != "success":
    raise SystemExit(f"l1 report status is not success: {report.get('status')}")
if report.get("manifest_key") != manifest_key:
    raise SystemExit("l1 report manifest_key does not match pointer manifest")
if output_key not in (report.get("output_object_keys") or []):
    raise SystemExit("l1 report does not reference sampled output object")
if int(report.get("slice_count_total", 0)) <= 0:
    raise SystemExit("l1 report slice_count_total must be positive")
PY

  aws_cli s3api head-object \
    --bucket "$MARKET_L1_BUCKET" \
    --key "$output_key" \
    --query '{key: Key, size: ContentLength, lastModified: LastModified}' \
    --output json >/dev/null

  log "L1 downstream readability ok: pointer=$pointer_key manifest=$manifest_key report=$report_key sample=$output_key"
}

validate_positive_integer() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    die "$name must be a positive integer"
  fi
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  if [[ "$haystack" != *"$needle"* ]]; then
    die "self-test expected text to contain: $needle"
  fi
}

assert_equals() {
  local actual="$1"
  local expected="$2"
  if [[ "$actual" != "$expected" ]]; then
    die "self-test expected '$expected' but got '$actual'"
  fi
}

assert_service_state() {
  local service_json="$1"
  SERVICE_JSON="$service_json" python3 - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["SERVICE_JSON"])
services = data.get("services", [])
if len(services) != 1:
    print("expected exactly one ECS service result", file=sys.stderr)
    sys.exit(1)
service = services[0]
failures = data.get("failures", [])
if failures:
    print(f"ECS describe-services returned failures: {failures}", file=sys.stderr)
    sys.exit(1)
desired = int(service.get("desiredCount", 0))
running = int(service.get("runningCount", 0))
pending = int(service.get("pendingCount", 0))
rollouts = {deployment.get("rolloutState", "") for deployment in service.get("deployments", [])}
capacity_providers = {item.get("capacityProvider", "") for item in service.get("capacityProviderStrategy", [])}
if desired < 1:
    print("ECS desiredCount is below 1", file=sys.stderr)
    sys.exit(1)
if running < desired:
    print(f"ECS runningCount {running} is below desiredCount {desired}", file=sys.stderr)
    sys.exit(1)
if pending != 0:
    print(f"ECS pendingCount is not zero: {pending}", file=sys.stderr)
    sys.exit(1)
if "COMPLETED" not in rollouts:
    print(f"ECS rollout is not completed: {sorted(rollouts)}", file=sys.stderr)
    sys.exit(1)
if "FARGATE_SPOT" not in capacity_providers:
    print(f"ECS service is not using FARGATE_SPOT capacity provider: {sorted(capacity_providers)}", file=sys.stderr)
    sys.exit(1)
print(f"ECS service ok: desired={desired} running={running} pending={pending} rollout=COMPLETED capacity=FARGATE_SPOT")
print(service.get("taskDefinition", ""))
PY
}

assert_task_definition_state() {
  local task_json="$1"
  TASK_JSON="$task_json" python3 - <<'PY'
import json
import os
import sys

task = json.loads(os.environ["TASK_JSON"]).get("taskDefinition", {})
platform = task.get("runtimePlatform", {})
containers = task.get("containerDefinitions", [])
if platform.get("cpuArchitecture") != "ARM64":
    print(f"task cpuArchitecture is not ARM64: {platform}", file=sys.stderr)
    sys.exit(1)
if "FARGATE" not in task.get("requiresCompatibilities", []):
    print("task definition is missing FARGATE compatibility", file=sys.stderr)
    sys.exit(1)
if len(containers) != 1:
    print("market-ingest production task must have exactly one supervisor container", file=sys.stderr)
    sys.exit(1)
container = containers[0]
if container.get("readonlyRootFilesystem") is not True:
    print("container readonlyRootFilesystem must be true", file=sys.stderr)
    sys.exit(1)
container_user = (container.get("user") or "").strip().lower()
if container_user in {"", "0", "0:0", "root", "root:root"}:
    print(f"container user must be an explicit non-root user, got: {container.get('user')}", file=sys.stderr)
    sys.exit(1)
capability_drops = {
    item.upper()
    for item in (
        container.get("linuxParameters", {})
        .get("capabilities", {})
        .get("drop", [])
    )
}
if "ALL" not in capability_drops:
    print("container linuxParameters.capabilities.drop must include ALL", file=sys.stderr)
    sys.exit(1)
command = container.get("command", [])
if "--l0-s3-bucket" not in command or "--l1-s3-bucket" not in command:
    print("supervisor command must pass explicit L0/L1 buckets", file=sys.stderr)
    sys.exit(1)
print(f"task definition ok: cpu={task.get('cpu')} memory={task.get('memory')} arch=ARM64 container={container.get('name')} user={container.get('user')}")
PY
}

assert_log_group_retention_state() {
  local log_groups_json="$1"
  LOG_GROUPS_JSON="$log_groups_json" python3 - "$MARKET_INGEST_LOG_GROUP" <<'PY'
import json
import os
import sys

expected_name = sys.argv[1]
data = json.loads(os.environ["LOG_GROUPS_JSON"])
matches = [
    group
    for group in data.get("logGroups", [])
    if group.get("logGroupName") == expected_name
]
if len(matches) != 1:
    print(f"expected exactly one CloudWatch log group named {expected_name}", file=sys.stderr)
    sys.exit(1)
retention_days = matches[0].get("retentionInDays")
if retention_days != 3:
    print(
        f"CloudWatch log group retention must be 3 days, got {retention_days}",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"CloudWatch log retention ok: group={expected_name} retentionDays=3")
PY
}

assert_log_group_retention() {
  local log_groups_json
  log_groups_json="$(aws_cli logs describe-log-groups \
    --log-group-name-prefix "$MARKET_INGEST_LOG_GROUP" \
    --output json)"
  assert_log_group_retention_state "$log_groups_json"
}

assert_metric_threshold() {
  local metric_name="$1"
  local metric_json="$2"
  local threshold="$3"
  METRIC_JSON="$metric_json" python3 - "$metric_name" "$threshold" <<'PY'
import json
import os
import sys

metric_name = sys.argv[1]
threshold = float(sys.argv[2])
data = json.loads(os.environ["METRIC_JSON"])
datapoints = data.get("Datapoints", [])
if not datapoints:
    print(f"{metric_name} has no CloudWatch datapoints in the runtime window", file=sys.stderr)
    sys.exit(1)
maximum = max(float(point.get("Maximum", 0)) for point in datapoints)
if maximum > threshold:
    print(
        f"{metric_name} maximum {maximum:.2f}% exceeds threshold {threshold:.2f}%",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"{metric_name} ok: max={maximum:.2f}% threshold={threshold:.2f}% datapoints={len(datapoints)}")
PY
}

metric_window_args() {
  python3 - "$MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES" <<'PY'
from datetime import datetime, timedelta, timezone
import sys

lookback_minutes = int(sys.argv[1])
end = datetime.now(timezone.utc).replace(microsecond=0)
start = end - timedelta(minutes=lookback_minutes)
print(start.isoformat())
print(end.isoformat())
PY
}

cloudwatch_ecs_metric() {
  local metric_name="$1"
  local window
  local start_time
  local end_time
  window="$(metric_window_args)"
  start_time="$(printf '%s\n' "$window" | sed -n '1p')"
  end_time="$(printf '%s\n' "$window" | sed -n '2p')"
  aws_cli cloudwatch get-metric-statistics \
    --namespace AWS/ECS \
    --metric-name "$metric_name" \
    --dimensions "Name=ClusterName,Value=$MARKET_INGEST_CLUSTER" "Name=ServiceName,Value=$MARKET_INGEST_SERVICE" \
    --start-time "$start_time" \
    --end-time "$end_time" \
    --period 60 \
    --statistics Maximum \
    --output json
}

check_resource_pressure() {
  local cpu_json
  local memory_json
  cpu_json="$(cloudwatch_ecs_metric CPUUtilization)"
  memory_json="$(cloudwatch_ecs_metric MemoryUtilization)"
  assert_metric_threshold CPUUtilization "$cpu_json" "$MARKET_INGEST_MAX_CPU_UTILIZATION_PCT"
  assert_metric_threshold MemoryUtilization "$memory_json" "$MARKET_INGEST_MAX_MEMORY_UTILIZATION_PCT"
}

assert_recent_logs_clean() {
  local now_ms
  local start_ms
  now_ms="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"
  start_ms="$((now_ms - MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES * 60 * 1000))"

  local error_count
  error_count="$(sum_log_matches "$start_ms" \
    error panic SIGKILL Killed OutOfMemory AccessDenied)"
  if [[ "$error_count" != "0" ]]; then
    die "CloudWatch has $error_count recent error events in $MARKET_INGEST_LOG_GROUP"
  fi

  local lifecycle_count
  lifecycle_count="$(sum_log_matches "$start_ms" \
    market_ingest_report \
    market_normalize_finished \
    market_normalize_index_published \
    crypto_market_ingest_bootstrap_chunk_done \
    crypto_market_ingest_live_priority_normalize_start)"
  if [[ "$lifecycle_count" == "0" ]]; then
    die "CloudWatch has no recent lifecycle output in $MARKET_INGEST_LOG_GROUP"
  fi
  log "CloudWatch logs ok: recent_error_events=0 recent_lifecycle_events=$lifecycle_count"
}

sum_log_matches() {
  local start_ms="$1"
  shift
  local total=0
  local term
  local count
  for term in "$@"; do
    count="$(aws_cli logs filter-log-events \
      --log-group-name "$MARKET_INGEST_LOG_GROUP" \
      --start-time "$start_ms" \
      --filter-pattern "$term" \
      --query 'events' \
      --output json | python3 -c 'import json, sys; print(len(json.load(sys.stdin)))')"
    total="$((total + count))"
  done
  printf '%s\n' "$total"
}

check_aws_identity() {
  aws_cli sts get-caller-identity --query 'Arn' --output text >/dev/null
  log "AWS identity ok"
}

check_ecs_service() {
  local service_json
  local service_output
  service_json="$(aws_cli ecs describe-services \
    --cluster "$MARKET_INGEST_CLUSTER" \
    --services "$MARKET_INGEST_SERVICE" \
    --output json)"
  service_output="$(assert_service_state "$service_json")"
  printf '%s\n' "$service_output"
  TASK_DEFINITION_ARN="$(printf '%s\n' "$service_output" | tail -n 1)"
  printf '%s\n' "$TASK_DEFINITION_ARN" > "$RUNTIME_CHECK_TMP_DIR/task-definition-arn"
}

check_ecs_task_definition() {
  local task_json
  if [[ -z "${TASK_DEFINITION_ARN:-}" && -f "$RUNTIME_CHECK_TMP_DIR/task-definition-arn" ]]; then
    TASK_DEFINITION_ARN="$(sed -n '1p' "$RUNTIME_CHECK_TMP_DIR/task-definition-arn")"
  fi
  if [[ -z "${TASK_DEFINITION_ARN:-}" ]]; then
    printf 'ECS task definition skipped because ECS service check did not return a task definition\n' >&2
    return 1
  fi
  task_json="$(aws_cli ecs describe-task-definition \
    --task-definition "$TASK_DEFINITION_ARN" \
    --output json)"
  assert_task_definition_state "$task_json"
}

check_recent_s3_artifact() {
  local label="$1"
  local bucket="$2"
  local latest
  if ! latest="$(find_recent_s3_object_json "$bucket")"; then
    printf '%s missing recent object in the last %s UTC hours\n' \
      "$label" "$MARKET_INGEST_S3_LOOKBACK_HOURS" >&2
    return 1
  fi
  print_s3_object_json "$label" "$latest"
}

check_s3_outputs() {
  local failures=0
  local output
  local latest
  local l1_index_key=""

  if output="$(check_recent_s3_artifact "L0 raw market event" "$MARKET_L0_BUCKET" < <(recent_l0_raw_prefixes) 2>&1)"; then
    printf '%s\n' "$output"
  else
    printf '%s\n' "$output" >&2
    failures=1
  fi

  if output="$(check_recent_s3_artifact "L0 source health" "$MARKET_L0_BUCKET" < <(recent_l0_family_prefixes source_health) 2>&1)"; then
    printf '%s\n' "$output"
  else
    printf '%s\n' "$output" >&2
    failures=1
  fi

  if output="$(check_recent_s3_artifact "L0 symbol health" "$MARKET_L0_BUCKET" < <(recent_l0_family_prefixes symbol_health) 2>&1)"; then
    printf '%s\n' "$output"
  else
    printf '%s\n' "$output" >&2
    failures=1
  fi

  if latest="$(find_recent_s3_object_json "$MARKET_L1_BUCKET" < <(recent_l1_index_prefixes))"; then
    print_s3_object_json "L1 success index" "$latest"
    l1_index_key="$(object_key_from_json "$latest")"
  else
    printf 'L1 success index missing recent object in the last %s UTC hours\n' \
      "$MARKET_INGEST_S3_LOOKBACK_HOURS" >&2
    failures=1
  fi

  if output="$(check_recent_s3_artifact "L1 normalized slice" "$MARKET_L1_BUCKET" < <(recent_l1_slice_prefixes) 2>&1)"; then
    printf '%s\n' "$output"
  else
    printf '%s\n' "$output" >&2
    failures=1
  fi

  if [[ -n "$l1_index_key" ]]; then
    if output="$(validate_l1_pointer_readability "$l1_index_key" 2>&1)"; then
      printf '%s\n' "$output"
    else
      printf '%s\n' "$output" >&2
      failures=1
    fi
  fi

  if output="$(require_s3_object "Bootstrap marker" "$MARKET_L1_BUCKET" "supervisor/bootstrap/" 2>&1)"; then
    printf '%s\n' "$output"
  else
    printf '%s\n' "$output" >&2
    failures=1
  fi

  if [[ "$failures" != "0" ]]; then
    return 1
  fi
}

self_test_prefix_generators() {
  local raw_prefixes
  local source_prefixes
  local index_prefixes
  local slice_prefixes
  local raw_count

  MARKET_INGEST_S3_LOOKBACK_HOURS=2
  MARKET_INGEST_VENUE=binance
  MARKET_INGEST_WINDOW_MS=1000
  MARKET_INGEST_RAW_EVENT_TYPES="trade depth_delta"

  raw_prefixes="$(recent_l0_raw_prefixes)"
  source_prefixes="$(recent_l0_family_prefixes source_health)"
  index_prefixes="$(recent_l1_index_prefixes)"
  slice_prefixes="$(recent_l1_slice_prefixes)"
  raw_count="$(printf '%s\n' "$raw_prefixes" | sed '/^$/d' | wc -l | tr -d ' ')"

  assert_equals "$raw_count" "4"
  assert_contains "$raw_prefixes" "raw_market_event/venue=binance/event_type=trade/event_date="
  assert_contains "$raw_prefixes" "raw_market_event/venue=binance/event_type=depth_delta/event_date="
  assert_contains "$source_prefixes" "source_health/venue=binance/event_date="
  assert_contains "$index_prefixes" "l1_index/window_ms=1000/event_date="
  assert_contains "$slice_prefixes" "normalized_market_slice/venue=binance/event_date="
}

self_test_ecs_validators() {
  local service_json
  local task_json
  local task_arn

  service_json='{
    "services": [{
      "desiredCount": 1,
      "runningCount": 1,
      "pendingCount": 0,
      "deployments": [{"rolloutState": "COMPLETED"}],
      "capacityProviderStrategy": [{"capacityProvider": "FARGATE_SPOT"}],
      "taskDefinition": "arn:aws:ecs:ap-northeast-2:<aws-account-id>:task-definition/mock:1"
    }],
    "failures": []
  }'
  task_arn="$(assert_service_state "$service_json" | tail -n 1)"
  assert_equals "$task_arn" "arn:aws:ecs:ap-northeast-2:<aws-account-id>:task-definition/mock:1"

  task_json='{
    "taskDefinition": {
      "runtimePlatform": {"cpuArchitecture": "ARM64"},
      "requiresCompatibilities": ["FARGATE"],
      "cpu": "2048",
      "memory": "4096",
      "containerDefinitions": [{
        "name": "market-ingest-app",
        "readonlyRootFilesystem": true,
        "user": "nonroot:nonroot",
        "linuxParameters": {
          "capabilities": {
            "drop": ["ALL"]
          }
        },
        "command": ["--l0-s3-bucket", "mock-l0", "--l1-s3-bucket", "mock-l1"]
      }]
    }
  }'
  assert_task_definition_state "$task_json" >/dev/null

  MARKET_INGEST_LOG_GROUP="/ecs/nangman/dev/crypto-market-ingest"
  assert_log_group_retention_state '{
    "logGroups": [{
      "logGroupName": "/ecs/nangman/dev/crypto-market-ingest",
      "retentionInDays": 3
    }]
  }' >/dev/null

  assert_metric_threshold CPUUtilization '{
    "Datapoints": [
      {"Maximum": 12.5},
      {"Maximum": 20.0}
    ],
    "Label": "CPUUtilization"
  }' "85" >/dev/null

  if assert_metric_threshold MemoryUtilization '{
    "Datapoints": [
      {"Maximum": 91.0}
    ],
    "Label": "MemoryUtilization"
  }' "85" >/dev/null 2>&1; then
    die "self-test expected memory threshold violation"
  fi

  if assert_metric_threshold CPUUtilization '{
    "Datapoints": [],
    "Label": "CPUUtilization"
  }' "85" >/dev/null 2>&1; then
    die "self-test expected missing datapoints violation"
  fi
}

mock_s3_put() {
  local bucket="$1"
  local key="$2"
  local body="$3"
  local path="$MOCK_S3_ROOT/$bucket/$key"
  mkdir -p "$(dirname "$path")"
  printf '%s\n' "$body" > "$path"
}

mock_aws_cli() {
  if [[ "$1" != "s3api" ]]; then
    die "self-test mock only supports s3api, got: $*"
  fi
  shift

  local operation="$1"
  shift
  local bucket=""
  local key=""
  local prefix=""
  local output_path=""
  local previous=""
  local arg

  for arg in "$@"; do
    case "$previous" in
      --bucket)
        bucket="$arg"
        previous=""
        continue
        ;;
      --key)
        key="$arg"
        previous=""
        continue
        ;;
      --prefix)
        prefix="$arg"
        previous=""
        continue
        ;;
    esac
    case "$arg" in
      --bucket|--key|--prefix|--max-items|--query|--output)
        previous="$arg"
        ;;
      --*)
        previous=""
        ;;
      *)
        previous=""
        if [[ "$operation" == "get-object" ]]; then
          output_path="$arg"
        fi
        ;;
    esac
  done

  case "$operation" in
    list-objects-v2)
      mock_list_objects "$bucket" "$prefix"
      ;;
    get-object)
      cp "$MOCK_S3_ROOT/$bucket/$key" "$output_path"
      printf '{"ContentLength":1}\n'
      ;;
    head-object)
      test -f "$MOCK_S3_ROOT/$bucket/$key"
      printf '{"ContentLength":1,"LastModified":"2026-05-22T00:00:00Z"}\n'
      ;;
    *)
      die "self-test mock unsupported s3api operation: $operation"
      ;;
  esac
}

mock_list_objects() {
  local bucket="$1"
  local prefix="$2"
  local root="$MOCK_S3_ROOT/$bucket"
  local base="$root/$prefix"
  local file
  local key
  local size

  if [[ ! -d "$base" ]]; then
    printf 'null\n'
    return 0
  fi

  file="$(find "$base" -type f | sort | sed -n '1p')"
  if [[ -z "$file" ]]; then
    printf 'null\n'
    return 0
  fi

  key="${file#"$root/"}"
  size="$(wc -c < "$file" | tr -d ' ')"
  python3 - "$key" "$size" <<'PY'
import json
import sys

print(json.dumps({
    "key": sys.argv[1],
    "size": int(sys.argv[2]),
    "lastModified": "2026-05-22T00:00:00Z",
}))
PY
}

self_test_l1_readability() {
  MARKET_L1_BUCKET=mock-l1
  MOCK_S3_ROOT="$RUNTIME_CHECK_TMP_DIR/mock-s3"
  local index_prefix
  local pointer_key
  local manifest_key
  local report_key
  local slice_key

  index_prefix="$(recent_l1_index_prefixes | sed -n '1p')"
  pointer_key="${index_prefix}window_start_ms=1779408000000.json"
  manifest_key="runs/run_id=l1-self-test/manifest.json"
  report_key="normalization_report/run_id=l1-self-test/report.json"
  slice_key="normalized_market_slice/venue=binance/event_date=2026-05-22/hour=00/window_ms=1000/shard=00/run_id=l1-self-test-part-000001.parquet"

  mock_s3_put "$MARKET_L1_BUCKET" "$pointer_key" "{
    \"schema_version\": \"l1_index_pointer_v1\",
    \"canonical_manifest_key\": \"$manifest_key\",
    \"l1_run_id\": \"l1-self-test\",
    \"status\": \"success\",
    \"finished_at_ms\": 1779408001000,
    \"input_time_range_start_ms\": 1779408000000,
    \"input_time_range_end_ms\": 1779408001000,
    \"indexed_window_start_ms\": 1779408000000,
    \"indexed_window_end_ms\": 1779408001000,
    \"schema_version_emitted\": \"normalized_market_slice_v1\"
  }"
  mock_s3_put "$MARKET_L1_BUCKET" "$manifest_key" "{
    \"schema_version\": \"l1_manifest_v1\",
    \"l1_run_id\": \"l1-self-test\",
    \"status\": \"success\",
    \"input_time_range_start_ms\": 1779408000000,
    \"input_time_range_end_ms\": 1779408001000,
    \"schema_version_emitted\": \"normalized_market_slice_v1\",
    \"report_key\": \"$report_key\",
    \"output_object_keys\": [\"$slice_key\"],
    \"output_record_count\": 1,
    \"slice_count_total\": 1,
    \"finished_at_ms\": 1779408001000
  }"
  mock_s3_put "$MARKET_L1_BUCKET" "$report_key" "{
    \"schema_version\": \"normalization_report_v1\",
    \"l1_run_id\": \"l1-self-test\",
    \"status\": \"success\",
    \"manifest_key\": \"$manifest_key\",
    \"output_object_keys\": [\"$slice_key\"],
    \"slice_count_total\": 1
  }"
  mock_s3_put "$MARKET_L1_BUCKET" "$slice_key" "PARQUET-BYTES"

  aws_cli() {
    mock_aws_cli "$@"
  }

  require_recent_s3_object "self-test L1 success index" "$MARKET_L1_BUCKET" < <(printf '%s\n' "$index_prefix")
  assert_equals "$FOUND_S3_KEY" "$pointer_key"
  validate_l1_pointer_readability "$pointer_key" >/dev/null
}

self_test() {
  require_command python3
  RUNTIME_CHECK_TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "$RUNTIME_CHECK_TMP_DIR"' EXIT

  self_test_prefix_generators
  self_test_ecs_validators
  self_test_l1_readability

  log "check-runtime self-test passed"
}

main() {
  require_command aws
  require_command python3
  load_env_file

  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  MARKET_INGEST_CLUSTER="${MARKET_INGEST_CLUSTER:-ecs-nangman-dev-invest-apn2}"
  MARKET_INGEST_SERVICE="${MARKET_INGEST_SERVICE:-svc-nangman-dev-crypto-market-ingest}"
  MARKET_INGEST_LOG_GROUP="${MARKET_INGEST_LOG_GROUP:-/ecs/nangman/dev/crypto-market-ingest}"
  MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES="${MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES:-60}"
  MARKET_INGEST_S3_LOOKBACK_HOURS="${MARKET_INGEST_S3_LOOKBACK_HOURS:-6}"
  MARKET_INGEST_MAX_CPU_UTILIZATION_PCT="${MARKET_INGEST_MAX_CPU_UTILIZATION_PCT:-85}"
  MARKET_INGEST_MAX_MEMORY_UTILIZATION_PCT="${MARKET_INGEST_MAX_MEMORY_UTILIZATION_PCT:-85}"
  MARKET_INGEST_VENUE="${MARKET_INGEST_VENUE:-binance}"
  MARKET_INGEST_WINDOW_MS="${MARKET_INGEST_WINDOW_MS:-1000}"
  MARKET_INGEST_RAW_EVENT_TYPES="${MARKET_INGEST_RAW_EVENT_TYPES:-trade book_ticker depth_delta funding_rate_snapshot open_interest_snapshot}"
  MARKET_L0_BUCKET="${MARKET_L0_BUCKET:-${L0_S3_BUCKET:-}}"
  MARKET_L1_BUCKET="${MARKET_L1_BUCKET:-${L1_S3_BUCKET:-}}"
  RUNTIME_CHECK_TMP_DIR="$(mktemp -d)"
  RUNTIME_CHECK_FAILURES=()
  TASK_DEFINITION_ARN=""
  trap 'rm -rf "$RUNTIME_CHECK_TMP_DIR"' EXIT

  require_real_value AWS_REGION "$AWS_REGION"
  require_real_value MARKET_INGEST_CLUSTER "$MARKET_INGEST_CLUSTER"
  require_real_value MARKET_INGEST_SERVICE "$MARKET_INGEST_SERVICE"
  require_real_value MARKET_INGEST_LOG_GROUP "$MARKET_INGEST_LOG_GROUP"
  require_real_value MARKET_L0_BUCKET "$MARKET_L0_BUCKET"
  require_real_value MARKET_L1_BUCKET "$MARKET_L1_BUCKET"
  validate_positive_integer MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES "$MARKET_INGEST_RUNTIME_LOOKBACK_MINUTES"
  validate_positive_integer MARKET_INGEST_S3_LOOKBACK_HOURS "$MARKET_INGEST_S3_LOOKBACK_HOURS"
  validate_positive_integer MARKET_INGEST_MAX_CPU_UTILIZATION_PCT "$MARKET_INGEST_MAX_CPU_UTILIZATION_PCT"
  validate_positive_integer MARKET_INGEST_MAX_MEMORY_UTILIZATION_PCT "$MARKET_INGEST_MAX_MEMORY_UTILIZATION_PCT"
  validate_positive_integer MARKET_INGEST_WINDOW_MS "$MARKET_INGEST_WINDOW_MS"

  run_runtime_check "[1/8] AWS identity" check_aws_identity
  run_runtime_check "[2/8] ECS service" check_ecs_service
  run_runtime_check "[3/8] ECS task definition" check_ecs_task_definition
  run_runtime_check "[4/8] CloudWatch retention" assert_log_group_retention
  run_runtime_check "[5/8] CloudWatch logs" assert_recent_logs_clean
  run_runtime_check "[6/8] CloudWatch ECS resource metrics" check_resource_pressure
  run_runtime_check "[7/8] S3 latest L0/L1 outputs" check_s3_outputs

  log "[8/8] runtime verdict"
  if [[ "${#RUNTIME_CHECK_FAILURES[@]}" -gt 0 ]]; then
    printf 'market-ingest runtime check failed with %s issue(s):\n' "${#RUNTIME_CHECK_FAILURES[@]}" >&2
    local failure
    for failure in "${RUNTIME_CHECK_FAILURES[@]}"; do
      printf -- '- %s\n' "$failure" >&2
    done
    exit 1
  fi
  log "market-ingest runtime check passed"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ "${MARKET_INGEST_RUNTIME_SELF_TEST:-}" == "1" ]]; then
    self_test
  else
    main "$@"
  fi
fi
