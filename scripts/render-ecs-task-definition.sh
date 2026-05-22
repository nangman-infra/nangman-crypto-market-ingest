#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'render task definition failed: %s\n' "$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "missing required command: $1"
  fi
}

aws_cli() {
  local args=(--region "$AWS_REGION")
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    args+=(--profile "$AWS_PROFILE")
  fi
  aws "${args[@]}" "$@"
}

render_task_definition() {
  local input_json="$1"
  local image_uri="${2:-}"
  TASK_DEFINITION_JSON="$input_json" MARKET_INGEST_ECR_IMAGE_URI="$image_uri" python3 <<'PY'
import json
import os
import sys

source = json.loads(os.environ["TASK_DEFINITION_JSON"])
task = source.get("taskDefinition", source)
image_uri = os.environ.get("MARKET_INGEST_ECR_IMAGE_URI", "").strip()

allowed_task_fields = [
    "family",
    "taskRoleArn",
    "executionRoleArn",
    "networkMode",
    "containerDefinitions",
    "volumes",
    "placementConstraints",
    "requiresCompatibilities",
    "cpu",
    "memory",
    "runtimePlatform",
    "ephemeralStorage",
    "proxyConfiguration",
    "inferenceAccelerators",
    "pidMode",
    "ipcMode",
]

rendered = {
    key: task[key]
    for key in allowed_task_fields
    if key in task and task[key] not in (None, [], {})
}
required_writable_mounts = {
    "market-l0-spool": "/opt/nangman-crypto/data/spool/market-ingest/l0",
    "market-l1-spool": "/opt/nangman-crypto/data/spool/market-ingest/l1",
    "market-normalize-catchup": "/opt/nangman-crypto/data/spool/market-normalize/catchup",
}


def ensure_volume(name):
    volumes = rendered.setdefault("volumes", [])
    if not any(volume.get("name") == name for volume in volumes):
        volumes.append({"name": name})


def ensure_mount(container, source_volume, container_path):
    mount_points = container.setdefault("mountPoints", [])
    for mount in mount_points:
        if mount.get("sourceVolume") == source_volume or mount.get("containerPath") == container_path:
            mount["sourceVolume"] = source_volume
            mount["containerPath"] = container_path
            mount["readOnly"] = False
            return
    mount_points.append(
        {
            "sourceVolume": source_volume,
            "containerPath": container_path,
            "readOnly": False,
        }
    )

platform = rendered.get("runtimePlatform", {})
if platform.get("cpuArchitecture") != "ARM64":
    raise SystemExit(f"task cpuArchitecture must be ARM64, got: {platform}")
if "FARGATE" not in rendered.get("requiresCompatibilities", []):
    raise SystemExit("task definition must require FARGATE compatibility")

containers = rendered.get("containerDefinitions", [])
if len(containers) != 1:
    raise SystemExit("market-ingest production task must have exactly one supervisor container")

container = containers[0]
if image_uri:
    container["image"] = image_uri

container["readonlyRootFilesystem"] = True
container["user"] = "nonroot:nonroot"
for source_volume, container_path in required_writable_mounts.items():
    ensure_volume(source_volume)
    ensure_mount(container, source_volume, container_path)

linux_parameters = container.setdefault("linuxParameters", {})
capabilities = linux_parameters.setdefault("capabilities", {})
drops = capabilities.setdefault("drop", [])
upper_drops = {item.upper() for item in drops}
if "ALL" not in upper_drops:
    drops.append("ALL")

command = container.get("command", [])
if "--l0-s3-bucket" not in command or "--l1-s3-bucket" not in command:
    raise SystemExit("supervisor command must pass explicit L0/L1 buckets")

print(json.dumps(rendered, indent=2, sort_keys=True))
PY
}

self_test() {
  local rendered
  rendered="$(render_task_definition '{
    "taskDefinition": {
      "family": "td-nangman-dev-crypto-market-ingest",
      "networkMode": "awsvpc",
      "requiresCompatibilities": ["FARGATE"],
      "cpu": "2048",
      "memory": "4096",
      "runtimePlatform": {
        "cpuArchitecture": "ARM64",
        "operatingSystemFamily": "LINUX"
      },
      "taskDefinitionArn": "arn:aws:ecs:ap-northeast-2:<aws-account-id>:task-definition/mock:16",
      "revision": 16,
      "status": "ACTIVE",
      "containerDefinitions": [{
        "name": "crypto-market-ingest",
        "image": "old-image",
        "essential": true,
        "readonlyRootFilesystem": false,
        "command": ["--l0-s3-bucket", "mock-l0", "--l1-s3-bucket", "mock-l1"]
      }]
    }
  }' "new-image")"

  RENDERED_JSON="$rendered" python3 <<'PY'
import json
import os

data = json.loads(os.environ["RENDERED_JSON"])
container = data["containerDefinitions"][0]
assert "taskDefinitionArn" not in data
assert "revision" not in data
assert data["runtimePlatform"]["cpuArchitecture"] == "ARM64"
assert container["image"] == "new-image"
assert container["readonlyRootFilesystem"] is True
assert container["user"] == "nonroot:nonroot"
assert "ALL" in container["linuxParameters"]["capabilities"]["drop"]
volumes = {volume["name"] for volume in data["volumes"]}
mounts = {
    mount["sourceVolume"]: mount
    for mount in container["mountPoints"]
}
for volume in ["market-l0-spool", "market-l1-spool", "market-normalize-catchup"]:
    assert volume in volumes
    assert mounts[volume]["readOnly"] is False
PY
  log "render-ecs-task-definition self-test passed"
}

main() {
  require_command aws
  require_command python3

  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  MARKET_INGEST_CLUSTER="${MARKET_INGEST_CLUSTER:-ecs-nangman-dev-invest-apn2}"
  MARKET_INGEST_SERVICE="${MARKET_INGEST_SERVICE:-svc-nangman-dev-crypto-market-ingest}"
  MARKET_INGEST_RENDER_OUTPUT="${MARKET_INGEST_RENDER_OUTPUT:-}"
  MARKET_INGEST_TASK_DEFINITION="${MARKET_INGEST_TASK_DEFINITION:-}"
  MARKET_INGEST_ECR_IMAGE_URI="${MARKET_INGEST_ECR_IMAGE_URI:-}"

  local task_definition="$MARKET_INGEST_TASK_DEFINITION"
  if [[ -z "$task_definition" ]]; then
    task_definition="$(aws_cli ecs describe-services \
      --cluster "$MARKET_INGEST_CLUSTER" \
      --services "$MARKET_INGEST_SERVICE" \
      --query 'services[0].taskDefinition' \
      --output text)"
  fi
  if [[ -z "$task_definition" || "$task_definition" == "None" ]]; then
    die "could not resolve current ECS task definition"
  fi

  local task_json
  task_json="$(aws_cli ecs describe-task-definition \
    --task-definition "$task_definition" \
    --output json)"

  local rendered
  rendered="$(render_task_definition "$task_json" "$MARKET_INGEST_ECR_IMAGE_URI")"
  if [[ -n "$MARKET_INGEST_RENDER_OUTPUT" ]]; then
    printf '%s\n' "$rendered" > "$MARKET_INGEST_RENDER_OUTPUT"
    log "rendered hardened task definition: $MARKET_INGEST_RENDER_OUTPUT"
  else
    printf '%s\n' "$rendered"
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ "${MARKET_INGEST_RENDER_TASK_DEFINITION_SELF_TEST:-}" == "1" ]]; then
    require_command python3
    self_test
  else
    main "$@"
  fi
fi
