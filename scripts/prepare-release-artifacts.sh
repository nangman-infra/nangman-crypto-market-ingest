#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'prepare release artifacts failed: %s\n' "$*" >&2
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

git_sha() {
  git -C "$APP_DIR" rev-parse --short=12 HEAD
}

git_dirty() {
  if [[ -n "$(git -C "$APP_DIR" status --porcelain --untracked-files=all)" ]]; then
    printf 'true\n'
  else
    printf 'false\n'
  fi
}

release_blockers_json() {
  local dirty="$1"
  RELEASE_GIT_DIRTY="$dirty" python3 <<'PY'
import json
import os

blockers = []
if os.environ["RELEASE_GIT_DIRTY"] == "true":
    blockers.append("git worktree is dirty; commit before publishing a release image")
print(json.dumps(blockers))
PY
}

write_manifest() {
  local output_path="$1"
  local task_definition_path="$2"
  local sha="$3"
  local dirty="$4"
  local image_tag="$5"
  local image_uri="$6"
  local release_ready="$7"
  local blockers_json="$8"

  RELEASE_OUTPUT_PATH="$output_path" \
    RELEASE_TASK_DEFINITION_PATH="$task_definition_path" \
    RELEASE_GIT_SHA="$sha" \
    RELEASE_GIT_DIRTY="$dirty" \
    RELEASE_IMAGE_TAG="$image_tag" \
    RELEASE_IMAGE_URI="$image_uri" \
    RELEASE_READY="$release_ready" \
    RELEASE_BLOCKERS_JSON="$blockers_json" \
    python3 <<'PY'
import json
import os
from pathlib import Path

image_uri = os.environ["RELEASE_IMAGE_URI"]
task_definition_path = os.environ["RELEASE_TASK_DEFINITION_PATH"]
manifest = {
    "schema_version": "market_ingest_release_artifacts_v1",
    "producer_app": "market-ingest-app",
    "git_sha": os.environ["RELEASE_GIT_SHA"],
    "git_dirty": os.environ["RELEASE_GIT_DIRTY"] == "true",
    "release_ready": os.environ["RELEASE_READY"] == "true",
    "release_blockers": json.loads(os.environ["RELEASE_BLOCKERS_JSON"]),
    "platform": "linux/arm64",
    "image_tag": os.environ["RELEASE_IMAGE_TAG"],
    "image_uri": image_uri,
    "task_definition_register_json": task_definition_path,
    "mutation_performed": False,
    "publish_commands": [
        (
            "docker buildx build --platform linux/arm64 "
            f"--build-arg NANGMAN_GIT_SHA={os.environ['RELEASE_GIT_SHA']} "
            f"--build-arg NANGMAN_GIT_DIRTY={os.environ['RELEASE_GIT_DIRTY']} "
            f"-t {image_uri} . --push"
        ),
        f"aws ecs register-task-definition --cli-input-json file://{task_definition_path}",
    ],
    "post_deploy_read_only_checks": [
        "scripts/check-ecr-scan.sh",
        "scripts/check-runtime.sh",
        "scripts/diagnose-l1-staleness.sh",
    ],
}
Path(os.environ["RELEASE_OUTPUT_PATH"]).write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
}

self_test() {
  local tmp_dir
  tmp_dir="$(mktemp -d /tmp/market-ingest-release-self-test.XXXXXX)"
  write_manifest \
    "$tmp_dir/manifest.json" \
    "$tmp_dir/task-definition.register.json" \
    "abc123def456" \
    "true" \
    "git-abc123def456-arm64" \
    "example.invalid/repo:git-abc123def456-arm64" \
    "false" \
    '["git worktree is dirty; commit before publishing a release image"]'

  python3 - "$tmp_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert manifest["schema_version"] == "market_ingest_release_artifacts_v1"
assert manifest["git_sha"] == "abc123def456"
assert manifest["git_dirty"] is True
assert manifest["release_ready"] is False
assert manifest["release_blockers"] == [
    "git worktree is dirty; commit before publishing a release image"
]
assert manifest["mutation_performed"] is False
assert manifest["image_tag"] == "git-abc123def456-arm64"
PY
  log "prepare-release-artifacts self-test passed"
}

main() {
  require_command git
  require_command python3

  if [[ "${MARKET_INGEST_PREPARE_RELEASE_SELF_TEST:-}" == "1" ]]; then
    self_test
    return
  fi

  require_command aws

  cd "$APP_DIR"
  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  MARKET_INGEST_ECR_REPOSITORY="${MARKET_INGEST_ECR_REPOSITORY:-ecr-nangman-dev-crypto-market-ingest-apn2}"

  local sha
  local dirty
  local image_tag
  local registry
  local image_uri
  local output_dir
  local task_definition_path
  local manifest_path
  local blockers_json
  local release_ready

  sha="$(git_sha)"
  dirty="$(git_dirty)"
  image_tag="${MARKET_INGEST_ECR_IMAGE_TAG:-git-${sha}-arm64}"

  if [[ -n "${MARKET_INGEST_ECR_IMAGE_URI:-}" ]]; then
    image_uri="$MARKET_INGEST_ECR_IMAGE_URI"
  else
    registry="${MARKET_INGEST_ECR_REGISTRY:-}"
    if [[ -z "$registry" ]]; then
      local account_id
      account_id="$(aws_cli sts get-caller-identity --query Account --output text)"
      registry="${account_id}.dkr.ecr.${AWS_REGION}.amazonaws.com"
    fi
    image_uri="${registry}/${MARKET_INGEST_ECR_REPOSITORY}:${image_tag}"
  fi

  output_dir="${MARKET_INGEST_RELEASE_OUTPUT_DIR:-/tmp/market-ingest-release-${sha}}"
  mkdir -p "$output_dir"
  task_definition_path="$output_dir/task-definition.register.json"
  manifest_path="$output_dir/release-manifest.json"

  MARKET_INGEST_RENDER_OUTPUT="$task_definition_path" \
    MARKET_INGEST_ECR_IMAGE_URI="$image_uri" \
    "$SCRIPT_DIR/render-ecs-task-definition.sh"

  blockers_json="$(release_blockers_json "$dirty")"
  release_ready=true
  if [[ "$dirty" == "true" ]]; then
    release_ready=false
  fi

  write_manifest \
    "$manifest_path" \
    "$task_definition_path" \
    "$sha" \
    "$dirty" \
    "$image_tag" \
    "$image_uri" \
    "$release_ready" \
    "$blockers_json"

  log "release manifest: $manifest_path"
  log "task definition register json: $task_definition_path"
  log "release_ready=$release_ready git_sha=$sha git_dirty=$dirty image_tag=$image_tag"
  log "no AWS/ECR/ECS/S3 mutation was performed"
}

main "$@"
