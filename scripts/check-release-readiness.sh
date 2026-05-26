#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"

IMAGE_TAG="${MARKET_INGEST_RELEASE_IMAGE_TAG:-nangman-crypto-market-ingest:release-readiness-arm64}"
PLATFORM="${MARKET_INGEST_RELEASE_PLATFORM:-linux/arm64}"

log() {
  printf '%s\n' "$*"
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'release readiness failed: missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

git_sha() {
  if command -v git >/dev/null 2>&1 && git -C "$APP_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$APP_DIR" rev-parse --short=12 HEAD
  else
    printf 'unknown\n'
  fi
}

git_dirty() {
  if command -v git >/dev/null 2>&1 && git -C "$APP_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if [[ -n "$(git -C "$APP_DIR" status --porcelain --untracked-files=all)" ]]; then
      printf 'true\n'
    else
      printf 'false\n'
    fi
  else
    printf 'true\n'
  fi
}

run_script_syntax_checks() {
  log "[1/9] script syntax checks"
  bash -n scripts/deploy.sh
  bash -n scripts/check-runtime.sh
  bash -n scripts/check-ecr-scan.sh
  bash -n scripts/render-ecs-task-definition.sh
  bash -n scripts/diagnose-l1-staleness.sh
  bash -n scripts/check-universe-readiness.sh
  bash -n scripts/prepare-release-artifacts.sh
  bash -n scripts/send-market-runtime-alert.sh
  python3 -m py_compile scripts/check-repository-contract.py
}

run_script_self_tests() {
  log "[2/9] script self-tests"
  MARKET_INGEST_RUNTIME_SELF_TEST=1 bash scripts/check-runtime.sh
  MARKET_INGEST_ECR_SCAN_SELF_TEST=1 bash scripts/check-ecr-scan.sh
  MARKET_INGEST_RENDER_TASK_DEFINITION_SELF_TEST=1 bash scripts/render-ecs-task-definition.sh
  MARKET_INGEST_DIAGNOSE_L1_SELF_TEST=1 bash scripts/diagnose-l1-staleness.sh
  MARKET_INGEST_PREPARE_RELEASE_SELF_TEST=1 bash scripts/prepare-release-artifacts.sh
  MARKET_INGEST_ALERT_SELF_TEST=1 bash scripts/send-market-runtime-alert.sh
}

run_repository_contract_gate() {
  log "[3/9] repository contract gate"
  ./scripts/check-repository-contract.py
}

run_rust_gates() {
  log "[4/8] cargo fmt"
  cargo fmt --all -- --check

  log "[5/8] cargo clippy"
  cargo clippy --all-targets -- -D warnings

  log "[6/8] cargo test"
  cargo test --all-targets
}

run_docker_gate() {
  local sha
  local dirty

  sha="$(git_sha)"
  dirty="$(git_dirty)"

  log "[7/8] Docker ${PLATFORM} build"
  docker buildx build \
    --platform "$PLATFORM" \
    --build-arg "NANGMAN_GIT_SHA=$sha" \
    --build-arg "NANGMAN_GIT_DIRTY=$dirty" \
    -t "$IMAGE_TAG" \
    . \
    --load

  log "[8/8] Docker image smoke"
  local image_shape
  image_shape="$(docker image inspect "$IMAGE_TAG" --format 'OS={{.Os}} ARCH={{.Architecture}} USER={{.Config.User}} ENTRYPOINT={{json .Config.Entrypoint}}')"
  log "$image_shape"
  [[ "$image_shape" == *"OS=linux"* ]] || {
    printf 'release readiness failed: image OS is not linux\n' >&2
    exit 1
  }
  [[ "$image_shape" == *"ARCH=arm64"* ]] || {
    printf 'release readiness failed: image architecture is not arm64\n' >&2
    exit 1
  }
  [[ "$image_shape" == *"USER=nonroot:nonroot"* ]] || {
    printf 'release readiness failed: image user is not nonroot:nonroot\n' >&2
    exit 1
  }
  [[ "$image_shape" == *"crypto-market-ingest-supervisor"* ]] || {
    printf 'release readiness failed: supervisor entrypoint missing\n' >&2
    exit 1
  }
  docker run --rm --platform "$PLATFORM" "$IMAGE_TAG" --help >/dev/null
}

check_no_leftover_containers() {
  if docker ps -a --format '{{.ID}}' | grep -q .; then
    printf 'release readiness failed: Docker containers are left behind after smoke\n' >&2
    docker ps -a
    exit 1
  fi
}

main() {
  cd "$APP_DIR"
  require_command bash
  require_command python3
  require_command cargo
  require_command docker

  run_script_syntax_checks
  run_script_self_tests
  run_repository_contract_gate
  run_rust_gates
  run_docker_gate
  check_no_leftover_containers

  log "release readiness gate ok: image=$IMAGE_TAG platform=$PLATFORM git_sha=$(git_sha) git_dirty=$(git_dirty)"
}

main "$@"
