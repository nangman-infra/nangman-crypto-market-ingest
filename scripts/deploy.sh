#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
REPO_ROOT="$APP_DIR"
ENV_FILE="$APP_DIR/.env"
ENV_EXAMPLE="$APP_DIR/.env.example"
COMPOSE="$APP_DIR/compose.yml"

log() {
  printf '%s\n' "$*"
}

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    printf 'missing required file: %s\n' "$file" >&2
    exit 1
  fi
}

set_env_value() {
  local key="$1"
  local value="$2"
  if grep -q "^$key=" "$ENV_FILE"; then
    sed -i "s|^$key=.*|$key=$value|" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >> "$ENV_FILE"
  fi
}

ensure_env_file() {
  if [[ ! -f "$ENV_FILE" ]]; then
    require_file "$ENV_EXAMPLE"
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    log "created $ENV_FILE from .env.example"
  fi
  set_env_value "MARKET_INGEST_REPO_ROOT" "$REPO_ROOT"
  if ! grep -q '^MARKET_INGEST_LOG_LEVEL=' "$ENV_FILE"; then
    printf 'MARKET_INGEST_LOG_LEVEL=info\n' >> "$ENV_FILE"
  fi
}

load_env_file() {
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a

  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  AWS_PROFILE="${AWS_PROFILE:-}"
  if [[ -z "$AWS_PROFILE" || "$AWS_PROFILE" == *"<"* ]]; then
    printf 'AWS_PROFILE must be set in %s or the shell before deploy\n' "$ENV_FILE" >&2
    exit 1
  fi
  export MARKET_L0_BUCKET="${MARKET_L0_BUCKET:-${L0_S3_BUCKET:-nangman-crypto-dev-market-ingest-l0-962214}}"
  export MARKET_L1_BUCKET="${MARKET_L1_BUCKET:-${L1_S3_BUCKET:-nangman-crypto-dev-market-ingest-l1-962214}}"
}

check_clock_sync() {
  if command -v timedatectl >/dev/null 2>&1; then
    local ntp_sync
    ntp_sync="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
    if [[ "$ntp_sync" == "yes" ]]; then
      log "clock sync ok: timedatectl NTPSynchronized=yes"
      return
    fi
    if [[ "$ntp_sync" == "no" ]]; then
      printf 'host clock is not NTP synchronized: timedatectl NTPSynchronized=no\n' >&2
      exit 1
    fi
  fi

  if command -v chronyc >/dev/null 2>&1; then
    chronyc tracking >/dev/null
    log "clock sync ok: chronyc tracking succeeded"
    return
  fi

  log "warning: cannot verify NTP sync; configure a host NTP service before production"
}

run_runtime_preflight() {
  sudo docker compose -f "$COMPOSE" --env-file "$ENV_FILE" run \
    --rm \
    --no-deps \
    -e MARKET_NORMALIZE_PREFLIGHT=1 \
    market-normalize
  log "AWS/S3 runtime preflight ok: profile=$AWS_PROFILE region=$AWS_REGION"
}

log "[1/7] config check"
ensure_env_file
require_file "$ENV_FILE"
load_env_file

log "[2/7] clock preflight"
check_clock_sync

log "[3/7] compose config"
sudo docker compose -f "$COMPOSE" --env-file "$ENV_FILE" config >/dev/null

log "[4/7] build"
sudo docker compose -f "$COMPOSE" --env-file "$ENV_FILE" build

log "[5/7] AWS/S3 runtime preflight"
run_runtime_preflight

log "[6/7] recreate compose services"
sudo docker compose -f "$COMPOSE" --env-file "$ENV_FILE" up -d --force-recreate

log "[7/7] service status"
sudo docker compose -f "$COMPOSE" --env-file "$ENV_FILE" ps

cat <<EOF
Follow structured logs with:

sudo docker compose -f $COMPOSE --env-file $ENV_FILE logs -f --no-log-prefix \\
  | jq -c 'select(.schema_version == "market_ingest_log_v1")'

Follow L1 normalize worker with:

sudo docker compose -f $COMPOSE --env-file $ENV_FILE logs -f market-normalize
EOF
