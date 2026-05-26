#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
APP_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd -P)"
ENV_FILE="${MARKET_INGEST_ENV_FILE:-$APP_DIR/.env}"

WEBHOOK_URL="${NANGMAN_ALERT_WEBHOOK_URL:-${MATTERMOST_WEBHOOK_URL:-}}"
ALERT_ENV="${NANGMAN_ALERT_ENV:-dev}"
INCLUDE_SUCCESS="${MARKET_INGEST_ALERT_INCLUDE_SUCCESS:-false}"

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'market runtime alert failed: %s\n' "$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "missing required command: $1"
  fi
}

is_true() {
  case "$1" in
    1 | true | TRUE | yes | YES) return 0 ;;
    *) return 1 ;;
  esac
}

json_string() {
  jq -Rn --arg value "$1" '$value'
}

send_mattermost() {
  local text="$1"
  if [[ -z "$WEBHOOK_URL" ]]; then
    die "NANGMAN_ALERT_WEBHOOK_URL or MATTERMOST_WEBHOOK_URL is required"
  fi
  local payload
  payload="$(jq -nc --arg text "$text" '{text:$text}')"
  curl -fsS \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$WEBHOOK_URL" >/dev/null
}

compact_tail() {
  local file="$1"
  local lines="${2:-12}"
  tail -n "$lines" "$file" | sed -E 's/[0-9]{12}/<aws-account-id>/g; s/[[:space:]]+$//'
}

failure_message() {
  local status="$1"
  local output_file="$2"
  local now_kst
  now_kst="$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')"
  cat <<EOF
[P1][market-ingest-app] runtime check failed

결론:
Market-L0/L1 runtime check가 실패했습니다. research replay가 필요한 시장 데이터를 못 받을 수 있습니다.

현재 상태:
- env: ${ALERT_ENV}
- check: scripts/check-runtime.sh
- exit_status: ${status}
- app_dir: ${APP_DIR}

주요 원인:
$(compact_tail "$output_file" 10 | sed 's/^/- /')

다음 행동:
- ECS service desired/running count 확인
- L0 raw_market_event, source_health, symbol_health 최신 object 확인
- L1 l1_index와 normalized_market_slice freshness 확인
- gap_alert, buffered_overflow, depth_update_id_gap 증가 여부 확인

안전 상태:
- 이 알림은 시장 데이터 품질 알림입니다.
- paper/live/order execution을 변경하지 않습니다.

발송 시각: ${now_kst}
EOF
}

success_message() {
  local output_file="$1"
  local now_kst
  now_kst="$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')"
  cat <<EOF
[P3][market-ingest-app] runtime check summary

결론:
Market-L0/L1 runtime check가 통과했습니다.

현재 상태:
- env: ${ALERT_ENV}
- check: scripts/check-runtime.sh

요약:
$(compact_tail "$output_file" 8 | sed 's/^/- /')

다음 행동:
- 일반 성공 알림은 기본적으로 끕니다.
- MARKET_INGEST_ALERT_INCLUDE_SUCCESS=true일 때만 이 요약을 보냅니다.

발송 시각: ${now_kst}
EOF
}

self_test() {
  require_command jq
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp" <<'EOF'
runtime check failed: L1 index missing recent object
L0 raw trade: ok
L1 index: missing under l1_index/window_ms=1000/event_date=2026-05-26/hour=00/
EOF
  local message
  message="$(failure_message 1 "$tmp")"
  [[ "$message" == *"[P1][market-ingest-app]"* ]] || die "self-test expected P1 title"
  [[ "$message" == *"Market-L0/L1 runtime check"* ]] || die "self-test expected conclusion"
  [[ "$message" == *"다음 행동:"* ]] || die "self-test expected next actions"
  rm -f "$tmp"
  log "send-market-runtime-alert self-test passed"
}

main() {
  if is_true "${MARKET_INGEST_ALERT_SELF_TEST:-false}"; then
    self_test
    return
  fi

  require_command curl
  require_command jq
  require_command tail
  require_command sed

  local output_file
  output_file="$(mktemp)"
  set +e
  MARKET_INGEST_ENV_FILE="$ENV_FILE" "$SCRIPT_DIR/check-runtime.sh" > "$output_file" 2>&1
  local status=$?
  set -e

  if [[ "$status" -ne 0 ]]; then
    send_mattermost "$(failure_message "$status" "$output_file")"
    rm -f "$output_file"
    return "$status"
  fi

  if is_true "$INCLUDE_SUCCESS"; then
    send_mattermost "$(success_message "$output_file")"
  fi
  rm -f "$output_file"
}

main "$@"
