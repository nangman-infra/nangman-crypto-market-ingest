#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

UNIVERSE_CONFIG="${MARKET_UNIVERSE_CONFIG:-$REPO_ROOT/config/universe.major-50.toml}"
LOCAL_ROLLUP_DIR="${MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_DIR:-${1:-}}"
OVERLAY_ROLLUP_DIR="${MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_OVERLAY_DIR:-}"
EXPECTED_UNIVERSE_SIZE="${MARKET_UNIVERSE_EXPECTED_SIZE:-50}"
MIN_BOOTSTRAP_DAYS="${MARKET_UNIVERSE_BOOTSTRAP_MIN_DAYS:-30}"
REQUIRE_FULL_APPROVAL="${MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_REQUIRE_FULL_APPROVAL:-false}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_absolute_path() {
  local label="$1"
  local path="$2"
  if [[ -z "$path" || "$path" != /* ]]; then
    echo "$label must be an absolute path" >&2
    exit 1
  fi
}

positive_integer_arg() {
  local label="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "$label must be a positive integer" >&2
    exit 1
  fi
}

enabled_config_symbols_json() {
  awk -F '"' '
    function flush() {
      if (base != "" && enabled == "true") {
        print base
      }
      base = ""
      enabled = ""
    }
    /^[[:space:]]*\[\[symbols\]\]/ {
      flush()
      next
    }
    /^[[:space:]]*base[[:space:]]*=/ {
      base = $2
      next
    }
    /^[[:space:]]*enabled[[:space:]]*=/ {
      enabled = ($0 ~ /true/) ? "true" : "false"
      next
    }
    END {
      flush()
    }
  ' "$UNIVERSE_CONFIG" | jq -R -s 'split("\n") | map(select(length > 0)) | unique'
}

append_rollups_from_dir() {
  local source_name="$1"
  local source_priority="$2"
  local rollup_dir="$3"
  local output_file="$4"

  if [[ -z "$rollup_dir" ]]; then
    return
  fi
  require_absolute_path "rollup directory" "$rollup_dir"
  if [[ ! -d "$rollup_dir" ]]; then
    echo "rollup directory does not exist: $rollup_dir" >&2
    exit 1
  fi

  while IFS= read -r file; do
    jq -c \
      --arg source_name "$source_name" \
      --argjson source_priority "$source_priority" \
      --arg path "$file" '
        {
          source_name:$source_name,
          source_priority:$source_priority,
          path:$path,
          schema_version,
          event_date,
          day_start_ms,
          symbol_count:((.symbols // []) | length),
          symbols:[
            (.symbols // [])[] |
            {
              symbol:.symbol_canonical,
              has_notional:((.traded_notional_sum // 0) > 0),
              has_spread:(((.spread_bps_median_samples // []) | length) > 0),
              gap_count:(.gap_count // 0),
              window_count:(.window_count // 0),
              mapping_confidence:(.mapping_confidence // null)
            }
          ]
        }
      ' "$file" >> "$output_file"
  done < <(find "$rollup_dir" -path '*/event_date=*/latest.json' -type f | sort)
}

require_command awk
require_command find
require_command jq
require_command mktemp
require_command sort

require_absolute_path "MARKET_UNIVERSE_CONFIG" "$UNIVERSE_CONFIG"
require_absolute_path "MARKET_UNIVERSE_BOOTSTRAP_PREVIEW_DIR or first argument" "$LOCAL_ROLLUP_DIR"
positive_integer_arg "MARKET_UNIVERSE_EXPECTED_SIZE" "$EXPECTED_UNIVERSE_SIZE"
positive_integer_arg "MARKET_UNIVERSE_BOOTSTRAP_MIN_DAYS" "$MIN_BOOTSTRAP_DAYS"

if [[ ! -f "$UNIVERSE_CONFIG" ]]; then
  echo "universe config does not exist: $UNIVERSE_CONFIG" >&2
  exit 1
fi

rollups_jsonl="$(mktemp)"
trap 'rm -f "$rollups_jsonl"' EXIT

append_rollups_from_dir "local" 0 "$LOCAL_ROLLUP_DIR" "$rollups_jsonl"
append_rollups_from_dir "overlay" 1 "$OVERLAY_ROLLUP_DIR" "$rollups_jsonl"

if [[ ! -s "$rollups_jsonl" ]]; then
  echo "no bootstrap rollups found below $LOCAL_ROLLUP_DIR" >&2
  exit 1
fi

configured_symbols="$(enabled_config_symbols_json)"

echo "== market-ingest bootstrap admission preview =="
echo "universe_config=$UNIVERSE_CONFIG"
echo "local_rollup_dir=$LOCAL_ROLLUP_DIR"
if [[ -n "$OVERLAY_ROLLUP_DIR" ]]; then
  echo "overlay_rollup_dir=$OVERLAY_ROLLUP_DIR"
fi
echo "expected_universe_size=$EXPECTED_UNIVERSE_SIZE"
echo "min_bootstrap_days=$MIN_BOOTSTRAP_DAYS"
echo

preview_json="$(
  jq -s \
    --arg universe_config "$UNIVERSE_CONFIG" \
    --arg local_rollup_dir "$LOCAL_ROLLUP_DIR" \
    --arg overlay_rollup_dir "$OVERLAY_ROLLUP_DIR" \
    --argjson expected "$EXPECTED_UNIVERSE_SIZE" \
    --argjson min_days "$MIN_BOOTSTRAP_DAYS" \
    --argjson configured_symbols "$configured_symbols" '
      def chosen_days:
        sort_by(.event_date, .source_priority, .path)
        | group_by(.event_date)
        | map(max_by(.source_priority));

      chosen_days as $days
      | ($days | map(.event_date) | sort) as $event_dates
      | [
          $days[] as $day
          | ($day.symbols // [])[]
          | {
              event_date:$day.event_date,
              symbol:.symbol,
              has_notional:.has_notional,
              has_spread:.has_spread
            }
        ] as $rows
      | [
          $configured_symbols[] as $symbol
          | ($rows | map(select(.symbol == $symbol))) as $symbol_rows
          | {
              symbol:$symbol,
              days_available:($symbol_rows | length),
              notional_days:($symbol_rows | map(select(.has_notional)) | length),
              spread_days:($symbol_rows | map(select(.has_spread)) | length),
              missing_days:($min_days - ($symbol_rows | length) | if . < 0 then 0 else . end),
              missing_event_dates:($event_dates - ($symbol_rows | map(.event_date)))
            }
        ] as $symbols
      | ($symbols | map(select(
          .days_available >= $min_days
          and .notional_days >= $min_days
          and .spread_days >= $min_days
        ))) as $approved_symbols
      | ($symbols - $approved_symbols) as $blocked_symbols
      | {
          schema_version:"market_universe_bootstrap_admission_preview_v1",
          universe_config:$universe_config,
          local_rollup_dir:$local_rollup_dir,
          overlay_rollup_dir:(if $overlay_rollup_dir == "" then null else $overlay_rollup_dir end),
          expected_universe_size:$expected,
          configured_symbol_count:($configured_symbols | length),
          min_bootstrap_days:$min_days,
          analyzed_day_count:($days | length),
          event_dates:$event_dates,
          day_summaries:[
            $days[]
            | {
                event_date,
                source_name,
                path,
                symbol_count,
                symbols_with_notional:([(.symbols // [])[] | select(.has_notional)] | length),
                symbols_with_spread_samples:([(.symbols // [])[] | select(.has_spread)] | length)
              }
          ],
          approved_symbol_count:($approved_symbols | length),
          approved_symbols:($approved_symbols | map(.symbol)),
          blocked_symbol_count:($blocked_symbols | length),
          blocked_symbols:$blocked_symbols,
          stage_state:{
            configured_major50_fixed:(($configured_symbols | length) == $expected),
            analyzed_days_complete:(($days | length) >= $min_days),
            would_open_current_approved_subset:(($approved_symbols | length) > 0),
            would_open_full_major50_approval:(
              (($configured_symbols | length) == $expected)
              and (($days | length) >= $min_days)
              and (($approved_symbols | length) >= $expected)
              and (($blocked_symbols | length) == 0)
            )
          },
          bottlenecks:[
            if (($configured_symbols | length) != $expected) then "configured_universe_size_mismatch" else empty end,
            if (($days | length) < $min_days) then "insufficient_rollup_day_count" else empty end,
            if (($approved_symbols | length) == 0) then "no_approved_symbols_previewed" else empty end,
            if (($approved_symbols | length) < $expected) then "major50_full_approval_incomplete" else empty end
          ]
        }
    ' "$rollups_jsonl"
)"

jq . <<<"$preview_json"

if [[ "$REQUIRE_FULL_APPROVAL" == "true" ]] \
  && [[ "$(jq -r '.stage_state.would_open_full_major50_approval' <<<"$preview_json")" != "true" ]]; then
  echo "bootstrap admission preview did not reach full major-50 approval" >&2
  exit 1
fi

echo "market-ingest bootstrap admission preview completed"
