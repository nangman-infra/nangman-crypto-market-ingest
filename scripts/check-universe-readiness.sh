#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-ap-northeast-2}}"
MARKET_L1_BUCKET="${MARKET_L1_BUCKET:-${L1_S3_BUCKET:-}}"
EXPECTED_UNIVERSE_SIZE="${MARKET_UNIVERSE_EXPECTED_SIZE:-50}"
ROLLUP_READ_LIMIT="${MARKET_UNIVERSE_ROLLUP_READ_LIMIT:-30}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
UNIVERSE_CONFIG="${MARKET_UNIVERSE_CONFIG:-$REPO_ROOT/config/universe.major-50.toml}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

redact() {
  sed -E \
    -e 's/nangman-crypto-dev-[A-Za-z0-9-]+-[0-9]{6}/nangman-crypto-dev-<bucket-family>-<account-suffix>/g' \
    -e 's/[0-9]{12}\.dkr\.ecr/<aws-account-id>.dkr.ecr/g' \
    -e 's/account=[0-9]{12}/account=<aws-account-id>/g' \
    -e 's/"Account"[[:space:]]*:[[:space:]]*"[0-9]{12}"/"Account":"<aws-account-id>"/g'
}

aws_cmd() {
  aws --region "$REGION" "$@"
}

latest_universe_snapshot_object_json() {
  local bucket="$1"
  local prefix="$2"
  aws_cmd s3api list-objects-v2 \
    --bucket "$bucket" \
    --prefix "$prefix" \
    --output json \
  | jq -c --arg prefix "$prefix" '
      def with_run_id_times:
        . as $object
        | ($object.Key | capture("run_id=l1_(?<start>[0-9]+)_(?<end>[0-9]+)_(?<generated>[0-9]+)")? // {}) as $run
        | $object + {
            run_start_ms:(($run.start // "0") | tonumber),
            run_end_ms:(($run.end // "0") | tonumber),
            run_generated_ms:(($run.generated // "0") | tonumber)
          };

      (.Contents // [])
      | map(with_run_id_times)
      | sort_by(.run_end_ms, .LastModified, .Key)
      | last as $last
      | if $last == null then
          {
            prefix:$prefix,
            selection:"latest_universe_as_of",
            lastModified:null,
            size:null,
            key:null,
            run_start_ms:null,
            run_end_ms:null,
            run_generated_ms:null
          }
        else
          {
            prefix:$prefix,
            selection:"latest_universe_as_of",
            lastModified:$last.LastModified,
            size:$last.Size,
            key:$last.Key,
            run_start_ms:$last.run_start_ms,
            run_end_ms:$last.run_end_ms,
            run_generated_ms:$last.run_generated_ms
          }
        end
    '
}

verify_aws_access() {
  local identity_output
  if ! identity_output="$(aws_cmd sts get-caller-identity --output json 2>&1)"; then
    {
      echo "AWS credentials unavailable or expired for region=$REGION"
      echo "Refresh the AWS login/session, then rerun this check."
      echo "$identity_output"
    } | redact >&2
    exit 1
  fi

  echo "aws identity ok: account=$(jq -r '.Account' <<<"$identity_output")" | redact
}

require_command aws
require_command jq
require_command sed
require_command mktemp

if [[ -z "$MARKET_L1_BUCKET" || "$MARKET_L1_BUCKET" == *"<"* ]]; then
  echo "MARKET_L1_BUCKET or L1_S3_BUCKET must be set to a real bucket" >&2
  exit 1
fi

echo "== market-ingest universe readiness =="
echo "region=$REGION"
echo "market_l1_bucket=$MARKET_L1_BUCKET" | redact
echo "universe_config=$UNIVERSE_CONFIG"
echo "expected_universe_size=$EXPECTED_UNIVERSE_SIZE"
echo "rollup_read_limit=$ROLLUP_READ_LIMIT"
echo

verify_aws_access

snapshot_object_json="$(mktemp)"
snapshot_json="$(mktemp)"
rollup_objects_json="$(mktemp)"
rollup_keys="$(mktemp)"
rollup_records_json="$(mktemp)"
configured_symbols_json="$(mktemp)"
trap 'rm -f "$snapshot_object_json" "$snapshot_json" "$rollup_objects_json" "$rollup_keys" "$rollup_records_json" "$configured_symbols_json"' EXIT

if [[ -f "$UNIVERSE_CONFIG" ]]; then
  sed -nE 's/^base = "([^"]+)"/\1/p' "$UNIVERSE_CONFIG" \
  | jq -R -s -c 'split("\n") | map(select(length > 0)) | unique' \
  > "$configured_symbols_json"
else
  printf '[]\n' > "$configured_symbols_json"
fi

latest_universe_snapshot_object_json \
  "$MARKET_L1_BUCKET" \
  "symbol_universe_snapshot/run_id=" > "$snapshot_object_json"

snapshot_key="$(jq -r '.key // empty' "$snapshot_object_json")"
if [[ -n "$snapshot_key" ]]; then
  aws_cmd s3 cp "s3://${MARKET_L1_BUCKET}/${snapshot_key}" - > "$snapshot_json"
else
  printf '{}\n' > "$snapshot_json"
fi

snapshot_universe_as_of_ms="$(jq -r '.universe_as_of_ms // empty' "$snapshot_json")"
if [[ -n "$snapshot_universe_as_of_ms" ]]; then
  jq -n -r \
    --argjson end_ms "$snapshot_universe_as_of_ms" \
    --argjson limit "$ROLLUP_READ_LIMIT" '
      range(0; $limit)
      | "symbol_universe_snapshot/bootstrap_rollup/event_date="
        + (((($end_ms - 1) / 1000) - (. * 86400)) | strftime("%Y-%m-%d"))
        + "/latest.json"
    ' > "$rollup_keys"
else
  aws_cmd s3api list-objects-v2 \
    --bucket "$MARKET_L1_BUCKET" \
    --prefix "symbol_universe_snapshot/bootstrap_rollup/event_date=" \
    --output json \
  | jq -r --argjson limit "$ROLLUP_READ_LIMIT" '
      (.Contents // []) | sort_by(.Key) | reverse | .[0:$limit][]?.Key
    ' > "$rollup_keys"
fi

: > "$rollup_records_json"
while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  event_date="${key#*event_date=}"
  event_date="${event_date%%/*}"
  if ! rollup_body="$(aws_cmd s3 cp "s3://${MARKET_L1_BUCKET}/${key}" - 2>/dev/null)"; then
    jq -n -c --arg key "$key" --arg event_date "$event_date" '{
      key:$key,
      event_date:$event_date,
      missing:true,
      source_window_count:0,
      symbol_count:0,
      symbols_with_notional:0,
      symbols_with_spread_samples:0,
      symbols:[],
      symbols_with_notional_list:[],
      symbols_with_spread_samples_list:[],
      top_missing_spread_symbols:[]
    }' >> "$rollup_records_json"
    continue
  fi
  jq -c --arg key "$key" '{
      key:$key,
      missing:false,
      event_date,
      source_window_count:((.source_windows // []) | length),
      symbol_count:((.symbols // []) | length),
      symbols_with_notional:([
        (.symbols // [])[]? | select((.traded_notional_sum // 0) > 0)
      ] | length),
      symbols_with_spread_samples:([
        (.symbols // [])[]? | select(((.spread_bps_median_samples // []) | length) > 0)
      ] | length),
      symbols:[(.symbols // [])[]?.symbol_canonical],
      symbols_with_notional_list:[
        (.symbols // [])[]? | select((.traded_notional_sum // 0) > 0) | .symbol_canonical
      ],
      symbols_with_spread_samples_list:[
        (.symbols // [])[]?
        | select(((.spread_bps_median_samples // []) | length) > 0)
        | .symbol_canonical
      ],
      top_missing_spread_symbols:[
        (.symbols // [])[]?
        | select(((.spread_bps_median_samples // []) | length) == 0)
        | .symbol_canonical
      ][0:10]
    }' <<<"$rollup_body" >> "$rollup_records_json"
done < "$rollup_keys"

snapshot_summary="$(
  jq -c \
    --argjson object "$(cat "$snapshot_object_json")" \
    --argjson configured "$(cat "$configured_symbols_json")" \
    --argjson expected "$EXPECTED_UNIVERSE_SIZE" '
      def status_reason_counts:
        ((.included_symbols // []) + (.excluded_symbols // []))
        | map(.status_reason // "unknown")
        | group_by(.)
        | map({reason:.[0], count:length})
        | sort_by(.count)
        | reverse;
      def observed_count:
        ((.liquidity_rank_at_that_time // []) | length);
      def snapshot_symbols:
        [
          (.liquidity_rank_at_that_time // [])[]?.symbol_canonical,
          (.included_symbols // [])[]?.symbol_canonical,
          (.excluded_symbols // [])[]?.symbol_canonical
        ] | unique;
      if $object.key == null then
        {
          present:false,
          key:null,
          last_modified:null,
          configured_universe_symbol_count:($configured | length),
          observed_symbol_count:0,
          approved_symbol_count:0,
          excluded_symbol_count:0,
          observed_complete:false,
          approved_complete:false,
          configured_symbols:$configured,
          missing_configured_symbols:$configured,
          unexpected_snapshot_symbols:[],
          excluded_symbols:[],
          excluded_symbols_by_reason:[],
          status_reason_counts:[]
        }
      else
        {
          present:true,
          key:$object.key,
          last_modified:$object.lastModified,
          selection:$object.selection,
          run_start_ms:$object.run_start_ms,
          run_end_ms:$object.run_end_ms,
          run_generated_ms:$object.run_generated_ms,
          schema_version,
          symbol_universe_snapshot_id,
          universe_as_of_ms,
          configured_universe_symbol_count:($configured | length),
          observed_symbol_count:observed_count,
          approved_symbol_count:((.included_symbols // []) | length),
          excluded_symbol_count:((.excluded_symbols // []) | length),
          observed_complete:(observed_count >= $expected),
          approved_complete:(((.included_symbols // []) | length) >= $expected),
          configured_symbols:$configured[0:$expected],
          missing_configured_symbols:($configured - snapshot_symbols),
          unexpected_snapshot_symbols:(snapshot_symbols - $configured),
          top_observed_symbols:[(.liquidity_rank_at_that_time // [])[]?.symbol_canonical][0:$expected],
          approved_symbols:[(.included_symbols // [])[]?.symbol_canonical][0:$expected],
          excluded_symbols:[
            (.excluded_symbols // [])[]?
            | {
                symbol:.symbol_canonical,
                liquidity_rank:(.liquidity_rank // null),
                status_reason:(.status_reason // "unknown")
              }
          ],
          excluded_symbols_by_reason:(
            (.excluded_symbols // [])
            | group_by(.status_reason // "unknown")
            | map({
                reason:(.[0].status_reason // "unknown"),
                symbols:map(.symbol_canonical),
                count:length
              })
            | sort_by(.count)
            | reverse
          ),
          status_reason_counts:status_reason_counts
        }
      end
    ' "$snapshot_json"
)"

rollup_summary="$(jq -s -c \
  --argjson expected "$EXPECTED_UNIVERSE_SIZE" \
  --argjson limit "$ROLLUP_READ_LIMIT" \
  --argjson configured "$(cat "$configured_symbols_json")" '
  def epoch_day($date):
    (($date | strptime("%Y-%m-%d") | mktime) / 86400 | floor);
  def date_after_days($date; $days):
    (($date | strptime("%Y-%m-%d") | mktime) + ($days * 86400) | strftime("%Y-%m-%d"));
  def max_date_or_null:
    if length == 0 then null else sort | last end;

  . as $rollups
  | ([.[].event_date] | sort) as $event_dates
  | ($event_dates[0] // null) as $window_start_date
  | ($event_dates[-1] // null) as $latest_event_date
  | [
      $configured[] as $symbol
      | {
          symbol:$symbol,
          missing_event_dates:[
            $rollups[]
            | select(((.symbols // []) | index($symbol)) == null)
            | .event_date
          ],
          missing_notional_event_dates:[
            $rollups[]
            | select(((.symbols_with_notional_list // []) | index($symbol)) == null)
            | .event_date
          ],
          missing_spread_event_dates:[
            $rollups[]
            | select(((.symbols_with_spread_samples_list // []) | index($symbol)) == null)
            | .event_date
          ]
        }
      | . + {
          missing_day_count:(.missing_event_dates | length),
          missing_notional_day_count:(.missing_notional_event_dates | length),
          missing_spread_day_count:(.missing_spread_event_dates | length)
        }
      | (
          [
            (.missing_event_dates[]?),
            (.missing_notional_event_dates[]?),
            (.missing_spread_event_dates[]?)
          ]
          | unique
          | max_date_or_null
        ) as $latest_missing_date
      | . + {
          bootstrap_window_start_event_date:$window_start_date,
          latest_event_date:$latest_event_date,
          latest_missing_event_date:$latest_missing_date,
          additional_complete_days_required:(
            if $latest_missing_date == null or $window_start_date == null then 0
            else ((epoch_day($latest_missing_date) - epoch_day($window_start_date) + 1) | if . < 0 then 0 else . end)
            end
          )
        }
      | . + {
          estimated_full_bootstrap_event_date:(
            if .additional_complete_days_required == 0 or $latest_event_date == null then null
            else date_after_days($latest_event_date; .additional_complete_days_required)
            end
          )
        }
      | select(
          .missing_day_count > 0
          or .missing_notional_day_count > 0
          or .missing_spread_day_count > 0
        )
    ] as $missing_symbol_days
  | {
  analyzed_rollup_count:length,
  expected_rollup_count:$limit,
  present_rollup_count:([.[] | select((.missing // false) == false)] | length),
  missing_rollup_count:([.[] | select(.missing == true)] | length),
  event_dates:[.[].event_date],
  bootstrap_window_start_event_date:$window_start_date,
  latest_event_date:$latest_event_date,
  min_symbol_count:([.[].symbol_count] | min // 0),
  max_symbol_count:([.[].symbol_count] | max // 0),
  min_symbols_with_notional:([.[].symbols_with_notional] | min // 0),
  max_symbols_with_notional:([.[].symbols_with_notional] | max // 0),
  min_symbols_with_spread_samples:([.[].symbols_with_spread_samples] | min // 0),
  max_symbols_with_spread_samples:([.[].symbols_with_spread_samples] | max // 0),
  rollups_with_complete_symbol_coverage:([.[] | select(.symbol_count >= $expected)] | length),
  rollups_with_any_spread_samples:([.[] | select(.symbols_with_spread_samples > 0)] | length),
  incomplete_rollups:[
    .[]
    | select(
        (.symbol_count < $expected)
        or (.symbols_with_notional < $expected)
        or (.symbols_with_spread_samples < $expected)
      )
    | {
        event_date,
        symbol_count,
        symbols_with_notional,
        symbols_with_spread_samples,
        missing_symbols:($configured - (.symbols // [])),
        missing_notional_symbols:($configured - (.symbols_with_notional_list // [])),
        missing_spread_symbols:($configured - (.symbols_with_spread_samples_list // []))
      }
  ],
  missing_symbol_days:$missing_symbol_days,
  bootstrap_approval_projection:{
    full_major50_blocked_by_missing_symbol_days:(($missing_symbol_days | length) > 0),
    blocking_symbol_count:($missing_symbol_days | length),
    latest_estimated_full_bootstrap_event_date:(
      [$missing_symbol_days[]?.estimated_full_bootstrap_event_date]
      | map(select(. != null))
      | max_date_or_null
    ),
    blocking_symbols:(
      $missing_symbol_days
      | map({
          symbol,
          latest_missing_event_date,
          additional_complete_days_required,
          estimated_full_bootstrap_event_date
        })
      | sort_by(.additional_complete_days_required, .symbol)
      | reverse
    )
  },
  latest_rollups:.[0:5]
}' "$rollup_records_json")"

jq -n \
  --arg region "$REGION" \
  --arg bucket "$MARKET_L1_BUCKET" \
  --argjson expected "$EXPECTED_UNIVERSE_SIZE" \
  --argjson snapshot "$snapshot_summary" \
  --argjson rollups "$rollup_summary" \
  '{
    region:$region,
    market_l1_bucket:$bucket,
    expected_universe_size:$expected,
    stage_state:{
      latest_snapshot_present:$snapshot.present,
      configured_major50_fixed:($snapshot.configured_universe_symbol_count == $expected),
      configured_major50_observed:(($snapshot.missing_configured_symbols | length) == 0),
      major50_observed:$snapshot.observed_complete,
      major50_approved:$snapshot.approved_complete,
      bootstrap_rollups_present:($rollups.present_rollup_count == $rollups.expected_rollup_count),
      bootstrap_notional_present:($rollups.min_symbols_with_notional > 0),
      bootstrap_spread_samples_present:($rollups.min_symbols_with_spread_samples > 0)
    },
    latest_snapshot:$snapshot,
    recent_bootstrap_rollups:$rollups,
    bottlenecks:([
      if ($snapshot.present | not) then "symbol_universe_snapshot_absent" else empty end,
      if ($snapshot.configured_universe_symbol_count != $expected) then "major50_config_file_not_expected_size" else empty end,
      if (($snapshot.missing_configured_symbols | length) > 0) then "major50_config_symbols_missing_from_snapshot" else empty end,
      if ($snapshot.observed_complete | not) then "major50_observed_universe_incomplete" else empty end,
      if ($snapshot.approved_complete | not) then "major50_approved_universe_incomplete" else empty end,
      if ($rollups.present_rollup_count < $rollups.expected_rollup_count) then "bootstrap_rollups_missing_for_snapshot_window" else empty end,
      if ($rollups.min_symbol_count < $expected) then "bootstrap_symbol_coverage_incomplete" else empty end,
      if ($rollups.min_symbols_with_spread_samples == 0) then "bootstrap_spread_samples_absent_for_snapshot_window" else empty end
    ])
  }' | redact

echo "market-ingest universe readiness check completed"
