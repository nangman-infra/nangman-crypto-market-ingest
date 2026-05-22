#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '%s\n' "$*"
}

die() {
  printf 'ECR scan check failed: %s\n' "$*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    die "missing required command: $1"
  fi
}

require_real_value() {
  local name="$1"
  local value="$2"
  if [[ -z "$value" || "$value" == *"<"* || "$value" == *">"* ]]; then
    die "$name must be set to a real value before ECR scan verification"
  fi
}

assert_equals() {
  local actual="$1"
  local expected="$2"
  if [[ "$actual" != "$expected" ]]; then
    die "self-test expected '$expected' but got '$actual'"
  fi
}

aws_cli() {
  local args=(--region "$AWS_REGION")
  if [[ -n "${AWS_PROFILE:-}" ]]; then
    args+=(--profile "$AWS_PROFILE")
  fi
  aws "${args[@]}" "$@"
}

image_media_type() {
  local image_json="$1"
  IMAGE_JSON="$image_json" python3 - "$MARKET_INGEST_ECR_IMAGE_TAG" <<'PY'
import json
import os
import sys

image_tag = sys.argv[1]
data = json.loads(os.environ["IMAGE_JSON"])
details = data.get("imageDetails", [])
if len(details) != 1:
    raise SystemExit(f"expected exactly one ECR image detail, got {len(details)}")
detail = details[0]
media_type = detail.get("imageManifestMediaType", "")
tags = set(detail.get("imageTags", []))
if image_tag not in tags:
    raise SystemExit(f"ECR image detail does not include requested tag: {image_tag}")
print(media_type)
PY
}

assert_single_arch_image() {
  local image_json="$1"
  IMAGE_JSON="$image_json" python3 - "$MARKET_INGEST_ECR_IMAGE_TAG" <<'PY'
import json
import os
import sys

image_tag = sys.argv[1]
detail = json.loads(os.environ["IMAGE_JSON"])["imageDetails"][0]
media_type = detail.get("imageManifestMediaType", "")
if "image.index" in media_type or "manifest.list" in media_type:
    raise SystemExit(
        f"{image_tag} points to a multi-arch image index; resolve the ARM64 child digest instead"
    )
print(f"ECR image ok: tag={image_tag} mediaType={media_type}")
PY
}

resolve_arm64_digest_from_index() {
  local index_manifest="$1"
  INDEX_MANIFEST="$index_manifest" python3 <<'PY'
import json
import os
import sys

manifest = json.loads(os.environ["INDEX_MANIFEST"])
matches = []
for item in manifest.get("manifests", []):
    platform = item.get("platform", {}) or {}
    annotations = item.get("annotations", {}) or {}
    if platform.get("os") != "linux":
        continue
    if platform.get("architecture") != "arm64":
        continue
    if annotations.get("vnd.docker.reference.type") == "attestation-manifest":
        continue
    digest = item.get("digest")
    media_type = item.get("mediaType", "")
    if not digest:
        continue
    if "manifest" not in media_type:
        continue
    matches.append(digest)

if len(matches) != 1:
    raise SystemExit(f"expected exactly one linux/arm64 image manifest in index, got {len(matches)}")
print(matches[0])
PY
}

assert_scan_findings() {
  local scan_json="$1"
  local image_ref="$2"
  SCAN_JSON="$scan_json" python3 - "$MARKET_INGEST_ECR_BLOCKING_SEVERITIES" "$image_ref" <<'PY'
import json
import os
import sys

blocking = [item.strip().upper() for item in sys.argv[1].split(",") if item.strip()]
image_ref = sys.argv[2]
data = json.loads(os.environ["SCAN_JSON"])
status = data.get("imageScanStatus", {}).get("status")
if status != "COMPLETE":
    description = data.get("imageScanStatus", {}).get("description")
    raise SystemExit(f"ECR image scan status is not COMPLETE for {image_ref}: {status} {description}")
counts = data.get("imageScanFindings", {}).get("findingSeverityCounts", {}) or {}
violations = {
    severity: int(counts.get(severity, 0))
    for severity in blocking
    if int(counts.get(severity, 0)) > 0
}
if violations:
    formatted = ", ".join(f"{severity}={count}" for severity, count in violations.items())
    raise SystemExit(f"ECR image has blocking scan findings: {formatted}")
summary = ", ".join(
    f"{severity}={int(counts.get(severity, 0))}"
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFORMATIONAL", "UNDEFINED"]
)
print(f"ECR scan ok: image={image_ref} status=COMPLETE blocking={','.join(blocking)} findings={summary}")
PY
}

self_test() {
  MARKET_INGEST_ECR_IMAGE_TAG="git-self-test-arm64"
  MARKET_INGEST_ECR_BLOCKING_SEVERITIES="CRITICAL,HIGH"

  assert_single_arch_image '{
    "imageDetails": [{
      "imageTags": ["git-self-test-arm64"],
      "imageManifestMediaType": "application/vnd.oci.image.manifest.v1+json"
    }]
  }' >/dev/null

  assert_scan_findings '{
    "imageScanStatus": {"status": "COMPLETE"},
    "imageScanFindings": {
      "findingSeverityCounts": {
        "CRITICAL": 0,
        "HIGH": 0,
        "MEDIUM": 2,
        "LOW": 4
      }
    }
  }' "imageTag=git-self-test-arm64" >/dev/null

  assert_equals "$(resolve_arm64_digest_from_index '{
    "schemaVersion": 2,
    "mediaType": "application/vnd.oci.image.index.v1+json",
    "manifests": [
      {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": "sha256:arm64selftest",
        "platform": {"architecture": "arm64", "os": "linux"}
      },
      {
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": "sha256:attestationselftest",
        "annotations": {"vnd.docker.reference.type": "attestation-manifest"},
        "platform": {"architecture": "unknown", "os": "unknown"}
      }
    ]
  }')" "sha256:arm64selftest"

  if assert_single_arch_image '{
    "imageDetails": [{
      "imageTags": ["git-self-test"],
      "imageManifestMediaType": "application/vnd.oci.image.index.v1+json"
    }]
  }' >/dev/null 2>&1; then
    die "self-test expected image index tags to be rejected"
  fi

  if assert_scan_findings '{
    "imageScanStatus": {"status": "COMPLETE"},
    "imageScanFindings": {
      "findingSeverityCounts": {
        "CRITICAL": 0,
        "HIGH": 1
      }
    }
  }' "imageTag=git-self-test-arm64" >/dev/null 2>&1; then
    die "self-test expected HIGH findings to be rejected"
  fi

  log "check-ecr-scan self-test passed"
}

main() {
  require_command aws
  require_command python3

  AWS_REGION="${AWS_REGION:-ap-northeast-2}"
  MARKET_INGEST_ECR_REPOSITORY="${MARKET_INGEST_ECR_REPOSITORY:-ecr-nangman-dev-crypto-market-ingest-apn2}"
  MARKET_INGEST_ECR_IMAGE_TAG="${MARKET_INGEST_ECR_IMAGE_TAG:-}"
  MARKET_INGEST_ECR_BLOCKING_SEVERITIES="${MARKET_INGEST_ECR_BLOCKING_SEVERITIES:-CRITICAL,HIGH}"

  require_real_value AWS_REGION "$AWS_REGION"
  require_real_value MARKET_INGEST_ECR_REPOSITORY "$MARKET_INGEST_ECR_REPOSITORY"
  require_real_value MARKET_INGEST_ECR_IMAGE_TAG "$MARKET_INGEST_ECR_IMAGE_TAG"
  if [[ "$MARKET_INGEST_ECR_IMAGE_TAG" != *"arm64"* && "${MARKET_INGEST_ECR_ALLOW_NON_ARM64_TAG:-0}" != "1" ]]; then
    die "MARKET_INGEST_ECR_IMAGE_TAG should be the single-arch ARM64 tag; set MARKET_INGEST_ECR_ALLOW_NON_ARM64_TAG=1 only for an explicit exception"
  fi

  local image_json
  image_json="$(aws_cli ecr describe-images \
    --repository-name "$MARKET_INGEST_ECR_REPOSITORY" \
    --image-ids "imageTag=$MARKET_INGEST_ECR_IMAGE_TAG" \
    --output json)"
  local media_type
  media_type="$(image_media_type "$image_json")"

  local scan_image_ref
  if [[ "$media_type" == *"image.index"* || "$media_type" == *"manifest.list"* ]]; then
    local index_manifest
    local arm64_digest
    log "ECR image tag is a multi-arch index; resolving linux/arm64 child digest"
    index_manifest="$(aws_cli ecr batch-get-image \
      --repository-name "$MARKET_INGEST_ECR_REPOSITORY" \
      --image-ids "imageTag=$MARKET_INGEST_ECR_IMAGE_TAG" \
      --accepted-media-types \
      "application/vnd.oci.image.index.v1+json" \
      "application/vnd.docker.distribution.manifest.list.v2+json" \
      --query 'images[0].imageManifest' \
      --output text)"
    arm64_digest="$(resolve_arm64_digest_from_index "$index_manifest")"
    scan_image_ref="imageDigest=$arm64_digest"
    log "ECR image ok: tag=$MARKET_INGEST_ECR_IMAGE_TAG mediaType=$media_type arm64Digest=$arm64_digest"
  else
    assert_single_arch_image "$image_json"
    scan_image_ref="imageTag=$MARKET_INGEST_ECR_IMAGE_TAG"
  fi

  local scan_json
  scan_json="$(aws_cli ecr describe-image-scan-findings \
    --repository-name "$MARKET_INGEST_ECR_REPOSITORY" \
    --image-id "$scan_image_ref" \
    --output json)"
  assert_scan_findings "$scan_json" "$scan_image_ref"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  if [[ "${MARKET_INGEST_ECR_SCAN_SELF_TEST:-}" == "1" ]]; then
    self_test
  else
    main "$@"
  fi
fi
