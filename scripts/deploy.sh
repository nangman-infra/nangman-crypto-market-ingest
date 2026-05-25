#!/usr/bin/env bash
set -euo pipefail

cat >&2 <<'MSG'
market-ingest local compose deploy is disabled.

Current deployment source of truth:
  /Volumes/WD/Developments/nangman-crypto/apps/market-ingest-app/ecs
  AWS ECS service state
  AWS ECR image tags
  AWS S3 market L0/L1 artifacts
  CloudWatch logs and metrics

Use the ECS deployment workflow for runtime changes.
MSG

exit 2
