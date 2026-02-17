#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${POLY_GCP_PROJECT:-}"
REGION="${POLY_GCP_REGION:-europe-west1}"
SERVICE="${POLY_SERVICE:-polymarket-bot}"
SECRET="${POLY_TV_SECRET:-}"

if [ -z "$PROJECT_ID" ]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
  echo "FAIL: POLY_GCP_PROJECT yok ve gcloud default project set degil" >&2
  exit 30
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 31
fi

env_file="$(mktemp)"
cleanup() { rm -f "$env_file"; }
trap cleanup EXIT

cat > "$env_file" <<ENVVARS
POLY_DRY_RUN: "true"
POLY_TV_SECRET: "$SECRET"
ENVVARS

echo "Deploying $SERVICE to Cloud Run ($PROJECT_ID/$REGION) with POLY_DRY_RUN=true"
gcloud run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source services/polymarket-bot \
  --allow-unauthenticated \
  --env-vars-file "$env_file"

URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"
echo "SERVICE_URL=$URL"
