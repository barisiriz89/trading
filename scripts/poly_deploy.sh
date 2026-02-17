#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${POLY_GCP_PROJECT:-}"
REGION="${POLY_GCP_REGION:-europe-west1}"
SERVICE="${POLY_SERVICE:-polymarket-bot}"
MARKET_SLUG="${POLY_MARKET_SLUG:-}"
YES_TOKEN_ID="${POLY_YES_TOKEN_ID:-}"
NO_TOKEN_ID="${POLY_NO_TOKEN_ID:-}"
DRY_RUN="${POLY_DRY_RUN:-true}"
LIVE_ENABLED="${POLY_LIVE_ENABLED:-}"
LIVE_CONFIRM="${POLY_LIVE_CONFIRM:-}"
AUTO_SIZE="${POLY_AUTO_SIZE:-}"
START_NOTIONAL="${POLY_START_NOTIONAL_USD:-}"
SIZE_MULT="${POLY_SIZE_MULT:-}"
MAX_NOTIONAL="${POLY_MAX_NOTIONAL_USD:-}"
FUNDER_ADDRESS="${POLY_FUNDER_ADDRESS:-}"
SIGNATURE_TYPE="${POLY_SIGNATURE_TYPE:-}"

if [ -z "$PROJECT_ID" ]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
  echo "FAIL: POLY_GCP_PROJECT yok ve gcloud default project set degil" >&2
  exit 30
fi
env_file="$(mktemp)"
cleanup() { rm -f "$env_file"; }
trap cleanup EXIT

cat > "$env_file" <<ENVVARS
POLY_DRY_RUN: "$DRY_RUN"
ENVVARS

if [ -n "$MARKET_SLUG" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_MARKET_SLUG: "$MARKET_SLUG"
ENVVARS
fi

if [ -n "$YES_TOKEN_ID" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_YES_TOKEN_ID: "$YES_TOKEN_ID"
ENVVARS
fi

if [ -n "$NO_TOKEN_ID" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_NO_TOKEN_ID: "$NO_TOKEN_ID"
ENVVARS
fi

if [ -n "$LIVE_ENABLED" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_LIVE_ENABLED: "$LIVE_ENABLED"
ENVVARS
fi

if [ -n "$LIVE_CONFIRM" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_LIVE_CONFIRM: "$LIVE_CONFIRM"
ENVVARS
fi

if [ -n "$AUTO_SIZE" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_AUTO_SIZE: "$AUTO_SIZE"
ENVVARS
fi

if [ -n "$START_NOTIONAL" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_START_NOTIONAL_USD: "$START_NOTIONAL"
ENVVARS
fi

if [ -n "$SIZE_MULT" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_SIZE_MULT: "$SIZE_MULT"
ENVVARS
fi

if [ -n "$MAX_NOTIONAL" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_MAX_NOTIONAL_USD: "$MAX_NOTIONAL"
ENVVARS
fi

if [ -n "$FUNDER_ADDRESS" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_FUNDER_ADDRESS: "$FUNDER_ADDRESS"
ENVVARS
fi

if [ -n "$SIGNATURE_TYPE" ]; then
  cat >> "$env_file" <<ENVVARS
POLY_SIGNATURE_TYPE: "$SIGNATURE_TYPE"
ENVVARS
fi

echo "Deploying $SERVICE to Cloud Run ($PROJECT_ID/$REGION) with POLY_DRY_RUN=$DRY_RUN"
gcloud run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source services/polymarket-bot \
  --allow-unauthenticated \
  --env-vars-file "$env_file" \
  --set-secrets "POLY_TV_SECRET=POLY_TV_SECRET:latest,POLY_PRIVATE_KEY=POLY_PRIVATE_KEY:latest"

URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"
echo "SERVICE_URL=$URL"
