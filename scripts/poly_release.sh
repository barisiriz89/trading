#!/usr/bin/env bash
set -euo pipefail

cd "$HOME/trading-platform"

echo "== git =="
git status -sb
git push origin main

echo "== gcloud context =="
gcloud config set account barisiriz@gmail.com >/dev/null
gcloud config set project project-fffcccdf-8872-401a-870 >/dev/null

: "${POLY_TV_SECRET:?POLY_TV_SECRET must be set in environment}"

echo "== deploy =="
POLY_GCP_PROJECT='project-fffcccdf-8872-401a-870' \
POLY_GCP_REGION='europe-west1' \
POLY_SERVICE='polymarket-bot' \
./scripts/poly_deploy.sh

echo "== url =="
URL="$(gcloud run services describe polymarket-bot \
  --project project-fffcccdf-8872-401a-870 \
  --region europe-west1 \
  --format='value(status.url)')"

echo "CLOUD_RUN_URL=$URL"

echo "== cloud smoke =="
POLY_TV_SECRET="$POLY_TV_SECRET" ./scripts/poly_cloud_smoke.sh "$URL"
