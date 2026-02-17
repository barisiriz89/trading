#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${POLY_CLOUD_URL:-}}"
SECRET="${POLY_TV_SECRET:-}"
PROJECT="${POLY_GCP_PROJECT:-project-fffcccdf-8872-401a-870}"
REGION="${POLY_GCP_REGION:-europe-west1}"
SERVICE="${POLY_SERVICE:-polymarket-bot}"

if [ -z "$BASE_URL" ]; then
  echo "FAIL: cloud url gerekli. Kullanim: ./scripts/poly_scheduler_smoke.sh https://<service-url>" >&2
  exit 30
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 31
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

resp="$tmp_dir/tick.json"
code="$(curl -sS -o "$resp" -w "%{http_code}" -X POST "$BASE_URL/tick" -H 'content-type: application/json' --data "{\"secret\":\"$SECRET\",\"env\":\"mainnet\",\"mode\":\"test\"}")"
if [ "$code" != "200" ]; then
  echo "FAIL: POST /tick status=$code" >&2
  cat "$resp" >&2 || true
  exit 32
fi

python3 - "$resp" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if not isinstance(j,dict):
    raise SystemExit("FAIL: /tick json degil")
if j.get("ok") is not True:
    raise SystemExit("FAIL: /tick ok=true degil")
PY

sleep 3
logs="$(gcloud run services logs read "$SERVICE" --project "$PROJECT" --region "$REGION" --limit 120 2>/dev/null || true)"
if ! printf '%s\n' "$logs" | grep -q 'DECISION:'; then
  echo "FAIL: DECISION marker loglarda bulunamadi" >&2
  exit 33
fi

echo "PASS: poly_scheduler_smoke (/tick + DECISION log)"
