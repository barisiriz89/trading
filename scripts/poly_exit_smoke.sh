#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${POLY_CLOUD_URL:-}}"
SECRET="${POLY_TV_SECRET:-}"

if [ -z "$BASE_URL" ]; then
  echo "FAIL: cloud url gerekli. Kullanim: ./scripts/poly_exit_smoke.sh https://<service-url>" >&2
  exit 71
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 72
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

TS="$(python3 - <<'PY'
import time,random
print(int(time.time()*1000) + (random.randint(1,999) * 300000))
PY
)"

PAYLOAD="$tmp_dir/p.json"
RESP="$tmp_dir/r.json"
cat > "$PAYLOAD" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"live","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"exit-smoke-$(date +%s)","ts":$TS}
JSON

CODE="$(curl -sS -o "$RESP" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$PAYLOAD")"
if [ "$CODE" != "200" ]; then
  echo "FAIL: POST /execute status=$CODE" >&2
  cat "$RESP" >&2 || true
  exit 73
fi

python3 - "$RESP" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
required=["rid","decision","mode","dryRun","tradeExecuted","deduped","reason","bucketKey","step","lossStreak","computedNotionalUSD","marketSlug","openTradesSummary","pendingUnresolvedCount","exitAttempted","exitResult"]
for key in required:
    if key not in j:
        raise SystemExit(f"FAIL: response field eksik: {key}")
print("PASS: poly_exit_smoke")
PY
