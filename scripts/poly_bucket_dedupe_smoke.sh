#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${POLY_CLOUD_URL:-}}"
SECRET="${POLY_TV_SECRET:-}"

if [ -z "$BASE_URL" ]; then
  echo "FAIL: cloud url gerekli. Kullanim: ./scripts/poly_bucket_dedupe_smoke.sh https://<service-url>" >&2
  exit 61
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 62
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

TS="$(python3 - <<'PY'
import time
print(int(time.time()*1000))
PY
)"

PAYLOAD="$tmp_dir/p.json"
FIRST="$tmp_dir/first.json"
SECOND="$tmp_dir/second.json"

cat > "$PAYLOAD" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"test","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"bucket-dedupe-$(date +%s)","ts":$TS}
JSON

curl -sS -o "$FIRST" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$PAYLOAD" >/tmp/poly_bucket_dedupe_code1.txt
curl -sS -o "$SECOND" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$PAYLOAD" >/tmp/poly_bucket_dedupe_code2.txt

python3 - "$FIRST" "$SECOND" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    first=json.load(f)
with open(sys.argv[2],"r",encoding="utf-8") as f:
    second=json.load(f)
if "deduped" not in second or "reason" not in second:
    raise SystemExit("FAIL: dedupe response alanlari eksik")
if second.get("deduped") is not True or second.get("reason") != "already_filled_this_bucket":
    raise SystemExit("FAIL: ikinci cagri deduped=true reason=already_filled_this_bucket olmali")
print("PASS: poly_bucket_dedupe_smoke")
PY
