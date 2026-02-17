#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${POLY_CLOUD_URL:-}}"
SECRET="${POLY_TV_SECRET:-}"

if [ -z "$BASE_URL" ]; then
  echo "FAIL: cloud url gerekli. Kullanim: ./scripts/poly_live_smoke.sh https://<service-url>" >&2
  exit 40
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 41
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

post_execute() {
  local payload_file="$1"
  local out_file="$2"
  local code
  code="$(curl -sS -o "$out_file" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$payload_file")"
  if [ "$code" != "200" ]; then
    echo "FAIL: POST /execute status=$code" >&2
    cat "$out_file" >&2 || true
    exit 42
  fi
}

ts1="$(python3 - <<'PY'
import time,random
print(int(time.time()*1000) + (random.randint(1,999) * 300000))
PY
)"

off_payload="$tmp_dir/live_off.json"
off_resp="$tmp_dir/live_off_resp.json"
cat > "$off_payload" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"live","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"live-off-$(date +%s)","ts":$ts1}
JSON

post_execute "$off_payload" "$off_resp"
if python3 - "$off_resp" 2>/dev/null <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("mode") != "live":
    raise SystemExit("FAIL: live gate off testinde mode live olmali")
if j.get("tradeExecuted") is not False:
    raise SystemExit("FAIL: live gate off testinde tradeExecuted false olmali")
if j.get("reason") != "live_not_enabled":
    raise SystemExit("FAIL: live gate off testinde reason live_not_enabled olmali")
PY
then
  echo "PASS: poly_live_smoke (live gate-off: live_not_enabled)"
  exit 0
fi

ts2="$(python3 - <<'PY'
import time,random
print(int(time.time()*1000) + (random.randint(1001,1999) * 300000))
PY
)"

on_payload="$tmp_dir/live_on.json"
on_first="$tmp_dir/live_on_first.json"
on_second="$tmp_dir/live_on_second.json"
cid="live-on-$(date +%s)"
cat > "$on_payload" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"live","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":999,"clientOrderId":"$cid","ts":$ts2}
JSON

post_execute "$on_payload" "$on_first"
python3 - "$on_first" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("mode") != "live":
    raise SystemExit("FAIL: gate-on testinde mode live olmali")
if j.get("dryRun") is not False:
    raise SystemExit("FAIL: gate-on testinde dryRun false olmali")
s=j.get("sizing") or {}
if not s.get("auto"):
    raise SystemExit("FAIL: gate-on testinde sizing.auto true olmali")
if not (s.get("computedNotionalUSD") is not None):
    raise SystemExit("FAIL: gate-on testinde sizing.computedNotionalUSD olmali")
if (j.get("order") or {}).get("reason") == "live_not_enabled":
    raise SystemExit("FAIL: gate-on testinde live_not_enabled olmamali")
if j.get("tradeExecuted") is not True:
    reason = j.get("reason")
    if reason not in {"geoblock", "api_key_create_failed"}:
        raise SystemExit(f"FAIL: gate-on blocker reason beklenmiyor: {reason}")
PY

post_execute "$on_payload" "$on_second"
python3 - "$on_second" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("deduped") is not True:
    raise SystemExit("FAIL: ikinci gate-on cagrida deduped true olmali")
PY

echo "PASS: poly_live_smoke (live gate-off + gate-on + dedupe)"
