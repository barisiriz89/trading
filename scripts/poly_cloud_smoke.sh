#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-${POLY_CLOUD_URL:-}}"
SECRET="${POLY_TV_SECRET:-}"

if [ -z "$BASE_URL" ]; then
  echo "FAIL: cloud url gerekli. Kullanim: ./scripts/poly_cloud_smoke.sh https://<service-url>" >&2
  exit 20
fi
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 21
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

check_get_json() {
  local path="$1"
  local code
  code="$(curl -sS -o "$tmp_dir/body.json" -w "%{http_code}" "$BASE_URL$path")"
  if [ "$code" != "200" ]; then
    return 1
  fi
  python3 - "$tmp_dir/body.json" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if not isinstance(j,dict):
    raise SystemExit(1)
PY
}

post_execute() {
  local payload_file="$1"
  local out_file="$2"
  local code
  code="$(curl -sS -o "$out_file" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$payload_file")"
  if [ "$code" != "200" ]; then
    echo "FAIL: POST /execute status=$code" >&2
    cat "$out_file" >&2 || true
    exit 23
  fi
}

if ! check_get_json "/healthz/" >/dev/null 2>&1 && ! check_get_json "/healthz" >/dev/null 2>&1; then
  echo "FAIL: GET /healthz and /healthz/ status!=200" >&2
  exit 22
fi
check_get_json "/status" || { echo "FAIL: GET /status status!=200" >&2; exit 22; }

CID="cloud-smoke-$(date +%s)"
TS="$(python3 - <<'PY'
import time,random
print(int(time.time()*1000) + (random.randint(1,999) * 300000))
PY
)"

PAYLOAD_FILE="$tmp_dir/payload.json"
FIRST_FILE="$tmp_dir/first.json"
SECOND_FILE="$tmp_dir/second.json"

cat > "$PAYLOAD_FILE" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"test","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"$CID","ts":$TS}
JSON

post_execute "$PAYLOAD_FILE" "$FIRST_FILE"
python3 - "$FIRST_FILE" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("decision") != "UP":
    raise SystemExit("FAIL: first decision UP degil")
if j.get("deduped") is True:
    raise SystemExit("FAIL: first call deduped olmamali")
PY

post_execute "$PAYLOAD_FILE" "$SECOND_FILE"
python3 - "$SECOND_FILE" <<'PY'
import json,sys
with open(sys.argv[1],"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("deduped") is not True:
    raise SystemExit("FAIL: second call deduped true olmali")
PY

echo "PASS: poly_cloud_smoke (healthz/status/execute/dedupe)"
