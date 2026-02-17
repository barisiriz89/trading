#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${POLY_BASE_URL:-http://localhost:19082}"
SECRET="${POLY_TV_SECRET:-${TV_SECRET_VAL:-}}"

if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET (veya TV_SECRET_VAL) set edilmemis" >&2
  exit 10
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

check_get_json() {
  local path="$1"
  local code
  code="$(curl -sS -o "$tmp_dir/body.json" -w "%{http_code}" "$BASE_URL$path")"
  if [ "$code" != "200" ]; then
    echo "FAIL: GET $path status=$code" >&2
    exit 11
  fi
  python3 - "$tmp_dir/body.json" <<'PY'
import json,sys
p=sys.argv[1]
with open(p,"r",encoding="utf-8") as f:
    j=json.load(f)
if not isinstance(j,dict):
    raise SystemExit(1)
print("ok")
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
    exit 12
  fi
}

check_get_json "/healthz/" >/dev/null
check_get_json "/status" >/dev/null

CID="smoke-$(date +%s)"
PAYLOAD_FILE="$tmp_dir/payload.json"
FIRST_FILE="$tmp_dir/first.json"
SECOND_FILE="$tmp_dir/second.json"

cat > "$PAYLOAD_FILE" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"test","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"$CID","ts":1771368900000}
JSON

post_execute "$PAYLOAD_FILE" "$FIRST_FILE"
python3 - "$FIRST_FILE" <<'PY'
import json,sys
p=sys.argv[1]
with open(p,"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("decision") != "UP":
    raise SystemExit("FAIL: first decision UP degil")
if j.get("deduped") is True:
    raise SystemExit("FAIL: first call deduped olmamali")
print("first_ok")
PY

post_execute "$PAYLOAD_FILE" "$SECOND_FILE"
python3 - "$SECOND_FILE" <<'PY'
import json,sys
p=sys.argv[1]
with open(p,"r",encoding="utf-8") as f:
    j=json.load(f)
if j.get("deduped") is not True:
    raise SystemExit("FAIL: second call deduped true olmali")
print("second_ok")
PY

echo "PASS: poly_smoke (healthz/status/execute/dedupe)"
