#!/usr/bin/env bash
set -euo pipefail

PORT="${POLY_RECONCILE_SMOKE_PORT:-19084}"
BASE_URL="${POLY_BASE_URL:-http://localhost:$PORT}"
SECRET="${POLY_TV_SECRET:-${TV_SECRET_VAL:-}}"
if [ -z "$SECRET" ]; then
  echo "FAIL: POLY_TV_SECRET set edilmemis" >&2
  exit 81
fi

tmp_dir="$(mktemp -d)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

find_slug() {
  local f
  f="$(mktemp)"
  curl -sS 'https://gamma-api.polymarket.com/events?order=id&ascending=false&closed=false&limit=400' > "$f"
  python3 - <<'PY' "$f"
import json,sys,time
now=int(time.time()*1000)
with open(sys.argv[1],'r',encoding='utf-8') as fp:
    obj=json.load(fp)
cands=[]
for e in obj if isinstance(obj,list) else []:
    for m in e.get('markets') or []:
        slug=str(m.get('slug') or '')
        if not slug.startswith('btc-updown-5m-'): continue
        if m.get('acceptingOrders') is False: continue
        if m.get('active') is False: continue
        if m.get('closed') is True: continue
        endv=m.get('endDate') or m.get('eventStartTime') or ''
        ts=0
        try:
            if isinstance(endv,(int,float)): ts=int(endv)
            else:
                import datetime
                ts=int(datetime.datetime.fromisoformat(str(endv).replace('Z','+00:00')).timestamp()*1000)
        except Exception:
            ts=0
        cands.append((abs(ts-now) if ts else 10**18, slug, m))
if not cands:
    print('')
    raise SystemExit(0)
cands.sort(key=lambda x:x[0])
print(cands[0][1])
PY
  rm -f "$f"
}

SLUG="${POLY_TEST_MARKET_SLUG:-$(find_slug)}"
if [ -z "$SLUG" ]; then
  echo "FAIL: active slug bulunamadi" >&2
  exit 82
fi

GAMMA_MARKET_JSON="$(curl -sS "https://gamma-api.polymarket.com/markets/slug/$SLUG")"
python3 - <<'PY' "$GAMMA_MARKET_JSON" > "$tmp_dir/market.env"
import json,sys
obj=json.loads(sys.argv[1])
m=obj[0] if isinstance(obj,list) and obj else (obj if isinstance(obj,dict) else {})
out=m.get('outcomes')
if isinstance(out,str):
    try: out=json.loads(out)
    except: out=[]
ids=m.get('clobTokenIds')
if isinstance(ids,str):
    try: ids=json.loads(ids)
    except: ids=[]
up=''; down=''
for i,name in enumerate(out if isinstance(out,list) else []):
    label=str(name).strip().lower()
    tid=str(ids[i]).strip() if isinstance(ids,list) and i<len(ids) else ''
    if label=='up': up=tid
    if label=='down': down=tid
print(f"COND={str(m.get('conditionId') or m.get('condition_id') or '').strip()}")
print(f"UP_TOKEN={up}")
print(f"DOWN_TOKEN={down}")
PY
source "$tmp_dir/market.env"
if [ -z "${UP_TOKEN:-}" ]; then
  echo "FAIL: UP token bulunamadi" >&2
  exit 83
fi

FIXTURE="$tmp_dir/gamma_resolved_fixture.json"
cat > "$FIXTURE" <<JSON
{"slug":"$SLUG","conditionId":"${COND:-fixture-cond}","closed":true,"resolved":true,"winner":"Up","outcomes":["Up","Down"],"clobTokenIds":["$UP_TOKEN","${DOWN_TOKEN:-x}"],"resolvedTime":$(python3 - <<'PY'
import time
print(int(time.time()*1000))
PY
)}
JSON

PORT="$PORT" POLY_DRY_RUN=true POLY_DEBUG_STATE=true POLY_RECONCILE_FIXTURE="$FIXTURE" POLY_TV_SECRET="$SECRET" \
  npm --workspace services/polymarket-bot run dev >"$tmp_dir/dev.log" 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID >/dev/null 2>&1 || true; cleanup' EXIT

for _ in $(seq 1 40); do
  if curl -sS -o /dev/null -w "%{http_code}" "$BASE_URL/healthz" | grep -q '^200$'; then
    break
  fi
  sleep 0.5
done
if ! curl -sS -o /dev/null -w "%{http_code}" "$BASE_URL/healthz" | grep -q '^200$'; then
  echo "FAIL: local reconcile smoke service baslatilamadi" >&2
  tail -n 80 "$tmp_dir/dev.log" >&2 || true
  exit 84
fi

TS_NOW="$(python3 - <<'PY'
import time
print(int(time.time()*1000))
PY
)"
BUCKET_NOW="$(python3 - <<'PY' "$TS_NOW"
import sys
ts=int(sys.argv[1])
print(ts//300000)
PY
)"
PENDING_BUCKET="$((BUCKET_NOW-1))"
PENDING_CREATED="$(python3 - <<'PY' "$PENDING_BUCKET"
import sys
print(int(sys.argv[1])*300000)
PY
)"

DEBUG_PAYLOAD="$tmp_dir/debug.json"
cat > "$DEBUG_PAYLOAD" <<JSON
{"env":"mainnet","marketSlug":"$SLUG","state":{"step":0,"lossStreak":0,"cumulativeLossUSD":0,"filledBuckets":{},"openTrades":[{"bucketKey":$PENDING_BUCKET,"marketSlug":"$SLUG","conditionId":"${COND:-fixture-cond}","tokenId":"$UP_TOKEN","side":"UP","notionalUSD":5,"priceEntry":0.5,"sizeEntry":10,"createdAtMs":$PENDING_CREATED,"status":"open","exit":null,"settlement":null}],"resolvedTrades":[]}}
JSON

DEBUG_RESP="$tmp_dir/debug_resp.json"
DEBUG_CODE="$(curl -sS -o "$DEBUG_RESP" -w "%{http_code}" -X POST "$BASE_URL/debug/exec-state" -H 'content-type: application/json' --data "@$DEBUG_PAYLOAD")"
if [ "$DEBUG_CODE" != "200" ]; then
  echo "FAIL: debug state set basarisiz code=$DEBUG_CODE" >&2
  cat "$DEBUG_RESP" >&2 || true
  exit 85
fi

EXEC_PAYLOAD="$tmp_dir/execute.json"
cat > "$EXEC_PAYLOAD" <<JSON
{"secret":"$SECRET","env":"mainnet","mode":"test","marketSlug":"$SLUG","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"reconcile-unit-$(date +%s)","ts":$TS_NOW}
JSON

EXEC_RESP="$tmp_dir/execute_resp.json"
EXEC_CODE="$(curl -sS -o "$EXEC_RESP" -w "%{http_code}" -X POST "$BASE_URL/execute" -H 'content-type: application/json' --data "@$EXEC_PAYLOAD")"
if [ "$EXEC_CODE" != "200" ]; then
  echo "FAIL: execute code=$EXEC_CODE" >&2
  cat "$EXEC_RESP" >&2 || true
  exit 86
fi

python3 - <<'PY' "$EXEC_RESP"
import json,sys
j=json.load(open(sys.argv[1]))
rr=j.get('reconcileResult') or {}
if j.get('reconcileAttempted') is not True:
    raise SystemExit('FAIL: reconcileAttempted true olmali')
if rr.get('status')!='resolved':
    raise SystemExit(f"FAIL: reconcile status resolved degil: {rr.get('status')}")
if rr.get('result')!='WIN':
    raise SystemExit(f"FAIL: reconcile result WIN degil: {rr.get('result')}")
if rr.get('stepAfter') not in (0,None):
    raise SystemExit('FAIL: stepAfter beklenmeyen deger')
if j.get('resolvedTradesSummary',{}).get('count',0) < 1:
    raise SystemExit('FAIL: resolvedTradesSummary.count >=1 olmali')
print('PASS: poly_reconcile_unit_smoke')
PY
