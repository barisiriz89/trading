#!/usr/bin/env bash
set -euo pipefail

ARG1="${1:-}"
ARG2="${2:-}"
if [ -z "$ARG1" ]; then
  echo "Usage: $0 <market-slug|conditionId> [tokenId|UP|DOWN]" >&2
  exit 1
fi

GAMMA_HOST="${POLY_GAMMA_HOST:-https://gamma-api.polymarket.com}"
CLOB_HOST="${POLY_CLOB_HOST:-https://clob.polymarket.com}"
TARGET="$ARG1"
EXPECT="${ARG2:-}"

is_slug=1
if [[ "$TARGET" =~ ^0x[a-fA-F0-9]{40,}$ ]]; then
  is_slug=0
fi
if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
  is_slug=0
fi

if [ "$is_slug" -eq 1 ]; then
  GAMMA_URL="$GAMMA_HOST/markets/slug/$TARGET"
else
  GAMMA_URL="$GAMMA_HOST/markets?condition_ids=$TARGET"
fi

GAMMA_JSON="$(curl -sS "$GAMMA_URL")"
python3 - <<'PY' "$GAMMA_JSON" "$EXPECT"
import json,sys
raw=sys.argv[1]
expect=sys.argv[2].strip()
obj=json.loads(raw)
m=obj[0] if isinstance(obj,list) and obj else (obj if isinstance(obj,dict) else {})
slug=str(m.get('slug') or '')
cond=str(m.get('conditionId') or m.get('condition_id') or '')
outcomes=m.get('outcomes')
if isinstance(outcomes,str):
    try: outcomes=json.loads(outcomes)
    except: outcomes=[]
if not isinstance(outcomes,list): outcomes=[]
clob=m.get('clobTokenIds')
if isinstance(clob,str):
    try: clob=json.loads(clob)
    except: clob=[]
if not isinstance(clob,list): clob=[]
winner=str(m.get('winner') or m.get('winningOutcome') or '').strip()
res=bool(m.get('resolved') or m.get('closed') or m.get('finalized'))
if not winner:
    prices=m.get('outcomePrices')
    if isinstance(prices,str):
        try: prices=json.loads(prices)
        except: prices=[]
    if isinstance(prices,list):
        low=[str(x).lower() for x in outcomes]
        if 'up' in low and 'down' in low:
            ui,di=low.index('up'),low.index('down')
            try:
                up=float(prices[ui]); down=float(prices[di])
                if up!=down: winner='Up' if up>down else 'Down'
            except: pass
print(f"SLUG={slug}")
print(f"CONDITION_ID={cond}")
print(f"RESOLVED={str(res).lower()}")
print(f"WINNER={winner or '-'}")
print(f"OUTCOMES={json.dumps(outcomes)}")
print(f"CLOB_TOKEN_IDS={json.dumps(clob)}")
if expect:
    exp=expect.upper()
    mapped=''
    if exp.startswith('0X') or exp.isdigit():
        for i,t in enumerate(clob):
            if str(t)==expect and i < len(outcomes):
                mapped=str(outcomes[i]).upper()
                break
    elif exp in ('UP','DOWN'):
        mapped=exp
    print(f"EXPECTED={expect}")
    print(f"EXPECTED_MAPPED={mapped or '-'}")
PY

COND_ID="$(python3 - <<'PY' "$GAMMA_JSON"
import json,sys
obj=json.loads(sys.argv[1])
m=obj[0] if isinstance(obj,list) and obj else (obj if isinstance(obj,dict) else {})
print(str(m.get('conditionId') or m.get('condition_id') or '').strip())
PY
)"

if [ -n "$COND_ID" ]; then
  CLOB_JSON="$(curl -sS "$CLOB_HOST/markets/$COND_ID" || true)"
  python3 - <<'PY' "$CLOB_JSON"
import json,sys
raw=sys.argv[1].strip()
if not raw:
    print('CLOB_MARKET=unavailable')
    raise SystemExit(0)
try:
    m=json.loads(raw)
except Exception:
    print('CLOB_MARKET=unavailable')
    raise SystemExit(0)
resolved=bool(m.get('resolved') or m.get('closed') or m.get('finalized'))
winner=str(m.get('winner') or m.get('winningOutcome') or '')
print(f"CLOB_RESOLVED={str(resolved).lower()}")
print(f"CLOB_WINNER={winner or '-'}")
PY
fi

echo "UI_NOTE=Polymarket UI -> Positions/History içinde ilgili market satırında final sonuç ve kapanış durumunu kontrol et; API'de RESOLVED=true + WINNER doğrulaması ile çapraz teyit et."
