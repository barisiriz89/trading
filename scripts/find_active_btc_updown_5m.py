#!/usr/bin/env python3
import json
import sys
import urllib.request
from datetime import datetime, timezone

URL = "https://gamma-api.polymarket.com/events?order=id&ascending=false&closed=false&limit=400"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://polymarket.com/",
}


def parse_arr(v):
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            p = json.loads(v)
            return p if isinstance(p, list) else []
        except Exception:
            return []
    return []


def is_tradable(m):
    slug = str(m.get("slug", "")).lower()
    if not slug.startswith("btc-updown-5m-"):
        return False
    if m.get("acceptingOrders") is False:
        return False
    if m.get("active") is False:
        return False
    if m.get("closed") is True:
        return False
    if m.get("approved") is False:
        return False
    return True


def parse_ts(m):
    for k in ("eventStartTime", "eventStart", "endDate", "endDateIso", "end_date"):
        raw = m.get(k)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
        except Exception:
            continue
    return float("inf")


def pick(events):
    now = datetime.now(tz=timezone.utc).timestamp()
    best = None
    best_score = float("inf")

    for ev in events:
        title = ev.get("title") or ev.get("name") or ""
        for m in ev.get("markets", []) or []:
            if not is_tradable(m):
                continue

            outcomes = parse_arr(m.get("outcomes"))
            token_ids = parse_arr(m.get("clobTokenIds"))
            if len(outcomes) < 2 or len(token_ids) < 2:
                continue

            t = parse_ts(m)
            score = abs(t - now)
            if score < best_score:
                best_score = score
                best = {
                    "ACTIVE_EVENT_TITLE": title,
                    "MARKET_QUESTION": m.get("question") or m.get("title") or "",
                    "SLUG": m.get("slug") or "",
                    "OUTCOMES": outcomes,
                    "CLOB_TOKEN_IDS": token_ids,
                }

    return best


def main():
    req = urllib.request.Request(URL, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read().decode("utf-8")
            data = json.loads(body)
    except Exception as e:
        print(f"ERROR: gamma fetch failed: {e}", file=sys.stderr)
        return 2

    if not isinstance(data, list):
        print("ERROR: unexpected gamma payload", file=sys.stderr)
        return 3

    chosen = pick(data)
    if not chosen:
        print("ERROR: no active btc-updown-5m market found", file=sys.stderr)
        return 4

    print(f"ACTIVE_EVENT_TITLE={chosen['ACTIVE_EVENT_TITLE']}")
    print(f"MARKET_QUESTION={chosen['MARKET_QUESTION']}")
    print(f"SLUG={chosen['SLUG']}")
    print(f"OUTCOMES={json.dumps(chosen['OUTCOMES'], ensure_ascii=False)}")
    print(f"CLOB_TOKEN_IDS={json.dumps(chosen['CLOB_TOKEN_IDS'], ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
