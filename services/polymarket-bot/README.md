# Polymarket Bot (5m Canary)

Service: `services/polymarket-bot`

## Endpoints
- `GET /healthz`
- `GET /status`
- `POST /tick` (internal indicator pipeline)
- `POST /execute` (TradingView quorum execution, alias of tick decision handler)

## TradingView `/execute` payload
```json
{
  "secret": "tv_...",
  "env": "mainnet",
  "mode": "test",
  "marketSlug": "btc-updown-5m-1771368900",
  "votes": [
    {"name":"ema","side":"UP"},
    {"name":"rsi","side":"UP"},
    {"name":"donchian","side":"DOWN"}
  ],
  "minAgree": 2,
  "notionalUSD": 5,
  "clientOrderId": "tv-btc-5m-123",
  "ts": 1771368900000
}
```

Validation order:
1. secret auth (`POLY_TV_SECRET`)
2. env/mode/votes/notional/clientOrderId/ts
3. idempotency key creation

Invalid payloads never consume idempotency slot.

## Discovery
If `marketSlug` is not provided, bot auto-selects tradable market from:
- `GET https://gamma-api.polymarket.com/events?order=id&ascending=false&closed=false&limit=400`
- filters slug prefix `btc-updown-5m-`
- requires tradable flags (`acceptingOrders`, `active`, not `closed`, `approved`)
- picks nearest upcoming start/end time
- caches selected market for 20 seconds

## Env Vars
- `PORT` default `19082`
- `POLY_TV_SECRET` required for `/execute`
- `POLY_YES_TOKEN_ID` + `POLY_NO_TOKEN_ID` override all token discovery
- `POLY_CLOB_TOKEN_IDS` optional JSON array override fallback, format: `["yesTokenId","noTokenId"]`
- `POLY_DRY_RUN` default `true`
- `POLY_KILL_SWITCH` default `false`
- `POLY_COOLDOWN_BARS` default `2`
- `POLY_MAX_TRADES_PER_HOUR` default `3`
- `POLY_MAX_POSITION_USD` default `50`
- `POLY_ORDER_USD` default `5` (used by `/tick`)
- `POLY_STRATEGY_VERSION` default `v1.0.0`
- `POLY_PRIVATE_KEY` required only for live order placement
- `POLY_FUNDER_ADDRESS` optional
- `POLY_CLOB_HOST` default `https://clob.polymarket.com`
- `POLY_GAMMA_HOST` default `https://gamma-api.polymarket.com`

## Run
```bash
PORT=19082 POLY_DRY_RUN=true POLY_TV_SECRET='tv_xxx' npm run poly:dev
```

## Cloud Run Deploy (DRY_RUN default)
```bash
POLY_GCP_PROJECT='your-project-id' POLY_GCP_REGION='europe-west1' POLY_SERVICE='polymarket-bot' POLY_TV_SECRET='tv_xxx' npm run poly:deploy
```

## Find Active BTC Up/Down 5m Market
```bash
python3 scripts/find_active_btc_updown_5m.py
```

## Execute Example (DRY_RUN)
```bash
curl -sS -X POST http://localhost:19082/execute -H 'content-type: application/json' -d '{"secret":"tv_xxx","env":"mainnet","mode":"test","votes":[{"name":"ema","side":"UP"},{"name":"rsi","side":"UP"},{"name":"donch","side":"DOWN"}],"minAgree":2,"notionalUSD":5,"clientOrderId":"tv-btc-5m-1","ts":1771368900000}'
```

## Test
```bash
npm run poly:test
```

## Local Smoke
Service acikken (`PORT=19082`):
```bash
POLY_TV_SECRET='tv_xxx' npm run poly:smoke
```

## Cloud Smoke
Cloud Run URL ile:
```bash
POLY_TV_SECRET='tv_xxx' npm run poly:cloud-smoke -- https://your-service-url
```
