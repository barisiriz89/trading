# Polymarket Bot (5m Canary v1)

Express service for canary live trading with 3 indicators and 2-of-3 confirmation.

## Endpoints
- `GET /healthz` -> `{ ok: true, ts }`
- `GET /status` -> in-memory state + last run summary
- `POST /tick` -> runs one strategy evaluation + optional trade

## Strategy
Runs on last fully closed 5m candle (built from 1m price history of YES token).

Indicators:
- Trend: `EMA20 vs EMA50` and `close vs EMA20`
- Momentum: `RSI14`
- Breakout: `Donchian20`

Decision:
- `LONG` (buy YES) when long votes >= 2
- `SHORT` (buy NO) when short votes >= 2
- otherwise `FLAT`

## Required Env
- `POLY_MARKET_SLUG` (unless overriding token ids)
- `POLY_YES_TOKEN_ID` + `POLY_NO_TOKEN_ID` (optional direct override)

## Risk Guardrails
- `POLY_KILL_SWITCH=true` blocks trading
- `POLY_DRY_RUN=true` default; compute only, no orders
- `POLY_COOLDOWN_BARS=2` default
- `POLY_MAX_TRADES_PER_HOUR=3` default
- `POLY_MAX_POSITION_USD=50` default
- `POLY_ORDER_USD=5` default
- `POLY_STRATEGY_VERSION=v1.0.0` default

## Order Execution Env
- `POLY_PRIVATE_KEY` (required when `POLY_DRY_RUN=false`)
- `POLY_FUNDER_ADDRESS` optional
- `POLY_CLOB_HOST=https://clob.polymarket.com`

## Other Env
- `PORT=19082`
- `POLY_LOOKBACK_SEC=21600` (6h)
- `POLY_GAMMA_HOST=https://gamma-api.polymarket.com`

## State
- In-memory state for cooldown/trade limits
- Optional local persistence: `services/polymarket-bot/.state.json`

## Local Run
```bash
PORT=19082 POLY_MARKET_SLUG='your-market-slug' POLY_DRY_RUN=true npm run poly:dev
```

## Trigger One Tick
```bash
curl -sS -X POST http://localhost:19082/tick
```

## Status
```bash
curl -sS http://localhost:19082/status
```
