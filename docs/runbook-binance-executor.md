# Binance Executor Runbook

Date: 2026-02-16  
Service: `binance-executor`  
Region: `europe-west1`  
Project: `project-fffcccdf-8872-401a-870`

## Scope
- Validate `/execute` behavior in `mode=test`.
- Confirm idempotency (`duplicate request`) behavior.
- Confirm `EXECUTE OUT` log schema.
- Verify `STATE_WRITE_SKIPPED` semantics and capture.

## Preconditions
- `gcloud` authenticated to project.
- Access to `EXECUTOR_SECRET` for authorized `/execute` calls.
- Endpoint: `https://binance-executor-qiwp7prwca-ew.a.run.app/execute`

## Key Behavior Rules
- `/execute` requires auth:
  - `Authorization: Bearer <EXECUTOR_SECRET>`, or
  - `secret` field in JSON body.
- Duplicate requests short-circuit before `persistState`.
- `STATE_WRITE_SKIPPED` appears only when:
  - request is authenticated,
  - request is not duplicate,
  - flow reaches `persistState`,
  - `mode=test`.

## Validation Steps
1. Read recent logs and check baseline health.
2. Send first authenticated request with unique `clientOrderId`.
3. Re-send same request to force duplicate path.
4. Confirm logs:
   - `EXECUTE OUT` exists for both requests.
   - duplicate response includes `skipped:"duplicate request"`.
   - `STATE_WRITE_SKIPPED` appears for non-duplicate `mode=test` path.

## Firestore TTL Verification Checklist
- TTL field: `expiresAtMs` in collection `executor_idempotency` (or value of `FIRESTORE_IDEMPOTENCY_COLLECTION`).
- Where to verify: Google Cloud Console -> Firestore -> Databases -> TTL policies.
- Good state:
  - policy exists for the idempotency collection group,
  - field is exactly `expiresAtMs`,
  - policy status is `Active/Enabled` (not `Creating` or `Disabled`).

## Commands
```bash
gcloud run services logs read binance-executor --project project-fffcccdf-8872-401a-870 --region europe-west1 --limit 120
```

```bash
SECRET='<EXECUTOR_SECRET>'; CID="state-skip-$(date +%s)"; curl -sS -X POST 'https://binance-executor-qiwp7prwca-ew.a.run.app/execute' -H "authorization: Bearer $SECRET" -H 'content-type: application/json' -d '{"env":"mainnet","mode":"test","binanceSymbol":"BTCUSDT","side":"BUY","orderType":"MARKET","notionalUSDT":10,"clientOrderId":"'"$CID"'","strategy":"auto"}'
```

```bash
SECRET='<EXECUTOR_SECRET>'; CID='<same-client-order-id>'; curl -sS -X POST 'https://binance-executor-qiwp7prwca-ew.a.run.app/execute' -H "authorization: Bearer $SECRET" -H 'content-type: application/json' -d '{"env":"mainnet","mode":"test","binanceSymbol":"BTCUSDT","side":"BUY","orderType":"MARKET","notionalUSDT":10,"clientOrderId":"'"$CID"'","strategy":"auto"}'
```

```bash
gcloud run services logs read binance-executor --project project-fffcccdf-8872-401a-870 --region europe-west1 --limit 300 | grep -E "EXECUTE IN|EXECUTE OUT|STATE_WRITE_SKIPPED|duplicate request"
```

## Expected Evidence
- First call: `ok:true` (or strategy-specific handled success path), no duplicate marker.
- Second call with same `clientOrderId`: `ok:true` and `skipped:"duplicate request"`.
- Logs include `EXECUTE OUT` with fields:
  - `clientOrderId, reqSide, execSide, strategy, env, symbol, mode, binanceStatus, endpoint`.
- `STATE_WRITE_SKIPPED` should be present for the non-duplicate `mode=test` flow.

## Latest Validation Snapshot (2026-02-16)
- CID: `state-skip-1771256603`
- First request RID `eebe4b0ab5a8`:
  - `STATE_WRITE_SKIPPED` observed.
  - `EXECUTE OUT` observed with `binanceStatus:200`, `endpoint:/api/v3/order/test`.
- Second request RID `00b2329a84b3` (same CID):
  - duplicate behavior observed in response: `skipped:"duplicate request"`.
  - `EXECUTE OUT` observed with `binanceStatus:null`, `endpoint:null`.

## Troubleshooting
- `401 unauthorized`:
  - Missing/invalid bearer token or secret mismatch.
- `STATE_WRITE_SKIPPED` missing:
  - Request likely duplicate (early return before `persistState`).
  - Request may fail before persist branch.
  - Validate unique `clientOrderId` and successful path.
- Noisy old errors in logs:
  - Use latest timestamps; ignore old revision startup failures.
