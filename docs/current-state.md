# Current State - 2026-02-16

## Environment
- Project: `project-fffcccdf-8872-401a-870`
- Region: `europe-west1`
- Service: `binance-executor`
- Worker: `https://webhook.barisiriz.com/tv`

## Repo
- Remote: `https://github.com/barisiriz89/trading.git`
- Branch: `main`
- Last applied commit: `5f90351`

## Confirmed Working
- `/execute` responds in test mode.
- Idempotency works with same `clientOrderId`.
- Response includes: `"skipped":"duplicate request"`.
- Log format includes `EXECUTE OUT` with:
  - `clientOrderId, reqSide, execSide, strategy, env, symbol, mode, binanceStatus, endpoint`.

## Recent Fixes
- Removed dependency risk from old cloud mismatch.
- Fixed `persistState` recursion bug.
- Restored missing `t/state` init in execute flow (fixed prior 500).

## Validation Update (2026-02-16)
- Code-path verification complete:
  - Duplicate requests return early before `persistState`, so they do not emit `STATE_WRITE_SKIPPED`.
  - `STATE_WRITE_SKIPPED` is emitted only when `mode=test` and execution reaches `persistState`.
- Runtime log check (`--limit 300`) confirms recent `EXECUTE OUT` records.
- Runtime verification completed with authenticated calls:
  - CID: `state-skip-1771256603`
  - First request RID `eebe4b0ab5a8`: `STATE_WRITE_SKIPPED` present, `EXECUTE OUT` present (`status:200`, `binanceStatus:200`, `endpoint:/api/v3/order/test`).
  - Second request RID `00b2329a84b3` (same CID): duplicate path confirmed (`skipped:"duplicate request"` in response), `EXECUTE OUT` present (`status:200`, `binanceStatus:null`, `endpoint:null`).

## Notes
- Old log lines include historical startup/module errors from earlier revisions; focus on latest timestamps.
- Unauthenticated calls return `401 unauthorized`; do not use them for runtime log validation.

## Runbook
- Finalized runbook: `docs/runbook-binance-executor.md`

## Next Ops Commands
- Deploy:
  - `gcloud run deploy binance-executor --project project-fffcccdf-8872-401a-870 --region europe-west1 --source services/binance-executor --allow-unauthenticated`
- Quick logs:
  - `gcloud run services logs read binance-executor --project project-fffcccdf-8872-401a-870 --region europe-west1 --limit 120`
- Filter specific:
  - `gcloud run services logs read binance-executor --project project-fffcccdf-8872-401a-870 --region europe-west1 --limit 300 | grep -E "EXECUTE OUT|STATE_WRITE_SKIPPED|duplicate request"`
