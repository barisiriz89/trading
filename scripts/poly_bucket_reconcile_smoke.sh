#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
node --test services/polymarket-bot/reconcile-state.test.mjs
echo "PASS: poly_bucket_reconcile_smoke"
