import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

test("saveState uses transaction + revision guard", () => {
  const src = readFileSync("services/binance-executor/server.js", "utf8");
  assert.match(src, /async function saveState\(env, symbol, state\)/);
  assert.match(src, /await db\.runTransaction\(async \(tx\) => \{/);
  assert.match(src, /state revision conflict/);
  assert.match(src, /rev: expectedRev \+ 1/);
});

test("idempotency uses IN_PROGRESS/SUCCEEDED/FAILED lifecycle", () => {
  const src = readFileSync("services/binance-executor/server.js", "utf8");
  assert.match(src, /status: "IN_PROGRESS"/);
  assert.match(src, /if \(data\.status === "SUCCEEDED"\)/);
  assert.match(src, /if \(idem\.status === "IN_PROGRESS"\)/);
  assert.match(src, /reason: "IN_PROGRESS"/);
  assert.match(src, /finalizeIdempotency\(idem\.key, state, summary\)/);
});
