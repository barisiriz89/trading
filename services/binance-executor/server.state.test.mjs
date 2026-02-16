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
