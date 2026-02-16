import test from "node:test";
import assert from "node:assert/strict";

process.env.EXECUTOR_DISABLE_AUTOSTART = "true";
process.env.EXECUTOR_STATE_BACKEND = "memory";

const mod = await import("./server.js");
const tmod = mod.__test;

test.beforeEach(() => {
  tmod.resetMemoryStore();
});

test("saveState rejects stale concurrent writes (revision guard)", async () => {
  const env = "testnet";
  const symbol = "BTCUSDT";

  const base = await tmod.loadState(env, symbol);
  const a = structuredClone(base);
  const b = structuredClone(base);

  a.lastActionMs = 111;
  b.lastActionMs = 222;

  const [ra, rb] = await Promise.allSettled([
    tmod.saveState(env, symbol, a),
    tmod.saveState(env, symbol, b),
  ]);

  const fulfilled = [ra, rb].filter((x) => x.status === "fulfilled").length;
  const rejected = [ra, rb].filter((x) => x.status === "rejected").length;
  assert.equal(fulfilled, 1);
  assert.equal(rejected, 1);

  const latest = await tmod.loadState(env, symbol);
  assert.equal(latest.rev, 1);
  assert.ok([111, 222].includes(latest.lastActionMs));
});

test("idempotency returns IN_PROGRESS for duplicate in-flight claim", async () => {
  const p = {
    env: "testnet",
    mode: "test",
    symbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "idem-in-progress",
    rid: "r1",
  };

  const first = await tmod.claimIdempotency(p);
  const second = await tmod.claimIdempotency({ ...p, rid: "r2" });

  assert.equal(first.status, "CLAIMED");
  assert.equal(second.status, "IN_PROGRESS");
  assert.equal(first.key, second.key);
});

test("idempotency returns SUCCEEDED summary for duplicate after finalize", async () => {
  const p = {
    env: "testnet",
    mode: "test",
    symbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "idem-succeeded",
    rid: "r1",
  };

  const first = await tmod.claimIdempotency(p);
  assert.equal(first.status, "CLAIMED");

  await tmod.finalizeIdempotency(first.key, "SUCCEEDED", {
    ok: true,
    reason: "cooldown",
    httpStatus: 200,
  });

  const second = await tmod.claimIdempotency({ ...p, rid: "r2" });
  assert.equal(second.status, "SUCCEEDED");
  assert.deepEqual(second.summary, {
    ok: true,
    reason: "cooldown",
    httpStatus: 200,
  });
});
