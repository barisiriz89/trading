import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";

const PORT = 19081;
const BASE = `http://127.0.0.1:${PORT}`;
const SECRET = "test-secret";
let child;

function startServer() {
  return new Promise((resolve, reject) => {
    child = spawn("node", ["services/binance-executor/server.js"], {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        PORT: String(PORT),
        EXECUTOR_SECRET: SECRET,
        ALLOW_MAINNET: "false",
        ALLOW_LIVE: "false",
      },
    });

    let ready = false;
    const onData = (buf) => {
      const s = String(buf || "");
      if (s.includes("binance-executor listening on")) {
        ready = true;
        resolve();
      }
    };

    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.on("exit", (code) => {
      if (!ready) reject(new Error(`server exited early: ${code}`));
    });
  });
}

function stopServer() {
  return new Promise((resolve) => {
    if (!child || child.killed) return resolve();
    child.once("exit", () => resolve());
    child.kill("SIGTERM");
  });
}

async function postExecute(body) {
  const r = await fetch(`${BASE}/execute`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${SECRET}`,
    },
    body: JSON.stringify(body),
  });
  return { status: r.status, body: await r.json() };
}

test.before(async () => {
  await startServer();
});

test.after(async () => {
  await stopServer();
});

test("invalid mode returns 400 and error", async () => {
  const res = await postExecute({
    env: "testnet",
    mode: "prod",
    binanceSymbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "t-invalid-mode",
  });
  assert.equal(res.status, 400);
  assert.equal(res.body.ok, false);
  assert.equal(res.body.error, "invalid mode");
});

test("invalid env returns 400 and error", async () => {
  const res = await postExecute({
    env: "prod",
    mode: "test",
    binanceSymbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "t-invalid-env",
  });
  assert.equal(res.status, 400);
  assert.equal(res.body.ok, false);
  assert.equal(res.body.error, "invalid env");
});

test("invalid side returns 400 and does not get deduped", async () => {
  const payload = {
    env: "testnet",
    mode: "test",
    binanceSymbol: "BTCUSDT",
    side: "HOLD",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "t-invalid-side",
  };

  const first = await postExecute(payload);
  const second = await postExecute(payload);

  assert.equal(first.status, 400);
  assert.equal(first.body.error, "invalid side");
  assert.equal(second.status, 400);
  assert.equal(second.body.error, "invalid side");
  assert.equal(second.body.skipped, undefined);
});

test("ALLOW_LIVE=false + mode=live returns 403", async () => {
  const res = await postExecute({
    env: "testnet",
    mode: "live",
    binanceSymbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "t-live-disabled",
  });
  assert.equal(res.status, 403);
  assert.equal(res.body.ok, false);
  assert.equal(res.body.error, "live disabled");
});

test("ALLOW_MAINNET=false + env=mainnet returns 403", async () => {
  const res = await postExecute({
    env: "mainnet",
    mode: "test",
    binanceSymbol: "BTCUSDT",
    side: "BUY",
    orderType: "MARKET",
    notionalUSDT: 10,
    clientOrderId: "t-mainnet-disabled",
  });
  assert.equal(res.status, 403);
  assert.equal(res.body.ok, false);
  assert.equal(res.body.error, "mainnet disabled");
});

test("pickBinanceCreds hard-fails unknown env", () => {
  const src = readFileSync("services/binance-executor/server.js", "utf8");
  assert.match(src, /return \{ error: "invalid env", base: "N\/A", key: "", secret: "" \};/);
});

test("binance endpoint selection is strict to mode===live", () => {
  const src = readFileSync("services/binance-executor/server.js", "utf8");
  assert.match(src, /const endpoint = mode === "live" \? "\/api\/v3\/order" : "\/api\/v3\/order\/test";/);
});
