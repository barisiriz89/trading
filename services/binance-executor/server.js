// server.js (ESM)
// Requires: package.json -> { "type": "module" }, Node >= 18

import express from "express";
import crypto from "crypto";
import { Firestore } from "@google-cloud/firestore";

const app = express();
app.disable("strict routing");
function healthz(_req, res) {
  res.status(200).json({ ok: true });
}
app.get("/healthz", healthz);
app.get("/healthz/", healthz);
app.head("/healthz", healthz);
app.head("/healthz/", healthz);
app.get("/health", healthz);

app.set("trust proxy", true);
app.use(express.json({ limit: "256kb" }));

function parseAllowlist(raw) {
  return String(raw || "").split(",").map((x) => x.trim().toUpperCase()).filter(Boolean);
}

function isAllowed(sym, allowlist) {
  const s = String(sym || "").toUpperCase();
  return Array.isArray(allowlist) && allowlist.includes(s);
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Env â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const ENV = {
  PORT: parseInt(process.env.PORT || "8080", 10),

  // Auth
  EXECUTOR_SECRET: process.env.EXECUTOR_SECRET || "",

  // Safety toggles
  ALLOW_MAINNET: (process.env.ALLOW_MAINNET || "false").toLowerCase() === "true",
  ALLOW_LIVE: (process.env.ALLOW_LIVE || "false").toLowerCase() === "true",

  // Strategy routing: core (default) | auto | scalp
  DEFAULT_STRATEGY: (process.env.DEFAULT_STRATEGY || "core").toLowerCase(),

  // Symbols
  SYMBOL_ALLOWLIST: parseAllowlist(process.env.SYMBOL_ALLOWLIST || "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT"),

  // Cooldowns
  COOLDOWN_MS: parseInt(process.env.COOLDOWN_MS || "15000", 10),

  // Notional controls
  MIN_NOTIONAL_USDT: parseFloat(process.env.MIN_NOTIONAL_USDT || "50"),
  MAX_NOTIONAL_USDT: parseFloat(process.env.MAX_NOTIONAL_USDT || "250"),

  // Default order sizes (if payload notional missing/invalid)
  BASE_ORDER_USDT: parseFloat(process.env.BASE_ORDER_USDT || "250"),   // core open / core dca
  SCALP_ORDER_USDT: parseFloat(process.env.SCALP_ORDER_USDT || "250"), // scalp new / scalp dca

  // Max total exposure across core + open scalp cycles (approx = sum(costUSDT))
  MAX_TOTAL_EXPOSURE_USDT: parseFloat(process.env.MAX_TOTAL_EXPOSURE_USDT || "2000"),

  // Fees / Profit lock
  // NOTE: We will compute fees from Binance fills when possible.
  // feeRate remains used for:
  //  - breakeven target price estimate
  //  - fallback fee estimate when fill commission conversion isn't possible
  TAKER_FEE_BPS: parseFloat(process.env.TAKER_FEE_BPS || "10"), // 10 = 0.10%

  // PROFIT TARGETS ARE NET (after all buy fees + sell fee)
  PROFIT_LOCK_NET_USDT: parseFloat(process.env.PROFIT_LOCK_NET_USDT || "1.0"),
  PROFIT_LOCK_ARM_USDT: parseFloat(process.env.PROFIT_LOCK_ARM_USDT || "1.4"),

  // Trailing (only used after armed)
  TRAIL_PCT: parseFloat(process.env.TRAIL_PCT || "0.25"), // %

  // DCA
  DCA_MAX: parseInt(process.env.DCA_MAX || "5", 10),
  DCA_STEP_PCT: parseFloat(process.env.DCA_STEP_PCT || "0.5"),
  DCA_COOLDOWN_MS: parseInt(process.env.DCA_COOLDOWN_MS || "300000", 10),

  // Scalp
  ALLOW_SCALP: (process.env.ALLOW_SCALP || "true").toLowerCase() === "true",
  SCALP_MAX_OPEN: parseInt(process.env.SCALP_MAX_OPEN || "2", 10),
  ALLOW_REBUY: (process.env.ALLOW_REBUY ?? "true").toString().toLowerCase() === "true",

  // Exit behavior:
  // - "market": TICK/SELL triggers may market sell if net>=target (default)
  // - "market_on_sell_signal": only SELL signal can market sell; TICK never sells
  TP_MODE: (process.env.TP_MODE || "market").toLowerCase(), // market | market_on_sell_signal

  // Circuit breaker (pause-only)
  STOP_DROP_PCT: parseFloat(process.env.STOP_DROP_PCT || "3.0"),
  STOP_WINDOW_MS: parseInt(process.env.STOP_WINDOW_MS || "120000", 10),
  PAUSE_MS: parseInt(process.env.PAUSE_MS || "600000", 10),

  // Firestore
  FIRESTORE_COLLECTION: process.env.FIRESTORE_COLLECTION || "positions",
  FIRESTORE_IDEMPOTENCY_COLLECTION: process.env.FIRESTORE_IDEMPOTENCY_COLLECTION || "executor_idempotency",
  IDEMPOTENCY_TTL_MS: parseInt(process.env.IDEMPOTENCY_TTL_MS || "86400000", 10), // 24h
  // Binance endpoints
  BINANCE_MAINNET_BASE: process.env.BINANCE_MAINNET_BASE || "https://api-gcp.binance.com",
  BINANCE_TESTNET_BASE: process.env.BINANCE_TESTNET_BASE || "https://testnet.binance.vision",

  BINANCE_MAINNET_API_KEY: process.env.BINANCE_MAINNET_API_KEY || "",
  BINANCE_MAINNET_API_SECRET: process.env.BINANCE_MAINNET_API_SECRET || "",
  BINANCE_TESTNET_API_KEY: process.env.BINANCE_TESTNET_API_KEY || "",
  BINANCE_TESTNET_API_SECRET: process.env.BINANCE_TESTNET_API_SECRET || "",

  // qty rounding
  QTY_DECIMALS: parseInt(process.env.QTY_DECIMALS || "6", 10),

  // anti-spam / perf
  PRICE_CACHE_MS: parseInt(process.env.PRICE_CACHE_MS || "2000", 10),
  FETCH_TIMEOUT_MS: parseInt(process.env.FETCH_TIMEOUT_MS || "8000", 10),

  // Profile toggles
  CANARY: (process.env.CANARY || "false").toLowerCase() === "true",
  STRATEGY_PROFILE: (process.env.STRATEGY_PROFILE || "").toLowerCase(),
};
const USE_MEMORY_BACKEND = (process.env.EXECUTOR_STATE_BACKEND || "").toLowerCase() === "memory";

const feeRate = ENV.TAKER_FEE_BPS / 10000;

if (ENV.CANARY || ENV.STRATEGY_PROFILE === "canary") {
  ENV.SCALP_MAX_OPEN = 1;
  ENV.DCA_MAX = 0;
  ENV.MAX_TOTAL_EXPOSURE_USDT = 25;
  ENV.MAX_NOTIONAL_USDT = 15;
  console.log("PROFILE OVERRIDE:", JSON.stringify({ profile: "canary", SCALP_MAX_OPEN: ENV.SCALP_MAX_OPEN, DCA_MAX: ENV.DCA_MAX, MAX_TOTAL_EXPOSURE_USDT: ENV.MAX_TOTAL_EXPOSURE_USDT, MAX_NOTIONAL_USDT: ENV.MAX_NOTIONAL_USDT }));
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function nowMs() {
  return Date.now();
}

function json(res, status, obj) {
  const payload = (obj && typeof obj === "object") ? obj : {};
  const idem = res?.locals?.idempotency;
  if (idem && idem.claimed && !idem.finalized) {
    idem.finalized = true;
    const summary = {
      ok: Boolean(payload.ok),
      reason: payload.reason || payload.error || payload.skipped || null,
      httpStatus: status,
    };
    const state = payload.ok ? "SUCCEEDED" : "FAILED";
    void finalizeIdempotency(idem.key, state, summary).catch((err) => {
      console.error("idempotency finalize failed:", err);
    });
  }
  const ctx = res?.locals?.executeLogContext;
  if (ctx) {
    const execSide = typeof payload.side === "string" ? String(payload.side).toUpperCase() : (ctx.reqSide || "");
    console.log("EXECUTE OUT:", JSON.stringify({ rid: ctx.rid || payload.rid || "", clientOrderId: ctx.clientOrderId || "", reqSide: ctx.reqSide || "", execSide, strategy: ctx.strategy || payload.strategy || "", env: ctx.env || payload.env || "", symbol: ctx.symbol || payload.symbol || "", mode: ctx.mode || payload.mode || "", binanceStatus: payload.binanceStatus ?? null, endpoint: payload.endpoint ?? null, status, ok: Boolean(payload.ok) }));
  }
  res.status(status).setHeader("Content-Type", "application/json");
  res.send(JSON.stringify(payload));
}

function rid() {
  return crypto.randomBytes(6).toString("hex");
}

function safeEqual(a, b) {
  if (typeof a !== "string" || typeof b !== "string") return false;
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function readBearer(req) {
  const h = req.headers["authorization"] || "";
  if (!h.startsWith("Bearer ")) return "";
  return h.slice(7).trim();
}

function isAuthed(req) {
  const headerToken = readBearer(req);
  const bodyToken = (req.body && typeof req.body.secret === "string") ? req.body.secret.trim() : "";
  const token = headerToken || bodyToken;
  return token && ENV.EXECUTOR_SECRET && safeEqual(token, ENV.EXECUTOR_SECRET);
}

function clampNotional(n, fallback) {
  const x = Number.isFinite(n) ? n : fallback;
  return Math.max(ENV.MIN_NOTIONAL_USDT, Math.min(ENV.MAX_NOTIONAL_USDT, x));
}

function isAllowedSymbol(sym) {
  return isAllowed(sym, ENV.SYMBOL_ALLOWLIST);
}

function assertEnvMode(env, mode) {
  if (!["mainnet", "testnet"].includes(env)) return { status: 400, error: "invalid env" };
  if (!["test", "live"].includes(mode)) return { status: 400, error: "invalid mode" };
  if (env === "mainnet" && !ENV.ALLOW_MAINNET) return { status: 403, error: "mainnet disabled" };
  if (mode === "live" && !ENV.ALLOW_LIVE) return { status: 403, error: "live disabled" };
  return null;
}

function roundQty(qty) {
  const p = Math.pow(10, ENV.QTY_DECIMALS);
  return Math.floor(qty * p) / p;
}

function docIdFor(env, symbol) {
  return `${env}:${symbol}`;
}

function scrubBodyForLog(body) {
  if (!body || typeof body !== "object") return body;
  const c = { ...body };
  if (c.secret) c.secret = "***";
  return c;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = ENV.FETCH_TIMEOUT_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const r = await fetch(url, { ...options, signal: controller.signal });
    return r;
  } finally {
    clearTimeout(t);
  }
}

async function safeJson(r) {
  try {
    return await r.json();
  } catch {
    return await r.text();
  }
}

function normalizeStrategy(s) {
  const raw = String(s || ENV.DEFAULT_STRATEGY || "core").toLowerCase();
  return (["core", "auto", "scalp"].includes(raw) ? raw : "core");
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Firestore state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const db = USE_MEMORY_BACKEND ? null : new Firestore();
const memStateDocs = new Map();
const memIdemDocs = new Map();

/**
 * State shape:
 * {
 *   env, symbol,
 *   core: { qty, costUSDT, avgPrice, feesUSDT, armed, peakPrice, floorPrice, dcaCount, lastDcaMs },
 *   cycles: [ { id, status, qty, costUSDT, avgPrice, feesUSDT, armed, peakPrice, floorPrice, dcaCount, lastDcaMs } ],
 *   lastActionMs, lastSeenPrice, lastSeenPriceMs, pausedUntilMs
 * }
 */
function emptyLeg() {
  return {
    qty: 0,
    costUSDT: 0,
    avgPrice: 0,
    feesUSDT: 0,   // cumulative buy+sell fees in USDT-equivalent (best-effort)
    armed: false,
    peakPrice: 0,
    floorPrice: 0,
    dcaCount: 0,
    lastDcaMs: 0,
  };
}

function migrateOldState(s) {
  if (s && typeof s === "object" && !("core" in s)) {
    const core = emptyLeg();
    core.qty = Number(s.qty || 0);
    core.costUSDT = Number(s.costUSDT || 0);
    core.avgPrice = Number(s.avgPrice || 0);
    core.feesUSDT = Number(s.feesUSDT || 0);
    core.dcaCount = Number(s.dcaCount || 0);
    core.lastDcaMs = Number(s.lastDcaMs || 0);

    return {
      env: s.env,
      symbol: s.symbol,
      core,
      cycles: [],
      lastActionMs: Number(s.lastActionMs || 0),
      lastSeenPrice: Number(s.lastSeenPrice || 0),
      lastSeenPriceMs: Number(s.lastSeenPriceMs || 0),
      pausedUntilMs: Number(s.pausedUntilMs || 0),
      rev: Number(s.rev || 0),
    };
  }
  const src = s || {};
  return {
    ...src,
    rev: Number(src.rev || 0),
  };
}

async function loadState(env, symbol) {
  if (USE_MEMORY_BACKEND) {
    const raw = memStateDocs.get(docIdFor(env, symbol));
    if (!raw) {
      return {
        env,
        symbol,
        core: emptyLeg(),
        cycles: [],
        lastActionMs: 0,
        lastSeenPrice: 0,
        lastSeenPriceMs: 0,
        pausedUntilMs: 0,
        rev: 0,
      };
    }
    return migrateOldState(raw);
  }
  const ref = db.collection(ENV.FIRESTORE_COLLECTION).doc(docIdFor(env, symbol));
  const snap = await ref.get();
  if (!snap.exists) {
    return {
      env,
      symbol,
      core: emptyLeg(),
      cycles: [],
      lastActionMs: 0,
      lastSeenPrice: 0,
      lastSeenPriceMs: 0,
      pausedUntilMs: 0,
      rev: 0,
    };
  }
  return migrateOldState(snap.data());
}

async function saveState(env, symbol, state) {
  const expectedRev = Number(state?.rev || 0);
  const id = docIdFor(env, symbol);
  if (USE_MEMORY_BACKEND) {
    const current = memStateDocs.get(id);
    const currentRev = Number(current?.rev || 0);
    if (currentRev !== expectedRev) throw new Error("state revision conflict");
    const next = {
      ...state,
      env,
      symbol,
      rev: expectedRev + 1,
      updatedAtMs: nowMs(),
    };
    memStateDocs.set(id, next);
    state.rev = next.rev;
    return;
  }
  const ref = db.collection(ENV.FIRESTORE_COLLECTION).doc(docIdFor(env, symbol));
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    const currentRev = snap.exists ? Number((snap.data() || {}).rev || 0) : 0;
    if (currentRev !== expectedRev) throw new Error("state revision conflict");
    tx.set(ref, {
      ...state,
      env,
      symbol,
      rev: expectedRev + 1,
      updatedAtMs: nowMs(),
    });
  });
  state.rev = expectedRev + 1;
}



function makeIdempotencyKey({ env, mode, symbol, side, orderType, notionalUSDT, clientOrderId, ts }) {
  if (clientOrderId && String(clientOrderId).trim()) return `cid:${env}:${symbol}:${String(clientOrderId).trim()}`;
  const base = JSON.stringify({ env, mode, symbol, side, orderType, notionalUSDT, ts: ts || Math.floor(nowMs() / 60000) });
  return `hash:${crypto.createHash("sha256").update(base).digest("hex")}`;
}

function logIdempotencyStatus({ env, symbol, clientOrderId, key, status, phase, rid, reason }) {
  console.log(
    "IDEMPOTENCY_STATUS:",
    JSON.stringify({
      env: env || "",
      symbol: symbol || "",
      clientOrderId: clientOrderId || "",
      key: key || "",
      status: status || "",
      phase: phase || "",
      rid: rid || "",
      reason: reason || "",
      ts: nowMs(),
    })
  );
}

async function claimIdempotency(params) {
  const key = makeIdempotencyKey(params);
  const createdAtMs = nowMs();
  if (USE_MEMORY_BACKEND) {
    const existing = memIdemDocs.get(key);
    if (!existing) {
      memIdemDocs.set(key, {
        key,
        status: "IN_PROGRESS",
        summary: null,
        createdAtMs,
        updatedAtMs: createdAtMs,
        expiresAtMs: createdAtMs + ENV.IDEMPOTENCY_TTL_MS,
        env: params.env,
        mode: params.mode,
        symbol: params.symbol,
        side: params.side,
        clientOrderId: params.clientOrderId || null,
        rid: params.rid,
      });
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:create", rid: params.rid });
      return { key, status: "CLAIMED" };
    }
    if (existing.status === "SUCCEEDED") {
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "SUCCEEDED", phase: "claim:duplicate", rid: params.rid });
      return { key, status: "SUCCEEDED", summary: existing.summary || null };
    }
    if (existing.status === "IN_PROGRESS") {
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:duplicate", rid: params.rid });
      return { key, status: "IN_PROGRESS" };
    }
    memIdemDocs.set(key, {
      ...existing,
      status: "IN_PROGRESS",
      summary: null,
      updatedAtMs: createdAtMs,
      rid: params.rid,
    });
    logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:reopen", rid: params.rid });
    return { key, status: "CLAIMED" };
  }
  const ref = db.collection(ENV.FIRESTORE_IDEMPOTENCY_COLLECTION).doc(key);
  let out = { key, status: "IN_PROGRESS" };
  await db.runTransaction(async (tx) => {
    const snap = await tx.get(ref);
    if (!snap.exists) {
      tx.create(ref, {
        key,
        status: "IN_PROGRESS",
        summary: null,
        createdAtMs,
        updatedAtMs: createdAtMs,
        expiresAtMs: createdAtMs + ENV.IDEMPOTENCY_TTL_MS,
        env: params.env,
        mode: params.mode,
        symbol: params.symbol,
        side: params.side,
        clientOrderId: params.clientOrderId || null,
        rid: params.rid,
      });
      out = { key, status: "CLAIMED" };
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:create", rid: params.rid });
      return;
    }
    const data = snap.data() || {};
    if (data.status === "SUCCEEDED") {
      out = { key, status: "SUCCEEDED", summary: data.summary || null };
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "SUCCEEDED", phase: "claim:duplicate", rid: params.rid });
      return;
    }
    if (data.status === "IN_PROGRESS") {
      out = { key, status: "IN_PROGRESS" };
      logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:duplicate", rid: params.rid });
      return;
    }
    tx.update(ref, { status: "IN_PROGRESS", summary: null, updatedAtMs: createdAtMs, rid: params.rid });
    out = { key, status: "CLAIMED" };
    logIdempotencyStatus({ env: params.env, symbol: params.symbol, clientOrderId: params.clientOrderId, key, status: "IN_PROGRESS", phase: "claim:reopen", rid: params.rid });
  });
  return out;
}

async function finalizeIdempotency(key, status, summary) {
  if (!key) return;
  const parts = String(key).split(":");
  const env = parts.length > 1 ? parts[1] : "";
  const symbol = parts.length > 2 ? parts[2] : "";
  const clientOrderId = parts.length > 3 ? parts.slice(3).join(":") : "";
  if (USE_MEMORY_BACKEND) {
    const existing = memIdemDocs.get(key);
    if (!existing) return;
    memIdemDocs.set(key, {
      ...existing,
      status,
      summary: summary || null,
      updatedAtMs: nowMs(),
    });
    logIdempotencyStatus({ env: existing.env || env, symbol: existing.symbol || symbol, clientOrderId: existing.clientOrderId || clientOrderId, key, status, phase: "finalize", rid: existing.rid || "", reason: summary?.reason || "" });
    return;
  }
  const ref = db.collection(ENV.FIRESTORE_IDEMPOTENCY_COLLECTION).doc(key);
  await ref.set({ status, summary: summary || null, updatedAtMs: nowMs() }, { merge: true });
  logIdempotencyStatus({ env, symbol, clientOrderId, key, status, phase: "finalize", reason: summary?.reason || "" });
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Binance helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function pickBinanceCreds(env) {
  if (env === "testnet") {
    return {
      base: ENV.BINANCE_TESTNET_BASE,
      key: ENV.BINANCE_TESTNET_API_KEY,
      secret: ENV.BINANCE_TESTNET_API_SECRET,
    };
  }
  if (env === "mainnet") {
    return {
      base: ENV.BINANCE_MAINNET_BASE,
      key: ENV.BINANCE_MAINNET_API_KEY,
      secret: ENV.BINANCE_MAINNET_API_SECRET,
    };
  }
  return { error: "invalid env", base: "N/A", key: "", secret: "" };
}

function signHmac(secret, queryString) {
  return crypto.createHmac("sha256", secret).update(queryString).digest("hex");
}

async function binancePublicPrice(base, symbol) {
  const url = `${base}/api/v3/ticker/price?symbol=${encodeURIComponent(symbol)}`;
  const r = await fetchWithTimeout(url, { method: "GET" });
  if (!r.ok) return { ok: false, status: r.status, body: await safeJson(r) };
  const j = await r.json();
  return { ok: true, price: parseFloat(j.price) };
}

async function binanceOrder({ env, mode, symbol, side, notionalUSDT, quantity, clientOrderId }) {
  const { base, key, secret, error } = pickBinanceCreds(env);
  if (error) {
    return { ok: false, status: 400, endpoint: "N/A", base, body: { code: -2, msg: error } };
  }

  if (!key || !secret) {
    return { ok: false, status: 401, endpoint: "N/A", base, body: { code: -1, msg: "missing api key/secret" } };
  }

  const ts = nowMs();
  const params = new URLSearchParams();
  params.set("symbol", symbol);
  params.set("side", side);
  params.set("type", "MARKET");
  params.set("timestamp", String(ts));

  if (clientOrderId) params.set("newClientOrderId", clientOrderId);

  if (side === "BUY") {
    params.set("quoteOrderQty", String(notionalUSDT));
  } else {
    params.set("quantity", String(quantity));
  }

  const queryString = params.toString();
  const signature = signHmac(secret, queryString);
  const endpoint = mode === "live" ? "/api/v3/order" : "/api/v3/order/test";
  const url = `${base}${endpoint}?${queryString}&signature=${signature}`;

  const r = await fetchWithTimeout(url, {
    method: "POST",
    headers: { "X-MBX-APIKEY": key },
  });

  const body = await safeJson(r);
  return { ok: r.ok, status: r.status, endpoint, base, body };
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Fill parsing (REAL qty/cost/fees) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function parseFillSummary(orderBody, fallbackPrice) {
  // Works for both BUY and SELL MARKET orders (mainnet response)
  // Returns: { executedQty, quoteQty, avgFillPrice, feeUSDT, feeBaseQty }
  // feeUSDT best-effort: if commissionAsset is USDT -> add; if BTC -> commission * avgPrice
  // otherwise -> 0 (we will fallback estimate outside if needed).
  const executedQty = parseFloat(orderBody?.executedQty || "0") || 0;
  const quoteQty = parseFloat(orderBody?.cummulativeQuoteQty || "0") || 0;

  let avgFillPrice = 0;
  const fills = Array.isArray(orderBody?.fills) ? orderBody.fills : null;

  if (fills && fills.length) {
    // weighted avg by qty
    let qtySum = 0;
    let pxQtySum = 0;
    let feeUSDT = 0;
    let feeBaseQty = 0;

    for (const f of fills) {
      const px = parseFloat(f?.price || "0") || 0;
      const q = parseFloat(f?.qty || "0") || 0;
      qtySum += q;
      pxQtySum += px * q;

      const commission = parseFloat(f?.commission || "0") || 0;
      const asset = String(f?.commissionAsset || "").toUpperCase();

      if (commission > 0) {
        if (asset === "USDT") feeUSDT += commission;
        else if (asset === "BTC") feeBaseQty += commission; // convert later using avg price
        else {
          // unknown fee asset (e.g., BNB) -> ignore here; handled by fallback estimates if needed
        }
      }
    }

    avgFillPrice = qtySum > 0 ? (pxQtySum / qtySum) : 0;
    if (!avgFillPrice && fallbackPrice) avgFillPrice = fallbackPrice;

    // convert base fee to USDT using avg fill price
    feeUSDT += feeBaseQty * (avgFillPrice || fallbackPrice || 0);

    return { executedQty, quoteQty, avgFillPrice, feeUSDT, feeBaseQty };
  }

  // no fills array -> fallback (some modes or endpoints)
  avgFillPrice = (quoteQty > 0 && executedQty > 0) ? (quoteQty / executedQty) : (fallbackPrice || 0);
  return { executedQty, quoteQty, avgFillPrice, feeUSDT: 0, feeBaseQty: 0 };
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Strategy helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function totalExposureUSDT(state) {
  const core = state.core?.costUSDT || 0;
  const cycles = (state.cycles || [])
    .filter((c) => c.status === "OPEN")
    .reduce((a, c) => a + (c.costUSDT || 0), 0);
  return core + cycles;
}

function isPaused(state) {
  const t = nowMs();
  return Number(state.pausedUntilMs || 0) > t;
}

function updateCircuitBreaker(state, price) {
  const t = nowMs();
  const last = Number(state.lastSeenPrice || 0);
  const lastMs = Number(state.lastSeenPriceMs || 0);

  state.lastSeenPrice = price;
  state.lastSeenPriceMs = t;

  if (!last || !lastMs) return;

  const within = t - lastMs <= ENV.STOP_WINDOW_MS;
  if (!within) return;

  const dropPct = ((last - price) / last) * 100;
  if (dropPct >= ENV.STOP_DROP_PCT) {
    state.pausedUntilMs = t + ENV.PAUSE_MS;
  }
}

function shouldDca(leg, currentPrice) {
  if (!leg || !leg.qty || leg.qty <= 0) return false;
  if (leg.dcaCount >= ENV.DCA_MAX) return false;

  const t = nowMs();
  if (leg.lastDcaMs && t - leg.lastDcaMs < ENV.DCA_COOLDOWN_MS) return false;

  const threshold = leg.avgPrice * (1 - ENV.DCA_STEP_PCT / 100);
  return currentPrice <= threshold;
}

/**
 * NET profit for a leg at currentPrice (estimate).
 * This is used for trailing/arming decisions while holding.
 *
 * IMPORTANT: This net includes:
 * - leg.buy fees already stored in leg.feesUSDT (and sell fees when realized)
 * - estimated sell fee (feeRate) at current price
 */
function netProfitUSDT(leg, currentPrice) {
  if (!leg || !leg.qty || leg.qty <= 0) return 0;

  const gross = (currentPrice - leg.avgPrice) * leg.qty;

  const sellNotional = currentPrice * leg.qty;
  const sellFeeEst = sellNotional * feeRate;

  const net = gross - (leg.feesUSDT || 0) - sellFeeEst;
  return net;
}

function breakevenPlusPrice(leg) {
  // price that yields net >= PROFIT_LOCK_NET_USDT (estimate)
  if (!leg || !leg.qty || leg.qty <= 0) return Infinity;

  const target = ENV.PROFIT_LOCK_NET_USDT;
  const numerator = leg.avgPrice * leg.qty + (leg.feesUSDT || 0) + target;
  const denom = leg.qty * (1 - feeRate);
  if (denom <= 0) return Infinity;
  return numerator / denom;
}

function updateProfitLock(leg, currentPrice) {
  if (!leg || !leg.qty || leg.qty <= 0) return;

  const net = netProfitUSDT(leg, currentPrice);

  if (!leg.armed && net >= ENV.PROFIT_LOCK_ARM_USDT) {
    leg.armed = true;
    leg.peakPrice = currentPrice;
  }

  if (!leg.armed) return;

  if (!leg.peakPrice || currentPrice > leg.peakPrice) {
    leg.peakPrice = currentPrice;
  }

  const trailFloor = leg.peakPrice * (1 - ENV.TRAIL_PCT / 100);
  const bep = breakevenPlusPrice(leg);
  leg.floorPrice = Math.max(trailFloor, bep);
}

function shouldExitByProfitLock(leg, currentPrice) {
  if (!leg || !leg.qty || leg.qty <= 0) return false;
  if (!leg.armed) return false;
  if (!leg.floorPrice || leg.floorPrice <= 0) return false;

  const net = netProfitUSDT(leg, currentPrice);
  if (net < ENV.PROFIT_LOCK_NET_USDT) return false;

  return currentPrice <= leg.floorPrice;
}

function pickBestExitCycle(state, price) {
  const opens = (state.cycles || []).filter((c) => c.status === "OPEN" && c.qty > 0);
  if (!opens.length) return null;

  let best = null;
  let bestNet = -Infinity;

  for (const c of opens) {
    const n = netProfitUSDT(c, price);
    if (n > bestNet) {
      bestNet = n;
      best = c;
    }
  }
  return best;
}

function newCycleId() {
  return `c_${nowMs()}_${crypto.randomBytes(3).toString("hex")}`;
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Request ID middleware â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
app.use((req, _res, next) => {
  req.rid = req.rid || rid();
  next();
});

app.get("/", (req, res) => json(res, 200, { ok: true, service: "binance-executor", ts: nowMs(), rid: req.rid }));

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Routes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
app.post("/execute", async (req, res) => {
  const requestId = req.rid || rid();

  try {
    if (!isAuthed(req)) return json(res, 401, { ok: false, error: "unauthorized", rid: requestId });

    const {
      env: rawEnv = "mainnet",
      mode: rawMode = "test",
      binanceSymbol,
      side,
      orderType = "MARKET",
      notionalUSDT,
      clientOrderId,
      strategy,
          ts,
    } = req.body || {};

    const STRATEGY = normalizeStrategy(strategy);
    const env = String(rawEnv || "").toLowerCase();
    const mode = String(rawMode || "").toLowerCase();

    // lightweight structured log (secret masked)
    console.log(
      "EXECUTE IN:",
      JSON.stringify({
        rid: requestId,
        path: req.path,
        ip: req.ip,
        ua: req.headers["user-agent"],
        contentType: req.headers["content-type"],
        strategy: STRATEGY,
        body: scrubBodyForLog(req.body),
      })
    );

    if (!binanceSymbol || typeof binanceSymbol !== "string") {
      return json(res, 400, { ok: false, error: "missing binanceSymbol", rid: requestId });
    }
    if (!isAllowedSymbol(binanceSymbol)) {
      return json(res, 400, { ok: false, error: "symbol not allowed", allowlist: ENV.SYMBOL_ALLOWLIST, rid: requestId });
    }

    const modeErr = assertEnvMode(env, mode);
    if (modeErr) return json(res, modeErr.status, { ok: false, error: modeErr.error, rid: requestId });

    if (String(orderType).toUpperCase() !== "MARKET") {
      return json(res, 400, { ok: false, error: "only MARKET supported", rid: requestId });
    }

    const actionSide = String(side || "").toUpperCase();
    if (!["BUY", "SELL", "TICK"].includes(actionSide)) {
      return json(res, 400, { ok: false, error: "invalid side", rid: requestId });
    }
    

    res.locals.executeLogContext = { rid: requestId, clientOrderId: String(clientOrderId || ''), reqSide: actionSide, strategy: STRATEGY, env, symbol: String(binanceSymbol || ''), mode };

    const idem = await claimIdempotency({
      env,
      mode,
      symbol: binanceSymbol,
      side: actionSide,
      orderType: String(orderType).toUpperCase(),
      notionalUSDT: Number(notionalUSDT || 0),
      clientOrderId,
      ts,
      rid: requestId,
    });
    if (idem.status === "IN_PROGRESS") {
      return json(res, 409, { ok: false, reason: "IN_PROGRESS", dedupeKey: idem.key, rid: requestId });
    }
    if (idem.status === "SUCCEEDED") {
      return json(res, 200, {
        ok: true,
        strategy: STRATEGY,
        env,
        mode,
        symbol: binanceSymbol,
        side: actionSide,
        skipped: "duplicate request",
        dedupeKey: idem.key,
        cached: idem.summary || null,
        rid: requestId,
      });
    }
    res.locals.idempotency = { key: idem.key, claimed: true, finalized: false };

    const t = nowMs();
    const state = await loadState(env, binanceSymbol);

    
    const persistState = async () => {
      if (mode === 'test') {
        console.log('STATE_WRITE_SKIPPED:', JSON.stringify({ rid: requestId, env, mode, symbol: binanceSymbol, strategy: STRATEGY, reason: 'mode=test' }));
        return;
      }
      await saveState(env, binanceSymbol, state);
    };

    // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ Price snapshot (with cache) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    const { base } = pickBinanceCreds(env);

    let currentPrice = 0;
    const cachedOk =
      Number(state.lastSeenPrice || 0) > 0 &&
      Number(state.lastSeenPriceMs || 0) > 0 &&
      (t - Number(state.lastSeenPriceMs || 0)) <= ENV.PRICE_CACHE_MS;

    if (cachedOk) {
      currentPrice = Number(state.lastSeenPrice);
    } else {
      const priceResp = await binancePublicPrice(base, binanceSymbol);
      if (!priceResp.ok || !Number.isFinite(priceResp.price)) {
        return json(res, 502, {
          ok: false,
          error: "price fetch failed",
          status: priceResp.status,
          body: priceResp.body,
          rid: requestId,
        });
      }
      currentPrice = priceResp.price;
    }

    // circuit breaker update (pause-only) + updates lastSeenPrice/lastSeenPriceMs
    updateCircuitBreaker(state, currentPrice);
    const paused = isPaused(state);

    // update profit locks on core + all open cycles
    updateProfitLock(state.core, currentPrice);
    for (const c of state.cycles || []) {
      if (c.status === "OPEN") updateProfitLock(c, currentPrice);
    }

    // Helper: do market sell for a leg (core/cycle) using REAL fill data
    async function marketSellLeg(legQty) {
      const qty = roundQty(legQty);
      if (qty <= 0) return { ok: false, skipped: "qty too small" };

      const order = await binanceOrder({
        env,
        mode,
        symbol: binanceSymbol,
        side: "SELL",
        quantity: qty,
        clientOrderId,
      });

      if (!order.ok) return { ok: false, order };

      const fill = parseFillSummary(order.body, currentPrice);

      // realized proceeds and fees
      const proceedsUSDT = fill.quoteQty > 0 ? fill.quoteQty : (fill.executedQty * (fill.avgFillPrice || currentPrice));
      let sellFeeUSDT = fill.feeUSDT;

      // fallback estimate if fee not captured from fills
      if (!Number.isFinite(sellFeeUSDT) || sellFeeUSDT <= 0) {
        sellFeeUSDT = proceedsUSDT * feeRate;
      }

      return {
        ok: true,
        order,
        executedQty: fill.executedQty,
        proceedsUSDT,
        sellFeeUSDT,
        avgFillPrice: fill.avgFillPrice || currentPrice,
      };
    }

    // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ TICK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (actionSide === "TICK") {
      if (ENV.TP_MODE === "market_on_sell_signal") {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: paused ? "tick: paused" : "tick: no action (tp_mode=market_on_sell_signal)",
          price: currentPrice,
          rid: requestId,
        });
      }

      // TP_MODE=market: profit-lock exits allowed on TICK
      const bestCycle = pickBestExitCycle(state, currentPrice);
      if (bestCycle && shouldExitByProfitLock(bestCycle, currentPrice)) {
        const r = await marketSellLeg(bestCycle.qty);
        if (!r.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: r.order ? "binance sell failed" : "sell skipped",
            binanceStatus: r.order?.status,
            binance: r.order?.body,
            rid: requestId,
          });
        }

        // realized net profit for this cycle
        const realizedNet = r.proceedsUSDT - bestCycle.costUSDT - (bestCycle.feesUSDT || 0) - r.sellFeeUSDT;

        // NEVER accept sell if realized net < target (safety, even if estimate said ok)
        if (realizedNet < ENV.PROFIT_LOCK_NET_USDT) {
          // We already sold on Binance â€” so we must reflect state as closed, but we can warn loudly.
          // (In practice, this should almost never happen with fill-based state and conservative trailing.)
          console.warn("WARN: realizedNet below target after SELL:", { rid: requestId, realizedNet });
        }

        // add sell fee to leg fees
        bestCycle.feesUSDT = (bestCycle.feesUSDT || 0) + r.sellFeeUSDT;

        // close cycle
        bestCycle.status = "CLOSED";
        bestCycle.qty = 0;
        bestCycle.costUSDT = 0;
        bestCycle.avgPrice = 0;
        bestCycle.armed = false;
        bestCycle.peakPrice = 0;
        bestCycle.floorPrice = 0;

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          side: "SELL",
          reason: "profit-lock (cycle)",
          price: currentPrice,
          realized: {
            proceedsUSDT: r.proceedsUSDT,
            sellFeeUSDT: r.sellFeeUSDT,
            realizedNetUSDT: realizedNet,
          },
          endpoint: r.order.endpoint,
          usedBaseUrl: r.order.base,
          binanceStatus: r.order.status,
          binance: r.order.body,
          rid: requestId,
        });
      }

      if (state.core?.qty > 0 && shouldExitByProfitLock(state.core, currentPrice)) {
        const r = await marketSellLeg(state.core.qty);
        if (!r.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: r.order ? "binance sell failed" : "sell skipped",
            binanceStatus: r.order?.status,
            binance: r.order?.body,
            rid: requestId,
          });
        }

        const realizedNet = r.proceedsUSDT - state.core.costUSDT - (state.core.feesUSDT || 0) - r.sellFeeUSDT;

        if (realizedNet < ENV.PROFIT_LOCK_NET_USDT) {
          console.warn("WARN: realizedNet below target after SELL(core):", { rid: requestId, realizedNet });
        }

        // core fees add sell fee, then reset core
        state.core.feesUSDT = (state.core.feesUSDT || 0) + r.sellFeeUSDT;
        state.core = emptyLeg();

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          side: "SELL",
          reason: "profit-lock (core)",
          price: currentPrice,
          realized: {
            proceedsUSDT: r.proceedsUSDT,
            sellFeeUSDT: r.sellFeeUSDT,
            realizedNetUSDT: realizedNet,
          },
          endpoint: r.order.endpoint,
          usedBaseUrl: r.order.base,
          binanceStatus: r.order.status,
          binance: r.order.body,
          rid: requestId,
        });
      }

      await persistState();
      return json(res, 200, {
        ok: true,
        strategy: STRATEGY,
        skipped: paused ? "tick: paused" : "tick: no action",
        price: currentPrice,
        rid: requestId
      });
    }

    // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ SELL signal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (actionSide === "SELL") {
      // Hard rule: NEVER sell unless estimated net >= target.
      // (Realized net is computed after actual sell)
      const bestCycle = pickBestExitCycle(state, currentPrice);
      if (bestCycle && netProfitUSDT(bestCycle, currentPrice) >= ENV.PROFIT_LOCK_NET_USDT) {
        const r = await marketSellLeg(bestCycle.qty);
        if (!r.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: r.order ? "binance sell failed" : "sell skipped",
            binanceStatus: r.order?.status,
            binance: r.order?.body,
            rid: requestId,
          });
        }

        const realizedNet = r.proceedsUSDT - bestCycle.costUSDT - (bestCycle.feesUSDT || 0) - r.sellFeeUSDT;

        bestCycle.feesUSDT = (bestCycle.feesUSDT || 0) + r.sellFeeUSDT;
        bestCycle.status = "CLOSED";
        bestCycle.qty = 0;
        bestCycle.costUSDT = 0;
        bestCycle.avgPrice = 0;
        bestCycle.armed = false;
        bestCycle.peakPrice = 0;
        bestCycle.floorPrice = 0;

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          side: "SELL",
          reason: "sell-signal (cycle) net>=target",
          price: currentPrice,
          realized: {
            proceedsUSDT: r.proceedsUSDT,
            sellFeeUSDT: r.sellFeeUSDT,
            realizedNetUSDT: realizedNet,
          },
          endpoint: r.order.endpoint,
          usedBaseUrl: r.order.base,
          binanceStatus: r.order.status,
          binance: r.order.body,
          rid: requestId,
        });
      }

      if (state.core?.qty > 0 && netProfitUSDT(state.core, currentPrice) >= ENV.PROFIT_LOCK_NET_USDT) {
        const r = await marketSellLeg(state.core.qty);
        if (!r.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: r.order ? "binance sell failed" : "sell skipped",
            binanceStatus: r.order?.status,
            binance: r.order?.body,
            rid: requestId,
          });
        }

        const realizedNet = r.proceedsUSDT - state.core.costUSDT - (state.core.feesUSDT || 0) - r.sellFeeUSDT;

        state.core.feesUSDT = (state.core.feesUSDT || 0) + r.sellFeeUSDT;
        state.core = emptyLeg();

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          side: "SELL",
          reason: "sell-signal (core) net>=target",
          price: currentPrice,
          realized: {
            proceedsUSDT: r.proceedsUSDT,
            sellFeeUSDT: r.sellFeeUSDT,
            realizedNetUSDT: realizedNet,
          },
          endpoint: r.order.endpoint,
          usedBaseUrl: r.order.base,
          binanceStatus: r.order.status,
          binance: r.order.body,
          rid: requestId,
        });
      }

      await persistState();
      return json(res, 200, {
        ok: true,
        strategy: STRATEGY,
        skipped: paused ? "sell ignored (paused)" : "sell ignored (net<target or no position)",
        price: currentPrice,
        netTarget: ENV.PROFIT_LOCK_NET_USDT,
        rid: requestId,
      });
    }

    // â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ BUY signal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if (actionSide === "BUY") {
      // Cooldown ONLY for BUY
      if (state.lastActionMs && t - state.lastActionMs < ENV.COOLDOWN_MS) {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "cooldown",
          waitMs: ENV.COOLDOWN_MS - (t - state.lastActionMs),
          rid: requestId,
        });
      }

      if (paused) {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "paused by circuit breaker",
          pausedUntilMs: state.pausedUntilMs,
          price: currentPrice,
          rid: requestId,
        });
      }

      const exposure = totalExposureUSDT(state);
      const baseOrder = clampNotional(notionalUSDT, ENV.BASE_ORDER_USDT);
      const coreHas = state.core?.qty > 0;

      // 1) No core -> open core
      if (!coreHas) {
        const desiredNotional = clampNotional(baseOrder, ENV.BASE_ORDER_USDT);

        if (exposure + desiredNotional > ENV.MAX_TOTAL_EXPOSURE_USDT) {
          await persistState();
          return json(res, 200, {
            ok: true,
            strategy: STRATEGY,
            skipped: "max total exposure reached",
            exposureUSDT: exposure,
            maxTotalUSDT: ENV.MAX_TOTAL_EXPOSURE_USDT,
            rid: requestId,
          });
        }

        const order = await binanceOrder({
          env,
          mode,
          symbol: binanceSymbol,
          side: "BUY",
          notionalUSDT: desiredNotional,
          clientOrderId,
        });

        if (!order.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: "binance buy failed",
            binanceStatus: order.status,
            binance: order.body,
            rid: requestId
          });
        }

        // REAL fill-based state update
        const fill = parseFillSummary(order.body, currentPrice);
        const qtyGross = fill.executedQty || (desiredNotional / currentPrice);
        const spentUSDT = fill.quoteQty > 0 ? fill.quoteQty : desiredNotional;

        // If commission asset is BTC, it reduces base received. We already converted to feeUSDT,
        // but qty should be net of base fee for most accurate holdings.
        // We can't perfectly subtract if the fee is in BTC but not provided; fill.feeBaseQty exists if fills present.
        const qtyNet = Math.max(0, qtyGross - (fill.feeBaseQty || 0));

        // Fee in USDT best-effort; fallback estimate if missing
        let buyFeeUSDT = fill.feeUSDT;
        if (!Number.isFinite(buyFeeUSDT) || buyFeeUSDT <= 0) {
          buyFeeUSDT = spentUSDT * feeRate;
        }

        // update core
        state.core.qty += qtyNet;
        state.core.costUSDT += spentUSDT;
        state.core.feesUSDT += buyFeeUSDT;
        state.core.avgPrice = state.core.qty > 0 ? (state.core.costUSDT / state.core.qty) : 0;

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          opened: "core",
          side: "BUY",
          price: currentPrice,
          notionalUSDT: desiredNotional,
          filled: {
            executedQty: fill.executedQty,
            qtyNet,
            spentUSDT,
            buyFeeUSDT,
            avgFillPrice: fill.avgFillPrice || currentPrice,
          },
          core: {
            qty: state.core.qty,
            avgPrice: state.core.avgPrice,
            costUSDT: state.core.costUSDT,
            feesUSDT: state.core.feesUSDT,
          },
          endpoint: order.endpoint,
          usedBaseUrl: order.base,
          binanceStatus: order.status,
          binance: order.body,
          rid: requestId,
        });
      }

      // 2) core exists and DCA triggers -> core DCA
      if (shouldDca(state.core, currentPrice)) {
        const desiredNotional = clampNotional(baseOrder, ENV.BASE_ORDER_USDT);

        if (exposure + desiredNotional > ENV.MAX_TOTAL_EXPOSURE_USDT) {
          await persistState();
          return json(res, 200, {
            ok: true,
            strategy: STRATEGY,
            skipped: "max total exposure reached (core dca)",
            exposureUSDT: exposure,
            maxTotalUSDT: ENV.MAX_TOTAL_EXPOSURE_USDT,
            rid: requestId,
          });
        }

        const order = await binanceOrder({
          env,
          mode,
          symbol: binanceSymbol,
          side: "BUY",
          notionalUSDT: desiredNotional,
          clientOrderId,
        });

        if (!order.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: "binance buy failed",
            binanceStatus: order.status,
            binance: order.body,
            rid: requestId
          });
        }

        const fill = parseFillSummary(order.body, currentPrice);
        const qtyGross = fill.executedQty || (desiredNotional / currentPrice);
        const spentUSDT = fill.quoteQty > 0 ? fill.quoteQty : desiredNotional;
        const qtyNet = Math.max(0, qtyGross - (fill.feeBaseQty || 0));

        let buyFeeUSDT = fill.feeUSDT;
        if (!Number.isFinite(buyFeeUSDT) || buyFeeUSDT <= 0) {
          buyFeeUSDT = spentUSDT * feeRate;
        }

        state.core.qty += qtyNet;
        state.core.costUSDT += spentUSDT;
        state.core.feesUSDT += buyFeeUSDT;
        state.core.avgPrice = state.core.qty > 0 ? (state.core.costUSDT / state.core.qty) : 0;
        state.core.dcaCount += 1;
        state.core.lastDcaMs = t;

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          opened: "core_dca",
          side: "BUY",
          price: currentPrice,
          notionalUSDT: desiredNotional,
          filled: {
            executedQty: fill.executedQty,
            qtyNet,
            spentUSDT,
            buyFeeUSDT,
            avgFillPrice: fill.avgFillPrice || currentPrice,
          },
          core: {
            qty: state.core.qty,
            avgPrice: state.core.avgPrice,
            costUSDT: state.core.costUSDT,
            feesUSDT: state.core.feesUSDT,
            dcaCount: state.core.dcaCount,
          },
          endpoint: order.endpoint,
          usedBaseUrl: order.base,
          binanceStatus: order.status,
          binance: order.body,
          rid: requestId,
        });
      }

      // 2.5) core exists but no DCA -> if strategy core-only, do NOT scalp
      if (STRATEGY === "core") {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "core: no dca trigger (holding)",
          price: currentPrice,
          core: { qty: state.core.qty, avgPrice: state.core.avgPrice, costUSDT: state.core.costUSDT, feesUSDT: state.core.feesUSDT },
          rid: requestId,
        });
      }

      // 3) scalp path (only if strategy auto/scalp)
      if (!ENV.ALLOW_SCALP) {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "scalp disabled (ALLOW_SCALP=false)",
          price: currentPrice,
          rid: requestId,
        });
      }

      const openCycles = (state.cycles || []).filter((c) => c.status === "OPEN");

      // 3a) DCA an underwater cycle first
      const underwater = [...openCycles].reverse().find((c) => shouldDca(c, currentPrice));
      if (underwater) {
        const desiredNotional = clampNotional(notionalUSDT, ENV.SCALP_ORDER_USDT);

        if (exposure + desiredNotional > ENV.MAX_TOTAL_EXPOSURE_USDT) {
          await persistState();
          return json(res, 200, {
            ok: true,
            strategy: STRATEGY,
            skipped: "max total exposure reached (scalp dca)",
            exposureUSDT: exposure,
            maxTotalUSDT: ENV.MAX_TOTAL_EXPOSURE_USDT,
            rid: requestId,
          });
        }

        const order = await binanceOrder({
          env,
          mode,
          symbol: binanceSymbol,
          side: "BUY",
          notionalUSDT: desiredNotional,
          clientOrderId,
        });

        if (!order.ok) {
          await persistState();
          return json(res, 502, {
            ok: false,
            error: "binance buy failed",
            binanceStatus: order.status,
            binance: order.body,
            rid: requestId
          });
        }

        const fill = parseFillSummary(order.body, currentPrice);
        const qtyGross = fill.executedQty || (desiredNotional / currentPrice);
        const spentUSDT = fill.quoteQty > 0 ? fill.quoteQty : desiredNotional;
        const qtyNet = Math.max(0, qtyGross - (fill.feeBaseQty || 0));

        let buyFeeUSDT = fill.feeUSDT;
        if (!Number.isFinite(buyFeeUSDT) || buyFeeUSDT <= 0) {
          buyFeeUSDT = spentUSDT * feeRate;
        }

        underwater.qty += qtyNet;
        underwater.costUSDT += spentUSDT;
        underwater.feesUSDT += buyFeeUSDT;
        underwater.avgPrice = underwater.qty > 0 ? (underwater.costUSDT / underwater.qty) : 0;
        underwater.dcaCount += 1;
        underwater.lastDcaMs = t;

        state.lastActionMs = t;

        await persistState();

        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          env,
          mode,
          symbol: binanceSymbol,
          opened: "scalp_dca",
          cycleId: underwater.id,
          side: "BUY",
          price: currentPrice,
          notionalUSDT: desiredNotional,
          filled: { executedQty: fill.executedQty, qtyNet, spentUSDT, buyFeeUSDT, avgFillPrice: fill.avgFillPrice || currentPrice },
          cycle: {
            qty: underwater.qty,
            avgPrice: underwater.avgPrice,
            costUSDT: underwater.costUSDT,
            feesUSDT: underwater.feesUSDT,
            dcaCount: underwater.dcaCount,
          },
          endpoint: order.endpoint,
          usedBaseUrl: order.base,
          binanceStatus: order.status,
          binance: order.body,
          rid: requestId,
        });
      }

      // 3b) open new scalp cycle
      if (!ENV.ALLOW_REBUY) {
        await persistState();
        return json(res, 200, { ok: true, strategy: STRATEGY, skipped: "rebuy disabled (ALLOW_REBUY=false)", price: currentPrice, rid: requestId });
      }

      if (openCycles.length >= ENV.SCALP_MAX_OPEN) {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "scalp max open reached",
          open: openCycles.length,
          max: ENV.SCALP_MAX_OPEN,
          price: currentPrice,
          rid: requestId
        });
      }

      const desiredNotional = clampNotional(notionalUSDT, ENV.SCALP_ORDER_USDT);
      if (exposure + desiredNotional > ENV.MAX_TOTAL_EXPOSURE_USDT) {
        await persistState();
        return json(res, 200, {
          ok: true,
          strategy: STRATEGY,
          skipped: "max total exposure reached (new scalp)",
          exposureUSDT: exposure,
          maxTotalUSDT: ENV.MAX_TOTAL_EXPOSURE_USDT,
          rid: requestId
        });
      }

      const order = await binanceOrder({
        env,
        mode,
        symbol: binanceSymbol,
        side: "BUY",
        notionalUSDT: desiredNotional,
        clientOrderId,
      });

      if (!order.ok) {
        await persistState();
        return json(res, 502, {
          ok: false,
          error: "binance buy failed",
          binanceStatus: order.status,
          binance: order.body,
          rid: requestId
        });
      }

      const fill = parseFillSummary(order.body, currentPrice);
      const qtyGross = fill.executedQty || (desiredNotional / currentPrice);
      const spentUSDT = fill.quoteQty > 0 ? fill.quoteQty : desiredNotional;
      const qtyNet = Math.max(0, qtyGross - (fill.feeBaseQty || 0));

      let buyFeeUSDT = fill.feeUSDT;
      if (!Number.isFinite(buyFeeUSDT) || buyFeeUSDT <= 0) {
        buyFeeUSDT = spentUSDT * feeRate;
      }

      const cycle = { id: newCycleId(), status: "OPEN", ...emptyLeg() };
      cycle.qty = qtyNet;
      cycle.costUSDT = spentUSDT;
      cycle.feesUSDT = buyFeeUSDT;
      cycle.avgPrice = cycle.qty > 0 ? (cycle.costUSDT / cycle.qty) : 0;

      state.cycles = state.cycles || [];
      state.cycles.push(cycle);

      state.lastActionMs = t;

      await persistState();

      return json(res, 200, {
        ok: true,
        strategy: STRATEGY,
        env,
        mode,
        symbol: binanceSymbol,
        opened: "scalp_new",
        cycleId: cycle.id,
        side: "BUY",
        price: currentPrice,
        notionalUSDT: desiredNotional,
        filled: { executedQty: fill.executedQty, qtyNet, spentUSDT, buyFeeUSDT, avgFillPrice: fill.avgFillPrice || currentPrice },
        cycle: { qty: cycle.qty, avgPrice: cycle.avgPrice, costUSDT: cycle.costUSDT, feesUSDT: cycle.feesUSDT },
        endpoint: order.endpoint,
        usedBaseUrl: order.base,
        binanceStatus: order.status,
        binance: order.body,
        rid: requestId,
      });
    }

    // should never reach
    await persistState();
    return json(res, 200, { ok: true, strategy: normalizeStrategy(strategy), skipped: "no-op", rid: requestId });
  } catch (e) {
    return json(res, 500, { ok: false, error: String(e?.message || e), rid: requestId });
  }
});

function startServer(port = ENV.PORT) {
  return app.listen(port, "0.0.0.0", () => {
    console.log(`binance-executor listening on :${port}`);
  });
}

if ((process.env.EXECUTOR_DISABLE_AUTOSTART || "").toLowerCase() !== "true") {
  startServer(ENV.PORT);
}

// Safety logs (Cloud Run)
process.on("unhandledRejection", (err) => console.error("unhandledRejection:", err));
process.on("uncaughtException", (err) => console.error("uncaughtException:", err));

export const __test = {
  loadState,
  saveState,
  claimIdempotency,
  finalizeIdempotency,
  pickBinanceCreds,
  makeIdempotencyKey,
  resetMemoryStore: () => {
    memStateDocs.clear();
    memIdemDocs.clear();
  },
};


