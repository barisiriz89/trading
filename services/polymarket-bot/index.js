import 'dotenv/config';
import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  chooseBtcUpDownMarketFromEvents,
  computeQuorumDecision,
  extractUpDownTokens,
  intervalKeyFromTsMs,
  isLiveExecutionEnabled,
  parseJsonArrayLike,
  validateExecutePayload,
} from './lib/execute-core.js';
import { applySettlementOutcome } from './lib/reconcile-state.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const STATE_FILE = path.join(__dirname, '.state.json');

const ENV = {
  PORT: Number(process.env.PORT || 19082),
  POLY_MARKET_SLUG: String(process.env.POLY_MARKET_SLUG || '').trim(),
  POLY_YES_TOKEN_ID: String(process.env.POLY_YES_TOKEN_ID || '').trim(),
  POLY_NO_TOKEN_ID: String(process.env.POLY_NO_TOKEN_ID || '').trim(),
  POLY_CLOB_TOKEN_IDS: String(process.env.POLY_CLOB_TOKEN_IDS || '').trim(),
  POLY_LOOKBACK_SEC: Number(process.env.POLY_LOOKBACK_SEC || 21600),
  POLY_CLOB_HOST: String(process.env.POLY_CLOB_HOST || 'https://clob.polymarket.com').trim(),
  POLY_GAMMA_HOST: String(process.env.POLY_GAMMA_HOST || 'https://gamma-api.polymarket.com').trim(),
  POLY_KILL_SWITCH: String(process.env.POLY_KILL_SWITCH || 'false').toLowerCase() === 'true',
  POLY_DRY_RUN: String(process.env.POLY_DRY_RUN || 'true').toLowerCase() === 'true',
  POLY_COOLDOWN_BARS: Number(process.env.POLY_COOLDOWN_BARS || 2),
  POLY_MAX_TRADES_PER_HOUR: Number(process.env.POLY_MAX_TRADES_PER_HOUR || 3),
  POLY_MAX_POSITION_USD: Number(process.env.POLY_MAX_POSITION_USD || 50),
  POLY_ORDER_USD: Number(process.env.POLY_ORDER_USD || 5),
  POLY_STRATEGY_VERSION: String(process.env.POLY_STRATEGY_VERSION || 'v1.0.0').trim(),
  POLY_PRIVATE_KEY: String(process.env.POLY_PRIVATE_KEY || '').trim(),
  POLY_FUNDER_ADDRESS: String(process.env.POLY_FUNDER_ADDRESS || '').trim(),
  POLY_SIGNATURE_TYPE: Number(process.env.POLY_SIGNATURE_TYPE || NaN),
  POLY_TV_SECRET: String(process.env.POLY_TV_SECRET || '').trim(),
  POLY_GEOBLOCK_URL: String(process.env.POLY_GEOBLOCK_URL || 'https://polymarket.com/api/geoblock').trim(),
  POLY_LIVE_ENABLED: String(process.env.POLY_LIVE_ENABLED || 'false').toLowerCase() === 'true',
  POLY_LIVE_CONFIRM: String(process.env.POLY_LIVE_CONFIRM || '').trim(),
  POLY_AUTO_SIZE: String(process.env.POLY_AUTO_SIZE || 'false').toLowerCase() === 'true',
  POLY_START_NOTIONAL_USD: Number(process.env.POLY_START_NOTIONAL_USD || 1),
  POLY_SIZE_MULT: Number(process.env.POLY_SIZE_MULT || 2),
  POLY_MAX_NOTIONAL_USD: Number(process.env.POLY_MAX_NOTIONAL_USD || 16),
  POLY_FEE_RATE_BPS_RAW: String(process.env.POLY_FEE_RATE_BPS || '').trim(),
  POLY_ORDER_TYPE: String(process.env.POLY_ORDER_TYPE || 'FOK').trim().toUpperCase(),
  POLY_GTC_TTL_MS: Number(process.env.POLY_GTC_TTL_MS || 30000),
  POLY_GTC_MAX_ATTEMPTS: Number(process.env.POLY_GTC_MAX_ATTEMPTS || 3),
  POLY_GTC_POLL_MS: Number(process.env.POLY_GTC_POLL_MS || 800),
  POLY_CANCEL_ON_NEW_BUCKET: String(process.env.POLY_CANCEL_ON_NEW_BUCKET || 'true').toLowerCase() === 'true',
  POLY_FORCE_FILL: String(process.env.POLY_FORCE_FILL || 'false').toLowerCase() === 'true',
  POLY_FORCE_FILL_PRICES: String(process.env.POLY_FORCE_FILL_PRICES || '0.60,0.70,0.80,0.90,0.95,0.99').trim(),
  POLY_RECOVERY_MODE: String(process.env.POLY_RECOVERY_MODE || 'dynamic').trim().toLowerCase(),
  POLY_TARGET_PROFIT_USD: Number(process.env.POLY_TARGET_PROFIT_USD || 1),
  POLY_MIN_NOTIONAL_USD: Number(process.env.POLY_MIN_NOTIONAL_USD || 5),
  POLY_MAX_NOTIONAL_USD_HARD: Number(process.env.POLY_MAX_NOTIONAL_USD_HARD || 160),
  POLY_EXIT_ENABLED: String(process.env.POLY_EXIT_ENABLED || 'true').toLowerCase() === 'true',
  POLY_EXIT_MIN_NET_PROFIT_USD: Number(process.env.POLY_EXIT_MIN_NET_PROFIT_USD || 0.05),
  POLY_EXIT_MAX_ATTEMPTS_PER_EXECUTE: Number(process.env.POLY_EXIT_MAX_ATTEMPTS_PER_EXECUTE || 1),
  POLY_EXIT_SLIPPAGE_BPS: Number(process.env.POLY_EXIT_SLIPPAGE_BPS || 0),
  POLY_EXIT_ONLY_AFTER_BUCKETS: Number(process.env.POLY_EXIT_ONLY_AFTER_BUCKETS || 1),
  POLY_EXIT_MIN_ORDER_USD: Number(process.env.POLY_EXIT_MIN_ORDER_USD || 5),
  POLY_EXIT_FORCE: String(process.env.POLY_EXIT_FORCE || 'false').toLowerCase() === 'true',
  POLY_EXIT_FORCE_NET_PROFIT_USD: Number(process.env.POLY_EXIT_FORCE_NET_PROFIT_USD || 0.1),
  POLY_MAX_OPEN_TRADES: Number(process.env.POLY_MAX_OPEN_TRADES || 6),
  POLY_STATE_COLLECTION: String(process.env.POLY_STATE_COLLECTION || 'polymarketBotState').trim(),
};

const BAR_SEC = 300;
const REQUIRED_CANDLES = 70;
const LADDER = [5, 10, 20, 40, 80, 160];

const state = {
  market: {
    slug: ENV.POLY_MARKET_SLUG,
    yesTokenId: ENV.POLY_YES_TOKEN_ID,
    noTokenId: ENV.POLY_NO_TOKEN_ID,
    orderMinSize: null,
    conditionId: null,
    takerBaseFee: null,
    loadedAtTs: null,
  },
  lastTradeBarClose: null,
  tradeHistory: [],
  positionUsd: 0,
  lastDirection: 'FLAT',
  lastRun: null,
  autoSizeStep: 0,
  pendingBets: [],
  gtcOpenByToken: {},
  gtcFilledByBucketTokenDecision: {},
};

const marketCache = {
  bySlug: new Map(),
  latestAuto: null,
};

let firestoreClientPromise = null;

const DEFAULT_EXECUTE_STATE = {
  step: 0,
  lossStreak: 0,
  cumulativeLossUSD: 0,
  lastBucketKeyPlaced: null, // legacy compatibility
  filledBuckets: {},
  openTrades: [],
  pausedUntilBucket: null,
  lastResolvedBucketKey: null,
};

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function rid() {
  return Math.random().toString(16).slice(2, 14);
}

function safeNum(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function bucket5m(tsSec) {
  return Math.floor(tsSec / BAR_SEC) * BAR_SEC;
}

function trimHistoryInPlace(tradeHistory, currentSec) {
  const cutoff = currentSec - 3600;
  while (tradeHistory.length && tradeHistory[0].ts < cutoff) {
    tradeHistory.shift();
  }
}

async function loadStateFromDisk() {
  try {
    const raw = await fs.readFile(STATE_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object') return;
    state.lastTradeBarClose = safeNum(parsed.lastTradeBarClose, null);
    state.tradeHistory = Array.isArray(parsed.tradeHistory)
      ? parsed.tradeHistory.map((x) => ({ ts: safeNum(x?.ts, 0), direction: String(x?.direction || 'FLAT') }))
      : [];
    state.positionUsd = safeNum(parsed.positionUsd, 0);
    state.lastDirection = String(parsed.lastDirection || 'FLAT');
    state.lastRun = parsed.lastRun && typeof parsed.lastRun === 'object' ? parsed.lastRun : null;
    state.autoSizeStep = Math.max(0, Math.min(5, safeNum(parsed.autoSizeStep, 0)));
    state.pendingBets = Array.isArray(parsed.pendingBets) ? parsed.pendingBets : [];
    state.gtcOpenByToken = parsed.gtcOpenByToken && typeof parsed.gtcOpenByToken === 'object' ? parsed.gtcOpenByToken : {};
    state.gtcFilledByBucketTokenDecision = parsed.gtcFilledByBucketTokenDecision && typeof parsed.gtcFilledByBucketTokenDecision === 'object'
      ? parsed.gtcFilledByBucketTokenDecision
      : {};
  } catch (_err) {
    // Optional persistence: ignore missing/corrupt state.
  }
}

async function saveStateToDisk() {
  const payload = {
    lastTradeBarClose: state.lastTradeBarClose,
    tradeHistory: state.tradeHistory,
    positionUsd: state.positionUsd,
    lastDirection: state.lastDirection,
    lastRun: state.lastRun,
    autoSizeStep: state.autoSizeStep,
    pendingBets: state.pendingBets,
    gtcOpenByToken: state.gtcOpenByToken,
    gtcFilledByBucketTokenDecision: state.gtcFilledByBucketTokenDecision,
  };
  await fs.writeFile(STATE_FILE, JSON.stringify(payload, null, 2));
}

async function fetchJson(url) {
  const resp = await fetch(url, { method: 'GET' });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`HTTP ${resp.status} from ${url}: ${body.slice(0, 250)}`);
  }
  return resp.json();
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

async function getFirestoreClient() {
  if (firestoreClientPromise) return firestoreClientPromise;
  firestoreClientPromise = (async () => {
    const mod = await import('@google-cloud/firestore');
    const Firestore = mod?.Firestore || mod?.default?.Firestore || mod?.default;
    if (!Firestore) throw new Error('firestore_client_not_found');
    return new Firestore();
  })();
  return firestoreClientPromise;
}

function executeStateDocId(envName, market) {
  const m = String(market?.conditionId || market?.slug || 'unknown').replace(/[^a-zA-Z0-9:_-]/g, '_');
  return `${String(envName || 'mainnet')}:${m}`;
}

async function loadExecuteState(envName, market) {
  try {
    const db = await getFirestoreClient();
    const docId = executeStateDocId(envName, market);
    const snap = await db.collection(ENV.POLY_STATE_COLLECTION).doc(docId).get();
    if (!snap.exists) return { ...DEFAULT_EXECUTE_STATE, _docId: docId };
    const data = snap.data() || {};
    const filledRaw = data.filledBuckets && typeof data.filledBuckets === 'object' ? data.filledBuckets : {};
    const filledBuckets = {};
    for (const [k, v] of Object.entries(filledRaw)) {
      if (v) filledBuckets[String(k)] = true;
    }
    const openTradesRaw = Array.isArray(data.openTrades) ? data.openTrades : [];
    const openTrades = openTradesRaw
      .filter((x) => x && typeof x === 'object')
      .map((x) => ({
        bucketKey: Number.isFinite(Number(x.bucketKey)) ? Number(x.bucketKey) : null,
        marketSlug: String(x.marketSlug || ''),
        conditionId: String(x.conditionId || ''),
        tokenId: String(x.tokenId || ''),
        side: String(x.side || '').toUpperCase() === 'DOWN' ? 'DOWN' : 'UP',
        notionalUSD: Math.max(0, safeNum(x.notionalUSD, 0)),
        priceEntry: Number.isFinite(Number(x.priceEntry)) ? Number(x.priceEntry) : null,
        sizeEntry: Number.isFinite(Number(x.sizeEntry)) ? Number(x.sizeEntry) : null,
        createdAtMs: Number.isFinite(Number(x.createdAtMs)) ? Number(x.createdAtMs) : Date.now(),
        status: String(x.status || 'open'),
        exit: x.exit && typeof x.exit === 'object' ? x.exit : null,
        settlement: x.settlement && typeof x.settlement === 'object' ? x.settlement : null,
      }));
    if (!openTrades.length && data.pendingTrade && typeof data.pendingTrade === 'object') {
      const p = data.pendingTrade;
      openTrades.push({
        bucketKey: Number.isFinite(Number(p.bucketKey)) ? Number(p.bucketKey) : null,
        marketSlug: String(p.marketSlug || ''),
        conditionId: String(p.conditionId || ''),
        tokenId: String(p.tokenId || ''),
        side: String(p.side || '').toUpperCase() === 'DOWN' ? 'DOWN' : 'UP',
        notionalUSD: Math.max(0, safeNum(p.notionalUSD, 0)),
        priceEntry: null,
        sizeEntry: null,
        createdAtMs: Number.isFinite(Number(p.createdAtMs)) ? Number(p.createdAtMs) : Date.now(),
        status: 'open',
        exit: null,
        settlement: null,
      });
    }
    return {
      step: clamp(safeNum(data.step, 0), 0, 5),
      lossStreak: Math.max(0, safeNum(data.lossStreak, 0)),
      cumulativeLossUSD: Math.max(0, safeNum(data.cumulativeLossUSD, 0)),
      lastBucketKeyPlaced: Number.isFinite(Number(data.lastBucketKeyPlaced)) ? Number(data.lastBucketKeyPlaced) : null,
      filledBuckets,
      openTrades,
      pausedUntilBucket: Number.isFinite(Number(data.pausedUntilBucket)) ? Number(data.pausedUntilBucket) : null,
      lastResolvedBucketKey: Number.isFinite(Number(data.lastResolvedBucketKey)) ? Number(data.lastResolvedBucketKey) : null,
      _docId: docId,
    };
  } catch {
    return { ...DEFAULT_EXECUTE_STATE, _docId: executeStateDocId(envName, market) };
  }
}

async function saveExecuteState(execState) {
  try {
    const db = await getFirestoreClient();
    const docId = String(execState?._docId || '');
    if (!docId) return;
    const payload = {
      step: clamp(safeNum(execState.step, 0), 0, 5),
      lossStreak: Math.max(0, safeNum(execState.lossStreak, 0)),
      cumulativeLossUSD: Math.max(0, safeNum(execState.cumulativeLossUSD, 0)),
      lastBucketKeyPlaced: Number.isFinite(Number(execState.lastBucketKeyPlaced)) ? Number(execState.lastBucketKeyPlaced) : null,
      filledBuckets: execState.filledBuckets && typeof execState.filledBuckets === 'object' ? execState.filledBuckets : {},
      openTrades: Array.isArray(execState.openTrades) ? execState.openTrades : [],
      pausedUntilBucket: Number.isFinite(Number(execState.pausedUntilBucket)) ? Number(execState.pausedUntilBucket) : null,
      lastResolvedBucketKey: Number.isFinite(Number(execState.lastResolvedBucketKey)) ? Number(execState.lastResolvedBucketKey) : null,
      updatedAtMs: Date.now(),
    };
    await db.collection(ENV.POLY_STATE_COLLECTION).doc(docId).set(payload, { merge: true });
  } catch {
    // best effort persistence
  }
}

async function fetchGeoblockStatus() {
  if (!ENV.POLY_GEOBLOCK_URL) return null;
  try {
    const payload = await fetchJson(ENV.POLY_GEOBLOCK_URL);
    if (!payload || typeof payload !== 'object') return null;
    return {
      blocked: Boolean(payload.blocked),
      country: String(payload.country || ''),
      region: String(payload.region || ''),
      ip: String(payload.ip || ''),
    };
  } catch {
    return null;
  }
}

function resolveEnvTokenOverrides() {
  if (ENV.POLY_YES_TOKEN_ID && ENV.POLY_NO_TOKEN_ID) {
    return { yesTokenId: ENV.POLY_YES_TOKEN_ID, noTokenId: ENV.POLY_NO_TOKEN_ID, source: 'POLY_YES_TOKEN_ID/POLY_NO_TOKEN_ID' };
  }
  const arr = parseJsonArrayLike(ENV.POLY_CLOB_TOKEN_IDS);
  if (arr.length >= 2) {
    const yesTokenId = String(arr[0] || '').trim();
    const noTokenId = String(arr[1] || '').trim();
    if (yesTokenId && noTokenId) {
      return { yesTokenId, noTokenId, source: 'POLY_CLOB_TOKEN_IDS' };
    }
  }
  return null;
}

function cachedMarketGet(slug, nowMs) {
  const cached = marketCache.bySlug.get(slug);
  if (!cached) return null;
  if (nowMs - cached.cachedAtMs > 20000) return null;
  return cached;
}

function cacheMarketEntry(entry, nowMs) {
  const normalized = { ...entry, cachedAtMs: nowMs };
  marketCache.bySlug.set(entry.slug, normalized);
  marketCache.latestAuto = normalized;
  return normalized;
}

function readOrderMinSize(market) {
  const candidates = [
    market?.orderMinSize,
    market?.minimumOrderSize,
    market?.minOrderSize,
    market?.min_size,
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return null;
}

function readTakerBaseFee(market) {
  const candidates = [
    market?.takerBaseFee,
    market?.taker_base_fee,
    market?.takerFeeBps,
    market?.taker_fee_bps,
  ];
  for (const c of candidates) {
    const n = Number(c);
    if (Number.isFinite(n) && n >= 0) return Math.floor(n);
  }
  return null;
}

function readConditionId(market) {
  const cid = String(
    market?.conditionId
    || market?.condition_id
    || market?.questionID
    || '',
  ).trim();
  return cid || null;
}

async function resolveUpDownMarket(marketSlug, nowMs = Date.now()) {
  const override = resolveEnvTokenOverrides();
  if (override) {
    return cacheMarketEntry({
      slug: marketSlug || ENV.POLY_MARKET_SLUG || 'env-override',
      yesTokenId: override.yesTokenId,
      noTokenId: override.noTokenId,
      orderMinSize: null,
      conditionId: null,
      takerBaseFee: null,
    }, nowMs);
  }

  if (marketSlug) {
    const cached = cachedMarketGet(marketSlug, nowMs);
    if (cached) return cached;
    const url = `${ENV.POLY_GAMMA_HOST}/markets/slug/${encodeURIComponent(marketSlug)}`;
    const payload = await fetchJson(url);
    const market = Array.isArray(payload) ? payload[0] : payload;
    if (!market || !market.slug) throw new Error(`market slug not found: ${marketSlug}`);
    const { yesTokenId, noTokenId } = extractUpDownTokens(market);
    return cacheMarketEntry({
      slug: String(market.slug),
      yesTokenId,
      noTokenId,
      orderMinSize: readOrderMinSize(market),
      conditionId: readConditionId(market),
      takerBaseFee: readTakerBaseFee(market),
    }, nowMs);
  }

  if (marketCache.latestAuto && (nowMs - marketCache.latestAuto.cachedAtMs <= 20000)) {
    return marketCache.latestAuto;
  }

  const url = `${ENV.POLY_GAMMA_HOST}/events?order=id&ascending=false&closed=false&limit=400`;
  const payload = await fetchJson(url);
  const selected = chooseBtcUpDownMarketFromEvents(payload, nowMs);
  return cacheMarketEntry({
    slug: selected.slug,
    yesTokenId: selected.yesTokenId,
    noTokenId: selected.noTokenId,
    orderMinSize: readOrderMinSize(selected.market),
    conditionId: readConditionId(selected.market),
    takerBaseFee: readTakerBaseFee(selected.market),
  }, nowMs);
}

function pickYesNoTokenIdsFromGamma(payload) {
  const markets = Array.isArray(payload) ? payload : [payload];
  for (const market of markets) {
    const outcomes = parseJsonArrayLike(market?.outcomes);
    const clobIds = parseJsonArrayLike(market?.clobTokenIds);
    let yesTokenId = '';
    let noTokenId = '';
    for (let i = 0; i < outcomes.length; i += 1) {
      const label = String(outcomes[i] || '').trim().toLowerCase();
      const tokenId = String(clobIds[i] || '').trim();
      if (!tokenId) continue;
      if (label === 'yes' || label === 'up') yesTokenId = tokenId;
      if (label === 'no' || label === 'down') noTokenId = tokenId;
    }
    if (!yesTokenId || !noTokenId) {
      const altTokens = Array.isArray(market?.tokens) ? market.tokens : [];
      for (const tok of altTokens) {
        const outcome = String(tok?.outcome || tok?.name || '').trim().toLowerCase();
        const tokenId = String(tok?.token_id || tok?.id || tok?.clobTokenId || '').trim();
        if (!tokenId) continue;
        if (outcome === 'yes') yesTokenId = tokenId;
        if (outcome === 'no') noTokenId = tokenId;
      }
    }
    if (yesTokenId && noTokenId) return { yesTokenId, noTokenId };
  }
  throw new Error('could not extract YES/NO token ids from gamma payload');
}

async function resolveMarketTokens() {
  if (state.market.yesTokenId && state.market.noTokenId) return state.market;
  const override = resolveEnvTokenOverrides();
  if (override) {
    state.market = {
      slug: ENV.POLY_MARKET_SLUG || 'manual',
      yesTokenId: override.yesTokenId,
      noTokenId: override.noTokenId,
      orderMinSize: null,
      conditionId: null,
      takerBaseFee: null,
      loadedAtTs: nowSec(),
    };
    return state.market;
  }
  if (!ENV.POLY_MARKET_SLUG) {
    throw new Error('POLY_MARKET_SLUG is required unless POLY_YES_TOKEN_ID and POLY_NO_TOKEN_ID are set');
  }
  const url = `${ENV.POLY_GAMMA_HOST}/markets/slug/${encodeURIComponent(ENV.POLY_MARKET_SLUG)}`;
  const payload = await fetchJson(url);
  const { yesTokenId, noTokenId } = pickYesNoTokenIdsFromGamma(payload);
  state.market = {
    slug: ENV.POLY_MARKET_SLUG,
    yesTokenId,
    noTokenId,
    orderMinSize: readOrderMinSize(Array.isArray(payload) ? payload[0] : payload),
    conditionId: readConditionId(Array.isArray(payload) ? payload[0] : payload),
    takerBaseFee: readTakerBaseFee(Array.isArray(payload) ? payload[0] : payload),
    loadedAtTs: nowSec(),
  };
  return state.market;
}

async function fetchMidpoint(tokenId) {
  const url = `${ENV.POLY_CLOB_HOST}/midpoint?token_id=${encodeURIComponent(tokenId)}`;
  const payload = await fetchJson(url);
  const price = safeNum(payload?.mid || payload?.midpoint || payload?.price, NaN);
  return Number.isFinite(price) ? price : null;
}

async function fetchPriceHistory(tokenId, startTs, endTs) {
  const params = new URLSearchParams({
    market: tokenId,
    startTs: String(startTs),
    endTs: String(endTs),
    fidelity: '1',
  });
  const url = `${ENV.POLY_CLOB_HOST}/prices-history?${params.toString()}`;
  const payload = await fetchJson(url);
  const points = Array.isArray(payload?.history)
    ? payload.history
    : Array.isArray(payload)
      ? payload
      : Array.isArray(payload?.data)
        ? payload.data
        : [];
  return points
    .map((p) => {
      const ts = safeNum(p?.t ?? p?.ts ?? p?.timestamp, NaN);
      const close = safeNum(p?.p ?? p?.price ?? p?.c ?? p?.close, NaN);
      return { ts, close };
    })
    .filter((x) => Number.isFinite(x.ts) && Number.isFinite(x.close))
    .sort((a, b) => a.ts - b.ts);
}

function build5mCandles(points) {
  const buckets = new Map();
  for (const point of points) {
    const b = bucket5m(point.ts);
    const existing = buckets.get(b);
    if (!existing) {
      buckets.set(b, {
        bucketStartTs: b,
        open: point.close,
        high: point.close,
        low: point.close,
        close: point.close,
        pointCount: 1,
      });
      continue;
    }
    existing.high = Math.max(existing.high, point.close);
    existing.low = Math.min(existing.low, point.close);
    existing.close = point.close;
    existing.pointCount += 1;
  }
  return [...buckets.values()].sort((a, b) => a.bucketStartTs - b.bucketStartTs);
}

function emaSeries(values, period) {
  if (!Array.isArray(values) || values.length < period) return [];
  const out = [];
  const k = 2 / (period + 1);
  let seed = 0;
  for (let i = 0; i < period; i += 1) seed += values[i];
  let prev = seed / period;
  out[period - 1] = prev;
  for (let i = period; i < values.length; i += 1) {
    const next = (values[i] * k) + (prev * (1 - k));
    out[i] = next;
    prev = next;
  }
  return out;
}

function rsiSeries(values, period = 14) {
  if (!Array.isArray(values) || values.length <= period) return [];
  const out = [];
  let gainSum = 0;
  let lossSum = 0;
  for (let i = 1; i <= period; i += 1) {
    const delta = values[i] - values[i - 1];
    if (delta >= 0) gainSum += delta;
    else lossSum += Math.abs(delta);
  }
  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;
  out[period] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
  for (let i = period + 1; i < values.length; i += 1) {
    const delta = values[i] - values[i - 1];
    const gain = delta > 0 ? delta : 0;
    const loss = delta < 0 ? Math.abs(delta) : 0;
    avgGain = ((avgGain * (period - 1)) + gain) / period;
    avgLoss = ((avgLoss * (period - 1)) + loss) / period;
    out[i] = avgLoss === 0 ? 100 : 100 - (100 / (1 + (avgGain / avgLoss)));
  }
  return out;
}

function donchian(candles, period = 20) {
  if (!Array.isArray(candles) || candles.length < period + 1) return null;
  const prevWindow = candles.slice(candles.length - period - 1, candles.length - 1);
  let hi = -Infinity;
  let lo = Infinity;
  for (const c of prevWindow) {
    hi = Math.max(hi, c.high);
    lo = Math.min(lo, c.low);
  }
  return { hi, lo };
}

function evaluateSignal(candles) {
  if (!Array.isArray(candles) || candles.length < REQUIRED_CANDLES) {
    return { error: `not_enough_candles:${candles?.length || 0}` };
  }
  const closes = candles.map((c) => c.close);
  const ema20s = emaSeries(closes, 20);
  const ema50s = emaSeries(closes, 50);
  const rsis = rsiSeries(closes, 14);
  const last = candles[candles.length - 1];
  const close = last.close;
  const ema20 = ema20s[ema20s.length - 1];
  const ema50 = ema50s[ema50s.length - 1];
  const rsi = rsis[rsis.length - 1];
  const donch = donchian(candles, 20);

  if (![ema20, ema50, rsi].every(Number.isFinite) || !donch) {
    return { error: 'indicator_nan' };
  }

  const longTrend = ema20 > ema50 && close > ema20;
  const shortTrend = ema20 < ema50 && close < ema20;
  const longRsi = rsi >= 55;
  const shortRsi = rsi <= 45;
  const longDonch = close > donch.hi;
  const shortDonch = close < donch.lo;

  const longVotes = Number(longTrend) + Number(longRsi) + Number(longDonch);
  const shortVotes = Number(shortTrend) + Number(shortRsi) + Number(shortDonch);

  let decision = 'FLAT';
  if (longVotes >= 2) decision = 'LONG';
  if (shortVotes >= 2) decision = 'SHORT';
  if (longVotes >= 2 && shortVotes >= 2) decision = 'FLAT';

  return {
    decision,
    votes: {
      trend: longTrend ? 'LONG' : shortTrend ? 'SHORT' : 'FLAT',
      rsi: longRsi ? 'LONG' : shortRsi ? 'SHORT' : 'FLAT',
      donchian: longDonch ? 'LONG' : shortDonch ? 'SHORT' : 'FLAT',
      longVotes,
      shortVotes,
    },
    indicators: {
      close,
      ema20,
      ema50,
      rsi,
      donchHi: donch.hi,
      donchLo: donch.lo,
    },
  };
}

function guardrailReason({ decision, barCloseTs, nowTs, notionalUSD = ENV.POLY_ORDER_USD, mode = 'live' }) {
  if (decision === 'FLAT' || decision === 'NO_TRADE') return 'flat_signal';
  if (ENV.POLY_KILL_SWITCH) return 'kill_switch';

  const barsSinceTrade = state.lastTradeBarClose == null
    ? Infinity
    : Math.floor((barCloseTs - state.lastTradeBarClose) / BAR_SEC);
  if (barsSinceTrade < ENV.POLY_COOLDOWN_BARS) return 'cooldown_active';

  trimHistoryInPlace(state.tradeHistory, nowTs);
  if (state.tradeHistory.length >= ENV.POLY_MAX_TRADES_PER_HOUR) return 'max_trades_per_hour';

  const nextPos = state.positionUsd + notionalUSD;
  if (nextPos > ENV.POLY_MAX_POSITION_USD) return 'max_position_usd';

  if (mode === 'live' && !ENV.POLY_DRY_RUN && !ENV.POLY_PRIVATE_KEY) return 'missing_private_key';
  return null;
}

function isLiveGateEnabled() {
  return isLiveExecutionEnabled(ENV.POLY_LIVE_ENABLED, ENV.POLY_LIVE_CONFIRM);
}

function deriveWinnerFromOutcomePrices(market) {
  const outcomes = parseJsonArrayLike(market?.outcomes).map((x) => String(x || '').toLowerCase());
  const prices = parseJsonArrayLike(market?.outcomePrices).map((x) => Number(x));
  let upIdx = outcomes.indexOf('up');
  let downIdx = outcomes.indexOf('down');
  if (upIdx < 0 || downIdx < 0) {
    upIdx = 0;
    downIdx = 1;
  }
  const upP = Number(prices[upIdx]);
  const downP = Number(prices[downIdx]);
  if (!Number.isFinite(upP) || !Number.isFinite(downP)) return null;
  if (upP === downP) return null;
  return upP > downP ? 'UP' : 'DOWN';
}

function makeClientOrderId(slug, barCloseTs, direction) {
  const safeSlug = String(slug || 'market').replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 48);
  return `${ENV.POLY_STRATEGY_VERSION}:${safeSlug}:${barCloseTs}:${direction}`;
}

function buildOrderIntent(direction, yesTokenId, noTokenId) {
  return direction === 'LONG'
    ? { side: 'BUY', tokenId: yesTokenId, outcome: 'YES' }
    : { side: 'BUY', tokenId: noTokenId, outcome: 'NO' };
}

function normalizeOrderPrice(midPrice) {
  const p = Number(midPrice);
  if (Number.isFinite(p) && p > 0 && p < 1) return p;
  return 0.5;
}

function normalizeUsdAmount(amount) {
  const n = Number(amount);
  if (!Number.isFinite(n)) return NaN;
  return Number((Math.max(0.01, n)).toFixed(2));
}

function floorToDecimals(value, decimals) {
  const dec = Math.max(0, Number(decimals) || 0);
  const raw = String(value ?? '').trim();
  if (!raw) return '0';
  const neg = raw.startsWith('-');
  const normalized = raw.replace(/^[+-]/, '');
  if (!/^\d+(\.\d+)?$/.test(normalized)) return '0';
  const [intPartRaw, fracPartRaw = ''] = normalized.split('.');
  const intPart = intPartRaw.replace(/^0+(?=\d)/, '') || '0';
  const fracPart = fracPartRaw.padEnd(dec, '0').slice(0, dec);
  let out = dec > 0 ? `${intPart}.${fracPart}` : intPart;
  if (dec > 0) out = out.replace(/\.?0+$/, '');
  if (!out) out = '0';
  if (neg && out !== '0') return `-${out}`;
  return out;
}

function applyMakerTakerPrecision(orderRequest) {
  const makerWas = orderRequest?.makerAmount ?? null;
  const takerWas = orderRequest?.takerAmount ?? orderRequest?.amount ?? null;
  const makerAmount = floorToDecimals(makerWas ?? 0, 4);
  const takerAmount = floorToDecimals(takerWas ?? 0, 2);
  const next = {
    ...orderRequest,
    makerAmount,
    takerAmount,
    amount: Number(takerAmount),
  };
  return {
    orderRequest: next,
    precisionApplied: {
      makerDecimals: 4,
      takerDecimals: 2,
      makerWas,
      takerWas,
    },
  };
}

function countDecimals(value) {
  const s = String(value ?? '').trim();
  const m = s.match(/^-?\d+(?:\.(\d+))?$/);
  if (!m) return 0;
  return m[1] ? m[1].length : 0;
}

function isPositiveNumberLike(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0;
}

function tickDecimalsFromValue(tickSize) {
  const raw = typeof tickSize === 'object' ? (tickSize?.minTickSize ?? tickSize?.tickSize ?? tickSize?.value ?? tickSize?.size) : tickSize;
  const s = String(raw ?? '').trim();
  if (!s) return 2;
  const m = s.match(/^\d+(?:\.(\d+))?$/);
  if (!m) return 2;
  return Math.max(0, (m[1] || '').length);
}

function parseForceFillPriceSteps(midPrice) {
  const base = normalizeOrderPrice(midPrice);
  const fromEnv = String(ENV.POLY_FORCE_FILL_PRICES || '')
    .split(',')
    .map((x) => Number(String(x).trim()))
    .filter((x) => Number.isFinite(x) && x > 0 && x < 1)
    .map((x) => Number(x.toFixed(4)));
  if (!fromEnv.length) return [base, 0.75, 0.85, 0.95, 0.99].map((x) => Number(x.toFixed(4)));
  const uniqSorted = [...new Set(fromEnv)].sort((a, b) => a - b);
  if (uniqSorted[0] > base) uniqSorted.unshift(Number(base.toFixed(4)));
  return uniqSorted;
}

function resolveOrderType(mod, orderTypeName = ENV.POLY_ORDER_TYPE) {
  const t = String(orderTypeName || 'FOK').toUpperCase();
  if (t === 'FOK') return mod?.OrderType?.FOK || 'FOK';
  if (t === 'GTD') return mod?.OrderType?.GTD || 'GTD';
  return mod?.OrderType?.GTC || 'GTC';
}

function validateManualFeeRateOverride(rawFee) {
  const raw = String(rawFee || '').trim();
  if (!raw) return { hasOverride: false, feeRateBps: null };
  if (!/^\d+$/.test(raw)) return { hasOverride: true, ok: false, reason: 'invalid_fee_rate' };
  const feeRateBps = Number(raw);
  if (!Number.isInteger(feeRateBps) || feeRateBps < 0 || feeRateBps > 200) {
    return { hasOverride: true, ok: false, reason: 'invalid_fee_rate' };
  }
  return { hasOverride: true, ok: true, feeRateBps };
}

async function resolveDynamicFeeRateBps(client, conditionId, fallbackTakerBaseFee) {
  const normalizeRawFeeToBps = (rawFee) => {
    const raw = Number(rawFee);
    if (!Number.isFinite(raw)) return null;
    if (raw >= 0 && raw <= 1) return Math.round(raw * 10000);
    if (raw > 1) return Math.floor(raw);
    return null;
  };

  let feeRateBps = null;
  if (conditionId && typeof client?.getMarket === 'function') {
    try {
      const m = await client.getMarket(conditionId);
      feeRateBps = normalizeRawFeeToBps(m?.taker_base_fee ?? m?.takerBaseFee);
      if (Number.isInteger(feeRateBps)) {
        return { feeRateBps, feeSource: 'clob_market', feeRaw: m?.taker_base_fee ?? m?.takerBaseFee ?? null };
      }
    } catch {
      feeRateBps = null;
    }
  }
  feeRateBps = normalizeRawFeeToBps(fallbackTakerBaseFee);
  if (Number.isInteger(feeRateBps)) return { feeRateBps, feeSource: 'gamma', feeRaw: fallbackTakerBaseFee ?? null };
  return { feeRateBps: null, feeSource: null, feeRaw: null };
}

function feeLog({ rid: runRid, feeRateBps, tokenId }) {
  console.log(`FEE: ${JSON.stringify({ rid: runRid, feeRateBps, tokenId })}`);
}

function sanitizeOrderRequest(orderRequest) {
  const feeRaw = Number(orderRequest?.feeRateBps);
  const makerAmount = orderRequest?.makerAmount ?? null;
  const takerAmount = orderRequest?.takerAmount ?? orderRequest?.amount ?? null;
  return {
    tokenID: String(orderRequest?.tokenID || ''),
    side: String(orderRequest?.side || ''),
    amount: Number(orderRequest?.amount ?? NaN),
    makerAmount: makerAmount == null ? null : String(makerAmount),
    takerAmount: takerAmount == null ? null : String(takerAmount),
    price: Number(orderRequest?.price ?? NaN),
    feeRateBps: Number.isFinite(feeRaw) ? feeRaw : null,
    orderType: String(orderRequest?.orderType || ''),
  };
}

async function postMarketOrderWithClient(client, mod, orderRequest, orderTypeName = ENV.POLY_ORDER_TYPE) {
  const orderType = resolveOrderType(mod, orderTypeName);
  const { orderRequest: quantizedReq, precisionApplied } = applyMakerTakerPrecision(orderRequest);
  const tickSize = typeof client.getTickSize === 'function'
    ? await client.getTickSize(quantizedReq.tokenID).catch(() => undefined)
    : undefined;
  const postOrder = client.postOrder || client.createAndPostOrder || client.placeOrder;
  const createOrder = client.createOrder;
  if (typeof createOrder !== 'function' || typeof postOrder !== 'function') {
    throw new Error('clob_create_order_method_not_found');
  }

  const priceDecimals = Math.max(2, tickDecimalsFromValue(tickSize));
  const takerAmountFinal = floorToDecimals(quantizedReq.amount ?? 0, 2);
  const priceFinal = floorToDecimals(quantizedReq.price ?? 0.5, priceDecimals);
  const sizeRaw = Number(takerAmountFinal) / Number(priceFinal || '0');
  const sizeFinal = floorToDecimals(sizeRaw, 4);
  const feeRateBpsValue = Number.isInteger(quantizedReq.feeRateBps) ? quantizedReq.feeRateBps : undefined;

  const userOrder = {
    tokenID: String(quantizedReq.tokenID || ''),
    side: quantizedReq.side,
    price: Number(priceFinal),
    size: Number(sizeFinal),
    ...(Number.isInteger(feeRateBpsValue) ? { feeRateBps: feeRateBpsValue } : {}),
  };

  const precisionDetail = {
    makerDecimals: 4,
    takerDecimals: 2,
    makerWas: quantizedReq.makerAmount ?? null,
    takerWas: quantizedReq.takerAmount ?? quantizedReq.amount ?? null,
    priceWas: quantizedReq.price ?? null,
    sizeWas: sizeRaw,
  };

  if (!isPositiveNumberLike(userOrder.price) || !isPositiveNumberLike(userOrder.size) || !isPositiveNumberLike(takerAmountFinal)) {
    return {
      preflightFailed: true,
      reason: 'amount_precision_preflight_failed',
      precisionApplied: precisionDetail,
      finalOrderRequest: {
        ...quantizedReq,
        price: Number(priceFinal),
        size: Number(sizeFinal),
        amount: Number(takerAmountFinal),
        takerAmount: takerAmountFinal,
      },
      clobPayloadPosted: {
        tokenId: userOrder.tokenID,
        side: String(userOrder.side || ''),
        price: priceFinal,
        size: sizeFinal,
        makerAmount: null,
        takerAmount: takerAmountFinal,
        orderType: String(orderType || ''),
      },
    };
  }

  const signedOrder = await createOrder.call(client, userOrder, tickSize);
  const makerAmountFinal = floorToDecimals(signedOrder?.makerAmount, 4);
  const takerAmountSignedFinal = floorToDecimals(signedOrder?.takerAmount, 2);
  const makerOk = countDecimals(makerAmountFinal) <= 4 && isPositiveNumberLike(makerAmountFinal);
  const takerOk = countDecimals(takerAmountSignedFinal) <= 2 && isPositiveNumberLike(takerAmountSignedFinal);
  if (!makerOk || !takerOk) {
    return {
      preflightFailed: true,
      reason: 'amount_precision_preflight_failed',
      precisionApplied: precisionDetail,
      finalOrderRequest: {
        ...quantizedReq,
        price: Number(priceFinal),
        size: Number(sizeFinal),
        amount: Number(takerAmountFinal),
        makerAmount: makerAmountFinal,
        takerAmount: takerAmountSignedFinal,
      },
      clobPayloadPosted: {
        tokenId: String(signedOrder?.tokenId || userOrder.tokenID),
        side: String(signedOrder?.side || userOrder.side || ''),
        price: priceFinal,
        size: sizeFinal,
        makerAmount: makerAmountFinal,
        takerAmount: takerAmountSignedFinal,
        orderType: String(orderType || ''),
      },
    };
  }

  const result = await postOrder.call(client, signedOrder, orderType);
  return {
    result,
    orderType,
    precisionApplied: precisionDetail,
    finalOrderRequest: {
      ...quantizedReq,
      price: Number(priceFinal),
      size: Number(sizeFinal),
      amount: Number(takerAmountFinal),
      makerAmount: makerAmountFinal,
      takerAmount: takerAmountSignedFinal,
    },
    makerAmountFinal,
    takerAmountFinal: takerAmountSignedFinal,
    priceFinal,
    sizeFinal,
    clobPayloadPosted: {
      tokenId: String(signedOrder?.tokenId || userOrder.tokenID),
      side: String(signedOrder?.side || userOrder.side || ''),
      price: priceFinal,
      size: sizeFinal,
      makerAmount: makerAmountFinal,
      takerAmount: takerAmountSignedFinal,
      orderType: String(orderType || ''),
    },
  };
}

async function loadClobClient() {
  const mod = await import('@polymarket/clob-client');
  const walletMod = await import('@ethersproject/wallet');
  const ClobClient = mod?.ClobClient || mod?.default?.ClobClient || mod?.default;
  const Wallet = walletMod?.Wallet || walletMod?.default;
  if (!ClobClient) throw new Error('clob_client_constructor_not_found');
  if (!Wallet) throw new Error('ethers_wallet_constructor_not_found');

  const signer = new Wallet(ENV.POLY_PRIVATE_KEY);
  const client = new ClobClient(
    ENV.POLY_CLOB_HOST,
    137,
    signer,
    undefined,
    resolveSignatureType(),
    ENV.POLY_FUNDER_ADDRESS || undefined,
  );
  const maybeCreate = client.createOrDeriveApiKey || client.createApiKey || client.deriveApiKey;
  if (typeof maybeCreate === 'function') {
    const credsRaw = await maybeCreate.call(client);
    if (credsRaw && typeof credsRaw === 'object') {
      const normalized = {
        key: credsRaw.key || credsRaw.apiKey || '',
        secret: credsRaw.secret || '',
        passphrase: credsRaw.passphrase || '',
      };
      if (normalized.key && normalized.secret && normalized.passphrase) client.creds = normalized;
    }
  }
  return { client, mod };
}

function sleepMs(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

function extractOrderId(result) {
  const candidates = [
    result?.orderID,
    result?.orderId,
    result?.id,
    result?.data?.orderID,
    result?.data?.orderId,
    result?.data?.id,
  ];
  for (const c of candidates) {
    const v = String(c || '').trim();
    if (v) return v;
  }
  return null;
}

function normalizeOrderStatus(orderLike) {
  const raw = String(
    orderLike?.status
    ?? orderLike?.orderStatus
    ?? orderLike?.state
    ?? '',
  ).trim().toUpperCase();
  if (!raw) return 'UNKNOWN';
  if (raw.includes('FILL') || raw.includes('MATCH')) return 'FILLED';
  if (raw.includes('CANCEL')) return 'CANCELED';
  if (raw.includes('REJECT')) return 'REJECTED';
  if (raw.includes('OPEN') || raw.includes('LIVE') || raw.includes('PENDING')) return 'OPEN';
  return raw;
}

function makeFilledMarkerKey(bucketKey, tokenId, decision) {
  return `${bucketKey}:${String(tokenId || '')}:${String(decision || '')}`;
}

async function fetchOrderStatus(client, orderId) {
  const getOrder = client?.getOrder || client?.getOrderById || client?.fetchOrder;
  if (typeof getOrder !== 'function') return null;
  try {
    const data = await getOrder.call(client, orderId);
    return normalizeOrderStatus(data);
  } catch {
    return null;
  }
}

async function cancelOrderById(client, orderId) {
  const cands = [
    client?.cancelOrder,
    client?.cancel,
    client?.cancelOrders,
    client?.cancelOrderById,
  ];
  for (const fn of cands) {
    if (typeof fn !== 'function') continue;
    try {
      if (fn === client?.cancelOrders) {
        await fn.call(client, [orderId]);
      } else {
        await fn.call(client, orderId);
      }
      return true;
    } catch {
      // continue
    }
  }
  return false;
}

function normalizeClobErrorReason(err) {
  const status = Number(
    err?.status
    ?? err?.response?.status
    ?? err?.response?.statusCode
    ?? err?.response?.data?.status
    ?? err?.data?.status
    ?? err?.data?.statusCode
    ?? err?.result?.status
    ?? err?.result?.statusCode
    ?? err?.statusCode
    ?? NaN,
  );
  let statusCode = Number.isFinite(status) ? Number(status) : null;

  let safeMsg = '';
  if (typeof err?.data?.error === 'string' && err.data.error.trim()) safeMsg = err.data.error.trim();
  else if (typeof err?.error === 'string' && err.error.trim()) safeMsg = err.error.trim();
  else safeMsg = (err instanceof Error ? err.message : String(err || '')).trim();
  if (safeMsg.length > 160) safeMsg = safeMsg.slice(0, 160);
  if (!Number.isFinite(statusCode)) {
    const m = safeMsg.match(/\b([1-5][0-9]{2})\b/);
    if (m) statusCode = Number(m[1]);
  }

  const msg = safeMsg.toLowerCase();
  if (msg.includes('trading restricted in your region') || msg.includes('geoblock')) {
    return { reason: 'geoblock', upstreamStatus: statusCode ?? 403, upstreamMessage: 'trading restricted in your region' };
  }
  if (msg.includes('could not create api key')) {
    return { reason: 'api_key_create_failed', upstreamStatus: statusCode ?? 400, upstreamMessage: 'could not create api key' };
  }
  if (msg.includes('api credentials are needed')) {
    return { reason: 'api_key_create_failed', upstreamStatus: statusCode ?? 400, upstreamMessage: 'api credentials are needed' };
  }
  return {
    reason: 'clob_error',
    upstreamStatus: Number.isFinite(statusCode) ? statusCode : 500,
    upstreamMessage: safeMsg || 'unknown clob error',
  };
}

function extractRequiredFeeBpsFromMessage(message) {
  const msg = String(message || '');
  const m = msg.match(/taker fee:\s*([0-9]+)/i);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isInteger(n) && n >= 0 ? n : null;
}

function computeRequestDryRun(mode) {
  return mode === 'test'
    || ENV.POLY_DRY_RUN
    || !ENV.POLY_LIVE_ENABLED
    || ENV.POLY_LIVE_CONFIRM !== 'I_UNDERSTAND';
}

function resolveSignatureType() {
  if (Number.isInteger(ENV.POLY_SIGNATURE_TYPE) && ENV.POLY_SIGNATURE_TYPE >= 0 && ENV.POLY_SIGNATURE_TYPE <= 2) {
    return ENV.POLY_SIGNATURE_TYPE;
  }
  return 0;
}

async function resolveProofIdentityFields() {
  const signatureType = resolveSignatureType();
  let signerAddress = null;
  if (ENV.POLY_PRIVATE_KEY) {
    try {
      const walletMod = await import('@ethersproject/wallet');
      const Wallet = walletMod?.Wallet || walletMod?.default;
      if (Wallet) signerAddress = new Wallet(ENV.POLY_PRIVATE_KEY).address;
    } catch {
      signerAddress = null;
    }
  }
  const funderAddress = ENV.POLY_FUNDER_ADDRESS || signerAddress || null;
  return { signerAddress, funderAddress, signatureType };
}

async function placeOrderViaClob({ direction, yesTokenId, noTokenId, clientOrderId, midPrice, runRid }) {
  const intent = buildOrderIntent(direction, yesTokenId, noTokenId);
  const amountUsd = ENV.POLY_ORDER_USD;
  if (ENV.POLY_DRY_RUN || ENV.POLY_KILL_SWITCH) {
    return {
      skipped: true,
      reason: ENV.POLY_KILL_SWITCH ? 'kill_switch' : 'dry_run',
      intent: { ...intent, amountUsd, clientOrderId, midPrice },
    };
  }

  const mod = await import('@polymarket/clob-client');
  const walletMod = await import('@ethersproject/wallet');
  const ClobClient = mod?.ClobClient || mod?.default?.ClobClient || mod?.default;
  const Wallet = walletMod?.Wallet || walletMod?.default;
  if (!ClobClient) {
    throw new Error('clob_client_constructor_not_found');
  }
  if (!Wallet) {
    throw new Error('ethers_wallet_constructor_not_found');
  }

  const signer = new Wallet(ENV.POLY_PRIVATE_KEY);
  const client = new ClobClient(
    ENV.POLY_CLOB_HOST,
    137,
    signer,
    undefined,
    resolveSignatureType(),
    ENV.POLY_FUNDER_ADDRESS || undefined,
  );
  const maybeCreate = client.createOrDeriveApiKey || client.createApiKey || client.deriveApiKey;
  if (typeof maybeCreate === 'function') {
    const credsRaw = await maybeCreate.call(client);
    if (credsRaw && typeof credsRaw === 'object') {
      const normalized = {
        key: credsRaw.key || credsRaw.apiKey || '',
        secret: credsRaw.secret || '',
        passphrase: credsRaw.passphrase || '',
      };
      if (normalized.key && normalized.secret && normalized.passphrase) client.creds = normalized;
    }
  }
  const feeResolved = await resolveDynamicFeeRateBps(client, null, null);
  if (!Number.isInteger(feeResolved.feeRateBps)) {
    return {
      skipped: true,
      reason: 'fee_rate_unavailable',
      upstreamMessage: 'fee not found in clob/gamma',
      feeRateBpsUsed: null,
      feeSource: null,
      feeRaw: null,
      orderRequest: sanitizeOrderRequest({
        tokenID: intent.tokenId,
        side: 'BUY',
        amount: Number(amountUsd),
        price: normalizeOrderPrice(midPrice),
        feeRateBps: null,
        orderType: ENV.POLY_ORDER_TYPE,
      }),
      intent: { ...intent, amountUsd, clientOrderId, midPrice },
    };
  }
  feeLog({ rid: runRid, feeRateBps: feeResolved.feeRateBps, tokenId: intent.tokenId });
  const orderRequest = {
    tokenID: intent.tokenId,
    side: 'BUY',
    amount: Number(amountUsd),
    price: normalizeOrderPrice(midPrice),
    feeRateBps: feeResolved.feeRateBps,
  };
  const posted = await postMarketOrderWithClient(client, mod, orderRequest);
  if (posted?.preflightFailed) {
    return {
      skipped: true,
      reason: posted.reason || 'amount_precision_preflight_failed',
      orderRequest: sanitizeOrderRequest(posted.finalOrderRequest || orderRequest),
      precisionApplied: posted.precisionApplied || null,
      clobPayloadPosted: posted.clobPayloadPosted || null,
      intent: { ...intent, amountUsd, clientOrderId, midPrice },
    };
  }
  const { result, orderType, finalOrderRequest, precisionApplied } = posted;
  return {
    skipped: false,
    result,
    intent: { ...intent, amountUsd, clientOrderId, midPrice, orderType },
    feeRateBpsUsed: feeResolved.feeRateBps,
    feeSource: feeResolved.feeSource,
    feeRaw: feeResolved.feeRaw ?? null,
    orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || orderRequest), orderType }),
    precisionApplied,
  };
}

async function placeOutcomeOrder({
  outcome,
  tokenId,
  side = 'BUY',
  notionalUSD,
  clientOrderId,
  mode,
  dryRun,
  midPrice,
  runRid,
  decision,
  bucketKey,
  conditionId,
  fallbackTakerBaseFee,
}) {
  const orderSide = String(side || 'BUY').toUpperCase() === 'SELL' ? 'SELL' : 'BUY';
  if (mode === 'test') {
    return {
      skipped: true,
      reason: 'mode_test',
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  if (mode === 'live' && dryRun) {
    return {
      skipped: true,
      reason: 'dry_run_gate',
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }
  if (ENV.POLY_KILL_SWITCH) {
    return {
      skipped: true,
      reason: 'kill_switch',
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  if (!ENV.POLY_PRIVATE_KEY) {
    return {
      skipped: true,
      reason: 'missing_private_key',
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  const feeOverride = validateManualFeeRateOverride(ENV.POLY_FEE_RATE_BPS_RAW);
  if (feeOverride.hasOverride && feeOverride.ok === false) {
    const parsedRaw = Number(ENV.POLY_FEE_RATE_BPS_RAW);
    feeLog({ rid: runRid, feeRateBps: Number.isFinite(parsedRaw) ? Math.floor(parsedRaw) : null, tokenId });
    return {
      skipped: true,
      reason: 'invalid_fee_rate',
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  const geo = await fetchGeoblockStatus();
  if (geo?.blocked) {
    const detail = [geo.country, geo.region].filter(Boolean).join('/');
    return {
      skipped: true,
      reason: 'geoblock',
      upstreamStatus: 403,
      upstreamMessage: detail ? `trading restricted in your region (${detail})` : 'trading restricted in your region',
      geo,
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  try {
    const { client, mod } = await loadClobClient();

    const amountUsdNum = normalizeUsdAmount(notionalUSD);
    if (!Number.isFinite(amountUsdNum) || amountUsdNum <= 0) throw new Error('invalid_notional_usd');

    const configuredOrderType = String(ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase();
    const canUseClientConvenience = typeof client.createAndPostMarketOrder === 'function';
    const feeResolved = feeOverride.hasOverride
      ? { feeRateBps: feeOverride.feeRateBps, feeSource: 'override', feeRaw: feeOverride.feeRateBps }
      : (canUseClientConvenience
        ? { feeRateBps: null, feeSource: 'client', feeRaw: null }
        : await resolveDynamicFeeRateBps(client, conditionId, fallbackTakerBaseFee));
    const feeRateBps = feeResolved.feeRateBps;
    const feeSource = feeResolved.feeSource;
    const feeRaw = feeResolved.feeRaw ?? null;
    if (!canUseClientConvenience && !Number.isInteger(feeRateBps)) {
      return {
        skipped: true,
        reason: 'fee_rate_unavailable',
        upstreamMessage: 'fee not found in clob/gamma',
        feeRateBpsUsed: null,
        feeSource: null,
        feeRaw: null,
        orderRequest: sanitizeOrderRequest({
          tokenID: tokenId,
          side: orderSide,
          amount: amountUsdNum,
          price: normalizeOrderPrice(midPrice),
          feeRateBps: null,
          orderType: ENV.POLY_ORDER_TYPE,
        }),
        intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
      };
    }
    feeLog({ rid: runRid, feeRateBps, tokenId });

    const baseOrderRequest = {
      tokenID: tokenId,
      side: orderSide,
      amount: amountUsdNum,
    };
    const price = normalizeOrderPrice(midPrice);
    const orderRequest = feeOverride.hasOverride
      ? { ...baseOrderRequest, price, feeRateBps }
      : (canUseClientConvenience ? baseOrderRequest : { ...baseOrderRequest, price, feeRateBps });

    if (configuredOrderType === 'GTC') {
      const ttlMs = Math.max(1000, Number(ENV.POLY_GTC_TTL_MS) || 30000);
      const maxAttempts = Math.max(1, Number(ENV.POLY_GTC_MAX_ATTEMPTS) || 3);
      const pollMs = Math.max(200, Number(ENV.POLY_GTC_POLL_MS) || 800);
      const forceFillSteps = ENV.POLY_FORCE_FILL ? parseForceFillPriceSteps(midPrice) : [];
      const effectiveAttempts = Math.max(maxAttempts, forceFillSteps.length || 0);
      const previousOpen = state.gtcOpenByToken[tokenId];
      if (previousOpen && previousOpen.orderId) {
        const prevBucket = Number(previousOpen.bucketKey);
        const shouldCancel = !Number.isFinite(prevBucket)
          || prevBucket !== bucketKey
          || ENV.POLY_CANCEL_ON_NEW_BUCKET;
        if (shouldCancel) {
          await cancelOrderById(client, previousOpen.orderId);
          delete state.gtcOpenByToken[tokenId];
        }
      }

      let lastOrderId = null;
      let lastStatus = 'UNKNOWN';
      for (let attemptCount = 1; attemptCount <= effectiveAttempts; attemptCount += 1) {
        if (state.gtcOpenByToken[tokenId]?.orderId) {
          await cancelOrderById(client, state.gtcOpenByToken[tokenId].orderId);
          delete state.gtcOpenByToken[tokenId];
        }
        const attemptPrice = (ENV.POLY_FORCE_FILL && !canUseClientConvenience)
          ? (forceFillSteps[Math.min(attemptCount - 1, forceFillSteps.length - 1)] || 0.99)
          : null;
        const attemptOrderRequest = (ENV.POLY_FORCE_FILL && !canUseClientConvenience)
          ? { ...orderRequest, price: Number(attemptPrice.toFixed(4)) }
          : orderRequest;
        const posted = await postMarketOrderWithClient(client, mod, attemptOrderRequest, 'GTC');
        if (posted?.preflightFailed) {
          return {
            skipped: true,
            reason: posted.reason || 'amount_precision_preflight_failed',
            feeRateBpsUsed: feeRateBps,
            feeSource,
            feeRaw,
            orderType: 'GTC',
            bucketKey,
            attemptCount,
            ttlMs,
            status: 'REJECTED',
            orderRequest: sanitizeOrderRequest(posted.finalOrderRequest || attemptOrderRequest),
            precisionApplied: posted.precisionApplied || null,
            clobPayloadPosted: posted.clobPayloadPosted || null,
            ...(geo ? { geo } : {}),
            intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
          };
        }
        const { result, orderType, finalOrderRequest, precisionApplied } = posted;
        if (result?.error) {
          const mapped = normalizeClobErrorReason(result.error);
          return {
            skipped: true,
            reason: mapped.reason,
            upstreamStatus: mapped.upstreamStatus,
            upstreamMessage: mapped.upstreamMessage,
            feeRateBpsUsed: feeRateBps,
            feeSource,
            feeRaw,
            orderType: 'GTC',
            bucketKey,
            attemptCount,
            ttlMs,
            status: 'REJECTED',
            orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || attemptOrderRequest), orderType: 'GTC' }),
            precisionApplied,
            clobPayloadPosted: posted.clobPayloadPosted || null,
            ...(geo ? { geo } : {}),
            intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
            result,
          };
        }

        const orderId = extractOrderId(result);
        const initialStatus = normalizeOrderStatus(result);
        lastOrderId = orderId;
        lastStatus = initialStatus;
        if (orderId) {
          state.gtcOpenByToken[tokenId] = { orderId, bucketKey, decision };
        }

        if (initialStatus === 'FILLED') {
          delete state.gtcOpenByToken[tokenId];
          return {
            skipped: false,
            feeRateBpsUsed: feeRateBps,
            feeSource,
            feeRaw,
            orderType: 'GTC',
            bucketKey,
            attemptCount,
            ttlMs,
            orderId,
            status: 'FILLED',
            filled: true,
            orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || attemptOrderRequest), orderType: 'GTC' }),
            precisionApplied,
            clobPayloadPosted: posted.clobPayloadPosted || null,
            ...(geo ? { geo } : {}),
            result,
            intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice, orderType },
          };
        }

        const startedMs = Date.now();
        while ((Date.now() - startedMs) < ttlMs) {
          await sleepMs(pollMs);
          if (!orderId) break;
          const polledStatus = await fetchOrderStatus(client, orderId);
          if (!polledStatus) continue;
          lastStatus = polledStatus;
          if (polledStatus === 'FILLED') {
            delete state.gtcOpenByToken[tokenId];
            return {
              skipped: false,
              feeRateBpsUsed: feeRateBps,
              feeSource,
              feeRaw,
              orderType: 'GTC',
              bucketKey,
              attemptCount,
              ttlMs,
              orderId,
              status: 'FILLED',
              filled: true,
              orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || attemptOrderRequest), orderType: 'GTC' }),
              precisionApplied,
              clobPayloadPosted: posted.clobPayloadPosted || null,
              ...(geo ? { geo } : {}),
              result,
              intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice, orderType },
            };
          }
        }

        const isLastAttempt = attemptCount >= effectiveAttempts;
        if (orderId && !(ENV.POLY_FORCE_FILL && isLastAttempt)) {
          await cancelOrderById(client, orderId);
          delete state.gtcOpenByToken[tokenId];
        }
      }

      if (ENV.POLY_FORCE_FILL && lastOrderId) {
        state.gtcOpenByToken[tokenId] = { orderId: lastOrderId, bucketKey, decision };
        return {
          skipped: true,
          reason: 'open_wait_fill_force_fill',
          feeRateBpsUsed: feeRateBps,
          feeSource,
          feeRaw,
          orderType: 'GTC',
          bucketKey,
          attemptCount: effectiveAttempts,
          ttlMs,
          orderId: lastOrderId,
          status: lastStatus || 'OPEN',
          filled: false,
          orderRequest: sanitizeOrderRequest({ ...orderRequest, orderType: 'GTC' }),
          precisionApplied: { makerDecimals: 4, takerDecimals: 2, makerWas: null, takerWas: orderRequest?.amount ?? null },
          clobPayloadPosted: null,
          ...(geo ? { geo } : {}),
          intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
        };
      }

      return {
        skipped: true,
        reason: 'gtc_unfilled_after_retries',
        feeRateBpsUsed: feeRateBps,
        feeSource,
        feeRaw,
        orderType: 'GTC',
        bucketKey,
        attemptCount: effectiveAttempts,
        ttlMs,
        ...(lastOrderId ? { orderId: lastOrderId } : {}),
        status: lastStatus || 'CANCELED',
        filled: false,
        orderRequest: sanitizeOrderRequest({ ...orderRequest, orderType: 'GTC' }),
        precisionApplied: { makerDecimals: 4, takerDecimals: 2, makerWas: null, takerWas: orderRequest?.amount ?? null },
        clobPayloadPosted: null,
        ...(geo ? { geo } : {}),
        intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
      };
    }

    const posted = await postMarketOrderWithClient(client, mod, orderRequest, configuredOrderType);
    if (posted?.preflightFailed) {
      return {
        skipped: true,
        reason: posted.reason || 'amount_precision_preflight_failed',
        feeRateBpsUsed: feeRateBps,
        feeSource,
        feeRaw,
        orderType: configuredOrderType,
        bucketKey,
        attemptCount: 1,
        ttlMs: null,
        orderRequest: sanitizeOrderRequest(posted.finalOrderRequest || orderRequest),
        precisionApplied: posted.precisionApplied || null,
        clobPayloadPosted: posted.clobPayloadPosted || null,
        ...(geo ? { geo } : {}),
        intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
      };
    }
    const { result, orderType, finalOrderRequest, precisionApplied } = posted;
    if (result?.error) {
      const mapped = normalizeClobErrorReason(result.error);
      const requiredFeeBps = extractRequiredFeeBpsFromMessage(mapped.upstreamMessage);
      if (configuredOrderType === 'FOK' && mapped.reason === 'clob_error' && Number.isInteger(requiredFeeBps) && requiredFeeBps > 0) {
        const retryRequest = { ...orderRequest, feeRateBps: requiredFeeBps };
        feeLog({ rid: runRid, feeRateBps: requiredFeeBps, tokenId });
        const retried = await postMarketOrderWithClient(client, mod, retryRequest, configuredOrderType);
        if (!retried?.result?.error) {
          return {
            skipped: false,
            feeRateBpsUsed: requiredFeeBps,
            feeSource: 'clob_market',
            orderType: configuredOrderType,
            bucketKey,
            attemptCount: 1,
            ttlMs: null,
            orderRequest: sanitizeOrderRequest({ ...(retried.finalOrderRequest || retryRequest), orderType: retried.orderType }),
            precisionApplied: retried.precisionApplied,
            clobPayloadPosted: retried.clobPayloadPosted || null,
            ...(geo ? { geo } : {}),
            result: retried.result,
            intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice, orderType: retried.orderType },
          };
        }
      }
      return {
        skipped: true,
        reason: mapped.reason,
        upstreamStatus: mapped.upstreamStatus,
        upstreamMessage: mapped.upstreamMessage,
        feeRateBpsUsed: feeRateBps,
        feeSource,
        feeRaw,
        orderType: configuredOrderType,
        bucketKey,
        attemptCount: 1,
        ttlMs: null,
        orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || orderRequest), orderType }),
        precisionApplied,
        clobPayloadPosted: posted.clobPayloadPosted || null,
        ...(geo ? { geo } : {}),
        intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
        result,
      };
    }
    return {
      skipped: false,
      feeRateBpsUsed: feeRateBps,
      feeSource,
      feeRaw,
      orderType: configuredOrderType,
      bucketKey,
      attemptCount: 1,
      ttlMs: null,
      orderRequest: sanitizeOrderRequest({ ...(finalOrderRequest || orderRequest), orderType }),
      precisionApplied,
      clobPayloadPosted: posted.clobPayloadPosted || null,
      ...(geo ? { geo } : {}),
      result,
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice, orderType },
    };
  } catch (err) {
    const mapped = normalizeClobErrorReason(err);
    const precisionSeed = applyMakerTakerPrecision({
      tokenID: String(tokenId || ''),
      side: orderSide,
      amount: Number(notionalUSD ?? NaN),
      price: normalizeOrderPrice(midPrice),
      feeRateBps: Number.isInteger(feeOverride?.feeRateBps) ? feeOverride.feeRateBps : 0,
      orderType: ENV.POLY_ORDER_TYPE,
    });
    return {
      skipped: true,
      reason: mapped.reason,
      upstreamStatus: mapped.upstreamStatus,
      upstreamMessage: mapped.upstreamMessage,
      orderRequest: sanitizeOrderRequest(precisionSeed.orderRequest),
      precisionApplied: precisionSeed.precisionApplied,
      clobPayloadPosted: null,
      feeRateBpsUsed: Number.isInteger(feeOverride?.feeRateBps) ? feeOverride.feeRateBps : 0,
      feeSource: Number.isInteger(feeOverride?.feeRateBps) ? 'override' : 'default_zero',
      feeRaw: Number.isInteger(feeOverride?.feeRateBps) ? feeOverride.feeRateBps : null,
      orderType: String(ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase(),
      bucketKey,
      attemptCount: 1,
      ttlMs: null,
      ...(geo ? { geo } : {}),
      intent: { side: orderSide, tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }
}

function tickLog(line) {
  console.log(JSON.stringify(line));
}

function decisionLog(line) {
  console.log('DECISION:', JSON.stringify(line));
}

function normalizeExecState(execState) {
  const filledRaw = execState?.filledBuckets && typeof execState.filledBuckets === 'object' ? execState.filledBuckets : {};
  const filledBuckets = {};
  for (const [k, v] of Object.entries(filledRaw)) {
    if (v) filledBuckets[String(k)] = true;
  }
  const openTrades = Array.isArray(execState?.openTrades) ? execState.openTrades : [];
  const normalizedOpenTrades = openTrades
    .filter((x) => x && typeof x === 'object')
    .map((x) => ({
      bucketKey: Number.isFinite(Number(x.bucketKey)) ? Number(x.bucketKey) : null,
      marketSlug: String(x.marketSlug || ''),
      conditionId: String(x.conditionId || ''),
      tokenId: String(x.tokenId || ''),
      side: String(x.side || '').toUpperCase() === 'DOWN' ? 'DOWN' : 'UP',
      notionalUSD: Math.max(0, safeNum(x.notionalUSD, 0)),
      priceEntry: Number.isFinite(Number(x.priceEntry)) ? Number(x.priceEntry) : null,
      sizeEntry: Number.isFinite(Number(x.sizeEntry)) ? Number(x.sizeEntry) : null,
      createdAtMs: Number.isFinite(Number(x.createdAtMs)) ? Number(x.createdAtMs) : Date.now(),
      status: String(x.status || 'open'),
      exit: x.exit && typeof x.exit === 'object' ? x.exit : null,
      settlement: x.settlement && typeof x.settlement === 'object' ? x.settlement : null,
    }));
  return {
    ...DEFAULT_EXECUTE_STATE,
    ...execState,
    step: clamp(safeNum(execState?.step, 0), 0, LADDER.length - 1),
    lossStreak: Math.max(0, safeNum(execState?.lossStreak, 0)),
    cumulativeLossUSD: Math.max(0, safeNum(execState?.cumulativeLossUSD, 0)),
    lastBucketKeyPlaced: Number.isFinite(Number(execState?.lastBucketKeyPlaced)) ? Number(execState.lastBucketKeyPlaced) : null,
    lastResolvedBucketKey: Number.isFinite(Number(execState?.lastResolvedBucketKey)) ? Number(execState.lastResolvedBucketKey) : null,
    pausedUntilBucket: Number.isFinite(Number(execState?.pausedUntilBucket)) ? Number(execState.pausedUntilBucket) : null,
    filledBuckets,
    openTrades: normalizedOpenTrades,
  };
}

async function reconcileExecuteState(execState, currentBucketKey) {
  const next = normalizeExecState(execState);
  const openTrades = Array.isArray(next.openTrades) ? next.openTrades : [];
  const unresolved = openTrades
    .filter((t) => t.status === 'open' && Number.isFinite(Number(t.bucketKey)) && Number(t.bucketKey) < currentBucketKey)
    .sort((a, b) => Number(a.bucketKey) - Number(b.bucketKey));

  let reconcileReason = null;
  let changed = false;
  for (const trade of unresolved) {
    try {
      const payload = await fetchJson(`${ENV.POLY_GAMMA_HOST}/markets/slug/${encodeURIComponent(trade.marketSlug)}`);
      const market = Array.isArray(payload) ? payload[0] : payload;
      if (!market || !market.closed) continue;
      const winner = deriveWinnerFromOutcomePrices(market);
      if (!winner || (winner !== 'UP' && winner !== 'DOWN')) {
        reconcileReason = reconcileReason || 'reconcile_unavailable';
        continue;
      }
      const settled = applySettlementOutcome(next, trade, winner, currentBucketKey, LADDER.length - 1);
      next.step = settled.state.step;
      next.lossStreak = settled.state.lossStreak;
      next.cumulativeLossUSD = settled.state.cumulativeLossUSD;
      next.pausedUntilBucket = settled.state.pausedUntilBucket ?? next.pausedUntilBucket;
      next.lastResolvedBucketKey = Number.isFinite(Number(trade.bucketKey)) ? Number(trade.bucketKey) : next.lastResolvedBucketKey;
      trade.status = 'settled';
      trade.settlement = {
        resolvedAtMs: Date.now(),
        winningSide: winner,
        winBool: winner === trade.side,
      };
      changed = true;
      reconcileReason = settled.reason;
    } catch {
      reconcileReason = reconcileReason || 'reconcile_unavailable';
    }
  }
  if (changed) {
    next.openTrades = openTrades;
  }
  return { state: next, reconcileReason, pendingResolved: changed };
}

function computeRecoverySizing({ execState, marketOrderMinSize, forceBaseStake = false }) {
  const normalized = normalizeExecState(execState);
  const step = clamp(safeNum(normalized.step, 0), 0, LADDER.length - 1);
  const ladderNotional = forceBaseStake ? LADDER[0] : (LADDER[step] || LADDER[0]);
  const minFromEnv = Math.max(0.01, safeNum(ENV.POLY_MIN_NOTIONAL_USD, 5));
  const minFromMarket = Number.isFinite(Number(marketOrderMinSize)) && Number(marketOrderMinSize) > 0
    ? Number(marketOrderMinSize)
    : 0;
  const minNotional = Math.max(5, minFromEnv, minFromMarket);
  const maxNotional = Math.max(minNotional, safeNum(ENV.POLY_MAX_NOTIONAL_USD_HARD, 160));
  let computedNotionalUSD = clamp(ladderNotional, minNotional, maxNotional);
  const adjustedToMin = computedNotionalUSD > ladderNotional;
  return {
    step,
    ladderNotional,
    computedNotionalUSD: Number(computedNotionalUSD.toFixed(6)),
    minNotional,
    maxNotional,
    adjustedToMin,
    belowMinSize: computedNotionalUSD < minNotional,
    recoveryUnreachable: false,
    reason: null,
  };
}

function summarizeOpenTrades(execState) {
  const openTrades = Array.isArray(execState?.openTrades) ? execState.openTrades : [];
  return openTrades.map((t) => ({
    tradeId: `${String(t.bucketKey ?? '')}:${String(t.tokenId || '')}:${String(t.side || '')}`,
    bucketKey: Number.isFinite(Number(t.bucketKey)) ? Number(t.bucketKey) : null,
    side: String(t.side || ''),
    tokenId: String(t.tokenId || ''),
    status: String(t.status || 'open'),
    notionalUSD: Math.max(0, safeNum(t.notionalUSD, 0)),
    createdAtMs: Number.isFinite(Number(t.createdAtMs)) ? Number(t.createdAtMs) : null,
  }));
}

function countOpenTrades(execState) {
  const openTrades = Array.isArray(execState?.openTrades) ? execState.openTrades : [];
  const summary = { open: 0, closed_exit: 0, settled: 0, total: openTrades.length };
  for (const t of openTrades) {
    if (t.status === 'open') summary.open += 1;
    else if (t.status === 'closed_exit') summary.closed_exit += 1;
    else if (t.status === 'settled') summary.settled += 1;
  }
  return summary;
}

function oppositeSide(side) {
  return String(side || '').toUpperCase() === 'UP' ? 'DOWN' : 'UP';
}

async function fetchBestBid(tokenId) {
  try {
    const url = `${ENV.POLY_CLOB_HOST}/book?token_id=${encodeURIComponent(String(tokenId || ''))}`;
    const payload = await fetchJson(url);
    const bids = Array.isArray(payload?.bids) ? payload.bids : [];
    if (!bids.length) return null;
    const best = bids[0];
    const p = Number(best?.price ?? best?.p ?? best?.rate);
    return Number.isFinite(p) ? p : null;
  } catch {
    return null;
  }
}

async function fetchBestAsk(tokenId) {
  try {
    const url = `${ENV.POLY_CLOB_HOST}/book?token_id=${encodeURIComponent(String(tokenId || ''))}`;
    const payload = await fetchJson(url);
    const asks = Array.isArray(payload?.asks) ? payload.asks : [];
    if (!asks.length) return null;
    const best = asks[0];
    const p = Number(best?.price ?? best?.p ?? best?.rate);
    return Number.isFinite(p) ? p : null;
  } catch {
    return null;
  }
}

function exitPolicySnapshot(marketOrderMinSize) {
  return {
    minNetProfitUSD: Number(ENV.POLY_EXIT_MIN_NET_PROFIT_USD),
    maxExitAttemptsPerExecute: Math.max(1, Number(ENV.POLY_EXIT_MAX_ATTEMPTS_PER_EXECUTE) || 1),
    useBidForYesSell: true,
    useAskForNoSell: false,
    slippageBps: Number(ENV.POLY_EXIT_SLIPPAGE_BPS || 0),
    minOrderSizeUSD: Math.max(
      Number(ENV.POLY_EXIT_MIN_ORDER_USD || 5),
      Number.isFinite(Number(marketOrderMinSize)) ? Number(marketOrderMinSize) : 0,
    ),
  };
}

async function runTick(reqRid) {
  const runRid = reqRid || rid();
  const ts = new Date().toISOString();
  const nowTs = nowSec();

  const market = await resolveMarketTokens();
  const endTs = nowTs;
  const startTs = Math.max(0, endTs - ENV.POLY_LOOKBACK_SEC);
  const points = await fetchPriceHistory(market.yesTokenId, startTs, endTs);
  const candles = build5mCandles(points);

  if (candles.length < 2) {
    const result = {
      ok: true,
      rid: runRid,
      ts,
      slug: market.slug,
      yesTokenId: market.yesTokenId,
      noTokenId: market.noTokenId,
      decision: 'FLAT',
      reason: 'not_enough_5m_candles',
      candleCount: candles.length,
      dryRun: ENV.POLY_DRY_RUN,
      killSwitch: ENV.POLY_KILL_SWITCH,
    };
    state.lastRun = result;
    await saveStateToDisk();
    tickLog({ ts, rid: runRid, slug: market.slug, yesTokenId: market.yesTokenId, votes: null, indicators: null, decision: 'FLAT', reason: 'not_enough_5m_candles', dryRun: ENV.POLY_DRY_RUN, killSwitch: ENV.POLY_KILL_SWITCH });
    decisionLog({ ts, rid: runRid, marketSlug: market.slug, decision: 'FLAT', reason: 'not_enough_5m_candles', tokenId: null });
    return result;
  }

  const currentBucket = bucket5m(nowTs);
  const closed = candles.filter((c) => c.bucketStartTs < currentBucket);
  const bar = closed[closed.length - 1];

  if (!bar) {
    const result = {
      ok: true,
      rid: runRid,
      ts,
      slug: market.slug,
      yesTokenId: market.yesTokenId,
      noTokenId: market.noTokenId,
      decision: 'FLAT',
      reason: 'no_closed_bar',
      dryRun: ENV.POLY_DRY_RUN,
      killSwitch: ENV.POLY_KILL_SWITCH,
    };
    state.lastRun = result;
    await saveStateToDisk();
    tickLog({ ts, rid: runRid, slug: market.slug, yesTokenId: market.yesTokenId, votes: null, indicators: null, decision: 'FLAT', reason: 'no_closed_bar', dryRun: ENV.POLY_DRY_RUN, killSwitch: ENV.POLY_KILL_SWITCH });
    decisionLog({ ts, rid: runRid, marketSlug: market.slug, decision: 'FLAT', reason: 'no_closed_bar', tokenId: null });
    return result;
  }

  const evalOut = evaluateSignal(closed);
  if (evalOut.error) {
    const result = {
      ok: true,
      rid: runRid,
      ts,
      slug: market.slug,
      yesTokenId: market.yesTokenId,
      noTokenId: market.noTokenId,
      decision: 'FLAT',
      reason: evalOut.error,
      barCloseTs: bar.bucketStartTs,
      dryRun: ENV.POLY_DRY_RUN,
      killSwitch: ENV.POLY_KILL_SWITCH,
    };
    state.lastRun = result;
    await saveStateToDisk();
    tickLog({ ts, rid: runRid, slug: market.slug, yesTokenId: market.yesTokenId, votes: null, indicators: null, decision: 'FLAT', reason: evalOut.error, dryRun: ENV.POLY_DRY_RUN, killSwitch: ENV.POLY_KILL_SWITCH });
    decisionLog({ ts, rid: runRid, marketSlug: market.slug, decision: 'FLAT', reason: evalOut.error, tokenId: null });
    return result;
  }

  const decision = evalOut.decision;
  const reason = guardrailReason({ decision, barCloseTs: bar.bucketStartTs, nowTs });
  const clientOrderId = makeClientOrderId(market.slug, bar.bucketStartTs, decision);
  const midpoint = await fetchMidpoint(market.yesTokenId).catch(() => null);

  let order = null;
  let tradeExecuted = false;

  if (!reason) {
    order = await placeOrderViaClob({
      direction: decision,
      yesTokenId: market.yesTokenId,
      noTokenId: market.noTokenId,
      clientOrderId,
      midPrice: midpoint,
      runRid,
    });
    if (!order.skipped) {
      tradeExecuted = true;
      state.lastTradeBarClose = bar.bucketStartTs;
      state.tradeHistory.push({ ts: nowTs, direction: decision });
      trimHistoryInPlace(state.tradeHistory, nowTs);
      state.positionUsd = Number((state.positionUsd + ENV.POLY_ORDER_USD).toFixed(6));
      state.lastDirection = decision;
    }
  }

  const finalReason = reason || (order?.skipped ? order.reason : 'trade_executed');
  const result = {
    ok: true,
    rid: runRid,
    ts,
    slug: market.slug,
    yesTokenId: market.yesTokenId,
    noTokenId: market.noTokenId,
    decision,
    reason: finalReason,
    barCloseTs: bar.bucketStartTs,
    clientOrderId,
    votes: evalOut.votes,
    indicators: evalOut.indicators,
    dryRun: ENV.POLY_DRY_RUN,
    killSwitch: ENV.POLY_KILL_SWITCH,
    order,
    tradeExecuted,
    state: {
      lastTradeBarClose: state.lastTradeBarClose,
      tradesLastHour: state.tradeHistory.length,
      positionUsd: state.positionUsd,
      lastDirection: state.lastDirection,
    },
  };

  state.lastRun = result;
  await saveStateToDisk();

  tickLog({
    ts,
    rid: runRid,
    slug: market.slug,
    yesTokenId: market.yesTokenId,
    votes: evalOut.votes,
    indicators: evalOut.indicators,
    decision,
    reason: finalReason,
    dryRun: ENV.POLY_DRY_RUN,
    killSwitch: ENV.POLY_KILL_SWITCH,
  });
  decisionLog({
    ts,
    rid: runRid,
    marketSlug: market.slug,
    decision,
    reason: finalReason,
    tokenId: decision === 'LONG' ? market.yesTokenId : decision === 'SHORT' ? market.noTokenId : null,
  });

  return result;
}

async function runExecute(body, requestRid) {
  const nowMs = Date.now();
  const parsed = validateExecutePayload(body, ENV.POLY_TV_SECRET);
  if (!parsed.ok) {
    return { ok: false, status: parsed.status, error: parsed.error, rid: requestRid, ts: new Date().toISOString() };
  }

  const req = parsed.value;
  const requestDryRun = computeRequestDryRun(req.mode) || ENV.POLY_KILL_SWITCH;
  const proofIdentity = await resolveProofIdentityFields();
  const quorum = computeQuorumDecision(req.votes, req.minAgree);
  const decision = quorum.decision;
  const outcome = decision === 'UP' ? 'Up' : decision === 'DOWN' ? 'Down' : null;
  const bucketKey = intervalKeyFromTsMs(req.ts);

  const market = await resolveUpDownMarket(req.marketSlug, nowMs);
  const tokenId = decision === 'UP' ? market.yesTokenId : decision === 'DOWN' ? market.noTokenId : null;
  let execState = normalizeExecState(await loadExecuteState(req.env, market));
  const reconcile = await reconcileExecuteState(execState, bucketKey);
  execState = normalizeExecState(reconcile.state);
  const unresolvedTradesBefore = (Array.isArray(execState.openTrades) ? execState.openTrades : []).filter((t) => t.status === 'open');
  const pendingUnresolvedCountBefore = unresolvedTradesBefore.length;
  const hasPendingUnresolved = pendingUnresolvedCountBefore > 0;
  const midPrice = tokenId ? await fetchMidpoint(tokenId).catch(() => null) : null;
  const sizing = computeRecoverySizing({
    execState,
    marketOrderMinSize: market.orderMinSize,
    forceBaseStake: hasPendingUnresolved,
  });
  const exitPolicy = exitPolicySnapshot(market.orderMinSize);
  let exitAttempted = false;
  let exitEligibleCount = 0;
  let exitResult = null;
  const mkBase = () => ({
    ok: true,
    rid: requestRid,
    ts: new Date().toISOString(),
    marketSlug: market.slug,
    decision,
    outcome,
    tokenId,
    voteCounts: quorum.counts,
    voteSummary: quorum.voteSummary,
    bucketKey,
    mode: req.mode,
    dryRun: requestDryRun,
    killSwitch: ENV.POLY_KILL_SWITCH,
    signerAddress: proofIdentity.signerAddress,
    funderAddress: proofIdentity.funderAddress,
    signatureType: proofIdentity.signatureType,
    computedNotionalUSD: sizing.computedNotionalUSD,
    step: Math.max(0, Number(execState.step ?? sizing.step)),
    lossStreak: Math.max(0, safeNum(execState.lossStreak, 0)),
    pendingUnresolvedCount: (Array.isArray(execState.openTrades) ? execState.openTrades : []).filter((t) => t.status === 'open').length,
    openTradesSummary: summarizeOpenTrades(execState),
    exitAttempted,
    exitEligibleCount,
    exitResult,
    exitPolicy,
    sizing: {
      auto: false,
      step: sizing.step,
      computedNotionalUSD: sizing.computedNotionalUSD,
      max: sizing.maxNotional,
      min: sizing.minNotional,
      adjustedToMin: sizing.adjustedToMin,
      orderMinSize: market.orderMinSize,
      ladderNotional: sizing.ladderNotional,
    },
  });

  const logDecision = (resp) => {
    decisionLog({
      rid: requestRid,
      marketSlug: market.slug,
      bucketKey,
      decision,
      mode: req.mode,
      dryRun: resp?.dryRun,
      tradeExecuted: Boolean(resp?.tradeExecuted),
      deduped: Boolean(resp?.deduped),
      reason: resp?.reason,
      step: resp?.step,
      lossStreak: resp?.lossStreak,
      computedNotionalUSD: resp?.computedNotionalUSD,
      openTradesCount: Array.isArray(resp?.openTradesSummary) ? resp.openTradesSummary.filter((x) => x.status === 'open').length : null,
      exitAttempted: Boolean(resp?.exitAttempted),
      orderType: resp?.orderType ?? null,
      attemptCount: Number.isFinite(resp?.attemptCount) ? resp.attemptCount : null,
      upstreamStatus: resp?.upstreamStatus ?? null,
    });
  };

  if (reconcile.reconcileReason === 'max_step_reached_reset_pause') {
    const resp = {
      ...mkBase(),
      deduped: false,
      reason: 'max_step_reached_reset_pause',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }

  if (execState.filledBuckets[String(bucketKey)] === true) {
    const resp = {
      ...mkBase(),
      deduped: true,
      reason: 'already_filled_this_bucket',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }

  if (Number.isFinite(Number(execState.pausedUntilBucket)) && bucketKey < Number(execState.pausedUntilBucket)) {
    const resp = {
      ...mkBase(),
      deduped: false,
      reason: 'paused',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }

  const openTradeCap = Math.max(1, Number(ENV.POLY_MAX_OPEN_TRADES) || 6);
  if (pendingUnresolvedCountBefore >= openTradeCap) {
    const resp = {
      ...mkBase(),
      deduped: false,
      reason: 'open_trades_cap_reached',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }

  if (decision === 'NO_TRADE') {
    const resp = {
      ...mkBase(),
      deduped: false,
      reason: 'no_quorum',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }
  if (sizing.belowMinSize) {
    const resp = { ...mkBase(), deduped: false, reason: 'below_min_size', tradeExecuted: false };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    await saveExecuteState(execState);
    logDecision(resp);
    return resp;
  }

  const order = await placeOutcomeOrder({
    outcome,
    tokenId,
    side: 'BUY',
    notionalUSD: sizing.computedNotionalUSD,
    clientOrderId: req.clientOrderId,
    mode: req.mode,
    dryRun: requestDryRun,
    midPrice,
    runRid: requestRid,
    decision,
    bucketKey,
    conditionId: market.conditionId,
    fallbackTakerBaseFee: market.takerBaseFee,
  });

  let tradeExecuted = false;
  let reason = order.skipped ? order.reason : 'trade_executed';
  if (!order.skipped) {
    tradeExecuted = true;
    execState.openTrades = [...(Array.isArray(execState.openTrades) ? execState.openTrades : []), {
      bucketKey,
      marketSlug: market.slug,
      conditionId: String(market.conditionId || ''),
      tokenId: String(tokenId || ''),
      side: decision,
      notionalUSD: sizing.computedNotionalUSD,
      priceEntry: Number.isFinite(Number(order?.priceFinal ?? order?.clobPayloadPosted?.price)) ? Number(order.priceFinal ?? order.clobPayloadPosted.price) : null,
      sizeEntry: Number.isFinite(Number(order?.sizeFinal ?? order?.clobPayloadPosted?.size)) ? Number(order.sizeFinal ?? order.clobPayloadPosted.size) : null,
      createdAtMs: nowMs,
      status: 'open',
      exit: { attemptedBuckets: [] },
      settlement: null,
    }];
  }

  execState.lastBucketKeyPlaced = bucketKey;
  execState.filledBuckets[String(bucketKey)] = true;

  if (tradeExecuted) {
    state.lastTradeBarClose = Math.floor(req.ts / 1000 / BAR_SEC) * BAR_SEC;
    state.tradeHistory.push({ ts: Math.floor(nowMs / 1000), direction: decision });
    trimHistoryInPlace(state.tradeHistory, Math.floor(nowMs / 1000));
    state.positionUsd = Number((state.positionUsd + sizing.computedNotionalUSD).toFixed(6));
    state.lastDirection = decision;
  }

  if (ENV.POLY_EXIT_ENABLED) {
    const openTrades = (Array.isArray(execState.openTrades) ? execState.openTrades : []).filter((t) => t.status === 'open');
    let exitAttempts = 0;
    for (const trade of openTrades) {
      if (exitAttempts >= Math.max(1, Number(ENV.POLY_EXIT_MAX_ATTEMPTS_PER_EXECUTE) || 1)) break;
      const tBucket = Number(trade.bucketKey);
      if (!Number.isFinite(tBucket)) continue;
      if (bucketKey < (tBucket + Math.max(0, Number(ENV.POLY_EXIT_ONLY_AFTER_BUCKETS) || 1))) continue;

      const attemptedBuckets = Array.isArray(trade?.exit?.attemptedBuckets) ? trade.exit.attemptedBuckets.map((x) => Number(x)) : [];
      if (attemptedBuckets.includes(bucketKey)) continue;

      const bestBid = await fetchBestBid(trade.tokenId);
      const bestAsk = await fetchBestAsk(trade.tokenId);
      const entryPrice = Number.isFinite(Number(trade.priceEntry)) ? Number(trade.priceEntry) : null;
      const markPriceUsedRaw = Number.isFinite(Number(bestBid)) ? Number(bestBid) : null;
      const markPriceUsed = Number.isFinite(markPriceUsedRaw)
        ? Number((markPriceUsedRaw * (1 - (Number(ENV.POLY_EXIT_SLIPPAGE_BPS || 0) / 10000))).toFixed(6))
        : null;
      const size = Number.isFinite(Number(trade.sizeEntry))
        ? Number(trade.sizeEntry)
        : (entryPrice && entryPrice > 0 ? Number(trade.notionalUSD) / entryPrice : null);
      if (!Number.isFinite(size) || !Number.isFinite(markPriceUsed) || markPriceUsed <= 0) {
        exitResult = {
          tradeId: `${trade.bucketKey}:${trade.tokenId}:${trade.side}`,
          attempted: false,
          executed: false,
          reason: 'exit_no_liquidity',
          bestBid,
          bestAsk,
          entryPrice,
          markPriceUsed,
          estGrossPnL: null,
          estNetPnL: null,
          feeRateBpsUsed: null,
          orderType: String(ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase(),
          attemptCount: 0,
          upstreamStatus: null,
          upstreamMessage: null,
        };
        continue;
      }
      const feeBps = Number.isFinite(Number(market.takerBaseFee)) ? Number(market.takerBaseFee) : 0;
      const estGross = (markPriceUsed - (entryPrice || markPriceUsed)) * size;
      const estFees = ((trade.notionalUSD || 0) * feeBps / 10000) + ((size * markPriceUsed) * feeBps / 10000);
      let estNet = estGross - estFees;
      if (ENV.POLY_EXIT_FORCE) estNet = Number(ENV.POLY_EXIT_FORCE_NET_PROFIT_USD || 0.1);
      if (estNet < Number(ENV.POLY_EXIT_MIN_NET_PROFIT_USD || 0.05)) {
        exitResult = {
          tradeId: `${trade.bucketKey}:${trade.tokenId}:${trade.side}`,
          attempted: false,
          executed: false,
          reason: 'exit_not_profitable',
          bestBid,
          bestAsk,
          entryPrice,
          markPriceUsed,
          estGrossPnL: Number(estGross.toFixed(6)),
          estNetPnL: Number(estNet.toFixed(6)),
          feeRateBpsUsed: feeBps,
          orderType: String(ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase(),
          attemptCount: 0,
          upstreamStatus: null,
          upstreamMessage: null,
        };
        continue;
      }

      exitEligibleCount += 1;
      exitAttempted = true;
      exitAttempts += 1;
      const exitNotional = Math.max(Number(ENV.POLY_EXIT_MIN_ORDER_USD || 5), Math.min(Number(trade.notionalUSD || 0), Number(trade.notionalUSD || 0)));
      const exitOrder = await placeOutcomeOrder({
        outcome: String(trade.side || '').toUpperCase() === 'UP' ? 'Up' : 'Down',
        tokenId: trade.tokenId,
        side: 'SELL',
        notionalUSD: exitNotional,
        clientOrderId: `${req.clientOrderId}:exit:${trade.bucketKey}`,
        mode: req.mode,
        dryRun: requestDryRun,
        midPrice: markPriceUsed,
        runRid: requestRid,
        decision: String(trade.side || '').toUpperCase(),
        bucketKey,
        conditionId: trade.conditionId || market.conditionId,
        fallbackTakerBaseFee: market.takerBaseFee,
      });
      trade.exit = {
        attemptedBuckets: [...attemptedBuckets, bucketKey],
        closedAtMs: exitOrder.skipped ? null : nowMs,
        reason: exitOrder.skipped ? String(exitOrder.reason || 'exit_unfilled') : 'exit_executed_profit',
        price: exitOrder?.priceFinal ?? markPriceUsed,
        size: exitOrder?.sizeFinal ?? size,
      };
      if (!exitOrder.skipped) {
        trade.status = 'closed_exit';
        execState.step = 0;
        execState.lossStreak = 0;
        execState.cumulativeLossUSD = 0;
      }
      exitResult = {
        tradeId: `${trade.bucketKey}:${trade.tokenId}:${trade.side}`,
        attempted: true,
        executed: !exitOrder.skipped,
        reason: exitOrder.skipped ? 'exit_unfilled' : 'exit_executed_profit',
        bestBid,
        bestAsk,
        entryPrice,
        markPriceUsed,
        estGrossPnL: Number(estGross.toFixed(6)),
        estNetPnL: Number(estNet.toFixed(6)),
        feeRateBpsUsed: Number.isInteger(exitOrder?.feeRateBpsUsed) ? exitOrder.feeRateBpsUsed : feeBps,
        orderType: String(exitOrder?.orderType || ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase(),
        attemptCount: Number(exitOrder?.attemptCount || 1),
        upstreamStatus: exitOrder?.upstreamStatus ?? null,
        upstreamMessage: exitOrder?.upstreamMessage ?? null,
      };
      if (!exitOrder.skipped && reason === 'trade_executed') reason = 'exit_executed_profit';
      break;
    }
  }

  const upstreamStatus = order.skipped ? (order.upstreamStatus ?? null) : null;
  const upstreamMessage = order.skipped ? (order.upstreamMessage ?? null) : null;
  const feeRateBpsUsed = Number.isInteger(order?.feeRateBpsUsed) ? order.feeRateBpsUsed : null;
  const feeSource = typeof order?.feeSource === 'string' ? order.feeSource : null;
  const feeRaw = order?.feeRaw ?? null;
  const orderType = typeof order?.orderType === 'string' ? order.orderType : String(ENV.POLY_ORDER_TYPE || 'FOK').toUpperCase();
  const attemptCount = Number.isFinite(order?.attemptCount) ? Number(order.attemptCount) : 0;
  const ttlMs = Number.isFinite(order?.ttlMs) ? Number(order.ttlMs) : null;
  const orderStatus = typeof order?.status === 'string' ? order.status : null;
  const orderId = typeof order?.orderId === 'string' ? order.orderId : null;
  const filled = order?.filled === true;
  const orderRequest = order?.orderRequest && typeof order.orderRequest === 'object'
    ? order.orderRequest
    : null;
  const precisionApplied = order?.precisionApplied && typeof order.precisionApplied === 'object'
    ? order.precisionApplied
    : null;
  const clobPayloadPosted = order?.clobPayloadPosted && typeof order.clobPayloadPosted === 'object'
    ? order.clobPayloadPosted
    : null;
  const makerAmountFinal = order?.makerAmountFinal
    ?? clobPayloadPosted?.makerAmount
    ?? orderRequest?.makerAmount
    ?? null;
  const takerAmountFinal = order?.takerAmountFinal
    ?? clobPayloadPosted?.takerAmount
    ?? orderRequest?.takerAmount
    ?? null;
  const priceFinal = order?.priceFinal
    ?? clobPayloadPosted?.price
    ?? (orderRequest?.price ?? null);
  const sizeFinal = order?.sizeFinal
    ?? clobPayloadPosted?.size
    ?? (orderRequest?.size ?? null);
  const geo = order.geo || null;
  const resp = {
    ...mkBase(),
    deduped: false,
    reason: tradeExecuted && hasPendingUnresolved ? 'pending_unresolved_base_stake' : reason,
    feeRateBpsUsed,
    feeSource,
    feeRaw,
    makerAmountFinal,
    takerAmountFinal,
    priceFinal,
    sizeFinal,
    orderType,
    attemptCount,
    ttlMs,
    voteSummary: quorum.voteSummary,
    ...(upstreamStatus ? { upstreamStatus } : {}),
    ...(upstreamMessage ? { upstreamMessage } : {}),
    ...(orderStatus ? { orderStatus } : {}),
    ...(orderId ? { orderId } : {}),
    ...(order?.skipped ? { filled: false } : { filled }),
    ...(order.skipped && (reason === 'clob_error' || reason === 'fee_rate_unavailable' || reason === 'open_wait_fill_force_fill' || reason === 'amount_precision_preflight_failed') && orderRequest ? { orderRequest } : {}),
    ...(order.skipped && (reason === 'clob_error' || reason === 'fee_rate_unavailable' || reason === 'open_wait_fill_force_fill' || reason === 'amount_precision_preflight_failed') && precisionApplied ? { precisionApplied } : {}),
    ...(order.skipped && (reason === 'clob_error' || reason === 'fee_rate_unavailable' || reason === 'open_wait_fill_force_fill' || reason === 'amount_precision_preflight_failed') && clobPayloadPosted ? { clobPayloadPosted } : {}),
    ...(geo ? { geo: { blocked: Boolean(geo.blocked), country: geo.country || '', region: geo.region || '', ip: geo.ip || '' } } : {}),
    ...(!order.skipped ? { order } : {}),
    tradeExecuted,
    pendingUnresolvedCount: (Array.isArray(execState.openTrades) ? execState.openTrades : []).filter((t) => t.status === 'open').length,
    openTradesSummary: summarizeOpenTrades(execState),
    exitAttempted,
    exitEligibleCount,
    exitResult,
  };
  state.lastRun = resp;
  await saveStateToDisk().catch(() => {});
  await saveExecuteState(execState);
  logDecision(resp);
  return resp;
}

const app = express();
app.use(express.json({ limit: '128kb' }));

app.get('/healthz', (_req, res) => {
  res.status(200).json({ ok: true, ts: new Date().toISOString() });
});
app.get('/healthz/', (_req, res) => {
  res.status(200).json({ ok: true, ts: new Date().toISOString() });
});

app.get('/status', (_req, res) => {
  res.status(200).json({
    ok: true,
    ts: new Date().toISOString(),
    market: state.market,
    state: {
      lastTradeBarClose: state.lastTradeBarClose,
      tradesLastHour: state.tradeHistory.length,
      positionUsd: state.positionUsd,
      lastDirection: state.lastDirection,
    },
    lastRun: state.lastRun,
  });
});

async function handleDecisionRoute(req, res, forceExecute = false) {
  try {
    const requestRid = rid();
    const shouldRunExecute = forceExecute || Array.isArray(req.body?.votes);
    if (shouldRunExecute) {
      const out = await runExecute(req.body, requestRid);
      res.status(out.status || 200).json(out);
      return;
    }

    const summary = await runTick(requestRid);
    res.status(200).json(summary);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const fallback = {
      ok: false,
      status: 500,
      error: `route_error:${message.slice(0, 180)}`,
      ts: new Date().toISOString(),
      rid: rid(),
    };
    res.status(500).json(fallback);
  }
}

app.post('/tick', async (req, res) => handleDecisionRoute(req, res, false));
app.post('/execute', async (req, res) => handleDecisionRoute(req, res, true));

app.use((err, _req, res, _next) => {
  const message = err instanceof Error ? err.message : String(err);
  res.status(400).json({ ok: false, status: 400, error: `invalid_json:${message.slice(0, 160)}` });
});

async function bootstrap() {
  await loadStateFromDisk();
  app.listen(ENV.PORT, '0.0.0.0', () => {
    console.log(JSON.stringify({ ts: new Date().toISOString(), service: 'polymarket-bot', msg: 'listening', port: ENV.PORT, dryRun: ENV.POLY_DRY_RUN, killSwitch: ENV.POLY_KILL_SWITCH, slug: ENV.POLY_MARKET_SLUG || null }));
  });
}

bootstrap().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  console.error(JSON.stringify({ ts: new Date().toISOString(), service: 'polymarket-bot', level: 'fatal', error: message }));
  process.exit(1);
});
