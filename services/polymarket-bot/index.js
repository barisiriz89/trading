import 'dotenv/config';
import express from 'express';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  chooseBtcUpDownMarketFromEvents,
  computeExecuteNotional,
  computeQuorumDecision,
  extractUpDownTokens,
  intervalKeyFromTsMs,
  isLiveExecutionEnabled,
  makeExecuteDedupKey,
  parseJsonArrayLike,
  validateExecutePayload,
} from './lib/execute-core.js';

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
};

const BAR_SEC = 300;
const REQUIRED_CANDLES = 70;

const state = {
  market: {
    slug: ENV.POLY_MARKET_SLUG,
    yesTokenId: ENV.POLY_YES_TOKEN_ID,
    noTokenId: ENV.POLY_NO_TOKEN_ID,
    orderMinSize: null,
    loadedAtTs: null,
  },
  lastTradeBarClose: null,
  tradeHistory: [],
  positionUsd: 0,
  lastDirection: 'FLAT',
  lastRun: null,
  autoSizeStep: 0,
};

const marketCache = {
  bySlug: new Map(),
  latestAuto: null,
};

const executeDedup = new Map();

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

function purgeDedup(nowMs) {
  const cutoff = nowMs - (6 * 3600 * 1000);
  for (const [key, value] of executeDedup.entries()) {
    if (value.tsMs < cutoff) executeDedup.delete(key);
  }
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

async function resolveUpDownMarket(marketSlug, nowMs = Date.now()) {
  const override = resolveEnvTokenOverrides();
  if (override) {
    return cacheMarketEntry({
      slug: marketSlug || ENV.POLY_MARKET_SLUG || 'env-override',
      yesTokenId: override.yesTokenId,
      noTokenId: override.noTokenId,
      orderMinSize: null,
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
    return cacheMarketEntry({ slug: String(market.slug), yesTokenId, noTokenId, orderMinSize: readOrderMinSize(market) }, nowMs);
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

function computeExecuteSizing(requestNotionalUSD, marketOrderMinSize) {
  return computeExecuteNotional({
    autoSize: ENV.POLY_AUTO_SIZE,
    startNotionalUSD: ENV.POLY_START_NOTIONAL_USD,
    sizeMult: ENV.POLY_SIZE_MULT,
    maxNotionalUSD: ENV.POLY_MAX_NOTIONAL_USD,
    step: state.autoSizeStep,
    requestNotionalUSD,
    orderMinSize: marketOrderMinSize,
  });
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

function normalizeClobErrorReason(err) {
  const status = Number(
    err?.status
    ?? err?.response?.status
    ?? err?.data?.status
    ?? err?.result?.status
    ?? NaN,
  );
  const statusCode = Number.isFinite(status) ? Number(status) : null;

  let safeMsg = '';
  if (typeof err?.data?.error === 'string' && err.data.error.trim()) safeMsg = err.data.error.trim();
  else if (typeof err?.error === 'string' && err.error.trim()) safeMsg = err.error.trim();
  else safeMsg = (err instanceof Error ? err.message : String(err || '')).trim();
  if (safeMsg.length > 160) safeMsg = safeMsg.slice(0, 160);

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
  return { reason: 'clob_error', upstreamStatus: statusCode, upstreamMessage: safeMsg || 'unknown clob error' };
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
  return ENV.POLY_FUNDER_ADDRESS ? 1 : 0;
}

async function placeOrderViaClob({ direction, yesTokenId, noTokenId, clientOrderId, midPrice }) {
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

  const orderPayload = {
    tokenID: intent.tokenId,
    side: intent.side,
    orderType: 'MARKET',
    amount: String(amountUsd),
    clientOrderId,
  };

  const postOrder = client.postOrder || client.createAndPostOrder || client.placeOrder;
  if (typeof postOrder !== 'function') {
    throw new Error('clob_post_order_method_not_found');
  }

  const result = await postOrder.call(client, orderPayload);
  return {
    skipped: false,
    result,
    intent: { ...intent, amountUsd, clientOrderId, midPrice },
  };
}

async function placeOutcomeOrder({ outcome, tokenId, notionalUSD, clientOrderId, mode, dryRun, midPrice }) {
  if (mode === 'test') {
    return {
      skipped: true,
      reason: 'mode_test',
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  if (mode === 'live' && !isLiveGateEnabled()) {
    return {
      skipped: true,
      reason: 'live_not_enabled',
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  if (dryRun || ENV.POLY_KILL_SWITCH) {
    return {
      skipped: true,
      reason: ENV.POLY_KILL_SWITCH ? 'kill_switch' : 'dry_run',
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  if (!ENV.POLY_PRIVATE_KEY) {
    return {
      skipped: true,
      reason: 'missing_private_key',
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
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
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }

  try {
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

    const orderPayload = {
      tokenID: tokenId,
      side: 'BUY',
      orderType: 'MARKET',
      amount: String(notionalUSD),
      clientOrderId,
    };
    const postOrder = client.postOrder || client.createAndPostOrder || client.placeOrder;
    if (typeof postOrder !== 'function') throw new Error('clob_post_order_method_not_found');

    const result = await postOrder.call(client, orderPayload);
    if (result?.error) {
      const mapped = normalizeClobErrorReason(result.error);
      return {
        skipped: true,
        reason: mapped.reason,
        upstreamStatus: mapped.upstreamStatus,
        upstreamMessage: mapped.upstreamMessage,
        intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
        result,
      };
    }
    return {
      skipped: false,
      result,
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  } catch (err) {
    const mapped = normalizeClobErrorReason(err);
    return {
      skipped: true,
      reason: mapped.reason,
      upstreamStatus: mapped.upstreamStatus,
      upstreamMessage: mapped.upstreamMessage,
      intent: { side: 'BUY', tokenId, outcome, amountUsd: notionalUSD, clientOrderId, midPrice },
    };
  }
}

function tickLog(line) {
  console.log(JSON.stringify(line));
}

function decisionLog(line) {
  console.log('DECISION:', JSON.stringify(line));
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
  const nowTsSec = Math.floor(nowMs / 1000);
  const parsed = validateExecutePayload(body, ENV.POLY_TV_SECRET);
  if (!parsed.ok) {
    return { ok: false, status: parsed.status, error: parsed.error, rid: requestRid, ts: new Date().toISOString() };
  }

  const req = parsed.value;
  const requestDryRun = computeRequestDryRun(req.mode);
  const quorum = computeQuorumDecision(req.votes, req.minAgree);
  const decision = quorum.decision;
  const outcome = decision === 'UP' ? 'Up' : decision === 'DOWN' ? 'Down' : null;
  const intervalKey = intervalKeyFromTsMs(req.ts);

  const market = await resolveUpDownMarket(req.marketSlug, nowMs);
  const tokenId = decision === 'UP' ? market.yesTokenId : decision === 'DOWN' ? market.noTokenId : null;
  const sizing = computeExecuteSizing(req.notionalUSD, market.orderMinSize);

  const base = {
    ok: true,
    rid: requestRid,
    ts: new Date().toISOString(),
    marketSlug: market.slug,
    decision,
    outcome,
    tokenId,
    voteCounts: quorum.counts,
    voteSummary: quorum.voteSummary,
    intervalKey,
    mode: req.mode,
    dryRun: requestDryRun,
    killSwitch: ENV.POLY_KILL_SWITCH,
    sizing: {
      auto: sizing.auto,
      step: sizing.step,
      computedNotionalUSD: sizing.computedNotionalUSD,
      max: sizing.max,
      adjustedToMin: sizing.adjustedToMin,
      orderMinSize: sizing.orderMinSize,
    },
  };

  const logDecision = (resp) => {
    decisionLog({
      rid: requestRid,
      marketSlug: market.slug,
      decision,
      mode: req.mode,
      dryRun: resp?.dryRun,
      tradeExecuted: Boolean(resp?.tradeExecuted),
      deduped: Boolean(resp?.deduped),
      reason: resp?.reason,
      upstreamStatus: resp?.upstreamStatus ?? null,
      computedNotionalUSD: sizing.computedNotionalUSD,
    });
  };

  if (decision === 'NO_TRADE') {
    const resp = {
      ...base,
      deduped: false,
      reason: 'no_quorum',
      tokenId: null,
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    logDecision(resp);
    return resp;
  }

  purgeDedup(nowMs);
  const dedupKey = makeExecuteDedupKey(market.slug, intervalKey, decision);
  if (executeDedup.has(dedupKey)) {
    const resp = {
      ...base,
      deduped: true,
      dedupKey,
      reason: 'deduped',
      tradeExecuted: false,
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    logDecision(resp);
    return resp;
  }
  executeDedup.set(dedupKey, { tsMs: nowMs });

  const barCloseTs = Math.floor(req.ts / 1000 / BAR_SEC) * BAR_SEC;
  const skipForMin = sizing.belowOrderMinSize;
  const guardrail = skipForMin
    ? 'below_order_min_size'
    : guardrailReason({
      decision,
      barCloseTs,
      nowTs: nowTsSec,
      notionalUSD: sizing.computedNotionalUSD,
      mode: req.mode,
    });

  if (guardrail) {
    const resp = {
      ...base,
      deduped: false,
      dedupKey,
      reason: guardrail,
      tradeExecuted: false,
      state: {
        lastTradeBarClose: state.lastTradeBarClose,
        tradesLastHour: state.tradeHistory.length,
        positionUsd: state.positionUsd,
        lastDirection: state.lastDirection,
      },
    };
    state.lastRun = resp;
    await saveStateToDisk().catch(() => {});
    logDecision(resp);
    return resp;
  }

  const midPrice = await fetchMidpoint(tokenId).catch(() => null);
  const order = await placeOutcomeOrder({
    outcome,
    tokenId,
    notionalUSD: sizing.computedNotionalUSD,
    clientOrderId: req.clientOrderId,
    mode: req.mode,
    dryRun: requestDryRun,
    midPrice,
  });

  let tradeExecuted = false;
  if (!order.skipped) {
    tradeExecuted = true;
    state.lastTradeBarClose = barCloseTs;
    state.tradeHistory.push({ ts: nowTsSec, direction: decision });
    trimHistoryInPlace(state.tradeHistory, nowTsSec);
    state.positionUsd = Number((state.positionUsd + sizing.computedNotionalUSD).toFixed(6));
    state.lastDirection = decision;
    if (ENV.POLY_AUTO_SIZE) state.autoSizeStep += 1;
  }

  const reason = order.skipped ? order.reason : 'trade_executed';
  const upstreamStatus = order.skipped ? (order.upstreamStatus ?? null) : null;
  const upstreamMessage = order.skipped ? (order.upstreamMessage ?? null) : null;
  const resp = {
    ...base,
    deduped: false,
    dedupKey,
    reason,
    voteSummary: quorum.voteSummary,
    ...(upstreamStatus ? { upstreamStatus } : {}),
    ...(upstreamMessage ? { upstreamMessage } : {}),
    ...(!order.skipped ? { order } : {}),
    tradeExecuted,
    state: {
      lastTradeBarClose: state.lastTradeBarClose,
      tradesLastHour: state.tradeHistory.length,
      positionUsd: state.positionUsd,
      lastDirection: state.lastDirection,
    },
  };
  state.lastRun = resp;
  await saveStateToDisk().catch(() => {});
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
