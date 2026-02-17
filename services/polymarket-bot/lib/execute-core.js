export function normalizeVoteSide(side) {
  const v = String(side || '').toUpperCase();
  return v === 'UP' || v === 'DOWN' ? v : null;
}

export function parseJsonArrayLike(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== 'string') return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

export function validateExecutePayload(body, expectedSecret) {
  if (!body || typeof body !== 'object') {
    return { ok: false, status: 400, error: 'invalid body' };
  }

  const secret = String(body.secret || '');
  if (!expectedSecret || secret !== expectedSecret) {
    return { ok: false, status: 401, error: 'unauthorized' };
  }

  const env = String(body.env || '').toLowerCase();
  if (env !== 'mainnet' && env !== 'testnet') {
    return { ok: false, status: 400, error: 'invalid env' };
  }

  const mode = String(body.mode || '').toLowerCase();
  if (mode !== 'test' && mode !== 'live') {
    return { ok: false, status: 400, error: 'invalid mode' };
  }

  const votes = Array.isArray(body.votes) ? body.votes : [];
  if (votes.length !== 3) {
    return { ok: false, status: 400, error: 'votes must have length 3' };
  }

  const normalizedVotes = [];
  for (const vote of votes) {
    const side = normalizeVoteSide(vote?.side);
    const name = String(vote?.name || '').trim();
    if (!name || !side) {
      return { ok: false, status: 400, error: 'invalid votes format' };
    }
    normalizedVotes.push({ name, side });
  }

  const minAgree = Number.isFinite(Number(body.minAgree)) ? Number(body.minAgree) : 2;
  if (!Number.isInteger(minAgree) || minAgree < 2 || minAgree > 3) {
    return { ok: false, status: 400, error: 'invalid minAgree' };
  }

  const notionalUSD = Number(body.notionalUSD);
  if (!Number.isFinite(notionalUSD) || notionalUSD <= 0) {
    return { ok: false, status: 400, error: 'invalid notionalUSD' };
  }

  const clientOrderId = String(body.clientOrderId || '').trim();
  if (!clientOrderId) {
    return { ok: false, status: 400, error: 'invalid clientOrderId' };
  }

  const ts = Number(body.ts);
  if (!Number.isFinite(ts) || ts <= 0) {
    return { ok: false, status: 400, error: 'invalid ts' };
  }

  const marketSlug = body.marketSlug ? String(body.marketSlug).trim() : '';

  return {
    ok: true,
    value: {
      env,
      mode,
      votes: normalizedVotes,
      minAgree,
      notionalUSD,
      clientOrderId,
      ts,
      marketSlug,
    },
  };
}

export function computeQuorumDecision(votes, minAgree = 2) {
  const up = votes.reduce((n, x) => n + (x.side === 'UP' ? 1 : 0), 0);
  const down = votes.reduce((n, x) => n + (x.side === 'DOWN' ? 1 : 0), 0);

  let decision = 'NO_TRADE';
  if (up >= minAgree && up > down) decision = 'UP';
  if (down >= minAgree && down > up) decision = 'DOWN';

  return {
    decision,
    counts: { up, down },
    voteSummary: votes.map((x) => `${x.name}:${x.side}`).join(','),
  };
}

function boolishTrue(value) {
  return value === true || value === 'true' || value === 1 || value === '1';
}

function boolishFalse(value) {
  return value === false || value === 'false' || value === 0 || value === '0';
}

export function isTradableBtcUpDown5mMarket(market) {
  if (!market || typeof market !== 'object') return false;
  const slug = String(market.slug || '').toLowerCase();
  if (!slug.startsWith('btc-updown-5m-')) return false;

  const acceptingOrders = market.acceptingOrders;
  if (boolishFalse(acceptingOrders)) return false;

  const active = market.active;
  if (boolishFalse(active)) return false;

  const closed = market.closed;
  if (boolishTrue(closed)) return false;

  const approved = market.approved;
  if (boolishFalse(approved)) return false;

  return true;
}

export function extractUpDownTokens(market) {
  const outcomesRaw = parseJsonArrayLike(market?.outcomes);
  const idsRaw = parseJsonArrayLike(market?.clobTokenIds);

  let yesTokenId = '';
  let noTokenId = '';

  for (let i = 0; i < outcomesRaw.length; i += 1) {
    const outcome = String(outcomesRaw[i] || '').trim().toLowerCase();
    const tokenId = String(idsRaw[i] || '').trim();
    if (!tokenId) continue;
    if (outcome === 'up') yesTokenId = tokenId;
    if (outcome === 'down') noTokenId = tokenId;
  }

  if ((!yesTokenId || !noTokenId) && Array.isArray(market?.tokens)) {
    for (const token of market.tokens) {
      const outcome = String(token?.outcome || token?.name || '').trim().toLowerCase();
      const tokenId = String(token?.token_id || token?.id || token?.clobTokenId || '').trim();
      if (!tokenId) continue;
      if (outcome === 'up') yesTokenId = tokenId;
      if (outcome === 'down') noTokenId = tokenId;
    }
  }

  if (!yesTokenId || !noTokenId) {
    throw new Error('up/down token ids missing');
  }

  return { yesTokenId, noTokenId };
}

function candidateTime(market, nowMs) {
  const t1 = Date.parse(String(market?.eventStartTime || market?.eventStart || ''));
  const t2 = Date.parse(String(market?.endDate || market?.endDateIso || market?.end_date || ''));
  const times = [t1, t2].filter((x) => Number.isFinite(x));
  if (!times.length) return Number.POSITIVE_INFINITY;

  const future = times.filter((x) => x >= nowMs);
  if (future.length) {
    return Math.min(...future) - nowMs;
  }
  return nowMs - Math.max(...times) + 1e12;
}

export function chooseBtcUpDownMarketFromEvents(eventsPayload, nowMs = Date.now()) {
  const events = Array.isArray(eventsPayload)
    ? eventsPayload
    : Array.isArray(eventsPayload?.data)
      ? eventsPayload.data
      : [];

  const candidates = [];
  for (const event of events) {
    const markets = Array.isArray(event?.markets) ? event.markets : [];
    for (const market of markets) {
      if (!isTradableBtcUpDown5mMarket(market)) continue;
      try {
        const { yesTokenId, noTokenId } = extractUpDownTokens(market);
        candidates.push({
          slug: String(market.slug),
          yesTokenId,
          noTokenId,
          market,
          score: candidateTime(market, nowMs),
        });
      } catch {
        // skip incomplete markets
      }
    }
  }

  if (!candidates.length) {
    throw new Error('no tradable btc-updown-5m market found');
  }

  candidates.sort((a, b) => a.score - b.score);
  return candidates[0];
}

export function intervalKeyFromTsMs(tsMs) {
  return Math.floor(tsMs / 300000);
}

export function makeExecuteDedupKey(marketSlug, intervalKey, decision) {
  return `${marketSlug}:${intervalKey}:${decision}`;
}

export function isLiveExecutionEnabled(liveEnabled, liveConfirm) {
  return Boolean(liveEnabled) && String(liveConfirm || '') === 'I_UNDERSTAND';
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

export function computeExecuteNotional({
  autoSize = false,
  startNotionalUSD = 1,
  sizeMult = 2,
  maxNotionalUSD = 16,
  step = 0,
  requestNotionalUSD = 1,
  orderMinSize = null,
}) {
  const start = Number.isFinite(Number(startNotionalUSD)) && Number(startNotionalUSD) > 0 ? Number(startNotionalUSD) : 1;
  const mult = Number.isFinite(Number(sizeMult)) && Number(sizeMult) > 0 ? Number(sizeMult) : 2;
  const max = Number.isFinite(Number(maxNotionalUSD)) && Number(maxNotionalUSD) > 0 ? Number(maxNotionalUSD) : 16;
  const safeStep = Math.max(0, Number(step) || 0);

  let computedNotionalUSD = autoSize
    ? clamp(start * (mult ** safeStep), start, max)
    : Number(requestNotionalUSD);

  const minSize = Number(orderMinSize);
  let adjustedToMin = false;
  if (Number.isFinite(minSize) && minSize > 0 && computedNotionalUSD < minSize && minSize <= max) {
    computedNotionalUSD = minSize;
    adjustedToMin = true;
  }

  const belowOrderMinSize = Number.isFinite(minSize) && minSize > 0 && computedNotionalUSD < minSize;

  return {
    auto: Boolean(autoSize),
    step: safeStep,
    computedNotionalUSD: Number(computedNotionalUSD),
    max,
    adjustedToMin,
    orderMinSize: Number.isFinite(minSize) && minSize > 0 ? minSize : null,
    belowOrderMinSize,
  };
}
