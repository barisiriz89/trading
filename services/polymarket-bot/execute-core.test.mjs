import test from 'node:test';
import assert from 'node:assert/strict';
import {
  chooseBtcUpDownMarketFromEvents,
  computeQuorumDecision,
  extractUpDownTokens,
  intervalKeyFromTsMs,
  makeExecuteDedupKey,
  parseJsonArrayLike,
  validateExecutePayload,
} from './lib/execute-core.js';

test('validateExecutePayload enforces auth and required fields', () => {
  const body = {
    secret: 'ok-secret',
    env: 'mainnet',
    mode: 'live',
    votes: [
      { name: 'ema', side: 'UP' },
      { name: 'rsi', side: 'UP' },
      { name: 'donch', side: 'DOWN' },
    ],
    notionalUSD: 5,
    clientOrderId: 'cid-1',
    ts: 1771368900000,
  };

  assert.equal(validateExecutePayload({ ...body, secret: 'bad' }, 'ok-secret').status, 401);
  assert.equal(validateExecutePayload({ ...body, env: 'dev' }, 'ok-secret').status, 400);
  assert.equal(validateExecutePayload({ ...body, votes: [{ name: 'ema', side: 'UP' }] }, 'ok-secret').status, 400);
  assert.equal(validateExecutePayload({ ...body, notionalUSD: 0 }, 'ok-secret').status, 400);
  assert.equal(validateExecutePayload({ ...body, clientOrderId: '' }, 'ok-secret').status, 400);

  const valid = validateExecutePayload(body, 'ok-secret');
  assert.equal(valid.ok, true);
  assert.equal(valid.value.minAgree, 2);
});

test('computeQuorumDecision returns UP/DOWN/NO_TRADE with 2-of-3', () => {
  const up = computeQuorumDecision([
    { name: 'ema', side: 'UP' },
    { name: 'rsi', side: 'UP' },
    { name: 'donch', side: 'DOWN' },
  ]);
  assert.equal(up.decision, 'UP');

  const down = computeQuorumDecision([
    { name: 'ema', side: 'DOWN' },
    { name: 'rsi', side: 'DOWN' },
    { name: 'donch', side: 'UP' },
  ]);
  assert.equal(down.decision, 'DOWN');

  const none = computeQuorumDecision([
    { name: 'ema', side: 'UP' },
    { name: 'rsi', side: 'DOWN' },
    { name: 'donch', side: 'UP' },
  ], 3);
  assert.equal(none.decision, 'NO_TRADE');
});

test('chooseBtcUpDownMarketFromEvents selects nearest tradable market', () => {
  const now = Date.parse('2026-02-17T10:00:00Z');
  const payload = [
    {
      id: 1,
      markets: [
        {
          slug: 'btc-updown-5m-1771368000',
          acceptingOrders: true,
          active: true,
          closed: false,
          approved: true,
          eventStartTime: '2026-02-17T10:20:00Z',
          outcomes: '[\"Up\",\"Down\"]',
          clobTokenIds: '[\"token-up-far\",\"token-down-far\"]',
        },
        {
          slug: 'btc-updown-5m-1771368900',
          acceptingOrders: true,
          active: true,
          closed: false,
          approved: true,
          eventStartTime: '2026-02-17T10:05:00Z',
          outcomes: '[\"Up\",\"Down\"]',
          clobTokenIds: '[\"token-up-near\",\"token-down-near\"]',
        },
        {
          slug: 'btc-updown-5m-1771368600',
          acceptingOrders: false,
          active: true,
          closed: false,
          approved: true,
          eventStartTime: '2026-02-17T10:03:00Z',
          outcomes: ['Up', 'Down'],
          clobTokenIds: ['token-up-closed', 'token-down-closed'],
        },
      ],
    },
  ];

  const selected = chooseBtcUpDownMarketFromEvents(payload, now);
  assert.equal(selected.slug, 'btc-updown-5m-1771368900');
  assert.equal(selected.yesTokenId, 'token-up-near');
  assert.equal(selected.noTokenId, 'token-down-near');
});

test('extractUpDownTokens maps Up/Down order to yes/no token ids', () => {
  const market = {
    outcomes: '[\"Up\",\"Down\"]',
    clobTokenIds: '[\"id-up\",\"id-down\"]',
  };
  const out = extractUpDownTokens(market);
  assert.equal(out.yesTokenId, 'id-up');
  assert.equal(out.noTokenId, 'id-down');
});

test('parseJsonArrayLike parses string arrays robustly', () => {
  assert.deepEqual(parseJsonArrayLike('[\"a\",\"b\"]'), ['a', 'b']);
  assert.deepEqual(parseJsonArrayLike(['a', 'b']), ['a', 'b']);
  assert.deepEqual(parseJsonArrayLike('bad json'), []);
});

test('dedup key uses slug + 5m interval + decision', () => {
  const ts = 1771368900123;
  const interval = intervalKeyFromTsMs(ts);
  const key = makeExecuteDedupKey('btc-updown-5m-1771368900', interval, 'UP');
  assert.equal(key, 'btc-updown-5m-1771368900:5904563:UP');
});
