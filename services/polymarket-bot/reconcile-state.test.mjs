import test from 'node:test';
import assert from 'node:assert/strict';
import { applySettlementOutcome } from './lib/reconcile-state.js';

test('applySettlementOutcome resets step/loss on WIN', () => {
  const state = { step: 3, lossStreak: 3, cumulativeLossUSD: 35, pendingTrade: { bucketKey: 10, side: 'UP', notionalUSD: 40 } };
  const out = applySettlementOutcome(state, state.pendingTrade, 'UP', 20, 5);
  assert.equal(out.reason, 'reconciled_win');
  assert.equal(out.state.step, 0);
  assert.equal(out.state.lossStreak, 0);
  assert.equal(out.state.cumulativeLossUSD, 0);
  assert.equal(out.state.pendingTrade, null);
});

test('applySettlementOutcome increments step/loss on LOSS under max step', () => {
  const state = { step: 2, lossStreak: 2, cumulativeLossUSD: 15, pendingTrade: { bucketKey: 10, side: 'UP', notionalUSD: 20 } };
  const out = applySettlementOutcome(state, state.pendingTrade, 'DOWN', 20, 5);
  assert.equal(out.reason, 'reconciled_loss');
  assert.equal(out.state.step, 3);
  assert.equal(out.state.lossStreak, 3);
  assert.equal(out.state.cumulativeLossUSD, 35);
  assert.equal(out.state.pendingTrade, null);
});

test('applySettlementOutcome resets and pauses on LOSS at max step', () => {
  const state = { step: 5, lossStreak: 5, cumulativeLossUSD: 155, pendingTrade: { bucketKey: 10, side: 'UP', notionalUSD: 160 } };
  const out = applySettlementOutcome(state, state.pendingTrade, 'DOWN', 42, 5);
  assert.equal(out.reason, 'max_step_reached_reset_pause');
  assert.equal(out.state.step, 0);
  assert.equal(out.state.lossStreak, 0);
  assert.equal(out.state.cumulativeLossUSD, 0);
  assert.equal(out.state.pausedUntilBucket, 43);
  assert.equal(out.state.pendingTrade, null);
});
