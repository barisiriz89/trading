export function applySettlementOutcome(state, pendingTrade, winnerSide, currentBucketKey, maxStep = 5) {
  const side = String(pendingTrade?.side || '').toUpperCase();
  const pendingBucketKey = Number(pendingTrade?.bucketKey);
  const notionalUSD = Number(pendingTrade?.notionalUSD || 0);
  const next = {
    ...state,
    step: Number(state?.step || 0),
    lossStreak: Number(state?.lossStreak || 0),
    cumulativeLossUSD: Number(state?.cumulativeLossUSD || 0),
    pendingTrade: null,
    lastResolvedBucketKey: Number.isFinite(pendingBucketKey) ? pendingBucketKey : null,
  };

  if (winnerSide === side) {
    next.step = 0;
    next.lossStreak = 0;
    next.cumulativeLossUSD = 0;
    return { state: next, reason: 'reconciled_win' };
  }

  if (next.step >= maxStep) {
    next.step = 0;
    next.lossStreak = 0;
    next.cumulativeLossUSD = 0;
    next.pausedUntilBucket = Number(currentBucketKey) + 1;
    return { state: next, reason: 'max_step_reached_reset_pause' };
  }

  next.step += 1;
  next.lossStreak += 1;
  next.cumulativeLossUSD = Number((next.cumulativeLossUSD + (Number.isFinite(notionalUSD) ? Math.max(0, notionalUSD) : 0)).toFixed(6));
  return { state: next, reason: 'reconciled_loss' };
}
