'use strict';

function failure(message, code = 'ADVISORY_SNAPSHOT_INVALID') {
  const error = new Error(message);
  error.code = code;
  return error;
}

const identityFields = ['eligible_instruments', 'quoted_instruments', 'expected_instruments',
  'missing_expected_instruments', 'unknown_delta_instruments'];

function validateSnapshot(snapshot) {
  if (!snapshot || !Number.isFinite(snapshot.spotPrice) || snapshot.spotPrice <= 0
    || !Number.isFinite(Date.parse(snapshot.marketTimestamp))
    || !Array.isArray(snapshot.positions) || !Array.isArray(snapshot.instruments)
    || !snapshot.tickerMap || typeof snapshot.tickerMap !== 'object' || Array.isArray(snapshot.tickerMap)) {
    throw failure('Advisory publication requires a valid fresh market snapshot');
  }
  for (const side of ['put', 'call']) {
    const value = snapshot.quoteAvailability?.[side];
    if (!value || !['available', 'quotes_unavailable', 'no_eligible_candidates', 'unknown'].includes(value.status)
      || !['complete', 'partial', 'unknown'].includes(value.coverage_status)
      || identityFields.some(field => !Array.isArray(value[field]) || value[field].some(id => typeof id !== 'string'))) {
      throw failure(`Advisory publication requires explicit ${side} quote coverage and identities`);
    }
  }
  if (snapshot.positions.some(position => !position || typeof position.instrument_name !== 'string'
    || !['long', 'short'].includes(position.direction) || position.amount == null || position.amount === ''
    || !Number.isFinite(Number(position.amount)))) throw failure('Advisory publication position state is malformed');
}

const positionIdentity = positions => positions.map(position => ({
  instrument: position.instrument_name,
  direction: position.direction,
  amount: Number(position.amount),
  entry_price: position.avg_entry_price ?? position.avg_price ?? position.average_price ?? null,
})).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

// ponytail: only side-level quote status and held positions trigger a rerun.
// Per-instrument identity sets and held-option bid/ask/Greek presence flicker
// on every multi-minute deliberation and were doubling model spend for nothing.
function compareAdvisorySnapshots(inputSnapshot, checkedSnapshot) {
  validateSnapshot(inputSnapshot);
  validateSnapshot(checkedSnapshot);
  const changes = [];
  for (const side of ['put', 'call']) {
    const before = inputSnapshot.quoteAvailability[side].status;
    const after = checkedSnapshot.quoteAvailability[side].status;
    if (before !== after) changes.push(`${side} status: ${before} -> ${after}`);
  }
  if (JSON.stringify(positionIdentity(inputSnapshot.positions)) !== JSON.stringify(positionIdentity(checkedSnapshot.positions))) {
    changes.push('held positions changed');
  }
  return { fresh: changes.length === 0, changes, reason: changes.join('; ') || 'Quote availability and held positions remain consistent' };
}

// All reviews finish before the final read. Only this boundary invokes publish;
// repeated quote changes and read failures leave the existing rulebook intact.
async function runReviewedPublication({ readSnapshot, review, publish, compare = compareAdvisorySnapshots,
  maxAttempts = 2, onRefresh = () => {} }) {
  if (![readSnapshot, review, publish, compare, onRefresh].every(fn => typeof fn === 'function')
    || !Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 2) {
    throw failure('Invalid advisory publication callbacks or attempt limit');
  }
  let inputSnapshot = await readSnapshot();
  validateSnapshot(inputSnapshot);
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const draft = await review(inputSnapshot, { attempt });
    if (!draft || typeof draft !== 'object') throw failure('Advisory review returned no publishable draft');
    const checkedSnapshot = await readSnapshot();
    validateSnapshot(checkedSnapshot);
    const comparison = compare(inputSnapshot, checkedSnapshot);
    if (!comparison || typeof comparison.fresh !== 'boolean') throw failure('Invalid advisory freshness comparison');
    if (comparison.fresh) return await publish(draft, { inputSnapshot, checkedSnapshot, attempt });
    if (attempt === maxAttempts) throw failure(`Advisory publication deferred: ${comparison.reason}`, 'ADVISORY_PUBLICATION_STALE');
    await onRefresh({ inputSnapshot, checkedSnapshot, attempt, comparison });
    inputSnapshot = checkedSnapshot;
  }
}

module.exports = { compareAdvisorySnapshots, runReviewedPublication };
