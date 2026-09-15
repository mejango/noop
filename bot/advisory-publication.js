'use strict';

const { instrumentFromName } = require('./observations');

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

const normalizedIds = values => [...new Set(values)].sort();
const positionIdentity = positions => positions.map(position => ({
  instrument: position.instrument_name,
  direction: position.direction,
  amount: Number(position.amount),
  entry_price: position.avg_entry_price ?? position.avg_price ?? position.average_price ?? null,
})).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

const usableNumber = value => (typeof value === 'number' || (typeof value === 'string' && value.trim() !== ''))
  && Number.isFinite(Number(value));
const usablePrice = value => usableNumber(value) && Number(value) > 0;
const heldOptionQuoteAvailability = snapshot => normalizedIds(snapshot.positions
  .filter(position => Math.abs(Number(position.amount)) > 0 && instrumentFromName(position.instrument_name))
  .map(position => position.instrument_name))
  .map(instrument => {
    const ticker = snapshot.tickerMap[instrument];
    return {
      instrument,
      present: Boolean(ticker && typeof ticker === 'object' && !Array.isArray(ticker)),
      ask: usablePrice(ticker?.a),
      bid: usablePrice(ticker?.b),
      mark: usablePrice(ticker?.M),
      // Zero is valid for Greeks. Their availability, rather than ordinary
      // numerical movement during deliberation, determines whether to rereview.
      greeks: Object.fromEntries(['d', 'g', 't', 'v', 'r'].map(key => [key, usableNumber(ticker?.option_pricing?.[key])])),
      implied_vol: usablePrice(ticker?.option_pricing?.i),
    };
  });

function compareAdvisorySnapshots(inputSnapshot, checkedSnapshot) {
  validateSnapshot(inputSnapshot);
  validateSnapshot(checkedSnapshot);
  const changes = [];
  for (const side of ['put', 'call']) {
    const before = inputSnapshot.quoteAvailability[side];
    const after = checkedSnapshot.quoteAvailability[side];
    for (const field of ['status', 'coverage_status']) {
      if (before[field] !== after[field]) changes.push(`${side} ${field}: ${before[field]} -> ${after[field]}`);
    }
    for (const field of identityFields) {
      if (JSON.stringify(normalizedIds(before[field])) !== JSON.stringify(normalizedIds(after[field]))) {
        changes.push(`${side} ${field} changed`);
      }
    }
  }
  if (JSON.stringify(positionIdentity(inputSnapshot.positions)) !== JSON.stringify(positionIdentity(checkedSnapshot.positions))) {
    changes.push('held positions changed');
  }
  if (JSON.stringify(heldOptionQuoteAvailability(inputSnapshot)) !== JSON.stringify(heldOptionQuoteAvailability(checkedSnapshot))) {
    changes.push('held option quote availability changed');
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
