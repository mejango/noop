'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { compareAdvisorySnapshots, runReviewedPublication } = require('../bot/advisory-publication');
const { summarizeAdvisoryQuotes } = require('../bot/advisory-quotes');

const nowMs = Date.parse('2026-09-15T19:00:00Z');
const held = 'ETH-20260918-2800-C'; // Held exit is outside the CALL entry window.
const put = 'ETH-20261127-1600-P';
const call = 'ETH-20260925-2800-C';
const quote = (delta = 0.06, ask = 2.2, bid = 2) => ({
  a: ask, b: bid, M: 2.1, option_pricing: { d: delta, g: 0, t: -0.1, v: 0.02, r: 0, i: 0.6 },
});
function snapshot(heldTicker = quote(), positions = [{ instrument_name: held, direction: 'short', amount: 1 }]) {
  const tickerMap = { [put]: quote(-0.06, 20, 18), [call]: quote(0.06, 10, 8) };
  if (heldTicker !== null) tickerMap[held] = heldTicker;
  const instruments = [put, call, held].map(instrument_name => ({ instrument_name }));
  return {
    spotPrice: 2367.7, positions, tickerMap, instruments, marketTimestamp: new Date(nowMs).toISOString(),
    quoteAvailability: summarizeAdvisoryQuotes(tickerMap, { nowMs, expectedInstruments: instruments }),
  };
}

test('held option ask, bid and mark recovery outside entry eligibility invalidate the reviewed snapshot', () => {
  for (const key of ['a', 'b', 'M']) {
    const missing = quote(); missing[key] = 0;
    const comparison = compareAdvisorySnapshots(snapshot(missing), snapshot());
    assert.equal(comparison.fresh, false, key);
    assert.deepEqual(comparison.changes, ['held option quote availability changed']);
  }
});

test('held option quote presence is checked even when both records lack all usable prices', () => {
  const comparison = compareAdvisorySnapshots(snapshot(null), snapshot({}));
  assert.equal(comparison.fresh, false);
  assert.match(comparison.reason, /held option quote availability changed/);
});

test('held option Greek and IV availability changes trigger review while finite zero Greeks remain usable', () => {
  for (const key of ['d', 'g', 't', 'v', 'r', 'i']) {
    const missing = quote(); delete missing.option_pricing[key];
    assert.equal(compareAdvisorySnapshots(snapshot(missing), snapshot()).fresh, false, key);
  }
  const zeros = quote(); Object.assign(zeros.option_pricing, { d: 0, g: 0, t: 0, v: 0, r: 0 });
  assert.equal(compareAdvisorySnapshots(snapshot(zeros), snapshot()).fresh, true);
  for (const invalid of [null, undefined, '', ' ', Infinity, NaN, true, [], {}]) {
    const missing = quote(); missing.option_pricing.g = invalid;
    assert.equal(compareAdvisorySnapshots(snapshot(missing), snapshot()).fresh, false, String(invalid));
  }
});

test('routine held option prices and Greek values may move without another full review', () => {
  const moved = quote(0.08, 2.5, 2.3);
  moved.M = 2.4;
  Object.assign(moved.option_pricing, { g: 0.0002, t: -0.2, v: 0.04, r: 0.0001, i: 0.7 });
  assert.equal(compareAdvisorySnapshots(snapshot(), snapshot(moved)).fresh, true);
});

test('non-option holdings remain supported without requiring option quotes', () => {
  const positions = [{ instrument_name: 'ETH-PERP', direction: 'long', amount: 1 }];
  const before = snapshot(null, positions);
  const after = snapshot(null, positions);
  after.tickerMap['ETH-PERP'] = quote();
  assert.equal(compareAdvisorySnapshots(before, after).fresh, true);
});

test('held quote recovery discards the first draft and publishes only the complete second review', async () => {
  const unquoted = quote(); unquoted.a = 0;
  const snapshots = [snapshot(unquoted), snapshot(), snapshot()];
  const reviewed = [];
  const published = [];
  let reads = 0;
  const result = await runReviewedPublication({
    readSnapshot: async () => snapshots[reads++],
    review: async (input, { attempt }) => {
      reviewed.push(attempt);
      return { attempt, heldAsk: input.tickerMap[held].a };
    },
    publish: async draft => { published.push(draft); return draft; },
  });
  assert.deepEqual(reviewed, [1, 2]);
  assert.equal(reads, 3);
  assert.deepEqual(published, [{ attempt: 2, heldAsk: 2.2 }]);
  assert.equal(result.attempt, 2);
});

test('a second held quote availability change aborts without publishing either draft', async () => {
  const unquoted = quote(); unquoted.a = 0;
  const snapshots = [snapshot(unquoted), snapshot(), snapshot(unquoted)];
  let reads = 0, reviews = 0, publications = 0;
  await assert.rejects(runReviewedPublication({
    readSnapshot: async () => snapshots[reads++],
    review: async () => ({ attempt: ++reviews }),
    publish: async () => { publications++; },
  }), error => error.code === 'ADVISORY_PUBLICATION_STALE');
  assert.equal(reviews, 2);
  assert.equal(reads, 3);
  assert.equal(publications, 0);
});
