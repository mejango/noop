'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { loadProduction } = require('./helpers/load-production');

const { getLatestHourlyDeltaPct, enrichCandidateFromTicker } = loadProduction([
  'getLatestHourlyDeltaPct', 'enrichCandidateFromTicker',
]);
const HOUR_MS = 60 * 60 * 1000;
const NOW = Date.parse('2026-09-11T19:37:12.000Z');
const LATEST = Date.parse('2026-09-11T18:00:00.000Z');
const row = (offsetHours, value) => ({
  hour: new Date(LATEST + offsetHours * HOUR_MS).toISOString(), value,
});
const trend = (rows, lookback = 24, now = NOW) => getLatestHourlyDeltaPct(rows, lookback, now);

test('OI trend compares the previous completed UTC hour with the exact 24-hour baseline', () => {
  assert.equal(trend([row(0, 125), row(-12, 5), row(-24, 100)]), 25);
  assert.equal(trend([row(-25, 100), row(0, 125)]), null);
  assert.equal(trend([row(-23, 100), row(0, 125)]), null);
  assert.equal(trend([row(-6, 200), row(0, 100)], 6), -50);
});

test('a missing latest completed hour cannot reuse a stale or partially collected current hour', () => {
  assert.equal(trend([row(-25, 100), row(-1, 125)]), null);
  assert.equal(trend([row(-24, 100), row(1, 150)]), null);
  assert.equal(trend([row(-24, 100), row(0, 125), row(1, 1000), row(2, 2000)]), 25);
  assert.equal(trend([row(-24, 100), row(0, 125)], 24, LATEST + HOUR_MS), 25);
  assert.equal(trend([row(-24, 100), row(0, 125)], 24, LATEST + HOUR_MS - 1), null);
});

test('actual zero OI is a complete decline and a zero baseline is undefined', () => {
  assert.equal(trend([row(-24, 100), row(0, 0)]), -100);
  assert.equal(trend([row(-24, '100'), row(0, '0')]), -100);
  assert.equal(trend([row(-24, 0), row(0, 100)]), null);
  assert.equal(trend([row(-24, 0), row(0, 0)]), null);
});

test('invalid or unknown endpoints do not fall back to older positive observations', () => {
  for (const value of [undefined, null, '', ' ', -1, '-1', NaN, Infinity, 'invalid', false, true, [], {}]) {
    assert.equal(trend([row(-25, 100), row(-24, 100), row(-1, 120), row(0, value)]), null);
    assert.equal(trend([row(-25, 100), row(-24, value), row(0, 120)]), null);
  }
  assert.equal(trend([row(-24, 100), { ...row(0, null), total_oi: 125 }]), null);
  assert.equal(trend([row(-24, Number.MIN_VALUE), row(0, Number.MAX_VALUE)]), null);
});

test('hour identities must be exact and unambiguous while legacy field names remain supported', () => {
  assert.equal(trend([
    { timestamp: row(-24).hour, total_oi: 100 },
    { timestamp: row(0).hour, total_oi: 125 },
  ]), 25);
  assert.equal(trend([row(-24, 100), { hour: 'not-a-date', value: 125 }]), null);
  assert.equal(trend([row(-24, 100), { hour: new Date(LATEST + 1).toISOString(), value: 125 }]), null);
  assert.equal(trend([row(-24, 100), row(0, 125), row(0, 150)]), null);
  assert.equal(trend([row(-24, 100), row(-24, 110), row(0, 125)]), null);
  for (const lookback of [0, -1, 0.5, NaN, Infinity, '24']) {
    assert.equal(trend([row(-24, 100), row(0, 125)], lookback), null);
  }
  assert.equal(trend([row(-24, 100), row(0, 125)], 24, NaN), null);
  assert.equal(trend(null), null);
});

test('the default clock still requires the previous completed hour', () => {
  class FixedDate extends Date { static now() { return NOW; } }
  const actual = loadProduction(['getLatestHourlyDeltaPct'], { bindings: { Date: FixedDate } });
  assert.equal(actual.getLatestHourlyDeltaPct([row(-24, 100), row(0, 125)]), 25);
});

test('future candidate observations preserve zero OI and supported ticker fields', () => {
  const instrument = { instrument_name: 'ETH-20261127-1600-P', option_details: { expiry: 1795766400 } };
  const ticker = { option_pricing: { d: -0.05, i: 0.5 }, a: '10', A: '2', b: '9', B: '3', M: '9.5', I: '2300' };
  for (const fields of [
    { stats: { oi: 0 } }, { stats: { oi: '0' } },
    { stats: { open_interest: 0 } }, { open_interest: 0 }, { details: { openInterest: 0 } },
  ]) {
    assert.equal(enrichCandidateFromTicker(instrument, { ...ticker, ...fields }, 2300).details.openInterest, 0);
  }
  for (const value of [undefined, null, '', NaN, Infinity, 'invalid']) {
    assert.equal(enrichCandidateFromTicker(instrument, { ...ticker, stats: { oi: value } }, 2300).details.openInterest, null);
  }
  assert.equal(enrichCandidateFromTicker(instrument, { ...ticker, stats: { oi: '125' } }, 2300).details.openInterest, 125);
});
