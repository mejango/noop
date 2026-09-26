'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const { loadProduction } = require('./helpers/load-production');

const { buildMandelbrotSpotPathContext, formatSpotPathStructureForAdvisor, buildMandelbrotContextBlock } = loadProduction([
  'buildMandelbrotSpotPathContext', 'formatSpotPathStructureForAdvisor', 'buildMandelbrotContextBlock',
], { bindings: { path: require('node:path') } });

const HOUR = 3_600_000;
const NOW = Date.parse('2026-09-26T12:00:00Z');
const pathOf = (moves) => {
  let price = 2000;
  return moves.map((pct, i) => {
    price *= 1 + pct / 100;
    return { hour: new Date(NOW - (moves.length - i) * HOUR).toISOString(), close: price };
  });
};

test('tail and scaling statistics separate wild from mild and persistent from mean-reverting', () => {
  const n = 24 * 30;
  // Mild: steady ±0.3% noise. Wild: the same, plus rare 4% jumps.
  const mildMoves = Array.from({ length: n }, (_, i) => (i % 3 === 0 ? 0.3 : -0.15));
  const wildMoves = mildMoves.map((m, i) => (i % 97 === 50 ? (i % 2 ? 4 : -4) : m));
  const mild = buildMandelbrotSpotPathContext({ spotPrice: 2000, spotRows: pathOf(mildMoves), nowMs: NOW });
  const wild = buildMandelbrotSpotPathContext({ spotPrice: 2000, spotRows: pathOf(wildMoves), nowMs: NOW });
  assert.ok(wild.tail_and_scaling.excess_kurtosis_hourly > 10, `wild kurtosis ${wild.tail_and_scaling.excess_kurtosis_hourly}`);
  assert.ok(mild.tail_and_scaling.excess_kurtosis_hourly < 1);

  // Alternating ±1% cancels within each day: far below square-root-of-time scaling.
  const reverting = buildMandelbrotSpotPathContext({ spotPrice: 2000, spotRows: pathOf(Array.from({ length: n }, (_, i) => (i % 2 ? 1 : -1))), nowMs: NOW });
  assert.ok(reverting.tail_and_scaling.variance_ratio_24h < 0.1);
  // Trending: each day moves one way for all 24 hours, alternating by day.
  const trending = buildMandelbrotSpotPathContext({ spotPrice: 2000, spotRows: pathOf(Array.from({ length: n }, (_, i) => (Math.floor(i / 24) % 2 ? 0.2 : -0.2) + (i % 2 ? 0.05 : -0.05))), nowMs: NOW });
  assert.ok(trending.tail_and_scaling.variance_ratio_24h > 5);

  const text = formatSpotPathStructureForAdvisor(wild);
  assert.match(text, /Fat tails: excess kurtosis \d/);
  assert.match(text, /variance ratio/);
  assert.doesNotMatch(text, /samples_oldest_to_newest/);
  assert.match(formatSpotPathStructureForAdvisor(null), /unavailable/);
});

test('advisors get the Mandelbrot regime and invalidations, not the narrative', () => {
  const block = buildMandelbrotContextBlock({
    regime: 'transitional', confidence: 0.78, roughness_score: 0.71,
    geometry_notes: ['long prose'], market_rationale: 'more prose', invalidations: ['funding flips negative', 'skew collapses'],
  });
  assert.equal(block, 'Regime: transitional (confidence 0.78).\nInvalidated if: funding flips negative | skew collapses');
  assert.match(buildMandelbrotContextBlock(null), /No Mandelbrot/);
});
