'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { analyzeMomentum } = require('../bot/market-momentum');

const now = Date.parse('2026-10-23T12:00:00Z');
const history = Array.from({ length: 7 * 24 * 6 + 1 }, (_, i) => ({
  timestamp: now - (7 * 24 * 6 - i) * 10 * 60_000,
  price: 2000 + i * 0.2 + Math.sin(i / 12) * 3,
}));

test('ETH momentum baseline on numeric millisecond spot history', () => {
  const result = analyzeMomentum(history, { now, logger: { log() {} } });
  assert.deepEqual(result.mediumTermMomentum, { main: 'upward', derivative: 'decelerating' });
  assert.deepEqual(result.shortTermMomentum, {
    main: 'neutral', derivative: 'flat',
    threeDayHigh: 2203.546000102131, threeDayLow: 2112.895236016029,
    sevenDayHigh: 2203.546000102131, sevenDayLow: 2000,
    priorSevenDayHigh: 2173.6707022417145,
  });
});
