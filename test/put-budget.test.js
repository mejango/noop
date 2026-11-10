'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const BOT_CONFIG = require('../bot/config.json');
const { assertPutBudgetConfig, protectedValueUsd, putWindowBudgetUsd, offDeriveEth, describePutBudget } = require('../bot/put-budget');

const config = { PUT_PROTECTED_ETH: 5, PUT_WINDOW_RATE: 0.0075, PERIOD_DAYS: 15 };

test('the shipped config is valid', () => assertPutBudgetConfig(BOT_CONFIG));

test('window budget is the window rate of protected ETH at spot', () => {
  assert.equal(protectedValueUsd(config, 2046.8), 10234);
  assert.ok(Math.abs(putWindowBudgetUsd(config, 2046.8) - 76.755) < 1e-9);
  // Four windows of spend is ~3% of protected value: $300 protects $10k.
  assert.ok(Math.abs(putWindowBudgetUsd(config, 2000) * 4 - 300) < 1e-9);
});

test('budget ignores Derive balances and is zero without a usable spot', () => {
  for (const spot of [0, -1, NaN, undefined, null, 'x']) assert.equal(putWindowBudgetUsd(config, spot), 0);
});

test('off-Derive ETH tops Derive ETH up to the protected amount and never goes negative', () => {
  assert.ok(Math.abs(offDeriveEth(config, 2.036890470862) - 2.963109529138) < 1e-12);
  assert.equal(offDeriveEth(config, 0), 5);
  assert.equal(offDeriveEth(config, 7), 0);
  assert.equal(offDeriveEth(config, undefined), 5);
});

test('invalid config fails loudly', () => {
  for (const bad of [{}, { PUT_PROTECTED_ETH: 5 }, { PUT_PROTECTED_ETH: 0, PUT_WINDOW_RATE: 0.0075 }, { PUT_PROTECTED_ETH: 5, PUT_WINDOW_RATE: 1.5 }]) {
    assert.throws(() => assertPutBudgetConfig(bad));
  }
});

test('description states the protected amount, window rate and held share', () => {
  assert.equal(describePutBudget(config), '5 ETH protected in total (including the ETH held as Derive collateral). Each 15-day window may buy puts costing 0.75% of that ETH at spot, so about 3% of protected value is held in puts across four windows. Call premium pays for the puts. Unspent budget rolls over.');
});
