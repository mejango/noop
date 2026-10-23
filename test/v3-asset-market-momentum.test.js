'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { assetMomentum } = require('../bot/asset-market-momentum');

test('asset momentum waits for real short and medium history before publishing labels', () => {
  const at = Date.parse('2026-10-23T12:00:00Z');
  const observedAt = new Date(at).toISOString();
  const sparse = { getAssetPriceHistory: () => [] };
  assert.deepEqual(assetMomentum(sparse, 'BTC', observedAt, 80000), {});

  const shortHistory = [{ timestamp: at - 20 * 60000, price: 79900 }];
  const short = assetMomentum({ getAssetPriceHistory: asset => {
    assert.equal(asset, 'BTC');
    return shortHistory;
  } }, 'BTC', observedAt, 80000);
  assert.equal(short.short_momentum_main, 'upward');
  assert.equal(short.medium_momentum_main, null);

  const fullHistory = Array.from({ length: 360 }, (_, i) => ({
    timestamp: at - 30 * 3600000 + i * 5 * 60000,
    price: 76000 + i * 11,
  }));
  const full = assetMomentum({ getAssetPriceHistory: () => fullHistory }, 'BTC', observedAt, 80000);
  assert.ok(['upward', 'downward', 'neutral'].includes(full.short_momentum_main));
  assert.ok(['upward', 'downward', 'neutral'].includes(full.medium_momentum_main));

  const gapped = [fullHistory[0], ...fullHistory.slice(-60)];
  const missingMedium = assetMomentum({ getAssetPriceHistory: () => gapped }, 'BTC', observedAt, 80000);
  assert.equal(missingMedium.medium_momentum_main, null);
});
