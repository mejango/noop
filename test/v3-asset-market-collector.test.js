'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { collectAsset } = require('../bot/asset-market-collector');

test('collects V3 market quotes and preserves the fullest hourly SQLite snapshot', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-asset-market-'));
  const oldPath = process.env.NOOP_DB_PATH;
  process.env.NOOP_DB_PATH = path.join(dir, 'noop.db');
  const db = require('../bot/db');
  const now = Date.parse('2026-10-23T12:34:00Z');
  const post = async (method, params) => {
    if (method === 'get_all_instruments') return {
      instruments: [
        { instrument_name: 'ZEC-20261030-1100-P', option_details: { expiry: 1793347200, strike: '1100', option_type: 'P' } },
        { instrument_name: 'ZEC-20261030-1400-C', option_details: { expiry: 1793347200, strike: '1400', option_type: 'C' } },
      ], pagination: { count: 2, num_pages: 1 },
    };
    if (params.instrument_type === 'perp') return { tickers: { 'ZEC-PERP': { I: '1226.86', f: '0.0000125' } } };
    return { tickers: {
      'ZEC-20261030-1100-P': { b: '20', a: '22', M: '21', I: '1226.86', B: '4', A: '5', option_pricing: { d: '-0.2', i: '0.7' }, stats: { oi: '10' } },
      'ZEC-20261030-1400-C': { b: '15', a: '17', M: '16', I: '1226.86', B: '3', A: '2', option_pricing: { d: '0.1', i: '0.6' }, stats: { oi: '12' } },
      'BTC-20261030-1400-C': { b: '1', a: '2' },
    } };
  };
  try {
    const { summary, options } = await collectAsset('ZEC', post, now);
    assert.equal(summary.hour, '2026-10-23T12:00:00Z');
    assert.equal(summary.spot, 1226.86);
    assert.equal(summary.funding_rate, 0.0000125);
    assert.equal(summary.quote_status, 'complete');
    assert.equal(summary.instrument_count, 2);
    assert.equal(summary.option_count, 2);
    assert.equal(summary.quoted_count, 2);
    assert.equal(summary.put_oi, 10);
    assert.equal(summary.call_iv, 0.6);
    assert.equal(options.length, 2);
    assert.equal(db.insertAssetMarketSnapshot(summary, options), true);
    assert.equal(db.insertAssetMarketSnapshot({ ...summary, quoted_count: 0, quote_status: 'partial' }, []), false);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_option_quotes').get().n, 2);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM spot_prices').get().n, 0);
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM options_snapshots').get().n, 0);
    assert.throws(() => db.insertAssetMarketSnapshot(summary, [{ ...options[0], instrument_name: 'BTC-20261030-1100-P' }]));
    assert.equal(db.db.prepare('SELECT count(*) AS n FROM asset_option_quotes').get().n, 2);
  } finally {
    db.close();
    delete require.cache[require.resolve('../bot/db')];
    if (oldPath == null) delete process.env.NOOP_DB_PATH;
    else process.env.NOOP_DB_PATH = oldPath;
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('empty far expiry records partial coverage and unknown aggregate values', async () => {
  const post = async (method, params) => {
    if (method === 'get_all_instruments') return { instruments: [
      { instrument_name: 'SOL-20270625-200-P', option_details: { expiry: 1813881600, strike: 200, option_type: 'P' } },
    ], pagination: { count: 1, num_pages: 1 } };
    if (params.instrument_type === 'perp') return { tickers: { 'SOL-PERP': { I: '210' } } };
    return { tickers: {} };
  };
  const { summary, options } = await collectAsset('SOL', post, Date.parse('2026-10-23T12:00:00Z'));
  assert.equal(summary.quote_status, 'partial');
  assert.equal(summary.option_count, 0);
  assert.equal(summary.put_oi, null);
  assert.equal(options.length, 0);
  await assert.rejects(collectAsset('ETH', post), /Unsupported/);
});
