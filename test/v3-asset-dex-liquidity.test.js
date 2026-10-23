'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { collectAssetLiquidity, SOURCES } = require('../bot/asset-dex-liquidity');

test('BTC and ZEC Maya reserves become explicitly estimated whole-pool TVL', async () => {
  for (const asset of ['BTC', 'ZEC']) {
    const source = SOURCES[asset];
    const at = '2026-10-23T12:34:00.000Z';
    const sample = await collectAssetLiquidity(asset, at, async url => {
      assert.equal(url, source.url);
      return { ok: true, json: async () => ({ asset: `${asset}.${asset}`, status: 'available',
        assetDepth: '250000000', assetPriceUSD: '100', volume24h: '5000' }) };
    });
    assert.equal(sample.asset, asset);
    assert.equal(sample.observed_at, at);
    assert.equal(sample.tvl_usd, 500);
    assert.match(sample.source, /estimate/);
    await assert.rejects(collectAssetLiquidity(asset, at, async () => ({ ok: true,
      json: async () => ({ asset: 'ETH.ETH', status: 'available', assetDepth: '250000000', assetPriceUSD: '100' }) })),
    /identity/);
  }
});

test('fixed SOL and HYPE pools require the expected wrapped native token', async () => {
  for (const asset of ['SOL', 'HYPE']) {
    const source = SOURCES[asset];
    const body = token => ({ data: { id: `${source.network}_${source.pool}`, attributes: {
      name: source.pair, reserve_in_usd: '1250000', volume_usd: { h24: '24000' } },
      relationships: { base_token: { data: { id: `${source.network}_${asset === 'HYPE' ? source.quoteToken : token}` } },
        quote_token: { data: { id: `${source.network}_${asset === 'HYPE' ? token : source.quoteToken}` } },
        dex: { data: { id: source.dex } } } } });
    const fetchJson = data => async () => ({ ok: true, json: async () => data });
    const sample = await collectAssetLiquidity(asset, '2026-10-23T12:34:00Z', fetchJson(body(source.token)));
    assert.equal(sample.tvl_usd, 1250000);
    assert.equal(sample.volume_24h_usd, 24000);
    assert.equal(sample.pool_id, source.pool);
    await assert.rejects(collectAssetLiquidity(asset, '2026-10-23T12:34:00Z', fetchJson(body('wrong-token'))),
      /identity/);
    const wrongPair = body(source.token);
    wrongPair.data.relationships[asset === 'HYPE' ? 'base_token' : 'quote_token'].data.id = `${source.network}_wrong-quote`;
    await assert.rejects(collectAssetLiquidity(asset, '2026-10-23T12:34:00Z', fetchJson(wrongPair)),
      /identity/);
    await assert.rejects(collectAssetLiquidity(asset, '2026-10-23T12:34:00Z', fetchJson({
      ...body(source.token), data: { ...body(source.token).data,
        attributes: { name: source.pair, reserve_in_usd: '0' } } })), /reserves/);
  }
});
