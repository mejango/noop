'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { fetchV4OnchainLiquidity, lens } = require('../bot/v4-onchain-liquidity');
const { calculateValidatedLiquidityFlow, dexForAdvisor, invalidDexReason } = require('../bot/liquidity-quality');
const { repairV4Liquidity, VERSION } = require('../scripts/repair-v4-liquidity');

const nowMs = Date.parse('2026-10-11T12:00:00Z');
const block = 26168876;
const onchain = { pools: 1, totalLiquidity: 5.2e6, tvlDefinition: 'core_liquidity_principal_v1', source: 'ethereum_reserves_lens' };
const legacy = { pools: 1, totalLiquidity: 12062213.943053098, poolDetails: [{ tvlToken0: -481.389586881219, tvlToken1: 13260313.437151 }] };
function response(overrides = {}) {
  return lens.encodeFunctionResult('getPoolTVL', [[1696441826749163729465n, 974432956461n, 0n, 0n, 0n, 0n,
    3962483016802448616617960n, -198075, 288908883732590933n, overrides.block ?? block,
    '0x0000000000000000000000000000000000000000', 0, false, 0]]);
}
function rpcFixture(overrides = {}) {
  const calls = [];
  return {
    calls,
    post: async (url, body) => {
      calls.push({ url, ...body });
      let result;
      if (body.method === 'eth_chainId') result = overrides.chainId || '0x1';
      if (body.method === 'eth_getBlockByNumber') result = { number: '0x' + block.toString(16), hash: '0x' + 'a'.repeat(64), timestamp: '0x' + Math.floor((nowMs - (overrides.ageMs ?? 10000)) / 1000).toString(16) };
      if (body.method === 'eth_call') result = overrides.result || response();
      return { data: { result } };
    },
  };
}

test('onchain collector pins all reserves to one fresh Ethereum block and stores provenance without inventing volume', async () => {
  const rpc = rpcFixture();
  const sample = await fetchV4OnchainLiquidity(2500, { ...rpc, nowMs, rpcUrls: ['fixture'] });
  assert.equal(sample.sourceBlock, block);
  assert.equal(sample.poolDetails[0].rawToken0, '1696441826749163729465');
  assert.equal(sample.poolDetails[0].rawToken1, '974432956461');
  assert.equal(sample.totalLiquidity, 1696.4418267491637 * 2500 + 974432.956461);
  assert.equal(sample.totalVolume, undefined);
  assert.equal(sample.totalTxCount, undefined);
  assert.equal(rpc.calls.find(call => call.method === 'eth_call').params[1], '0x' + block.toString(16));
  assert.equal(invalidDexReason('uniswap_v4', sample), null);
});

test('wrong chains, stale blocks, block mismatch and missing ETH valuation cannot become live TVL', async () => {
  for (const overrides of [{ chainId: '0x89' }, { ageMs: 16 * 60000 }, { result: response({ block: block - 1 }) }]) {
    await assert.rejects(fetchV4OnchainLiquidity(2500, { ...rpcFixture(overrides), nowMs, rpcUrls: ['fixture'] }), /unavailable/);
  }
  await assert.rejects(fetchV4OnchainLiquidity(null), /Missing ETH/);
});

test('RPC failover does not return a replay of the old subgraph balance', async () => {
  const good = rpcFixture();
  const urls = [];
  const post = async (url, body) => {
    urls.push(url);
    if (url === 'failed') throw new Error('offline');
    return good.post(url, body);
  };
  const result = await fetchV4OnchainLiquidity(2500, { post, nowMs, rpcUrls: ['failed', 'good'] });
  assert.equal(result.source, 'ethereum_reserves_lens');
  assert.equal(urls[0], 'failed');
  assert.equal(result.stale, undefined);
});

test('source transition compares the same validated pool set instead of manufacturing a withdrawal', () => {
  const history = [1, 24, 168].map(hours => ({ timestamp: new Date(nowMs - hours * 3600000).toISOString(), dexes: { uniswap_v3: { totalLiquidity: 100 }, uniswap_v4: legacy } }));
  const flow = calculateValidatedLiquidityFlow({ dexes: { uniswap_v3: { totalLiquidity: 110 }, uniswap_v4: onchain } }, history, nowMs);
  assert.equal(flow.direction, 'inflow');
  assert.ok(Math.abs(flow.magnitude - 0.1) < 1e-12);
  assert.deepEqual(flow.timeframes.weekly.dexes, ['uniswap_v3']);
  assert.equal(flow.dataReliability, 'partial');
  assert.equal(dexForAdvisor('uniswap_v4', legacy).total_liquidity, null);
  assert.equal(dexForAdvisor('uniswap_v4', { ...legacy, poolDetails: [{ tvlToken0: 1, tvlToken1: 1 }] }).status, 'unavailable');
  assert.equal(calculateValidatedLiquidityFlow({ dexes: { uniswap_v4: onchain } }, history, nowMs).magnitude, null);
});

test('historical repair preserves hashed raw evidence, clears advisor inputs and rollups, and resumes idempotently', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-liquidity-repair-'));
  const prior = process.env.DATA_DIR;
  process.env.DATA_DIR = dir;
  const production = require('../bot/db');
  if (prior == null) delete process.env.DATA_DIR; else process.env.DATA_DIR = prior;
  const db = production.db;
  try {
    for (const hours of [168, 24, 1]) {
      production.insertOnchainData({ timestamp: new Date(nowMs - hours * 3600000).toISOString(), dexLiquidity: { dexes: { uniswap_v3: { totalLiquidity: 100 }, uniswap_v4: legacy }, flowAnalysis: { direction: 'outflow', magnitude: 0.5 } } });
    }
    production.insertTick(new Date(nowMs - 3600000).toISOString(), { price: 2500, onchain: { liquidity_flow: { direction: 'outflow', magnitude: 0.5 } } });
    production.insertOnchainData({ timestamp: new Date(nowMs).toISOString(), dexLiquidity: { dexes: { uniswap_v3: { totalLiquidity: 110 }, uniswap_v4: onchain } } });
    // Reproduce bad existing rollups generated by the previous algorithm.
    db.prepare('INSERT INTO onchain_hourly (hour,dex,tvl) VALUES (?,?,?)').run('2026-10-11T11:00:00Z', 'uniswap_v4', legacy.totalLiquidity);
    const original = db.prepare('SELECT * FROM onchain_data ORDER BY id LIMIT 1').get();
    const changes = db.prepare('SELECT total_changes() AS n').get().n;
    const dry = repairV4Liquidity(db);
    assert.equal(dry.negativeBalances, 3);
    assert.equal(db.prepare('SELECT total_changes() AS n').get().n, changes);
    const receipt = repairV4Liquidity(db, { apply: true, batchSize: 1 });
    assert.equal(receipt.repairedRows, 4);
    assert.equal(receipt.repairedTicks, 1);
    assert.equal(receipt.archiveVerified, true);
    const archive = db.prepare("SELECT * FROM liquidity_repair_archive WHERE source_table='onchain_data' AND source_id=?").get(original.id);
    assert.deepEqual(JSON.parse(archive.original_row), original);
    assert.equal(archive.sha256, crypto.createHash('sha256').update(archive.original_row).digest('hex'));
    assert.throws(() => db.prepare('DELETE FROM liquidity_repair_archive').run(), /immutable/);
    const clean = db.prepare('SELECT * FROM onchain_data ORDER BY id DESC LIMIT 1').get();
    assert.equal(JSON.parse(clean.raw_data).dexLiquidity.repairVersion, VERSION);
    assert.ok(Math.abs(clean.liquidity_flow_magnitude - 0.1) < 1e-12);
    assert.equal(db.prepare("SELECT COUNT(*) AS n FROM onchain_hourly WHERE dex='uniswap_v4'").get().n, 1);
    assert.equal(JSON.parse(db.prepare('SELECT summary FROM bot_ticks').get().summary).onchain.liquidity_flow.magnitude, null);
    const bad = JSON.parse(db.prepare('SELECT raw_data FROM onchain_data ORDER BY id LIMIT 1').get().raw_data).dexLiquidity.dexes.uniswap_v4;
    assert.equal(bad.totalLiquidity, undefined);
    assert.equal(bad.error, 'invalid_token_balances');
    const again = repairV4Liquidity(db, { apply: true });
    assert.equal(again.repairedRows, 0);
    assert.equal(again.repairedTicks, 0);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM ai_journal').get().n, 1);
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM liquidity_repair_archive').get().n, 5);
  } finally {
    production.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
