'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { getSpotAccumulationSettings, readSpotSourceHistory, readSpotPendingActions } = require('../bot/spot-accumulation-config');
const NOW = Date.parse('2026-11-10T16:00:00Z');
const derive = { version: 'v3', network: 'mainnet', subaccountId: 25923,
  wallet: '0x1111111111111111111111111111111111111111', historyFrom: '2026-10-06T00:00:00Z' };
const env = { SPOT_ACCUMULATION_MODE: 'dry_run', SPOT_ACCUMULATION_DESTINATION_ID: '120934',
  SPOT_ACCUMULATION_FROM: '2026-11-10T15:00:00Z' };

test('off is inert; dry run is explicit and creates no state; live requires a bounded batch', t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  const get = e => getSpotAccumulationSettings({ env: e, derive, sqlite, nowMs: NOW });
  assert.deepEqual(get({}), { mode: 'off', config: null });
  assert.equal(get(env).config.destinationAccountId, '120934');
  assert.equal(sqlite.prepare("SELECT count(*) n FROM sqlite_master").get().n, 0);
  assert.throws(() => get({ ...env, SPOT_ACCUMULATION_MODE: 'live' }), /MAX_BATCH/);
  assert.equal(get({ ...env, SPOT_ACCUMULATION_MODE: 'live', SPOT_ACCUMULATION_MAX_BATCH_USDC: '10' }).config.maxBatchUsdc, '10');
  for (const patch of [{ SPOT_ACCUMULATION_MODE: 'on' }, { SPOT_ACCUMULATION_DESTINATION_ID: '25923' },
    { SPOT_ACCUMULATION_FROM: '' }, { SPOT_ACCUMULATION_FROM: '2026-11-11' },
    { SPOT_ACCUMULATION_MAX_BATCH_USDC: '-1' }]) assert.throws(() => get({ ...env, ...patch }));
});

test('persisted identity survives off for recovery and cannot be silently rebound', t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  const config = getSpotAccumulationSettings({ env, derive, sqlite, nowMs: NOW }).config;
  sqlite.exec('CREATE TABLE spot_accumulation_config(scope TEXT,config_json TEXT)');
  sqlite.prepare('INSERT INTO spot_accumulation_config VALUES(?,?)').run('mainnet:v3:25923', JSON.stringify(config));
  assert.deepEqual(getSpotAccumulationSettings({ env: {}, derive, sqlite, nowMs: NOW }).config, config);
  for (const patch of [{ SPOT_ACCUMULATION_DESTINATION_ID: '9' }, { SPOT_ACCUMULATION_FROM: '2026-11-10T15:01:00Z' }]) {
    assert.throws(() => getSpotAccumulationSettings({ env: { ...env, ...patch }, derive, sqlite, nowMs: NOW }), /conflicts/);
  }
});

test('sample opt-in pins five dollars and persists its first startup boundary across restarts', t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  const sampleEnv = { SPOT_ACCUMULATION_MODE: 'sample', SPOT_ACCUMULATION_DESTINATION_ID: '120934' };
  const config = getSpotAccumulationSettings({ env: sampleEnv, derive, sqlite, nowMs: NOW }).config;
  assert.equal(config.maxBatchUsdc, '5');
  assert.equal(config.activationTimestamp, new Date(NOW).toISOString());
  assert.equal(sqlite.prepare('SELECT count(*) n FROM sqlite_master').get().n, 0);
  const { createSpotAccumulationStore } = require('../bot/spot-accumulation-store');
  createSpotAccumulationStore(sqlite, config, { readOnly: false }).authorizeStartupSample();
  assert.equal(getSpotAccumulationSettings({ env: sampleEnv, derive, sqlite, nowMs: NOW + 60000 }).config.activationTimestamp,
    config.activationTimestamp);
  assert.equal(getSpotAccumulationSettings({ env: {}, derive, sqlite, nowMs: NOW + 60000 }).mode, 'off');
  for (const cap of ['4', '5.01', '50']) assert.throws(() => getSpotAccumulationSettings({
    env: { ...sampleEnv, SPOT_ACCUMULATION_MAX_BATCH_USDC: cap }, derive, sqlite, nowMs: NOW }), /fixed at 5/);
  const empty = new Database(':memory:'); t.after(() => empty.close());
  assert.throws(() => getSpotAccumulationSettings({ env: { SPOT_ACCUMULATION_MODE: 'sample' }, derive,
    sqlite: empty, nowMs: NOW }), /destination/);
});

test('startup wiring uses operator funding without inventing history or a successful options cycle', async () => {
  const { declaration, SCRIPT_SOURCE } = require('./helpers/load-production');
  const vm = require('node:vm');
  const calls = [];
  const runtime = { advance: async input => { calls.push(input); return { status: 'deferred' }; } };
  const bindings = {
    process: { env: { SPOT_ACCUMULATION_MODE: 'sample' } },
    getSpotAccumulationRuntime: () => runtime,
    spotAccumulationSettings: { config: {} },
    readSpotSourceHistory: () => { throw new Error('Sample cannot ingest ordinary sale history'); },
    readSpotPendingActions: () => [{ id: 7, status: 'pending' }],
    botData: { putBudgetForCycle: 73 }, db: { db: {} }, console: { log: () => {} },
  };
  const advanceSpotAccumulation = vm.compileFunction(
    `${declaration(SCRIPT_SOURCE, 'advanceSpotAccumulation')}\nreturn advanceSpotAccumulation;`, Object.keys(bindings)
  )(...Object.values(bindings));
  await advanceSpotAccumulation(false);
  assert.equal(calls[0].sourceHistory, undefined);
  assert.equal(calls[0].optionsCycleSucceeded, false);
  assert.equal(calls[0].pendingActions[0].id, 7);
  await advanceSpotAccumulation(true);
  assert.equal(calls[1].optionsCycleSucceeded, true);
});

test('source history requires contiguous known coverage and never writes missing tables', t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  const config = getSpotAccumulationSettings({ env, derive, sqlite, nowMs: NOW }).config;
  assert.equal(readSpotSourceHistory({ sqlite, config, nowMs: NOW }).complete, false);
  assert.equal(sqlite.prepare('SELECT count(*) n FROM sqlite_master').get().n, 0);
  const { createEconomicStore } = require('../bot/economic-events'); createEconomicStore(sqlite);
  sqlite.exec('CREATE TABLE orders(action TEXT,success INTEGER)');
  const insert = sqlite.prepare('INSERT INTO economic_coverage(account_id,dataset,from_timestamp,to_timestamp,complete) VALUES(?,?,?,?,?)');
  insert.run('25923','trades','2026-11-10T15:00:00.000Z','2026-11-10T15:10:00.000Z',1);
  insert.run('25923','trades','2026-11-10T15:20:00.000Z','2026-11-10T15:30:00.000Z',1);
  assert.equal(readSpotSourceHistory({ sqlite, config, nowMs: NOW }).complete, false);
  insert.run('25923','trades','2026-11-10T15:10:00.000Z','2026-11-10T15:20:00.000Z',1);
  assert.equal(readSpotSourceHistory({ sqlite, config, nowMs: NOW }).complete, true);
});

test('runtime and diagnostics share pending commitments without double-counting resting-order ownership', t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  sqlite.exec('CREATE TABLE pending_actions(id INTEGER,triggered_at TEXT,status TEXT)');
  const insert = sqlite.prepare('INSERT INTO pending_actions VALUES(?,?,?)');
  for (const [id, status] of ['pending','confirmed','resting','executed','cancelled'].entries()) insert.run(id, '2026-11-10T15:00:00Z', status);
  assert.deepEqual(readSpotPendingActions({ sqlite }).map(row => row.status), ['pending','confirmed']);
  assert.equal(sqlite.prepare('SELECT count(*) n FROM pending_actions').get().n, 5);
});
