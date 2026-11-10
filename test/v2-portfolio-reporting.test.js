'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Database = require('better-sqlite3');
const { createPortfolioReportingCollector, readReportingPortfolio, PERIOD_MS, FRESHNESS_MS } = require('../bot/portfolio-reporting');
const { createSpotAccumulationStore } = require('../bot/spot-accumulation-store');
const START = Date.parse('2026-11-10T15:00:00Z'), NOW = START + 3 * PERIOD_MS + 1000;
const derive = { network: 'mainnet', version: 'v3', subaccountId: 25923, wallet: '0x' + '11'.repeat(20), historyFrom: new Date(START).toISOString() };
const config = { network: derive.network, apiVersion: 'v3', accountId: '25923', destinationAccountId: '120934',
  accountAddress: derive.wallet, activationTimestamp: new Date(START).toISOString() };
function account(id, value, timestamp = NOW) {
  return { subaccount_id: id, failed_to_fetch: false, subaccount_value: value, positions: [],
    snapshot_received_at: new Date(timestamp).toISOString(), collaterals: id === 120934
      ? [{ asset_name: 'DRV', amount: '16', mark_price: '0.3', mark_value: '4.8' }, { asset_name: 'USDC', amount: '5.12', mark_price: '1', mark_value: '5.12' }]
      : [{ asset_name: 'ETH', amount: '2', mark_price: '2000', mark_value: '4000' }] };
}
const historyRow = (id, ts, value) => ({ subaccount_id: id, timestamp: ts, subaccount_value: value, margin_type: 'SM', currency: 'USDC,DRV,ETH' });
function harness(t, options = {}) {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  if (options.configured !== false) createSpotAccumulationStore(sqlite, config, { readOnly: false }).authorizeStartupSample();
  let clock = NOW;
  const calls = [];
  const state = { source: account(25923, '5000'), destination: account(120934, '9.92'),
    history: { 120934: [historyRow(120934, START + PERIOD_MS, '5'), historyRow(120934, START + 2 * PERIOD_MS, '10'), historyRow(120934, START + 3 * PERIOD_MS, '9.92')],
      25923: [historyRow(25923, START + PERIOD_MS, '5005'), historyRow(25923, START + 2 * PERIOD_MS, '5000'), historyRow(25923, START + 3 * PERIOD_MS, '5000')] } };
  const read = async (method, body) => {
    calls.push({ method, body });
    if (state.fail) throw new Error('venue unavailable');
    if (method === 'private/get_subaccount') return state.destination;
    if (state.failHistory) throw new Error('history unavailable');
    const rows = state.history[body.subaccount_id].filter(row => row.timestamp >= body.start_timestamp && row.timestamp <= body.end_timestamp);
    return { subaccount_value_history: rows, pagination: { count: rows.length, num_pages: rows.length ? 1 : 0 } };
  };
  const create = () => createPortfolioReportingCollector({ sqlite, derive, read: options.read || read, now: () => clock });
  return { sqlite, state, calls, create, collector: create(), setNow: value => { clock = value; },
    collect() { return this.collector.collect({ sourceAccount: state.source, spotPrice: 2000 }); },
    report: () => readReportingPortfolio(sqlite, { derive, from: START, to: clock, now: clock }) };
}

test('unconfigured and missing reporting readers are inert and create no tables', async t => {
  const h = harness(t, { configured: false }), before = h.sqlite.serialize();
  assert.equal(h.report().coverage.configured, false);
  assert.equal((await h.collect()).status, 'unconfigured');
  assert.deepEqual(h.sqlite.serialize(), before); assert.equal(h.calls.length, 0);
  const configured = harness(t), configuredBefore = configured.sqlite.serialize();
  assert.equal(configured.report().coverage.configured, true);
  assert.equal(configured.report().latest, null);
  assert.deepEqual(configured.sqlite.serialize(), configuredBefore);
});

test('current paired NAV includes every collateral, retains marks and preserves source snapshots and option accounting', async t => {
  const h = harness(t);
  h.sqlite.exec('CREATE TABLE portfolio_snapshots(usdc_balance REAL,eth_balance REAL,portfolio_value_usd REAL); INSERT INTO portfolio_snapshots VALUES(1000,2,5000)');
  const before = h.sqlite.prepare('SELECT * FROM portfolio_snapshots').all();
  assert.equal((await h.collect()).status, 'observed');
  const r = h.report();
  assert.equal(r.latest.combinedValue, 5009.92); assert.equal(r.latest.sourceValue, 5000);
  assert.deepEqual(r.latest.accounts[1].collaterals[0], { asset: 'DRV', amount: 16, markPrice: 0.3, markValue: 4.8 });
  assert.equal(r.coverage.latestStatus, 'complete');
  assert.deepEqual(h.sqlite.prepare('SELECT * FROM portfolio_snapshots').all(), before);
  assert.equal(h.sqlite.prepare("SELECT 1 FROM sqlite_master WHERE name='economic_events'").get(), undefined);
  assert.deepEqual(h.calls.filter(call => call.method.includes('history')).map(call => call.body.subaccount_id), [120934, 25923]);
  assert.equal(h.calls[2].body.start_timestamp, START + PERIOD_MS);
  const historical = r.rows.filter(row => row.kind === 'history');
  assert.equal(historical.length, 3);
  assert.equal(historical[0].combinedValue, historical[1].combinedValue, 'internal movement creates no net gain or expense');
  assert.equal(historical[0].timestamp, new Date(START + PERIOD_MS).toISOString(), 'no fabricated zero history before destination');
  assert.equal(h.sqlite.prepare('SELECT evidence_json FROM reporting_portfolio_pairs WHERE kind=? LIMIT 1').get('history').evidence_json.includes('subaccount_value'), true);
});

test('zero and negative historical NAV remain known, while missing matching timestamps are gaps', async t => {
  const h = harness(t);
  h.state.history[120934][0].subaccount_value = '0';
  h.state.history[120934][2].subaccount_value = '-2';
  h.state.history[25923].splice(1, 1);
  await h.collect();
  const rows = h.report().rows.filter(row => row.kind === 'history');
  assert.equal(rows[0].combinedValue, 5005);
  assert.equal(rows[1].combinedValue, null); assert.equal(rows[1].reason, 'historical_account_value_missing');
  assert.equal(rows[2].combinedValue, 4998);
});

for (const [name, mutate] of [
  ['missing source', h => { h.state.source = null; }],
  ['failed destination', h => { h.state.destination.failed_to_fetch = true; }],
  ['wrong destination', h => { h.state.destination.subaccount_id = 8; }],
  ['stale source', h => { h.state.source.snapshot_received_at = new Date(NOW - 31000).toISOString(); }],
  ['missing collateral evidence', h => { delete h.state.destination.collaterals; }],
  ['unknown transfer', h => { h.sqlite.prepare('INSERT INTO spot_transfers(scope,id,amount,request_json,status) VALUES(?,?,?,?,?)').run('mainnet:v3:25923','pending','5','{}','unknown'); }],
]) test(`${name} cannot become a complete combined observation`, async t => {
  const h = harness(t); mutate(h);
  assert.equal((await h.collect()).status, 'incomplete');
  assert.equal(h.report().latest.combinedValue, null);
  assert.equal(h.report().coverage.latestStatus, 'incomplete');
});

test('stale latest is identified without erasing historical evidence, and a restart throttles historical reads', async t => {
  const h = harness(t); await h.collect();
  h.setNow(NOW + FRESHNESS_MS + 1);
  assert.equal(h.report().coverage.latestStatus, 'stale');
  assert.equal(h.report().latest.combinedValue, 5009.92);
  h.setNow(NOW + 1000); h.state.source.snapshot_received_at = new Date(NOW + 1000).toISOString();
  await h.create().collect({ sourceAccount: h.state.source, spotPrice: 2000 });
  assert.equal(h.calls.filter(call => call.method.includes('history')).length, 2);
});

for (const [name, change] of [
  ['count mismatch', result => { result.pagination.count++; }],
  ['wrong account', result => { result.subaccount_value_history[0].subaccount_id = 7; }],
  ['duplicate timestamp', result => { result.subaccount_value_history[1].timestamp = result.subaccount_value_history[0].timestamp; }],
  ['off-grid timestamp', result => { result.subaccount_value_history[0].timestamp++; }],
  ['nonfinite NAV', result => { result.subaccount_value_history[0].subaccount_value = 'Infinity'; }],
  ['oversized pagination', result => { result.pagination = { count: 9000, num_pages: 9 }; }],
]) test(`history ${name} is not accepted as portfolio coverage`, async t => {
  const read = async (method, body) => {
    if (method === 'private/get_subaccount') return account(120934, '9.92');
    const result = { subaccount_value_history: [historyRow(body.subaccount_id, START + PERIOD_MS, '5'), historyRow(body.subaccount_id, START + 2 * PERIOD_MS, '10')], pagination: { count: 2, num_pages: 1 } };
    change(result); return result;
  };
  const h = harness(t, { read }); await h.collect();
  assert.equal(h.report().coverage.historyStatus, 'incomplete');
  assert.equal(h.report().rows.filter(row => row.kind === 'history').every(row => row.combinedValue === null), true);
  assert.equal(h.report().latest.combinedValue, 5009.92, 'historical failure does not erase independent live evidence');
});

test('configured owner mismatch fails closed before reads or new tables', async t => {
  const h = harness(t), before = h.sqlite.serialize();
  const badDerive = { ...derive, wallet: '0x' + '22'.repeat(20) };
  await assert.rejects(createPortfolioReportingCollector({ sqlite: h.sqlite, derive: badDerive, read: () => { throw new Error('must not read'); } })
    .collect({ sourceAccount: h.state.source, spotPrice: 2000 }), /conflicts/);
  assert.deepEqual(h.sqlite.serialize(), before);
});

test('reader exposes a bounded-window baseline and shared cadence without writing', async t => {
  const h = harness(t); await h.collect();
  const before = h.sqlite.serialize();
  const result = readReportingPortfolio(h.sqlite, { derive, from: START + 2 * PERIOD_MS, to: NOW, now: NOW });
  assert.equal(result.baseline.ts, START + PERIOD_MS);
  assert.equal(result.baseline.kind, 'history');
  assert.equal(result.coverage.historyPeriodMs, PERIOD_MS);
  assert.equal(result.coverage.freshnessMs, FRESHNESS_MS);
  assert.ok(result.rows.every(row => row.ts >= START + 2 * PERIOD_MS));
  assert.deepEqual(h.sqlite.serialize(), before);
});

test('incomplete matching history is retried from the last contiguous point with overlap', async t => {
  const h = harness(t), missing = h.state.history[25923].splice(1, 1)[0];
  await h.collect();
  assert.equal(h.report().coverage.historyStatus, 'incomplete');
  h.state.history[25923].splice(1, 0, missing);
  h.setNow(NOW + PERIOD_MS); h.state.source.snapshot_received_at = new Date(NOW + PERIOD_MS).toISOString();
  h.state.history[25923].push(historyRow(25923, START + 4 * PERIOD_MS, '5000'));
  h.state.history[120934].push(historyRow(120934, START + 4 * PERIOD_MS, '10'));
  await h.create().collect({ sourceAccount: h.state.source, spotPrice: 2000 });
  assert.equal(h.report().coverage.historyStatus, 'complete');
  assert.equal(h.report().rows.find(row => row.kind === 'history' && row.ts === missing.timestamp).combinedValue, 5010);
  const destReads = h.calls.filter(call => call.method.includes('history') && call.body.subaccount_id === 120934);
  assert.equal(destReads[1].body.start_timestamp, START, 'watermark retains a one-period overlap');
});

test('complete multi-page history is validated before any pairs are committed', async t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  createSpotAccumulationStore(sqlite, config, { readOnly: false }).authorizeStartupSample();
  const end = START + 1000 * PERIOD_MS, calls = [];
  const read = async (method, body) => {
    if (method === 'private/get_subaccount') return account(120934, '1', end);
    calls.push(body);
    const rows = Array.from({ length: body.page === 1 ? 1000 : 1 }, (_, index) =>
      historyRow(body.subaccount_id, START + ((body.page - 1) * 1000 + index) * PERIOD_MS, body.subaccount_id === 25923 ? '5000' : '1'));
    return { subaccount_value_history: rows, pagination: { count: 1001, num_pages: 2 } };
  };
  await createPortfolioReportingCollector({ sqlite, derive, read, now: () => end })
    .collect({ sourceAccount: account(25923, '5000', end), spotPrice: 2000 });
  const result = readReportingPortfolio(sqlite, { derive, from: START, to: end, now: end });
  assert.equal(result.coverage.historyStatus, 'complete');
  assert.equal(result.rows.filter(row => row.kind === 'history').length, 1001);
  assert.deepEqual(calls.map(call => [call.subaccount_id, call.page]), [[120934, 1], [120934, 2], [25923, 1], [25923, 2]]);
});

test('reporting import has no trading dependency graph or external runtime package', () => {
  const { execFileSync } = require('node:child_process');
  const modulePath = require.resolve('../bot/portfolio-reporting');
  const source = `const Module = require('node:module'); const load = Module._load;
    Module._load = function(name, ...args) {
      if (!name.startsWith('.') && !name.startsWith('/') || /spot-accumulation(?:-config|-store)?$/.test(name)) throw new Error('Trading dependency: '+name);
      return load.call(this,name,...args);
    }; require(${JSON.stringify(modulePath)});`;
  assert.equal(execFileSync(process.execPath, ['-e', source], { encoding: 'utf8' }), '');
});

for (const scenario of ['failed refresh', 'empty refresh', 'missing paired point']) {
  test(`${scenario} preserves prior complete history and raw provenance`, async t => {
    const h = harness(t); await h.collect();
    const before = h.sqlite.prepare("SELECT timestamp,pair_json,evidence_json FROM reporting_portfolio_pairs WHERE kind='history' ORDER BY timestamp").all();
    // Exercise a refresh of the same close as well as overlapping history.
    h.sqlite.exec("UPDATE reporting_portfolio_sync SET state_json=json_set(state_json,'$.attemptedAt',0)");
    if (scenario === 'failed refresh') h.state.failHistory = true;
    if (scenario === 'empty refresh') h.state.history[120934] = [];
    if (scenario === 'missing paired point') h.state.history[25923].pop();
    await h.collect();
    assert.deepEqual(h.sqlite.prepare("SELECT timestamp,pair_json,evidence_json FROM reporting_portfolio_pairs WHERE kind='history' ORDER BY timestamp").all(), before);
    assert.notEqual(h.report().coverage.historyStatus, 'complete');
  });
}

for (const duplicate of [false, true]) test(`descending paginated numeric-ms history ${duplicate ? 'rejects cross-page duplicates' : 'pairs in chronological order'}`, async t => {
  const sqlite = new Database(':memory:'); t.after(() => sqlite.close());
  createSpotAccumulationStore(sqlite, config, { readOnly: false }).authorizeStartupSample();
  const end = START + 1000 * PERIOD_MS;
  const read = async (method, body) => {
    if (method === 'private/get_subaccount') return account(120934, '1', end);
    const rows = Array.from({ length: body.page === 1 ? 1000 : 1 }, (_, index) => {
      const offset = body.page === 1 ? 1000 - index : duplicate ? 1 : 0;
      return historyRow(body.subaccount_id, String(START + offset * PERIOD_MS), body.subaccount_id === 25923 ? '5000' : '1');
    });
    return { subaccount_value_history: rows, pagination: { count: 1001, num_pages: 2 } };
  };
  await createPortfolioReportingCollector({ sqlite, derive, read, now: () => end })
    .collect({ sourceAccount: account(25923, '5000', end), spotPrice: 2000 });
  const result = readReportingPortfolio(sqlite, { derive, from: START, to: end, now: end });
  assert.equal(result.coverage.historyStatus, duplicate ? 'incomplete' : 'complete');
  const historical = result.rows.filter(row => row.kind === 'history');
  if (duplicate) assert.ok(historical.every(row => row.combinedValue == null));
  else {
    assert.equal(historical.length, 1001);
    assert.equal(historical[0].ts, START);
    assert.equal(historical.at(-1).ts, end);
    assert.ok(historical.every(row => row.combinedValue === 5001));
  }
});
