'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const Database = require('better-sqlite3');
const { buildReport, readEvidence, parseArgs } = require('../scripts/spot-accumulation');
const fixturePath = path.join(__dirname, 'fixtures/spot-accumulation-snapshot.json');
const fixture = () => JSON.parse(fs.readFileSync(fixturePath, 'utf8'));
const nowMs = 1794241487000;
const entry = path.join(__dirname, '../scripts/spot-accumulation.js');

test('offline report allocates every net proceed to DRV, never caller allocation or live authority', () => {
  const input = fixture();
  input.planner_input.allocations = { ETH: { source_usdc: '999999' } };
  const before = JSON.stringify(input);
  const result = buildReport(input, { nowMs, replay: true });
  assert.equal(JSON.stringify(input), before);
  assert.deepEqual(result.credits.allocated_usdc, { ETH: '0', DRV: '998' });
  assert.equal(result.plan.routes.ETH.status, 'disabled');
  assert.equal(result.plan.routes.ETH.order, undefined);
  assert.equal(result.plan.routes.DRV.reason, 'ALT_destination_unavailable');
  assert.equal(result.ready_to_trade, false); assert.equal(result.synthetic, true);
  assert.equal(result.replay, true); assert.equal(result.plan.executable, false);
  input.planner_input.destination = { ...input.planner_input.source,
    subaccount_id: 99, risk_universe_id: 3, usdc_balance: '0', committed_usdc: '0' };
  const withAlt = buildReport(input, { nowMs, replay: true });
  assert.equal(withAlt.plan.routes.DRV.status, 'transfer_preview_required');
  assert.equal(withAlt.plan.routes.DRV.source_spend_usdc, '998');
  assert.equal(withAlt.plan.routes.DRV.transfer.amount, '996.5');
  assert.equal(withAlt.plan.routes.ETH.order, undefined);
  assert.throws(() => buildReport({ ...input, account_id: 7 }, { nowMs }), /mismatch/);
});

test('stale market evidence and missing source ownership never become a ready purchase', () => {
  const stale = buildReport(fixture(), { nowMs: nowMs + 31000 });
  assert.equal(stale.plan.routes.DRV.status, 'deferred');
  const input = fixture(); input.orders = [];
  const missing = buildReport(input, { nowMs });
  assert.equal(missing.credits.total_net_usdc, '0');
  assert.equal(missing.plan.routes.ETH.status, 'disabled');
  assert.ok(missing.blockers.some(x => x.includes('could not be credited')));
  input.events[0].timestamp = new Date(nowMs + 1).toISOString();
  assert.throws(() => buildReport(input, { nowMs }), /Future/);
  assert.throws(() => parseArgs(['--input', 'x', '--live']), /Unknown/);
});

test('actual CLI replays without modifying its input and refuses to overwrite a report', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-spot-cli-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const before = fs.readFileSync(fixturePath);
  const out = path.join(dir, 'report.json');
  const args = [entry, '--input', fixturePath, '--as-of', new Date(nowMs).toISOString(), '--out', out];
  const first = spawnSync(process.execPath, args, { encoding: 'utf8' });
  assert.equal(first.status, 0, first.stderr);
  assert.equal(JSON.parse(fs.readFileSync(out, 'utf8')).credits.total_net_usdc, '998');
  assert.deepEqual(fs.readFileSync(fixturePath), before);
  const previous = fs.readFileSync(out);
  const repeated = spawnSync(process.execPath, args, { encoding: 'utf8' });
  assert.notEqual(repeated.status, 0); assert.match(repeated.stderr, /overwrite|new file/);
  assert.deepEqual(fs.readFileSync(out), previous);
});

test('existing database reads are non-mutating, coverage-aware and refuse existing execution state', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-spot-db-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'noop.db'); const input = fixture();
  let db = new Database(file);
  db.exec(`CREATE TABLE economic_events(event_id TEXT, account_id TEXT, event_type TEXT, timestamp TEXT,
    instrument_name TEXT,currency TEXT,amount TEXT,cashflow_usd TEXT,realized_pnl_usd TEXT,fee_usd TEXT,source TEXT,raw_json TEXT);
    CREATE TABLE orders(action TEXT,success INTEGER,instrument_name TEXT,raw_response TEXT);
    CREATE TABLE bot_state(id INTEGER,put_budget_for_cycle REAL);
    INSERT INTO bot_state VALUES(1,150);`);
  const event = input.events[0];
  db.prepare(`INSERT INTO economic_events(${Object.keys(event).join(',')}) VALUES(${Object.keys(event).map(() => '?').join(',')})`).run(...Object.values(event));
  const order = input.orders[0]; db.prepare('INSERT INTO orders VALUES(?,?,?,?)').run(order.action,order.success,order.instrument_name,order.raw_response);
  db.close(); const before = fs.readFileSync(file);
  const evidence = readEvidence(file, 25923, input.activation_timestamp, new Date(nowMs).toISOString());
  assert.equal(evidence.history_complete, false); assert.equal(evidence.cycle_budget_usdc, '150');
  const report = buildReport(input, { evidence, nowMs });
  assert.equal(report.plan.source_float_usdc, '600');
  assert.equal(report.history_complete, false);
  assert.deepEqual(fs.readFileSync(file), before);
  const alias = path.join(dir, 'alias'); fs.symlinkSync(dir, alias);
  for (const suffix of ['-wal', '-shm', '-journal']) {
    const target = path.join(alias, 'noop.db' + suffix);
    const attempt = spawnSync(process.execPath, [entry, '--input', fixturePath, '--db', file,
      '--as-of', new Date(nowMs).toISOString(), '--out', target], { encoding: 'utf8' });
    assert.notEqual(attempt.status, 0); assert.match(attempt.stderr, /sidecar/);
    assert.equal(fs.existsSync(target), false);
  }
  assert.deepEqual(fs.readFileSync(file), before);
  db = new Database(file);
  db.exec('CREATE TABLE spot_accumulation_config(scope TEXT, config_json TEXT)');
  const legacyConfig = JSON.stringify({ network: 'mainnet', apiVersion: 'v3', accountId: '25923',
    activationTimestamp: input.activation_timestamp, destinationAccountId: '99' });
  db.prepare('INSERT INTO spot_accumulation_config VALUES(?,?)').run('mainnet:v3:25923', legacyConfig);
  db.close();
  const withLegacyPolicy = fs.readFileSync(file);
  assert.throws(() => readEvidence(file, 25923, input.activation_timestamp, new Date(nowMs).toISOString()), /requires reconciliation/);
  assert.deepEqual(fs.readFileSync(file), withLegacyPolicy);
  db = new Database(file); db.exec('DROP TABLE spot_accumulation_config'); db.close();
  db = new Database(file); db.exec('CREATE TABLE spot_reservations(id TEXT); INSERT INTO spot_reservations VALUES(\'pending\')'); db.close();
  assert.throws(() => readEvidence(file, 25923, input.activation_timestamp, new Date(nowMs).toISOString()), /requires reconciliation/);
  const missing = path.join(dir, 'missing.db');
  assert.throws(() => readEvidence(missing, 25923, input.activation_timestamp, new Date(nowMs).toISOString()));
  assert.equal(fs.existsSync(missing), false);
});
