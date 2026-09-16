const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-v2-data-test-'));
process.env.DATA_DIR = dir;
delete process.env.NOOP_DB_PATH;
const store = require('../bot/db');
const db = store.db;
test.after(() => { store.close(); fs.rmSync(dir, { recursive: true, force: true }); });
test.beforeEach(() => {
  db.exec('DELETE FROM decision_outcomes; DELETE FROM candidate_observations; DELETE FROM spot_prices; DELETE FROM options_snapshots; DELETE FROM spot_prices_hourly; DELETE FROM options_hourly; DELETE FROM orders; DELETE FROM position_lifecycle; DELETE FROM lifecycle_dirty_instruments; DELETE FROM resting_orders;');
});
const now = '2026-09-11T10:00:00.000Z';
const due = '2026-09-11T11:00:00.000Z';
const name = 'ETH-20261127-1600-P';
const observation = (instrument_name = name, changes = {}) => ({ observed_at: now, action: 'buy_put', instrument_name, option_type: 'put', expiry: Date.parse('2026-11-27T08:00:00Z') / 1000, spot_price: 2000, ask_price: 10, bid_price: 9, delta: -.05, ...changes });
const quote = (instrument_name = name, changes = {}) => ({ instrument_name, option_details: { option_type: 'put', expiry: Date.parse('2026-11-27T08:00:00Z') / 1000, strike: 1600 }, details: { bidPrice: 20, askPrice: 21, delta: -.1, markPrice: 20.5, ...changes } });
const spot = (timestamp = due, price = 1800) => store.insertSpotPrice(price, {}, {}, timestamp);
const outcome = () => db.prepare('SELECT * FROM decision_outcomes ORDER BY id LIMIT 1').get();

test('spot alone remains pending; later usable option quote completes the label', () => {
  store.insertCandidateObservations([observation()], [1]);
  spot();
  store.insertOptionsSnapshotBatch([quote(name, { bidPrice: null, askPrice: null })], due);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: due }).pending, 1);
  assert.equal(outcome().spot_complete, 1);
  assert.equal(outcome().quote_complete, 0);
  assert.equal(outcome().evaluated_at, null);
  assert.equal(store.getDecisionOutcomeCompleteness()[0].with_spot, 1);
  assert.equal(store.getDecisionOutcomeCompleteness()[0].with_option_quote, 0);
  const later = '2026-09-11T11:01:00.000Z';
  store.insertOptionsSnapshotBatch([quote()], later);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: later }).evaluated, 1);
  assert.equal(outcome().future_quote_at, later);
  assert.equal(outcome().buy_entry_pnl, 10);
  assert.equal(outcome().quote_complete, 1);
});

test('outcome cannot see future data, stays pending until real deadline, and repairs after backfill', () => {
  store.insertCandidateObservations([observation()], [1]);
  spot('2026-09-11T11:01:00.000Z');
  store.insertOptionsSnapshotBatch([quote()], '2026-09-11T18:00:00.000Z');
  assert.equal(store.evaluateDueDecisionOutcomes({ now: due }).pending, 1);
  assert.equal(outcome().future_spot, null);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: '2026-09-11T16:59:59.000Z' }).pending, 1);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: '2026-09-11T17:00:00.000Z' }).missing, 1);
  assert.equal(outcome().quote_complete, 0);
  store.insertOptionsSnapshotBatch([quote(name, { bidPrice: 0 })], '2026-09-11T11:02:00.000Z');
  assert.equal(store.repairDecisionOutcomes({ now: '2026-09-11T18:00:00.000Z', batchSize: 1 }).evaluated, 1);
  assert.equal(outcome().buy_entry_pnl, -10);
  assert.deepEqual(store.repairDecisionOutcomes({ now: '2026-09-11T18:00:00.000Z' }), { reopened: 0, scanned: 0, evaluated: 0, missing: 0, pending: 0 });
});

test('pending outcomes are retried fairly when batch size is smaller than backlog', () => {
  store.insertCandidateObservations([observation('ETH-20261127-1500-P'), observation()], [1]);
  spot();
  store.insertOptionsSnapshotBatch([quote()], due);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: due, limit: 1 }).pending, 1);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: '2026-09-11T11:01:00.000Z', limit: 1 }).evaluated, 1);
});

test('observation universe tracks unexpired outstanding horizons independent of entry eligibility', () => {
  store.insertCandidateObservations([observation(), observation('ETH-20260910-1600-P', { expiry: Date.parse('2026-09-10T08:00:00Z') / 1000 }), observation(name)], [1, 24]);
  assert.deepEqual(store.getObservationInstruments(due), [name]);
  assert.deepEqual(store.getObservationInstruments('2026-09-12T16:00:00.000Z'), []);
  assert.equal(db.prepare('SELECT option_type FROM candidate_observations LIMIT 1').get().option_type, 'P');
});

test('sell outcome requires executable ask, not merely any option row', () => {
  const call = 'ETH-20260918-3000-C';
  store.insertCandidateObservations([observation(call, { action: 'sell_call', option_type: 'C', bid_price: 10 })], [1]);
  spot();
  store.insertOptionsSnapshotBatch([quote(call, { bidPrice: 2, askPrice: 0 })], due);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: due }).pending, 1);
  store.insertOptionsSnapshotBatch([quote(call, { bidPrice: 2, askPrice: 3 })], '2026-09-11T11:01:00.000Z');
  assert.equal(store.evaluateDueDecisionOutcomes({ now: '2026-09-11T11:01:00.000Z' }).evaluated, 1);
  assert.equal(outcome().sell_entry_pnl, 7);
});

test('raw spot and option writes roll back when derived storage fails', () => {
  db.exec("CREATE TRIGGER fail_spot_hour BEFORE INSERT ON spot_prices_hourly BEGIN SELECT RAISE(ABORT, 'forced rollup failure'); END;");
  assert.throws(() => spot(), /forced rollup failure/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM spot_prices').get().n, 0);
  db.exec('DROP TRIGGER fail_spot_hour;');
  db.exec("CREATE TRIGGER fail_option_hour BEFORE INSERT ON options_hourly BEGIN SELECT RAISE(ABORT, 'forced rollup failure'); END;");
  assert.throws(() => store.insertOptionsSnapshotBatch([quote()], due), /forced rollup failure/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM options_snapshots').get().n, 0);
  db.exec('DROP TRIGGER fail_option_hour;');
});

test('lifecycle refresh visits affected instruments and handles corrections, deletions, and expiry', () => {
  const insert = (instrument_name, amount) => store.insertOrder({ timestamp: now, action: 'buy_put', success: true, instrument_name, filled_amount: amount, total_value: 10 * amount });
  insert(name, 2);
  insert('ETH-20260918-1600-P', 1);
  assert.equal(store.refreshPositionLifecycle({ nowMs: Date.parse(now) }).refreshed, 2);
  assert.equal(store.refreshPositionLifecycle({ nowMs: Date.parse(now) }).refreshed, 0);
  db.prepare('UPDATE orders SET filled_amount = 3, total_value = 30 WHERE instrument_name = ?').run(name);
  assert.equal(store.refreshPositionLifecycle({ nowMs: Date.parse(now) }).refreshed, 1);
  assert.equal(db.prepare('SELECT net_amount FROM position_lifecycle WHERE instrument_name = ?').get(name).net_amount, 3);
  assert.equal(store.refreshPositionLifecycle({ nowMs: Date.parse('2026-09-18T09:00:00Z') }).expired, 1);
  db.prepare('DELETE FROM orders WHERE instrument_name = ?').run(name);
  store.refreshPositionLifecycle({ nowMs: Date.parse(now) });
  assert.equal(db.prepare('SELECT 1 FROM position_lifecycle WHERE instrument_name = ?').get(name), undefined);
});

test('resting fills preserve unknown economics and reject overstatement or regression', () => {
  store.insertRestingOrder({ order_id: 'r1', instrument_name: name, action: 'buy_put', direction: 'buy', amount: 5, limit_price: 10, filled_amount: 1, exit_intent: 'profit_capture' });
  let row = store.getOpenRestingOrders()[0];
  assert.equal(row.filled_value, null);
  assert.equal(row.approved_limit_price, 10);
  assert.equal(row.exit_intent, 'profit_capture');
  store.updateRestingOrder('r1', 'open', 1, 9);
  store.updateRestingOrder('r1', 'open', 1);
  assert.equal(store.getOpenRestingOrders()[0].filled_value, 9);
  assert.throws(() => store.updateRestingOrder('r1', 'open', 6, 50), /Invalid cumulative/);
  assert.throws(() => store.updateRestingOrder('r1', 'open', .5, 9), /cannot decrease/);
  assert.throws(() => store.updateRestingOrder('r1', 'open', 1, 8), /cannot decrease/);
  store.updateRestingOrder('r1', 'open', 2);
  assert.equal(store.getOpenRestingOrders()[0].filled_value, null);
});

test('call premium metric uses canonical enum and a stable maturity/delta population', () => {
  const ts = new Date().toISOString();
  const expiry = Date.now() / 1000 + 8 * 86400;
  const good = quote('ETH-20260918-3000-C', { delta: .08, bidPrice: 10 });
  good.option_details = { option_type: 'call', expiry, strike: 3000 };
  const deep = quote('ETH-20260918-4000-C', { delta: .5, bidPrice: 100 });
  deep.option_details = { option_type: 'C', expiry, strike: 4000 };
  const far = quote('ETH-20261127-3000-C', { delta: .08, bidPrice: 200 });
  far.option_details = { option_type: 'C', expiry: expiry + 60 * 86400, strike: 3000 };
  store.insertOptionsSnapshotBatch([good, deep, far], ts);
  assert.equal(store.getAvgCallPremium7d().avg_premium, 10);
  db.prepare("UPDATE options_snapshots SET option_type = 'call' WHERE option_type = 'C'").run();
  assert.equal(store.getAvgCallPremium7d().avg_premium, 10);
  assert.equal(db.prepare('SELECT option_type FROM options_snapshots LIMIT 1').get().option_type, 'call');
});

test('repair command requires an explicit existing database', () => {
  const env = { ...process.env }; delete env.DATA_DIR; delete env.NOOP_DB_PATH;
  const result = spawnSync(process.execPath, [path.resolve(__dirname, '../bot/repair-decision-outcomes.js')], { env, encoding: 'utf8' });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /Explicit --db or DATA_DIR is required/);
});

test('boot preserves historical evidence and leaves optional repairs explicit', () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-v2-legacy-'));
  const modulePath = path.resolve(__dirname, '../bot/db');
  const run = (code) => {
    const result = spawnSync(process.execPath, ['-e', code], { env: { ...process.env, DATA_DIR: fixture, NOOP_DB_PATH: path.join(fixture, 'noop.db') }, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  try {
    const before = JSON.parse(run(`const s=require(${JSON.stringify(modulePath)});
      s.insertRestingOrder({order_id:'legacy',instrument_name:'ETH-20261127-1600-P',action:'buy_put',direction:'buy',amount:5,limit_price:10,filled_amount:2,filled_value:19});
      s.insertCandidateObservations([{observed_at:'2026-09-11T10:00:00.000Z',action:'buy_put',instrument_name:'ETH-20261127-1600-P',option_type:'P'}],[1]);
      s.insertSpotPrice(2000,{}, {},'2026-09-11T11:00:00.000Z');
      s.insertOrder({timestamp:'2026-09-11T10:00:00.000Z',action:'buy_put',success:true,instrument_name:'ETH-20261127-1600-P',filled_amount:1,total_value:10});
      s.insertPortfolioSnapshot({timestamp:'2026-09-11T10:00:00.000Z',spot_price:2000,total_realized_pnl:42,total_unrealized_pnl:3,portfolio_value_usd:123});
      s.db.exec("UPDATE decision_outcomes SET status='evaluated',evaluated_at='2026-09-11T11:00:00.000Z',future_spot=2000,future_spot_at='2026-09-11T11:00:00.000Z',error='legacy evidence'; UPDATE candidate_observations SET option_type='put'; INSERT INTO options_snapshots(timestamp,instrument_name,option_type) VALUES ('2026-09-11T10:00:00.000Z','ETH-20260918-3000-C','call'); DELETE FROM lifecycle_dirty_instruments; ALTER TABLE resting_orders DROP COLUMN filled_value; ALTER TABLE decision_outcomes DROP COLUMN spot_complete; ALTER TABLE decision_outcomes DROP COLUMN quote_complete; ALTER TABLE portfolio_snapshots DROP COLUMN gross_options_cashflow;");console.log(JSON.stringify(Object.fromEntries(['spot_prices','options_snapshots','candidate_observations','decision_outcomes','portfolio_snapshots','position_lifecycle','orders'].map(table=>[table,{columns:s.db.prepare('PRAGMA table_info('+table+')').all().map(column=>column.name),rows:s.db.prepare('SELECT * FROM '+table+' ORDER BY id').all()}]))));s.close();`));
    const migrated = JSON.parse(run(`const s=require(${JSON.stringify(modulePath)});
      console.log(JSON.stringify({historical:Object.fromEntries(['spot_prices','options_snapshots','candidate_observations','decision_outcomes','portfolio_snapshots','position_lifecycle','orders'].map(table=>[table,s.db.prepare('SELECT * FROM '+table+' ORDER BY id').all()])),fill:s.getOpenRestingOrders()[0],outcome:s.db.prepare('SELECT * FROM decision_outcomes').get(),portfolio:s.getLatestPortfolioSnapshot(),rawType:s.db.prepare('SELECT option_type FROM options_snapshots').get().option_type,candidateType:s.db.prepare('SELECT option_type FROM candidate_observations').get().option_type,dirty:s.db.prepare('SELECT COUNT(*) n FROM lifecycle_dirty_instruments').get().n,normalEvaluation:s.evaluateDueDecisionOutcomes({now:'2026-09-11T18:00:00.000Z'})}));s.db.exec('UPDATE resting_orders SET filled_value=19');s.close();`));
    for (const [table, prior] of Object.entries(before)) {
      const originalColumnsAfterBoot = migrated.historical[table].map(row => Object.fromEntries(prior.columns.map(column => [column, row[column]])));
      assert.deepEqual(originalColumnsAfterBoot, prior.rows, `${table} historical columns changed on boot`);
    }
    assert.equal(migrated.fill.filled_amount, 2);
    assert.equal(migrated.fill.filled_value, null);
    assert.equal(migrated.outcome.status, 'evaluated');
    assert.equal(migrated.outcome.evaluated_at, '2026-09-11T11:00:00.000Z');
    assert.equal(migrated.outcome.future_spot, 2000);
    assert.equal(migrated.outcome.error, 'legacy evidence');
    assert.equal(migrated.outcome.spot_complete, null);
    assert.equal(migrated.outcome.quote_complete, null);
    assert.equal(migrated.portfolio.total_realized_pnl, 42);
    assert.equal(migrated.portfolio.portfolio_value_usd, 123);
    assert.equal(migrated.portfolio.gross_options_cashflow, null);
    assert.equal(migrated.rawType, 'call');
    assert.equal(migrated.candidateType, 'put');
    assert.equal(migrated.dirty, 0);
    assert.equal(migrated.normalEvaluation.scanned, 0);
    assert.equal(run(`const s=require(${JSON.stringify(modulePath)});console.log(s.getOpenRestingOrders()[0].filled_value);s.close();`), '19');
    const explicit = JSON.parse(run(`const s=require(${JSON.stringify(modulePath)});console.log(JSON.stringify(s.repairDecisionOutcomes({now:'2026-09-11T18:00:00.000Z'})));s.close();`));
    assert.equal(explicit.reopened, 1);
    assert.equal(explicit.missing, 1);
  } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
});

test('a later batch envelope cannot turn a pre-horizon quote into future evidence', () => {
  store.insertCandidateObservations([observation()], [1]);
  spot();
  const oldReceipt = '2026-09-11T10:59:59.000Z';
  const laterFrame = '2026-09-11T11:00:01.000Z';
  store.insertOptionsSnapshotBatch([quote(name, { quoteReceivedAt: oldReceipt, quoteSource: 'derive-v2/get_tickers' })], laterFrame);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: laterFrame }).pending, 1);
  assert.equal(outcome().future_quote_at, null);
  const raw = db.prepare('SELECT timestamp, quote_received_at, quote_source FROM options_snapshots').get();
  assert.deepEqual(raw, { timestamp: laterFrame, quote_received_at: oldReceipt, quote_source: 'derive-v2/get_tickers' });
  const trueReceipt = '2026-09-11T11:00:02.000Z';
  const nextFrame = '2026-09-11T11:00:03.000Z';
  store.insertOptionsSnapshotBatch([quote(name, { quoteReceivedAt: trueReceipt })], nextFrame);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: trueReceipt }).pending, 1);
  assert.equal(store.evaluateDueDecisionOutcomes({ now: nextFrame }).evaluated, 1);
  assert.equal(outcome().future_quote_at, trueReceipt);
});

test('quote receipts after their availability envelope reject the whole raw batch', () => {
  assert.throws(() => store.insertOptionsSnapshotBatch([quote(name, { quoteReceivedAt: '2026-09-11T11:00:01.000Z' })], due), /Quote receipt/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM options_snapshots').get().n, 0);
});

test('portfolio storage preserves unknown profit and truthful gross cashflow separately', () => {
  store.insertPortfolioSnapshot({ timestamp: now, spot_price: 2000, usdc_balance: 0, eth_balance: 0,
    positions_json: [], total_unrealized_pnl: null, total_realized_pnl: null,
    gross_options_cashflow: 123, portfolio_value_usd: -5 });
  const row = store.getLatestPortfolioSnapshot();
  assert.equal(row.total_realized_pnl, null);
  assert.equal(row.total_unrealized_pnl, null);
  assert.equal(row.gross_options_cashflow, 123);
  assert.equal(row.portfolio_value_usd, -5);
  assert.equal(row.usdc_balance, 0);
});

test('monetization tranche count ignores open resting tranches and dry runs', () => {
  const put = 'ETH-20261030-1900-P';
  const pending = store.insertPendingAction({ rule_id: null, action: 'sell_put', instrument_name: put, amount: 1, price: 250 });
  const pendingId = Number(pending.lastInsertRowid);
  store.insertRestingOrder({ order_id: 'm1', instrument_name: put, action: 'sell_put', direction: 'sell', amount: 1, limit_price: 250, filled_amount: 0, exit_intent: 'monetize_tail_win', pending_action_id: pendingId });
  const fill = (amount, extra = {}) => store.insertOrder({ timestamp: now, action: 'sell_put', success: true, instrument_name: put, filled_amount: amount, total_value: 250 * amount, pending_action_id: pendingId, ...extra });
  assert.equal(store.countSellPutTranches(put), 0);
  fill(0.3);                                   // partial fill while the tranche still rests
  assert.equal(store.countSellPutTranches(put), 0);
  fill(0.7);
  store.updateRestingOrder('m1', 'filled', 1, 250);
  assert.equal(store.countSellPutTranches(put), 1); // one tranche, two fill rows
  store.insertOrder({ timestamp: now, action: 'sell_put', success: true, reason: 'DRY RUN: simulated sell_put (post_only)', instrument_name: put, filled_amount: 1, total_value: 250 });
  assert.equal(store.countSellPutTranches(put), 1);
});
