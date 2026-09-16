'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { openLedger } = require('../ledger/store');
const { fixture, clone, inputForState, NOW } = require('../../test/helpers/strategy-fixtures');

function setup(t) {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'noop-product-ledger-'));
  const filename = path.join(directory, 'ledger.sqlite');
  const stores = [];
  t.after(() => { for (const store of stores) store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const open = () => { const store = openLedger({ filename, ledgerId: 'test-ledger', clock: () => NOW }); stores.push(store); return store; };
  const store = open();
  const f = fixture();
  store.registerMandate({ customer_id: 'alice', release: f.release, mandate: f.mandate });
  return { directory, filename, store, open, f, scope: store.scope({ customer_id: 'alice', mandate_id: f.mandate.mandate_id }) };
}
function event(kind, payload, id = kind) {
  return { event_id: `event:${id}`, source_event_id: `source:${id}`, kind,
    occurred_at: NOW, evidence_ref: `evidence:${id}`, payload };
}
const deposit = (amount = '100', id = 'fund') => event('deposit', { asset: 'USDC', amount, transfer_id: `transfer:${id}` }, id);
function balance(scope, account, asset = 'USDC') { return scope.getBalances().find(row => row.account === account && row.asset === asset)?.amount || '0'; }
const option = 'ETH-20300201-2000-P';
const optionFill = (id, side, quantity, gross = '10') => event('option_fill', {
  fill_id: `fill:${id}`, order_attempt_id: `attempt:${id}`, instrument: option,
  side, quantity, gross_premium: gross, quote_asset: 'USDC', fees: [{ asset: 'USDC', amount: '0.1' }],
}, id);

test('module import is inert and has no default database path', () => {
  const imported = spawn(process.execPath, ['-e', `
    const Module = require('node:module');
    const original = Module._load;
    Module._load = function (name, ...args) { if (name === 'better-sqlite3') throw Error('native SQLite loaded at import'); return original.call(this, name, ...args); };
    require(${JSON.stringify(path.resolve(__dirname, '../ledger/store'))});
  `], { stdio: ['ignore', 'ignore', 'pipe'] });
  return new Promise((resolve, reject) => {
    let stderr = ''; imported.stderr.on('data', data => { stderr += data; });
    imported.once('error', reject); imported.once('exit', code => code === 0 ? resolve() : reject(new Error(stderr)));
  });
});

test('requires explicit private isolated path and creates WAL FULL private marked storage', t => {
  assert.throws(() => openLedger(), /ledgerId/);
  assert.throws(() => openLedger({ ledgerId: 'test', filename: './data.db' }), /absolute/);
  const { store, filename } = setup(t);
  const db = store.privileged().db;
  assert.equal(db.pragma('journal_mode', { simple: true }), 'wal');
  assert.equal(db.pragma('synchronous', { simple: true }), 2);
  assert.equal(db.pragma('foreign_keys', { simple: true }), 1);
  assert.equal(fs.statSync(filename).mode & 0o777, 0o600);
  assert.throws(() => openLedger({ filename, ledgerId: 'different' }), /identity mismatch/);
});

test('rejects foreign SQLite before mutation or sidecar creation', t => {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'noop-foreign-ledger-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const filename = path.join(directory, 'foreign.sqlite');
  const Database = require('better-sqlite3');
  const foreign = new Database(filename); foreign.exec('CREATE TABLE customer_secret (value TEXT)'); foreign.close(); fs.chmodSync(filename, 0o600);
  const before = fs.readFileSync(filename);
  assert.throws(() => openLedger({ filename, ledgerId: 'test' }), /Foreign database/);
  assert.deepEqual(fs.readFileSync(filename), before);
  assert.deepEqual(fs.readdirSync(directory), ['foreign.sqlite']);
});

test('rejects symlink and hardlink database files and sidecars', t => {
  const { store, directory, filename } = setup(t); store.close();
  const outside = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'noop-link-target-'));
  t.after(() => fs.rmSync(outside, { recursive: true, force: true }));
  const target = path.join(outside, 'target'); fs.writeFileSync(target, 'untouched', { mode: 0o600 });
  fs.symlinkSync(target, filename + '-wal');
  assert.throws(() => openLedger({ filename, ledgerId: 'test-ledger' }), /symlinks or hardlinks/);
  fs.unlinkSync(filename + '-wal');
  fs.linkSync(filename, path.join(outside, 'duplicate'));
  assert.throws(() => openLedger({ filename, ledgerId: 'test-ledger' }), /symlinks or hardlinks/);
  fs.unlinkSync(path.join(outside, 'duplicate'));
  fs.renameSync(filename, path.join(outside, 'original')); fs.symlinkSync(path.join(outside, 'original'), filename);
  assert.throws(() => openLedger({ filename, ledgerId: 'test-ledger' }), /symlinks or hardlinks/);
  assert.equal(fs.readFileSync(target, 'utf8'), 'untouched');
  assert.deepEqual(fs.readdirSync(directory), ['ledger.sqlite']);
});

test('rejects shared and permissive directories without changing them', t => {
  const { store, directory, filename } = setup(t); store.close();
  fs.writeFileSync(path.join(directory, 'unrelated'), 'do not touch', { mode: 0o600 });
  assert.throws(() => openLedger({ filename, ledgerId: 'test-ledger' }), /dedicated/);
  fs.unlinkSync(path.join(directory, 'unrelated')); fs.chmodSync(directory, 0o755);
  assert.throws(() => openLedger({ filename, ledgerId: 'test-ledger' }), /0700/);
  fs.chmodSync(directory, 0o700);
});

test('one immutable full account identity belongs to one customer mandate', t => {
  const { store, f, scope } = setup(t);
  assert.equal(store.registerMandate({ customer_id: 'alice', release: f.release, mandate: f.mandate }).status, 'replayed');
  const other = clone(f.mandate); other.mandate_id = 'other-mandate'; other.account.manager_id = '99';
  assert.throws(() => store.registerMandate({ customer_id: 'bob', release: f.release, mandate: other }), /already assigned/);
  assert.throws(() => store.registerMandate({ customer_id: 'bob', release: f.release, mandate: f.mandate }), /different content/);
  const returned = scope.getMandate(); returned.account.owner = 'changed';
  assert.equal(scope.getMandate().account.owner, f.mandate.account.owner);
  other.account.subaccount_id = '2';
  store.registerMandate({ customer_id: 'bob', release: f.release, mandate: other });
  const bob = store.scope({ customer_id: 'bob', mandate_id: other.mandate_id });
  scope.appendEvent(deposit());
  assert.deepEqual(bob.getBalances(), []); assert.deepEqual(bob.listEvents(), []);
  assert.throws(() => store.scope({ customer_id: 'bob', mandate_id: f.mandate.mandate_id }), /scope is unavailable/);
  assert.throws(() => store.scope({ customer_id: 'alice', mandate_id: 'missing' }), /scope is unavailable/);
  assert.equal('db' in scope, false); assert.equal('privileged' in scope, false);
});

test('owner changes and address case variants cannot allocate a Derive subaccount twice', t => {
  const { store, f } = setup(t);
  const changedOwner = clone(f.mandate); changedOwner.mandate_id = 'changed-owner'; changedOwner.account.owner = '0x' + 'a'.repeat(40);
  assert.throws(() => store.registerMandate({ customer_id: 'bob', release: f.release, mandate: changedOwner }), /already assigned/);
  const lower = clone(f.mandate); lower.mandate_id = 'lower'; lower.account.owner = '0x' + 'a'.repeat(40); lower.account.subaccount_id = '2';
  store.registerMandate({ customer_id: 'alice', release: f.release, mandate: lower });
  const upper = clone(lower); upper.mandate_id = 'upper'; upper.account.owner = '0x' + 'A'.repeat(40);
  assert.throws(() => store.registerMandate({ customer_id: 'bob', release: f.release, mandate: upper }), /already assigned/);
});

test('economic receipt, balanced postings, exact balances and idempotency survive reopen', t => {
  const { store, scope, open, f } = setup(t);
  const source = deposit('100.000000000000000000000000000001');
  const recorded = scope.appendEvent(source);
  assert.equal(recorded.status, 'recorded');
  assert.equal(scope.appendEvent(source).status, 'replayed');
  const alias = { ...source, event_id: 'alias:same-source' };
  assert.equal(scope.appendEvent(alias).sequence, recorded.sequence);
  assert.throws(() => scope.appendEvent({ ...deposit('1', 'new-source'), event_id: alias.event_id }), /conflicting/);
  assert.equal(balance(scope, 'assets:cash'), '100.000000000000000000000000000001');
  const changed = clone(source); changed.payload.amount = '101';
  assert.throws(() => scope.appendEvent(changed), /conflicting/);
  const renamedSource = { ...source, event_id: 'another', source_event_id: 'another-source' };
  assert.throws(() => scope.appendEvent(renamedSource), /conflicting/);
  assert.equal(scope.listEvents().length, 1); assert.equal(scope.verify().valid, true);
  store.close();
  const recovered = open().scope({ customer_id: 'alice', mandate_id: f.mandate.mandate_id });
  assert.equal(recovered.appendEvent(source).status, 'replayed');
  assert.deepEqual(recovered.listEvents()[0], recorded); assert.equal(recovered.verify().events, 1);
});

test('business fill identity cannot be rebooked with renamed event and source IDs', t => {
  const { scope } = setup(t); scope.appendEvent(deposit());
  const fill = optionFill('buy', 'buy', '0.1'); scope.appendEvent(fill);
  assert.throws(() => scope.appendEvent({ ...fill, event_id: 'renamed', source_event_id: 'renamed-source' }), /conflicting/);
  assert.equal(balance(scope, 'assets:options', `OPTION:${option}`), '0.1');
  assert.equal(balance(scope, 'assets:cash'), '89.9');
});

test('accounting accepts evidenced negative cash without fabricating borrowing', t => {
  const { scope } = setup(t);
  scope.appendEvent(optionFill('unfunded-actual-fill', 'buy', '1', '10'));
  assert.equal(balance(scope, 'assets:cash'), '-10.1');
  assert.equal(scope.getBalances().some(row => row.account.startsWith('liabilities:principal')), false);
  assert.equal(scope.verify().valid, true);
});

test('loan principal and accrued interest must exist before repayment is recorded', t => {
  const { scope } = setup(t);
  const repayment = event('repay', { loan_id: 'loan:1', asset: 'USDC', principal: '100', interest: '1' }, 'repay');
  assert.throws(() => scope.appendEvent(repayment), /financing evidence/); assert.equal(scope.listEvents().length, 0);
  scope.appendEvent(event('borrow', { loan_id: 'loan:1', asset: 'USDC', amount: '100' }, 'borrow'));
  assert.throws(() => scope.appendEvent(repayment), /financing evidence/);
  scope.appendEvent(event('interest_accrual', { loan_id: 'loan:1', asset: 'USDC', amount: '1', direction: 'payable' }, 'accrual'));
  scope.appendEvent(repayment);
  assert.equal(balance(scope, 'liabilities:principal:loan:1'), '0');
  assert.equal(balance(scope, 'liabilities:interest:loan:1'), '0');
  assert.equal(balance(scope, 'expenses:interest'), '1');
  assert.equal(balance(scope, 'assets:cash'), '-1');
  assert.equal(scope.verify().valid, true);
});

test('settlement closes only the exact signed instrument position', t => {
  const { scope } = setup(t); scope.appendEvent(deposit()); scope.appendEvent(optionFill('buy', 'buy', '0.1'));
  const settlement = event('option_settlement', { settlement_id: 'settlement:1', instrument: option,
    position_quantity: '0.2', cash_amount: '100', quote_asset: 'USDC' }, 'settlement');
  assert.throws(() => scope.appendEvent(settlement), /exact remaining/);
  settlement.payload.position_quantity = '0.1'; scope.appendEvent(settlement);
  assert.equal(balance(scope, 'assets:options', `OPTION:${option}`), '0');
  assert.equal(balance(scope, 'assets:cash'), '189.9');
  assert.equal(scope.appendEvent(settlement).status, 'replayed');
  assert.equal(scope.verify().events, 3);
});

test('correction reverses stored postings once and refuses dependent history', t => {
  const { scope } = setup(t); scope.appendEvent(deposit());
  const reversal = event('reversal', { reverses_event_id: 'event:fund', reason: 'Correct import with retained evidence' }, 'reverse');
  scope.appendEvent(reversal);
  assert.equal(balance(scope, 'assets:cash'), '0'); assert.equal(scope.verify().events, 2);
  assert.equal(scope.appendEvent(reversal).status, 'replayed');
  assert.throws(() => scope.appendEvent({ ...reversal, event_id: 'second-reverse', source_event_id: 'second-reverse' }), /conflicting/);
  scope.appendEvent(deposit('200', 'next'));
  scope.appendEvent(event('fee', { fee_id: 'fee:next', asset: 'USDC', amount: '1' }, 'fee'));
  assert.throws(() => scope.appendEvent(event('reversal', { reverses_event_id: 'event:next', reason: 'Too late' }, 'late-reverse')), /subsequent/);
  assert.equal(balance(scope, 'assets:cash'), '199');
});

test('decisions atomically persist revision, receipt, private CAS and exact prior replay result', t => {
  const { scope, f, store, open } = setup(t);
  const accepted = scope.acceptDecision(f); assert.equal(accepted.status, 'accepted');
  assert.equal(scope.getControlState().private_state_version, 1);
  const nextBundle = inputForState(f.inputBundle, accepted.state);
  const second = { ...clone(f.decision), decision_id: 'decision:2', expected_control_revision: 1,
    input_bundle_id: nextBundle.input_bundle_id, private_state: { expected_version: 1, proposed_version: 2, content_ref: 'state:fixture/2' },
    operations: [{ op: 'no_action', reason: 'Record new private state' }] };
  scope.acceptDecision({ decision: second, inputBundle: nextBundle });
  const replay = scope.acceptDecision(f);
  assert.equal(replay.status, 'replayed'); assert.equal(replay.accepted_control_revision, 1);
  assert.equal(replay.state.control_revision, 1); assert.equal(scope.getControlState().control_revision, 2);
  const changed = clone(f.decision); changed.operations[0].intent.reason = 'changed';
  assert.throws(() => scope.acceptDecision({ ...f, decision: changed }), /different content/);
  assert.equal(scope.verify().decisions, 2);
  store.close(); const again = open().scope({ customer_id: 'alice', mandate_id: f.mandate.mandate_id });
  assert.equal(again.getControlState().private_state_version, 2); assert.equal(again.acceptDecision(f).status, 'replayed');
});

test('a later invalid operation rolls back earlier decisions and private state', t => {
  const { scope, f } = setup(t);
  const invalid = clone(f.decision);
  const second = clone(invalid.operations[0]); second.intent.intent_id = 'second'; second.intent.when.right.literal = '35';
  invalid.operations.push(second);
  assert.throws(() => scope.acceptDecision({ ...f, decision: invalid }), /policy rejected/);
  assert.equal(scope.getControlState().control_revision, 0); assert.equal(scope.getControlState().private_state_version, 0);
  assert.equal(scope.verify().decisions, 0);
  assert.equal(scope.acceptDecision(f).status, 'accepted');
});

test('callbacks cannot reenter ledger mutation, and same-process stale snapshots fail', t => {
  const { scope, f } = setup(t);
  assert.throws(() => scope.acceptDecision({ ...f, validateEconomicPolicy() {
    scope.appendEvent(deposit()); return { valid: true, reasons: [] };
  } }), /cannot reenter/);
  assert.equal(scope.listEvents().length, 0); assert.equal(scope.getControlState().control_revision, 0);
  scope.acceptDecision(f);
  assert.throws(() => scope.acceptDecision({ ...f, decision: { ...f.decision, decision_id: 'stale-id' } }), /fresh control|stale control/);
});

test('two SQLite connections accept only one stale control snapshot', t => {
  const { scope, f, open } = setup(t);
  const other = open().scope({ customer_id: 'alice', mandate_id: f.mandate.mandate_id });
  scope.acceptDecision(f);
  assert.throws(() => other.acceptDecision({ ...f, decision: { ...f.decision, decision_id: 'other-worker' } }), /fresh control|stale control/);
  assert.equal(other.getControlState().control_revision, 1); assert.equal(other.verify().decisions, 1);
});

test('nested batch failure rolls back receipts and all material balances', t => {
  const { store, scope } = setup(t);
  assert.throws(() => store.privileged().transaction(() => {
    scope.appendEvent(deposit()); scope.appendEvent(optionFill('buy', 'buy', '0.1'));
    throw Error('interruption before commit');
  }), /interruption/);
  assert.deepEqual(scope.getBalances(), []); assert.deepEqual(scope.listEvents(), []); assert.equal(scope.verify().valid, true);
});

test('decimal capacity failure cannot commit an event before all balances update', t => {
  const { scope } = setup(t);
  scope.appendEvent(deposit('9'.repeat(60)));
  assert.throws(() => scope.appendEvent(deposit('1', 'overflow')), /integer digits exceed/);
  assert.equal(scope.listEvents().length, 1);
  assert.equal(balance(scope, 'assets:cash'), '9'.repeat(60));
  assert.equal(scope.verify().valid, true);
});

test('receipt rows are immutable and projection corruption blocks writes and reopen', t => {
  const { store, scope, open } = setup(t); scope.appendEvent(deposit());
  const db = store.privileged().db;
  assert.throws(() => db.prepare('DELETE FROM events').run(), /immutable/);
  assert.throws(() => db.prepare("UPDATE postings SET amount = '1'").run(), /immutable/);
  db.prepare("UPDATE balances SET amount = '999' WHERE account = 'assets:cash'").run();
  assert.throws(() => scope.verify(), /balances are corrupt/);
  assert.throws(() => scope.appendEvent(deposit('1', 'next')), /balances are corrupt/);
  assert.equal(scope.listEvents().length, 1); store.close();
  assert.throws(() => open(), /balances are corrupt/);
});

test('registration ownership corruption is detected before further accounting', t => {
  const { store, f } = setup(t);
  store.privileged().db.prepare("UPDATE mandates SET customer_id = 'mallory' WHERE mandate_id = ?").run(f.mandate.mandate_id);
  assert.throws(() => store.privileged().verify(f.mandate.mandate_id), /registration digest/);
});

test('SIGKILL during a real SQLite transaction leaves no partial receipts or state', { timeout: 15000 }, async t => {
  const { store, scope, filename, f, open } = setup(t); scope.appendEvent(deposit()); store.close();
  const child = spawn(process.execPath, ['-e', `
    const { openLedger } = require(${JSON.stringify(path.resolve(__dirname, '../ledger/store'))});
    const ledger = openLedger({filename: process.argv[1], ledgerId: 'test-ledger'});
    const scope = ledger.scope({customer_id:'alice', mandate_id:'fixture-mandate'});
    ledger.privileged().transaction(() => {
      scope.appendEvent(${JSON.stringify(deposit('50', 'uncommitted'))});
      scope.acceptDecision(require(${JSON.stringify(path.resolve(__dirname, '../../test/helpers/strategy-fixtures'))}).fixture());
      process.send({inside_transaction:true});
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
    });
  `, filename], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  t.after(() => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); });
  let stderr = ''; child.stderr.on('data', data => { stderr += data; });
  await Promise.race([once(child, 'message'), once(child, 'exit').then(() => { throw new Error(`Child exited before transaction: ${stderr}`); })]);
  child.kill('SIGKILL'); await once(child, 'exit');
  const recovered = open().scope({ customer_id: 'alice', mandate_id: f.mandate.mandate_id });
  assert.equal(recovered.listEvents().length, 1); assert.equal(balance(recovered, 'assets:cash'), '100');
  assert.equal(recovered.getControlState().control_revision, 0); assert.equal(recovered.getControlState().private_state_version, 0);
  assert.equal(recovered.verify().valid, true); assert.equal(recovered.appendEvent(deposit('50', 'uncommitted')).status, 'recorded');
  assert.equal(recovered.acceptDecision(f).status, 'accepted');
  assert.equal(balance(recovered, 'assets:cash'), '150');
});
