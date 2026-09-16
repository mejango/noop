'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { openLedger, createOperations } = require('../index');
const { createReplayFixture } = require('../../strategies/noop-v2-reference/fixtures');
const { runDemo } = require('../demo');

const NOW = '2030-01-01T00:00:00.000Z';

test('response loss, restart, partial recovery, replay and later settlement preserve exact balances', async () => {
  const result = await runDemo();
  assert.equal(result.mode, 'offline_simulation');
  assert.deepEqual(result.restart, { status: 'submission_unknown', retry_blocked: true, simulated_sends: 1 });
  assert.deepEqual(result.partial_fill, { usdc: '9.596', reserved_usdc: '0.606' });
  assert.deepEqual(result.completed_order, { status: 'accounted', accounted_quantity: '0.1', usdc: '8.99', reserved_usdc: '0' });
  assert.deepEqual(result.settled, { eth: '2', usdc: '9.49', option_quantity: '0' });
  assert.deepEqual(result.ledger, { valid: true, events: 5, decisions: 1 });
});

test('product and demo imports cannot load live V2, database, SDK, network or credential dependencies', () => {
  const source = `
    const Module = require('node:module');
    const fs = require('node:fs');
    const load = Module._load;
    Module._load = function(request, parent, isMain) {
      if (/^(?:node:)?(?:http|https|net|tls|dgram|child_process)$/.test(request)
        || /(?:ethers|axios|sqlite|integrations[\\\\/]|bot[\\\\/]|[\\\\/]script(?:\\.js)?$)/.test(request)) {
        throw new Error('Forbidden dependency: ' + request);
      }
      return load.apply(this, arguments);
    };
    const read = fs.readFileSync;
    fs.readFileSync = function(file) {
      if (typeof file === 'string' && /(?:\\.env(?:$|\\.)|\\.key$|\\.derive-v3|noop\\.db)/.test(file)) {
        throw new Error('Forbidden credential or database read');
      }
      return read.apply(this, arguments);
    };
    for (const name of ['writeFileSync', 'appendFileSync', 'mkdirSync', 'mkdtempSync', 'chmodSync', 'rmSync', 'unlinkSync', 'renameSync', 'createWriteStream', 'writeSync']) {
      fs[name] = () => { throw new Error('Forbidden import mutation'); };
    }
    for (const name of ['fetch', 'WebSocket', 'setTimeout', 'setInterval', 'setImmediate']) {
      globalThis[name] = () => { throw new Error('Forbidden runtime effect'); };
    }
    Date.now = () => { throw new Error('Implicit wall clock'); };
    require(${JSON.stringify(require.resolve('../index'))});
    require(${JSON.stringify(require.resolve('../demo'))});
    process.stdout.write('import-safe');
  `;
  assert.equal(execFileSync(process.execPath, ['-e', source], { cwd: os.tmpdir(), encoding: 'utf8', timeout: 10000 }), 'import-safe');
});

test('two customer accounts can reuse local IDs without sharing cash, reservations or financing', t => {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'noop-product-integration-')));
  fs.chmodSync(directory, 0o700);
  const filename = path.join(directory, 'ledger.sqlite');
  let store = openLedger({ filename, ledgerId: 'integration-ledger', clock: () => NOW });
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const fixtures = ['a', 'b'].map((id, i) => {
    const fixture = createReplayFixture({ action: 'buy_put', mandateId: `mandate-${id}` });
    // Each fixture starts with the same physical account. Rebind before any
    // decision generation; the input bundle must match the changed account too.
    if (i) {
      fixture.mandate = structuredClone(fixture.mandate);
      fixture.mandate.account.subaccount_id = '2';
      fixture.inputBundle = structuredClone(fixture.inputBundle);
      fixture.inputBundle.account = structuredClone(fixture.mandate.account);
      const { contentDigest } = require('../../strategy/canonical');
      delete fixture.inputBundle.input_bundle_id;
      fixture.inputBundle.input_bundle_id = contentDigest(fixture.inputBundle);
    }
    return fixture;
  });
  const append = (scope, id, kind, payload) => store.scope(scope).appendEvent({ event_id: id,
    source_event_id: `venue:${id}`, kind, occurred_at: NOW, evidence_ref: `fixture:${id}`, payload });
  const scopes = fixtures.map((fixture, i) => {
    const scope = { customer_id: `customer-${i}`, mandate_id: fixture.mandate.mandate_id };
    store.registerMandate({ customer_id: scope.customer_id, mandate: fixture.mandate,
      release: fixture.release, catalog: fixture.strategy.fieldCatalog });
    append(scope, 'deposit', 'deposit', { transfer_id: 'same-venue-local-id', asset: 'USDC', amount: i ? '20' : '10' });
    const decision = fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate });
    store.scope(scope).acceptDecision({ decision, inputBundle: fixture.inputBundle, validateEconomicPolicy: fixture.strategy.validateEconomicPolicy });
    createOperations(store.privileged()).forScope(scope).queue({ operation_id: 'same-operation-id',
      intent_id: fixture.binding.intent_id, intent_revision: 1, mandate_revision: 1, action: 'buy_put',
      instrument_ref: fixture.instrumentRef, quantity: '0.1', limit_price: '10', max_fee_usdc: '0.01',
      reservations: [{ asset: 'USDC', amount: '1.01' }] });
    return scope;
  });
  append(scopes[0], 'borrow', 'borrow', { loan_id: 'loan-1', asset: 'USDC', amount: '5' });
  append(scopes[0], 'interest', 'interest_accrual', { loan_id: 'loan-1', asset: 'USDC', amount: '0.1', direction: 'payable' });
  store.close();
  store = openLedger({ filename, ledgerId: 'integration-ledger', clock: () => NOW });
  const balance = (scope, account) => store.scope(scope).getBalances()
    .find(row => row.account === account && row.asset === 'USDC')?.amount || '0';
  assert.equal(balance(scopes[0], 'assets:cash'), '15');
  assert.equal(balance(scopes[0], 'liabilities:principal:loan-1'), '-5');
  assert.equal(balance(scopes[0], 'liabilities:interest:loan-1'), '-0.1');
  assert.equal(balance(scopes[1], 'assets:cash'), '20');
  assert.equal(balance(scopes[1], 'liabilities:principal:loan-1'), '0');
  append(scopes[0], 'repay', 'repay', { loan_id: 'loan-1', asset: 'USDC', principal: '5', interest: '0.1' });
  // Exact repeat after restart does not repay twice or expense interest twice.
  append(scopes[0], 'repay', 'repay', { loan_id: 'loan-1', asset: 'USDC', principal: '5', interest: '0.1' });
  assert.equal(balance(scopes[0], 'assets:cash'), '9.9');
  assert.equal(balance(scopes[0], 'expenses:interest'), '0.1');
  assert.equal(balance(scopes[0], 'liabilities:principal:loan-1'), '0');
  assert.equal(balance(scopes[0], 'liabilities:interest:loan-1'), '0');
  createOperations(store.privileged()).forScope(scopes[0]).cancelQueued('same-operation-id');
  const other = createOperations(store.privileged()).forScope(scopes[1]).get('same-operation-id');
  assert.equal(other.status, 'queued');
  assert.equal(other.reservations[0].remaining_amount, '1.01');
  assert.throws(() => store.scope({ ...scopes[0], customer_id: scopes[1].customer_id }), /scope|customer/i);
  for (const scope of scopes) assert.equal(store.scope(scope).verify().valid, true);
});

test('demo refuses execution and database arguments', () => {
  for (const argument of ['--execute', '--database=/tmp/live.db']) {
    assert.throws(() => execFileSync(process.execPath, [require.resolve('../demo'), argument], {
      cwd: os.tmpdir(), encoding: 'utf8', timeout: 10000, stdio: ['ignore', 'pipe', 'pipe'],
    }), /Unknown arguments/);
  }
});
