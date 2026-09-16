'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { openLedger } = require('../ledger/store');
const { createOperations, createGateway } = require('../ledger/operations');
const { createReplayFixture } = require('../../strategies/noop-v2-reference/fixtures');
const { contentDigest } = require('../../strategy/canonical');

const NOW = '2030-01-01T00:00:00.000Z';
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
function setup(t, options = {}) {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'noop-recovery-')));
  fs.chmodSync(directory, 0o700);
  const filename = path.join(directory, 'recovery.sqlite');
  let time = Date.parse(NOW);
  let store = openLedger({ filename, ledgerId: 'recovery-test', clock: () => new Date(time).toISOString() });
  const fixture = createReplayFixture({ action: 'buy_put' });
  const scope = { customer_id: 'customer-a', mandate_id: fixture.mandate.mandate_id };
  store.registerMandate({ ...scope, mandate: fixture.mandate, release: fixture.release, catalog: fixture.strategy.fieldCatalog });
  const decision = fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate });
  store.scope(scope).acceptDecision({ decision, inputBundle: fixture.inputBundle, validateEconomicPolicy: fixture.strategy.validateEconomicPolicy });
  store.scope(scope).appendEvent({ event_id: 'deposit-1', source_event_id: 'venue:deposit-1', kind: 'deposit', occurred_at: NOW,
    evidence_ref: 'fixture:deposit-1', payload: { transfer_id: 'transfer-1', asset: 'USDC', amount: options.cash || '100' } });
  let api = createOperations(store.privileged()).forScope(scope);
  const counters = { sign: 0, send: 0 };
  let gateway;
  const callbacks = {
    mode: 'offline_simulation',
    sign: async payload => { counters.sign += 1; return `signed:${payload.nonce}`; },
    send: async (signed, payload, context) => { counters.send += 1; assert.match(signed, /^signed:/); return { order_id: `order-${payload.nonce}`, attempt_id: payload.attempt_id, nonce: payload.nonce, payload_digest: context.payload_digest }; },
    verifyEvidence: async () => true,
    ...options.callbacks,
  };
  function build() { api = createOperations(store.privileged()).forScope(scope); gateway = createGateway(store.privileged(), callbacks); }
  build();
  t.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
  const env = {
    filename, directory, fixture, scope, counters, callbacks,
    get store() { return store; }, get api() { return api; }, get gateway() { return gateway; },
    advance(ms) { time += ms; },
    reopen() { store.close(); store = openLedger({ filename, ledgerId: 'recovery-test', clock: () => new Date(time).toISOString() }); build(); },
    acquire(worker = 'worker-a', ttl = 1000) { return gateway.acquire(scope, { worker_id: worker, ttl_ms: ttl }); },
    operation(id = 'operation-1', edits = {}) { return { operation_id: id, intent_id: fixture.binding.intent_id, intent_revision: 1, mandate_revision: 1,
      action: 'buy_put', instrument_ref: fixture.instrumentRef, quantity: '0.1', limit_price: '10', max_fee_usdc: '0.01',
      reservations: [{ asset: 'USDC', amount: '1.01' }], ...edits }; },
    fill(id = 'fill-1', quantity = '0.1', gross = '1', fee = '0.01') {
      return { event_id: `event:${id}`, source_event_id: `venue:${id}`, kind: 'option_fill', occurred_at: NOW, evidence_ref: `fixture:${id}`,
        payload: { fill_id: id, order_attempt_id: api.get('operation-1').attempt.attempt_id, instrument: fixture.instrumentRef,
          side: 'buy', quantity, gross_premium: gross, quote_asset: 'USDC', fees: [{ asset: 'USDC', amount: fee }] } };
    },
    evidence(edits = {}) {
      const { attempt } = api.get('operation-1');
      return { evidence_id: 'evidence-1', account: fixture.mandate.account, attempt_id: attempt.attempt_id, nonce: attempt.nonce,
        payload_digest: attempt.payload_digest, order_id: attempt.order_id || `order-${attempt.nonce}`, status: 'filled',
        cumulative_filled_quantity: '0.1', fills: [env.fill()], complete_fill_set: true, ...edits };
    },
    cash() { return store.scope(scope).getBalances().find(item => item.account === 'assets:cash' && item.asset === 'USDC').amount; },
  };
  return env;
}

test('queue is immutable, scoped, cash-backed and cannot spend the same intent twice', t => {
  const e = setup(t);
  assert.equal(e.api.queue(e.operation()).status, 'queued');
  assert.equal(e.api.queue(e.operation()).status, 'queued');
  assert.throws(() => e.api.queue(e.operation('operation-1', { quantity: '0.09' })), /idempotency conflict/);
  assert.throws(() => e.api.queue(e.operation('operation-2')), /cumulative/);
  assert.throws(() => createOperations(e.store.privileged()).forScope({ ...e.scope, customer_id: 'customer-b' }), /scope denied/);
  assert.throws(() => e.api.queue({ ...e.operation('operation-3'), destination: 'attacker' }), /Unknown operation field/);
  assert.equal(e.cash(), '100');
});

test('an operation must reference an accepted active intent and preserve its bounds', t => {
  const e = setup(t);
  for (const edits of [{ intent_id: 'invented' }, { intent_revision: 2 }, { quantity: '1' }, { limit_price: '11' }, { max_fee_usdc: '0.02' }, { action: 'sell_put' }]) {
    assert.throws(() => e.api.queue(e.operation('invalid', edits)), /accepted|bounds|differs/);
  }
  assert.equal(e.api.list().length, 0);
});

test('queued cancellation releases cash but cannot cancel an attempted operation', async t => {
  const e = setup(t);
  e.api.queue(e.operation());
  assert.equal(e.api.cancelQueued('operation-1').reservations[0].remaining_amount, '0');
  e.api.queue(e.operation('operation-2'));
  await e.gateway.submit(e.acquire(), 'operation-2');
  assert.throws(() => e.api.cancelQueued('operation-2'), /authoritative reconciliation/);
});

test('a restart before submission preserves queue and uses a unique durable nonce', async t => {
  const e = setup(t);
  e.api.queue(e.operation()); e.reopen();
  const result = await e.gateway.submit(e.acquire(), 'operation-1');
  assert.equal(result.attempt.nonce, '1');
  assert.equal(result.status, 'acknowledged');
  assert.equal(result.attempt.accounted_quantity, '0');
  assert.equal(result.reservations[0].remaining_amount, '1.01');
  assert.equal(e.cash(), '100');
  assert.equal(Object.hasOwn(result, 'signed'), false);
});

for (const crashAt of ['sign', 'send']) test(`a crash during ${crashAt} leaves durable uncertainty across restart`, async t => {
  const e = setup(t, { callbacks: { [crashAt]: async () => { throw new Error('simulated crash'); } } });
  e.api.queue(e.operation()); const lease = e.acquire();
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /simulated crash/);
  e.reopen();
  assert.equal(e.api.get('operation-1').status, 'submission_unknown');
  assert.equal(e.api.get('operation-1').reservations[0].remaining_amount, '1.01');
  assert.throws(() => e.api.queue(e.operation('operation-2')), /recovery required|cumulative/);
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /already attempted/);
});

test('ACK is not an economic fill and a restart cannot resubmit it', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire();
  await e.gateway.submit(lease, 'operation-1'); e.reopen();
  assert.equal(e.store.scope(e.scope).listEvents().length, 1);
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /already attempted/);
  assert.equal(e.api.get('operation-1').attempt.resolved, false);
});

test('malformed or misidentified ACK leaves the account uncertain', async t => {
  const e = setup(t, { callbacks: { send: async () => ({ order_id: 'x', attempt_id: 'wrong', nonce: '1', payload_digest: contentDigest('wrong') }) } });
  e.api.queue(e.operation());
  await assert.rejects(e.gateway.submit(e.acquire(), 'operation-1'), /identity mismatch/);
  assert.equal(e.api.get('operation-1').status, 'submission_unknown');
});

test('authoritative recovery atomically imports fills and clears only terminal accounted work', async t => {
  const e = setup(t, { callbacks: { send: async () => { throw new Error('response lost'); } } });
  e.api.queue(e.operation()); let lease = e.acquire();
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /response lost/);
  e.reopen(); e.advance(1000); lease = e.acquire('worker-b');
  const evidence = e.evidence();
  const result = await e.gateway.recover(lease, 'operation-1', evidence);
  assert.equal(result.status, 'accounted'); assert.equal(result.attempt.resolved, true);
  assert.equal(e.cash(), '98.99');
  assert.equal(result.reservations[0].remaining_amount, '0');
  e.reopen();
  await e.gateway.recover(lease, 'operation-1', evidence);
  assert.equal(e.cash(), '98.99'); assert.equal(e.store.scope(e.scope).listEvents().length, 2);
  assert.equal(e.store.scope(e.scope).verify().valid, true);
});

test('partial fills consume only confirmed cash and retain remaining commitment across restart', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  const partial = e.fill('fill-1', '0.04', '0.4', '0.004');
  await e.gateway.recover(lease, 'operation-1', e.evidence({ status: 'open', cumulative_filled_quantity: '0.04', fills: [partial] }));
  assert.equal(e.cash(), '99.596');
  assert.equal(e.api.get('operation-1').reservations[0].remaining_amount, '0.606');
  e.reopen();
  await e.gateway.recover(lease, 'operation-1', e.evidence({ evidence_id: 'evidence-2', fills: [partial, e.fill('fill-2', '0.06', '0.6', '0.006')] }));
  assert.equal(e.cash(), '98.99');
  assert.equal(e.api.get('operation-1').attempt.accounted_quantity, '0.1');
  assert.equal(e.store.scope(e.scope).listEvents().length, 3);
});

test('terminal venue history with missing fill pages cannot flatten or release commitment', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  await e.gateway.recover(lease, 'operation-1', e.evidence({ fills: [], complete_fill_set: false }));
  assert.equal(e.api.get('operation-1').attempt.resolved, false);
  assert.equal(e.api.get('operation-1').reservations[0].remaining_amount, '1.01');
  await assert.rejects(e.gateway.recover(lease, 'operation-1', e.evidence({ evidence_id: 'missing', fills: [] })), /missing economic events/);
  assert.equal(e.cash(), '100');
  await e.gateway.recover(lease, 'operation-1', e.evidence({ evidence_id: 'complete' }));
  assert.equal(e.api.get('operation-1').attempt.resolved, true);
});

test('failure after appending one fill rolls back both economic and recovery records', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  const good = e.fill('fill-1', '0.04', '0.4', '0.004');
  const bad = e.fill('fill-2', '0.06', '0.6', '0.006'); bad.payload.side = 'sell';
  await assert.rejects(e.gateway.recover(lease, 'operation-1', e.evidence({ fills: [good, bad] })), /does not match/);
  assert.equal(e.cash(), '100');
  assert.equal(e.store.scope(e.scope).listEvents().length, 1);
  assert.equal(e.store.privileged().db.prepare('SELECT count(*) n FROM operation_fills').get().n, 0);
  assert.equal(e.api.get('operation-1').reservations[0].remaining_amount, '1.01');
});

test('conflicting fill replay cannot create or modify economic performance', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  const first = e.fill('fill-1', '0.04', '0.4', '0.004');
  await e.gateway.recover(lease, 'operation-1', e.evidence({ status: 'open', cumulative_filled_quantity: '0.04', fills: [first] }));
  const changed = structuredClone(first); changed.payload.gross_premium = '0.3';
  await assert.rejects(e.gateway.recover(lease, 'operation-1', e.evidence({ evidence_id: 'conflict', status: 'open', cumulative_filled_quantity: '0.04', fills: [changed] })), /idempotency conflict/);
  assert.equal(e.cash(), '99.596');
});

test('recovery rejects wrong account, attempt, nonce, order, side, asset and unverified evidence', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  for (const edits of [{ account: { ...e.fixture.mandate.account, subaccount_id: '2' } }, { attempt_id: 'other' }, { nonce: '99' }, { order_id: 'other' }]) {
    await assert.rejects(e.gateway.recover(lease, 'operation-1', e.evidence(edits)), /identity mismatch/);
  }
  const gateway = createGateway(e.store.privileged(), { ...e.callbacks, verifyEvidence: async () => false });
  await assert.rejects(gateway.recover(lease, 'operation-1', e.evidence()), /not verified/);
  assert.equal(e.cash(), '100');
});

test('a current absence is insufficient; irreversible nonacceptance releases zero-fill work', async t => {
  const e = setup(t, { callbacks: { sign: async () => { throw new Error('crash'); } } });
  e.api.queue(e.operation()); const lease = e.acquire(); await assert.rejects(e.gateway.submit(lease, 'operation-1'));
  const absence = e.evidence({ status: 'not_accepted', order_id: null, fills: [], cumulative_filled_quantity: '0',
    not_accepted_proof: { kind: 'not_found', evidence_ref: 'fixture:absence' } });
  await assert.rejects(e.gateway.recover(lease, 'operation-1', absence), /irreversible nonacceptance/);
  const invalidated = { ...absence, not_accepted_proof: { kind: 'nonce_invalidated', evidence_ref: 'fixture:irreversible-invalidation' } };
  await e.gateway.recover(lease, 'operation-1', invalidated);
  assert.equal(e.api.get('operation-1').attempt.resolved, true);
  assert.equal(e.api.get('operation-1').reservations[0].remaining_amount, '0');
  assert.equal(e.cash(), '100');
});

test('expired workers cannot submit or renew after a monotonic lease takeover', async t => {
  const e = setup(t); e.api.queue(e.operation()); const stale = e.acquire('old', 1000);
  assert.throws(() => e.acquire('new'), /lease is held/);
  e.advance(1000); const next = e.acquire('new');
  assert.equal(next.fence_token, stale.fence_token + 1);
  await assert.rejects(e.gateway.submit(stale, 'operation-1'), /Stale or expired/);
  assert.throws(() => e.gateway.renew(stale, 1000), /Stale or expired/);
  await e.gateway.submit(next, 'operation-1'); assert.equal(e.counters.send, 1);
});

test('a separate process cannot acquire a currently held account writer lease', t => {
  const e = setup(t); e.acquire('parent', 1000);
  const source = `const {openLedger}=require(${JSON.stringify(require.resolve('../ledger/store'))});
    const {createGateway}=require(${JSON.stringify(require.resolve('../ledger/operations'))});
    const store=openLedger({filename:${JSON.stringify(e.filename)},ledgerId:'recovery-test',clock:()=>${JSON.stringify(NOW)}});
    try {createGateway(store.privileged(),{mode:'offline_simulation',sign:()=>null,send:()=>null,verifyEvidence:()=>true}).acquire(${JSON.stringify(e.scope)},{worker_id:'child',ttl_ms:1000}); process.exitCode=2;}
    catch(error){if(!/lease is held/.test(error.message))throw error;} finally{store.close();}`;
  const child = spawnSync(process.execPath, ['-e', source], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
});

test('lease expiry during async signing discards signed bytes and blocks a replacement send', async t => {
  const signing = deferred(); const e = setup(t, { callbacks: { sign: () => signing.promise } });
  e.api.queue(e.operation()); const old = e.acquire('old', 1000);
  const sending = e.gateway.submit(old, 'operation-1');
  e.advance(1000); const replacement = e.acquire('new');
  await assert.rejects(e.gateway.submit(replacement, 'operation-1'), /already attempted/);
  signing.resolve('signed:1');
  await assert.rejects(sending, /Stale or expired/);
  assert.equal(e.counters.send, 0);
  assert.equal(e.api.get('operation-1').status, 'submission_unknown');
});

test('async transport expiry cannot treat a late ACK as accounted', async t => {
  const transport = deferred(); let payload, context;
  const e = setup(t, { callbacks: { send: async (_signed, p, c) => { payload = p; context = c; return transport.promise; } } });
  e.api.queue(e.operation()); const old = e.acquire('old', 1000); const pending = e.gateway.submit(old, 'operation-1');
  await new Promise(resolve => setImmediate(resolve));
  e.advance(1000); e.acquire('new');
  transport.resolve({ order_id: 'order-1', attempt_id: payload.attempt_id, nonce: payload.nonce, payload_digest: context.payload_digest });
  await assert.rejects(pending, /Stale or expired/);
  assert.equal(e.api.get('operation-1').status, 'submission_unknown'); assert.equal(e.cash(), '100');
});

test('reentrant signing cannot dispatch a second attempt', async t => {
  let e, lease;
  e = setup(t, { callbacks: { sign: async payload => {
    await assert.rejects(e.gateway.submit(lease, 'operation-1'), /already attempted/);
    return `signed:${payload.nonce}`;
  } } });
  e.api.queue(e.operation()); lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  assert.equal(e.counters.send, 1);
});

test('cash drained after queue creation prevents signing', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire();
  e.store.scope(e.scope).appendEvent({ event_id: 'withdrawal', source_event_id: 'venue:withdrawal', kind: 'withdrawal', occurred_at: NOW,
    evidence_ref: 'fixture:withdrawal', payload: { transfer_id: 'withdrawal', asset: 'USDC', amount: '100' } });
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /no longer funded/);
  assert.equal(e.counters.sign, 0); assert.equal(e.api.get('operation-1').status, 'queued');
});

test('cash drained during signing prevents dispatch while preserving uncertainty', async t => {
  let e;
  e = setup(t, { callbacks: { sign: async () => {
    e.store.scope(e.scope).appendEvent({ event_id: 'withdrawal', source_event_id: 'venue:withdrawal', kind: 'withdrawal', occurred_at: NOW,
      evidence_ref: 'fixture:withdrawal', payload: { transfer_id: 'withdrawal', asset: 'USDC', amount: '100' } });
    return 'signed:1';
  } } });
  e.api.queue(e.operation()); await assert.rejects(e.gateway.submit(e.acquire(), 'operation-1'), /no longer funded/);
  assert.equal(e.counters.send, 0); assert.equal(e.api.get('operation-1').status, 'submission_unknown');
});

test('backwards host time cannot extend an expired or superseded writer', t => {
  const e = setup(t); const lease = e.acquire(); e.advance(100); e.gateway.renew(lease, 1000); e.advance(-101);
  assert.throws(() => e.gateway.renew(lease, 1000), /clock moved backwards/);
  assert.throws(() => e.acquire('other'), /clock moved backwards/);
});

test('accounted execution and option settlement remain independent after restart', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  await e.gateway.recover(lease, 'operation-1', e.evidence()); e.reopen();
  assert.equal(e.store.scope(e.scope).getBalances().find(item => item.account === 'assets:options').amount, '0.1');
  assert.equal(e.store.scope(e.scope).listEvents().some(item => item.event.kind === 'option_settlement'), false);
  e.store.scope(e.scope).appendEvent({ event_id: 'settled', source_event_id: 'venue:settled', kind: 'option_settlement', occurred_at: NOW,
    evidence_ref: 'fixture:settled', payload: { settlement_id: 'settlement-1', instrument: e.fixture.instrumentRef, position_quantity: '0.1', cash_amount: '5', quote_asset: 'USDC' } });
  assert.equal(e.cash(), '103.99'); assert.equal(e.api.get('operation-1').status, 'accounted');
});

test('corrupt reservation and lifecycle projections cannot enable dispatch', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire();
  e.store.privileged().db.prepare("UPDATE operation_reservations SET remaining_amount='0'").run();
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /Corrupt reservation/);
  assert.equal(e.counters.sign, 0);
});

test('corrupt resolved flag cannot erase an uncertain attempt after reopening', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  e.store.privileged().db.prepare('UPDATE operation_attempts SET resolved=1').run();
  assert.throws(() => e.api.get('operation-1'), /Corrupt attempt resolution/);
  assert.throws(() => e.reopen(), /Corrupt attempt resolution/);
});

test('negative ETH cash blocks new options even when USDC reservations remain funded', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire();
  e.store.scope(e.scope).appendEvent({ event_id: 'eth-fee', source_event_id: 'venue:eth-fee', kind: 'fee', occurred_at: NOW,
    evidence_ref: 'fixture:eth-fee', payload: { fee_id: 'eth-fee', asset: 'ETH', amount: '0.01' } });
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /Negative account cash/);
  assert.equal(e.counters.sign, 0);
});

test('accepted cancellation after queue creation prevents signing the old intent', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire();
  const state = e.store.scope(e.scope).getControlState();
  const input = structuredClone(e.fixture.inputBundle);
  input.sequence = 2; input.control_revision = state.control_revision;
  input.private_state = { version: state.private_state_version, content_ref: state.private_state_ref };
  input.active_intents = [{ intent_id: e.fixture.binding.intent_id, intent_revision: 1, status: 'active' }];
  delete input.input_bundle_id; input.input_bundle_id = contentDigest(input);
  const decision = { contract_version: 'noop.strategy/v1', strategy_release_id: e.fixture.release.strategy_release_id,
    release_digest: e.fixture.release.release_digest, mandate_id: e.scope.mandate_id, mandate_revision: 1,
    input_bundle_id: input.input_bundle_id, decision_id: 'cancelled', expected_control_revision: state.control_revision,
    operations: [{ op: 'cancel_intent', intent_id: e.fixture.binding.intent_id, expected_intent_revision: 1, reason: 'Customer selected cancellation' }] };
  e.store.scope(e.scope).acceptDecision({ inputBundle: input, decision, validateEconomicPolicy: e.fixture.strategy.validateEconomicPolicy });
  await assert.rejects(e.gateway.submit(lease, 'operation-1'), /not an active accepted revision/);
  assert.equal(e.counters.sign, 0);
  assert.equal(e.api.cancelQueued('operation-1').reservations[0].remaining_amount, '0');
});

test('zero-fill terminal cancellation permits a new attempt with the next durable nonce', async t => {
  const e = setup(t); e.api.queue(e.operation()); const lease = e.acquire(); await e.gateway.submit(lease, 'operation-1');
  await e.gateway.recover(lease, 'operation-1', e.evidence({ status: 'cancelled', cumulative_filled_quantity: '0', fills: [] }));
  e.api.queue(e.operation('operation-2')); e.reopen();
  const second = await e.gateway.submit(lease, 'operation-2');
  assert.equal(second.attempt.nonce, '2'); assert.equal(e.counters.send, 2);
});

test('gateway rejects absent or live mode instead of exposing incomplete live dispatch', t => {
  const e = setup(t);
  const { mode: ignored, ...callbacks } = e.callbacks;
  assert.throws(() => createGateway(e.store.privileged(), callbacks), /Only explicit offline_simulation/);
  assert.throws(() => createGateway(e.store.privileged(), { ...callbacks, mode: 'live' }), /live dispatch is unavailable/);
  assert.equal(e.gateway.mode, 'offline_simulation');
});

test('caller mutation cannot exchange a pinned lease across asynchronous signing', async t => {
  const signing = deferred(); const e = setup(t, { callbacks: { sign: () => signing.promise } });
  const other = createReplayFixture({ action: 'buy_put', mandateId: 'second-mandate' });
  other.mandate.account = { ...other.mandate.account, subaccount_id: '2' };
  e.store.registerMandate({ customer_id: 'customer-b', release: other.release, mandate: other.mandate, catalog: other.strategy.fieldCatalog });
  e.api.queue(e.operation());
  const callerLease = { ...e.acquire('old', 1000) };
  const pending = e.gateway.submit(callerLease, 'operation-1');
  e.advance(1000);
  const otherLease = e.gateway.acquire({ customer_id: 'customer-b', mandate_id: 'second-mandate' }, { worker_id: 'new', ttl_ms: 1000 });
  Object.assign(callerLease, otherLease);
  signing.resolve('signed:1');
  await assert.rejects(pending, /Stale or expired/);
  assert.equal(e.counters.send, 0);
  assert.equal(e.api.get('operation-1').status, 'submission_unknown');
});
