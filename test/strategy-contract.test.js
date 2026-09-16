'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const {
  validateRelease, validateMandate, validateInputBundle, validateDecision,
  validateSchema, validateStoredIntent, contentDigest, canonicalize,
} = require('../strategy/contract');

const clone = value => JSON.parse(JSON.stringify(value));
function sign(value, key) { delete value[key]; value[key] = contentDigest(value); return value; }

function fixture() {
  const release = sign({
    contract_version: 'noop.strategy/v1', strategy_id: 'test-protection', strategy_release_id: 'test-protection/1.0.0',
    artifact: { digest: contentDigest('artifact'), entrypoint: 'run', dependency_lock_digest: contentDigest('lock') },
    parameters: {
      put_roll_dte: { type: 'decimal', unit: 'day', minimum: '0', maximum: '365', default: '25', description: 'Release-owned roll threshold.' },
      call_capture_pct: { type: 'decimal', unit: 'percent', minimum: '0', maximum: '100', default: '80', description: 'Release-owned call capture threshold.' },
      margin_target: { type: 'decimal', unit: 'ratio', minimum: '0', maximum: '1', default: '0.45', description: 'Projected initial margin over margin equity.' },
    },
    capabilities: { actions: ['buy_put', 'sell_put', 'sell_call', 'buyback_call', 'buy_spot_eth', 'sell_spot_eth'], instrument_kinds: ['put', 'call'], venues: ['derive-v3'], spot_routes: ['route:fixture-eth'] },
    runtime: { max_duration_ms: 1000, max_memory_mb: 128, max_output_bytes: 65536 }, extensions: {},
  }, 'release_digest');
  const mandate = {
    contract_version: 'noop.strategy/v1', mandate_id: 'mandate-example', mandate_revision: 1,
    strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
    account: { network: 'testnet', chain_id: 901, venue: 'derive-v3', deployment: 'fixture-deployment', owner: '0x1111111111111111111111111111111111111111', subaccount_id: '123', manager_id: '1', risk_universe: 'ETH' },
    protected_eth: '20', parameters: { put_roll_dte: '25', call_capture_pct: '80', margin_target: '0.45' },
    authority: { actions: [...release.capabilities.actions], instrument_refs: ['instrument:put', 'instrument:call'], spot_routes: ['route:fixture-eth'], budget_refs: ['budget:premium'], inventory_refs: ['inventory:put', 'inventory:call', 'inventory:eth'], liability_refs: ['liability:calls'] },
    status: 'active', extensions: {},
  };
  const now = '2030-01-01T12:00:00.000Z';
  const field = (value, unit) => ({ value, unit, observed_at: now, available_at: now, quality: 'valid' });
  const inputBundle = sign({
    contract_version: 'noop.strategy/v1', strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
    mandate_id: mandate.mandate_id, mandate_revision: 1, sequence: 1, evaluated_at: now, invocation_reason: 'replay', account: clone(mandate.account),
    fields: { 'account.reconciled': field(true, 'boolean') },
    instrument_fields: { 'instrument:put': { 'instrument.best_ask': field('100', 'USDC/contract') } },
    instruments: {
      'instrument:put': { kind: 'put', base_asset: 'ETH', quote_asset: 'USDC', expiry: '2030-02-01T08:00:00.000Z', contract_size: '1' },
      'instrument:call': { kind: 'call', base_asset: 'ETH', quote_asset: 'USDC', expiry: '2030-02-01T08:00:00.000Z', contract_size: '1' },
    },
    private_state: { version: 0, content_ref: 'state:mandate-example/0' }, control_revision: 0, active_intents: [], extensions: {},
  }, 'input_bundle_id');
  const intent = {
    intent_id: 'renewal-put-1', intent_revision: 1, action: 'buy_put', purpose: 'protection_renewal', authority_ref: 'mandate:mandate-example/1', instrument_ref: 'instrument:put',
    active_from: now, expires_at: '2030-01-01T12:05:00.000Z', evaluation_interval_ms: 1000, max_quote_age_ms: 2000, max_attempts: 1, priority: 10,
    budget_ref: 'budget:premium', reservation_policy: 'reserve_on_activation', quantity: { max_total: '0.1', unit: 'contract' },
    order: { side: 'buy', time_in_force: 'ioc', reduce_only: false, limit_price: { value: '100', unit: 'USDC/contract' }, max_total_outlay: { value: '10.10', unit: 'USDC' }, max_total_fees: { value: '0.10', unit: 'USDC' } },
    requires_reconciled_account: true, dependencies: [], when: { op: 'lte', left: { ref: 'instrument.best_ask' }, right: { literal: '100', unit: 'USDC/contract' } }, reason: 'Renew protection.',
  };
  const decision = {
    contract_version: 'noop.strategy/v1', strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
    mandate_id: mandate.mandate_id, mandate_revision: 1, input_bundle_id: inputBundle.input_bundle_id, decision_id: 'decision-1', expected_control_revision: 0,
    private_state: { expected_version: 0, proposed_version: 1, content_ref: 'state:mandate-example/1' },
    operations: [{ op: 'upsert_intent', expected_intent_revision: null, intent }],
  };
  const controlState = { mandate_id: mandate.mandate_id, mandate_revision: 1, control_revision: 0, private_state_version: 0, intents: {}, decisions: {} };
  return { release, mandate, inputBundle, decision, controlState, intent };
}
function valid(f) { return validateDecision(f.decision, f); }
function repinInput(f) { sign(f.inputBundle, 'input_bundle_id'); f.decision.input_bundle_id = f.inputBundle.input_bundle_id; }
function previous(f) {
  const old = clone(f.intent);
  f.controlState.intents[old.intent_id] = { intent: old, status: 'active', prior_revisions: [] };
  f.inputBundle.active_intents = [{ intent_id: old.intent_id, intent_revision: old.intent_revision, status: 'active' }];
  f.intent.intent_revision = 2;
  f.decision.operations[0].expected_intent_revision = 1;
  repinInput(f);
}

test('realistically bounded release, mandate, bundle and decision validate without mutation', () => {
  const f = fixture(); const before = canonicalize(f);
  assert.equal(validateRelease(f.release), f.release);
  assert.equal(validateMandate(f.mandate, f), f.mandate);
  assert.equal(validateInputBundle(f.inputBundle, f), f.inputBundle);
  assert.equal(valid(f), f.decision);
  assert.equal(canonicalize(f), before);
});

test('economic settings are materialized Strategy parameters, not V2 platform thresholds', () => {
  const f = fixture();
  f.mandate.parameters = { put_roll_dte: '35', call_capture_pct: '70', margin_target: '0.30' };
  assert.doesNotThrow(() => valid(f));
});

for (const [name, change, message] of [
  ['unknown root field', f => { f.decision.transfer_destination = 'attacker'; }, /forbidden/],
  ['unknown intent field', f => { f.intent.margin_target = '0.9'; }, /forbidden/],
  ['unknown nested order field', f => { f.intent.order.gas_override = '100'; }, /forbidden/],
  ['unknown version', f => { f.decision.contract_version = 'noop.strategy/v2'; }, /constant/],
  ['numeric financial value', f => { f.intent.quantity.max_total = 0.1; }, /safe integers|expected string/],
  ['exponent decimal', f => { f.intent.quantity.max_total = '1e-1'; }, /encoding/],
  ['leading zero decimal', f => { f.intent.quantity.max_total = '00.1'; }, /encoding/],
  ['negative quantity', f => { f.intent.quantity.max_total = '-0.1'; }, /positive/],
  ['zero quantity', f => { f.intent.quantity.max_total = '0'; }, /positive/],
  ['wrong option quantity unit', f => { f.intent.quantity.unit = 'ETH'; }, /quantity unit/],
  ['wrong limit price currency', f => { f.intent.order.limit_price.unit = 'USDC/ETH'; }, /price unit/],
  ['wrong fees currency', f => { f.intent.order.max_total_fees.unit = 'ETH'; }, /fee currency/],
  ['negative fee', f => { f.intent.order.max_total_fees.value = '-0.1'; }, /nonnegative/],
  ['insufficient bounded outlay', f => { f.intent.order.max_total_outlay.value = '10.09'; }, /outlay/],
  ['wrong action side', f => { f.intent.order.side = 'sell'; }, /side mismatch/],
  ['false close-only claim on entry', f => { f.intent.order.reduce_only = true; }, /reduce-only/],
  ['unreconciled execution permission', f => { f.intent.requires_reconciled_account = false; }, /constant/],
  ['zero attempts', f => { f.intent.max_attempts = 0; }, /minimum/],
  ['ambiguous timestamp precision', f => { f.intent.expires_at = '2030-01-01T12:05:00Z'; }, /milliseconds/],
  ['invalid calendar date', f => { f.intent.expires_at = '2030-02-30T12:05:00.000Z'; }, /calendar/],
  ['empty activation interval', f => { f.intent.expires_at = f.intent.active_from; }, /interval/],
  ['intent outlives option', f => { f.intent.expires_at = '2031-01-01T00:00:00.000Z'; }, /outlives/],
  ['incorrect instrument kind', f => { f.intent.instrument_ref = 'instrument:call'; }, /kind mismatch/],
  ['unapproved funds', f => { f.intent.budget_ref = 'budget:other-tenant'; }, /unauthorized/],
  ['wrong mandate authority', f => { f.intent.authority_ref = 'mandate:other/1'; }, /authority reference/],
  ['wrong release digest', f => { f.decision.release_digest = contentDigest('other'); }, /digest mismatch/],
  ['wrong customer identity', f => { f.decision.mandate_id = 'other'; }, /mandate_id mismatch/],
  ['wrong mandate revision', f => { f.decision.mandate_revision = 2; }, /mandate_revision mismatch/],
  ['stale control revision', f => { f.controlState.control_revision = 1; }, /stale control/],
  ['stale private-state CAS', f => { f.decision.private_state.expected_version = 1; f.decision.private_state.proposed_version = 2; }, /private state mismatch/],
  ['private state version skip', f => { f.decision.private_state.proposed_version = 2; }, /advance once/],
  ['new intent revision skip', f => { f.intent.intent_revision = 2; }, /advance once/],
  ['exit lifecycle upsert', f => { f.mandate.status = 'exit_requested'; }, /lifecycle/],
  ['self dependency', f => { f.intent.dependencies = [{ intent_id: f.intent.intent_id, intent_revision: 1, condition: 'cancelled_and_reconciled' }]; }, /self dependency/],
  ['acknowledgement dependency', f => { f.intent.dependencies = [{ intent_id: 'other', intent_revision: 1, condition: 'acknowledged' }]; }, /oneOf/],
  ['absent predecessor', f => { f.intent.dependencies = [{ intent_id: 'other', intent_revision: 1, condition: 'cancelled_and_reconciled' }]; }, /predecessor missing/],
  ['unknown condition ref', f => { f.intent.when.left.ref = 'designer.available_money'; }, /registered|catalog/],
  ['condition unit mismatch', f => { f.intent.when.right.unit = 'ETH'; }, /unit/],
  ['duplicate intent operations', f => { f.decision.operations.push(clone(f.decision.operations[0])); }, /multiple operations/],
  ['no_action mixed with mutations', f => { f.decision.operations.push({ op: 'no_action', reason: 'No change.' }); }, /only operation/],
  ['prototype identifier', f => { f.intent.intent_id = 'constructor'; }, /encoding/],
]) test(`rejects ${name}`, () => { const f = fixture(); change(f); assert.throws(() => valid(f), message); });

test('release digest commits code, dependency lock, parameters and capability changes', () => {
  const f = fixture(); f.release.parameters.put_roll_dte.default = '35';
  assert.throws(() => validateRelease(f.release), /does not match/);
  sign(f.release, 'release_digest');
  assert.doesNotThrow(() => validateRelease(f.release));
  assert.throws(() => validateMandate(f.mandate, f), /digest mismatch/);
});

test('missing, additional and out-of-range accepted parameters fail', () => {
  for (const mutate of [m => { delete m.parameters.put_roll_dte; }, m => { m.parameters.extra = '1'; }, m => { m.parameters.margin_target = '1.1'; }]) {
    const f = fixture(); mutate(f.mandate); assert.throws(() => validateMandate(f.mandate, f), /materialize|range/);
  }
});

test('release must declare instrument kinds and spot routes for its actions', () => {
  const f = fixture(); f.release.capabilities.spot_routes = []; sign(f.release, 'release_digest');
  assert.throws(() => validateRelease(f.release), /route/);
});

test('input digest detects silent evidence changes', () => {
  const f = fixture(); f.inputBundle.instrument_fields['instrument:put']['instrument.best_ask'].value = '1';
  assert.throws(() => valid(f), /does not match canonical bundle/);
});

test('instrument evidence cannot live in account scope or use another tenant route', () => {
  const f = fixture(); f.inputBundle.fields['instrument.best_ask'] = f.inputBundle.instrument_fields['instrument:put']['instrument.best_ask']; repinInput(f);
  assert.throws(() => valid(f), /scope mismatch/);
  delete f.inputBundle.fields['instrument.best_ask'];
  f.inputBundle.instrument_fields['route:other-tenant'] = {}; repinInput(f);
  assert.throws(() => valid(f), /unauthorized/);
});

test('future evidence and wrong registered units fail input validation', () => {
  const f = fixture(); const field = f.inputBundle.instrument_fields['instrument:put']['instrument.best_ask'];
  field.available_at = '2030-01-01T12:00:01.000Z'; repinInput(f);
  assert.throws(() => valid(f), /future or inverted/);
  field.available_at = field.observed_at; field.unit = 'ETH'; repinInput(f);
  assert.throws(() => valid(f), /registered catalog/);
});

test('missing evidence is distinct from zero and valid evidence requires a typed value', () => {
  const f = fixture(); const field = f.inputBundle.instrument_fields['instrument:put']['instrument.best_ask'];
  field.value = null; field.quality = 'missing'; repinInput(f);
  assert.doesNotThrow(() => valid(f));
  field.quality = 'valid'; repinInput(f);
  assert.throws(() => valid(f), /typed value/);
});

test('account identity pins manager, deployment, network, subaccount and owner', () => {
  for (const key of ['manager_id', 'deployment', 'network', 'subaccount_id', 'owner']) {
    const f = fixture();
    f.inputBundle.account[key] = key === 'owner' ? '0x2222222222222222222222222222222222222222' : key === 'network' ? 'mainnet' : key.endsWith('_id') ? '999' : 'other';
    repinInput(f); assert.throws(() => valid(f), /account scope/);
  }
});

test('replacement and cancellation require the retained intent revision', () => {
  const f = fixture(); previous(f); assert.doesNotThrow(() => valid(f));
  f.decision.operations[0].expected_intent_revision = 2;
  assert.throws(() => valid(f), /stale or missing intent/);
  f.decision.operations = [{ op: 'cancel_intent', intent_id: f.intent.intent_id, expected_intent_revision: 1, reason: 'Customer stopped this condition.' }];
  f.mandate.status = 'exit_requested'; assert.doesNotThrow(() => valid(f));
});

test('stale active-intent projection cannot masquerade as current evidence', () => {
  const f = fixture(); previous(f); f.inputBundle.active_intents = []; repinInput(f);
  assert.throws(() => valid(f), /control projection/);
});

test('no_action preserves standing intentions and may advance private state', () => {
  const f = fixture(); previous(f);
  f.decision.operations = [{ op: 'no_action', reason: 'No opportunity.' }];
  assert.doesNotThrow(() => valid(f));
});

test('close intents require source inventory and native reduce-only request', () => {
  const f = fixture(); f.intent.action = 'sell_put'; f.intent.order.side = 'sell'; f.intent.order.reduce_only = true;
  delete f.intent.order.max_total_outlay; f.intent.inventory_ref = 'inventory:put';
  assert.doesNotThrow(() => valid(f));
  f.intent.order.reduce_only = false; assert.throws(() => valid(f), /reduce-only/);
});

test('call entries require explicit finite capacity, not a global margin percentage', () => {
  const f = fixture(); f.intent.action = 'sell_call'; f.intent.instrument_ref = 'instrument:call'; f.intent.order.side = 'sell';
  delete f.intent.order.max_total_outlay; f.intent.liability_ref = 'liability:calls';
  f.intent.order.max_total_liability = { value: '1000', unit: 'USDC' };
  assert.doesNotThrow(() => valid(f));
  delete f.intent.order.max_total_liability; assert.throws(() => valid(f), /finite stressed-liability/);
});

test('spot proposals require route, ETH unit and accepted funds without derivative substitution', () => {
  const f = fixture(); f.intent.action = 'buy_spot_eth'; delete f.intent.instrument_ref; f.intent.route_ref = 'route:fixture-eth';
  f.intent.quantity.unit = 'ETH'; f.intent.order.limit_price.unit = 'USDC/ETH';
  assert.doesNotThrow(() => valid(f));
  f.intent.route_ref = 'route:unvalidated'; assert.throws(() => valid(f), /unauthorized/);
});

test('dependency threshold uses accounted quantity and exact predecessor unit', () => {
  const f = fixture(); const next = clone(f.intent); next.intent_id = 'renewal-put-2';
  next.dependencies = [{ intent_id: f.intent.intent_id, intent_revision: 1, condition: 'filled_and_accounted', minimum_quantity: { value: '0.05', unit: 'contract' } }];
  f.decision.operations.push({ op: 'upsert_intent', expected_intent_revision: null, intent: next }); assert.doesNotThrow(() => valid(f));
  next.dependencies[0].minimum_quantity.value = '0.11'; assert.throws(() => valid(f), /exceeds predecessor/);
  next.dependencies[0].minimum_quantity.value = '0.05'; next.dependencies[0].minimum_quantity.unit = 'ETH'; assert.throws(() => valid(f), /dependency quantity unit/);
});

test('dependencies reject multi-intent cycles', () => {
  const f = fixture(); const second = clone(f.intent); second.intent_id = 'renewal-put-2';
  second.dependencies = [{ intent_id: f.intent.intent_id, intent_revision: 1, condition: 'cancelled_and_reconciled' }];
  f.intent.dependencies = [{ intent_id: second.intent_id, intent_revision: 1, condition: 'cancelled_and_reconciled' }];
  f.decision.operations.push({ op: 'upsert_intent', expected_intent_revision: null, intent: second });
  assert.throws(() => valid(f), /cycle/);
});

test('canonical digests are stable under object order and preserve financial decimal encoding', () => {
  assert.equal(contentDigest({ b: 2, a: '0.10' }), contentDigest({ a: '0.10', b: 2 }));
  assert.notEqual(contentDigest({ a: '0.10' }), contentDigest({ a: '0.1' }));
  assert.equal(canonicalize({ z: [true, null], a: 'ETH' }), '{"a":"ETH","z":[true,null]}');
});

test('canonicalization rejects unsafe host objects, hidden values and ambiguous numbers', () => {
  const cyclic = {}; cyclic.self = cyclic;
  const getter = {}; Object.defineProperty(getter, 'secret', { enumerable: true, get() { throw new Error('Getter was invoked'); } });
  const hidden = {}; Object.defineProperty(hidden, 'secret', { value: 1 });
  for (const bad of [undefined, NaN, Infinity, -0, 0.1, 9007199254740992, new Date(), cyclic, getter, hidden, [1, , 3], JSON.parse('{"__proto__":1}'), '\ud800']) {
    assert.throws(() => canonicalize(bad), error => !/Getter was invoked/.test(error.message));
  }
});

test('schema-only interface rejects unknown schema names and oversized conditions', () => {
  assert.throws(() => validateSchema({}, 'unknown.json'), /unknown schema/);
  const f = fixture(); const leaf = clone(f.intent.when);
  f.intent.when = { op: 'all', args: Array.from({ length: 33 }, () => leaf) };
  assert.throws(() => valid(f), /too many|oneOf/);
});

test('retained intents validate their shape and action even after expiry', () => {
  const f = fixture();
  f.inputBundle.evaluated_at = '2030-01-02T12:00:00.000Z'; repinInput(f);
  assert.equal(validateStoredIntent(f.intent, f), f.intent);
  assert.throws(() => valid(f), /already expired/);
  f.intent.order.side = 'sell'; assert.throws(() => validateStoredIntent(f.intent, f), /side mismatch/);
  f.intent.order.side = 'buy'; f.intent.quantity.max_total = '0'; assert.throws(() => validateStoredIntent(f.intent, f), /positive/);
});

test('canonical JSON byte limit is enforced while constructing output', () => {
  assert.throws(() => canonicalize(Array.from({ length: 5 }, () => 'x'.repeat(1048576))), /byte capacity/);
});

test('a replacement retains dependencies on the superseded current revision', () => {
  const f = fixture(); previous(f);
  const dependent = clone(f.controlState.intents[f.intent.intent_id].intent);
  dependent.intent_id = 'dependent-put';
  dependent.dependencies = [{ intent_id: f.intent.intent_id, intent_revision: 1, condition: 'cancelled_and_reconciled' }];
  f.controlState.intents[dependent.intent_id] = { intent: dependent, status: 'active', prior_revisions: [] };
  f.inputBundle.active_intents.push({ intent_id: dependent.intent_id, intent_revision: 1, status: 'active' });
  // rev2 -> dependent rev1 -> original rev1 is acyclic even though IDs repeat.
  f.intent.dependencies = [{ intent_id: dependent.intent_id, intent_revision: 1, condition: 'cancelled_and_reconciled' }];
  repinInput(f);
  assert.doesNotThrow(() => valid(f));
});
