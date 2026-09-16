'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createControlState, acceptDecision, previewSchedule } = require('../strategy/replay');
const { fixture, clone, withDigest, inputForState, NOW } = require('./helpers/strategy-fixtures');

function accepted(rollDte) {
  const context = fixture(rollDte);
  const state = createControlState(context.mandate);
  const result = acceptDecision({ ...context, state });
  return { ...context, state: result.state };
}

test('acceptance is atomic, immutable, and idempotent, including private state', () => {
  const context = fixture();
  const original = createControlState(context.mandate);
  const first = acceptDecision({ ...context, state: original });
  assert.equal(original.control_revision, 0);
  assert.deepEqual(original.intents, {});
  assert.equal(first.state.control_revision, 1);
  assert.equal(first.state.private_state_version, 1);
  assert.ok(Object.isFrozen(first.state.intents['roll-put-a'].intent.order));
  const replay = acceptDecision({ ...context, state: first.state });
  assert.equal(replay.status, 'replayed');
  assert.deepEqual(replay.state, first.state);
  const conflict = clone(context.decision);
  conflict.operations[0].intent.reason = 'Changed after acceptance';
  assert.throws(() => acceptDecision({ ...context, decision: conflict, state: first.state }), /different content/);
});

test('a late invalid operation commits neither earlier operations nor private state', () => {
  const context = fixture();
  const state = createControlState(context.mandate);
  context.decision.operations.push({ op: 'cancel_intent', intent_id: 'missing', expected_intent_revision: 1, reason: 'test' });
  assert.throws(() => acceptDecision({ ...context, state }));
  assert.deepEqual(state.intents, {});
  assert.equal(state.private_state_version, 0);
});

test('a distinct stale decision cannot reuse an old control/private revision', () => {
  const context = accepted();
  context.decision = { ...context.decision, decision_id: 'stale-decision' };
  assert.throws(() => acceptDecision(context), /revision|stale|snapshot/i);
});

test('accepted policy cannot be changed under the same mandate revision', () => {
  const context = accepted();
  const mandate = clone(context.mandate);
  mandate.parameters.put_roll_dte = '35';
  assert.throws(() => previewSchedule({ ...context, mandate,
    inputBundle: inputForState(context.inputBundle, context.state), now: NOW }), /mandate content changed/);
});

test('policy callbacks cannot mutate caller objects between validation and commit', () => {
  const context = fixture('35');
  const state = createControlState(context.mandate);
  let receivedClock;
  const validateEconomicPolicy = (intent, bundle, options) => {
    receivedClock = options.now;
    assert.equal(options.phase, 'admission');
    context.decision.operations[0].intent.quantity.max_total = 'NaN';
    context.mandate.parameters.put_roll_dte = '90';
    return { valid: true, reasons: [] };
  };
  const acceptedState = acceptDecision({ ...context, state, validateEconomicPolicy }).state;
  assert.equal(acceptedState.intents['roll-put-a'].intent.quantity.max_total, '1');
  assert.equal(receivedClock, NOW);
});

test('preview policy receives the actual evaluation clock and cannot mutate pinned fields', () => {
  const context = accepted('35');
  const inputBundle = inputForState(context.inputBundle, context.state);
  const now = '2030-01-01T12:00:01.000Z';
  const rows = previewSchedule({ ...context, inputBundle, now,
    validateEconomicPolicy: (intent, bundle, options) => {
      assert.equal(options.now, now);
      assert.equal(options.phase, 'schedule');
      inputBundle.instrument_fields['instrument:put-a']['instrument.dte'].value = '99';
      return { valid: true, reasons: [] };
    },
  });
  assert.equal(rows[0].status, 'ready_for_risk_checks');
});

test('preview rejects fabricated active-intent or private-state projections', () => {
  const context = accepted();
  const inputBundle = inputForState(context.inputBundle, context.state);
  inputBundle.active_intents = [];
  assert.throws(() => previewSchedule({ ...context, inputBundle: withDigest(inputBundle, 'input_bundle_id'), now: NOW }), /Active-intent snapshot/);
  const privateInput = inputForState(context.inputBundle, context.state);
  privateInput.private_state.content_ref = 'state:wrong-content';
  assert.throws(() => previewSchedule({ ...context, inputBundle: withDigest(privateInput, 'input_bundle_id'), now: NOW }), /Private-state content reference/);
});

test('first acceptance pins private content even without a private-state update', () => {
  const context = fixture('35');
  delete context.decision.private_state;
  const state = acceptDecision({ ...context, state: createControlState(context.mandate) }).state;
  assert.equal(state.private_state_ref, context.inputBundle.private_state.content_ref);
  const inputBundle = inputForState(context.inputBundle, state,
    { private_state: { version: 0, content_ref: 'state:different-content' } });
  assert.throws(() => previewSchedule({ ...context, state, inputBundle, now: NOW }), /Private-state content reference/);
});

test('accepted private/control history cannot be reused at an earlier input clock', () => {
  const context = accepted('35');
  const earlier = '2030-01-01T11:59:59.000Z';
  const input = inputForState(context.inputBundle, context.state, { evaluated_at: earlier });
  for (const fields of [input.fields, ...Object.values(input.instrument_fields)]) {
    for (const evidence of Object.values(fields)) evidence.observed_at = evidence.available_at = earlier;
  }
  const inputBundle = withDigest(input, 'input_bundle_id');
  assert.throws(() => previewSchedule({ ...context, inputBundle, now: earlier }), /Input clock precedes/);
  const decision = { ...context.decision, decision_id: 'earlier-decision', input_bundle_id: inputBundle.input_bundle_id,
    expected_control_revision: context.state.control_revision, operations: [{ op: 'no_action', reason: 'Old snapshot' }] };
  delete decision.private_state;
  assert.throws(() => acceptDecision({ ...context, inputBundle, decision }), /Input clock precedes/);
});

test('persisted control records cannot smuggle malformed stored intents into preview', () => {
  const context = accepted('35');
  for (const change of [
    intent => { intent.action = 'sell_call'; },
    intent => { intent.order.side = 'buy'; },
    intent => { intent.quantity.max_total = 'NaN'; },
  ]) {
    const state = clone(context.state);
    change(state.intents['roll-put-a'].intent);
    assert.throws(() => previewSchedule({ ...context, state,
      inputBundle: inputForState(context.inputBundle, context.state), now: NOW }));
  }
  const wrongId = clone(context.state);
  wrongId.intents['roll-put-a'].intent.intent_id = 'mismatched';
  assert.throws(() => previewSchedule({ ...context, state: wrongId,
    inputBundle: inputForState(context.inputBundle, context.state), now: NOW }), /identity/);
});

test('no_action leaves standing instructions intact; cancellation keeps reconciliation state', () => {
  const context = accepted();
  const inputBundle = inputForState(context.inputBundle, context.state);
  const decision = { ...context.decision, decision_id: 'no-action-2', input_bundle_id: inputBundle.input_bundle_id,
    expected_control_revision: 1, operations: [{ op: 'no_action', reason: 'Hold existing instructions' }] };
  delete decision.private_state;
  const held = acceptDecision({ ...context, inputBundle, decision }).state;
  assert.equal(held.intents['roll-put-a'].status, 'active');
  const cancelInput = inputForState(inputBundle, held);
  const cancel = { ...decision, decision_id: 'cancel-3', expected_control_revision: 2,
    input_bundle_id: cancelInput.input_bundle_id,
    operations: [{ op: 'cancel_intent', intent_id: 'roll-put-a', expected_intent_revision: 1, reason: 'Customer requested cancellation' }] };
  const cancelled = acceptDecision({ ...context, state: held, inputBundle: cancelInput, decision: cancel }).state;
  assert.equal(cancelled.intents['roll-put-a'].status, 'cancel_requested');
  assert.equal(cancelled.intents['roll-put-a'].intent.intent_revision, 1);
});

test('replacement retains old bounds and waits for reconciliation', () => {
  const context = accepted();
  const inputBundle = inputForState(context.inputBundle, context.state);
  const intent = clone(context.decision.operations[0].intent);
  intent.intent_revision = 2;
  intent.quantity.max_total = '0.5';
  const decision = { ...context.decision, decision_id: 'replace-2', input_bundle_id: inputBundle.input_bundle_id,
    expected_control_revision: 1, operations: [{ op: 'upsert_intent', expected_intent_revision: 1, intent }] };
  delete decision.private_state;
  const next = acceptDecision({ ...context, inputBundle, decision }).state;
  assert.equal(next.intents['roll-put-a'].status, 'replacement_pending_reconciliation');
  assert.equal(next.intents['roll-put-a'].prior_revisions[0].intent.quantity.max_total, '1');
  const rows = previewSchedule({ ...context, state: next, inputBundle: inputForState(inputBundle, next), now: NOW });
  assert.equal(rows[0].status, 'replacement_pending_reconciliation');
});

test('same observed DTE follows accepted 25-day versus 35-day policy through scheduling', () => {
  for (const [rollDte, expected] of [['25', 'waiting'], ['35', 'ready_for_risk_checks']]) {
    const context = accepted(rollDte);
    const rows = previewSchedule({ ...context, inputBundle: inputForState(context.inputBundle, context.state), now: NOW });
    assert.equal(rows[0].status, expected);
    assert.equal(context.state.intents['roll-put-a'].intent.when.right.literal, rollDte);
  }
});

test('each instrument uses its own scoped evidence, never a neighbor quote or DTE', () => {
  const context = fixture('35');
  const second = clone(context.decision.operations[0]);
  second.intent.intent_id = 'roll-put-b';
  second.intent.instrument_ref = 'instrument:put-b';
  second.intent.inventory_ref = 'inventory:put-b';
  context.decision.operations.push(second);
  const state = acceptDecision({ ...context, state: createControlState(context.mandate) }).state;
  const inputBundle = inputForState(context.inputBundle, state);
  const rows = previewSchedule({ ...context, state, inputBundle, now: NOW });
  assert.deepEqual(rows.map(row => [row.intent_id, row.status]), [
    ['roll-put-a', 'ready_for_risk_checks'], ['roll-put-b', 'waiting'],
  ]);
  delete inputBundle.instrument_fields['instrument:put-a'];
  const missing = previewSchedule({ ...context, state, inputBundle: withDigest(inputBundle, 'input_bundle_id'), now: NOW });
  assert.equal(missing[0].condition_result, 'unknown');
});

test('reconciliation check cannot be bypassed by a permissive Strategy condition', () => {
  const context = accepted('35');
  const inputBundle = inputForState(context.inputBundle, context.state);
  inputBundle.fields['account.reconciled'].value = false;
  const rows = previewSchedule({ ...context, inputBundle: withDigest(inputBundle, 'input_bundle_id'), now: NOW });
  assert.equal(rows[0].status, 'blocked');
  assert.match(rows[0].reasons.join(' '), /reconciled account/);
});

test('customer exit prevents new proposals and blocks existing Strategy scheduling', () => {
  const context = accepted('35');
  context.mandate = { ...context.mandate, status: 'exit_requested' };
  const rows = previewSchedule({ ...context, inputBundle: inputForState(context.inputBundle, context.state), now: NOW });
  assert.equal(rows[0].status, 'blocked');
  assert.match(rows[0].reasons.join(' '), /Customer lifecycle/);
  const fresh = fixture('35');
  fresh.mandate.status = 'exit_requested';
  assert.throws(() => acceptDecision({ ...fresh, state: createControlState(fresh.mandate) }), /active|exit|lifecycle/i);
});

test('expiry stops preview without erasing the instruction or assuming cancellation', () => {
  const context = accepted('35');
  const rows = previewSchedule({ ...context, inputBundle: inputForState(context.inputBundle, context.state), now: '2030-01-01T12:05:00.000Z' });
  assert.equal(rows[0].status, 'expired');
  assert.equal(context.state.intents['roll-put-a'].status, 'active');
  assert.match(rows[0].reasons.join(' '), /cancellation and reconciliation/);
});

test('offline preview does not equate dependency acknowledgement with accounted fills', () => {
  const context = fixture('35');
  context.decision.operations[0].intent.dependencies = [{ condition: 'ledger_event', event_ref: 'settlement:replace-put' }];
  const state = acceptDecision({ ...context, state: createControlState(context.mandate) }).state;
  const rows = previewSchedule({ ...context, state, inputBundle: inputForState(context.inputBundle, state), now: NOW });
  assert.equal(rows[0].status, 'blocked');
  assert.match(rows[0].reasons.join(' '), /authoritative ledger evidence/);
});
