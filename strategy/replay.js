'use strict';

// Pure, in-memory control and scheduling preview. This module never reserves
// funds, submits an order, or treats a preview as permission to sign.
const {
  canonicalize, contentDigest, validateRelease, validateMandate,
  validateInputBundle, validateDecision, validateStoredIntent, timestamp,
} = require('./contract');
const { evaluateCondition, DEFAULT_FIELD_CATALOG } = require('./conditions');

const clone = (value) => JSON.parse(canonicalize(value));
const owns = (object, key) => Object.prototype.hasOwnProperty.call(object, key);

function freeze(value) {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

function assertRevision(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw new Error(`${name} must be a nonnegative safe integer`);
}

function mandateDigest(mandate) {
  const accepted = clone(mandate);
  // Lifecycle is a current projection; changing economic terms, authority, or
  // account identity requires a new accepted revision instead.
  delete accepted.status;
  return contentDigest(accepted);
}

function exactKeys(value, keys, label) {
  if (!value || Array.isArray(value) || typeof value !== 'object'
      || canonicalize(Object.keys(value).sort()) !== canonicalize([...keys].sort())) {
    throw new Error(`${label} has an invalid shape`);
  }
}

function assertState(state, mandate, { release, catalog = DEFAULT_FIELD_CATALOG } = {}) {
  // Canonicalization rejects accessors, cycles, unsafe keys, and non-JSON data.
  canonicalize(state);
  exactKeys(state, ['mandate_id', 'mandate_revision', 'mandate_digest', 'control_revision', 'private_state_version', 'private_state_ref', 'last_input_at', 'intents', 'decisions'], 'Control state');
  if (state.mandate_id !== mandate.mandate_id || state.mandate_revision !== mandate.mandate_revision) {
    throw new Error('Control state does not belong to this mandate revision');
  }
  if (state.mandate_digest !== mandateDigest(mandate)) throw new Error('Accepted mandate content changed without a new revision');
  assertRevision(state.control_revision, 'control_revision');
  assertRevision(state.private_state_version, 'private_state_version');
  if (!state.intents || Array.isArray(state.intents) || typeof state.intents !== 'object'
      || !state.decisions || Array.isArray(state.decisions) || typeof state.decisions !== 'object') {
    throw new Error('Control state requires intent and decision maps');
  }
  if (state.private_state_ref !== null && (typeof state.private_state_ref !== 'string' || !state.private_state_ref)) {
    throw new Error('Invalid private-state reference');
  }
  if (state.control_revision > 0 && state.private_state_ref === null) throw new Error('Accepted history must pin its private-state content');
  if (state.last_input_at !== null) timestamp(state.last_input_at);
  if ((state.control_revision === 0) !== (state.last_input_at === null)) throw new Error('Invalid accepted input clock');
  const statuses = ['active', 'cancel_requested', 'replacement_pending_reconciliation'];
  if (Object.keys(state.intents).length > 1000) throw new Error('Offline intent capacity exceeded');
  for (const [id, record] of Object.entries(state.intents)) {
    exactKeys(record, ['intent', 'status', 'prior_revisions'], 'Intent record');
    if (record.intent.intent_id !== id || !statuses.includes(record.status) || !Array.isArray(record.prior_revisions)) {
      throw new Error('Intent record identity, status, or history is invalid');
    }
    validateStoredIntent(record.intent, { release, mandate, catalog });
    if (record.prior_revisions.length !== record.intent.intent_revision - 1) throw new Error('Incomplete intent revision history');
    record.prior_revisions.forEach((previous, index) => {
      exactKeys(previous, ['intent', 'status'], 'Prior intent record');
      if (previous.intent.intent_id !== id || previous.intent.intent_revision !== index + 1 || !statuses.includes(previous.status)) {
        throw new Error('Invalid prior intent revision');
      }
      validateStoredIntent(previous.intent, { release, mandate, catalog });
    });
  }
  const revisions = new Set();
  for (const record of Object.values(state.decisions)) {
    exactKeys(record, ['digest', 'accepted_control_revision'], 'Decision receipt');
    if (!/^sha256:[a-f0-9]{64}$/.test(record.digest)) throw new Error('Invalid decision receipt digest');
    assertRevision(record.accepted_control_revision, 'accepted_control_revision');
    if (record.accepted_control_revision < 1 || record.accepted_control_revision > state.control_revision) throw new Error('Invalid decision receipt revision');
    revisions.add(record.accepted_control_revision);
  }
  if (revisions.size !== Object.keys(state.decisions).length || revisions.size !== state.control_revision) {
    throw new Error('Incomplete or conflicting offline decision receipts');
  }
}

function assertBundleState(inputBundle, state) {
  if (state.last_input_at !== null && timestamp(inputBundle.evaluated_at) < timestamp(state.last_input_at)) {
    throw new Error('Input clock precedes already accepted decision history');
  }
  if (inputBundle.control_revision !== state.control_revision
      || inputBundle.private_state.version !== state.private_state_version) {
    throw new Error('A fresh control/private-state snapshot is required');
  }
  if (state.private_state_ref !== null && inputBundle.private_state.content_ref !== state.private_state_ref) {
    throw new Error('Private-state content reference differs from accepted state');
  }
  const byId = (left, right) => left.intent_id < right.intent_id ? -1 : left.intent_id > right.intent_id ? 1 : 0;
  const projected = Object.values(state.intents).map(record => ({
    intent_id: record.intent.intent_id, intent_revision: record.intent.intent_revision, status: record.status,
  })).sort(byId);
  if (canonicalize(projected) !== canonicalize([...inputBundle.active_intents].sort(byId))) {
    throw new Error('Active-intent snapshot differs from accepted control state');
  }
}

function createControlState(mandate, privateStateVersion = 0) {
  validateMandate(mandate);
  assertRevision(privateStateVersion, 'private_state_version');
  return freeze({
    mandate_id: mandate.mandate_id,
    mandate_revision: mandate.mandate_revision,
    mandate_digest: mandateDigest(mandate),
    control_revision: 0,
    private_state_version: privateStateVersion,
    private_state_ref: null,
    last_input_at: null,
    intents: {},
    decisions: {},
  });
}

function checkPolicy(validator, intent, inputBundle, mandate, release, now, phase) {
  if (typeof validator !== 'function') throw new Error('An explicit Strategy economic validator is required');
  const result = validator(freeze(clone(intent)), freeze(clone(inputBundle)), {
    mandate: freeze(clone(mandate)), release: freeze(clone(release)), now, phase,
  });
  if (!result || result.valid !== true || !Array.isArray(result.reasons) || result.reasons.length !== 0) {
    const reasons = Array.isArray(result?.reasons) ? result.reasons.join('; ') : 'invalid validator result';
    throw new Error(`Strategy policy rejected ${intent.intent_id}: ${reasons}`);
  }
}

function acceptDecision({ state, decision, release, mandate, inputBundle, catalog = DEFAULT_FIELD_CATALOG, validateEconomicPolicy }) {
  // Callbacks must not change the caller-owned objects between validation and
  // commit. Work exclusively on pinned JSON snapshots throughout this call.
  [state, decision, release, mandate, inputBundle, catalog] = [state, decision, release, mandate, inputBundle, catalog].map(value => freeze(clone(value)));
  validateRelease(release);
  validateMandate(mandate, { release });
  assertState(state, mandate, { release, catalog });
  const digest = contentDigest(decision);
  if (typeof decision.decision_id === 'string' && owns(state.decisions, decision.decision_id)) {
    const previous = state.decisions[decision.decision_id];
    if (previous.digest !== digest) throw new Error('Decision ID reused with different content');
    return { status: 'replayed', state, accepted_control_revision: previous.accepted_control_revision };
  }
  validateInputBundle(inputBundle, { release, mandate, catalog });
  assertBundleState(inputBundle, state);
  validateDecision(decision, { release, mandate, inputBundle, controlState: state, catalog });
  if (state.control_revision === Number.MAX_SAFE_INTEGER) throw new Error('Control revision exhausted');

  // Validate the entire transaction before constructing the next state. A later
  // failure cannot leave earlier upserts or private-state updates committed.
  for (const operation of decision.operations) {
    if (operation.op === 'upsert_intent') {
      if (mandate.status !== 'active') throw new Error('Mandate is not active for Strategy proposals');
      checkPolicy(validateEconomicPolicy, operation.intent, inputBundle, mandate, release, inputBundle.evaluated_at, 'admission');
    }
  }

  const next = clone(state);
  if (next.private_state_ref === null) next.private_state_ref = inputBundle.private_state.content_ref;
  next.last_input_at = inputBundle.evaluated_at;
  for (const operation of decision.operations) {
    if (operation.op === 'upsert_intent') {
      const intent = operation.intent;
      const previous = owns(next.intents, intent.intent_id) ? next.intents[intent.intent_id] : null;
      next.intents[intent.intent_id] = {
        intent: clone(intent),
        status: previous ? 'replacement_pending_reconciliation' : 'active',
        prior_revisions: previous
          ? [...previous.prior_revisions, { intent: previous.intent, status: previous.status }]
          : [],
      };
    } else if (operation.op === 'cancel_intent') {
      next.intents[operation.intent_id].status = 'cancel_requested';
    }
  }
  if (decision.private_state) {
    next.private_state_version = decision.private_state.proposed_version;
    next.private_state_ref = decision.private_state.content_ref;
  }
  next.control_revision += 1;
  next.decisions[decision.decision_id] = { digest, accepted_control_revision: next.control_revision };
  assertState(next, mandate, { release, catalog });
  return { status: 'accepted', state: freeze(next), accepted_control_revision: next.control_revision };
}

function previewSchedule({ state, release, mandate, inputBundle, now, catalog = DEFAULT_FIELD_CATALOG, validateEconomicPolicy }) {
  [state, release, mandate, inputBundle, catalog] = [state, release, mandate, inputBundle, catalog].map(value => freeze(clone(value)));
  validateRelease(release);
  validateMandate(mandate, { release });
  validateInputBundle(inputBundle, { release, mandate, catalog });
  assertState(state, mandate, { release, catalog });
  assertBundleState(inputBundle, state);
  // This comparison also validates the evaluation clock through the evaluator.
  const account = evaluateCondition({
    op: 'eq', left: { ref: 'account.reconciled' }, right: { literal: true, unit: 'boolean' },
  }, { fields: inputBundle.fields, now, catalog });
  const nowMs = Date.parse(now);
  if (Date.parse(inputBundle.evaluated_at) > nowMs) throw new Error('Input bundle is from the future');

  return Object.values(state.intents).map((record) => {
    const intent = record.intent;
    const row = {
      intent_id: intent.intent_id, intent_revision: intent.intent_revision,
      action: intent.action, priority: intent.priority,
      status: 'blocked', condition_result: 'unknown', reasons: [],
    };
    if (record.status !== 'active') {
      row.status = record.status;
      row.reasons.push('Prior venue operations must be reconciled before further fulfillment');
    } else if (mandate.status !== 'active') {
      row.reasons.push('Customer lifecycle blocks Strategy scheduling');
    } else if (nowMs >= Date.parse(intent.expires_at)) {
      row.status = 'expired';
      row.reasons.push('Expired instructions stop new work; any venue orders still require cancellation and reconciliation');
    } else if (nowMs < Date.parse(intent.active_from)) {
      row.status = 'waiting';
      row.reasons.push('Activation time has not arrived');
    } else if (account.value !== true) {
      row.reasons.push('A fresh, reconciled account is required', ...account.reasons);
    } else if (intent.dependencies.length > 0) {
      row.reasons.push('Dependencies require authoritative ledger evidence; offline preview cannot satisfy them');
    } else {
      try {
        validateStoredIntent(intent, { release, mandate, inputBundle, catalog });
        const result = evaluateCondition(intent.when, {
          fields: { ...inputBundle.fields, ...(inputBundle.instrument_fields[intent.instrument_ref || intent.route_ref] || {}) },
          now, catalog, maxQuoteAgeMs: intent.max_quote_age_ms,
        });
        row.condition_result = result.value;
        row.reasons.push(...result.reasons);
        if (result.value === true) checkPolicy(validateEconomicPolicy, intent, inputBundle, mandate, release, now, 'schedule');
        row.status = result.value === true ? 'ready_for_risk_checks' : 'waiting';
        if (result.value === true) row.reasons.push('Preview only: funds, inventory, margin, reservations, venue permissions, and signing are not implemented');
      } catch (error) {
        row.reasons.push(error.message);
      }
    }
    return row;
  }).sort((left, right) => right.priority - left.priority
    || (left.intent_id < right.intent_id ? -1 : left.intent_id > right.intent_id ? 1 : 0));
}

module.exports = { createControlState, acceptDecision, previewSchedule };
