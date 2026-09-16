'use strict';

const { contentDigest } = require('../../strategy/canonical');
const NOW = '2030-01-01T12:00:00.000Z';
const clone = (value) => JSON.parse(JSON.stringify(value));
function withDigest(value, key) {
  const copy = clone(value);
  delete copy[key];
  return { ...copy, [key]: contentDigest(copy) };
}
const field = (value, unit) => ({ value, unit, observed_at: NOW, available_at: NOW, quality: 'valid' });

function fixture(rollDte = '25') {
  const release = withDigest({
    contract_version: 'noop.strategy/v1', strategy_id: 'test-roll',
    strategy_release_id: 'test-roll/1',
    artifact: { digest: contentDigest('fixture-only'), entrypoint: 'test:fixture', dependency_lock_digest: contentDigest([]) },
    parameters: { put_roll_dte: { type: 'decimal', unit: 'day', minimum: '1', maximum: '90', default: '25', description: 'Test-only roll threshold' } },
    capabilities: { actions: ['sell_put'], instrument_kinds: ['put'], venues: ['derive'], spot_routes: [] },
    runtime: { max_duration_ms: 1000, max_memory_mb: 64, max_output_bytes: 65536 }, extensions: {},
  }, 'release_digest');
  const account = {
    network: 'testnet', chain_id: 11155111, venue: 'derive', deployment: 'fixture-only',
    owner: `0x${'1'.repeat(40)}`, subaccount_id: '1', manager_id: '1', risk_universe: 'fixture-eth',
  };
  const mandate = {
    contract_version: 'noop.strategy/v1', mandate_id: 'fixture-mandate', mandate_revision: 1,
    strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
    account, protected_eth: '20', parameters: { put_roll_dte: rollDte }, status: 'active', extensions: {},
    authority: { actions: ['sell_put'], instrument_refs: ['instrument:put-a', 'instrument:put-b'],
      spot_routes: [], budget_refs: [], inventory_refs: ['inventory:put-a', 'inventory:put-b'], liability_refs: [] },
  };
  const inputBundle = withDigest({
    contract_version: 'noop.strategy/v1', strategy_release_id: release.strategy_release_id,
    release_digest: release.release_digest, mandate_id: mandate.mandate_id, mandate_revision: 1,
    sequence: 1, evaluated_at: NOW, invocation_reason: 'replay', account,
    fields: { 'account.reconciled': field(true, 'boolean') },
    instrument_fields: {
      'instrument:put-a': { 'instrument.dte': field('29', 'day'), 'instrument.best_bid': field('100', 'USDC/contract') },
      'instrument:put-b': { 'instrument.dte': field('40', 'day'), 'instrument.best_bid': field('50', 'USDC/contract') },
    },
    instruments: {
      'instrument:put-a': { kind: 'put', base_asset: 'ETH', quote_asset: 'USDC', expiry: '2030-01-30T12:00:00.000Z', contract_size: '1' },
      'instrument:put-b': { kind: 'put', base_asset: 'ETH', quote_asset: 'USDC', expiry: '2030-02-10T12:00:00.000Z', contract_size: '1' },
    },
    private_state: { version: 0, content_ref: 'state:fixture/0' }, control_revision: 0,
    active_intents: [], extensions: {},
  }, 'input_bundle_id');
  const intent = {
    intent_id: 'roll-put-a', intent_revision: 1, action: 'sell_put', purpose: 'roll_protection',
    authority_ref: 'mandate:fixture-mandate/1', instrument_ref: 'instrument:put-a', inventory_ref: 'inventory:put-a',
    active_from: NOW, expires_at: '2030-01-01T12:05:00.000Z', evaluation_interval_ms: 1000,
    max_quote_age_ms: 2000, max_attempts: 1, priority: 10, reservation_policy: 'reserve_on_trigger',
    quantity: { max_total: '1', unit: 'contract' },
    order: { side: 'sell', time_in_force: 'ioc', reduce_only: true,
      limit_price: { value: '100', unit: 'USDC/contract' }, max_total_fees: { value: '0.1', unit: 'USDC' } },
    requires_reconciled_account: true, dependencies: [],
    when: { op: 'lte', left: { ref: 'instrument.dte' }, right: { literal: rollDte, unit: 'day' } },
    reason: 'Test-only reference for offline control semantics',
  };
  const decision = {
    contract_version: 'noop.strategy/v1', strategy_release_id: release.strategy_release_id,
    release_digest: release.release_digest, mandate_id: mandate.mandate_id, mandate_revision: 1,
    input_bundle_id: inputBundle.input_bundle_id, decision_id: 'fixture-decision-1', expected_control_revision: 0,
    private_state: { expected_version: 0, proposed_version: 1, content_ref: 'state:fixture/1' },
    operations: [{ op: 'upsert_intent', expected_intent_revision: null, intent }],
  };
  const validateEconomicPolicy = (candidate, input, { mandate: accepted }) => ({
    valid: candidate.when.right?.literal === accepted.parameters.put_roll_dte,
    reasons: candidate.when.right?.literal === accepted.parameters.put_roll_dte ? [] : ['Unexpected test policy threshold'],
  });
  return { release, mandate, inputBundle, decision, validateEconomicPolicy };
}

function inputForState(input, state, edits = {}) {
  return withDigest({ ...clone(input), sequence: input.sequence + 1,
    control_revision: state.control_revision,
    private_state: { version: state.private_state_version, content_ref: state.private_state_ref || input.private_state.content_ref },
    active_intents: Object.values(state.intents).map(record => ({
      intent_id: record.intent.intent_id, intent_revision: record.intent.intent_revision, status: record.status,
    })), ...edits,
  }, 'input_bundle_id');
}

module.exports = { fixture, field, clone, withDigest, inputForState, NOW };
