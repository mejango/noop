'use strict';

const { contentDigest } = require('../../strategy/canonical');
const { REFERENCE_FIELD_CATALOG } = require('./fields');

// Public fictitious identities and offline values. This fixture never references
// the running account, testnet wallet, filesystem, process environment, or keys.
function createReplayFixture({ strategy = require('./index').createReferenceStrategy(), action = 'buyback_call',
  evaluatedAt = '2030-01-01T00:00:00.000Z', fieldOverrides = {}, mandateId = 'reference-fixture' } = {}) {
  const p = strategy.defaults;
  const isPut = ['buy_put', 'sell_put'].includes(action);
  const dte = fieldOverrides['instrument.dte'] ?? (action === 'buy_put' ? '60' : action === 'sell_call' ? '8.5' : '30');
  const expiry = new Date(Date.parse(evaluatedAt) + Number(dte) * 86400000).toISOString();
  const expiryDate = expiry.slice(0, 10).replaceAll('-', '');
  const instrumentRef = isPut ? `ETH-${expiryDate}-2000-P` : `ETH-${expiryDate}-4000-C`;
  const account = { network: 'testnet', chain_id: 901, venue: 'derive-v3', deployment: 'offline-fixture',
    owner: '0x0000000000000000000000000000000000000001', subaccount_id: '1', manager_id: '1', risk_universe: 'ETH' };
  const mandate = {
    contract_version: 'noop.strategy/v1', mandate_id: mandateId, mandate_revision: 1,
    strategy_release_id: strategy.release.strategy_release_id, release_digest: strategy.release.release_digest,
    account, protected_eth: '20', parameters: { ...p }, status: 'active',
    authority: { actions: ['buy_put', 'sell_put', 'sell_call', 'buyback_call'], instrument_refs: [instrumentRef], spot_routes: [],
      budget_refs: ['budget:fixture'], inventory_refs: ['inventory:fixture'], liability_refs: ['liability:fixture'] }, extensions: {},
  };
  const rules = {
    buyback_call: { id: 'capture', rule_type: 'exit', action, instrument_name: instrumentRef, preferred_order_type: 'ioc',
      criteria: { buyback_intent: 'profit_capture', conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: p.call_profit_capture_pct }], condition_logic: 'all' } },
    sell_put: { id: 'roll', rule_type: 'exit', action, instrument_name: instrumentRef, preferred_order_type: 'ioc',
      criteria: { put_exit_intent: 'roll_protection', conditions: [{ field: 'dte', op: 'lte', value: p.put_roll_dte }], condition_logic: 'all', requires_longer_dated_protection: true } },
    sell_call: { id: 'call', rule_type: 'entry', action, preferred_order_type: 'ioc',
      criteria: { option_type: 'C', delta_range: [p.call_min_delta, p.call_max_delta], dte_range: [p.call_min_dte, p.call_max_dte], min_bid: p.call_fallback_min_bid, min_score: p.call_fallback_min_score } },
    buy_put: { id: 'put', rule_type: 'entry', action, preferred_order_type: 'ioc', budget_limit: '10.1',
      criteria: { option_type: 'P', delta_range: [p.put_min_delta, p.put_max_delta], dte_range: [p.put_min_dte, p.put_max_dte], min_score: '0.003' } },
  };
  const rule = rules[action];
  if (!rule) throw new Error('Unsupported reference fixture action');
  const closing = ['sell_put', 'buyback_call'].includes(action);
  const buying = ['buy_put', 'buyback_call'].includes(action);
  const limit = action === 'buyback_call' ? '2' : action === 'buy_put' ? '10' : '8';
  const binding = {
    intent_id: `intent:${rule.id}`, intent_revision: 1, authority_ref: `mandate:${mandateId}/1`, instrument_ref: instrumentRef,
    active_from: evaluatedAt, expires_at: '2030-01-02T00:00:00.000Z', evaluation_interval_ms: 1000,
    max_quote_age_ms: 2000, max_attempts: 3, priority: 10, reservation_policy: 'reserve_on_trigger',
    quantity: { max_total: '0.1', unit: 'contract' },
    order: { side: buying ? 'buy' : 'sell', time_in_force: 'ioc', reduce_only: closing,
      limit_price: { value: limit, unit: 'USDC/contract' }, max_total_fees: { value: '0.01', unit: 'USDC' },
      ...(buying ? { max_total_outlay: { value: action === 'buyback_call' ? '0.21' : '1.01', unit: 'USDC' } } : {}),
      ...(action === 'sell_call' ? { max_total_liability: { value: '1000', unit: 'USDC' } } : {}) },
    dependencies: [],
    ...(buying ? { budget_ref: 'budget:fixture' } : {}),
    ...(closing ? { inventory_ref: 'inventory:fixture' } : {}),
    ...(action === 'sell_call' ? { liability_ref: 'liability:fixture' } : {}),
  };
  const values = {
    'account.reconciled': true,
    'instrument.dte': dte,
    'instrument.delta': isPut ? '-0.04' : '0.08',
    'instrument.best_ask': action === 'buyback_call' ? '2.5' : '10',
    'instrument.best_bid': '8',
    'portfolio.has_longer_dated_put': true,
    'noop_v2.call_profit_capture_pct': '75',
    'noop_v2.put_executable_pnl_pct': '1200',
    'noop_v2.projected_displayed_margin_ratio': '0.35',
    'noop_v2.breakout_override_active': false,
    'noop_v2.position_quantity': '1',
    'noop_v2.position_avg_entry_price': '10',
    'noop_v2.put_budget_available': '10',
    ...fieldOverrides,
  };
  const fields = {}, instrumentFields = {};
  for (const [name, value] of Object.entries(values)) {
    const definition = REFERENCE_FIELD_CATALOG[name];
    if (!definition) throw new Error(`Unknown reference fixture field: ${name}`);
    (definition.scope === 'account' ? fields : instrumentFields)[name] = {
      value, unit: definition.unit, observed_at: evaluatedAt, available_at: evaluatedAt, quality: 'valid',
    };
  }
  const bundle = {
    contract_version: 'noop.strategy/v1', strategy_release_id: strategy.release.strategy_release_id, release_digest: strategy.release.release_digest,
    mandate_id: mandateId, mandate_revision: 1, sequence: 1, evaluated_at: evaluatedAt, invocation_reason: 'replay', account,
    fields, instrument_fields: { [instrumentRef]: instrumentFields },
    instruments: { [instrumentRef]: { kind: isPut ? 'put' : 'call', base_asset: 'ETH', quote_asset: 'USDC',
      expiry, contract_size: '1' } },
    private_state: { version: 0, content_ref: `state:${mandateId}/0` }, control_revision: 0, active_intents: [],
    extensions: { reference_v2: { recorded_rules_json: JSON.stringify([rule]), bindings: { [rule.id]: binding } } },
  };
  if (action === 'sell_call') {
    bundle.extensions.reference_v2.margin_projections = {
      [binding.intent_id]: require('./index').marginProjectionBinding(binding, bundle),
    };
  }
  bundle.input_bundle_id = contentDigest(bundle);
  return { strategy, release: strategy.release, mandate, inputBundle: bundle, rule, binding, instrumentRef };
}

module.exports = { createReplayFixture };
