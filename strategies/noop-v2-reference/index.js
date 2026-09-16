'use strict';

const { contentDigest, canonicalize } = require('../../strategy/canonical');
const { compareDecimals, addDecimals, subtractDecimals, multiplyDecimals, normalizeDecimal, parseDecimal } = require('../../strategy/decimal');
const { DEFAULT_PARAMETERS, PARAMETER_DEFINITIONS, materializeParameters } = require('./config');
const { createLegacyPolicy } = require('./legacy');
const { REFERENCE_FIELD_CATALOG } = require('./fields');
const provenance = require('./provenance.json');
const artifact = require('./artifact.json');

const ACTIONS = Object.freeze(['buy_put', 'sell_put', 'sell_call', 'buyback_call']);
const EXIT_ACTIONS = new Set(['sell_put', 'buyback_call']);

function freeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    Object.values(value).forEach(freeze);
    Object.freeze(value);
  }
  return value;
}

function legacyDecimal(value, label = 'legacy numeric value') {
  // This conversion explicitly preserves the already-parsed V2 research value.
  // New financial bindings never pass through it and must be decimal strings.
  if (typeof value !== 'number' && typeof value !== 'string') throw new TypeError(`Invalid ${label}`);
  return normalizeDecimal(String(value));
}

function compare(ref, op, value) {
  const definition = REFERENCE_FIELD_CATALOG[ref];
  if (!definition) throw new TypeError(`Unregistered reference field: ${ref}`);
  return { op, left: { ref }, right: {
    literal: definition.unit === 'boolean' ? value : legacyDecimal(value), unit: definition.unit,
  } };
}

function readField(bundle, intent, ref, evaluationTime = bundle.evaluated_at) {
  const definition = REFERENCE_FIELD_CATALOG[ref];
  if (!definition) throw new TypeError(`Unregistered reference field: ${ref}`);
  const field = definition.scope === 'account' ? bundle.fields?.[ref]
    : bundle.instrument_fields?.[intent.instrument_ref]?.[ref];
  if (!field || field.quality !== 'valid' || field.unit !== definition.unit) throw new Error(`Missing or invalid reference evidence: ${ref}`);
  const now = Date.parse(evaluationTime);
  const observed = Date.parse(field.observed_at);
  const available = Date.parse(field.available_at);
  const maxAge = definition.quote ? Math.min(definition.max_age_ms, intent.max_quote_age_ms) : definition.max_age_ms;
  if (![now, observed, available].every(Number.isFinite) || observed > now || available > now || now - observed > maxAge) {
    throw new Error(`Stale or future reference evidence: ${ref}`);
  }
  if (definition.unit === 'boolean') {
    if (typeof field.value !== 'boolean') throw new TypeError(`Invalid boolean evidence: ${ref}`);
  } else normalizeDecimal(field.value);
  return field.value;
}

function positive(value, label) {
  if (compareDecimals(value, '0') <= 0) throw new RangeError(`${label} must be positive`);
  return value;
}

function boundedLegacyRule(rule, legacy) {
  const ruleKeys = new Set(['id', 'rule_type', 'action', 'instrument_name', 'criteria', 'budget_limit', 'priority', 'reasoning', 'advisory_id', 'preferred_order_type']);
  if (!rule || typeof rule !== 'object' || Array.isArray(rule)) throw new TypeError('Recorded rule must be an object');
  for (const key of Object.keys(rule)) {
    if (!ruleKeys.has(key)) throw new Error(`Legacy rule field needs an explicit adapter before replay: ${key}`);
  }
  const validation = legacy.validateAdvisorRuleContract(rule);
  if (!validation.valid) throw new Error(`Reference rule rejected: ${validation.reason}`);
  const criteria = legacy.parseMaybeJsonObject(rule.criteria);
  const knownKeys = new Set({
    buy_put: ['option_type', 'delta_range', 'dte_range', 'min_score', 'target_score'],
    sell_call: ['option_type', 'delta_range', 'dte_range', 'min_score', 'min_bid'],
    sell_put: ['put_exit_intent', 'conditions', 'condition_logic', 'requires_longer_dated_protection',
      'min_exit_price', 'limit_price', 'target_exit_price', 'tranche_fraction', 'max_tranche_fraction', 'retain_downside_protection'],
    buyback_call: ['buyback_intent', 'conditions', 'condition_logic', 'target_capture_pct', 'capture_floor_pct', 'max_buyback_price', 'limit_price'],
  }[rule.action]);
  for (const key of Object.keys(criteria)) {
    if (!knownKeys.has(key)) throw new Error(`Legacy criterion needs an explicit adapter before replay: ${key}`);
  }
  if (criteria.buyback_intent === 'threat_management') throw new Error('Threat-management interpretation requires a separately accepted evidence adapter');
  if (EXIT_ACTIONS.has(rule.action) && (typeof rule.instrument_name !== 'string' || !rule.instrument_name)) throw new Error('Captured exit must identify its owned instrument');
  if (rule.budget_limit != null && rule.action !== 'buy_put') throw new Error('Legacy budget_limit outside buy_put needs an explicit adapter');
  if (criteria.put_exit_intent === 'roll_protection' && (criteria.tranche_fraction != null || criteria.max_tranche_fraction != null)) {
    throw new Error('A roll with an additional tranche constraint needs an explicit adapter');
  }
  if (criteria.put_exit_intent === 'monetize_tail_win' && criteria.requires_longer_dated_protection != null) {
    throw new Error('Monetization with an additional replacement-protection constraint needs an explicit adapter');
  }
  for (const condition of criteria.conditions || []) {
    if (!['dte', 'delta', 'unrealized_pnl_pct'].includes(condition.field) || !['lt', 'lte', 'gt', 'gte'].includes(condition.op)) {
      throw new Error(`Unsupported legacy condition: ${condition.field}/${condition.op}`);
    }
    if (Object.keys(condition).some(key => !['field', 'op', 'value', 'threshold'].includes(key))) throw new Error('Unsupported legacy condition property');
    legacyDecimal(condition.value ?? condition.threshold);
  }
  // Source validation allows an any branch to coexist with a policy condition.
  // The bounded adapter refuses that ambiguity, while source helpers preserve it
  // for honest historical comparison.
  if (criteria.conditions && (criteria.condition_logic || 'all') !== 'all') throw new Error('Reference adapter requires all legacy exit conditions');
  for (const aliases of [['min_exit_price', 'limit_price', 'target_exit_price'], ['max_buyback_price', 'limit_price'], ['tranche_fraction', 'max_tranche_fraction'], ['target_capture_pct', 'capture_floor_pct']]) {
    const present = aliases.filter(name => criteria[name] != null);
    if (present.length > 1 && present.some(name => compareDecimals(legacyDecimal(criteria[name]), legacyDecimal(criteria[present[0]])) !== 0)) {
      throw new Error(`Conflicting legacy economic aliases require explicit resolution: ${present.join(', ')}`);
    }
  }
  return criteria;
}

function marginProjectionBinding(intent, bundle) {
  const candidate = { instrument_ref: intent.instrument_ref, quantity: intent.quantity, order: intent.order };
  const context = { account: bundle.account, fields: bundle.fields, instrument_fields: bundle.instrument_fields,
    instruments: bundle.instruments, control_revision: bundle.control_revision, active_intents: bundle.active_intents,
    evaluated_at: bundle.evaluated_at };
  return { candidate_digest: contentDigest(candidate), context_digest: contentDigest(context) };
}

function conditionsFor(rule, criteria, policy) {
  const clauses = [];
  for (const [key, ref] of [['delta_range', 'instrument.delta'], ['dte_range', 'instrument.dte']]) {
    if (criteria[key]) {
      clauses.push(compare(ref, 'gte', criteria[key][0]), compare(ref, 'lte', criteria[key][1]));
    }
  }
  for (const condition of criteria.conditions || []) {
    const ref = condition.field === 'dte' ? 'instrument.dte'
      : condition.field === 'delta' ? 'instrument.delta'
        : rule.action === 'buyback_call' ? 'noop_v2.call_profit_capture_pct' : 'noop_v2.put_executable_pnl_pct';
    clauses.push(compare(ref, condition.op, condition.value ?? condition.threshold));
  }
  if (rule.action === 'sell_call') clauses.push(compare('instrument.best_bid', 'gte', criteria.min_bid));
  if (criteria.put_exit_intent === 'roll_protection') {
    clauses.push(compare('instrument.dte', 'lte', policy.put_roll_dte));
    clauses.push(compare('portfolio.has_longer_dated_put', 'eq', true));
  }
  if (criteria.put_exit_intent === 'monetize_tail_win') clauses.push(compare('noop_v2.put_executable_pnl_pct', 'gt', policy.put_monetization_profit_pct));
  if (criteria.buyback_intent === 'profit_capture') clauses.push(compare('noop_v2.call_profit_capture_pct', 'gte', policy.call_profit_capture_pct));
  if (!clauses.length) throw new Error('Reference adapter produced no bounded condition');
  return { op: 'all', args: clauses };
}

function createReferenceStrategy({ parameters = {}, strategyReleaseId = 'noop-v2-reference/1.0.0' } = {}) {
  const defaults = materializeParameters(parameters);
  const releaseWithoutDigest = {
    contract_version: 'noop.strategy/v1', strategy_id: 'noop-v2-reference', strategy_release_id: strategyReleaseId,
    artifact: { digest: contentDigest(artifact.files), entrypoint: 'generateDecision', dependency_lock_digest: contentDigest(artifact.dependencies) },
    parameters: Object.fromEntries(Object.entries(PARAMETER_DEFINITIONS).map(([key, definition]) => [key, { ...definition, default: defaults[key] }])),
    capabilities: { actions: [...ACTIONS], instrument_kinds: ['put', 'call'], venues: ['derive-v3'], spot_routes: [] },
    runtime: { max_duration_ms: 1000, max_memory_mb: 128, max_output_bytes: 65536 },
    extensions: { mode: 'offline_reference', provenance_digest: contentDigest(provenance), field_catalog_digest: contentDigest(REFERENCE_FIELD_CATALOG),
      legacy_budget_basis: 'Pinned insured base value, including explicitly declared external ETH; external ETH never supplies spendable capital.',
      legacy_budget_accrual: 'Discrete period windows; unused authorization carries forward. Reconciled put sales reduce legacy net put cost; this does not establish cash availability.',
      implementation_scope: 'Captured research rule boundary and extracted pure helpers; no model calls, full-history orchestration, synthetic resting exits, or live authorization.' },
  };
  const release = freeze({ ...releaseWithoutDigest, release_digest: contentDigest(releaseWithoutDigest) });

  function policyFor(mandate) {
    if (!mandate) return defaults;
    if (mandate.strategy_release_id !== release.strategy_release_id || mandate.release_digest !== release.release_digest) throw new Error('Mandate selects a different reference release');
    return materializeParameters(mandate.parameters, { requireComplete: true });
  }

  function legacyFor(mandate, researchState) { return createLegacyPolicy(policyFor(mandate), researchState); }

  function reportPolicy({ mandate } = {}) {
    const p = policyFor(mandate);
    return freeze({ strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
      parameters: p, parameters_digest: contentDigest(p),
      summary: `Roll puts at <= ${p.put_roll_dte} DTE with replacement protection already owned; capture calls at >= ${p.call_profit_capture_pct}%; normal call entry target ${multiplyDecimals(p.call_entry_margin_ratio, '100')}%, breakout target ${multiplyDecimals(p.call_breakout_margin_ratio, '100')}%, execution buffer ${multiplyDecimals(p.call_execution_buffer_ratio, '100')} percentage points.`,
      premium_budget: `${multiplyDecimals(p.put_annual_rate, '100')}% annual insured-base authorization in ${p.put_budget_period_days}-day windows; legacy net put cost and unused carry govern remaining authorization.`,
      limitation: 'These are reference Strategy economics; the offline result does not certify solvency, available cash, venue manager parity, or live execution.' });
  }

  function buildPrompts({ mandate } = {}) {
    const legacy = legacyFor(mandate);
    const report = reportPolicy({ mandate });
    const common = [legacy.getCallMarginDisciplinePrompt(), legacy.getCallBuybackDisciplinePrompt(), legacy.getPutExitDisciplinePrompt()].join('\n\n');
    return freeze({
      generation: [legacy.getStandingRulebookDisciplinePrompt(), report.premium_budget, common].join('\n\n'),
      confirmation: [legacy.getConfirmationScopePrompt(), common].join('\n\n'),
      reporting: [report.summary, report.premium_budget, report.limitation].join('\n'),
    });
  }

  function buildFallbackRules(context = {}, { mandate } = {}) {
    const legacy = legacyFor(mandate, context.researchState);
    const requirements = legacy.buildRulebookRequirements(context);
    return requirements.map(requirement => legacy.buildCanonicalRequiredWatcherRule(requirement, context)).filter(Boolean);
  }

  function calculatePremiumAuthorization({ insured_base_value, carry_forward = '0', net_put_cost = '0', reserved = '0' }, { mandate } = {}) {
    const p = policyFor(mandate);
    positive(insured_base_value, 'Pinned insured base value');
    for (const [name, value] of Object.entries({ carry_forward, reserved })) {
      if (compareDecimals(value, '0') < 0) throw new RangeError(`${name} must be nonnegative`);
    }
    // The V2 formula is base * annual_rate / (365 / period_days).
    // Return a conservative 30-place floor of the exact rational. This explicit
    // rounding differs from V2 floating-point bookkeeping and never expands it
    // through an undocumented binary tolerance.
    const numerator = multiplyDecimals(multiplyDecimals(insured_base_value, p.put_annual_rate), p.put_budget_period_days);
    const parsed = parseDecimal(numerator);
    const scaled = (parsed.coefficient * 10n ** BigInt(30 - parsed.scale)) / 365n;
    const digits = scaled.toString().padStart(31, '0');
    const cycle_budget = normalizeDecimal(`${digits.slice(0, -30)}.${digits.slice(-30)}`);
    const available = subtractDecimals(subtractDecimals(addDecimals(cycle_budget, carry_forward), net_put_cost), reserved);
    return freeze({ cycle_budget, carry_forward, net_put_cost, reserved,
      available_premium_authorization: compareDecimals(available, '0') > 0 ? available : '0',
      currency: 'USDC', basis_value: insured_base_value, parameters_digest: contentDigest(p),
      rounding: 'floor_30_decimal_places',
      scope: 'One explicitly supplied cycle; does not advance a clock, persist a ledger, certify spendable cash, or reuse a stale valuation.' });
  }

  function adaptRecordedRules(rules, bindings, inputBundle, { mandate } = {}) {
    const p = policyFor(mandate);
    const legacy = legacyFor(mandate);
    if (!Array.isArray(rules) || rules.length > 128) throw new TypeError('Recorded rules must be a bounded array');
    return rules.map((rule, index) => {
      const criteria = boundedLegacyRule(rule, legacy);
      const binding = bindings?.[String(rule.id ?? index)];
      if (!binding) throw new Error(`Missing finite replay binding for legacy rule ${rule.id ?? index}`);
      // The host supplies concrete instrument/resources, finite quantity/price,
      // lifetime and revision. Strategy generation supplies economic semantics.
      canonicalize(binding);
      const intent = structuredClone(binding);
      if (rule.instrument_name && intent.instrument_ref !== rule.instrument_name) {
        throw new Error('Recorded rule instrument does not match the bound instrument metadata');
      }
      const requestedType = rule.preferred_order_type || intent.order?.time_in_force;
      if (!['ioc', 'gtc'].includes(requestedType) || requestedType !== intent.order?.time_in_force) throw new Error('Legacy order mode requires an explicit supported adapter; no implicit post_only conversion');
      if (EXIT_ACTIONS.has(rule.action) && requestedType !== 'ioc') throw new Error('Synthetic resting exit parity is not established; reference adapter supports native reduce-only IOC exits');
      intent.action = rule.action;
      intent.purpose = criteria.put_exit_intent || criteria.buyback_intent || (rule.action === 'buy_put' ? 'acquire_protection' : 'earn_call_premium');
      intent.order.side = ['buy_put', 'buyback_call'].includes(rule.action) ? 'buy' : 'sell';
      intent.order.reduce_only = EXIT_ACTIONS.has(rule.action);
      intent.when = conditionsFor(rule, criteria, p);
      intent.requires_reconciled_account = true;
      intent.reason = String(rule.reasoning || `Offline replay of V2 ${rule.action} rule`).slice(0, 4096);
      intent.evidence = { legacy_rule_json: JSON.stringify(rule), parameters_digest: contentDigest(p), source_mode: 'captured_v2_research' };
      return { op: 'upsert_intent', expected_intent_revision: intent.intent_revision === 1 ? null : intent.intent_revision - 1, intent };
    });
  }

  function generateDecision(inputBundle, { mandate } = {}) {
    if (!mandate) throw new Error('A materialized customer mandate is required to generate executable conditions');
    const { validateInputBundle, validateDecision } = require('../../strategy/contract');
    validateInputBundle(inputBundle, { release, mandate, catalog: REFERENCE_FIELD_CATALOG });
    const input = inputBundle.extensions?.reference_v2;
    let operations;
    if (!input) operations = [{ op: 'no_action', reason: 'No captured reference research supplied; existing intents are unchanged.' }];
    else {
      if (Object.keys(input).some(key => !['recorded_rules_json', 'bindings', 'cancel_intents', 'margin_projections'].includes(key))) throw new Error('Unknown reference replay input');
      const rules = JSON.parse(input.recorded_rules_json || '[]');
      operations = adaptRecordedRules(rules, input.bindings || {}, inputBundle, { mandate });
      for (const cancellation of input.cancel_intents || []) {
        operations.push({ op: 'cancel_intent', intent_id: cancellation.intent_id, expected_intent_revision: cancellation.expected_intent_revision,
          reason: cancellation.reason || 'Explicit recorded-rule cancellation' });
      }
      if (!operations.length) operations = [{ op: 'no_action', reason: 'Captured reference research contains no explicit intent changes.' }];
    }
    const decision = {
      contract_version: 'noop.strategy/v1', strategy_release_id: release.strategy_release_id, release_digest: release.release_digest,
      mandate_id: mandate.mandate_id, mandate_revision: mandate.mandate_revision, input_bundle_id: inputBundle.input_bundle_id,
      decision_id: `reference-decision:${contentDigest({ input_bundle_id: inputBundle.input_bundle_id, operations }).slice(7)}`,
      expected_control_revision: inputBundle.control_revision, operations,
    };
    validateDecision(decision, { release, mandate, inputBundle, catalog: REFERENCE_FIELD_CATALOG });
    for (const operation of operations) {
      if (operation.op !== 'upsert_intent') continue;
      const validation = validateEconomicPolicy(operation.intent, inputBundle, { mandate, phase: 'admission' });
      if (!validation.valid) throw new Error(`Reference proposal violates its economic policy: ${validation.reasons.join('; ')}`);
    }
    return freeze(decision);
  }

  function validateEconomicPolicy(intent, inputBundle, { mandate, phase = 'schedule', now = inputBundle.evaluated_at } = {}) {
    const reasons = [];
    try {
      if (!mandate) throw new Error('Materialized mandate is required for reference economic validation');
      const p = policyFor(mandate);
      const legacy = legacyFor(mandate);
      if (!['admission', 'schedule'].includes(phase)) throw new Error('Unknown reference economic-validation phase');
      if (!ACTIONS.includes(intent.action)) throw new Error('Action outside this reference release');
      if (intent.evidence?.parameters_digest !== contentDigest(p)) throw new Error('Intent policy evidence does not match accepted materialized parameters');
      const rule = JSON.parse(intent.evidence.legacy_rule_json);
      const criteria = boundedLegacyRule(rule, legacy);
      if (rule.action !== intent.action) throw new Error('Intent action differs from recorded rule');
      if (rule.instrument_name && rule.instrument_name !== intent.instrument_ref) throw new Error('Intent instrument differs from recorded rule');
      if (!inputBundle.instruments?.[intent.instrument_ref]) throw new Error('Bound instrument metadata is missing');
      if (inputBundle.instruments[intent.instrument_ref].kind !== (intent.action.includes('put') ? 'put' : 'call')) throw new Error('Bound instrument kind differs from the reference action');
      if (intent.order.side !== (['buy_put', 'buyback_call'].includes(intent.action) ? 'buy' : 'sell')) throw new Error('Intent order side differs from recorded rule');
      if (rule.preferred_order_type && rule.preferred_order_type !== intent.order.time_in_force) throw new Error('Intent order mode differs from recorded rule');
      const expectedPurpose = criteria.put_exit_intent || criteria.buyback_intent || (rule.action === 'buy_put' ? 'acquire_protection' : 'earn_call_premium');
      if (intent.purpose !== expectedPurpose) throw new Error('Intent purpose differs from recorded rule');
      if (canonicalize(intent.when) !== canonicalize(conditionsFor(rule, criteria, p))) throw new Error('Intent conditions differ from the recorded rule and accepted policy');
      if (intent.order.reduce_only !== EXIT_ACTIONS.has(intent.action)) throw new Error('Reference close-only semantics violated');
      if (EXIT_ACTIONS.has(intent.action) && intent.order.time_in_force !== 'ioc') throw new Error('Reference synthetic resting exits are unsupported');
      const quantity = positive(intent.quantity.max_total, 'Maximum quantity');
      const limit = positive(intent.order.limit_price.value, 'Limit price');
      if (intent.quantity.unit !== 'contract' || intent.order.limit_price.unit !== 'USDC/contract') throw new Error('Reference option economics require normalized contract units');
      if (criteria.min_exit_price != null || criteria.target_exit_price != null || (intent.action === 'sell_put' && criteria.limit_price != null)) {
        if (compareDecimals(limit, legacyDecimal(criteria.min_exit_price ?? criteria.limit_price ?? criteria.target_exit_price)) < 0) throw new Error('Intent limit weakens the recorded put exit floor');
      }
      if (intent.action === 'buyback_call' && (criteria.max_buyback_price != null || criteria.limit_price != null)) {
        if (compareDecimals(limit, legacyDecimal(criteria.max_buyback_price ?? criteria.limit_price)) > 0) throw new Error('Intent limit weakens the recorded call buyback ceiling');
      }
      // Standing watchers can be admitted before their market conditions become
      // true. Admission does not reserve funds or authorize an execution.
      if (phase === 'admission') return { valid: true, reasons: [] };
      const read = ref => readField(inputBundle, intent, ref, now);
      const requireAtMost = (actual, maximum, label) => { if (compareDecimals(actual, maximum) > 0) throw new Error(`${label} exceeds selected reference policy`); };
      const requireAtLeast = (actual, minimum, label) => { if (compareDecimals(actual, minimum) < 0) throw new Error(`${label} is below selected reference policy`); };
      const dte = read('instrument.dte');
      if (!EXIT_ACTIONS.has(intent.action)) {
        const side = intent.action === 'buy_put' ? 'put' : 'call';
        const delta = read('instrument.delta');
        requireAtLeast(dte, p[`${side}_min_dte`], 'DTE');
        requireAtMost(dte, p[`${side}_max_dte`], 'DTE');
        requireAtLeast(delta, p[`${side}_min_delta`], 'Delta');
        requireAtMost(delta, p[`${side}_max_delta`], 'Delta');
        // Only the versioned research score uses V2 Number/Pow arithmetic.
        // Monetary outlay/price/quantity comparisons below stay exact.
        const score = intent.action === 'buy_put'
          ? legacy.normalizeBuyPutScore(Math.abs(Number(delta)) / Number(limit), Number(dte))
          : legacy.normalizeSellCallScore(Number(limit) / Math.abs(Number(delta)), Number(dte));
        if (!Number.isFinite(score)) throw new Error('Nonfinite reference research score');
        const scoreText = score.toFixed(18);
        const minimumScore = legacyDecimal(criteria.min_score);
        requireAtLeast(scoreText, minimumScore, 'Reference edge at the proposed limit');
        if (criteria.target_score != null) requireAtLeast(scoreText, legacyDecimal(criteria.target_score), 'Target edge at the proposed limit');
      }
      if (intent.action === 'buy_put') {
        const maxOutlay = positive(intent.order.max_total_outlay?.value, 'Maximum premium outlay');
        requireAtMost(maxOutlay, read('noop_v2.put_budget_available'), 'Premium outlay');
        if (rule.budget_limit != null) requireAtMost(maxOutlay, legacyDecimal(rule.budget_limit), 'Recorded rule budget');
      }
      if (intent.action === 'sell_call') {
        requireAtLeast(limit, legacyDecimal(criteria.min_bid), 'Call premium limit');
        const projection = inputBundle.extensions?.reference_v2?.margin_projections?.[intent.intent_id];
        if (!projection || canonicalize(projection) !== canonicalize(marginProjectionBinding(intent, inputBundle))) {
          throw new Error('Call margin projection is not pinned to this exact candidate and account context');
        }
        const breakout = read('noop_v2.breakout_override_active');
        requireAtMost(read('noop_v2.projected_displayed_margin_ratio'), breakout ? p.call_breakout_margin_ratio : p.call_entry_margin_ratio, 'Projected displayed margin');
      }
      if (EXIT_ACTIONS.has(intent.action)) {
        const remaining = positive(read('noop_v2.position_quantity'), 'Closeable position');
        requireAtMost(quantity, remaining, 'Close quantity');
        if (intent.action === 'buyback_call') {
          const entry = positive(read('noop_v2.position_avg_entry_price'), 'Call premium basis');
          const target = criteria.target_capture_pct ?? criteria.capture_floor_pct ?? p.call_profit_capture_pct;
          const threshold = compareDecimals(legacyDecimal(target), p.call_profit_capture_pct) > 0 ? legacyDecimal(target) : p.call_profit_capture_pct;
          const maxPrice = multiplyDecimals(entry, subtractDecimals('1', multiplyDecimals(threshold, '0.01')));
          requireAtMost(limit, maxPrice, 'Call buyback limit');
          if (criteria.max_buyback_price != null || criteria.limit_price != null) requireAtMost(limit, legacyDecimal(criteria.max_buyback_price ?? criteria.limit_price), 'Recorded call buyback ceiling');
          requireAtLeast(read('noop_v2.call_profit_capture_pct'), p.call_profit_capture_pct, 'Executable call capture');
        } else if (intent.purpose === 'roll_protection') {
          requireAtMost(dte, p.put_roll_dte, 'Put roll DTE');
          if (read('portfolio.has_longer_dated_put') !== true) throw new Error('Replacement put protection is not already owned');
        } else {
          if (compareDecimals(read('noop_v2.put_executable_pnl_pct'), p.put_monetization_profit_pct) <= 0) throw new Error('Tail monetization needs strictly greater executable profit');
          const requestedTranche = legacyDecimal(criteria.tranche_fraction ?? criteria.max_tranche_fraction ?? p.put_monetization_tranche_ratio);
          requireAtMost(quantity, multiplyDecimals(remaining, requestedTranche), 'Put monetization tranche');
          if (compareDecimals(quantity, remaining) >= 0) throw new Error('Put monetization must retain downside protection');
          requireAtLeast(limit, legacyDecimal(criteria.min_exit_price ?? criteria.limit_price ?? criteria.target_exit_price), 'Put monetization exit floor');
        }
      }
    } catch (error) { reasons.push(error.message); }
    return { valid: reasons.length === 0, reasons };
  }

  return freeze({ release, defaults, fieldCatalog: REFERENCE_FIELD_CATALOG, policyFor, legacyFor, reportPolicy, buildPrompts,
    buildFallbackRules, calculatePremiumAuthorization, adaptRecordedRules, generateDecision, validateEconomicPolicy });
}

function createExampleStrategy() {
  return createReferenceStrategy({ strategyReleaseId: 'noop-v2-reference/example-35-70-30', parameters: {
    put_roll_dte: '35', call_profit_capture_pct: '70', call_entry_margin_ratio: '0.30',
  } });
}

module.exports = { createReferenceStrategy, createExampleStrategy, createLegacyPolicy, DEFAULT_PARAMETERS,
  PARAMETER_DEFINITIONS, materializeParameters, REFERENCE_FIELD_CATALOG, marginProjectionBinding };
