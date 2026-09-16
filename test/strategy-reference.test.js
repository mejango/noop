'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createHash } = require('node:crypto');
const { spawnSync } = require('node:child_process');

const ROOT = path.resolve(__dirname, '..');
const SOURCE = fs.readFileSync(path.join(ROOT, 'script.js'), 'utf8');
const BOT_CONFIG = JSON.parse(fs.readFileSync(path.join(ROOT, 'bot/config.json'), 'utf8'));
const reference = require('../strategies/noop-v2-reference');
const { createReplayFixture } = require('../strategies/noop-v2-reference/fixtures');
const { contentDigest } = require('../strategy/canonical');

// Deliberately do not require script.js: it starts production work. These exact,
// reviewed, bounded declarations contain only constants and pure functions.
// Every parity expectation below executes the actual checked-out V2 source,
// rather than another hand-maintained copy of its implementation.
function section(startName, endName) {
  const start = SOURCE.indexOf(`\nconst ${startName} =`);
  const end = SOURCE.indexOf(`\nconst ${endName} =`, start + 1);
  assert.ok(start >= 0 && end > start, `review source boundary ${startName} -> ${endName}`);
  const source = SOURCE.slice(start, end);
  assert.doesNotMatch(source, /\b(?:require|fetch|setInterval|setTimeout)\s*\(|\b(?:process|fs|botDb|db)\./,
    `pure-source allowlist ${startName} must not acquire runtime capabilities`);
  return source;
}

const PURE_SECTIONS = [
  ['CALL_EXPOSURE_CAP_PCT', 'SUBACCOUNT_ID'],
  ['PUT_EXPIRATION_RANGE', 'JOURNAL_INTERVAL_MS'],
  ['roundForAdvisory', 'computeDteAt'],
  ['BUY_PUT_VALUE_SIGNALS', 'buildRollingOptionValueContext'],
  ['buildRulebookRequirements', 'parseCriteriaForSummary'],
  ['parseMaybeJsonObject', 'hasLongerDatedPutProtection'],
  ['hasLongerDatedPutProtectionSnapshot', 'refinePatientBuybackPlanPrice'],
  ['validateAdvisorRuleContract', 'getSellPutProtectionGate'],
  ['getMarginCapacityBase', 'estimateStandardShortCallInitialMarginPerUnit'],
  ['ACTION_POLICY', 'getMomentumEvidenceDisciplinePrompt'],
  ['getFreshBestBuyPutDisciplinePrompt', 'normalizeLearningText'],
  ['normalizePreferredOrderType', 'formatPostOnlyContext'],
];

const PARITY_NAMES = [
  'buildRulebookRequirements', 'formatRulebookRequirements', 'findMissingRulebookRequirements',
  'buildAgendaFromValidatedRules', 'buildCanonicalRequiredWatcherRule',
  'validateAdvisorRuleContract', 'normalizeBuybackCaptureFloor', 'getBuybackCaptureGate',
  'getSellPutExitAmount', 'getPatientSellPutPlan', 'getPatientBuybackPlan',
  'getBuyPutEntryPricing', 'normalizeBuyPutScore', 'getBuyPutPriceForEdgeScore',
  'getBuyPutDteNormalizationFactor', 'normalizeSellCallScore',
  'getCallExposureLimitPct', 'getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt',
  'getPutExitDisciplinePrompt', 'getStandingRulebookDisciplinePrompt',
  'getMarginCapacityBase', 'getMarginUtilizationBase', 'estimateMarginUtilizationFromComponents',
  'estimateMarginUtilization', 'estimateDisplayedMarginUtilization', 'estimateProjectedDisplayedMarginUtilization',
  'getDisplayedMarginHeadroomAtCap', 'getEffectiveCallExposureCapPct', 'getEffectiveCallExposureLimitPct',
];

function scoreSource(file) {
  const source = fs.readFileSync(path.join(ROOT, 'bot', file), 'utf8');
  const end = source.indexOf('\nmodule.exports =');
  assert.ok(end > 0, `${file} exports boundary`);
  assert.doesNotMatch(source.slice(0, end), /\brequire\s*\(|\b(?:process|fs|db)\./);
  return source.slice(0, end);
}

function actualV2(researchState = {}) {
  const context = vm.createContext({ BOT_CONFIG: structuredClone(BOT_CONFIG), botData: structuredClone(researchState) },
    { codeGeneration: { strings: false, wasm: false } });
  const code = [
    scoreSource('put-score.js'), scoreSource('call-score.js'),
    ...PURE_SECTIONS.map(([start, end]) => section(start, end)),
    `this.api = { ${PARITY_NAMES.join(', ')} };`,
    'this.constants = { PUT_ROLL_DTE_THRESHOLD, CALL_BUYBACK_PROFIT_THRESHOLD, CALL_EXPOSURE_CAP_PCT, CALL_BREAKOUT_OVERRIDE_CAP_PCT, CALL_EXPOSURE_BUFFER_PCT, CALL_ENTRY_BUFFER_PCT, PUT_MONETIZATION_PROFIT_THRESHOLD, PUT_MONETIZATION_MAX_TRANCHE_FRACTION, BUY_PUT_ADVISORY_DTE_RANGE, PUT_DELTA_RANGE, CALL_EXPIRATION_RANGE, CALL_DELTA_RANGE, SELL_CALL_FALLBACK_MIN_BID, SELL_CALL_FALLBACK_MIN_SCORE, BUY_PUT_EDGE_REFERENCE_DTE, BUY_PUT_EDGE_DTE_EXPONENT, SELL_CALL_EDGE_REFERENCE_DTE, SELL_CALL_EDGE_DTE_EXPONENT };',
  ].join('\n');
  vm.runInContext(code, context, { timeout: 1000, filename: 'allowlisted-v2-source-parity.js' });
  return context;
}

const v2 = actualV2();
const stable = (value) => JSON.parse(JSON.stringify(value));
const clone = (value) => structuredClone(value);
const v2Reference = reference.createReferenceStrategy();
const legacy = v2Reference.legacyFor();

function parity(name, args) {
  assert.equal(typeof legacy[name], 'function', `reference must export ${name}`);
  const sourceArgs = clone(args);
  const packageArgs = clone(args);
  const expected = v2.api[name](...sourceArgs);
  const actual = legacy[name](...packageArgs);
  assert.deepEqual(stable(actual), stable(expected), `${name} output for ${JSON.stringify(args)}`);
  assert.deepEqual(packageArgs, sourceArgs, `${name} mutation parity`);
  return actual;
}

const agingPut = { instrument: 'ETH-20300201-2000-P', direction: 'long', option_type: 'P', amount: 2, dte: 25, avg_entry_price: 10 };
const longerPut = { instrument: 'ETH-20300401-2000-P', direction: 'long', option_type: 'P', amount: 1, dte: 60, avg_entry_price: 10 };
const shortCall = { instrument: 'ETH-20300201-4000-C', direction: 'short', option_type: 'C', amount: 1, dte: 8, avg_entry_price: 20 };
const exitReq = (action, instrument_name) => ({ type: 'exit', action, instrument_name });
const captureRule = (threshold = 80) => ({
  rule_type: 'exit', action: 'buyback_call', instrument_name: shortCall.instrument,
  preferred_order_type: 'post_only',
  criteria: { buyback_intent: 'profit_capture', conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: threshold }], condition_logic: 'all', target_capture_pct: threshold },
});
const rollRule = (threshold = 25) => ({
  rule_type: 'exit', action: 'sell_put', instrument_name: agingPut.instrument,
  preferred_order_type: 'ioc',
  criteria: { put_exit_intent: 'roll_protection', conditions: [{ field: 'dte', op: 'lte', value: threshold }], requires_longer_dated_protection: true },
});

test('reference defaults match actual V2 config, policy constants, and score constants', () => {
  const defaults = reference.DEFAULT_PARAMETERS;
  const names = {
    put_roll_dte: 'PUT_ROLL_DTE_THRESHOLD', call_profit_capture_pct: 'CALL_BUYBACK_PROFIT_THRESHOLD',
    call_entry_margin_ratio: 'CALL_EXPOSURE_CAP_PCT', call_breakout_margin_ratio: 'CALL_BREAKOUT_OVERRIDE_CAP_PCT',
    call_execution_buffer_ratio: 'CALL_EXPOSURE_BUFFER_PCT', call_entry_caution_buffer_ratio: 'CALL_ENTRY_BUFFER_PCT',
    put_monetization_profit_pct: 'PUT_MONETIZATION_PROFIT_THRESHOLD', put_monetization_tranche_ratio: 'PUT_MONETIZATION_MAX_TRANCHE_FRACTION',
    call_fallback_min_bid: 'SELL_CALL_FALLBACK_MIN_BID', call_fallback_min_score: 'SELL_CALL_FALLBACK_MIN_SCORE',
    put_score_reference_dte: 'BUY_PUT_EDGE_REFERENCE_DTE', put_score_dte_exponent: 'BUY_PUT_EDGE_DTE_EXPONENT',
    call_score_reference_dte: 'SELL_CALL_EDGE_REFERENCE_DTE', call_score_dte_exponent: 'SELL_CALL_EDGE_DTE_EXPONENT',
  };
  for (const [key, sourceName] of Object.entries(names)) assert.equal(Number(defaults[key]), v2.constants[sourceName], key);
  for (const [prefix, sourceName] of [['put', 'BUY_PUT_ADVISORY_DTE_RANGE'], ['call', 'CALL_EXPIRATION_RANGE']]) {
    assert.equal(Number(defaults[`${prefix}_min_dte`]), v2.constants[sourceName][0]);
    assert.equal(Number(defaults[`${prefix}_max_dte`]), v2.constants[sourceName][1]);
  }
  for (const [prefix, sourceName] of [['put', 'PUT_DELTA_RANGE'], ['call', 'CALL_DELTA_RANGE']]) {
    assert.equal(Number(defaults[`${prefix}_min_delta`]), v2.constants[sourceName][0]);
    assert.equal(Number(defaults[`${prefix}_max_delta`]), v2.constants[sourceName][1]);
  }
  assert.equal(Number(defaults.put_annual_rate), BOT_CONFIG.PUT_ANNUAL_RATE);
  assert.equal(Number(defaults.put_budget_period_days), BOT_CONFIG.PERIOD_DAYS);
});

test('required watcher discovery, full canonical fallback, and missing coverage match source', () => {
  const contexts = [
    {}, { putBudgetRemaining: 1 }, { putBudgetRemaining: 1.01 },
    { accountHealth: { margin: { margin_usage_pct: 49 }, callMarginDiscipline: { bufferedLimitPct: 0.5 } } },
    { accountHealth: { margin: { margin_usage_pct: 50 }, callMarginDiscipline: { bufferedLimitPct: 0.5 } } },
    { accountHealth: { margin: { margin_usage_pct: 10, is_under_liquidation: true }, callMarginDiscipline: { bufferedLimitPct: 0.5 } } },
    { putBudgetRemaining: 100, positionSnapshots: [agingPut, longerPut, shortCall] },
  ];
  for (const context of contexts) {
    const requirements = parity('buildRulebookRequirements', [context]);
    parity('formatRulebookRequirements', [requirements]);
    parity('findMissingRulebookRequirements', [{ entry_rules: [{ action: 'buy_put' }], exit_rules: [{ action: 'buyback_call', instrument_name: shortCall.instrument }] }, requirements]);
  }
  const requirements = [
    { type: 'entry', action: 'sell_call' }, { type: 'entry', action: 'buy_put' },
    exitReq('sell_put', agingPut.instrument), exitReq('buyback_call', shortCall.instrument),
    exitReq('sell_put', 'unknown'), { type: 'exit', action: 'unknown' }, null,
  ];
  const snapshots = [[], [agingPut], [agingPut, longerPut, shortCall], [{ ...agingPut, dte: 25.01 }, longerPut, shortCall], [{ ...agingPut, avg_entry_price: 0 }, shortCall]];
  for (const positionSnapshots of snapshots) for (const requirement of requirements) {
    parity('buildCanonicalRequiredWatcherRule', [requirement, { positionSnapshots, advisoryId: 'fixture-advisory' }]);
  }
  parity('buildAgendaFromValidatedRules', [[rollRule(), captureRule(), { rule_type: 'entry', action: 'sell_call' }, { rule_type: 'invalid' }]]);
});

test('rule validation covers all four actions and source rejection branches', () => {
  const put = { rule_type: 'entry', action: 'buy_put', criteria: { option_type: 'P', delta_range: [-0.12, -0.02], dte_range: [45, 78], min_score: 0.0036 } };
  const call = { rule_type: 'entry', action: 'sell_call', criteria: { option_type: 'C', delta_range: [0.04, 0.12], dte_range: [5, 12], min_bid: 4, min_score: 65 } };
  const monetization = { rule_type: 'exit', action: 'sell_put', criteria: { put_exit_intent: 'monetize_tail_win', conditions: [{ field: 'unrealized_pnl_pct', op: 'gt', value: 1000 }], min_exit_price: 110.01, tranche_fraction: 0.25, retain_downside_protection: true } };
  const threat = { ...captureRule(), criteria: { buyback_intent: 'threat_management', allow_below_profit_floor: true, conditions: [{ field: 'delta', op: 'gte', value: 0.8 }, { field: 'dte', op: 'lte', value: 2 }] } };
  const cases = [null, {}, put, call, rollRule(), monetization, captureRule(), threat];
  for (const [base, alterations] of [
    [put, [{ option_type: 'C' }, { delta_range: [-0.15, -0.02] }, { dte_range: [44, 78] }, { max_cost: 1 }, { value_signal: 'surprise' }, { min_score: 0 }]],
    [call, [{ option_type: 'P' }, { delta_range: [0.01, 0.1] }, { dte_range: [5, 13] }, { min_score: 0 }, { min_bid: 0 }, { market_conditions: [{ field: 'iv' }] }]],
    [rollRule(), [{ conditions: [] }, { put_exit_intent: 'unknown' }, { conditions: [{ field: 'dte', op: 'lte', value: 26 }] }, { requires_longer_dated_protection: false }]],
    [monetization, [{ conditions: [{ field: 'unrealized_pnl_pct', op: 'gt', value: 999 }] }, { min_exit_price: 0 }, { tranche_fraction: 0.251 }, { retain_downside_protection: false }]],
    [captureRule(), [{ allow_below_profit_floor: true }, { condition_logic: 'any' }, { conditions: [{ field: 'dte', op: 'lte', value: 2 }] }, { conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 79 }] }]],
    [threat, [{ allow_below_profit_floor: false }, { conditions: [{ field: 'dte', op: 'lte', value: 2 }] }, { conditions: [{ field: 'delta', op: 'gte', value: 0.8 }] }]],
  ]) for (const patch of alterations) cases.push({ ...base, criteria: { ...base.criteria, ...patch } });
  cases.push({ ...rollRule(), preferred_order_type: 'post_only' });
  cases.push({ ...captureRule(), criteria: JSON.stringify(captureRule().criteria) });
  cases.push({ ...captureRule(), criteria: '{invalid-json' });
  for (const rule of cases) parity('validateAdvisorRuleContract', [rule, { positionSnapshots: [agingPut, longerPut, shortCall] }]);
  parity('validateAdvisorRuleContract', [rollRule(), { positionSnapshots: [agingPut] }]);
});

test('capture normalization, trigger gates, and patient call prices match source', () => {
  const rules = [captureRule(70), captureRule(80), captureRule(90), rollRule(), null,
    { ...captureRule(60), criteria: { ...captureRule(60).criteria, condition_logic: 'any', conditions: [...captureRule(60).criteria.conditions, { field: 'dte', op: 'lte', value: 2 }, { field: 'mark_price', op: 'lte', value: 4 }] } },
    { ...captureRule(), criteria: { buyback_intent: 'threat_management', allow_below_profit_floor: true, conditions: [] } },
    { ...captureRule(), criteria: { buyback_intent: 'profit_capture', conditions: [] } },
  ];
  for (const rule of rules) parity('normalizeBuybackCaptureFloor', [rule]);
  for (const capture of [79.999, 80, 80.001, 90, null]) {
    parity('getBuybackCaptureGate', [captureRule(), captureRule().criteria, { unrealized_pnl_pct: capture }]);
    parity('getBuybackCaptureGate', [captureRule(), captureRule().criteria, { unrealized_pnl_pct: 1, patient_buyback_capture_pct: capture }]);
  }
  for (const threshold of [70, 80, 90, 100]) for (const maxPrice of [undefined, 1, 10]) {
    const rule = captureRule(threshold);
    if (maxPrice !== undefined) rule.criteria.max_buyback_price = maxPrice;
    parity('getPatientBuybackPlan', [rule, rule.criteria, { avg_entry_price: 20 }]);
  }
});

test('put tranche sizing and patient fair value proofs match source', () => {
  const position = { instrument_name: agingPut.instrument, direction: 'long', amount: 2, avg_entry_price: 10 };
  const rule = { rule_type: 'exit', action: 'sell_put', preferred_order_type: 'post_only' };
  for (const intent of ['roll_protection', 'monetize_tail_win', undefined]) for (const fraction of [undefined, 0, 0.1, 0.25, 1]) {
    const criteria = { put_exit_intent: intent, tranche_fraction: fraction };
    for (const dte of [25, 26]) for (const pnl of [1000, 1001]) parity('getSellPutExitAmount', [rule, criteria, position, { dte, unrealized_pnl_pct: pnl }]);
  }
  for (const exitPrice of [110, 110.01, 200]) for (const markPrice of [0, 110, 110.01, 200]) for (const spotPrice of [1900, 1800, 3000]) {
    parity('getPatientSellPutPlan', [rule, { put_exit_intent: 'monetize_tail_win', min_exit_price: exitPrice }, position, { mark_price: markPrice, spot_price: spotPrice }]);
  }
});

test('scores and patient put pricing execute actual V2 score source across boundaries', () => {
  for (const dte of [0, 5, 8.5, 12, 25, 45, 60, 78]) {
    parity('getBuyPutDteNormalizationFactor', [dte]);
    for (const raw of [0, 0.0036, 4, 65]) {
      parity('normalizeBuyPutScore', [raw, dte]);
      parity('normalizeSellCallScore', [raw, dte]);
    }
    for (const delta of [0, -0.08, 0.12]) parity('getBuyPutPriceForEdgeScore', [delta, 0.0036, dte]);
  }
  for (const askPrice of [0, 10, 100]) for (const targetScore of [0, 0.0036, 0.005]) {
    parity('getBuyPutEntryPricing', [{ askPrice, absDelta: 0.08, dte: 60, minScore: 0.0036, targetScore }]);
  }
});

test('default policy prompts retain actual V2 discipline wording', () => {
  for (const name of ['getCallMarginDisciplinePrompt', 'getCallBuybackDisciplinePrompt', 'getPutExitDisciplinePrompt', 'getStandingRulebookDisciplinePrompt']) parity(name, []);
});

test('margin utilization, target headroom, and explicitly supplied breakout research match V2 source', () => {
  const states = [null, {},
    { collaterals_value: 1000, positions_initial_margin: -250, open_orders_margin: -50 },
    { collaterals_initial_margin: 800, collaterals_maintenance_margin: -1000, maintenance_margin: 600, aggregated_positions_initial_margin: -300 },
    { subaccount_value: 1000, initial_margin: 750, margin_usage_pct: 90 },
    { aggregated_collaterals_maintenance_margin: -1000, maintenance_margin: 550, open_orders_margin: 20 },
  ];
  for (const state of states) {
    for (const name of ['getMarginCapacityBase', 'getMarginUtilizationBase', 'estimateDisplayedMarginUtilization', 'getDisplayedMarginHeadroomAtCap']) parity(name, [state]);
    for (const additional of [0, 50, 1000]) for (const name of ['estimateMarginUtilizationFromComponents', 'estimateMarginUtilization', 'estimateProjectedDisplayedMarginUtilization']) parity(name, [state, additional]);
  }
  const positions = [{ instrument_name: shortCall.instrument, direction: 'short', amount: 1 }];
  for (const researchState of [{},
    { shortTermMomentum: { main: 'upward', derivative: 'moving', threeDayHigh: 3000 }, mediumTermMomentum: { main: 'upward' } },
    { shortTermMomentum: { main: 'upward', derivative: 'moving', threeDayHigh: 3000 }, mediumTermMomentum: { main: 'downward' } },
  ]) {
    const original = actualV2(researchState).api;
    const extracted = v2Reference.legacyFor(undefined, researchState);
    for (const name of ['getEffectiveCallExposureCapPct', 'getEffectiveCallExposureLimitPct']) for (const spot of [2800, 3000]) {
      assert.equal(extracted[name](positions, spot), original[name](positions, spot), `${name}, spot=${spot}`);
    }
  }
});

test('alternate release propagates 35 DTE / 70% capture / 30% target to fallback, validation, pricing, and prompts', () => {
  const strategy = reference.createExampleStrategy();
  const alternate = strategy.legacyFor();
  const parameters = strategy.policyFor();
  assert.equal(parameters.put_roll_dte, '35');
  assert.equal(parameters.call_profit_capture_pct, '70');
  assert.equal(parameters.call_entry_margin_ratio, '0.3');
  const context = { positionSnapshots: [{ ...agingPut, dte: 30 }, longerPut, shortCall] };
  const put = alternate.buildCanonicalRequiredWatcherRule(exitReq('sell_put', agingPut.instrument), context);
  const call = alternate.buildCanonicalRequiredWatcherRule(exitReq('buyback_call', shortCall.instrument), context);
  assert.equal(put.criteria.put_exit_intent, 'roll_protection');
  assert.equal(put.criteria.conditions[0].value, 35);
  assert.equal(call.criteria.target_capture_pct, 70);
  assert.equal(call.criteria.max_buyback_price, 6);
  assert.equal(alternate.validateAdvisorRuleContract(rollRule(35), context).valid, true);
  assert.equal(legacy.validateAdvisorRuleContract(rollRule(35), context).valid, false);
  assert.equal(alternate.validateAdvisorRuleContract(captureRule(70), context).valid, true);
  assert.equal(legacy.validateAdvisorRuleContract(captureRule(70), context).valid, false);
  assert.equal(alternate.normalizeBuybackCaptureFloor(captureRule(60)).rule.criteria.conditions[0].value, 70);
  assert.ok(Math.abs(alternate.getPatientBuybackPlan(captureRule(70), captureRule(70).criteria, { avg_entry_price: 20 }).limitPrice - 6) < 1e-12);
  assert.ok(Math.abs(alternate.getCallExposureLimitPct(0.3) - 0.35) < 1e-12);
  const margin = { collaterals_maintenance_margin: -1000, maintenance_margin: 700 };
  assert.equal(alternate.getDisplayedMarginHeadroomAtCap(margin), 0);
  assert.ok(Math.abs(legacy.getDisplayedMarginHeadroomAtCap(margin) - 150) < 1e-12);
  assert.equal(alternate.getEffectiveCallExposureCapPct([], 3000), 0.3);
  assert.match(alternate.getCallMarginDisciplinePrompt(), /normally target 30%/);
  assert.match(alternate.getPutExitDisciplinePrompt(), /DTE <= 35/);
  assert.match(alternate.getCallBuybackDisciplinePrompt(), />= 70%/);
  assert.match(alternate.getStandingRulebookDisciplinePrompt(), /70%\+ executable capture/);
  assert.match(alternate.buildRulebookRequirements({ positionSnapshots: [shortCall] })[0].instruction, /70%\+ capture/);
  const prompts = strategy.buildPrompts();
  for (const phase of ['generation', 'confirmation', 'reporting']) {
    assert.equal(typeof prompts[phase], 'string');
    assert.match(prompts[phase], /35/);
    assert.match(prompts[phase], /70/);
    assert.match(prompts[phase], /30/);
  }
  assert.equal(strategy.reportPolicy().parameters.call_profit_capture_pct, '70');
  assert.equal(reference.createReferenceStrategy().policyFor().call_profit_capture_pct, '80');
});

test('selected releases require complete accepted parameters and cannot mutate another mandate policy', () => {
  const overrides = { put_roll_dte: '35', call_profit_capture_pct: '70', call_entry_margin_ratio: '0.30' };
  const strategy = reference.createReferenceStrategy({ parameters: overrides });
  overrides.call_profit_capture_pct = '80';
  assert.equal(strategy.policyFor().call_profit_capture_pct, '70');
  assert.throws(() => { strategy.policyFor().call_profit_capture_pct = '80'; }, TypeError);
  assert.throws(() => { strategy.release.parameters.call_profit_capture_pct.default = '80'; }, TypeError);
  const mandate = {
    strategy_release_id: strategy.release.strategy_release_id,
    release_digest: strategy.release.release_digest,
    parameters: { ...strategy.policyFor(), call_profit_capture_pct: '60' },
  };
  assert.equal(strategy.policyFor(mandate).call_profit_capture_pct, '60');
  assert.equal(strategy.legacyFor(mandate).normalizeBuybackCaptureFloor(captureRule(50)).rule.criteria.conditions[0].value, 60);
  assert.equal(strategy.policyFor().call_profit_capture_pct, '70');
  assert.throws(() => strategy.policyFor({ ...mandate, parameters: { call_profit_capture_pct: '60' } }), /Missing materialized parameter/);
  assert.throws(() => strategy.policyFor({ ...mandate, strategy_release_id: 'another-release' }), /different reference release/);
  assert.throws(() => strategy.policyFor({ ...mandate, release_digest: 'sha256:' + '0'.repeat(64) }), /different reference release/);
  for (const parameters of [
    { hidden_global_cap: '0.45' }, { put_roll_dte: '-1' }, { call_profit_capture_pct: '100' },
    { call_entry_margin_ratio: '0.7', call_breakout_margin_ratio: '0.6' },
    { put_min_dte: '78', put_max_dte: '45' }, { put_min_delta: '-0.02', put_max_delta: '-0.12' },
  ]) assert.throws(() => reference.createReferenceStrategy({ parameters }));
});

function refreshBundle(fixture) {
  const { input_bundle_id, ...content } = fixture.inputBundle;
  fixture.inputBundle.input_bundle_id = contentDigest(content);
}

function generatedFixture(options = {}) {
  const fixture = createReplayFixture(options);
  const decision = fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate });
  return { ...fixture, decision, intent: clone(decision.operations[0].intent) };
}

function checkEconomics(fixture, options = {}) {
  return fixture.strategy.validateEconomicPolicy(fixture.intent, fixture.inputBundle, { mandate: fixture.mandate, ...options });
}

test('all four recorded action adapters produce deterministic contract-valid decisions', () => {
  for (const strategy of [reference.createReferenceStrategy(), reference.createExampleStrategy()]) {
    for (const action of ['buy_put', 'sell_put', 'sell_call', 'buyback_call']) {
      const fixture = generatedFixture({ strategy, action });
      assert.deepEqual(fixture.decision, strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate }));
      assert.equal(fixture.intent.action, action);
      assert.equal(fixture.intent.order.reduce_only, ['sell_put', 'buyback_call'].includes(action));
      assert.equal(checkEconomics(fixture, { phase: 'admission' }).valid, true, action);
    }
  }
});

test('dormant watchers are admitted while trigger-time policy enforces each selected release', () => {
  for (const action of ['sell_put', 'buyback_call']) {
    const baseline = generatedFixture({ action });
    const alternate = generatedFixture({ strategy: reference.createExampleStrategy(), action });
    assert.equal(checkEconomics(baseline, { phase: 'admission' }).valid, true);
    assert.equal(checkEconomics(alternate, { phase: 'admission' }).valid, true);
    assert.equal(checkEconomics(baseline).valid, false, `${action} has not reached the V2 trigger`);
    assert.equal(checkEconomics(alternate).valid, true, `${action} has reached the alternate trigger`);
  }
  const baselineCall = generatedFixture({ action: 'sell_call' });
  const alternateCall = generatedFixture({ strategy: reference.createExampleStrategy(), action: 'sell_call' });
  assert.equal(checkEconomics(baselineCall).valid, true);
  assert.equal(checkEconomics(alternateCall, { phase: 'admission' }).valid, true);
  assert.equal(checkEconomics(alternateCall).valid, false, '35% current projected margin exceeds selected 30% target');
  assert.equal(checkEconomics(baselineCall, { phase: 'unknown' }).valid, false);
});

test('financial policy boundaries use exact decimals beyond binary number precision', () => {
  const buyback = generatedFixture({ action: 'buyback_call', fieldOverrides: { 'noop_v2.call_profit_capture_pct': '80' } });
  assert.equal(checkEconomics(buyback).valid, true);
  buyback.intent.order.limit_price.value = '2.0000000000000000001';
  const overCaptureCeiling = checkEconomics(buyback);
  assert.equal(overCaptureCeiling.valid, false);
  assert.match(overCaptureCeiling.reasons.join(' '), /Call buyback limit/);

  const call = generatedFixture({ action: 'sell_call', fieldOverrides: { 'noop_v2.projected_displayed_margin_ratio': '0.45' } });
  assert.equal(checkEconomics(call).valid, true);
  call.inputBundle.instrument_fields[call.instrumentRef]['noop_v2.projected_displayed_margin_ratio'].value = '0.4500000000000000001';
  call.inputBundle.extensions.reference_v2.margin_projections[call.intent.intent_id] = reference.marginProjectionBinding(call.intent, call.inputBundle);
  const overMarginTarget = checkEconomics(call);
  assert.equal(overMarginTarget.valid, false);
  assert.match(overMarginTarget.reasons.join(' '), /Projected displayed margin/);

  const put = generatedFixture({ action: 'buy_put', fieldOverrides: { 'noop_v2.put_budget_available': '1.01' } });
  assert.equal(checkEconomics(put).valid, true);
  put.intent.order.max_total_outlay.value = '1.0100000000000000001';
  assert.equal(checkEconomics(put).valid, false);

  const roll = generatedFixture({ action: 'sell_put', fieldOverrides: { 'instrument.dte': '25' } });
  assert.equal(checkEconomics(roll).valid, true);
  roll.inputBundle.instrument_fields[roll.instrumentRef]['instrument.dte'].value = '25.0000000000000000001';
  assert.equal(checkEconomics(roll).valid, false);
});

test('put monetization preserves a strict profit trigger and exact retained tranche', () => {
  const fixture = createReplayFixture({ action: 'sell_put', fieldOverrides: {
    'noop_v2.put_executable_pnl_pct': '1000.0000000000000000001',
    'instrument.best_bid': '110.00000000000000000001',
  } });
  fixture.rule.criteria = {
    put_exit_intent: 'monetize_tail_win', conditions: [{ field: 'unrealized_pnl_pct', op: 'gte', value: 1000 }],
    condition_logic: 'all', min_exit_price: '110.01', tranche_fraction: '0.25', retain_downside_protection: true,
  };
  fixture.binding.order.limit_price.value = '120';
  fixture.binding.quantity.max_total = '0.25';
  fixture.inputBundle.extensions.reference_v2.recorded_rules_json = JSON.stringify([fixture.rule]);
  refreshBundle(fixture);
  fixture.intent = clone(fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate }).operations[0].intent);
  // The original V2 validator accepts gte 1000, but the bounded adapter adds
  // its explicit > 1000 Strategy condition; do not conceal that source quirk.
  assert.equal(legacy.validateAdvisorRuleContract(fixture.rule).valid, true);
  assert.equal(checkEconomics(fixture).valid, true);
  fixture.intent.quantity.max_total = '0.2500000000000000001';
  assert.equal(checkEconomics(fixture).valid, false);
  fixture.intent.quantity.max_total = '0.25';
  fixture.inputBundle.instrument_fields[fixture.instrumentRef]['noop_v2.put_executable_pnl_pct'].value = '1000';
  assert.equal(checkEconomics(fixture).valid, false);
});

test('premium authorization preserves the source formula with explicit exact rounding and reservations', () => {
  const rate = SOURCE.match(/^const PUT_ANNUAL_RATE = BOT_CONFIG\.PUT_ANNUAL_RATE[^\n]+/m)?.[0];
  const cycles = SOURCE.match(/^    const cyclesPerYear = [^\n]+/m)?.[0];
  const expression = SOURCE.match(/const newBudget = canUsePortfolioValue\s*\? ([^\n]+)\s*:/)?.[1];
  assert.ok(rate && cycles && expression, 'review the bounded V2 budget formula extraction');
  const sourceBudget = vm.runInNewContext(`${rate}\n${cycles}\n(numericPortfolioValue) => (${expression})`, { BOT_CONFIG }, { timeout: 1000 });
  for (const base of ['1', '7300', '36500', '1000000']) {
    const result = v2Reference.calculatePremiumAuthorization({ insured_base_value: base });
    const expected = sourceBudget(Number(base));
    assert.ok(Math.abs(Number(result.cycle_budget) - expected) <= Math.max(Number.EPSILON, Math.abs(expected) * Number.EPSILON * 4));
    assert.equal(result.rounding, 'floor_30_decimal_places');
  }
  assert.equal(v2Reference.calculatePremiumAuthorization({ insured_base_value: '1' }).cycle_budget, '0.001368493150684931506849315068');
  assert.equal(v2Reference.calculatePremiumAuthorization({ insured_base_value: '7300' }).cycle_budget, '9.99');
  assert.equal(v2Reference.calculatePremiumAuthorization({ insured_base_value: '7300', carry_forward: '5', net_put_cost: '3', reserved: '2' }).available_premium_authorization, '9.99');
  assert.equal(v2Reference.calculatePremiumAuthorization({ insured_base_value: '7300', net_put_cost: '-5' }).available_premium_authorization, '14.99');
  assert.equal(v2Reference.calculatePremiumAuthorization({ insured_base_value: '7300', reserved: '20' }).available_premium_authorization, '0');
  const custom = reference.createReferenceStrategy({ parameters: { put_annual_rate: '0.05', put_budget_period_days: '30' } });
  assert.equal(custom.calculatePremiumAuthorization({ insured_base_value: '7300' }).cycle_budget, '30');
  assert.throws(() => v2Reference.calculatePremiumAuthorization({ insured_base_value: '0' }), /must be positive/);
  assert.throws(() => v2Reference.calculatePremiumAuthorization({ insured_base_value: '7300', reserved: '-1' }), /nonnegative/);
});

test('economic evidence binds intent identity, order semantics, and the accepted condition tree', () => {
  const original = generatedFixture({ action: 'buyback_call', fieldOverrides: { 'noop_v2.call_profit_capture_pct': '80' } });
  const mutations = [
    f => { f.intent.order.side = 'sell'; },
    f => { f.intent.order.reduce_only = false; },
    f => { f.intent.order.time_in_force = 'gtc'; },
    f => { f.intent.quantity.unit = 'ETH'; },
    f => { f.intent.order.limit_price.unit = 'USDC/ETH'; },
    f => { f.intent.purpose = 'earn_call_premium'; },
    f => { f.intent.evidence.parameters_digest = 'sha256:' + '0'.repeat(64); },
    f => { const rule = JSON.parse(f.intent.evidence.legacy_rule_json); rule.instrument_name = 'ETH-20300131-5000-C'; f.intent.evidence.legacy_rule_json = JSON.stringify(rule); },
    f => { f.intent.when.args = f.intent.when.args.slice(0, 1); },
    f => { delete f.inputBundle.instruments[f.instrumentRef]; },
    f => { f.inputBundle.instruments[f.instrumentRef].kind = 'put'; },
  ];
  for (const mutate of mutations) {
    const fixture = { ...original, intent: clone(original.intent), inputBundle: clone(original.inputBundle) };
    mutate(fixture);
    assert.equal(checkEconomics(fixture, { phase: 'admission' }).valid, false, String(mutate));
  }
});

test('reference adapter refuses unsupported V2 criteria and order conversions explicitly', () => {
  const cases = [
    ['buy_put', f => { f.rule.max_quantity = '1'; }, /explicit adapter|Unsupported|Unknown/i],
    ['buy_put', f => { f.rule.criteria.value_signal = 'any_actionable_buy_put'; }, /explicit adapter/],
    ['buy_put', f => { f.rule.criteria.max_buyback_price = '2'; }, /explicit adapter/],
    ['sell_call', f => { f.rule.criteria.market_conditions = [{ field: 'spot_price', op: 'gte', value: 3000 }]; }, /explicit adapter/],
    ['sell_call', f => { f.rule.budget_limit = '10'; }, /budget_limit.*explicit adapter/],
    ['buyback_call', f => { delete f.rule.instrument_name; }, /must identify its owned instrument/],
    ['buyback_call', f => { f.rule.preferred_order_type = 'post_only'; f.binding.order.time_in_force = 'post_only'; }, /no implicit post_only conversion/],
    ['buyback_call', f => { f.rule.preferred_order_type = 'gtc'; f.binding.order.time_in_force = 'gtc'; }, /Synthetic resting exit parity/],
    ['sell_put', f => { f.rule.criteria.condition_logic = 'any'; }, /requires all legacy exit conditions/],
    ['sell_put', f => { f.rule.criteria.tranche_fraction = '0.25'; }, /roll.*tranche constraint.*explicit adapter/i],
    ['sell_put', f => {
      f.rule.criteria = { put_exit_intent: 'monetize_tail_win', conditions: [{ field: 'unrealized_pnl_pct', op: 'gt', value: 1000 }],
        min_exit_price: '110.01', tranche_fraction: '0.25', retain_downside_protection: true, requires_longer_dated_protection: true };
    }, /Monetization.*replacement-protection.*explicit adapter/],
    ['buyback_call', f => { f.rule.criteria.max_buyback_price = '2'; f.rule.criteria.limit_price = '3'; }, /conflict/i],
  ];
  for (const [action, mutate, expected] of cases) {
    const fixture = createReplayFixture({ action });
    mutate(fixture);
    fixture.inputBundle.extensions.reference_v2.recorded_rules_json = JSON.stringify([fixture.rule]);
    refreshBundle(fixture);
    assert.throws(() => fixture.strategy.generateDecision(fixture.inputBundle, { mandate: fixture.mandate }), expected);
  }
});

test('margin projections cannot be reused for a different quantity or account context', () => {
  const original = generatedFixture({ action: 'sell_call' });
  assert.equal(checkEconomics(original).valid, true);
  const changedQuantity = { ...original, intent: clone(original.intent) };
  changedQuantity.intent.quantity.max_total = '0.2';
  const wrongCandidate = checkEconomics(changedQuantity);
  assert.equal(wrongCandidate.valid, false);
  assert.match(wrongCandidate.reasons.join(' '), /projection.*exact candidate/i);
  const changedContext = { ...original, inputBundle: clone(original.inputBundle) };
  changedContext.inputBundle.fields['noop_v2.put_budget_available'].value = '9';
  const wrongContext = checkEconomics(changedContext);
  assert.equal(wrongContext.valid, false);
  assert.match(wrongContext.reasons.join(' '), /projection.*exact candidate/i);
});

test('trigger-time evidence expires against the supplied scheduler clock', () => {
  const fixture = generatedFixture({ action: 'buyback_call', fieldOverrides: { 'noop_v2.call_profit_capture_pct': '80' } });
  assert.equal(checkEconomics(fixture, { now: '2030-01-01T00:00:00.000Z' }).valid, true);
  const stale = checkEconomics(fixture, { now: '2030-01-01T00:00:06.000Z' });
  assert.equal(stale.valid, false);
  assert.match(stale.reasons.join(' '), /Stale or future/);
  fixture.inputBundle.instrument_fields[fixture.instrumentRef]['noop_v2.call_profit_capture_pct'].available_at = '2030-01-01T00:00:01.000Z';
  assert.equal(checkEconomics(fixture).valid, false);
});

test('release artifact pins cover every reference runtime file and exact contract dependency bytes', () => {
  const packageRoot = path.join(ROOT, 'strategies/noop-v2-reference');
  const artifact = JSON.parse(fs.readFileSync(path.join(packageRoot, 'artifact.json'), 'utf8'));
  const packageFiles = ['config.js', 'fields.js', 'fixtures.js', 'index.js', 'legacy.js', 'provenance.json'];
  const dependencies = ['strategy/canonical.js', 'strategy/conditions.js', 'strategy/contract.js',
    'strategy/decimal.js', 'strategy/fields.js', 'strategy/schemas/common.json', 'strategy/schemas/decision.json',
    'strategy/schemas/input-bundle.json', 'strategy/schemas/mandate.json', 'strategy/schemas/release.json'];
  assert.deepEqual(Object.keys(artifact.files).sort(), [...packageFiles].sort());
  assert.deepEqual(Object.keys(artifact.dependencies).sort(), [...dependencies].sort());
  // The explicit updater is development tooling. Adding another runtime .js
  // file requires expanding the release pins and this reviewed inventory.
  assert.deepEqual(fs.readdirSync(packageRoot).filter(file => file.endsWith('.js')).sort(),
    packageFiles.filter(file => file.endsWith('.js')).sort());
  for (const [base, entries] of [[packageRoot, artifact.files], [ROOT, artifact.dependencies]]) {
    for (const [file, expected] of Object.entries(entries)) {
      const actual = 'sha256:' + createHash('sha256').update(fs.readFileSync(path.join(base, file))).digest('hex');
      assert.equal(actual, expected, `${file} changed: review it and explicitly refresh the artifact pins`);
    }
  }
  assert.equal(v2Reference.release.artifact.digest, contentDigest(artifact.files));
  assert.equal(v2Reference.release.artifact.dependency_lock_digest, contentDigest(artifact.dependencies));
});

test('every recorded V2 source snippet hash identifies the checked-out bounded declaration', () => {
  const provenance = JSON.parse(fs.readFileSync(path.join(ROOT, 'strategies/noop-v2-reference/provenance.json'), 'utf8'));
  assert.equal(provenance.snippets.length, 64, 'review changes to the extracted helper inventory');
  assert.equal(new Set(provenance.snippets.map(snippet => snippet.name)).size, provenance.snippets.length);
  for (const snippet of provenance.snippets) {
    assert.equal(snippet.source, 'script.js');
    assert.match(snippet.name, /^[A-Za-z][A-Za-z0-9_]*$/);
    const prefix = `const ${snippet.name} =`;
    const start = SOURCE.indexOf(`\n${prefix}`) + 1;
    assert.ok(start > 0, `source declaration exists: ${snippet.name}`);
    const remaining = SOURCE.slice(start + prefix.length);
    const next = remaining.search(/^const /m);
    assert.ok(next >= 0, `review end boundary for ${snippet.name}`);
    const declarationRegion = SOURCE.slice(start, start + prefix.length + next);
    assert.ok(declarationRegion.length < 32768, 'source provenance region remains bounded');
    // Hash candidate semicolon endings only inside this declaration's region.
    // This avoids parsing template literals or executing source to locate its
    // original statement boundary; no updater or production module is loaded.
    let matched = false;
    for (let end = declarationRegion.indexOf(';'); end >= 0; end = declarationRegion.indexOf(';', end + 1)) {
      const actual = createHash('sha256').update(declarationRegion.slice(0, end + 1)).digest('hex');
      if (actual === snippet.source_sha256) { matched = true; break; }
    }
    assert.equal(matched, true, `V2 source changed for ${snippet.name}; review extraction and recorded provenance`);
  }
});

test('reference import and construction do not acquire environment, network, database, filesystem writes, clocks, or timers', () => {
  const program = `
    const assert = require('node:assert/strict');
    const Module = require('node:module');
    const path = require('node:path');
    const fs = require('node:fs');
    const root = process.argv[1];
    const originalLoad = Module._load;
    const forbid = label => () => { throw new Error('forbidden capability: ' + label); };
    const deny = /^(?:node:)?(?:fs(?:\\/promises)?|http|https|http2|net|tls|dgram|child_process|cluster|worker_threads|process)$|^(?:axios|ethers|better-sqlite3)$/;
    Module._load = function (request, parent, isMain) {
      if (deny.test(request) || /(?:^|[\\\\/])(?:script|dashboard_server)\\.js$/.test(request) || /(?:^|[\\\\/])bot[\\\\/]db(?:\\.js)?$/.test(request)) forbid(request)();
      return originalLoad.call(this, request, parent, isMain);
    };
    for (const name of ['writeFileSync', 'appendFileSync', 'mkdirSync', 'createWriteStream', 'writeFile', 'appendFile', 'mkdir', 'open']) fs[name] = forbid('fs.' + name);
    const originalOpen = fs.openSync;
    fs.openSync = function (file, flags, ...rest) {
      const relative = path.relative(root, String(file));
      if (flags !== 'r' || !/^(?:strategy[\\\\/]|strategies[\\\\/]noop-v2-reference[\\\\/])/.test(relative) || !/\\.(?:js|json)$/.test(relative)) forbid('fs.openSync ' + relative)();
      return originalOpen.call(this, file, flags, ...rest);
    };
    globalThis.fetch = forbid('fetch');
    globalThis.setInterval = forbid('setInterval');
    globalThis.setTimeout = forbid('setTimeout');
    Math.random = forbid('Math.random');
    const NativeDate = Date;
    globalThis.Date = class extends NativeDate {
      constructor(...args) { if (args.length === 0) forbid('new Date()')(); super(...args); }
      static now() { forbid('Date.now')(); }
    };
    const oldEnv = process.env;
    process.env = new Proxy(oldEnv, { get(_target, key) { throw new Error('environment read: ' + String(key)); }, ownKeys() { throw new Error('environment enumeration'); } });
    try {
      const ref = require(path.join(root, 'strategies/noop-v2-reference'));
      const strategy = ref.createReferenceStrategy();
      const example = ref.createExampleStrategy();
      strategy.policyFor(); strategy.legacyFor(); strategy.buildPrompts(); strategy.reportPolicy();
      example.policyFor(); example.legacyFor(); example.buildPrompts(); example.reportPolicy();
      assert.equal(Object.keys(require.cache).some(file => /(?:^|[\\\\/])bot[\\\\/]db\\.js$/.test(file)), false);
    } finally { process.env = oldEnv; }
  `;
  const child = spawnSync(process.execPath, ['-e', program, ROOT], { cwd: ROOT, encoding: 'utf8', timeout: 10000 });
  assert.equal(child.status, 0, `${child.stdout}\n${child.stderr}`);
  assert.equal(child.error, undefined);
});
