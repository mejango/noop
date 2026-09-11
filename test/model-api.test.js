const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

// Exercise the actual helpers without importing the bot and starting trading.
const source = fs.readFileSync(path.join(__dirname, '..', 'script.js'), 'utf8');
const helperSource = source.slice(
  source.indexOf('const OPENAI_STRATEGY_MODEL ='),
  source.indexOf('const getAnthropicErrorMessage ='),
);
function loadHelpers(post, env = { OPENAI_API_KEY: 'test-key' }) {
  const context = vm.createContext({ axios: { post }, process: { env }, console: { log() {} } });
  vm.runInContext(`${helperSource}\nthis.api = {
    callOpenAI, getAnthropicResponseText, getAnthropicResponseFailure, OPENAI_STRATEGY_MODEL,
    OPENAI_CONFIRMATION_MODEL, ANTHROPIC_STRATEGY_MODEL, ANTHROPIC_SONNET_MODEL,
  };`, context);
  return context.api;
}

test('Astra sends a reasoning-compatible request and returns the answer', async () => {
  const api = loadHelpers(async (url, body, options) => {
    assert.equal(url, 'https://api.openai.com/v1/chat/completions');
    assert.equal(body.model, 'gpt-6-astra');
    assert.equal(body.reasoning_effort, 'medium');
    assert.equal(body.max_completion_tokens, 16384);
    assert.equal('max_tokens' in body, false);
    assert.equal(body.messages[0].content, 'system');
    assert.equal(body.messages[1].content, 'user');
    assert.equal(options.timeout, 120000);
    return { data: { choices: [{ finish_reason: 'stop', message: { content: '{"regime":"calm"}' } }] } };
  });
  assert.equal(await api.callOpenAI('system', 'user'), '{"regime":"calm"}');
  assert.equal(api.ANTHROPIC_STRATEGY_MODEL, 'claude-fable-5-1');
  assert.equal(api.ANTHROPIC_SONNET_MODEL, 'claude-sonnet-5');
});

test('Terra confirmation uses the short non-reasoning request', async () => {
  const api = loadHelpers(async (_url, body, options) => {
    assert.equal(body.model, 'gpt-5.6-terra');
    assert.equal(body.reasoning_effort, 'none');
    assert.equal(body.max_completion_tokens, 512);
    assert.equal(options.timeout, 15000);
    return { data: { choices: [{ finish_reason: 'stop', message: { content: '{"confirm":true}' } }] } };
  });
  assert.equal(await api.callOpenAI('system', 'user', {
    model: api.OPENAI_CONFIRMATION_MODEL, reasoningEffort: 'none', maxTokens: 512, timeout: 15000,
  }), '{"confirm":true}');
});

test('incomplete OpenAI output is rejected even if it contains valid JSON', async () => {
  const api = loadHelpers(async () => ({ data: { choices: [
    { finish_reason: 'length', message: { content: '{"confirm":true}' } },
  ] } }));
  assert.equal(await api.callOpenAI('s', 'u'), null);
});

test('API failures and missing keys retain the existing null fallback', async () => {
  const api = loadHelpers(async () => { throw new Error('timeout'); });
  assert.equal(await api.callOpenAI('s', 'u'), null);
  const missingKey = loadHelpers(() => assert.fail('must not call API'), {});
  assert.equal(await missingKey.callOpenAI('s', 'u'), null);
});

test('Claude reads text after thinking blocks and rejects truncated answers', () => {
  const api = loadHelpers();
  const content = [
    { type: 'thinking', thinking: '{"confirm":false}' },
    { type: 'text', text: '{"confirm":' },
    { type: 'text', text: 'true}' },
  ];
  assert.equal(api.getAnthropicResponseText({ stop_reason: 'end_turn', content }), '{"confirm":true}');
  assert.equal(api.getAnthropicResponseText({ stop_reason: 'max_tokens', content }), '');
  assert.equal(api.getAnthropicResponseText(undefined), '');
  assert.equal(api.getAnthropicResponseText({ stop_reason: 'end_turn', content: [content[0]] }), '');
});

test('Claude failure diagnostics distinguish truncation, refusal, and empty text', () => {
  const api = loadHelpers();
  assert.equal(api.getAnthropicResponseFailure({ stop_reason: 'max_tokens', usage: { output_tokens: 256 } }),
    'truncated response (max_tokens; output_tokens=256)');
  assert.match(api.getAnthropicResponseFailure({ stop_reason: 'refusal' }), /stop_reason=refusal/);
  assert.match(api.getAnthropicResponseFailure({ stop_reason: 'end_turn' }), /empty text response/);
});

function loadMakerHelpers() {
  const context = vm.createContext({
    normalizeBuyPutScore: require('../bot/put-score').normalizeBuyPutScore,
  });
  for (const name of [
    'parseMaybeJsonObject', 'getInstrumentPriceStep', 'roundToStep', 'getStepDecimals',
    'normalizePriceToStep', 'avoidRoundNumberRestingPrice', 'computePostOnlyRetryPrice',
    'formatBuyPutConfirmationContext',
  ]) {
    const start = source.indexOf(`const ${name} =`);
    assert.ok(start >= 0, `Missing production helper ${name}`);
    const end = source.indexOf('\n};', start) + 3;
    vm.runInContext(source.slice(start, end), context);
  }
  return vm.runInContext('({ computePostOnlyRetryPrice, formatBuyPutConfirmationContext })', context);
}

test('screenshot scenario supplies a cheaper maker bid within the approved cap', () => {
  const api = loadMakerHelpers();
  const ticker = { b: 12.8, a: 13.28, option_pricing: { d: -0.1 } };
  const instrument = { option_details: { option_type: 'P' }, price_step: 0.1 };
  const plan = api.computePostOnlyRetryPrice('buy', ticker, instrument, 13.28);
  assert.ok(plan.retryPrice > 0 && plan.retryPrice < 13.28);
  assert.ok(3.38 * plan.retryPrice < 45);
  assert.ok(Math.abs(plan.retryPrice * 10 - Math.round(plan.retryPrice * 10)) < 1e-9);
  const prompt = api.formatBuyPutConfirmationContext({
    action: { action: 'buy_put', amount: 3.38, rule_criteria: { min_score: 0.001 } },
    triggerData: { delta: -0.1, dte: 78, target_score: 0.001 },
    ticker, currentPrice: 13.28, advisorLimitPrice: 13.28, instrument,
  });
  assert.ok(prompt.includes(`Computed post_only bid on this book: $${plan.retryPrice.toFixed(4)}`));
  assert.ok(prompt.includes('maximum price, not an exact required fill price'));
  assert.ok(prompt.includes('Keep all value, budget, margin, and live-data checks'));
});

test('a rising ask cannot raise a post-only buy retry above its approved price', () => {
  const api = loadMakerHelpers();
  const instrument = { price_step: 0.1 };
  const plan = api.computePostOnlyRetryPrice('buy', { b: 14, a: 15 }, instrument, 13.28);
  assert.ok(plan.retryPrice <= 13.28);
  assert.equal(api.computePostOnlyRetryPrice('buy', { a: 0 }, instrument, 13.28), null);
  assert.equal(api.computePostOnlyRetryPrice('buy', { a: 0.1 }, instrument, 0.1), null);
});

test('confirmation economics use the proposed maker price, not the reference cap', () => {
  const api = loadMakerHelpers();
  const ticker = { b: 13.4, a: 13.56, option_pricing: { d: -0.1 } };
  const instrument = { price_step: 0.1 };
  const plan = api.computePostOnlyRetryPrice('buy', ticker, instrument, 13.56);
  const normalizeScore = require('../bot/put-score').normalizeBuyPutScore;
  const prompt = api.formatBuyPutConfirmationContext({
    action: { action: 'buy_put', amount: 3, rule_criteria: { min_score: 0.001 } },
    triggerData: { delta: -0.1, dte: 78, target_score: 0.001 },
    ticker, currentPrice: 13.56, advisorLimitPrice: 13.56, instrument,
  });
  assert.ok(prompt.includes(`Planned execution limit: $${plan.retryPrice.toFixed(4)}`));
  assert.ok(prompt.includes('capped by advisor_limit_price=$13.5600'));
  assert.ok(prompt.includes(`planned PUT_EDGE=${normalizeScore(0.1 / plan.retryPrice, 78).toFixed(6)}`));
  assert.ok(prompt.includes(`Planned premium outlay (excluding fees): $${(3 * plan.retryPrice).toFixed(4)}`));
});

test('missing maker book is explicitly identified as reference-only economics', () => {
  const api = loadMakerHelpers();
  const prompt = api.formatBuyPutConfirmationContext({
    action: { action: 'buy_put', amount: 3 },
    triggerData: { delta: -0.1, dte: 78 },
    ticker: {}, currentPrice: 13.56, advisorLimitPrice: 13.56,
    instrument: { price_step: 0.1 },
  });
  assert.ok(prompt.includes('Planned execution limit: $13.5600'));
  assert.ok(prompt.includes('reference-limit economics only; a maker price has not been established'));
  assert.ok(prompt.includes('No computed maker bid is available'));
});
