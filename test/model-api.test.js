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
    callOpenAI, getAnthropicResponseText, OPENAI_STRATEGY_MODEL,
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
