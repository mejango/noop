'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

test('Strategy modules import and construct without bot, SDK, database, network, timer, or credential access', () => {
  const root = path.resolve(__dirname, '..');
  const script = `
    const Module = require('node:module');
    const fs = require('node:fs');
    const load = Module._load;
    Module._load = function(request, parent, isMain) {
      if (/^(?:node:)?(?:http|https|net|tls|dgram|child_process)$/.test(request)
        || /(?:ethers|axios|sqlite|derive-v3|bot\\/db|bot\\/index|\\/script(?:\\.js)?$)/.test(request)) {
        throw new Error('Forbidden runtime dependency: ' + request);
      }
      return load.apply(this, arguments);
    };
    const read = fs.readFileSync;
    fs.readFileSync = function(file) {
      if (typeof file === 'string' && /(?:\\.env(?:$|\\.)|\\.key$|\\.derive-v3|[\\/]data[\\/]|noop\\.db)/.test(file)) {
        throw new Error('Forbidden credential or database read');
      }
      return read.apply(this, arguments);
    };
    for (const method of ['writeFileSync','appendFileSync','mkdirSync','rmSync','rmdirSync','unlinkSync','renameSync','copyFileSync','truncateSync','createWriteStream','writeSync']) {
      fs[method] = () => { throw new Error('Forbidden filesystem mutation'); };
    }
    for (const name of ['fetch','WebSocket','setTimeout','setInterval','setImmediate']) {
      globalThis[name] = () => { throw new Error('Forbidden runtime side effect'); };
    }
    Date.now = () => { throw new Error('Implicit wall clock'); };
    require(${JSON.stringify(path.join(root, 'strategy/contract'))});
    require(${JSON.stringify(path.join(root, 'strategy/replay'))});
    const reference = require(${JSON.stringify(path.join(root, 'strategies/noop-v2-reference'))});
    reference.createReferenceStrategy();
    reference.createExampleStrategy();
    require(${JSON.stringify(path.join(root, 'scripts/strategy-replay'))});
    process.stdout.write('import-safe');
  `;
  const output = execFileSync(process.execPath, ['-e', script], { cwd: os.tmpdir(), encoding: 'utf8', timeout: 10000 });
  assert.equal(output, 'import-safe');
});

test('runnable demo distinguishes all three Strategy-specific thresholds', () => {
  const cli = path.resolve(__dirname, '../scripts/strategy-replay.js');
  const output = JSON.parse(execFileSync(process.execPath, [cli], { encoding: 'utf8', timeout: 10000 }));
  assert.equal(output.mode, 'offline_preview');
  const states = Object.fromEntries(output.scenarios.map(row => [`${row.strategy}/${row.action}`, row.schedule[0].status]));
  assert.equal(states['v2-reference/sell_put'], 'waiting');
  assert.equal(states['example-alternative/sell_put'], 'ready_for_risk_checks');
  assert.equal(states['v2-reference/buyback_call'], 'waiting');
  assert.equal(states['example-alternative/buyback_call'], 'ready_for_risk_checks');
  assert.equal(states['v2-reference/sell_call'], 'ready_for_risk_checks');
  assert.equal(states['example-alternative/sell_call'], 'blocked');
});

test('CLI replays a captured-input file and rejects unknown command/input fields', () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-strategy-cli-'));
  try {
    const { createReplayFixture } = require('../strategies/noop-v2-reference/fixtures');
    const fixture = createReplayFixture({ action: 'sell_call' });
    const file = path.join(directory, 'input.json');
    const cli = path.resolve(__dirname, '../scripts/strategy-replay.js');
    fs.writeFileSync(file, JSON.stringify({ mandate: fixture.mandate, input_bundle: fixture.inputBundle }));
    const run = (...args) => execFileSync(process.execPath, [cli, ...args], { encoding: 'utf8', timeout: 10000, stdio: ['ignore', 'pipe', 'pipe'] });
    const result = JSON.parse(run('--input', file));
    assert.equal(result.acceptance, 'accepted');
    assert.equal(result.control_state.control_revision, 1);
    assert.equal(result.schedule[0].status, 'blocked');
    assert.match(result.schedule[0].reasons.join(' '), /projection.*context/i);
    assert.equal(result.evidence_mode, 'captured_input');
    for (const control_state of [false, 0, '', null]) {
      fs.writeFileSync(file, JSON.stringify({ mandate: fixture.mandate, input_bundle: fixture.inputBundle, control_state }));
      assert.throws(() => run('--input', file));
    }
    assert.throws(() => run('--execute'));
    fs.writeFileSync(file, JSON.stringify({ mandate: fixture.mandate, input_bundle: fixture.inputBundle, secret: 'unrecognized' }));
    assert.throws(() => run('--input', file));
    fs.writeFileSync(file, 'credential-looking-value');
    try { run('--input', file); assert.fail('Malformed input should fail'); }
    catch (error) { assert.doesNotMatch(String(error.stderr), /credential-looking-value/); }
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});

test('a separate producer evaluation bundle can supply fresh candidate projection evidence', () => {
  const { createReplayFixture } = require('../strategies/noop-v2-reference/fixtures');
  const { marginProjectionBinding } = require('../strategies/noop-v2-reference');
  const { contentDigest } = require('../strategy/canonical');
  const { replayOne } = require('../scripts/strategy-replay');
  const fixture = createReplayFixture({ action: 'sell_call' });
  const document = { mandate: fixture.mandate, input_bundle: fixture.inputBundle };
  const initial = replayOne(fixture.strategy, document);
  assert.equal(initial.schedule[0].status, 'blocked');
  const evaluation = JSON.parse(JSON.stringify(fixture.inputBundle));
  evaluation.control_revision = initial.control_state.control_revision;
  evaluation.private_state = { version: initial.control_state.private_state_version, content_ref: initial.control_state.private_state_ref };
  evaluation.active_intents = Object.values(initial.control_state.intents).map(record => ({
    intent_id: record.intent.intent_id, intent_revision: record.intent.intent_revision, status: record.status,
  }));
  // The test acts as a new synthetic producer. The CLI never updates captured
  // projection proofs on the caller's behalf.
  const intent = initial.decision.operations[0].intent;
  evaluation.extensions.reference_v2.margin_projections[intent.intent_id] = marginProjectionBinding(intent, evaluation);
  delete evaluation.input_bundle_id;
  evaluation.input_bundle_id = contentDigest(evaluation);
  const fresh = replayOne(fixture.strategy, { ...document, evaluation_bundle: evaluation });
  assert.equal(fresh.schedule[0].status, 'ready_for_risk_checks');
  const changed = JSON.parse(JSON.stringify(evaluation));
  changed.fields['account.reconciled'].observed_at = new Date(Date.parse(changed.fields['account.reconciled'].observed_at) - 1000).toISOString();
  delete changed.input_bundle_id;
  changed.input_bundle_id = contentDigest(changed);
  const invalid = replayOne(fixture.strategy, { ...document, evaluation_bundle: changed });
  assert.equal(invalid.schedule[0].status, 'blocked');
});
