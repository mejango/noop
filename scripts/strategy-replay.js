#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const { canonicalize, contentDigest } = require('../strategy/canonical');
const { createControlState, acceptDecision, previewSchedule } = require('../strategy/replay');
const { createReferenceStrategy, createExampleStrategy, marginProjectionBinding } = require('../strategies/noop-v2-reference');
const { createReplayFixture } = require('../strategies/noop-v2-reference/fixtures');

function replayOne(strategy, { mandate, input_bundle: inputBundle, control_state: controlState, evaluation_bundle: evaluationBundle }, { syntheticMarginEvidence = false } = {}) {
  const state = controlState === undefined ? createControlState(mandate, inputBundle.private_state.version) : controlState;
  const decision = strategy.generateDecision(inputBundle, { mandate });
  const context = { release: strategy.release, mandate, inputBundle,
    catalog: strategy.fieldCatalog, validateEconomicPolicy: strategy.validateEconomicPolicy };
  const accepted = acceptDecision({ ...context, state, decision });
  const fresh = JSON.parse(canonicalize(evaluationBundle === undefined ? inputBundle : evaluationBundle));
  if (evaluationBundle === undefined) {
    fresh.control_revision = accepted.state.control_revision;
    fresh.private_state = { version: accepted.state.private_state_version,
      content_ref: accepted.state.private_state_ref };
    fresh.active_intents = Object.values(accepted.state.intents).map(record => ({
      intent_id: record.intent.intent_id, intent_revision: record.intent.intent_revision, status: record.status,
    }));
    // This path is used exclusively by the labelled synthetic demo. Captured
    // files keep their original projection evidence and require a new producer
    // snapshot when the accepted control/commitment context changes.
    if (syntheticMarginEvidence) {
      for (const record of Object.values(accepted.state.intents)) {
        if (record.intent.action === 'sell_call') {
          fresh.extensions.reference_v2.margin_projections[record.intent.intent_id] = marginProjectionBinding(record.intent, fresh);
        }
      }
    }
    delete fresh.input_bundle_id;
    fresh.input_bundle_id = contentDigest(fresh);
  }
  const schedule = previewSchedule({ ...context, state: accepted.state, inputBundle: fresh, now: fresh?.evaluated_at });
  return {
    mode: 'offline_preview', evidence_mode: syntheticMarginEvidence ? 'synthetic_demo' : 'captured_input', policy: strategy.reportPolicy({ mandate }),
    decision, acceptance: accepted.status, control_state: accepted.state, schedule,
  };
}

function demo() {
  const scenarios = [];
  for (const [name, strategy] of [['v2-reference', createReferenceStrategy()], ['example-alternative', createExampleStrategy()]]) {
    for (const action of ['sell_put', 'buyback_call', 'sell_call']) {
      const fixture = createReplayFixture({ strategy, action });
      const result = replayOne(strategy, { mandate: fixture.mandate, input_bundle: fixture.inputBundle }, { syntheticMarginEvidence: true });
      scenarios.push({ strategy: name, action, acceptance: result.acceptance, schedule: result.schedule });
    }
  }
  return {
    mode: 'offline_preview',
    evidence: 'Independent synthetic scenarios: 30 DTE, 75% gross call capture, 35% V2 displayed margin. These are not historical fills or performance claims.',
    policies: {
      reference: createReferenceStrategy().reportPolicy(),
      example: createExampleStrategy().reportPolicy(),
    },
    scenarios,
    execution: 'No reservations, live venue checks, order submission, database, or signing. ready_for_risk_checks is only a scheduling preview.',
  };
}

function main(args = process.argv.slice(2)) {
  let inputPath;
  let strategyName = 'reference';
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === '--help') {
      process.stdout.write('Usage: node scripts/strategy-replay.js [--input replay.json --strategy reference|example]\nNo arguments runs the synthetic comparison. This command cannot submit trades.\n');
      return;
    }
    if ((arg === '--input' || arg === '--strategy') && args[index + 1] && !args[index + 1].startsWith('--')) {
      if (arg === '--input') {
        if (inputPath) throw new Error('Duplicate --input');
        inputPath = args[++index];
      } else strategyName = args[++index];
    } else throw new Error('Unknown argument or missing value; use --help');
  }
  if (!['reference', 'example'].includes(strategyName)) throw new Error('--strategy must be reference or example');
  if (!inputPath) {
    process.stdout.write(JSON.stringify(demo(), null, 2) + '\n');
    return;
  }
  const fd = fs.openSync(inputPath, fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
  let bytes;
  try {
    const metadata = fs.fstatSync(fd);
    if (!metadata.isFile() || metadata.size > 4 * 1024 * 1024) throw new Error('Replay input must be a regular file of at most 4 MiB');
    // Bounded read even if another process grows the file after fstat.
    const buffer = Buffer.alloc(4 * 1024 * 1024 + 1);
    let count = 0;
    while (count < buffer.length) {
      const amount = fs.readSync(fd, buffer, count, buffer.length - count, null);
      if (amount === 0) break;
      count += amount;
    }
    if (count === buffer.length) throw new Error('Replay input exceeds 4 MiB');
    try { bytes = new TextDecoder('utf-8', { fatal: true }).decode(buffer.subarray(0, count)); }
    catch { throw new Error('Replay input must be valid UTF-8'); }
  } finally { fs.closeSync(fd); }
  let document;
  try { document = JSON.parse(bytes); } catch { throw new Error('Replay input is not valid JSON'); }
  canonicalize(document);
  if (!document || typeof document !== 'object' || Array.isArray(document)
      || !document.mandate || !document.input_bundle
      || Object.keys(document).some(key => !['mandate', 'input_bundle', 'control_state', 'evaluation_bundle'].includes(key))) {
    throw new Error('Replay input requires {mandate,input_bundle,control_state?,evaluation_bundle?}');
  }
  const strategy = strategyName === 'reference' ? createReferenceStrategy() : createExampleStrategy();
  process.stdout.write(JSON.stringify(replayOne(strategy, document), null, 2) + '\n');
}

if (require.main === module) {
  try { main(); } catch (error) {
    process.stderr.write(`Strategy replay failed: ${error.message}\n`);
    process.exitCode = 1;
  }
}

module.exports = { replayOne, demo, main };
