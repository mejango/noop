'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const vm = require('node:vm');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { compareAdvisorySnapshots, runReviewedPublication } = require('../bot/advisory-publication');
const { summarizeAdvisoryQuotes } = require('../bot/advisory-quotes');
const { declaration, SCRIPT_SOURCE } = require('./helpers/load-production');

const put = 'ETH-20261127-1600-P';
const call = 'ETH-20260925-2800-C';
function snapshot({ ask = 20, putName = put, minute = 0, positions = [] } = {}) {
  const nowMs = Date.parse('2026-09-15T18:55:00.000Z') + minute * 60000;
  const marketTimestamp = new Date(nowMs).toISOString();
  const instruments = [putName, call].map(instrument_name => ({ instrument_name }));
  const tickerMap = {
    [putName]: { a: ask, b: ask > 0 ? 18 : 0, M: 19, option_pricing: { d: -0.06 }, quote_received_at: marketTimestamp },
    [call]: { a: 6, b: 5, M: 5.5, option_pricing: { d: 0.08 }, quote_received_at: marketTimestamp },
  };
  return { spotPrice: 2367.7, marketTimestamp, positions, instruments, tickerMap,
    quoteAvailability: summarizeAdvisoryQuotes(tickerMap, { nowMs, inputTimestamp: marketTimestamp, expectedInstruments: instruments }) };
}

function productionFlow(reads, { storageFailure = false, notificationFailure = null,
  retryCount = 0, missingApiKey = false, initiallyInFlight = false } = {}) {
  const reviews = [], publications = [], saves = [], notifications = [], events = [];
  let snapshotReads = 0;
  const botData = { lastAdvisorySuccess: 1, advisoryRetryCount: retryCount };
  const bindings = {
    process: { env: missingApiKey ? {} : { ANTHROPIC_API_KEY: 'fixture' } },
    console: { log() {} }, botData,
    sendTelegram: message => {
      notifications.push(message);
      events.push('notification');
      if (notificationFailure === 'throw') throw new Error('fixture Telegram synchronous failure');
      if (notificationFailure === 'reject') return Promise.reject(new Error('fixture Telegram rejected'));
      if (notificationFailure === 'pending') return new Promise(() => {});
      return Promise.resolve();
    },
    persistCycleState: () => saves.push(structuredClone(botData)),
    getAdvisoryRetryDelayMs: n => n * 60000,
    selectAssessmentText: (a, b) => a || b,
    runReviewedPublication,
    readFreshTradingAdvisorySnapshot: async () => {
      snapshotReads++;
      const value = reads.shift();
      if (value instanceof Error) throw value;
      assert.ok(value, 'fixture supplies every refresh');
      return value;
    },
    buildTradingAdvisoryDraft: async (input, advisoryId) => {
      events.push('review');
      reviews.push(input);
      const assessment = `PUT quote status ${input.quoteAvailability.put.status}`;
      return { advisoryId, allRules: [{ action: 'buy_put' }], finalAgenda: { assessment }, primaryAgenda: { assessment },
        secondOpinion: { stance: assessment }, mandelbrotContext: null,
        rollingOptionValueContext: { put_value_context: { availability: input.quoteAvailability.put } },
        persistedAgenda: { entry_rules: [{}], exit_rules: [] }, spotPrice: input.spotPrice };
    },
    db: { publishAdvisory: (...args) => {
      if (storageFailure) throw new Error('fixture transaction failed');
      publications.push(args);
    } },
  };
  const source = `let _advisoryInFlight = ${Boolean(initiallyInFlight)};\n${declaration(SCRIPT_SOURCE, 'formatAdvisoryRunNotification')}\n${declaration(SCRIPT_SOURCE, 'notifyAdvisoryRun')}\n${declaration(SCRIPT_SOURCE, 'publishTradingAdvisoryDraft')}\n${declaration(SCRIPT_SOURCE, 'generateTradingAdvisory')}\nreturn { run: generateTradingAdvisory, inFlight: () => _advisoryInFlight };`;
  return { ...vm.compileFunction(source, Object.keys(bindings))(...Object.values(bindings)),
    botData, reviews, publications, saves, notifications, events, snapshotReads: () => snapshotReads };
}

test('actual advisory wrapper publishes once with separate input and final-check clocks', async () => {
  const initial = snapshot(), checked = snapshot({ minute: 4, ask: 21 });
  const f = productionFlow([initial, checked]);
  const result = await f.run();
  assert.equal(result.rulesCount, 1);
  assert.equal(f.reviews.length, 1, 'Ordinary price drift does not rerun the LLMs');
  assert.equal(f.publications.length, 1);
  const entries = f.publications[0][2];
  assert.match(entries.find(e => e.entry_type === 'advisory_main').content, /Market inputs as of .*18:55:00.*rechecked .*18:59:00/);
  const context = JSON.parse(entries.find(e => e.entry_type === 'advisory_context').content);
  assert.equal(context.publication.input_as_of, initial.marketTimestamp);
  assert.equal(context.publication.quote_state_checked_at, checked.marketTimestamp);
  assert.equal(context.publication.review_attempts, 1);
  assert.equal(f.botData.lastAdvisoryError, null);
  assert.equal(f.botData.advisoryRetryCount, 0);
  assert.ok(f.botData.lastAdvisorySuccess > 1);
  assert.equal(f.inFlight(), false);
  assert.equal(f.notifications.length, 1);
  assert.match(f.notifications[0], /ADVISORY STARTED/);
  assert.match(f.notifications[0], /Normal schedule/);
  assert.match(f.notifications[0], /Full AI review: 1/);
  assert.ok(f.notifications[0].includes(`Run: \`${result.advisoryId}\``));
  assert.deepEqual(f.events, ['notification', 'review'], 'Notify immediately before starting model work');
});

test('quotes recovering during review rerun the full draft and publish only the refreshed assessment', async () => {
  const f = productionFlow([snapshot({ ask: 0 }), snapshot({ minute: 3 }), snapshot({ minute: 7 })]);
  await f.run();
  assert.equal(f.reviews.length, 2);
  assert.equal(f.reviews[0].quoteAvailability.put.status, 'quotes_unavailable');
  assert.equal(f.reviews[1].quoteAvailability.put.status, 'available');
  assert.equal(f.publications.length, 1);
  const entries = f.publications[0][2];
  assert.match(entries.find(e => e.entry_type === 'advisory_main').content, /PUT quote status available/);
  assert.doesNotMatch(entries.find(e => e.entry_type === 'advisory_main').content, /quotes_unavailable/);
  assert.equal(JSON.parse(entries.find(e => e.entry_type === 'advisory_context').content).publication.review_attempts, 2);
  assert.equal(f.notifications.length, 2);
  assert.match(f.notifications[0], /ADVISORY STARTED/);
  assert.match(f.notifications[1], /ADVISORY EXTRA REVIEW/);
  assert.match(f.notifications[1], /Additional review: market data or positions changed/);
  assert.match(f.notifications[1], /Full AI review: 2/);
  const runLine = text => text.split('\n').find(line => line.startsWith('Run:'));
  assert.equal(runLine(f.notifications[0]), runLine(f.notifications[1]), 'Extra model work belongs to the same advisory run');
  assert.deepEqual(f.events, ['notification', 'review', 'notification', 'review']);
});

test('second quote change aborts without writing rules or assessment and schedules the existing retry', async () => {
  const f = productionFlow([snapshot({ ask: 0 }), snapshot({ minute: 3 }), snapshot({ minute: 7, ask: 0 })]);
  await assert.rejects(f.run(), { code: 'ADVISORY_PUBLICATION_STALE' });
  assert.equal(f.reviews.length, 2);
  assert.equal(f.publications.length, 0);
  assert.equal(f.botData.lastAdvisorySuccess, 1);
  assert.equal(f.botData.advisoryRetryCount, 1);
  assert.ok(f.botData.nextAdvisoryRetryAt > Date.now());
  assert.equal(f.inFlight(), false);
});

test('initial and final refresh errors preserve successful state and release the advisory mutex', async () => {
  for (const reads of [[new Error('refresh failed')], [snapshot(), new Error('refresh failed')]]) {
    const expectedNotifications = reads.length - 1;
    const f = productionFlow(reads);
    await assert.rejects(f.run(), /refresh failed/);
    assert.equal(f.publications.length, 0);
    assert.equal(f.botData.lastAdvisorySuccess, 1);
    assert.equal(f.botData.advisoryRetryCount, 1);
    assert.equal(f.inFlight(), false);
    assert.equal(f.notifications.length, expectedNotifications, 'No notification before a valid initial snapshot');
  }
});

test('actual retry run announces the failed or deferred advisory and prior failure count', async () => {
  const f = productionFlow([snapshot(), snapshot({ minute: 4 })], { retryCount: 2 });
  await f.run({ trigger: 'retry' });
  assert.equal(f.notifications.length, 1);
  assert.match(f.notifications[0], /ADVISORY STARTED/);
  assert.match(f.notifications[0], /Retry after failed or deferred advisory \(2 prior failures\)/);
  assert.doesNotMatch(f.notifications[0], /Normal schedule|ADVISORY EXTRA REVIEW/);
  assert.equal(f.publications.length, 1);
});

test('missing API key and an occupied advisory mutex skip notifications, reads and model work', async () => {
  for (const options of [{ missingApiKey: true }, { initiallyInFlight: true }]) {
    const f = productionFlow([], options);
    assert.equal(await f.run(), null);
    assert.equal(f.notifications.length, 0);
    assert.equal(f.snapshotReads(), 0);
    assert.equal(f.reviews.length, 0);
    assert.equal(f.publications.length, 0);
    assert.equal(f.botData.lastAdvisorySuccess, 1);
    assert.equal(f.botData.advisoryRetryCount, 0);
  }
});

test('Telegram exceptions, rejected delivery and slow delivery cannot prevent successful publication', async () => {
  for (const notificationFailure of ['throw', 'reject', 'pending']) {
    const f = productionFlow([snapshot(), snapshot({ minute: 4 })], { notificationFailure });
    const result = await f.run();
    assert.equal(result.rulesCount, 1, notificationFailure);
    assert.equal(f.notifications.length, 1);
    assert.equal(f.publications.length, 1);
    assert.equal(f.botData.lastAdvisoryError, null);
    assert.equal(f.botData.advisoryRetryCount, 0);
    assert.equal(f.inFlight(), false);
  }
});

test('publication storage failure is propagated, never reported as advisory success', async () => {
  const f = productionFlow([snapshot(), snapshot({ minute: 4 })], { storageFailure: true });
  await assert.rejects(f.run(), /transaction failed/);
  assert.equal(f.botData.lastAdvisorySuccess, 1);
  assert.equal(f.botData.advisoryRetryCount, 1);
  assert.equal(f.inFlight(), false);
});

test('same-count contract replacement and changed position quantity or cost basis require another review', async () => {
  assert.equal(compareAdvisorySnapshots(snapshot(), snapshot({ putName: 'ETH-20261127-1800-P' })).fresh, false);
  const position = { instrument_name: put, direction: 'long', amount: 1, average_price: 20 };
  for (const change of [{ amount: 2 }, { average_price: 21 }, { direction: 'short' }]) {
    const a = snapshot({ positions: [position] });
    const b = snapshot({ positions: [{ ...position, ...change }] });
    const f = productionFlow([a, b, b]);
    await f.run();
    assert.equal(f.reviews.length, 2);
    assert.equal(f.publications.length, 1);
  }
});

test('coverage changes at fixed candidate counts and missing identities fail freshness', () => {
  const a = snapshot(), b = snapshot();
  b.quoteAvailability.put.coverage_status = 'partial';
  b.quoteAvailability.put.missing_expected_instruments = ['ETH-20261127-1800-P'];
  assert.equal(compareAdvisorySnapshots(a, b).fresh, false);
  delete b.quoteAvailability.put.quoted_instruments;
  assert.throws(() => compareAdvisorySnapshots(a, b), { code: 'ADVISORY_SNAPSHOT_INVALID' });
});

test('no review or publish occurs for an invalid snapshot, and no publish for an invalid draft', async () => {
  let reviews = 0, published = 0;
  const callbacks = { review: async () => { reviews++; return {}; }, publish: () => published++ };
  await assert.rejects(runReviewedPublication({ ...callbacks, readSnapshot: async () => ({}) }), { code: 'ADVISORY_SNAPSHOT_INVALID' });
  assert.equal(reviews, 0);
  await assert.rejects(runReviewedPublication({ ...callbacks, readSnapshot: async () => snapshot(), review: async () => null }), /no publishable draft/);
  assert.equal(published, 0);
});

test('real database publication rolls back rule replacement and all journal entries together', t => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-advisory-atomic-'));
  const priorDataDir = process.env.DATA_DIR, priorDbPath = process.env.NOOP_DB_PATH;
  process.env.DATA_DIR = dir;
  process.env.NOOP_DB_PATH = path.join(dir, 'fixture.db');
  const store = require('../bot/db');
  if (priorDataDir === undefined) delete process.env.DATA_DIR; else process.env.DATA_DIR = priorDataDir;
  if (priorDbPath === undefined) delete process.env.NOOP_DB_PATH; else process.env.NOOP_DB_PATH = priorDbPath;
  t.after(() => { store.close(); fs.rmSync(dir, { recursive: true, force: true }); });
  const rule = { rule_type: 'entry', action: 'buy_put', criteria: { target_score: 0.0036 } };
  store.replaceActiveRules('old', [rule]);
  store.insertJournalEntry('advisory_main', 'old assessment');
  const beforeRules = store.db.prepare('SELECT * FROM trading_rules ORDER BY id').all();
  const beforeJournal = store.db.prepare('SELECT * FROM ai_journal ORDER BY id').all();
  const entries = [{ entry_type: 'advisory', content: 'new assessment' }, { entry_type: 'advisory_main', content: 'new assessment' }];
  store.db.exec("CREATE TRIGGER fixture_failure BEFORE INSERT ON ai_journal WHEN NEW.entry_type = 'advisory_main' BEGIN SELECT RAISE(ABORT, 'fixture journal failure'); END");
  assert.throws(() => store.publishAdvisory('new', [rule], entries), /fixture journal failure/);
  assert.deepEqual(store.db.prepare('SELECT * FROM trading_rules ORDER BY id').all(), beforeRules);
  assert.deepEqual(store.db.prepare('SELECT * FROM ai_journal ORDER BY id').all(), beforeJournal);
  store.db.exec('DROP TRIGGER fixture_failure');
  store.publishAdvisory('new', [rule], entries);
  assert.equal(store.db.prepare('SELECT advisory_id FROM trading_rules WHERE is_active = 1').get().advisory_id, 'new');
  assert.equal(store.db.prepare("SELECT content FROM ai_journal WHERE entry_type = 'advisory_main' ORDER BY id DESC LIMIT 1").get().content, 'new assessment');
});
