'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { loadProduction, declaration, SCRIPT_SOURCE } = require('./helpers/load-production');

// The actual legacy database module initializes only this disposable database.
// script.js is never imported; its selected declarations run with explicit mocks.
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'noop-lesson-evidence-'));
const previousEnv = { DATA_DIR: process.env.DATA_DIR, NOOP_DB_PATH: process.env.NOOP_DB_PATH };
process.env.DATA_DIR = directory;
process.env.NOOP_DB_PATH = path.join(directory, 'fixture.db');
const store = require('../bot/db');
for (const [name, value] of Object.entries(previousEnv)) {
  if (value === undefined) delete process.env[name]; else process.env[name] = value;
}
test.after(() => { store.close(); fs.rmSync(directory, { recursive: true, force: true }); });
test.beforeEach(() => {
  store.db.exec(`DELETE FROM trade_lesson_evidence; DELETE FROM trade_lesson_revisions;
    DELETE FROM trade_lessons; DELETE FROM trade_reviews;
    INSERT INTO trade_lessons(id,lesson_key,title,lesson,status)
    VALUES(1,'short_call.exit_insurance','Exit insurance','Retain uncertainty when interpreting campaign results.','disputed');
    INSERT INTO trade_reviews(id,instrument_name,action_family,closed_at,order_ids,review_status,
      review_confidence,summary,lessons,review_window_days,pnl_realized,spot_open,spot_close)
    VALUES(1,'ETH-20261023-2500-C','short_call_campaign','2026-10-23T08:00:00Z','[1]','unclear',
      0.05,'Active low-confidence assessment','["Sizing remained uncertain"]',7,12.5,2400,2450),
      (2,'ETH-20261023-2600-C','short_call_campaign','2026-10-23T08:00:00Z','[2]','unclear',
      0.9,'INACTIVE_REVIEW_SENTINEL','[]',1,4,2400,2450),
      (3,'ETH-20261023-2700-C','short_call_campaign','2026-10-23T08:00:00Z','[3]','unclear',
      NULL,'Active assessment without confidence','[]',1,3,2400,2450);
    INSERT INTO trade_lesson_evidence(lesson_id,review_id,stance) VALUES(1,1,'contradicting'),(1,2,'contradicting');
    UPDATE trade_reviews SET is_active=0 WHERE id=2;`);
});

const formatters = loadProduction(['formatTradeReviewForLessonSynthesis', 'formatTradeLessonForPrompt']);

test('legacy review readers exclude inactive evidence while retaining active qualifications and historical links', () => {
  const rowsBefore = store.db.prepare('SELECT * FROM trade_reviews ORDER BY id').all();
  const linksBefore = store.db.prepare('SELECT * FROM trade_lesson_evidence ORDER BY review_id').all();
  const changesBefore = store.db.prepare('SELECT total_changes() AS n').get().n;
  for (const reviews of [store.getRecentTradeReviews(20), store.getTradeReviewsSinceId(0, 20)]) {
    assert.deepEqual(reviews.map(row => row.id).sort(), [1, 3]);
    assert.ok(reviews.every(row => row.is_active === 1));
    assert.equal(reviews.find(row => row.id === 1).review_confidence, 0.05);
    assert.equal(reviews.find(row => row.id === 3).review_confidence, null);
  }
  const [opposing] = store.getContradictingReviewsForLesson(1);
  assert.deepEqual(store.getContradictingReviewsForLesson(1).map(row => row.id), [1]);
  assert.equal(opposing.is_active, 1);
  assert.equal(opposing.review_confidence, 0.05);
  assert.equal(store.db.prepare('SELECT total_changes() AS n').get().n, changesBefore);
  assert.deepEqual(store.db.prepare('SELECT * FROM trade_reviews ORDER BY id').all(), rowsBefore);
  assert.deepEqual(store.db.prepare('SELECT * FROM trade_lesson_evidence ORDER BY review_id').all(), linksBefore);
});

test('explicit rejudging retains the existing cleanup semantics for inactive contradictory links', () => {
  const rowsBefore = store.db.prepare('SELECT * FROM trade_reviews ORDER BY id').all();
  assert.deepEqual(store.getContradictingReviewsForLesson(1).map(row => row.id), [1]);
  const revised = store.upsertCanonicalTradeLesson({
    lesson_key: 'short_call.exit_insurance', title: 'Exit insurance',
    lesson: 'The active campaign reviews now support this qualified assessment.',
    supporting_review_ids: [1, 3], contradicting_review_ids: [],
  }, new Set([1, 3]), { rejudgeContradictions: true });
  assert.equal(revised.status, 'active');
  assert.deepEqual(store.getContradictingReviewsForLesson(1), []);
  assert.deepEqual(store.db.prepare('SELECT review_id, stance FROM trade_lesson_evidence ORDER BY review_id').all(), [
    { review_id: 1, stance: 'supporting' }, { review_id: 3, stance: 'supporting' },
  ]);
  assert.deepEqual(store.db.prepare('SELECT * FROM trade_reviews ORDER BY id').all(), rowsBefore);
});

test('synthesis text retains campaign context and distinguishes low, zero and unknown confidence', () => {
  const format = formatters.formatTradeReviewForLessonSynthesis;
  const review = store.getContradictingReviewsForLesson(1)[0];
  const text = format(review);
  for (const value of ['#1', 'ETH-20261023-2500-C', '[unclear]', '[7d]', 'activity=active', 'confidence=0.05',
    'family=short_call_campaign', 'strike=2500', 'spot_open=2400', 'spot_close=2450', 'pnl=$12.50',
    'summary=Active low-confidence assessment', 'observations=Sizing remained uncertain']) {
    assert.ok(text.includes(value), `Missing campaign context: ${value}`);
  }
  assert.match(format({ ...review, review_confidence: 0 }), /confidence=0(?: |$)/);
  for (const confidence of [null, undefined, NaN, Infinity]) {
    assert.match(format({ ...review, review_confidence: confidence }), /confidence=unknown/);
  }
  assert.match(format({ ...review, is_active: 0 }), /activity=inactive/);
  assert.match(format({ ...review, is_active: undefined }), /activity=unknown/);
});

test('the actual synthesis prompt includes qualified active contradictions and excludes inactive review text', async () => {
  let prompt;
  const bindings = {
    ...formatters,
    db: store,
    botData: {},
    process: { env: { ANTHROPIC_API_KEY: 'test-only' } },
    persistCycleState() {},
    console: { log() {} },
    TRADE_LESSON_BOOTSTRAP_REVIEW_LIMIT: 20,
    TRADE_LESSON_SYNTHESIS_TIMEOUT_MS: 100,
    ANTHROPIC_SONNET_MODEL: 'test-only',
    axios: { async post(_url, payload) {
      prompt = payload.messages[0].content;
      throw new Error('Captured prompt without a model request');
    } },
  };
  const extract = vm.compileFunction(`let _tradeLessonInFlight = false;
    ${declaration(SCRIPT_SOURCE, 'TRADE_LESSON_TAXONOMY')}
    ${declaration(SCRIPT_SOURCE, 'extractTradeLessons')}
    return extractTradeLessons;`, Object.keys(bindings))(...Object.values(bindings));
  const result = await extract();
  assert.equal(result.error, 'Captured prompt without a model request');
  assert.match(prompt, /Reviewed campaigns[\s\S]*#1[^\n]*activity=active confidence=0\.05/);
  assert.match(prompt, /Open contradictions[\s\S]*#1[^\n]*activity=active confidence=0\.05/);
  assert.match(prompt, /#3[^\n]*activity=active confidence=unknown/);
  assert.doesNotMatch(prompt, /INACTIVE_REVIEW_SENTINEL|#2(?:\s|$)/);
  assert.match(prompt, /Stored linked counts may include historical or inactive reviews/);
  assert.match(prompt, /active review records and their qualifications are authoritative/);
  assert.match(prompt, /Preserve uncertainty for low or unknown confidence/);
  assert.match(prompt, /unknown means missing metadata, not zero confidence/);
  assert.equal(store.db.prepare('SELECT COUNT(*) AS n FROM trade_lesson_evidence').get().n, 2);
});
