'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const recompile = require('../bot/wiki-recompile');
const { WIKI_EXPECTED_HEADERS, getWikiPageRuleErrors } = require('../bot/wiki-rules');

const root = path.resolve(__dirname, '..');
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'wiki-recompile-'));
const compiled = (pagePath, body) => recompile.skeletonPage(pagePath)
  .replace('**Not yet compiled.**', '**History shows a pattern.**')
  .replace('Nothing compiled yet.', body);
const update = (pagePath, body) => `<wiki_update path="${pagePath}">\n${compiled(pagePath, body)}\n</wiki_update>`;
const entry = (timestamp, content, extra = {}) => ({ timestamp, entry_type: 'observation', content, ...extra });

test('recompile covers exactly the research pages that are not current-state pages', () => {
  const research = Object.keys(WIKI_EXPECTED_HEADERS).filter((page) => !page.startsWith('strategy/'));
  const assessment = ['regimes/current.md', 'protection/pricing.md', 'revenue/pricing.md'];
  assert.deepEqual([...recompile.RECOMPILE_PAGES].sort(), research.filter((page) => !assessment.includes(page)).sort());
  // The dashboard's comparison route lists the same pages.
  const route = fs.readFileSync(path.join(root, 'dashboard/src/app/api/wiki/recompile/route.ts'), 'utf8');
  for (const page of recompile.RECOMPILE_PAGES) assert.ok(route.includes(`'${page}'`), page);
});

test('every skeleton satisfies the page rules', () => {
  for (const page of recompile.RECOMPILE_PAGES) {
    assert.deepEqual(getWikiPageRuleErrors({ pagePath: page, previousContent: '', replacementContent: recompile.skeletonPage(page) }), [], page);
  }
});

test('sources are folded oldest first in half-month periods, split when a period is too large', () => {
  const chunks = recompile.chunkSources([
    entry('2026-05-20T00:00:00Z', 'late may'),
    entry('2026-04-10T00:00:00Z', 'april'),
    entry('2026-05-03T00:00:00Z', 'early may', { entry_type: 'hypothesis', outcome_verdict: 'confirmed_convex' }),
    entry('not a date', 'ignored'),
  ]);
  assert.deepEqual(chunks.map((chunk) => chunk.label), ['2026-04-a', '2026-05-a', '2026-05-b']);
  assert.match(chunks[1].text, /2026-05-03 \| hypothesis \| reviewed outcome: confirmed_convex/);
  const big = recompile.chunkSources([entry('2026-06-01T00:00:00Z', 'x'.repeat(60)), entry('2026-06-02T00:00:00Z', 'y'.repeat(60))], 100);
  assert.deepEqual(big.map((chunk) => chunk.label), ['2026-06-a-1', '2026-06-a-2']);
});

test('a run folds each period, keeps a draft when an update breaks a rule, and resumes after a failure', async () => {
  const stagingDir = tmp();
  const sources = [entry('2026-04-10T00:00:00Z', 'april note'), entry('2026-05-03T00:00:00Z', 'may note')];
  const seen = [];
  let failMay = true;
  const callModel = async (prompt) => {
    const period = /## Sources: period (\S+)/.exec(prompt)?.[1] || recompile.CURRENT_PAGES_PERIOD;
    const first = prompt.includes('\n- regimes/history.md\n');
    if (first) seen.push(period);
    if (period === '2026-05-a' && failMay) throw new Error('529 overloaded');
    if (!first) return '<no_updates/>';
    if (period === '2026-04-a') return update('regimes/history.md', 'Fear to transition, 1 episode [2026-04-10].');
    // May drops a required section: rejected, April's draft stays.
    if (period === '2026-05-a') return `<wiki_update path="regimes/history.md">\n**TLDR**\n## Regime Transitions\nA long enough body that only lacks the Patterns section.\n</wiki_update>`;
    return update('regimes/history.md', 'Fear to transition, 2 episodes [2026-04-10] [2026-11-01]. [review:#7]');
  };
  const args = { stagingDir, sources, currentPages: { 'regimes/history.md': 'live page [review:#7]' }, schema: 'SCHEMA', callModel };
  await assert.rejects(recompile.runWikiRecompile(args), /529/);
  assert.ok(!fs.existsSync(path.join(stagingDir, 'done.json')));
  assert.match(recompile.readDrafts(stagingDir)['regimes/history.md'], /1 episode/);

  failMay = false;
  const summary = await recompile.runWikiRecompile(args);
  // April was not repeated; May was retried; the live pages were folded last.
  assert.deepEqual(seen, ['2026-04-a', '2026-05-a', '2026-05-a', recompile.CURRENT_PAGES_PERIOD]);
  assert.deepEqual(summary.periods, ['2026-04-a', '2026-05-a', recompile.CURRENT_PAGES_PERIOD]);
  assert.equal(summary.rejected.length, 1);
  assert.match(summary.rejected[0].reason, /missing sections: Patterns/);
  assert.match(recompile.readDrafts(stagingDir)['regimes/history.md'], /2 episodes/);
  // A completed run is not repeated.
  await recompile.runWikiRecompile({ ...args, callModel: async () => { throw new Error('must not be called'); } });
});

test('apply takes only complete, rule-abiding drafts whose markers exist', () => {
  const getMarkers = (content) => new Set(content.match(/\[review:#\d+\]/g) || []);
  const staged = Object.fromEntries(recompile.RECOMPILE_PAGES.map((page) => [page, recompile.skeletonPage(page)]));
  staged['regimes/history.md'] = compiled('regimes/history.md', 'Two episodes. [review:#7]');
  staged['indicators/leading.md'] = compiled('indicators/leading.md', 'Invented. [review:#999]');
  staged['indicators/correlations.md'] = compiled('indicators/correlations.md', 'Same as live.');
  const live = { 'indicators/correlations.md': staged['indicators/correlations.md'] };
  const { apply, skipped } = recompile.selectRecompileApplicable({ staged, live, allowedMarkers: new Set(['[review:#7]']), getMarkers });
  assert.deepEqual(apply, ['regimes/history.md']);
  const reasons = Object.fromEntries(skipped.map((item) => [item.pagePath, item.reason]));
  assert.match(reasons['indicators/leading.md'], /unknown source marker\(s\): \[review:#999\]/);
  assert.equal(reasons['indicators/correlations.md'], 'identical to live page');
  assert.equal(reasons['protection/windows.md'], 'no compiled draft');
});

test('the bot stages in the background and applies only on its own explicit trigger', () => {
  const script = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
  const stage = script.slice(script.indexOf('const maybeStageWikiRecompile'), script.indexOf('const maybeApplyWikiRecompile'));
  assert.ok(stage.includes('process.env.WIKI_RECOMPILE_RUN'));
  assert.ok(!/writeFileSync\(path\.join\(WIKI_DIR/.test(stage), 'staging never writes a live page');
  const apply = script.slice(script.indexOf('const maybeApplyWikiRecompile'), script.indexOf('const saveWikiHistory'));
  assert.ok(apply.includes('process.env.WIKI_RECOMPILE_APPLY'));
  assert.ok(apply.indexOf('saveWikiHistory(pagePath') < apply.indexOf('fs.writeFileSync(path.join(WIKI_DIR, pagePath)'), 'history is saved before a page is replaced');
});
