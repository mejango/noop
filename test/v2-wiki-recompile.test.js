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
    const first = prompt.includes('\n- regimes/history.md (draft is ');
    if (first && !prompt.includes('## Your previous update was rejected')) seen.push(period);
    if (period === '2026-05-a' && failMay) throw new Error('529 overloaded');
    if (!first) return { text: '<no_updates/>', truncated: false };
    if (period === '2026-04-a') return { text: update('regimes/history.md', 'Fear to transition, 1 episode [2026-04-10].'), truncated: false };
    // May drops a required section: rejected, April's draft stays.
    if (period === '2026-05-a') return { text: `<wiki_update path="regimes/history.md">\n**TLDR**\n## Regime Transitions\nA long enough body that only lacks the Patterns section.\n</wiki_update>`, truncated: false };
    return { text: update('regimes/history.md', 'Fear to transition, 2 episodes [2026-04-10] [2026-11-01]. [review:#7]'), truncated: false };
  };
  const args = { stagingDir, sources, currentPages: { 'regimes/history.md': 'live page [review:#7]' }, schema: 'SCHEMA', callModel };
  await assert.rejects(recompile.runWikiRecompile(args), /529/);
  assert.ok(!fs.existsSync(path.join(stagingDir, 'done.json')));
  assert.match(recompile.readDrafts(stagingDir)['regimes/history.md'], /1 episode/);

  failMay = false;
  const summary = await recompile.runWikiRecompile(args);
  // April was not repeated; May was retried; the live pages were folded; May's rejected page was replayed.
  assert.deepEqual(seen, ['2026-04-a', '2026-05-a', '2026-05-a', recompile.CURRENT_PAGES_PERIOD, '2026-05-a']);
  assert.deepEqual(summary.periods, ['2026-04-a', '2026-05-a', recompile.CURRENT_PAGES_PERIOD]);
  assert.equal(summary.rejected.length, 1);
  assert.match(summary.rejected[0].reason, /missing sections: Patterns/);
  assert.equal(summary.rejected[0].replayed, 4);
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
  assert.ok(stage.includes('const runId = WIKI_RECOMPILE_RUN;'));
  assert.ok(!/writeFileSync\(path\.join\(WIKI_DIR/.test(stage), 'staging never writes a live page');
  const apply = script.slice(script.indexOf('const maybeApplyWikiRecompile'), script.indexOf('const saveWikiHistory'));
  assert.ok(apply.includes('const runId = WIKI_RECOMPILE_APPLY;'));
  // Nothing is applied until a later commit names the run.
  assert.ok(script.includes("const WIKI_RECOMPILE_APPLY = process.env.WIKI_RECOMPILE_APPLY || '';"));
  assert.ok(apply.indexOf('saveWikiHistory(pagePath') < apply.indexOf('fs.writeFileSync(path.join(WIKI_DIR, pagePath)'), 'history is saved before a page is replaced');
});

test('a cut-off response asks again, one page per call, for the pages it lost', async () => {
  const stagingDir = tmp();
  const calls = [];
  const callModel = async (prompt) => {
    const pages = recompile.RECOMPILE_PAGES.filter((page) => prompt.includes(`\n- ${page} (draft is `));
    calls.push(pages);
    if (pages.length === 2 && pages[0] === 'regimes/history.md') return { text: update(pages[0], 'First page arrived [2026-04-10].'), truncated: true };
    if (pages.length === 1) return { text: update(pages[0], 'Recovered alone [2026-04-10].'), truncated: false };
    return { text: '<no_updates/>', truncated: false };
  };
  await recompile.runWikiRecompile({ stagingDir, sources: [entry('2026-04-10T00:00:00Z', 'note')], currentPages: {}, schema: 'S', callModel });
  const drafts = recompile.readDrafts(stagingDir);
  assert.match(drafts['regimes/history.md'], /First page arrived/);
  assert.match(drafts[recompile.RECOMPILE_PAGES[1]], /Recovered alone/);
  // The page that arrived was not asked for again.
  assert.equal(calls.filter((pages) => pages.length === 1 && pages[0] === 'regimes/history.md').length, 0);
});

test('a rejected update gets corrected attempts on the rejected page before the period is dropped from the page', async () => {
  const stagingDir = tmp();
  const prompts = [];
  const callModel = async (prompt) => {
    if (!prompt.includes('\n- regimes/history.md (draft is ')) return { text: '<no_updates/>', truncated: false };
    prompts.push(prompt);
    if (prompt.includes('## Sources: the current live pages')) return { text: '<no_updates/>', truncated: false };
    if (!prompt.includes('## Your previous update was rejected')) {
      return { text: `<wiki_update path="regimes/history.md">\n**TLDR**\n## Regime Transitions\nA long enough body that only lacks the Patterns section.\n</wiki_update>`, truncated: false };
    }
    return { text: update('regimes/history.md', 'Corrected with both sections [2026-04-10].'), truncated: false };
  };
  const summary = await recompile.runWikiRecompile({ stagingDir, sources: [entry('2026-04-10T00:00:00Z', 'note')], currentPages: {}, schema: 'S', callModel });
  assert.equal(summary.rejected.length, 0);
  assert.match(recompile.readDrafts(stagingDir)['regimes/history.md'], /Corrected with both sections/);
  assert.match(prompts[1], /regimes\/history\.md: Replacement is missing sections: Patterns/);
  assert.match(prompts[0], /draft is \d+ words; return at most 3100, hard limit 3500/);
});

test('a page-and-period dropped by an earlier rejection is replayed once the periods are folded', async () => {
  const stagingDir = tmp();
  fs.writeFileSync(path.join(stagingDir, 'progress.json'), JSON.stringify({
    done: ['2026-04-a'], rejected: [{ period: '2026-04-a', pagePath: 'regimes/history.md', reason: 'over budget' }],
  }));
  const calls = [];
  const callModel = async (prompt) => {
    const period = /## Sources: period (\S+)/.exec(prompt)?.[1] || recompile.CURRENT_PAGES_PERIOD;
    calls.push(period);
    if (period !== '2026-04-a') return { text: '<no_updates/>', truncated: false };
    assert.ok(prompt.includes('\n- regimes/history.md (draft is ') && !prompt.includes('\n- protection/windows.md (draft is '));
    return { text: update('regimes/history.md', 'April folded on replay [2026-04-10].'), truncated: false };
  };
  const summary = await recompile.runWikiRecompile({ stagingDir, sources: [entry('2026-04-10T00:00:00Z', 'note')], currentPages: {}, schema: 'S', callModel });
  assert.deepEqual(summary.rejected, []);
  assert.equal(calls.filter((period) => period === '2026-04-a').length, 1);
  assert.equal(recompile.hasUnreplayedGaps(summary), false);
  // A finished run that still lists a dropped period is picked up again, once.
  assert.equal(recompile.hasUnreplayedGaps({ rejected: [{ period: '2026-04-a', pagePath: 'regimes/history.md' }] }), true);
  assert.equal(recompile.hasUnreplayedGaps({ rejected: [{ period: '2026-04-a', pagePath: 'regimes/history.md', replayed: 4 }] }), false);
  // A gap replayed by the earlier, weaker replay is tried again.
  assert.equal(recompile.hasUnreplayedGaps({ rejected: [{ period: '2026-04-a', pagePath: 'regimes/history.md', replayed: 2 }] }), true);
  assert.match(recompile.readDrafts(stagingDir)['regimes/history.md'], /April folded on replay/);
});

test('an over-budget fold is kept and consolidated on its own; a failed consolidation never drops the period', async () => {
  const long = (n) => compiled('regimes/history.md', Array.from({ length: n }, (_, i) => `w${i}${i % 50 === 49 ? '\n' : ''}`).join(' '));
  const run = async (consolidated) => {
    const stagingDir = tmp();
    const consolidations = [];
    const logs = [];
    const callModel = async (prompt) => {
      if (prompt.startsWith('This wiki page')) {
        consolidations.push(prompt);
        assert.ok(!prompt.includes('SOURCE-NOTE'), 'consolidation carries only the page');
        return { text: consolidated(consolidations.length), truncated: false };
      }
      if (!prompt.includes('\n- regimes/history.md (draft is ') || prompt.includes('## Sources: the current live pages')) return { text: '<no_updates/>', truncated: false };
      return { text: `<wiki_update path="regimes/history.md">\n${long(4400)}\n</wiki_update>`, truncated: false };
    };
    const summary = await recompile.runWikiRecompile({ stagingDir, sources: [entry('2026-04-10T00:00:00Z', 'SOURCE-NOTE')], currentPages: {}, schema: 'S', callModel, log: (line) => logs.push(line) });
    return { summary, consolidations, logs, draft: recompile.readDrafts(stagingDir)['regimes/history.md'] };
  };
  // The first consolidation is still too long; the second fits.
  const fits = await run((n) => `<wiki_update path="regimes/history.md">\n${long(n === 1 ? 3700 : 2900)}\n</wiki_update>`);
  assert.deepEqual(fits.summary.rejected, []);
  assert.equal(fits.consolidations.length, 2);
  assert.match(fits.consolidations[0], /is 44\d\d words by the counter that enforces its 3500-word limit.* Rewrite it about 3\d% shorter \(at most 3100 words\)/);
  assert.ok(fits.draft.includes('w2899') && !fits.draft.includes('w2900'));
  assert.ok(fits.logs.some((line) => /consolidate regimes\/history\.md \(44\d\d words\): 37\d\d words/.test(line)));
  // The model never returns a page: the long draft stays, the period is not dropped.
  const stuck = await run(() => 'I cannot do that.');
  assert.deepEqual(stuck.summary.rejected, []);
  assert.equal(stuck.consolidations.length, 3);
  assert.ok(stuck.draft.includes('w4399'));
  assert.ok(stuck.logs.some((line) => line.includes('no page in the response')));
});
