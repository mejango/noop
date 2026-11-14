'use strict';
// The wiki page rules have one owner (bot/wiki-rules.js). The bot's automatic
// ingest and the dashboard's manual repair must both call it, and advisors
// must read every page in full.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { WIKI_EXPECTED_HEADERS, WIKI_PAGE_WORD_BUDGET, WIKI_HISTORY_PAGE_WORD_BUDGET, WIKI_HISTORY_PAGES, getWikiPageWordBudget, REFERENCE_ONLY_SECTIONS, getWikiPageRuleErrors } = require('../bot/wiki-rules');
const { loadProduction } = require('./helpers/load-production');

const root = path.resolve(__dirname, '..');
const page = (pagePath, extra = '') => [
  '**TLDR**',
  ...WIKI_EXPECTED_HEADERS[pagePath].map((heading) => `## ${heading}\n${REFERENCE_ONLY_SECTIONS[pagePath]?.[heading] || 'body'}`),
  extra,
].join('\n');
const errorsFor = (pagePath, previousContent, replacementContent) => getWikiPageRuleErrors({ pagePath, previousContent, replacementContent });

test('a complete page passes as its own rewrite, for every page', () => {
  for (const pagePath of Object.keys(WIKI_EXPECTED_HEADERS)) {
    assert.deepEqual(errorsFor(pagePath, page(pagePath), page(pagePath)), [], pagePath);
  }
});

test('a rewrite that drops any schema section is rejected, not only the first one or two', () => {
  const full = page('regimes/current.md');
  const errors = errorsFor('regimes/current.md', full, full.replace('## Falsification', '## Notes'));
  assert.deepEqual(errors, ['Replacement is missing sections: Falsification']);
  // A heading mentioned in prose is not the section.
  assert.equal(errorsFor('regimes/history.md', '', '**TLDR**\n## Regime Transitions\nsee Patterns below').length, 1);
});

test('reference-only sections must keep their canonical text', () => {
  const full = page('revenue/pricing.md');
  const errors = errorsFor('revenue/pricing.md', full, full.replace('Current skew and IV readings are perishable.', 'Skew is 12.6 today.'));
  assert.equal(errors.length, 1);
  assert.match(errors[0], /^Skew & IV Context section must contain only:/);
});

test('a page cannot grow past the word budget, but an oversized page is never frozen', () => {
  const words = (count) => Array(count).fill('word').join(' ');
  const base = page('regimes/history.md');
  const under = `${base}\n${words(WIKI_HISTORY_PAGE_WORD_BUDGET - 100)}`;
  const over = `${base}\n${words(WIKI_HISTORY_PAGE_WORD_BUDGET + 200)}`;
  assert.match(errorsFor('regimes/history.md', under, over)[0], /grows the page past the 3500-word budget/);
  // Only the pages that compile the whole history get the larger budget.
  assert.equal(getWikiPageWordBudget('regimes/current.md'), WIKI_PAGE_WORD_BUDGET);
  assert.equal(getWikiPageWordBudget('strategy/lessons.md'), 2500);
  assert.equal(WIKI_HISTORY_PAGES.length, 8);
  const current = page('regimes/current.md');
  assert.match(errorsFor('regimes/current.md', current, `${current}\n${words(2600)}`)[0], /grows the page past the 2500-word budget/);
  // Already over budget: the same size or smaller is accepted, larger is not.
  assert.deepEqual(errorsFor('regimes/history.md', over, over), []);
  assert.deepEqual(errorsFor('regimes/history.md', over, under), []);
  assert.equal(errorsFor('regimes/history.md', over, `${over} ${words(5)}`).length, 1);
});

test('strategy pages may not gain tick evidence', () => {
  const previous = page('strategy/playbook.md', 'old [tick:#10]');
  assert.deepEqual(errorsFor('strategy/playbook.md', previous, previous), []);
  assert.match(errorsFor('strategy/playbook.md', previous, `${previous} [tick:#11]`)[0], /adds perishable tick evidence: \[tick:#11\]/);
  // Research pages cite ticks freely.
  assert.deepEqual(errorsFor('regimes/current.md', page('regimes/current.md'), page('regimes/current.md', '[tick:#11]')), []);
});

test('advisors read every wiki page in full', () => {
  const { WIKI_ALL_PAGES } = loadProduction(['WIKI_ALL_PAGES']);
  assert.deepEqual([...WIKI_ALL_PAGES].sort(), Object.keys(WIKI_EXPECTED_HEADERS).sort());
  const long = `**TLDR**\n## Historical Windows\n${'history '.repeat(4000)}END-OF-PAGE`;
  const pages = Object.fromEntries(WIKI_ALL_PAGES.map((pagePath) => [pagePath, long]));
  pages['revenue/windows.md'] = '# Premium Windows\n\nAwaiting initial assessment';
  const { queryWikiContext } = loadProduction(['queryWikiContext'], {
    bindings: { readWikiPage: (pagePath) => pages[pagePath], readWikiMeta: () => ({}) },
  });
  const context = queryWikiContext();
  for (const pagePath of WIKI_ALL_PAGES) {
    assert.equal(context.includes(`--- ${pagePath} ---`), pagePath !== 'revenue/windows.md', pagePath);
  }
  assert.equal(context.split('END-OF-PAGE').length - 1, WIKI_ALL_PAGES.length - 1);
  assert.ok(!context.includes('[truncated]'));
});

test('both write paths call the shared rules', () => {
  const script = fs.readFileSync(path.join(root, 'script.js'), 'utf8');
  const dashboard = fs.readFileSync(path.join(root, 'dashboard/src/lib/wiki.ts'), 'utf8');
  assert.match(script, /const ruleErrors = getWikiPageRuleErrors\(/);
  assert.match(dashboard, /errors\.push\(\.\.\.getWikiPageRuleErrors\(/);
  assert.ok(!/WIKI_EXPECTED_HEADERS\s*(?::[^=]+)?=\s*\{/.test(script + dashboard), 'no second copy of the section list');
  // The schema the LLM reads states the same budget the code enforces.
  assert.ok(fs.readFileSync(path.join(root, 'knowledge/schema.md'), 'utf8').includes(`${WIKI_PAGE_WORD_BUDGET.toLocaleString('en-US')} words, or ${WIKI_HISTORY_PAGE_WORD_BUDGET.toLocaleString('en-US')} words`));
});
