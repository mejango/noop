'use strict';

// One-off recompile of the wiki's knowledge pages from the full journal and
// review history. The regular ingest only ever sees the current pages plus the
// latest evidence packet, so a learning that was trimmed off a page is gone for
// good; this job rebuilds the pages by folding in every past period, oldest
// first, under the same compile rules.
//
// It never touches live pages. Drafts are staged in <wiki>/.recompile/<run>/
// and progress is saved after every period, because the bot restarts on every
// deploy. Applying staged drafts is a separate, explicit step in script.js.
//
// The run ids are the WIKI_RECOMPILE_RUN and WIKI_RECOMPILE_APPLY constants in
// script.js (environment variables of the same names override them); each id runs once.

const fs = require('fs');
const path = require('path');
const { WIKI_EXPECTED_HEADERS, REFERENCE_ONLY_SECTIONS, WIKI_HISTORY_PAGES, getWikiPageWordBudget, countWikiWords, getWikiPageRuleErrors, getDroppedRequiredStatements } = require('./wiki-rules');

// Assessment pages (regimes/current, the two pricing pages) describe the present,
// and strategy pages are views of Learning's canonical lessons: neither has
// history to recompile.
const RECOMPILE_PAGES = WIKI_HISTORY_PAGES;
// Two pages keep a call's output well under the model's limit; four overran it once pages filled up.
const PAGES_PER_CALL = 2;
// Working target below the hard budget, so a page has room to take the next period.
const RECOMPILE_HEADROOM_WORDS = 400;
const targetWords = (pagePath) => getWikiPageWordBudget(pagePath) - RECOMPILE_HEADROOM_WORDS;
const MAX_CORRECTIONS = 3;
// ponytail: bump to replay the recorded gaps once more after a fix to the replay itself.
const REPLAY_VERSION = 4;
// ponytail: ~60k tokens of sources per call next to ~35k of drafts; a busier journal just yields more parts.
const MAX_PERIOD_CHARS = 240000;
const CURRENT_PAGES_PERIOD = 'current-pages';

const skeletonPage = (pagePath) => [
  '**Not yet compiled.**',
  '',
  ...WIKI_EXPECTED_HEADERS[pagePath].flatMap((heading) => [
    `## ${heading}`,
    REFERENCE_ONLY_SECTIONS[pagePath]?.[heading] || 'Nothing compiled yet.',
    '',
  ]),
].join('\n');

const formatSource = (source) => {
  const verdict = source.outcome_verdict || (source.outcome_status && source.outcome_status !== 'pending' ? source.outcome_status : '');
  const tag = [String(source.timestamp || '').slice(0, 10), source.entry_type, verdict && `reviewed outcome: ${verdict}`].filter(Boolean).join(' | ');
  return `[${tag}]\n${String(source.content || '').trim()}`;
};

// Half-month periods, oldest first; a period over the size cap is split into parts.
const chunkSources = (sources = [], maxChars = MAX_PERIOD_CHARS) => {
  const periods = new Map();
  [...sources].sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp))).forEach((source) => {
    const day = String(source.timestamp || '').slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(day)) return;
    const key = `${day.slice(0, 7)}-${Number(day.slice(8)) <= 15 ? 'a' : 'b'}`;
    if (!periods.has(key)) periods.set(key, []);
    periods.get(key).push(formatSource(source));
  });
  const chunks = [];
  periods.forEach((texts, key) => {
    const parts = [[]];
    let size = 0;
    texts.forEach((text) => {
      if (size + text.length > maxChars && parts[parts.length - 1].length > 0) { parts.push([]); size = 0; }
      parts[parts.length - 1].push(text);
      size += text.length;
    });
    parts.forEach((part, i) => chunks.push({
      label: parts.length > 1 ? `${key}-${i + 1}` : key,
      text: part.join('\n\n---\n\n'),
    }));
  });
  return chunks;
};

const buildPrompt = ({ schema, drafts, batchPages, period, sourcesText, feedback = '' }) => {
  const foldingCurrent = period === CURRENT_PAGES_PERIOD;
  return `You are recompiling the knowledge pages of an ETH options trading bot's wiki from its full history, one period at a time, oldest first. Earlier periods are already compiled into the drafts below. Fold in the sources for this step.

## Wiki Schema
${schema}

## Drafts compiled so far (all knowledge pages, for cross-page consistency)
${RECOMPILE_PAGES.map((page) => `--- ${page} ---\n${drafts[page]}`).join('\n\n')}

## Pages you may update in this call
${batchPages.map((page) => `- ${page} (draft is ${countWikiWords(drafts[page])} words; return at most ${targetWords(page)}, hard limit ${getWikiPageWordBudget(page)})`).join('\n')}
${feedback ? `\n## Your previous update was rejected\n${feedback}\nReturn a corrected full page. If the page is at its budget, make room by merging entries about one pattern, not by leaving this period out.\n` : ''}
## Sources: ${foldingCurrent ? 'the current live pages (the last step)' : `period ${period}`}
${foldingCurrent
    ? 'These are the live versions of the same pages. They cover the most recent weeks in detail and carry cited evidence markers. Merge what they establish into the drafts; keep their source markers exactly, and do not copy their per-pass snapshot lists.'
    : 'These are the bot\'s journal entries for the period: analyst notes, the lowest tier of the source hierarchy. A hypothesis entry that carries a reviewed outcome is stronger evidence than a note.'}

${sourcesText || 'No sources for this step.'}

## Instructions
1. Compile, do not append. Each page states what the whole history so far shows: one entry per pattern or per episode (a regime, a window or a divergence, from open to resolution), each with its sample (how many episodes), its period and its outcome. When this period bears on an entry the draft already holds, revise that entry in place: strengthen, weaken or overturn it and update its sample. Never add one entry per period or per note.
2. Weigh all history. This period moves a pattern built on earlier periods by its share of the sample; it does not replace it. Keep what earlier periods established unless this period refutes it, and say so when it does.
3. Compile only what recurs across notes or is backed by a reviewed hypothesis outcome or a cited review. A claim made in a single note is left out or marked as unconfirmed with its date.
4. These periods are past. Write no current-state content, and keep the schema's Reference-only sections exactly as given.
5. Date every claim with [YYYY-MM-DD] stamps for the period it rests on. Never invent a [tick:#ID], [order:#ID], [review:#ID] or [lesson:key] marker; use one only if it appears verbatim in the sources or drafts.
6. Replace "Not yet compiled" and "Nothing compiled yet" once a page or section has content; leave them where the history so far says nothing.
7. Each page starts with a bold TLDR of at most 40 words stating what history shows, and is returned at no more than the word count given for it above. A draft above that is consolidated in the same rewrite: merge entries about one pattern, shorten wording, never drop an earlier period to make room.

Output full pages as XML blocks, only for pages that change:

<wiki_update path="regimes/history.md">
[full updated page content]
</wiki_update>

If nothing changes, output: <no_updates/>`;
};

const buildConsolidationPrompt = ({ pagePath, content, words }) => {
  const target = targetWords(pagePath);
  const cut = Math.ceil((1 - target / words) * 100);
  return `This wiki page of an ETH options trading bot is ${words} words by the counter that enforces its ${getWikiPageWordBudget(pagePath)}-word limit. The counter counts every space-separated token, including table cells and source markers, so the page is longer than it reads. Rewrite it about ${cut}% shorter (at most ${target} words).

Rules:
1. Lose wording, not knowledge. Merge entries that describe one pattern, merge adjacent table rows of one arc, cut restated numbers and qualifiers. Every month the page covers stays represented, and every pattern keeps its sample and outcome.
2. Do not favour recent periods: cut detail in proportion across the whole history.
3. Keep every section heading, the bold TLDR (at most 40 words) and any Reference-only section exactly.
4. Keep source markers ([review:#ID], [order:#ID], [tick:#ID], [lesson:key]) verbatim where the claim they support stays; add none.

Output only:

<wiki_update path="${pagePath}">
[full consolidated page]
</wiki_update>

--- ${pagePath} ---
${content}`;
};

const parseUpdates = (text) => Array.from(
  String(text || '').matchAll(/<wiki_update\s+path="([^"]+)">([\s\S]*?)<\/wiki_update>/g),
  (match) => ({ pagePath: match[1], content: match[2].trim() }),
);

// Pages a cut-off response did not finish: no complete <wiki_update> block came back for them.
const getPagesWithoutUpdate = (text, pages) => {
  const returned = new Set(parseUpdates(text).map((update) => update.pagePath));
  return pages.filter((page) => !returned.has(page));
};

const readJson = (file, fallback) => {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
};
const draftPath = (stagingDir, pagePath) => path.join(stagingDir, pagePath);
const readDrafts = (stagingDir) => Object.fromEntries(RECOMPILE_PAGES.map((pagePath) => {
  try { return [pagePath, fs.readFileSync(draftPath(stagingDir, pagePath), 'utf8')]; } catch { return [pagePath, skeletonPage(pagePath)]; }
}));

/**
 * Stage a recompile. Resumable: periods already recorded in progress.json are skipped.
 * callModel(prompt) resolves to { text, truncated }; a rejected call stops the run so a
 * later start resumes from the same period.
 */
async function runWikiRecompile({ stagingDir, sources, currentPages, schema, callModel, log = () => {} }) {
  fs.mkdirSync(stagingDir, { recursive: true });
  const progressFile = path.join(stagingDir, 'progress.json');
  const progress = readJson(progressFile, { done: [], rejected: [] });
  const steps = [
    ...chunkSources(sources),
    {
      label: CURRENT_PAGES_PERIOD,
      text: RECOMPILE_PAGES.map((page) => `--- ${page} ---\n${currentPages[page] || ''}`).join('\n\n'),
    },
  ];
  const saveProgress = () => fs.writeFileSync(progressFile, JSON.stringify(progress, null, 2));
  // Fold one step into the given page batches; returns how many pages were updated.
  const foldStep = async (step, batches) => {
    const drafts = readDrafts(stagingDir);
    // One call at a time: the job shares the API rate limit with the journal, ingest and advisory.
    // A cut-off response may have lost pages, so those are asked for again one page per call;
    // otherwise the period would be marked folded for pages that never saw it.
    const foldBatch = async (batchPages) => {
      const { text, truncated } = await callModel(buildPrompt({ schema, drafts, batchPages, period: step.label, sourcesText: step.text }));
      const updates = parseUpdates(text).filter((update) => batchPages.includes(update.pagePath));
      if (!truncated || batchPages.length === 1) return updates;
      for (const pagePath of getPagesWithoutUpdate(text, batchPages)) {
        updates.push(...await foldBatch([pagePath]));
      }
      return updates;
    };
    const updates = [];
    for (const batchPages of batches) updates.push(...await foldBatch(batchPages));
    // The word budget never rejects a fold: dropping a period loses knowledge, while an over-budget
    // draft only needs shortening. Staging holds the long draft, a separate pass consolidates it,
    // and the apply step still refuses a page that grows a live page past the budget.
    const ruleErrors = (pagePath, content, previousContent = drafts[pagePath]) => (content.length < 50
      ? ['too short']
      : getWikiPageRuleErrors({ pagePath, previousContent: previousContent === skeletonPage(pagePath) ? '' : previousContent, replacementContent: content })
        .filter((error) => !error.includes('word budget')));
    let updated = 0;
    for (let { pagePath, content } of updates) {
      let errors = ruleErrors(pagePath, content);
      if (errors.length > 0) {
        // One corrected attempt with the reason; a plain rejection would drop this period from the page.
        const retry = await callModel(buildPrompt({
          schema, drafts, batchPages: [pagePath], period: step.label, sourcesText: step.text, feedback: `${pagePath}: ${errors.join('; ')}`,
        }));
        const corrected = parseUpdates(retry.text).find((update) => update.pagePath === pagePath);
        if (corrected && ruleErrors(pagePath, corrected.content).length === 0) {
          content = corrected.content;
          errors = [];
        }
      }
      if (errors.length > 0) {
        progress.rejected.push({ period: step.label, pagePath, reason: errors.join('; ') });
        log(`rejected ${pagePath} for ${step.label}: ${errors.join('; ')}`);
        continue;
      }
      // Consolidate on its own: a short prompt holding only the page, its measured count and the
      // share to cut. The model cannot hold a word limit while it compiles from the sources.
      for (let attempt = 0; attempt < MAX_CORRECTIONS && countWikiWords(content) > getWikiPageWordBudget(pagePath); attempt++) {
        const words = countWikiWords(content);
        const retry = await callModel(buildConsolidationPrompt({ pagePath, content, words }));
        const shorter = parseUpdates(retry.text).find((update) => update.pagePath === pagePath);
        const problems = !shorter ? [`no page in the response${retry.truncated ? ' (cut off)' : ''}`]
          : countWikiWords(shorter.content) >= words ? [`not shorter (${countWikiWords(shorter.content)} words)`]
            : ruleErrors(pagePath, shorter.content, content);
        log(`consolidate ${pagePath} (${words} words): ${problems.length ? problems.join('; ') : `${countWikiWords(shorter.content)} words`}`);
        if (problems.length === 0) content = shorter.content;
        // A cut-off reply spent its whole output allowance; asking again repeats the cost.
        if (!shorter && retry.truncated) break;
      }
      fs.mkdirSync(path.dirname(draftPath(stagingDir, pagePath)), { recursive: true });
      fs.writeFileSync(draftPath(stagingDir, pagePath), `${content}\n`);
      updated++;
    }
    return updated;
  };
  for (const step of steps) {
    if (progress.done.includes(step.label)) continue;
    const batches = [];
    for (let i = 0; i < RECOMPILE_PAGES.length; i += PAGES_PER_CALL) batches.push(RECOMPILE_PAGES.slice(i, i + PAGES_PER_CALL));
    const updated = await foldStep(step, batches);
    progress.done.push(step.label);
    saveProgress();
    log(`period ${step.label} folded (${progress.done.length}/${steps.length}), ${updated} page(s) updated`);
  }
  // Replay each page-and-period a rejection dropped, once. The gap leaves progress.json only after
  // its replay ran, so a restart mid-replay tries it again; a replay that fails is recorded as
  // replayed and is not tried again.
  for (const gap of progress.rejected.filter((item) => item.replayed !== REPLAY_VERSION)) {
    const step = steps.find((candidate) => candidate.label === gap.period);
    progress.rejected.splice(progress.rejected.indexOf(gap), 1);
    const updated = step ? await foldStep(step, [[gap.pagePath]]) : 0;
    if (!updated && !progress.rejected.some((item) => item.period === gap.period && item.pagePath === gap.pagePath)) progress.rejected.push(gap);
    for (const item of progress.rejected) if (item.period === gap.period && item.pagePath === gap.pagePath) item.replayed = REPLAY_VERSION;
    saveProgress();
    log(`gap ${gap.period} ${gap.pagePath}: ${updated ? 'folded' : 'still missing'}`);
  }
  const summary = { completed_at: new Date().toISOString(), periods: steps.map((step) => step.label), rejected: progress.rejected };
  fs.writeFileSync(path.join(stagingDir, 'done.json'), JSON.stringify(summary, null, 2));
  return summary;
}

/**
 * Which staged drafts may replace their live page. A draft still holding the skeleton,
 * breaking a page rule, or citing a marker the live wiki and reviews do not contain is skipped.
 */
/**
 * A run whose pages were written outside the bot ships in the repo under
 * knowledge/recompiled/<run>/ and is copied to the staging folder once, so it passes
 * through the same apply checks as a run the bot staged itself. Returns the pages copied.
 */
function seedRunFromRepo({ repoRunDir, stagingDir }) {
  if (!fs.existsSync(repoRunDir) || fs.existsSync(path.join(stagingDir, 'done.json'))) return [];
  const seeded = RECOMPILE_PAGES.filter((pagePath) => fs.existsSync(path.join(repoRunDir, pagePath)));
  for (const pagePath of seeded) {
    fs.mkdirSync(path.dirname(draftPath(stagingDir, pagePath)), { recursive: true });
    fs.copyFileSync(path.join(repoRunDir, pagePath), draftPath(stagingDir, pagePath));
  }
  if (seeded.length > 0) {
    fs.writeFileSync(path.join(stagingDir, 'done.json'), JSON.stringify({ completed_at: new Date().toISOString(), source: 'repo', periods: [], rejected: [] }, null, 2));
  }
  return seeded;
}

// A finished run still has work when a dropped page-and-period was never replayed.
const hasUnreplayedGaps = (summary) => (summary?.rejected || []).some((item) => item.replayed !== REPLAY_VERSION);

function selectRecompileApplicable({ staged, live, allowedMarkers, getMarkers }) {
  const apply = [];
  const skipped = [];
  RECOMPILE_PAGES.forEach((pagePath) => {
    const content = staged[pagePath];
    if (!content || content.includes('Not yet compiled')) return skipped.push({ pagePath, reason: 'no compiled draft' });
    // Size is judged against the budget alone: the live page is being replaced, not extended.
    const errors = getWikiPageRuleErrors({ pagePath, previousContent: '', replacementContent: content });
    const unknown = Array.from(getMarkers(content)).filter((marker) => !allowedMarkers.has(marker));
    if (unknown.length > 0) errors.push(`unknown source marker(s): ${unknown.slice(0, 5).join(', ')}`);
    const dropped = getDroppedRequiredStatements(pagePath, live[pagePath], content);
    if (dropped.length > 0) errors.push(`drops required statement(s): ${dropped.join('; ')}`);
    if (errors.length > 0) return skipped.push({ pagePath, reason: errors.join('; ') });
    if (content.trim() === String(live[pagePath] || '').trim()) return skipped.push({ pagePath, reason: 'identical to live page' });
    apply.push(pagePath);
  });
  return { apply, skipped };
}

module.exports = {
  RECOMPILE_PAGES,
  CURRENT_PAGES_PERIOD,
  skeletonPage,
  chunkSources,
  buildPrompt,
  parseUpdates,
  getPagesWithoutUpdate,
  readDrafts,
  runWikiRecompile,
  hasUnreplayedGaps,
  seedRunFromRepo,
  selectRecompileApplicable,
};
