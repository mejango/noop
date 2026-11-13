'use strict';

// One home for the wiki page rules. The bot's automatic ingest (script.js) and
// the dashboard's manual repair (dashboard/src/lib/wiki.ts) both call
// getWikiPageRuleErrors, so a rewrite is held to the same rules on either path.

/** @type {Record<string, string[]>} */
const WIKI_EXPECTED_HEADERS = {
  'regimes/current.md': ['Classification', 'Evidence', 'Falsification', 'Confidence'],
  'regimes/history.md': ['Regime Transitions', 'Patterns'],
  'protection/pricing.md': ['Current IV Environment', 'Skew Analysis', 'Term Structure', 'Cost Assessment'],
  'protection/windows.md': ['Active Windows', 'Historical Windows', 'Window Indicators'],
  'protection/convexity.md': ['Current Convexity Map', 'Strike-Delta Sweet Spots', 'Convexity Shifts'],
  'revenue/pricing.md': ['Current Premium Environment', 'Skew & IV Context', 'Premium Assessment'],
  'revenue/windows.md': ['Active Windows', 'Historical Windows', 'Window Indicators'],
  'revenue/efficiency.md': ['Premium Per Unit Risk', 'Strike Selection Patterns', 'Buyback Patterns'],
  'indicators/leading.md': ['Confirmed Leading Indicators', 'Experimental Indicators', 'Failed Indicators'],
  'indicators/correlations.md': ['Strong Correlations', 'Weakening Correlations', 'New Correlations'],
  'indicators/divergences.md': ['Active Divergences', 'Historical Divergence Episodes', 'Divergence Playbook'],
  'strategy/lessons.md': ['Active Lessons', 'Archived Lessons', 'Evidence Tracker'],
  'strategy/mistakes.md': ['Costly Patterns', 'Near Misses', 'Anti-Patterns'],
  'strategy/playbook.md': ['Core Rules', 'Regime-Specific Actions', 'Sizing Guidelines', 'Timing Rules'],
};

/** @type {Record<string, Record<string, string>>} */
const REFERENCE_ONLY_SECTIONS = {
  'revenue/pricing.md': {
    'Skew & IV Context': 'Current skew and IV readings are perishable. Consult protection/pricing.md and regimes/current.md for current values.',
  },
  'revenue/efficiency.md': {
    'Strike Selection Patterns': 'Strikes 11–22% OTM have produced consistent disciplined wins; select strike distance by the OTM buffer tolerable if spot touches the top of the expected range. See [lesson:short_call.strike_and_sizing].',
    'Buyback Patterns': 'A buyback is insurance, not a rescue: once most of the premium has decayed, buying back below strike is a disciplined exit when its certain cost is smaller than the risk it removes, and spot later staying below strike does not make it a mistake. Assess strike distance, DTE, IV trend, and momentum before any buyback. See [lesson:short_call.exit_insurance] and [lesson:process.decision_quality].',
  },
};

// Advisors read every page in full, so this bounds their wiki context at
// 14 pages x 2,500 words. A page already over budget may stay its size; it may
// not grow. ponytail: one budget for every page; per-page budgets if one page
// proves to need more room than the rest.
const WIKI_PAGE_WORD_BUDGET = 2500;

/** @param {string} content */
const countWikiWords = (content) => String(content || '').trim().split(/\s+/).filter(Boolean).length;

/** @param {string} content @param {string} heading */
const hasH2 = (content, heading) => String(content).split('\n').some((line) => line.trim() === `## ${heading}`);

/** @param {string} content @param {string} heading @returns {string | null} */
const getH2Body = (content, heading) => {
  const lines = String(content).split('\n');
  const headingIndex = lines.findIndex((line) => line.trim() === `## ${heading}`);
  if (headingIndex < 0) return null;
  const nextHeadingOffset = lines.slice(headingIndex + 1).findIndex((line) => /^##\s+/.test(line.trim()));
  const endIndex = nextHeadingOffset < 0 ? lines.length : headingIndex + 1 + nextHeadingOffset;
  return lines.slice(headingIndex + 1, endIndex).join('\n').trim();
};

/** @param {string} content */
const normalizeProse = (content) => String(content).replace(/\s+/g, ' ').trim();

/** @param {string} content @returns {number[]} */
const getTickIds = (content) => Array.from(String(content).matchAll(/\[tick:#(\d+)\]/gi), (match) => Number(match[1]))
  .filter(Number.isFinite);

/**
 * Rules every rewrite of a wiki page must satisfy, whoever proposes it.
 * @param {{ pagePath: string, previousContent?: string, replacementContent: string }} args
 * @returns {string[]}
 */
function getWikiPageRuleErrors({ pagePath, previousContent = '', replacementContent }) {
  const errors = [];
  const replacement = String(replacementContent || '').trim();

  const missingHeaders = (WIKI_EXPECTED_HEADERS[pagePath] || []).filter((heading) => !hasH2(replacement, heading));
  if (missingHeaders.length > 0) errors.push(`Replacement is missing sections: ${missingHeaders.join(', ')}`);

  Object.entries(REFERENCE_ONLY_SECTIONS[pagePath] || {}).forEach(([heading, canonicalBody]) => {
    const replacementBody = getH2Body(replacement, heading);
    if (replacementBody != null && normalizeProse(replacementBody) !== normalizeProse(canonicalBody)) {
      errors.push(`${heading} section must contain only: ${canonicalBody}`);
    }
  });

  const previousWords = countWikiWords(previousContent);
  const replacementWords = countWikiWords(replacement);
  if (replacementWords > Math.max(WIKI_PAGE_WORD_BUDGET, previousWords)) {
    errors.push(
      `Replacement grows the page past the ${WIKI_PAGE_WORD_BUDGET}-word budget (${previousWords} → ${replacementWords} words); consolidate instead of appending`,
    );
  }

  if (pagePath.startsWith('strategy/')) {
    const previousTickIds = new Set(getTickIds(previousContent));
    const addedTickIds = Array.from(new Set(getTickIds(replacement))).filter((tickId) => !previousTickIds.has(tickId));
    if (addedTickIds.length > 0) {
      errors.push(
        `Learning-owned strategy page adds perishable tick evidence: ${addedTickIds.map((tickId) => `[tick:#${tickId}]`).join(', ')}`,
      );
    }
  }

  return errors;
}

module.exports = {
  WIKI_EXPECTED_HEADERS,
  REFERENCE_ONLY_SECTIONS,
  WIKI_PAGE_WORD_BUDGET,
  countWikiWords,
  hasH2,
  getH2Body,
  getTickIds,
  getWikiPageRuleErrors,
};
