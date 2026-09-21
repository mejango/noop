import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { noul, TypeSafeClient } from '@typesafe-ai/sdk';

import { countWikiEvidenceReferences, extractWikiTldr, WIKI_PAGES } from '@/lib/wikiCatalog';

export function resolveWikiDir(): string {
  if (process.env.WIKI_DIR) return process.env.WIKI_DIR;

  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
  const sharedWikiDir = path.join(dataDir, 'knowledge');
  if (fs.existsSync(sharedWikiDir)) return sharedWikiDir;

  return path.join(process.cwd(), '..', 'knowledge');
}

export const WIKI_PAGE_PATHS = WIKI_PAGES.map((page) => page.path);

export const WIKI_EXPECTED_HEADERS: Record<string, string[]> = {
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

export function hashWikiContent(content: string): string {
  return crypto.createHash('sha256').update(content).digest('hex');
}

export function readWikiMeta(wikiDir = resolveWikiDir()): Record<string, unknown> {
  try {
    return JSON.parse(fs.readFileSync(path.join(wikiDir, '.meta.json'), 'utf-8')) as Record<string, unknown>;
  } catch {
    return {};
  }
}

export function writeWikiMeta(meta: Record<string, unknown>, wikiDir = resolveWikiDir()): void {
  fs.writeFileSync(path.join(wikiDir, '.meta.json'), `${JSON.stringify(meta, null, 2)}\n`);
}

export function saveWikiHistory(pagePath: string, content: string, wikiDir = resolveWikiDir()): void {
  const historyDir = path.join(wikiDir, '.history');
  fs.mkdirSync(historyDir, { recursive: true });
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const safeName = pagePath.replace(/\//g, '__');
  fs.writeFileSync(path.join(historyDir, `${timestamp}__${safeName}`), content);
}

export function appendWikiLog(kind: string, title: string, bulletLines: string[], wikiDir = resolveWikiDir()): void {
  const logPath = path.join(wikiDir, 'log.md');
  if (!fs.existsSync(logPath)) fs.writeFileSync(logPath, '# Knowledge Log\n\n');
  const lines = [`## [${new Date().toISOString()}] ${kind} | ${title}`, ...bulletLines.map((line) => `- ${line}`), ''];
  fs.appendFileSync(logPath, `${lines.join('\n')}\n`);
}

export function refreshWikiIndex(wikiDir = resolveWikiDir()): void {
  const meta = readWikiMeta(wikiDir);
  const storedPages = meta.pages && typeof meta.pages === 'object' && !Array.isArray(meta.pages)
    ? meta.pages as Record<string, { last_reviewed_at?: string | null; issues?: string[] }>
    : {};
  const groups = new Map<string, string[]>();
  for (const page of WIKI_PAGES) {
    const content = fs.readFileSync(path.join(wikiDir, page.path), 'utf-8');
    const stored = storedPages[page.path] || {};
    const issues = Array.isArray(stored.issues) ? stored.issues.length : 0;
    const entry = `- [${page.path}](${page.path}) — ${extractWikiTldr(content)} (evidence refs: ${countWikiEvidenceReferences(content)}; reviewed: ${stored.last_reviewed_at || 'never'}; issues: ${issues}; ${page.owner === 'learning' ? 'Learning-owned view' : 'Wiki research'})`;
    const entries = groups.get(page.category) || [];
    entries.push(entry);
    groups.set(page.category, entries);
  }
  const lines = [
    '# Knowledge Index',
    '',
    'System-maintained catalog of the compiled trading wiki. Read this first to understand what pages exist and where current knowledge lives.',
    '',
    `Updated: ${new Date().toISOString()}`,
    '',
    '## System Files',
    '- [schema.md](schema.md) — wiki structure, source hierarchy, and update rules',
    '- [log.md](log.md) — append-only maintenance timeline',
    '- Raw evidence packets live in [raw/evidence](raw/evidence)',
    '',
  ];
  groups.forEach((entries, category) => lines.push(`## ${category}`, ...entries, ''));
  fs.writeFileSync(path.join(wikiDir, 'index.md'), `${lines.join('\n').trim()}\n`);
}

export function getStructuredWikiMarkers(content: string): Set<string> {
  return new Set(
    (content.match(/\[(?:source:\s*[^\]]+|(?:tick|order|review):#\d+|lesson:[^\]]+)\]/gi) || [])
      .map((value) => value.toLowerCase()),
  );
}

function getTickIds(content: string): number[] {
  return Array.from(content.matchAll(/\[tick:#(\d+)\]/gi), (match) => Number(match[1]))
    .filter(Number.isFinite);
}

function latestTickId(content: string): number | null {
  const tickIds = getTickIds(content);
  return tickIds.length > 0 ? Math.max(...tickIds) : null;
}

const REFERENCE_ONLY_SECTIONS: Record<string, Record<string, string>> = {
  'revenue/pricing.md': {
    'Skew & IV Context': 'Current skew and IV readings are perishable. Consult protection/pricing.md and regimes/current.md for current values.',
  },
  'revenue/efficiency.md': {
    'Strike Selection Patterns': 'Strikes 11–22% OTM have produced consistent disciplined wins; select strike distance by the OTM buffer tolerable if spot touches the top of the expected range. See [lesson:short_call.strike_and_sizing].',
    'Buyback Patterns': 'When spot is ≥10% below strike with DTE collapsing, buying back converts near-certain theta income into a certain realized loss. Assess strike distance, DTE, and momentum before any buyback; a buyback below strike requires a credible breakout thesis. See [lesson:short_call.exit_insurance] and [lesson:process.decision_quality].',
  },
};

const SUPPORTED_NUMERIC_MARKER_TYPES = new Set(['tick', 'order', 'review']);

function getUnsupportedNumericMarkerTypes(content: string): Set<string> {
  return new Set(
    Array.from(content.matchAll(/\[([a-z][a-z0-9_-]*):#(?:\d+|NNN)\]/gi), (match) => match[1].toLowerCase())
      .filter((type) => !SUPPORTED_NUMERIC_MARKER_TYPES.has(type)),
  );
}

function getPlaceholderMarkerTypes(content: string): Set<string> {
  return new Set(
    Array.from(content.matchAll(/\[([a-z][a-z0-9_-]*):#NNN\]/gi), (match) => match[1].toLowerCase()),
  );
}

export function isUnsupportedStructuredMarkerIssue(issue: string): boolean {
  return getUnsupportedNumericMarkerTypes(issue).size > 0
    && /(?:schema|marker|evidence|required|compliant|enforce)/i.test(issue);
}

function hasH2(content: string, heading: string): boolean {
  return content.split('\n').some((line) => line.trim() === `## ${heading}`);
}

function getH2Body(content: string, heading: string): string | null {
  const lines = content.split('\n');
  const headingIndex = lines.findIndex((line) => line.trim() === `## ${heading}`);
  if (headingIndex < 0) return null;
  const nextHeadingOffset = lines
    .slice(headingIndex + 1)
    .findIndex((line) => /^##\s+/.test(line.trim()));
  const endIndex = nextHeadingOffset < 0 ? lines.length : headingIndex + 1 + nextHeadingOffset;
  return lines.slice(headingIndex + 1, endIndex).join('\n').trim();
}

function normalizeProse(content: string): string {
  return content.replace(/\s+/g, ' ').trim();
}

function getDuplicateMatches(content: string, pattern: RegExp): string[] {
  const counts = new Map<string, number>();
  Array.from(content.matchAll(pattern), (match) => match[1].toUpperCase()).forEach((value) => {
    counts.set(value, (counts.get(value) || 0) + 1);
  });
  return Array.from(counts.entries()).filter(([, count]) => count > 1).map(([value]) => value);
}

function getProvenanceRequirementTypes(content: string): Set<string> {
  const types = new Set<string>();
  Array.from(content.matchAll(/\[([a-z][a-z0-9_-]*):#(?:\d+|NNN)\]/gi), (match) => match[1].toLowerCase())
    .forEach((type) => types.add(type));
  Array.from(
    content.matchAll(/\b(tick|order|review|task)\s+(?:source\s+)?markers?\b/gi),
    (match) => match[1].toLowerCase(),
  ).forEach((type) => types.add(type));
  return types;
}

function findTaskParagraph(content: string, taskLabel: string): string {
  return content.split(/\n\s*\n/).find((paragraph) => paragraph.includes(taskLabel)) || '';
}

function findNumberedMetadataTask(content: string, taskNumber: number): string {
  const metadata = content.split(/\n\s*\n/).find((paragraph) => /Page metadata:/i.test(paragraph)) || '';
  const nextBoundary = taskNumber + 1;
  return metadata.match(new RegExp(
    `\\(${taskNumber}\\)([\\s\\S]*?)(?=;\\s*\\(${nextBoundary}\\)|\\.\\s*Additionally:|\\*?$)`,
    'i',
  ))?.[1] || '';
}

export function validateWikiReplacement(args: {
  pagePath: string;
  previousContent: string;
  replacementContent: string;
  allowedMarkerContent: string;
  canonicalLessonContent: string;
  // Tick markers the semantic judgments identified as expired live state (see judgeWikiReplacement).
  expiredTickIds?: Set<number>;
}): string[] {
  const {
    pagePath,
    previousContent,
    replacementContent,
    allowedMarkerContent,
    canonicalLessonContent,
    expiredTickIds = new Set<number>(),
  } = args;
  const errors: string[] = [];
  const replacement = replacementContent.trim();
  if (!WIKI_PAGE_PATHS.includes(pagePath)) errors.push('Unknown Wiki page');
  if (replacement.length < 50) errors.push('Replacement content is too short');
  if (previousContent.length > 100 && replacement.length < previousContent.length * 0.5) {
    errors.push('Replacement shrinks the page by more than 50%');
  }
  const previousWordCount = previousContent.trim().split(/\s+/).filter(Boolean).length;
  const replacementWordCount = replacement.split(/\s+/).filter(Boolean).length;
  // 2,000 words is a maintenance target, not a safety boundary. Allow a small
  // tokenizer/editing margin so a human-reviewed repair is not rejected for a
  // handful of words, while preventing meaningful growth of long pages.
  const permittedWordCount = Math.max(2_050, previousWordCount);
  if (replacementWordCount > permittedWordCount) {
    errors.push(
      previousWordCount > 2_050
        ? `Oversized page grows during repair (${previousWordCount} → ${replacementWordCount} words); preserve or reduce its length`
        : `Replacement exceeds the 2050-word repair ceiling (${replacementWordCount})`,
    );
  }
  if (!/^\s*(?:#(?!#)[^\n]*\n+\s*)?\*\*[^\n]+\*\*/.test(replacement)) {
    errors.push('Replacement must keep a bold TLDR immediately after the optional H1');
  }
  const missingHeaders = (WIKI_EXPECTED_HEADERS[pagePath] || [])
    .filter((heading) => !hasH2(replacement, heading));
  if (missingHeaders.length > 0) errors.push(`Replacement is missing sections: ${missingHeaders.join(', ')}`);

  const referenceOnlySections = REFERENCE_ONLY_SECTIONS[pagePath] || {};
  Object.entries(referenceOnlySections).forEach(([heading, canonicalBody]) => {
    const replacementBody = getH2Body(replacement, heading);
    if (replacementBody != null && normalizeProse(replacementBody) !== normalizeProse(canonicalBody)) {
      errors.push(`${heading} section must contain only: ${canonicalBody}`);
    }
  });

  if (pagePath === 'revenue/efficiency.md') {
    const premiumRiskBody = getH2Body(replacement, 'Premium Per Unit Risk') || '';
    const duplicateCampaignRows = getDuplicateMatches(
      premiumRiskBody,
      /^\|\s*(ETH-\d{8}-\d+-[CP])\s*\|/gmi,
    );
    if (duplicateCampaignRows.length > 0) {
      errors.push(`Premium Per Unit Risk repeats campaign rows: ${duplicateCampaignRows.join(', ')}`);
    }
    const callScoreNoteCount = (premiumRiskBody.match(/^>\s*\*\*Call Score Context note/gm) || []).length;
    if (callScoreNoteCount > 1) errors.push('Premium Per Unit Risk repeats the Call Score Context note');

    const currentCycleBody = getH2Body(replacement, 'Current Cycle Status') || '';
    const duplicateCurrentCampaigns = getDuplicateMatches(
      currentCycleBody,
      /^\s*-\s*(ETH-\d{8}-\d+-[CP])\s+(?:—|-)/gmi,
    );
    if (duplicateCurrentCampaigns.length > 0) {
      errors.push(`Current Cycle Status repeats active campaigns: ${duplicateCurrentCampaigns.join(', ')}`);
    }
    const stalenessNoticeCount = (currentCycleBody.match(/^>\s*⚠️\s*\*\*Staleness notice/gm) || []).length;
    if (stalenessNoticeCount > 1) errors.push('Current Cycle Status repeats the staleness notice');
  }

  if (pagePath === 'strategy/mistakes.md') {
    const taskScopes = [
      ['Open review task', findTaskParagraph(previousContent, 'Open review task'), findTaskParagraph(replacement, 'Open review task')],
      ['Open provenance task', findTaskParagraph(previousContent, 'Open provenance task'), findTaskParagraph(replacement, 'Open provenance task')],
      ['Metadata task 1', findNumberedMetadataTask(previousContent, 1), findNumberedMetadataTask(replacement, 1)],
      ['Metadata task 2', findNumberedMetadataTask(previousContent, 2), findNumberedMetadataTask(replacement, 2)],
    ];
    taskScopes.forEach(([taskLabel, previousTask, replacementTask]) => {
      if (!previousTask || !replacementTask) return;
      const previousTypes = getProvenanceRequirementTypes(previousTask);
      const inventedTypes = Array.from(getProvenanceRequirementTypes(replacementTask))
        .filter((type) => !previousTypes.has(type));
      if (inventedTypes.length > 0) {
        errors.push(`${taskLabel} invents provenance requirements: ${inventedTypes.join(', ')}`);
      }
    });
  }

  const unsupportedMarkerTypes = Array.from(getUnsupportedNumericMarkerTypes(replacement));
  if (unsupportedMarkerTypes.length > 0) {
    errors.push(`Replacement retains unsupported marker types: ${unsupportedMarkerTypes.join(', ')}`);
  }
  const placeholderMarkerTypes = Array.from(getPlaceholderMarkerTypes(replacement));
  if (placeholderMarkerTypes.length > 0) {
    errors.push(`Replacement retains placeholder source markers: ${placeholderMarkerTypes.join(', ')}`);
  }
  const unsupportedProvenanceRequirements = Array.from(getProvenanceRequirementTypes(replacement))
    .filter((type) => !SUPPORTED_NUMERIC_MARKER_TYPES.has(type));
  if (unsupportedProvenanceRequirements.length > 0) {
    errors.push(
      `Replacement retains unsupported provenance requirements: ${unsupportedProvenanceRequirements.join(', ')}`,
    );
  }

  if (pagePath.startsWith('strategy/')) {
    const previousTickIds = new Set(getTickIds(previousContent));
    const addedTickIds = getTickIds(replacement).filter((tickId) => !previousTickIds.has(tickId));
    if (addedTickIds.length > 0) {
      errors.push(
        `Learning-owned strategy page adds perishable tick evidence: ${Array.from(new Set(addedTickIds)).map((tickId) => `[tick:#${tickId}]`).join(', ')}`,
      );
    }
    const retainedExpiredTickIds = Array.from(expiredTickIds)
      .filter((tickId) => getTickIds(replacement).includes(tickId));
    if (retainedExpiredTickIds.length > 0) {
      errors.push(
        `Learning-owned strategy page retains expired live tick evidence: ${retainedExpiredTickIds.map((tickId) => `[tick:#${tickId}]`).join(', ')}`,
      );
    }
  }

  const previousMarkers = getStructuredWikiMarkers(previousContent);
  const replacementMarkers = getStructuredWikiMarkers(replacement);
  const allowedMarkers = getStructuredWikiMarkers(allowedMarkerContent);
  if (previousMarkers.size > 0 && replacementMarkers.size === 0) errors.push('Replacement removes all structured source markers');
  const unknownMarkers = Array.from(replacementMarkers).filter((marker) => !allowedMarkers.has(marker));
  if (unknownMarkers.length > 0) errors.push(`Replacement invents source markers: ${unknownMarkers.slice(0, 5).join(', ')}`);
  const canonicalLessonMarkers = getStructuredWikiMarkers(canonicalLessonContent);
  const unsupportedLessonMarkers = Array.from(replacementMarkers)
    .filter((marker) => marker.startsWith('[lesson:') && !canonicalLessonMarkers.has(marker));
  if (unsupportedLessonMarkers.length > 0) {
    errors.push(`Replacement uses non-canonical lesson markers: ${unsupportedLessonMarkers.slice(0, 5).join(', ')}`);
  }

  // A targeted repair may replace stale evidence, but it must not silently make
  // the page less current by dropping the newest tick cited anywhere on it.
  const previousLatestTick = latestTickId(previousContent);
  const replacementLatestTick = latestTickId(replacement);
  const removesExpiredStrategyTick = pagePath.startsWith('strategy/')
    && previousLatestTick != null
    && expiredTickIds.has(previousLatestTick);
  if (
    previousLatestTick != null
    && !removesExpiredStrategyTick
    && (replacementLatestTick == null || replacementLatestTick < previousLatestTick)
  ) {
    errors.push(`Replacement drops newest tick evidence [tick:#${previousLatestTick}]`);
  }
  return errors;
}

// ── Semantic judgments ────────────────────────────────────────────────────────
// The checks above are structural (headers, marker vocabulary, exact canonical
// sections, tick-id monotonicity). The ones below are about what the prose
// means, which regexes over model-written English guessed badly. They run as
// System One questions over the paired page state; without TYPESAFE_API_KEY
// they are skipped and the Sonnet approval gate remains the semantic check.
const JUDGMENT_THRESHOLD = 0.6; // ponytail: untuned; raise if repairs get rejected on clean proposals

let typesafe: TypeSafeClient | null = null;
let loggedMode = false;
function getTypeSafe(): TypeSafeClient | null {
  if (!loggedMode) {
    loggedMode = true;
    console.log(`[wiki] semantic repair safeguards: ${process.env.TYPESAFE_API_KEY ? 'TypeSafe judgments' : 'OFF (no TYPESAFE_API_KEY); structural checks + reviewer only'}`);
  }
  if (!process.env.TYPESAFE_API_KEY) return null;
  typesafe ??= new TypeSafeClient();
  return typesafe;
}

// A judgment outage degrades to the no-key path instead of failing the repair
// (and discarding the proposal the caller already paid for).
async function judged<T>(label: string, run: () => Promise<T>): Promise<T | null> {
  try {
    return await run();
  } catch (error) {
    console.log(`[wiki] ${label} judgment failed; continuing without it: ${(error as Error).message}`);
    return null;
  }
}

export async function judgeWikiReplacement(args: {
  pagePath: string;
  previousContent: string;
  replacementContent: string;
  relatedContext: string;
  validationIssues: string[];
}): Promise<{ errors: string[]; expiredTickIds: Set<number> }> {
  const client = getTypeSafe();
  const expiredTickIds = new Set<number>();
  if (!client) return { errors: [], expiredTickIds };
  const { pagePath, previousContent, replacementContent, relatedContext, validationIssues } = args;
  const isStrategy = pagePath.startsWith('strategy/');
  const isLeading = pagePath === 'indicators/leading.md';
  const previousTickIds = isStrategy ? Array.from(new Set(getTickIds(previousContent))) : [];

  const checks: Record<string, [string, ReturnType<typeof noul>]> = {
    revives_resolved: ['Replacement revives resolved escalations', noul(
      'Does `replacement_page` describe as open, unresolved, or still pending an escalation or topic that `previous_page`, `related_pages`, or `findings` mark as resolved?',
    )],
    upgrades_uncertainty: ['Replacement turns an uncertain artifact interpretation into fact', noul(
      'Does `replacement_page` state as established fact or confirmed causal mechanism something that `previous_page` or `findings` describe only as likely, possible, potential, unverified, unconfirmed, or needing reconciliation?',
    )],
    invents_trigger: ['Replacement invents unsupported numeric trading triggers', noul(
      'Does `replacement_page` add a numeric trading trigger (a price, percentage, dollar amount, or count tied to entering, exiting, buying back, or accumulating) that appears nowhere in `previous_page`, `related_pages`, or `findings`?',
      {
        true: 'A new number is attached to obligation language such as must, never, only when, automatically, or triggers',
        false: 'Every number in obligation language already appeared in the supplied context, or the new language is advisory (assess, consider, prefer)',
      },
    )],
    invents_tick_gate: ['Replacement invents a consecutive-tick gate rule', noul(
      'Does `replacement_page` introduce a requirement for a specific number of consecutive ticks that does not appear in `previous_page`, `related_pages`, or `findings`?',
    )],
    misclassifies_unresolved: ['Replacement classifies outcomes without recorded resolution as failed', noul(
      'Do `findings` describe an episode whose outcome or resolution was never recorded, and does `replacement_page` classify that episode as failed or confirmed?',
      { false: 'No finding describes an unrecorded outcome, or the replacement keeps the episode unresolved or experimental' },
    )],
  };
  if (isLeading) {
    checks.unresolved_in_confirmed = ['Unresolved episodes remain as rows in Confirmed Leading Indicators', noul(
      'Do `findings` describe an episode with no recorded outcome that still appears as a row in the "Confirmed Leading Indicators" section of `replacement_page`?',
    )];
    checks.unresolved_missing_experimental = ['Unresolved episodes must be classified under Experimental Indicators', noul(
      'Do `findings` describe an episode with no recorded outcome that is absent from the "Experimental Indicators" section of `replacement_page`?',
    )];
  }
  if (isStrategy) {
    checks.adds_live_state = ['Learning-owned strategy page adds perishable live-state claims', noul(
      'Does `replacement_page` add a claim about the current spot price, current call or put score, current gate status, or current momentum that was not already in `previous_page`?',
      { false: 'Only durable rules remain, or live values are replaced by references to regimes/current.md or protection/revenue pages' },
    )];
    previousTickIds.forEach((tickId) => {
      checks[`expired_tick_${tickId}`] = ['', noul(
        `Do \`findings\` identify the tick marker [tick:#${tickId}] as stale, expired, or superseded live evidence?`,
      )];
    });
  }

  const result = await judged('replacement', () => client.systemOne({
    state: {
      page_path: pagePath,
      previous_page: previousContent,
      replacement_page: replacementContent,
      related_pages: relatedContext,
      findings: validationIssues,
    },
    questions: Object.fromEntries(Object.entries(checks).map(([key, [, question]]) => [key, question])),
  }));
  if (!result) return { errors: [], expiredTickIds };
  const { answers } = result;

  const errors: string[] = [];
  for (const [key, [message]] of Object.entries(checks)) {
    const answer = answers[key];
    if (!answer || answer.noul < JUDGMENT_THRESHOLD) continue;
    if (key.startsWith('expired_tick_')) expiredTickIds.add(Number(key.slice('expired_tick_'.length)));
    else errors.push(message);
  }
  return { errors, expiredTickIds };
}

// Stored lint findings can lag a related page repaired later: drop the ones the
// current wiki already resolves.
export async function findObsoleteUnresolvedEscalationIssues(
  issues: string[],
  wikiContext: string,
): Promise<Set<string>> {
  const client = getTypeSafe();
  const obsolete = new Set<string>();
  if (!client || issues.length === 0) return obsolete;
  const result = await judged('obsolete-findings', () => client.systemOne({
    state: { wiki: wikiContext, findings: issues },
    questions: Object.fromEntries(issues.map((_issue, index) => [`issue_${index}`, noul(
      `Does \`findings[${index}]\` report an open or unresolved escalation that \`wiki\` now explicitly marks as resolved?`,
      { false: 'The finding is not about an escalation, or the wiki does not confirm a resolution for it' },
    )])),
  }));
  if (!result) return obsolete;
  const { answers } = result;
  issues.forEach((issue, index) => {
    if ((answers[`issue_${index}`]?.noul ?? 0) >= JUDGMENT_THRESHOLD) obsolete.add(issue);
  });
  return obsolete;
}
