import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

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

function getConsecutiveTickRequirements(content: string): Set<number> {
  return new Set(
    Array.from(
      content.matchAll(/(?:≥|>=|at\s+least)?\s*`?(\d+)`?\s+consecutive\s+ticks/gi),
      (match) => Number(match[1]),
    ).filter(Number.isFinite),
  );
}

const REFERENCE_ONLY_SECTIONS: Record<string, Record<string, string>> = {
  'revenue/pricing.md': {
    'Skew & IV Context': 'Current skew and IV readings are perishable. Consult protection/pricing.md and regimes/current.md for current values.',
  },
};

function getEscalationTopics(content: string): Set<string> {
  return new Set(content.match(/\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b/g) || []);
}

function hasResolvedEscalationForTopic(content: string, topic: string): boolean {
  return content.split(/\n\s*\n/).some((paragraph) => (
    paragraph.includes(topic)
    && /(?:ESCALATION\s*(?:—|-)\s*RESOLVED|resolution confirmed)/i.test(paragraph)
  ));
}

export function isObsoleteUnresolvedEscalationIssue(
  issue: string,
  referencedContent: string,
): boolean {
  if (!/(?:open\s+unresolved|unresolved\s+(?:cross-page\s+)?escalation|still\s+open)/i.test(issue)) return false;
  const topics = getEscalationTopics(issue);
  return Array.from(topics).some((topic) => hasResolvedEscalationForTopic(referencedContent, topic));
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

export function validateWikiReplacement(args: {
  pagePath: string;
  previousContent: string;
  replacementContent: string;
  allowedMarkerContent: string;
  canonicalLessonContent: string;
}): string[] {
  const { pagePath, previousContent, replacementContent, allowedMarkerContent, canonicalLessonContent } = args;
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

  const previousParagraphs = new Set(
    previousContent.split(/\n\s*\n/).map(normalizeProse).filter(Boolean),
  );
  const revivedResolvedTopics = new Set<string>();
  replacement.split(/\n\s*\n/).forEach((paragraph) => {
    const normalized = normalizeProse(paragraph);
    if (previousParagraphs.has(normalized)) return;
    if (!/(?:open\s+unresolved|unresolved\s+(?:cross-page\s+)?escalation|still\s+open)/i.test(paragraph)) return;
    getEscalationTopics(paragraph).forEach((topic) => {
      if (hasResolvedEscalationForTopic(allowedMarkerContent, topic)) revivedResolvedTopics.add(topic);
    });
  });
  if (revivedResolvedTopics.size > 0) {
    errors.push(`Replacement revives resolved escalations: ${Array.from(revivedResolvedTopics).join(', ')}`);
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

  const allowedTickRequirements = getConsecutiveTickRequirements(
    `${previousContent}\n${allowedMarkerContent}\n${canonicalLessonContent}`,
  );
  const inventedTickRequirements = Array.from(getConsecutiveTickRequirements(replacement))
    .filter((count) => !allowedTickRequirements.has(count));
  if (inventedTickRequirements.length > 0) {
    errors.push(`Replacement invents a consecutive-tick gate rule: ${inventedTickRequirements.join(', ')} ticks`);
  }

  // A targeted repair may replace stale evidence, but it must not silently make
  // the page less current by dropping the newest tick cited anywhere on it.
  const previousLatestTick = latestTickId(previousContent);
  const replacementLatestTick = latestTickId(replacement);
  if (previousLatestTick != null && (replacementLatestTick == null || replacementLatestTick < previousLatestTick)) {
    errors.push(`Replacement drops newest tick evidence [tick:#${previousLatestTick}]`);
  }
  return errors;
}
