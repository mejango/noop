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
  const wordCount = replacement.split(/\s+/).filter(Boolean).length;
  if (wordCount > 2000) errors.push(`Replacement exceeds 2000 words (${wordCount})`);
  if (!/^\s*(?:#(?!#)[^\n]*\n+\s*)?\*\*[^\n]+\*\*/.test(replacement)) {
    errors.push('Replacement must keep a bold TLDR immediately after the optional H1');
  }
  const missingHeaders = (WIKI_EXPECTED_HEADERS[pagePath] || [])
    .filter((heading) => !replacement.split('\n').some((line) => line.trim() === `## ${heading}`));
  if (missingHeaders.length > 0) errors.push(`Replacement is missing sections: ${missingHeaders.join(', ')}`);

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
  return errors;
}
