export type WikiPageStatus = 'current' | 'stale' | 'needs_attention' | 'unreviewed' | 'missing';

export interface WikiPageDefinition {
  path: string;
  title: string;
  category: 'regimes' | 'protection' | 'revenue' | 'indicators' | 'strategy';
  freshnessDays: number;
  owner: 'wiki' | 'learning';
  briefing: boolean;
}

export interface StoredWikiPageMeta {
  last_checked_at?: string | null;
  last_evidence_at?: string | null;
  last_changed_at?: string | null;
  last_reviewed_at?: string | null;
  change_summary?: string | null;
  evidence_packet?: string | null;
  issues?: string[];
}

export const WIKI_PAGES: WikiPageDefinition[] = [
  { path: 'regimes/current.md', title: 'Current Regime', category: 'regimes', freshnessDays: 3, owner: 'wiki', briefing: true },
  { path: 'regimes/history.md', title: 'Regime History', category: 'regimes', freshnessDays: 90, owner: 'wiki', briefing: false },
  { path: 'protection/pricing.md', title: 'Protection Pricing', category: 'protection', freshnessDays: 3, owner: 'wiki', briefing: true },
  { path: 'protection/windows.md', title: 'Protection Windows', category: 'protection', freshnessDays: 14, owner: 'wiki', briefing: true },
  { path: 'protection/convexity.md', title: 'Convexity Map', category: 'protection', freshnessDays: 30, owner: 'wiki', briefing: false },
  { path: 'revenue/pricing.md', title: 'Premium Environment', category: 'revenue', freshnessDays: 3, owner: 'wiki', briefing: true },
  { path: 'revenue/windows.md', title: 'Premium Windows', category: 'revenue', freshnessDays: 14, owner: 'wiki', briefing: false },
  { path: 'revenue/efficiency.md', title: 'Call Efficiency', category: 'revenue', freshnessDays: 45, owner: 'wiki', briefing: false },
  { path: 'indicators/leading.md', title: 'Leading Indicators', category: 'indicators', freshnessDays: 30, owner: 'wiki', briefing: false },
  { path: 'indicators/correlations.md', title: 'Correlations', category: 'indicators', freshnessDays: 45, owner: 'wiki', briefing: false },
  { path: 'indicators/divergences.md', title: 'Divergences', category: 'indicators', freshnessDays: 14, owner: 'wiki', briefing: true },
  { path: 'strategy/lessons.md', title: 'Strategy Lessons', category: 'strategy', freshnessDays: 90, owner: 'learning', briefing: false },
  { path: 'strategy/mistakes.md', title: 'Mistakes & Anti-Patterns', category: 'strategy', freshnessDays: 90, owner: 'learning', briefing: false },
  { path: 'strategy/playbook.md', title: 'Strategy Playbook', category: 'strategy', freshnessDays: 90, owner: 'learning', briefing: true },
];

export function extractWikiTldr(content: string): string {
  const lines = content.split('\n').map((line) => line.trim()).filter(Boolean);
  const boldLine = lines.find((line) => line.startsWith('**') && line.endsWith('**'));
  if (boldLine) return boldLine.replace(/^\*\*/, '').replace(/\*\*$/, '').trim();
  return lines.find((line) => !line.startsWith('#')) || 'Awaiting assessment';
}

export function countWikiEvidenceReferences(content: string): number {
  const structured = content.match(/\[(?:source:\s*[^\]]+|(?:tick|order|review):#\d+|lesson:[^\]]+)\]/gi) || [];
  if (structured.length > 0) return new Set(structured.map((value) => value.toLowerCase())).size;
  return new Set(content.match(/\[\d{4}-\d{2}-\d{2}\]/g) || []).size;
}

export function deriveWikiPageStatus(args: {
  exists: boolean;
  lastModified: string;
  lastReviewed: string | null;
  issues: string[];
  freshnessDays: number;
  nowMs?: number;
}): WikiPageStatus {
  if (!args.exists) return 'missing';
  if (args.issues.length > 0) return 'needs_attention';
  if (!args.lastReviewed) return 'unreviewed';
  const changedMs = new Date(args.lastModified).getTime();
  const reviewedMs = new Date(args.lastReviewed).getTime();
  if (!Number.isFinite(reviewedMs) || reviewedMs < changedMs) return 'unreviewed';
  const ageMs = (args.nowMs ?? Date.now()) - changedMs;
  if (!Number.isFinite(ageMs) || ageMs > args.freshnessDays * 86400000) return 'stale';
  return 'current';
}
