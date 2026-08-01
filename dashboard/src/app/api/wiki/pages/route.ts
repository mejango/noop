import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { resolveWikiDir } from '@/lib/wiki';
import {
  WIKI_PAGES,
  countWikiEvidenceReferences,
  deriveWikiPageStatus,
  extractWikiTldr,
  type StoredWikiPageMeta,
} from '@/lib/wikiCatalog';

export const dynamic = 'force-dynamic';

const WIKI_DIR = resolveWikiDir();

interface WikiPageMeta {
  path: string;
  title: string;
  category: string;
  wordCount: number;
  lastModified: string;
  lastReviewed: string | null;
  lastEvidenceAt: string | null;
  lastCheckedAt: string | null;
  summary: string;
  status: ReturnType<typeof deriveWikiPageStatus>;
  issues: string[];
  changeSummary: string | null;
  evidenceCount: number;
  freshnessDays: number;
  owner: 'wiki' | 'learning';
  briefing: boolean;
}

export function GET() {
  try {
    let meta: Record<string, unknown> = {};
    try {
      meta = JSON.parse(fs.readFileSync(path.join(WIKI_DIR, '.meta.json'), 'utf-8'));
    } catch { /* no meta yet */ }

    const storedPages = meta.pages && typeof meta.pages === 'object'
      ? meta.pages as Record<string, StoredWikiPageMeta>
      : {};

    const pages: WikiPageMeta[] = WIKI_PAGES.map((page) => {
      const fullPath = path.join(WIKI_DIR, page.path);
      let content = '';
      let lastModified = new Date(0).toISOString();
      let exists = false;

      try {
        content = fs.readFileSync(fullPath, 'utf-8');
        const stat = fs.statSync(fullPath);
        lastModified = stat.mtime.toISOString();
        exists = true;
      } catch {
        // File doesn't exist yet
      }

      const wordCount = content.split(/\s+/).filter(Boolean).length;
      const stored = storedPages[page.path] || {};
      const issues = Array.isArray(stored.issues) ? stored.issues.filter((issue): issue is string => typeof issue === 'string') : [];
      const lastReviewed = typeof stored.last_reviewed_at === 'string' ? stored.last_reviewed_at : null;
      const lastEvidenceAt = typeof stored.last_evidence_at === 'string' ? stored.last_evidence_at : null;
      const lastCheckedAt = typeof stored.last_checked_at === 'string' ? stored.last_checked_at : null;
      const lastChangedAt = typeof stored.last_changed_at === 'string' ? stored.last_changed_at : lastModified;
      const status = deriveWikiPageStatus({
        exists,
        lastModified: lastEvidenceAt || lastChangedAt,
        lastReviewed,
        issues,
        freshnessDays: page.freshnessDays,
      });

      return {
        ...page,
        wordCount,
        lastModified: lastChangedAt,
        lastReviewed,
        lastEvidenceAt,
        lastCheckedAt,
        summary: extractWikiTldr(content),
        status,
        issues,
        changeSummary: typeof stored.change_summary === 'string' ? stored.change_summary : null,
        evidenceCount: countWikiEvidenceReferences(content),
      };
    });

    const counts = pages.reduce<Record<string, number>>((acc, page) => {
      acc[page.status] = (acc[page.status] || 0) + 1;
      return acc;
    }, {});
    const briefing = pages.filter((page) => page.briefing);
    const attention = pages.filter((page) => page.status !== 'current');
    const recentChanges = pages
      .filter((page) => page.changeSummary)
      .sort((a, b) => new Date(b.lastModified).getTime() - new Date(a.lastModified).getTime())
      .slice(0, 6);

    return NextResponse.json({
      pages,
      briefing,
      attention,
      recentChanges,
      summary: {
        current: counts.current || 0,
        stale: counts.stale || 0,
        needsAttention: counts.needs_attention || 0,
        unreviewed: counts.unreviewed || 0,
        missing: counts.missing || 0,
        lastLint: typeof meta.last_lint === 'string' ? meta.last_lint : null,
        lastIngest: typeof meta.last_ingest === 'string' ? meta.last_ingest : null,
      },
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, pages: [] }, { status: 500 });
  }
}
