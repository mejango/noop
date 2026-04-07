import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export const dynamic = 'force-dynamic';

const WIKI_DIR = process.env.WIKI_DIR || path.join(process.cwd(), '..', 'knowledge');

const WIKI_PAGES = [
  'regimes/current.md',
  'regimes/history.md',
  'protection/pricing.md',
  'protection/windows.md',
  'protection/convexity.md',
  'indicators/leading.md',
  'indicators/correlations.md',
  'indicators/divergences.md',
  'strategy/lessons.md',
  'strategy/mistakes.md',
  'strategy/playbook.md',
];

interface WikiPageMeta {
  path: string;
  title: string;
  category: string;
  wordCount: number;
  lastModified: string;
  stale: boolean;
}

export function GET() {
  try {
    const STALE_THRESHOLD_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
    const now = Date.now();

    const pages: WikiPageMeta[] = WIKI_PAGES.map((pagePath) => {
      const fullPath = path.join(WIKI_DIR, pagePath);
      let content = '';
      let lastModified = new Date().toISOString();

      try {
        content = fs.readFileSync(fullPath, 'utf-8');
        const stat = fs.statSync(fullPath);
        lastModified = stat.mtime.toISOString();
      } catch {
        // File doesn't exist yet
      }

      // Extract title from first heading
      const titleMatch = content.match(/^#\s+(.+)/m);
      const title = titleMatch ? titleMatch[1] : pagePath.replace('.md', '');

      const wordCount = content.split(/\s+/).filter(Boolean).length;
      const stale = now - new Date(lastModified).getTime() > STALE_THRESHOLD_MS;
      const category = pagePath.split('/')[0];

      return { path: pagePath, title, category, wordCount, lastModified, stale };
    });

    // Read meta for additional info
    let meta = {};
    try {
      meta = JSON.parse(fs.readFileSync(path.join(WIKI_DIR, '.meta.json'), 'utf-8'));
    } catch { /* no meta yet */ }

    return NextResponse.json({ pages, meta });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, pages: [] }, { status: 500 });
  }
}
