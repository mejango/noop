import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';

export const dynamic = 'force-dynamic';

const WIKI_DIR = process.env.WIKI_DIR || path.join(process.cwd(), '..', 'knowledge');

const WIKI_PAGES: { path: string; title: string }[] = [
  { path: 'regimes/current.md', title: 'Current Regime' },
  { path: 'regimes/history.md', title: 'Regime History' },
  { path: 'protection/pricing.md', title: 'Protection Pricing' },
  { path: 'protection/windows.md', title: 'Protection Windows' },
  { path: 'protection/convexity.md', title: 'Convexity Map' },
  { path: 'revenue/pricing.md', title: 'Premium Environment' },
  { path: 'revenue/windows.md', title: 'Premium Windows' },
  { path: 'revenue/efficiency.md', title: 'Call Efficiency' },
  { path: 'indicators/leading.md', title: 'Leading Indicators' },
  { path: 'indicators/correlations.md', title: 'Correlations' },
  { path: 'indicators/divergences.md', title: 'Divergences' },
  { path: 'strategy/lessons.md', title: 'Strategy Lessons' },
  { path: 'strategy/mistakes.md', title: 'Mistakes & Anti-Patterns' },
  { path: 'strategy/playbook.md', title: 'Strategy Playbook' },
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

    const pages: WikiPageMeta[] = WIKI_PAGES.map((page) => {
      const fullPath = path.join(WIKI_DIR, page.path);
      let content = '';
      let lastModified = new Date().toISOString();

      try {
        content = fs.readFileSync(fullPath, 'utf-8');
        const stat = fs.statSync(fullPath);
        lastModified = stat.mtime.toISOString();
      } catch {
        // File doesn't exist yet
      }

      const wordCount = content.split(/\s+/).filter(Boolean).length;
      const stale = now - new Date(lastModified).getTime() > STALE_THRESHOLD_MS;
      const category = page.path.split('/')[0];

      return { path: page.path, title: page.title, category, wordCount, lastModified, stale };
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
