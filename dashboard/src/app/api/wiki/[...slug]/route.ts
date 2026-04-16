import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { resolveWikiDir } from '@/lib/wiki';

export const dynamic = 'force-dynamic';

const WIKI_DIR = resolveWikiDir();
const HISTORY_DIR = path.join(WIKI_DIR, '.history');

const WIKI_PAGES = [
  'regimes/current.md',
  'regimes/history.md',
  'protection/pricing.md',
  'protection/windows.md',
  'protection/convexity.md',
  'revenue/pricing.md',
  'revenue/windows.md',
  'revenue/efficiency.md',
  'indicators/leading.md',
  'indicators/correlations.md',
  'indicators/divergences.md',
  'strategy/lessons.md',
  'strategy/mistakes.md',
  'strategy/playbook.md',
];

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ slug: string[] }> }
) {
  const { slug } = await params;
  if (!slug || slug.length === 0) {
    return NextResponse.json({ error: 'Page not found' }, { status: 404 });
  }

  try {
    const lastSegment = slug[slug.length - 1];
    const pagePath = slug.join('/') + (lastSegment.endsWith('.md') ? '' : '.md');

    if (!WIKI_PAGES.includes(pagePath)) {
      return NextResponse.json({ error: 'Page not found' }, { status: 404 });
    }

    const fullPath = path.join(WIKI_DIR, pagePath);
    let content = '';
    let lastModified = '';

    try {
      content = fs.readFileSync(fullPath, 'utf-8');
      const stat = fs.statSync(fullPath);
      lastModified = stat.mtime.toISOString();
    } catch {
      return NextResponse.json({ error: 'Page not found' }, { status: 404 });
    }

    // Find history versions
    const history: { timestamp: string; size: number }[] = [];
    try {
      if (fs.existsSync(HISTORY_DIR)) {
        const safeName = pagePath.replace(/\//g, '__');
        const files = fs.readdirSync(HISTORY_DIR)
          .filter(f => f.endsWith(safeName))
          .sort()
          .reverse()
          .slice(0, 20); // last 20 versions

        for (const file of files) {
          const stat = fs.statSync(path.join(HISTORY_DIR, file));
          // Extract timestamp from filename: 2026-04-07T12-00-00-000Z__regimes__current.md
          const tsMatch = file.match(/^(.+?)__/);
          const timestamp = tsMatch
            ? tsMatch[1].replace(/-(\d{2})-(\d{2})-(\d{3})Z/, ':$1:$2.$3Z')
            : stat.mtime.toISOString();
          history.push({ timestamp, size: stat.size });
        }
      }
    } catch { /* history not available */ }

    const wordCount = content.split(/\s+/).filter(Boolean).length;
    const titleMatch = content.match(/^#\s+(.+)/m);
    const title = titleMatch ? titleMatch[1] : pagePath.replace('.md', '');

    return NextResponse.json({
      path: pagePath,
      title,
      content,
      wordCount,
      lastModified,
      history,
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
