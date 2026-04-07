import { NextRequest, NextResponse } from 'next/server';
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

interface SearchResult {
  path: string;
  title: string;
  snippets: string[];
}

export function GET(request: NextRequest) {
  try {
    const q = request.nextUrl.searchParams.get('q')?.toLowerCase();
    if (!q || q.length < 2) {
      return NextResponse.json({ error: 'Query must be at least 2 characters', results: [] }, { status: 400 });
    }

    const results: SearchResult[] = [];

    for (const pagePath of WIKI_PAGES) {
      const fullPath = path.join(WIKI_DIR, pagePath);
      let content = '';
      try {
        content = fs.readFileSync(fullPath, 'utf-8');
      } catch {
        continue;
      }

      const lowerContent = content.toLowerCase();
      if (!lowerContent.includes(q)) continue;

      // Extract title
      const titleMatch = content.match(/^#\s+(.+)/m);
      const title = titleMatch ? titleMatch[1] : pagePath.replace('.md', '');

      // Extract context snippets around matches
      const snippets: string[] = [];
      const lines = content.split('\n');
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].toLowerCase().includes(q)) {
          // Get surrounding context (1 line before + match + 1 line after)
          const start = Math.max(0, i - 1);
          const end = Math.min(lines.length - 1, i + 1);
          const snippet = lines.slice(start, end + 1).join('\n').trim();
          if (snippet.length > 0 && snippets.length < 3) {
            snippets.push(snippet.length > 300 ? snippet.slice(0, 300) + '...' : snippet);
          }
        }
      }

      results.push({ path: pagePath, title, snippets });
    }

    return NextResponse.json({ query: q, results });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, results: [] }, { status: 500 });
  }
}
