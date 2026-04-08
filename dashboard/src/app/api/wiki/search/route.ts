import { NextRequest, NextResponse } from 'next/server';
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
  { path: 'indicators/leading.md', title: 'Leading Indicators' },
  { path: 'indicators/correlations.md', title: 'Correlations' },
  { path: 'indicators/divergences.md', title: 'Divergences' },
  { path: 'strategy/lessons.md', title: 'Strategy Lessons' },
  { path: 'strategy/mistakes.md', title: 'Mistakes & Anti-Patterns' },
  { path: 'strategy/playbook.md', title: 'Strategy Playbook' },
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

    for (const page of WIKI_PAGES) {
      const fullPath = path.join(WIKI_DIR, page.path);
      let content = '';
      try {
        content = fs.readFileSync(fullPath, 'utf-8');
      } catch {
        continue;
      }

      const lowerContent = content.toLowerCase();
      if (!lowerContent.includes(q)) continue;

      // Extract context snippets around matches
      const snippets: string[] = [];
      const lines = content.split('\n');
      for (let i = 0; i < lines.length; i++) {
        if (lines[i].toLowerCase().includes(q)) {
          const start = Math.max(0, i - 1);
          const end = Math.min(lines.length - 1, i + 1);
          const snippet = lines.slice(start, end + 1).join('\n').trim();
          if (snippet.length > 0 && snippets.length < 3) {
            snippets.push(snippet.length > 300 ? snippet.slice(0, 300) + '...' : snippet);
          }
        }
      }

      results.push({ path: page.path, title: page.title, snippets });
    }

    return NextResponse.json({ query: q, results });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message, results: [] }, { status: 500 });
  }
}
