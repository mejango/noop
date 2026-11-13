import { NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { resolveWikiDir } from '@/lib/wiki';

export const dynamic = 'force-dynamic';

// Read-only view of a staged wiki recompile (bot/wiki-recompile.js), for comparing
// its drafts with the live pages before anything is applied.
const readJson = (file: string): unknown => {
  try { return JSON.parse(fs.readFileSync(file, 'utf-8')); } catch { return null; }
};
const readText = (file: string): string | null => {
  try { return fs.readFileSync(file, 'utf-8'); } catch { return null; }
};
const words = (content: string | null) => (content ? content.trim().split(/\s+/).filter(Boolean).length : 0);

const PAGES = [
  'regimes/history.md', 'protection/windows.md', 'protection/convexity.md', 'revenue/windows.md',
  'revenue/efficiency.md', 'indicators/leading.md', 'indicators/correlations.md', 'indicators/divergences.md',
];

export function GET(request: Request) {
  const run = new URL(request.url).searchParams.get('run') || '';
  if (!/^[A-Za-z0-9_-]{1,40}$/.test(run)) return NextResponse.json({ error: 'Invalid run id' }, { status: 400 });
  const wikiDir = resolveWikiDir();
  const stagingDir = path.join(wikiDir, '.recompile', run);
  if (!fs.existsSync(stagingDir)) return NextResponse.json({ error: 'No such run' }, { status: 404 });
  return NextResponse.json({
    run,
    progress: readJson(path.join(stagingDir, 'progress.json')),
    done: readJson(path.join(stagingDir, 'done.json')),
    applied: readJson(path.join(stagingDir, 'applied.json')),
    pages: PAGES.map((pagePath) => {
      const staged = readText(path.join(stagingDir, pagePath));
      const live = readText(path.join(wikiDir, pagePath));
      return { path: pagePath, stagedWords: words(staged), liveWords: words(live), staged, live };
    }),
  });
}
