import { NextRequest, NextResponse } from 'next/server';
import fs from 'fs';
import path from 'path';
import { getWikiDiagnostics, resolveWikiDir } from '@/lib/wiki';

export const dynamic = 'force-dynamic';

const WIKI_DIR = resolveWikiDir();
const WIKI_META_PATH = path.join(WIKI_DIR, '.meta.json');
const WIKI_HISTORY_DIR = path.join(WIKI_DIR, '.history');
const WIKI_ALL_PAGES = [
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

function readWikiPage(pagePath: string): string {
  try {
    return fs.readFileSync(path.join(WIKI_DIR, pagePath), 'utf-8');
  } catch {
    return '';
  }
}

function readWikiMeta(): Record<string, unknown> {
  try {
    return JSON.parse(fs.readFileSync(WIKI_META_PATH, 'utf-8'));
  } catch {
    return {};
  }
}

function writeWikiMeta(meta: Record<string, unknown>) {
  fs.writeFileSync(WIKI_META_PATH, JSON.stringify(meta, null, 2));
}

function saveWikiHistory(pagePath: string, content: string) {
  try {
    fs.mkdirSync(WIKI_HISTORY_DIR, { recursive: true });
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const safeName = pagePath.replace(/\//g, '__').replace(/\.md$/, '');
    fs.writeFileSync(path.join(WIKI_HISTORY_DIR, `${safeName}__${ts}.md`), content);
  } catch {
    // non-fatal
  }
}

async function callAnthropic(prompt: string): Promise<string> {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) throw new Error('ANTHROPIC_API_KEY not configured');
  const model = process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-6';

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model,
      max_tokens: 4096,
      messages: [{ role: 'user', content: prompt }],
    }),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Anthropic API ${response.status}: ${text || response.statusText}`);
  }

  const data = await response.json() as { content?: Array<{ text?: string }> };
  return data.content?.[0]?.text || '';
}

export async function POST(request: NextRequest) {
  try {
    const configuredSecret = process.env.WIKI_LINT_SECRET;
    if (configuredSecret) {
      const provided = request.headers.get('x-wiki-lint-secret');
      if (provided !== configuredSecret) {
        return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
      }
    }

    let force = true;
    try {
      const body = await request.json();
      if (typeof body?.force === 'boolean') force = body.force;
    } catch {
      // empty body is fine; force defaults true for manual trigger
    }

    const before = getWikiDiagnostics();
    const meta = readWikiMeta();
    const LINT_INTERVAL_MS = 20 * 60 * 60 * 1000;
    const lastLint = typeof meta.last_lint === 'string' ? meta.last_lint : null;

    if (!force && lastLint && Date.now() - new Date(lastLint).getTime() < LINT_INTERVAL_MS) {
      return NextResponse.json({
        ok: true,
        skipped: 'throttled',
        before,
        after: getWikiDiagnostics(),
      });
    }

    const pages: Record<string, string> = {};
    let hasContent = false;
    for (const page of WIKI_ALL_PAGES) {
      pages[page] = readWikiPage(page);
      if (pages[page] && !pages[page].includes('Awaiting initial assessment')) hasContent = true;
    }

    if (!hasContent) {
      return NextResponse.json({
        ok: true,
        skipped: 'wiki_not_seeded',
        before,
        after: getWikiDiagnostics(),
      });
    }

    const schema = readWikiPage('schema.md');
    const pagesContext = Object.entries(pages)
      .map(([p, content]) => `--- ${p} ---\n${content}`)
      .join('\n\n');

    const prompt = `You are auditing a knowledge wiki for a Spitznagel-style tail-risk hedging bot. Check for quality issues.

## Wiki Schema
${schema}

## Current Wiki Pages
${pagesContext}

## Audit Checklist
1. Contradictions: Do any pages contradict each other?
2. Staleness: Are any observations older than 7 days without recent updates?
3. Redundancy: Is the same information repeated across pages?
4. Missing links: Do pages reference concepts that should be in another page but aren't?
5. Quality: Are TLDRs accurate? Are evidence values specific?

## Instructions
Return a JSON object with:
{
  "issues": [{"page": "path", "type": "contradiction|stale|redundant|missing_link|quality", "description": "..."}],
  "updates": [{"page": "path", "content": "full updated page content"}]
}

Only include updates for pages that genuinely need fixing. If no issues found, return {"issues":[],"updates":[]}.
Wrap your JSON in a <lint_result> tag.`;

    const text = await callAnthropic(prompt);
    const lintMatch = text.match(/<lint_result>([\s\S]*?)<\/lint_result>/);

    if (!lintMatch) {
      meta.last_lint = new Date().toISOString();
      writeWikiMeta(meta);
      return NextResponse.json({
        ok: true,
        result: 'no_structured_result',
        before,
        after: getWikiDiagnostics(),
      });
    }

    let result: { issues?: Array<{ page?: string; type?: string; description?: string }>; updates?: Array<{ page?: string; content?: string }> };
    try {
      result = JSON.parse(lintMatch[1].trim());
    } catch (e) {
      meta.last_lint = new Date().toISOString();
      writeWikiMeta(meta);
      return NextResponse.json({
        ok: true,
        result: 'malformed_json',
        parseError: e instanceof Error ? e.message : 'Unknown parse error',
        before,
        after: getWikiDiagnostics(),
      });
    }

    let updateCount = 0;
    for (const update of (result.updates || [])) {
      const pagePath = update.page;
      const newContent = update.content?.trim();
      if (!pagePath || !newContent) continue;
      if (!WIKI_ALL_PAGES.includes(pagePath)) continue;
      if (newContent.length < 50) continue;

      const existingContent = pages[pagePath] || '';
      if (existingContent.length > 100 && newContent.length < existingContent.length * 0.5) continue;

      if (existingContent && !existingContent.includes('Awaiting initial assessment')) {
        saveWikiHistory(pagePath, existingContent);
      }

      fs.writeFileSync(path.join(WIKI_DIR, pagePath), newContent);
      updateCount++;
    }

    meta.last_lint = new Date().toISOString();
    meta.last_lint_issues = result.issues?.length || 0;
    meta.last_lint_updates = updateCount;
    writeWikiMeta(meta);

    return NextResponse.json({
      ok: true,
      force,
      issues: result.issues || [],
      updateCount,
      before,
      after: getWikiDiagnostics(),
    });
  } catch (e: unknown) {
    const message = e instanceof Error ? e.message : 'Unknown error';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
