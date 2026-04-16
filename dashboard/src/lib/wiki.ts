import fs from 'fs';
import path from 'path';

export function resolveWikiDir(): string {
  if (process.env.WIKI_DIR) return process.env.WIKI_DIR;

  const dataDir = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
  const sharedWikiDir = path.join(dataDir, 'knowledge');
  if (fs.existsSync(sharedWikiDir)) return sharedWikiDir;

  return path.join(process.cwd(), '..', 'knowledge');
}

export function getWikiDiagnostics() {
  const wikiDir = resolveWikiDir();
  const metaPath = path.join(wikiDir, '.meta.json');
  const historyDir = path.join(wikiDir, '.history');

  let meta: Record<string, unknown> | null = null;
  try {
    meta = JSON.parse(fs.readFileSync(metaPath, 'utf-8'));
  } catch {
    meta = null;
  }

  let pageCount = 0;
  try {
    pageCount = fs.readdirSync(wikiDir, { recursive: true })
      .filter((entry) => typeof entry === 'string' && entry.endsWith('.md'))
      .length;
  } catch {
    pageCount = 0;
  }

  return {
    cwd: process.cwd(),
    dataDir: process.env.DATA_DIR || path.join(process.cwd(), '..', 'data'),
    wikiDirEnv: process.env.WIKI_DIR || null,
    resolvedWikiDir: wikiDir,
    wikiDirExists: fs.existsSync(wikiDir),
    metaPath,
    metaExists: fs.existsSync(metaPath),
    historyDir,
    historyExists: fs.existsSync(historyDir),
    pageCount,
    meta,
  };
}
