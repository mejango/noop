import Database from 'better-sqlite3';
import path from 'path';

const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'noop.db');

let db: Database.Database | null = null;

function getWriteDb(): Database.Database {
  if (!db) {
    db = new Database(DB_PATH);
    db.pragma('journal_mode = WAL');
    db.pragma('busy_timeout = 5000');
    // Ensure table exists (idempotent)
    db.exec(`
      CREATE TABLE IF NOT EXISTS ai_journal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        entry_type TEXT NOT NULL,
        content TEXT NOT NULL,
        series_referenced TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      );
      CREATE INDEX IF NOT EXISTS idx_ai_journal_timestamp ON ai_journal(timestamp);
    `);
  }
  return db;
}

// Matches option instrument names (e.g. ETH-20260313-2200-C)
const hasPositionRef = (s: string) => /ETH-\d{8}-\d+-[PC]/.test(s);

export function insertJournalEntry(
  entryType: string,
  content: string,
  seriesReferenced: string[] | null
): void {
  let sanitized = content;

  // Entries must not contain position references.
  // Strip sentences containing instrument names before storing.
  if (hasPositionRef(content)) {
    sanitized = content
      .split(/(?<=\.)\s+/)
      .filter(sentence => !hasPositionRef(sentence))
      .join(' ')
      .trim();

    // If stripping gutted the entry, skip storage entirely
    if (!sanitized || sanitized.length < 20) return;
  }

  const d = getWriteDb();
  d.prepare(`
    INSERT INTO ai_journal (timestamp, entry_type, content, series_referenced)
    VALUES (?, ?, ?, ?)
  `).run(
    new Date().toISOString(),
    entryType,
    sanitized,
    seriesReferenced ? JSON.stringify(seriesReferenced) : null
  );
}
