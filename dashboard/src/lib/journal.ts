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

export function insertJournalEntry(
  entryType: string,
  content: string,
  seriesReferenced: string[] | null
): void {
  const d = getWriteDb();
  d.prepare(`
    INSERT INTO ai_journal (timestamp, entry_type, content, series_referenced)
    VALUES (?, ?, ?, ?)
  `).run(
    new Date().toISOString(),
    entryType,
    content,
    seriesReferenced ? JSON.stringify(seriesReferenced) : null
  );
}
