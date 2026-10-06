#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

// Both connections are read-only. Hash every row rather than relying on counts.
function fingerprint(filename) {
  const db = new Database(filename, { readonly: true, fileMustExist: true });
  try {
    const schema = db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY type,name').all();
    const tables = {};
    for (const { name, sql } of schema.filter(row => row.type === 'table')) {
      const quoted = `"${name.replaceAll('"', '""')}"`;
      const columns = db.prepare(`PRAGMA table_info(${quoted})`).all();
      const primary = columns.filter(c => c.pk).sort((a, b) => a.pk - b.pk);
      const order = /WITHOUT\s+ROWID/i.test(sql)
        ? primary.map(c => `"${c.name.replaceAll('"', '""')}"`).join(',') : 'rowid';
      const hash = crypto.createHash('sha256');
      let count = 0;
      for (const row of db.prepare(`SELECT * FROM ${quoted} ORDER BY ${order}`).raw().iterate()) {
        hash.update(JSON.stringify(row)); hash.update('\n'); count++;
      }
      tables[name] = { count, sha256: hash.digest('hex') };
    }
    return { schema, tables };
  } finally { db.close(); }
}

function compare(before, after) {
  const differences = [];
  if (JSON.stringify(before.schema) !== JSON.stringify(after.schema)) differences.push('schema');
  for (const name of new Set([...Object.keys(before.tables), ...Object.keys(after.tables)])) {
    if (JSON.stringify(before.tables[name]) !== JSON.stringify(after.tables[name])) differences.push(name);
  }
  return { identical: !differences.length, differences, before: before.tables, after: after.tables };
}

if (require.main === module) {
  const [beforeFile, afterFile, reportFile] = process.argv.slice(2);
  if (!beforeFile || !afterFile || !reportFile) throw new Error('Usage: node scripts/verify-db-continuity.js <backup.db> <paused-live.db> <new-report.json>');
  const report = { checked_at: new Date().toISOString(), ...compare(fingerprint(beforeFile), fingerprint(afterFile)) };
  fs.writeFileSync(reportFile, JSON.stringify(report, null, 2) + '\n', { mode: 0o600, flag: 'wx' });
  console.log(JSON.stringify({ identical: report.identical, differences: report.differences, tables: Object.keys(report.before).length }));
  if (!report.identical) process.exitCode = 1;
}
module.exports = { fingerprint, compare };
