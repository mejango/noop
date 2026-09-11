#!/usr/bin/env node
'use strict';

// Import explicit, source-backed V2 account evidence. Does not contact the venue
// or infer history completeness from the presence of local order rows.
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { createEconomicStore } = require('../bot/economic-events');
function importEvidence(db, input) {
  if (!input || input.schema_version !== 1 || !Array.isArray(input.events) || !input.coverage) throw new Error('Expected schema_version=1, events and explicit coverage evidence');
  if (input.coverage.complete === true && !input.coverage.evidence_reference) throw new Error('Complete coverage requires an evidence_reference to the exhaustive venue export');
  return createEconomicStore(db).recordBatch(input.events,input.coverage);
}
if (require.main === module) {
  const [dbPath, inputPath] = process.argv.slice(2);
  if (!dbPath || !inputPath) throw new Error('Usage: node scripts/import-economic-events.js <existing-noop.db> <evidence.json>');
  const input = JSON.parse(fs.readFileSync(path.resolve(inputPath),'utf8'));
  const db = new Database(path.resolve(dbPath),{fileMustExist:true});
  try { console.log(JSON.stringify({inserted:importEvidence(db,input)})); } finally { db.close(); }
}
module.exports = { importEvidence };
