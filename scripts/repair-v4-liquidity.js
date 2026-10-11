#!/usr/bin/env node
'use strict';

const Database = require('better-sqlite3');
const crypto = require('node:crypto');
const { sanitizeOnchainRow, calculateValidatedLiquidityFlow, unknownLiquidityFlow, LIQUIDITY_DATA_NOTICE } = require('../bot/liquidity-quality');
const { createHourlyRollups } = require('../bot/hourly-rollups');
const VERSION = 'v4-onchain-principal-2026-10-11-v1';

function nearest(samples, target) {
  let lo = 0, hi = samples.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (samples[mid].ms < target) lo = mid + 1;
    else hi = mid;
  }
  return [samples[lo - 1], samples[lo]].filter(Boolean).sort((a, b) => Math.abs(a.ms - target) - Math.abs(b.ms - target))[0];
}

function repairV4Liquidity(db, { apply = false, batchSize = 100 } = {}) {
  const maxId = db.prepare('SELECT MAX(id) AS id FROM onchain_data').get().id || 0;
  const samples = [];
  const stats = { version: VERSION, apply, rows: 0, legacyV4: 0, negativeBalances: 0, repairedRows: 0, repairedTicks: 0, rebuiltHours: 0 };
  let firstAffected = null, lastAffected = null;
  for (const row of db.prepare('SELECT id,timestamp,raw_data FROM onchain_data WHERE id <= ? ORDER BY timestamp,id').iterate(maxId)) {
    let raw;
    try { raw = JSON.parse(row.raw_data); } catch { continue; }
    if (!raw?.dexLiquidity?.dexes) continue;
    stats.rows++;
    const v4 = raw.dexLiquidity.dexes.uniswap_v4;
    if (v4 && v4.tvlDefinition !== 'core_liquidity_principal_v1') {
      stats.legacyV4++;
      if (v4.poolDetails?.some(pool => Number(pool.tvlToken0) < 0 || Number(pool.tvlToken1) < 0)) stats.negativeBalances++;
      firstAffected = !firstAffected || row.timestamp < firstAffected ? row.timestamp : firstAffected;
      lastAffected = !lastAffected || row.timestamp > lastAffected ? row.timestamp : lastAffected;
    }
    const clean = JSON.parse(sanitizeOnchainRow(row).raw_data).dexLiquidity;
    // Only scalar fields needed for flow, keeping a long-running database's
    // repair memory bounded independently of the size of its raw JSON blobs.
    const dexes = Object.fromEntries(Object.entries(clean.dexes).map(([name, dex]) => [name, {
      totalLiquidity: dex.totalLiquidity, pools: dex.pools, error: dex.error,
      invalid: dex.invalid, stale: dex.stale, tvlDefinition: dex.tvlDefinition,
    }]));
    samples.push({ timestamp: row.timestamp, ms: Date.parse(row.timestamp), dexes });
  }
  if (!apply) return { ...stats, firstAffected, lastAffected };

  db.exec(`
    CREATE TABLE IF NOT EXISTS liquidity_repair_archive (
      repair_version TEXT NOT NULL, source_table TEXT NOT NULL, source_id INTEGER NOT NULL,
      timestamp TEXT NOT NULL, original_row TEXT NOT NULL, sha256 TEXT NOT NULL,
      archived_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (repair_version,source_table,source_id)
    );
    CREATE TRIGGER IF NOT EXISTS liquidity_archive_no_update BEFORE UPDATE ON liquidity_repair_archive
      BEGIN SELECT RAISE(ABORT,'Liquidity audit archive is immutable'); END;
    CREATE TRIGGER IF NOT EXISTS liquidity_archive_no_delete BEFORE DELETE ON liquidity_repair_archive
      BEGIN SELECT RAISE(ABORT,'Liquidity audit archive is immutable'); END;
    CREATE TABLE IF NOT EXISTS liquidity_data_repairs (version TEXT PRIMARY KEY, receipt TEXT NOT NULL);
  `);
  const archive = db.prepare('INSERT OR IGNORE INTO liquidity_repair_archive (repair_version,source_table,source_id,timestamp,original_row,sha256) VALUES (?,?,?,?,?,?)');
  const preserve = (table, row) => {
    const original = JSON.stringify(row);
    archive.run(VERSION, table, row.id, row.timestamp, original, crypto.createHash('sha256').update(original).digest('hex'));
  };
  const update = db.prepare('UPDATE onchain_data SET raw_data=?,liquidity_flow_direction=?,liquidity_flow_magnitude=?,liquidity_flow_confidence=? WHERE id=?');
  const hours = new Set();
  const read = db.prepare('SELECT * FROM onchain_data WHERE id > ? AND id <= ? ORDER BY id LIMIT ?');
  const transaction = db.transaction(rows => {
    let count = 0;
    for (const row of rows) {
      let raw;
      try { raw = JSON.parse(row.raw_data); } catch { continue; }
      if (!raw?.dexLiquidity?.dexes || raw.dexLiquidity.repairVersion === VERSION) continue;
      const clean = JSON.parse(sanitizeOnchainRow(row).raw_data);
      const ms = Date.parse(row.timestamp);
      const history = [1, 24, 168].map(hoursAgo => nearest(samples, ms - hoursAgo * 3600000)).filter(Boolean);
      const flow = calculateValidatedLiquidityFlow({ dexes: clean.dexLiquidity.dexes }, history, ms);
      clean.dexLiquidity.flowAnalysis = flow;
      clean.dexLiquidity.repairVersion = VERSION;
      clean.dexLiquidity.dataQualityNotice = LIQUIDITY_DATA_NOTICE;
      preserve('onchain_data', row);
      update.run(JSON.stringify(clean), flow.direction, flow.magnitude, flow.confidence, row.id);
      hours.add(new Date(ms).toISOString().slice(0, 13) + ':00:00Z');
      count++;
    }
    return count;
  });
  for (let lastId = 0; lastId < maxId;) {
    const rows = read.all(lastId, maxId, batchSize);
    if (!rows.length) break;
    stats.repairedRows += transaction.immediate(rows);
    lastId = rows[rows.length - 1].id;
  }

  // Old tick summaries contain previously derived flow. Preserve the summaries
  // and invalidate only this signal; no fills, budgets or order state changes.
  if (firstAffected && db.prepare("SELECT 1 FROM sqlite_master WHERE name='bot_ticks'").get()) {
    const readTicks = db.prepare('SELECT * FROM bot_ticks WHERE timestamp >= ? AND timestamp <= ? AND id > ? ORDER BY id LIMIT ?');
    const updateTick = db.prepare('UPDATE bot_ticks SET summary=? WHERE id=?');
    const repairTicks = db.transaction(rows => {
      let count = 0;
      for (const row of rows) {
        let summary;
        try { summary = JSON.parse(row.summary); } catch { continue; }
        const parent = summary.onchain?.liquidity_flow ? summary.onchain : summary;
        if (!parent.liquidity_flow || summary.liquidityRepairVersion === VERSION) continue;
        preserve('bot_ticks', row);
        parent.liquidity_flow = unknownLiquidityFlow(['legacy_v4_tvl']);
        summary.liquidity_data_quality_notice = LIQUIDITY_DATA_NOTICE;
        summary.liquidityRepairVersion = VERSION;
        updateTick.run(JSON.stringify(summary), row.id);
        count++;
      }
      return count;
    });
    const from = new Date(Date.parse(firstAffected) - 3600000).toISOString();
    const to = new Date(Date.parse(lastAffected) + 3600000).toISOString();
    for (let lastId = 0;;) {
      const rows = readTicks.all(from, to, lastId, batchSize);
      if (!rows.length) break;
      stats.repairedTicks += repairTicks.immediate(rows);
      lastId = rows[rows.length - 1].id;
    }
  }
  // Leave a dated correction in the advisors' journal without rewriting their
  // earlier reasoning or presenting it as fresh, verified evidence.
  if (firstAffected && db.prepare("SELECT 1 FROM sqlite_master WHERE name='ai_journal'").get()) {
    const correction = `[${VERSION}] ${LIQUIDITY_DATA_NOTICE} Affected legacy records: ${firstAffected} through ${lastAffected}.`;
    db.transaction(() => {
      if (!db.prepare('SELECT 1 FROM ai_journal WHERE content=?').get(correction)) {
        db.prepare('INSERT INTO ai_journal (timestamp,entry_type,content,series_referenced) VALUES (?,?,?,?)')
          .run(new Date().toISOString(), 'observation', correction, JSON.stringify(['liquidity_flow']));
      }
    }).immediate();
  }
  const rollups = createHourlyRollups(db);
  // Rebuild only onchain hours, not the unrelated, much larger options tables.
  // Include leftover legacy rollup hours even if their raw data is absent.
  for (const row of db.prepare("SELECT DISTINCT hour FROM onchain_hourly WHERE dex='uniswap_v4'").all()) hours.add(row.hour);
  for (const hour of hours) {
    rollups.refreshOnchainHour(hour);
    stats.rebuiltHours++;
  }
  let badArchives = 0;
  for (const row of db.prepare('SELECT original_row,sha256 FROM liquidity_repair_archive WHERE repair_version=?').iterate(VERSION)) {
    if (crypto.createHash('sha256').update(row.original_row).digest('hex') !== row.sha256) badArchives++;
  }
  if (badArchives) throw new Error('Liquidity archive checksum verification failed');
  const receipt = { ...stats, firstAffected, lastAffected, completedAt: new Date().toISOString(), archiveVerified: true };
  db.prepare('INSERT OR REPLACE INTO liquidity_data_repairs (version,receipt) VALUES (?,?)').run(VERSION, JSON.stringify(receipt));
  return receipt;
}

function main(args = process.argv.slice(2)) {
  const index = args.indexOf('--db');
  if (index < 0 || !args[index + 1]) throw new Error('Use --db /explicit/path/noop.db [--apply]; default is a read-only audit');
  const apply = args.includes('--apply');
  const db = new Database(args[index + 1], { readonly: !apply, fileMustExist: true, timeout: 5000 });
  try { console.log(JSON.stringify(repairV4Liquidity(db, { apply }), null, 2)); } finally { db.close(); }
}
if (require.main === module) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
module.exports = { repairV4Liquidity, VERSION };
