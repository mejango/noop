'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { readProfile } = require('./profile');
const { stateDirectory, acquireWriter } = require('./state');
const { assertIsolatedDataPaths } = require('./isolation');

function assertIndependentTree(dir) {
  const stat = fs.lstatSync(dir);
  if (stat.isSymbolicLink()) throw new Error('Knowledge copy must not contain symlinks to shared state');
  if (stat.isDirectory()) {
    for (const entry of fs.readdirSync(dir)) assertIndependentTree(path.join(dir, entry));
  } else if (!stat.isFile()) throw new Error('Knowledge copy contains an unsupported file type');
}

async function prepareState(profile, sourceDb, sourceWiki) {
  if (profile.name !== 'v3-mainnet' || !profile.ownerAddress || !profile.subaccountId) throw new Error('An explicit verified V3 mainnet identity is required');
  if (profile.executionEnabled) throw new Error('Disable V3 execution while preparing the state');
  const Database = require('better-sqlite3');
  const dir = stateDirectory(profile);
  const dataDir = path.join(dir, 'data');
  const dbPath = path.join(dataDir, 'noop.db');
  const release = acquireWriter(profile);
  let source;
  try {
    assertIsolatedDataPaths(profile, { DATA_DIR: dataDir, WIKI_DIR: path.join(dir, 'knowledge') }, path.resolve(__dirname, '../..'));
    if (fs.existsSync(dbPath)) throw new Error('Prepared ledger already exists; refusing to overwrite it');
    const wiki = fs.realpathSync(sourceWiki);
    if (dir.startsWith(wiki + path.sep) || wiki.startsWith(dir + path.sep) || wiki === dir) throw new Error('Source/destination state directories overlap');
    assertIndependentTree(wiki);
    source = new Database(fs.realpathSync(sourceDb), { readonly: true, fileMustExist: true });
    const open = source.prepare("SELECT COUNT(*) AS n FROM resting_orders WHERE status = 'open'").get().n;
    const pending = source.prepare("SELECT COUNT(*) AS n FROM pending_actions WHERE status IN ('pending', 'confirmed', 'executing', 'resting')").get().n;
    if (open || pending) throw new Error('Reconcile V2 resting orders and pending actions before importing state');
    fs.mkdirSync(dataDir, { recursive: true, mode: 0o700 });
    // SQLite backup includes committed WAL state; copying noop.db alone does not.
    await source.backup(dbPath);
    const copy = new Database(dbPath);
    try {
      copy.transaction(() => {
        for (const table of ['orders', 'portfolio_snapshots', 'resting_orders', 'pending_actions']) {
          if (!copy.prepare(`PRAGMA table_info(${table})`).all().some(c => c.name === 'venue')) {
            copy.exec(`ALTER TABLE ${table} ADD COLUMN venue TEXT NOT NULL DEFAULT 'v3-mainnet'`);
            copy.exec(`UPDATE ${table} SET venue = 'v2'`);
          }
        }
      })();
    } finally { copy.close(); }
    fs.cpSync(wiki, path.join(dir, 'knowledge'), { recursive: true, errorOnExist: true, force: false });
    const manifest = { timestamp: new Date().toISOString(), owner: profile.ownerAddress.toLowerCase(),
      subaccount_id: profile.subaccountId, source_db: fs.realpathSync(sourceDb), source_wiki: wiki,
      note: 'Operator must keep V2 stopped after this backup. On-chain balances require a separate fresh handoff comparison.' };
    fs.writeFileSync(path.join(dir, 'prepared-state.json'), JSON.stringify(manifest, null, 2), { flag: 'wx', mode: 0o600 });
    return { data_dir: dataDir, knowledge_dir: path.join(dir, 'knowledge') };
  } finally { if (source) source.close(); release(); }
}

if (require.main === module) {
  const [sourceDb, sourceWiki] = process.argv.slice(2);
  if (!sourceDb || !sourceWiki) { console.error('Usage: prepare-state.js /path/to/stopped-v2/noop.db /path/to/knowledge'); process.exitCode = 1; }
  else prepareState(readProfile(), sourceDb, sourceWiki).then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}

module.exports = { prepareState };
