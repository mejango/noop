'use strict';

const fs = require('node:fs');
const path = require('node:path');
const { readProfile, loadKey, V2_IDENTITY } = require('./profile');
const { stateDirectory, acquireWriter, openJournal } = require('./state');
const { SNAPSHOT_SCHEMA_VERSION } = require('./handoff');
const { assertIsolatedDataPaths } = require('./isolation');

function validateHandoffReport(handoff, profile, { alreadyActivated = false, now = Date.now() } = {}) {
  const age = now - Date.parse(handoff?.timestamp);
  if (handoff?.schema_version !== SNAPSHOT_SCHEMA_VERSION || handoff.account_comparison_passed !== true
    || !Array.isArray(handoff.blockers) || handoff.blockers.length
    || (!alreadyActivated && (!Number.isFinite(age) || age < -30000 || age > 300000))
    || handoff.owner?.toLowerCase() !== profile.ownerAddress.toLowerCase() || handoff.subaccount_id !== profile.subaccountId
    || handoff.v2_identity?.owner?.toLowerCase() !== V2_IDENTITY.owner.toLowerCase()
    || handoff.v2_identity?.subaccount_id !== V2_IDENTITY.subaccount_id) {
    throw new Error('Handoff account comparison is stale, incomplete, failed, or for a different identity');
  }
}

async function main({ adapterFactory, startBot = () => require('../../bot/index'),
  initializeDatabase = () => require('../../bot/db').db.pragma('synchronous = FULL') } = {}) {
  const mainnet = process.argv.includes('--mainnet');
  if (mainnet) {
    if (process.env.NOOP_VENUE !== 'v3-mainnet') throw new Error('Mainnet runner requires NOOP_VENUE=v3-mainnet');
  } else {
    const env = require('./testnet-config').testnetEnvironment();
    for (const [key, value] of Object.entries(env)) if (value != null) process.env[key] = value;
  }
  const profile = readProfile();
  if (!profile.ownerAddress || !profile.subaccountId) throw new Error('Configure the dedicated V3 account first');
  loadKey(profile);
  const dir = stateDirectory(profile);
  // Own the account before reading journal/preparation state. A competing CLI
  // writer must not be able to add an unresolved intent after these checks.
  const unlock = acquireWriter(profile);
  let released = false;
  const release = () => { if (!released) { released = true; process.removeListener('exit', release); unlock(); } };
  process.once('exit', release);
  let createdActivation = null;
  try {
    const { DeriveV3 } = require('./index');
    const adapter = adapterFactory ? adapterFactory(profile) : new DeriveV3({ profile, authenticated: true });
    try { await adapter.account(); } finally { await adapter.close(); }
    const journal = openJournal(profile);
    if (journal.unresolved.length) throw new Error('Reconcile unresolved V3 order intents before starting the bot');
    const activationFile = path.join(dir, 'activation.json');
    let activationReport;
    if (mainnet) {
      const prepared = JSON.parse(fs.readFileSync(path.join(dir, 'prepared-state.json'), 'utf8'));
      if (prepared.owner !== profile.ownerAddress.toLowerCase() || prepared.subaccount_id !== profile.subaccountId) {
        throw new Error('Prepared production state belongs to a different V3 identity');
      }
      const alreadyActivated = fs.existsSync(activationFile);
      const handoffFile = alreadyActivated ? activationFile : process.env.DERIVE_V3_MAINNET_HANDOFF_FILE;
      if (!handoffFile) throw new Error('Set DERIVE_V3_MAINNET_HANDOFF_FILE to a fresh reviewed comparison report');
      const handoff = JSON.parse(fs.readFileSync(handoffFile, 'utf8'));
      validateHandoffReport(handoff, profile, { alreadyActivated });
      activationReport = handoff;
      if (!fs.existsSync(path.join(dir, 'data/noop.db'))) throw new Error('Prepared production ledger is missing');
    }
    process.env.DATA_DIR = path.join(dir, 'data');
    process.env.WIKI_DIR = path.join(dir, 'knowledge');
    process.env.NOOP_V3_ISOLATED_RUNNER = '1';
    assertIsolatedDataPaths(profile, process.env, path.resolve(__dirname, '../..'));
    if (!mainnet) {
      delete process.env.TELEGRAM_BOT_TOKEN;
      delete process.env.TELEGRAM_CHAT_ID;
    }
    // Never seed testnet budgets/positions from the production DB or knowledge history.
    fs.mkdirSync(process.env.DATA_DIR, { recursive: true });
    fs.mkdirSync(process.env.WIKI_DIR, { recursive: true });
    // Accounting checkpoints are journaled after their SQLite transaction;
    // FULL makes that ledger commit durable before the checkpoint, including WAL.
    initializeDatabase();
    if (mainnet && profile.executionEnabled && !fs.existsSync(activationFile)) {
      fs.writeFileSync(activationFile, JSON.stringify(activationReport, null, 2), { flag: 'wx', mode: 0o600 });
      createdActivation = activationFile;
    }
    console.log(`Starting ${profile.name} subaccount=${profile.subaccountId} execution=${profile.executionEnabled} data=${process.env.DATA_DIR}`);
    startBot();
    return release;
  } catch (error) {
    if (createdActivation) fs.unlinkSync(createdActivation);
    release(); throw error;
  }
}

if (require.main === module) main().catch(error => { console.error(error.message.split(' — ')[0]); process.exitCode = 1; });

module.exports = { main, validateHandoffReport };
