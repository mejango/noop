'use strict';

// Shared account identity only: no signer, amount policy, or trading dependencies.
function getSpotAccumulationIdentity({ env = {}, derive, sqlite, mode = 'off', nowMs = Date.now() }) {
  const hasConfig = sqlite?.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='spot_accumulation_config'").get();
  const scope = `${derive.network}:${derive.version}:${derive.subaccountId}`;
  const row = hasConfig ? sqlite.prepare('SELECT config_json FROM spot_accumulation_config WHERE scope=?').get(scope) : null;
  const stored = row ? JSON.parse(row.config_json) : null;
  // Existing reservations must still be recoverable when new sends are disabled.
  if (mode === 'off' && !stored) return null;
  if (!sqlite || derive.version !== 'v3') throw new Error('Spot accumulation requires a V3 account and SQLite');
  const destination = String(env.SPOT_ACCUMULATION_DESTINATION_ID || stored?.destinationAccountId || '');
  if (!/^[1-9]\d*$/.test(destination) || !Number.isSafeInteger(Number(destination))
    || Number(destination) === derive.subaccountId) throw new Error('Explicit distinct spot destination required');
  const from = env.SPOT_ACCUMULATION_FROM || stored?.activationTimestamp
    || (mode === 'sample' ? new Date(nowMs).toISOString() : null);
  const boundary = from == null ? NaN : Date.parse(from);
  if (!Number.isFinite(boundary) || boundary > nowMs || boundary < Date.parse(derive.historyFrom)) {
    throw new Error('Explicit spot activation boundary within V3 history is required');
  }
  const owner = String(derive.wallet).toLowerCase();
  if (!/^0x[\da-f]{40}$/.test(owner)) throw new Error('Invalid spot owner');
  if (stored && (stored.network !== derive.network || stored.apiVersion !== derive.version
    || String(stored.accountId) !== String(derive.subaccountId) || stored.accountAddress !== owner
    || String(stored.destinationAccountId) !== destination || stored.activationTimestamp !== new Date(boundary).toISOString())) {
    throw new Error('Stored accumulation identity or boundary conflicts with configuration');
  }
  return { network: derive.network, apiVersion: derive.version,
    accountId: String(derive.subaccountId), destinationAccountId: destination, accountAddress: owner,
    activationTimestamp: new Date(boundary).toISOString() };
}

module.exports = { getSpotAccumulationIdentity };
