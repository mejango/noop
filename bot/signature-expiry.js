'use strict';

// Derive rejects any signed action that would outlive its session key
// (code 14038), so order signatures are capped just before the key expires.
const KEY_MARGIN_SEC = 60;
const LOOKUP_TTL_MS = 10 * 60 * 1000;

function boundedSignatureExpirySec({ nowSec, timeInForce, keyExpirySec }) {
  const wanted = nowSec + (timeInForce === 'ioc' ? 900 : 86400); // IOC: 15min, GTC/post_only: 24h
  if (!(Number(keyExpirySec) > 0)) return wanted;
  return Math.min(wanted, Number(keyExpirySec) - KEY_MARGIN_SEC);
}

// fetchKeys() returns private/session_keys' public_session_keys. A failed lookup
// keeps the last known expiry (or none), so a lookup outage never blocks orders.
function createKeyExpiryLookup({ fetchKeys, ttlMs = LOOKUP_TTL_MS, now = () => Date.now(), log = console.log }) {
  const cache = new Map();
  return async (signerAddress) => {
    const id = String(signerAddress).toLowerCase();
    const hit = cache.get(id);
    if (hit && now() - hit.at < ttlMs) return hit.expiry;
    try {
      const keys = await fetchKeys();
      const key = (Array.isArray(keys) ? keys : []).find(k => String(k?.public_session_key).toLowerCase() === id);
      const expiry = Number(key?.expiry_sec) > 0 ? Number(key.expiry_sec) : null;
      cache.set(id, { at: now(), expiry });
      return expiry;
    } catch (error) {
      log(`⚠️ Session key expiry lookup failed: ${error.message}`);
      return hit?.expiry ?? null;
    }
  };
}

module.exports = { boundedSignatureExpirySec, createKeyExpiryLookup, KEY_MARGIN_SEC };
