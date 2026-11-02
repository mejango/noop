'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { boundedSignatureExpirySec, createKeyExpiryLookup, KEY_MARGIN_SEC } = require('../bot/signature-expiry');

const NOW = 1793644578; // 2026-11-02 18:36 UTC, the first rejected post_only order
const KEY_EXPIRY = 1793728835; // NOOP key, 2026-11-03 18:00:35 UTC

test('post_only signature is capped just before the session key expires', () => {
  assert.equal(boundedSignatureExpirySec({ nowSec: NOW, timeInForce: 'post_only', keyExpirySec: KEY_EXPIRY }), KEY_EXPIRY - KEY_MARGIN_SEC);
});

test('far-off key expiry keeps the normal 24h and 15min windows', () => {
  const far = NOW + 365 * 86400;
  assert.equal(boundedSignatureExpirySec({ nowSec: NOW, timeInForce: 'gtc', keyExpirySec: far }), NOW + 86400);
  assert.equal(boundedSignatureExpirySec({ nowSec: NOW, timeInForce: 'ioc', keyExpirySec: far }), NOW + 900);
});

test('unknown key expiry falls back to the unbounded window', () => {
  assert.equal(boundedSignatureExpirySec({ nowSec: NOW, timeInForce: 'post_only', keyExpirySec: null }), NOW + 86400);
});

test('lookup matches the signer case-insensitively, caches, and survives a failed refresh', async () => {
  let calls = 0; let fail = false; let t = 0;
  const lookup = createKeyExpiryLookup({
    now: () => t, ttlMs: 1000, log: () => {},
    fetchKeys: async () => {
      calls++;
      if (fail) throw new Error('down');
      return [{ public_session_key: '0xOTHER', expiry_sec: 1 }, { public_session_key: '0x4E67ABC', expiry_sec: KEY_EXPIRY }];
    },
  });
  assert.equal(await lookup('0x4e67abc'), KEY_EXPIRY);
  assert.equal(await lookup('0x4e67abc'), KEY_EXPIRY);
  assert.equal(calls, 1);
  t = 2000; fail = true;
  assert.equal(await lookup('0x4e67abc'), KEY_EXPIRY);
  assert.equal(await lookup('0xmissing'), null);
});
