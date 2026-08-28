// Run: node scripts/expiry-settlement.test.js
const ts = require('typescript');
const fs = require('fs');
const src = fs.readFileSync(__dirname + '/../src/lib/expiry-settlement.ts', 'utf8');
const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 } }).outputText;
const m = { exports: {} }; new Function('module', 'exports', js)(m, m.exports);
const { deriveExpirySettlements } = m.exports;
const assert = require('assert');

const spot = [{ timestamp: '2026-08-21T07:55:00Z', price: 4950 }, { timestamp: '2026-08-21T09:00:00Z', price: 5200 }];
const orders = [
  { timestamp: '2026-08-18T10:00:00Z', action: 'sell_call', success: 1, instrument_name: 'ETH-20260821-4800-C', filled_amount: 2 },
  { timestamp: '2026-08-19T10:00:00Z', action: 'buyback_call', success: 1, instrument_name: 'ETH-20260821-4800-C', filled_amount: 0.5 },
  { timestamp: '2026-08-18T10:00:00Z', action: 'sell_call', success: 1, instrument_name: 'ETH-20260821-5500-C', filled_amount: 1 }, // OTM
  { timestamp: '2026-08-18T10:00:00Z', action: 'buy_put', success: 1, instrument_name: 'ETH-20260821-5000-P', filled_amount: 1 },
  { timestamp: '2026-08-18T10:00:00Z', action: 'sell_call', success: 1, instrument_name: 'ETH-20260925-4000-C', filled_amount: 1 }, // not expired
];
const now = Date.parse('2026-08-28T00:00:00Z');
const out = deriveExpirySettlements(orders, spot, now);
assert.deepStrictEqual(out.map(o => [o.action, o.instrument_name, o.filled_amount, o.total_value]), [
  ['settle_call', 'ETH-20260821-4800-C', 1.5, 150 * 1.5], // spot 4950 at 07:55, not 5200
  ['settle_put', 'ETH-20260821-5000-P', 1, 50],
]);
assert.strictEqual(deriveExpirySettlements(orders, [], now).length, 0, 'no spot -> no settlement');
console.log('expiry-settlement ok');
