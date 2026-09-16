'use strict';

const REGISTERED_UNITS = Object.freeze([
  'boolean', 'ratio', 'percent', 'day', 'ms', 'count',
  'contract', 'ETH', 'USDC', 'USDC/contract', 'USDC/ETH',
]);

// Catalog metadata specifies the meaning expected from an evidence producer;
// this offline package does not certify a producer or fetch any live evidence.
const DEFAULT_FIELD_CATALOG = Object.freeze(Object.fromEntries(Object.entries({
  'instrument.best_ask': {
    unit: 'USDC/contract', max_age_ms: 2000, quote: true, precision: 30,
    source: 'normalized executable venue quote', calculation_version: 'normalized-option-premium/v1',
    description: 'Executable ask per normalized contract. Contract multiplier is applied once by the evidence producer.',
  },
  'instrument.best_bid': {
    unit: 'USDC/contract', max_age_ms: 2000, quote: true, precision: 30,
    source: 'normalized executable venue quote', calculation_version: 'normalized-option-premium/v1',
    description: 'Executable bid per normalized contract. Contract multiplier is applied once by the evidence producer.',
  },
  'instrument.dte': {
    unit: 'day', max_age_ms: 5000, precision: 30,
    source: 'venue instrument expiry and evaluation clock', calculation_version: 'utc-expiry-days/v1',
    description: 'Remaining seconds to the pinned expiry divided by 86400; non-terminating decimal division is rounded toward positive infinity at the declared precision.',
  },
  'instrument.delta': {
    unit: 'ratio', max_age_ms: 5000, precision: 30,
    source: 'versioned venue Greeks snapshot', calculation_version: 'signed-contract-delta/v1',
    description: 'Signed delta per normalized contract from the pinned venue model and observation.',
  },
  'position.call_profit_capture_pct': {
    unit: 'percent', max_age_ms: 5000, precision: 30,
    source: 'accounted short-call lots and executable close quote', calculation_version: 'net-short-call-capture/v1',
    description: '100 times (net opening premium minus executable remaining close cost including fees) divided by net opening premium, on identical remaining lots; nonpositive basis is missing. Producer rounds down at declared precision.',
  },
  'position.put_executable_pnl_pct': {
    unit: 'percent', max_age_ms: 5000, precision: 30,
    source: 'accounted long-put lots and executable close quote', calculation_version: 'net-long-put-pnl/v1',
    description: '100 times (executable net close proceeds minus allocated entry cost including fees) divided by entry cost, on identical remaining lots; nonpositive basis is missing. Producer rounds down at declared precision.',
  },
  'portfolio.has_longer_dated_put': {
    unit: 'boolean', max_age_ms: 5000,
    source: 'reconciled owned positions', calculation_version: 'positive-long-put-later-expiry/v1',
    description: 'An accounted positive long-put quantity exists with strictly later expiry than the candidate put; excludes acknowledged or unaccounted purchases.',
  },
  'account.projected_initial_margin_utilization': {
    unit: 'ratio', max_age_ms: 5000, precision: 30,
    source: 'authoritative venue portfolio simulation and NOOP commitments', calculation_version: 'projected-initial-margin-over-equity/v1',
    description: 'Projected required initial margin divided by projected venue margin equity after the candidate and all working orders and unresolved commitments; nonpositive equity or incomplete simulation is missing. Producer rounds up at declared precision.',
  },
  'account.reconciled': {
    unit: 'boolean', max_age_ms: 5000,
    source: 'NOOP accounting watermark and observed venue checkpoint', calculation_version: 'account-watermark-complete/v1',
    description: 'All observed execution and account events through the pinned checkpoint are durably accounted with no unresolved submissions.',
  },
}).map(([name, definition]) => [name, Object.freeze({
  ...definition,
  scope: name === 'account.reconciled' ? 'account' : 'instrument',
})])));

module.exports = { DEFAULT_FIELD_CATALOG, REGISTERED_UNITS };
