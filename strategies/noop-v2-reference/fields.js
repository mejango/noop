'use strict';

const { DEFAULT_FIELD_CATALOG } = require('../../strategy/fields');

// These deliberately do not reuse the default net-fee P&L or initial-margin
// metrics: V2 thresholds were calibrated against different definitions.
const REFERENCE_FIELD_CATALOG = Object.freeze({
  ...DEFAULT_FIELD_CATALOG,
  ...Object.fromEntries(Object.entries({
    'noop_v2.call_profit_capture_pct': ['percent', 'instrument', '100 * (remaining-lot average opening premium - executable close ask) / average opening premium, excluding fees; round down at declared precision. Missing for nonpositive basis.', 'gross-short-call-capture/v2'],
    'noop_v2.put_executable_pnl_pct': ['percent', 'instrument', '100 * (executable close bid - remaining-lot average opening premium) / average opening premium, excluding fees; round down at declared precision. Missing for nonpositive basis.', 'gross-long-put-executable-pnl/v2'],
    'noop_v2.projected_displayed_margin_ratio': ['ratio', 'instrument', 'V2 estimateProjectedDisplayedMarginUtilization for this candidate after all pending commitments, using the absolute aggregated maintenance collateral base where available. This is research evidence, not a verified Derive V3 risk result.', 'inferred-displayed-margin/v2'],
    'noop_v2.breakout_override_active': ['boolean', 'account', 'V2 isCallBreakoutAddWindow computed from existing short calls, spot, and pinned short/medium momentum snapshots. Upward momentum, accepted derivative and proximity to recent high are all required.', 'existing-short-call-breakout-window/v2'],
    'noop_v2.position_quantity': ['contract', 'instrument', 'Positive remaining reconciled long-put or short-call quantity of this instrument. Direction is established by the action-specific inventory/liability reference.', 'reconciled-reference-position/v1'],
    'noop_v2.position_avg_entry_price': ['USDC/contract', 'instrument', 'V2 average entry premium per normalized remaining contract, excluding fees. Lot allocation and multiplier must match the recorded position.', 'gross-average-entry-price/v2'],
    'noop_v2.put_budget_available': ['USDC', 'account', 'Reconciled reference premium authorization remaining after confirmed net put cost and all outstanding reservations; not a cash balance or margin guarantee.', 'put-cycle-net-cost-plus-carry/v2'],
  }).map(([name, [unit, scope, description, calculation_version]]) => [name, Object.freeze({
    unit, scope, description, calculation_version, max_age_ms: 5000,
    ...(unit === 'boolean' ? {} : { precision: 30 }),
    source: 'Pinned offline V2 replay evidence; producer verification remains required before live execution',
  })])),
});

module.exports = { REFERENCE_FIELD_CATALOG };
