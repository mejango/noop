'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { canonicalize, contentDigest } = require('../../strategy/canonical');
const { addDecimals } = require('../../strategy/decimal');
const { KINDS, normalizeEvent, reverseNormalizedEvent } = require('../ledger/events');

const PUT = 'ETH-20300201-2000-P';
const CALL = 'ETH-20300201-4000-C';
const now = '2030-02-01T08:00:01.000Z';
const clone = value => JSON.parse(JSON.stringify(value));
function event(kind, payload, id = '1') {
  return { event_id: `event:${id}`, source_event_id: `venue:${id}`, kind, occurred_at: now, evidence_ref: `evidence:${id}`, payload };
}
function fill(side, instrument = PUT, id = 'fill:1') {
  return { fill_id: id, order_attempt_id: 'attempt:1', instrument, side, quantity: '0.25', gross_premium: '25', quote_asset: 'USDC', fees: [{ asset: 'USDC', amount: '0.5' }] };
}
function balances(results) {
  const result = new Map();
  for (const normalized of results) {
    for (const posting of normalized.postings) {
      const key = `${posting.account}|${posting.asset}`;
      result.set(key, addDecimals(result.get(key) || '0', posting.amount));
    }
  }
  return (account, asset) => result.get(`${account}|${asset}`) || '0';
}
function balanced(normalized) {
  const sums = new Map();
  for (const posting of normalized.postings) sums.set(posting.asset, addDecimals(sums.get(posting.asset) || '0', posting.amount));
  for (const amount of sums.values()) assert.equal(amount, '0');
}

test('activation deposit is capital, not fee revenue; withdrawal removes actual capital', () => {
  const deposit = normalizeEvent(event('deposit', { asset: 'ETH', amount: '2.000', transfer_id: 'deposit:1' }));
  const withdrawal = normalizeEvent(event('withdrawal', { asset: 'ETH', amount: '0.25', transfer_id: 'withdrawal:1' }, '2'));
  const balance = balances([deposit, withdrawal]);
  assert.equal(balance('assets:cash', 'ETH'), '1.75');
  assert.equal(balance('capital:contributions', 'ETH'), '-2');
  assert.equal(balance('capital:withdrawals', 'ETH'), '0.25');
  assert.equal(balance('expenses:fees', 'ETH'), '0');
  assert.equal(deposit.economic_ref, 'deposit:deposit:1');
  assert.equal(withdrawal.economic_ref, 'withdrawal:withdrawal:1');
});

test('borrowed ETH increases assets and outstanding liabilities equally, not ETH gain', () => {
  const borrowed = normalizeEvent(event('borrow', { loan_id: 'financing:1', asset: 'ETH', amount: '2' }));
  const balance = balances([borrowed]);
  assert.equal(balance('assets:cash', 'ETH'), '2');
  assert.equal(balance('liabilities:principal:financing:1', 'ETH'), '-2');
  assert.equal(addDecimals(balance('assets:cash', 'ETH'), balance('liabilities:principal:financing:1', 'ETH')), '0');
  assert.equal(balance('capital:contributions', 'ETH'), '0');
});

test('accrued financing is expensed once; principal and interest repayments clear their own liability', () => {
  const events = [
    event('deposit', { asset: 'USDC', amount: '1', transfer_id: 'deposit:1' }, '1'),
    event('borrow', { loan_id: 'loan:1', asset: 'USDC', amount: '100' }, '2'),
    event('interest_accrual', { loan_id: 'loan:1', asset: 'USDC', amount: '0.2', direction: 'payable' }, '3'),
    event('interest_accrual', { loan_id: 'loan:1', asset: 'USDC', amount: '0.3', direction: 'payable' }, '4'),
    event('repay', { loan_id: 'loan:1', asset: 'USDC', principal: '40', interest: '0.2' }, '5'),
    event('interest_payment', { loan_id: 'loan:1', asset: 'USDC', amount: '0.3', direction: 'paid' }, '6'),
    event('repay', { loan_id: 'loan:1', asset: 'USDC', principal: '60', interest: '0' }, '7'),
  ].map(normalizeEvent);
  const balance = balances(events);
  assert.equal(balance('assets:cash', 'USDC'), '0.5');
  assert.equal(balance('expenses:interest', 'USDC'), '0.5');
  assert.equal(balance('liabilities:principal:loan:1', 'USDC'), '0');
  assert.equal(balance('liabilities:interest:loan:1', 'USDC'), '0');
  for (const entry of events) balanced(entry);
});

test('interest receipt converts the accrued receivable to cash without counting income twice', () => {
  const earned = normalizeEvent(event('interest_accrual', { loan_id: 'yield:1', asset: 'ETH', amount: '0.000000000000000001', direction: 'receivable' }));
  const received = normalizeEvent(event('interest_payment', { loan_id: 'yield:1', asset: 'ETH', amount: '0.000000000000000001', direction: 'received' }, '2'));
  const balance = balances([earned, received]);
  assert.equal(balance('assets:cash', 'ETH'), '0.000000000000000001');
  assert.equal(balance('assets:interest_receivable:yield:1', 'ETH'), '0');
  assert.equal(balance('income:interest', 'ETH'), '-0.000000000000000001');
  assert.equal(balance('expenses:interest', 'ETH'), '0');
});

test('two partial option fills accumulate exact position and cash with each fill fee once', () => {
  const first = fill('buy');
  const second = { ...fill('buy', PUT, 'fill:2'), quantity: '0.1', gross_premium: '10.25', fees: [{ asset: 'USDC', amount: '0.125' }] };
  const normalized = [normalizeEvent(event('option_fill', first)), normalizeEvent(event('option_fill', second, '2'))];
  const balance = balances(normalized);
  assert.equal(balance('assets:options', `OPTION:${PUT}`), '0.35');
  assert.equal(balance('assets:cash', 'USDC'), '-35.875');
  assert.equal(balance('expenses:fees', 'USDC'), '0.625');
  assert.equal(balance('clearing:options', 'USDC'), '35.25');
  assert.equal(balance('income:options', 'USDC'), '0');
  assert.equal(normalized[0].economic_ref, 'fill:fill:1');
  assert.equal(normalized[1].economic_ref, 'fill:fill:2');
});

test('short calls carry a negative option position; premium cash is not labeled realized profit', () => {
  const normalized = normalizeEvent(event('option_fill', fill('sell', CALL)));
  const balance = balances([normalized]);
  assert.equal(balance('assets:options', `OPTION:${CALL}`), '-0.25');
  assert.equal(balance('assets:cash', 'USDC'), '24.5');
  assert.equal(balance('clearing:options', 'USDC'), '-25');
  assert.equal(balance('expenses:fees', 'USDC'), '0.5');
  assert.ok(normalized.postings.every(posting => !posting.account.startsWith('income:')));
});

test('put monetization followed by spot recycling journals each asset without fictional mark profit', () => {
  const sold = normalizeEvent(event('option_fill', { ...fill('sell'), gross_premium: '500', fees: [{ asset: 'USDC', amount: '1' }] }));
  const purchased = normalizeEvent(event('spot_fill', {
    fill_id: 'spot:1', order_attempt_id: 'attempt:2', side: 'buy', base_asset: 'ETH', quote_asset: 'USDC',
    quantity: '0.2', gross_quote: '498', fees: [{ asset: 'ETH', amount: '0.0001' }, { asset: 'USDC', amount: '1' }],
  }, '2'));
  const balance = balances([sold, purchased]);
  assert.equal(balance('assets:cash', 'USDC'), '0');
  assert.equal(balance('assets:cash', 'ETH'), '0.1999');
  assert.equal(balance('expenses:fees', 'USDC'), '2');
  assert.equal(balance('expenses:fees', 'ETH'), '0.0001');
  assert.ok(purchased.postings.every(posting => !posting.asset.startsWith('OPTION:')));
});

test('spot sale cash can fund a short-call cash settlement and remove the full liability position', () => {
  const short = normalizeEvent(event('option_fill', { ...fill('sell', CALL), fees: [] }));
  const spot = normalizeEvent(event('spot_fill', {
    fill_id: 'spot:1', order_attempt_id: 'attempt:2', side: 'sell', base_asset: 'ETH', quote_asset: 'USDC', quantity: '0.1', gross_quote: '500', fees: [],
  }, '2'));
  const settlement = normalizeEvent(event('option_settlement', {
    settlement_id: 'expiry:1', instrument: CALL, position_quantity: '-0.25', cash_amount: '-525', quote_asset: 'USDC',
  }, '3'));
  const balance = balances([short, spot, settlement]);
  assert.equal(balance('assets:options', `OPTION:${CALL}`), '0');
  assert.equal(balance('assets:cash', 'ETH'), '-0.1');
  assert.equal(balance('assets:cash', 'USDC'), '0');
  assert.equal(settlement.economic_ref, `settlement:expiry:1:${CALL}`);
});

test('long settlement and zero-payoff expiry both remove the position without invented proceeds', () => {
  const long = normalizeEvent(event('option_fill', { ...fill('buy'), fees: [] }));
  const settlement = normalizeEvent(event('option_settlement', {
    settlement_id: 'expiry:1', instrument: PUT, position_quantity: '0.25', cash_amount: '500', quote_asset: 'USDC',
  }, '2'));
  const worthless = normalizeEvent(event('option_settlement', {
    settlement_id: 'expiry:2', instrument: PUT, position_quantity: '0.25', cash_amount: '0', quote_asset: 'USDC',
  }, '3'));
  assert.equal(balances([long, settlement])('assets:options', `OPTION:${PUT}`), '0');
  assert.equal(balances([long, settlement])('assets:cash', 'USDC'), '475');
  assert.equal(balances([long, worthless])('assets:options', `OPTION:${PUT}`), '0');
  assert.equal(balances([worthless])('assets:cash', 'USDC'), '0');
});

test('additional transfer fee is an expense and never changes the recorded customer contribution', () => {
  const deposit = normalizeEvent(event('deposit', { asset: 'ETH', amount: '2', transfer_id: 'transfer:1' }));
  const fee = normalizeEvent(event('fee', { fee_id: 'network:1', asset: 'ETH', amount: '0.0004' }, '2'));
  const balance = balances([deposit, fee]);
  assert.equal(balance('assets:cash', 'ETH'), '1.9996');
  assert.equal(balance('capital:contributions', 'ETH'), '-2');
  assert.equal(balance('expenses:fees', 'ETH'), '0.0004');
});

test('canonical normalization preserves the caller, exact decimal precision, and stable content digest', () => {
  const raw = event('option_fill', { ...fill('buy'), quantity: '0.2500', gross_premium: '25.0000', fees: [{ asset: 'USDC', amount: '0.5000' }] });
  const before = canonicalize(raw);
  const first = normalizeEvent(raw);
  const second = normalizeEvent({ ...first.event, event_id: 'another-local-wrapper' });
  assert.equal(canonicalize(raw), before);
  assert.equal(first.event.payload.quantity, '0.25');
  assert.equal(first.digest, second.digest);
  assert.equal(first.source_key, 'venue:1');
  assert.equal(first.economic_ref, second.economic_ref);
  assert.match(first.digest, /^sha256:[a-f0-9]{64}$/);
  const { event_id: omitted, ...sourceEvent } = first.event;
  assert.equal(first.digest, contentDigest(sourceEvent));
  const alteredEvidence = normalizeEvent({ ...raw, evidence_ref: 'evidence:revised' });
  assert.notEqual(first.digest, alteredEvidence.digest);
});

test('business fill identity survives alternate event wrappers, source IDs, and order-attempt claims', () => {
  const original = normalizeEvent(event('option_fill', fill('buy')));
  const changed = event('option_fill', { ...fill('buy'), order_attempt_id: 'attempt:2' }, 'different');
  const renamed = normalizeEvent(changed);
  assert.equal(original.economic_ref, renamed.economic_ref);
  assert.notEqual(original.source_key, renamed.source_key);
  assert.notEqual(original.digest, renamed.digest);
  const spot = normalizeEvent(event('spot_fill', {
    fill_id: 'fill:1', order_attempt_id: 'attempt:3', side: 'buy', base_asset: 'ETH', quote_asset: 'USDC', quantity: '0.25', gross_quote: '25', fees: [],
  }, 'spot'));
  assert.equal(spot.economic_ref, original.economic_ref);
});

test('all supported event kinds balance per asset without comparing unlike units', () => {
  const fixtures = [
    event('deposit', { asset: 'ETH', amount: '1', transfer_id: 't1' }),
    event('withdrawal', { asset: 'USDC', amount: '1', transfer_id: 't2' }),
    event('option_fill', fill('sell', CALL)),
    event('spot_fill', { fill_id: 'f1', order_attempt_id: 'a1', side: 'buy', base_asset: 'ETH', quote_asset: 'USDC', quantity: '1', gross_quote: '5000', fees: [{ asset: 'ETH', amount: '0.01' }] }),
    event('borrow', { loan_id: 'l1', asset: 'ETH', amount: '1' }),
    event('repay', { loan_id: 'l1', asset: 'ETH', principal: '0.1', interest: '0.001' }),
    event('interest_accrual', { loan_id: 'l1', asset: 'ETH', amount: '0.001', direction: 'payable' }),
    event('interest_payment', { loan_id: 'l1', asset: 'ETH', amount: '0.001', direction: 'paid' }),
    event('fee', { fee_id: 'fee1', asset: 'USDC', amount: '1' }),
    event('option_settlement', { settlement_id: 's1', instrument: CALL, position_quantity: '-1', cash_amount: '-1000', quote_asset: 'USDC' }),
  ];
  assert.deepEqual([...new Set(fixtures.map(item => item.kind))].sort(), [...KINDS].sort());
  for (const fixture of fixtures) balanced(normalizeEvent(fixture));
});

test('correction reverses a stored event exactly, including every fee and asset leg', () => {
  const original = normalizeEvent(event('spot_fill', {
    fill_id: 'f1', order_attempt_id: 'a1', side: 'buy', base_asset: 'ETH', quote_asset: 'USDC', quantity: '1', gross_quote: '3000', fees: [{ asset: 'USDC', amount: '0.5' }, { asset: 'ETH', amount: '0.001' }],
  }));
  const reversal = reverseNormalizedEvent(original, event('reversal', { reverses_event_id: original.event.event_id, reason: 'Venue corrected this execution evidence.' }, 'reverse:1'));
  const balance = balances([original, reversal]);
  for (const posting of original.postings) assert.equal(balance(posting.account, posting.asset), '0');
  assert.equal(reversal.economic_ref, 'reversal:event:1');
  balanced(reversal);
  assert.throws(() => normalizeEvent(reversal.event), /stored original/);
});

test('corrections cannot inject arbitrary postings, target another original, or reverse a reversal', () => {
  const original = normalizeEvent(event('deposit', { asset: 'ETH', amount: '2', transfer_id: 't1' }));
  const reversalEvent = event('reversal', { reverses_event_id: original.event.event_id, reason: 'Correction' }, 'r1');
  const forged = clone(original);
  forged.postings[0].amount = '200';
  assert.throws(() => reverseNormalizedEvent(forged, reversalEvent), /does not match/);
  assert.throws(() => reverseNormalizedEvent(original, { ...reversalEvent, payload: { ...reversalEvent.payload, reverses_event_id: 'another' } }), /identify its stored original/);
  assert.throws(() => reverseNormalizedEvent(original, { ...reversalEvent, source_event_id: original.source_key }), /own event and source/);
  const reversed = reverseNormalizedEvent(original, reversalEvent);
  assert.throws(() => reverseNormalizedEvent(reversed, event('reversal', { reverses_event_id: reversed.event.event_id, reason: 'Undo' }, 'r2')), /stored original/);
});

const invalid = [
  ['numeric amount', e => { e.payload.amount = 2; }, /decimal string/],
  ['exponent amount', e => { e.payload.amount = '2e0'; }, /decimal string/],
  ['leading zeros', e => { e.payload.amount = '02'; }, /decimal string/],
  ['unbounded decimal', e => { e.payload.amount = '1'.repeat(61); }, /digits|bounded/],
  ['unsupported asset', e => { e.payload.asset = 'WETH'; }, /ETH or USDC/],
  ['zero contribution', e => { e.payload.amount = '0'; }, /positive/],
  ['negative contribution', e => { e.payload.amount = '-1'; }, /positive/],
  ['account injection', e => { e.account = { subaccount_id: 'another' }; }, /forbidden/],
  ['arbitrary posting injection', e => { e.postings = []; }, /forbidden/],
  ['payload unknown field', e => { e.payload.owner = 'another'; }, /forbidden/],
  ['missing transfer evidence identity', e => { delete e.payload.transfer_id; }, /required/],
  ['missing evidence reference', e => { delete e.evidence_ref; }, /required/],
  ['invalid digest', e => { e.evidence_ref = 'sha256:wrong'; }, /invalid sha256/],
  ['missing milliseconds', e => { e.occurred_at = '2030-02-01T08:00:00Z'; }, /UTC ISO/],
  ['non-UTC timestamp', e => { e.occurred_at = '2030-02-01T08:00:00.000-03:00'; }, /UTC ISO/],
  ['impossible calendar date', e => { e.occurred_at = '2030-02-30T08:00:00.000Z'; }, /calendar/],
  ['unsupported adjustment', e => { e.kind = 'adjustment'; }, /Unsupported economic/],
];
for (const [name, mutate, message] of invalid) {
  test(`rejects ${name}`, () => {
    const value = event('deposit', { asset: 'ETH', amount: '2', transfer_id: 't1' });
    mutate(value);
    assert.throws(() => normalizeEvent(value), message);
  });
}

test('rejects cumulative fills, missing attempt identity, ambiguous units, invalid fees and instruments', () => {
  const changes = [
    value => { value.payload.cumulative_filled_quantity = '0.25'; },
    value => { delete value.payload.order_attempt_id; },
    value => { value.payload.quantity = '0'; },
    value => { value.payload.gross_premium = '-1'; },
    value => { value.payload.quote_asset = 'ETH'; },
    value => { value.payload.instrument = 'BTC-20300201-100000-C'; },
    value => { value.payload.instrument = 'ETH-20300230-4000-C'; },
    value => { value.payload.instrument = 'ETH-20300201-4000.00-C'; },
    value => { value.payload.fees = [{ asset: 'USDC', amount: '1' }, { asset: 'USDC', amount: '2' }]; },
    value => { value.payload.fees[0].amount = '-1'; },
    value => { value.payload.fees[0].price = '2'; },
    value => { value.payload.fees = {}; },
  ];
  for (const mutate of changes) {
    const value = event('option_fill', fill('buy'));
    mutate(value);
    assert.throws(() => normalizeEvent(value));
  }
});

test('rejects settlements with zero positions or payoff direction inconsistent with the signed position', () => {
  for (const [position_quantity, cash_amount] of [['0', '0'], ['1', '-1'], ['-1', '1']]) {
    assert.throws(() => normalizeEvent(event('option_settlement', {
      settlement_id: 's1', instrument: PUT, position_quantity, cash_amount, quote_asset: 'USDC',
    })), /nonzero|direction/);
  }
  assert.throws(() => normalizeEvent(event('repay', { loan_id: 'l1', asset: 'ETH', principal: '0', interest: '0' })), /must clear/);
  assert.throws(() => normalizeEvent(event('interest_payment', { loan_id: 'l1', asset: 'ETH', amount: '1', direction: 'payable' })), /paid or received/);
});

test('rejects hostile JSON objects without invoking accessors or consulting caller prototypes', () => {
  let reads = 0;
  const value = event('deposit', { asset: 'ETH', amount: '2', transfer_id: 't1' });
  Object.defineProperty(value.payload, 'amount', { enumerable: true, get() { reads += 1; return '2'; } });
  assert.throws(() => normalizeEvent(value), /accessor/);
  assert.equal(reads, 0);
  const inherited = Object.create({ injected: true });
  Object.assign(inherited, event('deposit', { asset: 'ETH', amount: '2', transfer_id: 't1' }));
  assert.throws(() => normalizeEvent(inherited), /Non-plain/);
  const hidden = event('deposit', { asset: 'ETH', amount: '2', transfer_id: 't1' });
  Object.defineProperty(hidden, 'secret', { value: 'x', enumerable: false });
  assert.throws(() => normalizeEvent(hidden), /Hidden JSON/);
});

test('import has no network, database, key, live bot, or filesystem module dependency', () => {
  const Module = require('node:module');
  const originalLoad = Module._load;
  const resolved = require.resolve('../ledger/events');
  const loaded = [];
  delete require.cache[resolved];
  Module._load = function guarded(request, parent, isMain) {
    loaded.push(request);
    if (/sqlite|ethers|axios|dotenv|(?:^|[/])bot(?:[/]|$)|script\.js|(?:^|:)fs$|(?:^|:)https?$|(?:^|:)net$/.test(request)) {
      throw new Error(`Forbidden runtime dependency: ${request}`);
    }
    return originalLoad.call(this, request, parent, isMain);
  };
  try {
    assert.equal(typeof require('../ledger/events').normalizeEvent, 'function');
    assert.ok(loaded.includes('../../strategy/decimal'));
    assert.ok(loaded.includes('../../strategy/canonical'));
  } finally {
    Module._load = originalLoad;
    delete require.cache[resolved];
  }
});
