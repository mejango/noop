'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const fs = require('node:fs');
const ts = require('../dashboard/node_modules/typescript');
const React = require('../dashboard/node_modules/react');
const { renderToStaticMarkup } = require('../dashboard/node_modules/react-dom/server');

function load(relativePath, requireModule) {
  const mod = { exports: {} };
  const code = ts.transpileModule(fs.readFileSync(`${__dirname}/../dashboard/src/${relativePath}`, 'utf8'), {
    compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, jsx: ts.JsxEmit.ReactJSX },
  }).outputText;
  new Function('module', 'exports', 'require', code)(mod, mod.exports, requireModule);
  return mod.exports;
}
const pricing = load('lib/volatility-pricing.ts', name => { throw new Error(`Unexpected module: ${name}`); });

function fixture() {
  const asOf = new Date().toISOString();
  const quote = (iv, percentile) => ({ iv, percentile, samples: 251, history: [{ at: asOf, iv, percentile }] });
  return {
    asOf, score: 13, label: 'Cheap', qualifier: 'Cheap broadly', spot: 2615, currentIv: 49.5,
    historyDays: 11, historySamples: 251, historyFrom: null, historyTo: null,
    provisional: false, measured: 135, total: 135,
    cells: pricing.OFFSETS.flatMap(offset => pricing.TENORS.map(dte => ({
      offset, dte, strike: 2615 * (1 + offset), ...quote(49.5, 13),
      bid: quote(38.8, 0), ask: quote(58.8, 97), instruments: [],
    }))),
  };
}

// Keep React's rendered elements while driving the component's state and event
// handlers directly. Polling is isolated; quote formatting and colors are real.
function mount(data, { loading = false, error = null } = {}) {
  let cursor = 0;
  const states = [];
  const Component = load('components/VolatilityPricing.tsx', name => {
    if (name === 'react/jsx-runtime') return require('../dashboard/node_modules/react/jsx-runtime');
    if (name === 'react') return {
      ...React, useEffect: () => {}, useRef: () => ({ current: null }),
      useState: initial => {
        const index = cursor++;
        if (!(index in states)) states[index] = initial;
        return [states[index], value => { states[index] = typeof value === 'function' ? value(states[index]) : value; }];
      },
    };
    if (name === '@/lib/hooks') return { usePolling: () => ({ data, error, loading, refetch: () => {} }), useLiveTimeAgo: () => '1m ago' };
    if (name === '@/lib/volatility-pricing') return pricing;
    throw new Error(`Unexpected module: ${name}`);
  }).default;
  return () => { cursor = 0; return Component(); };
}

function find(tree, predicate) {
  if (!tree || typeof tree !== 'object') return undefined;
  if (predicate(tree)) return tree;
  for (const child of React.Children.toArray(tree.props?.children)) {
    const found = find(child, predicate);
    if (found) return found;
  }
  return undefined;
}
const cell = tree => find(tree, node => node.type === 'button' && node.props['aria-label'] === 'At spot, 30 DTE: view contracts');
const quoteToggle = tree => find(tree, node => node.type === 'button' && node.props.children === 'Bid & ask');
const markToggle = tree => find(tree, node => node.type === 'button' && node.props.children === 'Mark');
const card = tree => find(tree, node => node.type === 'button' && node.props['aria-haspopup'] === 'dialog');
const summary = (tree, side) => find(tree, node => typeof node.type === 'function' && node.props.side === side && node.props.summary);
const meters = html => Array.from(html.matchAll(/<div\b[^>]*\brole="meter"[^>]*>/g), match => ({
  label: match[0].match(/aria-label="([^"]+)"/)[1],
  score: Number(match[0].match(/aria-valuenow="([^"]+)"/)[1]),
  status: match[0].match(/aria-valuetext="([^"]+)"/)[1],
}));

test('card and dialog lead with independent bid and ask prices, with mark only a secondary reference', () => {
  const tree = mount(fixture())();
  const headline = renderToStaticMarkup(card(tree));
  assert.deepEqual(meters(headline), [
    { label: 'Sell at bid historical IV percentile', score: 0, status: 'Measly, 0 out of 100' },
    { label: 'Buy at ask historical IV percentile', score: 97, status: 'Expensive, 97 out of 100' },
  ]);
  assert.ok(headline.includes('Mark reference: Cheap · 13/100 · IV 49.5%'));
  assert.ok(!headline.includes('Mark IV · Cheap'));
  const renderedBid = renderToStaticMarkup(summary(card(tree), 'bid'));
  const renderedAsk = renderToStaticMarkup(summary(card(tree), 'ask'));
  assert.ok(renderedBid.includes('Measly'));
  assert.ok(!renderedBid.includes('Cheap'));
  assert.ok(renderedAsk.includes('Expensive'));
  const dialog = find(tree, node => node.type === 'dialog');
  assert.deepEqual(meters(renderToStaticMarkup(dialog)), meters(headline));
});

test('breakdown opens with red bid and green ask squares while mark remains an optional view', () => {
  const render = mount(fixture());
  let tree = render();
  assert.equal(quoteToggle(tree).props['aria-pressed'], true);
  let renderedCell = renderToStaticMarkup(cell(tree));
  assert.ok(renderedCell.includes('>Bid</div>'));
  assert.ok(renderedCell.includes('>Ask</div>'));
  assert.ok(renderedCell.includes(`background-color:${pricing.pricingColor(0, 'bid')}`));
  assert.ok(renderedCell.includes(`background-color:${pricing.pricingColor(97, 'ask')}`));
  assert.ok(renderedCell.includes('background-color:hsl(0 '), 'bid square must use the red palette');
  assert.ok(renderedCell.includes('background-color:hsl(165 '), 'ask square must use the green palette');
  assert.ok(renderedCell.includes('IV 38.8%'));
  assert.ok(renderedCell.includes('IV 58.8%'));
  assert.ok(!renderedCell.includes('IV 49.5%'));

  markToggle(tree).props.onClick();
  tree = render();
  assert.equal(markToggle(tree).props['aria-pressed'], true);
  renderedCell = renderToStaticMarkup(cell(tree));
  assert.ok(renderedCell.includes('>Mark</div>'));
  assert.ok(renderedCell.includes(`background-color:${pricing.pricingColor(13, 'mark')}`));
  assert.ok(renderedCell.includes('IV 49.5%'));
  assert.ok(!renderedCell.includes('IV 38.8%'));
  assert.ok(!renderedCell.includes('IV 58.8%'));

  card(tree).props.onClick();
  tree = render();
  assert.equal(quoteToggle(tree).props['aria-pressed'], true, 'reopening always restores bid and ask squares');
  renderedCell = renderToStaticMarkup(cell(tree));
  assert.ok(renderedCell.includes('>Bid</div>'));
  assert.ok(renderedCell.includes('>Ask</div>'));
});

test('stale prices cannot leak through either view, summary or history tooltip', () => {
  const data = fixture();
  data.asOf = new Date(Date.now() - 6 * 60_000).toISOString();
  const render = mount(data);
  for (const view of ['mark', 'quotes']) {
    let tree = render();
    (view === 'quotes' ? quoteToggle(tree) : markToggle(tree)).props.onClick();
    tree = render();
    const headline = renderToStaticMarkup(card(tree));
    assert.ok(headline.includes('Quotes unavailable'));
    assert.ok(!headline.includes('role="meter"'));
    assert.ok(!headline.includes('/100'));
    assert.equal(cell(tree).props.disabled, true);
    const renderedCell = renderToStaticMarkup(cell(tree));
    assert.ok(renderedCell.includes('Unrated'));
    assert.ok(renderedCell.includes('quotes unavailable'));
    assert.ok(!renderedCell.includes('49.5%'));
    assert.ok(!renderedCell.includes('38.8%'));
    assert.ok(!renderedCell.includes('58.8%'));
    assert.ok(!renderedCell.includes('hsl('));
  }
});

test('insufficient ask coverage is unrated while the mark remains rated', () => {
  const data = fixture();
  data.cells.forEach(c => { c.ask.percentile = null; });
  const tree = mount(data)();
  const renderedAsk = renderToStaticMarkup(summary(card(tree), 'ask'));
  assert.ok(renderedAsk.includes('Unrated'));
  assert.deepEqual(meters(renderedAsk), []);
  assert.deepEqual(meters(renderToStaticMarkup(card(tree))).map(m => m.score), [0]);
  assert.ok(renderToStaticMarkup(card(tree)).includes('Mark reference: Cheap · 13/100'));
});

test('bid and ask remain independently rated when mark history is unavailable', () => {
  const data = fixture();
  data.score = null;
  data.label = 'Insufficient history / coverage';
  data.currentIv = null;
  data.cells.forEach(c => { c.percentile = null; c.iv = null; });
  const headline = renderToStaticMarkup(card(mount(data)()));
  assert.deepEqual(meters(headline).map(m => [m.score, m.status]), [
    [0, 'Measly, 0 out of 100'], [97, 'Expensive, 97 out of 100'],
  ]);
  assert.ok(headline.includes('Mark reference: Unrated'));
});

test('initial loading and failed quotes never show old side scores or mark context', () => {
  const data = fixture();
  data.asOf = '';
  for (const options of [{ loading: true }, { error: 'Quotes failed' }]) {
    const tree = mount(data, options)();
    const headline = renderToStaticMarkup(card(tree));
    assert.deepEqual(meters(headline), []);
    assert.ok(!headline.includes('13/100'));
    assert.ok(!headline.includes('49.5%'));
    assert.ok(headline.includes(options.loading ? 'Loading…' : 'Quotes unavailable'));
    assert.equal(cell(tree).props.disabled, true);
    const renderedCell = renderToStaticMarkup(cell(tree));
    assert.ok(!renderedCell.includes('38.8%'));
    assert.ok(!renderedCell.includes('58.8%'));
  }
});

test('bid summary names measly, typical and lucrative bids without a buy-side cheap label', () => {
  for (const [score, label] of [[0, 'Measly'], [50, 'Typical bids'], [100, 'Lucrative']]) {
    const data = fixture();
    data.cells.forEach(c => { c.bid.percentile = score; });
    const html = renderToStaticMarkup(summary(card(mount(data)()), 'bid'));
    assert.equal(meters(html)[0].status, `${label}, ${score} out of 100`);
    assert.ok(!html.includes('Cheap'));
  }
});

test('contract selection still works in mark and bid/ask views', () => {
  const data = fixture();
  data.cells.find(c => c.offset === 0 && c.dte === 30).instruments = [{
    name: 'ETH-20261106-2600-P', type: 'P', strike: 2600, expiry: 1793923200, dte: 30,
    ask: 150, askAmount: 2, askIv: 58.8, bid: 120, bidAmount: 1, bidIv: 38.8,
  }];
  const render = mount(data);
  for (const view of ['mark', 'quotes']) {
    let tree = render();
    (view === 'quotes' ? quoteToggle(tree) : markToggle(tree)).props.onClick();
    tree = render();
    cell(tree).props.onClick();
    tree = render();
    const details = find(tree, node => node.type === 'section' && node.props['aria-label'] === 'Selected volatility contracts');
    assert.ok(renderToStaticMarkup(details).includes('ETH-20261106-2600-P'));
    assert.equal(cell(tree).props['aria-pressed'], true);
    find(tree, node => node.type === 'button' && node.props['aria-label'] === 'Close contract details').props.onClick();
  }
});
