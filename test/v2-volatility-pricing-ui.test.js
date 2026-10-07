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
    provisional: false, measured: 81, total: 81,
    cells: pricing.OFFSETS.flatMap(offset => pricing.TENORS.map(dte => ({
      offset, dte, strike: 2615 * (1 + offset), ...quote(49.5, 13),
      bid: quote(38.8, 0), ask: quote(58.8, 97), instruments: [],
    }))),
  };
}

// Keep React's rendered elements while driving the component's state and event
// handlers directly. Polling is isolated; quote formatting and colors are real.
function mount(data) {
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
    if (name === '@/lib/hooks') return { usePolling: () => ({ data, error: null, loading: false, refetch: () => {} }), useLiveTimeAgo: () => '1m ago' };
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

test('breakdown opens with red bid and green ask squares while mark remains an optional view', () => {
  const render = mount(fixture());
  let tree = render();
  const headline = renderToStaticMarkup(card(tree));
  assert.ok(headline.includes('Mark IV · Cheap'));
  assert.ok(headline.includes('Ask IV · buy at ask'));
  assert.ok(headline.includes('Expensive · 97/100'));
  assert.ok(headline.includes('Bid IV · sell at bid'));
  assert.ok(headline.includes('Cheap · 0/100'));
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
  const summaries = find(card(tree), node => node.type === 'div' && node.props.className.includes('border-t'));
  const ask = React.Children.toArray(summaries.props.children)[0];
  const renderedAsk = renderToStaticMarkup(ask);
  assert.ok(renderedAsk.includes('Unrated'));
  assert.ok(!renderedAsk.includes('97/100'));
  assert.ok(renderToStaticMarkup(card(tree)).includes('Mark IV · Cheap'));
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
