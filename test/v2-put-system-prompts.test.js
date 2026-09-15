'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const vm = require('node:vm');
const ts = require('../dashboard/node_modules/typescript');
const { SCRIPT_SOURCE, loadProduction } = require('./helpers/load-production');

// Render the production system templates and their real policy helpers without
// importing script.js (which starts the bot) or invoking either model provider.
const source = ts.createSourceFile('script.js', SCRIPT_SOURCE, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
const globals = new Set([...SCRIPT_SOURCE.matchAll(/^const ([A-Za-z_$][\w$]*)\s*=/gm)].map(match => match[1]));
for (const name of Object.keys({ ...require('../bot/put-score'), ...require('../bot/call-score') })) globals.add(name);
const systems = new Map();
const prefixes = new Map([
  ['sonnet confirmation', 'You are a Spitznagel-style risk advisor.'],
  ['openai confirmation', 'You are a Taleb-style risk advisor.'],
  ['primary advisory', 'You are a senior options strategist with Mark Spitznagel'],
  ['advisory verifier', 'You are the Taleb Advisor reviewing a trading agenda'],
  ['two-advisor synthesis', 'You are the Synthesizer on a trading council. You have two advisor inputs. Your job is to produce the final trading agenda.'],
  ['single-advisor synthesis', 'You are a risk-management synthesizer for an options trading bot.'],
  ['rulebook repair', 'You repair an options bot standing rulebook.'],
]);
function visit(node) {
  if (ts.isTemplateExpression(node)) {
    const match = [...prefixes].find(([, prefix]) => node.head.text.startsWith(prefix));
    if (match) {
      const names = new Set();
      function dependency(child) {
        if (ts.isIdentifier(child) && globals.has(child.text)) names.add(child.text);
        ts.forEachChild(child, dependency);
      }
      for (const span of node.templateSpans) dependency(span.expression);
      const bindings = loadProduction([...names], { bindings: { PUT_INSURED_EXTERNAL_ETH: 20, path: undefined } });
      assert.equal(systems.has(match[0]), false, `Duplicate system template: ${match[0]}`);
      systems.set(match[0], vm.compileFunction(`return (${node.getText(source)});`, Object.keys(bindings))(...Object.values(bindings)));
    }
  }
  ts.forEachChild(node, visit);
}
visit(source);

test('every actual advisory and confirmation system uses direct PUT EDGE without legacy composite or IV vetoes', () => {
  assert.equal(systems.size, prefixes.size, 'All seven production system templates must be exercised');
  for (const [name, prompt] of systems) {
    assert.match(prompt, /PUT EDGE/, name);
    assert.match(prompt, /min_score/, name);
    assert.match(prompt, /target_score/, name);
    assert.match(prompt, /(?:Ignore|ignore|ignored)[^.\n]*min_edge_score|min_edge_score[^.\n]*ignored/, name);
    assert.match(prompt, /descriptive diagnostics|descriptive context/, name);
    assert.doesNotMatch(prompt, /Reject puts that overpay for protection \(high IV, crowd panic\)|buying puts at spiked IV overpays|Buying puts when IV is spiked means overpaying|Cheap insurance in calm, expensive insurance in fear|In calm markets, insurance is cheap|bounded cost, unbounded upside|Every trade should have bounded downside and unbounded upside/i, name);
  }
});

test('both real confirmation systems and primary/verifier advisory systems receive the approved bid price contract', () => {
  const { getBuyPutEntryPriceDisciplinePrompt } = loadProduction(['getBuyPutEntryPriceDisciplinePrompt']);
  const policy = getBuyPutEntryPriceDisciplinePrompt();
  assert.match(policy, /approved min_score\/target_score using fresh delta, DTE, and the proposed executable bid/);
  assert.match(policy, /High or spiked IV alone.*cannot add a standalone PUT veto/);
  assert.match(policy, /live-quote, venue-price, quantity, budget, margin, and liquidation checks/);
  for (const name of ['sonnet confirmation', 'openai confirmation', 'primary advisory', 'advisory verifier']) {
    assert.ok(systems.get(name).includes(policy), `${name} must receive the complete shared price discipline`);
  }
});

test('confirmation systems retain real call-margin and put-exit policy while stating the bounded put payoff accurately', () => {
  const { getCallMarginDisciplinePrompt, getPutExitDisciplinePrompt } = loadProduction(['getCallMarginDisciplinePrompt', 'getPutExitDisciplinePrompt']);
  for (const name of ['sonnet confirmation', 'openai confirmation']) {
    const prompt = systems.get(name);
    assert.ok(prompt.includes(getCallMarginDisciplinePrompt()), `${name}: call margin discipline`);
    assert.ok(prompt.includes(getPutExitDisciplinePrompt()), `${name}: owned put exit discipline`);
  }
  assert.match(systems.get('openai confirmation'), /Long puts have limited premium loss and provide nonlinear downside protection/);
  assert.match(systems.get('openai confirmation'), /intrinsic payoff is bounded by the strike when the underlying reaches zero/);
  assert.match(systems.get('advisory verifier'), /actual option payoff and portfolio exposure; require survival within the stated position, margin, and budget constraints/);
});
