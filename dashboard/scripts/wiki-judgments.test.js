// Run: node scripts/wiki-judgments.test.js
// Checks the judgment → error mapping and the expired-tick exception without a TypeSafe key.
const ts = require('typescript');
const fs = require('fs');
const assert = require('assert');

const load = (file, deps) => {
  const src = fs.readFileSync(`${__dirname}/../src/lib/${file}`, 'utf8');
  const js = ts.transpileModule(src, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 } }).outputText;
  const m = { exports: {} };
  new Function('module', 'exports', 'require', js)(m, m.exports, (name) => deps[name] || require(name));
  return m.exports;
};

const canned = { answers: {} };
let outage = false;
const sdk = {
  noul: (instructions, criteria) => ({ type: 'noul', instructions, criteria }),
  TypeSafeClient: class { systemOne() { return outage ? Promise.reject(new Error('503')) : Promise.resolve(canned); } },
};
const wikiCatalog = load('wikiCatalog.ts', {});
const wiki = load('wiki.ts', { '@/lib/wikiCatalog': wikiCatalog, '@typesafe-ai/sdk': sdk });

(async () => {
  const page = '**TLDR** [tick:#10] [tick:#12]\n\n## Core Rules\n- keep\n## Regime-Specific Actions\n## Sizing Guidelines\n## Timing Rules\n';
  const args = { pagePath: 'strategy/playbook.md', previousContent: page, replacementContent: page.replace(' [tick:#12]', ''), relatedContext: '', validationIssues: ['[tick:#12] is stale'] };

  delete process.env.TYPESAFE_API_KEY;
  assert.deepStrictEqual(await wiki.judgeWikiReplacement(args), { errors: [], expiredTickIds: new Set() });

  process.env.TYPESAFE_API_KEY = 'test';
  canned.answers = { invents_trigger: { noul: 0.9 }, adds_live_state: { noul: 0.2 }, expired_tick_12: { noul: 0.8 }, expired_tick_10: { noul: 0.1 } };
  const judged = await wiki.judgeWikiReplacement(args);
  assert.deepStrictEqual(judged.errors, ['Replacement invents unsupported numeric trading triggers']);
  assert.deepStrictEqual(judged.expiredTickIds, new Set([12]));

  const base = { pagePath: args.pagePath, previousContent: page, replacementContent: args.replacementContent, allowedMarkerContent: page, canonicalLessonContent: '' };
  // Dropping the newest tick is only allowed when the judgments marked it expired.
  assert.ok(wiki.validateWikiReplacement(base).includes('Replacement drops newest tick evidence [tick:#12]'));
  assert.deepStrictEqual(wiki.validateWikiReplacement({ ...base, expiredTickIds: judged.expiredTickIds }), []);
  assert.ok(wiki.validateWikiReplacement({ ...base, replacementContent: page, expiredTickIds: judged.expiredTickIds })
    .includes('Learning-owned strategy page retains expired live tick evidence: [tick:#12]'));
  // A judgment outage degrades to the no-key path instead of throwing.
  outage = true;
  assert.deepStrictEqual(await wiki.judgeWikiReplacement(args), { errors: [], expiredTickIds: new Set() });
  assert.deepStrictEqual(await wiki.findObsoleteUnresolvedEscalationIssues(['x'], 'wiki'), new Set());
  console.log('wiki-judgments: ok');
})().catch((e) => { console.error(e); process.exit(1); });
