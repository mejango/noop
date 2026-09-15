'use strict';

// script.js starts the bot when imported. Compile only the declarations a test
// requests (and their named dependencies), never the module's startup code.
// This is a test bridge until the helpers have their own import-safe modules.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const ROOT = path.resolve(__dirname, '../..');
const SCRIPT_SOURCE = fs.readFileSync(path.join(ROOT, 'script.js'), 'utf8');
const BOT_CONFIG = JSON.parse(fs.readFileSync(path.join(ROOT, 'bot/config.json'), 'utf8'));

function declaration(source, name) {
  if (!/^[A-Za-z_$][\w$]*$/.test(name)) throw new Error(`Invalid production helper: ${name}`);
  const match = new RegExp(`^const ${name}\\s*=`, 'm').exec(source);
  if (!match) throw new Error(`Production declaration is missing: ${name}`);
  // Compile candidate boundaries without executing them. The first complete
  // declaration handles strings, templates, regular expressions and nested
  // blocks using Node's parser instead of a hand-maintained brace scanner.
  for (let end = source.indexOf(';', match.index); end >= 0 && end - match.index < 100000; end = source.indexOf(';', end + 1)) {
    const text = source.slice(match.index, end + 1);
    try { new vm.Script(text); return text; } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
    }
  }
  throw new Error(`Production declaration boundary is missing: ${name}`);
}

function loadProduction(names, { bindings = {}, source = SCRIPT_SOURCE } = {}) {
  const policyModules = ['trade-policy', 'order-pricing'].map(name => path.join(ROOT, `bot/${name}.js`));
  const supplied = {
    BOT_CONFIG,
    STRATEGY_FACTS: require('../../bot/strategy-facts.json'),
    ...require('../../bot/put-score'),
    ...require('../../bot/call-score'),
    ...require('../../bot/funding-rates'),
    ...require('../../bot/advisory-quotes'),
    ...Object.assign({}, ...policyModules.filter(file => fs.existsSync(file)).map(file => require(file))),
    botData: {},
    db: null,
    // These declarations must not acquire runtime capabilities in unit tests.
    require: undefined, process: undefined, fetch: undefined,
    setInterval: undefined, setTimeout: undefined, global: undefined,
    ...bindings,
  };
  const available = new Set([...source.matchAll(/^const ([A-Za-z_$][\w$]*)\s*=/gm)].map(match => match[1]));
  const loaded = new Map();
  function include(name) {
    if (Object.hasOwn(supplied, name) || loaded.has(name)) return;
    const text = declaration(source, name);
    if (/\brequire\s*\(|\bprocess\s*\.|\b(?:setInterval|setTimeout|fetch)\s*\(/.test(text)) {
      throw new Error(`Production helper ${name} requires an explicit test dependency`);
    }
    loaded.set(name, text);
    for (const word of text.matchAll(/\b[A-Za-z_$][\w$]*\b/g)) {
      if (available.has(word[0])) include(word[0]);
    }
  }
  names.forEach(include);
  // Preserve production declaration order for constants initialized from other
  // constants. compileFunction keeps ordinary result prototypes for assertions.
  const ordered = [...loaded.values()].sort((a, b) => source.indexOf(a) - source.indexOf(b));
  const execute = vm.compileFunction(`${ordered.join('\n')}\nreturn { ${names.join(', ')} };`, Object.keys(supplied), {
    filename: 'script.js (selected production declarations)',
  });
  return execute(...Object.values(supplied));
}

module.exports = { loadProduction, declaration, SCRIPT_SOURCE };
