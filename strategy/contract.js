'use strict';

// Import-safe contract validation only. Acceptance is not live risk admission,
// execution authorization, an account ledger, or a signing service.
const { canonicalize, contentDigest } = require('./canonical');
const { validateCondition, DEFAULT_FIELD_CATALOG } = require('./conditions');
const { compareDecimals, multiplyDecimals, addDecimals } = require('./decimal');

const schemas = Object.freeze({
  'common.json': require('./schemas/common.json'),
  'release.json': require('./schemas/release.json'),
  'mandate.json': require('./schemas/mandate.json'),
  'input-bundle.json': require('./schemas/input-bundle.json'),
  'decision.json': require('./schemas/decision.json'),
});
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const SUPPORTED = new Set(['$schema', '$id', '$defs', '$ref', 'description', 'type', 'const', 'enum', 'properties', 'propertyNames', 'required', 'additionalProperties', 'minProperties', 'maxProperties', 'items', 'minItems', 'maxItems', 'uniqueItems', 'minLength', 'maxLength', 'pattern', 'format', 'minimum', 'maximum', 'anyOf', 'oneOf']);

function fail(path, message) { throw new Error(`${path}: ${message}`); }
function assert(test, path, message) { if (!test) fail(path, message); }
function timestamp(value) {
  assert(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value), '$time', 'requires UTC milliseconds');
  const ms = Date.parse(value);
  assert(Number.isFinite(ms) && new Date(ms).toISOString() === value, '$time', 'invalid calendar timestamp');
  return ms;
}

function resolveRef(ref) {
  const [file, fragment = ''] = ref.split('#');
  assert(own(schemas, file), '$schema', `unknown local schema ${file}`);
  let schema = schemas[file];
  assert(!fragment || fragment.startsWith('/'), '$schema', 'invalid reference pointer');
  for (const segment of fragment.split('/').slice(1)) {
    const key = segment.replace(/~1/g, '/').replace(/~0/g, '~');
    assert(schema && own(schema, key), '$schema', 'unresolved local schema reference');
    schema = schema[key];
  }
  return schema;
}

// This deliberately small JSON Schema 2020-12 subset is documented in the
// schema directory. Unknown keywords fail at import rather than being ignored.
function checkSchema(schema) {
  if (typeof schema === 'boolean') return;
  for (const keyword of Object.keys(schema)) assert(SUPPORTED.has(keyword), '$schema', `unsupported keyword ${keyword}`);
  if (schema.$ref) resolveRef(schema.$ref);
  if (schema.format) assert(schema.format === 'utc-milliseconds', '$schema', 'unsupported format');
  if (schema.pattern) new RegExp(schema.pattern, 'u');
  for (const group of ['properties', '$defs']) for (const sub of Object.values(schema[group] || {})) checkSchema(sub);
  for (const key of ['propertyNames', 'additionalProperties', 'items']) if (schema[key] !== undefined) checkSchema(schema[key]);
  for (const key of ['oneOf', 'anyOf']) for (const sub of schema[key] || []) checkSchema(sub);
}
for (const schema of Object.values(schemas)) checkSchema(schema);

function check(value, schema, path, depth = 0) {
  assert(depth <= 128, path, 'schema nesting capacity exceeded');
  if (typeof schema === 'boolean') { assert(schema, path, 'value forbidden'); return; }
  if (schema.$ref) check(value, resolveRef(schema.$ref), path, depth + 1);
  if (own(schema, 'const')) assert(canonicalize(value) === canonicalize(schema.const), path, 'unexpected constant');
  if (schema.enum) assert(schema.enum.some(entry => canonicalize(entry) === canonicalize(value)), path, 'unsupported enum value');
  for (const keyword of ['anyOf', 'oneOf']) {
    if (!schema[keyword]) continue;
    let matches = 0;
    const errors = [];
    for (const branch of schema[keyword]) {
      try { check(value, branch, path, depth + 1); matches++; } catch (error) { errors.push(error.message); }
    }
    assert(keyword === 'oneOf' ? matches === 1 : matches > 0, path, `${keyword} mismatch (${errors.slice(0, 3).join('; ')})`);
  }
  if (schema.type) {
    const actual = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
    assert(schema.type === 'integer' ? Number.isSafeInteger(value) : actual === schema.type, path, `expected ${schema.type}`);
  }
  if (typeof value === 'string') {
    const length = [...value].length;
    if (schema.minLength !== undefined) assert(length >= schema.minLength, path, 'string too short');
    if (schema.maxLength !== undefined) assert(length <= schema.maxLength, path, 'string too long');
    if (schema.pattern) assert(new RegExp(schema.pattern, 'u').test(value), path, 'invalid string encoding');
    if (schema.format) timestamp(value);
  }
  if (typeof value === 'number') {
    if (schema.minimum !== undefined) assert(value >= schema.minimum, path, 'below minimum');
    if (schema.maximum !== undefined) assert(value <= schema.maximum, path, 'above maximum');
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined) assert(value.length >= schema.minItems, path, 'too few array items');
    if (schema.maxItems !== undefined) assert(value.length <= schema.maxItems, path, 'too many array items');
    if (schema.uniqueItems) assert(new Set(value.map(canonicalize)).size === value.length, path, 'duplicate array item');
    if (schema.items !== undefined) value.forEach((entry, index) => check(entry, schema.items, `${path}[${index}]`, depth + 1));
  } else if (value && typeof value === 'object') {
    const keys = Object.keys(value);
    if (schema.minProperties !== undefined) assert(keys.length >= schema.minProperties, path, 'too few properties');
    if (schema.maxProperties !== undefined) assert(keys.length <= schema.maxProperties, path, 'too many properties');
    for (const key of schema.required || []) assert(own(value, key), path, `missing ${key}`);
    if (schema.propertyNames) for (const key of keys) check(key, schema.propertyNames, path + '.<key>', depth + 1);
    for (const key of keys) {
      if (schema.properties && own(schema.properties, key)) check(value[key], schema.properties[key], `${path}.${key}`, depth + 1);
      else if (schema.additionalProperties !== undefined) check(value[key], schema.additionalProperties, `${path}.${key}`, depth + 1);
    }
  }
}

function validateSchema(value, name) {
  canonicalize(value);
  assert(own(schemas, name), '$schema', 'unknown schema');
  check(value, schemas[name], '$');
  return value;
}
function digestWithout(value, key) {
  const copy = { ...value };
  delete copy[key];
  return contentDigest(copy);
}
function match(a, b, keys, path) {
  for (const key of keys) assert(a[key] === b[key], path, `${key} mismatch`);
}
function positive(value, path, zeroAllowed = false) {
  assert(compareDecimals(value, '0') >= (zeroAllowed ? 0 : 1), path, zeroAllowed ? 'must be nonnegative' : 'must be positive');
}
function subset(values, allowed, path) {
  for (const value of values) assert(allowed.includes(value), path, `unauthorized ${value}`);
}

function validateRelease(release) {
  validateSchema(release, 'release.json');
  assert(release.release_digest === digestWithout(release, 'release_digest'), '$release', 'release_digest does not match canonical manifest');
  for (const [name, declaration] of Object.entries(release.parameters)) {
    assert(compareDecimals(declaration.minimum, declaration.maximum) <= 0, `$release.parameters.${name}`, 'inverted range');
    assert(compareDecimals(declaration.default, declaration.minimum) >= 0 && compareDecimals(declaration.default, declaration.maximum) <= 0, `$release.parameters.${name}`, 'default outside declared range');
  }
  for (const action of release.capabilities.actions) {
    if (action.includes('spot')) assert(release.capabilities.spot_routes.length > 0, '$release', 'spot action needs declared route');
    else assert(release.capabilities.instrument_kinds.includes(action.includes('put') ? 'put' : 'call'), '$release', 'action instrument kind not declared');
  }
  return release;
}

function validateMandate(mandate, { release } = {}) {
  validateSchema(mandate, 'mandate.json');
  positive(mandate.protected_eth, '$mandate.protected_eth');
  if (release) {
    validateRelease(release);
    match(mandate, release, ['strategy_release_id', 'release_digest'], '$mandate');
    assert(canonicalize(Object.keys(mandate.parameters).sort()) === canonicalize(Object.keys(release.parameters).sort()), '$mandate.parameters', 'must materialize every declared parameter and no others');
    for (const [name, value] of Object.entries(mandate.parameters)) {
      const declaration = release.parameters[name];
      assert(compareDecimals(value, declaration.minimum) >= 0 && compareDecimals(value, declaration.maximum) <= 0, `$mandate.parameters.${name}`, 'outside accepted release range');
    }
    subset(mandate.authority.actions, release.capabilities.actions, '$mandate.authority.actions');
    subset(mandate.authority.spot_routes, release.capabilities.spot_routes, '$mandate.authority.spot_routes');
    assert(release.capabilities.venues.includes(mandate.account.venue), '$mandate.account', 'venue not supported by release');
  }
  return mandate;
}

function validateInputBundle(bundle, { release, mandate, catalog = DEFAULT_FIELD_CATALOG } = {}) {
  validateSchema(bundle, 'input-bundle.json');
  assert(bundle.input_bundle_id === digestWithout(bundle, 'input_bundle_id'), '$input', 'input_bundle_id does not match canonical bundle');
  if (release) { validateRelease(release); match(bundle, release, ['strategy_release_id', 'release_digest'], '$input'); }
  if (mandate) {
    validateMandate(mandate, { release });
    match(bundle, mandate, ['strategy_release_id', 'release_digest', 'mandate_id', 'mandate_revision'], '$input');
    assert(canonicalize(bundle.account) === canonicalize(mandate.account), '$input.account', 'account scope mismatch');
    subset(Object.keys(bundle.instruments), mandate.authority.instrument_refs, '$input.instruments');
    subset(Object.keys(bundle.instrument_fields), [...mandate.authority.instrument_refs, ...mandate.authority.spot_routes], '$input.instrument_fields');
  }
  const evaluated = timestamp(bundle.evaluated_at);
  function checkFields(fields, scope, path) { for (const [name, field] of Object.entries(fields)) {
    assert(own(catalog, name), '$input.fields', `unregistered field ${name}`);
    assert(catalog[name].scope === scope, `${path}.${name}`, 'field scope mismatch');
    assert(field.unit === catalog[name].unit, `${path}.${name}`, 'unit differs from registered catalog');
    assert(timestamp(field.observed_at) <= timestamp(field.available_at) && timestamp(field.available_at) <= evaluated, `${path}.${name}`, 'future or inverted evidence timestamps');
    if (field.quality === 'valid') assert(field.unit === 'boolean' ? typeof field.value === 'boolean' : typeof field.value === 'string', `${path}.${name}`, 'valid field requires typed value');
    if (field.value !== null) assert(field.unit === 'boolean' ? typeof field.value === 'boolean' : typeof field.value === 'string', `${path}.${name}`, 'wrong value type');
  } }
  checkFields(bundle.fields, 'account', '$input.fields');
  for (const [instrument, fields] of Object.entries(bundle.instrument_fields)) checkFields(fields, 'instrument', `$input.instrument_fields.${instrument}`);
  for (const [name, instrument] of Object.entries(bundle.instruments)) positive(instrument.contract_size, `$input.instruments.${name}.contract_size`);
  const ids = bundle.active_intents.map(entry => entry.intent_id);
  assert(new Set(ids).size === ids.length, '$input.active_intents', 'duplicate intent identity');
  return bundle;
}

function validateIntent(intent, { mandate, release, inputBundle, catalog, allowExpired = false }) {
  const path = `$intent.${intent.intent_id}`;
  validateCondition(intent.when, catalog);
  const spot = intent.action.includes('spot');
  const buy = ['buy_put', 'buyback_call', 'buy_spot_eth'].includes(intent.action);
  const close = ['sell_put', 'buyback_call'].includes(intent.action);
  assert(intent.order.side === (buy ? 'buy' : 'sell'), path, 'action side mismatch');
  assert(intent.order.reduce_only === close, path, 'action reduce-only mismatch');
  assert(intent.quantity.unit === (spot ? 'ETH' : 'contract'), path, 'action quantity unit mismatch');
  positive(intent.quantity.max_total, path + '.quantity');
  positive(intent.order.limit_price.value, path + '.limit_price');
  assert(intent.order.limit_price.unit === (spot ? 'USDC/ETH' : 'USDC/contract'), path, 'action limit price unit mismatch');
  positive(intent.order.max_total_fees.value, path + '.max_total_fees', true);
  assert(intent.order.max_total_fees.unit === 'USDC', path, 'fee currency mismatch');
  assert(spot ? own(intent, 'route_ref') && !own(intent, 'instrument_ref') : own(intent, 'instrument_ref') && !own(intent, 'route_ref'), path, 'must identify exactly one appropriate instrument or route');
  assert(timestamp(intent.active_from) < timestamp(intent.expires_at), path, 'intent interval must be nonempty');
  if (inputBundle) {
    if (!allowExpired) assert(timestamp(intent.expires_at) > timestamp(inputBundle.evaluated_at), path, 'intent already expired');
    if (!spot) {
      const instrument = inputBundle.instruments[intent.instrument_ref];
      assert(instrument, path, 'instrument absent from pinned metadata');
      assert(instrument.kind === (intent.action.includes('put') ? 'put' : 'call'), path, 'action instrument kind mismatch');
      assert(timestamp(intent.expires_at) <= timestamp(instrument.expiry), path, 'intent outlives option');
    }
  }
  if (buy) {
    assert(own(intent, 'budget_ref') && own(intent.order, 'max_total_outlay'), path, 'buy needs authorized funds and outlay cap');
    assert(!own(intent.order, 'max_total_liability'), path, 'buy cannot declare liability authority');
    assert(intent.order.max_total_outlay.unit === 'USDC', path, 'outlay currency mismatch');
    positive(intent.order.max_total_outlay.value, path + '.max_total_outlay');
    const boundedCost = addDecimals(multiplyDecimals(intent.quantity.max_total, intent.order.limit_price.value), intent.order.max_total_fees.value);
    assert(compareDecimals(boundedCost, intent.order.max_total_outlay.value) <= 0, path, 'outlay cannot cover maximum quantity, limit price and fees');
  } else {
    assert(!own(intent.order, 'max_total_outlay'), path, 'sell cannot declare buy outlay');
  }
  if (intent.action === 'sell_call') {
    assert(own(intent, 'liability_ref') && own(intent.order, 'max_total_liability'), path, 'call entry needs named capacity and finite stressed-liability cap');
    assert(intent.order.max_total_liability.unit === 'USDC', path, 'liability currency mismatch');
    positive(intent.order.max_total_liability.value, path + '.max_total_liability');
  } else assert(!own(intent.order, 'max_total_liability'), path, 'liability cap belongs to call entry');
  if (['sell_put', 'buyback_call', 'sell_spot_eth'].includes(intent.action)) assert(own(intent, 'inventory_ref'), path, 'inventory action needs named inventory');
  if (mandate) {
    assert(intent.authority_ref === `mandate:${mandate.mandate_id}/${mandate.mandate_revision}`, path, 'authority reference mismatch');
    subset([intent.action], mandate.authority.actions, path);
    if (spot) subset([intent.route_ref], mandate.authority.spot_routes, path);
    else subset([intent.instrument_ref], mandate.authority.instrument_refs, path);
    for (const [key, authorityKey] of [['budget_ref', 'budget_refs'], ['inventory_ref', 'inventory_refs'], ['liability_ref', 'liability_refs']]) if (own(intent, key)) subset([intent[key]], mandate.authority[authorityKey], path);
  }
  if (release) subset([intent.action], release.capabilities.actions, path);
  for (const dependency of intent.dependencies) {
    assert(dependency.intent_id !== intent.intent_id, path, 'self dependency forbidden');
    if (dependency.minimum_quantity) positive(dependency.minimum_quantity.value, path + '.dependency.minimum_quantity');
  }
}

function validateStoredIntent(intent, { mandate, release, inputBundle, catalog = DEFAULT_FIELD_CATALOG } = {}) {
  canonicalize(intent);
  check(intent, resolveRef('common.json#/$defs/intent'), '$intent');
  validateIntent(intent, { mandate, release, inputBundle, catalog, allowExpired: true });
  return intent;
}

function validateDecision(decision, { release, mandate, inputBundle, controlState, catalog = DEFAULT_FIELD_CATALOG } = {}) {
  validateSchema(decision, 'decision.json');
  if (release) { validateRelease(release); match(decision, release, ['strategy_release_id', 'release_digest'], '$decision'); }
  if (mandate) {
    validateMandate(mandate, { release });
    match(decision, mandate, ['strategy_release_id', 'release_digest', 'mandate_id', 'mandate_revision'], '$decision');
  }
  if (inputBundle) {
    validateInputBundle(inputBundle, { release, mandate, catalog });
    match(decision, inputBundle, ['strategy_release_id', 'release_digest', 'mandate_id', 'mandate_revision', 'input_bundle_id'], '$decision');
    assert(decision.expected_control_revision === inputBundle.control_revision, '$decision', 'input control revision mismatch');
  }
  if (controlState) {
    match(decision, controlState, ['mandate_id', 'mandate_revision'], '$decision');
    assert(decision.expected_control_revision === controlState.control_revision, '$decision', 'stale control revision');
    if (inputBundle) {
      const compareIdentity = (a, b) => a.intent_id < b.intent_id ? -1 : a.intent_id > b.intent_id ? 1 : 0;
      const projected = Object.entries(controlState.intents).map(([id, entry]) => ({ intent_id: id, intent_revision: entry.intent.intent_revision, status: entry.status })).sort(compareIdentity);
      const reported = [...inputBundle.active_intents].sort(compareIdentity);
      assert(canonicalize(projected) === canonicalize(reported), '$input.active_intents', 'control projection mismatch');
      assert(inputBundle.private_state.version === controlState.private_state_version, '$input.private_state', 'control private state mismatch');
    }
  }
  if (decision.private_state) {
    assert(decision.private_state.proposed_version === decision.private_state.expected_version + 1, '$decision.private_state', 'private version must advance once');
    if (inputBundle) assert(decision.private_state.expected_version === inputBundle.private_state.version, '$decision.private_state', 'input private state mismatch');
    if (controlState) assert(decision.private_state.expected_version === controlState.private_state_version, '$decision.private_state', 'stale private state version');
  }
  const changed = new Set();
  const graph = new Map();
  if (controlState) for (const [id, entry] of Object.entries(controlState.intents)) graph.set(id, entry.intent);
  const noActions = decision.operations.filter(operation => operation.op === 'no_action');
  assert(noActions.length === 0 || decision.operations.length === 1, '$decision.operations', 'no_action must be the only operation');
  for (const operation of decision.operations) {
    if (operation.op === 'no_action') continue;
    const intent = operation.intent;
    const id = intent ? intent.intent_id : operation.intent_id;
    assert(!changed.has(id), '$decision.operations', 'multiple operations for one intent');
    changed.add(id);
    const previous = controlState && own(controlState.intents, id) ? controlState.intents[id].intent : undefined;
    if (controlState) assert(operation.expected_intent_revision === (previous ? previous.intent_revision : null), '$decision.operations', 'stale or missing intent revision');
    if (operation.op === 'cancel_intent') {
      if (controlState) assert(previous, '$decision.operations', 'cannot cancel missing intent');
      continue;
    }
    assert(intent.intent_revision === (operation.expected_intent_revision === null ? 1 : operation.expected_intent_revision + 1), '$decision.operations', 'intent revision must advance once');
    if (mandate) assert(mandate.status === 'active', '$decision.operations', 'mandate lifecycle blocks Strategy upserts');
    validateIntent(intent, { mandate, release, inputBundle, catalog });
    graph.set(id, intent);
  }
  // Validate the resulting graph against retained control state. Historical
  // predecessor revisions can be named if the control ledger retains them.
  for (const [id, intent] of graph) {
    for (const dependency of intent.dependencies) {
      if (dependency.condition === 'ledger_event') continue;
      const predecessor = graph.get(dependency.intent_id);
      if (!predecessor && !controlState) continue; // schema-only validation lacks retained intents
      assert(predecessor, `$intent.${id}`, 'dependency predecessor missing');
      let source = predecessor;
      if (source.intent_revision !== dependency.intent_revision && controlState) {
        const stored = controlState.intents[dependency.intent_id];
        const old = stored && [stored.intent, ...(stored.prior_revisions || [])].find(entry => (entry.intent || entry).intent_revision === dependency.intent_revision);
        source = old && (old.intent || old);
      }
      assert(source && source.intent_revision === dependency.intent_revision, `$intent.${id}`, 'dependency revision unavailable');
      if (dependency.minimum_quantity) {
        assert(dependency.minimum_quantity.unit === source.quantity.unit, `$intent.${id}`, 'dependency quantity unit mismatch');
        assert(compareDecimals(dependency.minimum_quantity.value, source.quantity.max_total) <= 0, `$intent.${id}`, 'dependency exceeds predecessor quantity');
      }
    }
  }
  const revisionKey = (id, revision) => JSON.stringify([id, revision]);
  const revisions = new Map();
  if (controlState) for (const [id, stored] of Object.entries(controlState.intents)) {
    for (const record of [stored.intent, ...(stored.prior_revisions || [])]) {
      const intent = record.intent || record;
      revisions.set(revisionKey(id, intent.intent_revision), intent);
    }
  }
  for (const [id, intent] of graph) revisions.set(revisionKey(id, intent.intent_revision), intent);
  const visiting = new Set();
  const visited = new Set();
  function visit(key) {
    if (visited.has(key)) return;
    assert(!visiting.has(key), '$decision.dependencies', 'cycle forbidden');
    visiting.add(key);
    const node = revisions.get(key);
    if (node) for (const dependency of node.dependencies) if (dependency.intent_id) visit(revisionKey(dependency.intent_id, dependency.intent_revision));
    visiting.delete(key);
    visited.add(key);
  }
  for (const [id, intent] of graph) visit(revisionKey(id, intent.intent_revision));
  return decision;
}

module.exports = { CONTRACT_VERSION: 'noop.strategy/v1', validateRelease, validateMandate, validateInputBundle, validateDecision, validateStoredIntent, validateSchema, canonicalize, contentDigest, timestamp };
