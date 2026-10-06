const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const ts = require('../dashboard/node_modules/typescript');
const source = fs.readFileSync('dashboard/src/app/api/migration-backup/route.ts', 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022, esModuleInterop: true } }).outputText;
function load(env, filesystem = { statSync() { throw new Error('No artifact in unit test'); } }) {
  const exports = {};
  vm.runInNewContext(code, { exports, require(name) {
    if (name === 'node:fs') return filesystem;
    return require(name);
  }, process: { env }, Buffer, Date, Response });
  return exports.GET;
}
test('encrypted backup endpoint stays closed without maintenance, valid token and bounded expiry', async () => {
  const token = 'a'.repeat(64), base = { DERIVE_MAINTENANCE: 'true', NOOP_BACKUP_DOWNLOAD_TOKEN: token,
    NOOP_BACKUP_DOWNLOAD_EXPIRES: new Date(Date.now() + 1800000).toISOString() };
  for (const change of [{ DERIVE_MAINTENANCE: 'false' }, { NOOP_BACKUP_DOWNLOAD_TOKEN: '' },
    { NOOP_BACKUP_DOWNLOAD_EXPIRES: 'invalid' }, { NOOP_BACKUP_DOWNLOAD_EXPIRES: new Date(0).toISOString() },
    { NOOP_BACKUP_DOWNLOAD_EXPIRES: new Date(Date.now() + 7200000).toISOString() }]) {
    const result = await load({ ...base, ...change })(new Request('https://noop.invalid/api/migration-backup', { headers: { Authorization: `Bearer ${token}` } }));
    assert.equal(result.status, 404);
  }
  for (const authorization of ['', 'Bearer wrong', `Bearer ${'b'.repeat(64)}`, `Bearer ${token}`]) {
    const result = await load(base)(new Request('https://noop.invalid/api/migration-backup', { headers: { authorization } }));
    assert.equal(result.status, 404); // Even valid auth cannot expose a missing artifact.
    assert.equal(result.headers.get('cache-control'), 'no-store');
  }
});

test('valid authorization serves only the fixed encrypted artifact and invalid auth never touches disk', async () => {
  const token = 'c'.repeat(64), payload = Buffer.from('encrypted bytes');
  const env = { DERIVE_MAINTENANCE: 'true', NOOP_BACKUP_DOWNLOAD_TOKEN: token,
    NOOP_BACKUP_DOWNLOAD_EXPIRES: new Date(Date.now() + 1800000).toISOString(), DATA_DIR: '/fixture' };
  let reads = 0;
  const handler = load(env, {
    statSync(filename) { reads++; assert.equal(filename, '/fixture/archive/derive-v3-export-20261006.enc'); return { isFile: () => true, size: payload.length }; },
    createReadStream(filename, { start, end }) { assert.equal(filename, '/fixture/archive/derive-v3-export-20261006.enc'); return require('node:stream').Readable.from([payload.subarray(start, end + 1)]); },
  });
  assert.equal((await handler(new Request('https://noop.invalid/api/migration-backup'))).status, 404);
  assert.equal(reads, 0);
  const response = await handler(new Request('https://noop.invalid/api/migration-backup?file=/data/noop.db', { headers: { Authorization: `Bearer ${token}` } }));
  assert.equal(response.status, 200);
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), payload);
  assert.equal(reads, 1);
  const partial = await handler(new Request('https://noop.invalid/api/migration-backup', { headers: { Authorization: `Bearer ${token}`, Range: 'bytes=2-5' } }));
  assert.equal(partial.status, 206);
  assert.equal(partial.headers.get('content-range'), `bytes 2-5/${payload.length}`);
  assert.deepEqual(Buffer.from(await partial.arrayBuffer()), payload.subarray(2, 6));
  for (const range of ['bytes=999999-', 'bytes=5-2', 'bytes=0-1,3-4', 'bad']) {
    assert.equal((await handler(new Request('https://noop.invalid/api/migration-backup', { headers: { Authorization: `Bearer ${token}`, Range: range } }))).status, 416);
  }
});
