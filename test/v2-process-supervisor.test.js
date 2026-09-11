const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { setTimeout: delay } = require('node:timers/promises');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..');

async function readJSON(file) {
  try {
    return JSON.parse(await fs.readFile(file, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

async function waitFor(check, message) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await delay(20);
  }
  assert.fail(message);
}

function stubSource(root, name, options) {
  return `
    const fs = require('node:fs');
    const path = require('node:path');
    const root = ${JSON.stringify(root)};
    const name = ${JSON.stringify(name)};
    const options = ${JSON.stringify(options)};
    function writeJSON(file, value) {
      fs.writeFileSync(file + '.tmp', JSON.stringify(value));
      fs.renameSync(file + '.tmp', file);
    }
    let malformedIndex = 0;
    function sendHeartbeat() {
      if (name !== 'bot' || !process.connected || options.heartbeat === 'none') return;
      if (fs.existsSync(path.join(root, 'bot.stop-heartbeats'))) return;
      const now = Date.now();
      const malformed = [
        { type: 'unrelated', at: now },
        { type: 'bot_heartbeat' },
        { type: 'bot_heartbeat', at: String(now) },
        { type: 'bot_heartbeat', at: now - 60000 },
        { type: 'bot_heartbeat', at: now + 60000 },
        { type: 'bot_heartbeat', at: null },
      ];
      const message = options.heartbeat === 'malformed'
        ? malformed[malformedIndex++ % malformed.length]
        : { type: 'bot_heartbeat', at: now };
      process.send(message, () => {});
    }
    sendHeartbeat();
    setInterval(sendHeartbeat, 20);
    for (const signal of ['SIGTERM', 'SIGINT']) {
      process.on(signal, () => {
        fs.writeFileSync(path.join(root, name + '.signal'), signal);
        if (!options.ignoreSignals) process.exit(0);
      });
    }
    if (options.descendant) {
      require('node:child_process').spawn(process.execPath, ['-e',
        "process.on('SIGTERM', () => {}); process.on('SIGINT', () => {}); const fs = require('node:fs'); const file = process.argv[1]; fs.writeFileSync(file + '.tmp', JSON.stringify({ pid: process.pid })); fs.renameSync(file + '.tmp', file); setInterval(() => {}, 1000);",
        path.join(root, name + '.descendant.json')
      ], { stdio: 'ignore' });
    }
    setInterval(() => {
      if (fs.existsSync(path.join(root, name + '.exit'))) process.exit(options.exitCode || 0);
      if (fs.existsSync(path.join(root, name + '.hang'))) {
        writeJSON(path.join(root, name + '.hung.json'), { pid: process.pid });
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
      }
    }, 10);
    writeJSON(path.join(root, name + '.ready.json'), {
      pid: process.pid, cwd: process.cwd(), hostname: process.env.HOSTNAME,
      supervised: process.env.BOT_SUPERVISED
    });
  `;
}

async function launchFixture(t, {
  bot = {}, dashboard = {}, missingDashboard = false, shutdownTimeoutMs, heartbeatTimeoutMs,
} = {}) {
  const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), 'noop-supervisor-')));
  await fs.mkdir(path.join(root, 'bot'));
  await fs.mkdir(path.join(root, 'scripts'));
  await fs.copyFile(path.join(repoRoot, 'start.sh'), path.join(root, 'start.sh'));
  await fs.copyFile(path.join(repoRoot, 'scripts/process-supervisor.js'), path.join(root, 'scripts/process-supervisor.js'));
  await fs.writeFile(path.join(root, 'bot/index.js'), stubSource(root, 'bot', bot));
  if (!missingDashboard) {
    await fs.mkdir(path.join(root, 'dashboard'));
    await fs.writeFile(path.join(root, 'dashboard/server.js'), stubSource(root, 'dashboard', dashboard));
  }

  const useShell = shutdownTimeoutMs === undefined && heartbeatTimeoutMs === undefined;
  const command = useShell ? '/bin/sh' : process.execPath;
  const args = useShell ? [path.join(root, 'start.sh')] : [
    '-e',
    `require(${JSON.stringify(path.join(repoRoot, 'scripts/process-supervisor.js'))})
      .startSupervisor(${JSON.stringify({ appRoot: root, shutdownTimeoutMs, heartbeatTimeoutMs })})
      .then((code) => { process.exitCode = code; });`,
  ];
  const child = spawn(command, args, {
    cwd: os.tmpdir(),
    env: { ...process.env, PATH: `${path.dirname(process.execPath)}${path.delimiter}${process.env.PATH || ''}` },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (data) => { output += data; });
  child.stderr.on('data', (data) => { output += data; });
  const completion = new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });

  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    for (const name of ['bot', 'dashboard']) {
      const ready = await readJSON(path.join(root, `${name}.ready.json`));
      if (!ready) continue;
      try { process.kill(process.platform === 'win32' ? ready.pid : -ready.pid, 'SIGKILL'); }
      catch (error) { if (error.code !== 'ESRCH') throw error; }
    }
    await completion;
    await fs.rm(root, { recursive: true, force: true });
  });

  return {
    root,
    child,
    output: () => output,
    completion,
    ready: (name) => waitFor(() => readJSON(path.join(root, `${name}.ready.json`)), `${name} never started: ${output}`),
    exit: (name) => fs.writeFile(path.join(root, `${name}.exit`), ''),
    signal: (name) => fs.readFile(path.join(root, `${name}.signal`), 'utf8'),
  };
}

for (const { name, code } of [
  { name: 'bot', code: 17 },
  { name: 'bot', code: 0 },
  { name: 'dashboard', code: 23 },
]) {
  test(`unexpected ${name} exit ${code} fails the supervisor and stops its peer`, { timeout: 10000 }, async (t) => {
    const fixture = await launchFixture(t, { [name]: { exitCode: code } });
    const [bot, dashboard] = await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
    assert.equal(bot.cwd, fixture.root);
    assert.equal(bot.supervised, '1');
    assert.equal(dashboard.cwd, path.join(fixture.root, 'dashboard'));
    assert.equal(dashboard.hostname, '0.0.0.0');
    await fixture.exit(name);
    assert.deepEqual(await fixture.completion, { code: code || 1, signal: null });
    assert.equal(await fixture.signal(name === 'bot' ? 'dashboard' : 'bot'), 'SIGTERM');
    assert.match(fixture.output(), /exited unexpectedly/);
  });
}

test('a bot killed by a signal fails the supervisor and stops the dashboard', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t);
  const [bot] = await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  process.kill(bot.pid, 'SIGKILL');
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.equal(await fixture.signal('dashboard'), 'SIGTERM');
  assert.match(fixture.output(), /SIGKILL/);
});

for (const signal of ['SIGTERM', 'SIGINT']) {
  test(`${signal} reaches both services and exits cleanly`, { timeout: 10000 }, async (t) => {
    const fixture = await launchFixture(t);
    await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
    fixture.child.kill(signal);
    assert.deepEqual(await fixture.completion, { code: 0, signal: null });
    assert.equal(await fixture.signal('bot'), signal);
    assert.equal(await fixture.signal('dashboard'), signal);
  });
}

test('a service that ignores shutdown is killed after the grace period', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { bot: { ignoreSignals: true }, shutdownTimeoutMs: 100 });
  const [bot] = await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  fixture.child.kill('SIGTERM');
  assert.deepEqual(await fixture.completion, { code: 0, signal: null });
  assert.equal(await fixture.signal('bot'), 'SIGTERM');
  assert.throws(() => process.kill(bot.pid, 0), { code: 'ESRCH' });
  assert.match(fixture.output(), /Shutdown grace period expired/);
});

test('a failed service spawn fails the supervisor', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { missingDashboard: true });
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.match(fixture.output(), /dashboard failed to start/);
});

test('shutdown also kills a descendant left behind by a service', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { bot: { descendant: true } });
  await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  const descendant = await waitFor(
    () => readJSON(path.join(fixture.root, 'bot.descendant.json')),
    'service descendant never became ready',
  );
  fixture.child.kill('SIGTERM');
  assert.deepEqual(await fixture.completion, { code: 0, signal: null });
  await waitFor(() => {
    try { process.kill(descendant.pid, 0); return false; }
    catch (error) { if (error.code === 'ESRCH') return true; throw error; }
  }, 'service descendant survived supervisor shutdown');
});

test('missing startup heartbeat fails healthy-looking processes and stops both services', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { bot: { heartbeat: 'none' }, heartbeatTimeoutMs: 500 });
  const [bot, dashboard] = await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  assert.doesNotThrow(() => process.kill(bot.pid, 0));
  assert.doesNotThrow(() => process.kill(dashboard.pid, 0));
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.equal(await fixture.signal('bot'), 'SIGTERM');
  assert.equal(await fixture.signal('dashboard'), 'SIGTERM');
  assert.match(fixture.output(), /Bot heartbeat overdue/);
});

test('fresh heartbeats keep services running, then stale heartbeats fail the supervisor', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { heartbeatTimeoutMs: 500 });
  await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  await delay(1200);
  assert.equal(fixture.child.exitCode, null);
  assert.equal(fixture.child.signalCode, null);
  await fs.writeFile(path.join(fixture.root, 'bot.stop-heartbeats'), '');
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.match(fixture.output(), /Bot heartbeat overdue/);
});

test('wrong-type, malformed, old, and future IPC messages do not refresh heartbeat health', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { bot: { heartbeat: 'malformed' }, heartbeatTimeoutMs: 500 });
  await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.match(fixture.output(), /Bot heartbeat overdue/);
});

test('the external heartbeat watchdog stops a bot with a blocked event loop', { timeout: 10000 }, async (t) => {
  const fixture = await launchFixture(t, { heartbeatTimeoutMs: 500, shutdownTimeoutMs: 100 });
  const [bot] = await Promise.all([fixture.ready('bot'), fixture.ready('dashboard')]);
  await fs.writeFile(path.join(fixture.root, 'bot.hang'), '');
  await waitFor(() => readJSON(path.join(fixture.root, 'bot.hung.json')), 'bot did not block its event loop');
  assert.deepEqual(await fixture.completion, { code: 1, signal: null });
  assert.throws(() => process.kill(bot.pid, 0), { code: 'ESRCH' });
  assert.equal(await fixture.signal('dashboard'), 'SIGTERM');
  assert.match(fixture.output(), /Bot heartbeat overdue/);
  assert.match(fixture.output(), /Shutdown grace period expired/);
});

test('invalid deadline options cannot silently disable supervision', () => {
  const { startSupervisor } = require('../scripts/process-supervisor');
  for (const heartbeatTimeoutMs of [0, -1, Infinity, NaN, 2147483648]) {
    assert.throws(() => startSupervisor({ heartbeatTimeoutMs }), RangeError);
  }
});
