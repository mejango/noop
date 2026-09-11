const { spawn } = require('node:child_process');
const path = require('node:path');

// Keep the container alive only while both services are running. A container
// restart policy, rather than an in-container bot restart, owns recovery.
function startSupervisor({
  appRoot = path.resolve(__dirname, '..'),
  shutdownTimeoutMs = 5000,
  heartbeatTimeoutMs = 15 * 60 * 1000,
} = {}) {
  for (const [name, value] of Object.entries({ shutdownTimeoutMs, heartbeatTimeoutMs })) {
    if (!Number.isFinite(value) || value < 1 || value > 2147483647) {
      throw new RangeError(`${name} must be between 1 and 2147483647 milliseconds`);
    }
  }
  return new Promise((resolve) => {
    const useProcessGroups = process.platform !== 'win32';
    const services = [];
    let stopping = false;
    let exitCode = 0;
    let shutdownTimer;
    let heartbeatTimer;
    let finished = false;

    function armHeartbeatDeadline() {
      clearTimeout(heartbeatTimer);
      // Receipt starts the next deadline; the bot's timestamp cannot extend it.
      // This also bounds startup before the first completed trading cycle.
      heartbeatTimer = setTimeout(() => {
        console.error(`[supervisor] Bot heartbeat overdue: no completed cycle received in ${heartbeatTimeoutMs}ms`);
        stop(1);
      }, heartbeatTimeoutMs);
    }

    function onHeartbeat(message) {
      if (stopping || !message || message.type !== 'bot_heartbeat') return;
      const now = Date.now();
      if (!Number.isSafeInteger(message.at) || message.at <= 0 || message.at > now || now - message.at > heartbeatTimeoutMs) return;
      armHeartbeatDeadline();
    }

    function signalService(service, signal) {
      if (!service.child.pid) return;
      try {
        // Separate process groups also cover subprocesses started by a service.
        if (useProcessGroups) process.kill(-service.child.pid, signal);
        else if (!service.exited) service.child.kill(signal);
      } catch (error) {
        if (error.code !== 'ESRCH') {
          console.error(`[supervisor] Could not send ${signal} to ${service.name}: ${error.message}`);
        }
      }
    }

    function forceStop() {
      for (const service of services) signalService(service, 'SIGKILL');
    }

    function finishIfStopped() {
      if (!stopping || finished || services.some((service) => !service.exited)) return;
      finished = true;
      clearTimeout(shutdownTimer);
      clearTimeout(heartbeatTimer);
      process.removeListener('SIGTERM', onSignal);
      process.removeListener('SIGINT', onSignal);
      // A service can exit before a subprocess does. Do not leave descendants
      // behind after all service leaders have exited.
      forceStop();
      resolve(exitCode);
    }

    function stop(code, signal = 'SIGTERM') {
      if (stopping) return;
      stopping = true;
      exitCode = code;
      clearTimeout(heartbeatTimer);
      console.log(`[supervisor] Stopping services with ${signal}`);
      for (const service of services) signalService(service, signal);
      shutdownTimer = setTimeout(() => {
        console.error('[supervisor] Shutdown grace period expired; forcing services to stop');
        forceStop();
      }, shutdownTimeoutMs);
      finishIfStopped();
    }

    function onSignal(signal) {
      if (stopping) forceStop();
      else stop(0, signal);
    }

    process.on('SIGTERM', onSignal);
    process.on('SIGINT', onSignal);

    for (const spec of [
      {
        name: 'bot',
        cwd: appRoot,
        entry: 'bot/index.js',
        env: { ...process.env, BOT_SUPERVISED: '1' },
      },
      {
        name: 'dashboard',
        cwd: path.join(appRoot, 'dashboard'),
        entry: 'server.js',
        env: { ...process.env, HOSTNAME: '0.0.0.0' },
      },
    ]) {
      console.log(`[supervisor] Starting ${spec.name}`);
      const child = spawn(process.execPath, [spec.entry], {
        cwd: spec.cwd,
        env: spec.env,
        stdio: spec.name === 'bot' ? ['inherit', 'inherit', 'inherit', 'ipc'] : 'inherit',
        detached: useProcessGroups,
      });
      const service = { name: spec.name, child, exited: false };
      services.push(service);
      if (spec.name === 'bot') {
        child.on('message', onHeartbeat);
        armHeartbeatDeadline();
      }
      child.once('error', (error) => {
        service.exited = true;
        console.error(`[supervisor] ${spec.name} failed to start: ${error.message}`);
        if (!stopping) stop(1);
        finishIfStopped();
      });
      child.once('exit', (code, signal) => {
        service.exited = true;
        if (!stopping) {
          console.error(`[supervisor] ${spec.name} exited unexpectedly (${signal || `code ${code}`})`);
          stop(Number.isInteger(code) && code > 0 ? code : 1);
        }
        finishIfStopped();
      });
    }
  });
}

if (require.main === module) {
  startSupervisor().then((code) => { process.exitCode = code; });
}

module.exports = { startSupervisor };
