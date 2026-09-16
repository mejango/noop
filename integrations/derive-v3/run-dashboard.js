'use strict';

const path = require('node:path');
const { spawn } = require('node:child_process');
const { readProfile } = require('./profile');
const { stateDirectory } = require('./state');

const env = require('./testnet-config').testnetEnvironment();
const profile = readProfile(env);
if (!profile.ownerAddress || !profile.subaccountId) throw new Error('Configure the testnet owner and subaccount first');
const dir = stateDirectory(profile);
const child = spawn(process.execPath, [path.resolve(__dirname, '../../dashboard/node_modules/next/dist/bin/next'), 'dev', '-p', '3001'], {
  cwd: path.resolve(__dirname, '../../dashboard'), stdio: 'inherit',
  env: { ...env, DATA_DIR: path.join(dir, 'data'),
    WIKI_DIR: path.join(dir, 'knowledge'), NOOP_V3_ISOLATED_RUNNER: '1' },
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => child.kill(signal));
child.on('exit', code => { process.exitCode = code ?? 1; });
child.on('error', error => { console.error(error.message); process.exitCode = 1; });
