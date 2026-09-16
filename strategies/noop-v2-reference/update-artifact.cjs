'use strict';

// Explicit development command only; never imported by a Strategy. Run after
// reviewing a source change and update/reapprove release identity as appropriate.
// This pins source bytes; it is not a deployment or signature operation.
const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');

const root = path.resolve(__dirname, '../..');
const packageFiles = ['config.js', 'fields.js', 'fixtures.js', 'index.js', 'legacy.js', 'provenance.json'];
const dependencyFiles = ['strategy/canonical.js', 'strategy/conditions.js', 'strategy/contract.js',
  'strategy/decimal.js', 'strategy/fields.js', 'strategy/schemas/common.json', 'strategy/schemas/decision.json',
  'strategy/schemas/input-bundle.json', 'strategy/schemas/mandate.json', 'strategy/schemas/release.json'];

function digest(file) { return 'sha256:' + createHash('sha256').update(fs.readFileSync(file)).digest('hex'); }

function updateArtifact() {
  const result = {
    files: Object.fromEntries(packageFiles.map(file => [file, digest(path.join(__dirname, file))])),
    dependencies: Object.fromEntries(dependencyFiles.map(file => [file, digest(path.join(root, file))])),
  };
  fs.writeFileSync(path.join(__dirname, 'artifact.json'), JSON.stringify(result, null, 2) + '\n');
  return result;
}

if (require.main === module) {
  updateArtifact();
  process.stdout.write('Updated offline reference artifact source pins.\n');
}

module.exports = { updateArtifact };
