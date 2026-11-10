'use strict';

const fs = require('node:fs');
const path = require('node:path');

const SQLITE_SUFFIXES = Object.freeze(['', '-wal', '-shm', '-journal']);

function stat(filename) {
  try { return fs.lstatSync(filename); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}

function canonical(filename) {
  if (typeof filename !== 'string' || !filename.trim() || filename.includes('\0')) throw new Error('Invalid database path');
  const absolute = path.resolve(filename);
  if (stat(absolute)) return fs.realpathSync(absolute);
  // A protected database need not exist yet. Resolve its nearest existing
  // parent so aliases remain detectable without creating any directories.
  return path.join(canonical(path.dirname(absolute)), path.basename(absolute));
}

function identity(filename) {
  const resolved = canonical(filename);
  return { path: resolved, stat: stat(resolved) };
}

function sameFile(a, b) {
  return a.path === b.path || (a.stat && b.stat && a.stat.dev === b.stat.dev && a.stat.ino === b.stat.ino);
}

function checkedSqlitePath(filename, { protectedPaths = [], mustBeNew = false, extraOutputs = [], label = 'Database' } = {}) {
  const target = canonical(filename);
  const outputs = [...SQLITE_SUFFIXES.map(suffix => target + suffix), ...extraOutputs.map(canonical)].map(identity);
  const sources = protectedPaths.flatMap(source => {
    const resolved = canonical(source);
    return SQLITE_SUFFIXES.map(suffix => identity(resolved + suffix));
  });
  if (outputs.some(output => sources.some(source => sameFile(output, source)))) {
    throw new Error(`${label} paths must differ from the source and its SQLite sidecars; aliases must remain separate`);
  }
  // Reserved suffixes are unsafe even when no legacy/source path is supplied.
  if (SQLITE_SUFFIXES.slice(1).some(suffix => target.toLowerCase().endsWith(suffix))) {
    throw new Error(`${label} filename must not use a reserved SQLite sidecar suffix`);
  }
  const newOutputs = mustBeNew || !stat(target) ? outputs : extraOutputs.map(identity);
  for (const output of newOutputs) {
    if (output.stat) throw new Error(`Refusing to overwrite existing path; destination must be a new file: ${output.path}`);
  }
  return target;
}

module.exports = { checkedSqlitePath, SQLITE_SUFFIXES };
