// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'anchorage-entry-point-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const root = join(directory, 'repo');
  mkdirSync(join(root, 'scripts'), { recursive: true });
  copyFileSync(
    join(repositoryRoot, 'scripts/entry-point.mjs'),
    join(root, 'scripts/entry-point.mjs'),
  );
  const events = join(root, 'events.log');
  const entry = join(root, 'scripts/guarded.mjs');
  writeFileSync(
    entry,
    `import { appendFileSync } from 'node:fs';
import { isInvokedAsEntryPoint } from './entry-point.mjs';

if (isInvokedAsEntryPoint(import.meta.url)) {
  appendFileSync(${JSON.stringify(events)}, 'ran\\n');
}
`,
  );
  return { root, directory, entry, events };
}

async function run(
  f,
  { flags = [], entry = f.entry, argv = [], cwd = f.directory, input } = {},
) {
  const child = spawn(
    process.execPath,
    ['--no-warnings', ...flags, entry, ...argv],
    {
      cwd,
      env: { ...process.env, NODE_OPTIONS: '' },
      stdio: [input === undefined ? 'ignore' : 'pipe', 'pipe', 'pipe'],
      detached: process.platform !== 'win32',
    },
  );
  let stdout = '';
  let stderr = '';
  let timedOut = false;
  if (input !== undefined) child.stdin.end(input);
  child.stdout.setEncoding('utf8').on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.setEncoding('utf8').on('data', (chunk) => {
    stderr += chunk;
  });
  const timer = setTimeout(() => {
    timedOut = true;
    if (process.platform === 'win32') child.kill('SIGKILL');
    else process.kill(-child.pid, 'SIGKILL');
  }, 15_000);
  try {
    const result = await new Promise((settle, reject) => {
      child.once('error', reject);
      child.once('close', (status, signal) => settle({ status, signal }));
    });
    assert.equal(
      timedOut,
      false,
      `entry-point watchdog expired\n${stdout}\n${stderr}`,
    );
    assert.equal(result.signal, null, stderr);
    return { ...result, stdout, stderr };
  } finally {
    clearTimeout(timer);
  }
}

function activity(f) {
  return existsSync(f.events) ? readFileSync(f.events, 'utf8') : '';
}

for (const entryKind of ['direct', 'file symlink', 'directory symlink']) {
  test(`${entryKind} invocation is the entry point`, async (t) => {
    const f = fixture(t);
    let entry = f.entry;
    if (entryKind === 'file symlink') {
      entry = join(f.directory, 'linked.mjs');
      symlinkSync(f.entry, entry, 'file');
    } else if (entryKind === 'directory symlink') {
      const linked = join(f.directory, 'linked');
      symlinkSync(f.root, linked, 'junction');
      entry = join(linked, 'scripts/guarded.mjs');
    }
    const result = await run(f, { entry });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout + result.stderr, '');
    assert.equal(activity(f), 'ran\n');
  });
}

for (const alias of [false, true]) {
  test(`stdin imports stay inert with an existing dash alias ${alias}`, async (t) => {
    const f = fixture(t);
    if (alias) symlinkSync(f.entry, join(f.directory, '-'), 'file');
    const result = await run(f, {
      entry: '-',
      flags: ['--input-type=module'],
      input: `process.exitCode = 7; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
    });
    assert.equal(result.status, 7, result.stderr);
    assert.equal(result.stdout + result.stderr, '');
    assert.equal(activity(f), '');
  });
}

test('an absent importing entry path stays inert', async (t) => {
  const f = fixture(t);
  const entry = join(f.root, 'importer.mjs');
  writeFileSync(
    entry,
    "process.exitCode = 7; process.argv[1] = 'missing-importer.mjs'; await import('./scripts/guarded.mjs');",
  );
  const result = await run(f, { entry });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
});

test('an eval worker import stays inert when its virtual name aliases the module', async (t) => {
  const f = fixture(t);
  symlinkSync(f.entry, join(f.directory, '[worker eval]'), 'file');
  const program = `process.exitCode = 7; void import(${JSON.stringify(pathToFileURL(f.entry).href)});`;
  const entry = join(f.root, 'worker-importer.cjs');
  writeFileSync(
    entry,
    `const { Worker } = require('node:worker_threads');
const worker = new Worker(${JSON.stringify(program)}, { eval: true, stdout: true, stderr: true });
worker.stdout.pipe(process.stdout);
worker.stderr.pipe(process.stderr);
worker.on('error', error => { throw error; });
worker.on('exit', code => { process.exitCode = code; });`,
  );
  const result = await run(f, { entry });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
});
