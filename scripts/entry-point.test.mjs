// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { symlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import { pathToFileURL } from 'node:url';
import {
  activity,
  createChildProcessFixture,
} from './child-process-fixture.mjs';

const { createRoot, run } = createChildProcessFixture({
  prefix: 'anchorage-entry-point-',
  watchdog: 'entry-point',
  scripts: ['entry-point.mjs'],
  flags: [],
});

function fixture(t) {
  const { root, directory, events } = createRoot(t);
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
    const result = await run(f, [], { entry });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout + result.stderr, '');
    assert.equal(activity(f), 'ran\n');
  });
}

for (const alias of [false, true]) {
  test(`stdin imports stay inert with an existing dash alias ${alias}`, async (t) => {
    const f = fixture(t);
    if (alias) symlinkSync(f.entry, join(f.directory, '-'), 'file');
    const result = await run(f, [], {
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
  const result = await run(f, [], { entry });
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
  const result = await run(f, [], { entry });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
});
