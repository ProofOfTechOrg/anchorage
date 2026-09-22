// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { test } from 'node:test';
import { pathToFileURL } from 'node:url';
import {
  activity,
  createChildProcessFixture,
  repositoryRoot,
} from './child-process-fixture.mjs';

const { createRoot, run } = createChildProcessFixture({
  prefix: 'anchorage-entry-point-',
  watchdog: 'entry-point',
  scripts: ['entry-point.mjs'],
  flags: [],
});

const packageScripts = {
  seed: 'packages/flowsafe/scripts/seed-deployment-identity.mjs',
  mint: 'packages/agent-starter/scripts/mint-token.mjs',
  emit: 'packages/agent-starter/scripts/emit-conformance-config.mjs',
};

function copy(root, source, destination) {
  const target = join(root, destination);
  mkdirSync(dirname(target), { recursive: true });
  copyFileSync(join(repositoryRoot, source), target);
  return target;
}

function fixture(t, kind = 'root') {
  const { root, directory, events } = createRoot(t);
  let entry;
  let output;
  if (kind === 'root') {
    entry = join(root, 'scripts/guarded.mjs');
    writeFileSync(
      entry,
      `import { appendFileSync } from 'node:fs';
import { isInvokedAsEntryPoint } from './entry-point.mjs';

if (isInvokedAsEntryPoint(import.meta.url)) {
  appendFileSync(${JSON.stringify(events)}, 'ran\\n');
}
`,
    );
  } else {
    entry = copy(root, packageScripts[kind], `scripts/${kind}.mjs`);
    if (kind === 'seed') {
      copy(root, 'packages/flowsafe/package.json', 'package.json');
      copy(
        root,
        'packages/flowsafe/deployment-identity-protocol.mjs',
        'deployment-identity-protocol.mjs',
      );
    } else if (kind === 'mint') {
      symlinkSync(
        join(repositoryRoot, 'packages/agent-starter/node_modules'),
        join(root, 'node_modules'),
        'junction',
      );
    } else {
      copy(
        root,
        'packages/agent-starter/src/conformance/contract.json',
        'src/conformance/contract.json',
      );
      mkdirSync(join(root, 'conformance'));
      output = join(root, 'conformance/anchorage-starter.conformance.json');
    }
  }
  return {
    root,
    directory,
    entry,
    events,
    kind,
    output,
    argv: kind === 'seed' ? ['--entry-guard-probe'] : [],
    env: { AUTH_HMAC_SECRET: '' },
  };
}

function assertDirect(f, result) {
  if (f.kind === 'seed') {
    assert.equal(result.status, 1, result.stderr);
    assert.equal(result.stdout, '');
    assert.match(result.stderr, /unknown option '--entry-guard-probe'/);
  } else if (f.kind === 'mint') {
    assert.equal(result.status, 1, result.stderr);
    assert.equal(result.stdout, '');
    assert.match(result.stderr, /AUTH_HMAC_SECRET is required/);
  } else {
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stderr, '');
    if (f.kind === 'emit') {
      assert.equal(result.stdout, `wrote ${f.output}\n`);
      assert.equal(
        JSON.parse(readFileSync(f.output, 'utf8')).contractVersion,
        1,
      );
    } else {
      assert.equal(result.stdout, '');
      assert.equal(activity(f), 'ran\n');
    }
  }
}

function assertInert(f, result, stdout = '', existingOutput) {
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout, stdout);
  assert.equal(result.stderr, '');
  assert.equal(activity(f), '');
  if (f.output) {
    if (existingOutput === undefined) assert.equal(existsSync(f.output), false);
    else assert.equal(readFileSync(f.output, 'utf8'), existingOutput);
  }
}

function workerProgram(target, options) {
  return `const { Worker } = require('node:worker_threads');
const worker = new Worker(${JSON.stringify(target)}, ${JSON.stringify(options)});
worker.stdout.pipe(process.stdout);
worker.stderr.pipe(process.stderr);
worker.on('error', error => { throw error; });
worker.on('exit', code => { process.exitCode = code; });`;
}

for (const kind of ['root', 'seed', 'mint', 'emit']) {
  for (const entryKind of ['direct', 'file symlink', 'directory symlink']) {
    test(`${kind}: ${entryKind} invocation is the entry point`, async (t) => {
      const f = fixture(t, kind);
      let entry = f.entry;
      if (entryKind === 'file symlink') {
        entry = join(f.directory, 'linked.mjs');
        symlinkSync(f.entry, entry, 'file');
      } else if (entryKind === 'directory symlink') {
        const linked = join(f.directory, 'linked');
        symlinkSync(f.root, linked, 'junction');
        entry = join(linked, relative(f.root, f.entry));
      }
      const result = await run(f, { entry, argv: f.argv, env: f.env });
      assertDirect(f, result);
    });
  }

  test(`${kind}: an importing entry remains inert and preserves output`, async (t) => {
    const f = fixture(t, kind);
    const existingOutput = f.output ? 'existing configuration\n' : undefined;
    if (f.output) writeFileSync(f.output, existingOutput);
    const entry = join(f.root, 'importer.mjs');
    writeFileSync(
      entry,
      `process.exitCode = 7; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
    );
    const result = await run(f, { entry, argv: f.argv, env: f.env });
    assertInert(f, result, '', existingOutput);
  });

  for (const alias of [false, true]) {
    test(`${kind}: stdin imports stay inert with an existing dash alias ${alias}`, async (t) => {
      const f = fixture(t, kind);
      if (alias) symlinkSync(f.entry, join(f.directory, '-'), 'file');
      const result = await run(f, {
        entry: '-',
        argv: f.argv,
        env: f.env,
        flags: ['--input-type=module'],
        input: `process.exitCode = 7; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
      });
      assertInert(f, result);
    });
  }

  for (const option of [
    '-e',
    '--eval',
    '--eval=',
    '-p',
    '--print',
    '--print=true',
  ]) {
    for (const matchingFile of [false, true]) {
      test(`${kind}: ${option} imports stay inert with a matching file argument ${matchingFile}`, async (t) => {
        const f = fixture(t, kind);
        const program = `process.exitCode = 7; void import(${JSON.stringify(pathToFileURL(f.entry).href)});`;
        const positional = matchingFile ? f.entry : 'not-a-script';
        const attached = option.endsWith('=');
        const result = await run(f, {
          entry: attached ? positional : program,
          argv: attached ? f.argv : [positional, ...f.argv],
          flags: [attached ? `${option}${program}` : option],
          env: f.env,
        });
        assertInert(f, result, option.includes('p') ? 'undefined\n' : '');
      });
    }
  }

  for (const entryValue of [
    undefined,
    '',
    'missing-importer.mjs',
    'not-a-directory',
  ]) {
    test(`${kind}: invalid importing entry ${JSON.stringify(entryValue)} stays inert`, async (t) => {
      const f = fixture(t, kind);
      const entry = join(f.root, 'importer.mjs');
      const value =
        entryValue === 'not-a-directory' ? `${f.entry}/child` : entryValue;
      writeFileSync(
        entry,
        `process.exitCode = 7; process.argv[1] = ${JSON.stringify(value)}; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
      );
      const result = await run(f, { entry, argv: f.argv, env: f.env });
      assertInert(f, result);
    });
  }

  test(`${kind}: a self-referential argv symlink fails loudly`, async (t) => {
    const f = fixture(t, kind);
    const loop = join(f.directory, 'argv-loop');
    symlinkSync(loop, loop, 'file');
    const entry = join(f.root, 'importer.mjs');
    writeFileSync(
      entry,
      `process.exitCode = 7; process.argv[1] = ${JSON.stringify(loop)}; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
    );
    const result = await run(f, { entry, argv: f.argv, env: f.env });
    assert.equal(result.status, 1);
    assert.equal(result.stdout, '');
    assert.match(result.stderr, /ELOOP: too many symbolic links encountered/u);
    assert.equal(activity(f), '');
    if (f.output) assert.equal(existsSync(f.output), false);
  });

  test(`${kind}: an eval worker import stays inert when its virtual name aliases the module`, async (t) => {
    const f = fixture(t, kind);
    symlinkSync(f.entry, join(f.directory, '[worker eval]'), 'file');
    const program = `process.exitCode = 7; void import(${JSON.stringify(pathToFileURL(f.entry).href)});`;
    const entry = join(f.root, 'worker-importer.cjs');
    writeFileSync(
      entry,
      workerProgram(program, {
        eval: true,
        argv: f.argv,
        stdout: true,
        stderr: true,
      }),
    );
    const result = await run(f, { entry, argv: [], env: f.env });
    assertInert(f, result);
  });

  test(`${kind}: a worker file remains an entry when its parent uses eval`, async (t) => {
    const f = fixture(t, kind);
    const program = workerProgram(f.entry, {
      argv: f.argv,
      stdout: true,
      stderr: true,
    });
    const result = await run(f, {
      entry: program,
      argv: [],
      flags: ['-e'],
      env: f.env,
    });
    assertDirect(f, result);
  });
}

for (const options of [
  undefined,
  {},
  { entry: undefined },
  { entry: null },
  { entry: '' },
  { entry: 7 },
]) {
  test(`run rejects invalid entry options ${JSON.stringify(options)} before spawning`, async (t) => {
    const f = createRoot(t);
    await assert.rejects(run(f, options), {
      name: 'TypeError',
      message: 'child-process fixture entry must be a nonempty string',
    });
    assert.equal(activity(f), '');
  });
}

for (const override of [false, true]) {
  test(`fixture environment defaults permit caller overrides ${override}`, async (t) => {
    const f = createRoot(t);
    const expected = override ? ['--no-deprecation', '1', '1'] : ['', '0', '0'];
    const result = await run(f, {
      entry:
        'console.log(JSON.stringify([process.env.NODE_OPTIONS, process.env.COREPACK_ENABLE_NETWORK, process.env.COREPACK_ENABLE_AUTO_PIN]))',
      flags: ['-e'],
      env: override
        ? {
            NODE_OPTIONS: expected[0],
            COREPACK_ENABLE_NETWORK: expected[1],
            COREPACK_ENABLE_AUTO_PIN: expected[2],
          }
        : {},
    });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stderr, '');
    assert.deepEqual(JSON.parse(result.stdout), expected);
  });
}
