// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { delimiter, dirname, join, resolve } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));

function put(root, path, source) {
  const target = join(root, path);
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, source);
  return target;
}

function fixture(
  t,
  {
    value = '{ answer: 42 }',
    committed = `export const BASELINE = ${value};`,
    worldModule = 'fixtures/world.ts',
    baselineFile = 'fixtures/baseline.ts',
    world = `export function run() { return { value: ${value} }; }`,
    config = '',
    before = '',
    after = '',
  } = {},
) {
  const directory = mkdtempSync(join(tmpdir(), 'anchorage-baseline-recorder-'));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const root = join(directory, 'repo');
  mkdirSync(join(root, 'scripts'), { recursive: true });
  copyFileSync(
    join(repositoryRoot, 'scripts/baseline-recorder.mjs'),
    join(root, 'scripts/baseline-recorder.mjs'),
  );
  const { packageManager } = JSON.parse(
    readFileSync(join(repositoryRoot, 'package.json'), 'utf8'),
  );
  put(
    root,
    'package.json',
    `${JSON.stringify({ type: 'module', packageManager })}\n`,
  );
  symlinkSync(
    join(repositoryRoot, 'node_modules'),
    join(root, 'node_modules'),
    'junction',
  );
  put(
    root,
    'biome.json',
    '{"javascript":{"formatter":{"quoteStyle":"single"}}}\n',
  );
  const events = join(root, 'events.log');
  const mark = `import { appendFileSync } from 'node:fs';
const mark = (event) => appendFileSync(${JSON.stringify(events)}, event + '\\n');`;
  const worldPath = put(
    root,
    worldModule,
    `${mark}\nmark('import');\n${world}`,
  );
  const baselinePath =
    committed === null
      ? join(root, baselineFile)
      : put(root, baselineFile, committed);
  const entry = put(
    root,
    'scripts/record.mjs',
    `${mark}
${before}
const { runBaselineRecorder } = await import('./baseline-recorder.mjs');
await runBaselineRecorder({
  scriptUrl: import.meta.url,
  noun: 'fixture',
  worldModule: ${JSON.stringify(worldModule)},
  baselineFile: ${JSON.stringify(baselineFile)},
  run: async (world) => { mark('run'); return await world.run(); },
  imports: '',
  exports: [{ name: 'BASELINE', key: 'value', satisfies: 'unknown' }],
  summary: () => 'fixture summary',
  ${config}
});
${after}
`,
  );
  return {
    root,
    directory,
    entry,
    events,
    worldPath,
    baselinePath,
    baselineFile,
  };
}

async function run(
  f,
  argv = ['--check'],
  {
    flags = ['--experimental-transform-types'],
    entry = f.entry,
    cwd = f.directory,
    env = {},
    input,
  } = {},
) {
  const child = spawn(
    process.execPath,
    ['--no-warnings', ...flags, entry, ...argv],
    {
      cwd,
      env: {
        ...process.env,
        NODE_OPTIONS: '',
        COREPACK_ENABLE_NETWORK: '0',
        COREPACK_ENABLE_AUTO_PIN: '0',
        ...env,
      },
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
    const result = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (status, signal) => resolve({ status, signal }));
    });
    assert.equal(
      timedOut,
      false,
      `recorder watchdog expired\n${stdout}\n${stderr}`,
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

function matchOutput(f) {
  return `fixture baseline matches ${f.baselineFile}: fixture summary\n`;
}

function assertError(result, message) {
  assert.equal(result.status, 1, result.stderr);
  assert.equal(result.stdout, '');
  assert.ok(
    result.stderr.split('\n').includes(`Error: ${message}`),
    result.stderr,
  );
}

for (const [argv, diagnostic] of [
  [[], 'missing mode: choose --check or --write'],
  [['--unknown'], "unknown argument '--unknown'"],
  [['--check', '--unknown'], "unknown argument '--unknown'"],
  [['--write', '--unknown'], "unknown argument '--unknown'"],
  [['--check', '--write'], 'conflicting modes: choose --check or --write'],
  [['--write', '--check'], 'conflicting modes: choose --check or --write'],
]) {
  test(`rejects modes ${JSON.stringify(argv)} before world execution`, async (t) => {
    const f = fixture(t, { after: "mark('returned');" });
    const before = readFileSync(f.baselinePath);
    const result = await run(f, argv, { flags: [] });
    assert.equal(result.status, 2, result.stderr);
    assert.equal(result.stdout, '');
    assert.equal(
      result.stderr,
      `${diagnostic}\nusage: node scripts/record.mjs --check | --write\n`,
    );
    assert.equal(activity(f), 'returned\n');
    assert.deepEqual(readFileSync(f.baselinePath), before);
  });
}

test('repeated check mode matches outside the repository without writing', async (t) => {
  const f = fixture(t);
  const before = readFileSync(f.baselinePath);
  const result = await run(f, ['--check', '--check']);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
  assert.equal(result.stderr, '');
  assert.equal(activity(f), 'import\nrun\n');
  assert.deepEqual(readFileSync(f.baselinePath), before);
});

for (const entryKind of [
  'direct',
  'file symlink',
  'directory symlink',
  'encoded script URL',
]) {
  test(`${entryKind} reports drift and preserves the target`, async (t) => {
    const f = fixture(t, {
      committed: 'export const BASELINE = { answer: 41 };',
      config:
        entryKind === 'encoded script URL'
          ? "scriptUrl: import.meta.url.replace('record.mjs', '%72ecord.mjs'),"
          : '',
    });
    let entry = f.entry;
    if (entryKind === 'file symlink') {
      entry = join(f.directory, 'linked.mjs');
      symlinkSync(f.entry, entry, 'file');
    } else if (entryKind === 'directory symlink') {
      const linked = join(f.directory, 'linked');
      symlinkSync(f.root, linked, 'junction');
      entry = join(linked, 'scripts/record.mjs');
    }
    const before = readFileSync(f.baselinePath);
    const result = await run(f, ['--check'], { entry, flags: [] });
    assert.equal(result.status, 1, result.stderr);
    assert.equal(result.stdout, '');
    assert.equal(
      result.stderr,
      `fixture baseline drifted from fixtures/baseline.ts\n1 structural difference(s), committed vs re-derived from the unchanged world:\n  value.answer: committed 41 / derived 42\n`,
    );
    assert.equal(activity(f), 'import\nrun\n');
    assert.deepEqual(readFileSync(f.baselinePath), before);
  });
}

test('an importing entry remains inert and keeps its exit status', async (t) => {
  const f = fixture(t);
  const before = readFileSync(f.baselinePath);
  const entry = put(
    f.root,
    'importer.mjs',
    `process.exitCode = 7; await import('./scripts/record.mjs');`,
  );
  const result = await run(f, ['--write'], { entry, flags: [] });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
  assert.deepEqual(readFileSync(f.baselinePath), before);
});

for (const alias of [false, true]) {
  test(`stdin imports stay inert with an existing dash alias ${alias}`, async (t) => {
    const f = fixture(t);
    if (alias) symlinkSync(f.entry, join(f.directory, '-'), 'file');
    const before = readFileSync(f.baselinePath);
    const result = await run(f, ['--write'], {
      entry: '-',
      flags: ['--input-type=module'],
      input: `process.exitCode = 7; await import(${JSON.stringify(pathToFileURL(f.entry).href)});`,
    });
    assert.equal(result.status, 7, result.stderr);
    assert.equal(result.stdout + result.stderr, '');
    assert.equal(activity(f), '');
    assert.deepEqual(readFileSync(f.baselinePath), before);
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
    test(`${option} imports stay inert with a matching file argument ${matchingFile}`, async (t) => {
      const f = fixture(t);
      const before = readFileSync(f.baselinePath);
      const program = `process.exitCode = 7; void import(${JSON.stringify(pathToFileURL(f.entry).href)});`;
      const positional = matchingFile ? f.entry : 'not-a-script';
      const attached = option.endsWith('=');
      const result = await run(
        f,
        attached ? ['--write'] : [positional, '--write'],
        {
          flags: [attached ? `${option}${program}` : option],
          entry: attached ? positional : program,
        },
      );
      assert.equal(result.status, 7, result.stderr);
      assert.equal(result.stdout, option.includes('p') ? 'undefined\n' : '');
      assert.equal(result.stderr, '');
      assert.equal(activity(f), '');
      assert.deepEqual(readFileSync(f.baselinePath), before);
    });
  }
}

test('an absent importing entry path stays inert', async (t) => {
  const f = fixture(t);
  const before = readFileSync(f.baselinePath);
  const entry = put(
    f.root,
    'importer.mjs',
    "process.exitCode = 7; process.argv[1] = 'missing-importer.mjs'; await import('./scripts/record.mjs');",
  );
  const result = await run(f, ['--write'], { entry });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
  assert.deepEqual(readFileSync(f.baselinePath), before);
});

test('a worker file remains an entry when its parent uses eval', async (t) => {
  const f = fixture(t);
  const program = `const { Worker } = require('node:worker_threads');
const worker = new Worker(${JSON.stringify(f.entry)}, { argv: ['--check'], stdout: true, stderr: true });
worker.stdout.pipe(process.stdout);
worker.stderr.pipe(process.stderr);
worker.on('error', error => { throw error; });
worker.on('exit', code => { process.exitCode = code; });`;
  const result = await run(f, [], { flags: ['-e'], entry: program });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
  assert.equal(result.stderr, '');
  assert.equal(activity(f), 'import\nrun\n');
});

test('an eval worker import stays inert when its virtual name aliases the recorder', async (t) => {
  const f = fixture(t);
  symlinkSync(f.entry, join(f.directory, '[worker eval]'), 'file');
  const before = readFileSync(f.baselinePath);
  const program = `process.exitCode = 7; void import(${JSON.stringify(pathToFileURL(f.entry).href)});`;
  const entry = put(
    f.root,
    'worker-importer.cjs',
    `const { Worker } = require('node:worker_threads');
const worker = new Worker(${JSON.stringify(program)}, { eval: true, argv: ['--write'], stdout: true, stderr: true });
worker.stdout.pipe(process.stdout);
worker.stderr.pipe(process.stderr);
worker.on('error', error => { throw error; });
worker.on('exit', code => { process.exitCode = code; });`,
  );
  const result = await run(f, [], { entry });
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
  assert.deepEqual(readFileSync(f.baselinePath), before);
});

test('an absent argv entry remains inert even without a configured URL', async (t) => {
  const f = fixture(t, {
    before: 'process.argv.splice(1); process.exitCode = 7;',
    config: 'scriptUrl: undefined,',
  });
  const result = await run(f, ['--write']);
  assert.equal(result.status, 7, result.stderr);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), '');
});

for (const scriptUrl of [
  "'not a URL'",
  "'https://example.invalid/recorder.mjs'",
  "'data:text/javascript,'",
]) {
  test(`rejects invalid recorder identity ${scriptUrl}`, async (t) => {
    const f = fixture(t, { config: `scriptUrl: ${scriptUrl},` });
    assertError(await run(f), 'recorder scriptUrl must be a file URL');
    assert.equal(activity(f), '');
  });
}

test('a missing script URL target fails loudly', async (t) => {
  const f = fixture(t, {
    config: "scriptUrl: new URL('./missing.mjs', import.meta.url),",
  });
  const result = await run(f);
  assert.equal(result.status, 1);
  assert.match(result.stderr, /Error: ENOENT:.*missing\.mjs/);
  assert.equal(result.stdout, '');
  assert.equal(activity(f), '');
});

test('a directory script URL target fails loudly', async (t) => {
  const f = fixture(t, {
    config: "scriptUrl: new URL('./', import.meta.url),",
  });
  assertError(await run(f), 'recorder scriptUrl must name a file');
  assert.equal(activity(f), '');
});

for (const flags of [[], ['--experimental-transform-types']]) {
  test(`type transformation and .js resolution run once with ${JSON.stringify(flags)}`, async (t) => {
    const f = fixture(t, {
      world: `import { Box } from './box.js'; import { choice } from './choice.js';
export function run() { return { value: { answer: new Box(42).value, choice } }; }`,
      committed: `export const BASELINE = { answer: 42, choice: 'js' };`,
      after: "mark('returned');",
    });
    put(
      f.root,
      'fixtures/box.ts',
      'export class Box { constructor(public value: number) {} }',
    );
    put(f.root, 'fixtures/choice.js', "export const choice = 'js';");
    put(f.root, 'fixtures/choice.ts', "export const choice = 'ts';");
    const result = await run(f, ['--check'], { flags });
    assert.equal(result.status, 0, result.stderr);
    assert.equal(result.stdout, matchOutput(f));
    assert.equal(result.stderr, '');
    assert.equal(
      activity(f),
      `import\nrun\nreturned\n${flags.length === 0 ? 'returned\n' : ''}`,
    );
  });
}

test('transform child errors return to the parent without another world run', async (t) => {
  const f = fixture(t, {
    world: "export function run() { throw new Error('WORLD_REFUSED'); }",
    after: "mark('returned');",
  });
  assertError(await run(f, ['--check'], { flags: [] }), 'WORLD_REFUSED');
  assert.equal(activity(f), 'import\nrun\nreturned\n');
});

test('preserves a transform child exit status set during natural completion', async (t) => {
  const f = fixture(t, {
    world: `export function run() {
      process.once('beforeExit', () => { process.exitCode = 23; });
      return { value: { answer: 42 } };
    }`,
    after: "mark('returned');",
  });
  const result = await run(f, ['--check'], { flags: [] });
  assert.equal(result.status, 23, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
  assert.equal(result.stderr, '');
  assert.equal(activity(f), 'import\nrun\nreturned\nreturned\n');
});

test('transform child spawn errors remain failures', async (t) => {
  const f = fixture(t, {
    before:
      "Object.defineProperty(process, 'execPath', { value: '/missing-recorder-node' });",
  });
  const result = await run(f, ['--check'], { flags: [] });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /spawnSync \/missing-recorder-node ENOENT/);
  assert.equal(result.stdout, '');
  assert.equal(activity(f), '');
});

test('signal termination of a transform child returns failure', {
  skip: process.platform === 'win32',
}, async (t) => {
  const f = fixture(t, {
    world: "export function run() { process.kill(process.pid, 'SIGTERM'); }",
    after: "mark('returned');",
  });
  const result = await run(f, ['--check'], { flags: [] });
  assert.equal(result.status, 1);
  assert.equal(result.stdout + result.stderr, '');
  assert.equal(activity(f), 'import\nrun\nreturned\n');
});

for (const flags of [[], ['--experimental-transform-types']]) {
  test(`drains the complete large report with ${JSON.stringify(flags)}`, async (t) => {
    const rows = Array.from(
      { length: 4096 },
      (_, index) => `row-${index}-${'x'.repeat(80)}`,
    );
    const f = fixture(t, {
      value: JSON.stringify([...rows, 'DERIVED_FINAL_SENTINEL']),
      committed: `export const BASELINE = ${JSON.stringify([...rows.map((row) => `old-${row}`), 'COMMITTED_FINAL_SENTINEL'])};`,
    });
    const expected = [
      ...rows.map(
        (row, index) =>
          `  value[${index}]: committed 'old-${row}' / derived '${row}'`,
      ),
      `  value[4096]: committed 'COMMITTED_FINAL_SENTINEL' / derived 'DERIVED_FINAL_SENTINEL'`,
    ];
    const result = await run(f, ['--check'], { flags });
    assert.equal(result.status, 1);
    assert.equal(result.stdout, '');
    assert.deepEqual(result.stderr.split('\n'), [
      'fixture baseline drifted from fixtures/baseline.ts',
      '4097 structural difference(s), committed vs re-derived from the unchanged world:',
      ...expected,
      '',
    ]);
  });
}

test('file URL imports preserve spaces, percent, query and fragment characters', async (t) => {
  const f = fixture(t, {
    worldModule: 'fixtures/space % # ?/world.ts',
    baselineFile: 'fixtures/space % # ?/baseline.ts',
  });
  const result = await run(f);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
  assert.equal(result.stderr, '');
});

for (const field of ['worldModule', 'baselineFile']) {
  for (const [path, diagnostic] of [
    ['', 'repository-relative filesystem path'],
    [null, 'repository-relative filesystem path'],
    [42, 'repository-relative filesystem path'],
    [resolve('/absolute-recorder.ts'), 'repository-relative filesystem path'],
    ['file:///world.ts', 'repository-relative filesystem path'],
    ['https://example.invalid/world.ts', 'repository-relative filesystem path'],
    ['data:text/javascript,', 'repository-relative filesystem path'],
    ['../outside.ts', 'resolve to a path inside the repository'],
    ['fixtures/../../outside.ts', 'resolve to a path inside the repository'],
    ['.', 'resolve to a path inside the repository'],
    ['fixtures/..', 'resolve to a path inside the repository'],
  ]) {
    test(`refuses ${field} ${JSON.stringify(path)} before world execution`, async (t) => {
      const f = fixture(t, { config: `${field}: ${JSON.stringify(path)},` });
      const before = readFileSync(f.baselinePath);
      const prefix = diagnostic.startsWith('resolve') ? 'must' : 'must be a';
      assertError(
        await run(f, ['--write']),
        `${field} ${prefix} ${diagnostic}`,
      );
      assert.equal(activity(f), '');
      assert.deepEqual(readFileSync(f.baselinePath), before);
    });
  }
}

test('normalizes relative paths within the repository', async (t) => {
  const f = fixture(t, {
    config:
      "worldModule: './fixtures/../fixtures/world.ts', baselineFile: './fixtures/../fixtures/baseline.ts',",
  });
  const result = await run(f);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stderr, '');
  assert.equal(activity(f), 'import\nrun\n');
});

test('distinguishes a missing world from an existing world with a missing dependency', async (t) => {
  const missing = fixture(t, { config: "worldModule: 'fixtures/absent.ts'," });
  const result = await run(missing);
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.equal(result.stderr, 'fixture world is missing: fixtures/absent.ts\n');
  assert.equal(activity(missing), '');
  const nested = fixture(t, {
    world:
      "import './nested-missing.js'; export function run() { return { value: 42 }; }",
  });
  const failure = await run(nested);
  assert.equal(failure.status, 1);
  assert.equal(failure.stdout, '');
  assert.match(failure.stderr, /ERR_MODULE_NOT_FOUND/);
  assert.match(failure.stderr, /nested-missing\.js/);
  assert.doesNotMatch(failure.stderr, /fixture world is missing/);
  assert.equal(activity(nested), '');
});

test('missing baseline prints an explicit write hint without creating a file', async (t) => {
  const f = fixture(t, { committed: null });
  const result = await run(f);
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.equal(
    result.stderr,
    'fixture baseline is missing: fixtures/baseline.ts\nrun `node scripts/record.mjs --write` to record the baseline\n',
  );
  assert.equal(existsSync(f.baselinePath), false);
});

test('a broken committed import retains its own error', async (t) => {
  const f = fixture(t, {
    committed: "import './baseline-dependency.js'; export const BASELINE = {};",
  });
  const result = await run(f);
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /baseline-dependency\.js/);
  assert.match(result.stderr, /ERR_MODULE_NOT_FOUND/);
  assert.doesNotMatch(result.stderr, /baseline is missing/);
});

test('requires configured committed export presence even opposite undefined', async (t) => {
  const f = fixture(t, {
    value: 'undefined',
    committed: 'export const OTHER = undefined;',
  });
  assertError(
    await run(f),
    "committed export 'BASELINE': missing own property",
  );
});

for (const mode of ['--check', '--write']) {
  for (const returned of [
    '{}',
    'Object.create({ value: undefined })',
    'null',
  ]) {
    test(`${mode} requires an own derived key in ${returned}`, async (t) => {
      const f = fixture(t, {
        value: 'undefined',
        world: `export function run() { return ${returned}; }`,
      });
      const before = readFileSync(f.baselinePath);
      assertError(
        await run(f, [mode]),
        "derived key 'value': missing own property",
      );
      assert.deepEqual(readFileSync(f.baselinePath), before);
    });
  }
}

test('an extra unconfigured export remains outside comparison', async (t) => {
  const f = fixture(t, {
    committed:
      'export const BASELINE = { answer: 42 }; export const OTHER = new Date();',
  });
  const result = await run(f);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
});

for (const [name, value, committed, differences] of [
  [
    'undefined key presence',
    '{ key: undefined }',
    '{}',
    [
      'value: committed keys [] / derived keys ["key"]',
      'value.key: derived only undefined',
    ],
  ],
  [
    'comma keys',
    '{ "a,b": undefined }',
    '{ a: undefined, b: undefined }',
    [
      'value: committed keys ["a","b"] / derived keys ["a,b"]',
      'value.a: committed only undefined',
      'value.b: committed only undefined',
      'value.a,b: derived only undefined',
    ],
  ],
  [
    'key ordering',
    '{ b: 2, a: 1 }',
    '{ a: 1, b: 2 }',
    ['value: committed keys ["a","b"] / derived keys ["b","a"]'],
  ],
  ['negative zero', '-0', '0', ['value: committed 0 / derived -0']],
  [
    'prototype-named key presence',
    '{ constructor: undefined, toString: undefined }',
    '{}',
    [
      'value: committed keys [] / derived keys ["constructor","toString"]',
      'value.constructor: derived only undefined',
      'value.toString: derived only undefined',
    ],
  ],
  [
    'array length',
    '[1, undefined]',
    '[2]',
    [
      'value: committed 1 item(s) / derived 2 item(s)',
      'value[0]: committed 2 / derived 1',
      'value[1]: derived only undefined',
    ],
  ],
  [
    'committed array tail',
    '[]',
    '[null]',
    [
      'value: committed 1 item(s) / derived 0 item(s)',
      'value[0]: committed only null',
    ],
  ],
  [
    'array versus map',
    '{}',
    '[]',
    ['value: committed array(0) / derived object{}'],
  ],
  [
    'composite versus primitive',
    'null',
    '[]',
    ['value: committed array(0) / derived null'],
  ],
]) {
  test(`structural comparison distinguishes ${name}`, async (t) => {
    const f = fixture(t, {
      value,
      committed: `export const BASELINE = ${committed};`,
    });
    const before = readFileSync(f.baselinePath);
    const result = await run(f);
    assert.equal(result.status, 1, result.stderr);
    assert.equal(result.stdout, '');
    assert.equal(
      result.stderr,
      `fixture baseline drifted from fixtures/baseline.ts\n${differences.length} structural difference(s), committed vs re-derived from the unchanged world:\n${differences.map((line) => `  ${line}`).join('\n')}\n`,
    );
    assert.deepEqual(readFileSync(f.baselinePath), before);
  });
}

const unsupported = [
  ['date', 'new Date(0)', 'expected a plain map or ordinary array'],
  ['map', 'new Map()', 'expected a plain map or ordinary array'],
  ['set', 'new Set()', 'expected a plain map or ordinary array'],
  [
    'class',
    'new (class Example {})()',
    'expected a plain map or ordinary array',
  ],
  [
    'array subclass',
    'new (class Example extends Array {})()',
    'expected a plain map or ordinary array',
  ],
  [
    'typed array',
    'new Uint8Array(0)',
    'expected a plain map or ordinary array',
  ],
  ['function', '() => 1', "unsupported baseline value type 'function'"],
  ['bigint', '1n', "unsupported baseline value type 'bigint'"],
  ['symbol', 'Symbol("value")', "unsupported baseline value type 'symbol'"],
  [
    'cycle',
    '(() => { const value = {}; value.self = value; return value; })()',
    'cyclic baseline value',
    '["self"]',
  ],
  ['sparse array', '[,]', 'expected a dense array without extra properties'],
  [
    'array extra',
    'Object.assign([], { extra: 1 })',
    'expected a dense array without extra properties',
  ],
  [
    'array hole with extra',
    'Object.assign(Array(1), { extra: 1 })',
    'expected a dense array without extra properties',
  ],
  [
    'accessor',
    '({ get bad() { throw new Error("GETTER_EXECUTED"); } })',
    'expected an enumerable data property',
    '["bad"]',
  ],
  [
    'non-enumerable',
    'Object.defineProperty({}, "bad", { value: 1 })',
    'expected an enumerable data property',
    '["bad"]',
  ],
  [
    'symbol property',
    '({ [Symbol("bad")]: 1 })',
    'symbol properties are unsupported',
  ],
  [
    'array accessor',
    'Object.defineProperty([1], "0", { get() { throw new Error("GETTER_EXECUTED"); } })',
    'expected an enumerable data property',
    '[0]',
  ],
  [
    'array non-enumerable',
    'Object.defineProperty([1], "0", { enumerable: false })',
    'expected an enumerable data property',
    '[0]',
  ],
];

for (const [name, expression, message, suffix = ''] of unsupported) {
  for (const side of ['derived check', 'derived write', 'committed check']) {
    test(`${side} rejects nested ${name} before accepting or replacing the target`, async (t) => {
      const derived = side.startsWith('derived');
      const value = `({ nested: ${expression} })`;
      const f = fixture(t, {
        value: derived ? value : 'null',
        committed: `export const BASELINE = ${derived ? 'null' : value};`,
      });
      const before = readFileSync(f.baselinePath);
      const result = await run(f, [
        side.endsWith('write') ? '--write' : '--check',
      ]);
      const location = derived
        ? "derived key 'value'"
        : "committed export 'BASELINE'";
      assertError(result, `${location}["nested"]${suffix}: ${message}`);
      assert.doesNotMatch(result.stderr, /^Error: GETTER_EXECUTED$/m);
      assert.deepEqual(readFileSync(f.baselinePath), before);
    });
  }
}

test('rejects a configured key accessor without calling it', async (t) => {
  const f = fixture(t, {
    world:
      'export function run() { return { get value() { throw new Error("GETTER_EXECUTED"); } }; }',
  });
  assertError(
    await run(f, ['--write']),
    "derived key 'value': expected an enumerable data property",
  );
});

test('checks later declarations even after an earlier structural mismatch', async (t) => {
  const f = fixture(t, {
    value: 'null',
    committed:
      'export const BASELINE = 1; export const SECOND = { nested: new Map() };',
    world: 'export function run() { return { value: null, second: null }; }',
    config:
      "exports: [{ name: 'BASELINE', key: 'value', satisfies: 'unknown' }, { name: 'SECOND', key: 'second', satisfies: 'unknown' }],",
  });
  assertError(
    await run(f),
    'committed export \'SECOND\'["nested"]: expected a plain map or ordinary array',
  );
});

test('explicit repeated write preserves literal fidelity and world source with the installed formatter', async (t) => {
  const value = `(() => {
    const shared = Object.freeze({ value: undefined });
    return { text: "quote' slash\\\\ newline\\n carriage\\r tab\\t", absent: undefined, nil: null,
      yes: true, no: false, numbers: [-0, 0, NaN, Infinity, -Infinity, 1.25],
      nested: [shared, shared], empty: Object.create(null),
      map: Object.assign(Object.create(null), { key: undefined }),
      ['__proto__']: { safe: true } };
  })()`;
  const f = fixture(t, {
    value,
    config:
      "exports: [{ name: 'BASELINE', key: 'value', satisfies: 'unknown', jsDoc: '/** Preserve key presence. */' }, { name: 'SECOND', key: 'value', satisfies: 'unknown' }],",
  });
  const before = readFileSync(f.worldPath);
  const written = await run(f, ['--write', '--write']);
  assert.equal(written.status, 0, written.stderr);
  assert.equal(written.stdout, 'wrote fixtures/baseline.ts: fixture summary\n');
  assert.equal(written.stderr, '');
  assert.equal(activity(f), 'import\nrun\n');
  assert.deepEqual(readFileSync(f.worldPath), before);
  const source = readFileSync(f.baselinePath, 'utf8');
  assert.match(source, /GENERATED FILE\. DO NOT EDIT BY HAND\./);
  assert.match(source, /\/\*\* Preserve key presence\. \*\//);
  assert.doesNotMatch(source, /^undefined$/m);
  assert.match(source, /\[['"]__proto__['"]\]/);
  assert.match(source, /-0/);
  const checked = await run(f);
  assert.equal(checked.status, 0, checked.stderr);
  assert.equal(checked.stdout, matchOutput(f));
  assert.equal(checked.stderr, '');
  const entry = put(
    f.root,
    'assert-generated.mjs',
    `import assert from 'node:assert/strict';
import { BASELINE } from './fixtures/baseline.ts';
assert.equal(Object.hasOwn(BASELINE, '__proto__'), true);
assert.deepEqual(BASELINE.__proto__, { safe: true });
assert.equal(Object.getPrototypeOf(BASELINE), Object.prototype);
assert.equal(Object.hasOwn(BASELINE, 'absent'), true);
assert.equal(Object.hasOwn(BASELINE.map, 'key'), true);
assert.equal(Object.is(BASELINE.numbers[0], -0), true);
assert.equal(Number.isNaN(BASELINE.numbers[2]), true);
assert.equal(BASELINE.numbers[3], Infinity);
assert.equal(BASELINE.numbers[4], -Infinity);
`,
  );
  const imported = await run(f, [], { entry });
  assert.equal(imported.status, 0, imported.stderr);
  assert.equal(imported.stdout + imported.stderr, '');
});

test('formatter nonzero status fails without a success message', {
  skip: process.platform === 'win32',
}, async (t) => {
  const f = fixture(t);
  const command = put(
    f.root,
    'bin/pnpm',
    `#!${process.execPath}
import { writeFileSync } from 'node:fs';
writeFileSync(${JSON.stringify(join(f.root, 'formatter-call.json'))}, JSON.stringify({ cwd: process.cwd(), argv: process.argv.slice(2) }));
process.stderr.write('FORMATTER_REFUSED\\n'); process.exitCode = 17;\n`,
  );
  chmodSync(command, 0o755);
  const result = await run(f, ['--write'], {
    env: { PATH: `${join(f.root, 'bin')}${delimiter}${process.env.PATH}` },
  });
  assertError(result, 'biome refused the generated baseline');
  assert.ok(result.stderr.split('\n').includes('FORMATTER_REFUSED'));
  assert.deepEqual(
    JSON.parse(readFileSync(join(f.root, 'formatter-call.json'), 'utf8')),
    {
      cwd: f.root,
      argv: ['exec', 'biome', 'check', '--write', f.baselinePath],
    },
  );
});

test('formatter spawn errors remain failures', async (t) => {
  const f = fixture(t);
  const result = await run(f, ['--write'], {
    env: { PATH: join(f.root, 'missing-bin') },
  });
  assert.equal(result.status, 1);
  assert.equal(result.stdout, '');
  assert.match(result.stderr, /spawnSync pnpm(?:\.cmd)? ENOENT/);
});

test('unsupported later derived declarations cannot replace an earlier valid export', async (t) => {
  const f = fixture(t, {
    world: 'export function run() { return { value: 42, second: new Set() }; }',
    config:
      "exports: [{ name: 'BASELINE', key: 'value', satisfies: 'unknown' }, { name: 'SECOND', key: 'second', satisfies: 'unknown' }],",
  });
  const before = readFileSync(f.baselinePath);
  assertError(
    await run(f, ['--write']),
    "derived key 'second': expected a plain map or ordinary array",
  );
  assert.deepEqual(readFileSync(f.baselinePath), before);
});

test('a non-enumerable configured key is refused', async (t) => {
  const f = fixture(t, {
    world:
      'export function run() { return Object.defineProperty({}, "value", { value: 42 }); }',
  });
  assertError(
    await run(f, ['--write']),
    "derived key 'value': expected an enumerable data property",
  );
});

test('drains a long usage diagnostic before returning status 2', async (t) => {
  const argument = `--${'x'.repeat(64 * 1024)}`;
  const f = fixture(t);
  const result = await run(f, [argument], { flags: [] });
  assert.equal(result.status, 2);
  assert.equal(result.stdout, '');
  assert.equal(
    result.stderr,
    `unknown argument '${argument}'\nusage: node scripts/record.mjs --check | --write\n`,
  );
  assert.equal(activity(f), '');
});

test('drains a long success summary through the transform parent', async (t) => {
  const summary = `${'s'.repeat(256 * 1024)}FINAL_SUMMARY`;
  const f = fixture(t, {
    config: `summary: () => ${JSON.stringify(summary)},`,
  });
  const result = await run(f, ['--check'], { flags: [] });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(
    result.stdout,
    `fixture baseline matches fixtures/baseline.ts: ${summary}\n`,
  );
  assert.equal(result.stderr, '');
});

test('writes valid TypeScript for scalar literals, undefined, null and nonfinite numbers', async (t) => {
  const f = fixture(t, {
    world: `export function run() { return { u: undefined, n: null, nan: NaN, pos: Infinity, neg: -Infinity, zero: -0, bool: false, text: 'ok' }; }`,
    config: `exports: ['u', 'n', 'nan', 'pos', 'neg', 'zero', 'bool', 'text'].map(key => ({ name: key.toUpperCase(), key, satisfies: 'unknown' })),`,
  });
  const written = await run(f, ['--write']);
  assert.equal(written.status, 0, written.stderr);
  assert.equal(written.stderr, '');
  const require = createRequire(
    join(repositoryRoot, 'packages/fleet-control/package.json'),
  );
  const ts = require('typescript');
  const program = ts.createProgram([f.baselinePath], {
    strict: true,
    noEmit: true,
    types: [],
  });
  assert.deepEqual(
    ts
      .getPreEmitDiagnostics(program)
      .map((diagnostic) =>
        ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'),
      ),
    [],
  );
  const checked = await run(f);
  assert.equal(checked.status, 0, checked.stderr);
  assert.equal(checked.stdout, matchOutput(f));
  assert.equal(checked.stderr, '');
});

test('installed formatter refusal propagates for invalid generated TypeScript', async (t) => {
  const f = fixture(t, { config: "imports: 'import {'," });
  const before = readFileSync(f.worldPath);
  const result = await run(f, ['--write']);
  assertError(result, 'biome refused the generated baseline');
  assert.match(result.stderr, /parse/);
  assert.deepEqual(readFileSync(f.worldPath), before);
});

test('string rendering preserves control characters, Unicode and lone surrogates', async (t) => {
  const value =
    'quotes \'" backslash \\ control \0\b\f\n\r\t\v Unicode \u2028\u2029 \u{1f680} lone \ud800 \udfff';
  const f = fixture(t, { value: JSON.stringify(value) });
  const written = await run(f, ['--write']);
  assert.equal(written.status, 0, written.stderr);
  assert.equal(written.stderr, '');
  const checked = await run(f);
  assert.equal(checked.status, 0, checked.stderr);
  assert.equal(checked.stdout, matchOutput(f));
  assert.equal(checked.stderr, '');
});

test('accepts a null-prototype derived container with an own undefined key', async (t) => {
  const f = fixture(t, {
    value: 'undefined',
    world:
      'export function run() { return Object.assign(Object.create(null), { value: undefined }); }',
  });
  const result = await run(f);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, matchOutput(f));
  assert.equal(result.stderr, '');
});
