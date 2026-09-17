import { spawnSync } from 'node:child_process';
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertAttwEsmPackage } from './attw-pack-check.mjs';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const temporary = mkdtempSync(join(tmpdir(), 'flowsafe-signals-client-'));

function run(command, args, cwd = root) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: 'utf8',
    stdio: 'pipe',
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} failed\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout;
}

try {
  run('pnpm', ['pack', '--pack-destination', temporary]);
  const packedName = readdirSync(temporary).find((name) =>
    name.endsWith('.tgz'),
  );
  if (!packedName) {
    throw new Error('pnpm pack returned no archive');
  }
  const archive = join(temporary, packedName);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'publint',
    'run',
    archive,
    '--strict',
  ]);
  assertAttwEsmPackage(archive, root);
  run('tar', ['-xf', archive], temporary);

  const consumer = join(temporary, 'consumer');
  const scope = join(consumer, 'node_modules', '@proofoftech');
  mkdirSync(scope, { recursive: true });
  symlinkSync(join(temporary, 'package'), join(scope, 'flowsafe'), 'dir');
  writeFileSync(
    join(consumer, 'index.ts'),
    "import { SignalClient } from '@proofoftech/flowsafe/signals/client';\nnew SignalClient();\n",
  );
  writeFileSync(
    join(consumer, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        module: 'ESNext',
        moduleResolution: 'Bundler',
        target: 'ES2023',
        lib: ['ES2023', 'DOM'],
        noEmit: true,
        strict: true,
      },
      files: ['index.ts'],
    }),
  );
  run(join(root, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.json'], consumer);

  const packedSignalsFile = (fileName) =>
    readFileSync(
      join(temporary, 'package', 'dist', 'signals', fileName),
      'utf8',
    );
  // Each pattern is one line form a module names another in: a `from` clause
  // on an import or a re-export, a side-effect import, a dynamic import, a
  // `require`, a module augmentation, a triple-slash reference. A form missing
  // from this list is an edge the two checks below cannot see.
  const edgePatterns = [
    /\bfrom\s*['"][^'"]+['"]\s*;?\s*$/,
    /^\s*import\s*['"][^'"]+['"]\s*;?\s*$/,
    /\bimport\(\s*['"][^'"]+['"]\s*\)/,
    /\brequire\(\s*['"][^'"]+['"]\s*\)/,
    /^\s*declare\s+module\s+['"][^'"]+['"]/,
    /^\s*\/\/\/\s*<reference\s+(?:path|types|lib)\s*=/,
  ];
  const namesAnotherModule = (source) =>
    source
      .split('\n')
      .some((line) => edgePatterns.some((pattern) => pattern.test(line)));

  const client = packedSignalsFile('client.js');
  if (
    namesAnotherModule(client) ||
    /node:|agent-runner|do-runner/.test(client)
  ) {
    throw new Error(
      'packed signals/client pulled a runtime or Node-only import',
    );
  }
  // The declaration axis of the same entry. A browser consumer resolves this
  // `.d.ts` and whatever it names, so a type reaching in from the runner or the
  // host kit would hand that graph to a consumer who installed the browser
  // entry alone.
  if (namesAnotherModule(packedSignalsFile('client.d.ts'))) {
    throw new Error(
      'packed signals/client declaration named a module outside the browser entry',
    );
  }
  console.log('packed browser signals/client import passed');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
