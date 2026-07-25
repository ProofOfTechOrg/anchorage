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

  const client = readFileSync(
    join(temporary, 'package', 'dist', 'signals', 'client.js'),
    'utf8',
  );
  if (
    /^import\s/m.test(client) ||
    /node:|agent-runner|do-runner/.test(client)
  ) {
    throw new Error(
      'packed signals/client pulled a runtime or Node-only import',
    );
  }
  console.log('packed browser signals/client import passed');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
