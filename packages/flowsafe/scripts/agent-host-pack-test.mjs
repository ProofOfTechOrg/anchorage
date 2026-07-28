import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
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
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = resolve(packageRoot, '..', '..');
const temporary = mkdtempSync(join(tmpdir(), 'flowsafe-agent-host-'));

function run(command, args, cwd = packageRoot) {
  execFileSync(command, args, {
    cwd,
    encoding: 'utf8',
    stdio: 'pipe',
  });
}

try {
  const packed = join(temporary, 'packed');
  const extracted = join(temporary, 'extracted');
  const consumer = join(temporary, 'consumer');
  mkdirSync(packed);
  mkdirSync(extracted);
  mkdirSync(consumer);

  run('pnpm', ['run', 'build']);
  run('pnpm', ['pack', '--pack-destination', packed]);
  const archives = readdirSync(packed).filter((name) => name.endsWith('.tgz'));
  assert.equal(archives.length, 1);
  run('tar', ['-xzf', join(packed, archives[0]), '-C', extracted]);

  const packageDirectory = join(extracted, 'package');
  const manifest = JSON.parse(
    readFileSync(join(packageDirectory, 'package.json'), 'utf8'),
  );
  assert.equal(manifest.exports['./agent-host'], './dist/agent-host/index.js');
  assert.equal(
    manifest.peerDependencies['@proofoftech/breakwater'],
    '>=0.6.0 <1.0.0',
  );
  readFileSync(join(packageDirectory, 'dist/agent-host/index.js'), 'utf8');
  readFileSync(join(packageDirectory, 'dist/agent-host/index.d.ts'), 'utf8');

  const consumerModules = join(consumer, 'node_modules');
  mkdirSync(join(consumerModules, '@proofoftech'), { recursive: true });
  symlinkSync(
    packageDirectory,
    join(consumerModules, '@proofoftech', 'flowsafe'),
    'dir',
  );

  const packageModules = join(packageDirectory, 'node_modules');
  mkdirSync(join(packageModules, '@proofoftech'), { recursive: true });
  mkdirSync(join(packageModules, '@mastra'), { recursive: true });
  symlinkSync(
    join(repositoryRoot, 'packages', 'breakwater'),
    join(packageModules, '@proofoftech', 'breakwater'),
    'dir',
  );
  for (const dependency of ['core', 'cloudflare-d1']) {
    symlinkSync(
      join(packageRoot, 'node_modules', '@mastra', dependency),
      join(packageModules, '@mastra', dependency),
      'dir',
    );
  }

  writeFileSync(
    join(consumer, 'consumer.ts'),
    `import {
  createAgentCatalog,
  createAgentRouter,
  createAgentThreadTopology,
  createThreadAgentHost,
  createAgentApprovalResumer,
  type AgentMeta,
  type AgentRunEnvelope,
} from '@proofoftech/flowsafe/agent-host';

const meta: AgentMeta = {
  id: 'writer',
  title: 'Writer',
  description: 'Writes an approved record',
};
const envelope = null as AgentRunEnvelope | null;
void createAgentCatalog([meta]);
void createAgentRouter;
void createAgentThreadTopology;
void createThreadAgentHost;
void createAgentApprovalResumer;
void envelope;
`,
  );
  writeFileSync(
    join(consumer, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        noEmit: true,
        skipLibCheck: true,
      },
      files: ['consumer.ts'],
    }),
  );
  run(
    join(packageRoot, 'node_modules', '.bin', 'tsc'),
    ['-p', 'tsconfig.json'],
    consumer,
  );
  writeFileSync(
    join(consumer, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import * as host from '@proofoftech/flowsafe/agent-host';
assert.equal(typeof host.createAgentCatalog, 'function');
assert.equal(typeof host.createAgentRouter, 'function');
assert.equal(typeof host.createAgentThreadTopology, 'function');
assert.equal(typeof host.createThreadAgentHost, 'function');
assert.equal(typeof host.createAgentApprovalResumer, 'function');
`,
  );
  run(process.execPath, ['runtime.mjs'], consumer);
  console.log('packed agent-host import passed');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
