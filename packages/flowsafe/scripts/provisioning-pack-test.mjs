// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { assertAttwEsmPackage } from './attw-pack-check.mjs';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const temporary = mkdtempSync(join(tmpdir(), 'flowsafe-provisioning-'));

function run(command, args, cwd = root, env = process.env) {
  const result = spawnSync(command, args, {
    cwd,
    env,
    encoding: 'utf8',
    stdio: 'pipe',
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(' ')} failed\n${result.stdout}\n${result.stderr}`,
      { cause: result.error },
    );
  }
  return result.stdout;
}

function invokeProvision(cwd, args, env = {}) {
  return spawnSync('pnpm', ['exec', 'flowsafe-provision', '--', ...args], {
    cwd,
    env: { ...process.env, ...env },
    encoding: 'utf8',
    stdio: 'pipe',
  });
}

const wranglerShim = `#!/usr/bin/env node
import { appendFileSync, existsSync, readFileSync, writeFileSync } from 'node:fs';

const args = process.argv.slice(2);
const commandAt = args.indexOf('--command');
const sql = commandAt === -1 ? undefined : args[commandAt + 1];
appendFileSync(
  process.env.FAKE_WRANGLER_LOG,
  JSON.stringify({ args, cwd: process.cwd(), entrypoint: import.meta.url }) + '\\n',
);
if (args.includes('--preview') && !args.includes('--remote')) {
  process.stderr.write('--preview requires --remote\\n');
  process.exit(4);
}
if (process.env.FAKE_WRANGLER_FAILURE === '1') {
  process.stdout.write('wrangler stdout diagnostic\\n');
  process.stderr.write('wrangler stderr diagnostic\\n');
  process.exit(7);
}
if (typeof sql !== 'string') {
  process.stderr.write('missing --command SQL\\n');
  process.exit(2);
}
const statePath = process.env.FAKE_WRANGLER_STATE;
const state = existsSync(statePath)
  ? JSON.parse(readFileSync(statePath, 'utf8'))
  : { created: false, tag: undefined, fence: false, fenceState: undefined };
const FENCE = 'flowsafe_execution_fence';
const schema = \`CREATE TABLE flowsafe_deployment (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tenant_tag TEXT NOT NULL,
  provisioned_at TEXT NOT NULL
)\`;
let results;
// Fence statements are matched by their TARGET table and BEFORE the generic
// arms: the ownership insert names the fence table inside its exclusion list,
// so a substring test would route it to the wrong branch.
if (sql.startsWith('SELECT name, sql')) {
  results = [
    ...(state.created ? [{ name: 'flowsafe_deployment', sql: schema }] : []),
    ...(state.fence ? [{ name: FENCE, sql: 'CREATE' }] : []),
  ];
} else if (sql.startsWith('CREATE TABLE IF NOT EXISTS ' + FENCE)) {
  state.fence = true;
  results = [];
} else if (sql.startsWith('INSERT OR IGNORE INTO ' + FENCE)) {
  if (!state.fence) {
    process.stderr.write('fence row seeded before its table\\n');
    process.exit(5);
  }
  state.fenceState =
    state.fenceState ?? sql.match(/'deployment', '([^']+)'/)?.[1];
  results = [];
} else if (sql.startsWith('CREATE TABLE')) {
  state.created = true;
  results = [];
} else if (sql.startsWith('SELECT sql')) {
  results = state.created ? [{ sql: schema }] : [];
} else if (sql.startsWith('PRAGMA')) {
  results = [
    { name: 'id', type: 'INTEGER', notnull: 0, pk: 1 },
    { name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 },
    { name: 'provisioned_at', type: 'TEXT', notnull: 1, pk: 0 },
  ];
} else if (sql.startsWith('SELECT id')) {
  results = state.tag ? [{ id: 1, tenant_tag: state.tag }] : [];
} else if (sql.startsWith('INSERT OR IGNORE')) {
  state.tag = sql.match(/SELECT 1, '([^']+)'/)?.[1];
  results = [];
} else {
  process.stderr.write(\`unexpected SQL: \${sql}\\n\`);
  process.exit(3);
}
writeFileSync(statePath, JSON.stringify(state));
process.stdout.write(JSON.stringify([{ results }]));
`;

try {
  run('pnpm', ['pack', '--pack-destination', temporary]);
  const packedName = readdirSync(temporary).find((name) =>
    name.endsWith('.tgz'),
  );
  if (!packedName) throw new Error('pnpm pack returned no archive');
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
  const consumerRoot = join(temporary, 'consumer');
  const wranglerRoot = join(consumerRoot, 'fake-wrangler');
  const cloudflareD1Root = join(consumerRoot, 'fake-cloudflare-d1');
  mkdirSync(join(wranglerRoot, 'bin'), { recursive: true });
  mkdirSync(cloudflareD1Root, { recursive: true });
  writeFileSync(
    join(wranglerRoot, 'package.json'),
    JSON.stringify({
      name: 'wrangler',
      version: '4.118.0',
      type: 'module',
      bin: { wrangler: './bin/wrangler.mjs' },
    }),
  );
  writeFileSync(join(wranglerRoot, 'bin', 'wrangler.mjs'), wranglerShim);
  writeFileSync(
    join(cloudflareD1Root, 'package.json'),
    JSON.stringify({
      name: '@mastra/cloudflare-d1',
      version: '1.1.1',
      type: 'module',
    }),
  );
  writeFileSync(
    join(consumerRoot, 'postinstall.mjs'),
    `import { writeFileSync } from 'node:fs';
writeFileSync(new URL('./install-script-ran', import.meta.url), 'unexpected');
`,
  );
  writeFileSync(
    join(consumerRoot, 'package.json'),
    JSON.stringify({
      name: 'flowsafe-provision-consumer',
      private: true,
      packageManager: 'pnpm@10.34.4',
      engines: { node: '>=22.22.0', pnpm: '>=10.16.0' },
      scripts: { postinstall: 'node postinstall.mjs' },
      dependencies: {
        '@mastra/core': `link:${realpathSync(
          join(root, 'node_modules', '@mastra', 'core'),
        )}`,
        '@proofoftech/flowsafe': `file:../${packedName}`,
        wrangler: 'file:./fake-wrangler',
      },
      pnpm: {
        overrides: {
          '@mastra/cloudflare-d1': 'file:./fake-cloudflare-d1',
          jose: `file:${realpathSync(join(root, 'node_modules', 'jose'))}`,
        },
      },
    }),
  );
  writeFileSync(
    join(consumerRoot, 'pnpm-workspace.yaml'),
    'packages: []\nminimumReleaseAge: 10080\n',
  );
  writeFileSync(
    join(consumerRoot, '.npmrc'),
    'ignore-scripts=true\nengine-strict=true\nauto-install-peers=false\n',
  );
  const isolatedStore = join(temporary, 'pnpm-store');
  run(
    'pnpm',
    [
      'install',
      '--lockfile-only',
      '--offline',
      '--ignore-scripts',
      '--store-dir',
      isolatedStore,
    ],
    consumerRoot,
  );
  run(
    'pnpm',
    [
      'install',
      '--offline',
      '--frozen-lockfile',
      '--ignore-scripts',
      '--store-dir',
      isolatedStore,
    ],
    consumerRoot,
  );
  if (existsSync(join(consumerRoot, 'install-script-ran'))) {
    throw new Error('packed consumer install executed a lifecycle script');
  }

  writeFileSync(
    join(consumerRoot, 'protocol-runtime.mjs'),
    `import assert from 'node:assert/strict';
import {
  DEPLOYMENT_IDENTITY_HEADER,
  EXECUTION_FENCE_DDL,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
  INITIAL_EXECUTION_FENCE_STATES,
  deploymentIdentityHeaders,
} from '@proofoftech/flowsafe/deployment-identity-protocol';

const secret = 'x'.repeat(32);
assert.equal(typeof EXECUTION_FENCE_DDL, 'string');
assert.equal(EXECUTION_FENCE_ROW_ID, 'deployment');
assert.deepEqual(EXECUTION_FENCE_STATES, [
  'open',
  'draining',
  'migration-locked',
  'proof-only',
]);
assert.equal(EXECUTION_FENCE_TABLE, 'flowsafe_execution_fence');
assert.deepEqual(INITIAL_EXECUTION_FENCE_STATES, [
  'open',
  'migration-locked',
]);
assert.deepEqual(
  deploymentIdentityHeaders(secret, {
    'content-type': 'application/json',
    'X-Flowsafe-Deployment-Identity': 'forged',
  }),
  {
    'content-type': 'application/json',
    [DEPLOYMENT_IDENTITY_HEADER]: secret,
  },
);
`,
  );
  writeFileSync(
    join(consumerRoot, 'protocol-consumer.ts'),
    `import {
  DEPLOYMENT_IDENTITY_HEADER,
  deploymentIdentityHeaders,
  type InitialExecutionFenceState,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import {
  DEPLOYMENT_IDENTITY_HEADER as LEGACY_DEPLOYMENT_IDENTITY_HEADER,
  deploymentIdentityHeaders as legacyDeploymentIdentityHeaders,
} from '@proofoftech/flowsafe/do-runner';

const secret = 'x'.repeat(32);
const headers: Record<string, string> = deploymentIdentityHeaders(secret);
const legacyHeaders: Record<string, string> =
  legacyDeploymentIdentityHeaders(secret);
const header: typeof DEPLOYMENT_IDENTITY_HEADER =
  LEGACY_DEPLOYMENT_IDENTITY_HEADER;
const initialFenceState: InitialExecutionFenceState = 'open';
void headers;
void legacyHeaders;
void header;
void initialFenceState;
`,
  );
  writeFileSync(
    join(consumerRoot, 'tsconfig.json'),
    JSON.stringify({
      compilerOptions: {
        target: 'ES2022',
        module: 'NodeNext',
        moduleResolution: 'NodeNext',
        strict: true,
        noEmit: true,
        skipLibCheck: true,
        types: [],
      },
      include: ['protocol-consumer.ts'],
    }),
  );
  run(process.execPath, ['protocol-runtime.mjs'], consumerRoot);
  run(
    join(root, 'node_modules', '.bin', 'tsc'),
    ['-p', 'tsconfig.json'],
    consumerRoot,
  );

  const packageRoot = join(
    consumerRoot,
    'node_modules',
    '@proofoftech',
    'flowsafe',
  );

  const manifest = JSON.parse(
    readFileSync(join(packageRoot, 'package.json'), 'utf8'),
  );
  if (
    manifest.peerDependencies?.wrangler !== undefined ||
    manifest.peerDependenciesMeta?.wrangler !== undefined
  ) {
    throw new Error('packed flowsafe package leaks Wrangler as a peer');
  }
  const relativeBin = manifest.bin?.['flowsafe-provision'];
  if (relativeBin !== './scripts/seed-deployment-identity.mjs') {
    throw new Error('packed package does not expose flowsafe-provision');
  }
  const protocolImport = manifest.imports?.['#deployment-identity-protocol'];
  if (
    protocolImport?.default !== './deployment-identity-protocol.mjs' ||
    protocolImport?.types !== './deployment-identity-protocol.d.mts'
  ) {
    throw new Error(
      'packed package does not map the deployment identity protocol and its declaration',
    );
  }
  const protocolSource = readFileSync(
    join(packageRoot, 'deployment-identity-protocol.mjs'),
    'utf8',
  );
  const protocolDeclaration = readFileSync(
    join(packageRoot, 'deployment-identity-protocol.d.mts'),
    'utf8',
  );
  if (
    !protocolSource.includes('provisionDeploymentIdentityProtocol') ||
    !protocolSource.includes('deploymentIdentityHeaders') ||
    !protocolDeclaration.includes('DeploymentIdentityProtocolExecutor') ||
    !protocolDeclaration.includes('deploymentIdentityHeaders')
  ) {
    throw new Error(
      'packed package is missing the deployment identity protocol implementation or declaration',
    );
  }
  // The fence schema ships in the SAME root module the runtime store imports
  // it from; if it were missing here, the packed store would fail to resolve
  // its own DDL rather than silently create a different table.
  if (
    !protocolSource.includes('EXECUTION_FENCE_DDL') ||
    !protocolSource.includes('assertInitialExecutionFenceState') ||
    !protocolDeclaration.includes('InitialExecutionFenceState') ||
    !protocolDeclaration.includes('EXECUTION_FENCE_DDL')
  ) {
    throw new Error(
      'packed deployment identity protocol is missing the execution fence seeding contract',
    );
  }
  const runtimeIdentity = await import(
    pathToFileURL(
      join(packageRoot, 'dist', 'do-runner', 'deployment-identity.js'),
    ).href
  );
  if (runtimeIdentity.DEPLOYMENT_TAG_PATTERN.source !== '^[a-z0-9]{3,32}$') {
    throw new Error(
      'packed runtime could not resolve the shared deployment identity protocol',
    );
  }
  const protocolIdentity = await import(
    pathToFileURL(join(packageRoot, 'dist', 'deployment-identity-protocol.js'))
      .href
  );
  const deploymentSecret = 'x'.repeat(32);
  const identityHeaders = protocolIdentity.deploymentIdentityHeaders(
    deploymentSecret,
    {
      'content-type': 'application/json',
      [protocolIdentity.DEPLOYMENT_IDENTITY_HEADER]: 'forged',
    },
  );
  if (
    identityHeaders['content-type'] !== 'application/json' ||
    identityHeaders[protocolIdentity.DEPLOYMENT_IDENTITY_HEADER] !==
      deploymentSecret
  ) {
    throw new Error(
      'packed deployment identity protocol did not stamp the internal credential',
    );
  }

  const entrypoint = join(packageRoot, relativeBin);
  const source = readFileSync(entrypoint, 'utf8');
  if (!source.startsWith('#!/usr/bin/env node\n')) {
    throw new Error('packed provisioning entrypoint is not executable by Node');
  }

  const installedBin = join(
    consumerRoot,
    'node_modules',
    '.bin',
    'flowsafe-provision',
  );
  if (!existsSync(installedBin)) {
    throw new Error('pnpm did not install the flowsafe-provision executable');
  }
  const invalid = invokeProvision(consumerRoot, ['--unknown']);
  if (
    invalid.status !== 1 ||
    !invalid.stderr.includes('Usage: flowsafe-provision')
  ) {
    throw new Error(
      `packed provisioning CLI did not execute its argument guard (status=${invalid.status}, signal=${invalid.signal}, error=${invalid.error?.message ?? 'none'})\n${invalid.stdout}\n${invalid.stderr}`,
    );
  }

  rmSync(join(packageRoot, 'dist'), { recursive: true, force: true });

  const installedWranglerRoot = realpathSync(
    join(consumerRoot, 'node_modules', 'wrangler'),
  );
  const installedWranglerUrl = `${pathToFileURL(installedWranglerRoot).href}/`;
  const wranglerManifestPath = join(installedWranglerRoot, 'package.json');

  const statePath = join(consumerRoot, 'wrangler-state.json');
  const logPath = join(consumerRoot, 'wrangler-invocations.ndjson');
  writeFileSync(logPath, '');
  // The fence state has NO default, so an otherwise-complete invocation that
  // omits it must fail rather than quietly provision an executing deployment.
  const withoutFenceState = invokeProvision(consumerRoot, [
    '--database',
    'consumer-db',
    '--tag',
    'acme',
    '--local',
  ]);
  if (
    withoutFenceState.status !== 1 ||
    !withoutFenceState.stderr.includes('--initial-fence-state')
  ) {
    throw new Error(
      `packed provisioning CLI accepted a missing --initial-fence-state (status=${withoutFenceState.status})\n${withoutFenceState.stdout}\n${withoutFenceState.stderr}`,
    );
  }
  const badFenceState = invokeProvision(consumerRoot, [
    '--database',
    'consumer-db',
    '--tag',
    'acme',
    '--initial-fence-state',
    'draining',
    '--local',
  ]);
  if (
    badFenceState.status !== 1 ||
    !badFenceState.stderr.includes('must be one of open, migration-locked')
  ) {
    throw new Error(
      `packed provisioning CLI accepted a non-birth fence state (status=${badFenceState.status})\n${badFenceState.stdout}\n${badFenceState.stderr}`,
    );
  }

  const validArgs = [
    '--database',
    'consumer-db',
    '--tag',
    'acme',
    '--initial-fence-state',
    'migration-locked',
    '--local',
    '--config',
    'wrangler.jsonc',
    '--persist-to',
    '.wrangler/state',
  ];
  const valid = invokeProvision(consumerRoot, validArgs, {
    FAKE_WRANGLER_LOG: logPath,
    FAKE_WRANGLER_STATE: statePath,
  });
  if (
    valid.status !== 0 ||
    valid.stdout !==
      "Deployment identity 'acme' verified in consumer-db (local), initial execution fence state 'migration-locked'.\n" ||
    valid.stderr !== ''
  ) {
    throw new Error(
      `packed provisioning CLI did not complete through consumer Wrangler JSON (status=${valid.status}, signal=${valid.signal}, error=${valid.error?.message ?? 'none'})\n${valid.stdout}\n${valid.stderr}`,
      { cause: valid.error },
    );
  }
  const invocations = readFileSync(logPath, 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
  if (invocations.length < 8) {
    throw new Error(
      `expected full provisioning query sequence, got ${invocations.length}`,
    );
  }
  // The SQL follows `--command`; `--config`/`--persist-to` come after it, so
  // the last argument is not the statement.
  const executedSql = invocations.map(
    (invocation) => invocation.args[invocation.args.indexOf('--command') + 1],
  );
  const fenceDdlAt = executedSql.findIndex((sql) =>
    sql.startsWith('CREATE TABLE IF NOT EXISTS flowsafe_execution_fence'),
  );
  const fenceRowAt = executedSql.findIndex((sql) =>
    sql.startsWith('INSERT OR IGNORE INTO flowsafe_execution_fence'),
  );
  const ownershipAt = executedSql.findIndex((sql) =>
    sql.startsWith('INSERT OR IGNORE INTO flowsafe_deployment'),
  );
  if (
    fenceDdlAt === -1 ||
    fenceRowAt !== fenceDdlAt + 1 ||
    ownershipAt === -1 ||
    ownershipAt > fenceDdlAt
  ) {
    throw new Error(
      `packed provisioning CLI did not seed the fence after proving ownership: ${JSON.stringify(executedSql)}`,
    );
  }
  const seededState = JSON.parse(readFileSync(statePath, 'utf8')).fenceState;
  if (seededState !== 'migration-locked') {
    throw new Error(
      `packed provisioning CLI seeded fence state '${seededState}', expected 'migration-locked'`,
    );
  }
  for (const invocation of invocations) {
    const expectedPrefix = [
      'd1',
      'execute',
      'consumer-db',
      '--local',
      '--yes',
      '--json',
      '--command',
    ];
    if (
      invocation.cwd !== consumerRoot ||
      !invocation.entrypoint.startsWith(installedWranglerUrl) ||
      expectedPrefix.some((value, index) => invocation.args[index] !== value) ||
      invocation.args.at(-4) !== '--config' ||
      invocation.args.at(-3) !== 'wrangler.jsonc' ||
      invocation.args.at(-2) !== '--persist-to' ||
      invocation.args.at(-1) !== '.wrangler/state'
    ) {
      throw new Error(
        `packed provisioning CLI passed incorrect consumer Wrangler argv: ${JSON.stringify(invocation)}`,
      );
    }
  }

  writeFileSync(logPath, '');
  const previewArgs = [
    '--database',
    'consumer-db',
    '--tag',
    'acme',
    '--initial-fence-state',
    'open',
    '--preview',
    '--config',
    'wrangler.jsonc',
  ];
  const preview = invokeProvision(consumerRoot, previewArgs, {
    FAKE_WRANGLER_LOG: logPath,
    FAKE_WRANGLER_STATE: statePath,
  });
  if (
    preview.status !== 0 ||
    preview.stdout !==
      "Deployment identity 'acme' verified in consumer-db (preview), initial execution fence state 'open'.\n" ||
    preview.stderr !== ''
  ) {
    throw new Error(
      `packed provisioning CLI preview mode failed (status=${preview.status}, signal=${preview.signal}, error=${preview.error?.message ?? 'none'})\n${preview.stdout}\n${preview.stderr}`,
      { cause: preview.error },
    );
  }
  const previewInvocations = readFileSync(logPath, 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
  if (previewInvocations.length === 0) {
    throw new Error('preview provisioning made no Wrangler calls');
  }
  for (const invocation of previewInvocations) {
    const expectedPrefix = [
      'd1',
      'execute',
      'consumer-db',
      '--remote',
      '--preview',
      '--yes',
      '--json',
      '--command',
    ];
    if (
      invocation.cwd !== consumerRoot ||
      !invocation.entrypoint.startsWith(installedWranglerUrl) ||
      expectedPrefix.some((value, index) => invocation.args[index] !== value) ||
      invocation.args.at(-2) !== '--config' ||
      invocation.args.at(-1) !== 'wrangler.jsonc'
    ) {
      throw new Error(
        `packed provisioning CLI passed incorrect preview Wrangler argv: ${JSON.stringify(invocation)}`,
      );
    }
  }

  writeFileSync(
    wranglerManifestPath,
    JSON.stringify({
      name: 'wrangler',
      version: '5.0.0',
      type: 'module',
      bin: { wrangler: './bin/wrangler.mjs' },
    }),
  );
  const wrongMajor = invokeProvision(consumerRoot, validArgs, {
    FAKE_WRANGLER_LOG: logPath,
    FAKE_WRANGLER_STATE: statePath,
  });
  if (
    wrongMajor.status !== 1 ||
    !wrongMajor.stderr.includes(
      'flowsafe-provision requires Wrangler >=4.118 <5; found 5.0.0',
    )
  ) {
    throw new Error(
      `packed provisioning CLI accepted the wrong Wrangler major (status=${wrongMajor.status})\n${wrongMajor.stdout}\n${wrongMajor.stderr}`,
      { cause: wrongMajor.error },
    );
  }

  writeFileSync(
    wranglerManifestPath,
    JSON.stringify({
      name: 'wrangler',
      version: '4.117.1',
      type: 'module',
      bin: { wrangler: './bin/wrangler.mjs' },
    }),
  );
  const belowMinimum = invokeProvision(consumerRoot, validArgs, {
    FAKE_WRANGLER_LOG: logPath,
    FAKE_WRANGLER_STATE: statePath,
  });
  if (
    belowMinimum.status !== 1 ||
    !belowMinimum.stderr.includes(
      'flowsafe-provision requires Wrangler >=4.118 <5; found 4.117.1',
    )
  ) {
    throw new Error(
      `packed provisioning CLI accepted Wrangler below the supported minimum (status=${belowMinimum.status})\n${belowMinimum.stdout}\n${belowMinimum.stderr}`,
      { cause: belowMinimum.error },
    );
  }

  writeFileSync(
    wranglerManifestPath,
    JSON.stringify({
      name: 'wrangler',
      version: '4.118.0',
      type: 'module',
      bin: { wrangler: './bin/wrangler.mjs' },
    }),
  );
  const failedWrangler = invokeProvision(consumerRoot, validArgs, {
    FAKE_WRANGLER_FAILURE: '1',
    FAKE_WRANGLER_LOG: logPath,
    FAKE_WRANGLER_STATE: statePath,
  });
  if (
    failedWrangler.status !== 1 ||
    !failedWrangler.stdout.includes('wrangler stdout diagnostic') ||
    !failedWrangler.stderr.includes('wrangler stderr diagnostic') ||
    !failedWrangler.stderr.includes(
      'Wrangler failed while provisioning deployment identity',
    ) ||
    !failedWrangler.stderr.includes('Caused by: Wrangler exited with status 7')
  ) {
    throw new Error(
      `packed provisioning CLI lost Wrangler failure diagnostics (status=${failedWrangler.status}, signal=${failedWrangler.signal}, error=${failedWrangler.error?.message ?? 'none'})\n${failedWrangler.stdout}\n${failedWrangler.stderr}`,
      { cause: failedWrangler.error },
    );
  }

  console.log('packed flowsafe-provision CLI passed');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
