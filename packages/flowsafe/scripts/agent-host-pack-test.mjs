import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
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
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertAttwEsmPackage } from './attw-pack-check.mjs';

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
  const breakwaterPacked = join(temporary, 'breakwater-packed');
  const extracted = join(temporary, 'extracted');
  const consumer = join(temporary, 'consumer');
  mkdirSync(packed);
  mkdirSync(breakwaterPacked);
  mkdirSync(extracted);
  mkdirSync(consumer);

  const staleBuildArtifact = join(
    packageRoot,
    'dist',
    'stale-package-probe.js',
  );
  mkdirSync(dirname(staleBuildArtifact), { recursive: true });
  writeFileSync(staleBuildArtifact, 'throw new Error("stale build output");\n');
  run('pnpm', ['run', 'build']);
  run('pnpm', ['pack', '--pack-destination', packed]);
  run(
    'pnpm',
    [
      '--filter',
      '@proofoftech/breakwater',
      'pack',
      '--pack-destination',
      breakwaterPacked,
    ],
    repositoryRoot,
  );
  const archives = readdirSync(packed).filter((name) => name.endsWith('.tgz'));
  assert.equal(archives.length, 1);
  const archive = join(packed, archives[0]);
  const breakwaterArchives = readdirSync(breakwaterPacked).filter((name) =>
    name.endsWith('.tgz'),
  );
  assert.equal(breakwaterArchives.length, 1);
  const breakwaterArchive = join(breakwaterPacked, breakwaterArchives[0]);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'publint',
    'run',
    archive,
    '--strict',
  ]);
  assertAttwEsmPackage(archive, packageRoot);
  run('tar', ['-xzf', archive, '-C', extracted]);

  const packageDirectory = join(extracted, 'package');
  assert.equal(
    existsSync(join(packageDirectory, 'dist', 'stale-package-probe.js')),
    false,
  );
  const manifest = JSON.parse(
    readFileSync(join(packageDirectory, 'package.json'), 'utf8'),
  );
  // Compared against the SOURCE manifest, not a copy of its value: this script
  // is a CI-only step, so a hardcoded range silently goes stale the moment the
  // peer floor moves and only fails after the change is pushed. The regex
  // beside each equality is not redundant with it: equality catches a pack-time
  // REWRITE of the value, while the regex enforces the exact-pin POLICY, which
  // two equal-but-both-wrong values would satisfy.
  const sourceManifest = JSON.parse(
    readFileSync(join(packageRoot, 'package.json'), 'utf8'),
  );
  const corePeer = sourceManifest.peerDependencies['@mastra/core'];
  const d1Pin = sourceManifest.dependencies['@mastra/cloudflare-d1'];
  assert.equal(manifest.dependencies['@mastra/cloudflare-d1'], d1Pin);
  assert.match(
    manifest.dependencies['@mastra/cloudflare-d1'],
    /^\d+\.\d+\.\d+$/,
    'the packed @mastra/cloudflare-d1 dep must stay an exact version',
  );
  assert.equal(manifest.peerDependencies['@mastra/core'], corePeer);
  assert.match(
    manifest.peerDependencies['@mastra/core'],
    /^\d+\.\d+\.\d+$/,
    'the packed @mastra/core peer must stay an exact version',
  );
  assert.equal(manifest.dependencies.jose, sourceManifest.dependencies.jose);
  assert.equal(
    manifest.peerDependencies['@proofoftech/breakwater'],
    sourceManifest.peerDependencies['@proofoftech/breakwater'],
  );
  assert.match(
    manifest.peerDependencies['@proofoftech/breakwater'],
    /^>=\d+\.\d+\.\d+ <1\.0\.0$/,
    'the packed peer range must stay a bounded 0.x floor',
  );
  const rootManifest = JSON.parse(
    readFileSync(join(repositoryRoot, 'package.json'), 'utf8'),
  );
  writeFileSync(
    join(consumer, 'package.json'),
    `${JSON.stringify(
      {
        name: 'flowsafe-agent-host-clean-consumer',
        private: true,
        type: 'module',
        packageManager: rootManifest.packageManager,
        engines: rootManifest.engines,
        dependencies: {
          '@mastra/core': corePeer,
          '@proofoftech/breakwater': `file:${breakwaterArchive}`,
          '@proofoftech/flowsafe': `file:${archive}`,
        },
        devDependencies: {
          typescript: '5.9.3',
          wrangler: rootManifest.devDependencies.wrangler,
        },
      },
      null,
      2,
    )}\n`,
  );
  writeFileSync(
    join(consumer, 'pnpm-workspace.yaml'),
    'packages:\n  - "."\nminimumReleaseAge: 10080\n',
  );
  writeFileSync(
    join(consumer, '.npmrc'),
    'ignore-scripts=true\nengine-strict=true\nauto-install-peers=false\n',
  );
  run('pnpm', ['install', '--ignore-scripts'], consumer);

  const installedFlowsafeRoot = join(
    consumer,
    'node_modules',
    '@proofoftech',
    'flowsafe',
  );
  const requireFromFlowsafe = createRequire(
    join(realpathSync(installedFlowsafeRoot), 'package.json'),
  );
  const installedAdapterManifest = JSON.parse(
    readFileSync(
      requireFromFlowsafe.resolve('@mastra/cloudflare-d1/package.json'),
      'utf8',
    ),
  );
  const installedCoreManifest = JSON.parse(
    readFileSync(
      join(consumer, 'node_modules', '@mastra', 'core', 'package.json'),
      'utf8',
    ),
  );
  assert.equal(installedAdapterManifest.version, d1Pin);
  assert.equal(installedCoreManifest.version, corePeer);

  writeFileSync(
    join(consumer, 'consumer.ts'),
    `import {
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  connectorGrantsForLeg,
  type ConnectorApprovalGrant,
} from '@proofoftech/flowsafe';
import type {
  ApprovalGrantScope,
  ConnectorApprovalSuspension,
} from '@proofoftech/flowsafe/approval-api';
import {
  sweepExpiredRunDeadlines,
  type DurableObjectRunLifecycleHooks,
  type RunTerminalErrorEnvelope,
} from '@proofoftech/flowsafe/do-runner';
import {
  createFlowsafeRunnerLifecycle,
  createRunRouter,
} from '@proofoftech/flowsafe/host-kit';
import {
  createAgentCatalog,
  createAgentRouter,
  createAgentThreadTopology,
  createThreadAgentHost,
  createAgentApprovalResumer,
  isPermissionIdentifier,
  type AgentAutomationRule,
  type AgentMeta,
  type AgentRunEnvelope,
  type AutomatedEntryAuthorizer,
  type AutomatedEntryRequest,
  type AutomationCheck,
  type Permission,
  type PrincipalPermissionResolution,
  type PrincipalPermissionResolver,
} from '@proofoftech/flowsafe/agent-host';

const automation: AgentAutomationRule = {
  kind: 'system',
  entryPaths: ['schedule.fire'],
};
const permission: Permission = 'reports.write';
const meta: AgentMeta = {
  id: 'writer',
  title: 'Writer',
  description: 'Writes an approved record',
  allowedAutomation: [automation],
  requiredPermissions: [permission],
};
const automationCheck: AutomationCheck = () => true;
const authorizeAutomatedEntry: AutomatedEntryAuthorizer = (
  request: AutomatedEntryRequest,
) => request.agentId === meta.id;
const permissionResolution: PrincipalPermissionResolution = {
  permissions: [permission],
  policyVersion: 'permissions-v1',
};
const resolvePrincipalPermissions: PrincipalPermissionResolver = () =>
  permissionResolution;
const envelope = null as AgentRunEnvelope | null;
const scope: ApprovalGrantScope = 'tool-call';
const suspension: ConnectorApprovalSuspension = {
  stepPath: ['publish'],
  suspendedAt: 1,
  resumeCount: 1,
};
const grant: ConnectorApprovalGrant = {
  scope,
  connectorId: 'publisher',
  workflowId: 'launch',
  runId: 'run-1',
  isolationScope: 'acme',
  suspension,
  toolCallId: 'call-1',
};
const terminalError: RunTerminalErrorEnvelope = {
  code: 'CANCELLED',
  message: 'run was cancelled',
};
const lifecycleHooks = null as DurableObjectRunLifecycleHooks | null;
void BREAKWATER_CONNECTOR_EXECUTION_KEY;
void BREAKWATER_CONNECTOR_GRANTS_KEY;
void connectorGrantsForLeg;
void createAgentCatalog([meta]);
void createAgentRouter;
void createAgentThreadTopology;
void createThreadAgentHost;
void createAgentApprovalResumer;
void isPermissionIdentifier;
void automationCheck;
void authorizeAutomatedEntry;
void resolvePrincipalPermissions;
void envelope;
void grant;
void terminalError;
void lifecycleHooks;
void sweepExpiredRunDeadlines;
void createFlowsafeRunnerLifecycle;
void createRunRouter;
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
  run('pnpm', ['exec', 'tsc', '-p', 'tsconfig.json'], consumer);
  writeFileSync(
    join(consumer, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import * as host from '@proofoftech/flowsafe/agent-host';
import * as flowsafe from '@proofoftech/flowsafe';
import * as approvals from '@proofoftech/flowsafe/approval-api';
import {
  createD1Storage,
  sweepExpiredRunDeadlines,
} from '@proofoftech/flowsafe/do-runner';
import {
  createFlowsafeRunnerLifecycle,
  createRunRouter,
} from '@proofoftech/flowsafe/host-kit';
assert.equal(typeof createD1Storage, 'function');
assert.equal(typeof sweepExpiredRunDeadlines, 'function');
assert.equal(typeof createFlowsafeRunnerLifecycle, 'function');
assert.equal(typeof createRunRouter, 'function');
assert.equal(typeof host.createAgentCatalog, 'function');
assert.equal(typeof host.createAgentRouter, 'function');
assert.equal(typeof host.createAgentThreadTopology, 'function');
assert.equal(typeof host.createThreadAgentHost, 'function');
assert.equal(typeof host.createAgentApprovalResumer, 'function');
assert.equal(typeof host.isPermissionIdentifier, 'function');
assert.equal(host.isPermissionIdentifier('reports.read'), true);
assert.equal(host.isPermissionIdentifier('Reports.read'), false);
assert.equal(
  flowsafe.BREAKWATER_CONNECTOR_GRANTS_KEY,
  'breakwater.connectorGrants',
);
assert.equal(
  flowsafe.BREAKWATER_CONNECTOR_EXECUTION_KEY,
  'breakwater.connectorExecution',
);
assert.equal(
  flowsafe.BREAKWATER_PRINCIPAL_PERMISSIONS_KEY,
  'breakwater.principalPermissions',
);
assert.equal(typeof flowsafe.connectorGrantsForLeg, 'function');
assert.equal(
  approvals.BREAKWATER_CONNECTOR_GRANTS_KEY,
  flowsafe.BREAKWATER_CONNECTOR_GRANTS_KEY,
);
assert.equal(
  approvals.BREAKWATER_PRINCIPAL_PERMISSIONS_KEY,
  flowsafe.BREAKWATER_PRINCIPAL_PERMISSIONS_KEY,
);
`,
  );
  run(process.execPath, ['runtime.mjs'], consumer);
  writeFileSync(
    join(consumer, 'worker.mjs'),
    `import { createD1Storage } from '@proofoftech/flowsafe/do-runner';

export default {
  fetch() {
    return new Response(typeof createD1Storage === 'function' ? 'ok' : 'unavailable');
  },
};
`,
  );
  writeFileSync(
    join(consumer, 'wrangler.jsonc'),
    JSON.stringify({
      name: 'flowsafe-packed-consumer',
      main: './worker.mjs',
      compatibility_date: '2026-08-12',
      compatibility_flags: ['nodejs_compat'],
    }),
  );
  run(
    'pnpm',
    [
      'exec',
      'wrangler',
      'deploy',
      '--dry-run',
      '--config',
      'wrangler.jsonc',
      '--outdir',
      'bundle',
    ],
    consumer,
  );
  console.log(
    `packed agent-host clean core-${corePeer} import and bundle passed`,
  );
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
