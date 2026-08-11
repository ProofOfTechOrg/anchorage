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
  const extracted = join(temporary, 'extracted');
  const consumer = join(temporary, 'consumer');
  mkdirSync(packed);
  mkdirSync(extracted);
  mkdirSync(consumer);

  run('pnpm', ['run', 'build']);
  run('pnpm', ['pack', '--pack-destination', packed]);
  const archives = readdirSync(packed).filter((name) => name.endsWith('.tgz'));
  assert.equal(archives.length, 1);
  const archive = join(packed, archives[0]);
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
  const manifest = JSON.parse(
    readFileSync(join(packageDirectory, 'package.json'), 'utf8'),
  );
  // Compared against the SOURCE manifest, not a copy of its value: this script
  // is a CI-only step, so a hardcoded range silently goes stale the moment the
  // peer floor moves and only fails after the change is pushed.
  const sourceManifest = JSON.parse(
    readFileSync(join(packageRoot, 'package.json'), 'utf8'),
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
  symlinkSync(
    join(packageRoot, 'node_modules', 'jose'),
    join(packageModules, 'jose'),
    'dir',
  );

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
import * as flowsafe from '@proofoftech/flowsafe';
import * as approvals from '@proofoftech/flowsafe/approval-api';
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
  console.log('packed agent-host import passed');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
