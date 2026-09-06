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
import { ModuleKind, ScriptTarget, transpile } from 'typescript';
import { parse as parseYaml } from 'yaml';
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
  for (const leaf of [
    'fenced-workflows-d1',
    'fenced-workflow-capability',
    'run-terminal-state',
    'run-lifecycle',
    'run-provenance',
  ]) {
    const declaration = readFileSync(
      join(packageDirectory, 'dist', 'do-runner', `${leaf}.d.ts`),
      'utf8',
    );
    assert.doesNotMatch(
      declaration,
      /node:async_hooks|AsyncLocalStorage|NodeJS|<reference\s+types=["']node["']/,
    );
  }
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
  const siblingManifest = JSON.parse(
    readFileSync(
      join(repositoryRoot, 'packages', 'breakwater', 'package.json'),
      'utf8',
    ),
  );
  // Both libraries run in one host, so a split Mastra pin is unsupported.
  assert.equal(
    corePeer,
    siblingManifest.peerDependencies['@mastra/core'],
    'breakwater and flowsafe must pin the same @mastra/core peer',
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
  const rootPackagePolicy = parseYaml(
    readFileSync(join(repositoryRoot, 'pnpm-workspace.yaml'), 'utf8'),
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
          zod: sourceManifest.devDependencies.zod,
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
    `${JSON.stringify(
      {
        packages: ['.'],
        minimumReleaseAge: 10080,
        minimumReleaseAgeExclude:
          rootPackagePolicy.minimumReleaseAgeExclude ?? [],
      },
      null,
      2,
    )}\n`,
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
  type InitialTerminalizationRequest as RootTerminalizationRequest,
  type InitialTerminalizationResult as RootTerminalizationResult,
  type RunTerminalCleanup as RootTerminalCleanup,
} from '@proofoftech/flowsafe';
import type {
  ApprovalGrantScope,
  ConnectorApprovalSuspension,
} from '@proofoftech/flowsafe/approval-api';
import {
  sweepExpiredRunDeadlines,
  FENCED_WORKFLOW_STORAGE,
  FencedWorkflowsStorageD1,
  type InitialRunAdmission,
  type InitialTerminalizationRequest,
  type InitialTerminalizationResult,
  type RunTerminalCleanup,
  type FencedWorkflowAdmissionCapability,
  type DeploymentInventory,
  type DrainProofContract,
  type DurableObjectRunLifecycleHooks,
  type ExecutionFenceState,
  type ExecutionFenceWiring,
  type RunTerminalErrorEnvelope,
  type StartIdempotencyWiring,
} from '@proofoftech/flowsafe/do-runner';
import {
  createFlowsafeRunnerLifecycle,
  createRunRouter,
  type FlowsafeWorker,
  type FlowsafeWorkerConfig,
  type FlowsafeWorkerEnv,
  type RunRouterOptions,
  type RunRouterStartIdempotency,
  type InitialTerminalizationRequest as HostTerminalizationRequest,
  type InitialTerminalizationResult as HostTerminalizationResult,
  type RunTerminalCleanup as HostTerminalCleanup,
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
  type AgentThreadTopologyOptions,
  type AutomatedEntryAuthorizer,
  type AutomatedEntryRequest,
  type AutomationCheck,
  type Permission,
  type PrincipalPermissionResolution,
  type PrincipalPermissionResolver,
} from '@proofoftech/flowsafe/agent-host';
import type {
  BackgroundTaskHost,
  BackgroundTaskReads,
} from '@proofoftech/flowsafe/background-tasks';

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
const fenceState: ExecutionFenceState = 'open';
const fenceWiring = null as ExecutionFenceWiring | null;
const startWiring = null as StartIdempotencyWiring | null;
const deploymentInventory = null as DeploymentInventory | null;
const drainProofReading = null as DrainProofContract['reading'] | null;
const routerOptions = null as RunRouterOptions | null;
const routerStart = null as RunRouterStartIdempotency | null;
const routerOptionStart = null as
  | RunRouterOptions['startIdempotency']
  | null;
const workerConfig = null as FlowsafeWorkerConfig<FlowsafeWorkerEnv> | null;
const worker = null as FlowsafeWorker<FlowsafeWorkerEnv> | null;
const topologyOptions = null as AgentThreadTopologyOptions | null;
const backgroundReads = null as BackgroundTaskReads | null;
declare const bgHost: BackgroundTaskHost;
declare const domainConfig: ConstructorParameters<typeof FencedWorkflowsStorageD1>[0];
declare const admission: InitialRunAdmission;
declare const terminalRequest: InitialTerminalizationRequest;
const hostTerminalRequest: HostTerminalizationRequest = terminalRequest;
const terminalResult = null as InitialTerminalizationResult | null;
const hostTerminalResult: HostTerminalizationResult | null = terminalResult;
const cleanup = null as RunTerminalCleanup | null;
const hostCleanup: HostTerminalCleanup | null = cleanup;
const rootTerminalRequest: RootTerminalizationRequest = terminalRequest;
const rootTerminalResult: RootTerminalizationResult | null = terminalResult;
const rootCleanup: RootTerminalCleanup | null = cleanup;
const rejectsNullNamespace: null extends InitialTerminalizationRequest['execution']['tablePrefix'] ? false : true = true;
void hostTerminalRequest;
void hostTerminalResult;
void hostCleanup;
void rootTerminalRequest;
void rootTerminalResult;
void rootCleanup;
void rejectsNullNamespace;
const owned = new FencedWorkflowsStorageD1(domainConfig);
const capability: FencedWorkflowAdmissionCapability | undefined = owned[FENCED_WORKFLOW_STORAGE];
if (capability) {
  void capability.withInitialAdmission(admission, async () => ({ id: 'run' }));
  void capability.readSnapshot({ workflowId: 'workflow', runId: 'run' });
  void capability.terminalizeInitialAdmission(terminalRequest);
  // @ts-expect-error callers cannot choose the disposition
  void capability.terminalizeInitialAdmission({ ...terminalRequest, requestedStatus: 'failed' });
  // @ts-expect-error callers cannot supply a replacement snapshot
  void capability.terminalizeInitialAdmission({ ...terminalRequest, failedSnapshot: {} });
}
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
void fenceState;
void fenceWiring;
void startWiring;
void deploymentInventory;
void drainProofReading;
void routerOptions;
void routerStart;
void routerOptionStart;
void workerConfig;
void worker;
void topologyOptions;
void backgroundReads;
void bgHost.enqueue;
void bgHost.getTask;
void bgHost.listTasks;
void bgHost.stream;
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
    join(consumer, 'tsconfig.es2022.json'),
    JSON.stringify({
      extends: './tsconfig.json',
      compilerOptions: { lib: ['ES2022'], types: [] },
    }),
  );
  run('pnpm', ['exec', 'tsc', '-p', 'tsconfig.es2022.json'], consumer);
  writeFileSync(
    join(consumer, 'sqlite-fixture.mjs'),
    transpile(
      readFileSync(join(packageRoot, 'test-support', 'sqlite.ts'), 'utf8'),
      { target: ScriptTarget.ES2022, module: ModuleKind.ESNext },
    ),
  );
  writeFileSync(
    join(consumer, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import * as host from '@proofoftech/flowsafe/agent-host';
import * as flowsafe from '@proofoftech/flowsafe';
import * as approvals from '@proofoftech/flowsafe/approval-api';
import * as backgroundTasks from '@proofoftech/flowsafe/background-tasks';
import * as doRunner from '@proofoftech/flowsafe/do-runner';
import * as hostKit from '@proofoftech/flowsafe/host-kit';
import { Mastra } from '@mastra/core/mastra';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from './sqlite-fixture.mjs';
for (const name of [
  'createD1Storage',
  'FencedWorkflowsStorageD1',
  'isDefinitiveInitialAdmissionRefusal',
  'RunAdmissionConflictError',
  'sweepExpiredRunDeadlines',
  'ExecutionFenceStore',
  'executionFenceFor',
  'readExecutionFence',
  'ExecutionFencedError',
  'FenceTransitionConflictError',
  'ExecutionFenceUnreadableError',
  'admitsDrainableExecution',
  'admitsExistingRun',
  'admitsRunStart',
  'admitsWorkAuthoring',
  'StartIdempotencyStore',
  'startIdempotencyFor',
  'StartReservationOwnerMismatchError',
  'StartReservationTargetMismatchError',
  'IdempotentStartPendingError',
  'IdempotentStartUnresolvableError',
  'IdempotentStartAlreadySettledError',
  'DeploymentInventory',
]) {
  assert.equal(typeof doRunner[name], 'function', name);
}
for (const name of [
  'INVENTORY_CATEGORIES',
  'FLOWSAFE_TABLES',
  'INVENTORY_UNENUMERABLE',
]) {
  assert.equal(Array.isArray(doRunner[name]), true, name);
}
assert.equal(typeof doRunner.INVENTORY_DRAIN_PROOF, 'object');
assert.equal(typeof doRunner.INVENTORY_DRAIN_PROOF.reading, 'string');
assert.equal(Array.isArray(doRunner.INVENTORY_DRAIN_PROOF.reachableFrom), true);
assert.equal(typeof hostKit.createFlowsafeRunnerLifecycle, 'function');
assert.equal(typeof hostKit.createRunRouter, 'function');
assert.equal(typeof hostKit.createFlowsafeWorker, 'function');
assert.equal(hostKit.FENCED_WORKFLOW_STORAGE, doRunner.FENCED_WORKFLOW_STORAGE);
assert.equal('FencedWorkflowsStorageD1' in hostKit, false);
assert.equal(typeof doRunner.RunLifecycleBlockedError, 'function');
assert.equal(doRunner.RunLifecycleBlockedError, flowsafe.RunLifecycleBlockedError);
assert.equal('RunLifecycleBlockedError' in hostKit, false);
const blocked = new doRunner.RunLifecycleBlockedError({ code: 'DISPUTED_SETTLEMENT', message: 'run termination is blocked while an economic operation is disputed' });
assert.equal(blocked instanceof flowsafe.RunLifecycleBlockedError, true);
assert.equal(blocked.name, 'RunLifecycleBlockedError');
assert.equal(blocked.reason.code, 'DISPUTED_SETTLEMENT');
const binding = sqliteUnitDatabase(openSqlite());
const storage = doRunner.createD1Storage({ binding });
let engineCalls = 0;
const workflow = createWorkflow({ id: 'packed-initial', inputSchema: z.object({}), outputSchema: z.object({}) })
  .then(createStep({ id: 'effect', inputSchema: z.object({}), outputSchema: z.object({}), execute: async () => { engineCalls += 1; return {}; } }))
  .commit();
new Mastra({ storage, workflows: { 'packed-initial': workflow } });
await storage.init();
const domain = await storage.getStore('workflows');
assert.equal(domain instanceof doRunner.FencedWorkflowsStorageD1, true);
const capability = domain[doRunner.FENCED_WORKFLOW_STORAGE];
assert.equal(capability.database, binding);
assert.equal(capability.tablePrefix, '');
const execution = { tablePrefix: '', workflowId: workflow.id, runId: 'packed-run', startToken: 'packed-generation' };
const admitted = await capability.withInitialAdmission({ execution, attemptToken: 'packed-correlation',
  fence: new doRunner.ExecutionFenceStore(binding), onInitialWriteAttempt() {},
  requestContext: { 'flowsafe.runProvenance': { version: 2, startToken: execution.startToken, attemptToken: 'packed-correlation', resumeCounts: [] } },
}, () => workflow.createRun({ runId: execution.runId }));
assert.deepEqual(admitted.witness.execution, execution);
assert.deepEqual(await capability.readSnapshot(execution), admitted.witness.row);
assert.equal(JSON.parse(admitted.witness.row.snapshot).status, 'pending');
assert.equal(engineCalls, 0);
await admitted.value.start({ inputData: {} });
assert.equal(engineCalls, 1);
const repairExecution = { ...execution, runId: 'packed-repair', startToken: 'repair-generation' };
const repair = await capability.withInitialAdmission({ execution: repairExecution, attemptToken: 'repair-correlation',
  fence: new doRunner.ExecutionFenceStore(binding), onInitialWriteAttempt() {},
  requestContext: { app: 'retained', 'flowsafe.runProvenance': { version: 2, startToken: repairExecution.startToken, attemptToken: 'repair-correlation', resumeCounts: [] } },
}, () => workflow.createRun({ runId: repairExecution.runId }));
const repairRequest = { expected: repair.witness.row, execution: repairExecution, attemptToken: 'repair-correlation', nowMs: 1700000000123 };
const repaired = await capability.terminalizeInitialAdmission(repairRequest);
assert.equal(repaired.kind, 'terminalized');
const repairedSnapshot = JSON.parse(repaired.row.snapshot);
assert.equal(repairedSnapshot.status, 'failed');
assert.equal(repairedSnapshot.error.name, 'StartOutcomeUnknown');
assert.equal('initialAdmission' in repairedSnapshot.requestContext['flowsafe.runProvenance'], false);
assert.equal(repairedSnapshot.requestContext.app, 'retained');
assert.equal(repaired.row.createdAt, repair.witness.row.createdAt);
assert.equal(repaired.row.updatedAt, '2023-11-14T22:13:20.123Z');
assert.deepEqual(await capability.readSnapshot(repairExecution), repaired.row);
assert.equal((await capability.terminalizeInitialAdmission(repairRequest)).kind, 'already-terminalized');
assert.equal(engineCalls, 1);
for (const name of ['nextLifecycleRevision', 'nextResumeCount', 'terminalStateFields', 'decodeProgressRunProvenance']) {
  assert.equal(name in doRunner, false, name);
  assert.equal(name in hostKit, false, name);
}
assert.equal(
  backgroundTasks.EXECUTION_FENCE_SUSPEND_KEY,
  'flowsafe.executionFenced',
);
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
