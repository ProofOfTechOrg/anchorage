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
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
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
  assert.deepEqual(
    Object.keys(manifest.exports).sort(),
    [
      '.',
      './agent-host',
      './agent-runner',
      './approval-api',
      './approval-ui',
      './artifacts',
      './audit-export',
      './background-tasks',
      './deployment-identity-protocol',
      './do-runner',
      './do-runner/constants',
      './do-runner/testing',
      './goals',
      './host-kit',
      './host-kit/module',
      './package.json',
      './schedules',
      './signal-providers',
      './signals',
      './signals/client',
    ].sort(),
  );
  const deadlineConsumer = join(temporary, 'deadline-consumer');
  const deadlineScope = join(deadlineConsumer, 'node_modules', '@proofoftech');
  mkdirSync(deadlineScope, { recursive: true });
  symlinkSync(packageDirectory, join(deadlineScope, 'flowsafe'), 'dir');
  const deadlineGraph = join(deadlineConsumer, 'module-graph.jsonl');
  const packedDistUrl = pathToFileURL(
    `${join(packageDirectory, 'dist')}/`,
  ).href;
  writeFileSync(
    join(deadlineConsumer, 'graph-loader.mjs'),
    `import { appendFileSync } from 'node:fs';
export async function resolve(specifier, context, nextResolve) {
  const result = await nextResolve(specifier, context);
  if (context.parentURL?.startsWith(${JSON.stringify(packedDistUrl)}) &&
      !result.url.startsWith(${JSON.stringify(packedDistUrl)})) {
    throw new Error('deadline runtime imports outside the packed dist: ' + result.url);
  }
  appendFileSync(${JSON.stringify(deadlineGraph)}, JSON.stringify({
    specifier, parentURL: context.parentURL, url: result.url,
  }) + '\\n');
  return result;
}
`,
  );
  writeFileSync(
    join(deadlineConsumer, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
const requireFromPackage = createRequire(${JSON.stringify(pathToFileURL(join(packageDirectory, 'package.json')).href)});
for (const name of ['@mastra/core', '@mastra/cloudflare-d1', 'jose']) {
  assert.throws(() => requireFromPackage.resolve(name), { code: 'MODULE_NOT_FOUND' }, name);
}
const constants = await import('@proofoftech/flowsafe/do-runner/constants');
const testing = await import('@proofoftech/flowsafe/do-runner/testing');
assert.deepEqual(Object.keys(constants).sort(), [
  'isArmableSuspensionDeadlineMs', 'isSuspensionTimeoutResumeData',
  'MAX_SUSPENSION_DEADLINE_MS', 'MIN_SUSPENSION_DEADLINE_MS',
  'SUSPENSION_DEADLINE_PAYLOAD_KEY', 'SUSPENSION_TIMEOUT_RESUME_KEY',
].sort());
assert.deepEqual(Object.keys(testing), ['suspensionTimeoutResumeData']);
assert.equal(constants.MIN_SUSPENSION_DEADLINE_MS, 1000);
assert.equal(constants.MAX_SUSPENSION_DEADLINE_MS, 31536000000);
assert.equal(constants.SUSPENSION_DEADLINE_PAYLOAD_KEY, 'flowsafe.deadlineMs');
assert.equal(constants.SUSPENSION_TIMEOUT_RESUME_KEY, 'flowsafe.suspensionTimeout');
for (const value of [1000, 1001, 86400000, 31536000000]) {
  assert.equal(constants.isArmableSuspensionDeadlineMs(value), true, String(value));
}
for (const value of [undefined, null, true, '1000', [], {}, NaN, Infinity, -Infinity, 0, -1, 999, 1000.5, 31536000001, Number.MAX_SAFE_INTEGER + 1]) {
  assert.equal(constants.isArmableSuspensionDeadlineMs(value), false, String(value));
}
const timeout = testing.suspensionTimeoutResumeData({ step: 'gate', deadlineAt: 2000 }, 2500);
assert.equal(JSON.stringify(timeout), '{"flowsafe.suspensionTimeout":{"step":"gate","deadlineAt":2000,"expiredAt":2500}}');
assert.equal(constants.isSuspensionTimeoutResumeData(timeout), true);
assert.equal(constants.isSuspensionTimeoutResumeData({
  'flowsafe.suspensionTimeout': { step: 'gate', deadlineAt: 2000, expiredAt: 2500 },
}), true);
for (const value of [undefined, null, {}, [], { 'flowsafe.suspensionTimeout': {} }, {
  'flowsafe.suspensionTimeout': { step: 'gate', deadlineAt: '2000', expiredAt: 2500 },
}]) {
  assert.equal(constants.isSuspensionTimeoutResumeData(value), false);
}
`,
  );
  run(
    process.execPath,
    ['--experimental-loader', './graph-loader.mjs', 'runtime.mjs'],
    deadlineConsumer,
  );
  const deadlineModules = readFileSync(deadlineGraph, 'utf8')
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line).url)
    .filter((url) => url.startsWith(packedDistUrl));
  assert.deepEqual(
    [...new Set(deadlineModules)].sort(),
    ['constants', 'testing', 'suspension-deadline', 'path-safe-id']
      .map((name) => `${packedDistUrl}do-runner/${name}.js`)
      .sort(),
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
  purgeExpiredWorkflowRuns,
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
  type MutationEpochContext,
  type RunTerminalErrorEnvelope,
  type RunRetentionCursor,
  type RunRetentionScanPosition,
  type SnapshotDatabase,
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
import {
  createScheduleRouter,
  createScheduleStorageDomains,
  D1SchedulesStorage,
  FENCED_SCHEDULE_STORAGE,
  ScheduleMutationConflictError,
  ScheduleMutationOutcomeUnknownError,
  type AuthorizedSchedule,
  type FencedScheduleMutationCapability,
  type Schedule,
  type ScheduleDatabase,
  type ScheduleFacadeStore,
  type ScheduleResumeMutation,
  type ScheduleRouter,
  type ScheduleRouterOptions,
} from '@proofoftech/flowsafe/schedules';
// @ts-expect-error internal context capture is not a schedules export
import type { captureActorContext as ScheduleCapture } from '@proofoftech/flowsafe/schedules';

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
declare const scheduleDatabase: ScheduleDatabase;
declare const scheduleRow: Schedule;
declare const authorizedSchedule: AuthorizedSchedule;
declare const scheduleRouterOptions: Omit<ScheduleRouterOptions, 'store' | 'executionFence'>;
const scheduleOwner = { kind: 'human', id: 'schedule-owner' } as const;
const scheduleMutation: MutationEpochContext = { mutationEpoch: 1 };
const scheduleResume: ScheduleResumeMutation = {
  expectedCron: scheduleRow.cron,
  expectedTimezone: undefined,
  nextFireAt: scheduleRow.nextFireAt,
};
const scheduleStore = new D1SchedulesStorage(scheduleDatabase);
void createScheduleStorageDomains(scheduleDatabase);
void scheduleStore.createSchedule(scheduleRow);
void scheduleStore.createSchedule(scheduleRow, scheduleMutation);
void scheduleStore.createOwnedSchedule(authorizedSchedule, scheduleOwner, 10);
void scheduleStore.createOwnedSchedule(authorizedSchedule, scheduleOwner, 10, scheduleMutation);
void scheduleStore.updateSchedule(scheduleRow.id, { metadata: {} });
void scheduleStore.updateSchedule(scheduleRow.id, { metadata: {} }, scheduleMutation);
void scheduleStore.pauseSchedule(scheduleRow.id);
void scheduleStore.pauseSchedule(scheduleRow.id, scheduleMutation);
void scheduleStore.resumeSchedule(scheduleRow.id, scheduleResume);
void scheduleStore.resumeSchedule(scheduleRow.id, scheduleResume, scheduleMutation);
void scheduleStore.deleteSchedule(scheduleRow.id);
void scheduleStore.deleteSchedule(scheduleRow.id, scheduleMutation);
void scheduleStore.deleteOwnedSchedule(scheduleRow.id);
void scheduleStore.deleteOwnedSchedule(scheduleRow.id, scheduleMutation);
const scheduleCapability: FencedScheduleMutationCapability | undefined = scheduleStore[FENCED_SCHEDULE_STORAGE];
if (scheduleCapability) {
  void scheduleCapability.createOwnedSchedule(authorizedSchedule, scheduleOwner, 10, scheduleMutation);
  void scheduleCapability.updateSchedule(scheduleRow.id, { metadata: {} }, scheduleMutation);
  void scheduleCapability.pauseSchedule(scheduleRow.id, scheduleMutation);
  void scheduleCapability.resumeSchedule(scheduleRow.id, scheduleResume, scheduleMutation);
  void scheduleCapability.deleteOwnedSchedule(scheduleRow.id, scheduleMutation);
  void scheduleCapability.observeScheduleMutation(scheduleRow.id, 'pause', scheduleMutation);
  void scheduleCapability.observeScheduleMutation(scheduleRow.id, 'resume', scheduleMutation);
  // @ts-expect-error owned creation requires the captured context argument
  void scheduleCapability.createOwnedSchedule(authorizedSchedule, scheduleOwner, 10);
  // @ts-expect-error update requires the captured context argument
  void scheduleCapability.updateSchedule(scheduleRow.id, { metadata: {} });
  // @ts-expect-error pause requires the captured context argument
  void scheduleCapability.pauseSchedule(scheduleRow.id);
  // @ts-expect-error resume requires the captured context argument
  void scheduleCapability.resumeSchedule(scheduleRow.id, scheduleResume);
  // @ts-expect-error deletion requires the captured context argument
  void scheduleCapability.deleteOwnedSchedule(scheduleRow.id);
  // @ts-expect-error observation requires the captured context argument
  void scheduleCapability.observeScheduleMutation(scheduleRow.id, 'pause');
  // @ts-expect-error observation cannot select an authoring operation
  void scheduleCapability.observeScheduleMutation(scheduleRow.id, 'update', scheduleMutation);
}
// @ts-expect-error fixed pause accepts no caller patch
void scheduleStore.pauseSchedule(scheduleRow.id, { status: 'paused' });
// @ts-expect-error resume requires the observed cron
void scheduleStore.resumeSchedule(scheduleRow.id, { expectedTimezone: undefined, nextFireAt: scheduleRow.nextFireAt });
// @ts-expect-error an omitted expectedTimezone is not an undefined observation
void scheduleStore.resumeSchedule(scheduleRow.id, { expectedCron: scheduleRow.cron, nextFireAt: scheduleRow.nextFireAt });
// @ts-expect-error the epoch is numeric trusted context
void scheduleStore.updateSchedule(scheduleRow.id, { metadata: {} }, { mutationEpoch: '1' });
const legacyScheduleFacade: ScheduleFacadeStore = {
  createOwnedSchedule: async () => scheduleRow,
  getSchedule: async () => scheduleRow,
  listSchedules: async () => [scheduleRow],
  updateSchedule: async () => scheduleRow,
  deleteOwnedSchedule: async () => 'deleted',
  listTriggers: async () => [],
};
const legacyScheduleRouter: ScheduleRouter = createScheduleRouter({ ...scheduleRouterOptions, store: legacyScheduleFacade, executionFence: 'none' });
void legacyScheduleRouter;
void new ScheduleMutationConflictError('schedule-changed');
void new ScheduleMutationOutcomeUnknownError({ cause: new Error('private cause') });
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
declare const retentionDatabase: SnapshotDatabase & Required<Pick<SnapshotDatabase, 'batch'>>;
declare const snapshotReader: SnapshotDatabase;
const retentionPosition: RunRetentionScanPosition = { afterRowId: -1, highWaterRowId: 12 };
let retentionCursor: RunRetentionCursor | undefined = {
  version: 1, tablePrefix: '', snapshots: retentionPosition,
};
const persistRetentionCursor = async (next: RunRetentionCursor): Promise<void> => { retentionCursor = next; };
void purgeExpiredWorkflowRuns(retentionDatabase, {
  ttlMs: 1000, cursor: retentionCursor, advanceCursor: persistRetentionCursor,
});
// @ts-expect-error retention requires a cursor persistence callback
void purgeExpiredWorkflowRuns(retentionDatabase, { ttlMs: 1000 });
// @ts-expect-error a reader with optional batch cannot guarantee a transaction
void purgeExpiredWorkflowRuns(snapshotReader, { ttlMs: 1000, advanceCursor: persistRetentionCursor });
void createFlowsafeRunnerLifecycle;
void createRunRouter;
`,
  );
  writeFileSync(
    join(consumer, 'deadline-consumer.ts'),
    `import {
  isArmableSuspensionDeadlineMs,
  suspensionDeadlinesOf,
  type RejectedSuspensionDeadline,
  type RunSummary,
  type SuspensionDeadlineEntry,
} from '@proofoftech/flowsafe/do-runner';
import * as constants from '@proofoftech/flowsafe/do-runner/constants';
import * as testing from '@proofoftech/flowsafe/do-runner/testing';

declare const summary: RunSummary;
const derived: {
  entries: SuspensionDeadlineEntry[];
  rejected: RejectedSuspensionDeadline[];
} = suspensionDeadlinesOf(summary);
declare const candidate: unknown;
if (isArmableSuspensionDeadlineMs(candidate)) {
  const duration: number = candidate;
  void duration;
}
if (constants.isArmableSuspensionDeadlineMs(candidate)) {
  const duration: number = candidate;
  void duration;
}
const timeout: testing.SuspensionTimeoutResumeData = testing.suspensionTimeoutResumeData(
  { step: 'gate', deadlineAt: 2000 }, 2500,
);
const constantsTimeout: constants.SuspensionTimeoutResumeData = timeout;
const envelope: testing.SuspensionTimeoutEnvelope = timeout[constants.SUSPENSION_TIMEOUT_RESUME_KEY];
const constantsEnvelope: constants.SuspensionTimeoutEnvelope = envelope;
if (constants.isSuspensionTimeoutResumeData(candidate)) {
  const detected: constants.SuspensionTimeoutResumeData = candidate;
  void detected[constants.SUSPENSION_TIMEOUT_RESUME_KEY].expiredAt;
}
// @ts-expect-error the test minter requires the step id
testing.suspensionTimeoutResumeData({ deadlineAt: 2000 }, 2500);
// @ts-expect-error the test minter requires an epoch millisecond deadline
testing.suspensionTimeoutResumeData({ step: 'gate', deadlineAt: '2000' }, 2500);
// @ts-expect-error the storage record parser is private
void constants.parseSuspensionDeadlineRecord;
// @ts-expect-error the deadline merge helper is private
void constants.mergeSuspensionDeadlines;
// @ts-expect-error the storage key is private
void constants.SUSPENSION_DEADLINE_STORAGE_KEY;
// @ts-expect-error the storage record parser is private
void testing.parseSuspensionDeadlineRecord;
// @ts-expect-error the retry ledger is private
void testing.tombstoned;
void [derived, constantsTimeout, constantsEnvelope];
`,
  );
  writeFileSync(
    join(consumer, 'transport-consumer.ts'),
    `import {
  type ActorContext, ApprovalService, createActorResolver,
  createPrincipalActorContext, humanPrincipal, InMemoryApprovalStoreFactory,
} from '@proofoftech/flowsafe/approval-api';
import {
  type RunnerRuntime, type RunExecutionIdentity, type StartRunOptions,
  type ThreadScope, type StartReservationReading, type PersistedStartResult,
  type RunSummary,
} from '@proofoftech/flowsafe/do-runner';
import {
  createFlowsafeWorker, type FlowsafeWorkerConfig, type FlowsafeWorkerEnv,
  type RunStartInput, type DoRunStartInput, type RunRouterOptions,
  type RunRouterStartIdempotency,
} from '@proofoftech/flowsafe/host-kit';
import {
  type AgentStartAuthority, type FlowsafeDurableAgent,
} from '@proofoftech/flowsafe/agent-runner';
// @ts-expect-error internal context capture is not a root export
import type { captureActorContext as RootCapture } from '@proofoftech/flowsafe';
// @ts-expect-error internal context capture is not an approval export
import type { captureActorContext as ApprovalCapture } from '@proofoftech/flowsafe/approval-api';
// @ts-expect-error internal context capture is not a host-kit export
import type { captureActorContext as HostCapture } from '@proofoftech/flowsafe/host-kit';
// @ts-expect-error internal context capture is not an agent-host export
import type { captureActorContext as AgentCapture } from '@proofoftech/flowsafe/agent-host';
// @ts-expect-error internal context capture is not an agent-runner export
import type { captureActorContext as AgentRunnerCapture } from '@proofoftech/flowsafe/agent-runner';
// @ts-expect-error internal context capture is not a do-runner export
import type { captureActorContext as RunnerCapture } from '@proofoftech/flowsafe/do-runner';
// @ts-expect-error agent authority belongs only to agent-runner
import type { AgentStartAuthority as RootAuthority } from '@proofoftech/flowsafe';
// @ts-expect-error agent authority belongs only to agent-runner
import type { AgentStartAuthority as ApprovalAuthority } from '@proofoftech/flowsafe/approval-api';
// @ts-expect-error agent authority belongs only to agent-runner
import type { AgentStartAuthority as HostAuthority } from '@proofoftech/flowsafe/host-kit';
// @ts-expect-error agent authority belongs only to agent-runner
import type { AgentStartAuthority as AgentAuthority } from '@proofoftech/flowsafe/agent-host';
// @ts-expect-error agent authority belongs only to agent-runner
import type { AgentStartAuthority as RunnerAuthority } from '@proofoftech/flowsafe/do-runner';

const actor = { id: 'owner', role: 'operator' } as const;
const principal = humanPrincipal(actor);
const factory = new InMemoryApprovalStoreFactory();
const service = new ApprovalService({ store: factory.store(), executionFence: 'none' });
const legacyContext: ActorContext = {
  actor, principal, resourceOwner: { kind: 'human', id: actor.id },
  service: () => service, newRunId: () => 'run', newThreadId: () => 'thread',
  resourceIdFromKey: key => key, claimResource: async () => {},
  releaseResource: async () => {}, resourceOwnerFor: async () => undefined,
  canAccessResource: async () => true, canSelfDecide: () => false,
};
const epochContext: ActorContext = { ...legacyContext, mutationEpoch: 2 };
createActorResolver({
  authenticate: () => actor, storeFactory: factory,
  buildService: () => service, mutationEpoch: 2,
});
createPrincipalActorContext({
  principal, storeFactory: factory, buildService: () => service, mutationEpoch: 2,
});
declare const hostInit: ThreadScope['init'];
const legacyScope: ThreadScope = { threadId: 'thread', principal, init: hostInit };
const epochScope: ThreadScope = { ...legacyScope, mutationEpoch: 2 };
const legacyInput: RunStartInput = { workflowId: 'workflow', runId: 'run', inputData: {}, principal };
const epochInput: RunStartInput = { ...legacyInput, mutationEpoch: 2 };
const applicationContext: Record<string, unknown> = { 'app.attribution': 'accepted', nested: { value: 1 } };
const contextInput: RunStartInput = { ...legacyInput, requestContext: applicationContext };
const doContextInput: DoRunStartInput = { ...contextInput, initialState: {} };
const legacyDoInput: DoRunStartInput = { ...legacyInput, initialState: {} };
const routerHook: NonNullable<RunRouterOptions['beforeStart']> = async (context, workflowId, inputData, requestContext) => {
  const application: Record<string, unknown> | undefined = requestContext;
  void [context.actor, workflowId, inputData, application];
};
const legacyRouterHook: NonNullable<RunRouterOptions['beforeStart']> = async (_context, _workflowId, _inputData) => {};
const routerPolicyResult: Promise<void> = routerHook(legacyContext, 'workflow', {}, applicationContext);
type EpochEnv = FlowsafeWorkerEnv & { artifactEpoch: number };
const workerHook: NonNullable<FlowsafeWorkerConfig<EpochEnv>['beforeStart']> = async (context, env, workflowId, inputData, requestContext) => {
  const application: Record<string, unknown> | undefined = requestContext;
  void [context.actor, env.artifactEpoch, workflowId, inputData, application];
};
const legacyWorkerHook: NonNullable<FlowsafeWorkerConfig<EpochEnv>['beforeStart']> = async (_context, _env, _workflowId, _inputData) => {};
declare const workerEnv: EpochEnv;
const workerPolicyResult: Promise<void> = workerHook(legacyContext, workerEnv, 'workflow', {}, applicationContext);
const workerConfig: FlowsafeWorkerConfig<EpochEnv> = {
  systemPrincipalId: 'system', workflows: [],
  buildVerifier: () => ({ verify: async () => actor }),
  maintenance: { sweepIntervalMs: 1000, purgeIntervalMs: 1000 },
  mutationEpoch: env => env.artifactEpoch,
  beforeStart: workerHook,
};
createFlowsafeWorker(workerConfig);
createFlowsafeWorker({ ...workerConfig, mutationEpoch: 0 });
createFlowsafeWorker({ ...workerConfig, beforeStart: legacyWorkerHook });
void [contextInput, doContextInput, legacyDoInput, legacyRouterHook, routerPolicyResult, workerPolicyResult];
const onPrepared = (execution: RunExecutionIdentity): void => { void execution.startToken; };
const legacyOptions: StartRunOptions = { runId: 'legacy' };
const options: StartRunOptions = {
  runId: 'run', requestedBy: actor.id, requestedByKind: 'human',
  attemptToken: 'attempt', mutationEpoch: 2,
  startIdentity: { owner: { kind: 'human', id: actor.id }, target: { kind: 'workflow', id: 'workflow' } },
  onPreparedStartIdentity: onPrepared,
  runOwnerGuard: { owner: { kind: 'service', id: 'resource-owner' }, reservationToken: 'attempt' },
};
// @ts-expect-error requester identity is an all-or-neither pair
const partialRequester: StartRunOptions = { runId: 'run', requestedBy: actor.id };
// @ts-expect-error direct Runtime epoch is numeric
const stringEpoch: StartRunOptions = { runId: 'run', mutationEpoch: '2' };
declare const runtime: RunnerRuntime;
void runtime.start('workflow', options);
declare const winningClaim: StartReservationReading;
const keyedOptions: StartRunOptions = { ...options, startReservation: winningClaim };
const keyedInput: RunStartInput = { ...epochInput, startReservation: winningClaim };
const persistedResult: PersistedStartResult<RunSummary> = {
  kind: 'result', value: { runId: 'run', status: 'success' },
  execution: { tablePrefix: null, workflowId: 'workflow', runId: 'run', startToken: 'generation',
    owner: { kind: 'human', id: actor.id }, target: { kind: 'workflow', id: 'workflow' } },
};
const privateStart: Exclude<RunRouterStartIdempotency, 'none'>['persistedStart'] = async () => persistedResult;
// @ts-expect-error ordinary status does not provide execution identity
const invalidPrivateStart: typeof privateStart = async () => ({ runId: 'run', status: 'success' });
// @ts-expect-error token-only recovery is removed
void runtime.recoverStartAttempt('workflow', 'run', 'attempt');
const authority: AgentStartAuthority = {
  mutationEpoch: 2,
  startIdentity: { owner: { kind: 'human', id: actor.id }, target: { kind: 'agent', id: 'agent', threadId: 'thread' } },
  agentStart: { threaded: true }, onPreparedStartIdentity: undefined,
};
const callbackAuthority: AgentStartAuthority = { ...authority, onPreparedStartIdentity: onPrepared };
const keyedAuthority: AgentStartAuthority = { ...callbackAuthority, startReservation: winningClaim };
declare const durable: FlowsafeDurableAgent;
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined, authority);
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined, callbackAuthority);
// @ts-expect-error the eighth trusted authority argument is required
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined);
const { onPreparedStartIdentity: omitted, ...withoutCallback } = authority;
// @ts-expect-error the callback property is required even when undefined
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined, withoutCallback);
// @ts-expect-error callback must be a function or undefined
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined, { ...authority, onPreparedStartIdentity: 'invalid' });
// @ts-expect-error the bridge requires an agent target
void durable.streamUntilPersisted('input', { runId: 'run' }, actor.id, 'human', 'attempt', undefined, undefined, { ...authority, startIdentity: { owner: authority.startIdentity.owner, target: { kind: 'workflow', id: 'workflow' } } });
void [legacyContext, epochContext, legacyScope, epochScope, legacyInput, epochInput, legacyOptions, partialRequester, stringEpoch, omitted, keyedOptions, keyedInput, privateStart, invalidPrivateStart, keyedAuthority];
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
      files: ['consumer.ts', 'transport-consumer.ts', 'deadline-consumer.ts'],
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
import * as deadlineConstants from '@proofoftech/flowsafe/do-runner/constants';
import * as deadlineTesting from '@proofoftech/flowsafe/do-runner/testing';
import * as hostKit from '@proofoftech/flowsafe/host-kit';
import * as agentRunner from '@proofoftech/flowsafe/agent-runner';
import * as schedules from '@proofoftech/flowsafe/schedules';
import { Mastra } from '@mastra/core/mastra';
import { InMemoryStore } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from './sqlite-fixture.mjs';
assert.equal(doRunner.isArmableSuspensionDeadlineMs, deadlineConstants.isArmableSuspensionDeadlineMs);
assert.equal(doRunner.isSuspensionTimeoutResumeData, deadlineConstants.isSuspensionTimeoutResumeData);
assert.equal(doRunner.MIN_SUSPENSION_DEADLINE_MS, deadlineConstants.MIN_SUSPENSION_DEADLINE_MS);
assert.equal(doRunner.MAX_SUSPENSION_DEADLINE_MS, deadlineConstants.MAX_SUSPENSION_DEADLINE_MS);
const deadlineSummary = {
  runId: 'packed-deadline-run', status: 'suspended', suspended: [['gate']],
  suspendPayload: { gate: { [deadlineConstants.SUSPENSION_DEADLINE_PAYLOAD_KEY]: 1000 } },
  suspendedAt: { gate: 2000 }, resumeCount: { gate: 3 },
};
const derivedDeadlines = doRunner.suspensionDeadlinesOf(deadlineSummary);
assert.deepEqual(derivedDeadlines, {
  entries: [{ step: 'gate', deadlineAt: 3000, suspendedAt: 2000, resumeCount: 3 }],
  rejected: [],
});
const packedTimeout = deadlineTesting.suspensionTimeoutResumeData(derivedDeadlines.entries[0], 3500);
assert.deepEqual(packedTimeout, {
  'flowsafe.suspensionTimeout': { step: 'gate', deadlineAt: 3000, expiredAt: 3500 },
});
assert.equal(doRunner.isSuspensionTimeoutResumeData(packedTimeout), true);
assert.deepEqual(doRunner.suspensionDeadlinesOf({
  ...deadlineSummary,
  suspendPayload: { gate: { [deadlineConstants.SUSPENSION_DEADLINE_PAYLOAD_KEY]: 999 } },
}), {
  entries: [],
  rejected: [{ step: 'gate', reason: 'flowsafe.deadlineMs must be between 1000 and 31536000000 ms' }],
});
for (const name of ['parseSuspensionDeadlineRecord', 'mergeSuspensionDeadlines', 'SUSPENSION_DEADLINE_STORAGE_KEY', 'suspensionTimeoutResumeData']) {
  assert.equal(name in doRunner, false, name);
}
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
  'RunStartPendingError',
  'isRunStartPendingError',
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
const contextTransports = [];
const contextPolicies = [];
const contextPrincipal = { kind: 'human', id: 'context-owner', role: 'operator' };
const contextTopology = hostKit.createDoRunTopology({
  idFromName: name => name,
  get: name => ({ fetch: async (url, init) => {
    const body = JSON.parse(init.body);
    contextTransports.push({ name, url, body, headers: init.headers });
    return Response.json({ runId: body.runId, status: 'success' });
  } }),
}, 'packed-context-deployment-identity-secret');
const contextFactory = new approvals.InMemoryApprovalStoreFactory();
const contextRouter = hostKit.createRunRouter({
  workflows: [{ id: 'context-workflow', title: 'Context', description: 'Packed transport', sampleInput: {} }],
  resolve: approvals.createActorResolver({
    authenticate: () => ({ id: contextPrincipal.id, role: contextPrincipal.role }),
    storeFactory: contextFactory,
    buildService: () => new approvals.ApprovalService({ store: contextFactory.store(), executionFence: 'none' }),
    newRunId: () => 'context-run',
    mutationEpoch: 2,
  }),
  startIdempotency: 'none',
  start: contextTopology.start,
  status: contextTopology.status,
  resume: contextTopology.resume,
  beforeStart: async (context, workflowId, inputData, requestContext) => {
    contextPolicies.push({ principal: context.principal, workflowId, inputData, requestContext });
    return { 'app.attribution': 'ignored-hook-return' };
  },
});
const packedApplicationContext = { 'app.attribution': 'accepted', nested: { values: [1, true] } };
const contextResponse = await contextRouter(new Request('https://packed.test/runs', {
  method: 'POST',
  body: JSON.stringify({ workflowId: 'context-workflow', inputData: { topic: 'launch' }, requestContext: packedApplicationContext }),
}));
assert.equal(contextResponse.status, 200);
assert.deepEqual(contextPolicies, [{
  principal: contextPrincipal, workflowId: 'context-workflow', inputData: { topic: 'launch' }, requestContext: packedApplicationContext,
}]);
assert.deepEqual(contextTransports[0].body, {
  workflowId: 'context-workflow', runId: 'context-run', inputData: { topic: 'launch' }, requestContext: packedApplicationContext,
});
assert.equal(contextTransports[0].name, 'context-workflow:context-run');
assert.equal(new Headers(contextTransports[0].headers).get(doRunner.MUTATION_EPOCH_HEADER), '2');
assert.deepEqual(JSON.parse(new Headers(contextTransports[0].headers).get(doRunner.EXECUTION_PRINCIPAL_HEADER)), contextPrincipal);
await contextTopology.start({
  workflowId: 'context-workflow', runId: 'context-scheduled-run', inputData: { forged: true },
  initialState: { forged: true }, requestContext: { 'app.attribution': 'forged' },
  principal: contextPrincipal, scheduleId: 'context-schedule', dispatchId: 'context-dispatch', deadlineMs: 60000,
});
assert.deepEqual(contextTransports[1].body, {
  workflowId: 'context-workflow', runId: 'context-scheduled-run',
  scheduleId: 'context-schedule', dispatchId: 'context-dispatch', deadlineMs: 60000,
});
assert.equal(hostKit.FENCED_WORKFLOW_STORAGE, doRunner.FENCED_WORKFLOW_STORAGE);
assert.equal('FencedWorkflowsStorageD1' in hostKit, false);
assert.equal(typeof agentRunner.FlowsafeDurableAgent, 'function');
for (const name of ['claim', 'release', 'settleRun']) {
  assert.equal(name in doRunner.StartIdempotencyStore.prototype, false, name);
}
for (const api of [flowsafe, doRunner, hostKit]) assert.equal('rollbackFencedStart' in api, false);
for (const api of [flowsafe, approvals, doRunner, hostKit, host, agentRunner, schedules]) {
  for (const name of ['captureActorContext', 'captureAgentStartAuthority', 'captureStartRunOptions', 'startAuthorities', 'AgentStartAuthority', 'executionFenceAdmissionValues', 'captureExecutionFenceAdmissionSchema', 'executionFenceAdmissionSql', 'AgentRunSelectorMismatchError']) {
    assert.equal(name in api, false, name);
  }
}
for (const api of [flowsafe, doRunner, hostKit]) {
  for (const name of ['FENCED_SCHEDULE_STORAGE', 'ScheduleMutationConflictError', 'ScheduleMutationOutcomeUnknownError']) {
    assert.equal(name in api, false, name);
  }
}
for (const name of ['normalizeMutationEpoch', 'stampMutationEpoch', 'mutationEpochFromHeader', 'InvalidMutationEpochError', 'MutationEpochMismatchError']) {
  assert.equal(doRunner[name], hostKit[name], name);
  assert.equal(doRunner[name], flowsafe[name], name);
}
assert.equal(typeof doRunner.RunLifecycleBlockedError, 'function');
assert.equal(doRunner.RunLifecycleBlockedError, flowsafe.RunLifecycleBlockedError);
assert.equal('RunLifecycleBlockedError' in hostKit, false);
const blocked = new doRunner.RunLifecycleBlockedError({ code: 'DISPUTED_SETTLEMENT', message: 'run termination is blocked while an economic operation is disputed' });
assert.equal(blocked instanceof flowsafe.RunLifecycleBlockedError, true);
assert.equal(blocked.name, 'RunLifecycleBlockedError');
assert.equal(blocked.reason.code, 'DISPUTED_SETTLEMENT');
assert.equal(typeof schedules.FENCED_SCHEDULE_STORAGE, 'symbol');
assert.equal(typeof schedules.ScheduleMutationConflictError, 'function');
assert.equal(typeof schedules.ScheduleMutationOutcomeUnknownError, 'function');
const scheduleNative = sqliteUnitDatabase(openSqlite());
const lostScheduleResponse = new Error('packed schedule batch response lost');
let loseScheduleResponse = false;
let scheduleBatches = 0;
const scheduleBinding = {
  prepare(sql) { return scheduleNative.prepare(sql); },
  async batch(statements) {
    scheduleBatches += 1;
    const result = await scheduleNative.batch(statements);
    if (loseScheduleResponse) {
      loseScheduleResponse = false;
      throw lostScheduleResponse;
    }
    return result;
  },
};
const scheduleStore = new schedules.D1SchedulesStorage(scheduleBinding);
const scheduleCapability = scheduleStore[schedules.FENCED_SCHEDULE_STORAGE];
assert.ok(scheduleCapability);
assert.equal(scheduleCapability.database, scheduleBinding);
const scheduleStorage = doRunner.createD1Storage({
  binding: scheduleBinding,
  domains: schedules.createScheduleStorageDomains(scheduleBinding),
});
await scheduleStorage.init();
const scheduleDomain = await scheduleStorage.getStore('schedules');
assert.ok(scheduleDomain instanceof schedules.D1SchedulesStorage);
const scheduleDomainCapability = scheduleDomain[schedules.FENCED_SCHEDULE_STORAGE];
assert.ok(scheduleDomainCapability);
assert.equal(scheduleDomainCapability.database, scheduleBinding);
const scheduleRow = schedules.scheduleWithCreatorRole({
  id: 'packed-schedule',
  target: { type: 'workflow', workflowId: 'packed-schedule-workflow', inputData: {} },
  cron: '*/5 * * * *', status: 'active',
  nextFireAt: 1700000300000, createdAt: 1700000000000, updatedAt: 1700000000000,
  metadata: { source: 'packed' },
}, 'operator');
const scheduleOwner = { kind: 'human', id: 'packed-schedule-owner' };
const createdSchedule = await scheduleCapability.createOwnedSchedule(scheduleRow, scheduleOwner, 10, {});
assert.equal(createdSchedule.id, scheduleRow.id);
assert.equal(createdSchedule.status, 'active');
assert.deepEqual(createdSchedule.metadata, scheduleRow.metadata);
const storedScheduleOwner = await scheduleBinding.prepare("SELECT owner_kind, owner_id FROM flowsafe_resource_owners WHERE resource_kind = 'schedule' AND resource_id = ?")
  .bind(scheduleRow.id).first();
assert.equal(storedScheduleOwner.owner_kind, scheduleOwner.kind);
assert.equal(storedScheduleOwner.owner_id, scheduleOwner.id);
const scheduleFence = new doRunner.ExecutionFenceStore(scheduleBinding);
const scheduleRouterOptions = {
  store: scheduleStore, executionFence: scheduleFence,
  resolve: async () => undefined,
  targetPolicy: schedules.createScheduleTargetPolicy({ workflows: [{ id: 'packed-schedule-workflow' }], agents: [] }),
  validateThreadTarget: async () => undefined,
};
assert.equal(typeof schedules.createScheduleRouter(scheduleRouterOptions), 'function');
assert.throws(() => schedules.createScheduleRouter({
  ...scheduleRouterOptions,
  executionFence: new doRunner.ExecutionFenceStore(sqliteUnitDatabase(openSqlite())),
}), /schedule storage binding disagrees with execution fence/);
const legacyScheduleFacade = {
  createOwnedSchedule: async (schedule) => schedule,
  getSchedule: async () => null,
  listSchedules: async () => [],
  updateSchedule: async () => scheduleRow,
  deleteOwnedSchedule: async () => 'deleted',
  listTriggers: async () => [],
};
assert.equal(schedules.FENCED_SCHEDULE_STORAGE in legacyScheduleFacade, false);
assert.equal(typeof schedules.createScheduleRouter({
  ...scheduleRouterOptions, store: legacyScheduleFacade, executionFence: 'none',
}), 'function');
await scheduleFence.transition({
  expected: 'open', next: 'draining', expectedMutationEpoch: 0,
  expectedRevision: 0, advanceMutationEpoch: true,
});
await scheduleFence.transition({
  expected: 'draining', next: 'open', expectedMutationEpoch: 1,
  expectedRevision: 1,
});
await assert.rejects(() => scheduleStore.updateSchedule(scheduleRow.id, { metadata: { unauthorized: true } }), (error) => {
  assert.ok(error instanceof doRunner.MutationEpochMismatchError);
  assert.equal(error.status, 409);
  assert.deepEqual(error.reason, { code: 'MUTATION_EPOCH_MISMATCH', classification: 'missing', mutationEpoch: 1 });
  return true;
});
assert.deepEqual((await scheduleStore.getSchedule(scheduleRow.id)).metadata, scheduleRow.metadata);
const scheduleMutation = { mutationEpoch: 1 };
const pausedSchedule = await scheduleCapability.pauseSchedule(scheduleRow.id, scheduleMutation);
await scheduleCapability.updateSchedule(scheduleRow.id, { cron: '*/10 * * * *' }, scheduleMutation);
await assert.rejects(() => scheduleCapability.resumeSchedule(scheduleRow.id, {
  expectedCron: pausedSchedule.cron, expectedTimezone: pausedSchedule.timezone,
  nextFireAt: pausedSchedule.nextFireAt + 60000,
}, scheduleMutation), (error) => {
  assert.ok(error instanceof schedules.ScheduleMutationConflictError);
  assert.equal(error.status, 409);
  assert.deepEqual(error.reason, { code: 'SCHEDULE_MUTATION_CONFLICT', classification: 'schedule-changed' });
  return true;
});
const conflictedSchedule = await scheduleStore.getSchedule(scheduleRow.id);
assert.equal(conflictedSchedule.status, 'paused');
assert.equal(conflictedSchedule.cron, '*/10 * * * *');
assert.equal(conflictedSchedule.nextFireAt, pausedSchedule.nextFireAt);
const committedScheduleMetadata = { source: 'committed-response-loss' };
const batchesBeforeLoss = scheduleBatches;
loseScheduleResponse = true;
await assert.rejects(() => scheduleCapability.updateSchedule(scheduleRow.id, {
  metadata: committedScheduleMetadata,
}, scheduleMutation), (error) => {
  assert.ok(error instanceof schedules.ScheduleMutationOutcomeUnknownError);
  assert.equal(error.status, 503);
  assert.deepEqual(error.reason, { code: 'SCHEDULE_MUTATION_OUTCOME_UNKNOWN' });
  assert.equal(error.cause, lostScheduleResponse);
  return true;
});
assert.equal(scheduleBatches, batchesBeforeLoss + 1);
assert.equal(loseScheduleResponse, false);
const committedSchedule = await scheduleBinding.prepare('SELECT metadata FROM mastra_schedules WHERE id = ?').bind(scheduleRow.id).first();
assert.deepEqual(JSON.parse(committedSchedule.metadata), committedScheduleMetadata);
const binding = sqliteUnitDatabase(openSqlite());
const retentionBinding = sqliteUnitDatabase(openSqlite());
await retentionBinding.prepare('CREATE TABLE mastra_workflow_snapshot (workflow_name TEXT NOT NULL, run_id TEXT NOT NULL, resourceId TEXT, snapshot TEXT NOT NULL, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL, UNIQUE(workflow_name, run_id))').run();
for (const [runId, provenance] of [
  ['malformed-retention', { version: 2, startToken: 42 }],
  ['eligible-retention', { version: 2, startToken: 'packed-retention-generation', attemptToken: 'packed-retention-attempt', resumeCounts: [] }],
]) {
  await retentionBinding.prepare('INSERT INTO mastra_workflow_snapshot (workflow_name, run_id, snapshot, createdAt, updatedAt) VALUES (?, ?, ?, ?, ?)')
    .bind('packed-retention', runId, JSON.stringify({ status: 'success', requestContext: { 'flowsafe.runProvenance': provenance } }), '2020-01-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z').run();
}
let retainedCursor;
const retentionOptions = { ttlMs: 1000, limit: 1, now: () => Date.parse('2026-09-08T00:00:00.000Z'), advanceCursor: async next => { retainedCursor = structuredClone(next); } };
assert.equal(await doRunner.purgeExpiredWorkflowRuns(retentionBinding, retentionOptions), 0);
assert.ok(retainedCursor.snapshots);
assert.equal(await doRunner.purgeExpiredWorkflowRuns(retentionBinding, { ...retentionOptions, cursor: retainedCursor }), 1);
assert.equal(retainedCursor.snapshots, undefined);
assert.deepEqual((await retentionBinding.prepare('SELECT run_id FROM mastra_workflow_snapshot').all()).results.map(row => row.run_id), ['malformed-retention']);
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
const observer = doRunner.init({ storage }, { executionFence: new doRunner.ExecutionFenceStore(binding), startIdempotency: 'none' }).runtime;
observer.register(workflow);
const initial = await observer.authoritativeStartState(workflow.id, repairExecution.runId);
assert.equal(initial.kind, 'initial');
assert.equal('summary' in initial, false);
await assert.rejects(() => observer.status(workflow.id, repairExecution.runId), doRunner.isRunStartPendingError);
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
const reservations = new doRunner.StartIdempotencyStore(binding);
const generations = new Set();
let modernEffects = 0;
for (const mode of [
  { name: 'fenced', storage: doRunner.createD1Storage({ binding }), fence: new doRunner.ExecutionFenceStore(binding), prefix: '' },
  { name: 'optional', storage: doRunner.createD1Storage({ binding, tablePrefix: 'packed_opt_' }), fence: 'none', prefix: 'packed_opt_' },
  { name: 'custom', storage: new InMemoryStore(), fence: 'none', prefix: null },
]) {
  await mode.storage.init();
  const app = doRunner.init({ storage: mode.storage }, { executionFence: mode.fence, startIdempotency: reservations });
  const workflowId = 'packed-runtime-' + mode.name;
  const runId = 'packed-run-' + mode.name;
  const owner = { kind: 'human', id: 'packed-owner' };
  const startIdentity = { owner, target: { kind: 'workflow', id: workflowId } };
  app.createWorkflow({
    id: workflowId, inputSchema: z.object({}), outputSchema: z.object({ value: z.string() }),
    ...(mode.name === 'optional' ? { options: { shouldPersistSnapshot: ({ workflowStatus }) => workflowStatus !== 'pending' } } : {}),
  }).then(app.createStep({
    id: 'effect', inputSchema: z.object({}), outputSchema: z.object({ value: z.string() }),
    execute: async () => { modernEffects += 1; return { value: mode.name }; },
  })).commit();
  const request = { key: 'packed-key-' + mode.name, owner, targetKind: 'workflow', targetId: workflowId, mintRunId: () => runId };
  const reserved = await reservations.reserve(request);
  assert.equal(reserved.reservation.binding.kind, 'unbound');
  const claimed = await reservations.claimReservation(reserved.reservation);
  assert.equal(claimed.state, 'started');
  const summary = await app.runtime.start(workflowId, {
    runId, inputData: {}, requestedBy: owner.id, requestedByKind: owner.kind,
    attemptToken: 'packed-shared-H', startIdentity, idempotencyKey: request.key, startReservation: claimed,
  });
  const selected = await app.runtime.authoritativeStartState(workflowId, runId);
  assert.equal(selected.kind, 'result');
  assert.equal(selected.execution.tablePrefix, mode.prefix);
  assert.notEqual(selected.execution.startToken, 'packed-shared-H');
  generations.add(selected.execution.startToken);
  assert.deepEqual(summary.result, { value: mode.name });
  const complete = { ...selected.execution, ...startIdentity };
  const storedClaim = await reservations.read(request.key);
  assert.equal(storedClaim.state, 'terminal');
  assert.deepEqual(storedClaim.binding, { kind: 'bound', execution: selected.execution });
  let reads = 0;
  const surface = {
    persisted: async () => { reads += 1; return { kind: 'result', execution: complete, value: selected.summary }; },
    live: async () => { throw new Error('a nonpending replay must not probe liveness'); },
  };
  const replay = await doRunner.beginIdempotentStart(reservations, request, surface, mode.fence);
  assert.equal(replay.kind, 'replay');
  assert.equal(replay.persisted, selected.summary);
  assert.equal(reads, 1);
  const aliasRequest = { ...request, key: 'packed-alias-' + mode.name };
  await reservations.reserve(aliasRequest);
  const alias = await doRunner.beginIdempotentStart(reservations, aliasRequest, surface, mode.fence);
  assert.equal(alias.kind, 'replay');
  assert.equal(alias.persisted, selected.summary);
  assert.deepEqual(alias.reservation.binding.execution, selected.execution);
  assert.equal(await reservations.settleExecution(complete), 1);
}
assert.equal(generations.size, 3);
assert.equal(modernEffects, 3);
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
