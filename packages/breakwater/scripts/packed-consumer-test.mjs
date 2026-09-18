// SPDX-License-Identifier: Apache-2.0
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import {
  mkdir,
  mkdtemp,
  readdir,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const workspaceRoot = resolve(packageRoot, '..', '..');
const temporaryRoot = await mkdtemp(
  join(tmpdir(), 'breakwater-packed-consumer-'),
);

function run(command, args, options = {}) {
  execFileSync(command, args, {
    cwd: options.cwd ?? packageRoot,
    encoding: 'utf8',
    stdio: options.stdio ?? 'pipe',
  });
}

try {
  const packedDirectory = join(temporaryRoot, 'packed');
  const extractedDirectory = join(temporaryRoot, 'extracted');
  const consumerDirectory = join(temporaryRoot, 'consumer');
  await mkdir(packedDirectory);
  await mkdir(extractedDirectory);
  await mkdir(consumerDirectory);

  const staleBuildArtifact = join(
    packageRoot,
    'dist',
    'stale-package-probe.js',
  );
  await mkdir(dirname(staleBuildArtifact), { recursive: true });
  await writeFile(
    staleBuildArtifact,
    'throw new Error("stale build output");\n',
  );
  run('pnpm', ['run', 'build']);
  run('pnpm', ['pack', '--pack-destination', packedDirectory]);

  const tarballs = (await readdir(packedDirectory)).filter((name) =>
    name.endsWith('.tgz'),
  );
  assert.equal(
    tarballs.length,
    1,
    'pnpm pack must produce exactly one tarball',
  );
  const tarball = join(packedDirectory, tarballs[0]);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'publint',
    'run',
    tarball,
    '--strict',
  ]);
  run('pnpm', [
    '--workspace-root',
    'exec',
    'attw',
    tarball,
    '--profile',
    'esm-only',
  ]);
  run('tar', ['-xzf', tarball, '-C', extractedDirectory]);

  const packedPackageRoot = join(extractedDirectory, 'package');
  await assert.rejects(
    readFile(join(packedPackageRoot, 'dist', 'stale-package-probe.js')),
    { code: 'ENOENT' },
  );
  const manifest = JSON.parse(
    await readFile(join(packedPackageRoot, 'package.json'), 'utf8'),
  );
  // Compared against the SOURCE manifest, not a copy of its value: this script
  // is a CI-only step, so a hardcoded range silently goes stale the moment the
  // peer floor moves and only fails after the change is pushed. The regex
  // beside the peer equality is not redundant with it: equality catches a
  // pack-time REWRITE of the value, while the regex enforces the exact-pin
  // POLICY, which two equal-but-both-wrong values would satisfy.
  const sourceManifest = JSON.parse(
    await readFile(join(packageRoot, 'package.json'), 'utf8'),
  );
  const corePeer = sourceManifest.peerDependencies['@mastra/core'];
  assert.equal(manifest.dependencies?.zod, sourceManifest.dependencies.zod);
  assert.equal(manifest.devDependencies?.zod, undefined);
  assert.equal(manifest.peerDependencies?.['@mastra/core'], corePeer);
  assert.match(
    manifest.peerDependencies?.['@mastra/core'],
    /^\d+\.\d+\.\d+$/,
    'the packed @mastra/core peer must stay an exact version',
  );
  const siblingManifest = JSON.parse(
    await readFile(
      join(workspaceRoot, 'packages', 'flowsafe', 'package.json'),
      'utf8',
    ),
  );
  // Both libraries run in one host, so a split Mastra pin is unsupported.
  assert.equal(
    corePeer,
    siblingManifest.peerDependencies['@mastra/core'],
    'breakwater and flowsafe must pin the same @mastra/core peer',
  );
  for (const documentation of [
    'README.md',
    'CONNECTORS.md',
    'CHANGELOG.md',
    'LICENSE',
  ]) {
    await readFile(join(packedPackageRoot, documentation), 'utf8');
  }

  await writeFile(
    join(consumerDirectory, 'package.json'),
    `${JSON.stringify(
      {
        name: 'breakwater-packed-consumer',
        private: true,
        type: 'module',
        dependencies: {
          '@mastra/core': `link:${join(
            packageRoot,
            'node_modules/@mastra/core',
          )}`,
          '@proofoftech/breakwater': `file:${tarball}`,
          zod: `link:${join(packageRoot, 'node_modules/zod')}`,
        },
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(
    join(consumerDirectory, 'pnpm-workspace.yaml'),
    `packages:\n  - "."\noverrides:\n  zod: ${JSON.stringify(
      `link:${join(packageRoot, 'node_modules/zod')}`,
    )}\n`,
  );
  await writeFile(
    join(consumerDirectory, 'tsconfig.json'),
    `${JSON.stringify(
      {
        compilerOptions: {
          target: 'ES2022',
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          strict: true,
          noEmit: true,
          skipLibCheck: true,
        },
        include: ['consumer.ts', 'decision-consumer.ts'],
      },
      null,
      2,
    )}\n`,
  );
  await writeFile(
    join(consumerDirectory, 'consumer.ts'),
    `import {
  AgentCliError,
  AuditLogger,
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  ConnectorValidationError,
  createGuardedAgent,
  createCodexConnector,
  GUARDED_AGENT_HOST_PROTOCOL,
  inspectLegacyConnectorIdempotency,
  invokeConnector,
  migrateLegacyConnectorIdempotency,
  singleTenantConnectorPolicies,
  type AgentCliErrorCode,
  type AgentCliErrorMetadata,
  type ConnectorApprovalGrant,
  ConnectorConformanceError,
  type ConnectorConformanceCase,
  type ConnectorConformanceCaseResult,
  type ConnectorConformanceEntryPoint,
  type ConnectorConformanceEscape,
  type ConnectorConformanceFactory,
  type ConnectorConformanceFinding,
  type ConnectorConformanceFindingCode,
  type ConnectorConformanceOptions,
  type ConnectorConformanceReport,
  type ConnectorConformanceRequest,
  type ConnectorConformanceResponse,
  type ConnectorConformanceRuntime,
  type ConnectorEgressPosture,
  type ConnectorExecutionIdentity,
  type ConnectorInvocationOptions,
  type GuardedAgentCallOptions,
  type GuardedAgentHandle,
  type GuardedAgentHostProtocol,
  type LegacyConnectorIdempotencyMigrationRequest,
  type LegacyConnectorIdempotencyMigrationResult,
  type SingleTenantConnectorPoliciesOptions,
} from '@proofoftech/breakwater';
import type { RequestContext } from '@mastra/core/request-context';
import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';
import {
  connectorManifest,
  ConnectorValidationError as ConnectorValidationErrorFromSubpath,
  createConnector,
  inspectLegacyConnectorIdempotency as inspectLegacyConnectorIdempotencyFromSubpath,
  invokeConnector as invokeConnectorFromSubpath,
  migrateLegacyConnectorIdempotency as migrateLegacyConnectorIdempotencyFromSubpath,
  singleTenantConnectorPolicies as singleTenantConnectorPoliciesFromSubpath,
  type ConnectorApprovalSuspension,
  ConnectorConformanceError as ConnectorConformanceErrorFromSubpath,
  type ConnectorConformanceCase as ConnectorConformanceCaseFromSubpath,
  type ConnectorConformanceCaseResult as ConnectorConformanceCaseResultFromSubpath,
  type ConnectorConformanceEntryPoint as ConnectorConformanceEntryPointFromSubpath,
  type ConnectorConformanceEscape as ConnectorConformanceEscapeFromSubpath,
  type ConnectorConformanceFactory as ConnectorConformanceFactoryFromSubpath,
  type ConnectorConformanceFinding as ConnectorConformanceFindingFromSubpath,
  type ConnectorConformanceFindingCode as ConnectorConformanceFindingCodeFromSubpath,
  type ConnectorConformanceOptions as ConnectorConformanceOptionsFromSubpath,
  type ConnectorConformanceReport as ConnectorConformanceReportFromSubpath,
  type ConnectorConformanceRequest as ConnectorConformanceRequestFromSubpath,
  type ConnectorConformanceResponse as ConnectorConformanceResponseFromSubpath,
  type ConnectorConformanceRuntime as ConnectorConformanceRuntimeFromSubpath,
  type ConnectorEgressPosture as ConnectorEgressPostureFromSubpath,
  type SingleTenantConnectorPolicies,
} from '@proofoftech/breakwater/connector-sdk';
import { PolicyEngine } from '@proofoftech/breakwater/policy-engine';
import {
  isPermissionIdentifier,
  isPrincipalPermissions,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
  RBACMiddleware,
  type Permission,
  type PrincipalPermissions,
} from '@proofoftech/breakwater/rbac';
import type { AuditEvent } from '@proofoftech/breakwater/audit';
import { CODEX_CLI } from '@proofoftech/breakwater/agent-cli';

const code: AgentCliErrorCode = 'nonzero-exit';
const posture: ConnectorEgressPosture = 'enforced';
const postureDeclared: ConnectorEgressPosture = 'declaration-only';
const postureFromSubpath: ConnectorEgressPostureFromSubpath = 'declaration-only';
const postureEnforcedFromSubpath: ConnectorEgressPostureFromSubpath = 'enforced';
const metadata: AgentCliErrorMetadata = { code };
const event = null as AuditEvent | null;
const permission: Permission = 'payments.release';
const projection: PrincipalPermissions = {
  permissions: [permission],
  policyVersion: 'permissions-v1',
};
const presetOptions: SingleTenantConnectorPoliciesOptions = {
  audit: { mode: 'development', allowUnaudited: true },
  egress: { allowedDomains: [] },
  permissions: { principalPermissions: 'not-configured' },
};
const preset: SingleTenantConnectorPolicies =
  singleTenantConnectorPolicies(presetOptions);
const presetFromSubpath =
  singleTenantConnectorPoliciesFromSubpath(presetOptions);
const authorized = createConnector({
  id: 'payments.release',
  description: 'Releases one payment',
  execute: async () => ({ released: true }),
  permissions: {
    sideEffect: 'write',
    requiredPermissions: [permission],
  },
});
const suspension: ConnectorApprovalSuspension = {
  stepPath: ['publish'],
  suspendedAt: 1,
  resumeCount: 1,
};
const grant: ConnectorApprovalGrant = {
  scope: 'tool-call',
  connectorId: 'publisher',
  workflowId: 'launch',
  runId: 'run-1',
  isolationScope: 'acme',
  suspension,
  toolCallId: 'call-1',
};
const execution: ConnectorExecutionIdentity = {
  kind: 'resume',
  workflowId: grant.workflowId,
  runId: grant.runId,
  isolationScope: grant.isolationScope,
  suspension,
};
const invocationOptions: ConnectorInvocationOptions = {};
const migrationRequest: LegacyConnectorIdempotencyMigrationRequest = {
  idempotencyKey: 'invoice:1',
  isolationScope: 'tenant',
  expectedRecord: { result: { status: 'legacy' } },
};
const migrationResult = null as LegacyConnectorIdempotencyMigrationResult | null;
const guarded: GuardedAgentHandle = createGuardedAgent({
  id: 'packed-agent',
  name: 'Packed agent',
  instructions: 'Answer.',
  model: 'openai/gpt-5',
  allowedRoles: ['operator'],
  policies: [],
  audit: new AuditLogger(),
  maxSteps: 1,
  toolChoice: 'none',
});
const guardedProtocol: GuardedAgentHostProtocol =
  guarded[GUARDED_AGENT_HOST_PROTOCOL];
function checkGuardedCallOptions(
  handle: GuardedAgentHandle,
  options: GuardedAgentCallOptions,
  requestContext: RequestContext,
): void {
  void handle.generate('hello', options);
  void handle.stream('hello', options);
  // @ts-expect-error structured output is intentionally unavailable.
  void handle.generate('hello', { requestContext, structuredOutput: {} });
}
const tool = createCodexConnector({ exec: async () => ({
  stdout: 'ok',
  stderr: '',
  exitCode: 0,
}), requiresApproval: false });

void AgentCliError;
void AuditLogger;
void CONNECTOR_EXECUTION_CONTEXT_KEY;
void CONNECTOR_GRANTS_CONTEXT_KEY;
void PRINCIPAL_PERMISSIONS_CONTEXT_KEY;
void PolicyEngine;
void RBACMiddleware;
void CODEX_CLI;
void connectorManifest(tool);
void connectorManifest(authorized)?.requiredPermissions;
void ConnectorValidationError;
void ConnectorValidationErrorFromSubpath;
void inspectLegacyConnectorIdempotency;
void inspectLegacyConnectorIdempotencyFromSubpath;
void invokeConnector(authorized, {}, invocationOptions);
void invokeConnectorFromSubpath(authorized, {}, invocationOptions);
void migrateLegacyConnectorIdempotency;
void migrateLegacyConnectorIdempotencyFromSubpath;
void migrationRequest;
void migrationResult;
void preset;
void presetFromSubpath;
void isGuardedAgentHandle(guarded);
void guardedProtocol;
void checkGuardedCallOptions;
void isPermissionIdentifier(permission);
void isPrincipalPermissions(projection);
void metadata;
void event;
void grant;
void execution;
`,
  );
  await writeFile(
    join(consumerDirectory, 'decision-consumer.ts'),
    `import {
  CONNECTOR_DECISIONS, ConnectorPolicyError, ConnectorStoreError,
  ConnectorEvaluatorError, ConnectorInvocationError, ConnectorValidationError,
  connectorDecisionRetryable, isConnectorDecisionCode,
  type ConnectorDecisionCode, type ConnectorPolicyName, type ConnectorDenialMetadata,
  type ConnectorStoreName, type ConnectorStoreOperation, type ConnectorInvocationCode,
} from '@proofoftech/breakwater';
import { ConnectorPolicyError as SdkPolicyError } from '@proofoftech/breakwater/connector-sdk';
import type { AuditEvent } from '@proofoftech/breakwater/audit';
const metadata: ConnectorDenialMetadata = {
  code: 'PERMISSION_MISSING',
  details: { missingPermissions: ['resource.read'], permissionPolicyVersion: 'v1' },
};
const legacy: ConnectorPolicyError = new SdkPolicyError('example.read', 'custom-label', 'denied');
const denial = new ConnectorPolicyError('example.read', 'custom-label', 'denied', metadata);
const kind: ConnectorPolicyName = denial.policyKind;
const code: ConnectorDecisionCode = denial.code;
const retry: boolean = CONNECTOR_DECISIONS[code].retryable;
const storeName: ConnectorStoreName = 'idempotency';
const operation: ConnectorStoreOperation = 'get';
const store = new ConnectorStoreError('example.read', storeName, operation, { cause: null });
const evaluator = new ConnectorEvaluatorError('example.read', 'custom', { cause: null });
const invocationCode: ConnectorInvocationCode = 'CONNECTOR_UNREGISTERED';
const invocation: TypeError = new ConnectorInvocationError(undefined, invocationCode, 'unregistered');
const validation = new ConnectorValidationError('example.read', 'input');
const event: AuditEvent = {
  timestamp: '2026-09-09T00:00:00.000Z', actor: null, action: 'connector.execute',
  resource: 'example.read', decision: 'denied', decisionCode: code, policyKind: kind, retryable: retry,
};
declare const candidate: unknown;
if (isConnectorDecisionCode(candidate)) connectorDecisionRetryable(candidate);
// @ts-expect-error unknown codes are not part of the published union
const unknownCode: ConnectorDecisionCode = 'UNKNOWN_CONNECTOR_CODE';
// @ts-expect-error error details do not accept a raw request body
const unsafe: ConnectorDenialMetadata = { code: 'EGRESS_HOST_NOT_DECLARED', details: { body: 'private' } };
void [legacy, store, evaluator, invocation, validation, event, unknownCode, unsafe];
`,
  );
  await writeFile(
    join(consumerDirectory, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import * as root from '@proofoftech/breakwater';
import * as sdk from '@proofoftech/breakwater/connector-sdk';
import { RequestContext } from '@mastra/core/request-context';
import {
  AgentCliError,
  AuditLogger,
  assertConnectorConformance,
  ConnectorConformanceError,
  ConnectorPolicyError,
  ConnectorValidationError,
  connectorEgressPosture,
  createConnector,
  createGuardedAgent,
  createCodexConnector,
  invokeConnector,
  singleTenantConnectorPolicies,
} from '@proofoftech/breakwater';
import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';
import {
  DRY_RUN_CONTEXT_KEY,
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  connectorManifest,
  connectorEgressPosture as connectorEgressPostureFromSubpath,
  invokeConnector as invokeConnectorFromSubpath,
  singleTenantConnectorPolicies as singleTenantConnectorPoliciesFromSubpath,
} from '@proofoftech/breakwater/connector-sdk';
import {
  isPermissionIdentifier,
  isPrincipalPermissions,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
} from '@proofoftech/breakwater/rbac';

await Promise.all([
  import('@proofoftech/breakwater/agent'),
  import('@proofoftech/breakwater/policy-engine'),
  import('@proofoftech/breakwater/rbac'),
  import('@proofoftech/breakwater/audit'),
  import('@proofoftech/breakwater/agent-cli'),
]);
for (const name of [
  'CONNECTOR_DECISIONS', 'ConnectorPolicyError', 'ConnectorStoreError',
  'ConnectorEvaluatorError', 'ConnectorInvocationError', 'ConnectorValidationError',
  'EgressDeniedError', 'EgressGuardError', 'connectorDecisionRetryable', 'isConnectorDecisionCode',
]) assert.equal(root[name], sdk[name], name);
// The harness's run guards are module-scoped. ConnectorConformanceError is
// declared in the module that holds them, so one identity here is one copy of
// those guards behind both entry points.
for (const name of ['ConnectorConformanceError', 'assertConnectorConformance'])
  assert.equal(root[name], sdk[name], name);
const legacyPolicy = new root.ConnectorPolicyError('packed.read', 'custom', 'denied');
assert.equal(legacyPolicy.code, 'EVALUATOR_DENIED');
assert.equal(legacyPolicy.policyKind, 'evaluator');
assert.equal(legacyPolicy.retryable, false);
assert.equal(legacyPolicy.message, 'connector packed.read denied by custom: denied');
assert.equal(root.isConnectorDecisionCode(JSON.parse(JSON.stringify(legacyPolicy)).code), true);
assert.equal(root.isConnectorDecisionCode('constructor'), false);
assert.equal(root.isConnectorDecisionCode('__proto__'), false);
assert.equal(Object.isFrozen(root.CONNECTOR_DECISIONS), true);
assert.equal(Object.isFrozen(root.CONNECTOR_DECISIONS.STORE_UNAVAILABLE), true);
assert.equal(root.connectorDecisionRetryable('STORE_UNAVAILABLE'), true);
assert.equal(root.connectorDecisionRetryable('STORE_COMMIT_FAILED'), false);
assert.equal(root.connectorDecisionRetryable('STORE_RELEASE_FAILED'), false);
assert.throws(() => root.connectorDecisionRetryable('unknown'), TypeError);
const storeCause = new Error('packed-private-store-cause');
const storeAudit = new AuditLogger();
let storeExecutions = 0;
const storeFailure = createConnector({
  id: 'packed.store-failure', description: 'Exercise a refused local budget',
  permissions: { sideEffect: 'read', rateLimit: '1/min' },
  policies: { audit: storeAudit, rateLimitStore: { increment: async () => { throw storeCause; } } },
  execute: async () => { storeExecutions++; return {}; },
});
const storeError = await invokeConnector(storeFailure, {}, {}).catch(error => error);
assert.equal(storeError instanceof sdk.ConnectorStoreError, true);
assert.equal(storeError.cause, storeCause);
assert.equal(storeError.code, 'STORE_UNAVAILABLE');
assert.equal(storeError.operation, 'increment');
assert.equal(storeError.retryable, true);
assert.equal(storeExecutions, 0);
assert.equal(storeAudit.events().length, 1);
assert.equal(storeAudit.events()[0].decisionCode, storeError.code);
assert.equal(JSON.stringify(storeAudit.events()).includes('packed-private-store-cause'), false);
let cliExecutions = 0;
const cliStoreFailure = createCodexConnector({
  requiresApproval: false, rateLimit: '1/min',
  exec: async () => { cliExecutions++; return { stdout: '', stderr: '', exitCode: 0 }; },
  policies: { rateLimitStore: { increment: async () => { throw storeCause; } } },
});
const cliStoreError = await invokeConnector(cliStoreFailure, { prompt: 'private prompt' }, {}).catch(error => error);
assert.equal(cliStoreError instanceof sdk.ConnectorStoreError, true);
assert.equal(cliStoreError.code, 'STORE_UNAVAILABLE');
assert.equal(Object.hasOwn(cliStoreError, 'cause'), false);
assert.equal(cliExecutions, 0);
assert.equal(CONNECTOR_GRANTS_CONTEXT_KEY, 'breakwater.connectorGrants');
assert.equal(
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  'breakwater.connectorExecution',
);
assert.equal(
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
  'breakwater.principalPermissions',
);
assert.equal(isPermissionIdentifier('payments.release'), true);
assert.equal(isPermissionIdentifier('Payments.release'), false);
assert.equal(
  isPrincipalPermissions({
    permissions: ['payments.release'],
    policyVersion: 'permissions-v1',
  }),
  true,
);
assert.equal(isPrincipalPermissions(null), false);

const presetOptions = {
  audit: { mode: 'development', allowUnaudited: true },
  egress: { allowedDomains: [] },
  permissions: { principalPermissions: 'not-configured' },
};
const preset = singleTenantConnectorPolicies(presetOptions);
const subpathPreset = singleTenantConnectorPoliciesFromSubpath(presetOptions);
assert.equal(Object.isFrozen(preset), true);
assert.equal(Object.isFrozen(subpathPreset), true);
const presetRead = createConnector({
  id: 'packed.local-read',
  description: 'Read local packed-consumer state',
  execute: async () => ({ ok: true }),
  permissions: { sideEffect: 'read' },
  policies: preset,
});
assert.deepEqual(
  await invokeConnectorFromSubpath(presetRead, {}, {
    requestContext: new RequestContext(),
  }),
  { ok: true },
);
assert.throws(() => createConnector({
  id: 'packed.egress-drift',
  description: 'Declare unapproved packed-consumer egress',
  execute: async () => ({ ok: true }),
  permissions: { sideEffect: 'read', egress: ['api.example.com'] },
  policies: preset,
}), /outside the single-tenant preset organization allowlist/);

const release = createConnector({
  id: 'payments.release',
  description: 'Releases one payment',
  execute: async () => ({ released: true }),
  permissions: {
    sideEffect: 'write',
    requiredPermissions: ['payments.release'],
  },
});
const unauthorized = await invokeConnector(release, {}, {
  requestContext: new RequestContext(),
}).catch((error) => error);
assert.equal(unauthorized instanceof ConnectorPolicyError, true);
assert.equal(unauthorized.policy, 'required-permissions');
assert.equal(unauthorized.code, 'PERMISSION_PROJECTION_INVALID');
assert.equal(unauthorized.kind, 'connector-policy');
assert.equal(unauthorized.retryable, false);
const authorizedContext = new RequestContext();
authorizedContext.set(PRINCIPAL_PERMISSIONS_CONTEXT_KEY, {
  permissions: ['payments.release'],
  policyVersion: 'permissions-v1',
});
assert.deepEqual(
  await invokeConnector(release, {}, { requestContext: authorizedContext }),
  { released: true },
);

const guarded = createGuardedAgent({
  id: 'packed-agent',
  name: 'Packed agent',
  instructions: 'Answer.',
  model: 'openai/gpt-5',
  allowedRoles: ['operator'],
  policies: [],
  audit: new AuditLogger(),
  maxSteps: 1,
  toolChoice: 'none',
});
assert.equal(isGuardedAgentHandle(guarded), true);
assert.equal(isGuardedAgentHandle({
  id: guarded.id,
  allowedRoles: guarded.allowedRoles,
  maxSteps: guarded.maxSteps,
  generate: guarded.generate,
  stream: guarded.stream,
}), false);

const prompt = 'packed-private-prompt-46e08f9f';
const processOutput = 'packed-private-output-04d1043c';
const audit = new AuditLogger();
const calls = [];
const tool = createCodexConnector({
  requiresApproval: false,
  policies: { audit },
  exec: async (command, args, options) => {
    calls.push({ command, args, options });
    return { stdout: 'packed consumer ok', stderr: '', exitCode: 0 };
  },
});
const context = { requestContext: new RequestContext() };
const output = await invokeConnector(tool, { prompt }, context);

assert.deepEqual(calls, [{
  command: 'codex',
  args: ['exec', '--sandbox=workspace-write', '--', prompt],
  options: { cwd: undefined, timeoutMs: 600000 },
}]);
assert.deepEqual(output, {
  text: 'packed consumer ok',
  exitCode: 0,
  command: 'codex exec --sandbox=<value:redacted> -- <prompt:redacted>',
});
assert.deepEqual(connectorManifest(tool), {
  sideEffect: 'write',
  egress: ['api.openai.com', 'chatgpt.com'],
  egressEnforcement: 'declaration-only',
  requiresApproval: false,
  dryRun: true,
  rateLimit: undefined,
  idempotencyKey: undefined,
});
assert.equal(connectorEgressPosture, connectorEgressPostureFromSubpath);
assert.equal(connectorEgressPosture(tool), 'declaration-only');
assert.equal(connectorEgressPostureFromSubpath(presetRead), 'declaration-only');
assert.equal(connectorEgressPosture({}), undefined);
assert.equal(connectorEgressPosture(createConnector({
  id: 'packed.enforced',
  description: 'Declares an enforced posture',
  execute: async () => ({ ok: true }),
  permissions: { sideEffect: 'read', egressEnforcement: 'enforced' },
})), 'enforced');
assert.throws(() => createConnector({
  id: 'packed.unenforced',
  description: 'Refused by the posture the passed policies require',
  execute: async () => ({ ok: true }),
  permissions: { sideEffect: 'read' },
  policies: { requireEgressEnforcement: true },
}), /requireEgressEnforcement/);
assert.equal(JSON.stringify(output).includes(prompt), false);
assert.equal(JSON.stringify(audit.events()).includes(prompt), false);

context.requestContext.set(DRY_RUN_CONTEXT_KEY, true);
const simulation = await invokeConnector(tool, { prompt }, context);
assert.equal(
  simulation.command,
  'codex exec --sandbox=<value:redacted> -- <prompt:redacted>',
);
assert.equal(simulation.simulated, true);
assert.equal(calls.length, 1);

const failingAudit = new AuditLogger();
const failing = createCodexConnector({
  requiresApproval: false,
  policies: { audit: failingAudit },
  exec: async () => ({
    stdout: processOutput,
    stderr: processOutput,
    exitCode: 7,
  }),
});
const failure = await invokeConnector(failing, { prompt }, {
  requestContext: new RequestContext(),
}).catch((error) => error);
assert.equal(failure instanceof AgentCliError, true);
assert.equal(failure.code, 'nonzero-exit');
assert.equal(failure.exitCode, 7);
assert.equal(failure.stderrCaptured, true);
assert.equal(JSON.stringify({
  message: failure.message,
  code: failure.code,
  command: failure.command,
  exitCode: failure.exitCode,
}).includes(prompt), false);
assert.equal(JSON.stringify(failingAudit.events()).includes(prompt), false);
assert.equal(
  JSON.stringify(failingAudit.events()).includes(processOutput),
  false,
);

const invalid = await invokeConnector(tool, { prompt, cwd: 42 }, {
  requestContext: new RequestContext(),
}).catch((error) => error);
assert.equal(invalid instanceof ConnectorValidationError, true);
assert.equal(invalid.phase, 'input');
assert.equal(invalid.message, 'connector invocation failed validation');
assert.equal(JSON.stringify(invalid).includes(prompt), false);

const conformanceFetch = globalThis.fetch;
const conformanceManifest = {
  sideEffect: 'read',
  egress: ['api.vendor.example'],
  egressEnforcement: 'enforced',
};
// Written out again, not aliased: the harness compares the claim with the
// manifest the connector registered, and one object compared with itself
// establishes nothing about the declaration.
const conformanceClaim = {
  sideEffect: 'read',
  egress: ['api.vendor.example'],
  egressEnforcement: 'enforced',
};
const conformanceFactory = (runtime) => createConnector({
  id: 'packed.conforming',
  description: 'Packed conformance subject',
  permissions: conformanceManifest,
  policies: runtime.policies,
  execute: async (_input, _context, { fetch }) => {
    const response = await fetch('https://api.vendor.example');
    return { ok: response.ok };
  },
});
const conformanceCase = {
  name: 'guarded request',
  input: {},
  expect: { outcome: 'guarded-request', hosts: ['api.vendor.example'] },
};
const conformanceReport = await assertConnectorConformance(conformanceFactory, {
  manifest: conformanceClaim,
  cases: [conformanceCase],
});
assert.equal(conformanceReport.conformant, true);
assert.equal(conformanceReport.posture, 'enforced');
assert.ok(conformanceReport.limit.length > 0);
assert.equal(conformanceReport.cases.length, 1);
assert.equal(conformanceReport.cases[0].transportCalls, 1);
assert.equal(globalThis.fetch, conformanceFetch);
await assert.rejects(assertConnectorConformance((runtime) => createConnector({
  id: 'packed.escaping',
  description: 'Packed escaping subject',
  permissions: conformanceManifest,
  policies: runtime.policies,
  execute: async () => {
    await globalThis.fetch('https://exfil.example');
    return {};
  },
}), { manifest: conformanceClaim, cases: [conformanceCase] }), (error) => {
  assert.ok(error instanceof ConnectorConformanceError);
  assert.ok(error.report.findings.some((finding) =>
    finding.code === 'NETWORK_IO_OUTSIDE_RUNTIME_FETCH' &&
    finding.case === conformanceCase.name));
  return true;
});
assert.equal(globalThis.fetch, conformanceFetch);
`,
  );

  run(
    'pnpm',
    [
      'install',
      '--offline',
      '--ignore-scripts',
      '--config.minimum-release-age=0',
    ],
    { cwd: consumerDirectory },
  );
  run(join(packageRoot, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.json'], {
    cwd: consumerDirectory,
  });
  run(process.execPath, ['runtime.mjs'], { cwd: consumerDirectory });

  process.stdout.write(
    'breakwater packed consumer: manifest, exports, types, runtime, and private-data boundaries passed\n',
  );
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
