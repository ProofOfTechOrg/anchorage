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
        include: ['consumer.ts'],
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
    join(consumerDirectory, 'runtime.mjs'),
    `import assert from 'node:assert/strict';
import { RequestContext } from '@mastra/core/request-context';
import {
  AgentCliError,
  AuditLogger,
  ConnectorPolicyError,
  ConnectorValidationError,
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
  requiresApproval: false,
  dryRun: true,
  rateLimit: undefined,
  idempotencyKey: undefined,
});
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
