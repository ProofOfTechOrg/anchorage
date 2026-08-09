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
  run('tar', ['-xzf', tarball, '-C', extractedDirectory]);

  const packedPackageRoot = join(extractedDirectory, 'package');
  const manifest = JSON.parse(
    await readFile(join(packedPackageRoot, 'package.json'), 'utf8'),
  );
  assert.equal(manifest.dependencies?.zod, '^4.4.3');
  assert.equal(manifest.devDependencies?.zod, undefined);
  assert.equal(manifest.peerDependencies?.['@mastra/core'], '^1.50.0');
  assert.equal(manifest.engines?.node, '>=22');
  assert.equal(manifest.type, 'module');

  for (const [subpath, target] of Object.entries(manifest.exports)) {
    if (subpath === './package.json') continue;
    assert.equal(
      typeof target,
      'string',
      `export ${subpath} must resolve to a single JavaScript file`,
    );
    await readFile(join(packedPackageRoot, target), 'utf8');
    await readFile(
      join(packedPackageRoot, target.replace(/\.js$/u, '.d.ts')),
      'utf8',
    );
  }
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
  createGuardedAgent,
  createCodexConnector,
  type AgentCliErrorCode,
  type AgentCliErrorMetadata,
  type ConnectorApprovalGrant,
  type ConnectorExecutionIdentity,
  type GuardedAgentHandle,
} from '@proofoftech/breakwater';
import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';
import {
  connectorManifest,
  createConnector,
  type ConnectorApprovalSuspension,
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
void isGuardedAgentHandle(guarded);
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
  createConnector,
  createGuardedAgent,
  createCodexConnector,
} from '@proofoftech/breakwater';
import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';
import {
  DRY_RUN_CONTEXT_KEY,
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  connectorManifest,
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

const release = createConnector({
  id: 'payments.release',
  description: 'Releases one payment',
  execute: async () => ({ released: true }),
  permissions: {
    sideEffect: 'write',
    requiredPermissions: ['payments.release'],
  },
});
const unauthorized = await release.execute({}, {
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
  await release.execute({}, { requestContext: authorizedContext }),
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
const output = await tool.execute({ prompt }, context);

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
const simulation = await tool.execute({ prompt }, context);
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
const failure = await failing.execute({ prompt }, {
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

const invalid = await tool.execute({ prompt, cwd: 42 }, {
  requestContext: new RequestContext(),
});
assert.equal(invalid.error, true);
assert.equal(invalid.message, 'Agent CLI input or output validation failed.');
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
