import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { AuditLogger } from '../audit/index.js';
import {
  crossWorkflowIsolation,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from '../policy-engine/index.js';
import { ACTOR_CONTEXT_KEY, type Actor } from '../rbac/index.js';
import {
  APPROVED_CONNECTORS_CONTEXT_KEY,
  type AtomicIdempotencyStore,
  connectorManifest,
  ConnectorPolicyError,
  createConnector,
  DRY_RUN_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  type IdempotencyRecord,
  type IdempotencyReservation,
  type IdempotencyStore,
  InMemoryIdempotencyStore,
  InMemoryRateLimitStore,
} from './index.js';

function makeContext(
  options: {
    actor?: Actor;
    approved?: readonly string[];
    idempotencyKey?: string;
    agent?: boolean;
    dryRun?: boolean;
  } = {},
): ToolExecutionContext {
  const requestContext = new RequestContext();
  if (options.actor) requestContext.set(ACTOR_CONTEXT_KEY, options.actor);
  if (options.approved) {
    requestContext.set(APPROVED_CONNECTORS_CONTEXT_KEY, options.approved);
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

// Structural on purpose: connectors infer different TOutput per test, and
// Tool<...> instantiations don't cross-assign cleanly.
async function run(
  tool: {
    execute?: (
      inputData: unknown,
      context: ToolExecutionContext,
    ) => Promise<unknown>;
  },
  input: unknown,
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
              throw new Error('policy backend down');
            },
          },
        ],
      },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow('policy backend down');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { policy: 'flaky' } },
    ]);
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
    // #when
    const denied = await run(
      composite,
      input,
      makeContext({ agent: true }),
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
    const store = {
      get: vi.fn(
        () =>
          new Promise<IdempotencyRecord | undefined>((resolve) => {
            settleGet = resolve;
          }),
      ),
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
        throw new Error('d1 write failed');
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
      { decision: 'error', detail: { stage: 'idempotency-store' } },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
      { decision: 'error', detail: { stage: 'idempotency-store' } },
      { decision: 'allowed', detail: { idempotencyKey: 'k1' } },
    ]);
  });

  it('fails closed when the store get fails', async () => {
    // #given
    const audit = new AuditLogger();
    const store: IdempotencyStore = {
      get: () => {
        throw new Error('d1 read failed');
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
    ).rejects.toThrow('d1 read failed');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'idempotency-store' } },
    ]);
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
    expect(store.reserve).toHaveBeenCalledWith('salesforce.createContact:k1');
    expect(store.put).toHaveBeenCalledTimes(1);
    expect(store.get).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(1);
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
    expect(store.release).toHaveBeenCalledWith('salesforce.createContact:k1');
    expect(
      await run(tool, input, makeContext({ idempotencyKey: 'k1' })),
    ).toEqual({ ok: true });
    expect(execute).toHaveBeenCalledTimes(2);
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
    expect(store.release).toHaveBeenCalledWith('salesforce.createContact:k2');
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
    expect(store.release).toHaveBeenCalledWith('salesforce.createContact:k1');
    // #then — exactly ONE audit record, attributed to the rate-limit store;
    // never a second, false 'execute threw' record
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
        throw new Error('d1 claim failed');
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
    ).rejects.toThrow('d1 claim failed');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'idempotency-store' } },
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
    settleReserve().resolve({ state: 'reserved' });
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

  it.each(['nope', '0/min', '2/mins', '2 / min', '2/minutes', '/min', '2/'])(
    "rejects malformed rate limit '%s' at definition time",
    (rateLimit) => {
      // #when / #then
      expect(() =>
        makeConnector({
          permissions: { sideEffect: 'write', rateLimit },
          policies: { rateLimitStore: new InMemoryRateLimitStore() },
        }),
      ).toThrow(TypeError);
    },
  );

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
            throw new Error('counter backend down');
          },
        },
        audit,
      },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow('counter backend down');
    expect(execute).not.toHaveBeenCalled();
    expect(audit.events()).toMatchObject([
      { decision: 'error', detail: { stage: 'rate-limit-store' } },
    ]);
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
    expect(store.reserve('k1')).toEqual({ state: 'reserved' });
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
    expect(store.reserve('k1')).toEqual({ state: 'reserved' });
    // #when
    store.release('k1');
    // #then
    expect(store.reserve('k1')).toEqual({ state: 'reserved' });
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
    const execute = vi.fn(async () => {
      throw new Error('boom');
    });
    const tool = createConnector({
      id: 'salesforce.createContact',
      description: 'Create a Salesforce contact',
      execute,
      permissions: { sideEffect: 'write' },
      policies: { audit },
    });
    // #when / #then
    await expect(run(tool, input)).rejects.toThrow('boom');
    expect(audit.events()).toMatchObject([
      {
        decision: 'error',
        reason: 'execute threw: boom',
        detail: { stage: 'execute' },
      },
    ]);
  });
});
