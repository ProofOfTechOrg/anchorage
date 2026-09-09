// SPDX-License-Identifier: Apache-2.0
// createScheduleRouter: bounded ingestion gate order, no-oracle 404s, count and
// fire-rate caps, reserved-context rejection, and audit
// coverage (accept + every post-auth denial; benign GET + pre-auth NOT audited).

import type {
  Schedule,
  ScheduleTrigger,
  ScheduleUpdate,
} from '@mastra/core/storage';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  createActorResolver,
  D1ApprovalStoreFactory,
  type ResourceOwner,
} from '../approval-api/index.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFencedError,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
  InvalidMutationEpochError,
  MutationEpochMismatchError,
} from '../do-runner/index.js';
import { RunRouteError } from '../host-kit/index.js';
import {
  FENCED_SCHEDULE_STORAGE,
  type FencedScheduleMutationCapability,
  ScheduleMutationConflictError,
  ScheduleMutationOutcomeUnknownError,
} from './mutation-contract.js';
import {
  createScheduleRouter as createScheduleRouterImpl,
  type ScheduleFacadeStore,
  type ScheduleRouteAuditEvent,
  type ScheduleRouter,
  type ScheduleRouterOptions,
} from './router.js';
import { D1SchedulesStorage, type ScheduleDatabase } from './schedules-d1.js';
import type { ScheduleTargetPolicy } from './target-policy.js';
import {
  createScheduleTargetPolicy,
  scheduleWithCreatorRole,
} from './target-policy.js';

afterEach(() => vi.restoreAllMocks());

const TARGET_POLICY = createScheduleTargetPolicy({
  workflows: [{ id: 'wf' }],
  agents: [
    {
      id: 'a1',
      allowedAutomation: [{ kind: 'system', entryPaths: ['schedule.fire'] }],
    },
  ],
});

describe('schedule target policy catalog', () => {
  it.each([
    ['workflow', { workflows: [{ id: 'wf' }, { id: 'wf' }], agents: [] }],
    ['agent', { workflows: [], agents: [{ id: 'a1' }, { id: 'a1' }] }],
  ] as const)('rejects duplicate %s ids', (kind, options) => {
    expect(() => createScheduleTargetPolicy(options)).toThrow(
      `duplicate ${kind} target id`,
    );
  });

  it.each([
    ['workflow', { workflows: [{ id: 'wf/unsafe' }], agents: [] }],
    [
      'agent',
      {
        workflows: [],
        agents: [{ id: 123 as unknown as string }],
      },
    ],
  ] as const)('rejects non-path-safe %s ids', (kind, options) => {
    expect(() => createScheduleTargetPolicy(options)).toThrow(
      `${kind} target id must be path-safe`,
    );
  });
});

function createScheduleRouter(
  options: Omit<
    ScheduleRouterOptions,
    'targetPolicy' | 'validateThreadTarget'
  > & {
    targetPolicy?: ScheduleTargetPolicy;
    validateThreadTarget?: ScheduleRouterOptions['validateThreadTarget'];
  },
) {
  return createScheduleRouterImpl({
    ...options,
    targetPolicy: options.targetPolicy ?? TARGET_POLICY,
    validateThreadTarget:
      options.validateThreadTarget ?? (async () => undefined),
  });
}

/** An in-memory facade store. */
class MemStore implements ScheduleFacadeStore {
  readonly m = new Map<string, Schedule>();
  readonly owners = new Map<string, ResourceOwner>();
  readonly triggers: ScheduleTrigger[] = [];

  async createOwnedSchedule(
    schedule: Schedule,
    owner: ResourceOwner,
    maxSchedules: number,
  ): Promise<Schedule | null> {
    if (this.m.size >= maxSchedules) return null;
    if (this.m.has(schedule.id)) throw new Error('exists');
    this.m.set(schedule.id, schedule);
    this.owners.set(schedule.id, owner);
    return schedule;
  }
  async getSchedule(id: string): Promise<Schedule | null> {
    return this.m.get(id) ?? null;
  }
  async listSchedules(): Promise<Schedule[]> {
    return [...this.m.values()];
  }
  async updateSchedule(id: string, patch: ScheduleUpdate): Promise<Schedule> {
    const s = this.m.get(id);
    if (!s) throw new Error('not found');
    const next = { ...s, ...patch, updatedAt: Date.now() };
    this.m.set(id, next);
    return next;
  }
  async deleteOwnedSchedule(id: string): Promise<'deleted' | 'pending'> {
    this.m.delete(id);
    this.owners.delete(id);
    return 'deleted';
  }
  async listTriggers(scheduleId: string): Promise<ScheduleTrigger[]> {
    return this.triggers.filter((t) => t.scheduleId === scheduleId);
  }
}

function ctx(
  actorLabel: string,
  role: ApprovalRole,
  canAccess: ActorContext['canAccessResource'] = async () => true,
  releaseResource: ActorContext['releaseResource'] = async () => undefined,
): ActorContext {
  return {
    actor: { id: `${role}-${actorLabel}`, role },
    principal: { kind: 'human', id: `${role}-${actorLabel}`, role },
    resourceOwner: { kind: 'human', id: `${role}-${actorLabel}` },
    service: () => {
      throw new Error('approval service is not used in schedule tests');
    },
    newRunId: () => `run-${actorLabel}`,
    newThreadId: () => `thread-${actorLabel}`,
    resourceIdFromKey: (key) => key,
    claimResource: async () => undefined,
    releaseResource,
    resourceOwnerFor: async () => ({
      kind: 'human',
      id: `${role}-${actorLabel}`,
    }),
    canAccessResource: canAccess,
    canSelfDecide: () => false,
  };
}

function resolveAs(context: ActorContext | undefined): ActorResolver {
  return async () => context;
}

interface Harness {
  store: MemStore;
  events: ScheduleRouteAuditEvent[];
  call: (
    method: string,
    path: string,
    body?: unknown,
  ) => Promise<{ status: number; body: Record<string, unknown> }>;
}

function routerCaller(router: ScheduleRouter): Harness['call'] {
  return async (method, path, body) => {
    const res = await router(
      new Request(`http://host${path}`, {
        method,
        ...(body !== undefined
          ? {
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify(body),
            }
          : {}),
      }),
    );
    if (!res) throw new Error(`router returned null for ${method} ${path}`);
    return {
      status: res.status,
      body: (await res.json()) as Record<string, unknown>,
    };
  };
}

function harness(
  context: ActorContext | undefined,
  overrides: {
    resolve?: ActorResolver;
    maxSchedules?: number;
    minFireIntervalMs?: number;
    maxContentBytes?: number;
    targetPolicy?: ScheduleTargetPolicy;
    audit?: ScheduleRouterOptions['audit'];
    validateThreadTarget?: ScheduleRouterOptions['validateThreadTarget'];
  } = {},
): Harness {
  const store = new MemStore();
  const events: ScheduleRouteAuditEvent[] = [];
  const router = createScheduleRouter({
    resolve: overrides.resolve ?? resolveAs(context),
    store,
    audit:
      overrides.audit ??
      ((e) => {
        events.push(e);
      }),
    ...(overrides.maxSchedules !== undefined
      ? { maxSchedules: overrides.maxSchedules }
      : {}),
    ...(overrides.minFireIntervalMs !== undefined
      ? { minFireIntervalMs: overrides.minFireIntervalMs }
      : {}),
    ...(overrides.maxContentBytes !== undefined
      ? { maxContentBytes: overrides.maxContentBytes }
      : {}),
    ...(overrides.targetPolicy !== undefined
      ? { targetPolicy: overrides.targetPolicy }
      : {}),
    ...(overrides.validateThreadTarget !== undefined
      ? { validateThreadTarget: overrides.validateThreadTarget }
      : {}),
    executionFence: 'none',
  });
  const call = routerCaller(router);
  return { store, events, call };
}

const WORKFLOW_CREATE = {
  workflowId: 'wf',
  cron: '*/5 * * * *',
  inputData: { topic: 'x' },
};

describe('createScheduleRouter — gate order', () => {
  it('returns null for a non-schedules path (composes ahead of other routers)', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('acme', 'operator')),
      store: new MemStore(),
      executionFence: 'none',
    });
    expect(await router(new Request('http://host/api/other'))).toBeNull();
    // sanity: our own base IS handled
    const res = await call('GET', '/api/schedules');
    expect(res.status).toBe(200);
  });

  it('is route-absent on a malformed percent-encoded id (no pre-auth URIError)', async () => {
    // A lone '%' in the schedule-id segment — bare decodeURIComponent would THROW
    // out of the handler BEFORE auth; safeDecodeSegment makes it route-absent.
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('acme', 'operator')),
      store: new MemStore(),
      executionFence: 'none',
    });
    const res = await router(
      new Request('http://host/api/schedules/%', { method: 'GET' }),
    );
    expect(res).toBeNull();
  });

  it('401s an unauthenticated request (resolve -> undefined), not audited', async () => {
    const { call, events } = harness(undefined);
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(401);
    expect(events).toEqual([]);
  });

  it('403s a pre-auth resolver throw (ActorResolutionError), not audited', async () => {
    const { call, events } = harness(undefined, {
      resolve: async () => {
        throw new ActorResolutionError('bad token');
      },
    });
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(403);
    expect(events).toEqual([]);
  });

  it('403s a MUTATION by a non-RUN_START role (viewer) and audits the denial', async () => {
    const { call, events } = harness(ctx('acme', 'viewer'));
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(403);
    expect(events).toContainEqual(
      expect.objectContaining({
        operation: 'create',
        outcome: 'rejected',
        reason: 'forbidden-role',
      }),
    );
  });

  it('allows a viewer to READ (list) — reads are coarse, not role-gated', async () => {
    const { call } = harness(ctx('acme', 'viewer'));
    const res = await call('GET', '/api/schedules');
    expect(res.status).toBe(200);
    expect(res.body.schedules).toEqual([]);
  });
});

describe('createScheduleRouter — create', () => {
  it('creates a deployment schedule with a server-minted id, audited accepted', async () => {
    const { call, store, events } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(201);
    const schedule = res.body.schedule as { id: string; workflowId: string };
    expect(schedule.id).toMatch(/^schedule_[0-9a-f]{8}-/);
    expect(schedule.workflowId).toBe('wf');
    const stored = store.m.get(schedule.id);
    expect(stored?.metadata).toBeUndefined();
    expect(events).toContainEqual(
      expect.objectContaining({ operation: 'create', outcome: 'accepted' }),
    );
  });

  it('returns the committed schedule when the accepted audit sink fails', async () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { call, store } = harness(ctx('acme', 'operator'), {
      audit: async () => {
        throw new Error('audit unavailable');
      },
    });

    const result = await call('POST', '/api/schedules', WORKFLOW_CREATE);

    expect(result.status).toBe(201);
    const schedule = result.body.schedule as { id: string };
    expect(store.m.has(schedule.id)).toBe(true);
    expect(store.owners.has(schedule.id)).toBe(true);
    expect(logged).toHaveBeenCalledWith(
      expect.stringContaining('schedule.route-audit-error'),
      expect.objectContaining({ message: 'audit unavailable' }),
    );
    logged.mockRestore();
  });

  it('creates an agent schedule with the agent_ prefix (CRUD ships; firing is guarded off in the tick)', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
    });
    expect(res.status).toBe(201);
    expect((res.body.schedule as { id: string }).id).toMatch(/^agent_/);
  });

  it('rejects an unknown target before creating either domain or owner state', async () => {
    const { call, store, events } = harness(ctx('acme', 'operator'));

    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      workflowId: 'missing-workflow',
    });

    expect(res.status).toBe(404);
    expect(store.m.size).toBe(0);
    expect(store.owners.size).toBe(0);
    expect(events).toContainEqual(
      expect.objectContaining({ reason: 'unknown-target' }),
    );
  });

  it('rejects a creator whose role is forbidden by the workflow catalog', async () => {
    const policy = createScheduleTargetPolicy({
      workflows: [{ id: 'wf', allowedRoles: ['admin'] }],
      agents: [],
    });
    const { call, store, events } = harness(ctx('acme', 'operator'), {
      targetPolicy: policy,
    });

    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);

    expect(res.status).toBe(403);
    expect(store.m.size).toBe(0);
    expect(store.owners.size).toBe(0);
    expect(events).toContainEqual(
      expect.objectContaining({ reason: 'target-role-forbidden' }),
    );
  });

  it('rejects an agent that has not allowed schedule automation', async () => {
    const policy = createScheduleTargetPolicy({
      workflows: [],
      agents: [{ id: 'a1' }],
    });
    const { call, store, events } = harness(ctx('acme', 'operator'), {
      targetPolicy: policy,
    });

    const res = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
    });

    expect(res.status).toBe(403);
    expect(store.m.size).toBe(0);
    expect(store.owners.size).toBe(0);
    expect(events).toContainEqual(
      expect.objectContaining({ reason: 'automation-forbidden' }),
    );
  });

  it('rejects a threaded target owned by a different principal before create', async () => {
    const context: ActorContext = {
      ...ctx('admin', 'admin'),
      resourceOwnerFor: async () => ({ kind: 'human', id: 'other-user' }),
    };
    const { call, store } = harness(context);

    const res = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'thread-foreign',
      resourceId: 'resource-foreign',
    });

    expect(res.status).toBe(404);
    expect(store.m.size).toBe(0);
    expect(store.owners.size).toBe(0);
  });

  it('refuses to attach a schedule to an owned but unbound ephemeral thread', async () => {
    const validateThreadTarget = vi.fn(async () => {
      throw new RunRouteError(404, 'agent not found');
    });
    const { call, store } = harness(ctx('acme', 'operator'), {
      validateThreadTarget,
    });

    const response = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
    });

    expect(response.status).toBe(404);
    expect(validateThreadTarget).toHaveBeenCalledWith(expect.anything(), {
      agentId: 'a1',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
    });
    expect(store.m.size).toBe(0);
    expect(store.owners.size).toBe(0);
  });

  it('normalizes a valid threaded agent target and strips unsupported nested fields', async () => {
    const { call, store } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      ifIdle: {
        streamOptions: {
          requestContext: { safe: 'kept' },
          temperature: 0.5,
          unsupported: 'dropped',
        },
        unsupported: 'dropped',
      },
    });

    expect(res.status).toBe(201);
    const id = (res.body.schedule as { id: string }).id;
    expect(store.m.get(id)?.target).toMatchObject({
      type: 'agent',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      ifIdle: {
        streamOptions: {
          requestContext: { safe: 'kept' },
        },
      },
    });
    expect(JSON.stringify(store.m.get(id)?.target)).not.toContain(
      'unsupported',
    );
  });

  it.each([
    [{ resourceId: 'acme_resource' }, 'resourceId'],
    [{ signalType: 'ping' }, 'signalType'],
    [{ ifActive: { strategy: 'join' } }, 'ifActive'],
    [{ ifIdle: { strategy: 'start' } }, 'ifIdle'],
  ])('rejects threadless agent option %s', async (extra, expectedField) => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      ...extra,
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toContain(expectedField);
  });

  it('requires both path-safe memory ids for a threaded agent schedule', async () => {
    const { call, events } = harness(ctx('acme', 'operator'));
    expect(
      (
        await call('POST', '/api/schedules', {
          agentId: 'a1',
          prompt: 'go',
          cron: '*/5 * * * *',
          threadId: 'acme_thread',
        })
      ).status,
    ).toBe(400);
    const pathSafe = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'acme_thread',
      resourceId: 'other_resource',
    });
    expect(pathSafe.status).toBe(201);
    expect(events).toContainEqual(
      expect.objectContaining({ operation: 'create', outcome: 'accepted' }),
    );
  });

  it('rejects agent top-level requestContext and malformed metadata', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const context = await call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      requestContext: { safe: true },
    });
    expect(context.status).toBe(400);
    expect(context.body.error).toMatch(/requestContext/);
    const metadata = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      metadata: [],
    });
    expect(metadata.status).toBe(400);
    expect(metadata.body.error).toMatch(/metadata/);
  });

  it('preserves client metadata without adding a retired tenant stamp', async () => {
    const { call, store } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      metadata: { note: 'kept' },
    });
    const id = (res.body.schedule as { id: string }).id;
    const meta = store.m.get(id)?.metadata as Record<string, unknown>;
    expect(meta.tenantId).toBeUndefined();
    expect(meta.note).toBe('kept');
  });

  it('400s when neither workflowId nor agentId is present', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', { cron: '*/5 * * * *' });
    expect(res.status).toBe(400);
  });

  it('400s + audits a reserved requestContext key', async () => {
    const { call, events } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      requestContext: { 'breakwater.connectorGrants': ['forged'] },
    });
    expect(res.status).toBe(400);
    expect(events).toContainEqual(
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'reserved-context-key',
      }),
    );
  });

  it("400s the goal key 'mastra:goal' in requestContext too", async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      requestContext: { 'mastra:goal': 'injected' },
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/mastra:goal/);
  });

  it('400s an invalid cron', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      workflowId: 'wf',
      cron: 'not a cron',
    });
    expect(res.status).toBe(400);
  });

  it('400s + audits a calendrically-impossible cron (validateCron passes, no future occurrence) — never an unaudited 500 (M3)', async () => {
    // '0 0 30 2 *' (Feb 30) is syntactically legal so validateCron accepts it, but
    // computeNextFireAt throws (no future occurrence). It must surface as a clean,
    // audited 400 cron-invalid, not a raw 500 through the outer catch.
    const { call, events } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      workflowId: 'wf',
      cron: '0 0 30 2 *',
    });
    expect(res.status).toBe(400);
    expect(events).toContainEqual(
      expect.objectContaining({ outcome: 'rejected', reason: 'cron-invalid' }),
    );
  });

  it('400s a cron that fires faster than the fire-rate floor', async () => {
    // floor 2min; a per-minute cron (60s interval) is under it
    const { call, events } = harness(ctx('acme', 'operator'), {
      minFireIntervalMs: 120_000,
    });
    const res = await call('POST', '/api/schedules', {
      workflowId: 'wf',
      cron: '* * * * *',
    });
    expect(res.status).toBe(400);
    expect(events).toContainEqual(
      expect.objectContaining({ reason: 'fire-rate-too-high' }),
    );
  });

  it('400s at the deployment COUNT cap', async () => {
    const { call, events } = harness(ctx('acme', 'operator'), {
      maxSchedules: 1,
    });
    expect((await call('POST', '/api/schedules', WORKFLOW_CREATE)).status).toBe(
      201,
    );
    const second = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(second.status).toBe(400);
    expect(events).toContainEqual(
      expect.objectContaining({ reason: 'schedule-count-cap' }),
    );
  });

  it('400s an unknown body field (allowlist)', async () => {
    const { call } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      surprise: true,
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/surprise/);
  });
});

describe('createScheduleRouter — resource-scoped reads', () => {
  async function seedFor(
    actorLabel: string,
  ): Promise<{ store: MemStore; id: string }> {
    const h = harness(ctx(actorLabel, 'operator'));
    const res = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
    return { store: h.store, id: (res.body.schedule as { id: string }).id };
  }

  it('404s another operator before loading a schedule', async () => {
    const { store, id } = await seedFor('acme');
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('xyz', 'operator', async () => false)),
      store,
      executionFence: 'none',
    });
    const response = await router(
      new Request(`http://host/api/schedules/${id}`),
    );
    expect(response?.status).toBe(404);
  });

  it('allows an explicitly authorized read-only actor to inspect a schedule', async () => {
    const { store, id } = await seedFor('acme');
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('review', 'viewer', async () => true)),
      store,
      executionFence: 'none',
    });
    const response = await router(
      new Request(`http://host/api/schedules/${id}`),
    );
    expect(response?.status).toBe(200);
  });

  it('list filters out schedules the actor cannot read', async () => {
    const store = new MemStore();
    const mk = (context: ActorContext) =>
      createScheduleRouter({
        resolve: resolveAs(context),
        store,
        executionFence: 'none',
      });
    const ids: string[] = [];
    for (const actorLabel of ['acme', 'xyz']) {
      const created = await mk(ctx(actorLabel, 'operator'))(
        new Request('http://host/api/schedules', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(WORKFLOW_CREATE),
        }),
      );
      const body = (await created?.json()) as {
        schedule: { id: string };
      };
      ids.push(body.schedule.id);
    }
    const listed = await mk(
      ctx(
        'acme',
        'operator',
        async (kind, id) => kind === 'schedule' && id === ids[0],
      ),
    )(new Request('http://host/api/schedules'));
    const body = (await listed?.json()) as { schedules: unknown[] };
    expect(body.schedules).toHaveLength(1);
    expect(store.m.size).toBe(2);
  });

  it('a benign GET is NOT audited (only a denied read is)', async () => {
    const h = harness(ctx('acme', 'operator'));
    const created = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
    const id = (created.body.schedule as { id: string }).id;
    h.events.length = 0; // drop the create audit
    const res = await h.call('GET', `/api/schedules/${id}`);
    expect(res.status).toBe(200);
    expect(h.events).toEqual([]);
  });
});

describe('createScheduleRouter — mutations', () => {
  async function seed(
    context: ActorContext = ctx('acme', 'operator'),
  ): Promise<Harness & { id: string }> {
    const h = harness(context);
    const created = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
    const id = (created.body.schedule as { id: string }).id;
    h.events.length = 0;
    return { ...h, id };
  }

  async function seedAgent(): Promise<Harness & { id: string }> {
    const h = harness(ctx('acme', 'operator'));
    const created = await h.call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
    });
    const id = (created.body.schedule as { id: string }).id;
    h.events.length = 0;
    return { ...h, id };
  }

  it('pause then resume flips status', async () => {
    const { call, id } = await seed();
    expect(
      (
        (await call('POST', `/api/schedules/${id}/pause`)).body.schedule as {
          status: string;
        }
      ).status,
    ).toBe('paused');
    expect(
      (
        (await call('POST', `/api/schedules/${id}/resume`)).body.schedule as {
          status: string;
        }
      ).status,
    ).toBe('active');
  });

  it('update rejects a reserved requestContext key', async () => {
    const { call, events, id } = await seed();
    const res = await call('PATCH', `/api/schedules/${id}`, {
      requestContext: { 'breakwater.isolationScope': 'forged' },
    });
    expect(res.status).toBe(400);
    expect(events).toContainEqual(
      expect.objectContaining({
        operation: 'update',
        reason: 'reserved-context-key',
      }),
    );
  });

  it('update rejects a reserved key in the agent ifIdle.streamOptions.requestContext', async () => {
    const { call, id } = await seedAgent();
    const res = await call('PATCH', `/api/schedules/${id}`, {
      ifIdle: { streamOptions: { requestContext: { 'mastra:goal': 'x' } } },
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/ifIdle\.streamOptions\.requestContext/);
  });

  it('rejects wrong-kind update fields instead of accepting a no-op', async () => {
    const workflow = await seed();
    const workflowResult = await workflow.call(
      'PATCH',
      `/api/schedules/${workflow.id}`,
      { prompt: 'wrong kind' },
    );
    expect(workflowResult.status).toBe(400);
    expect(workflowResult.body.error).toMatch(/prompt/);

    const agent = await seedAgent();
    const agentResult = await agent.call(
      'PATCH',
      `/api/schedules/${agent.id}`,
      { inputData: { wrong: true } },
    );
    expect(agentResult.status).toBe(400);
    expect(agentResult.body.error).toMatch(/inputData/);
  });

  it('updates an agent prompt and persists the normalized target', async () => {
    const { call, store, id } = await seedAgent();
    const res = await call('PATCH', `/api/schedules/${id}`, {
      prompt: 'updated',
      ifIdle: {
        streamOptions: {
          requestContext: { safe: true },
          unsupported: 'dropped',
        },
      },
    });
    expect(res.status).toBe(200);
    expect(store.m.get(id)?.target).toMatchObject({
      type: 'agent',
      prompt: 'updated',
      ifIdle: { streamOptions: { requestContext: { safe: true } } },
    });
    expect(JSON.stringify(store.m.get(id)?.target)).not.toContain(
      'unsupported',
    );
  });

  it('rejects malformed update metadata', async () => {
    const { call, id } = await seed();
    const res = await call('PATCH', `/api/schedules/${id}`, {
      metadata: 'not-an-object',
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/metadata/);
  });

  it('revalidates a target-changing update against the stored creator role', async () => {
    let allowed = true;
    const policy: ScheduleTargetPolicy = {
      authorize: () =>
        allowed
          ? { allowed: true }
          : {
              allowed: false,
              status: 403,
              reason: 'target-role-forbidden',
            },
    };
    const { call, store } = harness(ctx('acme', 'operator'), {
      targetPolicy: policy,
    });
    const created = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    const id = (created.body.schedule as { id: string }).id;
    allowed = false;

    const updated = await call('PATCH', `/api/schedules/${id}`, {
      inputData: { changed: true },
    });

    expect(updated.status).toBe(403);
    expect(store.m.get(id)?.target).toMatchObject({ workflowId: 'wf' });
  });

  it('update 400s a non-string timezone (the create guard, now enforced on update too)', async () => {
    const { call, id } = await seed();
    const res = await call('PATCH', `/api/schedules/${id}`, { timezone: 123 });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/timezone/);
  });

  it('delete removes the schedule + audits accepted', async () => {
    const { call, store, events, id } = await seed();
    const res = await call('DELETE', `/api/schedules/${id}`);
    expect(res.status).toBe(200);
    expect(store.m.has(id)).toBe(false);
    expect(store.owners.has(id)).toBe(false);
    expect(events).toContainEqual(
      expect.objectContaining({ operation: 'delete', outcome: 'accepted' }),
    );
  });

  it('a retry after domain deletion releases the remaining owner claim', async () => {
    const { call, store, id } = await seed();
    store.m.delete(id);

    expect((await call('DELETE', `/api/schedules/${id}`)).status).toBe(200);
    expect(store.owners.has(id)).toBe(false);
  });

  it('keeps a delete-requested schedule addressable until dispatch settles', async () => {
    const { call, store, id } = await seed();
    store.deleteOwnedSchedule = async () => 'pending';
    store.triggers.push({
      id: 'trigger-pending',
      scheduleId: id,
      runId: 'run-pending',
      scheduledFireAt: 1,
      actualFireAt: 1,
      outcome: 'deferred',
    });

    const deleted = await call('DELETE', `/api/schedules/${id}`);
    expect(deleted.status).toBe(202);
    expect(deleted.body).toEqual({ ok: true, pending: true });
    const history = await call('GET', `/api/schedules/${id}/triggers`);
    expect(history.status).toBe(200);
    expect(history.body.triggers).toEqual([
      expect.objectContaining({
        id: 'trigger-pending',
        outcome: 'deferred',
      }),
    ]);
  });

  it('triggers returns the read-only history for an owned schedule', async () => {
    const { call, store, id } = await seed();
    store.triggers.push({
      scheduleId: id,
      runId: 'acme_r1',
      scheduledFireAt: 1,
      actualFireAt: 1,
      outcome: 'published',
    });
    const res = await call('GET', `/api/schedules/${id}/triggers`);
    expect(res.status).toBe(200);
    expect(res.body.triggers as unknown[]).toHaveLength(1);
  });
});

describe('createScheduleRouter — numeric configuration', () => {
  it.each([
    [{ maxSchedules: -1 }],
    [{ maxSchedules: Number.NaN }],
    [{ maxSchedules: 1.5 }],
    [{ maxContentBytes: Number.POSITIVE_INFINITY }],
    [{ maxContentBytes: Number.MAX_SAFE_INTEGER + 1 }],
    [{ minFireIntervalMs: 0 }],
  ])('fails synchronously for invalid numeric options: %o', (overrides) => {
    expect(() => harness(ctx('acme', 'operator'), overrides)).toThrow(
      RangeError,
    );
  });

  it('supports intentional zero count/body caps', async () => {
    const count = harness(ctx('acme', 'operator'), {
      maxSchedules: 0,
    });
    expect(
      (await count.call('POST', '/api/schedules', WORKFLOW_CREATE)).status,
    ).toBe(400);

    const body = harness(ctx('acme', 'operator'), { maxContentBytes: 0 });
    expect(
      (await body.call('POST', '/api/schedules', WORKFLOW_CREATE)).status,
    ).toBe(413);
  });
});

describe('createScheduleRouter internal errors', () => {
  it('returns a generic 500 and logs the private store detail', async () => {
    const logged: string[] = [];
    const log = vi.spyOn(console, 'error').mockImplementation((value) => {
      logged.push(String(value));
    });
    const store = new MemStore();
    store.listSchedules = async () => {
      throw new Error('private schedule store detail');
    };
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('acme', 'operator')),
      store,
      executionFence: 'none',
    });

    try {
      const response = await router(new Request('http://host/api/schedules'));
      expect(response?.status).toBe(500);
      expect(await response?.json()).toEqual({ error: 'internal error' });
      expect(response?.headers.get('cache-control')).toBe('no-store');
      expect(logged.join('\n')).toContain('private schedule store detail');
    } finally {
      log.mockRestore();
    }
  });
});

describe('createScheduleRouter and the deployment execution fence', () => {
  it('will not compile without explicit fence wiring', () => {
    // A TYPE-level pin on the forcing function, and the representative for the
    // whole required-`executionFence` sweep: the compile error is what stops a
    // host wiring the runtime's fence and forgetting a router's, which is the
    // partially-fenced deployment the option exists to prevent.
    //
    // An unused suppression directive is itself an error in this package's
    // tsconfig, so `tsc` exiting 0 is what proves the negative. (The directive
    // below must be the only one in this comment block — a prose line that
    // BEGINS with the directive text is parsed as one.)
    const build = (): unknown =>
      // @ts-expect-error a schedule router must state its fence wiring
      createScheduleRouterImpl({
        resolve: resolveAs(ctx('acme', 'operator')),
        store: new MemStore(),
        targetPolicy: TARGET_POLICY,
        validateThreadTarget: async () => undefined,
      });
    expect(build).toBeTypeOf('function');
  });

  it('degrades a mutation closed with 503 when the fence cannot be read', async () => {
    const h = await fencedHarness();
    h.sqlite.exec('DELETE FROM flowsafe_execution_fence');
    const res = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(503);
    expect(res.body.reason).toEqual({ code: 'EXECUTION_FENCE_UNREADABLE' });
    await expect(h.store.listSchedules()).resolves.toEqual([]);
  });

  it('refuses create, update, and resume once the deployment is draining', async () => {
    const { store, events, call, fence } = await fencedHarness();
    await store.createSchedule(scheduleRow('paused'));
    await fence.transition({ expected: 'open', next: 'draining' });
    for (const [method, path, body] of [
      ['POST', '/api/schedules', WORKFLOW_CREATE],
      ['PATCH', '/api/schedules/s1', { cron: '*/10 * * * *' }],
      ['POST', '/api/schedules/s1/resume', undefined],
    ] as const) {
      const res = await call(method, path, body);
      expect(res.status).toBe(503);
      expect(res.body.reason).toEqual({
        code: 'EXECUTION_FENCED',
        state: 'draining',
      });
    }
    await expect(store.listSchedules()).resolves.toHaveLength(1);
    expect(
      events.filter((event) => event.reason === 'execution-fenced'),
    ).toHaveLength(3);
  });

  it('keeps pause, delete, and every read available while draining', async () => {
    const { store, call, fence } = await fencedHarness();
    await store.createSchedule(scheduleRow());
    await fence.transition({ expected: 'open', next: 'draining' });
    expect((await call('GET', '/api/schedules')).status).toBe(200);
    expect((await call('GET', '/api/schedules/s1')).status).toBe(200);
    expect((await call('POST', '/api/schedules/s1/pause')).status).toBe(200);
    expect((await call('DELETE', '/api/schedules/s1')).status).toBe(200);
    await expect(store.listSchedules()).resolves.toEqual([]);
  });
});

function scheduleRow(status: Schedule['status'] = 'active'): Schedule {
  return {
    id: 's1',
    target: { type: 'workflow', workflowId: 'wf', inputData: {} },
    cron: '*/5 * * * *',
    status,
    nextFireAt: 300_000,
    createdAt: 10,
    updatedAt: 20,
    metadata: {},
  };
}

async function fencedHarness(context: ActorContext = ctx('acme', 'operator')) {
  const sqlite = openSqlite();
  const database = sqliteUnitDatabase(sqlite) as ScheduleDatabase;
  const fence = new ExecutionFenceStore(database as ExecutionFenceDatabase);
  await fence.seed('open');
  const store = new D1SchedulesStorage(database);
  await store.init();
  const events: ScheduleRouteAuditEvent[] = [];
  const options = {
    store,
    executionFence: fence,
    resolve: resolveAs(context),
    audit: (event: ScheduleRouteAuditEvent) => {
      events.push(event);
    },
  };
  return {
    sqlite,
    database,
    store,
    fence,
    events,
    options,
    call: routerCaller(createScheduleRouter(options)),
  };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function activateAndReopen(fence: ExecutionFenceStore) {
  await fence.transition({
    expected: 'open',
    next: 'draining',
    expectedMutationEpoch: 0,
    expectedRevision: 0,
    advanceMutationEpoch: true,
  });
  await fence.transition({
    expected: 'draining',
    next: 'migration-locked',
    expectedMutationEpoch: 1,
    expectedRevision: 1,
  });
  await fence.transition({
    expected: 'migration-locked',
    next: 'open',
    expectedMutationEpoch: 1,
    expectedRevision: 2,
  });
}

function customFacade() {
  const store = new MemStore();
  const database = sqliteUnitDatabase(
    openSqlite(),
  ) as FencedScheduleMutationCapability['database'];
  const capability: FencedScheduleMutationCapability = {
    database,
    createOwnedSchedule: vi.fn(function (
      this: FencedScheduleMutationCapability,
      schedule,
      owner,
      cap,
      _context,
    ) {
      expect(this).toBe(capability);
      return MemStore.prototype.createOwnedSchedule.call(
        store,
        schedule,
        owner,
        cap,
      );
    }),
    updateSchedule: vi.fn(function (
      this: FencedScheduleMutationCapability,
      id,
      patch,
      _context,
    ) {
      expect(this).toBe(capability);
      return MemStore.prototype.updateSchedule.call(store, id, patch);
    }),
    pauseSchedule: vi.fn(function (
      this: FencedScheduleMutationCapability,
      id,
      _context,
    ) {
      expect(this).toBe(capability);
      return MemStore.prototype.updateSchedule.call(store, id, {
        status: 'paused',
      });
    }),
    resumeSchedule: vi.fn(function (
      this: FencedScheduleMutationCapability,
      id,
      resume,
      _context,
    ) {
      expect(this).toBe(capability);
      return MemStore.prototype.updateSchedule.call(store, id, {
        status: 'active',
        nextFireAt: resume.nextFireAt,
      });
    }),
    deleteOwnedSchedule: vi.fn(function (
      this: FencedScheduleMutationCapability,
      id,
      _context,
    ) {
      expect(this).toBe(capability);
      return MemStore.prototype.deleteOwnedSchedule.call(store, id);
    }),
    observeScheduleMutation: vi.fn(function (
      this: FencedScheduleMutationCapability,
      id,
      _operation,
      _context,
    ) {
      expect(this).toBe(capability);
      return store.getSchedule(id);
    }),
  };
  Object.defineProperty(store, FENCED_SCHEDULE_STORAGE, {
    configurable: true,
    value: capability,
  });
  const options = {
    store,
    executionFence: 'none' as const,
    resolve: resolveAs({ ...ctx('acme', 'operator'), mutationEpoch: 7 }),
  };
  return { store, capability, database, options };
}

describe('schedule mutation capability construction', () => {
  it('requires a capability before an epoch-optional fenced router authenticates', async () => {
    const h = await fencedHarness();
    const resolve = vi.fn(resolveAs(ctx('acme', 'operator')));
    expect(() =>
      createScheduleRouter({
        ...h.options,
        store: new MemStore(),
        resolve,
      }),
    ).toThrow('fenced schedule storage capability is unavailable');
    expect(resolve).not.toHaveBeenCalled();
  });

  it.each([
    'none',
    'fenced',
  ] as const)('rejects malformed advertised capabilities with %s wiring', async (wiring) => {
    const h = await fencedHarness();
    const { capability } = customFacade();
    const malformed: unknown[] = [
      null,
      [],
      1,
      {},
      { ...capability, database: null },
      { ...capability, database: Object.assign([], capability.database) },
    ];
    for (const method of [
      'createOwnedSchedule',
      'updateSchedule',
      'pauseSchedule',
      'resumeSchedule',
      'deleteOwnedSchedule',
      'observeScheduleMutation',
    ])
      malformed.push({ ...capability, [method]: undefined });
    for (const method of ['prepare', 'batch']) {
      malformed.push({
        ...capability,
        database: { ...h.database, [method]: undefined },
      });
    }
    for (const value of malformed) {
      const store = Object.assign(new MemStore(), {
        [FENCED_SCHEDULE_STORAGE]: value,
      });
      expect(() =>
        createScheduleRouter({
          ...h.options,
          store: store as ScheduleFacadeStore,
          executionFence: wiring === 'none' ? 'none' : h.fence,
        }),
      ).toThrow('schedule mutation capability is malformed');
    }
  });

  it('rejects a concrete store over a different binding before activation', async () => {
    const h = await fencedHarness();
    const other = await fencedHarness();
    expect(() =>
      createScheduleRouter({
        ...h.options,
        store: other.store,
      }),
    ).toThrow('schedule storage binding disagrees with execution fence');
  });

  it('captures the capability and receiver before mutable properties change', async () => {
    const { store, capability, options } = customFacade();
    const selected = capability.createOwnedSchedule;
    const readMethod = vi.fn(() => selected);
    Object.defineProperty(capability, 'createOwnedSchedule', {
      configurable: true,
      get: readMethod,
    });
    const readCapability = vi.fn(() => capability);
    Object.defineProperty(store, FENCED_SCHEDULE_STORAGE, {
      configurable: true,
      get: readCapability,
    });
    const waiting = deferred();
    const entered = deferred();
    const context = ctx('acme', 'operator');
    context.resourceOwnerFor = async () => {
      entered.resolve();
      await waiting.promise;
      return context.resourceOwner;
    };
    const call = routerCaller(
      createScheduleRouter({
        ...options,
        resolve: resolveAs(context),
      }),
    );
    store.createOwnedSchedule = vi.fn(async () => {
      throw new Error('legacy write');
    });
    const pending = call('POST', '/api/schedules', {
      agentId: 'a1',
      prompt: 'go',
      cron: '*/5 * * * *',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
    });
    await entered.promise;
    Object.defineProperty(store, FENCED_SCHEDULE_STORAGE, { value: undefined });
    Object.defineProperty(capability, 'createOwnedSchedule', {
      value: async () => {
        throw new Error('replacement write');
      },
    });
    waiting.resolve();
    expect((await pending).status).toBe(201);
    expect(readCapability).toHaveBeenCalledTimes(1);
    expect(readMethod).toHaveBeenCalledTimes(1);
    expect(selected).toHaveBeenCalledTimes(1);
    expect(store.createOwnedSchedule).not.toHaveBeenCalled();
  });
});

describe('captured schedule request authority', () => {
  it('retains actor, owner, epoch, and service receiver through a body wait', async () => {
    const { store, capability, options } = customFacade();
    const context = {
      ...ctx('acme', 'operator'),
      actor: { id: 'operator-acme', role: 'operator' as ApprovalRole },
      deploymentTag: 'original',
      mutationEpoch: 7,
    };
    const epoch = vi.fn(() => 7);
    Object.defineProperty(context, 'mutationEpoch', {
      configurable: true,
      get: epoch,
    });
    const audit = vi.fn();
    const entered = deferred();
    const release = deferred();
    const request = new Request('http://host/api/schedules', {
      method: 'POST',
      body: JSON.stringify(WORKFLOW_CREATE),
    });
    const body = request.body;
    if (!body) throw new Error('missing test body');
    const getReader = body.getReader.bind(body);
    Object.defineProperty(body, 'getReader', {
      value: () => {
        const reader = getReader();
        const read = reader.read.bind(reader);
        reader.read = async () => {
          entered.resolve();
          await release.promise;
          return read();
        };
        return reader;
      },
    });
    const router = createScheduleRouter({
      ...options,
      resolve: resolveAs(context),
      audit,
    });
    const response = router(request);
    await entered.promise;
    context.actor.id = 'changed';
    context.actor.role = 'viewer';
    context.resourceOwner = { kind: 'human', id: 'changed' };
    context.deploymentTag = 'changed';
    Object.defineProperty(context, 'mutationEpoch', { value: 99 });
    release.resolve();
    expect((await response)?.status).toBe(201);
    expect(epoch).toHaveBeenCalledTimes(1);
    expect(capability.createOwnedSchedule).toHaveBeenCalledWith(
      expect.any(Object),
      { kind: 'human', id: 'operator-acme' },
      100,
      { mutationEpoch: 7 },
    );
    expect([...store.owners.values()]).toEqual([
      { kind: 'human', id: 'operator-acme' },
    ]);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({
        actorId: 'operator-acme',
        deploymentTag: 'original',
        outcome: 'accepted',
      }),
    );
    const passed = vi.mocked(capability.createOwnedSchedule).mock.calls[0]?.[3];
    expect(Object.isFrozen(passed)).toBe(true);
  });

  it('retains bound ownership methods and the original role during an access wait', async () => {
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow());
    const context = {
      ...ctx('acme', 'operator'),
      mutationEpoch: 7,
      actor: { id: 'operator-acme', role: 'operator' as ApprovalRole },
    };
    const entered = deferred();
    const release = deferred();
    context.canAccessResource = async function () {
      expect(this).toBe(context);
      entered.resolve();
      await release.promise;
      return true;
    };
    const call = routerCaller(
      createScheduleRouter({ ...options, resolve: resolveAs(context) }),
    );
    const pending = call('POST', '/api/schedules/s1/pause');
    await entered.promise;
    context.actor.role = 'viewer';
    context.mutationEpoch = 99;
    context.canAccessResource = async () => false;
    release.resolve();
    expect((await pending).status).toBe(200);
    expect(capability.pauseSchedule).toHaveBeenCalledWith('s1', {
      mutationEpoch: 7,
    });
  });

  it.each([
    NaN,
    -1,
    1.5,
    null,
    '7',
  ])('rejects malformed resolved epoch %s before storage or audit', async (mutationEpoch) => {
    const { capability, options } = customFacade();
    const audit = vi.fn();
    const call = routerCaller(
      createScheduleRouter({
        ...options,
        audit,
        resolve: resolveAs({
          ...ctx('acme', 'operator'),
          mutationEpoch,
        } as ActorContext),
      }),
    );
    const result = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(result.status).toBe(400);
    expect(result.body.reason).toEqual({ code: 'INVALID_MUTATION_EPOCH' });
    expect(capability.createOwnedSchedule).not.toHaveBeenCalled();
    expect(audit).not.toHaveBeenCalled();
  });

  it('rejects tenant epoch headers through the real resolver', async () => {
    const h = await fencedHarness();
    const authenticate = vi.fn(() => ({
      id: 'actor',
      role: 'operator' as const,
    }));
    const router = createScheduleRouter({
      ...h.options,
      resolve: createActorResolver({
        authenticate,
        storeFactory: new D1ApprovalStoreFactory(h.database),
        mutationEpoch: 7,
        buildService: () => {
          throw new Error('unused');
        },
      }),
    });
    const response = await router(
      new Request('http://host/api/schedules', {
        method: 'POST',
        headers: { 'x-flowsafe-mutation-epoch': '7' },
        body: JSON.stringify(WORKFLOW_CREATE),
      }),
    );
    expect(response?.status).toBe(403);
    expect(authenticate).not.toHaveBeenCalled();
    await expect(h.store.listSchedules()).resolves.toEqual([]);
  });

  it('does not accept a body epoch as authority', async () => {
    const { capability, options } = customFacade();
    const call = routerCaller(createScheduleRouter(options));
    expect(
      (
        await call('POST', '/api/schedules', {
          ...WORKFLOW_CREATE,
          mutationEpoch: 7,
        })
      ).status,
    ).toBe(400);
    expect(capability.createOwnedSchedule).not.toHaveBeenCalled();
  });
});

describe('schedule capability mutation dispatch', () => {
  it.each([
    'pause',
    'resume',
  ] as const)('observes an already-%s row without changing timestamps', async (operation) => {
    const { store, capability, options } = customFacade();
    const status = operation === 'pause' ? 'paused' : 'active';
    store.m.set('s1', scheduleRow(status));
    const observed = {
      ...scheduleRow(status),
      cron: '*/10 * * * *',
      updatedAt: 99,
    };
    vi.mocked(capability.observeScheduleMutation).mockResolvedValue(observed);
    const call = routerCaller(createScheduleRouter(options));
    const result = await call('POST', `/api/schedules/s1/${operation}`);
    expect(result.status).toBe(200);
    expect(result.body.schedule).toMatchObject({
      cron: observed.cron,
      updatedAt: observed.updatedAt,
    });
    expect(capability.observeScheduleMutation).toHaveBeenCalledWith(
      's1',
      operation,
      { mutationEpoch: 7 },
    );
    expect(capability.pauseSchedule).not.toHaveBeenCalled();
    expect(capability.resumeSchedule).not.toHaveBeenCalled();
    expect(capability.updateSchedule).not.toHaveBeenCalled();
    expect(store.m.get('s1')).toEqual(scheduleRow(status));
  });

  it.each([
    'pause',
    'resume',
  ] as const)('returns 404 if a %s no-op observation finds no row', async (operation) => {
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow(operation === 'pause' ? 'paused' : 'active'));
    vi.mocked(capability.observeScheduleMutation).mockResolvedValue(null);
    const audit = vi.fn();
    const call = routerCaller(createScheduleRouter({ ...options, audit }));
    expect(await call('POST', `/api/schedules/s1/${operation}`)).toEqual({
      status: 404,
      body: { error: 'not found' },
    });
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({ reason: 'not-found' }),
    );
    expect(capability.pauseSchedule).not.toHaveBeenCalled();
    expect(capability.resumeSchedule).not.toHaveBeenCalled();
  });

  it.each([
    'pause',
    'resume',
  ] as const)('uses the observed row when a %s no-op races another status change', async (operation) => {
    vi.spyOn(Date, 'now').mockReturnValue(1_800_000_000_000);
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow(operation === 'pause' ? 'paused' : 'active'));
    const observed = {
      ...scheduleRow(operation === 'pause' ? 'active' : 'paused'),
      cron: '15 * * * *',
      timezone: 'UTC',
    };
    vi.mocked(capability.observeScheduleMutation).mockResolvedValue(observed);
    const call = routerCaller(createScheduleRouter(options));
    expect((await call('POST', `/api/schedules/s1/${operation}`)).status).toBe(
      200,
    );
    if (operation === 'pause') {
      expect(capability.pauseSchedule).toHaveBeenCalledWith('s1', {
        mutationEpoch: 7,
      });
    } else {
      expect(capability.resumeSchedule).toHaveBeenCalledWith(
        's1',
        {
          expectedCron: observed.cron,
          expectedTimezone: 'UTC',
          nextFireAt: 1_800_000_900_000,
        },
        { mutationEpoch: 7 },
      );
    }
    expect(capability.updateSchedule).not.toHaveBeenCalled();
  });

  it('passes an explicit undefined expected timezone when resuming', async () => {
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow('paused'));
    const call = routerCaller(createScheduleRouter(options));
    expect((await call('POST', '/api/schedules/s1/resume')).status).toBe(200);
    const input = vi.mocked(capability.resumeSchedule).mock.calls[0]?.[1];
    expect(input).toHaveProperty('expectedTimezone', undefined);
    expect(input?.expectedCron).toBe('*/5 * * * *');
    expect(input?.nextFireAt).toBeGreaterThan(Date.now());
  });

  it('uses generic authoring for PATCH status paused and the owned deletion capability', async () => {
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow());
    const call = routerCaller(createScheduleRouter(options));
    expect(
      (await call('PATCH', '/api/schedules/s1', { status: 'paused' })).status,
    ).toBe(200);
    expect(capability.updateSchedule).toHaveBeenCalledWith(
      's1',
      { status: 'paused' },
      { mutationEpoch: 7 },
    );
    expect(capability.pauseSchedule).not.toHaveBeenCalled();
    expect((await call('DELETE', '/api/schedules/s1')).status).toBe(200);
    expect(capability.deleteOwnedSchedule).toHaveBeenCalledWith('s1', {
      mutationEpoch: 7,
    });
  });
});

const MUTATION_ROUTES = [
  [
    'create',
    'POST',
    '/api/schedules',
    WORKFLOW_CREATE,
    'active',
    'createOwnedSchedule',
  ],
  [
    'update',
    'PATCH',
    '/api/schedules/s1',
    { cron: '*/10 * * * *' },
    'active',
    'updateSchedule',
  ],
  [
    'pause',
    'POST',
    '/api/schedules/s1/pause',
    undefined,
    'active',
    'pauseSchedule',
  ],
  [
    'resume',
    'POST',
    '/api/schedules/s1/resume',
    undefined,
    'paused',
    'resumeSchedule',
  ],
  [
    'delete',
    'DELETE',
    '/api/schedules/s1',
    undefined,
    'active',
    'deleteOwnedSchedule',
  ],
  [
    'pause no-op',
    'POST',
    '/api/schedules/s1/pause',
    undefined,
    'paused',
    'observeScheduleMutation',
  ],
  [
    'resume no-op',
    'POST',
    '/api/schedules/s1/resume',
    undefined,
    'active',
    'observeScheduleMutation',
  ],
] as const;

describe('schedule router final D1 epoch enforcement', () => {
  it.each([
    'cron',
    'timezone',
  ] as const)('refuses resume when its observed %s changes before mutation', async (field) => {
    const h = await fencedHarness();
    await h.store.createSchedule(scheduleRow('paused'));
    const capability = h.store[FENCED_SCHEDULE_STORAGE];
    if (!capability) throw new Error('missing D1 capability');
    const patch =
      field === 'cron' ? { cron: '15 * * * *' } : { timezone: 'Asia/Dubai' };
    Object.defineProperty(h.store, FENCED_SCHEDULE_STORAGE, {
      value: {
        ...capability,
        resumeSchedule: async (
          ...args: Parameters<
            FencedScheduleMutationCapability['resumeSchedule']
          >
        ) => {
          await h.store.updateSchedule('s1', patch);
          return capability.resumeSchedule(...args);
        },
      },
    });
    const call = routerCaller(createScheduleRouter(h.options));
    const result = await call('POST', '/api/schedules/s1/resume');
    expect(result.status).toBe(409);
    expect(result.body.reason).toEqual({
      code: 'SCHEDULE_MUTATION_CONFLICT',
      classification: 'schedule-changed',
    });
    await expect(h.store.getSchedule('s1')).resolves.toMatchObject({
      ...patch,
      status: 'paused',
      nextFireAt: scheduleRow().nextFireAt,
    });
  });

  it('uses a present D1 capability under none wiring and leaves reads epoch-optional', async () => {
    const h = await fencedHarness();
    await h.store.createSchedule(scheduleRow());
    await activateAndReopen(h.fence);
    const call = routerCaller(
      createScheduleRouter({ ...h.options, executionFence: 'none' }),
    );
    expect((await call('POST', '/api/schedules/s1/pause')).body.reason).toEqual(
      {
        code: 'MUTATION_EPOCH_MISMATCH',
        classification: 'missing',
        mutationEpoch: 1,
      },
    );
    expect((await call('GET', '/api/schedules')).status).toBe(200);
    expect((await call('GET', '/api/schedules/s1')).status).toBe(200);
    expect((await call('GET', '/api/schedules/s1/triggers')).status).toBe(200);
  });

  it.each(
    MUTATION_ROUTES,
  )('rejects an old held %s after activation and reopen', async (_name, method, path, body, status, selected) => {
    const context = { ...ctx('acme', 'operator'), mutationEpoch: 0 };
    const h = await fencedHarness(context);
    await h.store.createOwnedSchedule(
      scheduleWithCreatorRole(scheduleRow(status), 'operator'),
      context.resourceOwner,
      100,
    );
    await h.store.recordTrigger({
      id: 'trigger-s1',
      scheduleId: 's1',
      runId: 'run-s1',
      scheduledFireAt: 1,
      actualFireAt: 2,
      outcome: 'succeeded',
    });
    const snapshot = () =>
      JSON.stringify([
        h.sqlite.prepare('SELECT * FROM mastra_schedules').all(),
        h.sqlite.prepare('SELECT * FROM mastra_schedule_triggers').all(),
        h.sqlite.prepare('SELECT * FROM flowsafe_resource_owners').all(),
      ]);
    const before = snapshot();
    const capability = h.store[FENCED_SCHEDULE_STORAGE];
    if (!capability) throw new Error('missing D1 capability');
    const entered = deferred();
    const release = deferred();
    const selectedMethod = capability[selected];
    const held = vi.fn(async (...args: unknown[]) => {
      entered.resolve();
      await release.promise;
      return Reflect.apply(selectedMethod, capability, args);
    });
    Object.defineProperty(h.store, FENCED_SCHEDULE_STORAGE, {
      value: { ...capability, [selected]: held },
    });
    const call = routerCaller(createScheduleRouter(h.options));
    const pending = call(method, path, body);
    await entered.promise;
    try {
      await activateAndReopen(h.fence);
      context.mutationEpoch = 1;
    } finally {
      release.resolve();
    }
    const result = await pending;
    expect(result.status).toBe(409);
    expect(result.body.reason).toEqual({
      code: 'MUTATION_EPOCH_MISMATCH',
      classification: 'stale',
      mutationEpoch: 1,
    });
    expect(held).toHaveBeenCalledTimes(1);
    expect(snapshot()).toBe(before);
    expect(h.events).toContainEqual(
      expect.objectContaining({
        outcome: 'rejected',
        reason: 'MUTATION_EPOCH_MISMATCH:stale',
      }),
    );
  });

  it.each(
    MUTATION_ROUTES,
  )('accepts exact current epoch for %s and refuses missing/future epochs', async (_name, method, path, body, status) => {
    for (const mutationEpoch of [undefined, 2, 1]) {
      const h = await fencedHarness({
        ...ctx('acme', 'operator'),
        mutationEpoch,
      });
      await h.store.createOwnedSchedule(
        scheduleWithCreatorRole(scheduleRow(status), 'operator'),
        { kind: 'human', id: 'operator-acme' },
        100,
      );
      await activateAndReopen(h.fence);
      const before = await h.store.getSchedule('s1');
      const result = await h.call(method, path, body);
      if (mutationEpoch === 1) {
        expect(result.status).toBe(
          method === 'POST' && path === '/api/schedules' ? 201 : 200,
        );
        if (_name.endsWith('no-op'))
          await expect(h.store.getSchedule('s1')).resolves.toEqual(before);
      } else {
        expect(result.status).toBe(409);
        expect(result.body.reason).toEqual({
          code: 'MUTATION_EPOCH_MISMATCH',
          classification: mutationEpoch === undefined ? 'missing' : 'future',
          mutationEpoch: 1,
        });
        await expect(h.store.getSchedule('s1')).resolves.toEqual(before);
      }
    }
  });

  it.each([
    'draining',
    'migration-locked',
    'proof-only',
  ] as const)('allows exact-epoch pause/delete in %s while PATCH remains authoring', async (state) => {
    const h = await fencedHarness({
      ...ctx('acme', 'operator'),
      mutationEpoch: 1,
    });
    await h.store.createSchedule(scheduleRow());
    await activateAndReopen(h.fence);
    await h.fence.transition({
      expected: 'open',
      next: state,
      expectedMutationEpoch: 1,
      expectedRevision: 3,
      ...(state === 'proof-only' ? { proofKey: 'proof-schedule' } : {}),
    });
    expect(
      (await h.call('PATCH', '/api/schedules/s1', { status: 'paused' })).status,
    ).toBe(503);
    expect((await h.call('POST', '/api/schedules/s1/pause')).status).toBe(200);
    expect((await h.call('POST', '/api/schedules/s1/pause')).status).toBe(200);
    expect((await h.call('DELETE', '/api/schedules/s1')).status).toBe(200);
  });
});

describe('schedule route refusal responses', () => {
  const failures = [
    [
      'invalid epoch',
      () => new InvalidMutationEpochError(),
      'INVALID_MUTATION_EPOCH',
    ],
    [
      'missing epoch',
      () => new MutationEpochMismatchError('missing', 7),
      'MUTATION_EPOCH_MISMATCH:missing',
    ],
    [
      'stale epoch',
      () => new MutationEpochMismatchError('stale', 7),
      'MUTATION_EPOCH_MISMATCH:stale',
    ],
    [
      'future epoch',
      () => new MutationEpochMismatchError('future', 7),
      'MUTATION_EPOCH_MISMATCH:future',
    ],
    [
      'closed fence',
      () => new ExecutionFencedError('draining'),
      'execution-fenced',
    ],
    [
      'unreadable fence',
      () => new ExecutionFenceUnreadableError('fence unreadable'),
      'execution-fence-unreadable',
    ],
    [
      'changed fence',
      () => new ScheduleMutationConflictError('fence-changed'),
      'SCHEDULE_MUTATION_CONFLICT:fence-changed',
    ],
    [
      'changed schedule',
      () => new ScheduleMutationConflictError('schedule-changed'),
      'SCHEDULE_MUTATION_CONFLICT:schedule-changed',
    ],
    [
      'unknown outcome',
      () =>
        new ScheduleMutationOutcomeUnknownError({
          cause: new Error('private SQL detail'),
        }),
      'SCHEDULE_MUTATION_OUTCOME_UNKNOWN',
    ],
  ] as const;

  it.each(
    failures,
  )('retains the typed %s refusal when its audit fails', async (_label, build, reason) => {
    const error = build();
    const callbackError = new RunRouteError(418, 'private audit detail');
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { store, capability, options } = customFacade();
    vi.mocked(capability.createOwnedSchedule).mockRejectedValue(error);
    const audit = vi.fn(async () => {
      throw callbackError;
    });
    const call = routerCaller(createScheduleRouter({ ...options, audit }));
    const result = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(result).toEqual({
      status: error.status,
      body: { error: error.message, reason: error.reason },
    });
    expect(JSON.stringify(result.body)).not.toMatch(
      /private SQL|private audit/,
    );
    expect(audit).toHaveBeenCalledTimes(1);
    expect(audit).toHaveBeenCalledWith(
      expect.objectContaining({ outcome: 'rejected', reason }),
    );
    expect(logged).toHaveBeenCalledWith(
      expect.stringContaining('schedule.route-audit-error'),
      callbackError,
    );
    if (error instanceof ScheduleMutationOutcomeUnknownError) {
      expect(logged).toHaveBeenCalledWith(
        expect.stringContaining('schedule.route-mutation-error'),
        error,
      );
      expect(error.cause).toEqual(new Error('private SQL detail'));
    }
    expect(capability.createOwnedSchedule).toHaveBeenCalledTimes(1);
    expect(capability.deleteOwnedSchedule).not.toHaveBeenCalled();
    expect(store.m.size).toBe(0);
  });

  it('preserves a generic final-storage notfound error as a generic 500', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { store, capability, options } = customFacade();
    store.m.set('s1', scheduleRow());
    vi.mocked(capability.updateSchedule).mockRejectedValue(
      new Error('schedule s1 not found'),
    );
    const call = routerCaller(createScheduleRouter(options));
    expect(
      await call('PATCH', '/api/schedules/s1', { cron: '*/10 * * * *' }),
    ).toEqual({
      status: 500,
      body: { error: 'internal error' },
    });
  });
});

describe('contained schedule audits', () => {
  it('does not reclassify a rejected route when audit throws a route error', async () => {
    const error = new RunRouteError(404, 'audit private detail');
    const audit = vi.fn(() => {
      throw error;
    });
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const h = harness(ctx('acme', 'viewer'), { audit });
    await expect(
      h.call('POST', '/api/schedules', WORKFLOW_CREATE),
    ).resolves.toEqual({
      status: 403,
      body: { error: 'forbidden' },
    });
    expect(audit).toHaveBeenCalledTimes(1);
    expect(logged).toHaveBeenCalledWith(
      expect.stringContaining('schedule.route-audit-error'),
      error,
    );
  });

  const failures = [
    ['ordinary', () => new Error('audit unavailable'), 'audit unavailable'],
    [
      'throwing message',
      () =>
        Object.defineProperty(new Error(), 'message', {
          get() {
            throw new Error('message getter failed');
          },
        }),
      'unreadable error',
    ],
    [
      'throwing coercion',
      () => ({
        [Symbol.toPrimitive]() {
          throw new Error('coercion failed');
        },
      }),
      'unreadable error',
    ],
    [
      'non-string message',
      () => Object.defineProperty(new Error(), 'message', { value: 1n }),
      '1',
    ],
  ] as const;

  it.each(
    failures,
  )('contains %s audit failures for accepted and rejected outcomes', async (_label, build, diagnostic) => {
    for (const asyncFailure of [false, true]) {
      for (const scenario of [
        'create',
        'delete',
        'pending',
        'no-op',
        'role',
        'ownership',
      ] as const) {
        const error = build();
        const audit = vi.fn(() => {
          if (asyncFailure) return Promise.reject(error);
          throw error;
        });
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
        const context = ctx(
          'acme',
          scenario === 'role' ? 'viewer' : 'operator',
          async () => scenario !== 'ownership',
        );
        const h = harness(context, { audit });
        h.store.m.set('s1', scheduleRow('paused'));
        if (scenario === 'pending')
          h.store.deleteOwnedSchedule = async () => 'pending';
        const result =
          scenario === 'create' || scenario === 'role'
            ? await h.call('POST', '/api/schedules', WORKFLOW_CREATE)
            : scenario === 'no-op'
              ? await h.call('POST', '/api/schedules/s1/pause')
              : await h.call('DELETE', '/api/schedules/s1');
        const status = {
          create: 201,
          delete: 200,
          pending: 202,
          'no-op': 200,
          role: 403,
          ownership: 404,
        }[scenario];
        expect(result.status).toBe(status);
        expect(audit).toHaveBeenCalledTimes(1);
        const message = logged.mock.calls[0]?.[0];
        expect(JSON.parse(String(message))).toMatchObject({
          type: 'schedule.route-audit-error',
          reason: diagnostic,
        });
        expect(logged.mock.calls[0]?.[1]).toBe(error);
        expect(h.store.m.has('s1')).toBe(scenario !== 'delete');
        if (scenario === 'create') expect(h.store.m.size).toBe(2);
        logged.mockRestore();
      }
    }
  });

  it('preserves the selected response when audit diagnostic reporting throws', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {
      throw new Error('logger unavailable');
    });
    const audit = vi.fn(async () => {
      throw new RunRouteError(404, 'sink unavailable');
    });
    const h = harness(ctx('acme', 'viewer'), { audit });
    expect(await h.call('POST', '/api/schedules', WORKFLOW_CREATE)).toEqual({
      status: 403,
      body: { error: 'forbidden' },
    });
    expect(audit).toHaveBeenCalledTimes(1);
  });
});
