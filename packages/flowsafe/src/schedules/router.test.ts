// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — createScheduleRouter: the P6-lite gate order, the no-oracle
// 404s, the count + fire-rate caps, the P4 reserved-key rejection, and the audit
// coverage (accept + every post-auth denial; benign GET + pre-auth NOT audited).

import type {
  Schedule,
  ScheduleTrigger,
  ScheduleUpdate,
} from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  type ResourceOwner,
} from '../approval-api/index.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
} from '../do-runner/index.js';
import { RunRouteError } from '../host-kit/index.js';
import {
  createScheduleRouter as createScheduleRouterImpl,
  type ScheduleFacadeStore,
  type ScheduleRouteAuditEvent,
  type ScheduleRouterOptions,
} from './router.js';
import type { ScheduleTargetPolicy } from './target-policy.js';
import { createScheduleTargetPolicy } from './target-policy.js';

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
    executionFence?: ScheduleRouterOptions['executionFence'];
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
    // 'none' is the honest wiring for MemStore — no database, nothing to fence.
    // The fence cases below pass a real store.
    executionFence: overrides.executionFence ?? 'none',
  });
  const call = async (method: string, path: string, body?: unknown) => {
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
    const parsed = (await res.json()) as Record<string, unknown>;
    return { status: res.status, body: parsed };
  };
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

  it('400s + audits a reserved requestContext key (P4 barrier a)', async () => {
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

  it('400s a cron that fires faster than the fire-rate floor (DL-007)', async () => {
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

  it('400s at the deployment COUNT cap (DL-007)', async () => {
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

  async function drainingFence(): Promise<ExecutionFenceStore> {
    const fence = new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
    await fence.seed('draining');
    return fence;
  }

  function unreadableFence(): ExecutionFenceStore {
    // Storage that faults on every query — NOT the "no such table" a pre-0.20
    // database answers with, which legitimately reads as open.
    return new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          bind: () => {
            throw new Error('unreachable');
          },
          run: () => Promise.reject(new Error('D1_ERROR: network')),
          all: () => Promise.reject(new Error('D1_ERROR: network')),
        }),
        run: () => Promise.reject(new Error('D1_ERROR: network')),
        all: () => Promise.reject(new Error('D1_ERROR: network')),
      }),
    } as unknown as ExecutionFenceDatabase);
  }

  it('degrades a mutation closed with 503 when the fence cannot be read', async () => {
    // #given
    const { store, call } = harness(ctx('acme', 'operator'), {
      executionFence: unreadableFence(),
    });

    // #then — never the generic 500: an operator must be able to tell a
    // deployment being migrated from a broken one, and the write did not land.
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(503);
    expect(res.body.reason).toEqual({ code: 'EXECUTION_FENCE_UNREADABLE' });
    expect(store.m.size).toBe(0);
  });

  it('refuses create, update, and resume once the deployment is draining', async () => {
    // #given
    const executionFence = await drainingFence();
    const { store, events, call } = harness(ctx('acme', 'operator'), {
      executionFence,
    });
    store.m.set('s1', {
      id: 's1',
      target: { type: 'workflow', workflowId: 'wf', inputData: {} },
      cron: '*/5 * * * *',
      status: 'paused',
      nextFireAt: 0,
      createdAt: 0,
      updatedAt: 0,
      metadata: {},
    } as Schedule);

    // #when / #then — every operation that ARMS a future fire is refused with
    // the taxonomy's retryable status and code.
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
    expect(store.m.size).toBe(1);
    expect(
      events.filter((event) => event.reason === 'execution-fenced'),
    ).toHaveLength(3);
  });

  it('keeps pause, delete, and every read available while draining', async () => {
    // #given — pause and delete TAKE WORK AWAY, which is the direction a drain
    // is going, and a read moves nothing.
    const executionFence = await drainingFence();
    const { store, call } = harness(ctx('acme', 'operator'), {
      executionFence,
    });
    store.m.set('s1', {
      id: 's1',
      target: { type: 'workflow', workflowId: 'wf', inputData: {} },
      cron: '*/5 * * * *',
      status: 'active',
      nextFireAt: 0,
      createdAt: 0,
      updatedAt: 0,
      metadata: {},
    } as Schedule);

    // #then
    expect((await call('GET', '/api/schedules')).status).toBe(200);
    expect((await call('GET', '/api/schedules/s1')).status).toBe(200);
    expect((await call('POST', '/api/schedules/s1/pause')).status).toBe(200);
    expect((await call('DELETE', '/api/schedules/s1')).status).toBe(200);
    expect(store.m.size).toBe(0);
  });
});
