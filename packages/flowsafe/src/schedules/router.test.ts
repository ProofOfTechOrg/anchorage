// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — createScheduleRouter: the P6-lite gate order, the no-oracle
// 404s, the count + fire-rate caps, the P4 reserved-key rejection, and the audit
// coverage (accept + every post-auth denial; benign GET + pre-auth NOT audited).

import type {
  Schedule,
  ScheduleTrigger,
  ScheduleUpdate,
} from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import {
  type ApprovalRole,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '../approval-api/index.js';
import {
  createScheduleRouter,
  type ScheduleFacadeStore,
  type ScheduleRouteAuditEvent,
} from './router.js';

/** An in-memory facade store. */
class MemStore implements ScheduleFacadeStore {
  readonly m = new Map<string, Schedule>();
  readonly triggers: ScheduleTrigger[] = [];

  async createSchedule(schedule: Schedule): Promise<Schedule> {
    if (this.m.has(schedule.id)) throw new Error('exists');
    this.m.set(schedule.id, schedule);
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
  async deleteSchedule(id: string): Promise<void> {
    this.m.delete(id);
  }
  async listTriggers(scheduleId: string): Promise<ScheduleTrigger[]> {
    return this.triggers.filter((t) => t.scheduleId === scheduleId);
  }
}

function ctx(tenantId: string, role: ApprovalRole): TenantContext {
  return {
    tenantId,
    actor: { id: `${role}-1`, role, tenantId },
  } as unknown as TenantContext;
}

function resolveAs(tenant: TenantContext | undefined): TenantResolver {
  return async () => tenant;
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
  tenant: TenantContext | undefined,
  overrides: {
    resolve?: TenantResolver;
    maxSchedulesPerTenant?: number;
    minFireIntervalMs?: number;
  } = {},
): Harness {
  const store = new MemStore();
  const events: ScheduleRouteAuditEvent[] = [];
  const router = createScheduleRouter({
    resolve: overrides.resolve ?? resolveAs(tenant),
    store,
    audit: (e) => {
      events.push(e);
    },
    ...(overrides.maxSchedulesPerTenant !== undefined
      ? { maxSchedulesPerTenant: overrides.maxSchedulesPerTenant }
      : {}),
    ...(overrides.minFireIntervalMs !== undefined
      ? { minFireIntervalMs: overrides.minFireIntervalMs }
      : {}),
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

  it('403s a pre-auth resolver throw (TenantResolutionError), not audited', async () => {
    const { call, events } = harness(undefined, {
      resolve: async () => {
        throw new TenantResolutionError('bad token');
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
  it('creates a workflow schedule with a server-minted id + stamped tenant, audited accepted', async () => {
    const { call, store, events } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', WORKFLOW_CREATE);
    expect(res.status).toBe(201);
    const schedule = res.body.schedule as { id: string; workflowId: string };
    expect(schedule.id).toMatch(/^schedule_[0-9a-f]{8}-/);
    expect(schedule.workflowId).toBe('wf');
    // the persisted row carries the SERVER tenant, never a client value
    const stored = store.m.get(schedule.id);
    expect((stored?.metadata as { tenantId: string }).tenantId).toBe('acme');
    expect(events).toContainEqual(
      expect.objectContaining({ operation: 'create', outcome: 'accepted' }),
    );
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

  it('a client-supplied metadata.tenantId cannot override the server tenant', async () => {
    const { call, store } = harness(ctx('acme', 'operator'));
    const res = await call('POST', '/api/schedules', {
      ...WORKFLOW_CREATE,
      metadata: { tenantId: 'evil', note: 'kept' },
    });
    const id = (res.body.schedule as { id: string }).id;
    const meta = store.m.get(id)?.metadata as Record<string, unknown>;
    expect(meta.tenantId).toBe('acme');
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
      requestContext: { 'breakwater.approvedConnectors': ['forged'] },
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

  it('400s at the per-tenant COUNT cap (DL-007)', async () => {
    const { call, events } = harness(ctx('acme', 'operator'), {
      maxSchedulesPerTenant: 1,
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

describe('createScheduleRouter — ownership (no oracle) + reads', () => {
  async function seedFor(
    tenantId: string,
  ): Promise<{ store: MemStore; id: string }> {
    const h = harness(ctx(tenantId, 'operator'));
    const res = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
    return { store: h.store, id: (res.body.schedule as { id: string }).id };
  }

  it('404s a foreign schedule the SAME as a missing one (get/patch/delete/pause/resume/triggers)', async () => {
    // acme owns a schedule; xyz addresses it
    const { store, id } = await seedFor('acme');
    const events: ScheduleRouteAuditEvent[] = [];
    const router = createScheduleRouter({
      resolve: resolveAs(ctx('xyz', 'operator')),
      store,
      audit: (e) => {
        events.push(e);
      },
    });
    const hit = async (method: string, suffix = '') => {
      const res = await router(
        new Request(`http://host/api/schedules/${id}${suffix}`, {
          method,
          ...(method === 'PATCH'
            ? {
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ status: 'paused' }),
              }
            : {}),
        }),
      );
      return res?.status;
    };
    expect(await hit('GET')).toBe(404);
    expect(await hit('PATCH')).toBe(404);
    expect(await hit('DELETE')).toBe(404);
    expect(await hit('POST', '/pause')).toBe(404);
    expect(await hit('POST', '/resume')).toBe(404);
    expect(await hit('GET', '/triggers')).toBe(404);
    // the cross-tenant probe was audited (a foreign PATCH), byte-identical to missing
    expect(events).toContainEqual(
      expect.objectContaining({
        operation: 'update',
        outcome: 'rejected',
        reason: 'not-found',
      }),
    );
  });

  it('list is tenant-filtered on metadata.tenantId', async () => {
    // one shared store, two tenants each with a schedule
    const store = new MemStore();
    const mk = (tenantId: string) =>
      createScheduleRouter({
        resolve: resolveAs(ctx(tenantId, 'operator')),
        store,
      });
    for (const tenantId of ['acme', 'xyz']) {
      await mk(tenantId)(
        new Request('http://host/api/schedules', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(WORKFLOW_CREATE),
        }),
      );
    }
    const listed = await mk('acme')(new Request('http://host/api/schedules'));
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
  async function seed(): Promise<Harness & { id: string }> {
    const h = harness(ctx('acme', 'operator'));
    const created = await h.call('POST', '/api/schedules', WORKFLOW_CREATE);
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
    const { call, id } = await seed();
    const res = await call('PATCH', `/api/schedules/${id}`, {
      ifIdle: { streamOptions: { requestContext: { 'mastra:goal': 'x' } } },
    });
    expect(res.status).toBe(400);
    expect(res.body.error).toMatch(/ifIdle\.streamOptions\.requestContext/);
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
    expect(events).toContainEqual(
      expect.objectContaining({ operation: 'delete', outcome: 'accepted' }),
    );
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
