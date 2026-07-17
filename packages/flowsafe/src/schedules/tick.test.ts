// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — createScheduleTick: the mint posture (INV-1), the run-cap
// seam (D-S4), the agent-target guard-off (substrate-blocked), fail-closed skips,
// in-process exactly-once (CAS), and the P4 stored-context barrier (b).

import type { Schedule, ScheduleTrigger } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import {
  buildScheduledLegContext,
  createScheduleTick,
  isReservedScheduleContextKey,
  RESERVED_SCHEDULE_CONTEXT_KEYS,
  type ScheduleTickAuditEvent,
  type ScheduleTickStore,
  stripReservedScheduleContext,
} from './tick.js';

const NOW = Date.parse('2026-07-17T12:00:00.000Z');

/** An in-memory ScheduleTickStore with a real CAS on nextFireAt. */
class FakeStore implements ScheduleTickStore {
  readonly schedules = new Map<string, Schedule>();
  readonly triggers: ScheduleTrigger[] = [];

  seed(schedule: Schedule): void {
    this.schedules.set(schedule.id, schedule);
  }

  async listDueSchedules(now: number, limit?: number): Promise<Schedule[]> {
    const due = [...this.schedules.values()].filter(
      (s) => s.status === 'active' && s.nextFireAt <= now,
    );
    return limit !== undefined ? due.slice(0, limit) : due;
  }

  async updateScheduleNextFire(
    id: string,
    expected: number,
    next: number,
    lastFireAt: number,
    lastRunId: string,
  ): Promise<boolean> {
    // Atomic check+set (no await between) — mirrors the D1 CAS, INCLUDING its
    // `status === 'active'` guard (a paused schedule cannot be claimed).
    const s = this.schedules.get(id);
    if (!s || s.nextFireAt !== expected || s.status !== 'active') return false;
    this.schedules.set(id, { ...s, nextFireAt: next, lastFireAt, lastRunId });
    return true;
  }

  async recordTrigger(trigger: ScheduleTrigger): Promise<void> {
    this.triggers.push(trigger);
  }
}

function workflowSchedule(overrides: Partial<Schedule> = {}): Schedule {
  return {
    id: 'schedule_a',
    target: { type: 'workflow', workflowId: 'wf', inputData: { topic: 'x' } },
    cron: '* * * * *',
    status: 'active',
    nextFireAt: NOW - 1000,
    createdAt: NOW,
    updatedAt: NOW,
    metadata: { tenantId: 'acme' },
    ...overrides,
  };
}

describe('createScheduleTick', () => {
  it('fires a due workflow target through the start seam with a fresh INV-1 runId', async () => {
    // #given a due workflow schedule
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));

    // #when the tick runs
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then start was called once with the workflowId + inputData + a
    // tenant-salted `${tenantId}_${uuid}` runId (INV-1, no bare crypto.randomUUID)
    expect(result.fired).toBe(1);
    expect(start).toHaveBeenCalledTimes(1);
    const [wf, runId, inputData] = start.mock.calls[0] ?? [];
    expect(wf).toBe('wf');
    expect(inputData).toEqual({ topic: 'x' });
    expect(runId).toMatch(/^acme_[0-9a-f]{8}-[0-9a-f]{4}-/);
    // a 'published' trigger with the salted tenant carried in metadata (purge key)
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'published',
      runId,
      metadata: { tenantId: 'acme' },
    });
    // the schedule advanced (nextFireAt moved off the original due time)
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('strips reserved requestContext keys before handing the leg context to start (P4 barrier b)', async () => {
    // #given a due schedule whose STORED requestContext carries a reserved key
    // (a compromised/tampered row) plus a benign one
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        target: {
          type: 'workflow',
          workflowId: 'wf',
          requestContext: {
            'breakwater.approvedConnectors': ['forged'],
            'mastra:goal': 'injected objective',
            'my.custom': 'kept',
          },
        },
      }),
    );
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));

    // #when
    await createScheduleTick({ store, start, now: () => NOW })();

    // #then start received the NON-reserved key only — the breakwater.* and the
    // goal key were stripped, so a stored capability/objective never rides in
    const passedContext = start.mock.calls[0]?.[3];
    expect(passedContext).toEqual({ 'my.custom': 'kept' });
  });

  it('SKIPS a capped tenant (audited) but leaves the schedule healthy — the CAS already advanced it (D-S4)', async () => {
    // #given a due workflow schedule and a run cap that DENIES
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));
    const events: ScheduleTickAuditEvent[] = [];

    // #when the tick runs with runCap -> false
    const result = await createScheduleTick({
      store,
      start,
      runCap: () => false,
      audit: (e) => {
        events.push(e);
      },
      now: () => NOW,
    })();

    // #then no run started, an audited skip, and the schedule stayed HEALTHY
    // (nextFireAt advanced — a capped fire is consumed, never retried hot)
    expect(result).toMatchObject({ fired: 0, skipped: 1 });
    expect(start).not.toHaveBeenCalled();
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBeGreaterThan(NOW);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'skipped',
      metadata: { tenantId: 'acme', reason: 'run-capped' },
    });
    expect(events).toContainEqual(
      expect.objectContaining({ outcome: 'skipped', reason: 'run-capped' }),
    );
  });

  it('GUARDS OFF an agent target (substrate-blocked): no run, audited skip, schedule advanced', async () => {
    // #given a due AGENT schedule
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        id: 'agent_a',
        target: { type: 'agent', agentId: 'a1', prompt: 'go' },
        ownerType: 'agent',
        ownerId: 'a1',
      }),
    );
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));

    // #when
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then the tick NEVER calls run(id)/start for an agent target — it records a
    // guarded skip and advances the schedule (no hot-loop)
    expect(result.skipped).toBe(1);
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'skipped',
      metadata: { tenantId: 'acme', reason: 'agent-target-unsupported' },
    });
    expect(store.schedules.get('agent_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('fails closed on an invalid/missing metadata.tenantId (no runId can be minted)', async () => {
    // #given a due workflow schedule with a non-INV-3 tenant (a tampered row)
    const store = new FakeStore();
    store.seed(workflowSchedule({ metadata: { tenantId: 'BadTenant!' } }));
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));

    // #when
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then no run, a failed trigger, and the schedule still advanced (no hot-loop)
    expect(result.failed).toBe(1);
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'failed',
      runId: null,
      metadata: { reason: 'invalid-tenant' },
    });
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('records a failed trigger when the start seam throws (schedule already advanced)', async () => {
    // #given a due schedule and a start that throws
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async () => {
      throw new Error('DO unavailable');
    });

    // #when
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then a failed trigger carrying the error; the schedule stayed advanced
    expect(result.failed).toBe(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'failed',
      error: 'DO unavailable',
    });
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('two concurrent ticks over one due schedule fire EXACTLY once (in-process CAS)', async () => {
    // #given one due schedule and two ticks sharing the store
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));
    const tick = createScheduleTick({ store, start, now: () => NOW });

    // #when both ticks run concurrently
    const [a, b] = await Promise.all([tick(), tick()]);

    // #then exactly one fired, one lost the CAS, one trigger row total
    expect(a.fired + b.fired).toBe(1);
    expect(a.lost + b.lost).toBe(1);
    expect(start).toHaveBeenCalledTimes(1);
    expect(
      store.triggers.filter((t) => t.outcome === 'published'),
    ).toHaveLength(1);
  });

  it('a post-dispatch bookkeeping failure is NOT reclassified as a start failure (M1)', async () => {
    // #given a due schedule whose `published` recordTrigger throws AFTER start
    // succeeded (a transient D1 write error post-dispatch)
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));
    const events: ScheduleTickAuditEvent[] = [];
    const throwingStore: ScheduleTickStore = {
      listDueSchedules: (n, l) => store.listDueSchedules(n, l),
      updateScheduleNextFire: (...args) =>
        store.updateScheduleNextFire(...args),
      recordTrigger: async () => {
        throw new Error('D1 write failed post-dispatch');
      },
    };

    // #when
    const result = await createScheduleTick({
      store: throwingStore,
      start,
      audit: (e) => {
        events.push(e);
      },
      now: () => NOW,
    })();

    // #then the run DID dispatch and is counted as fired; the post-dispatch
    // failure is contained by the loop — it is NEVER audited as a start-error or
    // a failed outcome (which would slander a run that actually ran)
    expect(start).toHaveBeenCalledTimes(1);
    expect(result.fired).toBe(1);
    expect(events.some((e) => e.reason === 'start-error')).toBe(false);
    expect(events.some((e) => e.outcome === 'failed')).toBe(false);
  });

  it('an unexpected fireOne throw is contained per-schedule (the pass still finishes)', async () => {
    // #given two due schedules; the CAS claim throws for the FIRST fire only (a
    // throw OUTSIDE fireOne's inner start-error catch, so it hits the loop's
    // per-schedule isolation)
    const store = new FakeStore();
    store.seed(workflowSchedule({ id: 'schedule_1', nextFireAt: NOW - 2000 }));
    store.seed(workflowSchedule({ id: 'schedule_2', nextFireAt: NOW - 1000 }));
    let claims = 0;
    const throwingStore: ScheduleTickStore = {
      listDueSchedules: (n, l) => store.listDueSchedules(n, l),
      updateScheduleNextFire: (...args) => {
        claims += 1;
        if (claims === 1) throw new Error('transient store failure');
        return store.updateScheduleNextFire(...args);
      },
      recordTrigger: (t) => store.recordTrigger(t),
    };
    const start = vi.fn(async (_wf, runId, _input, _rc) => ({ runId }));

    // #when
    const result = await createScheduleTick({
      store: throwingStore,
      start,
      now: () => NOW,
    })();

    // #then both due schedules were CONSIDERED; the first threw at claim time and
    // was contained; the second still fired + recorded a trigger (isolation)
    expect(result.due).toBe(2);
    expect(start).toHaveBeenCalledTimes(1);
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]?.outcome).toBe('published');
  });
});

describe('the P4 reserved-key barrier helpers', () => {
  it('isReservedScheduleContextKey covers the whole breakwater namespace + the goal key', () => {
    expect(isReservedScheduleContextKey('breakwater.approvedConnectors')).toBe(
      true,
    );
    expect(isReservedScheduleContextKey('breakwater.actor')).toBe(true);
    expect(isReservedScheduleContextKey('breakwater.workflowScope')).toBe(true);
    expect(isReservedScheduleContextKey('breakwater.isolationScope')).toBe(
      true,
    );
    // a hypothetical future breakwater key is covered by the prefix
    expect(isReservedScheduleContextKey('breakwater.somethingNew')).toBe(true);
    expect(isReservedScheduleContextKey('mastra:goal')).toBe(true);
    // a benign key is not reserved
    expect(isReservedScheduleContextKey('my.custom')).toBe(false);
  });

  it('every RESERVED_SCHEDULE_CONTEXT_KEYS entry is matched by the prefix predicate (no drift)', () => {
    // The explicit list and the prefix-based matcher must not diverge.
    expect(RESERVED_SCHEDULE_CONTEXT_KEYS.length).toBeGreaterThan(0);
    for (const key of RESERVED_SCHEDULE_CONTEXT_KEYS) {
      expect(isReservedScheduleContextKey(key)).toBe(true);
    }
  });

  it('stripReservedScheduleContext drops reserved keys and keeps the rest', () => {
    expect(
      stripReservedScheduleContext({
        'breakwater.actor': { id: 'x' },
        'mastra:goal': 'obj',
        'my.custom': 1,
      }),
    ).toEqual({ 'my.custom': 1 });
    expect(stripReservedScheduleContext(undefined)).toEqual({});
  });

  it('buildScheduledLegContext merges stored UNDER the runtime-derived context (runtime wins)', () => {
    // A reserved key in the stored context is absent; a benign key survives; and
    // on a (defense-in-depth) collision the runtime value wins (spread last).
    const merged = buildScheduledLegContext(
      {
        'breakwater.workflowScope': 'stored-forged',
        'my.custom': 'kept',
        collide: 'stored',
      },
      { 'breakwater.workflowScope': 'runtime', collide: 'runtime' },
    );
    expect(merged).toEqual({
      'my.custom': 'kept',
      'breakwater.workflowScope': 'runtime',
      collide: 'runtime',
    });
  });
});
