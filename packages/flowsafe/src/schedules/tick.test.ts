// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — createScheduleTick: the mint posture (INV-1), the run-cap
// seam (D-S4), optional agent start, fail-closed fallback, in-process single-claim
// (CAS), lost-claim classification, and the P4 stored-context barrier (b).

import type { Schedule, ScheduleTrigger } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
} from '../do-runner/index.js';
import type { ScheduleFireClaim } from './schedules-d1.js';
import { createScheduleTargetPolicy } from './target-policy.js';
import {
  buildScheduledLegContext,
  canPersistScheduledAgentSignal,
  createScheduleStartSource,
  createScheduleTick as createScheduleTickImpl,
  isReservedScheduleContextKey,
  RESERVED_SCHEDULE_CONTEXT_KEYS,
  type ScheduleTickAuditEvent,
  type ScheduleTickOptions,
  type ScheduleTickSignalAgentInput,
  type ScheduleTickStore,
  stripReservedScheduleContext,
} from './tick.js';

const NOW = Date.parse('2026-07-17T12:00:00.000Z');
const TARGET_POLICY = createScheduleTargetPolicy({
  workflows: [{ id: 'wf' }],
  agents: [
    {
      id: 'a1',
      allowedAutomation: [{ kind: 'system', entryPaths: ['schedule.fire'] }],
    },
  ],
});

function createScheduleTick(
  options: Omit<ScheduleTickOptions, 'targetPolicy' | 'status'> & {
    status?: ScheduleTickOptions['status'];
  },
) {
  return createScheduleTickImpl({
    status: async () => undefined,
    ...options,
    targetPolicy: TARGET_POLICY,
  });
}

/** An in-memory ScheduleTickStore with a real CAS on nextFireAt. */
class FakeStore implements ScheduleTickStore {
  readonly schedules = new Map<string, Schedule>();
  readonly triggers: ScheduleTrigger[] = [];

  seed(schedule: Schedule): void {
    this.schedules.set(schedule.id, {
      creatorRole: 'operator',
      ...schedule,
    } as Schedule);
  }

  async listDueSchedules(now: number, limit?: number): Promise<Schedule[]> {
    const due = [...this.schedules.values()].filter(
      (s) => s.status === 'active' && s.nextFireAt <= now,
    );
    return limit !== undefined ? due.slice(0, limit) : due;
  }

  async getSchedule(id: string): Promise<Schedule | null> {
    return this.schedules.get(id) ?? null;
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

  async claimScheduleFire(claim: ScheduleFireClaim): Promise<boolean> {
    const schedule = this.schedules.get(claim.scheduleId);
    if (
      !schedule ||
      schedule.nextFireAt !== claim.expectedNextFireAt ||
      schedule.status !== 'active'
    ) {
      return false;
    }
    this.schedules.set(claim.scheduleId, {
      ...schedule,
      nextFireAt: claim.newNextFireAt,
      lastFireAt: claim.actualFireAt,
      lastRunId: claim.runId,
    });
    await this.recordTrigger(claim.trigger);
    return true;
  }

  async recordTrigger(trigger: ScheduleTrigger): Promise<void> {
    const index = trigger.id
      ? this.triggers.findIndex((stored) => stored.id === trigger.id)
      : -1;
    if (index === -1) this.triggers.push(trigger);
    else this.triggers[index] = trigger;
  }

  async touchDeferredTrigger(
    id: string,
    scheduleId: string,
    error: string | undefined,
    metadata: Record<string, unknown>,
  ): Promise<void> {
    const index = this.triggers.findIndex(
      (trigger) =>
        trigger.id === id &&
        trigger.scheduleId === scheduleId &&
        trigger.outcome === 'deferred',
    );
    if (index === -1) return;
    const trigger = this.triggers[index];
    if (!trigger) return;
    this.triggers[index] = {
      ...trigger,
      error,
      metadata: { ...trigger.metadata, ...metadata },
    };
  }

  async listDeferredTriggers(limit?: number): Promise<ScheduleTrigger[]> {
    const deferred = this.triggers.filter(
      (trigger) => trigger.outcome === 'deferred',
    );
    deferred.sort((left, right) => {
      const leftAfter = left.metadata?.reconcileAfter;
      const rightAfter = right.metadata?.reconcileAfter;
      const leftOrder =
        typeof leftAfter === 'number' ? leftAfter : left.actualFireAt;
      const rightOrder =
        typeof rightAfter === 'number' ? rightAfter : right.actualFireAt;
      return leftOrder - rightOrder || left.actualFireAt - right.actualFireAt;
    });
    return limit === undefined ? deferred : deferred.slice(0, limit);
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
    metadata: {},
    ...overrides,
  };
}

describe('canPersistScheduledAgentSignal', () => {
  it('requires the registered schedule, thread, and resource to share one owner', async () => {
    const source = {
      resolveScheduleTarget: vi.fn(async () => ({
        type: 'agent' as const,
        agentId: 'a1',
        prompt: 'later',
        threadId: 'thread_1',
        resourceId: 'resource_1',
      })),
    };
    const owner = { kind: 'human' as const, id: 'alice' };
    const resources = {
      owner: vi.fn(async (_kind: string, _id: string) => owner),
    };
    const input = {
      scheduleId: 'schedule_agent',
      dispatchId: 'dispatch_1',
      runId: 'run_1',
      agentId: 'a1',
      threadId: 'thread_1',
      resourceId: 'resource_1',
    };

    await expect(
      canPersistScheduledAgentSignal(source, resources, input),
    ).resolves.toBe(true);
    resources.owner.mockImplementation(async (kind) =>
      kind === 'resource' ? { kind: 'human', id: 'bob' } : owner,
    );
    await expect(
      canPersistScheduledAgentSignal(source, resources, input),
    ).resolves.toBe(false);
    await expect(
      canPersistScheduledAgentSignal(source, resources, {
        ...input,
        threadId: 'other_thread',
      }),
    ).resolves.toBe(false);
  });
});

describe('createScheduleStartSource', () => {
  it('returns the canonical target snapshot only for its prepared fire tuple', async () => {
    const trigger: ScheduleTrigger = {
      id: 'dispatch-1',
      scheduleId: 'schedule_a',
      runId: 'run-1',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          scheduleId: 'schedule_a',
          dispatchId: 'dispatch-1',
          runId: 'run-1',
          target: 'workflow',
          workflowId: 'wf',
          workflowTarget: {
            type: 'workflow',
            workflowId: 'wf',
            inputData: { source: 'stored' },
            initialState: { cursor: 7 },
            requestContext: {
              safe: 'kept',
              'breakwater.workflowScope': 'forged',
            },
          },
        },
      },
    };
    const getClaimedScheduleDispatch = vi.fn(
      async (scheduleId: string, dispatchId: string, runId: string) =>
        scheduleId === trigger.scheduleId &&
        dispatchId === trigger.id &&
        runId === trigger.runId
          ? trigger
          : null,
    );
    const source = createScheduleStartSource({
      getClaimedScheduleDispatch,
    });

    await expect(
      source.resolveScheduleTarget('schedule_a', 'dispatch-1', 'run-1'),
    ).resolves.toEqual({
      type: 'workflow',
      workflowId: 'wf',
      inputData: { source: 'stored' },
      initialState: { cursor: 7 },
      requestContext: { safe: 'kept' },
    });
    await expect(
      source.resolveScheduleTarget('schedule_a', 'dispatch-1', 'other-run'),
    ).resolves.toBeUndefined();
    await expect(
      source.resolveScheduleTarget('schedule_a', 'other-dispatch', 'run-1'),
    ).resolves.toBeUndefined();
  });

  it('rebuilds server-owned provider correlation from the stored agent target', async () => {
    const trigger: ScheduleTrigger = {
      id: 'dispatch-agent',
      scheduleId: 'schedule_agent',
      runId: 'run-agent',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          scheduleId: 'schedule_agent',
          dispatchId: 'dispatch-agent',
          runId: 'run-agent',
          target: 'agent',
          mode: 'start',
          agentId: 'a1',
          threadId: 'topology-thread',
          agentTarget: {
            type: 'agent',
            agentId: 'a1',
            prompt: 'stored prompt',
            providerOptions: {
              vendor: { mode: 'safe' },
              mastra: { schedule: { scheduleId: 'forged' } },
            },
          },
        },
      },
    };
    const source = createScheduleStartSource({
      getClaimedScheduleDispatch: async () => trigger,
    });

    await expect(
      source.resolveScheduleTarget(
        'schedule_agent',
        'dispatch-agent',
        'run-agent',
      ),
    ).resolves.toMatchObject({
      type: 'agent',
      prompt: 'stored prompt',
      providerOptions: {
        vendor: { mode: 'safe' },
        mastra: { schedule: { scheduleId: 'schedule_agent' } },
      },
    });
  });
});

describe('createScheduleTick', () => {
  it('fires a due workflow target through the start seam with a fresh INV-1 runId', async () => {
    // #given a due workflow schedule
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        target: {
          type: 'workflow',
          workflowId: 'wf',
          inputData: { topic: 'x' },
          initialState: { cursor: 7 },
        },
      }),
    );
    const start = vi.fn(async ({ runId }) => ({ runId }));

    // #when the tick runs
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then start was called once with the workflowId + inputData + an opaque
    // server-minted runId.
    expect(result.fired).toBe(1);
    expect(start).toHaveBeenCalledTimes(1);
    const dispatched = start.mock.calls[0]?.[0];
    expect(dispatched).toMatchObject({
      workflowId: 'wf',
      inputData: { topic: 'x' },
      initialState: { cursor: 7 },
      scheduleId: 'schedule_a',
      requestContext: {},
    });
    expect(dispatched?.dispatchId).toMatch(/^[0-9a-f-]{36}$/);
    expect(dispatched?.runId).toMatch(/^[0-9a-f-]{36}$/);
    // a deployment-wide published trigger
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'published',
      runId: dispatched?.runId,
      metadata: {},
    });
    // the schedule advanced (nextFireAt moved off the original due time)
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('revalidates catalog policy at fire time before dispatch', async () => {
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }) => ({ runId }));
    const targetPolicy = createScheduleTargetPolicy({
      workflows: [{ id: 'wf', allowedRoles: ['admin'] }],
      agents: [],
    });

    const result = await createScheduleTickImpl({
      store,
      start,
      status: async () => undefined,
      targetPolicy,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ fired: 0, failed: 1 });
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers).toContainEqual(
      expect.objectContaining({
        outcome: 'failed',
        metadata: { reason: 'target-role-forbidden' },
      }),
    );
  });

  it('fails closed on a legacy schedule without creator-role provenance', async () => {
    const store = new FakeStore();
    store.schedules.set('schedule_a', workflowSchedule());
    const start = vi.fn(async ({ runId }) => ({ runId }));

    const result = await createScheduleTick({
      store,
      start,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ fired: 0, failed: 1 });
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers).toContainEqual(
      expect.objectContaining({
        outcome: 'failed',
        metadata: { reason: 'invalid-creator-role' },
      }),
    );
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
            'breakwater.connectorGrants': [
              {
                scope: 'run',
                connectorId: 'forged',
                workflowId: 'wf',
                runId: 'acme_stale-run',
                isolationScope: 'acme',
              },
            ],
            'mastra:goal': 'injected objective',
            'my.custom': 'kept',
          },
        },
      }),
    );
    const start = vi.fn(async ({ runId }) => ({ runId }));

    // #when
    await createScheduleTick({ store, start, now: () => NOW })();

    // #then start received the NON-reserved key only — the breakwater.* and the
    // goal key were stripped, so a stored capability/objective never rides in
    const passedContext = start.mock.calls[0]?.[0].requestContext;
    expect(passedContext).toEqual({ 'my.custom': 'kept' });
  });

  it('SKIPS a capped deployment (audited) but leaves the schedule healthy — the CAS already advanced it (D-S4)', async () => {
    // #given a due workflow schedule and a run cap that DENIES
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }) => ({ runId }));
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
      metadata: { reason: 'run-capped' },
    });
    expect(events).toContainEqual(
      expect.objectContaining({ outcome: 'skipped', reason: 'run-capped' }),
    );
  });

  it('retains the audited agent fallback when no startAgent seam is configured', async () => {
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
    const start = vi.fn(async ({ runId }) => ({ runId }));

    // #when
    const result = await createScheduleTick({ store, start, now: () => NOW })();

    // #then the tick records a guarded skip and advances the schedule
    expect(result.skipped).toBe(1);
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'skipped',
      metadata: { reason: 'agent-target-unsupported' },
    });
    expect(store.schedules.get('agent_a')?.nextFireAt).toBeGreaterThan(NOW);
  });

  it('fires an agent target through the runtime seam with sanitized contexts', async () => {
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        id: 'agent_a',
        target: {
          type: 'agent',
          agentId: 'a1',
          prompt: 'go',
          threadId: 'acme_thread',
          resourceId: 'acme_resource',
          requestContext: {
            keep: 1,
            'breakwater.connectorGrants': ['forged'],
          },
          ifIdle: {
            behavior: 'wake',
            streamOptions: {
              requestContext: { nested: true, 'mastra:goal': 'forged' },
              unsupported: 'drop',
            },
            unsupported: 'drop',
          },
          unsupported: 'drop',
        } as never,
        ownerType: 'agent',
        ownerId: 'a1',
      }),
    );
    const start = vi.fn(async ({ runId }) => ({ runId }));
    const signalAgent = vi.fn(async (_input: ScheduleTickSignalAgentInput) => ({
      action: 'wake' as const,
      outcome: 'succeeded' as const,
      runId: 'acme_joined-run',
      signalId: 'acme_signal',
    }));

    const result = await createScheduleTick({
      store,
      start,
      signalAgent,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ fired: 1, failed: 0, skipped: 0 });
    expect(signalAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        scheduleId: 'agent_a',
        runId: expect.stringMatching(/^[0-9a-f-]{36}$/),
        threaded: true,
        requestContext: { keep: 1 },
        streamRequestContext: { nested: true },
      }),
    );
    const dispatched = signalAgent.mock.calls[0]?.[0]?.target;
    expect(dispatched).not.toBe(store.schedules.get('agent_a')?.target);
    expect(JSON.stringify(dispatched)).not.toContain('unsupported');
    expect(
      (
        store.schedules.get('agent_a')?.target as unknown as {
          unsupported: string;
        }
      ).unsupported,
    ).toBe('drop');
    expect(store.triggers[0]).toMatchObject({
      outcome: 'succeeded',
      runId: 'acme_joined-run',
    });
  });

  it('mints an ephemeral topology for a threadless agent target', async () => {
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        id: 'agent_threadless',
        target: { type: 'agent', agentId: 'a1', prompt: 'go' },
        ownerType: 'agent',
        ownerId: 'a1',
      }),
    );
    const startAgent = vi.fn(async ({ runId }) => ({ runId }));

    const result = await createScheduleTick({
      store,
      start: vi.fn(),
      startAgent,
      now: () => NOW,
    })();

    expect(result.fired).toBe(1);
    expect(startAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        threaded: false,
        topologyThreadId: expect.stringMatching(/^[0-9a-f-]{36}$/),
      }),
    );
  });

  it('dispatches arbitrary path-safe stored memory ids within the deployment', async () => {
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        target: {
          type: 'agent',
          agentId: 'a1',
          prompt: 'go',
          threadId: 'globex_thread',
          resourceId: 'globex_resource',
        },
      }),
    );
    const signalAgent = vi.fn(async ({ runId }) => ({
      action: 'wake' as const,
      outcome: 'succeeded' as const,
      runId,
    }));

    const result = await createScheduleTick({
      store,
      start: vi.fn(),
      signalAgent,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ fired: 1, failed: 0 });
    expect(signalAgent).toHaveBeenCalledWith(
      expect.objectContaining({
        threaded: true,
      }),
    );
    expect(store.triggers[0]).toMatchObject({
      outcome: 'succeeded',
    });
  });

  it('consumes and audits a malformed stored agent target without dispatching it', async () => {
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        id: 'agent_malformed',
        target: {
          type: 'agent',
          agentId: 'a1',
          prompt: '',
        },
      }),
    );
    const startAgent = vi.fn();

    const result = await createScheduleTick({
      store,
      start: vi.fn(),
      startAgent,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ failed: 1, fired: 0 });
    expect(startAgent).not.toHaveBeenCalled();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'failed',
      metadata: { reason: 'invalid-agent-target' },
    });
    expect(store.schedules.get('agent_malformed')?.nextFireAt).toBeGreaterThan(
      NOW,
    );
  });

  it('rejects invalid limits synchronously and treats zero as a no-op', async () => {
    const store = new FakeStore();
    store.seed(workflowSchedule());
    for (const limit of [
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(() =>
        createScheduleTick({ store, start: vi.fn(), limit }),
      ).toThrow(RangeError);
    }
    const start = vi.fn();
    await expect(
      createScheduleTick({ store, start, limit: 0 })(),
    ).resolves.toEqual({
      due: 0,
      fired: 0,
      skipped: 0,
      failed: 0,
      deferred: 0,
      reconciled: 0,
      lost: 0,
    });
    expect(start).not.toHaveBeenCalled();
  });

  it('uses the infrastructure deployment tag instead of schedule metadata', async () => {
    // #given a due workflow schedule carrying a forged legacy metadata tag
    const store = new FakeStore();
    store.seed(workflowSchedule({ metadata: { tenantId: 'forged' } }));
    const start = vi.fn(async ({ runId }) => ({ runId }));
    const events: ScheduleTickAuditEvent[] = [];

    // #when
    const result = await createScheduleTick({
      store,
      start,
      deploymentTag: 'acme',
      audit: (event) => {
        events.push(event);
      },
      now: () => NOW,
    })();

    // #then the legacy value has no authority; infrastructure attribution wins
    expect(result).toMatchObject({ fired: 1, failed: 0 });
    expect(start).toHaveBeenCalledOnce();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'published',
      metadata: { deploymentTag: 'acme' },
    });
    expect(events).toEqual([
      expect.objectContaining({
        deploymentTag: 'acme',
        outcome: 'published',
      }),
    ]);
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

  it('records a committed run as published when the start response is lost', async () => {
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async () => {
      throw new Error('response lost');
    });
    const status = vi.fn(async (ref) => ({
      runId: ref.runId,
      status: 'suspended',
    }));

    const result = await createScheduleTick({
      store,
      start,
      status,
      now: () => NOW,
    })();

    expect(result).toMatchObject({ fired: 1, failed: 0, deferred: 0 });
    expect(status).toHaveBeenCalledTimes(1);
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'published',
      runId: status.mock.calls[0]?.[0].runId,
      metadata: {
        reason: 'dispatch-reconciled',
        dispatchRef: expect.objectContaining({ target: 'workflow' }),
      },
    });
    expect(store.triggers[0]?.error).toBeUndefined();
  });

  it('persists an indeterminate dispatch and reconciles it on a later tick', async () => {
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async () => {
      throw new Error('response lost');
    });
    const status = vi
      .fn()
      .mockRejectedValueOnce(new Error('target unavailable'))
      .mockImplementation(async (ref) => ({
        runId: ref.runId,
        status: 'success',
      }));
    const tick = createScheduleTick({ store, start, status, now: () => NOW });

    const first = await tick();
    expect(first).toMatchObject({ fired: 0, failed: 0, deferred: 1 });
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'deferred',
      metadata: {
        reason: 'dispatch-indeterminate',
        dispatchRef: expect.objectContaining({ target: 'workflow' }),
      },
    });

    const second = await tick();
    expect(second).toMatchObject({
      due: 0,
      fired: 1,
      failed: 0,
      deferred: 0,
      reconciled: 1,
    });
    expect(start).toHaveBeenCalledTimes(1);
    expect(store.triggers).toHaveLength(1);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'published',
      metadata: { reason: 'dispatch-reconciled' },
    });
  });

  it('redispatches an indeterminate threaded signal with its canonical stable envelope', async () => {
    const store = new FakeStore();
    store.seed(
      workflowSchedule({
        id: 'agent_schedule',
        target: {
          type: 'agent',
          agentId: 'a1',
          prompt: 'go',
          threadId: 'acme_thread',
          resourceId: 'acme_resource',
          signalType: 'reactive',
          tagName: 'scheduled',
          attributes: { source: 'test' },
        },
      }),
    );
    const signalAgent = vi
      .fn(async (_input: ScheduleTickSignalAgentInput) => ({
        action: 'deliver' as const,
        outcome: 'delivered' as const,
        runId: 'active_run',
        signalId: 'unused',
      }))
      .mockRejectedValueOnce(new Error('response lost'));
    const status = vi.fn(async () => {
      throw new Error('target receipt pending');
    });
    const tick = createScheduleTick({
      store,
      start: vi.fn(),
      signalAgent,
      status,
      now: () => NOW,
    });

    expect(await tick()).toMatchObject({ deferred: 1, fired: 0 });
    const firstInput = signalAgent.mock.calls[0]?.[0];
    expect(firstInput).toBeDefined();
    expect(store.triggers[0]).toMatchObject({
      outcome: 'deferred',
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          mode: 'signal',
          agentTarget: expect.objectContaining({
            signalType: 'reactive',
            tagName: 'scheduled',
          }),
        },
      },
    });

    expect(await tick()).toMatchObject({
      due: 0,
      fired: 1,
      reconciled: 1,
      deferred: 0,
    });
    expect(signalAgent).toHaveBeenCalledTimes(2);
    expect(signalAgent.mock.calls[1]?.[0]).toEqual(firstInput);
    expect(store.triggers[0]).toMatchObject({
      outcome: 'delivered',
      runId: 'active_run',
    });
  });

  it('rotates permanently unavailable deferred rows so newer dispatches reconcile', async () => {
    const store = new FakeStore();
    for (const [index, runId] of [
      'run_bad_1',
      'run_bad_2',
      'run_good',
    ].entries()) {
      store.triggers.push({
        id: `trigger_${index}`,
        scheduleId: `schedule_${index}`,
        runId,
        scheduledFireAt: index + 1,
        actualFireAt: index + 1,
        outcome: 'deferred',
        metadata: {
          dispatchRef: {
            scheduleId: `schedule_${index}`,
            dispatchId: `trigger_${index}`,
            target: 'workflow',
            workflowId: 'wf',
            runId,
            workflowTarget: { type: 'workflow', workflowId: 'wf' },
          },
          reconcileAttempts: 0,
          reconcileAfter: index + 1,
        },
      });
    }
    const status = vi.fn(async (ref) => {
      if (ref.runId.startsWith('run_bad')) {
        throw new Error('target unavailable');
      }
      return { runId: ref.runId, status: 'success' };
    });
    const tick = createScheduleTick({
      store,
      start: async ({ runId }) => ({ runId }),
      status,
      limit: 2,
      now: () => NOW,
    });

    expect(await tick()).toMatchObject({ deferred: 2, reconciled: 0 });
    expect(await tick()).toMatchObject({ fired: 1, reconciled: 1 });
    expect(
      store.triggers.find((trigger) => trigger.runId === 'run_good'),
    ).toMatchObject({ outcome: 'published' });
  });

  it('allows one claim across two concurrent ticks (in-process CAS)', async () => {
    // #given one due schedule and two ticks sharing the store
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }) => ({ runId }));
    const tick = createScheduleTick({ store, start, now: () => NOW });

    // #when both ticks run concurrently
    const [a, b] = await Promise.all([tick(), tick()]);

    // #then exactly one fired and one lost the CAS
    expect(a.fired + b.fired).toBe(1);
    expect(a.lost + b.lost).toBe(1);
    expect(start).toHaveBeenCalledTimes(1);
    expect(
      store.triggers.filter((t) => t.outcome === 'published'),
    ).toHaveLength(1);
  });

  it.each([
    'concurrent-claim',
    'paused',
    'disappeared',
  ] as const)('reloads and records a lost CAS as %s', async (reason) => {
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const events: ScheduleTickAuditEvent[] = [];
    const losingStore: ScheduleTickStore = {
      listDueSchedules: (now, limit) => store.listDueSchedules(now, limit),
      getSchedule: (id) => store.getSchedule(id),
      claimScheduleFire: async ({ scheduleId: id }) => {
        const current = store.schedules.get(id);
        if (reason === 'disappeared') store.schedules.delete(id);
        else if (current) {
          store.schedules.set(id, {
            ...current,
            ...(reason === 'paused'
              ? { status: 'paused' as const }
              : { nextFireAt: NOW + 60_000 }),
          });
        }
        return false;
      },
      recordTrigger: (trigger) => store.recordTrigger(trigger),
      touchDeferredTrigger: (id, scheduleId, error, metadata) =>
        store.touchDeferredTrigger(id, scheduleId, error, metadata),
      listDeferredTriggers: (limit) => store.listDeferredTriggers(limit),
    };

    const result = await createScheduleTick({
      store: losingStore,
      start: vi.fn(),
      audit: (event) => {
        events.push(event);
      },
      now: () => NOW,
    })();

    expect(result).toMatchObject({ lost: 1, fired: 0, failed: 0 });
    expect(store.triggers).toHaveLength(0);
    expect(events[0]).toMatchObject({
      outcome: 'lost',
      reason: `lost: ${reason}`,
    });
  });

  it('a post-dispatch bookkeeping failure is NOT reclassified as a start failure (M1)', async () => {
    // #given a due schedule whose `published` recordTrigger throws AFTER start
    // succeeded (a transient D1 write error post-dispatch)
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }) => ({ runId }));
    const events: ScheduleTickAuditEvent[] = [];
    const throwingStore: ScheduleTickStore = {
      listDueSchedules: (n, l) => store.listDueSchedules(n, l),
      getSchedule: (id) => store.getSchedule(id),
      claimScheduleFire: (claim) => store.claimScheduleFire(claim),
      recordTrigger: async (trigger) => {
        if (trigger.outcome === 'published') {
          throw new Error('D1 write failed post-dispatch');
        }
        await store.recordTrigger(trigger);
      },
      touchDeferredTrigger: (id, scheduleId, error, metadata) =>
        store.touchDeferredTrigger(id, scheduleId, error, metadata),
      listDeferredTriggers: (limit) => store.listDeferredTriggers(limit),
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
      getSchedule: (id) => store.getSchedule(id),
      claimScheduleFire: (claim) => {
        claims += 1;
        if (claims === 1) throw new Error('transient store failure');
        return store.claimScheduleFire(claim);
      },
      recordTrigger: (t) => store.recordTrigger(t),
      touchDeferredTrigger: (id, scheduleId, error, metadata) =>
        store.touchDeferredTrigger(id, scheduleId, error, metadata),
      listDeferredTriggers: (limit) => store.listDeferredTriggers(limit),
    };
    const start = vi.fn(async ({ runId }) => ({ runId }));

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
    expect(isReservedScheduleContextKey('breakwater.connectorGrants')).toBe(
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

  it('drops object meta-keys parsed from JSON without changing the result prototype', () => {
    const stored = JSON.parse(
      '{"__proto__":{"breakwater.connectorGrants":["forged"]},"constructor":"bad","prototype":"bad","safe":1}',
    ) as Record<string, unknown>;
    const safe = stripReservedScheduleContext(stored);

    expect(safe).toEqual({ safe: 1 });
    expect(Object.getPrototypeOf(safe)).toBe(Object.prototype);
    expect(Object.hasOwn(safe, '__proto__')).toBe(false);
    expect(
      (safe as { breakwater?: { connectorGrants?: unknown[] } }).breakwater,
    ).toBeUndefined();
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

describe('createScheduleTick and the deployment execution fence', () => {
  function fence(): ExecutionFenceStore {
    return new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
  }

  it('leaves a due fire UNCLAIMED on a fenced pass, then fires it after reopen', async () => {
    // #given — a due schedule on a deployment that just started draining.
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }: { runId: string }) => ({ runId }));
    const executionFence = fence();
    await executionFence.seed('draining');
    const tick = createScheduleTick({
      store,
      start,
      executionFence,
      now: () => NOW,
    });

    // #when
    const fenced = await tick();

    // #then — the pass did nothing at all. Claiming a fire it will not run
    // would CONSUME it (the claim advances nextFireAt), so the fire would be
    // lost rather than deferred.
    expect(fenced).toEqual({
      due: 0,
      fired: 0,
      skipped: 0,
      failed: 0,
      deferred: 0,
      reconciled: 0,
      lost: 0,
    });
    expect(start).not.toHaveBeenCalled();
    expect(store.triggers).toEqual([]);
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBe(NOW - 1000);

    // #when — the migration finishes and the fence reopens.
    await executionFence.transition({ expected: 'draining', next: 'open' });
    const reopened = await tick();

    // #then — the SAME fire runs, exactly once: neither lost nor duplicated.
    expect(reopened.due).toBe(1);
    expect(reopened.fired).toBe(1);
    expect(start).toHaveBeenCalledTimes(1);
  });

  it('skips the pass rather than claiming when the fence cannot be read', async () => {
    // #given — a fence whose storage is down. This runs on a maintenance
    // alarm, so degrading closed means doing NOTHING, not throwing: a throw
    // would fail the duty, and proceeding would claim fires on a deployment
    // whose state is unknown.
    const store = new FakeStore();
    store.seed(workflowSchedule());
    const start = vi.fn(async ({ runId }: { runId: string }) => ({ runId }));
    const unreadable = new ExecutionFenceStore({
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
    const log = vi.spyOn(console, 'error').mockImplementation(() => undefined);

    // #when
    let result: Awaited<ReturnType<ReturnType<typeof createScheduleTick>>>;
    try {
      result = await createScheduleTick({
        store,
        start,
        executionFence: unreadable,
        now: () => NOW,
      })();
    } finally {
      log.mockRestore();
    }

    // #then
    expect(result.due).toBe(0);
    expect(start).not.toHaveBeenCalled();
    expect(store.schedules.get('schedule_a')?.nextFireAt).toBe(NOW - 1000);
  });
});
