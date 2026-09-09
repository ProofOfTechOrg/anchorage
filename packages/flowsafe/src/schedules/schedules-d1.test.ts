// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — fast schedule SQL units over node:sqlite. The Wrangler
// harness owns real-D1 CAS, concurrency, ownership, and rollback evidence.

import type { Schedule } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  D1ResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/index.js';
import {
  InvalidMutationEpochError,
  type MutationEpochContext,
  MutationEpochMismatchError,
} from '../do-runner/execution-admission.js';
import {
  ExecutionFencedError,
  type ExecutionFenceState,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
} from '../do-runner/execution-fence.js';
import {
  FENCED_SCHEDULE_STORAGE,
  ScheduleMutationConflictError,
  ScheduleMutationOutcomeUnknownError,
} from './mutation-contract.js';
import {
  D1SchedulesStorage,
  parseScheduleAgentDispatchReceipt,
  type ScheduleDatabase,
} from './schedules-d1.js';
import { scheduleWithCreatorRole } from './target-policy.js';

function storeOver(): {
  store: D1SchedulesStorage;
  sqlite: ReturnType<typeof openSqlite>;
} {
  const sqlite = openSqlite();
  const store = new D1SchedulesStorage(
    sqliteUnitDatabase(sqlite) as unknown as ScheduleDatabase,
  );
  return { store, sqlite };
}

const NOW = Date.parse('2026-07-17T12:00:00.000Z');

function workflowSchedule(overrides: Partial<Schedule> = {}): Schedule {
  return {
    id: 'schedule_a',
    target: { type: 'workflow', workflowId: 'wf', inputData: { topic: 'x' } },
    cron: '* * * * *',
    status: 'active',
    nextFireAt: NOW,
    createdAt: NOW,
    updatedAt: NOW,
    metadata: {},
    ...overrides,
  };
}

async function mutationFixture() {
  const sqlite = openSqlite();
  const native = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
    Required<Pick<ScheduleDatabase, 'batch'>>;
  const hooks: {
    beforeBatch?: () => void | Promise<void>;
    afterBatch?: (results: unknown[]) => unknown[];
  } = {};
  const sql: string[] = [];
  let batches = 0;
  const binding: ScheduleDatabase = {
    prepare(query) {
      expect(this).toBe(binding);
      sql.push(query);
      return native.prepare(query);
    },
    async batch(statements) {
      expect(this).toBe(binding);
      batches += 1;
      const before = hooks.beforeBatch;
      hooks.beforeBatch = undefined;
      await before?.();
      const results = await native.batch(statements);
      return hooks.afterBatch ? hooks.afterBatch(results) : results;
    },
  };
  const store = new D1SchedulesStorage(binding);
  const fence = new ExecutionFenceStore(binding);
  await store.createOwnedSchedule(
    scheduleWithCreatorRole(workflowSchedule(), 'operator'),
    { kind: 'human', id: 'opal' },
    100,
  );
  return {
    sqlite,
    store,
    fence,
    binding,
    native,
    hooks,
    sql,
    batches: () => batches,
  };
}

type MutationFixture = Awaited<ReturnType<typeof mutationFixture>>;

async function moveFence(
  fence: ExecutionFenceStore,
  next: ExecutionFenceState,
  advanceMutationEpoch = false,
) {
  const current = await fence.read();
  return fence.transition({
    expected: current.state,
    next,
    expectedMutationEpoch: current.mutationEpoch,
    expectedRevision: current.transitionRevision,
    advanceMutationEpoch,
    ...(next === 'proof-only' ? { proofKey: 'schedule-proof' } : {}),
  });
}

async function activateFence(fence: ExecutionFenceStore) {
  await moveFence(fence, 'draining', true);
  await moveFence(fence, 'open');
}

function rawScheduleState(fixture: MutationFixture) {
  return [
    fixture.sqlite.prepare('SELECT * FROM mastra_schedules ORDER BY id').all(),
    fixture.sqlite
      .prepare('SELECT * FROM mastra_schedule_triggers ORDER BY id')
      .all(),
    fixture.sqlite
      .prepare(
        'SELECT * FROM flowsafe_resource_owners ORDER BY resource_kind, resource_id',
      )
      .all(),
  ];
}

const mutationCases: Array<{
  name: string;
  operation: 'author' | 'drain';
  run: (
    fixture: MutationFixture,
    context?: MutationEpochContext,
  ) => Promise<unknown>;
}> = [
  {
    name: 'create',
    operation: 'author',
    run: (f, context) =>
      f.store.createSchedule(workflowSchedule({ id: 'created' }), context),
  },
  {
    name: 'owned create',
    operation: 'author',
    run: (f, context) =>
      f.store.createOwnedSchedule(
        scheduleWithCreatorRole(
          workflowSchedule({ id: 'created' }),
          'operator',
        ),
        { kind: 'human', id: 'opal' },
        100,
        context,
      ),
  },
  {
    name: 'update',
    operation: 'author',
    run: (f, context) =>
      f.store.updateSchedule(
        'schedule_a',
        { metadata: { changed: true } },
        context,
      ),
  },
  {
    name: 'pause',
    operation: 'drain',
    run: (f, context) => f.store.pauseSchedule('schedule_a', context),
  },
  {
    name: 'resume',
    operation: 'author',
    run: (f, context) =>
      f.store.resumeSchedule(
        'schedule_a',
        {
          expectedCron: '* * * * *',
          expectedTimezone: undefined,
          nextFireAt: NOW + 60_000,
        },
        context,
      ),
  },
  {
    name: 'delete',
    operation: 'drain',
    run: (f, context) => f.store.deleteSchedule('schedule_a', context),
  },
  {
    name: 'owned delete',
    operation: 'drain',
    run: (f, context) => f.store.deleteOwnedSchedule('schedule_a', context),
  },
  {
    name: 'pause observation',
    operation: 'drain',
    run: (f, context) =>
      f.store.observeScheduleMutation('schedule_a', 'pause', context ?? {}),
  },
  {
    name: 'resume observation',
    operation: 'author',
    run: (f, context) =>
      f.store.observeScheduleMutation('schedule_a', 'resume', context ?? {}),
  },
];

describe('schedule agent dispatch receipts', () => {
  it.each([
    ['wake', 'succeeded'],
    ['deliver', 'delivered'],
    ['persist', 'persisted'],
    ['discard', 'discarded'],
    ['blocked', 'skipped'],
  ] as const)('accepts the canonical %s/%s pair', (action, outcome) => {
    expect(
      parseScheduleAgentDispatchReceipt({
        action,
        outcome,
        runId: 'run-1',
        signalId: 'signal-1',
        ignored: true,
      }),
    ).toEqual({ action, outcome, runId: 'run-1', signalId: 'signal-1' });
  });

  it('rejects mismatched action/outcome pairs', () => {
    expect(
      parseScheduleAgentDispatchReceipt({
        action: 'wake',
        outcome: 'delivered',
      }),
    ).toBeUndefined();
  });

  it.each([
    { runId: 'run/unsafe' },
    { runId: 123 },
    { signalId: 'signal:unsafe' },
    { signalId: 123 },
  ])('rejects a non-path-safe receipt id: %j', (invalidId) => {
    expect(
      parseScheduleAgentDispatchReceipt({
        action: 'wake',
        outcome: 'succeeded',
        ...invalidId,
      }),
    ).toBeUndefined();
  });
});

describe('D1SchedulesStorage', () => {
  it('adds creatorRole to an existing schedules schema without rewriting rows', async () => {
    const sqlite = openSqlite();
    sqlite.exec(`CREATE TABLE mastra_schedules (
      id TEXT PRIMARY KEY,
      target TEXT NOT NULL,
      cron TEXT NOT NULL,
      timezone TEXT,
      status TEXT NOT NULL,
      nextFireAt INTEGER NOT NULL,
      lastFireAt INTEGER,
      lastRunId TEXT,
      createdAt INTEGER NOT NULL,
      updatedAt INTEGER NOT NULL,
      metadata TEXT,
      ownerType TEXT,
      ownerId TEXT
    )`);
    const store = new D1SchedulesStorage(
      sqliteUnitDatabase(sqlite) as ScheduleDatabase,
    );

    await store.init();

    const columns = sqlite
      .prepare('PRAGMA table_info(mastra_schedules)')
      .all() as Array<{ name: string }>;
    expect(columns.map((column) => column.name)).toContain('creatorRole');
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createSchedule(schedule);
    expect(await store.getSchedule(schedule.id)).toEqual(schedule);
  });

  it('round-trips a schedule row incl. target JSON and metadata', async () => {
    // #given
    const { store } = storeOver();
    const schedule = workflowSchedule();

    // #when
    await store.createSchedule(schedule);
    const loaded = await store.getSchedule('schedule_a');

    // #then — the stored row deserializes byte-equal (target + metadata JSON)
    expect(loaded).toEqual(schedule);
  });

  it('throws on a duplicate id (core createSchedule contract)', async () => {
    // #given a persisted schedule
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());

    // #then a second create with the same id throws
    await expect(store.createSchedule(workflowSchedule())).rejects.toThrow(
      /already exists/,
    );
  });

  it('atomically creates an owned schedule and persists its creator role', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const owner = { kind: 'human', id: 'opal' } as const;
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');

    const created = await store.createOwnedSchedule(schedule, owner, 1);

    expect(created).toEqual(schedule);
    expect(await store.getSchedule(schedule.id)).toEqual(schedule);
    expect(await resources.owner('schedule', schedule.id)).toEqual(owner);
  });

  it('enforces the schedule cap under concurrent creates without orphan owners', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const owner = { kind: 'human', id: 'opal' } as const;
    const first = scheduleWithCreatorRole(
      workflowSchedule({ id: 'schedule_first' }),
      'operator',
    );
    const second = scheduleWithCreatorRole(
      workflowSchedule({ id: 'schedule_second' }),
      'operator',
    );

    const outcomes = await Promise.all([
      store.createOwnedSchedule(first, owner, 1),
      store.createOwnedSchedule(second, owner, 1),
    ]);

    expect(outcomes.filter(Boolean)).toHaveLength(1);
    expect(await store.listSchedules()).toHaveLength(1);
    const winner = outcomes.find(Boolean) as Schedule;
    const loser = winner.id === first.id ? second : first;
    expect(await resources.owner('schedule', winner.id)).toEqual(owner);
    expect(await resources.owner('schedule', loser.id)).toBeUndefined();
  });

  it('does not adopt an existing unowned schedule when the cap rejects the insert', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createSchedule(schedule);

    const created = await store.createOwnedSchedule(
      schedule,
      { kind: 'human', id: 'opal' },
      1,
    );

    expect(created).toBeNull();
    expect(await resources.owner('schedule', schedule.id)).toBeUndefined();
  });

  it('rolls back the schedule row when its owner insert fails', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    await store.init();
    sqlite.exec(`CREATE TRIGGER reject_schedule_owner_insert
      BEFORE INSERT ON flowsafe_resource_owners
      WHEN NEW.resource_kind = 'schedule'
      BEGIN SELECT RAISE(ABORT, 'injected owner failure'); END`);
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');

    await expect(
      store.createOwnedSchedule(schedule, { kind: 'human', id: 'opal' }, 100),
    ).rejects.toMatchObject({
      name: 'ScheduleMutationOutcomeUnknownError',
      cause: expect.objectContaining({
        message: expect.stringMatching(/injected owner failure/),
      }),
    });

    expect(await store.getSchedule(schedule.id)).toBeNull();
  });

  it('listDueSchedules returns only active rows with nextFireAt <= now', async () => {
    // #given — one due active, one future active, one due but PAUSED
    const { store } = storeOver();
    await store.createSchedule(
      workflowSchedule({ id: 'schedule_due', nextFireAt: NOW - 1000 }),
    );
    await store.createSchedule(
      workflowSchedule({ id: 'schedule_future', nextFireAt: NOW + 60_000 }),
    );
    await store.createSchedule(
      workflowSchedule({
        id: 'schedule_paused',
        status: 'paused',
        nextFireAt: NOW - 1000,
      }),
    );

    // #when
    const due = await store.listDueSchedules(NOW);

    // #then — only the due ACTIVE row (paused + future excluded)
    expect(due.map((s) => s.id)).toEqual(['schedule_due']);
  });

  it('updateScheduleNextFire is a CAS: the expected value must still match', async () => {
    // #given a schedule at nextFireAt = NOW
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule({ nextFireAt: NOW }));

    // #when the CAS is attempted with the CURRENT expected value
    const won = await store.updateScheduleNextFire(
      'schedule_a',
      NOW,
      NOW + 60_000,
      NOW,
      'acme_run1',
    );

    // #then it wins, and nextFireAt/lastRunId advanced
    expect(won).toBe(true);
    const after = await store.getSchedule('schedule_a');
    expect(after?.nextFireAt).toBe(NOW + 60_000);
    expect(after?.lastRunId).toBe('acme_run1');
    expect(after?.lastFireAt).toBe(NOW);
  });

  it('updateScheduleNextFire LOSES when the expected value no longer matches (the concurrent-tick loser)', async () => {
    // #given a schedule the FIRST tick already advanced
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule({ nextFireAt: NOW }));
    const first = await store.updateScheduleNextFire(
      'schedule_a',
      NOW,
      NOW + 60_000,
      NOW,
      'acme_run1',
    );
    expect(first).toBe(true);

    // #when a SECOND tick tries to claim the SAME original fire (expected = NOW)
    const second = await store.updateScheduleNextFire(
      'schedule_a',
      NOW,
      NOW + 60_000,
      NOW,
      'acme_run2',
    );

    // #then it loses the single CAS claim, and the winner's runId is untouched
    expect(second).toBe(false);
    const after = await store.getSchedule('schedule_a');
    expect(after?.lastRunId).toBe('acme_run1');
  });

  it('records and lists trigger history newest-first, honoring the limit', async () => {
    // #given three triggers at increasing actualFireAt
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    for (const [i, at] of [NOW, NOW + 1000, NOW + 2000].entries()) {
      await store.recordTrigger({
        id: `t${i}`,
        scheduleId: 'schedule_a',
        runId: `acme_r${i}`,
        scheduledFireAt: at,
        actualFireAt: at,
        outcome: 'published',
        metadata: {},
      });
    }

    // #when
    const all = await store.listTriggers('schedule_a');
    const limited = await store.listTriggers('schedule_a', { limit: 2 });

    // #then — newest first, and the limit clamps
    expect(all.map((t) => t.id)).toEqual(['t2', 't1', 't0']);
    expect(limited.map((t) => t.id)).toEqual(['t2', 't1']);
  });

  it('recordTrigger generates an id when the trigger omits one', async () => {
    // #given a trigger with no id
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      scheduleId: 'schedule_a',
      runId: null,
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'skipped',
      metadata: { reason: 'run-capped' },
    });

    // #then — one row with a generated id and the null runId preserved
    const triggers = await store.listTriggers('schedule_a');
    expect(triggers).toHaveLength(1);
    expect(typeof triggers[0]?.id).toBe('string');
    expect(triggers[0]?.runId).toBeNull();
    expect(triggers[0]?.outcome).toBe('skipped');
    expect(triggers[0]?.triggerKind).toBe('schedule-fire');
  });

  it('preserves explicit trigger kind and parent correlation', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      id: 'queue-drain',
      scheduleId: 'schedule_a',
      runId: 'run_queue',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
      triggerKind: 'queue-drain',
      parentTriggerId: 'schedule-fire-parent',
      metadata: { queue: 'critical' },
    });

    expect(await store.listTriggers('schedule_a')).toEqual([
      expect.objectContaining({
        id: 'queue-drain',
        triggerKind: 'queue-drain',
        parentTriggerId: 'schedule-fire-parent',
        metadata: { queue: 'critical' },
      }),
    ]);
  });

  it('reads legacy null trigger kinds as schedule fires', async () => {
    const { store, sqlite } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      id: 'legacy-trigger',
      scheduleId: 'schedule_a',
      runId: 'legacy-run',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
    });
    sqlite
      .prepare(
        'UPDATE mastra_schedule_triggers SET triggerKind = NULL WHERE id = ?',
      )
      .run('legacy-trigger');

    expect((await store.listTriggers('schedule_a'))[0]?.triggerKind).toBe(
      'schedule-fire',
    );
  });

  it('lists only deferred dispatches and removes them after reconciliation', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      id: 'pending',
      scheduleId: 'schedule_a',
      runId: 'run_pending',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: { dispatchRef: { target: 'workflow' } },
    });
    await store.recordTrigger({
      id: 'complete',
      scheduleId: 'schedule_a',
      runId: 'run_complete',
      scheduledFireAt: NOW,
      actualFireAt: NOW + 1,
      outcome: 'published',
      metadata: {},
    });

    expect((await store.listDeferredTriggers()).map((row) => row.id)).toEqual([
      'pending',
    ]);

    await store.recordTrigger({
      id: 'pending',
      scheduleId: 'schedule_a',
      runId: 'run_pending',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
      metadata: { reason: 'dispatch-reconciled' },
    });
    expect(await store.listDeferredTriggers()).toEqual([]);
  });

  it('resolves only the exact prepared schedule fire tuple', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    const trigger = {
      id: 'dispatch-1',
      scheduleId: 'schedule_a',
      runId: 'run-1',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred' as const,
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          scheduleId: 'schedule_a',
          dispatchId: 'dispatch-1',
          runId: 'run-1',
          target: 'workflow',
          workflowId: 'wf',
        },
      },
    };
    await store.recordTrigger(trigger);

    await expect(
      store.getClaimedScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        trigger.runId,
      ),
    ).resolves.toMatchObject(trigger);

    for (const dispatchState of ['executing', 'settled'] as const) {
      const inFlight = {
        ...trigger,
        metadata: { ...trigger.metadata, dispatchState },
      };
      await store.recordTrigger(inFlight);
      await expect(
        store.getClaimedScheduleDispatch(
          trigger.scheduleId,
          trigger.id,
          trigger.runId,
        ),
      ).resolves.toMatchObject(inFlight);
    }
    await expect(
      store.getClaimedScheduleDispatch(
        trigger.scheduleId,
        'another-dispatch',
        trigger.runId,
      ),
    ).resolves.toBeNull();
    await expect(
      store.getClaimedScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        'another-run',
      ),
    ).resolves.toBeNull();

    await store.recordTrigger({ ...trigger, outcome: 'published' });
    await expect(
      store.getClaimedScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        trigger.runId,
      ),
    ).resolves.toBeNull();
  });

  it('force-discards a same-run wake receipt and refuses unrelated receipts', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      id: 'dispatch-force',
      scheduleId: 'schedule_a',
      runId: 'run-force',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: {
        dispatchState: 'prepared',
        dispatchRef: {
          scheduleId: 'schedule_a',
          dispatchId: 'dispatch-force',
          runId: 'run-force',
          target: 'agent',
          mode: 'start',
          agentId: 'writer',
        },
      },
    });
    await store.beginAgentScheduleDispatch('schedule_a', 'dispatch-force', NOW);
    await store.settleAgentScheduleDispatch('schedule_a', 'dispatch-force', {
      action: 'wake',
      outcome: 'succeeded',
      runId: 'run-force',
      signalId: 'dispatch-force',
    });

    await expect(
      store.discardAgentScheduleDispatch(
        'schedule_a',
        'dispatch-force',
        'run-force',
      ),
    ).resolves.toBeUndefined();
    await expect(
      store.discardAgentScheduleDispatch(
        'schedule_a',
        'dispatch-force',
        'run-force',
      ),
    ).resolves.toBeUndefined();
    await expect(
      store.agentScheduleDispatchState('schedule_a', 'dispatch-force'),
    ).resolves.toEqual({
      state: 'settled',
      receipt: {
        action: 'discard',
        outcome: 'discarded',
        runId: 'run-force',
      },
    });
    await expect(
      store.discardAgentScheduleDispatch(
        'schedule_a',
        'dispatch-force',
        'another-run',
      ),
    ).rejects.toThrow('belongs to another schedule or run');
  });

  it('treats exact-run final bookkeeping or a missing trigger as converged', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    const trigger = {
      id: 'dispatch-final-first',
      scheduleId: 'schedule_a',
      runId: 'run-final-first',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'succeeded' as const,
      metadata: { action: 'wake' },
    };
    await store.recordTrigger(trigger);

    await expect(
      store.discardAgentScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        trigger.runId,
      ),
    ).resolves.toBeUndefined();
    await expect(
      store.discardAgentScheduleDispatch(
        trigger.scheduleId,
        'already-retained-away',
        trigger.runId,
      ),
    ).resolves.toBeUndefined();

    await store.recordTrigger({ ...trigger, runId: 'another-run' });
    await expect(
      store.discardAgentScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        trigger.runId,
      ),
    ).rejects.toThrow('belongs to another schedule or run');

    await store.createSchedule(workflowSchedule({ id: 'schedule_b' }));
    await store.recordTrigger({
      ...trigger,
      scheduleId: 'schedule_b',
      runId: trigger.runId,
    });
    await expect(
      store.discardAgentScheduleDispatch(
        trigger.scheduleId,
        trigger.id,
        trigger.runId,
      ),
    ).rejects.toThrow('belongs to another schedule or run');
  });

  it('orders deferred dispatches by their durable reconciliation cursor', async () => {
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.createSchedule(workflowSchedule({ id: 'schedule_b' }));
    await store.recordTrigger({
      id: 'retry-later',
      scheduleId: 'schedule_a',
      runId: 'run_later',
      scheduledFireAt: NOW - 1,
      actualFireAt: NOW - 1,
      outcome: 'deferred',
      metadata: { reconcileAfter: NOW + 100 },
    });
    await store.recordTrigger({
      id: 'newer-ready',
      scheduleId: 'schedule_b',
      runId: 'run_ready',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: { reconcileAfter: NOW },
    });

    expect((await store.listDeferredTriggers(1))[0]?.id).toBe('newer-ready');
  });

  it('deleteSchedule removes the schedule AND its trigger history', async () => {
    // #given a schedule with a trigger
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      scheduleId: 'schedule_a',
      runId: 'acme_r1',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
    });

    // #when
    await store.deleteSchedule('schedule_a');

    // #then — both gone
    expect(await store.getSchedule('schedule_a')).toBeNull();
    expect(await store.listTriggers('schedule_a')).toEqual([]);
  });

  it('atomically deletes an owned schedule, triggers, and its owner row', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createOwnedSchedule(
      schedule,
      { kind: 'human', id: 'opal' },
      100,
    );
    await store.recordTrigger({
      id: 'trigger-owned',
      scheduleId: schedule.id,
      runId: 'run-owned',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
    });

    await store.deleteSchedule(schedule.id);

    expect(await store.getSchedule(schedule.id)).toBeNull();
    expect(await store.listTriggers(schedule.id)).toEqual([]);
    expect(await resources.owner('schedule', schedule.id)).toBeUndefined();
  });

  it('a delete that wins before the fire claim leaves no provisional trigger', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createOwnedSchedule(
      schedule,
      { kind: 'human', id: 'opal' },
      100,
    );

    expect(await store.deleteOwnedSchedule(schedule.id)).toBe('deleted');
    const claimed = await store.claimScheduleFire({
      scheduleId: schedule.id,
      expectedNextFireAt: NOW,
      newNextFireAt: NOW + 60_000,
      actualFireAt: NOW,
      runId: 'run-after-delete',
      trigger: {
        id: 'trigger-after-delete',
        scheduleId: schedule.id,
        runId: 'run-after-delete',
        scheduledFireAt: NOW,
        actualFireAt: NOW,
        outcome: 'deferred',
      },
    });

    expect(claimed).toBe(false);
    expect(await store.listTriggers(schedule.id)).toEqual([]);
    expect(await resources.owner('schedule', schedule.id)).toBeUndefined();
  });

  it.each([
    'published',
    'failed',
  ] as const)('a delete-requested %s dispatch settles and finalizes all schedule state', async (outcome) => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const owner = { kind: 'human', id: 'opal' } as const;
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createOwnedSchedule(schedule, owner, 100);
    const trigger = {
      id: 'trigger-indeterminate',
      scheduleId: schedule.id,
      runId: 'run-indeterminate',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred' as const,
      metadata: { reason: 'dispatch-indeterminate' },
    };
    expect(
      await store.claimScheduleFire({
        scheduleId: schedule.id,
        expectedNextFireAt: NOW,
        newNextFireAt: NOW + 60_000,
        actualFireAt: NOW,
        runId: 'run-indeterminate',
        trigger,
      }),
    ).toBe(true);

    expect(await store.deleteOwnedSchedule(schedule.id)).toBe('pending');
    expect(await store.getSchedule(schedule.id)).toBeNull();
    expect(await store.listTriggers(schedule.id)).toEqual([
      { ...trigger, triggerKind: 'schedule-fire' },
    ]);
    expect(await resources.owner('schedule', schedule.id)).toEqual(owner);

    const settled = {
      ...trigger,
      outcome,
      ...(outcome === 'failed' ? { error: 'authoritatively absent' } : {}),
    };
    await store.recordTrigger(settled);

    expect(await store.listTriggers(schedule.id)).toEqual([]);
    expect(await resources.owner('schedule', schedule.id)).toBeUndefined();
    expect(
      sqlite
        .prepare('SELECT COUNT(*) AS count FROM mastra_schedules WHERE id = ?')
        .get(schedule.id),
    ).toEqual({ count: 0 });

    // A lost acknowledgement may replay settlement. It must remain an
    // idempotent no-op and never resurrect trigger or ownership rows.
    await store.recordTrigger(settled);
    expect(await store.listTriggers(schedule.id)).toEqual([]);
    expect(await resources.owner('schedule', schedule.id)).toBeUndefined();
  });

  it('dangerouslyClearAll removes every schedule owner with the domain rows', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const owner = { kind: 'human', id: 'opal' } as const;
    for (const id of ['schedule_first', 'schedule_second']) {
      await store.createOwnedSchedule(
        scheduleWithCreatorRole(workflowSchedule({ id }), 'operator'),
        owner,
        100,
      );
    }

    await store.dangerouslyClearAll();

    expect(await store.listSchedules()).toEqual([]);
    expect(await resources.owner('schedule', 'schedule_first')).toBeUndefined();
    expect(
      await resources.owner('schedule', 'schedule_second'),
    ).toBeUndefined();
  });

  it('rolls back owned deletion when owner cleanup fails', async () => {
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase &
      ResourceOwnershipDatabase;
    const store = new D1SchedulesStorage(binding);
    const resources = new D1ResourceOwnershipStore(binding);
    const owner = { kind: 'human', id: 'opal' } as const;
    const schedule = scheduleWithCreatorRole(workflowSchedule(), 'operator');
    await store.createOwnedSchedule(schedule, owner, 100);
    sqlite.exec(`CREATE TRIGGER reject_schedule_owner_delete
      BEFORE DELETE ON flowsafe_resource_owners
      WHEN OLD.resource_kind = 'schedule'
      BEGIN SELECT RAISE(ABORT, 'injected owner delete failure'); END`);

    await expect(store.deleteOwnedSchedule(schedule.id)).rejects.toMatchObject({
      name: 'ScheduleMutationOutcomeUnknownError',
      cause: expect.objectContaining({
        message: expect.stringMatching(/injected owner delete failure/),
      }),
    });

    expect(await store.getSchedule(schedule.id)).toEqual(schedule);
    expect(await resources.owner('schedule', schedule.id)).toEqual(owner);
  });

  it('rolls back schedule and trigger deletion together when one statement fails', async () => {
    const { store, sqlite } = storeOver();
    await store.createSchedule(workflowSchedule());
    await store.recordTrigger({
      id: 'trigger-1',
      scheduleId: 'schedule_a',
      runId: 'run-1',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
    });
    sqlite.exec(`CREATE TRIGGER reject_schedule_delete
      BEFORE DELETE ON mastra_schedules
      BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END`);

    await expect(store.deleteSchedule('schedule_a')).rejects.toMatchObject({
      name: 'ScheduleMutationOutcomeUnknownError',
      cause: expect.objectContaining({
        message: expect.stringMatching(/injected delete failure/),
      }),
    });
    expect(await store.getSchedule('schedule_a')).not.toBeNull();
    expect(await store.listTriggers('schedule_a')).toHaveLength(1);
  });

  it('updateSchedule patches fields and bumps updatedAt', async () => {
    // #given
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule({ status: 'active' }));

    // #when
    const updated = await store.updateSchedule('schedule_a', {
      status: 'paused',
    });

    // #then
    expect(updated.status).toBe('paused');
    expect(updated.updatedAt).toBeGreaterThanOrEqual(NOW);
    expect((await store.getSchedule('schedule_a'))?.status).toBe('paused');
  });

  it('listSchedules filters on status and (workflowId in the JSON target)', async () => {
    // #given schedules for two workflows, one paused
    const { store } = storeOver();
    await store.createSchedule(
      workflowSchedule({
        id: 'schedule_wf1',
        target: { type: 'workflow', workflowId: 'wf1' },
      }),
    );
    await store.createSchedule(
      workflowSchedule({
        id: 'schedule_wf2',
        status: 'paused',
        target: { type: 'workflow', workflowId: 'wf2' },
      }),
    );

    // #then
    expect(
      (await store.listSchedules({ status: 'active' })).map((s) => s.id),
    ).toEqual(['schedule_wf1']);
    expect(
      (await store.listSchedules({ workflowId: 'wf2' })).map((s) => s.id),
    ).toEqual(['schedule_wf2']);
  });

  it('updateSchedule is a TARGETED update — it never clobbers CAS-advanced columns it was not asked to change (H1)', async () => {
    // #given a schedule the tick has just CAS-claimed (nextFireAt + lastRunId advanced)
    const { store } = storeOver();
    await store.createSchedule(workflowSchedule({ nextFireAt: NOW }));
    await store.updateScheduleNextFire(
      'schedule_a',
      NOW,
      NOW + 60_000,
      NOW,
      'acme_run1',
    );

    // #when a facade metadata-only PATCH lands (its own read saw the pre-CAS row)
    await store.updateSchedule('schedule_a', {
      metadata: { note: 'x' },
    });

    // #then the CAS-owned nextFireAt/lastRunId SURVIVE (a full-row replace would
    // have reverted them and re-armed the schedule for a double-fire); only the
    // metadata changed
    const after = await store.getSchedule('schedule_a');
    expect(after?.nextFireAt).toBe(NOW + 60_000);
    expect(after?.lastRunId).toBe('acme_run1');
    expect((after?.metadata as { note?: string }).note).toBe('x');
  });

  it('updateScheduleNextFire refuses to claim a PAUSED schedule (M4 pause race)', async () => {
    // #given a schedule paused AFTER a tick read it as due but BEFORE the claim
    const { store } = storeOver();
    await store.createSchedule(
      workflowSchedule({ nextFireAt: NOW, status: 'active' }),
    );
    await store.updateSchedule('schedule_a', { status: 'paused' });

    // #when the in-flight tick tries to claim the (still nextFireAt-matching) row
    const claimed = await store.updateScheduleNextFire(
      'schedule_a',
      NOW,
      NOW + 60_000,
      NOW,
      'acme_run1',
    );

    // #then the CAS fails (status is no longer active) — a just-paused schedule
    // does not fire one last time; nothing advanced
    expect(claimed).toBe(false);
    expect((await store.getSchedule('schedule_a'))?.nextFireAt).toBe(NOW);
  });
});

describe('schedule mutation epochs', () => {
  for (const testCase of mutationCases) {
    it(`${testCase.name} retains optional-epoch compatibility`, async () => {
      const f = await mutationFixture();
      await expect(testCase.run(f)).resolves.not.toBeNull();
    });

    it(`${testCase.name} accepts the active epoch`, async () => {
      const f = await mutationFixture();
      await activateFence(f.fence);
      await expect(
        testCase.run(f, { mutationEpoch: 1 }),
      ).resolves.not.toBeNull();
    });

    it.each([
      [undefined, 'missing'],
      [0, 'stale'],
      [2, 'future'],
    ] as const)(`${testCase.name} refuses %s with %s classification`, async (mutationEpoch, classification) => {
      const f = await mutationFixture();
      await activateFence(f.fence);
      const before = rawScheduleState(f);
      await expect(testCase.run(f, { mutationEpoch })).rejects.toMatchObject({
        status: 409,
        reason: {
          code: 'MUTATION_EPOCH_MISMATCH',
          classification,
          mutationEpoch: 1,
        },
      });
      expect(rawScheduleState(f)).toEqual(before);
    });

    it.each([
      -1,
      0.5,
      Number.NaN,
      Infinity,
      '1',
      null,
    ])(`${testCase.name} rejects malformed epoch %s before SQL`, async (mutationEpoch) => {
      const f = await mutationFixture();
      f.sql.length = 0;
      await expect(
        testCase.run(f, { mutationEpoch: mutationEpoch as number }),
      ).rejects.toBeInstanceOf(InvalidMutationEpochError);
      expect(f.sql).toEqual([]);
    });

    it(`${testCase.name} rejects a held legacy caller after activation and reopen`, async () => {
      const f = await mutationFixture();
      const before = rawScheduleState(f);
      f.hooks.beforeBatch = () => activateFence(f.fence);
      await expect(testCase.run(f)).rejects.toBeInstanceOf(
        MutationEpochMismatchError,
      );
      expect(rawScheduleState(f)).toEqual(before);
    });

    it(`${testCase.name} rejects an unchanged epoch after the observed frame changes`, async () => {
      const f = await mutationFixture();
      await activateFence(f.fence);
      const before = rawScheduleState(f);
      f.hooks.beforeBatch = async () => {
        await moveFence(f.fence, 'draining');
        await moveFence(f.fence, 'open');
      };
      await expect(testCase.run(f, { mutationEpoch: 1 })).rejects.toMatchObject(
        {
          reason: {
            code: 'SCHEDULE_MUTATION_CONFLICT',
            classification: 'fence-changed',
          },
        },
      );
      expect(rawScheduleState(f)).toEqual(before);
    });

    for (const state of [
      'draining',
      'migration-locked',
      'proof-only',
    ] as const) {
      it(`${testCase.name} applies its state policy in ${state}`, async () => {
        const f = await mutationFixture();
        await activateFence(f.fence);
        await moveFence(f.fence, state);
        const before = rawScheduleState(f);
        if (testCase.operation === 'author') {
          await expect(
            testCase.run(f, { mutationEpoch: 1 }),
          ).rejects.toBeInstanceOf(ExecutionFencedError);
          expect(rawScheduleState(f)).toEqual(before);
        } else {
          await expect(
            testCase.run(f, { mutationEpoch: 1 }),
          ).resolves.not.toBeNull();
        }
      });
    }
  }

  it('generic paused-status update remains authoring', async () => {
    const f = await mutationFixture();
    await activateFence(f.fence);
    await moveFence(f.fence, 'draining');
    const before = rawScheduleState(f);
    await expect(
      f.store.updateSchedule(
        'schedule_a',
        { status: 'paused' },
        { mutationEpoch: 1 },
      ),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
    expect(rawScheduleState(f)).toEqual(before);
  });

  it.each([
    'create',
    'update',
    'delete',
    'pause observation',
  ])('a %s final write refuses missing modern singleton state', async (name) => {
    const f = await mutationFixture();
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () =>
      f.sqlite.exec('DELETE FROM flowsafe_execution_fence');
    const testCase = mutationCases.find((entry) => entry.name === name);
    expect(testCase).toBeDefined();
    if (!testCase) throw new Error('mutation fixture is missing');
    await expect(testCase.run(f)).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
    expect(rawScheduleState(f)).toEqual(before);
  });

  it.each([
    [
      'extra schema column',
      'ALTER TABLE flowsafe_execution_fence ADD COLUMN unrelated TEXT',
    ],
    [
      'counter type',
      "PRAGMA ignore_check_constraints=ON; UPDATE flowsafe_execution_fence SET mutation_epoch=x'30'",
    ],
    [
      'receipt mismatch',
      "UPDATE flowsafe_execution_fence SET last_transition_request='[]'",
    ],
    [
      'proof binding type',
      "UPDATE flowsafe_execution_fence SET proof_key=x'4142'",
    ],
    [
      'singleton identity case',
      "PRAGMA ignore_check_constraints=ON; UPDATE flowsafe_execution_fence SET id='DEPLOYMENT'",
    ],
    [
      'extra singleton',
      "PRAGMA ignore_check_constraints=ON; INSERT INTO flowsafe_execution_fence SELECT 'extra',state,proof_key,proof_run_id,updated_at,last_transition_request,transition_revision,mutation_epoch,require_mutation_epoch,proof_table_prefix,proof_workflow_id,proof_start_token FROM flowsafe_execution_fence",
    ],
  ])('refuses %s introduced at the final batch', async (_name, sql) => {
    const f = await mutationFixture();
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () => f.sqlite.exec(sql);
    await expect(
      f.store.deleteOwnedSchedule('schedule_a'),
    ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(rawScheduleState(f)).toEqual(before);
  });

  it('refuses unsupported nullable semantic bindings before preparing a mutation', async () => {
    const f = await mutationFixture();
    f.sqlite.exec("UPDATE flowsafe_execution_fence SET proof_key=x'4142'");
    f.sql.length = 0;
    await expect(f.store.pauseSchedule('schedule_a')).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
    expect(f.sql.some((sql) => /UPDATE mastra_schedules/.test(sql))).toBe(
      false,
    );
  });

  it('compares semantic strings with binary equality', async () => {
    const f = await mutationFixture();
    f.sqlite.exec("UPDATE flowsafe_execution_fence SET proof_key='ProofKey'");
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () =>
      f.sqlite.exec("UPDATE flowsafe_execution_fence SET proof_key='proofkey'");
    await expect(f.store.pauseSchedule('schedule_a')).rejects.toBeInstanceOf(
      ScheduleMutationConflictError,
    );
    expect(rawScheduleState(f)).toEqual(before);
  });

  it('timestamp-only fence changes do not invalidate the semantic frame', async () => {
    const f = await mutationFixture();
    f.hooks.beforeBatch = () =>
      f.sqlite.exec(
        'UPDATE flowsafe_execution_fence SET updated_at=updated_at+1',
      );
    await expect(f.store.pauseSchedule('schedule_a')).resolves.toMatchObject({
      status: 'paused',
    });
  });
});

describe('schedule authoring capture and observations', () => {
  it('captures epoch, row JSON, owner getters and batch receiver before waits', async () => {
    const f = await mutationFixture();
    const schedule = scheduleWithCreatorRole(
      workflowSchedule({ id: 'captured' }),
      'operator',
    );
    const epoch = vi.fn(() => 0);
    const ownerKind = vi.fn(() => 'human' as const);
    const ownerId = vi.fn(() => 'opal');
    const owner = {
      get kind() {
        return ownerKind();
      },
      get id() {
        return ownerId();
      },
    };
    f.hooks.beforeBatch = () => {
      schedule.id = 'changed';
      schedule.target = { type: 'workflow', workflowId: 'changed' };
      schedule.metadata = { changed: true };
      ownerId.mockReturnValue('changed');
      epoch.mockReturnValue(100);
    };
    f.binding.batch = () => {
      throw new Error('replacement receiver');
    };
    const result = await f.store.createOwnedSchedule(schedule, owner, 100, {
      get mutationEpoch() {
        return epoch();
      },
    });
    expect(result).toMatchObject({
      id: 'captured',
      target: { workflowId: 'wf' },
      metadata: {},
    });
    expect(epoch).toHaveBeenCalledTimes(1);
    expect(ownerKind).toHaveBeenCalledTimes(1);
    expect(ownerId).toHaveBeenCalledTimes(1);
    expect(
      f.sqlite
        .prepare(
          "SELECT owner_id FROM flowsafe_resource_owners WHERE resource_id='captured'",
        )
        .get(),
    ).toEqual({ owner_id: 'opal' });
  });

  it('captures a mutable context even when a later getter would supply the active epoch', async () => {
    const f = await mutationFixture();
    const epoch = vi.fn().mockReturnValueOnce(0).mockReturnValue(1);
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () => activateFence(f.fence);
    await expect(
      f.store.pauseSchedule('schedule_a', {
        get mutationEpoch() {
          return epoch();
        },
      }),
    ).rejects.toMatchObject({ reason: { classification: 'stale' } });
    expect(epoch).toHaveBeenCalledTimes(1);
    expect(rawScheduleState(f)).toEqual(before);
  });

  it('captures update fields once and serializes nested metadata before waiting', async () => {
    const f = await mutationFixture();
    const metadata = { note: 'original' };
    const readMetadata = vi.fn(() => metadata);
    f.hooks.beforeBatch = () => {
      metadata.note = 'changed';
    };
    const updated = await f.store.updateSchedule('schedule_a', {
      get metadata() {
        return readMetadata();
      },
    });
    expect(updated.metadata).toEqual({ note: 'original' });
    expect(readMetadata).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["cron='*/5 * * * *'", 'cron'],
    ["timezone='UTC'", 'timezone'],
  ])('resume refuses a concurrent %s change', async (sql) => {
    const f = await mutationFixture();
    await f.store.pauseSchedule('schedule_a');
    f.hooks.beforeBatch = () =>
      f.sqlite.exec(`UPDATE mastra_schedules SET ${sql}`);
    await expect(
      f.store.resumeSchedule('schedule_a', {
        expectedCron: '* * * * *',
        expectedTimezone: undefined,
        nextFireAt: NOW + 60_000,
      }),
    ).rejects.toMatchObject({
      reason: {
        code: 'SCHEDULE_MUTATION_CONFLICT',
        classification: 'schedule-changed',
      },
    });
    expect(await f.store.getSchedule('schedule_a')).toMatchObject({
      status: 'paused',
      nextFireAt: NOW,
    });
  });

  it('resume captures its expected timezone and computed timestamp before waiting', async () => {
    const f = await mutationFixture();
    await f.store.pauseSchedule('schedule_a');
    const mutation = {
      expectedCron: '* * * * *',
      expectedTimezone: undefined as string | undefined,
      nextFireAt: NOW + 60_000,
    };
    f.hooks.beforeBatch = () => {
      mutation.expectedTimezone = 'UTC';
      mutation.nextFireAt = NOW + 120_000;
    };
    await expect(
      f.store.resumeSchedule('schedule_a', mutation),
    ).resolves.toMatchObject({ status: 'active', nextFireAt: NOW + 60_000 });
  });

  it('no-op observations preserve row timestamps and return current status', async () => {
    const f = await mutationFixture();
    const before = rawScheduleState(f);
    await expect(
      f.store.observeScheduleMutation('schedule_a', 'resume', {}),
    ).resolves.toMatchObject({ status: 'active', updatedAt: NOW });
    expect(rawScheduleState(f)).toEqual(before);
    await f.store.pauseSchedule('schedule_a');
    const paused = rawScheduleState(f);
    await expect(
      f.store.observeScheduleMutation('schedule_a', 'pause', {}),
    ).resolves.toMatchObject({ status: 'paused' });
    expect(rawScheduleState(f)).toEqual(paused);
    await expect(
      f.store.observeScheduleMutation('absent', 'pause', {}),
    ).resolves.toBeNull();
  });

  it('returns the post-race status from a guarded observation', async () => {
    const f = await mutationFixture();
    f.hooks.beforeBatch = () =>
      f.sqlite.exec("UPDATE mastra_schedules SET status='paused'");
    await expect(
      f.store.observeScheduleMutation('schedule_a', 'resume', {}),
    ).resolves.toMatchObject({ status: 'paused' });
  });

  it('rejects an invalid observer selector before SQL', async () => {
    const f = await mutationFixture();
    f.sql.length = 0;
    await expect(
      f.store.observeScheduleMutation('schedule_a', 'delete' as 'pause', {}),
    ).rejects.toThrow('observation is invalid');
    expect(f.sql).toEqual([]);
  });
});

describe('schedule transaction evidence', () => {
  it.each([
    [
      'missing batch slot',
      (results: unknown[]) => {
        delete results[1];
      },
    ],
    [
      'extra batch slot',
      (results: unknown[]) => {
        results.push(results[0]);
      },
    ],
    [
      'missing RETURNING',
      (results: unknown[]) => {
        delete (results[3] as Record<string, unknown>).results;
      },
    ],
    [
      'sparse RETURNING',
      (results: unknown[]) => {
        (results[3] as Record<string, unknown>).results = new Array(1);
      },
    ],
    [
      'false success',
      (results: unknown[]) => {
        (results[3] as Record<string, unknown>).success = false;
      },
    ],
    [
      'null metadata',
      (results: unknown[]) => {
        (results[3] as Record<string, unknown>).meta = null;
      },
    ],
    [
      'contradictory changes',
      (results: unknown[]) => {
        (results[3] as Record<string, unknown>).meta = { changes: 0 };
      },
    ],
    [
      'undefined changes',
      (results: unknown[]) => {
        (results[3] as Record<string, unknown>).meta = { changes: undefined };
      },
    ],
    [
      'missing owner witness',
      (results: unknown[]) => {
        results[4] = { results: [], meta: { changes: 0 } };
      },
    ],
  ] as const)('classifies %s as unknown without compensating', async (_name, corrupt) => {
    const f = await mutationFixture();
    f.hooks.afterBatch = (results) => {
      corrupt(results);
      return results;
    };
    const calls = f.batches();
    await expect(
      f.store.createOwnedSchedule(
        scheduleWithCreatorRole(
          workflowSchedule({ id: 'uncertain' }),
          'operator',
        ),
        { kind: 'human', id: 'opal' },
        100,
      ),
    ).rejects.toBeInstanceOf(ScheduleMutationOutcomeUnknownError);
    expect(f.batches()).toBe(calls + 1);
    expect(await f.store.getSchedule('uncertain')).not.toBeNull();
    expect(
      f.sqlite
        .prepare(
          "SELECT owner_id FROM flowsafe_resource_owners WHERE resource_id='uncertain'",
        )
        .get(),
    ).toEqual({ owner_id: 'opal' });
  });

  it('accepts complete bounded RETURNING without metadata and ignores SELECT change counts', async () => {
    const f = await mutationFixture();
    f.hooks.afterBatch = (results) => {
      delete (results[3] as Record<string, unknown>).meta;
      (results[2] as Record<string, unknown>).meta = { changes: 987 };
      return results;
    };
    await expect(f.store.pauseSchedule('schedule_a')).resolves.toMatchObject({
      status: 'paused',
    });
  });

  it.each([
    'before',
    'after',
  ] as const)('does not retry a thrown %s-batch response', async (when) => {
    const f = await mutationFixture();
    const cause = new Error('transport lost');
    if (when === 'before')
      f.hooks.beforeBatch = () => {
        throw cause;
      };
    else
      f.hooks.afterBatch = () => {
        throw cause;
      };
    const calls = f.batches();
    await expect(
      f.store.createSchedule(workflowSchedule({ id: 'uncertain' })),
    ).rejects.toMatchObject({
      name: 'ScheduleMutationOutcomeUnknownError',
      cause,
    });
    expect(f.batches()).toBe(calls + 1);
    expect(await f.store.getSchedule('uncertain')).toEqual(
      when === 'before' ? null : workflowSchedule({ id: 'uncertain' }),
    );
  });

  it('malformed responses take precedence over a diagnosable fence refusal', async () => {
    const f = await mutationFixture();
    f.hooks.beforeBatch = () => activateFence(f.fence);
    f.hooks.afterBatch = (results) => {
      delete results[3];
      return results;
    };
    await expect(f.store.pauseSchedule('schedule_a')).rejects.toBeInstanceOf(
      ScheduleMutationOutcomeUnknownError,
    );
  });

  it('a zero UPDATE cannot claim success from an unchanged row', async () => {
    const f = await mutationFixture();
    f.hooks.afterBatch = (results) => {
      results[3] = { results: [], meta: { changes: 0 } };
      return results;
    };
    await expect(
      f.store.updateSchedule('schedule_a', { metadata: {} }),
    ).rejects.toBeInstanceOf(ScheduleMutationOutcomeUnknownError);
  });

  it.each([
    'update',
    'observation',
  ])('refuses matching truncated %s rows', async (operation) => {
    const f = await mutationFixture();
    f.hooks.afterBatch = (results) => {
      for (const index of [2, 3]) {
        const result = results[index] as { results: Record<string, unknown>[] };
        const row = result.results[0];
        if (!row) throw new Error('fixture row missing');
        delete row.createdAt;
      }
      return results;
    };
    await expect(
      operation === 'update'
        ? f.store.updateSchedule('schedule_a', { metadata: {} })
        : f.store.observeScheduleMutation('schedule_a', 'pause', {}),
    ).rejects.toBeInstanceOf(ScheduleMutationOutcomeUnknownError);
  });

  it('preserves direct not-found errors after authoritative absence', async () => {
    const f = await mutationFixture();
    await expect(f.store.updateSchedule('absent', {})).rejects.toThrow(
      'schedule absent not found',
    );
    await expect(f.store.pauseSchedule('absent')).rejects.toThrow(
      'schedule absent not found',
    );
  });
});

describe('schedule deletion, preparation and compatibility', () => {
  it('pending deletions consume the original deployment cap', async () => {
    const f = await mutationFixture();
    await f.store.recordTrigger({
      id: 'deferred',
      scheduleId: 'schedule_a',
      runId: 'run-a',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
    });
    await expect(f.store.deleteOwnedSchedule('schedule_a')).resolves.toBe(
      'pending',
    );
    expect(await f.store.listSchedules()).toEqual([]);
    await activateFence(f.fence);
    await expect(
      f.store.createOwnedSchedule(
        scheduleWithCreatorRole(workflowSchedule({ id: 'capped' }), 'operator'),
        { kind: 'human', id: 'opal' },
        1,
        { mutationEpoch: 1 },
      ),
    ).resolves.toBeNull();
    expect(
      f.sqlite
        .prepare(
          "SELECT COUNT(*) AS count FROM flowsafe_resource_owners WHERE resource_id='capped'",
        )
        .get(),
    ).toEqual({ count: 0 });
  });

  it('a stale cap-zero create reports epoch refusal', async () => {
    const f = await mutationFixture();
    f.hooks.beforeBatch = () => activateFence(f.fence);
    await expect(
      f.store.createOwnedSchedule(
        scheduleWithCreatorRole(workflowSchedule({ id: 'capped' }), 'operator'),
        { kind: 'human', id: 'opal' },
        0,
      ),
    ).rejects.toBeInstanceOf(MutationEpochMismatchError);
  });

  it('keeps admitted trigger settlement independent of later epoch and state', async () => {
    const f = await mutationFixture();
    const trigger = {
      id: 'deferred',
      scheduleId: 'schedule_a',
      runId: 'run-a',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred' as const,
    };
    await f.store.recordTrigger(trigger);
    await activateFence(f.fence);
    await expect(
      f.store.deleteOwnedSchedule('schedule_a', { mutationEpoch: 1 }),
    ).resolves.toBe('pending');
    const marker = f.sqlite
      .prepare(
        "SELECT deletionRequestedAt FROM mastra_schedules WHERE id='schedule_a'",
      )
      .get();
    await expect(
      f.store.deleteOwnedSchedule('schedule_a', { mutationEpoch: 1 }),
    ).resolves.toBe('pending');
    expect(
      f.sqlite
        .prepare(
          "SELECT deletionRequestedAt FROM mastra_schedules WHERE id='schedule_a'",
        )
        .get(),
    ).toEqual(marker);
    await moveFence(f.fence, 'draining', true);
    await f.store.recordTrigger({
      ...trigger,
      outcome: 'failed',
      error: 'settled',
    });
    expect(rawScheduleState(f)).toEqual([[], [], []]);
    await f.store.recordTrigger({ ...trigger, outcome: 'failed' });
    expect(rawScheduleState(f)).toEqual([[], [], []]);
  });

  it('a held stale deletion preserves deferred triggers and owner bytes', async () => {
    const f = await mutationFixture();
    await f.store.recordTrigger({
      id: 'deferred',
      scheduleId: 'schedule_a',
      runId: 'run-a',
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
      metadata: { opaque: 'retain' },
    });
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () => activateFence(f.fence);
    await expect(
      f.store.deleteOwnedSchedule('schedule_a'),
    ).rejects.toBeInstanceOf(MutationEpochMismatchError);
    expect(rawScheduleState(f)).toEqual(before);
  });

  it('guards orphan cleanup independently when the first deletion UPDATE matches no schedule', async () => {
    const f = await mutationFixture();
    await f.store.recordTrigger({
      id: 'orphan',
      scheduleId: 'schedule_a',
      runId: null,
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'deferred',
    });
    f.sqlite.exec('DELETE FROM mastra_schedules');
    const before = rawScheduleState(f);
    f.hooks.beforeBatch = () => activateFence(f.fence);
    await expect(
      f.store.deleteOwnedSchedule('schedule_a'),
    ).rejects.toBeInstanceOf(MutationEpochMismatchError);
    expect(rawScheduleState(f)).toEqual(before);
    await expect(
      f.store.deleteOwnedSchedule('schedule_a', { mutationEpoch: 1 }),
    ).resolves.toBe('deleted');
    expect(rawScheduleState(f)).toEqual([[], [], []]);
  });

  it('deletes populated history with bounded returned rows and strict trigger changes', async () => {
    const f = await mutationFixture();
    const insert = f.sqlite.prepare(
      "INSERT INTO mastra_schedule_triggers (id,scheduleId,runId,scheduledFireAt,actualFireAt,outcome) VALUES (?,'schedule_a',NULL,?,?,'published')",
    );
    for (let index = 0; index < 300; index += 1)
      insert.run(`history-${index}`, NOW, NOW + index);
    let captured: unknown[] | undefined;
    f.hooks.afterBatch = (results) => {
      captured = results;
      return results;
    };
    await expect(f.store.deleteOwnedSchedule('schedule_a')).resolves.toBe(
      'deleted',
    );
    expect(captured?.[4]).toMatchObject({
      results: [],
      meta: { changes: 300 },
    });
    expect(JSON.stringify(captured).length).toBeLessThan(10_000);
    expect(rawScheduleState(f)).toEqual([[], [], []]);
  });

  it.each([
    undefined,
    { changes: 0 },
    { changes: 1.5 },
    { changes: undefined },
  ])('requires exact trigger DML metadata: %j', async (meta) => {
    const f = await mutationFixture();
    await f.store.recordTrigger({
      id: 'history',
      scheduleId: 'schedule_a',
      runId: null,
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: 'published',
    });
    f.hooks.afterBatch = (results) => {
      if (meta === undefined)
        delete (results[4] as Record<string, unknown>).meta;
      else (results[4] as Record<string, unknown>).meta = meta;
      return results;
    };
    const calls = f.batches();
    await expect(
      f.store.deleteOwnedSchedule('schedule_a'),
    ).rejects.toBeInstanceOf(ScheduleMutationOutcomeUnknownError);
    expect(f.batches()).toBe(calls + 1);
    expect(rawScheduleState(f)).toEqual([[], [], []]);
  });

  it('advertises the original binding and captures facade method receivers', async () => {
    const f = await mutationFixture();
    const capability = f.store[FENCED_SCHEDULE_STORAGE];
    expect(capability?.database).toBe(f.binding);
    expect(f.fence.usesDatabase(capability?.database as object)).toBe(true);
    if (!capability) throw new Error('schedule capability is missing');
    f.store.pauseSchedule = () => {
      throw new Error('replacement method');
    };
    const pause = capability.pauseSchedule;
    await expect(pause('schedule_a', {})).resolves.toMatchObject({
      status: 'paused',
    });
  });

  it('reads on a prepare-only binding without seeding the fence and refuses authoring', async () => {
    const sqlite = openSqlite();
    const native = sqliteUnitDatabase(sqlite) as ScheduleDatabase;
    const store = new D1SchedulesStorage({
      prepare: native.prepare.bind(native),
    });
    expect(store[FENCED_SCHEDULE_STORAGE]).toBeUndefined();
    await store.init();
    await expect(store.getSchedule('absent')).resolves.toBeNull();
    await expect(store.listSchedules()).resolves.toEqual([]);
    expect(
      sqlite
        .prepare(
          "SELECT name FROM sqlite_schema WHERE name='flowsafe_execution_fence'",
        )
        .all(),
    ).toEqual([]);
    await expect(store.createSchedule(workflowSchedule())).rejects.toThrow(
      'requires database.batch()',
    );
    expect(
      sqlite
        .prepare(
          "SELECT name FROM sqlite_schema WHERE name='flowsafe_execution_fence'",
        )
        .all(),
    ).toEqual([]);
  });

  it('migrates a legacy empty fence on authoring and does not recreate a deleted modern singleton', async () => {
    const sqlite = openSqlite();
    sqlite.exec(
      "CREATE TABLE flowsafe_execution_fence (id TEXT PRIMARY KEY CHECK(id='deployment'),state TEXT NOT NULL,proof_key TEXT,proof_run_id TEXT,updated_at INTEGER NOT NULL)",
    );
    const binding = sqliteUnitDatabase(sqlite) as ScheduleDatabase;
    const store = new D1SchedulesStorage(binding);
    await store.createSchedule(workflowSchedule());
    expect(
      await new ExecutionFenceStore(binding).readForAdmission(),
    ).toMatchObject({
      schemaStage: 7,
      reading: { state: 'open', requireMutationEpoch: false },
    });
    sqlite.exec('DELETE FROM flowsafe_execution_fence');
    await expect(store.pauseSchedule('schedule_a')).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
    expect(
      sqlite.prepare('SELECT * FROM flowsafe_execution_fence').all(),
    ).toEqual([]);
  });

  it('retries failed authoring preparation without seeding on read paths', async () => {
    const sqlite = openSqlite();
    const native = sqliteUnitDatabase(sqlite) as ScheduleDatabase;
    let fail = true;
    let fencePreparations = 0;
    const binding: ScheduleDatabase = {
      prepare(query) {
        if (query.includes('flowsafe_execution_fence')) {
          fencePreparations += 1;
          if (fail) {
            fail = false;
            throw new Error('seed unavailable');
          }
        }
        return native.prepare(query);
      },
      batch: native.batch?.bind(native),
    };
    const store = new D1SchedulesStorage(binding);
    await store.init();
    await store.listSchedules();
    expect(fencePreparations).toBe(0);
    await expect(
      store.createSchedule(workflowSchedule()),
    ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
    await expect(
      store.createSchedule(workflowSchedule()),
    ).resolves.toMatchObject({ id: 'schedule_a' });
    const seed = vi.spyOn(ExecutionFenceStore.prototype, 'seed');
    try {
      await store.pauseSchedule('schedule_a');
      await store.updateSchedule('schedule_a', { metadata: { ready: true } });
      expect(seed).not.toHaveBeenCalled();
    } finally {
      seed.mockRestore();
    }
  });
});
