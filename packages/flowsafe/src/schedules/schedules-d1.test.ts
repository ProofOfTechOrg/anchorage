// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — fast schedule SQL units over node:sqlite. The Wrangler
// harness owns real-D1 CAS, concurrency, ownership, and rollback evidence.

import type { Schedule } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  D1ResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/index.js';
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
    ).rejects.toThrow(/injected owner failure/);

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

    await expect(store.deleteOwnedSchedule(schedule.id)).rejects.toThrow(
      /injected owner delete failure/,
    );

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

    await expect(store.deleteSchedule('schedule_a')).rejects.toThrow(
      /injected delete failure/,
    );
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
