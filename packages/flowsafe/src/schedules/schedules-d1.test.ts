// SPDX-License-Identifier: Apache-2.0
// Track D (M-006) — the D1 schedules domain against REAL SQLite (node:sqlite),
// so the CAS UPDATE, the due predicate, and json/metadata round-trips run for
// real. Mirrors the notifications/thread-state domain tests.

import type { Schedule } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';

import { d1DatabaseLike, openSqlite } from '../../test-support/sqlite.js';
import { D1SchedulesStorage, type ScheduleDatabase } from './schedules-d1.js';

function storeOver(): {
  store: D1SchedulesStorage;
  sqlite: ReturnType<typeof openSqlite>;
} {
  const sqlite = openSqlite();
  const store = new D1SchedulesStorage(
    d1DatabaseLike(sqlite) as unknown as ScheduleDatabase,
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
    metadata: { tenantId: 'acme' },
    ...overrides,
  };
}

describe('D1SchedulesStorage', () => {
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

    // #then it loses (exactly-once), and the winner's runId is untouched
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
        metadata: { tenantId: 'acme' },
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
      metadata: { tenantId: 'acme', reason: 'run-capped' },
    });

    // #then — one row with a generated id and the null runId preserved
    const triggers = await store.listTriggers('schedule_a');
    expect(triggers).toHaveLength(1);
    expect(typeof triggers[0]?.id).toBe('string');
    expect(triggers[0]?.runId).toBeNull();
    expect(triggers[0]?.outcome).toBe('skipped');
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
      metadata: { tenantId: 'acme', note: 'x' },
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
