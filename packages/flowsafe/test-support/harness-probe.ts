// SPDX-License-Identifier: Apache-2.0

import type { CreateNotificationInput } from '@mastra/core/notifications';
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';
import { z } from 'zod';
import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';

import {
  type ActorContext,
  createActorResolver,
} from '../src/approval-api/actor-context.js';
import { D1ApprovalStore } from '../src/approval-api/d1-store.js';
import {
  D1ResourceOwnershipStore,
  RESOURCE_OWNERSHIP_TABLE,
} from '../src/approval-api/resource-ownership.js';
import { D1ApprovalStoreFactory } from '../src/approval-api/store-factory.js';
import type { ApprovalRecord } from '../src/approval-api/types.js';
import { createBackgroundTaskD1Domains } from '../src/background-tasks/d1-storage.js';
import {
  createD1Storage,
  purgeExpiredNotifications,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
  type RunRetentionCursor,
} from '../src/do-runner/d1-storage.js';
import { seedDeploymentIdentity } from '../src/do-runner/deployment-identity.js';
import { MUTATION_EPOCH_HEADER } from '../src/do-runner/execution-admission.js';
import { ExecutionFenceStore } from '../src/do-runner/execution-fence.js';
import { FENCED_WORKFLOW_STORAGE } from '../src/do-runner/fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from '../src/do-runner/fenced-workflows-d1.js';
import { init } from '../src/do-runner/init.js';
import { isDefinitiveInitialAdmissionRefusal } from '../src/do-runner/initial-admission-refusal.js';
import {
  parseRunLifecycle,
  projectTerminalLifecycle,
} from '../src/do-runner/run-lifecycle.js';
import type { StartRunOptions } from '../src/do-runner/runtime.js';
import { StartIdempotencyStore } from '../src/do-runner/start-idempotency.js';
import {
  START_IDEMPOTENCY_ADDITIONS,
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_TABLE,
} from '../src/do-runner/start-reservation-contract.js';
import { validateTablePrefix } from '../src/do-runner/table-prefix.js';
import type {
  SnapshotDatabase,
  SnapshotStatement,
} from '../src/do-runner/workflow-snapshot-row.js';
import { createRunRouter } from '../src/host-kit/run-router.js';
import { ScheduleMutationOutcomeUnknownError } from '../src/schedules/mutation-contract.js';
import { createScheduleRouter } from '../src/schedules/router.js';
import { D1SchedulesStorage } from '../src/schedules/schedules-d1.js';
import {
  createScheduleTargetPolicy,
  scheduleWithCreatorRole,
} from '../src/schedules/target-policy.js';
import { D1NotificationsStorage } from '../src/signals/notifications-d1.js';

interface Env {
  DB: D1Database;
}

const NOW = Date.parse('2026-08-10T12:00:00.000Z');
const DAY_MS = 24 * 60 * 60 * 1000;

function approval(id: string): ApprovalRecord {
  return {
    id,
    workflowId: 'workflow-cas',
    runId: 'run-cas',
    stepPath: ['gate'],
    title: 'Choose one reviewer',
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: '2026-08-10T12:00:00.000Z',
    updatedAt: '2026-08-10T12:00:00.000Z',
  };
}

function schedule(id: string) {
  return scheduleWithCreatorRole(
    {
      id,
      target: {
        type: 'workflow' as const,
        workflowId: 'workflow-schedule',
        inputData: {},
      },
      cron: '* * * * *',
      status: 'active' as const,
      nextFireAt: NOW,
      createdAt: NOW,
      updatedAt: NOW,
      metadata: {},
    },
    'operator',
  );
}

async function approvalProbe(db: D1Database): Promise<unknown> {
  const store = new D1ApprovalStore(db);
  const creates = await Promise.all([
    store.create(approval('approval-left')),
    store.create(approval('approval-right')),
  ]);
  const open = creates[0]?.record;
  if (!open) throw new Error('approval create race returned no record');
  const transitions = await Promise.all([
    store.transition(open.id, ['pending'], {
      status: 'claimed',
      claimedBy: 'alice',
      claimedAt: '2026-08-10T12:01:00.000Z',
      updatedAt: '2026-08-10T12:01:00.000Z',
    }),
    store.transition(open.id, ['pending'], {
      status: 'claimed',
      claimedBy: 'bob',
      claimedAt: '2026-08-10T12:01:00.000Z',
      updatedAt: '2026-08-10T12:01:00.000Z',
    }),
  ]);
  return {
    created: creates.filter((result) => result.created).length,
    openIds: [...new Set(creates.map((result) => result.record.id))],
    transitionWinners: transitions
      .filter((record) => record !== null)
      .map((record) => record?.claimedBy),
    stored: await store.get(open.id),
  };
}

function scheduleMutationFailure(error: unknown) {
  if (!(error instanceof ScheduleMutationOutcomeUnknownError)) throw error;
  return { reason: error.reason, cause: String(error.cause) };
}

async function scheduleProbe(db: D1Database): Promise<unknown> {
  const store = new D1SchedulesStorage(db);
  const resources = new D1ResourceOwnershipStore(db);
  const owner = { kind: 'human' as const, id: 'opal' };
  const candidates = [schedule('schedule-left'), schedule('schedule-right')];
  const created = await Promise.all(
    candidates.map((candidate) =>
      store.createOwnedSchedule(candidate, owner, 1),
    ),
  );
  const winner = created.find((candidate) => candidate !== null);
  if (!winner) throw new Error('schedule cap race returned no winner');
  const loser = candidates.find((candidate) => candidate.id !== winner.id);

  const claimTarget = schedule('schedule-claim');
  await store.createOwnedSchedule(claimTarget, owner, 10);
  const claims = await Promise.all(
    ['left', 'right'].map((side) =>
      store.claimScheduleFire({
        scheduleId: claimTarget.id,
        expectedNextFireAt: NOW,
        newNextFireAt: NOW + 60_000,
        actualFireAt: NOW,
        runId: `run-${side}`,
        trigger: {
          id: `trigger-${side}`,
          scheduleId: claimTarget.id,
          runId: `run-${side}`,
          scheduledFireAt: NOW,
          actualFireAt: NOW,
          outcome: 'deferred',
        },
      }),
    ),
  );

  const rollback = schedule('schedule-rollback');
  await store.createOwnedSchedule(rollback, owner, 10);
  await store.recordTrigger({
    id: 'trigger-rollback',
    scheduleId: rollback.id,
    runId: 'run-rollback',
    scheduledFireAt: NOW,
    actualFireAt: NOW,
    outcome: 'published',
  });
  await db
    .prepare(
      `CREATE TRIGGER reject_schedule_owner_delete
       BEFORE DELETE ON ${RESOURCE_OWNERSHIP_TABLE}
       WHEN OLD.resource_kind = 'schedule'
         AND OLD.resource_id = 'schedule-rollback'
       BEGIN SELECT RAISE(ABORT, 'injected owner delete failure'); END`,
    )
    .run();
  let rollbackError: ReturnType<typeof scheduleMutationFailure> | undefined;
  try {
    await store.deleteOwnedSchedule(rollback.id);
  } catch (error) {
    rollbackError = scheduleMutationFailure(error);
  }

  const successfulDelete = schedule('schedule-delete');
  await store.createOwnedSchedule(successfulDelete, owner, 10);
  await store.recordTrigger({
    id: 'trigger-delete',
    scheduleId: successfulDelete.id,
    runId: 'run-delete',
    scheduledFireAt: NOW,
    actualFireAt: NOW,
    outcome: 'published',
  });
  const deleteResult = await store.deleteOwnedSchedule(successfulDelete.id);

  const ownerFailure = schedule('schedule-owner-failure');
  await db
    .prepare(
      `CREATE TRIGGER reject_schedule_owner_insert
       BEFORE INSERT ON ${RESOURCE_OWNERSHIP_TABLE}
       WHEN NEW.resource_kind = 'schedule'
         AND NEW.resource_id = 'schedule-owner-failure'
       BEGIN SELECT RAISE(ABORT, 'injected owner insert failure'); END`,
    )
    .run();
  let ownerInsertError: ReturnType<typeof scheduleMutationFailure> | undefined;
  try {
    await store.createOwnedSchedule(ownerFailure, owner, 10);
  } catch (error) {
    ownerInsertError = scheduleMutationFailure(error);
  }

  return {
    capWinners: created.filter((candidate) => candidate !== null).length,
    winnerId: winner.id,
    loserId: loser?.id,
    storedSchedules: (await store.listSchedules()).map(({ id }) => id),
    winnerOwner: await resources.owner('schedule', winner.id),
    loserOwner: loser ? await resources.owner('schedule', loser.id) : undefined,
    claimWinners: claims.filter(Boolean).length,
    claimTriggers: await store.listTriggers(claimTarget.id),
    rollbackError,
    rollbackSchedule: await store.getSchedule(rollback.id),
    rollbackTriggers: await store.listTriggers(rollback.id),
    rollbackOwner: await resources.owner('schedule', rollback.id),
    deleteResult,
    deletedSchedule: await store.getSchedule(successfulDelete.id),
    deletedTriggers: await store.listTriggers(successfulDelete.id),
    deletedOwner: await resources.owner('schedule', successfulDelete.id),
    ownerInsertError,
    ownerFailureSchedule: await store.getSchedule(ownerFailure.id),
    ownerFailureOwner: await resources.owner('schedule', ownerFailure.id),
  };
}

async function createLegacyNotificationsTable(db: D1Database): Promise<void> {
  await db
    .prepare(
      `CREATE TABLE mastra_notifications (
         id TEXT NOT NULL,
         thread_id TEXT NOT NULL,
         source TEXT NOT NULL,
         kind TEXT NOT NULL,
         priority TEXT NOT NULL,
         status TEXT NOT NULL,
         summary TEXT NOT NULL,
         payload TEXT,
         resourceId TEXT,
         agentId TEXT,
         sourceId TEXT,
         dedupeKey TEXT,
         coalesceKey TEXT,
         coalescedCount INTEGER NOT NULL DEFAULT 1,
         attributes TEXT,
         createdAt TEXT NOT NULL,
         updatedAt TEXT NOT NULL,
         deliverAt TEXT,
         summaryAt TEXT,
         deliveryReason TEXT,
         deliveryAttempts INTEGER NOT NULL DEFAULT 0,
         lastDeliveryAttemptAt TEXT,
         lastDeliveryError TEXT,
         deliveredSignalId TEXT,
         summarySignalId TEXT,
         deliveredAt TEXT,
         seenAt TEXT,
         dismissedAt TEXT,
         archivedAt TEXT,
         discardedAt TEXT,
         metadata TEXT,
         PRIMARY KEY (thread_id, id)
       )`,
    )
    .run();
}

async function notificationProbe(db: D1Database): Promise<unknown> {
  await createLegacyNotificationsTable(db);
  const insertLegacy = async (
    id: string,
    summary: string,
    createdAt: string,
  ): Promise<void> => {
    await db
      .prepare(
        `INSERT INTO mastra_notifications (
           id, thread_id, source, kind, priority, status, summary,
           coalescedCount, createdAt, updatedAt, deliveryAttempts
         ) VALUES (?, 'thread-legacy', 'legacy', 'migration', 'medium',
                   'pending', ?, 1, ?, ?, 0)`,
      )
      .bind(id, summary, createdAt, createdAt)
      .run();
  };
  await insertLegacy(
    'physical-first',
    'later timestamp',
    '2026-08-10T11:05:00.000Z',
  );
  await insertLegacy(
    'physical-second',
    'earlier timestamp',
    '2026-08-10T11:00:00.000Z',
  );
  const left = new D1NotificationsStorage(db, '');
  const right = new D1NotificationsStorage(db, '');
  await Promise.all([left.init(), right.init()]);
  const migrated = await db
    .prepare(
      `SELECT id, insertionOrdinal
       FROM mastra_notifications
       WHERE thread_id = 'thread-legacy'
       ORDER BY rowid`,
    )
    .all<{ id: string; insertionOrdinal: number }>();
  await left.createNotification({
    id: 'notification-base',
    threadId: 'thread-notifications',
    source: 'github',
    kind: 'ci',
    summary: 'base',
    coalesceKey: 'ci-run',
    attributes: { base: true },
    metadata: { base: true },
  });
  const coalesced = await Promise.all([
    left.createNotification({
      threadId: 'thread-notifications',
      source: 'github',
      kind: 'ci',
      summary: 'left',
      coalesceKey: 'ci-run',
      attributes: { left: true },
      metadata: { left: true },
    }),
    right.createNotification({
      threadId: 'thread-notifications',
      source: 'github',
      kind: 'ci',
      summary: 'right',
      coalesceKey: 'ci-run',
      attributes: { right: true },
      metadata: { right: true },
    }),
  ]);
  const beforeRollback = await left.getNotification({
    threadId: 'thread-notifications',
    id: 'notification-base',
  });
  let rollbackError = '';
  try {
    await db.batch([
      db.prepare(
        `UPDATE mastra_notifications SET summary = 'should-rollback'
           WHERE thread_id = 'thread-notifications'
             AND id = 'notification-base'`,
      ),
      db.prepare('INSERT INTO missing_notification_table VALUES (1)'),
    ]);
  } catch (error) {
    rollbackError = String(error);
  }
  const afterRollback = await left.getNotification({
    threadId: 'thread-notifications',
    id: 'notification-base',
  });
  const ordinalColumns = await db
    .prepare('PRAGMA table_info(mastra_notifications)')
    .all<{ name: string }>();
  return {
    migrated: migrated.results,
    coalescedIds: [...new Set(coalesced.map(({ id }) => id))],
    record: beforeRollback,
    rollbackError,
    rollbackSummary: afterRollback?.summary,
    ordinalColumns: ordinalColumns.results.filter(
      ({ name }) => name === 'insertionOrdinal',
    ).length,
  };
}

function createChronologyNotification(
  storage: D1NotificationsStorage,
  input: Partial<CreateNotificationInput>,
) {
  return storage.createNotification({
    threadId: 'thread-chronology',
    source: 'source',
    kind: 'kind',
    summary: 'summary',
    createdAt: new Date('2026-09-09T10:00:00.000Z'),
    ...input,
  });
}

async function notificationChronologyFutureProbe(db: D1Database) {
  const storage = new D1NotificationsStorage(db, '');
  const outcomes = [];
  for (const cursor of ['deliverAt', 'summaryAt'] as const) {
    await createChronologyNotification(storage, {
      id: 'future',
      threadId: cursor,
      resourceId: cursor,
      [cursor]: new Date('+010000-01-01T00:00:00.000Z'),
    });
    await createChronologyNotification(storage, {
      id: 'due',
      threadId: cursor,
      resourceId: cursor,
      [cursor]: new Date('2026-09-09T11:59:59.000Z'),
    });
    const query = {
      now: new Date('2026-09-09T12:00:00.000Z'),
      resourceId: cursor,
    };
    outcomes.push({
      cursor,
      boundedIds: (
        await storage.listDueNotifications({ ...query, limit: 1 })
      ).map((record) => record.id),
      dueIds: (await storage.listDueNotifications(query)).map(
        (record) => record.id,
      ),
    });
  }
  return outcomes;
}

async function notificationChronologyOffsetsProbe(db: D1Database) {
  const storage = new D1NotificationsStorage(db, '');
  const outcomes = [];
  const fixtures = [
    { id: 'first', at: '2026-09-09T12:30:00.1239+02:00' },
    { id: 'second', at: '2026-09-09T09:45:00.123-0100' },
    { id: 'third', at: '2026-09-09T11:00:00.123Z' },
  ];
  for (const cursor of ['deliverAt', 'summaryAt'] as const) {
    for (const fixture of fixtures) {
      await createChronologyNotification(storage, {
        id: fixture.id,
        threadId: cursor,
        resourceId: cursor,
      });
      await db
        .prepare(
          `UPDATE mastra_notifications SET ${cursor} = ?
           WHERE thread_id = ? AND id = ?`,
        )
        .bind(fixture.at, cursor, fixture.id)
        .run();
    }
    const bounded = await storage.listDueNotifications({
      now: new Date('2026-09-09T10:30:00.123Z'),
      resourceId: cursor,
      limit: 1,
    });
    const due = await storage.listDueNotifications({
      now: new Date('2026-09-09T12:00:00.000Z'),
      resourceId: cursor,
    });
    outcomes.push({
      cursor,
      boundedIds: bounded.map((record) => record.id),
      due: due.map((record) => ({
        id: record.id,
        at: record[cursor]?.getTime() ?? null,
      })),
      rawCursor: await db
        .prepare(
          `SELECT ${cursor} AS cursor FROM mastra_notifications
           WHERE thread_id = ? AND id = ?`,
        )
        .bind(cursor, 'first')
        .first<{ cursor: string }>(),
    });
  }
  return outcomes;
}

async function notificationChronologyBoundsProbe(db: D1Database) {
  const storage = new D1NotificationsStorage(db, '');
  const outcomes = [];
  const fixtures = [
    {
      name: 'negative',
      past: '-000800-01-01T00:00:00.001Z',
      now: '-000400-01-01T00:00:00.000Z',
      future: '-000001-01-01T00:00:00.000Z',
    },
    {
      name: 'minimum',
      past: '-271821-04-20T00:00:00.000Z',
      now: '-271821-04-20T00:00:00.001Z',
      future: '-271821-04-20T00:00:00.002Z',
    },
    {
      name: 'maximum',
      past: '+275760-09-12T23:59:59.998Z',
      now: '+275760-09-12T23:59:59.999Z',
      future: '+275760-09-13T00:00:00.000Z',
    },
  ];
  for (const fixture of fixtures) {
    const scope = { threadId: fixture.name, resourceId: fixture.name };
    const now = new Date(fixture.now);
    await createChronologyNotification(storage, {
      ...scope,
      id: 'past',
      deliverAt: new Date(fixture.past),
    });
    await createChronologyNotification(storage, {
      ...scope,
      id: 'equal',
      summaryAt: now,
    });
    await createChronologyNotification(storage, {
      ...scope,
      id: 'future',
      deliverAt: new Date(fixture.future),
      summaryAt: new Date(fixture.future),
    });
    const query = { now, resourceId: fixture.name };
    const bounded = await storage.listDueNotifications({ ...query, limit: 1 });
    const due = await storage.listDueNotifications(query);
    outcomes.push({
      name: fixture.name,
      boundedIds: bounded.map((record) => record.id),
      due: due.map((record) => ({
        id: record.id,
        deliverAt: record.deliverAt?.getTime() ?? null,
        summaryAt: record.summaryAt?.getTime() ?? null,
      })),
    });
  }
  return outcomes;
}

async function notificationChronologyListProbe(db: D1Database) {
  const storage = new D1NotificationsStorage(db, '');
  const fixtures = [
    { id: 'minimum', at: '-271821-04-20T00:00:00.000Z' },
    { id: 'negative', at: '-000001-01-01T00:00:00.000Z' },
    { id: 'offset', at: '2026-09-09T12:30:00.000+02:00' },
    { id: 'ordinary', at: '2026-09-09T11:00:00.000Z' },
    { id: 'expanded', at: '+010000-01-01T00:00:00.000Z' },
    { id: 'maximum', at: '+275760-09-13T00:00:00.000Z' },
  ];
  for (const fixture of fixtures) {
    await createChronologyNotification(storage, {
      id: fixture.id,
      createdAt: new Date(fixture.at),
    });
  }
  await db
    .prepare(
      'UPDATE mastra_notifications SET updatedAt = ? WHERE thread_id = ? AND id = ?',
    )
    .bind('2026-09-09T12:30:00.000+02:00', 'thread-chronology', 'offset')
    .run();
  return {
    boundedIds: (
      await storage.listNotifications({
        threadId: 'thread-chronology',
        limit: 3,
      })
    ).map((record) => record.id),
    records: (
      await storage.listNotifications({ threadId: 'thread-chronology' })
    ).map((record) => ({
      id: record.id,
      updatedAt: record.updatedAt.getTime(),
    })),
  };
}

async function notificationChronologyTtlProbe(db: D1Database) {
  const storage = new D1NotificationsStorage(db, '');
  const fixtures = [
    {
      id: 'future',
      status: 'delivered',
      updatedAt: '+010000-01-01T00:00:00.000Z',
    },
    {
      id: 'older',
      status: 'delivered',
      updatedAt: '2026-09-09T12:59:59.999+02:00',
    },
    {
      id: 'equal',
      status: 'delivered',
      updatedAt: '2026-09-09T10:00:00.000-0100',
    },
    {
      id: 'pending',
      status: 'pending',
      updatedAt: '2020-01-01T00:00:00.000Z',
    },
  ] as const;
  for (const fixture of fixtures) {
    await createChronologyNotification(storage, {
      id: fixture.id,
      createdAt: new Date(fixture.updatedAt),
    });
    if (fixture.status === 'delivered') {
      await storage.updateNotification({
        threadId: 'thread-chronology',
        id: fixture.id,
        status: fixture.status,
        deliveredSignalId: `signal-${fixture.id}`,
      });
    }
    await db
      .prepare(
        'UPDATE mastra_notifications SET updatedAt = ? WHERE thread_id = ? AND id = ?',
      )
      .bind(fixture.updatedAt, 'thread-chronology', fixture.id)
      .run();
  }
  const options = {
    now: () => new Date('2026-09-09T12:00:00.000Z').getTime(),
    ttlMs: 60 * 60 * 1000,
  };
  const purged = await purgeExpiredNotifications(db, options);
  const { results: after } = await db
    .prepare(
      'SELECT id, status, updatedAt FROM mastra_notifications ORDER BY id',
    )
    .all<{ id: string; status: string; updatedAt: string }>();
  const future = await storage.getNotification({
    threadId: 'thread-chronology',
    id: 'future',
  });
  return {
    purged,
    after,
    futureSignalId: future?.deliveredSignalId ?? null,
    repeated: await purgeExpiredNotifications(db, options),
  };
}

async function backgroundProbe(db: D1Database): Promise<unknown> {
  const storage = createD1Storage({
    binding: db,
    domains: createBackgroundTaskD1Domains({ binding: db }),
  });
  await storage.init();
  const workflows = await storage.getStore('workflows');
  if (!workflows) throw new Error('workflow domain is unavailable');
  await workflows.persistWorkflowSnapshot({
    workflowName: '__background-task',
    runId: 'background-concurrent',
    snapshot: createEmptyWorkflowSnapshot('background-concurrent'),
  });
  await Promise.all([
    workflows.updateWorkflowState({
      workflowName: '__background-task',
      runId: 'background-concurrent',
      opts: { status: 'running' },
    }),
    workflows.updateWorkflowResults({
      workflowName: '__background-task',
      runId: 'background-concurrent',
      stepId: 'execute',
      result: {
        status: 'success',
        output: { ok: true },
        payload: {},
        startedAt: 1,
        endedAt: 2,
      },
      requestContext: { trace: 'yes' },
    }),
  ]);
  return {
    supportsConcurrentUpdates: workflows.supportsConcurrentUpdates(),
    stored: await workflows.loadWorkflowSnapshot({
      workflowName: '__background-task',
      runId: 'background-concurrent',
    }),
  };
}

async function createSnapshotTable(db: D1Database, prefix = ''): Promise<void> {
  validateTablePrefix(prefix);
  await db
    .prepare(
      `CREATE TABLE ${prefix}mastra_workflow_snapshot (
         workflow_name TEXT NOT NULL,
         run_id TEXT NOT NULL,
         resourceId TEXT,
         snapshot TEXT NOT NULL,
         createdAt TEXT NOT NULL,
         updatedAt TEXT NOT NULL,
         PRIMARY KEY (workflow_name, run_id)
       )`,
    )
    .run();
}

async function threadRetentionProbe(db: D1Database): Promise<unknown> {
  const old = new Date(NOW - 40 * DAY_MS).toISOString();
  const fresh = new Date(NOW).toISOString();
  await db
    .prepare(
      `CREATE TABLE mastra_threads (
         id TEXT PRIMARY KEY,
         resourceId TEXT,
         updatedAt TEXT NOT NULL
       )`,
    )
    .run();
  await db
    .prepare(
      `CREATE TABLE mastra_messages (
         id TEXT PRIMARY KEY,
         thread_id TEXT NOT NULL,
         createdAt TEXT NOT NULL
       )`,
    )
    .run();
  await db.batch([
    db
      .prepare(
        `INSERT INTO mastra_threads (id, resourceId, updatedAt)
         VALUES ('thread-torn', NULL, ?)`,
      )
      .bind(old),
    db
      .prepare(
        `INSERT INTO mastra_messages (id, thread_id, createdAt)
         VALUES ('message-old', 'thread-torn', ?)`,
      )
      .bind(old),
    db
      .prepare(
        `INSERT INTO mastra_messages (id, thread_id, createdAt)
         VALUES ('message-just-sent', 'thread-torn', ?)`,
      )
      .bind(fresh),
    db
      .prepare(
        `INSERT INTO mastra_threads (id, resourceId, updatedAt)
         VALUES ('thread-resurrected', NULL, ?)`,
      )
      .bind(fresh),
    db
      .prepare(
        `INSERT INTO mastra_messages (id, thread_id, createdAt)
         VALUES ('message-history', 'thread-resurrected', ?)`,
      )
      .bind(old),
    db
      .prepare(
        `INSERT INTO mastra_messages (id, thread_id, createdAt)
         VALUES ('message-resurrection', 'thread-resurrected', ?)`,
      )
      .bind(fresh),
  ]);
  const purged = await purgeExpiredThreads(db, {
    ttlMs: 30 * DAY_MS,
    now: () => NOW,
  });
  const threads = await db
    .prepare('SELECT id FROM mastra_threads ORDER BY id')
    .all<{ id: string }>();
  const messages = await db
    .prepare('SELECT id, thread_id FROM mastra_messages ORDER BY id')
    .all<{ id: string; thread_id: string }>();
  const orphans = await db
    .prepare(
      `SELECT m.id
       FROM mastra_messages m
       LEFT JOIN mastra_threads t ON t.id = m.thread_id
       WHERE t.id IS NULL`,
    )
    .all<{ id: string }>();
  return {
    purged,
    threads: threads.results,
    messages: messages.results,
    orphans: orphans.results,
  };
}

async function retentionProbe(db: D1Database): Promise<unknown> {
  await createSnapshotTable(db);
  const resources = new D1ResourceOwnershipStore(db);
  const old = new Date(NOW - 8 * DAY_MS).toISOString();
  const fresh = new Date(NOW).toISOString();
  await db
    .prepare(
      `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, snapshot, createdAt, updatedAt)
       VALUES ('workflow', 'run-race', ?, ?, ?)`,
    )
    .bind(JSON.stringify({ status: 'success' }), old, old)
    .run();
  const purgePromise = purgeExpiredWorkflowRuns(db, {
    ttlMs: 7 * DAY_MS,
    now: () => NOW,
    advanceCursor: async () => {},
  });
  const updatePromise = db
    .prepare(
      `UPDATE mastra_workflow_snapshot SET updatedAt = ?
       WHERE run_id = 'run-race'`,
    )
    .bind(fresh)
    .run();
  const [purged, update] = await Promise.all([purgePromise, updatePromise]);
  const racedRow = await db
    .prepare(
      `SELECT updatedAt FROM mastra_workflow_snapshot
       WHERE run_id = 'run-race'`,
    )
    .first<{ updatedAt: string }>();

  await db
    .prepare(
      `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, snapshot, createdAt, updatedAt)
       VALUES ('workflow', 'run-rollback', ?, ?, ?)`,
    )
    .bind(JSON.stringify({ status: 'success' }), old, old)
    .run();
  await resources.claim('run', 'run-rollback', {
    kind: 'human',
    id: 'owner-retention',
  });
  await db
    .prepare(
      `CREATE TRIGGER reject_snapshot_delete
       BEFORE DELETE ON mastra_workflow_snapshot
       WHEN OLD.run_id = 'run-rollback'
       BEGIN SELECT RAISE(ABORT, 'injected snapshot delete failure'); END`,
    )
    .run();
  let rollbackError = '';
  try {
    await purgeExpiredWorkflowRuns(db, {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      advanceCursor: async () => {},
    });
  } catch (error) {
    rollbackError = String(error);
  }
  const rollbackRow = await db
    .prepare(
      `SELECT run_id FROM mastra_workflow_snapshot
       WHERE run_id = 'run-rollback'`,
    )
    .first<{ run_id: string }>();
  return {
    purged,
    updateChanges: update.meta.changes,
    racedRow,
    rollbackError,
    rollbackRow,
    rollbackOwner: await resources.owner('run', 'run-rollback'),
    threadRetention: await threadRetentionProbe(db),
  };
}

const E_PREFIX = 'e_retention_';
const E_TABLE = `${E_PREFIX}mastra_workflow_snapshot`;
const E_KEYS = 'e_retention_start_requests';
const E_OLD = new Date(NOW - 8 * DAY_MS).toISOString();
const E_OWNER = { kind: 'human' as const, id: 'Resource owner' };
const E_START = {
  owner: { kind: 'human' as const, id: 'Initiating principal' },
  target: { kind: 'agent' as const, id: 'logical-agent', threadId: 'thread' },
};
const encoder = new TextEncoder();

function retentionSnapshot(token = 'generation-one') {
  return {
    status: 'success',
    requestContext: {
      'flowsafe.runProvenance': {
        version: 2,
        startToken: token,
        startIdentity: E_START,
        agentStart: { threaded: false },
      },
    },
  };
}

function insertRetentionSnapshot(
  db: D1Database,
  runId: string,
  snapshot: unknown = retentionSnapshot(),
  prefix = E_PREFIX,
  workflow = 'physical-workflow',
) {
  validateTablePrefix(prefix);
  return db
    .prepare(`INSERT INTO "${prefix}mastra_workflow_snapshot"
      (workflow_name, run_id, snapshot, createdAt, updatedAt)
      VALUES (?, ?, ?, ?, ?)`)
    .bind(
      workflow,
      runId,
      typeof snapshot === 'string' ? snapshot : JSON.stringify(snapshot),
      E_OLD,
      E_OLD,
    );
}

async function createRetentionKeys(db: D1Database, stage = 3) {
  const additions = START_IDEMPOTENCY_ADDITIONS.join(',\n    ');
  await db
    .prepare(
      START_IDEMPOTENCY_DDL.replace(START_IDEMPOTENCY_TABLE, E_KEYS).replace(
        `,\n    ${additions}`,
        stage === 0
          ? ''
          : `,\n    ${START_IDEMPOTENCY_ADDITIONS.slice(0, stage).join(',\n    ')}`,
      ),
    )
    .run();
}

function insertRetentionKey(
  db: D1Database,
  key: string,
  runId: string,
  patch: Record<string, string | number | null> = {},
  stage = 3,
) {
  const row: Record<string, string | number | null> = {
    key,
    owner_kind: E_START.owner.kind,
    owner_id: E_START.owner.id,
    target_kind: E_START.target.kind,
    target_id: E_START.target.id,
    run_id: runId,
    thread_id: E_START.target.threadId,
    state: 'terminal',
    created_at: NOW - 9 * DAY_MS,
    updated_at: NOW - 8 * DAY_MS,
    ...Object.fromEntries(
      [
        ['start_token', 'generation-one'],
        ['start_table_prefix', E_PREFIX],
        ['start_workflow_id', 'physical-workflow'],
      ].slice(0, stage),
    ),
    ...patch,
  };
  return db
    .prepare(`INSERT INTO ${E_KEYS} (${Object.keys(row).join(', ')})
    VALUES (${Object.keys(row)
      .map(() => '?')
      .join(', ')})`)
    .bind(...Object.values(row));
}

function measuredRetentionDatabase(
  db: D1Database,
  beforeBatch?: (ordinal: number) => Promise<void>,
  beforeRead?: (sql: string, values: readonly unknown[]) => Promise<void>,
) {
  const statements = new WeakMap<
    SnapshotStatement,
    { native: D1PreparedStatement; sql: string; values: unknown[] }
  >();
  const metrics = {
    statements: 0,
    batches: 0,
    maxSqlBytes: 0,
    maxBindings: 0,
    maxBoundStringBytes: 0,
    maxSelectorBytes: 0,
    maxResultBytes: 0,
    rowsRead: 0,
    rowsWritten: 0,
    sqlDurationMs: 0,
  };
  const recordStatement = (statement: SnapshotStatement) => {
    const entry = statements.get(statement);
    if (!entry) throw new Error('foreign measured statement');
    metrics.statements += 1;
    metrics.maxSqlBytes = Math.max(
      metrics.maxSqlBytes,
      encoder.encode(entry.sql).length,
    );
    metrics.maxBindings = Math.max(metrics.maxBindings, entry.values.length);
    for (const value of entry.values) {
      if (typeof value !== 'string') continue;
      const bytes = encoder.encode(value).length;
      metrics.maxBoundStringBytes = Math.max(
        metrics.maxBoundStringBytes,
        bytes,
      );
      if (value.startsWith('['))
        metrics.maxSelectorBytes = Math.max(metrics.maxSelectorBytes, bytes);
    }
  };
  const recordResult = (outcome: D1Result) => {
    metrics.maxResultBytes = Math.max(
      metrics.maxResultBytes,
      encoder.encode(JSON.stringify(outcome.results)).length,
    );
    metrics.rowsRead += outcome.meta.rows_read;
    metrics.rowsWritten += outcome.meta.rows_written;
    metrics.sqlDurationMs += outcome.meta.duration;
  };
  const wrap = (sql: string, values: unknown[]): SnapshotStatement => {
    const prepared = db.prepare(sql);
    const native = values.length ? prepared.bind(...values) : prepared;
    const statement: SnapshotStatement = {
      bind: (...bindings) => wrap(sql, bindings),
      run: async () => {
        recordStatement(statement);
        const outcome = await native.run();
        recordResult(outcome);
        return outcome;
      },
      all: async <T>() => {
        await beforeRead?.(sql, values);
        recordStatement(statement);
        const outcome = await native.all<T>();
        recordResult(outcome);
        return outcome;
      },
    };
    statements.set(statement, { native, sql, values });
    return statement;
  };
  const database: SnapshotDatabase & Required<Pick<SnapshotDatabase, 'batch'>> =
    {
      prepare: (sql) => wrap(sql, []),
      batch: async (batch) => {
        metrics.batches += 1;
        await beforeBatch?.(metrics.batches);
        const results = await db.batch(
          batch.map((statement) => {
            recordStatement(statement);
            const entry = statements.get(statement);
            if (!entry) throw new Error('foreign batch statement');
            return entry.native;
          }),
        );
        results.forEach((outcome) => {
          recordResult(outcome);
        });
        return results;
      },
    };
  return { database, metrics };
}

function retentionOptions(advances: RunRetentionCursor[]) {
  return {
    tablePrefix: E_PREFIX,
    ttlMs: 7 * DAY_MS,
    now: () => NOW,
    advanceCursor: async (cursor: RunRetentionCursor) => {
      advances.push(structuredClone(cursor));
    },
  };
}

async function retentionState(db: D1Database) {
  const [snapshots, keys, owners] = await Promise.all([
    db
      .prepare(
        `SELECT workflow_name, run_id, snapshot, updatedAt FROM ${E_TABLE} ORDER BY workflow_name, run_id`,
      )
      .all(),
    db.prepare(`SELECT * FROM ${E_KEYS} ORDER BY key`).all(),
    db
      .prepare(
        `SELECT * FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id GLOB 'e-retention-*' ORDER BY resource_id`,
      )
      .all(),
  ]);
  return {
    snapshots: snapshots.results,
    keys: keys.results,
    owners: owners.results,
  };
}

async function cleanupRetentionProbe(db: D1Database) {
  const tables = await db
    .prepare(`SELECT name, type FROM sqlite_schema
    WHERE name GLOB 'e_retention_*' AND type IN ('table', 'view')`)
    .all<{ name: string; type: string }>();
  for (const { name, type } of tables.results) {
    if (!/^[a-zA-Z0-9_]+$/.test(name)) throw new Error('invalid probe table');
    await db
      .prepare(`DROP ${type === 'view' ? 'VIEW' : 'TABLE'} "${name}"`)
      .run();
  }
  const ownerTable = await db
    .prepare(`SELECT name FROM sqlite_schema WHERE name = ?`)
    .bind(RESOURCE_OWNERSHIP_TABLE)
    .first();
  if (ownerTable)
    await db
      .prepare(
        `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id GLOB 'e-retention-*'`,
      )
      .run();
  return {
    remaining: (
      await db
        .prepare(
          `SELECT name FROM sqlite_schema WHERE name GLOB 'e_retention_*'`,
        )
        .all()
    ).results,
  };
}

async function heldRetentionProbe(db: D1Database, variant: string) {
  await createSnapshotTable(db, E_PREFIX);
  if (variant !== 'reservation-schema') await createRetentionKeys(db);
  const resources = new D1ResourceOwnershipStore(db);
  const runId = 'e-retention-held';
  await resources.claim('run', runId, E_OWNER);
  const legacy = variant === 'legacy';
  await insertRetentionSnapshot(
    db,
    runId,
    legacy ? { status: 'success', value: 'old' } : retentionSnapshot(),
  ).run();
  if (variant !== 'reservation-schema')
    await insertRetentionKey(db, 'held-key', runId, { state: 'started' }).run();
  const advances: RunRetentionCursor[] = [];
  let artifacts = 0;
  let intervened: Awaited<ReturnType<typeof retentionState>> | undefined;
  const measured = measuredRetentionDatabase(
    db,
    variant === 'schema-churn'
      ? async (ordinal) => {
          await createSnapshotTable(db, `${E_PREFIX}churn${ordinal}_`);
        }
      : undefined,
  );
  let purged: number | undefined;
  let error: string | undefined;
  try {
    purged = await purgeExpiredWorkflowRuns(measured.database, {
      ...retentionOptions(advances),
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: E_KEYS,
      artifactStore: {
        deleteRun: async () => {
          artifacts += 1;
          if (variant === 'reservation-schema') {
            await createRetentionKeys(db);
            await insertRetentionKey(db, 'held-key', runId, {
              state: 'started',
            }).run();
          } else if (variant === 'schema-view') {
            await db
              .prepare(
                `CREATE VIEW ${E_PREFIX}view_mastra_workflow_snapshot AS SELECT * FROM ${E_TABLE}`,
              )
              .run();
          } else if (variant === 'schema' || variant === 'schema-churn') {
            if (variant === 'schema') {
              await createSnapshotTable(db, `${E_PREFIX}sibling_`);
              await insertRetentionSnapshot(
                db,
                runId,
                '{malformed',
                `${E_PREFIX}sibling_`,
              ).run();
            }
          } else if (variant === 'eligibility') {
            await db
              .prepare(`UPDATE ${E_TABLE} SET updatedAt = ?`)
              .bind(new Date(NOW).toISOString())
              .run();
          } else {
            const snapshot = retentionSnapshot(
              variant === 'generation' ? 'generation-two' : 'generation-one',
            );
            const provenance =
              snapshot.requestContext['flowsafe.runProvenance'];
            let replacement: unknown = snapshot;
            if (variant === 'capsule')
              provenance.startIdentity = {
                ...E_START,
                owner: { kind: 'human', id: 'Different initiator' },
              };
            if (variant === 'boolean')
              replacement = JSON.stringify(snapshot).replace(
                '"threaded":false',
                '"threaded":0',
              );
            if (variant === 'null')
              replacement = JSON.stringify(snapshot).replace(
                '"agentStart":{"threaded":false}',
                '"agentStart":null',
              );
            if (variant === 'duplicate')
              replacement = JSON.stringify(snapshot).replace(
                '"startToken":"generation-one"',
                '"startToken":"generation-one","startToken":"generation-two"',
              );
            if (variant === 'legacy')
              replacement = { status: 'success', value: 'replacement' };
            await db
              .prepare(`UPDATE ${E_TABLE} SET snapshot = ?`)
              .bind(
                typeof replacement === 'string'
                  ? replacement
                  : JSON.stringify(replacement),
              )
              .run();
          }
          intervened = await retentionState(db);
          return 0;
        },
      },
    });
  } catch (caught) {
    error = String(caught);
  }
  return {
    purged,
    error,
    artifacts,
    advances,
    intervened,
    after: await retentionState(db),
    metrics: measured.metrics,
  };
}

async function duplicateEligibilityRetentionProbe(
  db: D1Database,
  phase: 'initial' | 'held',
) {
  await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db);
  const resources = new D1ResourceOwnershipStore(db);
  const cases = [
    'status',
    'escaped-status',
    'request-context',
    'lifecycle',
    'terminal',
    'cleanup',
    'success-control',
    'cleanup-control',
  ].map((variant) => {
    const runId = `e-retention-eligibility-${variant}`;
    const complete = {
      ...createEmptyWorkflowSnapshot(runId),
      ...retentionSnapshot(),
      status:
        variant === 'status' ||
        variant === 'escaped-status' ||
        variant === 'success-control'
          ? 'success'
          : 'cancelled',
      requestContext: {
        ...retentionSnapshot().requestContext,
        'flowsafe.runLifecycle': { terminal: { cleanupCompletedAt: 1 } },
      },
    };
    const eligible = JSON.stringify(complete);
    let snapshot = eligible;
    switch (variant) {
      case 'status':
        snapshot = eligible.replace(
          '"status":"success"',
          '"status":"success","status":"running"',
        );
        break;
      case 'escaped-status':
        snapshot = eligible.replace(
          '"status":"success"',
          '"status":"success","sta\\u0074us":"running"',
        );
        break;
      case 'request-context': {
        const context = JSON.stringify(complete.requestContext);
        snapshot = eligible.replace(
          `"requestContext":${context}`,
          `"requestContext":${context},"requestContext":${context.replace(
            '"cleanupCompletedAt":1',
            '"cleanupCompletedAt":null',
          )}`,
        );
        break;
      }
      case 'lifecycle':
        snapshot = eligible.replace(
          '"flowsafe.runLifecycle":{"terminal":{"cleanupCompletedAt":1}}',
          '"flowsafe.runLifecycle":{"terminal":{"cleanupCompletedAt":1}},"flowsafe.runLifecycle":{"terminal":{"cleanupCompletedAt":null}}',
        );
        break;
      case 'terminal':
        snapshot = eligible.replace(
          '"terminal":{"cleanupCompletedAt":1}',
          '"terminal":{"cleanupCompletedAt":1},"terminal":{"cleanupCompletedAt":null}',
        );
        break;
      case 'cleanup':
        snapshot = eligible.replace(
          '"cleanupCompletedAt":1',
          '"cleanupCompletedAt":1,"cleanupCompletedAt":null',
        );
        break;
    }
    return { variant, runId, eligible, snapshot };
  });
  for (const candidate of cases)
    await resources.claim('run', candidate.runId, E_OWNER);
  await db.batch(
    cases.flatMap(({ runId, eligible, snapshot }) => [
      insertRetentionSnapshot(
        db,
        runId,
        phase === 'held' ? eligible : snapshot,
      ),
      insertRetentionKey(db, `${runId}-expired`, runId),
      insertRetentionKey(db, `${runId}-started`, runId, { state: 'started' }),
    ]),
  );
  const before = await retentionState(db);
  const advances: RunRetentionCursor[] = [];
  const artifacts: string[] = [];
  const measured = measuredRetentionDatabase(db);
  const purged = await purgeExpiredWorkflowRuns(measured.database, {
    ...retentionOptions(advances),
    resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
    startIdempotencyTable: E_KEYS,
    artifactStore: {
      deleteRun: async (_workflowId, runId) => {
        artifacts.push(runId);
        if (phase === 'held') {
          const candidate = cases.find((entry) => entry.runId === runId);
          if (!candidate) throw new Error('unexpected retention candidate');
          await db
            .prepare(`UPDATE ${E_TABLE} SET snapshot = ? WHERE run_id = ?`)
            .bind(candidate.snapshot, runId)
            .run();
        }
        return 0;
      },
    },
  });
  const sqlEligibility = await db.batch(
    cases.map(({ snapshot }) =>
      db
        .prepare(`SELECT json_extract(?1, '$.status') AS status,
          json_extract(?1, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') AS cleanup`)
        .bind(snapshot),
    ),
  );
  return {
    cases: cases.map(({ variant, runId, snapshot }, index) => ({
      variant,
      runId,
      snapshot,
      sqlEligibility: sqlEligibility[index]?.results[0],
    })),
    purged,
    artifacts,
    advances,
    before,
    after: await retentionState(db),
    metrics: measured.metrics,
  };
}

async function cleanupTimestampRetentionProbe(
  db: D1Database,
  format: 'modern' | 'legacy',
  phase: 'initial' | 'held' | 'reread',
) {
  await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db);
  const resources = new D1ResourceOwnershipStore(db);
  const cases = [
    ['boolean', 'false'],
    ['string', '"done"'],
    ['object', '{}'],
    ['negative', '-1'],
    ['fractional', '0.5'],
    ['unsafe', '9007199254740992'],
    ['infinite', '1e309'],
    ['zero-control', '0'],
    ['real-control', '1.0'],
    ['exponent-control', '1e0'],
    ['safe-max-control', '9007199254740991'],
  ].map(([variant, marker], index) => {
    const runId = `e-retention-time-${variant}`;
    const status = index % 2 === 0 ? 'cancelled' : 'timed_out';
    const lifecycle = projectTerminalLifecycle(undefined, status, 0, [
      E_START.owner,
    ]);
    lifecycle.terminal.cleanupCompletedAt = 1;
    parseRunLifecycle(lifecycle);
    const eligible = JSON.stringify({
      ...createEmptyWorkflowSnapshot(runId),
      status,
      requestContext: {
        ...(format === 'modern' ? retentionSnapshot().requestContext : {}),
        'flowsafe.runLifecycle': lifecycle,
      },
    });
    const snapshot = eligible.replace(
      '"cleanupCompletedAt":1',
      `"cleanupCompletedAt":${marker}`,
    );
    let accepted = true;
    try {
      parseRunLifecycle(
        JSON.parse(snapshot).requestContext['flowsafe.runLifecycle'],
      );
    } catch {
      accepted = false;
    }
    return { variant, runId, eligible, snapshot, accepted };
  });
  const binding: Record<string, string | number | null> =
    format === 'legacy'
      ? {
          start_token: null,
          start_table_prefix: null,
          start_workflow_id: null,
        }
      : {};
  for (const candidate of cases)
    await resources.claim('run', candidate.runId, E_OWNER);
  await db.batch(
    cases.flatMap(({ runId, eligible, snapshot, accepted }) => [
      insertRetentionSnapshot(
        db,
        runId,
        phase === 'initial' || accepted ? snapshot : eligible,
      ),
      insertRetentionKey(db, `${runId}-expired`, runId, binding),
      insertRetentionKey(db, `${runId}-started`, runId, {
        ...binding,
        state: 'started',
      }),
    ]),
  );
  const replace = async (runId: unknown) => {
    const candidate = cases.find((entry) => entry.runId === runId);
    if (!candidate) throw new Error('unexpected cleanup timestamp candidate');
    await db
      .prepare(`UPDATE ${E_TABLE} SET snapshot = ? WHERE run_id = ?`)
      .bind(candidate.snapshot, candidate.runId)
      .run();
  };
  const before = await retentionState(db);
  const rawReads: unknown[] = [];
  const measured = measuredRetentionDatabase(
    db,
    undefined,
    async (sql, values) => {
      if (
        !sql.startsWith(
          'SELECT workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt',
        ) ||
        !sql.includes(`FROM "${E_TABLE}"`)
      )
        return;
      rawReads.push(values[1]);
      if (phase === 'reread') await replace(values[1]);
    },
  );
  const advances: RunRetentionCursor[] = [];
  const artifacts: string[] = [];
  const purged = await purgeExpiredWorkflowRuns(measured.database, {
    ...retentionOptions(advances),
    resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
    startIdempotencyTable: E_KEYS,
    artifactStore: {
      deleteRun: async (_workflowId, runId) => {
        artifacts.push(runId);
        if (phase === 'held') await replace(runId);
        return 0;
      },
    },
  });
  const jsonTypes = await db.batch<{ type: string }>(
    cases.map(({ snapshot }) =>
      db
        .prepare(
          `SELECT json_type(?, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') AS type`,
        )
        .bind(snapshot),
    ),
  );
  return {
    cases: cases.map(({ eligible: _eligible, ...candidate }, index) => ({
      ...candidate,
      sqlType: jsonTypes[index]?.results[0]?.type,
    })),
    purged,
    artifacts,
    rawReads,
    advances,
    before,
    after: await retentionState(db),
    metrics: measured.metrics,
  };
}

async function ownerAdmissionRetentionProbe(db: D1Database, order: string) {
  await createSnapshotTable(db, E_PREFIX);
  const resources = new D1ResourceOwnershipStore(db);
  const runId = 'e-retention-reused';
  const claimed = await resources.claim('run', runId, E_OWNER);
  const reserved = await resources.reserveAll(
    [{ kind: 'run', resourceId: runId }],
    E_OWNER,
    'correlation',
  );
  const beforeOwner = await db
    .prepare(
      `SELECT owner_id, reservation_token FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id = ?`,
    )
    .bind(runId)
    .first();
  await insertRetentionSnapshot(db, runId).run();
  const fence = new ExecutionFenceStore(db);
  const domain = new FencedWorkflowsStorageD1({
    binding: db,
    tablePrefix: E_PREFIX,
  });
  const capability = domain[FENCED_WORKFLOW_STORAGE];
  if (!capability) throw new Error('fenced admission capability missing');
  const startIdentity = {
    owner: E_START.owner,
    target: { kind: 'workflow' as const, id: 'new-physical-workflow' },
  };
  const execution = {
    tablePrefix: E_PREFIX,
    workflowId: startIdentity.target.id,
    runId,
    startToken: 'new-generation',
  };
  let admission: unknown;
  let definitive = false;
  const admit = async () => {
    try {
      const admitted = await capability.withInitialAdmission(
        {
          execution,
          attemptToken: 'correlation',
          startIdentity,
          fence,
          runOwnerGuard: { owner: E_OWNER, reservationToken: 'correlation' },
          requestContext: {
            runId,
            'breakwater.workflowScope': execution.workflowId,
            'flowsafe.runProvenance': {
              version: 2,
              startToken: execution.startToken,
              attemptToken: 'correlation',
              startIdentity,
              requestedBy: E_START.owner.id,
              requestedByKind: E_START.owner.kind,
              resumeCounts: [],
            },
          },
          onInitialWriteAttempt: () => {},
        },
        () =>
          domain.persistWorkflowSnapshot({
            workflowName: execution.workflowId,
            runId,
            snapshot: {
              ...createEmptyWorkflowSnapshot(runId),
              status: 'pending',
            },
          }),
      );
      admission = { execution: admitted.witness.execution };
    } catch (error) {
      definitive = isDefinitiveInitialAdmissionRefusal(error, execution);
      admission = {
        error: String(error),
        reason: (error as { reason?: unknown }).reason,
      };
    }
  };
  const advances: RunRetentionCursor[] = [];
  const purged = await purgeExpiredWorkflowRuns(db, {
    ...retentionOptions(advances),
    resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
    ...(order === 'admission-first'
      ? {
          artifactStore: {
            deleteRun: async () => {
              await admit();
              return 0;
            },
          },
        }
      : {}),
  });
  if (order === 'purge-first') await admit();
  return {
    claimed,
    reserved,
    beforeOwner,
    purged,
    advances,
    admission,
    definitive,
    owner: (await resources.owner('run', runId)) ?? null,
    snapshots: (
      await db
        .prepare(
          `SELECT workflow_name, run_id, snapshot FROM ${E_TABLE} ORDER BY workflow_name`,
        )
        .all()
    ).results,
  };
}

async function siblingOwnerRetentionProbe(db: D1Database) {
  await createSnapshotTable(db, E_PREFIX);
  await createSnapshotTable(db, `${E_PREFIX}sibling_`);
  const resources = new D1ResourceOwnershipStore(db);
  const runs = ['reserved', 'cross-workflow', 'malformed-sibling', 'unshared'];
  for (const name of runs) {
    const runId = `e-retention-${name}`;
    if (name === 'reserved')
      await resources.reserveAll(
        [{ kind: 'run', resourceId: runId }],
        E_OWNER,
        'held-reservation',
      );
    else await resources.claim('run', runId, E_OWNER);
    await insertRetentionSnapshot(db, runId).run();
  }
  await insertRetentionSnapshot(
    db,
    'e-retention-cross-workflow',
    { status: 'running' },
    E_PREFIX,
    'other-workflow',
  ).run();
  await insertRetentionSnapshot(
    db,
    'e-retention-malformed-sibling',
    '{not-json',
    `${E_PREFIX}sibling_`,
    'other-workflow',
  ).run();
  const advances: RunRetentionCursor[] = [];
  const purged = await purgeExpiredWorkflowRuns(db, {
    ...retentionOptions(advances),
    resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
  });
  return {
    purged,
    advances,
    owners: (
      await db
        .prepare(
          `SELECT resource_id, owner_id, reservation_token FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id GLOB 'e-retention-*' ORDER BY resource_id`,
        )
        .all()
    ).results,
    sibling: (
      await db
        .prepare(
          `SELECT run_id, snapshot FROM ${E_PREFIX}sibling_mastra_workflow_snapshot`,
        )
        .all()
    ).results,
    snapshots: (
      await db.prepare(`SELECT workflow_name, run_id FROM ${E_TABLE}`).all()
    ).results,
  };
}

async function bindingRetentionProbe(db: D1Database) {
  await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db);
  const runId = 'e-retention-binding';
  await new D1ResourceOwnershipStore(db).claim('run', runId, E_OWNER);
  await insertRetentionSnapshot(db, runId).run();
  const cases: Record<string, Record<string, string | number | null>> = {
    'alias-expired': {},
    'alias-recent': { updated_at: NOW },
    'alias-started': { state: 'started' },
    'other-token': { state: 'started', start_token: 'other' },
    'other-prefix': { state: 'started', start_table_prefix: 'other_' },
    'other-workflow': { state: 'started', start_workflow_id: 'other' },
    'other-owner': { state: 'started', owner_id: 'Other' },
    'other-owner-kind': { state: 'started', owner_kind: 'service' },
    'other-target': { state: 'started', target_id: 'other' },
    'other-target-kind': {
      state: 'started',
      target_kind: 'workflow',
      thread_id: null,
    },
    'other-thread': { state: 'started', thread_id: 'other' },
    'legacy-started': {
      state: 'started',
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
    },
    'unbound-started': {
      state: 'started',
      start_token: '',
      start_table_prefix: null,
      start_workflow_id: null,
    },
    'unbound-terminal': {
      start_token: '',
      start_table_prefix: null,
      start_workflow_id: null,
    },
    'null-started': { state: 'started', start_table_prefix: null },
    'null-terminal': { start_table_prefix: null },
  };
  await db.batch(
    Object.entries(cases).map(([key, patch]) =>
      insertRetentionKey(db, key, runId, patch),
    ),
  );
  const before = await retentionState(db);
  const advances: RunRetentionCursor[] = [];
  const purged = await purgeExpiredWorkflowRuns(db, {
    ...retentionOptions(advances),
    resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
    startIdempotencyTable: E_KEYS,
  });
  return { purged, advances, before, after: await retentionState(db) };
}

async function orphanRetentionProbe(
  db: D1Database,
  stage: number | 'replacement' | 'absent',
) {
  if (stage !== 'absent') await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db, typeof stage === 'number' ? stage : 3);
  const advances: RunRetentionCursor[] = [];
  if (typeof stage === 'number') {
    await insertRetentionSnapshot(
      db,
      'e-retention-present',
      '{malformed',
    ).run();
    const legacy = Object.fromEntries(
      ['start_token', 'start_table_prefix']
        .slice(0, stage)
        .map((key) => [key, null]),
    );
    await db.batch([
      insertRetentionKey(
        db,
        'legacy-present',
        'e-retention-present',
        legacy,
        stage,
      ),
      insertRetentionKey(
        db,
        'legacy-orphan',
        'e-retention-absent',
        legacy,
        stage,
      ),
      ...(stage > 0
        ? [
            insertRetentionKey(
              db,
              'partial-nonnull',
              'e-retention-ambiguous',
              { ...legacy, start_token: 'non-null' },
              stage,
            ),
          ]
        : []),
    ]);
    const before = (
      await db.prepare(`SELECT * FROM ${E_KEYS} ORDER BY key`).all()
    ).results;
    const purged = await purgeExpiredWorkflowRuns(db, {
      ...retentionOptions(advances),
      startIdempotencyTable: E_KEYS,
    });
    return {
      purged,
      advances,
      before,
      keys: (await db.prepare(`SELECT * FROM ${E_KEYS} ORDER BY key`).all())
        .results,
    };
  }
  await insertRetentionKey(db, 'orphan-key', 'e-retention-orphan').run();
  if (stage === 'absent') {
    const purged = await purgeExpiredWorkflowRuns(db, {
      ...retentionOptions(advances),
      startIdempotencyTable: E_KEYS,
    });
    return {
      purged,
      advances,
      keys: (await db.prepare(`SELECT * FROM ${E_KEYS}`).all()).results,
    };
  }
  const replacement = {
    ...retentionSnapshot('generation-two'),
    status: 'running',
  };
  await insertRetentionSnapshot(db, 'e-retention-orphan', replacement).run();
  let held = 0;
  const measured = measuredRetentionDatabase(db, async () => {
    held += 1;
    await db
      .prepare(`UPDATE ${E_TABLE} SET snapshot = ?`)
      .bind(JSON.stringify({ ...retentionSnapshot(), status: 'running' }))
      .run();
  });
  const first = await purgeExpiredWorkflowRuns(measured.database, {
    ...retentionOptions(advances),
    startIdempotencyTable: E_KEYS,
  });
  const heldKeys = (await db.prepare(`SELECT * FROM ${E_KEYS}`).all()).results;
  await db
    .prepare(`UPDATE ${E_TABLE} SET snapshot = ?`)
    .bind(JSON.stringify(replacement))
    .run();
  const second = await purgeExpiredWorkflowRuns(db, {
    ...retentionOptions(advances),
    startIdempotencyTable: E_KEYS,
  });
  return {
    first,
    second,
    held,
    heldKeys,
    advances,
    keys: (await db.prepare(`SELECT * FROM ${E_KEYS}`).all()).results,
    snapshot: await db.prepare(`SELECT snapshot FROM ${E_TABLE}`).first(),
  };
}

function maximalRetentionSnapshot(
  token: string,
  ownerId = '😀'.repeat(100),
  capsuleBytes = 4096,
) {
  const snapshot = retentionSnapshot(token);
  const provenance = snapshot.requestContext['flowsafe.runProvenance'];
  provenance.startIdentity = {
    owner: { kind: 'human', id: ownerId },
    target: { kind: 'agent', id: 'a'.repeat(200), threadId: 't'.repeat(200) },
  };
  const startIdentity = { ...provenance.startIdentity, padding: '' };
  const ownedBytes = () =>
    [2, token, startIdentity, provenance.agentStart].reduce<number>(
      (sum, value) => sum + encoder.encode(JSON.stringify(value)).length,
      0,
    );
  const remainder = capsuleBytes - ownedBytes();
  if (remainder < 0) throw new Error('capsule fixture exceeds requested size');
  startIdentity.padding =
    '\\'.repeat(Math.floor(remainder / 2)) + 'x'.repeat(remainder % 2);
  provenance.startIdentity = startIdentity;
  if (ownedBytes() !== capsuleBytes)
    throw new Error('capsule byte fixture is inconsistent');
  return snapshot;
}

async function setupMaximumRetentionProbe(
  db: D1Database,
  mode: 'modern' | 'legacy',
) {
  await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db);
  const census = await db
    .prepare(
      `SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND lower(name) GLOB '*mastra_workflow_snapshot' ORDER BY lower(name)`,
    )
    .all<{ name: string }>();
  const preexistingNamespaces = census.results.length - 1;
  if (census.results.length > 64)
    throw new Error('harness census already exceeds retention limit');
  const prefixes = [E_PREFIX];
  for (let index = census.results.length; index < 64; index += 1) {
    const prefix = `${E_PREFIX}namespace_${index}_`.padEnd(39, '_');
    await createSnapshotTable(db, prefix);
    prefixes.push(prefix);
  }
  const resources = new D1ResourceOwnershipStore(db);
  await resources.claim('run', 'e-retention-bootstrap', E_OWNER);
  await db
    .prepare(
      `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id = 'e-retention-bootstrap'`,
    )
    .run();
  const statements: D1PreparedStatement[] = [];
  let maxSnapshotBytes = 0;
  for (let index = 0; index < 90; index += 1) {
    const suffix = String(index).padStart(3, '0');
    const runId = `e-retention-${suffix}`.padEnd(200, 'r');
    const workflowId = `workflow-${suffix}`.padEnd(200, 'w');
    const token = `generation-${suffix}`.padEnd(200, 's');
    const snapshot = {
      ...(mode === 'modern'
        ? maximalRetentionSnapshot(token, '\\'.repeat(200))
        : { status: 'success' }),
      unrelated: 'x'.repeat(index === 0 ? 1_900_000 : 10_000),
    };
    maxSnapshotBytes = Math.max(
      maxSnapshotBytes,
      encoder.encode(JSON.stringify(snapshot)).length,
    );
    statements.push(
      insertRetentionSnapshot(db, runId, snapshot, E_PREFIX, workflowId),
      db
        .prepare(
          `INSERT INTO ${RESOURCE_OWNERSHIP_TABLE} (resource_kind, resource_id, owner_kind, owner_id) VALUES ('run', ?, 'human', ?)`,
        )
        .bind(runId, E_OWNER.id),
      insertRetentionKey(
        db,
        `orphan-${suffix}`,
        `e-retention-orphan-${suffix}`,
        { start_table_prefix: prefixes[index % prefixes.length] as string },
      ),
    );
  }
  // Orphan rows precede aliases so the same invocation exercises both full pages.
  for (let index = 0; index < statements.length; index += 20)
    await db.batch(statements.slice(index, index + 20));
  if (mode === 'modern') {
    const aliases = Array.from({ length: 90 }, (_, index) => {
      const suffix = String(index).padStart(3, '0');
      return insertRetentionKey(
        db,
        `alias-${suffix}`,
        `e-retention-${suffix}`.padEnd(200, 'r'),
        {
          start_token: `generation-${suffix}`.padEnd(200, 's'),
          start_workflow_id: `workflow-${suffix}`.padEnd(200, 'w'),
          owner_id: '\\'.repeat(200),
          target_id: 'a'.repeat(200),
          thread_id: 't'.repeat(200),
          state: 'started',
        },
      );
    });
    for (let index = 0; index < aliases.length; index += 20)
      await db.batch(aliases.slice(index, index + 20));
  }
  const siblingPrefix = prefixes.at(-1);
  if (!siblingPrefix || siblingPrefix === E_PREFIX)
    throw new Error('maximum fixture requires a separate namespace');
  await insertRetentionSnapshot(
    db,
    'e-retention-089'.padEnd(200, 'r'),
    '{malformed',
    siblingPrefix,
  ).run();
  return {
    namespaces: 64,
    preexistingNamespaces,
    createdNamespaces: prefixes.length,
    rows: 90,
    maxSnapshotBytes,
    capsuleBytes: mode === 'modern' ? 4096 : null,
    ownerUtf16Units: mode === 'modern' ? 200 : null,
  };
}

async function maximumRetentionProbe(db: D1Database, retry = false) {
  const measured = measuredRetentionDatabase(db);
  const advances: RunRetentionCursor[] = [];
  const started = performance.now();
  let purged: number | undefined;
  let error: string | undefined;
  let artifacts = 0;
  try {
    purged = await purgeExpiredWorkflowRuns(measured.database, {
      ...retentionOptions(advances),
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: E_KEYS,
      limit: 90,
      ...(retry
        ? {
            artifactStore: {
              deleteRun: async () => {
                artifacts += 1;
                if (artifacts === 1) {
                  const namespace = await db
                    .prepare(
                      `SELECT name FROM sqlite_schema WHERE type = 'table' AND name GLOB 'e_retention_namespace_*mastra_workflow_snapshot' ORDER BY name LIMIT 1`,
                    )
                    .first<{ name: string }>();
                  if (
                    !namespace ||
                    !/^e_retention_namespace_[a-z0-9_]+$/.test(namespace.name)
                  )
                    throw new Error('retry fixture namespace missing');
                  await db
                    .prepare(
                      `ALTER TABLE "${namespace.name}" RENAME TO ${E_PREFIX}retry_mastra_workflow_snapshot`,
                    )
                    .run();
                }
                return 0;
              },
            },
          }
        : {}),
    });
  } catch (caught) {
    error = String(caught);
  }
  const elapsedMs = performance.now() - started;
  const [snapshots, keys, owners, census] = await Promise.all([
    db.prepare(`SELECT count(*) AS count FROM ${E_TABLE}`).first(),
    db
      .prepare(`SELECT key, state, updated_at FROM ${E_KEYS} ORDER BY key`)
      .all(),
    db
      .prepare(
        `SELECT resource_id, owner_id FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id GLOB 'e-retention-*'`,
      )
      .all(),
    db
      .prepare(
        `SELECT count(*) AS count FROM sqlite_schema WHERE type IN ('table', 'view') AND lower(name) GLOB '*mastra_workflow_snapshot'`,
      )
      .first(),
  ]);
  return {
    purged,
    error,
    advances,
    artifacts,
    metrics: measured.metrics,
    elapsedMs,
    snapshots,
    keys: keys.results,
    owners: owners.results,
    census,
  };
}

async function overflowRetentionProbe(db: D1Database) {
  const countState = async () => {
    const rows = await db.batch([
      db.prepare(`SELECT count(*) AS count FROM ${E_TABLE}`),
      db.prepare(`SELECT count(*) AS count FROM ${E_KEYS}`),
      db.prepare(
        `SELECT count(*) AS count FROM ${RESOURCE_OWNERSHIP_TABLE} WHERE resource_id GLOB 'e-retention-*'`,
      ),
    ]);
    return rows.map(({ results }) => results);
  };
  const before = await countState();
  await createSnapshotTable(db, `${E_PREFIX}overflow_`);
  const advances: RunRetentionCursor[] = [];
  let artifacts = 0;
  let error: string | undefined;
  try {
    await purgeExpiredWorkflowRuns(db, {
      ...retentionOptions(advances),
      resourceOwnerTable: RESOURCE_OWNERSHIP_TABLE,
      startIdempotencyTable: E_KEYS,
      artifactStore: {
        deleteRun: async () => {
          artifacts += 1;
          return 0;
        },
      },
    });
  } catch (caught) {
    error = String(caught);
  } finally {
    await db
      .prepare(`DROP TABLE ${E_PREFIX}overflow_mastra_workflow_snapshot`)
      .run();
  }
  return { error, artifacts, advances, before, after: await countState() };
}

async function limitsRetentionProbe(db: D1Database) {
  await createSnapshotTable(db, E_PREFIX);
  await createRetentionKeys(db);
  await db.batch([
    insertRetentionKey(db, 'k'.repeat(4096), 'e-retention-oversize-key', {
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
    }),
    insertRetentionKey(db, 'short-orphan', 'e-retention-key-orphan', {
      start_token: null,
      start_table_prefix: null,
      start_workflow_id: null,
    }),
  ]);
  const longRegistry = `${E_PREFIX}registry_${'r'.repeat(70)}`;
  await db.prepare(`ALTER TABLE ${E_KEYS} RENAME TO ${longRegistry}`).run();
  const cases = [
    [
      'capsule-boundary',
      maximalRetentionSnapshot('generation', '😀'.repeat(100), 4096),
    ],
    [
      'capsule-overflow',
      maximalRetentionSnapshot('generation', '😀'.repeat(100), 4097),
    ],
    [
      'utf16-overflow',
      maximalRetentionSnapshot('generation', `${'😀'.repeat(100)}x`, 4096),
    ],
    ['unpaired-legacy', { status: 'success' }],
  ] as const;
  await db.batch(
    cases.map(([name, snapshot]) =>
      insertRetentionSnapshot(db, `e-retention-${name}`, snapshot),
    ),
  );
  const measured = measuredRetentionDatabase(db);
  const advances: RunRetentionCursor[] = [];
  const artifacts: string[] = [];
  const purged = await purgeExpiredWorkflowRuns(measured.database, {
    ...retentionOptions(advances),
    startIdempotencyTable: longRegistry,
    artifactStore: {
      deleteRun: async (_workflow, runId) => {
        artifacts.push(runId);
        return 0;
      },
    },
  });
  return {
    purged,
    advances,
    artifacts,
    registryLength: longRegistry.length,
    remainingKeyLengths: (
      await db
        .prepare(`SELECT length(key) AS length FROM ${longRegistry}`)
        .all()
    ).results,
    metrics: measured.metrics,
    remaining: (
      await db.prepare(`SELECT run_id FROM ${E_TABLE} ORDER BY run_id`).all()
    ).results,
  };
}

async function progressRetentionProbe(db: D1Database) {
  await createSnapshotTable(db, E_PREFIX);
  await db.batch(
    Array.from({ length: 93 }, (_, index) =>
      insertRetentionSnapshot(
        db,
        `e-retention-progress-${index}`,
        index < 91
          ? maximalRetentionSnapshot('generation', 'owner', 4097)
          : retentionSnapshot(),
      ),
    ),
  );
  await db
    .prepare(
      `CREATE TABLE ${E_PREFIX}cursor (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`,
    )
    .run();
  const pages: Array<{
    purged?: number;
    error?: string;
    cursor: RunRetentionCursor;
  }> = [];
  let artifactFailures = 0;
  const invoke = async () => {
    const stored = await db
      .prepare(`SELECT value FROM ${E_PREFIX}cursor WHERE id = 1`)
      .first<{ value: string }>();
    const options = {
      ...retentionOptions([]),
      ...(stored
        ? { cursor: JSON.parse(stored.value) as RunRetentionCursor }
        : {}),
      limit: 90,
      advanceCursor: async (cursor: RunRetentionCursor) => {
        await db.batch([
          db
            .prepare(
              `INSERT INTO ${E_PREFIX}cursor VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET value = excluded.value`,
            )
            .bind(JSON.stringify(cursor)),
        ]);
      },
      artifactStore: {
        deleteRun: async (_workflow: string, runId: string) => {
          if (runId === 'e-retention-progress-91' && artifactFailures === 0) {
            artifactFailures += 1;
            throw Object.create(null);
          }
          return 0;
        },
      },
    };
    let purged: number | undefined;
    let error: string | undefined;
    try {
      purged = await purgeExpiredWorkflowRuns(db, options);
    } catch (caught) {
      error = String(caught);
    }
    const checkpoint = await db
      .prepare(`SELECT value FROM ${E_PREFIX}cursor WHERE id = 1`)
      .first<{ value: string }>();
    if (!checkpoint) throw new Error('retention cursor was not persisted');
    pages.push({ purged, error, cursor: JSON.parse(checkpoint.value) });
  };
  await invoke();
  await insertRetentionSnapshot(db, 'e-retention-progress-new').run();
  await invoke();
  const afterFirstCycle = (
    await db
      .prepare(
        `SELECT run_id FROM ${E_TABLE} WHERE run_id IN ('e-retention-progress-91', 'e-retention-progress-92', 'e-retention-progress-new') ORDER BY run_id`,
      )
      .all()
  ).results;
  await invoke();
  await invoke();
  const remainingEligible = (
    await db
      .prepare(
        `SELECT run_id FROM ${E_TABLE} WHERE run_id IN ('e-retention-progress-91', 'e-retention-progress-92', 'e-retention-progress-new')`,
      )
      .all()
  ).results;
  return { pages, artifactFailures, afterFirstCycle, remainingEligible };
}

async function eRetentionProbe(
  db: D1Database,
  scenario: string,
  action: string | undefined,
) {
  if (scenario === 'cleanup') return cleanupRetentionProbe(db);
  if (scenario === 'maximum-modern' || scenario === 'maximum-legacy') {
    if (action === 'setup')
      return setupMaximumRetentionProbe(
        db,
        scenario === 'maximum-modern' ? 'modern' : 'legacy',
      );
    if (action === 'exercise') return maximumRetentionProbe(db);
    if (action === 'exercise-retry') return maximumRetentionProbe(db, true);
    if (action === 'overflow') return overflowRetentionProbe(db);
    throw new Error('maximum retention scenario requires setup or exercise');
  }
  try {
    if (
      (scenario === 'cleanup-time-modern' ||
        scenario === 'cleanup-time-legacy') &&
      (action === 'initial' ||
        action === 'held' ||
        (scenario === 'cleanup-time-legacy' && action === 'reread'))
    )
      return await cleanupTimestampRetentionProbe(
        db,
        scenario === 'cleanup-time-modern' ? 'modern' : 'legacy',
        action,
      );
    if (
      scenario === 'duplicate-eligibility' &&
      (action === 'initial' || action === 'held')
    )
      return await duplicateEligibilityRetentionProbe(db, action);
    if (
      [
        'generation',
        'capsule',
        'boolean',
        'null',
        'duplicate',
        'legacy',
        'eligibility',
        'schema',
        'schema-churn',
        'reservation-schema',
        'schema-view',
      ].includes(scenario)
    )
      return await heldRetentionProbe(db, scenario);
    if (scenario === 'purge-first' || scenario === 'admission-first')
      return await ownerAdmissionRetentionProbe(db, scenario);
    if (scenario === 'siblings') return await siblingOwnerRetentionProbe(db);
    if (scenario === 'bindings') return await bindingRetentionProbe(db);
    if (scenario === 'orphan-replacement')
      return await orphanRetentionProbe(db, 'replacement');
    if (scenario === 'orphan-absent')
      return await orphanRetentionProbe(db, 'absent');
    if (
      scenario === 'partial-0' ||
      scenario === 'partial-1' ||
      scenario === 'partial-2'
    )
      return await orphanRetentionProbe(db, Number(scenario.at(-1)));
    if (scenario === 'limits') return await limitsRetentionProbe(db);
    if (scenario === 'progress') return await progressRetentionProbe(db);
    throw new Error('unknown retention scenario');
  } finally {
    await cleanupRetentionProbe(db);
  }
}

const P3_PREFIX = 'p3_';
const P3_SCHEDULES = `${P3_PREFIX}mastra_schedules`;
const P3_TRIGGERS = `${P3_PREFIX}mastra_schedule_triggers`;
const P3_SNAPSHOTS = `${P3_PREFIX}mastra_workflow_snapshot`;
const P3_ID = 'schedule-p3-existing';
const P3_WORKFLOW = 'workflow-schedule';
const P3_TOKEN = 'p3-local-operator';
const P3_OWNER = { kind: 'human' as const, id: 'p3-owner' };

async function p3Cleanup(db: D1Database) {
  const shared = [
    EXECUTION_FENCE_TABLE,
    RESOURCE_OWNERSHIP_TABLE,
    START_IDEMPOTENCY_TABLE,
  ];
  const selected = () =>
    db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND (name GLOB 'p3_*' OR name IN (?, ?, ?)) ORDER BY name LIMIT 129",
      )
      .bind(...shared)
      .all<{ name: string }>();
  const tables = (await selected()).results;
  if (tables.length > 128) throw new Error('P3 cleanup table bound exceeded');
  if (tables.length)
    await db.batch(
      tables.map(({ name }) => {
        if (!name.startsWith(P3_PREFIX) && !shared.includes(name))
          throw new Error('P3 cleanup selected a foreign table');
        return db.prepare(`DROP TABLE "${name.replaceAll('"', '""')}"`);
      }),
    );
  const remaining = (await selected()).results;
  if (remaining.length) throw new Error('P3 cleanup left fixture tables');
  return { remaining };
}

type P3Phase = 'none' | 'auth' | 'final' | 'provider';
type P3Action =
  | 'none'
  | 'activate'
  | 'advance'
  | 'cycle'
  | 'capture'
  | 'resume-race';
type P3Operation =
  | 'create'
  | 'update'
  | 'pause'
  | 'resume'
  | 'delete'
  | 'pause-noop'
  | 'resume-noop';

function p3Choice<T extends string>(
  params: URLSearchParams,
  key: string,
  values: readonly T[],
  fallback: T,
): T {
  const value = params.get(key) ?? fallback;
  if (!values.includes(value as T)) throw new Error(`invalid P3 ${key}`);
  return value as T;
}

function p3Options(url: URL) {
  const epoch = p3Choice(
    url.searchParams,
    'epoch',
    ['missing', 'stale', 'future', 'current', 'invalid'],
    'current',
  );
  return {
    phase: p3Choice<P3Phase>(
      url.searchParams,
      'phase',
      ['none', 'auth', 'final', 'provider'],
      'none',
    ),
    action: p3Choice<P3Action>(
      url.searchParams,
      'action',
      ['none', 'activate', 'advance', 'cycle', 'capture', 'resume-race'],
      'none',
    ),
    epoch:
      epoch === 'missing'
        ? undefined
        : epoch === 'stale'
          ? 0
          : epoch === 'future'
            ? 3
            : epoch === 'invalid'
              ? -1
              : 2,
    closed: url.searchParams.get('closed') === 'true',
  };
}

function p3Metrics() {
  return {
    statements: 0,
    batches: 0,
    maxBatchStatements: 0,
    maxSqlBytes: 0,
    maxBindings: 0,
    maxBoundStringBytes: 0,
    maxResultBytes: 0,
    rowsRead: 0,
    rowsWritten: 0,
  };
}

interface P3Statement {
  native: D1PreparedStatement;
  sql: string;
  values: unknown[];
}

function p3Database(
  db: D1Database,
  hooks: {
    beforeBatch?: (statements: P3Statement[]) => Promise<void>;
    afterBatch?: (
      statements: P3Statement[],
      results: D1Result[],
    ) => Promise<void>;
    afterStatement?: (sql: string, method: string) => void;
  } = {},
) {
  let metrics = p3Metrics();
  const entries = new WeakMap<D1PreparedStatement, P3Statement>();
  const recordStatement = ({ sql, values }: P3Statement) => {
    metrics.statements++;
    metrics.maxSqlBytes = Math.max(
      metrics.maxSqlBytes,
      encoder.encode(sql).length,
    );
    metrics.maxBindings = Math.max(metrics.maxBindings, values.length);
    for (const value of values) {
      if (typeof value === 'string')
        metrics.maxBoundStringBytes = Math.max(
          metrics.maxBoundStringBytes,
          encoder.encode(value).length,
        );
    }
  };
  const recordResult = (result: unknown) => {
    const envelope = result as {
      results?: unknown;
      meta?: { rows_read?: number; rows_written?: number };
    } | null;
    metrics.maxResultBytes = Math.max(
      metrics.maxResultBytes,
      encoder.encode(JSON.stringify(envelope?.results ?? result) ?? '').length,
    );
    metrics.rowsRead += envelope?.meta?.rows_read ?? 0;
    metrics.rowsWritten += envelope?.meta?.rows_written ?? 0;
  };
  const wrap = (entry: P3Statement): D1PreparedStatement => {
    const statement = new Proxy(entry.native, {
      get(target, key) {
        if (key === 'bind')
          return (...values: unknown[]) =>
            wrap({ native: target.bind(...values), sql: entry.sql, values });
        const member = Reflect.get(target, key, target);
        if (typeof member !== 'function') return member;
        if (['first', 'all', 'run', 'raw'].includes(String(key)))
          return async (...args: unknown[]) => {
            recordStatement(entry);
            const outcome = await Reflect.apply(member, target, args);
            recordResult(outcome);
            hooks.afterStatement?.(entry.sql, String(key));
            return outcome;
          };
        return member.bind(target);
      },
    });
    entries.set(statement, entry);
    return statement;
  };
  const database = new Proxy(db, {
    get(target, key) {
      if (key === 'prepare')
        return (sql: string) =>
          wrap({ native: target.prepare(sql), sql, values: [] });
      if (key === 'batch')
        return async (statements: D1PreparedStatement[]) => {
          const selected = statements.map((statement) => {
            const entry = entries.get(statement);
            if (!entry) throw new Error('foreign P3 prepared statement');
            return entry;
          });
          await hooks.beforeBatch?.(selected);
          metrics.batches++;
          metrics.maxBatchStatements = Math.max(
            metrics.maxBatchStatements,
            selected.length,
          );
          selected.forEach(recordStatement);
          const result = await target.batch(
            selected.map(({ native }) => native),
          );
          result.forEach(recordResult);
          if (hooks.afterBatch) await hooks.afterBatch(selected, result);
          return result;
        };
      const member = Reflect.get(target, key, target);
      return typeof member === 'function' ? member.bind(target) : member;
    },
  });
  return {
    database,
    get metrics() {
      return metrics;
    },
    reset() {
      const previous = metrics;
      metrics = p3Metrics();
      return previous;
    },
  };
}

function p3Deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function p3Gate() {
  const entered = p3Deferred();
  const release = p3Deferred();
  let hits = 0;
  return {
    entered: entered.promise,
    release: release.resolve,
    get hits() {
      return hits;
    },
    async wait() {
      hits++;
      if (hits !== 1) throw new Error('P3 gate entered more than once');
      entered.resolve();
      await release.promise;
    },
  };
}

async function p3Deadline<T>(pending: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      pending,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`P3 ${label} exceeded 8000ms`)),
          8_000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function p3Held<T>(
  gate: ReturnType<typeof p3Gate>,
  work: () => Promise<T>,
  intervene: () => Promise<void>,
): Promise<T> {
  const pending = work();
  const observed = pending.then(
    (value) => ({ value }),
    (error: unknown) => ({ error }),
  );
  try {
    await p3Deadline(
      Promise.race([
        gate.entered,
        observed.then(() => {
          throw new Error('P3 request finished before the selected boundary');
        }),
      ]),
      'boundary wait',
    );
    await p3Deadline(intervene(), 'intervention');
  } finally {
    gate.release();
    await p3Deadline(observed, 'request cleanup');
  }
  const outcome = await observed;
  if ('error' in outcome) throw outcome.error;
  return outcome.value;
}

async function p3Transition(
  fence: ExecutionFenceStore,
  next: 'open' | 'draining',
  advanceMutationEpoch = false,
) {
  const reading = await fence.read();
  return fence.transition({
    expected: reading.state,
    next,
    expectedMutationEpoch: reading.mutationEpoch,
    expectedRevision: reading.transitionRevision,
    ...(advanceMutationEpoch ? { advanceMutationEpoch } : {}),
  });
}

async function p3Activate(fence: ExecutionFenceStore) {
  await p3Transition(fence, 'draining', true);
  await p3Transition(fence, 'open');
}

function p3Resolver(
  db: D1Database,
  epoch: number | undefined,
  gate?: ReturnType<typeof p3Gate>,
) {
  let source: ActorContext | undefined;
  const resolve = createActorResolver({
    authenticate: (request) =>
      request.headers.get('authorization') === `Bearer ${P3_TOKEN}`
        ? { id: P3_OWNER.id, role: 'operator' }
        : undefined,
    storeFactory: new D1ApprovalStoreFactory(db),
    mutationEpoch: epoch === -1 ? undefined : epoch,
    newRunId: () => 'p3-run',
    buildService: () => {
      throw new Error('P3 workflow does not request approval');
    },
  });
  return {
    async resolve(request: Request) {
      const context = await resolve(request);
      if (!context) return undefined;
      source = {
        ...context,
        ...(epoch === -1 ? { mutationEpoch: -1 } : {}),
        canAccessResource: async (...args) => {
          const allowed = await context.canAccessResource(...args);
          if (gate && gate.hits === 0) await gate.wait();
          return allowed;
        },
      };
      return source;
    },
    replace() {
      if (!source) throw new Error('P3 actor has not authenticated');
      Object.assign(source, {
        mutationEpoch: epoch === 2 ? 3 : 2,
        actor: { id: 'replacement', role: 'viewer' },
        principal: { kind: 'human', id: 'replacement', role: 'viewer' },
        resourceOwner: { kind: 'human', id: 'replacement' },
      });
    },
  };
}

function p3Request(
  path: string,
  method: string,
  body?: string,
  gate?: ReturnType<typeof p3Gate>,
) {
  const headers = {
    authorization: `Bearer ${P3_TOKEN}`,
    'content-type': 'application/json',
  };
  if (body === undefined)
    return new Request(`http://p3.test${path}`, { method, headers });
  const bytes = encoder.encode(body);
  const stream = gate
    ? new ReadableStream<Uint8Array>(
        {
          async pull(controller) {
            await gate.wait();
            controller.enqueue(bytes);
            controller.close();
          },
        },
        { highWaterMark: 0 },
      )
    : body;
  return new Request(`http://p3.test${path}`, {
    method,
    headers,
    body: stream,
  });
}

async function p3Response(response: Response | null) {
  if (!response) throw new Error('P3 router did not match request');
  return { status: response.status, body: await response.json() };
}

async function p3RawSchedules(db: D1Database) {
  const [schedules, triggers, owners] = await Promise.all([
    db.prepare(`SELECT * FROM ${P3_SCHEDULES} ORDER BY id`).all(),
    db.prepare(`SELECT * FROM ${P3_TRIGGERS} ORDER BY id`).all(),
    db
      .prepare(
        `SELECT * FROM ${RESOURCE_OWNERSHIP_TABLE} ORDER BY resource_kind, resource_id`,
      )
      .all(),
  ]);
  return {
    schedules: schedules.results,
    triggers: triggers.results,
    owners: owners.results,
  };
}

async function p3ScheduleProbe(db: D1Database, url: URL) {
  const options = p3Options(url);
  const operation = p3Choice<P3Operation>(
    url.searchParams,
    'operation',
    [
      'create',
      'update',
      'pause',
      'resume',
      'delete',
      'pause-noop',
      'resume-noop',
    ],
    'create',
  );
  const gate = p3Gate();
  let armed = false;
  let finalBatches = 0;
  const measured = p3Database(db, {
    beforeBatch: async (statements) => {
      if (
        !armed ||
        !statements.some(
          ({ sql }) =>
            sql.includes(P3_SCHEDULES) && sql.includes(EXECUTION_FENCE_TABLE),
        )
      )
        return;
      finalBatches++;
      if (options.phase === 'final' && gate.hits === 0) await gate.wait();
    },
  });
  const administration = p3Database(db);
  const evidence = p3Database(db);
  const fence = new ExecutionFenceStore(measured.database);
  const adminFence = new ExecutionFenceStore(administration.database);
  const store = new D1SchedulesStorage(measured.database, P3_PREFIX);
  const adminStore = new D1SchedulesStorage(administration.database, P3_PREFIX);
  await fence.seed('open');
  await store.createOwnedSchedule(
    {
      ...schedule(P3_ID),
      status:
        operation === 'resume' || operation === 'pause-noop'
          ? 'paused'
          : 'active',
    },
    P3_OWNER,
    10,
  );
  await db
    .prepare(`UPDATE ${P3_SCHEDULES} SET metadata = ?, target = ? WHERE id = ?`)
    .bind(
      '{ "seed": "original" }',
      '{ "type": "workflow", "workflowId": "workflow-schedule", "inputData": {} }',
      P3_ID,
    )
    .run();
  const deferred = url.searchParams.get('deferred') === 'true';
  const triggerCount = url.searchParams.get('history') === 'true' ? 120 : 2;
  for (let index = 0; index < triggerCount; index++) {
    await store.recordTrigger({
      id: `p3-trigger-${String(index).padStart(3, '0')}`,
      scheduleId: P3_ID,
      runId: `p3-prior-${index}`,
      scheduledFireAt: NOW,
      actualFireAt: NOW,
      outcome: deferred && index === 0 ? 'deferred' : 'published',
      metadata: { original: true },
    });
  }
  if (options.action !== 'activate') {
    await p3Activate(adminFence);
    await p3Activate(adminFence);
  }
  if (options.closed) await p3Transition(adminFence, 'draining');
  const resolver = p3Resolver(
    measured.database,
    options.epoch,
    options.phase === 'auth' && operation !== 'create' && operation !== 'update'
      ? gate
      : undefined,
  );
  const audit: unknown[] = [];
  const router = createScheduleRouter({
    resolve: resolver.resolve,
    store,
    executionFence: fence,
    targetPolicy: createScheduleTargetPolicy({
      workflows: [{ id: P3_WORKFLOW }],
      agents: [],
    }),
    validateThreadTarget: async () => {
      throw new Error('P3 workflow target cannot require a thread');
    },
    maxSchedules: url.searchParams.get('cap') === 'true' ? 1 : 10,
    audit: (event) => {
      audit.push(event);
    },
  });
  const method =
    operation === 'update'
      ? 'PATCH'
      : operation === 'delete'
        ? 'DELETE'
        : 'POST';
  const path =
    operation === 'create'
      ? '/api/schedules'
      : operation === 'update' || operation === 'delete'
        ? `/api/schedules/${P3_ID}`
        : `/api/schedules/${P3_ID}/${operation.startsWith('pause') ? 'pause' : 'resume'}`;
  const body: Record<string, unknown> =
    operation === 'create'
      ? {
          workflowId: P3_WORKFLOW,
          cron: '* * * * *',
          metadata: { edited: true },
        }
      : { metadata: { edited: true } };
  if (url.searchParams.get('patchPaused') === 'true') {
    delete body.metadata;
    body.status = 'paused';
  }
  const injected = url.searchParams.get('injection');
  if (injected === 'body') body.mutationEpoch = 2;
  if (injected === 'stored')
    body.requestContext = { mutationEpoch: 2, 'flowsafe.mutationEpoch': 2 };
  const bytes = url.searchParams.get('bytes');
  if (bytes === '16384' || bytes === '16385') {
    body.metadata = { unicode: 'é', pad: '' };
    const metadata = body.metadata as { unicode: string; pad: string };
    metadata.pad = 'x'.repeat(
      Number(bytes) - encoder.encode(JSON.stringify(body)).length,
    );
  }
  const rawBody =
    operation === 'create' || operation === 'update'
      ? JSON.stringify(body)
      : undefined;
  const request = p3Request(
    path,
    method,
    rawBody,
    options.phase === 'auth' &&
      (operation === 'create' || operation === 'update')
      ? gate
      : undefined,
  );
  if (injected === 'header') request.headers.set(MUTATION_EPOCH_HEADER, '2');
  const before = await p3RawSchedules(evidence.database);
  const setupMetrics = measured.reset();
  let intervened = before;
  let fenceBeforeRelease = await adminFence.read();
  armed = true;
  const intervene = async () => {
    if (options.action === 'activate' || options.action === 'advance')
      await p3Activate(adminFence);
    if (options.action === 'cycle') {
      await p3Transition(adminFence, 'draining');
      await p3Transition(adminFence, 'open');
    }
    if (options.action === 'capture') resolver.replace();
    if (options.action === 'resume-race')
      await adminStore.updateSchedule(
        P3_ID,
        { cron: '*/5 * * * *', timezone: 'UTC' },
        { mutationEpoch: 2 },
      );
    fenceBeforeRelease = await adminFence.read();
    intervened = await p3RawSchedules(evidence.database);
  };
  const call = () => router(request);
  const response = await p3Response(
    options.phase === 'none'
      ? await call()
      : await p3Held(gate, call, intervene),
  );
  armed = false;
  const mutationMetrics = measured.reset();
  const after = await p3RawSchedules(evidence.database);
  const reads = options.closed
    ? {
        list: await p3Response(
          await router(p3Request('/api/schedules', 'GET')),
        ),
        get: await p3Response(
          await router(p3Request(`/api/schedules/${P3_ID}`, 'GET')),
        ),
        history: await p3Response(
          await router(p3Request(`/api/schedules/${P3_ID}/triggers`, 'GET')),
        ),
      }
    : undefined;
  const readMetrics = measured.reset();
  let positive: Awaited<ReturnType<typeof p3Response>> | undefined;
  let positiveState: Awaited<ReturnType<typeof p3RawSchedules>> | undefined;
  if (options.action === 'resume-race') {
    positive = await p3Response(
      await router(p3Request(`/api/schedules/${P3_ID}/resume`, 'POST')),
    );
    positiveState = await p3RawSchedules(evidence.database);
  }
  let settled: Awaited<ReturnType<typeof p3RawSchedules>> | undefined;
  if (deferred && response.status === 202) {
    if ((await adminFence.read()).state === 'draining')
      await p3Transition(adminFence, 'open');
    await p3Activate(adminFence);
    await p3Transition(adminFence, 'draining');
    await store.recordTrigger({
      id: 'p3-trigger-000',
      scheduleId: P3_ID,
      runId: 'p3-prior-0',
      scheduledFireAt: NOW,
      actualFireAt: NOW + 1,
      outcome: 'published',
    });
    settled = await p3RawSchedules(evidence.database);
  }
  return {
    operation,
    quiescent: true,
    response,
    gateHits: gate.hits,
    finalBatches,
    before,
    intervened,
    after,
    reads,
    positive,
    positiveState,
    settled,
    audit,
    fenceBeforeRelease,
    fenceAfter: await adminFence.read(),
    bodyBytes: rawBody === undefined ? 0 : encoder.encode(rawBody).length,
    metrics: {
      setup: setupMetrics,
      mutation: mutationMetrics,
      reads: readMetrics,
      settlement: measured.metrics,
      administration: administration.metrics,
      evidence: evidence.metrics,
    },
  };
}

async function p3RawRuns(db: D1Database) {
  const [snapshots, keys, owners] = await Promise.all([
    db
      .prepare(`SELECT * FROM ${P3_SNAPSHOTS} ORDER BY workflow_name, run_id`)
      .all(),
    db.prepare(`SELECT * FROM ${START_IDEMPOTENCY_TABLE} ORDER BY key`).all(),
    db
      .prepare(
        `SELECT * FROM ${RESOURCE_OWNERSHIP_TABLE} ORDER BY resource_kind, resource_id`,
      )
      .all(),
  ]);
  return {
    snapshots: snapshots.results,
    keys: keys.results,
    owners: owners.results,
  };
}

async function p3RawFence(db: D1Database) {
  const [rows, schema] = await Promise.all([
    db.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE} ORDER BY id`).all(),
    db.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`).all(),
  ]);
  return { rows: rows.results, schema: schema.results };
}

async function p3RunProbe(db: D1Database, url: URL) {
  const options = p3Options(url);
  const structural = url.searchParams.has('structure');
  const structure = p3Choice(
    url.searchParams,
    'structure',
    ['none', 'schema-extension', 'null-singleton'],
    'none',
  );
  const proof = url.searchParams.get('proof') === 'true';
  const loseResponse = url.searchParams.get('loss') === 'true';
  const advanceAfterAdmission =
    url.searchParams.get('afterAdmission') === 'advance';
  if (structural && options.phase !== 'final')
    throw new Error(
      'P3 structural fixture requires the final Runtime boundary',
    );
  const gate = p3Gate();
  let armed = false;
  let initialBatches = 0;
  let responseLosses = 0;
  let initialBatchRows: number[] | undefined;
  const isInitialBatch = (statements: P3Statement[]) =>
    statements.some(
      ({ sql }) =>
        sql.includes(`INSERT INTO "${P3_SNAPSHOTS}"`) &&
        sql.includes(EXECUTION_FENCE_TABLE),
    );
  const measured = p3Database(db, {
    beforeBatch: async (statements) => {
      if (!armed || !isInitialBatch(statements)) return;
      initialBatches++;
      if (options.phase === 'final' && gate.hits === 0) await gate.wait();
    },
    afterBatch: structural
      ? async (statements, results) => {
          if (!armed || !isInitialBatch(statements)) return;
          initialBatchRows = results.map((result) => result.results.length);
          if (advanceAfterAdmission) {
            if ((await adminFence.read()).state !== 'open')
              await p3Transition(adminFence, 'open');
            await p3Activate(adminFence);
          }
          if (loseResponse && responseLosses === 0) {
            responseLosses++;
            throw new Error('P3 initial admission response was lost');
          }
        }
      : undefined,
  });
  const administration = p3Database(db);
  const evidence = p3Database(db);
  const fence = new ExecutionFenceStore(measured.database);
  const adminFence = new ExecutionFenceStore(administration.database);
  await fence.seed('open');
  const storage = createD1Storage({
    binding: measured.database,
    tablePrefix: P3_PREFIX,
  });
  await storage.init();
  const reservations = new StartIdempotencyStore(measured.database);
  await reservations.reserve({
    key: 'p3-unrelated-key',
    owner: P3_OWNER,
    targetKind: 'workflow',
    targetId: P3_WORKFLOW,
    mintRunId: () => 'p3-unrelated',
  });
  await new D1ResourceOwnershipStore(measured.database).claim(
    'run',
    'p3-unrelated',
    P3_OWNER,
  );
  let effects = 0;
  const app = init(
    { storage },
    {
      executionFence: fence,
      startIdempotency: reservations,
      requestContextForRun: async () => {
        if (armed && options.phase === 'provider' && gate.hits === 0)
          await gate.wait();
        return {};
      },
    },
  );
  const schema = z.object({ value: z.string() });
  const workflow = app
    .createWorkflow({
      id: P3_WORKFLOW,
      inputSchema: schema,
      outputSchema: schema,
    })
    .then(
      app.createStep({
        id: 'p3-count',
        inputSchema: schema,
        outputSchema: schema,
        execute: async ({ inputData }) => {
          effects++;
          return inputData;
        },
      }),
    )
    .commit();
  await app.runtime.status(P3_WORKFLOW, 'initialize');
  if (options.action !== 'activate') {
    await p3Activate(adminFence);
    await p3Activate(adminFence);
  }
  if (options.closed) await p3Transition(adminFence, 'draining');
  const keyed = url.searchParams.get('keyed') === 'true' || proof;
  if (proof) {
    const reading = await adminFence.read();
    await adminFence.transition({
      expected: reading.state,
      next: 'proof-only',
      proofKey: 'p3-key',
      expectedMutationEpoch: reading.mutationEpoch,
      expectedRevision: reading.transitionRevision,
    });
  }
  const resolver = p3Resolver(measured.database, options.epoch);
  let runtimeOptions: StartRunOptions | undefined;
  let capturedEpoch: number | undefined;
  const router = createRunRouter({
    resolve: resolver.resolve,
    workflows: [
      {
        id: P3_WORKFLOW,
        title: 'P3 counted workflow',
        description: 'Local epoch acceptance',
        sampleInput: { value: 'original' },
      },
    ],
    startIdempotency: {
      store: reservations,
      executionFence: fence,
      live: async (workflowId, runId) =>
        app.runtime.isRunActive(workflowId, runId),
      persistedStart: async (workflowId, runId) => {
        const state = await app.runtime.authoritativeStartState(
          workflowId,
          runId,
        );
        if (!state) return undefined;
        const identity = state.provenance.startIdentity;
        if (!identity)
          throw new Error('P3 stored start lacks its original identity');
        const execution = { ...state.execution, ...identity };
        return state.kind === 'initial'
          ? { kind: 'initial', execution }
          : { kind: 'result', execution, value: state.summary };
      },
    },
    beforeStart: async () => {
      if (options.phase === 'auth') await gate.wait();
    },
    start: (input) => {
      if (input.workflowId !== workflow.id || input.runId !== 'p3-run')
        throw new Error('P3 Runtime start tuple changed');
      capturedEpoch = input.mutationEpoch;
      runtimeOptions = {
        runId: input.runId,
        inputData: input.inputData,
        requestedBy: input.principal.id,
        requestedByKind: input.principal.kind,
        mutationEpoch: input.mutationEpoch,
        startReservation: input.startReservation,
        idempotencyKey: input.idempotencyKey,
      };
      return app.runtime.start(input.workflowId, runtimeOptions);
    },
    status: async (workflowId, runId) =>
      (await app.runtime.status(workflowId, runId)) ?? undefined,
    resume: async () => {
      throw new Error('P3 counted workflow cannot suspend');
    },
  });
  const before = await p3RawRuns(evidence.database);
  const fenceStructureBefore = structural
    ? await p3RawFence(evidence.database)
    : undefined;
  const setupMetrics = measured.reset();
  let fenceBeforeRelease = await adminFence.read();
  let intervened = before;
  let fenceStructureIntervened = fenceStructureBefore;
  armed = true;
  const intervene = async () => {
    if (options.action === 'activate' || options.action === 'advance')
      await p3Activate(adminFence);
    if (options.action === 'cycle') {
      await p3Transition(adminFence, 'draining');
      await p3Transition(adminFence, 'open');
    }
    if (options.action === 'capture') {
      resolver.replace();
      if (runtimeOptions)
        Object.assign(runtimeOptions, {
          mutationEpoch: 3,
          requestedBy: 'replacement',
          inputData: { value: 'replacement' },
        });
    }
    fenceBeforeRelease = await adminFence.read();
    if (structure === 'schema-extension') {
      await administration.database
        .prepare(
          `ALTER TABLE ${EXECUTION_FENCE_TABLE} ADD COLUMN p3_shape TEXT`,
        )
        .run();
    }
    if (structure === 'null-singleton') {
      await administration.database
        .prepare(
          `INSERT INTO ${EXECUTION_FENCE_TABLE} (id, state, updated_at) VALUES (NULL, 'open', 0)`,
        )
        .run();
    }
    if (structural)
      fenceStructureIntervened = await p3RawFence(evidence.database);
    intervened = await p3RawRuns(evidence.database);
  };
  const body = {
    workflowId: P3_WORKFLOW,
    inputData: { value: 'original' },
    ...(keyed ? { idempotencyKey: 'p3-key' } : {}),
  };
  const call = () => router(p3Request('/runs', 'POST', JSON.stringify(body)));
  const response = await p3Response(
    options.phase === 'none'
      ? await call()
      : await p3Held(gate, call, intervene),
  );
  armed = false;
  const mutationMetrics = measured.reset();
  const after = await p3RawRuns(evidence.database);
  const effectsAfterRequest = effects;
  const cachedAfterRequest = workflow.runs.has('p3-run');
  const activeAfterRequest = app.runtime.isRunActive(P3_WORKFLOW, 'p3-run');
  let positive: unknown;
  if (response.status === 409 && !keyed && !structural) {
    const current = await adminFence.read();
    positive = await app.runtime.start(P3_WORKFLOW, {
      runId: 'p3-run',
      inputData: { value: 'positive' },
      requestedBy: P3_OWNER.id,
      requestedByKind: 'human',
      mutationEpoch: current.mutationEpoch,
    });
  }
  const positiveRows = await p3RawRuns(evidence.database);
  const fenceStructureAfter = structural
    ? await p3RawFence(evidence.database)
    : undefined;
  const finalFence = structure === 'none' ? await adminFence.read() : undefined;
  const checkedRun = {
    workflowId: workflow.id,
    runId: runtimeOptions?.runId ?? 'p3-run',
  };
  if (checkedRun.workflowId !== P3_WORKFLOW || checkedRun.runId !== 'p3-run')
    throw new Error('P3 Runtime quiescence tuple changed');
  if (app.runtime.isRunActive(checkedRun.workflowId, checkedRun.runId))
    throw new Error('P3 Runtime remains active after its response');
  return {
    quiescent: true,
    checkedRun,
    response,
    capturedEpoch: capturedEpoch ?? null,
    gateHits: gate.hits,
    initialBatches,
    ...(structural
      ? {
          structural: {
            fixture: structure,
            proof,
            responseLosses,
            initialBatchRows,
            before: fenceStructureBefore,
            intervened: fenceStructureIntervened,
            after: fenceStructureAfter,
          },
        }
      : {}),
    before,
    intervened,
    after,
    effectsAfterRequest,
    effects,
    cachedAfterRequest,
    activeAfterRequest,
    positive,
    positiveRows,
    fenceBeforeRelease,
    fenceAfter: finalFence,
    metrics: {
      setup: setupMetrics,
      mutation: mutationMetrics,
      positive: measured.metrics,
      administration: administration.metrics,
      evidence: evidence.metrics,
    },
  };
}

async function p3CasLossProbe(db: D1Database) {
  let armed = false;
  let losses = 0;
  const measured = p3Database(db, {
    afterStatement: (sql) => {
      if (
        armed &&
        losses === 0 &&
        sql.trimStart().startsWith(`UPDATE ${EXECUTION_FENCE_TABLE}\n`)
      ) {
        losses++;
        throw new Error('P3 lost committed CAS response');
      }
    },
  });
  const fence = new ExecutionFenceStore(measured.database);
  const independent = new ExecutionFenceStore(db);
  await fence.seed('open');
  const before = await fence.read();
  const request = {
    expected: 'open' as const,
    next: 'draining' as const,
    expectedMutationEpoch: before.mutationEpoch,
    expectedRevision: before.transitionRevision,
    advanceMutationEpoch: true,
  };
  armed = true;
  let firstError: string | undefined;
  try {
    await fence.transition(request);
  } catch (error) {
    firstError = String(error);
  }
  const afterCommit = await independent.read();
  const rawAfterCommit = (
    await db.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`).all()
  ).results;
  const retry = await fence.transition(request);
  const rawAfterRetry = (
    await db.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`).all()
  ).results;
  let conflict: unknown;
  try {
    await fence.transition({ ...request, advanceMutationEpoch: false });
  } catch (error) {
    conflict = (error as { reason?: unknown }).reason;
  }
  return {
    quiescent: true,
    losses,
    firstError,
    before,
    afterCommit,
    retry,
    rawAfterCommit,
    rawAfterRetry,
    conflict,
    metrics: measured.metrics,
  };
}

const handler = {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const path = new URL(request.url).pathname;
      if (request.method !== 'POST') {
        return new Response('not found', { status: 404 });
      }
      if (path === '/seed') {
        await seedDeploymentIdentity(env.DB, 'spike', 'open');
        return Response.json({ ok: true });
      }
      if (path === '/notification-chronology/future')
        return Response.json(await notificationChronologyFutureProbe(env.DB));
      if (path === '/notification-chronology/offsets')
        return Response.json(await notificationChronologyOffsetsProbe(env.DB));
      if (path === '/notification-chronology/bounds')
        return Response.json(await notificationChronologyBoundsProbe(env.DB));
      if (path === '/notification-chronology/list')
        return Response.json(await notificationChronologyListProbe(env.DB));
      if (path === '/notification-chronology/ttl')
        return Response.json(await notificationChronologyTtlProbe(env.DB));
      if (path === '/p3-cleanup') return Response.json(await p3Cleanup(env.DB));
      if (path === '/epoch-p3/isolation-missing-witness')
        return Response.json({ metrics: { diagnostic: p3Metrics() } });
      if (path === '/epoch-p3/schedule')
        return Response.json(
          await p3ScheduleProbe(env.DB, new URL(request.url)),
        );
      if (path === '/epoch-p3/run')
        return Response.json(await p3RunProbe(env.DB, new URL(request.url)));
      if (path === '/epoch-p3/cas-loss')
        return Response.json(await p3CasLossProbe(env.DB));
      if (path.startsWith('/retention-e/')) {
        const [, , scenario, action] = path.split('/');
        return Response.json(
          await eRetentionProbe(env.DB, scenario ?? '', action),
        );
      }
      const result =
        path === '/approval'
          ? await approvalProbe(env.DB)
          : path === '/schedule'
            ? await scheduleProbe(env.DB)
            : path === '/notification'
              ? await notificationProbe(env.DB)
              : path === '/background'
                ? await backgroundProbe(env.DB)
                : path === '/retention'
                  ? await retentionProbe(env.DB)
                  : undefined;
      return result === undefined
        ? new Response('not found', { status: 404 })
        : Response.json(result);
    } catch (error) {
      return Response.json(
        {
          error: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        },
        { status: 500 },
      );
    }
  },
};

export default handler;
