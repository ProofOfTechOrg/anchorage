// SPDX-License-Identifier: Apache-2.0
import { createEmptyWorkflowSnapshot } from '@mastra/core/storage';

import { D1ApprovalStore } from '../src/approval-api/d1-store.js';
import {
  D1ResourceOwnershipStore,
  RESOURCE_OWNERSHIP_TABLE,
} from '../src/approval-api/resource-ownership.js';
import type { ApprovalRecord } from '../src/approval-api/types.js';
import { createBackgroundTaskD1Domains } from '../src/background-tasks/d1-storage.js';
import {
  createD1Storage,
  purgeExpiredThreads,
  purgeExpiredWorkflowRuns,
} from '../src/do-runner/d1-storage.js';
import { seedDeploymentIdentity } from '../src/do-runner/deployment-identity.js';
import { D1SchedulesStorage } from '../src/schedules/schedules-d1.js';
import { scheduleWithCreatorRole } from '../src/schedules/target-policy.js';
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
  let rollbackError = '';
  try {
    await store.deleteOwnedSchedule(rollback.id);
  } catch (error) {
    rollbackError = String(error);
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
  let ownerInsertError = '';
  try {
    await store.createOwnedSchedule(ownerFailure, owner, 10);
  } catch (error) {
    ownerInsertError = String(error);
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

async function createSnapshotTable(db: D1Database): Promise<void> {
  await db
    .prepare(
      `CREATE TABLE mastra_workflow_snapshot (
         workflow_name TEXT NOT NULL,
         run_id TEXT NOT NULL,
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
