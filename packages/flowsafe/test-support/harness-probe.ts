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
  type RunRetentionCursor,
} from '../src/do-runner/d1-storage.js';
import { seedDeploymentIdentity } from '../src/do-runner/deployment-identity.js';
import { ExecutionFenceStore } from '../src/do-runner/execution-fence.js';
import { FENCED_WORKFLOW_STORAGE } from '../src/do-runner/fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from '../src/do-runner/fenced-workflows-d1.js';
import { isDefinitiveInitialAdmissionRefusal } from '../src/do-runner/initial-admission-refusal.js';
import {
  parseRunLifecycle,
  projectTerminalLifecycle,
} from '../src/do-runner/run-lifecycle.js';
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
