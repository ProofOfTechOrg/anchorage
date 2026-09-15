// SPDX-License-Identifier: Apache-2.0
// D1NotificationsStorage round-trip / coalescing / listDue / update — mirrors the
// core InMemoryNotificationsStorage behavior over a node:sqlite SQL unit facade.

import type {
  CreateNotificationInput,
  NotificationRecord,
} from '@mastra/core/notifications';
import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  notificationTimestampMillis,
  notificationTimestampSql,
} from '../do-runner/notification-predicate.js';
import type { SignalDatabase, SignalStatement } from './d1-shared.js';
import {
  captureNotificationDeliveryObservation,
  type NotificationDeliveryFailure,
  type NotificationDeliveryObservation,
} from './notification-dispatch.js';
import { D1NotificationsStorage } from './notifications-d1.js';

function store(): D1NotificationsStorage {
  const db = sqliteUnitDatabase(openSqlite()) as unknown as SignalDatabase;
  return new D1NotificationsStorage(db, '');
}

function database(): SignalDatabase {
  return sqliteUnitDatabase(openSqlite()) as unknown as SignalDatabase;
}

function sharedStores(): [D1NotificationsStorage, D1NotificationsStorage] {
  const db = database();
  return [
    new D1NotificationsStorage(db, ''),
    new D1NotificationsStorage(db, ''),
  ];
}

function interceptFirst(
  db: SignalDatabase,
  intercept: (query: string, read: () => Promise<unknown>) => Promise<unknown>,
): SignalDatabase {
  function wrap(query: string, statement: SignalStatement): SignalStatement {
    return {
      bind(...values: unknown[]) {
        return wrap(query, statement.bind(...values));
      },
      async first<T = unknown>(): Promise<T | null> {
        return (await intercept(query, () => statement.first<T>())) as T | null;
      },
      all<T = unknown>() {
        return statement.all<T>();
      },
      run() {
        return statement.run();
      },
    };
  }
  return {
    prepare(query) {
      return wrap(query, db.prepare(query));
    },
    ...(db.batch ? { batch: db.batch.bind(db) } : {}),
  };
}

function coalescableReadBarrier(db: SignalDatabase): {
  db: SignalDatabase;
  selected: Promise<void>;
  release: () => void;
} {
  let markSelected: () => void = () => undefined;
  const selected = new Promise<void>((resolve) => {
    markSelected = resolve;
  });
  let releaseRead: () => void = () => undefined;
  const released = new Promise<void>((resolve) => {
    releaseRead = resolve;
  });
  let intercepted = false;

  return {
    db: interceptFirst(db, async (query, read) => {
      const row = await read();
      if (
        !intercepted &&
        row !== null &&
        query.includes('insertionOrdinal IS NULL ASC')
      ) {
        intercepted = true;
        markSelected();
        await released;
      }
      return row;
    }),
    selected,
    release: releaseRead,
  };
}

const ATTEMPT_TIME = '2026-09-09T12:00:00.000Z';
const WRITE_TIME = '2026-09-09T13:00:00.000Z';
const RETRY_TIME = '2026-09-09T12:00:01.000Z';

function createDueNotification(
  storage: D1NotificationsStorage,
  input: Partial<CreateNotificationInput> = {},
): Promise<NotificationRecord> {
  return storage.createNotification({
    id: 'receipt',
    threadId: 'thread',
    source: 'source',
    kind: 'kind',
    summary: 'summary',
    resourceId: 'resource',
    agentId: 'agent',
    createdAt: new Date('2026-09-09T10:00:00.000Z'),
    deliverAt: new Date('2026-09-09T11:59:59.000Z'),
    summaryAt: new Date('2026-09-09T14:00:00.000Z'),
    ...input,
  });
}

function retryFailure(): Extract<
  NotificationDeliveryFailure,
  { type: 'retry' }
> {
  return {
    type: 'retry',
    updatedAt: WRITE_TIME,
    deliveryAttempts: 1,
    lastDeliveryAttemptAt: ATTEMPT_TIME,
    lastDeliveryError: 'target refused',
    deliverAt: RETRY_TIME,
  };
}

function isDeliveryUpdate(query: string): boolean {
  return (
    query.trimStart().startsWith('UPDATE ') && query.includes('RETURNING *')
  );
}

function deliveryWriteBarrier(db: SignalDatabase): {
  db: SignalDatabase;
  writing: Promise<void>;
  release: () => void;
} {
  let markWriting: () => void = () => undefined;
  let release: () => void = () => undefined;
  const writing = new Promise<void>((resolve) => {
    markWriting = resolve;
  });
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  let intercepted = false;
  return {
    db: interceptFirst(db, async (query, read) => {
      if (!intercepted && isDeliveryUpdate(query)) {
        intercepted = true;
        markWriting();
        await released;
      }
      return read();
    }),
    writing,
    release,
  };
}

async function rawNotification(
  db: SignalDatabase,
): Promise<Record<string, unknown>> {
  const row = await db
    .prepare('SELECT * FROM mastra_notifications')
    .first<Record<string, unknown>>();
  if (!row) throw new Error('notification fixture is missing');
  return row;
}

async function createLegacyNotificationsTable(
  db: SignalDatabase,
  textCollation: 'BINARY' | 'NOCASE' = 'BINARY',
): Promise<void> {
  await db
    .prepare(
      `CREATE TABLE mastra_notifications (
         id TEXT NOT NULL COLLATE ${textCollation},
         thread_id TEXT NOT NULL COLLATE ${textCollation},
         source TEXT NOT NULL,
         kind TEXT NOT NULL,
         priority TEXT NOT NULL,
         status TEXT NOT NULL,
         summary TEXT NOT NULL COLLATE ${textCollation},
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

describe('D1NotificationsStorage', () => {
  it('creates and round-trips a notification with defaults', async () => {
    const s = store();
    const created = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'pr.opened',
      summary: 'PR #1 opened',
      payload: { number: 1 },
    });
    expect(created.status).toBe('pending');
    expect(created.priority).toBe('medium');
    expect(created.coalescedCount).toBe(1);
    const got = await s.getNotification({
      threadId: 'acme_t1',
      id: created.id,
    });
    expect(got?.summary).toBe('PR #1 opened');
    expect(got?.payload).toEqual({ number: 1 });
    expect(got?.createdAt).toBeInstanceOf(Date);
  });

  it('coalesces onto a pending record sharing a coalesceKey (bumps the count)', async () => {
    const s = store();
    const first = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI running',
      coalesceKey: 'ci-run-9',
    });
    const second = await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI passed',
      coalesceKey: 'ci-run-9',
    });
    // Same record id, summary refreshed, count bumped.
    expect(second.id).toBe(first.id);
    expect(second.summary).toBe('CI passed');
    expect(second.coalescedCount).toBe(2);
    const list = await s.listNotifications({ threadId: 'acme_t1' });
    expect(list).toHaveLength(1);
  });

  it('coalesces keyed explicit IDs before considering the supplied ID', async () => {
    const s = store();
    await s.createNotification({
      id: 'stable',
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'first',
      coalesceKey: 'ci-run-9',
    });

    const ignored = await s.createNotification({
      id: 'ignored',
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'second',
      coalesceKey: 'ci-run-9',
    });
    const repeated = await s.createNotification({
      id: 'stable',
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'third',
      coalesceKey: 'ci-run-9',
    });

    expect(ignored.id).toBe('stable');
    expect(repeated).toMatchObject({
      id: 'stable',
      summary: 'third',
      coalescedCount: 3,
    });
    await expect(
      s.getNotification({ threadId: 'acme_t1', id: 'ignored' }),
    ).resolves.toBeNull();
    await expect(
      s.listNotifications({ threadId: 'acme_t1' }),
    ).resolves.toHaveLength(1);
  });

  it('atomically coalesces simultaneous keyed creates with explicit IDs', async () => {
    const [left, right] = sharedStores();
    await Promise.all([left.init(), right.init()]);
    const input = {
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI update',
      coalesceKey: 'ci-run-9',
    };

    const created = await Promise.all([
      left.createNotification({ ...input, id: 'left' }),
      right.createNotification({ ...input, id: 'right' }),
    ]);

    expect(new Set(created.map((record) => record.id))).toHaveLength(1);
    const listed = await left.listNotifications({ threadId: 'acme_t1' });
    expect(listed).toHaveLength(1);
    expect(listed[0]?.coalescedCount).toBe(2);
  });

  it('atomically coalesces two simultaneous first creates from separate adapters', async () => {
    const [left, right] = sharedStores();
    await Promise.all([left.init(), right.init()]);
    const input = {
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'CI update',
      coalesceKey: 'ci-run-9',
    };

    const created = await Promise.all([
      left.createNotification({ ...input, attributes: { left: true } }),
      right.createNotification({ ...input, attributes: { right: true } }),
    ]);

    expect(new Set(created.map((record) => record.id))).toHaveLength(1);
    const listed = await left.listNotifications({ threadId: 'acme_t1' });
    expect(listed).toHaveLength(1);
    expect(listed[0]?.coalescedCount).toBe(2);
    expect(listed[0]?.attributes).toEqual({ left: true, right: true });
  });

  it('preserves concurrent map merges while incrementing coalescedCount in SQL', async () => {
    const [left, right] = sharedStores();
    await Promise.all([left.init(), right.init()]);
    await left.createNotification({
      id: 'base',
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'base',
      coalesceKey: 'ci-run-9',
      attributes: { base: true },
      metadata: { base: true },
    });

    await Promise.all([
      left.createNotification({
        threadId: 'acme_t1',
        source: 'github',
        kind: 'ci',
        summary: 'left',
        coalesceKey: 'ci-run-9',
        attributes: { left: true },
        metadata: { left: true },
      }),
      right.createNotification({
        threadId: 'acme_t1',
        source: 'github',
        kind: 'ci',
        summary: 'right',
        coalesceKey: 'ci-run-9',
        attributes: { right: true },
        metadata: { right: true },
      }),
    ]);

    const record = await left.getNotification({
      threadId: 'acme_t1',
      id: 'base',
    });
    expect(record?.coalescedCount).toBe(3);
    expect(record?.attributes).toEqual({
      base: true,
      left: true,
      right: true,
    });
    expect(record?.metadata).toEqual({
      base: true,
      left: true,
      right: true,
    });
  });

  it('does NOT coalesce across a different kind, source, or resource', async () => {
    const s = store();
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'ci',
      summary: 'a',
      coalesceKey: 'k',
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'github',
      kind: 'deploy', // different kind
      summary: 'b',
      coalesceKey: 'k',
    });
    expect(await s.listNotifications({ threadId: 'acme_t1' })).toHaveLength(2);
  });

  it('listDueNotifications returns only pending rows past their deliverAt', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'due',
      deliverAt: new Date(now.getTime() - 1000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'future',
      deliverAt: new Date(now.getTime() + 60_000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'c',
      summary: 'no deliverAt (never due via listDue)',
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['due']);
  });

  it.each(
    (
      ['delivered', 'seen', 'dismissed', 'archived', 'discarded'] as const
    ).flatMap((status) =>
      (['deliverAt', 'summaryAt'] as const).map((cursor) => ({
        status,
        cursor,
      })),
    ),
  )('excludes $status with a retained $cursor from a limited due window', async ({
    status,
    cursor,
  }) => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    const dueAt = new Date(now.getTime() - 1000);
    const terminal = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'terminal',
      [cursor]: dueAt,
    });
    await s.updateNotification({
      threadId: terminal.threadId,
      id: terminal.id,
      status,
    });
    expect(await s.getNotification(terminal)).toMatchObject({
      status,
      [cursor]: dueAt,
    });
    const pending = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'pending',
      [cursor]: now,
    });
    expect(await s.listDueNotifications({ now, limit: 1 })).toEqual([pending]);
  });

  it('updateNotification stamps the status timestamp and filters by status', async () => {
    const s = store();
    const created = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'hi',
    });
    const updated = await s.updateNotification({
      threadId: 'acme_t1',
      id: created.id,
      status: 'delivered',
    });
    expect(updated.status).toBe('delivered');
    expect(updated.deliveredAt).toBeInstanceOf(Date);
    const pending = await s.listNotifications({
      threadId: 'acme_t1',
      status: 'pending',
    });
    expect(pending).toHaveLength(0);
    const delivered = await s.listNotifications({
      threadId: 'acme_t1',
      status: ['delivered', 'seen'],
    });
    expect(delivered).toHaveLength(1);
  });

  it('composes disjoint concurrent updates without replacing the whole row', async () => {
    const [left, right] = sharedStores();
    await Promise.all([left.init(), right.init()]);
    await left.createNotification({
      id: 'n1',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'hi',
    });

    await Promise.all([
      left.updateNotification({
        threadId: 'acme_t1',
        id: 'n1',
        deliveryAttempts: 4,
      }),
      right.updateNotification({
        threadId: 'acme_t1',
        id: 'n1',
        lastDeliveryError: 'temporary',
      }),
    ]);

    expect(
      await left.getNotification({ threadId: 'acme_t1', id: 'n1' }),
    ).toMatchObject({
      deliveryAttempts: 4,
      lastDeliveryError: 'temporary',
    });
  });

  it('treats empty status and priority arrays as filters matching nothing', async () => {
    const s = store();
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'hi',
    });

    await expect(
      s.listNotifications({ threadId: 'acme_t1', status: [] }),
    ).resolves.toEqual([]);
    await expect(
      s.listNotifications({ threadId: 'acme_t1', priority: [] }),
    ).resolves.toEqual([]);
  });

  it('updateNotification throws for a missing record', async () => {
    const s = store();
    await expect(
      s.updateNotification({ threadId: 'acme_t1', id: 'nope', status: 'seen' }),
    ).rejects.toThrow(/not found/i);
  });

  it('scopes list/get to the threadId (a foreign thread never bleeds in)', async () => {
    const s = store();
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'acme',
    });
    await s.createNotification({
      threadId: 'other_t1',
      source: 'x',
      kind: 'a',
      summary: 'other',
    });
    const acme = await s.listNotifications({ threadId: 'acme_t1' });
    expect(acme).toHaveLength(1);
    expect(acme[0]?.summary).toBe('acme');
  });

  it('coalesces onto the first-inserted candidate regardless of createdAt', async () => {
    // #given — two DISTINCT pending records of one (thread, source, kind): one
    // matchable by dedupeKey, one by coalesceKey. The earlier-created record is
    // inserted SECOND, so Mastra Map insertion order and createdAt disagree.
    const s = store();
    const early = new Date('2026-01-01T00:00:00.000Z');
    const late = new Date('2026-01-01T00:05:00.000Z');
    const lateRec = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'late',
      dedupeKey: 'd',
      createdAt: late,
    });
    const earlyRec = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'early',
      coalesceKey: 'c',
      createdAt: early,
    });
    expect(earlyRec.id).not.toBe(lateRec.id);

    // #when — a create matching BOTH keys
    const merged = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'merged',
      dedupeKey: 'd',
      coalesceKey: 'c',
    });

    // #then — it follows Mastra's Map insertion order, not caller-controlled
    // createdAt ordering.
    expect(merged.id).toBe(lateRec.id);
    expect(merged.coalescedCount).toBe(2);
    expect(await s.listNotifications({ threadId: 'acme_t1' })).toHaveLength(2);
  });

  it('preserves insertion order when an explicit ID replaces the same key', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db, '');
    await s.createNotification({
      id: 'stable',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'first',
    });
    const before = await db
      .prepare(
        `SELECT insertionOrdinal
         FROM mastra_notifications
         WHERE thread_id = ? AND id = ?`,
      )
      .bind('acme_t1', 'stable')
      .first<{ insertionOrdinal: number }>();

    await s.createNotification({
      id: 'stable',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'replacement',
    });
    const after = await db
      .prepare(
        `SELECT insertionOrdinal
         FROM mastra_notifications
         WHERE thread_id = ? AND id = ?`,
      )
      .bind('acme_t1', 'stable')
      .first<{ insertionOrdinal: number }>();

    expect(before?.insertionOrdinal).toBe(1);
    expect(after?.insertionOrdinal).toBe(before?.insertionOrdinal);
  });

  it('orders post-init rollback writes and preserves their same-ID position', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db, '');
    await s.init();
    const rollbackWrite = async (
      id: string,
      summary: string,
      dedupeKey: string | null,
      coalesceKey: string | null,
      createdAt: string,
    ): Promise<void> => {
      await db
        .prepare(
          `INSERT OR REPLACE INTO mastra_notifications (
             id, thread_id, source, kind, priority, status, summary,
             dedupeKey, coalesceKey, createdAt, updatedAt
           ) VALUES (?, 'acme_t1', 'x', 'k', 'medium', 'pending', ?, ?, ?, ?, ?)`,
        )
        .bind(id, summary, dedupeKey, coalesceKey, createdAt, createdAt)
        .run();
    };

    await rollbackWrite(
      'rollback-first',
      'rollback first',
      'd',
      null,
      '2026-01-01T00:05:00.000Z',
    );
    await s.createNotification({
      id: 'new-second',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'new second',
      coalesceKey: 'c',
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
    });
    await rollbackWrite(
      'rollback-first',
      'rollback replacement',
      'd',
      null,
      '2026-01-01T00:10:00.000Z',
    );

    const ordered = await db
      .prepare(
        `SELECT id, insertionOrdinal
         FROM mastra_notifications
         ORDER BY insertionOrdinal`,
      )
      .all<{ id: string; insertionOrdinal: number }>();
    expect(ordered.results).toEqual([
      { id: 'rollback-first', insertionOrdinal: 1 },
      { id: 'new-second', insertionOrdinal: 2 },
    ]);
    const merged = await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'merged',
      dedupeKey: 'd',
      coalesceKey: 'c',
    });
    expect(merged).toMatchObject({
      id: 'rollback-first',
      summary: 'merged',
      coalescedCount: 2,
    });
  });

  it('migrates legacy rows concurrently in physical insertion order', async () => {
    const db = database();
    await createLegacyNotificationsTable(db);
    const insert = async (
      id: string,
      summary: string,
      dedupeKey: string | null,
      coalesceKey: string | null,
      createdAt: string,
    ): Promise<void> => {
      await db
        .prepare(
          `INSERT INTO mastra_notifications (
             id, thread_id, source, kind, priority, status, summary,
             dedupeKey, coalesceKey, coalescedCount, createdAt, updatedAt,
             deliveryAttempts
           ) VALUES (?, 'acme_t1', 'x', 'k', 'medium', 'pending', ?, ?, ?, 1, ?, ?, 0)`,
        )
        .bind(id, summary, dedupeKey, coalesceKey, createdAt, createdAt)
        .run();
    };
    await insert(
      'physical-first',
      'late',
      'd',
      null,
      '2026-01-01T00:05:00.000Z',
    );
    await insert(
      'physical-second',
      'early',
      null,
      'c',
      '2026-01-01T00:00:00.000Z',
    );
    const left = new D1NotificationsStorage(db, '');
    const right = new D1NotificationsStorage(db, '');

    await Promise.all([left.init(), right.init()]);

    const columns = await db
      .prepare('PRAGMA table_info(mastra_notifications)')
      .all<{ name: string }>();
    expect(
      columns.results.filter((column) => column.name === 'insertionOrdinal'),
    ).toHaveLength(1);
    const migrated = await db
      .prepare(
        `SELECT id, insertionOrdinal
         FROM mastra_notifications
         ORDER BY rowid`,
      )
      .all<{ id: string; insertionOrdinal: number }>();
    expect(migrated.results).toEqual([
      { id: 'physical-first', insertionOrdinal: 1 },
      { id: 'physical-second', insertionOrdinal: 2 },
    ]);

    await left.createNotification({
      id: 'later',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'other',
      summary: 'later',
    });
    const later = await db
      .prepare(
        `SELECT insertionOrdinal
         FROM mastra_notifications
         WHERE thread_id = 'acme_t1' AND id = 'later'`,
      )
      .first<{ insertionOrdinal: number }>();
    expect(later?.insertionOrdinal).toBe(3);

    const merged = await right.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'merged',
      dedupeKey: 'd',
      coalesceKey: 'c',
    });
    expect(merged.id).toBe('physical-first');

    await expect(
      db
        .prepare(
          `UPDATE mastra_notifications
           SET insertionOrdinal = 1
           WHERE thread_id = 'acme_t1' AND id = 'later'`,
        )
        .run(),
    ).rejects.toThrow(/unique/i);
  });

  it('fails closed when atomic schema batches are unavailable', async () => {
    const base = database();
    const prepareOnly: SignalDatabase = {
      prepare(query) {
        return base.prepare(query);
      },
    };

    await expect(
      new D1NotificationsStorage(prepareOnly, '').init(),
    ).rejects.toThrow(/requires database\.batch/i);
  });

  it('does not overwrite concurrent partial updates with stale coalescing fallbacks', async () => {
    const db = database();
    const barrier = coalescableReadBarrier(db);
    const left = new D1NotificationsStorage(barrier.db, '');
    const right = new D1NotificationsStorage(db, '');
    await left.init();
    await right.init();
    const oldDeliverAt = new Date('2026-01-01T00:00:00.000Z');
    const oldSummaryAt = new Date('2026-01-01T00:01:00.000Z');
    await right.createNotification({
      id: 'base',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'base',
      coalesceKey: 'c',
      priority: 'low',
      payload: { version: 'old' },
      attributes: { version: 'old' },
      metadata: { version: 'old' },
      deliverAt: oldDeliverAt,
      summaryAt: oldSummaryAt,
      deliveryReason: 'old',
    });

    const pending = left.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'coalesced',
      coalesceKey: 'c',
    });
    await barrier.selected;
    const newDeliverAt = new Date('2026-01-02T00:00:00.000Z');
    const newSummaryAt = new Date('2026-01-02T00:01:00.000Z');
    await right.updateNotification({
      threadId: 'acme_t1',
      id: 'base',
      payload: { version: 'new' },
      attributes: { version: 'new' },
      metadata: { version: 'new' },
      deliverAt: newDeliverAt,
      summaryAt: newSummaryAt,
      deliveryReason: 'new',
    });
    await db
      .prepare(
        `UPDATE mastra_notifications
         SET priority = 'urgent'
         WHERE thread_id = 'acme_t1' AND id = 'base'`,
      )
      .run();
    barrier.release();

    await expect(pending).resolves.toMatchObject({
      id: 'base',
      summary: 'coalesced',
      priority: 'urgent',
      payload: { version: 'new' },
      attributes: { version: 'new' },
      metadata: { version: 'new' },
      deliverAt: newDeliverAt,
      summaryAt: newSummaryAt,
      deliveryReason: 'new',
      coalescedCount: 2,
    });
  });

  it('retries instead of mutating a same-ID replacement with stale identity', async () => {
    const db = database();
    const barrier = coalescableReadBarrier(db);
    const left = new D1NotificationsStorage(barrier.db, '');
    const right = new D1NotificationsStorage(db, '');
    await left.init();
    await right.init();
    await right.createNotification({
      id: 'base',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'base',
      coalesceKey: 'c',
    });

    const pending = left.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'k',
      summary: 'coalesced',
      coalesceKey: 'c',
    });
    await barrier.selected;
    await right.createNotification({
      id: 'base',
      threadId: 'acme_t1',
      source: 'replacement',
      kind: 'other',
      summary: 'replacement',
    });
    barrier.release();

    const created = await pending;
    expect(created.id).not.toBe('base');
    await expect(
      left.getNotification({ threadId: 'acme_t1', id: 'base' }),
    ).resolves.toMatchObject({
      source: 'replacement',
      kind: 'other',
      summary: 'replacement',
      coalescedCount: 1,
    });
    await expect(
      left.getNotification({ threadId: 'acme_t1', id: created.id }),
    ).resolves.toMatchObject({
      source: 'x',
      kind: 'k',
      summary: 'coalesced',
      coalescedCount: 1,
    });
  });

  it('listDueNotifications treats summaryAt (not just deliverAt) as due', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'summary-due',
      summaryAt: new Date(now.getTime() - 1000),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['summary-due']);
  });

  it('listDueNotifications is inclusive at the boundary (deliverAt === now is due)', async () => {
    const s = store();
    const now = new Date('2026-01-01T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'exact',
      summary: 'exact',
      deliverAt: now,
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'future',
      summary: 'future',
      deliverAt: new Date(now.getTime() + 1),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['exact']);
  });

  it('listDueNotifications returns a BATCH ordered by earliest due time, honoring limit', async () => {
    const s = store();
    const now = new Date('2026-01-02T00:00:00.000Z');
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'second',
      deliverAt: new Date(now.getTime() - 1000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'first',
      deliverAt: new Date(now.getTime() - 5000),
    });
    await s.createNotification({
      threadId: 'acme_t1',
      source: 'x',
      kind: 'c',
      summary: 'third',
      summaryAt: new Date(now.getTime() - 100),
    });
    const due = await s.listDueNotifications({ now });
    expect(due.map((r) => r.summary)).toEqual(['first', 'second', 'third']);
    const limited = await s.listDueNotifications({ now, limit: 2 });
    expect(limited.map((r) => r.summary)).toEqual(['first', 'second']);
  });

  it('orders by the earliest of deliverAt and summaryAt before applying the SQL limit', async () => {
    const s = store();
    const now = new Date('2026-01-02T00:00:00.000Z');
    await s.createNotification({
      id: 'both',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'a',
      summary: 'both',
      deliverAt: new Date(now.getTime() - 1_000),
      summaryAt: new Date(now.getTime() - 9_000),
    });
    await s.createNotification({
      id: 'delivery',
      threadId: 'acme_t1',
      source: 'x',
      kind: 'b',
      summary: 'delivery',
      deliverAt: new Date(now.getTime() - 5_000),
    });

    const due = await s.listDueNotifications({ now, limit: 1 });
    expect(due.map((record) => record.id)).toEqual(['both']);
  });
});

describe('D1 notification conditional delivery bookkeeping', () => {
  it('advances the due cursor and preserves future cursor bytes and other fields', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s, {
      payload: { value: 1 },
      metadata: { origin: 'source' },
    });
    await db
      .prepare(
        `UPDATE mastra_notifications
         SET summaryAt = '2026-09-09T18:00:00+04:00',
             payload = '{ "value" : 1.0 }'`,
      )
      .run();
    const before = await rawNotification(db);
    const result = await s.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    expect(result).toMatchObject({
      applied: true,
      record: {
        status: 'pending',
        deliveryAttempts: 1,
        lastDeliveryError: 'target refused',
        lastDeliveryAttemptAt: new Date(ATTEMPT_TIME),
        updatedAt: new Date(WRITE_TIME),
      },
    });
    expect(await rawNotification(db)).toEqual({
      ...before,
      updatedAt: WRITE_TIME,
      deliveryAttempts: 1,
      lastDeliveryAttemptAt: ATTEMPT_TIME,
      lastDeliveryError: 'target refused',
      deliverAt: RETRY_TIME,
    });
  });

  it('moves both due cursors without clearing a completed summary receipt', async () => {
    const s = store();
    await createDueNotification(s, { summaryAt: new Date(ATTEMPT_TIME) });
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      summarySignalId: 'prior-summary',
    });
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: { ...retryFailure(), summaryAt: RETRY_TIME },
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: {
        summaryAt: new Date(RETRY_TIME),
        deliverAt: new Date(RETRY_TIME),
        summarySignalId: 'prior-summary',
      },
    });
  });

  it('persists the final failed round and keeps its terminal receipt visible', async () => {
    const s = store();
    await createDueNotification(s);
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      deliveryAttempts: 9,
      summarySignalId: 'prior-summary',
    });
    const failure: NotificationDeliveryFailure = {
      type: 'discard',
      updatedAt: WRITE_TIME,
      deliveryAttempts: 10,
      lastDeliveryAttemptAt: ATTEMPT_TIME,
      lastDeliveryError: 'last refusal',
    };
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure,
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: {
        status: 'discarded',
        deliveryReason: 'delivery-attempts-exhausted',
        deliveryAttempts: 10,
        lastDeliveryError: 'last refusal',
        lastDeliveryAttemptAt: new Date(ATTEMPT_TIME),
        discardedAt: new Date(WRITE_TIME),
        updatedAt: new Date(WRITE_TIME),
        summarySignalId: 'prior-summary',
        deliverAt: undefined,
        summaryAt: undefined,
      },
    });
    expect(await s.listDueNotifications({ now: new Date(WRITE_TIME) })).toEqual(
      [],
    );
    const listed = await s.listNotifications({ threadId: 'thread' });
    expect(listed).toHaveLength(1);
    expect(listed[0]).toEqual(
      await s.getNotification({ threadId: 'thread', id: 'receipt' }),
    );
  });

  it.each([
    10,
    Number.MAX_SAFE_INTEGER,
  ])('terminalizes an existing count of %s without replacing the failed receipt', async (deliveryAttempts) => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    await createDueNotification(s);
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      deliveryAttempts,
      lastDeliveryAttemptAt: new Date(ATTEMPT_TIME),
      lastDeliveryError: 'original refusal',
    });
    await db
      .prepare(
        `UPDATE mastra_notifications
           SET lastDeliveryAttemptAt = '2026-09-09T16:00:00+04:00'`,
      )
      .run();
    const before = await rawNotification(db);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: { type: 'exhausted', updatedAt: WRITE_TIME },
      }),
    ).resolves.toMatchObject({ applied: true });
    expect(await rawNotification(db)).toEqual({
      ...before,
      status: 'discarded',
      deliveryReason: 'delivery-attempts-exhausted',
      updatedAt: WRITE_TIME,
      discardedAt: WRITE_TIME,
      deliverAt: null,
      summaryAt: null,
    });
  });

  it('preserves absent prior error and attempt time during terminalization', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    await createDueNotification(s);
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      deliveryAttempts: 10,
    });
    await s.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: { type: 'exhausted', updatedAt: WRITE_TIME },
    });
    expect(await rawNotification(db)).toMatchObject({
      deliveryAttempts: 10,
      lastDeliveryAttemptAt: null,
      lastDeliveryError: null,
    });
  });

  it.each([
    ['status', 'delivered'],
    ['deliveredSignalId', 'existing-signal'],
  ] as const)('refuses an observed %s receipt', async (field, value) => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    await createDueNotification(s);
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      [field]: value,
    });
    const before = await rawNotification(db);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).resolves.toEqual({ applied: false });
    expect(await rawNotification(db)).toEqual(before);
  });

  it('returns no-write after a notification is removed', async () => {
    const s = store();
    const original = await createDueNotification(s);
    await s.dangerouslyClearAll();
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).resolves.toEqual({ applied: false });
  });

  it.each([
    ['thread_id', 'another-thread'],
    ['id', 'another-receipt'],
    ['source', 'another-source'],
    ['kind', 'another-kind'],
    ['priority', 'urgent'],
    ['status', 'discarded'],
    ['summary', 'another-summary'],
    ['payload', '{"changed":true}'],
    ['resourceId', 'another-resource'],
    ['agentId', 'another-agent'],
    ['sourceId', 'another-source-id'],
    ['dedupeKey', 'another-dedupe-key'],
    ['coalesceKey', 'another-coalesce-key'],
    ['coalescedCount', 2],
    ['attributes', '{"changed":true}'],
    ['createdAt', WRITE_TIME],
    ['updatedAt', WRITE_TIME],
    ['deliverAt', null],
    ['summaryAt', null],
    ['deliveryReason', 'another-reason'],
    ['deliveryAttempts', 1],
    ['lastDeliveryAttemptAt', ATTEMPT_TIME],
    ['lastDeliveryError', 'another-error'],
    ['deliveredSignalId', 'another-delivery'],
    ['summarySignalId', 'another-summary-signal'],
    ['deliveredAt', WRITE_TIME],
    ['seenAt', WRITE_TIME],
    ['dismissedAt', WRITE_TIME],
    ['archivedAt', WRITE_TIME],
    ['discardedAt', WRITE_TIME],
    ['metadata', '{"changed":true}'],
  ])('preserves a concurrent %s change at the final SQL write', async (column, value) => {
    const db = database();
    const barrier = deliveryWriteBarrier(db);
    const left = new D1NotificationsStorage(barrier.db);
    const right = new D1NotificationsStorage(db);
    const original = await createDueNotification(right);
    const pending = left.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    await barrier.writing;
    await db
      .prepare(`UPDATE mastra_notifications SET ${column} = ?`)
      .bind(value)
      .run();
    const newer = await rawNotification(db);
    barrier.release();
    await expect(pending).resolves.toEqual({ applied: false });
    expect(await rawNotification(db)).toEqual(newer);
  });

  it.each([
    'summary',
    'delivery',
    'denial',
    'failure',
    'coalescing',
    'replacement',
  ])('preserves a newer %s written by another storage adapter', async (outcome) => {
    const db = database();
    const barrier = deliveryWriteBarrier(db);
    const left = new D1NotificationsStorage(barrier.db);
    const right = new D1NotificationsStorage(db);
    const original = await createDueNotification(right, {
      coalesceKey: 'group',
    });
    const pending = left.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    await barrier.writing;
    if (outcome === 'coalescing') {
      await createDueNotification(right, {
        coalesceKey: 'group',
        summary: 'merged',
      });
    } else if (outcome === 'replacement') {
      await createDueNotification(right, {
        source: 'replacement',
        payload: { new: true },
      });
    } else if (outcome === 'failure') {
      await right.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: { ...retryFailure(), lastDeliveryError: 'original refusal' },
      });
    } else {
      await right.updateNotification({
        threadId: 'thread',
        id: 'receipt',
        ...(outcome === 'summary'
          ? { summaryAt: null, summarySignalId: 'summary-receipt' }
          : outcome === 'delivery'
            ? { status: 'delivered', deliveredSignalId: 'delivery-receipt' }
            : { status: 'discarded', deliveryReason: 'content-policy-denied' }),
      });
    }
    await db
      .prepare('UPDATE mastra_notifications SET updatedAt = ?')
      .bind(original.updatedAt.toISOString())
      .run();
    const newer = await rawNotification(db);
    barrier.release();
    await expect(pending).resolves.toEqual({ applied: false });
    expect(await rawNotification(db)).toEqual(newer);
  });

  it('does not manufacture an identity for an identical same-ID replacement', async () => {
    const db = database();
    const barrier = deliveryWriteBarrier(db);
    const left = new D1NotificationsStorage(barrier.db);
    const right = new D1NotificationsStorage(db);
    const original = await createDueNotification(right);
    const pending = left.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    await barrier.writing;
    await createDueNotification(right);
    barrier.release();
    await expect(pending).resolves.toMatchObject({ applied: true });
  });

  it('captures input scalars before its first await', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s);
    const expected = { ...captureNotificationDeliveryObservation(original) };
    const failure = retryFailure();
    const pending = s.updateNotificationDeliveryIfUnchanged({
      expected,
      failure,
    });
    expected.summary = 'mutated';
    expected.deliveryAttempts = 200;
    Object.assign(failure, {
      deliveryAttempts: 201,
      lastDeliveryError: 'mutated',
    });
    await expect(pending).resolves.toMatchObject({
      applied: true,
      record: {
        summary: 'summary',
        deliveryAttempts: 1,
        lastDeliveryError: 'target refused',
      },
    });
  });

  it.each([
    ['payload', 'not-json'],
    ['payload', '{"overflow":1e999}'],
    ['attributes', '{'],
    ['metadata', '{'],
    ['deliverAt', 'not-a-date'],
    ['summaryAt', ''],
    ['createdAt', ''],
    ['updatedAt', 'not-a-date'],
    ['lastDeliveryAttemptAt', 'not-a-date'],
    ['deliveryAttempts', -1],
    ['deliveryAttempts', 0.5],
    ['deliveryAttempts', 'broken'],
    ['coalescedCount', 'broken'],
  ])('rejects unreadable raw %s values instead of treating them as absent', async (column, value) => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s);
    await db
      .prepare(`UPDATE mastra_notifications SET ${column} = ?`)
      .bind(value)
      .run();
    const before = await rawNotification(db);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow();
    expect(await rawNotification(db)).toEqual(before);
  });

  it('keeps an unsafe SQLite integer unchanged when the driver cannot read it', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s);
    await db
      .prepare('UPDATE mastra_notifications SET deliveryAttempts = ?')
      .bind(Number.MAX_SAFE_INTEGER + 1)
      .run();
    const readReceipt = () =>
      db
        .prepare(
          `SELECT CAST(deliveryAttempts AS TEXT) AS attempts,
       updatedAt, lastDeliveryError, lastDeliveryAttemptAt
       FROM mastra_notifications`,
        )
        .first();
    const before = await readReceipt();
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow();
    expect(await readReceipt()).toEqual(before);
  });

  it('uses binary final guards when the stored content collation ignores case', async () => {
    const db = database();
    await createLegacyNotificationsTable(db, 'NOCASE');
    const barrier = deliveryWriteBarrier(db);
    const left = new D1NotificationsStorage(barrier.db);
    const right = new D1NotificationsStorage(db);
    const original = await createDueNotification(right);
    const pending = left.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    await barrier.writing;
    await db
      .prepare("UPDATE mastra_notifications SET summary = 'SUMMARY'")
      .run();
    const newer = await rawNotification(db);
    barrier.release();
    await expect(pending).resolves.toEqual({ applied: false });
    expect(await rawNotification(db)).toEqual(newer);
  });

  it.each([
    null,
    -1,
    0.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER + 1,
    '0',
  ])('rejects malformed captured deliveryAttempts %s without database work', async (value) => {
    const s = store();
    const original = await createDueNotification(s);
    const expected = {
      ...captureNotificationDeliveryObservation(original),
      deliveryAttempts: value,
    } as NotificationDeliveryObservation;
    await expect(
      new D1NotificationsStorage({
        prepare() {
          throw new Error('database must not be reached');
        },
      }).updateNotificationDeliveryIfUnchanged({
        expected,
        failure: retryFailure(),
      }),
    ).rejects.toThrow('nonnegative safe integer');
  });

  it.each([
    { type: 'unknown', updatedAt: WRITE_TIME },
    { ...retryFailure(), summary: 'unauthorized content edit' },
    { ...retryFailure(), deliveryAttempts: 0 },
    { ...retryFailure(), deliveryAttempts: 2 },
    { ...retryFailure(), lastDeliveryAttemptAt: 'not-a-date' },
    { ...retryFailure(), updatedAt: '2026-09-09T13:00:00Z' },
    { ...retryFailure(), lastDeliveryError: null },
    { ...retryFailure(), deliverAt: undefined },
    { ...retryFailure(), deliverAt: ATTEMPT_TIME },
    { ...retryFailure(), summaryAt: RETRY_TIME },
    { type: 'exhausted', updatedAt: WRITE_TIME, deliveryAttempts: 1 },
  ])('rejects malformed or broad failure patches %#', async (failure) => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s);
    const before = await rawNotification(db);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: failure as NotificationDeliveryFailure,
      }),
    ).rejects.toThrow();
    expect(await rawNotification(db)).toEqual(before);
  });

  it('cannot overflow a maximum safe counter while recording a new failure', async () => {
    const s = store();
    await createDueNotification(s);
    const original = await s.updateNotification({
      threadId: 'thread',
      id: 'receipt',
      deliveryAttempts: Number.MAX_SAFE_INTEGER,
    });
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: {
          ...retryFailure(),
          deliveryAttempts: Number.MAX_SAFE_INTEGER,
        },
      }),
    ).rejects.toThrow('increment once');
  });

  it('distinguishes absent JSON from JSON null and preserves serializer conversions', async () => {
    const s = store();
    const original = await createDueNotification(s, {
      payload: {
        date: new Date(ATTEMPT_TIME),
        omitted: undefined,
        infinite: Infinity,
      },
      attributes: { omitted: undefined },
      metadata: { values: [undefined, null] },
    });
    const expected = captureNotificationDeliveryObservation(original);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected,
        failure: retryFailure(),
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: {
        payload: { date: ATTEMPT_TIME, infinite: null },
        attributes: {},
        metadata: { values: [null, null] },
      },
    });
    const second = await createDueNotification(s, { id: 'second' });
    await s.updateNotification({
      threadId: 'thread',
      id: 'second',
      payload: null,
    });
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(second),
        failure: retryFailure(),
      }),
    ).resolves.toEqual({ applied: false });
  });

  it('accepts absent public counters and optional null bindings/dates', async () => {
    const s = store();
    const original = await createDueNotification(s, {
      agentId: undefined,
      summaryAt: undefined,
    });
    const compatible = {
      ...original,
      deliveryAttempts: undefined,
      coalescedCount: undefined,
      agentId: null,
      summaryAt: null,
    } as unknown as NotificationRecord;
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(compatible),
        failure: retryFailure(),
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: { deliveryAttempts: 1 },
    });
  });

  it('compares numeric values without inventing a coalescing generation', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const original = await createDueNotification(s);
    await db
      .prepare('UPDATE mastra_notifications SET coalescedCount = -2.5')
      .run();
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: {
          ...captureNotificationDeliveryObservation(original),
          deliveryAttempts: -0,
          coalescedCount: -2.5,
        },
        failure: retryFailure(),
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: { deliveryAttempts: 1, coalescedCount: -2.5 },
    });
  });

  it.each([
    'not-json',
    '{"overflow":1e999}',
  ])('rejects unreadable JSON in a supplied observation: %s', async (payload) => {
    const s = store();
    const original = await createDueNotification(s);
    const expected = {
      ...captureNotificationDeliveryObservation(original),
      payload,
    };
    await expect(
      new D1NotificationsStorage({
        prepare() {
          throw new Error('database must not be reached');
        },
      }).updateNotificationDeliveryIfUnchanged({
        expected,
        failure: retryFailure(),
      }),
    ).rejects.toMatchObject({
      name: 'TypeError',
      message: 'Notification delivery JSON is malformed',
    });
  });

  it.each([
    { deliveryAttempts: null },
    { deliveryAttempts: Infinity },
    { payload: {} },
    { deliverAt: 1 },
    { agentId: 1 },
  ])('rejects unreadable driver row values before forgiving conversion %#', async (patch) => {
    const db = database();
    const s = new D1NotificationsStorage(
      interceptFirst(db, async (query, read) => {
        const row = await read();
        return query.startsWith('SELECT *') &&
          query.includes('thread_id COLLATE BINARY')
          ? { ...(row as object), ...patch }
          : row;
      }),
    );
    const original = await createDueNotification(s);
    const before = await rawNotification(db);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow();
    expect(await rawNotification(db)).toEqual(before);
  });

  it('bookkeeps path-unsafe physical bindings without routing them', async () => {
    const s = store();
    const original = await createDueNotification(s, {
      threadId: '../thread',
      resourceId: '../resource',
      agentId: '',
    });
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).resolves.toMatchObject({
      applied: true,
      record: { threadId: '../thread', deliveryAttempts: 1 },
    });
  });

  it('isolates the configured table prefix', async () => {
    const db = database();
    const left = new D1NotificationsStorage(db, 'left_');
    const right = new D1NotificationsStorage(db, 'right_');
    const original = await createDueNotification(left);
    const neighbor = await createDueNotification(right);
    await left.updateNotificationDeliveryIfUnchanged({
      expected: captureNotificationDeliveryObservation(original),
      failure: retryFailure(),
    });
    expect(
      await right.getNotification({ threadId: 'thread', id: 'receipt' }),
    ).toEqual(neighbor);
  });

  it.each([
    'before',
    'after',
  ] as const)('surfaces response loss %s the write without replay', async (phase) => {
    const db = database();
    let writes = 0;
    const wrapped = interceptFirst(db, async (query, read) => {
      if (!isDeliveryUpdate(query)) return read();
      writes += 1;
      if (phase === 'after') await read();
      throw new Error('response lost');
    });
    const s = new D1NotificationsStorage(wrapped);
    const original = await createDueNotification(s);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow('response lost');
    expect(writes).toBe(1);
    expect((await rawNotification(db)).deliveryAttempts).toBe(
      phase === 'after' ? 1 : 0,
    );
  });

  it.each([
    undefined,
    false,
    [],
    {},
    { deliveryAttempts: 1 },
  ])('rejects a malformed database RETURNING row %#', async (returned) => {
    const db = database();
    const s = new D1NotificationsStorage(
      interceptFirst(db, async (query, read) => {
        const row = await read();
        return isDeliveryUpdate(query) ? returned : row;
      }),
    );
    const original = await createDueNotification(s);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow();
  });

  it.each([
    { id: 'wrong' },
    { updatedAt: ATTEMPT_TIME },
    { deliveryAttempts: 2 },
    { lastDeliveryError: 'different' },
    { summarySignalId: 'different' },
    { payload: 'null' },
  ])('rejects a database RETURNING row with a different receipt %#', async (patch) => {
    const db = database();
    const s = new D1NotificationsStorage(
      interceptFirst(db, async (query, read) => {
        const row = await read();
        return isDeliveryUpdate(query) ? { ...(row as object), ...patch } : row;
      }),
    );
    const original = await createDueNotification(s);
    await expect(
      s.updateNotificationDeliveryIfUnchanged({
        expected: captureNotificationDeliveryObservation(original),
        failure: retryFailure(),
      }),
    ).rejects.toThrow('different receipt');
  });
});

describe('chronological notification timestamps', () => {
  const now = new Date('2026-01-01T00:00:00.000Z');

  it.each([
    'deliverAt',
    'summaryAt',
  ] as const)('keeps a future extended-year %s out of a limited due window', async (cursor) => {
    const s = store();
    await s.createNotification({
      id: 'future',
      threadId: 'acme_t1',
      source: 'test',
      kind: 'ready',
      summary: 'future',
      [cursor]: new Date('+010000-01-01T00:00:00.000Z'),
    });
    await s.createNotification({
      id: 'due',
      threadId: 'acme_t1',
      source: 'test',
      kind: 'ready',
      summary: 'due',
      [cursor]: new Date(now.getTime() - 1),
    });
    expect(
      (await s.listDueNotifications({ now, limit: 1 })).map((row) => row.id),
    ).toEqual(['due']);
  });

  it('orders negative years by their instants before applying the limit', async () => {
    const s = store();
    for (const [id, timestamp] of [
      ['newer', '-000001-01-01T00:00:00.000Z'],
      ['older', '-000010-01-01T00:00:00.000Z'],
    ] as const) {
      await s.createNotification({
        id,
        threadId: 'acme_t1',
        source: 'test',
        kind: 'ready',
        summary: id,
        deliverAt: new Date(timestamp),
      });
    }
    expect(
      (await s.listDueNotifications({ now, limit: 1 })).map((row) => row.id),
    ).toEqual(['older']);
  });

  it.each([
    { timestamp: '2026-01-01T01:00:00+02:00', expected: 'offset' },
    { timestamp: '2025-12-31T23:00:00-02:00', expected: 'due' },
  ])('uses the instant of raw offset $timestamp in a limited due window', async ({
    timestamp,
    expected,
  }) => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    const record = await s.createNotification({
      id: 'offset',
      threadId: 'acme_t1',
      source: 'test',
      kind: 'ready',
      summary: 'offset',
      summaryAt: new Date(timestamp),
    });
    await db
      .prepare('UPDATE mastra_notifications SET summaryAt = ? WHERE id = ?')
      .bind(timestamp, record.id)
      .run();
    await s.createNotification({
      id: 'due',
      threadId: 'acme_t1',
      source: 'test',
      kind: 'ready',
      summary: 'due',
      summaryAt: new Date(now.getTime() - 1),
    });
    expect(
      (await s.listDueNotifications({ now, limit: 1 })).map((row) => row.id),
    ).toEqual([expected]);
  });

  it('lists updated timestamps chronologically across extended years', async () => {
    const db = database();
    const s = new D1NotificationsStorage(db);
    for (const [id, updatedAt] of [
      ['ordinary', now.toISOString()],
      ['future', '+010000-01-01T00:00:00.000Z'],
    ] as const) {
      const record = await s.createNotification({
        id,
        threadId: 'acme_t1',
        source: 'test',
        kind: 'ready',
        summary: id,
      });
      await db
        .prepare('UPDATE mastra_notifications SET updatedAt = ? WHERE id = ?')
        .bind(updatedAt, record.id)
        .run();
    }
    expect(
      (await s.listNotifications({ threadId: 'acme_t1', limit: 1 })).map(
        (row) => row.id,
      ),
    ).toEqual(['future']);
  });
});

describe('notification timestamp conversion', () => {
  const epochs = [
    -8_640_000_000_000_000, -8_639_999_999_999_999, -62_167_219_200_001,
    -62_167_219_200_000, -1, 0, 1, 253_402_300_799_999, 253_402_300_800_000,
    253_402_300_800_001, 8_639_999_999_999_999, 8_640_000_000_000_000,
  ];
  for (let index = 1; index <= 64; index += 1) {
    epochs.push(Math.trunc((index / 65) * 8_640_000_000_000_000));
    epochs.push(-Math.trunc((index / 65) * 8_640_000_000_000_000));
  }

  const timestamps = [
    ...epochs.map((epoch) => new Date(epoch).toISOString()),
    '0000',
    '0000-02',
    '2026-09-09',
    '-000001',
    '-000001-02',
    '+010000',
    '+010000-02',
    '1900-02-29T12:00:00.000Z',
    '2000-02-29T12:00:00.000Z',
    '2100-02-29T12:00:00.000Z',
    '2400-02-29T12:00:00.000Z',
    '9999-12-31T24:00:00.000Z',
    '-000001-12-31T23:59:59.999-23:59',
    '0000-01-01T00:00:00.001+23:59',
    '+010000-01-01T00:00:00.1239+2359',
    '+010000-12-31T23:59:59.9999-2359',
    '+275760-09-13T01:00:00.000+01:00',
    '-271821-04-19T23:00:00.000-0100',
    '2026-09-09T12:34Z',
    '2026-09-09T12:34:56.1Z',
    '2026-09-09T12:34:56.12Z',
    '2026-09-09T12:34:56.1239Z',
    '2026-09-09T12:34:56.9999Z',
    '2026-09-09T12:34:56.00000001Z',
    '2026-09-09t12:34:56.123z',
  ];

  it.each(timestamps)('matches Date for %s', async (timestamp) => {
    const db = database();
    const expected = new Date(timestamp).getTime();
    expect(Number.isFinite(expected)).toBe(true);
    expect(notificationTimestampMillis(timestamp)).toBe(expected);
    expect(notificationTimestampMillis(new Date(timestamp))).toBe(expected);
    const row = await db
      .prepare(
        `SELECT ${notificationTimestampSql('deliverAt')} AS epoch
         FROM (SELECT ? AS deliverAt)`,
      )
      .bind(timestamp)
      .first<{ epoch: number }>();
    expect(row?.epoch).toBe(expected);
  });

  it.each([
    'now',
    '1700000000000',
    'September 9, 2026',
    '2026-09-09T12:34:56',
    '2026-09-09 12:34:56Z',
    '2026-13-01',
    '2026-01-32',
    '2026-09-09T24:01:00Z',
    '2026-09-09T12:60:00Z',
    '2026-09-09T12:34:60Z',
    '2026-09-09T12:34:56.Z',
    '2026-09-09T12:34:56+2400',
    '2026-09-09T12:34:56+00:60',
    '-000000-01-01T00:00:00.000Z',
    '+275760-09-13T00:00:00.001Z',
    '-271821-04-19T23:59:59.999Z',
    '2026\n',
    '2026-09-09T12:34:56.123Z\0',
    '2026-09-09T12:34:56.123Z\0ignored',
    '+010000-01-01T00:00:00.123Z\0',
  ])('leaves %j unavailable as a portable stored instant', async (timestamp) => {
    expect(() => notificationTimestampMillis(timestamp)).toThrow(TypeError);
    const db = database();
    const row = await db
      .prepare(
        `SELECT ${notificationTimestampSql('updatedAt')} AS epoch
         FROM (SELECT ? AS updatedAt)`,
      )
      .bind(timestamp)
      .first<{ epoch: number | null }>();
    expect(row?.epoch).toBeNull();
  });
});
