// SPDX-License-Identifier: Apache-2.0
// D1NotificationsStorage round-trip / coalescing / listDue / update — mirrors the
// core InMemoryNotificationsStorage behavior over a node:sqlite SQL unit facade.

import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type { SignalDatabase, SignalStatement } from './d1-shared.js';
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

  function wrap(query: string, statement: SignalStatement): SignalStatement {
    return {
      bind(...values: unknown[]) {
        return wrap(query, statement.bind(...values));
      },
      async first<T = unknown>(): Promise<T | null> {
        const row = await statement.first<T>();
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
    db: {
      prepare(query: string) {
        return wrap(query, db.prepare(query));
      },
      batch(statements) {
        if (!db.batch) throw new Error('test database has no batch');
        return db.batch(statements);
      },
    },
    selected,
    release: releaseRead,
  };
}

async function createLegacyNotificationsTable(
  db: SignalDatabase,
): Promise<void> {
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
