// SPDX-License-Identifier: Apache-2.0
// Track C (M-004), CI-M-004-002 — the D1 NotificationsStorage domain over
// TABLE_NOTIFICATIONS ('mastra_notifications'), mirroring core's abstract
// NotificationsStorage + InMemoryNotificationsStorage reference (create / list /
// listDue / get({threadId,id}) / update + findCoalescable coalescing). Composed
// into createD1Storage's store so `agent.sendNotificationSignal` persists here
// (core resolves the domain via `mastra.getStorage().getStore('notifications')`).
//
// NAMING: mastra_notifications is the AGENT inbox — signals delivered TO the
// model on its next turn. Distinct from flowsafe's ApprovalNotificationSink,
// which notifies HUMANS about approvals. Different layers.
//
// TENANCY: the `thread_id` column holds the tenant-salted threadId
// (`${tenantId}_${uuid}`), so purgeTenant's `[tid_, tid\x60)` range is exact
// over it (registered in TENANT_RANGE_PURGE_TABLES) and the notification TTL
// reaps terminal rows (purgeExpiredNotifications). Timestamps are ISO-8601 TEXT
// — the encoding the retention purges and the schema guard ride on.

import {
  type CreateNotificationInput,
  type ListDueNotificationsInput,
  type ListNotificationsInput,
  type NotificationPriority,
  type NotificationRecord,
  type NotificationStatus,
  NotificationsStorage,
  type UpdateNotificationInput,
} from '@mastra/core/notifications';

import {
  dateOrUndefined,
  isoOrNull,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
} from './d1-shared.js';

/** The raw row shape `mastra_notifications` stores and `rowToRecord` reads. */
interface NotificationRow {
  id: string;
  thread_id: string;
  source: string;
  kind: string;
  priority: string;
  status: string;
  summary: string;
  payload: string | null;
  resourceId: string | null;
  agentId: string | null;
  sourceId: string | null;
  dedupeKey: string | null;
  coalesceKey: string | null;
  coalescedCount: number;
  attributes: string | null;
  createdAt: string;
  updatedAt: string;
  deliverAt: string | null;
  summaryAt: string | null;
  deliveryReason: string | null;
  deliveryAttempts: number;
  lastDeliveryAttemptAt: string | null;
  lastDeliveryError: string | null;
  deliveredSignalId: string | null;
  summarySignalId: string | null;
  deliveredAt: string | null;
  seenAt: string | null;
  dismissedAt: string | null;
  archivedAt: string | null;
  discardedAt: string | null;
  metadata: string | null;
}

/** status → the timestamp column that status stamps (mirrors core statusTimestamp). */
function statusTimestampColumn(status: NotificationStatus): string | undefined {
  switch (status) {
    case 'delivered':
      return 'deliveredAt';
    case 'seen':
      return 'seenAt';
    case 'dismissed':
      return 'dismissedAt';
    case 'archived':
      return 'archivedAt';
    case 'discarded':
      return 'discardedAt';
    default:
      return undefined;
  }
}

/** The earliest of deliverAt/summaryAt, or +Infinity when neither is set (core dueTime). */
function dueTime(record: NotificationRecord): number {
  const deliverAt = record.deliverAt?.getTime();
  const summaryAt = record.summaryAt?.getTime();
  if (deliverAt !== undefined && summaryAt !== undefined) {
    return Math.min(deliverAt, summaryAt);
  }
  return deliverAt ?? summaryAt ?? Number.POSITIVE_INFINITY;
}

export class D1NotificationsStorage extends NotificationsStorage {
  readonly #db: SignalDatabase;
  readonly #table: string;
  #ready?: Promise<void>;

  constructor(db: SignalDatabase, tablePrefix = '') {
    super();
    this.#db = db;
    this.#table = `${tablePrefix}mastra_notifications`;
  }

  /**
   * Lazy, memoized schema creation — the same clear-on-failure promise memo the
   * approval store uses: only SUCCESS memoizes, so a transient DDL failure
   * retries on the next call rather than pinning the domain to a dead promise.
   * `init()` (called by the composite store) and every operation await it.
   */
  #ensureSchema(): Promise<void> {
    if (!this.#ready) {
      this.#ready = Promise.resolve(
        this.#db
          .prepare(
            `CREATE TABLE IF NOT EXISTS ${this.#table} (
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
          .run()
          .then(() =>
            this.#db
              .prepare(
                `CREATE INDEX IF NOT EXISTS idx_${this.#table}_thread
                 ON ${this.#table} (thread_id)`,
              )
              .run(),
          )
          .then(() =>
            this.#db
              .prepare(
                `CREATE INDEX IF NOT EXISTS idx_${this.#table}_status
                 ON ${this.#table} (thread_id, status)`,
              )
              .run(),
          )
          .then(() => undefined),
      ).catch((error: unknown) => {
        this.#ready = undefined;
        throw error;
      });
    }
    return this.#ready;
  }

  async init(): Promise<void> {
    await this.#ensureSchema();
  }

  async createNotification(
    input: CreateNotificationInput,
  ): Promise<NotificationRecord> {
    await this.#ensureSchema();
    // Coalesce onto an existing pending record with the same dedupe/coalesce key
    // (bump the count, refresh the summary), exactly as the InMemory reference.
    const existing = await this.#findCoalescable(input);
    if (existing) {
      const next: NotificationRecord = {
        ...existing,
        summary: input.summary,
        payload: input.payload ?? existing.payload,
        priority: input.priority ?? existing.priority,
        attributes: input.attributes
          ? { ...existing.attributes, ...input.attributes }
          : existing.attributes,
        updatedAt: new Date(),
        deliverAt: input.deliverAt ?? existing.deliverAt,
        summaryAt: input.summaryAt ?? existing.summaryAt,
        deliveryReason: input.deliveryReason ?? existing.deliveryReason,
        coalescedCount: (existing.coalescedCount ?? 1) + 1,
        metadata: input.metadata
          ? { ...existing.metadata, ...input.metadata }
          : existing.metadata,
      };
      await this.#insert(next);
      return next;
    }
    const now = input.createdAt ?? new Date();
    const record: NotificationRecord = {
      id: input.id ?? crypto.randomUUID(),
      threadId: input.threadId,
      source: input.source,
      kind: input.kind,
      priority: input.priority ?? 'medium',
      status: 'pending',
      summary: input.summary,
      payload: input.payload,
      resourceId: input.resourceId,
      agentId: input.agentId,
      sourceId: input.sourceId,
      dedupeKey: input.dedupeKey,
      coalesceKey: input.coalesceKey,
      coalescedCount: 1,
      attributes: input.attributes,
      createdAt: now,
      updatedAt: now,
      deliverAt: input.deliverAt,
      summaryAt: input.summaryAt,
      deliveryReason: input.deliveryReason,
      deliveryAttempts: 0,
      metadata: input.metadata,
    };
    await this.#insert(record);
    return record;
  }

  async listNotifications(
    input: ListNotificationsInput,
  ): Promise<NotificationRecord[]> {
    await this.#ensureSchema();
    const clauses = ['thread_id = ?'];
    const binds: unknown[] = [input.threadId];
    this.#pushIn(clauses, binds, 'status', input.status);
    this.#pushIn(clauses, binds, 'priority', input.priority);
    if (input.source !== undefined) {
      clauses.push('source = ?');
      binds.push(input.source);
    }
    if (input.resourceId !== undefined) {
      clauses.push('resourceId = ?');
      binds.push(input.resourceId);
    }
    if (input.agentId !== undefined) {
      clauses.push('agentId = ?');
      binds.push(input.agentId);
    }
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#table} WHERE ${clauses.join(' AND ')}
         ORDER BY updatedAt DESC`,
      )
      .bind(...binds)
      .all<NotificationRow>();
    let records = results.map(rowToRecord);
    // `search` mirrors the InMemory case-insensitive scan over summary/kind/source
    // (kept in JS so the SQL stays index-friendly and the semantics match exactly).
    if (input.search !== undefined) {
      const needle = input.search.toLowerCase();
      records = records.filter(
        (r) =>
          r.summary.toLowerCase().includes(needle) ||
          r.kind.toLowerCase().includes(needle) ||
          r.source.toLowerCase().includes(needle),
      );
    }
    return input.limit !== undefined ? records.slice(0, input.limit) : records;
  }

  /**
   * Due pending notifications across ALL threads/tenants, filtered only by the
   * optional agentId/resourceId — GLOBALLY UNSCOPED by tenant, exactly like
   * core's InMemory reference (a cron dispatcher's cross-thread sweep). It is a
   * TCB-only read (no client route reaches it), so this is not a leak here — but
   * it is a CLOSE-BEFORE-WIRING constraint for Track A: whatever dispatcher
   * drives delivery from this list MUST scope each dispatch by the row's salted
   * `resourceId` (`${tenantId}_…`) / per-thread `threadId` before it acts, or a
   * cross-tenant sweep would deliver one tenant's inbox into another's loop.
   * (See signals/CLAUDE.md — the F3 dispatcher-scope note.)
   */
  async listDueNotifications(
    input: ListDueNotificationsInput,
  ): Promise<NotificationRecord[]> {
    await this.#ensureSchema();
    const now = input.now.toISOString();
    const clauses = [
      "status = 'pending'",
      '((deliverAt IS NOT NULL AND deliverAt <= ?) OR (summaryAt IS NOT NULL AND summaryAt <= ?))',
    ];
    const binds: unknown[] = [now, now];
    if (input.agentId !== undefined) {
      clauses.push('agentId = ?');
      binds.push(input.agentId);
    }
    if (input.resourceId !== undefined) {
      clauses.push('resourceId = ?');
      binds.push(input.resourceId);
    }
    const { results } = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE ${clauses.join(' AND ')}`)
      .bind(...binds)
      .all<NotificationRow>();
    // dueTime ordering (earliest deliver/summary first, then updatedAt) is a
    // min-of-two-nullables — computed in JS to mirror the reference exactly.
    const records = results
      .map(rowToRecord)
      .sort(
        (a, b) =>
          dueTime(a) - dueTime(b) ||
          a.updatedAt.getTime() - b.updatedAt.getTime(),
      );
    return input.limit !== undefined ? records.slice(0, input.limit) : records;
  }

  async getNotification(input: {
    threadId: string;
    id: string;
  }): Promise<NotificationRecord | null> {
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#table} WHERE thread_id = ? AND id = ?`)
      .bind(input.threadId, input.id)
      .first<NotificationRow>();
    return row ? rowToRecord(row) : null;
  }

  async updateNotification(
    input: UpdateNotificationInput,
  ): Promise<NotificationRecord> {
    await this.#ensureSchema();
    const existing = await this.getNotification({
      threadId: input.threadId,
      id: input.id,
    });
    if (!existing) {
      throw new Error(
        `Notification ${input.id} was not found for thread ${input.threadId}`,
      );
    }
    const now = new Date();
    const next: NotificationRecord = { ...existing, updatedAt: now };
    if (input.status !== undefined) {
      next.status = input.status;
      const column = statusTimestampColumn(input.status);
      if (column) {
        (next as unknown as Record<string, Date>)[column] = now;
      }
    }
    if (input.summary !== undefined) next.summary = input.summary;
    if (input.payload !== undefined) next.payload = input.payload;
    if (input.attributes !== undefined) next.attributes = input.attributes;
    if (input.metadata !== undefined) next.metadata = input.metadata;
    if (input.deliverAt !== undefined)
      next.deliverAt = input.deliverAt ?? undefined;
    if (input.summaryAt !== undefined)
      next.summaryAt = input.summaryAt ?? undefined;
    if (input.deliveryReason !== undefined) {
      next.deliveryReason = input.deliveryReason;
    }
    if (input.deliveryAttempts !== undefined) {
      next.deliveryAttempts = input.deliveryAttempts;
    }
    if (input.lastDeliveryAttemptAt !== undefined) {
      next.lastDeliveryAttemptAt = input.lastDeliveryAttemptAt;
    }
    if (input.lastDeliveryError !== undefined) {
      next.lastDeliveryError = input.lastDeliveryError;
    }
    if (input.deliveredSignalId !== undefined) {
      next.deliveredSignalId = input.deliveredSignalId;
    }
    if (input.summarySignalId !== undefined) {
      next.summarySignalId = input.summarySignalId;
    }
    await this.#insert(next);
    return next;
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#ensureSchema();
    await this.#db.prepare(`DELETE FROM ${this.#table}`).run();
  }

  /** Build a `column IN (?, ?)` clause for a single value or an array filter. */
  #pushIn(
    clauses: string[],
    binds: unknown[],
    column: string,
    filter: string | string[] | undefined,
  ): void {
    if (filter === undefined) return;
    const values = Array.isArray(filter) ? filter : [filter];
    if (values.length === 0) return;
    clauses.push(`${column} IN (${values.map(() => '?').join(', ')})`);
    binds.push(...values);
  }

  /**
   * The first PENDING record on the same (threadId, source, kind, agentId,
   * resourceId) whose dedupeKey OR coalesceKey matches — mirrors the reference's
   * `.find`. The agentId/resourceId equality (including undefined-vs-undefined)
   * is done in JS because SQL `= NULL` never matches; the candidate set is tiny
   * (one thread's pending rows of one source/kind).
   *
   * The `ORDER BY createdAt, id` is load-bearing: core's InMemory `.find` walks
   * rows in INSERTION order, so "the first coalescable" is the EARLIEST-created
   * one. D1 rows without an ORDER BY come back in an unspecified order, so with
   * two candidates sharing a key the coalesce target could drift between runs
   * (or engines). createdAt is set once and preserved through a coalesce-merge
   * (only updatedAt bumps), so it is the stable insertion key; `id` breaks a
   * same-millisecond tie deterministically.
   */
  async #findCoalescable(
    input: CreateNotificationInput,
  ): Promise<NotificationRecord | undefined> {
    if (!input.dedupeKey && !input.coalesceKey) return undefined;
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#table}
         WHERE thread_id = ? AND source = ? AND kind = ? AND status = 'pending'
         ORDER BY createdAt ASC, id ASC`,
      )
      .bind(input.threadId, input.source, input.kind)
      .all<NotificationRow>();
    return results.map(rowToRecord).find((record) => {
      if (record.agentId !== input.agentId) return false;
      if (record.resourceId !== input.resourceId) return false;
      return Boolean(
        (input.dedupeKey && record.dedupeKey === input.dedupeKey) ||
          (input.coalesceKey && record.coalesceKey === input.coalesceKey),
      );
    });
  }

  /** INSERT-or-REPLACE the full record (create, coalesce-merge, and update share it). */
  async #insert(record: NotificationRecord): Promise<void> {
    await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#table} (
           id, thread_id, source, kind, priority, status, summary, payload,
           resourceId, agentId, sourceId, dedupeKey, coalesceKey, coalescedCount,
           attributes, createdAt, updatedAt, deliverAt, summaryAt, deliveryReason,
           deliveryAttempts, lastDeliveryAttemptAt, lastDeliveryError,
           deliveredSignalId, summarySignalId, deliveredAt, seenAt, dismissedAt,
           archivedAt, discardedAt, metadata
         ) VALUES (${Array(31).fill('?').join(', ')})`,
      )
      .bind(
        record.id,
        record.threadId,
        record.source,
        record.kind,
        record.priority,
        record.status,
        record.summary,
        jsonOrNull(record.payload),
        record.resourceId ?? null,
        record.agentId ?? null,
        record.sourceId ?? null,
        record.dedupeKey ?? null,
        record.coalesceKey ?? null,
        record.coalescedCount ?? 1,
        jsonOrNull(record.attributes),
        record.createdAt.toISOString(),
        record.updatedAt.toISOString(),
        isoOrNull(record.deliverAt),
        isoOrNull(record.summaryAt),
        record.deliveryReason ?? null,
        record.deliveryAttempts ?? 0,
        isoOrNull(record.lastDeliveryAttemptAt),
        record.lastDeliveryError ?? null,
        record.deliveredSignalId ?? null,
        record.summarySignalId ?? null,
        isoOrNull(record.deliveredAt),
        isoOrNull(record.seenAt),
        isoOrNull(record.dismissedAt),
        isoOrNull(record.archivedAt),
        isoOrNull(record.discardedAt),
        jsonOrNull(record.metadata),
      )
      .run();
  }
}

/** Map a stored row back to the core NotificationRecord shape. */
function rowToRecord(row: NotificationRow): NotificationRecord {
  return {
    id: row.id,
    threadId: row.thread_id,
    source: row.source,
    kind: row.kind,
    priority: row.priority as NotificationPriority,
    status: row.status as NotificationStatus,
    summary: row.summary,
    payload: parseJsonOrUndefined(row.payload),
    resourceId: row.resourceId ?? undefined,
    agentId: row.agentId ?? undefined,
    sourceId: row.sourceId ?? undefined,
    dedupeKey: row.dedupeKey ?? undefined,
    coalesceKey: row.coalesceKey ?? undefined,
    coalescedCount: row.coalescedCount ?? undefined,
    attributes: parseJsonOrUndefined(row.attributes),
    createdAt: new Date(row.createdAt),
    updatedAt: new Date(row.updatedAt),
    deliverAt: dateOrUndefined(row.deliverAt),
    summaryAt: dateOrUndefined(row.summaryAt),
    deliveryReason: row.deliveryReason ?? undefined,
    deliveryAttempts: row.deliveryAttempts ?? undefined,
    lastDeliveryAttemptAt: dateOrUndefined(row.lastDeliveryAttemptAt),
    lastDeliveryError: row.lastDeliveryError ?? undefined,
    deliveredSignalId: row.deliveredSignalId ?? undefined,
    summarySignalId: row.summarySignalId ?? undefined,
    deliveredAt: dateOrUndefined(row.deliveredAt),
    seenAt: dateOrUndefined(row.seenAt),
    dismissedAt: dateOrUndefined(row.dismissedAt),
    archivedAt: dateOrUndefined(row.archivedAt),
    discardedAt: dateOrUndefined(row.discardedAt),
    metadata: parseJsonOrUndefined(row.metadata),
  };
}
