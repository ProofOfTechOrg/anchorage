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
  d1Changes,
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

const NOTIFICATION_COLUMNS = [
  'id',
  'thread_id',
  'source',
  'kind',
  'priority',
  'status',
  'summary',
  'payload',
  'resourceId',
  'agentId',
  'sourceId',
  'dedupeKey',
  'coalesceKey',
  'coalescedCount',
  'attributes',
  'createdAt',
  'updatedAt',
  'deliverAt',
  'summaryAt',
  'deliveryReason',
  'deliveryAttempts',
  'lastDeliveryAttemptAt',
  'lastDeliveryError',
  'deliveredSignalId',
  'summarySignalId',
  'deliveredAt',
  'seenAt',
  'dismissedAt',
  'archivedAt',
  'discardedAt',
  'metadata',
] as const;

function validLimit(limit: number | undefined): limit is number {
  return limit !== undefined && Number.isSafeInteger(limit) && limit >= 0;
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
    if (input.id !== undefined || (!input.dedupeKey && !input.coalesceKey)) {
      await this.#insert(record);
      return record;
    }

    for (let attempt = 0; attempt < 16; attempt += 1) {
      if (await this.#insertUnlessCoalescable(record, input)) {
        return record;
      }
      const existing = await this.#findCoalescableRow(input);
      if (!existing) continue;
      const existingRecord = rowToRecord(existing);
      const mergedAttributes = input.attributes
        ? { ...existingRecord.attributes, ...input.attributes }
        : existingRecord.attributes;
      const mergedMetadata = input.metadata
        ? { ...existingRecord.metadata, ...input.metadata }
        : existingRecord.metadata;
      const updatedAt = new Date();
      const sets = [
        'summary = ?',
        'payload = ?',
        'priority = ?',
        'attributes = ?',
        'updatedAt = ?',
        'deliverAt = ?',
        'summaryAt = ?',
        'deliveryReason = ?',
        'coalescedCount = coalescedCount + 1',
        'metadata = ?',
      ];
      const binds: unknown[] = [
        input.summary,
        jsonOrNull(input.payload ?? existingRecord.payload),
        input.priority ?? existingRecord.priority,
        jsonOrNull(mergedAttributes),
        updatedAt.toISOString(),
        isoOrNull(input.deliverAt ?? existingRecord.deliverAt),
        isoOrNull(input.summaryAt ?? existingRecord.summaryAt),
        input.deliveryReason ?? existingRecord.deliveryReason ?? null,
        jsonOrNull(mergedMetadata),
        existing.thread_id,
        existing.id,
        existing.coalescedCount,
      ];
      const guards = [
        'thread_id = ?',
        'id = ?',
        "status = 'pending'",
        'coalescedCount = ?',
      ];
      if (input.attributes) {
        guards.push('attributes IS ?');
        binds.push(existing.attributes);
      }
      if (input.metadata) {
        guards.push('metadata IS ?');
        binds.push(existing.metadata);
      }
      const updated = await this.#db
        .prepare(
          `UPDATE ${this.#table}
           SET ${sets.join(', ')}
           WHERE ${guards.join(' AND ')}
           RETURNING *`,
        )
        .bind(...binds)
        .first<NotificationRow>();
      if (updated) return rowToRecord(updated);
    }
    throw new Error(
      'Notification coalescing failed after 16 contention attempts',
    );
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
    const sqlLimit =
      input.search === undefined && validLimit(input.limit)
        ? input.limit
        : undefined;
    if (sqlLimit !== undefined) binds.push(sqlLimit);
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#table} WHERE ${clauses.join(' AND ')}
         ORDER BY updatedAt DESC${sqlLimit !== undefined ? ' LIMIT ?' : ''}`,
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
    return input.search !== undefined ||
      (input.limit !== undefined && !validLimit(input.limit))
      ? records.slice(0, input.limit)
      : records;
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
    const sqlLimit = validLimit(input.limit) ? input.limit : undefined;
    if (sqlLimit !== undefined) binds.push(sqlLimit);
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#table}
         WHERE ${clauses.join(' AND ')}
         ORDER BY
           CASE
             WHEN deliverAt IS NULL THEN summaryAt
             WHEN summaryAt IS NULL THEN deliverAt
             WHEN deliverAt <= summaryAt THEN deliverAt
             ELSE summaryAt
           END ASC,
           updatedAt ASC${sqlLimit !== undefined ? ' LIMIT ?' : ''}`,
      )
      .bind(...binds)
      .all<NotificationRow>();
    const records = results.map(rowToRecord);
    return input.limit !== undefined && !validLimit(input.limit)
      ? records.slice(0, input.limit)
      : records;
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
    const now = new Date();
    const sets = ['updatedAt = ?'];
    const binds: unknown[] = [now.toISOString()];
    if (input.status !== undefined) {
      sets.push('status = ?');
      binds.push(input.status);
      const column = statusTimestampColumn(input.status);
      if (column) {
        sets.push(`${column} = ?`);
        binds.push(now.toISOString());
      }
    }
    if (input.summary !== undefined) {
      sets.push('summary = ?');
      binds.push(input.summary);
    }
    if (input.payload !== undefined) {
      sets.push('payload = ?');
      binds.push(jsonOrNull(input.payload));
    }
    if (input.attributes !== undefined) {
      sets.push('attributes = ?');
      binds.push(jsonOrNull(input.attributes));
    }
    if (input.metadata !== undefined) {
      sets.push('metadata = ?');
      binds.push(jsonOrNull(input.metadata));
    }
    if (input.deliverAt !== undefined) {
      sets.push('deliverAt = ?');
      binds.push(isoOrNull(input.deliverAt ?? undefined));
    }
    if (input.summaryAt !== undefined) {
      sets.push('summaryAt = ?');
      binds.push(isoOrNull(input.summaryAt ?? undefined));
    }
    if (input.deliveryReason !== undefined) {
      sets.push('deliveryReason = ?');
      binds.push(input.deliveryReason);
    }
    if (input.deliveryAttempts !== undefined) {
      sets.push('deliveryAttempts = ?');
      binds.push(input.deliveryAttempts);
    }
    if (input.lastDeliveryAttemptAt !== undefined) {
      sets.push('lastDeliveryAttemptAt = ?');
      binds.push(isoOrNull(input.lastDeliveryAttemptAt));
    }
    if (input.lastDeliveryError !== undefined) {
      sets.push('lastDeliveryError = ?');
      binds.push(input.lastDeliveryError);
    }
    if (input.deliveredSignalId !== undefined) {
      sets.push('deliveredSignalId = ?');
      binds.push(input.deliveredSignalId);
    }
    if (input.summarySignalId !== undefined) {
      sets.push('summarySignalId = ?');
      binds.push(input.summarySignalId);
    }
    binds.push(input.threadId, input.id);
    const updated = await this.#db
      .prepare(
        `UPDATE ${this.#table}
         SET ${sets.join(', ')}
         WHERE thread_id = ? AND id = ?
         RETURNING *`,
      )
      .bind(...binds)
      .first<NotificationRow>();
    if (!updated) {
      throw new Error(
        `Notification ${input.id} was not found for thread ${input.threadId}`,
      );
    }
    return rowToRecord(updated);
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
    if (values.length === 0) {
      clauses.push('0 = 1');
      return;
    }
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
  async #findCoalescableRow(
    input: CreateNotificationInput,
  ): Promise<NotificationRow | undefined> {
    if (!input.dedupeKey && !input.coalesceKey) return undefined;
    const match = this.#coalescableMatch(input);
    const row = await this.#db
      .prepare(
        `SELECT * FROM ${this.#table}
         WHERE ${match.clause}
         ORDER BY createdAt ASC, id ASC
         LIMIT 1`,
      )
      .bind(...match.binds)
      .first<NotificationRow>();
    return row ?? undefined;
  }

  #coalescableMatch(input: CreateNotificationInput): {
    clause: string;
    binds: unknown[];
  } {
    return {
      clause: `thread_id = ?
        AND source = ?
        AND kind = ?
        AND status = 'pending'
        AND agentId IS ?
        AND resourceId IS ?
        AND (
          (? IS NOT NULL AND dedupeKey = ?)
          OR (? IS NOT NULL AND coalesceKey = ?)
        )`,
      binds: [
        input.threadId,
        input.source,
        input.kind,
        input.agentId ?? null,
        input.resourceId ?? null,
        input.dedupeKey ?? null,
        input.dedupeKey ?? null,
        input.coalesceKey ?? null,
        input.coalesceKey ?? null,
      ],
    };
  }

  async #insertUnlessCoalescable(
    record: NotificationRecord,
    input: CreateNotificationInput,
  ): Promise<boolean> {
    const match = this.#coalescableMatch(input);
    const result = await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#table} (${NOTIFICATION_COLUMNS.join(', ')})
         SELECT ${NOTIFICATION_COLUMNS.map(() => '?').join(', ')}
         WHERE NOT EXISTS (
           SELECT 1 FROM ${this.#table} WHERE ${match.clause}
         )`,
      )
      .bind(...recordValues(record), ...match.binds)
      .run();
    return d1Changes(result) > 0;
  }

  /** INSERT-or-REPLACE the full record (create, coalesce-merge, and update share it). */
  async #insert(record: NotificationRecord): Promise<void> {
    await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#table} (${NOTIFICATION_COLUMNS.join(', ')})
         VALUES (${NOTIFICATION_COLUMNS.map(() => '?').join(', ')})`,
      )
      .bind(...recordValues(record))
      .run();
  }
}

function recordValues(record: NotificationRecord): unknown[] {
  return [
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
  ];
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
