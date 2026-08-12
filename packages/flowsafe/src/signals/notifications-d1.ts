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
// `thread_id` holds the host-minted threadId. The notification TTL reaps
// terminal rows (purgeExpiredNotifications). Timestamps are ISO-8601 TEXT —
// the encoding the retention purges and the schema guard ride on.

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
import { validateTablePrefix } from '../do-runner/table-prefix.js';
import {
  d1Changes,
  dateOrUndefined,
  isoOrNull,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
  type SignalStatement,
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
  insertionOrdinal: number | null;
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

const NOTIFICATION_UPDATE_COLUMNS = NOTIFICATION_COLUMNS.filter(
  (column) => column !== 'id' && column !== 'thread_id',
);

function validLimit(limit: number | undefined): limit is number {
  return limit !== undefined && Number.isSafeInteger(limit) && limit >= 0;
}

function isDuplicateColumn(error: unknown): boolean {
  return error instanceof Error && /duplicate column/i.test(error.message);
}

export class D1NotificationsStorage extends NotificationsStorage {
  readonly #db: SignalDatabase;
  readonly #table: string;
  readonly #sequenceTable: string;
  readonly #ordinalIndex: string;
  readonly #legacyReplaceTrigger: string;
  readonly #legacyInsertTrigger: string;
  #ready?: Promise<void>;

  constructor(db: SignalDatabase, tablePrefix = '') {
    super();
    const prefix = validateTablePrefix(tablePrefix) ?? '';
    this.#db = db;
    this.#table = `${prefix}mastra_notifications`;
    this.#sequenceTable = `${prefix}flowsafe_notification_sequence`;
    this.#ordinalIndex = `idx_${this.#table}_insertion_ordinal`;
    this.#legacyReplaceTrigger = `trg_${this.#table}_preserve_insertion_ordinal`;
    this.#legacyInsertTrigger = `trg_${this.#table}_allocate_insertion_ordinal`;
  }

  /**
   * Lazy, memoized schema creation — the same clear-on-failure promise memo the
   * approval store uses: only SUCCESS memoizes, so a transient DDL failure
   * retries on the next call rather than pinning the domain to a dead promise.
   * `init()` (called by the composite store) and every operation await it.
   */
  #ensureSchema(): Promise<void> {
    if (!this.#ready) {
      this.#ready = this.#createSchema().catch((error: unknown) => {
        this.#ready = undefined;
        throw error;
      });
    }
    return this.#ready;
  }

  async #createSchema(): Promise<void> {
    await this.#db
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
           insertionOrdinal INTEGER,
           PRIMARY KEY (thread_id, id)
         )`,
      )
      .run();
    await this.#migrateOrdinalSchema(true);
  }

  async #migrateOrdinalSchema(retryDuplicateColumn: boolean): Promise<void> {
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1NotificationsStorage requires database.batch() for atomic schema migration',
      );
    }
    const { results: columns } = await this.#db
      .prepare(`PRAGMA table_info(${this.#table})`)
      .all<{ name: string }>();
    const needsColumn = !columns.some(
      (column) => column.name === 'insertionOrdinal',
    );
    const statements: SignalStatement[] = [
      this.#db.prepare(
        `CREATE TABLE IF NOT EXISTS ${this.#sequenceTable} (
           id INTEGER PRIMARY KEY CHECK (id = 1),
           value INTEGER NOT NULL
         )`,
      ),
      ...(needsColumn
        ? [
            this.#db.prepare(
              `ALTER TABLE ${this.#table} ADD COLUMN insertionOrdinal INTEGER`,
            ),
          ]
        : []),
      this.#db.prepare(
        `INSERT INTO ${this.#sequenceTable} (id, value)
         SELECT 1, COALESCE(MAX(insertionOrdinal), 0)
         FROM ${this.#table}
         WHERE true
         ON CONFLICT(id) DO UPDATE SET
           value = MAX(value, excluded.value)`,
      ),
      // Assign pre-trigger legacy rows ordinals after any values already
      // written by a newer binary. rowid is their only insertion-order
      // evidence.
      this.#db.prepare(
        `WITH ranked AS (
           SELECT
             rowid AS targetRowid,
             COALESCE(
               (SELECT MAX(insertionOrdinal) FROM ${this.#table}),
               0
             ) + ROW_NUMBER() OVER (ORDER BY rowid) AS ordinal
           FROM ${this.#table}
           WHERE insertionOrdinal IS NULL
         )
         UPDATE ${this.#table}
         SET insertionOrdinal = (
           SELECT ordinal
           FROM ranked
           WHERE ranked.targetRowid = ${this.#table}.rowid
         )
         WHERE insertionOrdinal IS NULL`,
      ),
      this.#db.prepare(
        `INSERT INTO ${this.#sequenceTable} (id, value)
         SELECT 1, COALESCE(MAX(insertionOrdinal), 0)
         FROM ${this.#table}
         WHERE true
         ON CONFLICT(id) DO UPDATE SET
           value = MAX(value, excluded.value)`,
      ),
      // Keep a rollback-era writer (which omits insertionOrdinal) in the same
      // ordering protocol. An existing key is updated in place, matching
      // Map.set; a new key atomically claims the next sequence value.
      this.#db.prepare(
        `CREATE TRIGGER IF NOT EXISTS ${this.#legacyReplaceTrigger}
         BEFORE INSERT ON ${this.#table}
         WHEN NEW.insertionOrdinal IS NULL
           AND EXISTS (
             SELECT 1 FROM ${this.#table}
             WHERE thread_id = NEW.thread_id AND id = NEW.id
           )
         BEGIN
           UPDATE ${this.#table}
           SET ${NOTIFICATION_UPDATE_COLUMNS.map(
             (column) => `${column} = NEW.${column}`,
           ).join(', ')}
           WHERE thread_id = NEW.thread_id AND id = NEW.id;
           SELECT RAISE(IGNORE);
         END`,
      ),
      this.#db.prepare(
        `CREATE TRIGGER IF NOT EXISTS ${this.#legacyInsertTrigger}
         AFTER INSERT ON ${this.#table}
         WHEN NEW.insertionOrdinal IS NULL
         BEGIN
           UPDATE ${this.#sequenceTable}
           SET value = value + 1
           WHERE id = 1;
           UPDATE ${this.#table}
           SET insertionOrdinal = (
             SELECT value FROM ${this.#sequenceTable} WHERE id = 1
           )
           WHERE rowid = NEW.rowid;
         END`,
      ),
      this.#db.prepare(
        `CREATE UNIQUE INDEX IF NOT EXISTS ${this.#ordinalIndex}
         ON ${this.#table} (insertionOrdinal)
         WHERE insertionOrdinal IS NOT NULL`,
      ),
      this.#db.prepare(
        `CREATE INDEX IF NOT EXISTS idx_${this.#table}_thread
         ON ${this.#table} (thread_id)`,
      ),
      this.#db.prepare(
        `CREATE INDEX IF NOT EXISTS idx_${this.#table}_status
         ON ${this.#table} (thread_id, status)`,
      ),
    ];
    try {
      await batch(statements);
    } catch (error) {
      // Two fresh isolates can both observe the legacy shape. D1 serializes
      // their batches; the loser re-reads the now-migrated schema rather than
      // treating the expected duplicate-column race as corruption.
      if (retryDuplicateColumn && needsColumn && isDuplicateColumn(error)) {
        await this.#migrateOrdinalSchema(false);
        return;
      }
      throw error;
    }
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
    const insertionOrdinal = await this.#allocateInsertionOrdinal();
    if (!input.dedupeKey && !input.coalesceKey) {
      await this.#insert(record, insertionOrdinal);
      return record;
    }

    for (let attempt = 0; attempt < 16; attempt += 1) {
      if (
        await this.#insertUnlessCoalescable(record, input, insertionOrdinal)
      ) {
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
      const sets = ['summary = ?', 'updatedAt = ?'];
      const binds: unknown[] = [input.summary, updatedAt.toISOString()];
      if (input.payload != null) {
        sets.push('payload = ?');
        binds.push(jsonOrNull(input.payload));
      }
      if (input.priority !== undefined) {
        sets.push('priority = ?');
        binds.push(input.priority);
      }
      if (input.attributes) {
        sets.push('attributes = ?');
        binds.push(jsonOrNull(mergedAttributes));
      }
      if (input.deliverAt !== undefined) {
        sets.push('deliverAt = ?');
        binds.push(isoOrNull(input.deliverAt));
      }
      if (input.summaryAt !== undefined) {
        sets.push('summaryAt = ?');
        binds.push(isoOrNull(input.summaryAt));
      }
      if (input.deliveryReason !== undefined) {
        sets.push('deliveryReason = ?');
        binds.push(input.deliveryReason);
      }
      if (input.metadata) {
        sets.push('metadata = ?');
        binds.push(jsonOrNull(mergedMetadata));
      }
      sets.push('coalescedCount = coalescedCount + 1');
      const match = this.#coalescableMatch(input);
      const guards = ['id = ?', 'coalescedCount = ?', `(${match.clause})`];
      binds.push(existing.id, existing.coalescedCount, ...match.binds);
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
   * Due pending notifications across all deployment threads, filtered only by
   * the optional agentId/resourceId, exactly like core's InMemory reference. It
   * is a trusted-computing-base-only read (no client route reaches it). The
   * dispatcher validates each row's thread/resource binding before addressing a
   * Durable Object.
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
   * Core's InMemory `.find` walks Map insertion order. insertionOrdinal
   * preserves that order independently of caller-controlled createdAt; the
   * nullable rowid fallback keeps legacy writers readable during rollback.
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
         ORDER BY
           insertionOrdinal IS NULL ASC,
           insertionOrdinal ASC,
           rowid ASC
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
    insertionOrdinal: number,
  ): Promise<boolean> {
    const match = this.#coalescableMatch(input);
    const result = await this.#db
      .prepare(
        `INSERT INTO ${this.#table} (${NOTIFICATION_COLUMNS.join(', ')}, insertionOrdinal)
         SELECT ${NOTIFICATION_COLUMNS.map(() => '?').join(', ')}, ?
         WHERE NOT EXISTS (
           SELECT 1 FROM ${this.#table} WHERE ${match.clause}
         )
         ON CONFLICT(thread_id, id) DO UPDATE SET
           ${NOTIFICATION_UPDATE_COLUMNS.map(
             (column) => `${column} = excluded.${column}`,
           ).join(', ')}`,
      )
      .bind(...recordValues(record), insertionOrdinal, ...match.binds)
      .run();
    return d1Changes(result) > 0;
  }

  /** Upsert the public record while preserving its original insertion ordinal. */
  async #insert(
    record: NotificationRecord,
    insertionOrdinal: number,
  ): Promise<void> {
    await this.#db
      .prepare(
        `INSERT INTO ${this.#table} (${NOTIFICATION_COLUMNS.join(', ')}, insertionOrdinal)
         VALUES (${NOTIFICATION_COLUMNS.map(() => '?').join(', ')}, ?)
         ON CONFLICT(thread_id, id) DO UPDATE SET
           ${NOTIFICATION_UPDATE_COLUMNS.map(
             (column) => `${column} = excluded.${column}`,
           ).join(', ')}`,
      )
      .bind(...recordValues(record), insertionOrdinal)
      .run();
  }

  async #allocateInsertionOrdinal(): Promise<number> {
    const row = await this.#db
      .prepare(
        `UPDATE ${this.#sequenceTable}
         SET value = value + 1
         WHERE id = 1
         RETURNING value`,
      )
      .first<{ value: number }>();
    if (!row || !Number.isSafeInteger(row.value) || row.value < 1) {
      throw new Error('Notification insertion ordinal allocation failed');
    }
    return row.value;
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
