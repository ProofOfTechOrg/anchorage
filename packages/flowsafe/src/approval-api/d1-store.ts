// SPDX-License-Identifier: Apache-2.0
// D1-backed ApprovalStore. The CAS in transition() is a status-guarded
// conditional UPDATE ... RETURNING — SQLite executes it atomically, so two
// racing transitions resolve to one winner (the loser's guard matches zero
// rows and it gets null). Open-request uniqueness is a partial unique index,
// so duplicate creates collapse to the existing open record even across
// concurrent Workers.
//
// TENANT BINDING (INV-2): the store is bound to ONE tenant at construction
// and every SELECT/UPDATE carries `tenant_id = ?` sourced from that field —
// never from a request parameter. `tenantId` is deliberately NOT a member of
// ApprovalListFilter: an omissible tenant filter is the canonical fail-open
// (an empty filter would scan every tenant). Construct through
// D1ApprovalStoreFactory (tenant-store.ts) — this class is not exported from
// the package barrel.

import { d1Changes } from '../do-runner/d1-storage.js';
import { TENANT_ID_PATTERN } from '../do-runner/path-safe-id.js';
import {
  type ApprovalPatch,
  type ApprovalStore,
  type CreateResult,
  stepKeyOf,
} from './store.js';
import { type SystemApprovalStore, TENANT_BOUND } from './tenant-brand.js';
import {
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  approvalListOrder,
  clampApprovalLimit,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  parseApprovalTimeBound,
  TERMINAL_APPROVAL_STATUSES,
} from './types.js';

/**
 * The subset of D1Database this store uses, held structurally so tests can
 * back it with real SQLite (node:sqlite) and Workers pass env.DB directly.
 */
export interface ApprovalDatabase {
  prepare(query: string): ApprovalPreparedStatement;
}

export interface ApprovalPreparedStatement {
  bind(...values: unknown[]): ApprovalPreparedStatement;
  first<T = unknown>(): Promise<T | null>;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

const TABLE = 'flowsafe_approvals';

// Partial unique index = the open-uniqueness invariant, now tenant-first.
// Decided rows fall out of the index, so a re-suspension of the same step can
// open a fresh request.
//
// THE FATAL TRAP, spelled out: `CREATE UNIQUE INDEX IF NOT EXISTS` matches on
// NAME ONLY — redefining the old `flowsafe_approvals_open_step` with a
// leading tenant_id would be a SILENT NO-OP on any database that already has
// it, leaving open-uniqueness enforced WITHOUT the tenant dimension (tenant
// B's create would collapse into tenant A's open record). The migration must
// DROP the old index and create the new one under a NEW NAME, and the
// schema-upgrade test creates the OLD index first to prove the drop happens.
const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS ${TABLE} (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_key TEXT NOT NULL DEFAULT '',
    step_path TEXT,
    suspended_at INTEGER,
    resumed_at INTEGER,
    resume_count INTEGER,
    run_scoped INTEGER,
    title TEXT NOT NULL,
    summary TEXT,
    payload TEXT,
    connectors TEXT NOT NULL DEFAULT '[]',
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    claimed_by TEXT,
    decided_by TEXT,
    decision TEXT,
    comment TEXT,
    delegated_to TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_at TEXT,
    decided_at TEXT,
    escalated_at TEXT,
    sla_deadline_at TEXT
  )`,
  `DROP INDEX IF EXISTS ${TABLE}_open_step`,
  `CREATE UNIQUE INDEX IF NOT EXISTS ${TABLE}_open_step_v2
    ON ${TABLE} (tenant_id, workflow_id, run_id, step_key)
    WHERE status IN ('pending', 'claimed', 'escalated')`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_status ON ${TABLE} (status)`,
  `DROP INDEX IF EXISTS ${TABLE}_run`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_run_v2
    ON ${TABLE} (tenant_id, workflow_id, run_id)`,
];

interface ApprovalRow {
  id: string;
  tenant_id: string;
  workflow_id: string;
  run_id: string;
  step_key: string;
  step_path: string | null;
  suspended_at: number | null;
  resumed_at: number | null;
  resume_count: number | null;
  /** SQLite has no boolean: 1 = run-scoped standing grant, 0/NULL = not. */
  run_scoped: number | null;
  title: string;
  summary: string | null;
  payload: string | null;
  connectors: string;
  priority: string;
  status: string;
  requested_by: string | null;
  claimed_by: string | null;
  decided_by: string | null;
  decision: string | null;
  comment: string | null;
  delegated_to: string | null;
  created_at: string;
  updated_at: string;
  claimed_at: string | null;
  decided_at: string | null;
  escalated_at: string | null;
  sla_deadline_at: string | null;
}

const PATCH_COLUMNS: Record<keyof ApprovalPatch, string> = {
  status: 'status',
  claimedBy: 'claimed_by',
  decidedBy: 'decided_by',
  decision: 'decision',
  comment: 'comment',
  delegatedTo: 'delegated_to',
  updatedAt: 'updated_at',
  claimedAt: 'claimed_at',
  decidedAt: 'decided_at',
  escalatedAt: 'escalated_at',
};

/**
 * The optional WHERE clauses of list(), shared verbatim by the tenant-bound
 * store and the cron-only system view. The bound store SEEDS the arrays with
 * its unconditional `tenant_id = ?`; the system view seeds them empty. Two
 * hand-written copies would drift, and only one of them is on the request
 * path (the other is the sweep) — so a fix would silently miss it.
 */
function appendListFilters(
  filter: ApprovalListFilter,
  where: string[],
  values: unknown[],
): void {
  if (filter.status !== undefined) {
    const statuses = Array.isArray(filter.status)
      ? filter.status
      : [filter.status];
    where.push(`status IN (${statuses.map(() => '?').join(', ')})`);
    values.push(...statuses);
  }
  if (filter.workflowId !== undefined) {
    where.push('workflow_id = ?');
    values.push(filter.workflowId);
  }
  if (filter.runId !== undefined) {
    where.push('run_id = ?');
    values.push(filter.runId);
  }
  if (filter.claimedBy !== undefined) {
    where.push('claimed_by = ?');
    values.push(filter.claimedBy);
  }
  if (filter.requestedBy !== undefined) {
    where.push('requested_by = ?');
    values.push(filter.requestedBy);
  }
  // Time bounds bind CANONICALIZED to the exact 24-char toISOString() format
  // the column stores: SQLite TEXT comparison is bytewise, so a caller-
  // formatted variant ('…T10:00:00Z' vs '…T10:00:00.000Z') would misorder at
  // the boundary. parseApprovalTimeBound is the same throw-on-garbage gate
  // the in-memory matchesFilter applies — backend parity.
  if (filter.createdBefore !== undefined) {
    where.push('created_at < ?');
    values.push(
      new Date(
        parseApprovalTimeBound(filter.createdBefore, 'createdBefore'),
      ).toISOString(),
    );
  }
  if (filter.createdAfter !== undefined) {
    where.push('created_at > ?');
    values.push(
      new Date(
        parseApprovalTimeBound(filter.createdAfter, 'createdAfter'),
      ).toISOString(),
    );
  }
  if (filter.after !== undefined) {
    const cursor = parseApprovalCursor(filter.after);
    // Row-value comparison — SQLite supports `(a, b) > (c, d)` as a single
    // expression equivalent to `a > c OR (a = c AND b > d)`, matching
    // byQueueOrder's (createdAt, id) tie-break exactly.
    where.push('(created_at, id) > (?, ?)');
    values.push(cursor.createdAt, cursor.id);
  }
}

/**
 * The ORDER BY of list(), shared by both views like appendListFilters. The
 * reviewer branch must rank exactly like types.ts's byReviewerOrder — the
 * in-memory stores sort with that comparator and store.test.ts pins the
 * cross-backend parity: the priority CASE (an out-of-enum TEXT value lands
 * in the ELSE arm, after 'low'), NULL SLA deadlines last, then FIFO. TEXT
 * comparison on the fixed-format ISO-8601 columns is bytewise ==
 * chronological, matching the comparator's string compares. Resolving
 * through approvalListOrder also rejects the reviewer/after combination
 * before any SQL runs.
 */
function listOrderBy(filter: ApprovalListFilter): string {
  if (approvalListOrder(filter) === 'reviewer') {
    return `ORDER BY
      CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 WHEN 'low' THEN 3 ELSE 4 END ASC,
      CASE WHEN sla_deadline_at IS NULL THEN 1 ELSE 0 END ASC,
      sla_deadline_at ASC, created_at ASC, id ASC`;
  }
  return 'ORDER BY created_at ASC, id ASC';
}

/**
 * One aggregate query, computed once per call — the D3 replacement for
 * loading every record into JS just to count. Must match
 * store.ts's computeApprovalMetrics field-for-field (store.test.ts pins the
 * cross-backend parity, including the AVG-over-julianday vs JS Date-ms/1000
 * arithmetic landing on the same value for round-second fixtures). SUM over
 * zero matching rows is SQL NULL, not 0 — COALESCE every count; AVG's NULL
 * (no decided-with-decidedAt rows) maps straight to avgResolutionSeconds:
 * null, so it is deliberately NOT coalesced.
 */
const METRICS_QUERY = `SELECT
    COALESCE(SUM(CASE WHEN status IN ('pending','claimed','escalated') THEN 1 ELSE 0 END), 0) AS open_count,
    COALESCE(SUM(CASE WHEN status IN ('pending','claimed','escalated') AND sla_deadline_at IS NOT NULL AND sla_deadline_at <= ? THEN 1 ELSE 0 END), 0) AS sla_breached_count,
    COALESCE(SUM(CASE WHEN escalated_at IS NOT NULL THEN 1 ELSE 0 END), 0) AS escalation_count,
    COALESCE(SUM(CASE WHEN status IN ('approved','rejected') THEN 1 ELSE 0 END), 0) AS decided_count,
    COALESCE(SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END), 0) AS approved_count,
    COALESCE(SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END), 0) AS rejected_count,
    AVG(CASE
          WHEN status IN ('approved','rejected') AND decided_at IS NOT NULL
          THEN (julianday(decided_at) - julianday(created_at)) * 86400.0
        END) AS avg_resolution_seconds
  FROM ${TABLE}
  WHERE tenant_id = ?`;

interface ApprovalMetricsRow {
  open_count: number;
  sla_breached_count: number;
  escalation_count: number;
  decided_count: number;
  approved_count: number;
  rejected_count: number;
  avg_resolution_seconds: number | null;
}

function rowToApprovalMetrics(row: ApprovalMetricsRow | null): ApprovalMetrics {
  if (!row) {
    return {
      openCount: 0,
      slaBreachedCount: 0,
      escalationCount: 0,
      decidedCount: 0,
      approvedCount: 0,
      rejectedCount: 0,
      avgResolutionSeconds: null,
    };
  }
  return {
    openCount: row.open_count,
    slaBreachedCount: row.sla_breached_count,
    escalationCount: row.escalation_count,
    decidedCount: row.decided_count,
    approvedCount: row.approved_count,
    rejectedCount: row.rejected_count,
    avgResolutionSeconds: row.avg_resolution_seconds,
  };
}

/** The `SET col = ?` fragments + bound values of a CAS patch — both views. */
function buildPatchSets(patch: ApprovalPatch): {
  sets: string[];
  values: unknown[];
} {
  const sets: string[] = [];
  const values: unknown[] = [];
  for (const [field, column] of Object.entries(PATCH_COLUMNS) as Array<
    [keyof ApprovalPatch, string]
  >) {
    const value = patch[field];
    if (value !== undefined) {
      sets.push(`${column} = ?`);
      values.push(value);
    }
  }
  return { sets, values };
}

function rowToRecord(row: ApprovalRow): ApprovalRecord {
  const record: ApprovalRecord = {
    id: row.id,
    tenantId: row.tenant_id,
    workflowId: row.workflow_id,
    runId: row.run_id,
    title: row.title,
    connectors: JSON.parse(row.connectors) as string[],
    priority: row.priority as ApprovalRecord['priority'],
    status: row.status as ApprovalStatus,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
  if (row.step_path !== null)
    record.stepPath = JSON.parse(row.step_path) as string[];
  // != null covers a pre-migration row read before the ALTER backfilled the
  // column (undefined) as well as the stored NULL.
  if (row.suspended_at != null) record.suspendedAt = row.suspended_at;
  if (row.resumed_at != null) record.resumedAt = row.resumed_at;
  if (row.resume_count != null) record.resumeCount = row.resume_count;
  if (row.run_scoped != null) record.runScoped = row.run_scoped === 1;
  if (row.summary !== null) record.summary = row.summary;
  if (row.payload !== null) record.payload = JSON.parse(row.payload);
  if (row.requested_by !== null) record.requestedBy = row.requested_by;
  if (row.claimed_by !== null) record.claimedBy = row.claimed_by;
  if (row.decided_by !== null) record.decidedBy = row.decided_by;
  if (row.decision !== null)
    record.decision = row.decision as ApprovalRecord['decision'];
  if (row.comment !== null) record.comment = row.comment;
  if (row.delegated_to !== null) record.delegatedTo = row.delegated_to;
  if (row.claimed_at !== null) record.claimedAt = row.claimed_at;
  if (row.decided_at !== null) record.decidedAt = row.decided_at;
  if (row.escalated_at !== null) record.escalatedAt = row.escalated_at;
  if (row.sla_deadline_at !== null) record.slaDeadlineAt = row.sla_deadline_at;
  return record;
}

function isUniqueViolation(error: unknown): boolean {
  return (
    error instanceof Error && /UNIQUE constraint failed/i.test(error.message)
  );
}

export interface D1ApprovalStoreOptions {
  tenantId: string;
  /**
   * Shared schema gate: the factory memoizes ONE schema-init promise across
   * every per-request forTenant() store, so DDL runs once per isolate rather
   * than once per request. Default: this instance memoizes its own.
   */
  ready?: () => Promise<void>;
}

/**
 * One-shot schema creation + fail-closed shape check. Exported for the
 * factory (which owns the cross-instance memo) and the schema tests.
 *
 * REFUSES a pre-tenant table: `ALTER TABLE ... ADD COLUMN tenant_id TEXT NOT
 * NULL` without a default is rejected by SQLite even on an empty table, and a
 * NULL/'' tenant is an isolation hole — so there is deliberately NO backfill.
 * A database created before the tenant column must be recreated (nothing is
 * deployed; local `.wrangler/` state is deleted, not migrated).
 */
export async function createApprovalSchema(
  db: ApprovalDatabase,
): Promise<void> {
  const existing = await db
    .prepare(`PRAGMA table_info(${TABLE})`)
    .all<{ name: string }>();
  const columns = new Set(existing.results.map((column) => column.name));
  if (columns.size > 0 && !columns.has('tenant_id')) {
    throw new Error(
      `${TABLE} exists WITHOUT the tenant_id column — a tenant-less approvals table cannot be isolated and cannot be backfilled (SQLite rejects ADD COLUMN ... NOT NULL, and a NULL tenant is an isolation hole). Recreate the database: for local dev, delete the .wrangler state directory; nothing production is deployed.`,
    );
  }
  for (const statement of SCHEMA_STATEMENTS) {
    await db.prepare(statement).run();
  }
  // Post-tenant, pre-{suspended_at,resumed_at,resume_count,run_scoped}
  // databases backfill those nullable INTEGER columns in place (see the
  // upgrade-window notes in the git history); tenant_id can never join this
  // loop — it is TEXT NOT NULL and semantically non-backfillable.
  for (const column of [
    'suspended_at',
    'resumed_at',
    'resume_count',
    'run_scoped',
  ]) {
    try {
      await db
        .prepare(`ALTER TABLE ${TABLE} ADD COLUMN ${column} INTEGER`)
        .run();
    } catch (error) {
      if (!isDuplicateColumn(error)) throw error;
    }
  }
}

export class D1ApprovalStore implements ApprovalStore {
  readonly tenantId: string;
  readonly [TENANT_BOUND] = true as const;
  readonly #db: ApprovalDatabase;
  readonly #sharedReady?: () => Promise<void>;
  #schemaReady?: Promise<void>;

  constructor(db: ApprovalDatabase, options: D1ApprovalStoreOptions) {
    if (
      typeof options.tenantId !== 'string' ||
      !TENANT_ID_PATTERN.test(options.tenantId)
    ) {
      throw new Error(
        `D1ApprovalStore: tenantId '${options.tenantId}' violates INV-3 (^[a-z0-9]{3,32}$)`,
      );
    }
    this.#db = db;
    this.tenantId = options.tenantId;
    this.#sharedReady = options.ready;
  }

  async create(record: ApprovalRecord): Promise<CreateResult> {
    await this.#ready();
    // Stamp the tenant from the binding, never from the record: a field the
    // caller cannot control cannot be spoofed. Clone BEFORE the INSERT: a
    // non-cloneable payload must fail the call without persisting anything
    // (the in-memory store fails at the same point), never orphan a row
    // behind a thrown create.
    const snapshot = structuredClone({ ...record, tenantId: this.tenantId });
    const stepKey = stepKeyOf(snapshot.stepPath);
    const insert = (): Promise<unknown> =>
      this.#db
        .prepare(
          `INSERT INTO ${TABLE} (
            id, tenant_id, workflow_id, run_id, step_key, step_path,
            suspended_at, resumed_at, resume_count, run_scoped, title, summary,
            payload, connectors, priority, status, requested_by, claimed_by,
            decided_by, decision, comment, delegated_to, created_at, updated_at,
            claimed_at, decided_at, escalated_at, sla_deadline_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(
          snapshot.id,
          this.tenantId,
          snapshot.workflowId,
          snapshot.runId,
          stepKey,
          snapshot.stepPath ? JSON.stringify(snapshot.stepPath) : null,
          snapshot.suspendedAt ?? null,
          snapshot.resumedAt ?? null,
          snapshot.resumeCount ?? null,
          snapshot.runScoped === undefined ? null : snapshot.runScoped ? 1 : 0,
          snapshot.title,
          snapshot.summary ?? null,
          snapshot.payload === undefined
            ? null
            : JSON.stringify(snapshot.payload),
          JSON.stringify(snapshot.connectors),
          snapshot.priority,
          snapshot.status,
          snapshot.requestedBy ?? null,
          snapshot.claimedBy ?? null,
          snapshot.decidedBy ?? null,
          snapshot.decision ?? null,
          snapshot.comment ?? null,
          snapshot.delegatedTo ?? null,
          snapshot.createdAt,
          snapshot.updatedAt,
          snapshot.claimedAt ?? null,
          snapshot.decidedAt ?? null,
          snapshot.escalatedAt ?? null,
          snapshot.slaDeadlineAt ?? null,
        )
        .run();

    // The idempotent-create contract ("Decided requests never block a new
    // one") must survive a concurrent decide-close. service.create mints a
    // fresh crypto.randomUUID id, so the ONLY possible UNIQUE collision is the
    // partial open-step index — a violation means an OPEN row existed at INSERT
    // time. Between the failed INSERT and the #openFor SELECT (two D1
    // round-trips) a concurrent decide()/transition CAS can close that row (its
    // status leaves OPEN_STATUSES and the partial index), so #openFor returns
    // null and the step is insertable again: retry the INSERT exactly ONCE
    // (R-003 — bounded, never a loop). A still-open record or a non-unique
    // error short-circuits before any retry.
    try {
      await insert();
      return { record: snapshot, created: true };
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      const open = await this.#openFor(
        snapshot.workflowId,
        snapshot.runId,
        stepKey,
      );
      if (open) return { record: open, created: false };
      // open === null: the conflicting open row was decided-and-gone between
      // the INSERT and this SELECT — fall through to the single retry.
    }
    // The ONE bounded retry. A SECOND unique violation re-reads #openFor and
    // returns the now-open record if present, else rethrows — no third attempt.
    try {
      await insert();
      return { record: snapshot, created: true };
    } catch (error) {
      if (isUniqueViolation(error)) {
        const open = await this.#openFor(
          snapshot.workflowId,
          snapshot.runId,
          stepKey,
        );
        if (open) return { record: open, created: false };
      }
      throw error;
    }
  }

  async get(id: string): Promise<ApprovalRecord | null> {
    await this.#ready();
    // Wrong tenant == unknown id: no oracle for other tenants' record ids.
    const row = await this.#db
      .prepare(`SELECT * FROM ${TABLE} WHERE id = ? AND tenant_id = ?`)
      .bind(id, this.tenantId)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    await this.#ready();
    // Tenant FIRST, unconditionally, before every optional clause — the WHERE
    // is never empty, so an empty filter scans one tenant, not the table.
    const where: string[] = ['tenant_id = ?'];
    const values: unknown[] = [this.tenantId];
    appendListFilters(filter, where, values);
    // D3: default a tenant-bound bare list() to the max, so a repeated poll can
    // never fall back to an unbounded SELECT — always bounded now, unlike the
    // cron-only d1SystemApprovalStore.list below, which stays complete.
    const limit = clampApprovalLimit(filter.limit) ?? MAX_APPROVAL_LIST_LIMIT;
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${TABLE} WHERE ${where.join(' AND ')} ${listOrderBy(filter)} LIMIT ?`,
      )
      .bind(...values, limit)
      .all<ApprovalRow>();
    return results.map(rowToRecord);
  }

  async metrics(nowMs: number): Promise<ApprovalMetrics> {
    await this.#ready();
    const row = await this.#db
      .prepare(METRICS_QUERY)
      .bind(new Date(nowMs).toISOString(), this.tenantId)
      .first<ApprovalMetricsRow>();
    return rowToApprovalMetrics(row);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
  ): Promise<ApprovalRecord | null> {
    if (from.length === 0) return null;
    await this.#ready();
    const { sets, values } = buildPatchSets(patch);
    // A wrong tenant matches zero rows and reuses the existing loser-of-CAS
    // null path (404/409 upstream) — no new error branch, no oracle.
    const row = await this.#db
      .prepare(
        `UPDATE ${TABLE} SET ${sets.join(', ')}
         WHERE id = ? AND tenant_id = ? AND status IN (${from.map(() => '?').join(', ')})
         RETURNING *`,
      )
      .bind(...values, id, this.tenantId, ...from)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  async #openFor(
    workflowId: string,
    runId: string,
    stepKey: string,
  ): Promise<ApprovalRecord | null> {
    const row = await this.#db
      .prepare(
        `SELECT * FROM ${TABLE}
         WHERE tenant_id = ? AND workflow_id = ? AND run_id = ? AND step_key = ?
           AND status IN (${OPEN_STATUSES.map(() => '?').join(', ')})
         LIMIT 1`,
      )
      .bind(this.tenantId, workflowId, runId, stepKey, ...OPEN_STATUSES)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  // Lazy, memoized schema creation; a failed attempt clears the memo so the
  // next call retries instead of pinning the store to a dead promise. When
  // the factory injected a shared gate, defer to it (one DDL pass per
  // isolate, not per request-scoped store).
  #ready(): Promise<void> {
    if (this.#sharedReady) return this.#sharedReady();
    this.#schemaReady ??= createApprovalSchema(this.#db).catch(
      (error: unknown) => {
        this.#schemaReady = undefined;
        throw error;
      },
    );
    return this.#schemaReady;
  }
}

function isDuplicateColumn(error: unknown): boolean {
  return error instanceof Error && /duplicate column/i.test(error.message);
}

/** The cron-only cross-tenant view over the same table — see tenant-brand.ts. */
export function d1SystemApprovalStore(
  db: ApprovalDatabase,
  ready: () => Promise<void>,
): SystemApprovalStore {
  return {
    // The bound store's queries minus the tenant predicate — same builders,
    // so a filter/CAS fix cannot reach the request path and miss the sweep.
    async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
      await ready();
      const where: string[] = [];
      const values: unknown[] = [];
      appendListFilters(filter, where, values);
      const clause = where.length > 0 ? ` WHERE ${where.join(' AND ')}` : '';
      const limit = clampApprovalLimit(filter.limit);
      const { results } = await db
        .prepare(
          `SELECT * FROM ${TABLE}${clause} ${listOrderBy(filter)}${
            limit !== undefined ? ' LIMIT ?' : ''
          }`,
        )
        .bind(...values, ...(limit !== undefined ? [limit] : []))
        .all<ApprovalRow>();
      return results.map(rowToRecord);
    },
    async transition(
      id: string,
      from: readonly ApprovalStatus[],
      patch: ApprovalPatch,
    ): Promise<ApprovalRecord | null> {
      if (from.length === 0) return null;
      await ready();
      const { sets, values } = buildPatchSets(patch);
      const row = await db
        .prepare(
          `UPDATE ${TABLE} SET ${sets.join(', ')}
           WHERE id = ? AND status IN (${from.map(() => '?').join(', ')})
           RETURNING *`,
        )
        .bind(...values, id, ...from)
        .first<ApprovalRow>();
      return row ? rowToRecord(row) : null;
    },
    // Retention purge (D3/PART 4) — mirrors do-runner's
    // purgeExpiredWorkflowRuns row-only batching: one LIMIT-batched DELETE
    // per firing, no ORDER BY (the shrinking eligible set across firings is
    // the cursor, not which N rows one firing picks). COALESCE(decided_at,
    // updated_at) is the terminal-timestamp choice — see retention.ts.
    async purgeExpired(cutoffIso: string, limit: number): Promise<number> {
      await ready();
      const placeholders = TERMINAL_APPROVAL_STATUSES.map(() => '?').join(', ');
      const result = await db
        .prepare(
          `DELETE FROM ${TABLE}
           WHERE id IN (
             SELECT id FROM ${TABLE}
             WHERE status IN (${placeholders})
               AND COALESCE(decided_at, updated_at) < ?
             LIMIT ?
           )`,
        )
        .bind(...TERMINAL_APPROVAL_STATUSES, cutoffIso, limit)
        .run();
      return d1Changes(result);
    },
  };
}
