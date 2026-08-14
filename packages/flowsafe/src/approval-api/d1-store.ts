// SPDX-License-Identifier: Apache-2.0
// D1-backed ApprovalStore. The CAS in transition() is a status-guarded
// conditional UPDATE ... RETURNING — SQLite executes it atomically, so two
// racing transitions resolve to one winner (the loser's guard matches zero
// rows and it gets null). Open-request uniqueness is a partial unique index,
// so duplicate creates collapse to the existing open record even across
// concurrent Workers.

import { d1Changes } from '../do-runner/d1-storage.js';
import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from './principal.js';
import {
  type ApprovalPatch,
  type ApprovalStore,
  type ApprovalTransitionOptions,
  type CreateResult,
  stepKeyOf,
} from './store.js';
import {
  type ApprovalListFilter,
  type ApprovalMetrics,
  type ApprovalRecord,
  type ApprovalStatus,
  approvalListOrder,
  canonicalApprovalResumeTarget,
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

// The partial index preserves open-step uniqueness. Captured suspensions add a
// second, all-status fingerprint index after the legacy nullable columns are
// present: terminal records must block a stale reconciler filing the SAME
// suspension, while a later re-suspension changes suspended_at/resume_count.
//
// Index names change with the single-deployment schema. CREATE INDEX IF NOT
// EXISTS matches on name only, so retaining the pooled names would silently
// preserve tenant-keyed definitions on a reused database.
const SCHEMA_STATEMENTS: readonly string[] = [
  `CREATE TABLE IF NOT EXISTS ${TABLE} (
    id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_key TEXT NOT NULL DEFAULT '',
    step_path TEXT,
    suspended_at INTEGER,
    resumed_at INTEGER,
    resume_count INTEGER,
    run_scoped INTEGER,
    resume_target TEXT,
    title TEXT NOT NULL,
    summary TEXT,
    payload TEXT,
    connectors TEXT NOT NULL DEFAULT '[]',
    grant_scope TEXT,
    tool_call_id TEXT,
    priority TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by TEXT,
    requested_by_kind TEXT,
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
  `DROP INDEX IF EXISTS ${TABLE}_open_step_v2`,
  `CREATE UNIQUE INDEX IF NOT EXISTS ${TABLE}_open_step_v3
    ON ${TABLE} (workflow_id, run_id, step_key)
    WHERE status IN ('pending', 'claimed', 'escalated')`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_status ON ${TABLE} (status)`,
  `DROP INDEX IF EXISTS ${TABLE}_run`,
  `DROP INDEX IF EXISTS ${TABLE}_run_v2`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_run_v3
    ON ${TABLE} (workflow_id, run_id)`,
];

// First suspensions have a NULL resume_count; SQLite UNIQUE indexes otherwise
// treat every NULL as distinct, so normalize it to text outside the number
// domain. Scope this to captured step suspensions only: legacy/unbound records
// keep the original open-only create semantics. This statement deliberately
// runs after the additive ALTER loop. If a legacy database already contains
// duplicate fingerprints, index creation fails closed instead of choosing one
// audit record silently.
const FINGERPRINT_INDEX = `CREATE UNIQUE INDEX IF NOT EXISTS ${TABLE}_fingerprint_v2
  ON ${TABLE} (
    workflow_id, run_id, step_key, suspended_at,
    COALESCE(resume_count, 'none')
  )
  WHERE step_path IS NOT NULL AND suspended_at IS NOT NULL`;

interface ApprovalRow {
  id: string;
  workflow_id: string;
  run_id: string;
  step_key: string;
  step_path: string | null;
  suspended_at: number | null;
  resumed_at: number | null;
  resume_count: number | null;
  /** SQLite has no boolean: 1 = run-scoped standing grant, 0/NULL = not. */
  run_scoped: number | null;
  resume_target: string | null;
  title: string;
  summary: string | null;
  payload: string | null;
  connectors: string;
  grant_scope: string | null;
  tool_call_id: string | null;
  priority: string;
  status: string;
  requested_by: string | null;
  requested_by_kind: string | null;
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
 * The optional WHERE clauses of list().
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
 * One aggregate query, computed once per call, instead of loading every
 * record into JavaScript just to count. Must match
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
  FROM ${TABLE}`;

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

/** The `SET col = ?` fragments + bound values of a CAS patch. */
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
  let connectors: unknown;
  try {
    connectors = JSON.parse(row.connectors);
  } catch {
    throw new Error(`approval '${row.id}' has invalid connectors JSON`);
  }
  if (
    !Array.isArray(connectors) ||
    !connectors.every(
      (connector): connector is string =>
        typeof connector === 'string' && connector.length > 0,
    )
  ) {
    throw new Error(`approval '${row.id}' has invalid connectors`);
  }
  const record: ApprovalRecord = {
    id: row.id,
    workflowId: row.workflow_id,
    runId: row.run_id,
    title: row.title,
    connectors,
    priority: row.priority as ApprovalRecord['priority'],
    status: row.status as ApprovalStatus,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
  if (
    row.grant_scope === 'tool-call' ||
    row.grant_scope === 'suspension' ||
    row.grant_scope === 'run'
  ) {
    record.grantScope = row.grant_scope;
  } else if (row.grant_scope !== null && row.grant_scope !== undefined) {
    throw new Error(`approval '${row.id}' has invalid grant_scope`);
  }
  if (row.tool_call_id !== null && row.tool_call_id !== undefined) {
    record.toolCallId = row.tool_call_id;
  }
  if (row.step_path !== null)
    record.stepPath = JSON.parse(row.step_path) as string[];
  // The explicit null and undefined checks cover a pre-migration row read
  // before the ALTER backfilled the column as well as the stored NULL.
  if (row.suspended_at !== null && row.suspended_at !== undefined) {
    record.suspendedAt = row.suspended_at;
  }
  if (row.resumed_at !== null && row.resumed_at !== undefined) {
    record.resumedAt = row.resumed_at;
  }
  if (row.resume_count !== null && row.resume_count !== undefined) {
    record.resumeCount = row.resume_count;
  }
  if (row.run_scoped !== null && row.run_scoped !== undefined) {
    record.runScoped = row.run_scoped === 1;
  }
  if (row.resume_target !== null && row.resume_target !== undefined) {
    let target: unknown;
    try {
      target = JSON.parse(row.resume_target);
    } catch {
      throw new Error(`approval '${row.id}' has invalid resume_target JSON`);
    }
    // Rows written before execution principals stored an ApprovalActor here.
    // Canonical target parsing rejects that shape, which is the intended migration:
    // a run started by the schedule tick stored `role: 'operator'`, so reading
    // it back as a human principal would hand a scheduled job the authority of
    // a human operator. Such a row fails closed and its run cannot resume.
    const canonical = canonicalApprovalResumeTarget(target);
    if (!canonical) {
      throw new Error(`approval '${row.id}' has an invalid resume_target`);
    }
    record.resumeTarget = canonical;
  }
  if (row.summary !== null) record.summary = row.summary;
  if (row.payload !== null) record.payload = JSON.parse(row.payload);
  if (row.requested_by !== null && row.requested_by !== undefined) {
    if (!isExecutionPrincipalId(row.requested_by)) {
      throw new Error(`approval '${row.id}' has invalid requested_by`);
    }
    record.requestedBy = row.requested_by;
  }
  if (row.requested_by_kind !== null && row.requested_by_kind !== undefined) {
    if (row.requested_by === null || row.requested_by === undefined) {
      throw new Error(
        `approval '${row.id}' has requested_by_kind without requested_by`,
      );
    }
    if (!isExecutionPrincipalKind(row.requested_by_kind)) {
      throw new Error(`approval '${row.id}' has invalid requested_by_kind`);
    }
    record.requestedByKind = row.requested_by_kind;
  }
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

function isMissingTable(error: unknown, table: string): boolean {
  return (
    error instanceof Error &&
    new RegExp(`no such table:\\s*(?:main\\.)?${table}(?:\\b|:)`, 'i').test(
      error.message,
    )
  );
}

function runTerminalFence(
  table: string,
  workflowIdExpression: string,
  runIdExpression: string,
): string {
  return `NOT EXISTS (
    SELECT 1 FROM ${table} AS run
    WHERE run.workflow_name = ${workflowIdExpression}
      AND run.run_id = ${runIdExpression}
      AND CASE WHEN json_valid(run.snapshot) THEN
        (
          json_extract(run.snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent') IS NOT NULL
          OR json_extract(run.snapshot, '$.requestContext."flowsafe.runLifecycle".terminal') IS NOT NULL
        )
      ELSE 1 END
  )`;
}

export interface D1ApprovalStoreOptions {
  /**
   * Shared schema gate: the factory memoizes one schema-init promise per
   * isolate. Default: this instance memoizes its own.
   */
  ready?: () => Promise<void>;
  /** Existing Mastra snapshot table used to fence human decisions. */
  workflowSnapshotTable?: string;
}

/**
 * One-shot schema creation + fail-closed shape check. Exported for the
 * factory (which owns the cross-instance memo) and the schema tests.
 *
 * Refuses the legacy pooled table. Dropping tenant_id in place would require a
 * table rebuild and could preserve stale tenant-keyed indexes; pre-1.0
 * deployments must recreate the database during this breaking migration.
 */
export async function createApprovalSchema(
  db: ApprovalDatabase,
): Promise<void> {
  const existing = await db
    .prepare(`PRAGMA table_info(${TABLE})`)
    .all<{ name: string }>();
  const columns = new Set(existing.results.map((column) => column.name));
  if (columns.has('tenant_id')) {
    throw new Error(
      `${TABLE} uses the retired pooled-tenant schema — recreate the database before serving this single-deployment release`,
    );
  }
  for (const statement of SCHEMA_STATEMENTS) {
    await db.prepare(statement).run();
  }
  // Older single-deployment schemas backfill nullable fields in place.
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
  for (const column of ['grant_scope', 'tool_call_id', 'requested_by_kind']) {
    try {
      await db.prepare(`ALTER TABLE ${TABLE} ADD COLUMN ${column} TEXT`).run();
    } catch (error) {
      if (!isDuplicateColumn(error)) throw error;
    }
  }
  await db.prepare(`DROP INDEX IF EXISTS ${TABLE}_fingerprint_v1`).run();
  try {
    await db
      .prepare(`ALTER TABLE ${TABLE} ADD COLUMN resume_target TEXT`)
      .run();
  } catch (error) {
    if (!isDuplicateColumn(error)) throw error;
  }
  try {
    await db.prepare(FINGERPRINT_INDEX).run();
  } catch (error) {
    if (!isUniqueViolation(error)) throw error;
    throw new Error(
      `${TABLE} contains duplicate captured suspension fingerprints; repair the duplicate approval history before serving this database`,
      { cause: error },
    );
  }
}

export class D1ApprovalStore implements ApprovalStore {
  readonly #db: ApprovalDatabase;
  readonly #sharedReady?: () => Promise<void>;
  readonly #workflowSnapshotTable?: string;
  #schemaReady?: Promise<void>;

  constructor(db: ApprovalDatabase, options: D1ApprovalStoreOptions = {}) {
    this.#db = db;
    this.#sharedReady = options.ready;
    if (
      options.workflowSnapshotTable !== undefined &&
      !/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(options.workflowSnapshotTable)
    ) {
      throw new Error('workflowSnapshotTable must be a safe SQL identifier');
    }
    this.#workflowSnapshotTable = options.workflowSnapshotTable;
  }

  async create(record: ApprovalRecord): Promise<CreateResult> {
    await this.#ready();
    // Clone BEFORE the INSERT: a non-cloneable payload must fail the call
    // without persisting anything (the in-memory store fails at the same
    // point), never orphan a row behind a thrown create.
    const snapshot = structuredClone(record);
    const stepKey = stepKeyOf(snapshot.stepPath);
    const values = [
      snapshot.id,
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
      snapshot.resumeTarget === undefined
        ? null
        : JSON.stringify(snapshot.resumeTarget),
      snapshot.payload === undefined ? null : JSON.stringify(snapshot.payload),
      JSON.stringify(snapshot.connectors),
      snapshot.grantScope ?? null,
      snapshot.toolCallId ?? null,
      snapshot.priority,
      snapshot.status,
      snapshot.requestedBy ?? null,
      snapshot.requestedByKind ?? null,
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
    ];
    const insert = async (): Promise<unknown> => {
      const fenced = this.#workflowSnapshotTable !== undefined;
      const statement = this.#db
        .prepare(
          `INSERT INTO ${TABLE} (
            id, workflow_id, run_id, step_key, step_path,
            suspended_at, resumed_at, resume_count, run_scoped, title, summary,
            resume_target, payload, connectors, grant_scope, tool_call_id,
            priority, status, requested_by, requested_by_kind, claimed_by,
            decided_by, decision, comment, delegated_to, created_at, updated_at,
            claimed_at, decided_at, escalated_at, sla_deadline_at
          ) ${
            fenced
              ? `SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                 WHERE ${runTerminalFence(this.#workflowSnapshotTable as string, '?', '?')}`
              : 'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
          }`,
        )
        .bind(
          ...values,
          ...(fenced ? [snapshot.workflowId, snapshot.runId] : []),
        );
      let outcome: unknown;
      try {
        outcome = await statement.run();
      } catch (error) {
        if (
          !this.#workflowSnapshotTable ||
          !isMissingTable(error, this.#workflowSnapshotTable)
        ) {
          throw error;
        }
        outcome = await this.#db
          .prepare(
            `INSERT INTO ${TABLE} (
              id, workflow_id, run_id, step_key, step_path,
              suspended_at, resumed_at, resume_count, run_scoped, title, summary,
              resume_target, payload, connectors, grant_scope, tool_call_id,
              priority, status, requested_by, requested_by_kind, claimed_by,
              decided_by, decision, comment, delegated_to, created_at, updated_at,
              claimed_at, decided_at, escalated_at, sla_deadline_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          )
          .bind(...values)
          .run();
      }
      if (d1Changes(outcome) === 0) {
        throw new Error(
          `approval creation refused: run '${snapshot.runId}' is terminating or terminal`,
        );
      }
      return outcome;
    };

    // A UNIQUE collision can now be either the all-status exact fingerprint or
    // the open-step index. Resolve the exact fingerprint FIRST: a decision may
    // have landed after a reconciler's history read, and returning that terminal
    // record is the atomic no-refile verdict. Otherwise retain the bounded
    // decide-close retry for a different-fingerprint open row.
    try {
      await insert();
      return { record: snapshot, created: true };
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      const current = await this.#fingerprintFor(snapshot, stepKey);
      if (current) return { record: current, created: false };
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
        const current = await this.#fingerprintFor(snapshot, stepKey);
        if (current) return { record: current, created: false };
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
    const row = await this.#db
      .prepare(`SELECT * FROM ${TABLE} WHERE id = ?`)
      .bind(id)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  async list(filter: ApprovalListFilter = {}): Promise<ApprovalRecord[]> {
    await this.#ready();
    const where: string[] = [];
    const values: unknown[] = [];
    appendListFilters(filter, where, values);
    const clause = where.length > 0 ? ` WHERE ${where.join(' AND ')}` : '';
    // D3: default a bare list() to the max, so a repeated poll can never fall
    // back to an unbounded SELECT.
    const limit = clampApprovalLimit(filter.limit) ?? MAX_APPROVAL_LIST_LIMIT;
    const { results } = await this.#db
      .prepare(`SELECT * FROM ${TABLE}${clause} ${listOrderBy(filter)} LIMIT ?`)
      .bind(...values, limit)
      .all<ApprovalRow>();
    return results.map(rowToRecord);
  }

  async metrics(nowMs: number): Promise<ApprovalMetrics> {
    await this.#ready();
    const row = await this.#db
      .prepare(METRICS_QUERY)
      .bind(new Date(nowMs).toISOString())
      .first<ApprovalMetricsRow>();
    return rowToApprovalMetrics(row);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
    options: ApprovalTransitionOptions = {},
  ): Promise<ApprovalRecord | null> {
    if (from.length === 0) return null;
    await this.#ready();
    const { sets, values } = buildPatchSets(patch);
    const decisionFence =
      options.requireRunDecidable && this.#workflowSnapshotTable
        ? `
           AND ${runTerminalFence(
             this.#workflowSnapshotTable,
             `${TABLE}.workflow_id`,
             `${TABLE}.run_id`,
           )}`
        : '';
    const transition = (fence: string) =>
      this.#db
        .prepare(
          `UPDATE ${TABLE} SET ${sets.join(', ')}
           WHERE id = ? AND status IN (${from.map(() => '?').join(', ')})${fence}
           RETURNING *`,
        )
        .bind(...values, id, ...from)
        .first<ApprovalRow>();
    let row: ApprovalRow | null;
    try {
      row = await transition(decisionFence);
    } catch (error) {
      if (
        !decisionFence ||
        !this.#workflowSnapshotTable ||
        !isMissingTable(error, this.#workflowSnapshotTable)
      ) {
        throw error;
      }
      row = await transition('');
    }
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
         WHERE workflow_id = ? AND run_id = ? AND step_key = ?
           AND status IN (${OPEN_STATUSES.map(() => '?').join(', ')})
         LIMIT 1`,
      )
      .bind(workflowId, runId, stepKey, ...OPEN_STATUSES)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  async #fingerprintFor(
    record: ApprovalRecord,
    stepKey: string,
  ): Promise<ApprovalRecord | null> {
    if (record.stepPath === undefined || record.suspendedAt === undefined) {
      return null;
    }
    const row = await this.#db
      .prepare(
        `SELECT * FROM ${TABLE}
         WHERE workflow_id = ? AND run_id = ? AND step_key = ?
           AND step_path IS NOT NULL AND suspended_at = ? AND resume_count IS ?
         LIMIT 1`,
      )
      .bind(
        record.workflowId,
        record.runId,
        stepKey,
        record.suspendedAt,
        record.resumeCount ?? null,
      )
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

  async purgeExpired(cutoffIso: string, limit: number): Promise<number> {
    await this.#ready();
    const placeholders = TERMINAL_APPROVAL_STATUSES.map(() => '?').join(', ');
    const result = await this.#db
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
  }
}

function isDuplicateColumn(error: unknown): boolean {
  return error instanceof Error && /duplicate column/i.test(error.message);
}
