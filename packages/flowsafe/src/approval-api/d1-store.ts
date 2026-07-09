// D1-backed ApprovalStore. The CAS in transition() is a status-guarded
// conditional UPDATE ... RETURNING — SQLite executes it atomically, so two
// racing transitions resolve to one winner (the loser's guard matches zero
// rows and it gets null). Open-request uniqueness is a partial unique index,
// so duplicate creates collapse to the existing open record even across
// concurrent Workers.

import {
  type ApprovalPatch,
  type ApprovalStore,
  type CreateResult,
  stepKeyOf,
} from './store.js';
import {
  type ApprovalListFilter,
  type ApprovalRecord,
  type ApprovalStatus,
  OPEN_STATUSES,
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

// Partial unique index = the open-uniqueness invariant. Decided rows fall out
// of the index, so a re-suspension of the same step can open a fresh request.
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
  `CREATE UNIQUE INDEX IF NOT EXISTS ${TABLE}_open_step
    ON ${TABLE} (workflow_id, run_id, step_key)
    WHERE status IN ('pending', 'claimed', 'escalated')`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_status ON ${TABLE} (status)`,
  `CREATE INDEX IF NOT EXISTS ${TABLE}_run ON ${TABLE} (workflow_id, run_id)`,
];

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

function rowToRecord(row: ApprovalRow): ApprovalRecord {
  const record: ApprovalRecord = {
    id: row.id,
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

export class D1ApprovalStore implements ApprovalStore {
  readonly #db: ApprovalDatabase;
  #schemaReady?: Promise<void>;

  constructor(db: ApprovalDatabase) {
    this.#db = db;
  }

  async create(record: ApprovalRecord): Promise<CreateResult> {
    await this.#ready();
    // Clone BEFORE the INSERT: a non-cloneable payload must fail the call
    // without persisting anything (the in-memory store fails at the same
    // point), never orphan a row behind a thrown create.
    const snapshot = structuredClone(record);
    try {
      await this.#db
        .prepare(
          `INSERT INTO ${TABLE} (
            id, workflow_id, run_id, step_key, step_path, suspended_at,
            resumed_at, resume_count, run_scoped, title, summary, payload,
            connectors, priority, status, requested_by, claimed_by, decided_by,
            decision, comment, delegated_to, created_at, updated_at, claimed_at,
            decided_at, escalated_at, sla_deadline_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(
          record.id,
          record.workflowId,
          record.runId,
          stepKeyOf(record.stepPath),
          record.stepPath ? JSON.stringify(record.stepPath) : null,
          record.suspendedAt ?? null,
          record.resumedAt ?? null,
          record.resumeCount ?? null,
          record.runScoped === undefined ? null : record.runScoped ? 1 : 0,
          record.title,
          record.summary ?? null,
          record.payload === undefined ? null : JSON.stringify(record.payload),
          JSON.stringify(record.connectors),
          record.priority,
          record.status,
          record.requestedBy ?? null,
          record.claimedBy ?? null,
          record.decidedBy ?? null,
          record.decision ?? null,
          record.comment ?? null,
          record.delegatedTo ?? null,
          record.createdAt,
          record.updatedAt,
          record.claimedAt ?? null,
          record.decidedAt ?? null,
          record.escalatedAt ?? null,
          record.slaDeadlineAt ?? null,
        )
        .run();
    } catch (error) {
      if (isUniqueViolation(error)) {
        // Could be the open-uniqueness index (expected: return the existing
        // open record) or the id primary key (caller bug: rethrow).
        const open = await this.#openFor(
          record.workflowId,
          record.runId,
          stepKeyOf(record.stepPath),
        );
        if (open) return { record: open, created: false };
      }
      throw error;
    }
    return { record: snapshot, created: true };
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
    const clause = where.length > 0 ? ` WHERE ${where.join(' AND ')}` : '';
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${TABLE}${clause} ORDER BY created_at ASC, id ASC`,
      )
      .bind(...values)
      .all<ApprovalRow>();
    return results.map(rowToRecord);
  }

  async transition(
    id: string,
    from: readonly ApprovalStatus[],
    patch: ApprovalPatch,
  ): Promise<ApprovalRecord | null> {
    if (from.length === 0) return null;
    await this.#ready();
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
    const row = await this.#db
      .prepare(
        `UPDATE ${TABLE} SET ${sets.join(', ')}
         WHERE id = ? AND status IN (${from.map(() => '?').join(', ')})
         RETURNING *`,
      )
      .bind(...values, id, ...from)
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
         WHERE workflow_id = ? AND run_id = ? AND step_key = ?
           AND status IN (${OPEN_STATUSES.map(() => '?').join(', ')})
         LIMIT 1`,
      )
      .bind(workflowId, runId, stepKey, ...OPEN_STATUSES)
      .first<ApprovalRow>();
    return row ? rowToRecord(row) : null;
  }

  // Lazy, memoized schema creation; a failed attempt clears the memo so the
  // next call retries instead of pinning the store to a dead promise.
  #ready(): Promise<void> {
    this.#schemaReady ??= this.#createSchema().catch((error: unknown) => {
      this.#schemaReady = undefined;
      throw error;
    });
    return this.#schemaReady;
  }

  async #createSchema(): Promise<void> {
    for (const statement of SCHEMA_STATEMENTS) {
      await this.#db.prepare(statement).run();
    }
    // Pre-existing databases (.wrangler/ spike state, or an earlier release)
    // predate suspended_at, resumed_at, resume_count, and/or run_scoped, and
    // there is no migration machinery: CREATE IF NOT EXISTS skips the table, so
    // backfill each missing column in place. A DB from an earlier release has
    // the older columns but not the newer ones — the per-column loop upgrades
    // it. Only the duplicate-column error (the table already has the column) is
    // swallowed; anything else propagates. The column names are fixed
    // literals, so the interpolation carries no injection surface. Upgrade
    // window: an approval decided under a pre-resume_count schema and bound to
    // a re-suspension reads back with resume_count NULL, so the tightened
    // (suspendedAt, resumeCount) pair binding re-denies it fail-closed — the
    // run stays suspended and a fresh approval for the current suspension mints
    // correctly (grants.ts). A pre-resume_count first-suspension approval still
    // mints (resume_count NULL matches a first-suspension leg's undefined). A
    // pre-run_scoped step-less record backfills to NULL and therefore mints
    // NOTHING — the same fail-closed direction: run-scope is now explicit, so a
    // record that never named it never had it.
    for (const column of [
      'suspended_at',
      'resumed_at',
      'resume_count',
      'run_scoped',
    ]) {
      try {
        await this.#db
          .prepare(`ALTER TABLE ${TABLE} ADD COLUMN ${column} INTEGER`)
          .run();
      } catch (error) {
        if (!isDuplicateColumn(error)) throw error;
      }
    }
  }
}

function isDuplicateColumn(error: unknown): boolean {
  return error instanceof Error && /duplicate column/i.test(error.message);
}
