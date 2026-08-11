// SPDX-License-Identifier: Apache-2.0
// Track D (M-006), CI-M-006-001 — the D1 schedules storage domain over
// TABLE_SCHEDULES ('mastra_schedules') and TABLE_SCHEDULE_TRIGGERS
// ('mastra_schedule_triggers'), mirroring core's abstract SchedulesStorage +
// InMemorySchedulesStorage reference. @mastra/cloudflare-d1 1.1.1 ships NO
// schedules domain (only background-tasks/memory/scores/workflows), so — as with
// Track C's notifications/thread-state — this is hand-written to core's contract,
// NOT reimplementing something the adapter owns.
//
// Core's SchedulesStorage is ONE domain covering BOTH tables (createSchedule …
// recordTrigger/listTriggers all on one class), and its InMemory reference is a
// single class; `mastra.schedules` resolves it via getStore('schedules'). So this
// is ONE D1SchedulesStorage class managing both tables (registered under the
// 'schedules' domain key), a faithful mirror of that single-domain surface — not
// the two separate classes the milestone's Code Intent named (a two-class split
// would fork what core unifies).
//
// Schedule ids are server-minted by the facade. Core lists schedules with no
// pagination ("schedule counts are expected to stay small"), so the router's
// deployment count cap bounds this domain.
//
// TIMESTAMPS are INTEGER ms-epoch, not the ISO-8601 TEXT the notifications/thread
// domains use: core types `Schedule.nextFireAt` / `ScheduleTrigger.actualFireAt`
// as `number`, and `listDueSchedules(now: number)` compares against a numeric
// `now`, so INTEGER is the faithful encoding and numeric `<` is a correct
// timestamp comparison (the trigger TTL rides this, purgeExpiredScheduleTriggers).

import {
  normalizeScheduleTarget,
  type Schedule,
  type ScheduleFilter,
  SchedulesStorage,
  type ScheduleTrigger,
  type ScheduleTriggerListOptions,
  type ScheduleUpdate,
} from '@mastra/core/storage';
import {
  APPROVAL_ROLES,
  canonicalResourceOwner,
  createResourceOwnershipSchema,
  RESOURCE_OWNERSHIP_TABLE,
  type ResourceOwner,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';

import {
  d1Changes,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
  type SignalStatement,
} from '../signals/d1-shared.js';
import type { AuthorizedSchedule } from './target-policy.js';

// The D1 seam + column helpers are Track C's canonical shared leaf
// (signals/d1-shared.ts, zero imports) — reused here so the two domains cannot
// drift on the `{ meta: { changes } }` envelope or the JSON encodings. The
// schedules-facing names are kept as aliases for a stable public API.
/** The prepared-statement subset the schedules domain uses. */
export type ScheduleStatement = SignalStatement;
/** The D1 database subset the schedules domain uses (workers-types-free). */
export type ScheduleDatabase = SignalDatabase;

export interface ScheduleFireClaim {
  scheduleId: string;
  expectedNextFireAt: number;
  newNextFireAt: number;
  actualFireAt: number;
  runId: string;
  trigger: ScheduleTrigger & { id: string; outcome: 'deferred' };
}

export type ScheduleAgentDispatchAction =
  | 'wake'
  | 'deliver'
  | 'persist'
  | 'discard'
  | 'blocked';

const AGENT_DISPATCH_OUTCOME_BY_ACTION = {
  wake: 'succeeded',
  deliver: 'delivered',
  persist: 'persisted',
  discard: 'discarded',
  blocked: 'skipped',
} as const satisfies Record<
  ScheduleAgentDispatchAction,
  Extract<
    ScheduleTrigger['outcome'],
    'succeeded' | 'delivered' | 'persisted' | 'discarded' | 'skipped'
  >
>;

/** Target-side receipt for a threaded agent schedule signal. */
export type ScheduleAgentDispatchReceipt = (
  | { action: 'wake'; outcome: 'succeeded' }
  | { action: 'deliver'; outcome: 'delivered' }
  | { action: 'persist'; outcome: 'persisted' }
  | { action: 'discard'; outcome: 'discarded' }
  | { action: 'blocked'; outcome: 'skipped' }
) & {
  runId?: string;
  signalId?: string;
};

export type ScheduleAgentDispatchState =
  | { state: 'ready' }
  | { state: 'pending' }
  | { state: 'missing' }
  | { state: 'settled'; receipt: ScheduleAgentDispatchReceipt };

export function createScheduleAgentDispatchReceipt<
  Action extends ScheduleAgentDispatchAction,
>(
  action: Action,
  ids: { runId?: string; signalId?: string } = {},
): Extract<ScheduleAgentDispatchReceipt, { action: Action }> {
  return {
    action,
    outcome: AGENT_DISPATCH_OUTCOME_BY_ACTION[action],
    ...ids,
  } as Extract<ScheduleAgentDispatchReceipt, { action: Action }>;
}

export function parseScheduleAgentDispatchReceipt(
  value: unknown,
): ScheduleAgentDispatchReceipt | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return undefined;
  }
  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.action !== 'string' ||
    !Object.hasOwn(AGENT_DISPATCH_OUTCOME_BY_ACTION, candidate.action)
  ) {
    return undefined;
  }
  const action = candidate.action as ScheduleAgentDispatchAction;
  if (
    candidate.outcome !== AGENT_DISPATCH_OUTCOME_BY_ACTION[action] ||
    (candidate.runId !== undefined && !isPathSafeId(candidate.runId)) ||
    (candidate.signalId !== undefined && !isPathSafeId(candidate.signalId))
  ) {
    return undefined;
  }
  return createScheduleAgentDispatchReceipt(action, {
    ...(candidate.runId !== undefined ? { runId: candidate.runId } : {}),
    ...(candidate.signalId !== undefined
      ? { signalId: candidate.signalId }
      : {}),
  });
}

/** The raw row shape `mastra_schedules` stores. */
interface ScheduleRow {
  id: string;
  target: string;
  cron: string;
  timezone: string | null;
  status: string;
  nextFireAt: number;
  lastFireAt: number | null;
  lastRunId: string | null;
  createdAt: number;
  updatedAt: number;
  metadata: string | null;
  ownerType: string | null;
  ownerId: string | null;
  creatorRole: string | null;
  deletionRequestedAt: number | null;
}

/** The raw row shape `mastra_schedule_triggers` stores. */
interface ScheduleTriggerRow {
  id: string;
  scheduleId: string;
  runId: string | null;
  scheduledFireAt: number;
  actualFireAt: number;
  outcome: string;
  error: string | null;
  triggerKind: string | null;
  parentTriggerId: string | null;
  metadata: string | null;
}

function rowToSchedule(row: ScheduleRow): Schedule {
  const target = parseJsonOrUndefined<Schedule['target']>(row.target);
  const schedule: Schedule = {
    id: row.id,
    // Legacy read-shim: base.d.ts mandates every SchedulesStorage run row
    // targets through normalizeScheduleTarget at deserialization so a legacy
    // `heartbeat` discriminator keeps dispatching as `agent`.
    target: target
      ? normalizeScheduleTarget(target)
      : ({ type: 'workflow', workflowId: '' } as Schedule['target']),
    cron: row.cron,
    ...(row.timezone !== null ? { timezone: row.timezone } : {}),
    status: row.status as Schedule['status'],
    nextFireAt: row.nextFireAt,
    ...(row.lastFireAt !== null ? { lastFireAt: row.lastFireAt } : {}),
    ...(row.lastRunId !== null ? { lastRunId: row.lastRunId } : {}),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    ...(row.metadata !== null
      ? {
          metadata: parseJsonOrUndefined<Record<string, unknown>>(row.metadata),
        }
      : {}),
    ...(row.ownerType !== null
      ? { ownerType: row.ownerType as Schedule['ownerType'] }
      : {}),
    ...(row.ownerId !== null ? { ownerId: row.ownerId } : {}),
  };
  return row.creatorRole !== null &&
    (APPROVAL_ROLES as readonly string[]).includes(row.creatorRole)
    ? ({ ...schedule, creatorRole: row.creatorRole } as AuthorizedSchedule)
    : schedule;
}

function rowToTrigger(row: ScheduleTriggerRow): ScheduleTrigger {
  return {
    id: row.id,
    scheduleId: row.scheduleId,
    runId: row.runId,
    scheduledFireAt: row.scheduledFireAt,
    actualFireAt: row.actualFireAt,
    outcome: row.outcome as ScheduleTrigger['outcome'],
    ...(row.error !== null ? { error: row.error } : {}),
    triggerKind:
      row.triggerKind === null
        ? 'schedule-fire'
        : (row.triggerKind as ScheduleTrigger['triggerKind']),
    ...(row.parentTriggerId !== null
      ? { parentTriggerId: row.parentTriggerId }
      : {}),
    ...(row.metadata !== null
      ? {
          metadata: parseJsonOrUndefined<Record<string, unknown>>(row.metadata),
        }
      : {}),
  };
}

/**
 * The D1 schedules domain: workflow + agent schedule rows and their trigger
 * history. Mirrors core's `SchedulesStorage` contract exactly (the abstract
 * methods `mastra.schedules` drives + the scheduler tick reads), so a host that
 * composes it into `createD1Storage({ domains })` gets D1-durable schedules with
 * no adapter change.
 */
export class D1SchedulesStorage extends SchedulesStorage {
  readonly #db: ScheduleDatabase;
  readonly #schedules: string;
  readonly #triggers: string;
  #ready?: Promise<void>;

  constructor(db: ScheduleDatabase, tablePrefix = '') {
    super();
    this.#db = db;
    this.#schedules = `${tablePrefix}mastra_schedules`;
    this.#triggers = `${tablePrefix}mastra_schedule_triggers`;
  }

  /**
   * Lazy, memoized schema creation — the same clear-on-failure promise memo the
   * approval store and the signal domains use: only SUCCESS memoizes, so a
   * transient DDL failure retries on the next call rather than pinning the
   * domain to a dead promise. `init()` (the composite store) and every operation
   * await it.
   */
  #ensureSchema(): Promise<void> {
    if (!this.#ready) {
      this.#ready = Promise.resolve(
        this.#db
          .prepare(
            `CREATE TABLE IF NOT EXISTS ${this.#schedules} (
               id TEXT PRIMARY KEY,
               target TEXT NOT NULL,
               cron TEXT NOT NULL,
               timezone TEXT,
               status TEXT NOT NULL,
               nextFireAt INTEGER NOT NULL,
               lastFireAt INTEGER,
               lastRunId TEXT,
               createdAt INTEGER NOT NULL,
               updatedAt INTEGER NOT NULL,
               metadata TEXT,
               ownerType TEXT,
               ownerId TEXT,
               creatorRole TEXT CHECK (creatorRole IN ('admin', 'builder', 'operator', 'reviewer', 'viewer')),
               deletionRequestedAt INTEGER
             )`,
          )
          .run()
          .then(() => this.#ensureScheduleColumns())
          .then(() => createResourceOwnershipSchema(this.#db))
          .then(() =>
            this.#db
              .prepare(
                // listDueSchedules rides (status, nextFireAt).
                `CREATE INDEX IF NOT EXISTS idx_${this.#schedules}_due
                 ON ${this.#schedules} (status, nextFireAt)`,
              )
              .run(),
          )
          .then(() =>
            this.#db
              .prepare(
                `CREATE TABLE IF NOT EXISTS ${this.#triggers} (
                   id TEXT PRIMARY KEY,
                   scheduleId TEXT NOT NULL,
                   runId TEXT,
                   scheduledFireAt INTEGER NOT NULL,
                   actualFireAt INTEGER NOT NULL,
                   outcome TEXT NOT NULL,
                   error TEXT,
                   triggerKind TEXT,
                   parentTriggerId TEXT,
                   metadata TEXT
                 )`,
              )
              .run(),
          )
          .then(() =>
            this.#db
              .prepare(
                // listTriggers rides (scheduleId, actualFireAt DESC); the TTL
                // purge scans actualFireAt.
                `CREATE INDEX IF NOT EXISTS idx_${this.#triggers}_schedule
                 ON ${this.#triggers} (scheduleId, actualFireAt)`,
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

  async #ensureScheduleColumns(): Promise<void> {
    const { results } = await this.#db
      .prepare(`PRAGMA table_info(${this.#schedules})`)
      .all<{ name: string }>();
    const names = new Set(results.map((column) => column.name));
    if (!names.has('creatorRole')) {
      await this.#db
        .prepare(
          `ALTER TABLE ${this.#schedules}
           ADD COLUMN creatorRole TEXT
           CHECK (creatorRole IN ('admin', 'builder', 'operator', 'reviewer', 'viewer'))`,
        )
        .run();
    }
    if (!names.has('deletionRequestedAt')) {
      await this.#db
        .prepare(
          `ALTER TABLE ${this.#schedules}
           ADD COLUMN deletionRequestedAt INTEGER`,
        )
        .run();
    }
  }

  async init(): Promise<void> {
    await this.#ensureSchema();
  }

  async createSchedule(schedule: Schedule): Promise<Schedule> {
    await this.#ensureSchema();
    try {
      await this.#insertSchedule(schedule);
    } catch (error) {
      if (String(error).includes('UNIQUE constraint failed')) {
        throw new Error(`schedule ${schedule.id} already exists`);
      }
      throw error;
    }
    return schedule;
  }

  /**
   * Flowsafe facade create: schedule row, owner row, and the deployment count
   * cap share one D1 transaction. The schedules domain and resource registry
   * must therefore use the same binding.
   */
  async createOwnedSchedule(
    schedule: AuthorizedSchedule,
    owner: ResourceOwner,
    maxSchedules: number,
  ): Promise<Schedule | null> {
    const safeOwner = canonicalResourceOwner(owner);
    if (!Number.isSafeInteger(maxSchedules) || maxSchedules < 0) {
      throw new Error('maxSchedules must be a nonnegative safe integer');
    }
    await this.#ensureSchema();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for atomic owned schedule creation',
      );
    }
    const [created] = await batch([
      this.#insertScheduleStatement(schedule, maxSchedules),
      this.#db
        .prepare(
          `INSERT INTO ${RESOURCE_OWNERSHIP_TABLE}
             (resource_kind, resource_id, owner_kind, owner_id)
           SELECT 'schedule', ?, ?, ?
           WHERE changes() = 1
             AND EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id = ?)`,
        )
        .bind(schedule.id, safeOwner.kind, safeOwner.id, schedule.id),
    ]);
    return d1Changes(created as { meta?: { changes?: number } }) === 1
      ? schedule
      : null;
  }

  async getSchedule(id: string): Promise<Schedule | null> {
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(
        `SELECT * FROM ${this.#schedules}
         WHERE id = ? AND deletionRequestedAt IS NULL`,
      )
      .bind(id)
      .first<ScheduleRow>();
    return row ? rowToSchedule(row) : null;
  }

  async listSchedules(filter?: ScheduleFilter): Promise<Schedule[]> {
    await this.#ensureSchema();
    const clauses = ['deletionRequestedAt IS NULL'];
    const binds: unknown[] = [];
    if (filter?.status !== undefined) {
      clauses.push('status = ?');
      binds.push(filter.status);
    }
    if (filter?.ownerType !== undefined) {
      if (filter.ownerType === null) {
        clauses.push('ownerType IS NULL');
      } else {
        clauses.push('ownerType = ?');
        binds.push(filter.ownerType);
      }
    }
    if (filter?.ownerId !== undefined) {
      if (filter.ownerId === null) {
        clauses.push('ownerId IS NULL');
      } else {
        clauses.push('ownerId = ?');
        binds.push(filter.ownerId);
      }
    }
    const where = clauses.length > 0 ? ` WHERE ${clauses.join(' AND ')}` : '';
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#schedules}${where} ORDER BY createdAt ASC`,
      )
      .bind(...binds)
      .all<ScheduleRow>();
    let schedules = results.map(rowToSchedule);
    // `workflowId` filters the target, held in the JSON column, so it is applied
    // in JS (the candidate set is small — core lists without pagination).
    if (filter?.workflowId !== undefined) {
      schedules = schedules.filter(
        (schedule) =>
          schedule.target.type === 'workflow' &&
          schedule.target.workflowId === filter.workflowId,
      );
    }
    return schedules;
  }

  async listDueSchedules(now: number, limit?: number): Promise<Schedule[]> {
    await this.#ensureSchema();
    // nextFireAt <= now AND status active (core's contract). Numeric compare —
    // nextFireAt is INTEGER ms-epoch.
    const limitClause = limit !== undefined ? ' LIMIT ?' : '';
    const stmt = this.#db.prepare(
      `SELECT * FROM ${this.#schedules}
       WHERE status = 'active' AND deletionRequestedAt IS NULL
         AND nextFireAt <= ?
       ORDER BY nextFireAt ASC${limitClause}`,
    );
    const bound = limit !== undefined ? stmt.bind(now, limit) : stmt.bind(now);
    const { results } = await bound.all<ScheduleRow>();
    return results.map(rowToSchedule);
  }

  async updateSchedule(id: string, patch: ScheduleUpdate): Promise<Schedule> {
    await this.#ensureSchema();
    // A TARGETED UPDATE — set ONLY the patched columns, never a full-row rewrite.
    // A read-modify-write `INSERT OR REPLACE` of the whole row would carry a
    // stale `nextFireAt`/`lastFireAt`/`lastRunId` back over whatever the tick's
    // CAS (`updateScheduleNextFire`) advanced in the read→write window — reverting
    // a claimed fire and re-arming the schedule for a SECOND fire of the same
    // occurrence. This statement touches only what the patch names, so a facade
    // mutation racing a tick can never clobber the CAS-owned columns it did not
    // ask to change.
    const sets = ['updatedAt = ?'];
    const binds: unknown[] = [Date.now()];
    if (patch.cron !== undefined) {
      sets.push('cron = ?');
      binds.push(patch.cron);
    }
    if (patch.timezone !== undefined) {
      sets.push('timezone = ?');
      binds.push(patch.timezone ?? null);
    }
    if (patch.status !== undefined) {
      sets.push('status = ?');
      binds.push(patch.status);
    }
    if (patch.nextFireAt !== undefined) {
      sets.push('nextFireAt = ?');
      binds.push(patch.nextFireAt);
    }
    if (patch.metadata !== undefined) {
      sets.push('metadata = ?');
      binds.push(jsonOrNull(patch.metadata));
    }
    if (patch.target !== undefined) {
      sets.push('target = ?');
      binds.push(JSON.stringify(patch.target));
    }
    if (patch.ownerType !== undefined) {
      sets.push('ownerType = ?');
      binds.push(patch.ownerType ?? null);
    }
    if (patch.ownerId !== undefined) {
      sets.push('ownerId = ?');
      binds.push(patch.ownerId ?? null);
    }
    binds.push(id);
    await this.#db
      .prepare(
        `UPDATE ${this.#schedules} SET ${sets.join(', ')}
         WHERE id = ? AND deletionRequestedAt IS NULL`,
      )
      .bind(...binds)
      .run();
    const updated = await this.getSchedule(id);
    if (!updated) throw new Error(`schedule ${id} not found`);
    return updated;
  }

  async updateScheduleNextFire(
    id: string,
    expectedNextFireAt: number,
    newNextFireAt: number,
    lastFireAt: number,
    lastRunId: string,
  ): Promise<boolean> {
    await this.#ensureSchema();
    // The CAS: advance only if nextFireAt still equals what the caller read AND
    // the row is still ACTIVE. Two concurrent ticks over one due schedule both
    // read nextFireAt = T; the first UPDATE advances it to T2 (changes = 1 ->
    // true), the second's `nextFireAt = T` no longer matches (changes = 0 ->
    // false). This serializes one claim under concurrent ticks on real workerd +
    // D1 (spike D-S1); it does not claim end-to-end exactly-once dispatch. The
    // `status = 'active'` guard closes the pause race: a
    // schedule paused AFTER a tick read it as due but BEFORE this claim (status
    // flips, nextFireAt unchanged) fails the CAS here, so a just-paused schedule
    // does not fire one last time.
    const result = await this.#db
      .prepare(
        `UPDATE ${this.#schedules}
         SET nextFireAt = ?, lastFireAt = ?, lastRunId = ?, updatedAt = ?
         WHERE id = ? AND nextFireAt = ? AND status = 'active'
           AND deletionRequestedAt IS NULL`,
      )
      .bind(
        newNextFireAt,
        lastFireAt,
        lastRunId,
        Date.now(),
        id,
        expectedNextFireAt,
      )
      .run();
    return d1Changes(result) === 1;
  }

  /** Atomically claim one due fire and persist its recoverable dispatch row. */
  async claimScheduleFire(claim: ScheduleFireClaim): Promise<boolean> {
    await this.#ensureSchema();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for atomic schedule fire claims',
      );
    }
    const { trigger } = claim;
    const [claimed] = await batch([
      this.#db
        .prepare(
          `UPDATE ${this.#schedules}
           SET nextFireAt = ?, lastFireAt = ?, lastRunId = ?, updatedAt = ?
           WHERE id = ? AND nextFireAt = ? AND status = 'active'
             AND deletionRequestedAt IS NULL`,
        )
        .bind(
          claim.newNextFireAt,
          claim.actualFireAt,
          claim.runId,
          Date.now(),
          claim.scheduleId,
          claim.expectedNextFireAt,
        ),
      this.#db
        .prepare(
          `INSERT INTO ${this.#triggers} (
             id, scheduleId, runId, scheduledFireAt, actualFireAt, outcome,
             error, triggerKind, parentTriggerId, metadata
           )
           SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           WHERE changes() = 1`,
        )
        .bind(
          trigger.id,
          trigger.scheduleId,
          trigger.runId,
          trigger.scheduledFireAt,
          trigger.actualFireAt,
          trigger.outcome,
          trigger.error ?? null,
          trigger.triggerKind ?? 'schedule-fire',
          trigger.parentTriggerId ?? null,
          jsonOrNull(trigger.metadata),
        ),
    ]);
    return d1Changes(claimed as { meta?: { changes?: number } }) === 1;
  }

  /**
   * Lease one target-side signal attempt. A retry waits while the lease is
   * live and replays a settled receipt. After an ambiguous crash, lease
   * takeover repeats the same stable dispatch id: delivery is deliberately
   * at-least-once because the agent action and D1 receipt cannot be committed
   * in one transaction.
   */
  async beginAgentScheduleDispatch(
    scheduleId: string,
    triggerId: string,
    now = Date.now(),
    leaseMs = 60_000,
  ): Promise<ScheduleAgentDispatchState> {
    await this.#ensureSchema();
    const claimed = await this.#db
      .prepare(
        `UPDATE ${this.#triggers}
         SET metadata = json_set(
           COALESCE(metadata, '{}'),
           '$.dispatchState', 'executing',
           '$.dispatchLeaseUntil', ?
         )
         WHERE id = ? AND scheduleId = ? AND outcome = 'deferred'
           AND (
             json_extract(metadata, '$.dispatchState') = 'prepared'
             OR (
               json_extract(metadata, '$.dispatchState') = 'executing'
               AND COALESCE(
                 json_extract(metadata, '$.dispatchLeaseUntil'), 0
               ) <= ?
             )
           )`,
      )
      .bind(now + leaseMs, triggerId, scheduleId, now)
      .run();
    if (d1Changes(claimed) === 1) return { state: 'ready' };
    return this.agentScheduleDispatchState(scheduleId, triggerId);
  }

  /** Read a threaded schedule signal receipt without changing dispatch state. */
  async agentScheduleDispatchState(
    scheduleId: string,
    triggerId: string,
  ): Promise<ScheduleAgentDispatchState> {
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(
        `SELECT metadata FROM ${this.#triggers}
         WHERE id = ? AND scheduleId = ? AND outcome = 'deferred'`,
      )
      .bind(triggerId, scheduleId)
      .first<{ metadata: string | null }>();
    if (!row) return { state: 'missing' };
    const metadata =
      row.metadata === null
        ? undefined
        : parseJsonOrUndefined<Record<string, unknown>>(row.metadata);
    const receipt = parseScheduleAgentDispatchReceipt(
      metadata?.dispatchReceipt,
    );
    return receipt ? { state: 'settled', receipt } : { state: 'pending' };
  }

  /** Persist the target decision before the thread DO returns it to the tick. */
  async settleAgentScheduleDispatch(
    scheduleId: string,
    triggerId: string,
    receipt: ScheduleAgentDispatchReceipt,
  ): Promise<void> {
    const canonical = parseScheduleAgentDispatchReceipt(receipt);
    if (!canonical)
      throw new TypeError('invalid agent schedule dispatch receipt');
    await this.#ensureSchema();
    const settled = await this.#db
      .prepare(
        `UPDATE ${this.#triggers}
         SET metadata = json_set(
           COALESCE(metadata, '{}'),
           '$.dispatchState', 'settled',
           '$.dispatchReceipt', json(?)
         )
         WHERE id = ? AND scheduleId = ? AND outcome = 'deferred'
           AND json_extract(metadata, '$.dispatchState') = 'executing'`,
      )
      .bind(JSON.stringify(canonical), triggerId, scheduleId)
      .run();
    if (d1Changes(settled) === 1) return;
    const current = await this.agentScheduleDispatchState(
      scheduleId,
      triggerId,
    );
    if (
      current.state === 'settled' &&
      JSON.stringify(current.receipt) === JSON.stringify(canonical)
    ) {
      return;
    }
    throw new Error('agent schedule dispatch receipt could not be persisted');
  }

  async deleteSchedule(id: string): Promise<void> {
    await this.deleteOwnedSchedule(id);
  }

  /** Delete an authorized facade schedule and its owner in one transaction. */
  async deleteOwnedSchedule(id: string): Promise<'deleted' | 'pending'> {
    await this.#ensureSchema();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for atomic owned schedule deletion',
      );
    }
    await batch([
      this.#db
        .prepare(
          `UPDATE ${this.#schedules}
           SET status = 'paused', updatedAt = ?,
               deletionRequestedAt = CASE
                 WHEN EXISTS (
                   SELECT 1 FROM ${this.#triggers}
                   WHERE scheduleId = ? AND outcome = 'deferred'
                 ) THEN COALESCE(deletionRequestedAt, ?)
                 ELSE NULL
               END
           WHERE id = ?`,
        )
        .bind(Date.now(), id, Date.now(), id),
      this.#db
        .prepare(
          `DELETE FROM ${this.#triggers}
           WHERE scheduleId = ?
             AND NOT EXISTS (
               SELECT 1 FROM ${this.#schedules}
               WHERE id = ? AND deletionRequestedAt IS NOT NULL
             )`,
        )
        .bind(id, id),
      this.#db
        .prepare(
          `DELETE FROM ${this.#schedules}
           WHERE id = ? AND deletionRequestedAt IS NULL`,
        )
        .bind(id),
      this.#db
        .prepare(
          `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
           WHERE resource_kind = 'schedule' AND resource_id = ?
             AND NOT EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id = ?)`,
        )
        .bind(id, id),
    ]);
    const pending = await this.#db
      .prepare(
        `SELECT deletionRequestedAt FROM ${this.#schedules} WHERE id = ?`,
      )
      .bind(id)
      .first<{ deletionRequestedAt: number | null }>();
    return pending?.deletionRequestedAt != null ? 'pending' : 'deleted';
  }

  async recordTrigger(trigger: ScheduleTrigger): Promise<void> {
    await this.#ensureSchema();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for atomic trigger settlement',
      );
    }
    const id = trigger.id ?? crypto.randomUUID();
    await batch([
      this.#db
        .prepare(
          `INSERT OR REPLACE INTO ${this.#triggers} (
             id, scheduleId, runId, scheduledFireAt, actualFireAt, outcome,
             error, triggerKind, parentTriggerId, metadata
           )
           SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
           WHERE EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id = ?)`,
        )
        .bind(
          id,
          trigger.scheduleId,
          trigger.runId,
          trigger.scheduledFireAt,
          trigger.actualFireAt,
          trigger.outcome,
          trigger.error ?? null,
          trigger.triggerKind ?? 'schedule-fire',
          trigger.parentTriggerId ?? null,
          jsonOrNull(trigger.metadata),
          trigger.scheduleId,
        ),
      this.#db
        .prepare(
          `DELETE FROM ${this.#triggers}
           WHERE scheduleId = ?
             AND EXISTS (
               SELECT 1 FROM ${this.#schedules}
               WHERE id = ? AND deletionRequestedAt IS NOT NULL
             )
             AND NOT EXISTS (
               SELECT 1 FROM ${this.#triggers}
               WHERE scheduleId = ? AND outcome = 'deferred'
             )`,
        )
        .bind(trigger.scheduleId, trigger.scheduleId, trigger.scheduleId),
      this.#db
        .prepare(
          `DELETE FROM ${this.#schedules}
           WHERE id = ? AND deletionRequestedAt IS NOT NULL
             AND NOT EXISTS (
               SELECT 1 FROM ${this.#triggers}
               WHERE scheduleId = ? AND outcome = 'deferred'
             )`,
        )
        .bind(trigger.scheduleId, trigger.scheduleId),
      this.#db
        .prepare(
          `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
           WHERE resource_kind = 'schedule' AND resource_id = ?
             AND NOT EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id = ?)`,
        )
        .bind(trigger.scheduleId, trigger.scheduleId),
    ]);
  }

  /**
   * Merge tick-owned retry diagnostics into a deferred trigger. Target-side
   * dispatch state can change between the tick's read and this write, so a
   * full-row replacement here would erase the current lease or receipt.
   */
  async touchDeferredTrigger(
    id: string,
    scheduleId: string,
    error: string | undefined,
    metadata: Record<string, unknown>,
  ): Promise<void> {
    await this.#ensureSchema();
    await this.#db
      .prepare(
        `UPDATE ${this.#triggers}
         SET error = ?, metadata = json_patch(
           COALESCE(metadata, '{}'),
           json(?)
         )
         WHERE id = ? AND scheduleId = ? AND outcome = 'deferred'`,
      )
      .bind(error ?? null, JSON.stringify(metadata), id, scheduleId)
      .run();
  }

  async listTriggers(
    scheduleId: string,
    opts?: ScheduleTriggerListOptions,
  ): Promise<ScheduleTrigger[]> {
    await this.#ensureSchema();
    const clauses = ['scheduleId = ?'];
    const binds: unknown[] = [scheduleId];
    if (opts?.fromActualFireAt !== undefined) {
      clauses.push('actualFireAt >= ?');
      binds.push(opts.fromActualFireAt);
    }
    if (opts?.toActualFireAt !== undefined) {
      clauses.push('actualFireAt < ?');
      binds.push(opts.toActualFireAt);
    }
    const limitClause = opts?.limit !== undefined ? ' LIMIT ?' : '';
    if (opts?.limit !== undefined) binds.push(opts.limit);
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#triggers}
         WHERE ${clauses.join(' AND ')}
         ORDER BY actualFireAt DESC${limitClause}`,
      )
      .bind(...binds)
      .all<ScheduleTriggerRow>();
    return results.map(rowToTrigger);
  }

  async listDeferredTriggers(limit = 100): Promise<ScheduleTrigger[]> {
    await this.#ensureSchema();
    if (!Number.isSafeInteger(limit) || limit < 0) {
      throw new Error(
        'deferred trigger limit must be a nonnegative safe integer',
      );
    }
    const { results } = await this.#db
      .prepare(
        `SELECT * FROM ${this.#triggers}
         WHERE outcome = 'deferred'
         ORDER BY COALESCE(
           CAST(json_extract(metadata, '$.reconcileAfter') AS INTEGER),
           actualFireAt
         ) ASC, actualFireAt ASC
         LIMIT ?`,
      )
      .bind(limit)
      .all<ScheduleTriggerRow>();
    return results.map(rowToTrigger);
  }

  /**
   * Read the exact claimed fire a target Durable Object is allowed to execute.
   * Prepared, executing, and settled are one target-side lease lifecycle; the
   * deferred trigger remains the authority until the tick settles its outcome.
   * A schedule id without its atomically claimed trigger and run id is never an
   * execution capability.
   */
  async getClaimedScheduleDispatch(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<ScheduleTrigger | null> {
    if (
      !isPathSafeId(scheduleId) ||
      !isPathSafeId(dispatchId) ||
      !isPathSafeId(runId)
    ) {
      return null;
    }
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(
        `SELECT * FROM ${this.#triggers}
         WHERE id = ? AND scheduleId = ? AND runId = ?
           AND outcome = 'deferred'
           AND COALESCE(triggerKind, 'schedule-fire') = 'schedule-fire'
           AND json_extract(metadata, '$.dispatchState') IN (
             'prepared', 'executing', 'settled'
           )
           AND EXISTS (
             SELECT 1 FROM ${this.#schedules} WHERE id = ?
           )`,
      )
      .bind(dispatchId, scheduleId, runId, scheduleId)
      .first<ScheduleTriggerRow>();
    return row ? rowToTrigger(row) : null;
  }

  async dangerouslyClearAll(): Promise<void> {
    await this.#ensureSchema();
    const batch = this.#db.batch?.bind(this.#db);
    if (!batch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for atomic schedule clearing',
      );
    }
    await batch([
      this.#db.prepare(`DELETE FROM ${this.#triggers}`),
      this.#db.prepare(`DELETE FROM ${this.#schedules}`),
      this.#db.prepare(
        `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
         WHERE resource_kind = 'schedule'
           AND resource_id NOT IN (SELECT id FROM ${this.#schedules})`,
      ),
    ]);
  }

  /** Insert a new core schedule row. */
  async #insertSchedule(schedule: Schedule): Promise<void> {
    await this.#insertScheduleStatement(schedule).run();
  }

  #insertScheduleStatement(
    schedule: Schedule,
    maxSchedules?: number,
  ): ScheduleStatement {
    const columns = `(
           id, target, cron, timezone, status, nextFireAt, lastFireAt,
           lastRunId, createdAt, updatedAt, metadata, ownerType, ownerId,
           creatorRole
         )`;
    const values = [
      schedule.id,
      JSON.stringify(schedule.target),
      schedule.cron,
      schedule.timezone ?? null,
      schedule.status,
      schedule.nextFireAt,
      schedule.lastFireAt ?? null,
      schedule.lastRunId ?? null,
      schedule.createdAt,
      schedule.updatedAt,
      jsonOrNull(schedule.metadata),
      schedule.ownerType ?? null,
      schedule.ownerId ?? null,
      (schedule as Partial<AuthorizedSchedule>).creatorRole ?? null,
    ];
    if (maxSchedules === undefined) {
      return this.#db
        .prepare(
          `INSERT INTO ${this.#schedules} ${columns}
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .bind(...values);
    }
    return this.#db
      .prepare(
        `INSERT INTO ${this.#schedules} ${columns}
         SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         WHERE (SELECT COUNT(*) FROM ${this.#schedules}) < ?`,
      )
      .bind(...values, maxSchedules);
  }
}

// re-exported so the tick/router type against the row shape without a second
// @mastra/core import path.
export type { Schedule, ScheduleTrigger } from '@mastra/core/storage';
