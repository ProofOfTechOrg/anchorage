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
// TENANCY (DL-013): the schedule rows have NO tenant column — core normalizes ids
// to slugified `agent_<slug>`/`schedule_<slug>`, so a tenant cannot ride the id.
// The tenant lives in `metadata.tenantId`, stamped by the facade (router) at
// create and by the tick on every trigger it records. This domain is
// tenant-AGNOSTIC (it persists the rows it is handed); tenancy is enforced one
// layer up (router post-filters `metadata.tenantId`; purgeTenant metadata-filters
// it — d1-storage.ts TENANT_METADATA_PURGE_TABLES). The scale caveat: core lists
// schedules with no pagination ("schedule counts are expected to stay small"), so
// the router's JS post-filter and the purge's json_extract scan are acceptable.
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
  d1Changes,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
  type SignalStatement,
} from '../signals/d1-shared.js';

// The D1 seam + column helpers are Track C's canonical shared leaf
// (signals/d1-shared.ts, zero imports) — reused here so the two domains cannot
// drift on the `{ meta: { changes } }` envelope or the JSON encodings. The
// schedules-facing names are kept as aliases for a stable public API.
/** The prepared-statement subset the schedules domain uses. */
export type ScheduleStatement = SignalStatement;
/** The D1 database subset the schedules domain uses (workers-types-free). */
export type ScheduleDatabase = SignalDatabase;

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
  return {
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
    ...(row.triggerKind !== null
      ? { triggerKind: row.triggerKind as ScheduleTrigger['triggerKind'] }
      : {}),
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
               ownerId TEXT
             )`,
          )
          .run()
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

  async init(): Promise<void> {
    await this.#ensureSchema();
  }

  async createSchedule(schedule: Schedule): Promise<Schedule> {
    await this.#ensureSchema();
    // Throws on a duplicate id (core's contract: "Throws if a row with the same
    // id already exists"). The facade server-mints ids so a client can never
    // collide with another tenant's — no existence oracle from this throw.
    const existing = await this.getSchedule(schedule.id);
    if (existing) {
      throw new Error(`schedule ${schedule.id} already exists`);
    }
    await this.#insertSchedule(schedule);
    return schedule;
  }

  async getSchedule(id: string): Promise<Schedule | null> {
    await this.#ensureSchema();
    const row = await this.#db
      .prepare(`SELECT * FROM ${this.#schedules} WHERE id = ?`)
      .bind(id)
      .first<ScheduleRow>();
    return row ? rowToSchedule(row) : null;
  }

  async listSchedules(filter?: ScheduleFilter): Promise<Schedule[]> {
    await this.#ensureSchema();
    const clauses: string[] = [];
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
       WHERE status = 'active' AND nextFireAt <= ?
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
      .prepare(`UPDATE ${this.#schedules} SET ${sets.join(', ')} WHERE id = ?`)
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
    // false). Exactly-once, proven under two concurrent ticks on real workerd +
    // D1 (spike D-S1). The `status = 'active'` guard closes the pause race: a
    // schedule paused AFTER a tick read it as due but BEFORE this claim (status
    // flips, nextFireAt unchanged) fails the CAS here, so a just-paused schedule
    // does not fire one last time.
    const result = await this.#db
      .prepare(
        `UPDATE ${this.#schedules}
         SET nextFireAt = ?, lastFireAt = ?, lastRunId = ?, updatedAt = ?
         WHERE id = ? AND nextFireAt = ? AND status = 'active'`,
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

  async deleteSchedule(id: string): Promise<void> {
    await this.#ensureSchema();
    // Delete the schedule and its trigger history (core's contract).
    await this.#db
      .prepare(`DELETE FROM ${this.#triggers} WHERE scheduleId = ?`)
      .bind(id)
      .run();
    await this.#db
      .prepare(`DELETE FROM ${this.#schedules} WHERE id = ?`)
      .bind(id)
      .run();
  }

  async recordTrigger(trigger: ScheduleTrigger): Promise<void> {
    await this.#ensureSchema();
    const id = trigger.id ?? crypto.randomUUID();
    await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#triggers} (
           id, scheduleId, runId, scheduledFireAt, actualFireAt, outcome,
           error, triggerKind, parentTriggerId, metadata
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
        id,
        trigger.scheduleId,
        trigger.runId,
        trigger.scheduledFireAt,
        trigger.actualFireAt,
        trigger.outcome,
        trigger.error ?? null,
        trigger.triggerKind ?? null,
        trigger.parentTriggerId ?? null,
        jsonOrNull(trigger.metadata),
      )
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

  async dangerouslyClearAll(): Promise<void> {
    await this.#ensureSchema();
    await this.#db.prepare(`DELETE FROM ${this.#triggers}`).run();
    await this.#db.prepare(`DELETE FROM ${this.#schedules}`).run();
  }

  /** INSERT-or-REPLACE the full schedule row (create and update share it). */
  async #insertSchedule(schedule: Schedule): Promise<void> {
    await this.#db
      .prepare(
        `INSERT OR REPLACE INTO ${this.#schedules} (
           id, target, cron, timezone, status, nextFireAt, lastFireAt,
           lastRunId, createdAt, updatedAt, metadata, ownerType, ownerId
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .bind(
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
      )
      .run();
  }
}

// re-exported so the tick/router type against the row shape without a second
// @mastra/core import path.
export type { Schedule, ScheduleTrigger } from '@mastra/core/storage';
