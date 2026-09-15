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
import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';
import {
  APPROVAL_ROLES,
  canonicalResourceOwner,
  createResourceOwnershipSchema,
  RESOURCE_OWNERSHIP_TABLE,
  type ResourceOwner,
} from '../approval-api/index.js';
import {
  assertMutationEpoch,
  ExecutionFenceUnreadableError,
  type MutationEpochContext,
  normalizeMutationEpoch,
} from '../do-runner/execution-admission.js';
import {
  admitsWorkAuthoring,
  captureExecutionFenceAdmissionSchema,
  decodeExecutionFenceAdmissionRow,
  type ExecutionFenceAdmissionObservation,
  ExecutionFencedError,
  ExecutionFenceStore,
  executionFenceAdmissionSql,
  executionFenceAdmissionValues,
} from '../do-runner/execution-fence.js';
import { isPathSafeId } from '../do-runner/path-safe-id.js';
import { validateTablePrefix } from '../do-runner/table-prefix.js';

import {
  d1Changes,
  jsonOrNull,
  parseJsonOrUndefined,
  type SignalDatabase,
  type SignalStatement,
} from '../signals/d1-shared.js';
import {
  FENCED_SCHEDULE_STORAGE,
  type FencedScheduleMutationCapability,
  ScheduleMutationConflictError,
  ScheduleMutationOutcomeUnknownError,
  type ScheduleResumeMutation,
} from './mutation-contract.js';
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

const SCHEDULE_COLUMNS = [
  'id',
  'target',
  'cron',
  'timezone',
  'status',
  'nextFireAt',
  'lastFireAt',
  'lastRunId',
  'createdAt',
  'updatedAt',
  'metadata',
  'ownerType',
  'ownerId',
  'creatorRole',
] as const;

type ScheduleMutationOperation =
  | 'create'
  | 'update'
  | 'pause'
  | 'resume'
  | 'delete';

interface PreparedScheduleMutation {
  epoch: number | undefined;
  operation: ScheduleMutationOperation;
  semantic: readonly unknown[];
  schema: string;
  bindings: unknown[];
}

interface ScheduleStatementResult {
  rows: Record<string, unknown>[];
  hasChanges: boolean;
  changes: unknown;
}

function mutationUnknown(cause: unknown): ScheduleMutationOutcomeUnknownError {
  return new ScheduleMutationOutcomeUnknownError({ cause });
}

function mutationRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('schedule mutation result is malformed');
  }
  return value as Record<string, unknown>;
}

function captureScheduleResult(value: unknown): ScheduleStatementResult {
  const result = mutationRecord(value);
  if ('success' in result && result.success !== true) {
    throw new Error('schedule mutation statement did not succeed');
  }
  const rows = result.results;
  if (!Array.isArray(rows))
    throw new Error('schedule mutation rows are missing');
  const captured = Array.from({ length: rows.length }, (_, index) => {
    if (!Object.hasOwn(rows, index))
      throw new Error('schedule mutation row is missing');
    const row = mutationRecord(rows[index]);
    return Object.freeze(
      Object.fromEntries(
        Object.getOwnPropertyNames(row).map((key) => [key, row[key]]),
      ),
    );
  });
  const meta = 'meta' in result ? mutationRecord(result.meta) : undefined;
  const hasChanges = meta !== undefined && 'changes' in meta;
  return {
    rows: captured,
    hasChanges,
    changes: hasChanges ? meta.changes : undefined,
  };
}

function captureScheduleBatch(
  value: unknown,
  length: number,
): ScheduleStatementResult[] {
  if (!Array.isArray(value) || value.length !== length) {
    throw new Error('schedule mutation batch cardinality is invalid');
  }
  return Array.from({ length }, (_, index) => {
    if (!Object.hasOwn(value, index))
      throw new Error('schedule mutation result is missing');
    return captureScheduleResult(value[index]);
  });
}

function scheduleChanges(
  result: ScheduleStatementResult | undefined,
  returning: boolean,
): number {
  if (!result) throw new Error('schedule mutation result is missing');
  const { rows, hasChanges, changes } = result;
  if (
    (returning && rows.length > 1) ||
    (!returning && (rows.length !== 0 || !hasChanges)) ||
    (hasChanges &&
      (typeof changes !== 'number' ||
        !Number.isSafeInteger(changes) ||
        changes < 0 ||
        (returning && changes !== rows.length)))
  )
    throw new Error('schedule mutation changes contradict its evidence');
  return returning ? rows.length : (changes as number);
}

function singleScheduleRow(
  result: ScheduleStatementResult | undefined,
): Record<string, unknown> | undefined {
  if (!result) throw new Error('schedule mutation result is missing');
  if (result.rows.length > 1)
    throw new Error('schedule mutation returned multiple rows');
  return result.rows[0];
}

function sameScheduleFields(
  row: Record<string, unknown>,
  expected: Record<string, unknown>,
): boolean {
  return Object.entries(expected).every(
    ([key, value]) => Object.hasOwn(row, key) && row[key] === value,
  );
}

function requireScheduleFields(
  row: Record<string, unknown> | undefined,
  expected: Record<string, unknown>,
): asserts row is Record<string, unknown> {
  if (!row || !sameScheduleFields(row, expected)) {
    throw new Error('schedule mutation returned a different row');
  }
}

function mutationScheduleRow(row: Record<string, unknown>): ScheduleRow {
  const text = ['id', 'target', 'cron', 'status'];
  const nullableText = [
    'timezone',
    'lastRunId',
    'metadata',
    'ownerType',
    'ownerId',
    'creatorRole',
  ];
  const numeric = ['nextFireAt', 'createdAt', 'updatedAt'];
  const nullableNumeric = ['lastFireAt', 'deletionRequestedAt'];
  if (
    text.some((key) => typeof row[key] !== 'string') ||
    nullableText.some(
      (key) => row[key] !== null && typeof row[key] !== 'string',
    ) ||
    numeric.some(
      (key) => typeof row[key] !== 'number' || !Number.isFinite(row[key]),
    ) ||
    nullableNumeric.some(
      (key) =>
        row[key] !== null &&
        (typeof row[key] !== 'number' || !Number.isFinite(row[key])),
    )
  ) {
    throw new Error('schedule mutation row is malformed');
  }
  return row as unknown as ScheduleRow;
}

function captureSchedule(schedule: Schedule): ScheduleRow {
  const {
    id,
    target,
    cron,
    timezone,
    status,
    nextFireAt,
    lastFireAt,
    lastRunId,
    createdAt,
    updatedAt,
    metadata,
    ownerType,
    ownerId,
    creatorRole,
  } = schedule as Schedule & { creatorRole?: string };
  return {
    id,
    target: JSON.stringify(target),
    cron,
    timezone: timezone ?? null,
    status,
    nextFireAt,
    lastFireAt: lastFireAt ?? null,
    lastRunId: lastRunId ?? null,
    createdAt,
    updatedAt,
    metadata: jsonOrNull(metadata),
    ownerType: ownerType ?? null,
    ownerId: ownerId ?? null,
    creatorRole: creatorRole ?? null,
    deletionRequestedAt: null,
  };
}

function captureSchedulePatch(patch: ScheduleUpdate): Partial<ScheduleRow> {
  const {
    cron,
    timezone,
    status,
    nextFireAt,
    metadata,
    target,
    ownerType,
    ownerId,
  } = patch;
  return {
    updatedAt: Date.now(),
    ...(cron !== undefined ? { cron } : {}),
    ...(timezone !== undefined ? { timezone: timezone ?? null } : {}),
    ...(status !== undefined ? { status } : {}),
    ...(nextFireAt !== undefined ? { nextFireAt } : {}),
    ...(metadata !== undefined ? { metadata: jsonOrNull(metadata) } : {}),
    ...(target !== undefined ? { target: JSON.stringify(target) } : {}),
    ...(ownerType !== undefined ? { ownerType: ownerType ?? null } : {}),
    ...(ownerId !== undefined ? { ownerId: ownerId ?? null } : {}),
  };
}

function requiresOpenFence(operation: ScheduleMutationOperation): boolean {
  return (
    operation === 'create' || operation === 'update' || operation === 'resume'
  );
}

function scheduleMutationGuard(prepared: PreparedScheduleMutation): string {
  return executionFenceAdmissionSql({
    callerEpoch: '?1',
    semantic: prepared.semantic.map((_, index) => `?${index + 2}`),
    schema: '?12',
    statePredicate: requiresOpenFence(prepared.operation)
      ? "f.state COLLATE BINARY = 'open'"
      : '1',
  });
}

interface ScheduleDeletionFacts {
  schedules: number;
  deletion_requested_at: number | null;
  triggers: number;
  deferred: number;
  owners: number;
}

function deletionFacts(result: ScheduleStatementResult): ScheduleDeletionFacts {
  const row = singleScheduleRow(result);
  if (
    !row ||
    ['schedules', 'triggers', 'deferred', 'owners'].some(
      (key) =>
        typeof row[key] !== 'number' ||
        !Number.isSafeInteger(row[key]) ||
        row[key] < 0,
    )
  )
    throw new Error('schedule deletion facts are malformed');
  const facts = row as unknown as ScheduleDeletionFacts;
  if (
    facts.schedules > 1 ||
    facts.owners > 1 ||
    facts.deferred > facts.triggers ||
    (facts.deletion_requested_at !== null &&
      (typeof facts.deletion_requested_at !== 'number' ||
        !Number.isFinite(facts.deletion_requested_at))) ||
    (facts.schedules === 0 && facts.deletion_requested_at !== null)
  ) {
    throw new Error('schedule deletion facts are inconsistent');
  }
  return facts;
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
  readonly [FENCED_SCHEDULE_STORAGE]?: FencedScheduleMutationCapability;
  readonly #db: ScheduleDatabase;
  readonly #fence: ExecutionFenceStore;
  readonly #mutationBatch?: (
    statements: ScheduleStatement[],
  ) => Promise<unknown[]>;
  readonly #schedules: string;
  readonly #triggers: string;
  #ready?: Promise<void>;
  #authoringReady?: Promise<void>;

  constructor(db: ScheduleDatabase, tablePrefix = '') {
    super();
    const prefix = validateTablePrefix(tablePrefix) ?? '';
    this.#db = db;
    this.#fence = new ExecutionFenceStore(db);
    this.#schedules = `${prefix}mastra_schedules`;
    this.#triggers = `${prefix}mastra_schedule_triggers`;
    const batch = db.batch;
    if (typeof batch === 'function') {
      this.#mutationBatch = (statements) =>
        Reflect.apply(batch, db, [statements]);
      this[FENCED_SCHEDULE_STORAGE] = Object.freeze({
        database: db as FencedScheduleMutationCapability['database'],
        createOwnedSchedule: this.createOwnedSchedule.bind(this),
        updateSchedule: this.updateSchedule.bind(this),
        pauseSchedule: this.pauseSchedule.bind(this),
        resumeSchedule: this.resumeSchedule.bind(this),
        deleteOwnedSchedule: this.deleteOwnedSchedule.bind(this),
        observeScheduleMutation: this.observeScheduleMutation.bind(this),
      });
    }
  }

  async #prepareMutation(
    epoch: number | undefined,
    operation: ScheduleMutationOperation,
  ): Promise<PreparedScheduleMutation> {
    if (!this.#mutationBatch) {
      throw new Error(
        'D1SchedulesStorage requires database.batch() for schedule mutations',
      );
    }
    if (!this.#authoringReady) {
      this.#authoringReady = this.#ensureSchema()
        .then(() => this.#fence.seed('open'))
        .catch((error: unknown) => {
          this.#authoringReady = undefined;
          throw error;
        });
    }
    await this.#authoringReady;
    const observation = await this.#fence.readForAdmission();
    const semantic = executionFenceAdmissionValues(observation);
    assertMutationEpoch(observation.reading, epoch);
    if (
      requiresOpenFence(operation) &&
      !admitsWorkAuthoring(observation.reading)
    ) {
      throw new ExecutionFencedError(observation.reading.state);
    }
    let schema: string;
    try {
      const captured = captureScheduleResult(
        await this.#db
          .prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`)
          .all(),
      );
      schema = await captureExecutionFenceAdmissionSchema({
        results: captured.rows,
      });
    } catch (cause) {
      throw new ExecutionFenceUnreadableError(
        'execution fence schema is not readable',
        { cause },
      );
    }
    return {
      epoch,
      operation,
      semantic,
      schema,
      bindings: [epoch ?? null, ...semantic, schema],
    };
  }

  #mutationDiagnostics(): [ScheduleStatement, ScheduleStatement] {
    return [
      this.#db.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE} LIMIT 2`),
      this.#db.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`),
    ];
  }

  async #executeMutation<const Statements extends readonly ScheduleStatement[]>(
    prepared: PreparedScheduleMutation,
    statements: Statements,
    returningSlots: readonly number[],
    changesSlot?: number,
  ): Promise<{ [Index in keyof Statements]: ScheduleStatementResult }> {
    let results: ScheduleStatementResult[];
    let writes: number;
    try {
      if (!this.#mutationBatch)
        throw new Error('schedule mutation batch is unavailable');
      results = captureScheduleBatch(
        await this.#mutationBatch([...statements]),
        statements.length,
      );
      writes = returningSlots.reduce(
        (count, slot) => count + scheduleChanges(results[slot], true),
        0,
      );
      if (changesSlot !== undefined)
        writes += scheduleChanges(results[changesSlot], false);
    } catch (cause) {
      throw mutationUnknown(cause);
    }
    let current: ExecutionFenceAdmissionObservation;
    let semantic: readonly unknown[];
    let schema: string;
    const schemaResult = results[1];
    try {
      const row = singleScheduleRow(results[0]);
      if (!row) throw new Error('execution fence singleton is missing');
      current = decodeExecutionFenceAdmissionRow(row);
      semantic = executionFenceAdmissionValues(current);
      if (!schemaResult)
        throw new Error('execution fence schema result is missing');
      schema = await captureExecutionFenceAdmissionSchema({
        results: schemaResult.rows,
      });
    } catch (cause) {
      const unreadable = new ExecutionFenceUnreadableError(
        'execution fence state is not readable',
        { cause },
      );
      throw writes === 0 ? unreadable : mutationUnknown(unreadable);
    }
    try {
      assertMutationEpoch(current.reading, prepared.epoch);
      if (
        requiresOpenFence(prepared.operation) &&
        !admitsWorkAuthoring(current.reading)
      ) {
        throw new ExecutionFencedError(current.reading.state);
      }
      if (
        semantic.some((value, index) => value !== prepared.semantic[index]) ||
        schema !== prepared.schema
      ) {
        throw new ScheduleMutationConflictError('fence-changed');
      }
    } catch (cause) {
      throw writes === 0 ? cause : mutationUnknown(cause);
    }
    return results as { [Index in keyof Statements]: ScheduleStatementResult };
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

  async createSchedule(
    schedule: Schedule,
    context?: MutationEpochContext,
  ): Promise<Schedule> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    const row = captureSchedule(schedule);
    const prepared = await this.#prepareMutation(epoch, 'create');
    const results = await this.#executeMutation(
      prepared,
      [
        ...this.#mutationDiagnostics(),
        this.#db
          .prepare(
            `SELECT id FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1 LIMIT 2`,
          )
          .bind(row.id),
        this.#insertScheduleStatement(row, prepared),
      ],
      [3],
    );
    try {
      const existing = singleScheduleRow(results[2]);
      if (existing) requireScheduleFields(existing, { id: row.id });
      const created = singleScheduleRow(results[3]);
      if (created) {
        if (existing)
          throw new Error('schedule insertion contradicts existing id');
        requireScheduleFields(created, { ...row });
        return rowToSchedule(mutationScheduleRow(created));
      }
      if (!existing) throw new Error('schedule insertion has no outcome');
    } catch (cause) {
      throw mutationUnknown(cause);
    }
    throw new Error(`schedule ${row.id} already exists`);
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
    context?: MutationEpochContext,
  ): Promise<Schedule | null> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    const row = captureSchedule(schedule);
    const safeOwner = canonicalResourceOwner(
      owner === null || typeof owner !== 'object'
        ? owner
        : { kind: owner.kind, id: owner.id },
    );
    if (!Number.isSafeInteger(maxSchedules) || maxSchedules < 0) {
      throw new Error('maxSchedules must be a nonnegative safe integer');
    }
    const prepared = await this.#prepareMutation(epoch, 'create');
    const results = await this.#executeMutation(
      prepared,
      [
        ...this.#mutationDiagnostics(),
        this.#db
          .prepare(`SELECT COUNT(*) AS total,
        EXISTS(SELECT 1 FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1) AS id_exists
        FROM ${this.#schedules}`)
          .bind(row.id),
        this.#insertScheduleStatement(row, prepared, maxSchedules),
        this.#db
          .prepare(
            `INSERT INTO ${RESOURCE_OWNERSHIP_TABLE}
             (resource_kind, resource_id, owner_kind, owner_id)
           SELECT 'schedule', ?13, ?14, ?15
           WHERE changes() = 1
             AND EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id COLLATE BINARY = ?13)
             AND ${scheduleMutationGuard(prepared)}
           RETURNING resource_kind, resource_id, owner_kind, owner_id, reservation_token`,
          )
          .bind(...prepared.bindings, row.id, safeOwner.kind, safeOwner.id),
      ],
      [3, 4],
    );
    try {
      const facts = singleScheduleRow(results[2]);
      if (
        !facts ||
        typeof facts.total !== 'number' ||
        !Number.isSafeInteger(facts.total) ||
        facts.total < 0 ||
        (facts.id_exists !== 0 && facts.id_exists !== 1)
      ) {
        throw new Error('schedule creation facts are malformed');
      }
      const created = singleScheduleRow(results[3]);
      const owned = singleScheduleRow(results[4]);
      if (created) {
        if (facts.total >= maxSchedules || facts.id_exists !== 0)
          throw new Error('schedule insertion contradicts its cap or id');
        requireScheduleFields(created, { ...row });
        requireScheduleFields(owned, {
          resource_kind: 'schedule',
          resource_id: row.id,
          owner_kind: safeOwner.kind,
          owner_id: safeOwner.id,
          reservation_token: null,
        });
        return rowToSchedule(mutationScheduleRow(created));
      }
      if (owned) throw new Error('zero schedule insertion acquired an owner');
      if (facts.total >= maxSchedules) return null;
      if (facts.id_exists !== 1)
        throw new Error('schedule insertion has no outcome');
    } catch (cause) {
      throw mutationUnknown(cause);
    }
    throw new Error(`schedule ${row.id} already exists`);
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

  async updateSchedule(
    id: string,
    patch: ScheduleUpdate,
    context?: MutationEpochContext,
  ): Promise<Schedule> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    return this.#updateSchedule(
      id,
      captureSchedulePatch(patch),
      epoch,
      'update',
    );
  }

  async pauseSchedule(
    id: string,
    context?: MutationEpochContext,
  ): Promise<Schedule> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    return this.#updateSchedule(
      id,
      { status: 'paused', updatedAt: Date.now() },
      epoch,
      'pause',
    );
  }

  async resumeSchedule(
    id: string,
    mutation: ScheduleResumeMutation,
    context?: MutationEpochContext,
  ): Promise<Schedule> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    const { expectedCron, expectedTimezone, nextFireAt } = mutation;
    return this.#updateSchedule(
      id,
      { status: 'active', nextFireAt, updatedAt: Date.now() },
      epoch,
      'resume',
      {
        cron: expectedCron,
        timezone: expectedTimezone ?? null,
      },
    );
  }

  async #updateSchedule(
    id: string,
    patch: Partial<ScheduleRow>,
    epoch: number | undefined,
    operation: 'update' | 'pause' | 'resume',
    expected?: { cron: string; timezone: string | null },
  ): Promise<Schedule> {
    const fields = Object.entries(patch);
    const prepared = await this.#prepareMutation(epoch, operation);
    const idParameter = fields.length + 13;
    // Targeted writes preserve columns advanced by a concurrent fire claim.
    const sets = fields.map(([key], index) => `${key} = ?${index + 13}`);
    const results = await this.#executeMutation(
      prepared,
      [
        ...this.#mutationDiagnostics(),
        this.#db
          .prepare(
            `SELECT * FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1 LIMIT 2`,
          )
          .bind(id),
        this.#db
          .prepare(`UPDATE ${this.#schedules} SET ${sets.join(', ')}
        WHERE id COLLATE BINARY = ?${idParameter} AND deletionRequestedAt IS NULL
          ${expected ? `AND cron COLLATE BINARY = ?${idParameter + 1} AND timezone COLLATE BINARY IS ?${idParameter + 2}` : ''}
          AND ${scheduleMutationGuard(prepared)} RETURNING *`)
          .bind(
            ...prepared.bindings,
            ...fields.map(([, value]) => value),
            id,
            ...(expected ? [expected.cron, expected.timezone] : []),
          ),
      ],
      [3],
    );
    let missing: boolean;
    let changed = false;
    try {
      const previous = singleScheduleRow(results[2]);
      const updated = singleScheduleRow(results[3]);
      if (previous) {
        requireScheduleFields(previous, { id });
        mutationScheduleRow(previous);
      }
      missing = !previous || previous.deletionRequestedAt !== null;
      if (updated) {
        if (
          missing ||
          (expected &&
            !sameScheduleFields(previous as Record<string, unknown>, expected))
        ) {
          throw new Error('schedule update contradicts its prior row');
        }
        requireScheduleFields(updated, { ...previous, ...patch, id });
        return rowToSchedule(mutationScheduleRow(updated));
      }
      changed =
        !missing &&
        expected !== undefined &&
        !sameScheduleFields(previous as Record<string, unknown>, expected);
      if (!missing && !changed)
        throw new Error('schedule update has no outcome');
    } catch (cause) {
      throw mutationUnknown(cause);
    }
    if (changed) throw new ScheduleMutationConflictError('schedule-changed');
    throw new Error(`schedule ${id} not found`);
  }

  async observeScheduleMutation(
    id: string,
    operation: 'pause' | 'resume',
    context: MutationEpochContext,
  ): Promise<Schedule | null> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    if (operation !== 'pause' && operation !== 'resume')
      throw new Error('schedule mutation observation is invalid');
    const prepared = await this.#prepareMutation(epoch, operation);
    const results = await this.#executeMutation(
      prepared,
      [
        ...this.#mutationDiagnostics(),
        this.#db
          .prepare(
            `SELECT * FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1 LIMIT 2`,
          )
          .bind(id),
        this.#db
          .prepare(`SELECT * FROM ${this.#schedules}
        WHERE id COLLATE BINARY = ?13 AND deletionRequestedAt IS NULL
          AND ${scheduleMutationGuard(prepared)} LIMIT 2`)
          .bind(...prepared.bindings, id),
      ],
      [],
    );
    try {
      const previous = singleScheduleRow(results[2]);
      const observed = singleScheduleRow(results[3]);
      if (previous) {
        requireScheduleFields(previous, { id });
        mutationScheduleRow(previous);
      }
      if (!previous || previous.deletionRequestedAt !== null) {
        if (observed)
          throw new Error('schedule observation contradicts absence');
        return null;
      }
      requireScheduleFields(observed, previous);
      return rowToSchedule(mutationScheduleRow(observed));
    } catch (cause) {
      throw mutationUnknown(cause);
    }
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

  /**
   * Make terminal-run discard authoritative for the exact leased run.
   * A schedule signal may have observed a suspension and settled a wake just
   * before terminal cleanup acquired the target-thread FIFO. That same-run
   * receipt is safe to replace; unrelated or non-execution receipts fail
   * closed.
   */
  async discardAgentScheduleDispatch(
    scheduleId: string,
    triggerId: string,
    runId: string,
  ): Promise<void> {
    if (!isPathSafeId(runId)) {
      throw new TypeError('invalid agent schedule dispatch run id');
    }
    const canonical = createScheduleAgentDispatchReceipt('discard', { runId });
    await this.#ensureSchema();
    const discarded = await this.#db
      .prepare(
        `UPDATE ${this.#triggers}
         SET metadata = json_set(
           COALESCE(metadata, '{}'),
           '$.dispatchState', 'settled',
           '$.dispatchReceipt', json(?)
         )
         WHERE id = ? AND scheduleId = ? AND outcome = 'deferred'
           AND json_extract(metadata, '$.dispatchRef.runId') = ?
           AND (
             json_extract(metadata, '$.dispatchState') = 'executing'
             OR (
               json_extract(metadata, '$.dispatchState') = 'settled'
               AND json_extract(metadata, '$.dispatchReceipt.runId') = ?
               AND json_extract(metadata, '$.dispatchReceipt.action')
                 IN ('wake', 'deliver', 'discard')
             )
           )`,
      )
      .bind(JSON.stringify(canonical), triggerId, scheduleId, runId, runId)
      .run();
    if (d1Changes(discarded) === 1) return;
    const row = await this.#db
      .prepare(
        `SELECT scheduleId, runId, outcome, metadata FROM ${this.#triggers}
         WHERE id = ?`,
      )
      .bind(triggerId)
      .first<{
        scheduleId: string;
        runId: string | null;
        outcome: ScheduleTrigger['outcome'];
        metadata: string | null;
      }>();
    // The tick's final bookkeeping is absorbing: once the exact run's row is
    // non-deferred, no later pass can begin or redispatch it. A deleted row is
    // equally converged (schedule/trigger retention already removed the work).
    if (!row) return;
    if (row.scheduleId !== scheduleId || row.runId !== runId) {
      throw new Error(
        'agent schedule dispatch belongs to another schedule or run',
      );
    }
    if (row.outcome !== 'deferred') return;
    const metadata =
      row.metadata === null
        ? undefined
        : parseJsonOrUndefined<Record<string, unknown>>(row.metadata);
    const receipt = parseScheduleAgentDispatchReceipt(
      metadata?.dispatchReceipt,
    );
    if (receipt && JSON.stringify(receipt) === JSON.stringify(canonical)) {
      return;
    }
    throw new Error('agent schedule dispatch could not be force-discarded');
  }

  async deleteSchedule(
    id: string,
    context?: MutationEpochContext,
  ): Promise<void> {
    await this.deleteOwnedSchedule(id, context);
  }

  /** Delete an authorized facade schedule and its owner in one transaction. */
  async deleteOwnedSchedule(
    id: string,
    context?: MutationEpochContext,
  ): Promise<'deleted' | 'pending'> {
    const epoch = normalizeMutationEpoch(context?.mutationEpoch);
    const now = Date.now();
    const prepared = await this.#prepareMutation(epoch, 'delete');
    const guard = scheduleMutationGuard(prepared);
    const results = await this.#executeMutation(
      prepared,
      [
        ...this.#mutationDiagnostics(),
        this.#deletionFactsStatement(id),
        this.#db
          .prepare(
            `UPDATE ${this.#schedules}
           SET status = 'paused', updatedAt = ?13,
               deletionRequestedAt = CASE
                 WHEN EXISTS (
                   SELECT 1 FROM ${this.#triggers}
                   WHERE scheduleId COLLATE BINARY = ?14 AND outcome = 'deferred'
                 ) THEN COALESCE(deletionRequestedAt, ?13)
                 ELSE NULL
               END
           WHERE id COLLATE BINARY = ?14 AND ${guard}
           RETURNING id, status, updatedAt, deletionRequestedAt`,
          )
          .bind(...prepared.bindings, now, id),
        this.#db
          .prepare(
            `DELETE FROM ${this.#triggers}
           WHERE scheduleId COLLATE BINARY = ?13 AND ${guard}
             AND NOT EXISTS (
               SELECT 1 FROM ${this.#schedules}
               WHERE id COLLATE BINARY = ?13 AND deletionRequestedAt IS NOT NULL
             )`,
          )
          .bind(...prepared.bindings, id),
        this.#db
          .prepare(
            `DELETE FROM ${this.#schedules}
           WHERE id COLLATE BINARY = ?13 AND deletionRequestedAt IS NULL
             AND ${guard} RETURNING id`,
          )
          .bind(...prepared.bindings, id),
        this.#db
          .prepare(
            `DELETE FROM ${RESOURCE_OWNERSHIP_TABLE}
           WHERE resource_kind COLLATE BINARY = 'schedule' AND resource_id COLLATE BINARY = ?13
             AND NOT EXISTS (SELECT 1 FROM ${this.#schedules} WHERE id COLLATE BINARY = ?13)
             AND ${guard}
           RETURNING resource_kind, resource_id, owner_kind, owner_id, reservation_token`,
          )
          .bind(...prepared.bindings, id),
        this.#deletionFactsStatement(id),
      ],
      [3, 5, 6],
      4,
    );
    try {
      const before = deletionFacts(results[2]);
      const after = deletionFacts(results[7]);
      const pending = before.schedules === 1 && before.deferred > 0;
      const marker = pending ? (before.deletion_requested_at ?? now) : null;
      if (
        results[3].rows.length !== before.schedules ||
        scheduleChanges(results[4], false) !==
          (pending ? 0 : before.triggers) ||
        results[5].rows.length !== (pending ? 0 : before.schedules) ||
        results[6].rows.length !== (pending ? 0 : before.owners)
      ) {
        throw new Error('schedule deletion writes contradict its prior state');
      }
      if (before.schedules === 1)
        requireScheduleFields(results[3].rows[0], {
          id,
          status: 'paused',
          updatedAt: now,
          deletionRequestedAt: marker,
        });
      for (const row of results[5].rows) requireScheduleFields(row, { id });
      for (const row of results[6].rows) {
        requireScheduleFields(row, {
          resource_kind: 'schedule',
          resource_id: id,
        });
        if (
          typeof row.owner_kind !== 'string' ||
          typeof row.owner_id !== 'string' ||
          (row.reservation_token !== null &&
            typeof row.reservation_token !== 'string')
        ) {
          throw new Error('schedule deletion owner is malformed');
        }
      }
      const expected = pending
        ? { ...before, deletion_requested_at: marker }
        : {
            schedules: 0,
            deletion_requested_at: null,
            triggers: 0,
            deferred: 0,
            owners: 0,
          };
      if (!sameScheduleFields({ ...after }, expected))
        throw new Error('schedule deletion did not converge');
      return pending ? 'pending' : 'deleted';
    } catch (cause) {
      throw mutationUnknown(cause);
    }
  }

  #deletionFactsStatement(id: string): ScheduleStatement {
    return this.#db
      .prepare(`SELECT
      (SELECT COUNT(*) FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1) AS schedules,
      (SELECT deletionRequestedAt FROM ${this.#schedules} WHERE id COLLATE BINARY = ?1) AS deletion_requested_at,
      (SELECT COUNT(*) FROM ${this.#triggers} WHERE scheduleId COLLATE BINARY = ?1) AS triggers,
      (SELECT COUNT(*) FROM ${this.#triggers} WHERE scheduleId COLLATE BINARY = ?1 AND outcome = 'deferred') AS deferred,
      (SELECT COUNT(*) FROM ${RESOURCE_OWNERSHIP_TABLE}
        WHERE resource_kind COLLATE BINARY = 'schedule' AND resource_id COLLATE BINARY = ?1) AS owners`)
      .bind(id);
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

  #insertScheduleStatement(
    row: ScheduleRow,
    prepared: PreparedScheduleMutation,
    maxSchedules?: number,
  ): ScheduleStatement {
    return this.#db
      .prepare(
        `INSERT INTO ${this.#schedules} (${SCHEDULE_COLUMNS.join(', ')})
         SELECT ${SCHEDULE_COLUMNS.map((_, index) => `?${index + 13}`).join(', ')}
         WHERE ${scheduleMutationGuard(prepared)}
           ${maxSchedules === undefined ? '' : `AND (SELECT COUNT(*) FROM ${this.#schedules}) < ?27`}
         ON CONFLICT (id) DO NOTHING RETURNING *`,
      )
      .bind(
        ...prepared.bindings,
        ...SCHEDULE_COLUMNS.map((key) => row[key]),
        ...(maxSchedules === undefined ? [] : [maxSchedules]),
      );
  }
}

// re-exported so the tick/router type against the row shape without a second
// @mastra/core import path.
export type { Schedule, ScheduleTrigger } from '@mastra/core/storage';
