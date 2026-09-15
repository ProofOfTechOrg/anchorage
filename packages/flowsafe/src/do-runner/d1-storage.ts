// SPDX-License-Identifier: Apache-2.0
// The D1 adapter wraps Mastra's store to pin flowsafe defaults and provide the
// seam where audit export and Queues hooks attach. Table auto-creation
// (CREATE TABLE IF NOT EXISTS)
// happens lazily via Mastra's storage-init proxy once the store is handed
// to `new Mastra({ storage })` — no migration step needed for the runner.

import type { D1Database } from '@cloudflare/workers-types';
import { D1Store } from '@mastra/cloudflare-d1';
import {
  MastraCompositeStore,
  type MastraStorageDomains,
} from '@mastra/core/storage';
import { missingTableReadsEmpty } from './cause-chain.js';
import type { D1DatabaseBinding } from './cf-types.js';
import {
  normalizeD1RunExecutionIdentity,
  normalizeStartExecutionIdentity,
} from './execution-admission.js';
import { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import {
  notificationTimestampMillis,
  notificationTimestampSql,
} from './notification-predicate.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  decodeRunStartIdentity,
  runExecutionIdentityFor,
} from './run-provenance.js';
import { RESOURCE_OWNER_TABLE } from './run-storage-tables.js';
import { START_IDEMPOTENCY_TABLE } from './start-idempotency.js';
import {
  admissionReservationFromRow,
  reservationFromRow,
  reservationSchemaStage,
  START_IDEMPOTENCY_COLUMNS,
  type StartReservationSchemaStage,
} from './start-reservation-contract.js';
import { validateTablePrefix } from './table-prefix.js';
import {
  decodeRawWorkflowSnapshotResult,
  prepareRawWorkflowSnapshotRead,
  type RawWorkflowSnapshot,
  type SnapshotDatabase,
  type SnapshotStatement,
  snapshotResultRows,
} from './workflow-snapshot-row.js';

export { RESOURCE_OWNER_TABLE } from './run-storage-tables.js';
export type {
  SnapshotDatabase,
  SnapshotStatement,
} from './workflow-snapshot-row.js';

export interface D1StorageOptions {
  /** D1 binding from the Worker/DO environment. */
  binding: D1DatabaseBinding;
  /** Storage instance id. Default: 'flowsafe'. */
  id?: string;
  /** Mastra-compatible SQL identifier prefix of at most 39 characters, or empty. */
  tablePrefix?: string;
  /**
   * Additional storage domains composed over the D1Store default, such as
   * notifications and thread state, which @mastra/cloudflare-d1 does not ship, so
   * they are flowsafe-owned D1 impls). Injected rather than imported so this
   * lower layer never depends on `signals/` (which imports do-runner) — build
   * them with `createSignalStorageDomains()` and pass them here. The default
   * workflow domain supports explicit initial-admission scopes; false/custom
   * workflow overrides retain precedence.
   */
  domains?: MastraStorageDomains;
}

export function createD1Storage(
  options: D1StorageOptions,
): MastraCompositeStore {
  const {
    binding,
    id: suppliedId,
    tablePrefix: suppliedPrefix,
    domains: suppliedDomains,
  } = options;
  const domainSource = suppliedDomains ?? {};
  const capturedDomains = {
    workflows: domainSource.workflows,
    scores: domainSource.scores,
    memory: domainSource.memory,
    channels: domainSource.channels,
    notifications: domainSource.notifications,
    observability: domainSource.observability,
    agents: domainSource.agents,
    datasets: domainSource.datasets,
    experiments: domainSource.experiments,
    promptBlocks: domainSource.promptBlocks,
    scorerDefinitions: domainSource.scorerDefinitions,
    mcpClients: domainSource.mcpClients,
    mcpServers: domainSource.mcpServers,
    workspaces: domainSource.workspaces,
    skills: domainSource.skills,
    favorites: domainSource.favorites,
    blobs: domainSource.blobs,
    backgroundTasks: domainSource.backgroundTasks,
    schedules: domainSource.schedules,
    harness: domainSource.harness,
    toolProviderConnections: domainSource.toolProviderConnections,
    threadState: domainSource.threadState,
  } satisfies Record<keyof MastraStorageDomains, unknown>;
  const { workflows: suppliedWorkflows, ...otherDomains } = capturedDomains;
  const id = suppliedId ?? 'flowsafe';
  const tablePrefix = validateTablePrefix(suppliedPrefix);
  const domainConfig = {
    binding: binding as unknown as D1Database,
    ...(tablePrefix === undefined ? {} : { tablePrefix }),
  };
  const d1 = new D1Store({
    id,
    // @mastra/cloudflare-d1's own D1Store signature wants the real
    // D1Database; D1DatabaseBinding is the structural subset this package
    // exposes instead, so consumers of its shipped types don't need
    // @cloudflare/workers-types installed.
    ...domainConfig,
  });
  const workflows =
    suppliedWorkflows === undefined
      ? new FencedWorkflowsStorageD1(domainConfig)
      : suppliedWorkflows;
  return new MastraCompositeStore({
    id,
    default: d1,
    domains: { ...otherDomains, workflows },
  });
}

/** Shared with the drain inventory so cleanup and liveness use the same vocabulary. */
export const RUN_TERMINAL_STATUSES = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
  'cancelled',
  'timed_out',
] as const;

/** Duplicate keys can disagree between SQLite and JSON.parse on terminal eligibility. */
export const RUN_TERMINAL_SNAPSHOT_SQL = `json_type(snapshot, '$') = 'object'
             AND (SELECT count(*) FROM json_each(snapshot) WHERE key COLLATE BINARY = 'status') = 1
             AND (SELECT count(*) FROM json_each(snapshot) WHERE key COLLATE BINARY = 'requestContext') <= 1
             AND (SELECT count(*) FROM json_each(snapshot, '$.requestContext') WHERE key COLLATE BINARY = 'flowsafe.runLifecycle') <= 1
             AND (SELECT count(*) FROM json_each(snapshot, '$.requestContext."flowsafe.runLifecycle"') WHERE key COLLATE BINARY = 'terminal') <= 1
             AND (SELECT count(*) FROM json_each(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal') WHERE key COLLATE BINARY = 'cleanupCompletedAt') <= 1
             AND json_extract(snapshot, '$.status') IN (${RUN_TERMINAL_STATUSES.map(
               () => '?',
             ).join(', ')})
             AND (
               json_extract(snapshot, '$.status') NOT IN ('cancelled', 'timed_out')
               OR (
                 json_type(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') IN ('integer', 'real')
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') BETWEEN 0 AND 9007199254740991
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') = CAST(
                   json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') AS INTEGER
                 )
               )
             )`;

const DEADLINE_LIVE_STATUSES = [
  'running',
  'waiting',
  'pending',
  'paused',
  'waiting_callback',
  'waiting_signal',
  'retry_wait',
  'suspended',
] as const;

export interface RunDeadlineCandidate {
  workflowId: string;
  runId: string;
  revision: number;
  deadlineAt: number;
}

/** Persistent scan position used to rotate bounded deadline passes. */
export interface RunDeadlineCursor {
  workflowId: string;
  runId: string;
  deadlineAt: number;
}

export interface SweepExpiredRunDeadlinesOptions {
  /** Bounded rows per duty pass. Default 100. */
  limit?: number;
  /** Must satisfy createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  now?: () => number;
  /** Last selected row from the prior pass. */
  cursor?: RunDeadlineCursor;
  /** Persists progress after every selected row, including failed rows. */
  advanceCursor?(cursor: RunDeadlineCursor): Promise<void>;
  /** Routes the CAS through the run's owner Durable Object. */
  transition(candidate: RunDeadlineCandidate, now: number): Promise<void>;
}

/**
 * Read-only deadline enumeration. Every mutation is delegated to the owner DO;
 * timed-out rows whose terminal cleanup is incomplete remain resumable cursors.
 */
export async function sweepExpiredRunDeadlines(
  db: SnapshotDatabase,
  options: SweepExpiredRunDeadlinesOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = (options.now ?? Date.now)();
  const limit = options.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
    throw new Error('deadline sweep limit must be an integer from 1 to 1000');
  }
  if (options.cursor !== undefined && options.advanceCursor === undefined) {
    throw new Error('deadline sweep cursor requires advanceCursor');
  }
  const cursor = options.cursor;
  if (
    cursor &&
    (!Number.isFinite(cursor.deadlineAt) ||
      typeof cursor.workflowId !== 'string' ||
      typeof cursor.runId !== 'string')
  ) {
    throw new Error('deadline sweep cursor is malformed');
  }
  const livePlaceholders = DEADLINE_LIVE_STATUSES.map(() => '?').join(', ');
  const cursorOrder = cursor
    ? `CASE WHEN (
         deadline_at > ?
         OR (deadline_at = ? AND workflow_name > ?)
         OR (deadline_at = ? AND workflow_name = ? AND run_id > ?)
       ) THEN 0 ELSE 1 END,`
    : '';
  const cursorBindings = cursor
    ? [
        cursor.deadlineAt,
        cursor.deadlineAt,
        cursor.workflowId,
        cursor.deadlineAt,
        cursor.workflowId,
        cursor.runId,
      ]
    : [];
  let rows: Array<{
    workflow_name: string;
    run_id: string;
    revision: number;
    deadline_at: number;
  }>;
  try {
    ({ results: rows } = await db
      .prepare(
        `WITH deadline_candidates AS (
         SELECT workflow_name, run_id,
                CASE
                  WHEN json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.status') = 'timed_out'
                  THEN json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.expectedRevision')
                  ELSE json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".revision')
                END AS revision,
                json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".deadlineAt') AS deadline_at
         FROM ${prefix}mastra_workflow_snapshot
         WHERE CASE WHEN json_valid(snapshot) THEN
           (
             json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".deadlineAt') <= ?
             AND NOT EXISTS (
               SELECT 1
               FROM json_each(
                 snapshot,
                 '$.requestContext."flowsafe.runLifecycle".economicOperations'
               ) AS operation
               WHERE json_extract(operation.value, '$.settlementState') = 'disputed'
             )
             AND (
               json_extract(snapshot, '$.status') IN (${livePlaceholders})
               OR (
                 json_extract(snapshot, '$.status') = 'timed_out'
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal.cleanupCompletedAt') IS NULL
               )
               OR (
                 json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".transitionIntent.status') = 'timed_out'
                 AND json_extract(snapshot, '$.requestContext."flowsafe.runLifecycle".terminal') IS NULL
               )
             )
           ) ELSE 0 END
         )
         SELECT workflow_name, run_id, revision, deadline_at
         FROM deadline_candidates
         ORDER BY ${cursorOrder} deadline_at, workflow_name, run_id
         LIMIT ?`,
      )
      .bind(now, ...DEADLINE_LIVE_STATUSES, ...cursorBindings, limit)
      .all<{
        workflow_name: string;
        run_id: string;
        revision: number;
        deadline_at: number;
      }>());
  } catch (error) {
    if (!isMissingTable(error, `${prefix}mastra_workflow_snapshot`))
      throw error;
    return 0;
  }
  const failures: string[] = [];
  let processed = 0;
  for (const row of rows) {
    const rowCursor = {
      workflowId: row.workflow_name,
      runId: row.run_id,
      deadlineAt: row.deadline_at,
    };
    const malformed =
      !isPathSafeId(row.workflow_name) ||
      !isPathSafeId(row.run_id) ||
      !Number.isSafeInteger(row.revision) ||
      row.revision < 1 ||
      !Number.isSafeInteger(row.deadline_at) ||
      row.deadline_at < 0;
    if (malformed) {
      failures.push(`${row.workflow_name}/${row.run_id}: malformed deadline`);
    } else {
      try {
        await options.transition(
          {
            workflowId: row.workflow_name,
            runId: row.run_id,
            revision: row.revision,
            deadlineAt: row.deadline_at,
          },
          now,
        );
        processed += 1;
      } catch (error) {
        failures.push(
          `${row.workflow_name}/${row.run_id}: ${errorMessageOf(error)}`,
        );
      }
    }
    try {
      await options.advanceCursor?.(rowCursor);
    } catch (error) {
      failures.push(
        `${row.workflow_name}/${row.run_id}: cursor ${errorMessageOf(error)}`,
      );
    }
  }
  if (failures.length > 0) {
    throw new Error(
      `sweepExpiredRunDeadlines: ${failures.length} of ${rows.length} run(s) failed (${failures.join('; ')})`,
    );
  }
  return processed;
}

/** Structural: R2ArtifactStore.deleteRun, without importing the artifacts module. */
export interface RunArtifactPurger {
  deleteRun(workflowId: string, runId: string): Promise<number>;
}

export interface RunRetentionScanPosition {
  readonly afterRowId: number;
  readonly highWaterRowId: number;
}

export interface RunRetentionCursor {
  readonly version: 1;
  readonly tablePrefix: string;
  readonly startIdempotencyTable?: string;
  readonly snapshots?: RunRetentionScanPosition;
  readonly reservations?: RunRetentionScanPosition;
}

export interface PurgeExpiredRunsOptions {
  /** Terminal snapshot retention measured from updatedAt. */
  ttlMs: number;
  tablePrefix?: string;
  /** Artifact deletion precedes the guarded snapshot transaction. */
  artifactStore?: RunArtifactPurger;
  resourceOwnerTable?: string;
  startIdempotencyTable?: string;
  /** Defaults to ttlMs and cannot shorten the snapshot retention horizon. */
  startIdempotencyTtlMs?: number;
  /** Physical rows scanned per phase, capped at 90. */
  limit?: number;
  now?: () => number;
  cursor?: RunRetentionCursor;
  advanceCursor: (next: RunRetentionCursor) => Promise<void>;
}

export const RUN_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_workflow_snapshot',
];

export const RUN_TTL_FLOWSAFE_PURGE_TABLES: readonly string[] = [
  RESOURCE_OWNER_TABLE,
  START_IDEMPOTENCY_TABLE,
];

const RETENTION_SQL_BYTES = 90_000;
const RETENTION_SELECTOR_BYTES = 1_000_000;
const RETENTION_FRAGMENT_BYTES = 4096;
const RETENTION_NAMESPACES = 64;
const RETENTION_PAGE = 90;
const RETENTION_SUFFIX = 'mastra_workflow_snapshot';
const RETENTION_PATH = '$.requestContext."flowsafe.runProvenance"';
const RETENTION_OWNED_KEYS = [
  'version',
  'startToken',
  'startIdentity',
  'agentStart',
] as const;
const retentionEncoder = new TextEncoder();

type RetentionDatabase = SnapshotDatabase &
  Required<Pick<SnapshotDatabase, 'batch'>>;
type RetentionOwned = readonly (string | null)[];
type RetentionStartTuple = readonly [
  string,
  string,
  string,
  string,
  string,
  string,
  string | null,
];
type RetentionSelector = readonly [
  string,
  string,
  RetentionOwned,
  RetentionStartTuple | null,
];
interface RetentionSchema {
  names: string[];
  table?: string;
  stage?: StartReservationSchemaStage;
  bindings: [string, string | null, string | null];
}
interface RetentionStatement {
  sql: string;
  values: unknown[];
}
interface RetentionPage {
  rows: Record<string, unknown>[];
  position?: RunRetentionScanPosition;
}
interface RetentionOrphan {
  raw: Record<string, unknown>;
  namespace?: string;
  observation: 'legacy' | 'missing' | 'absent' | RetentionOwned;
}

function retentionRegistry(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value))
    throw new Error('retention registry must be a safe SQL identifier');
  if (value.length >= RETENTION_SQL_BYTES)
    throw new Error('retention registry exceeds SQL byte budget');
  return value.toLowerCase();
}

function retentionRecord(
  value: unknown,
  keys: readonly string[],
): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Error('run retention cursor is malformed');
  const captured: Record<string, unknown> = {};
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== 'string' || !keys.includes(key))
      throw new Error('run retention cursor is malformed');
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !('value' in descriptor))
      throw new Error('run retention cursor is malformed');
    captured[key] = descriptor.value;
  }
  return captured;
}

/** @internal */
export function parseRunRetentionCursor(
  value: unknown,
): RunRetentionCursor | undefined {
  if (value === undefined) return undefined;
  const {
    version,
    tablePrefix,
    startIdempotencyTable,
    snapshots,
    reservations,
  } = retentionRecord(value, [
    'version',
    'tablePrefix',
    'startIdempotencyTable',
    'snapshots',
    'reservations',
  ]);
  if (version !== 1 || typeof tablePrefix !== 'string')
    throw new Error('run retention cursor is malformed');
  const prefix = validateTablePrefix(tablePrefix)?.toLowerCase() ?? '';
  const table = retentionRegistry(startIdempotencyTable);
  const capturePosition = (
    position: unknown,
  ): RunRetentionScanPosition | undefined => {
    if (position === undefined) return undefined;
    const { afterRowId, highWaterRowId } = retentionRecord(position, [
      'afterRowId',
      'highWaterRowId',
    ]);
    if (
      typeof afterRowId !== 'number' ||
      !Number.isSafeInteger(afterRowId) ||
      typeof highWaterRowId !== 'number' ||
      !Number.isSafeInteger(highWaterRowId) ||
      afterRowId > highWaterRowId
    )
      throw new Error('run retention position is malformed');
    return Object.freeze({ afterRowId, highWaterRowId });
  };
  const snapshotPosition = capturePosition(snapshots);
  const reservationPosition = capturePosition(reservations);
  if (reservationPosition && table === undefined)
    throw new Error('reservation cursor requires a registry');
  return Object.freeze({
    version: 1,
    tablePrefix: prefix,
    ...(table === undefined ? {} : { startIdempotencyTable: table }),
    ...(snapshotPosition === undefined ? {} : { snapshots: snapshotPosition }),
    ...(reservationPosition === undefined
      ? {}
      : { reservations: reservationPosition }),
  });
}

function retentionStatement(
  sql: string,
  values: unknown[],
): RetentionStatement {
  if (
    retentionEncoder.encode(sql).length > RETENTION_SQL_BYTES ||
    values.length > 100
  )
    throw new Error('run retention statement exceeds SQL or binding budget');
  return { sql, values };
}

function retentionSelectorJson(values: readonly unknown[]): string {
  const json = JSON.stringify(values);
  if (retentionEncoder.encode(json).length > RETENTION_SELECTOR_BYTES)
    throw new Error('run retention selector exceeds byte budget');
  return json;
}

function prepareRetentionStatement(
  db: SnapshotDatabase,
  statement: RetentionStatement,
): SnapshotStatement {
  return db.prepare(statement.sql).bind(...statement.values);
}

async function observeRunRetentionSchema(
  db: SnapshotDatabase,
  table: string | undefined,
): Promise<RetentionSchema> {
  const names = snapshotResultRows(
    await db
      .prepare(`SELECT name, type FROM sqlite_schema
    WHERE type IN ('table', 'view') AND lower(name) GLOB '*${RETENTION_SUFFIX}'
    ORDER BY lower(name) LIMIT ${RETENTION_NAMESPACES + 1}`)
      .all(),
  );
  if (names.length > RETENTION_NAMESPACES)
    throw new Error('run retention namespace overflow');
  const canonical: string[] = [];
  for (const row of names) {
    if (
      row.type !== 'table' ||
      typeof row.name !== 'string' ||
      !row.name.toLowerCase().endsWith(RETENTION_SUFFIX)
    )
      throw new Error('run retention namespace is not a supported table');
    validateTablePrefix(row.name.slice(0, -RETENTION_SUFFIX.length));
    const name = row.name.toLowerCase();
    if (canonical.includes(name))
      throw new Error('run retention duplicate namespace');
    canonical.push(name);
  }
  let stage: StartReservationSchemaStage | undefined;
  let metadata: unknown[][] | null = null;
  if (table !== undefined) {
    const rows = snapshotResultRows(
      await prepareRetentionStatement(
        db,
        retentionStatement(
          `SELECT s.type AS schema_type,
      p.cid, p.name, p.type, p."notnull", p.dflt_value, p.pk, p.hidden
      FROM sqlite_schema s LEFT JOIN pragma_table_xinfo(?1) p ON 1
      WHERE lower(s.name)=?1 AND s.type IN ('table','view') ORDER BY p.cid LIMIT 14`,
          [table],
        ),
      ).all(),
    );
    if (rows.length > 0) {
      if (
        rows.some(
          (row, index) => row.schema_type !== 'table' || row.cid !== index,
        )
      )
        throw new Error('run retention reservation schema is unsupported');
      stage = reservationSchemaStage({ results: rows });
      if (stage === undefined)
        throw new Error('run retention reservation schema is empty');
      metadata = rows.map((row) => [
        row.cid,
        row.name,
        row.type,
        row.notnull,
        row.dflt_value,
        row.pk,
        row.hidden,
      ]);
    }
  }
  return {
    names: canonical,
    table,
    stage,
    bindings: [
      JSON.stringify(canonical),
      table ?? null,
      metadata === null ? null : JSON.stringify(metadata),
    ],
  };
}

const RETENTION_SCHEMA_SQL = `WITH expected_names(name) AS (SELECT value FROM json_each(?1)),
current_names(name,type) AS (
 SELECT lower(name),type FROM sqlite_schema WHERE type IN ('table','view')
 AND lower(name) GLOB '*${RETENTION_SUFFIX}' LIMIT ${RETENTION_NAMESPACES + 1}
), ecols(cid,name,type,nn,dflt,pk,hidden) AS (
 SELECT json_extract(value,'$[0]'),json_extract(value,'$[1]'),json_extract(value,'$[2]'),
 json_extract(value,'$[3]'),json_extract(value,'$[4]'),json_extract(value,'$[5]'),json_extract(value,'$[6]') FROM json_each(?3)
), ccols(cid,name,type,nn,dflt,pk,hidden) AS (
 SELECT cid,name,type,"notnull",dflt_value,pk,hidden FROM pragma_table_xinfo(?2) ORDER BY cid LIMIT 14
), schema_ok(ok) AS (SELECT
 (SELECT count(*) FROM current_names)=(SELECT count(*) FROM expected_names)
 AND NOT EXISTS (SELECT name FROM current_names EXCEPT SELECT name FROM expected_names)
 AND NOT EXISTS (SELECT 1 FROM current_names WHERE type COLLATE BINARY <> 'table')
 AND CASE WHEN ?2 IS NULL THEN 1 WHEN ?3 IS NULL THEN NOT EXISTS (
 SELECT 1 FROM sqlite_schema WHERE lower(name)=?2 AND type IN ('table','view'))
 ELSE EXISTS (SELECT 1 FROM sqlite_schema WHERE lower(name)=?2 AND type='table')
 AND (SELECT count(*) FROM ccols)=(SELECT count(*) FROM ecols)
 AND NOT EXISTS (SELECT * FROM ccols EXCEPT SELECT * FROM ecols) END)`;

function retentionPathGuard(): string {
  return `json_type(s.snapshot,'$')='object'
    AND (SELECT count(*) FROM json_each(s.snapshot) WHERE key COLLATE BINARY='requestContext') <= 1
    AND (json_type(s.snapshot,'$.requestContext') IS NULL OR json_type(s.snapshot,'$.requestContext')='object')
    AND (SELECT count(*) FROM json_each(s.snapshot,'$.requestContext') WHERE key COLLATE BINARY='flowsafe.runProvenance') <= 1
    AND NOT EXISTS (SELECT key FROM json_each(s.snapshot,'${RETENTION_PATH}')
      WHERE key COLLATE BINARY IN ('version','startToken','startIdentity','agentStart') GROUP BY key COLLATE BINARY HAVING count(*) > 1)`;
}

function retentionOwnedExpressions(): string[] {
  return RETENTION_OWNED_KEYS.map(
    (key) => `s.snapshot -> '${RETENTION_PATH}.${key}'`,
  );
}

function retentionOwnedProjection(): string {
  const expressions = retentionOwnedExpressions();
  const size = expressions
    .map((expression) => `COALESCE(length(CAST((${expression}) AS BLOB)),0)`)
    .join('+');
  const bounded = `CASE WHEN json_valid(s.snapshot) THEN (${size})<=${RETENTION_FRAGMENT_BYTES} ELSE 0 END`;
  return `CASE WHEN json_valid(s.snapshot) THEN ${retentionPathGuard()} ELSE 0 END AS path_ok,
    CASE WHEN json_valid(s.snapshot) THEN json_type(s.snapshot,'${RETENTION_PATH}') END AS provenance_type,
    ${bounded} AS owned_ok,
    ${expressions.map((expression, index) => `CASE WHEN ${bounded} THEN ${expression} END AS owned_${index}`).join(',')}`;
}

function retentionOwnedEquality(path: string): string {
  return `CASE WHEN json_valid(s.snapshot) THEN json_type(s.snapshot,'${RETENTION_PATH}')='object'
    AND ${retentionPathGuard()} AND ${retentionOwnedExpressions()
      .map(
        (expression, index) =>
          `(${expression}) COLLATE BINARY IS json_extract(c.value,'${path}[${index}]')`,
      )
      .join(' AND ')} ELSE 0 END`;
}

function decodeRunRetentionCandidate(
  row: Record<string, unknown>,
  prefix: string,
): RetentionSelector | 'legacy' | undefined {
  for (const key of [
    'workflow_name',
    'run_id',
    'path_ok',
    'owned_ok',
    'provenance_type',
    ...RETENTION_OWNED_KEYS.map((_, index) => `owned_${index}`),
  ]) {
    if (!Object.hasOwn(row, key))
      throw new Error('run retention capsule projection is incomplete');
  }
  if (
    ![0, 1, null].includes(row.path_ok as number | null) ||
    ![0, 1].includes(row.owned_ok as number) ||
    ![
      null,
      'object',
      'array',
      'text',
      'integer',
      'real',
      'true',
      'false',
      'null',
    ].includes(row.provenance_type as string | null)
  )
    throw new Error('run retention capsule projection is malformed');
  const owned = RETENTION_OWNED_KEYS.map((_, index) => row[`owned_${index}`]);
  if (owned.some((value) => value !== null && typeof value !== 'string'))
    throw new Error('run retention capsule projection is malformed');
  let fragments: unknown[];
  try {
    fragments = owned.map((value) =>
      typeof value === 'string' ? JSON.parse(value) : undefined,
    );
  } catch {
    throw new Error('run retention capsule projection is malformed');
  }
  if (
    !isPathSafeId(row.workflow_name) ||
    !isPathSafeId(row.run_id) ||
    row.path_ok !== 1 ||
    row.owned_ok !== 1
  )
    return undefined;
  try {
    let value: Record<string, unknown> | undefined;
    if (row.provenance_type !== null) {
      if (row.provenance_type !== 'object') return undefined;
      const object: Record<string, unknown> = {};
      RETENTION_OWNED_KEYS.forEach((key, index) => {
        if (typeof owned[index] === 'string') object[key] = fragments[index];
      });
      value = object;
    }
    const decoded = decodeRunStartIdentity(value);
    if (decoded === undefined) return 'legacy';
    const execution = normalizeD1RunExecutionIdentity(
      runExecutionIdentityFor(
        {
          tablePrefix: prefix,
          workflowId: row.workflow_name,
          runId: row.run_id,
        },
        decoded,
      ),
    );
    const start =
      decoded.startIdentity === undefined
        ? undefined
        : normalizeStartExecutionIdentity({
            ...execution,
            ...decoded.startIdentity,
          });
    return [
      execution.workflowId,
      execution.runId,
      owned as RetentionOwned,
      start === undefined
        ? null
        : [
            prefix,
            start.startToken,
            start.owner.kind,
            start.owner.id,
            start.target.kind,
            start.target.id,
            start.target.kind === 'agent' ? start.target.threadId : null,
          ],
    ];
  } catch {
    return undefined;
  }
}

function retentionTerminalSql(first: number): string {
  let binding = first;
  return RUN_TERMINAL_SNAPSHOT_SQL.replace(/\bsnapshot\b/g, 's.snapshot')
    .replace(/\?/g, () => `?${binding++}`)
    .replace(
      /json_extract\(s.snapshot, '\$\.status'\)/g,
      "json_extract(s.snapshot, '$.status') COLLATE BINARY",
    );
}

function retentionPageStatement(
  table: string,
  position: RunRetentionScanPosition | undefined,
  limit: number,
  projection: string,
  extra: unknown[] = [],
): RetentionStatement {
  return retentionStatement(
    `WITH bounds(h) AS MATERIALIZED (SELECT COALESCE(?1,(SELECT MAX(rowid) FROM "${table}"))),
    page AS (SELECT rowid AS rid FROM "${table}",bounds WHERE rowid <= h ${position ? 'AND rowid > ?2' : ''} ORDER BY rowid LIMIT ?3)
    SELECT b.h,p.rid,${projection} FROM bounds b LEFT JOIN page p ON 1
    LEFT JOIN "${table}" s ON s.rowid=p.rid ORDER BY p.rid`,
    [
      position?.highWaterRowId ?? null,
      position?.afterRowId ?? null,
      limit,
      ...extra,
    ],
  );
}

function decodeRetentionPage(
  result: unknown,
  position: RunRetentionScanPosition | undefined,
  limit: number,
): RetentionPage {
  const rows = snapshotResultRows(result);
  if (rows.length === 0 || rows.length > limit)
    throw new Error('run retention page is malformed');
  const highWater = rows[0]?.h;
  if (
    highWater !== null &&
    (typeof highWater !== 'number' || !Number.isSafeInteger(highWater))
  )
    throw new Error('run retention high water is malformed');
  if (position && highWater !== position.highWaterRowId)
    throw new Error('run retention high water changed');
  let previous = position?.afterRowId;
  for (const row of rows) {
    if (row.h !== highWater)
      throw new Error('run retention high water disagrees');
    if (row.rid === null && rows.length === 1) return { rows: [] };
    if (
      typeof row.rid !== 'number' ||
      !Number.isSafeInteger(row.rid) ||
      typeof highWater !== 'number' ||
      row.rid > highWater ||
      (previous !== undefined && row.rid <= previous)
    )
      throw new Error('run retention row position is malformed');
    previous = row.rid;
  }
  if (previous === undefined || typeof highWater !== 'number')
    throw new Error('run retention page is malformed');
  return {
    rows,
    ...(rows.length < limit || previous === highWater
      ? {}
      : {
          position: Object.freeze({
            afterRowId: previous,
            highWaterRowId: highWater,
          }),
        }),
  };
}

function retentionMembership(
  names: readonly string[],
  selector: string,
): string {
  if (names.length === 0) return 'SELECT NULL AS run_id WHERE 0';
  const compoundLimit = 5;
  const grouped = names.length > compoundLimit;
  const definitions = grouped
    ? [`retention_ids(run_id) AS MATERIALIZED (${selector})`]
    : [];
  let selects = names.map(
    (name) =>
      `SELECT DISTINCT run_id COLLATE BINARY AS run_id FROM "${name}" WHERE run_id COLLATE BINARY IN (${grouped ? 'SELECT run_id FROM retention_ids' : selector})`,
  );
  // Materialized groups prevent flattening beyond workerd's compound limit.
  while (selects.length > compoundLimit) {
    const next: string[] = [];
    for (let index = 0; index < selects.length; index += compoundLimit) {
      const name = `retention_members_${definitions.length}`;
      definitions.push(
        `${name}(run_id) AS MATERIALIZED (${selects.slice(index, index + compoundLimit).join(' UNION ALL ')})`,
      );
      next.push(`SELECT run_id FROM ${name}`);
    }
    selects = next;
  }
  return `${definitions.length > 0 ? `WITH ${definitions.join(', ')} ` : ''}${selects.join(' UNION ALL ')}`;
}

function retentionSnapshotGroup(
  schema: RetentionSchema,
  table: string,
  owner: string | undefined,
  selectors: readonly RetentionSelector[],
  raw: RawWorkflowSnapshot | undefined,
  cutoff: string,
  keyCutoff: number,
  now: number,
): RetentionStatement[] {
  const result = [
    retentionStatement(
      `${RETENTION_SCHEMA_SQL} SELECT ok AS schema_ok FROM schema_ok`,
      [...schema.bindings],
    ),
  ];
  const json = retentionSelectorJson(selectors);
  if (schema.names.includes(table)) {
    result.push(
      raw
        ? retentionStatement(
            `${RETENTION_SCHEMA_SQL} DELETE FROM "${table}" AS s WHERE (SELECT ok FROM schema_ok)=1
      AND s.workflow_name COLLATE BINARY=?4 AND s.run_id COLLATE BINARY=?5 AND s.snapshot COLLATE BINARY=?6
      AND s.createdAt COLLATE BINARY IS ?7 AND s.updatedAt COLLATE BINARY IS ?8 AND s.resourceId COLLATE BINARY IS ?9
      AND s.updatedAt COLLATE BINARY < ?10 AND CASE WHEN json_valid(s.snapshot) THEN (${retentionTerminalSql(11)}) AND ${retentionPathGuard()} ELSE 0 END`,
            [
              ...schema.bindings,
              raw.workflowId,
              raw.runId,
              raw.snapshot,
              raw.createdAt,
              raw.updatedAt,
              raw.resourceId,
              cutoff,
              ...RUN_TERMINAL_STATUSES,
            ],
          )
        : retentionStatement(
            `${RETENTION_SCHEMA_SQL} DELETE FROM "${table}" AS s WHERE (SELECT ok FROM schema_ok)=1
      AND s.updatedAt COLLATE BINARY < ?5 AND CASE WHEN json_valid(s.snapshot) THEN (${retentionTerminalSql(6)}) ELSE 0 END
      AND EXISTS (SELECT 1 FROM json_each(?4) c WHERE s.workflow_name COLLATE BINARY=json_extract(c.value,'$[0]')
      AND s.run_id COLLATE BINARY=json_extract(c.value,'$[1]') AND ${retentionOwnedEquality('$[2]')})`,
            [...schema.bindings, json, cutoff, ...RUN_TERMINAL_STATUSES],
          ),
    );
  }
  if (owner !== undefined) {
    const candidates = "SELECT json_extract(value,'$[1]') FROM json_each(?4)";
    result.push(
      retentionStatement(
        `${RETENTION_SCHEMA_SQL}, present(run_id) AS (${retentionMembership(schema.names, candidates)})
      DELETE FROM "${owner}" WHERE (SELECT ok FROM schema_ok)=1 AND resource_kind COLLATE BINARY='run' AND reservation_token IS NULL
      AND resource_id COLLATE BINARY IN (${candidates}) AND NOT EXISTS (SELECT 1 FROM present WHERE present.run_id COLLATE BINARY=resource_id COLLATE BINARY)`,
        [...schema.bindings, json],
      ),
    );
  }
  if (!raw && schema.stage === 3 && schema.table !== undefined) {
    const absence = schema.names.includes(table)
      ? `NOT EXISTS (SELECT 1 FROM "${table}" s WHERE s.workflow_name COLLATE BINARY=json_extract(c.value,'$[0]') AND s.run_id COLLATE BINARY=json_extract(c.value,'$[1]'))`
      : '1';
    const fields = [
      'start_table_prefix',
      'start_token',
      'owner_kind',
      'owner_id',
      'target_kind',
      'target_id',
      'thread_id',
    ];
    const pair = `EXISTS (SELECT 1 FROM json_each(?4) c WHERE json_type(c.value,'$[3]')='array' AND
      ${fields.map((field, index) => `r.${field} COLLATE BINARY IS json_extract(c.value,'$[3][${index}]')`).join(' AND ')}
      AND r.start_workflow_id COLLATE BINARY=json_extract(c.value,'$[0]') AND r.run_id COLLATE BINARY=json_extract(c.value,'$[1]') AND ${absence})`;
    result.push(
      retentionStatement(
        `${RETENTION_SCHEMA_SQL} DELETE FROM "${schema.table}" AS r WHERE (SELECT ok FROM schema_ok)=1
      AND r.state COLLATE BINARY='terminal' AND ${retentionFiniteExpiry('r.updated_at', '?5')} AND ${pair}`,
        [...schema.bindings, json, keyCutoff],
      ),
    );
    result.push(
      retentionStatement(
        `${RETENTION_SCHEMA_SQL} UPDATE "${schema.table}" AS r SET state='terminal', updated_at=?5
      WHERE (SELECT ok FROM schema_ok)=1 AND r.state COLLATE BINARY IN ('reserved','started') AND ${pair}`,
        [...schema.bindings, json, now],
      ),
    );
  }
  return result;
}

function retentionFiniteExpiry(column: string, cutoff: string): string {
  return `typeof(${column}) IN ('integer','real') AND ${column} BETWEEN -1.7976931348623157e308 AND 1.7976931348623157e308 AND ${column} < ${cutoff}`;
}

function retentionBatchResult(
  value: unknown,
  count: number,
): { schemaOk: boolean; deleted: number } {
  if (!Array.isArray(value) || value.length !== count)
    throw new Error('run retention batch result is malformed');
  let deleted = 0;
  let schemaOk = false;
  for (let index = 0; index < count; index += 1) {
    if (!Object.hasOwn(value, index))
      throw new Error('run retention batch result is sparse');
    const result = value[index];
    if (
      result === null ||
      typeof result !== 'object' ||
      Array.isArray(result) ||
      ('success' in result && result.success !== true)
    )
      throw new Error('run retention batch result failed');
    if (index === 0) {
      const rows = snapshotResultRows(result);
      if (
        rows.length !== 1 ||
        (rows[0]?.schema_ok !== 0 && rows[0]?.schema_ok !== 1)
      )
        throw new Error('run retention schema result is malformed');
      schemaOk = rows[0]?.schema_ok === 1;
    } else {
      const changes = result.meta?.changes;
      if (
        typeof changes !== 'number' ||
        !Number.isSafeInteger(changes) ||
        changes < 0 ||
        (!schemaOk && changes !== 0)
      )
        throw new Error('run retention mutation result is uncertain');
      if (index === 1) deleted = changes;
    }
  }
  return { schemaOk, deleted };
}

function retentionReservationProjection(
  stage: StartReservationSchemaStage,
): string {
  const columns = START_IDEMPOTENCY_COLUMNS.slice(0, 10 + stage).map(
    ([name]) => name,
  );
  const scalars = columns
    .map((name) => `typeof(s."${name}") IN ('null','text','integer','real')`)
    .join(' AND ');
  const size = columns
    .map((name) => `COALESCE(length(CAST(s."${name}" AS BLOB)),0)`)
    .join('+');
  const json = `json_object(${columns.map((name) => `'${name}',s."${name}"`).join(',')})`;
  return `CASE WHEN ${scalars} AND (${size}) <= ${RETENTION_FRAGMENT_BYTES} THEN CASE WHEN length(CAST(${json} AS BLOB)) <= ${RETENTION_FRAGMENT_BYTES} THEN ${json} END END AS raw`;
}

function decodeRetentionReservationProjection(
  row: Record<string, unknown>,
  stage: StartReservationSchemaStage,
): Record<string, unknown> | undefined {
  if (row.raw === null) return undefined;
  if (typeof row.raw !== 'string')
    throw new Error('run retention reservation projection is malformed');
  let raw: unknown;
  try {
    raw = JSON.parse(row.raw);
  } catch {
    throw new Error('run retention reservation projection is malformed');
  }
  const columns = START_IDEMPOTENCY_COLUMNS.slice(0, 10 + stage).map(
    ([name]) => name,
  );
  if (
    raw === null ||
    typeof raw !== 'object' ||
    Array.isArray(raw) ||
    Object.keys(raw).length !== columns.length ||
    columns.some((name) => !Object.hasOwn(raw, name)) ||
    Object.values(raw).some(
      (value) =>
        value !== null &&
        typeof value !== 'string' &&
        typeof value !== 'number',
    )
  )
    throw new Error('run retention reservation projection is malformed');
  return raw as Record<string, unknown>;
}

function retentionOrphanGroup(
  schema: RetentionSchema,
  orphans: readonly RetentionOrphan[],
  keyCutoff: number,
): RetentionStatement[] {
  const first = orphans[0];
  if (!first || schema.stage === undefined)
    throw new Error('run retention orphan group is empty');
  const json = retentionSelectorJson(
    orphans.map((orphan) => [
      orphan.raw,
      Array.isArray(orphan.observation) ? orphan.observation : null,
    ]),
  );
  const columns = START_IDEMPOTENCY_COLUMNS.slice(0, 10 + schema.stage).map(
    ([name]) => name,
  );
  const exact = columns
    .map(
      (name) =>
        `r."${name}" COLLATE BINARY IS json_extract(c.value,'$[0].${name}')`,
    )
    .join(' AND ');
  let guard: string;
  let membership = '';
  if (first.observation === 'legacy') {
    membership = `, present(run_id) AS (${retentionMembership(schema.names, "SELECT json_extract(value,'$[0].run_id') FROM json_each(?4)")})`;
    guard =
      'NOT EXISTS (SELECT 1 FROM present WHERE run_id COLLATE BINARY=r.run_id COLLATE BINARY)';
  } else if (first.observation === 'missing') guard = '1';
  else {
    const address = `s.workflow_name COLLATE BINARY=json_extract(c.value,'$[0].start_workflow_id') AND s.run_id COLLATE BINARY=json_extract(c.value,'$[0].run_id')`;
    guard =
      first.observation === 'absent'
        ? `NOT EXISTS (SELECT 1 FROM "${first.namespace}" s WHERE ${address})`
        : `EXISTS (SELECT 1 FROM "${first.namespace}" s WHERE ${address} AND ${retentionOwnedEquality('$[1]')})`;
  }
  return [
    retentionStatement(
      `${RETENTION_SCHEMA_SQL} SELECT ok AS schema_ok FROM schema_ok`,
      [...schema.bindings],
    ),
    retentionStatement(
      `${RETENTION_SCHEMA_SQL}${membership} DELETE FROM "${schema.table}" AS r WHERE (SELECT ok FROM schema_ok)=1
      AND r.state COLLATE BINARY='terminal' AND ${retentionFiniteExpiry('r.updated_at', '?5')}
      AND EXISTS (SELECT 1 FROM json_each(?4) c WHERE ${exact} AND ${guard})`,
      [...schema.bindings, json, keyCutoff],
    ),
  ];
}

function retentionOrphanPackets(
  candidates: readonly RetentionOrphan[],
): RetentionOrphan[][] {
  const groups = new Map<string, RetentionOrphan[]>();
  for (const candidate of candidates) {
    const key = `${candidate.namespace ?? ''}/${typeof candidate.observation === 'string' ? candidate.observation : 'different'}`;
    const group = groups.get(key) ?? [];
    group.push(candidate);
    groups.set(key, group);
  }
  const packets: RetentionOrphan[][] = [];
  for (const group of groups.values()) {
    let packet: RetentionOrphan[] = [];
    let bytes = 2;
    for (const candidate of group) {
      const rowBytes = retentionEncoder.encode(
        JSON.stringify([
          candidate.raw,
          Array.isArray(candidate.observation) ? candidate.observation : null,
        ]),
      ).length;
      if (rowBytes + 2 > RETENTION_SELECTOR_BYTES)
        throw new Error('run retention orphan selector exceeds byte budget');
      if (
        bytes + rowBytes + (packet.length ? 1 : 0) >
        RETENTION_SELECTOR_BYTES
      ) {
        packets.push(packet);
        packet = [];
        bytes = 2;
      }
      bytes += rowBytes + (packet.length ? 1 : 0);
      packet.push(candidate);
    }
    if (packet.length) packets.push(packet);
  }
  return packets;
}

/** Delete expired snapshots with durable, independent physical scan positions. */
export async function purgeExpiredWorkflowRuns(
  db: SnapshotDatabase & Required<Pick<SnapshotDatabase, 'batch'>>,
  options: PurgeExpiredRunsOptions,
): Promise<number> {
  const {
    ttlMs,
    tablePrefix,
    artifactStore,
    resourceOwnerTable,
    startIdempotencyTable,
    startIdempotencyTtlMs,
    limit: suppliedLimit,
    now: suppliedNow,
    cursor: suppliedCursor,
    advanceCursor,
  } = options;
  const prepare = db.prepare;
  const batch = db.batch;
  const deleteRun = artifactStore?.deleteRun;
  if (
    typeof prepare !== 'function' ||
    typeof batch !== 'function' ||
    typeof advanceCursor !== 'function' ||
    (artifactStore !== undefined && typeof deleteRun !== 'function')
  )
    throw new Error(
      'purgeExpiredWorkflowRuns requires database.batch(), advanceCursor and a valid artifact callback',
    );
  const captured: RetentionDatabase = {
    prepare: prepare.bind(db),
    batch: batch.bind(db),
  };
  const deleteArtifacts = deleteRun?.bind(artifactStore);
  const prefix = validateTablePrefix(tablePrefix)?.toLowerCase() ?? '';
  const owner = retentionRegistry(resourceOwnerTable);
  const registry = retentionRegistry(startIdempotencyTable);
  let cursor = parseRunRetentionCursor(suppliedCursor);
  if (
    cursor &&
    (cursor.tablePrefix !== prefix || cursor.startIdempotencyTable !== registry)
  )
    throw new Error('run retention cursor scope mismatch');
  if (
    !Number.isFinite(ttlMs) ||
    ttlMs < 0 ||
    (startIdempotencyTtlMs !== undefined &&
      (!Number.isFinite(startIdempotencyTtlMs) || startIdempotencyTtlMs < 0))
  )
    throw new Error('run retention TTL must be finite and nonnegative');
  if (
    suppliedLimit !== undefined &&
    (!Number.isSafeInteger(suppliedLimit) || suppliedLimit <= 0)
  )
    throw new Error('run retention limit must be a positive safe integer');
  const limit = Math.min(suppliedLimit ?? RETENTION_PAGE, RETENTION_PAGE);
  const nowFunction = suppliedNow === undefined ? Date.now : suppliedNow;
  if (typeof nowFunction !== 'function')
    throw new Error('run retention clock is invalid');
  const now = nowFunction();
  const keyCutoff = now - Math.max(ttlMs, startIdempotencyTtlMs ?? ttlMs);
  const snapshotCutoff = now - ttlMs;
  if (
    !Number.isFinite(now) ||
    !Number.isFinite(keyCutoff) ||
    !Number.isFinite(snapshotCutoff)
  )
    throw new Error('run retention cutoff is invalid');
  const cutoff = new Date(snapshotCutoff).toISOString();
  if (!/^\d{4}-/.test(cutoff))
    throw new Error('run retention cutoff must have a four-digit ISO year');
  const table = `${prefix}${RETENTION_SUFFIX}`;
  let schema = await observeRunRetentionSchema(captured, registry);
  let schemaRetryUsed = false;
  let deleted = 0;
  const failures: string[] = [];
  const reportedSkips = new Set<string>();
  const reportUnsupported = (
    kind: 'snapshot' | 'legacy-snapshot' | 'reservation' | 'orphan-snapshot',
  ): void => {
    if (reportedSkips.has(kind)) return;
    reportedSkips.add(kind);
    console.warn(
      JSON.stringify({ type: 'run-retention-skip', kind, tablePrefix: prefix }),
    );
  };
  const knownSkip = (error: unknown) => {
    if (failures.length >= 8) return;
    let message = 'unreadable error';
    try {
      message = errorMessageOf(error).slice(0, 256);
    } catch {}
    failures.push(message);
  };
  const refresh = async () => {
    if (schemaRetryUsed)
      throw new Error('run retention schema changed repeatedly');
    schemaRetryUsed = true;
    schema = await observeRunRetentionSchema(captured, registry);
  };
  const execute = async (
    build: () => RetentionStatement[],
    onRetry?: () => Promise<void>,
  ): Promise<number> => {
    for (;;) {
      const statements = build();
      let result: unknown;
      try {
        const prepared = statements.map((statement) =>
          prepareRetentionStatement(captured, statement),
        );
        result = await captured.batch(prepared);
      } catch (error) {
        let missing = false;
        try {
          missing = [...schema.names, ...(registry ? [registry] : [])].some(
            (name) => missingTableReadsEmpty(error, name),
          );
        } catch {}
        if (!missing) throw error;
        const previous = JSON.stringify(schema.bindings);
        await refresh();
        if (JSON.stringify(schema.bindings) === previous) throw error;
        if (onRetry) await onRetry();
        continue;
      }
      const outcome = retentionBatchResult(result, statements.length);
      if (outcome.schemaOk) return outcome.deleted;
      await refresh();
      if (onRetry) await onRetry();
    }
  };
  const advance = async (
    component: 'snapshots' | 'reservations',
    position: RunRetentionScanPosition | undefined,
  ) => {
    const next = {
      version: 1 as const,
      tablePrefix: prefix,
      ...(registry === undefined ? {} : { startIdempotencyTable: registry }),
      ...cursor,
    };
    delete next[component];
    if (position) next[component] = position;
    cursor = Object.freeze(next);
    await advanceCursor(cursor);
  };
  const snapshotGroup = (
    selectors: readonly RetentionSelector[],
    raw?: RawWorkflowSnapshot,
  ) =>
    retentionSnapshotGroup(
      schema,
      table,
      owner,
      selectors,
      raw,
      cutoff,
      keyCutoff,
      now,
    );
  const artifacts = async (workflowId: string, runId: string) => {
    try {
      await deleteArtifacts?.(workflowId, runId);
      return true;
    } catch (error) {
      knownSkip(error);
      return false;
    }
  };
  if (schema.names.includes(table)) {
    const page = decodeRetentionPage(
      await prepareRetentionStatement(
        captured,
        retentionPageStatement(
          table,
          cursor?.snapshots,
          limit,
          `CASE WHEN length(s.workflow_name)<=200 THEN s.workflow_name END AS workflow_name,
       CASE WHEN length(s.run_id)<=200 THEN s.run_id END AS run_id,
       s.updatedAt COLLATE BINARY < ?4 AND CASE WHEN json_valid(s.snapshot) THEN (${retentionTerminalSql(5)}) ELSE 0 END AS eligible,
       ${retentionOwnedProjection()}`,
          [cutoff, ...RUN_TERMINAL_STATUSES],
        ),
      ).all(),
      cursor?.snapshots,
      limit,
    );
    const modern: RetentionSelector[] = [];
    for (const row of page.rows) {
      if (
        !Object.hasOwn(row, 'eligible') ||
        ![0, 1, null].includes(row.eligible as number | null)
      )
        throw new Error('run retention eligibility projection is malformed');
      if (row.eligible !== 1) continue;
      const candidate = decodeRunRetentionCandidate(row, prefix);
      if (candidate === undefined) {
        reportUnsupported('snapshot');
        continue;
      }
      if (candidate !== 'legacy') {
        modern.push(candidate);
        continue;
      }
      const address = {
        tablePrefix: prefix,
        workflowId: row.workflow_name as string,
        runId: row.run_id as string,
      };
      const read = prepareRawWorkflowSnapshotRead(captured, address);
      const result = await read.statement.all();
      const rawRows = snapshotResultRows(result);
      if (
        rawRows.length > 1 ||
        rawRows.some((row) =>
          [
            'workflow_name',
            'run_id',
            'resourceId',
            'snapshot',
            'createdAt',
            'updatedAt',
          ].some((key) => !Object.hasOwn(row, key)),
        )
      )
        throw new Error('run retention raw snapshot result is malformed');
      let raw: RawWorkflowSnapshot | undefined;
      try {
        raw = decodeRawWorkflowSnapshotResult(result, read.address);
        if (!raw || raw.updatedAt >= cutoff) continue;
        const snapshot = JSON.parse(raw.snapshot);
        const cleanupCompletedAt =
          snapshot.requestContext?.['flowsafe.runLifecycle']?.terminal
            ?.cleanupCompletedAt;
        if (
          decodeRunStartIdentity(
            snapshot.requestContext?.['flowsafe.runProvenance'],
          ) !== undefined ||
          !RUN_TERMINAL_STATUSES.includes(snapshot.status) ||
          ((snapshot.status === 'cancelled' ||
            snapshot.status === 'timed_out') &&
            (!Number.isSafeInteger(cleanupCompletedAt) ||
              cleanupCompletedAt < 0))
        )
          continue;
      } catch {
        reportUnsupported('legacy-snapshot');
        continue;
      }
      const selectors: RetentionSelector[] = [
        [address.workflowId, address.runId, [], null],
      ];
      snapshotGroup(selectors, raw);
      if (await artifacts(address.workflowId, address.runId)) {
        const count = await execute(() => snapshotGroup(selectors, raw));
        if (schema.names.includes(table)) deleted += count;
      }
    }
    if (modern.length > 0) {
      snapshotGroup(modern);
      const ready: RetentionSelector[] = [];
      for (const candidate of modern)
        if (await artifacts(candidate[0], candidate[1])) ready.push(candidate);
      if (ready.length > 0) {
        const count = await execute(() => snapshotGroup(ready));
        if (schema.names.includes(table)) deleted += count;
      }
    }
    await advance('snapshots', page.position);
  } else await advance('snapshots', undefined);

  const classifyOrphans = async (
    raws: Record<string, unknown>[],
  ): Promise<RetentionOrphan[]> => {
    const orphans: RetentionOrphan[] = [];
    const grouped = new Map<string, Record<string, unknown>[]>();
    if (schema.stage === undefined) return orphans;
    for (const raw of raws) {
      try {
        let row = reservationFromRow(raw, schema.stage);
        if (row.state !== 'terminal' || row.updatedAt >= keyCutoff) continue;
        if (row.binding.kind === 'legacy') {
          orphans.push({ raw, observation: 'legacy' });
          continue;
        }
        row = admissionReservationFromRow(raw, schema.stage);
        if (
          row.binding.kind !== 'bound' ||
          row.binding.execution.tablePrefix === null
        )
          continue;
        const namespace = `${row.binding.execution.tablePrefix}${RETENTION_SUFFIX}`;
        if (!schema.names.includes(namespace)) {
          orphans.push({ raw, namespace, observation: 'missing' });
          continue;
        }
        const values = grouped.get(namespace) ?? [];
        values.push(raw);
        grouped.set(namespace, values);
      } catch {
        reportUnsupported('reservation');
      }
    }
    for (const [namespace, values] of grouped) {
      const selectors = retentionSelectorJson(
        values.map((raw) => [raw.start_workflow_id, raw.run_id]),
      );
      const rows = snapshotResultRows(
        await prepareRetentionStatement(
          captured,
          retentionStatement(
            `SELECT c.key AS candidate, s.rowid AS present,
        json_extract(c.value,'$[0]') AS workflow_name, json_extract(c.value,'$[1]') AS run_id, ${retentionOwnedProjection()}
        FROM json_each(?1) c LEFT JOIN "${namespace}" s ON s.workflow_name COLLATE BINARY=json_extract(c.value,'$[0]')
        AND s.run_id COLLATE BINARY=json_extract(c.value,'$[1]') ORDER BY c.key`,
            [selectors],
          ),
        ).all(),
      );
      if (rows.length !== values.length)
        throw new Error('run retention orphan observation is malformed');
      for (const [index, row] of rows.entries()) {
        if (row.candidate !== index || !Object.hasOwn(row, 'present'))
          throw new Error('run retention orphan observation is malformed');
        const raw = values[index];
        if (!raw) throw new Error('run retention orphan candidate is missing');
        if (
          row.workflow_name !== raw.start_workflow_id ||
          row.run_id !== raw.run_id
        )
          throw new Error('run retention orphan address is malformed');
        if (
          row.present !== null &&
          (typeof row.present !== 'number' ||
            !Number.isSafeInteger(row.present))
        )
          throw new Error('run retention orphan rowid is malformed');
        const decoded = decodeRunRetentionCandidate(
          row,
          namespace.slice(0, -RETENTION_SUFFIX.length),
        );
        if (row.present === null) {
          orphans.push({ raw, namespace, observation: 'absent' });
          continue;
        }
        if (decoded === undefined) reportUnsupported('orphan-snapshot');
        if (
          decoded &&
          decoded !== 'legacy' &&
          typeof decoded[2][1] === 'string' &&
          JSON.parse(decoded[2][1]) !== raw.start_token
        )
          orphans.push({ raw, namespace, observation: decoded[2] });
      }
    }
    return orphans;
  };
  if (schema.stage !== undefined && registry !== undefined) {
    const page = decodeRetentionPage(
      await prepareRetentionStatement(
        captured,
        retentionPageStatement(
          registry,
          cursor?.reservations,
          limit,
          retentionReservationProjection(schema.stage),
        ),
      ).all(),
      cursor?.reservations,
      limit,
    );
    const raws: Record<string, unknown>[] = [];
    for (const row of page.rows) {
      const raw = decodeRetentionReservationProjection(row, schema.stage);
      if (raw === undefined) {
        reportUnsupported('reservation');
        continue;
      }
      raws.push(raw);
    }
    const candidates = await classifyOrphans(raws);
    for (const initial of retentionOrphanPackets(candidates)) {
      let pending = initial;
      const retry = async () => {
        if (schema.stage === undefined) {
          pending = [];
          return;
        }
        const keys = retentionSelectorJson(
          initial.map((candidate) => candidate.raw.key),
        );
        const rows = snapshotResultRows(
          await prepareRetentionStatement(
            captured,
            retentionStatement(
              `SELECT ${retentionReservationProjection(schema.stage)}
          FROM "${registry}" s WHERE s.key COLLATE BINARY IN (SELECT value FROM json_each(?1)) LIMIT ?2`,
              [keys, limit + 1],
            ),
          ).all(),
        );
        if (rows.length > initial.length)
          throw new Error('run retention orphan retry is ambiguous');
        const unchanged: Record<string, unknown>[] = [];
        for (const row of rows) {
          const raw = decodeRetentionReservationProjection(row, schema.stage);
          if (raw === undefined) {
            reportUnsupported('reservation');
            continue;
          }
          const prior = initial.find(
            (candidate) => candidate.raw.key === raw.key,
          )?.raw;
          if (
            prior &&
            Object.entries(prior).every(
              ([key, value]) => Object.hasOwn(raw, key) && raw[key] === value,
            ) &&
            Object.entries(raw).every(
              ([key, value]) => Object.hasOwn(prior, key) || value === null,
            )
          )
            unchanged.push(raw);
        }
        pending = await classifyOrphans(unchanged);
      };
      await execute(
        () => [
          retentionStatement(
            `${RETENTION_SCHEMA_SQL} SELECT ok AS schema_ok FROM schema_ok`,
            [...schema.bindings],
          ),
          ...retentionOrphanPackets(pending).flatMap((packet) =>
            retentionOrphanGroup(schema, packet, keyCutoff).slice(1),
          ),
        ],
        retry,
      );
    }
    await advance('reservations', page.position);
  } else await advance('reservations', undefined);
  if (failures.length > 0)
    throw new Error(
      `purgeExpiredWorkflowRuns: artifact deletion failed (${failures.join('; ')})`,
    );
  return deleted;
}

export interface PurgeExpiredThreadsOptions {
  /** Threads untouched for longer than this are eligible, with their messages. */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /**
   * Threads processed per call. Default 100 — the shrinking eligible set is the
   * cursor, so a first backlog drains across firings instead of blowing one
   * invocation's budget (same batching rationale as purgeExpiredWorkflowRuns).
   */
  limit?: number;
  /** Clock override for tests. */
  now?: () => number;
}

export interface PurgeExpiredThreadsResult {
  threads: number;
  messages: number;
}

// D1 binds at most 100 parameters per query. Each id chunk below shares its
// statement with the cutoff bind of the eligibility re-check, so the chunk
// ceiling must leave room for it — 90 keeps comfortable headroom without
// making the batch loop meaningfully longer.
const D1_BIND_CHUNK = 90;

function chunked<T>(values: readonly T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

/**
 * The tables `purgeExpiredThreads` deletes from under the thread TTL — the
 * production anchor the schema guard cross-checks the `thread-ttl` retention
 * declaration against, AND the target set a `cascade` child must appear in to be
 * believed. Both tables are here because the purge reaps a thread and
 * its messages together: `mastra_threads` by its own `updatedAt`,
 * `mastra_messages` by cascade (a message has no idleness signal of its own — see
 * the purge doc). So the inventory's `mastra_messages: { retention: cascade with
 * mastra_threads }` is only legal because `mastra_messages` is genuinely a delete
 * target here; a cascade naming a parent whose purge never touches the child is
 * the lie the guard now catches.
 */
export const THREAD_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_threads',
  'mastra_messages',
];

/**
 * Thread-level retention (docs/agent-memory-isolation.md#thread-retention): deletes agent
 * memory threads whose `updatedAt` is older than the TTL, each with its
 * messages. The terminal-status shape `purgeExpiredWorkflowRuns` uses does not
 * transfer — threads are not per-run and have no status, so there is nothing to
 * prove "finished" about one. Time since last write IS the only signal a thread
 * is done, which is why the TTL keys on `updatedAt` (the D1 memory domain stamps
 * it on every saveThread, so an active conversation never ages out).
 *
 * MESSAGES BEFORE THREADS: a message carries a `createdAt` but no `updatedAt`,
 * so there is no per-message IDLENESS signal — a message's lifetime is its
 * thread's, and its rows are reachable only through it. Deleting a thread first
 * would strand its messages beyond every later firing of this purge (only
 * decommissioning the deployment's database would still reap them).
 *
 * EVERY statement re-checks the CURRENT row rather than trusting the SELECT's id
 * list, and that is load-bearing here in a way it is not for
 * purgeExpiredWorkflowRuns (where terminal status is absorbing, so its re-check
 * is belt-and-braces). "Idle" is precisely NOT absorbing — a thread can come
 * back to life mid-purge — and the writer is not atomic either: the memory
 * domain's saveMessages issues its message insert and its `UPDATE mastra_threads
 * SET updatedAt` as two INDEPENDENT calls under one Promise.all, so a send's
 * message can be visible while its thread still reads idle. That TORN state is
 * what each guard answers:
 *
 *  - The message DELETE re-reads the thread's current `updatedAt` (subquery) AND
 *    bounds itself to `createdAt < cutoff`. The subquery alone would sweep a
 *    just-arrived message into the same statement as the genuinely old ones —
 *    it keys on the THREAD's staleness, and the bump has not landed — silently
 *    destroying the message the user just sent. `createdAt` is the message's own
 *    evidence of recency, and the only guard that survives a torn write.
 *  - The thread DELETE re-checks `updatedAt < cutoff` AND fires only when `NOT
 *    EXISTS` any message for it. Sequence alone cannot carry "messages before
 *    threads": a message landing after the message DELETE leaves an idle-looking
 *    thread whose row would go out from under it. The NOT EXISTS makes the
 *    invariant — never delete a thread a message points at — a property of the
 *    statement rather than of timing.
 *
 * Together: a torn or concurrent send at ANY point in the sequence keeps both
 * its message and its thread; the thread's next firing (by then bumped, or by
 * then genuinely idle again) decides its fate on fresh evidence.
 *
 * Residuals, both accepted, neither reachable by a read path: (1) a send landing
 * between the two DELETEs loses the already-expiring history while the thread
 * and the new message survive — strictly better than the deletion that was
 * milliseconds away. (2) A message inserted AFTER its thread's row is already
 * gone is orphaned by the writer, not by this purge; D1 offers no cross-statement
 * transaction through this seam to close it. An orphan is unreachable by recall
 * (nothing lists a thread that does not exist) and vanishes with the
 * deployment's database at decommission — a hygiene cost, not a leak.
 *
 * `mastra_resources` (working memory) is deliberately untouched: a resource is
 * the OWNER's, shared across every thread they have, so one thread aging out
 * says nothing about it. It lives until the deployment is decommissioned.
 *
 * Missing tables read as zero (a deployment with no agent memory never created
 * them). Scheduling stays with the caller — see createFlowsafeWorker's
 * THREAD_RETENTION_DAYS duty.
 */
export async function purgeExpiredThreads(
  db: SnapshotDatabase,
  options: PurgeExpiredThreadsOptions,
): Promise<PurgeExpiredThreadsResult> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
  // @mastra/cloudflare-d1 stores the memory tables' TIMESTAMP columns as
  // ISO-8601 TEXT (the same serialization the snapshot rows use), so a
  // lexicographic < against an ISO cutoff is a correct timestamp comparison.
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  let expired: Array<{ id: string }>;
  try {
    ({ results: expired } = await db
      .prepare(
        `SELECT id FROM ${prefix}mastra_threads
         WHERE updatedAt < ?
         ORDER BY updatedAt
         LIMIT ?`,
      )
      .bind(cutoff, options.limit ?? 100)
      .all<{ id: string }>());
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return { threads: 0, messages: 0 };
  }
  const ids = expired.map((row) => row.id);
  if (ids.length === 0) return { threads: 0, messages: 0 };
  const batches = chunked(ids, D1_BIND_CHUNK);
  const placeholders = (chunk: string[]): string =>
    chunk.map(() => '?').join(', ');
  // The eligibility re-check, as a subquery so the message delete rides the
  // thread's CURRENT updatedAt rather than the SELECT's stale membership.
  const stillExpired = (chunk: string[]): string =>
    `SELECT id FROM ${prefix}mastra_threads
     WHERE id IN (${placeholders(chunk)}) AND updatedAt < ?`;

  let messages = 0;
  let hasMessagesTable = true;
  for (const chunk of batches) {
    try {
      messages += d1Changes(
        await db
          .prepare(
            `DELETE FROM ${prefix}mastra_messages
             WHERE thread_id IN (${stillExpired(chunk)})
               AND createdAt < ?`,
          )
          .bind(...chunk, cutoff, cutoff)
          .run(),
      );
    } catch (error) {
      // A host whose memory domain never initialized has threads but no
      // messages table; anything else is a real failure and must reach the
      // purge duty's error surface rather than silently orphan a thread's history.
      if (!isMissingTable(error)) throw error;
      hasMessagesTable = false;
      break;
    }
  }
  // The structural invariant: never delete a thread a message still points at
  // (see the header — the writer's insert and its updatedAt bump are not atomic,
  // so updatedAt alone cannot carry this). Dropped only when the table does not
  // exist, where "no message points at it" is vacuously true and the subquery
  // would throw instead.
  const noMessagesLeft = hasMessagesTable
    ? ` AND NOT EXISTS (SELECT 1 FROM ${prefix}mastra_messages
                        WHERE thread_id = ${prefix}mastra_threads.id)`
    : '';
  let threads = 0;
  for (const chunk of batches) {
    threads += d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_threads
           WHERE id IN (${placeholders(chunk)}) AND updatedAt < ?${noMessagesLeft}`,
        )
        .bind(...chunk, cutoff)
        .run(),
    );
  }
  return { threads, messages };
}

/**
 * The table `purgeExpiredBackgroundTasks` deletes from under the background-task
 * TTL — the production anchor the schema guard cross-checks the
 * `background-task-ttl` retention declaration against. It is the background-task
 * counterpart to RUN_TTL_PURGE_TABLES. One table: the TTL rides `completedAt` on
 * `mastra_background_tasks` alone, no cascade.
 */
export const BACKGROUND_TASK_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_background_tasks',
];

export interface PurgeExpiredBackgroundTasksOptions {
  /**
   * Completed task records untouched for longer than this expire. Default
   * 3_600_000 (1h) — core `BackgroundTaskManager.cleanup`'s `completedTtlMs`.
   */
  completedTtlMs?: number;
  /**
   * Failed / cancelled / timed_out task records older than this expire. Default
   * 86_400_000 (24h) — core's `failedTtlMs`. Kept longer than completed so a
   * failure stays inspectable.
   */
  failedTtlMs?: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

export interface PurgeExpiredBackgroundTasksResult {
  /** Deleted 'completed' rows. */
  completed: number;
  /** Deleted 'failed' | 'cancelled' | 'timed_out' rows. */
  failed: number;
}

/** Terminal statuses the failed-class TTL reaps together (core cleanup mirrors this set). */
const BACKGROUND_TASK_FAILED_STATUSES = [
  'failed',
  'cancelled',
  'timed_out',
] as const;

/**
 * Every background-task status this purge is willing to reap — the two TTL
 * windows' sets together, and therefore the package's definition of a SETTLED
 * task.
 *
 * The drain inventory reads it as the complement it needs: a task whose status
 * is not here is still work, whether it is queued, running, or suspended
 * awaiting a webhook. Deriving both from one list is what stops "terminal"
 * meaning one thing to retention and another to a migration's emptiness proof.
 */
export const BACKGROUND_TASK_TERMINAL_STATUSES: readonly string[] = [
  'completed',
  ...BACKGROUND_TASK_FAILED_STATUSES,
];

/**
 * Background-task TTL cleanup: deletes terminal task
 * rows from `mastra_background_tasks` once their `completedAt` is older than the
 * TTL, mirroring core `BackgroundTaskManager.cleanup()` at the storage layer so
 * a maintenance purge can reap them WITHOUT a live manager: the same posture as
 * purgeExpiredWorkflowRuns/purgeExpiredThreads (raw D1 binding, failure-isolated
 * as a purge duty). Two windows, exactly as core: completed rows expire fast
 * (default 1h), failed/cancelled/timed_out slowly (default 24h) so a failure
 * stays inspectable.
 *
 * `completedAt` is stored as ISO-8601 TEXT (`toISOString()`), so lexicographic
 * `<` against an ISO cutoff is a correct timestamp comparison — the same bet the
 * other purges take. `completedAt IS NOT NULL` is load-bearing: a row without a
 * completion stamp cannot be proven old, so it survives (fail safe). Live rows
 * (pending / running / suspended) are never matched — deleting a suspended task
 * mid-flight would strand its resume. A missing table reads as zero (background
 * tasks may never have run). Scheduling stays with the caller.
 */
export async function purgeExpiredBackgroundTasks(
  db: SnapshotDatabase,
  options: PurgeExpiredBackgroundTasksOptions = {},
): Promise<PurgeExpiredBackgroundTasksResult> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
  const table = `${prefix}mastra_background_tasks`;
  const completedCutoff = new Date(
    now() - (options.completedTtlMs ?? 3_600_000),
  ).toISOString();
  const failedCutoff = new Date(
    now() - (options.failedTtlMs ?? 86_400_000),
  ).toISOString();
  const failedPlaceholders = BACKGROUND_TASK_FAILED_STATUSES.map(
    () => '?',
  ).join(', ');

  try {
    // Internal evented-engine snapshots use the UNSALTED task id. Consume that
    // association before deleting the task rows that make retention
    // ownership discoverable.
    try {
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = '__background-task'
             AND run_id IN (
               SELECT id FROM ${table}
               WHERE status = 'completed'
                 AND completedAt IS NOT NULL AND completedAt < ?
             )`,
        )
        .bind(completedCutoff)
        .run();
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_workflow_snapshot
           WHERE workflow_name = '__background-task'
             AND run_id IN (
               SELECT id FROM ${table}
               WHERE status IN (${failedPlaceholders})
                 AND completedAt IS NOT NULL AND completedAt < ?
             )`,
        )
        .bind(...BACKGROUND_TASK_FAILED_STATUSES, failedCutoff)
        .run();
    } catch (error) {
      // A deployment may have task rows before its workflow table is created;
      // no associated snapshots can exist in that case.
      if (!isMissingTable(error)) throw error;
    }
    const completed = d1Changes(
      await db
        .prepare(
          `DELETE FROM ${table}
           WHERE status = 'completed'
             AND completedAt IS NOT NULL AND completedAt < ?`,
        )
        .bind(completedCutoff)
        .run(),
    );
    const failed = d1Changes(
      await db
        .prepare(
          `DELETE FROM ${table}
           WHERE status IN (${failedPlaceholders})
             AND completedAt IS NOT NULL AND completedAt < ?`,
        )
        .bind(...BACKGROUND_TASK_FAILED_STATUSES, failedCutoff)
        .run(),
    );
    return { completed, failed };
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return { completed: 0, failed: 0 };
  }
}

/**
 * The table `purgeExpiredNotifications` deletes from under the notification TTL
 * — the anchor the schema guard cross-checks the `notification-ttl` retention
 * declaration against. It is the notification counterpart to
 * BACKGROUND_TASK_TTL_PURGE_TABLES.
 */
export const NOTIFICATION_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_notifications',
];

/** Terminal notification statuses the TTL reaps (a delivered/seen/… inbox item is done). */
const NOTIFICATION_TERMINAL_STATUSES = [
  'delivered',
  'seen',
  'dismissed',
  'archived',
  'discarded',
] as const;

export interface PurgeExpiredNotificationsOptions {
  /**
   * Resolved (delivered/seen/dismissed/archived/discarded) notifications whose
   * `updatedAt` is older than this expire. PENDING rows are never touched — one
   * may be waiting on a future `deliverAt`, and reaping it would drop a signal
   * the model has not seen. No default: the caller (createFlowsafeWorker's purge duty)
   * gates this on an opt-in retention window, since a durable inbox is meant to
   * be readable until the host says otherwise.
   */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Notification TTL cleanup: deletes terminal agent-inbox
 * rows from `mastra_notifications` once their `updatedAt` is older than the TTL,
 * at the storage layer so alarm maintenance reaps them without a live agent —
 * the same posture as the other purges (raw D1 binding, failure-isolated duty).
 * A missing table reads as zero (notifications may never have been sent).
 * Scheduling stays with the caller.
 */
export async function purgeExpiredNotifications(
  db: SnapshotDatabase,
  options: PurgeExpiredNotificationsOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
  const cutoff = notificationTimestampMillis(new Date(now() - options.ttlMs));
  const placeholders = NOTIFICATION_TERMINAL_STATUSES.map(() => '?').join(', ');
  try {
    return d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_notifications
           WHERE status IN (${placeholders}) AND ${notificationTimestampSql('updatedAt')} < ?`,
        )
        .bind(...NOTIFICATION_TERMINAL_STATUSES, cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

/**
 * The table `purgeExpiredThreadState` deletes from under the thread-state TTL —
 * the anchor the schema guard cross-checks the `thread-state-ttl` retention
 * declaration against.
 */
export const THREAD_STATE_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_thread_state',
];

export interface PurgeExpiredThreadStateOptions {
  /**
   * Thread-state rows (state-signal lanes + goals) whose `updatedAt` is older
   * than this expire. An actively-updated goal or task lane bumps `updatedAt`
   * on every write, so it never ages out; an abandoned thread's state does. No
   * default (opt-in, like the notification and thread TTLs — durable state is
   * kept until the host sets a window).
   */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Thread-state TTL cleanup: deletes thread-state rows
 * from `mastra_thread_state` once their `updatedAt` is older than the TTL. Keys
 * on `updatedAt` for the same reason `purgeExpiredThreads` does — thread state
 * has no terminal status, so time-since-last-write is the only "done" signal.
 * ISO-8601 TEXT encoding; missing table reads as zero; scheduling stays with the
 * caller. Independent of the thread TTL (an orphan row is reaped here or when
 * the deployment is decommissioned), so it self-bounds without touching the
 * delicate messages-before-threads ordering of purgeExpiredThreads.
 */
export async function purgeExpiredThreadState(
  db: SnapshotDatabase,
  options: PurgeExpiredThreadStateOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
  const cutoff = new Date(now() - options.ttlMs).toISOString();
  try {
    return d1Changes(
      await db
        .prepare(`DELETE FROM ${prefix}mastra_thread_state WHERE updatedAt < ?`)
        .bind(cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

/**
 * The table `purgeExpiredScheduleTriggers` deletes from under the trigger-row TTL
 * — the anchor the schema guard cross-checks the `schedule-trigger-ttl` retention
 * declaration against. It is the schedule-trigger counterpart to
 * THREAD_STATE_TTL_PURGE_TABLES.
 * Only the trigger HISTORY expires; the schedule rows themselves are standing
 * config (retention 'none' — they live until deleted over the schedule
 * surface or the deployment is decommissioned).
 */
export const SCHEDULE_TRIGGER_TTL_PURGE_TABLES: readonly string[] = [
  'mastra_schedule_triggers',
];

export interface PurgeExpiredScheduleTriggersOptions {
  /**
   * Trigger-history rows whose `actualFireAt` is older than this expire. No
   * default (opt-in, like the notification/thread-state TTLs): a schedule's fire
   * history is inspectable until the host sets a window.
   */
  ttlMs: number;
  /** Must satisfy and match createD1Storage's max-39 tablePrefix contract. */
  tablePrefix?: string;
  /** Clock override for tests. */
  now?: () => number;
}

/**
 * Schedule-trigger TTL cleanup: deletes terminal trigger-history
 * rows from `mastra_schedule_triggers` once their `actualFireAt` is older than the
 * TTL, at the storage layer so alarm maintenance reaps them without a live tick —
 * the same posture as the other purges (raw D1 binding, failure-isolated duty).
 *
 * `actualFireAt` is stored as INTEGER ms-epoch (core types
 * `ScheduleTrigger.actualFireAt` as `number`, unlike the ISO-TEXT timestamp
 * columns the other domains use), so the comparison is a NUMERIC `<` against a
 * numeric cutoff — a correct timestamp comparison over integers. A missing table
 * reads as zero (schedules may never have fired). Scheduling stays with the
 * caller. Deferred rows are live dispatch/reconciliation state and are never
 * TTL-purged; their eventual settlement also finalizes any pending schedule
 * deletion.
 */
export async function purgeExpiredScheduleTriggers(
  db: SnapshotDatabase,
  options: PurgeExpiredScheduleTriggersOptions,
): Promise<number> {
  const prefix = validateTablePrefix(options.tablePrefix) ?? '';
  const now = options.now ?? Date.now;
  const cutoff = now() - options.ttlMs;
  try {
    return d1Changes(
      await db
        .prepare(
          `DELETE FROM ${prefix}mastra_schedule_triggers
           WHERE actualFireAt < ? AND outcome <> 'deferred'`,
        )
        .bind(cutoff)
        .run(),
    );
  } catch (error) {
    if (!isMissingTable(error)) throw error;
    return 0;
  }
}

/** Rows affected by a D1 write, read from its `{ meta: { changes } }` envelope. */
export function d1Changes(result: unknown): number {
  const changes = (result as { meta?: { changes?: number } } | undefined)?.meta
    ?.changes;
  return typeof changes === 'number' ? changes : 0;
}

function errorMessageOf(error: unknown): string {
  try {
    return String(error instanceof Error ? error.message : error);
  } catch {
    return 'unreadable error';
  }
}

/**
 * SQLite/D1's "no such table". For the purges, a table that was never
 * created is an EMPTY table, not an error: Mastra creates the snapshot
 * table lazily with the first persisted run, and hosts without the approval
 * queue never create flowsafe_approvals. Matched on the message because the
 * structural SnapshotDatabase seam carries no error codes (D1 wraps the
 * SQLite text but preserves it).
 */
function isMissingTable(error: unknown, expectedTable?: string): boolean {
  const message = errorMessageOf(error);
  if (!/no such table/i.test(message)) return false;
  return expectedTable === undefined || message.includes(expectedTable);
}
