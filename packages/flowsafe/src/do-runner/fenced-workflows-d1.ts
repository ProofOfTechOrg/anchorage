// SPDX-License-Identifier: Apache-2.0
/// <reference types="node" />

import { AsyncLocalStorage } from 'node:async_hooks';
import { type D1DomainConfig, WorkflowsStorageD1 } from '@mastra/cloudflare-d1';
import { EXECUTION_FENCE_TABLE } from '#deployment-identity-protocol';
import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import { DoStatusError } from './do-status-error.js';
import {
  assertMutationEpoch,
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  InvalidExecutionIdentityError,
  normalizeD1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeStartIdentity,
  type ProofEntryExpectation,
  RunAdmissionConflictError,
} from './execution-admission.js';
import {
  admitsRunStart,
  captureExecutionFenceAdmissionSchema,
  decodeExecutionFenceAdmissionRow,
  type ExecutionFenceAdmissionObservation,
  ExecutionFencedError,
  executionFenceAdmissionSql,
  executionFenceAdmissionValues,
  validateExecutionFenceAdmissionSchema,
} from './execution-fence.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
  type InitialAdmissionDatabase,
  type InitialAdmissionWitness,
  type InitialRunAdmission,
  type InitialTerminalizationRequest,
  type InitialTerminalizationResult,
} from './fenced-workflow-capability.js';
import { definitiveInitialAdmissionRefusal } from './initial-admission-refusal.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  advanceLifecycle,
  hasDisputedSettlement,
  parseRunLifecycle,
  projectTerminalLifecycle,
  RUN_LIFECYCLE_CONTEXT_KEY,
  RUN_SETTLED_IDENTITY_PATHS,
  RUN_SETTLING_MARKERS,
  RunLifecycleBlockedError,
  RunSettledConflictError,
  RunStateNotStorableError,
  runTerminalError,
  terminalCleanupFor,
} from './run-lifecycle.js';
import {
  decodeInitialRunProvenance,
  decodeProgressRunProvenance,
  type ProgressRunProvenance,
} from './run-provenance.js';
import { RESOURCE_OWNER_TABLE } from './run-storage-tables.js';
import {
  isRunStatus,
  terminalStateFields,
  terminalStateUpdate,
} from './run-terminal-state.js';
import {
  decodeStartReservationAdmissionResult,
  START_IDEMPOTENCY_TABLE,
  StartReservationOwnerMismatchError,
  type StartReservationReading,
  StartReservationTargetMismatchError,
  validateStartReservationAdmissionSchema,
} from './start-idempotency.js';
import {
  captureReservation,
  sameReservationIdentity,
} from './start-reservation-contract.js';
import { validateTablePrefix } from './table-prefix.js';
import {
  decodeRawWorkflowSnapshotResult,
  parseSnapshotObject,
  prepareRawWorkflowSnapshotRead,
  type RawWorkflowSnapshot,
  readRawWorkflowSnapshot,
  type SnapshotStatement,
  snapshotResultRows,
} from './workflow-snapshot-row.js';

const PROVENANCE = 'flowsafe.runProvenance';
type PersistInput = Parameters<
  WorkflowsStorageD1['persistWorkflowSnapshot']
>[0];
interface AdmissionScope {
  readonly input: InitialRunAdmission;
  open: boolean;
  attempted: boolean;
  witness?: InitialAdmissionWitness;
  failure?: unknown;
  failed: boolean;
}

/**
 * @internal Runs `work` once every earlier call with the same `key` on `tails`
 * has settled, in call order; a failed call does not block the next. An entry
 * leaves `tails` when its last call settles. Each owner keeps its own `tails`:
 * a caller that holds its queue across a call into another owner's would
 * deadlock on a shared one.
 */
export function serializedByKey<T>(
  tails: Map<string, Promise<unknown>>,
  key: string,
  work: () => Promise<T>,
): Promise<T> {
  const previous = tails.get(key) ?? Promise.resolve();
  const current = previous.then(work, work);
  const settled = current.then(
    () => undefined,
    () => undefined,
  );
  tails.set(key, settled);
  return current.finally(() => {
    if (tails.get(key) === settled) tails.delete(key);
  });
}

/** @internal Match the pinned standalone resolver's property-presence precedence. */
export function captureD1DomainConfig(config: D1DomainConfig): D1DomainConfig {
  if ('client' in config) {
    const client = config.client;
    const tablePrefix = validateTablePrefix(config.tablePrefix);
    return { client, tablePrefix };
  }
  if ('binding' in config) {
    const binding = config.binding;
    const tablePrefix = validateTablePrefix(config.tablePrefix);
    return { binding, tablePrefix };
  }
  const { accountId, apiToken, databaseId, tablePrefix } = config;
  return {
    accountId,
    apiToken,
    databaseId,
    tablePrefix: validateTablePrefix(tablePrefix),
  };
}

function admissionDatabase(value: unknown): value is InitialAdmissionDatabase {
  return (
    value !== null &&
    typeof value === 'object' &&
    'prepare' in value &&
    typeof value.prepare === 'function' &&
    'batch' in value &&
    typeof value.batch === 'function'
  );
}

function record(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new InvalidExecutionIdentityError('admission');
  return value as Record<string, unknown>;
}

function assertInitialSnapshot(value: unknown, runId: string): void {
  const snapshot = record(value);
  if (
    snapshot.status !== 'pending' ||
    snapshot.runId !== runId ||
    !Array.isArray(snapshot.activePaths) ||
    snapshot.activePaths.length !== 0
  )
    throw new InvalidExecutionIdentityError('admission');
  for (const field of [
    'activeStepsPath',
    'suspendedPaths',
    'waitingPaths',
    'resumeLabels',
  ]) {
    if (Object.keys(record(snapshot[field])).length !== 0)
      throw new InvalidExecutionIdentityError('admission');
  }
}

function captureInitialProvenance(value: unknown) {
  const {
    version,
    startToken,
    attemptToken,
    requestedBy,
    requestedByKind,
    resumeCounts,
    mutationEpoch,
    startIdentity: rawIdentity,
    agentStart: rawAgent,
    initialAdmission,
  } = record(value);
  if (!Array.isArray(resumeCounts) || resumeCounts.length !== 0)
    throw new InvalidExecutionIdentityError('admission');
  const startIdentity =
    rawIdentity === undefined ? undefined : normalizeStartIdentity(rawIdentity);
  const agentStart =
    rawAgent === undefined
      ? undefined
      : { threaded: record(rawAgent).threaded };
  // Caller accessors run before the stored-data decoder's error translation.
  const captured = {
    version,
    startToken,
    attemptToken,
    requestedBy,
    requestedByKind,
    resumeCounts: [],
    mutationEpoch,
    startIdentity,
    agentStart,
    initialAdmission,
  };
  try {
    return decodeInitialRunProvenance(captured, 'absent');
  } catch {
    throw new InvalidExecutionIdentityError('admission');
  }
}

function captureAdmission(source: InitialRunAdmission): InitialRunAdmission {
  record(source);
  const {
    execution: rawExecution,
    attemptToken,
    mutationEpoch: rawEpoch,
    startIdentity: rawIdentity,
    requestContext,
    fence,
    reservationStore,
    reservation: rawReservation,
    proof: rawProof,
    runOwnerGuard: rawGuard,
    onInitialWriteAttempt,
  } = source;
  record(requestContext);
  const fenceMethods = record(fence);
  if (
    !['usesDatabase', 'seed', 'readForAdmission'].every(
      (method) => typeof fenceMethods[method] === 'function',
    )
  )
    throw new InvalidExecutionIdentityError('admission');
  if (reservationStore !== undefined) {
    const methods = record(reservationStore);
    if (
      !['usesDatabase', 'readForAdmission'].every(
        (method) => typeof methods[method] === 'function',
      )
    )
      throw new InvalidExecutionIdentityError('admission');
  }
  if (rawProof !== undefined) record(rawProof);
  if (rawGuard !== undefined) record(rawGuard);
  const execution = normalizeD1RunExecutionIdentity(rawExecution);
  const mutationEpoch = normalizeMutationEpoch(rawEpoch);
  const startIdentity =
    rawIdentity === undefined ? undefined : normalizeStartIdentity(rawIdentity);
  if (
    !isPathSafeId(attemptToken) ||
    typeof onInitialWriteAttempt !== 'function' ||
    (reservationStore === undefined) !== (rawReservation === undefined)
  )
    throw new InvalidExecutionIdentityError('admission');
  const reservation =
    rawReservation === undefined
      ? undefined
      : captureReservation(rawReservation, 'started');
  if (reservation) {
    if (!startIdentity) throw new InvalidExecutionIdentityError('admission');
    if (
      reservation.owner.kind !== startIdentity.owner.kind ||
      reservation.owner.id !== startIdentity.owner.id
    )
      throw new StartReservationOwnerMismatchError(reservation.key);
    if (
      reservation.targetKind !== startIdentity.target.kind ||
      reservation.targetId !== startIdentity.target.id
    )
      throw new StartReservationTargetMismatchError(
        reservation.key,
        reservation,
      );
    if (
      reservation.runId !== execution.runId ||
      reservation.threadId !==
        (startIdentity.target.kind === 'agent'
          ? startIdentity.target.threadId
          : undefined)
    )
      throw new InvalidExecutionIdentityError('admission');
  }
  if (
    startIdentity?.target.kind === 'workflow' &&
    startIdentity.target.id !== execution.workflowId
  )
    throw new InvalidExecutionIdentityError('admission');
  let proof: ProofEntryExpectation | undefined;
  if (rawProof !== undefined) {
    const {
      key,
      mutationEpoch: suppliedEpoch,
      transitionRevision: suppliedRevision,
    } = rawProof;
    const proofEpoch = normalizeMutationEpoch(suppliedEpoch);
    const transitionRevision = normalizeMutationEpoch(suppliedRevision);
    if (
      !isPathSafeId(key) ||
      proofEpoch === undefined ||
      transitionRevision === undefined ||
      (reservation && key !== reservation.key)
    )
      throw new InvalidExecutionIdentityError('admission');
    proof = Object.freeze({
      key,
      mutationEpoch: proofEpoch,
      transitionRevision,
    });
  }
  const runOwnerGuard =
    rawGuard === undefined
      ? undefined
      : Object.freeze({
          owner: normalizeStartIdentity({
            owner: rawGuard.owner,
            target: { kind: 'workflow', id: execution.workflowId },
          }).owner,
          reservationToken: rawGuard.reservationToken,
        });
  if (runOwnerGuard && runOwnerGuard.reservationToken !== attemptToken)
    throw new InvalidExecutionIdentityError('admission');
  const { [PROVENANCE]: rawProvenance, ...application } = requestContext;
  const provenance = captureInitialProvenance(rawProvenance);
  if (
    provenance.startToken !== execution.startToken ||
    provenance.attemptToken !== attemptToken ||
    provenance.mutationEpoch !== mutationEpoch ||
    JSON.stringify(provenance.startIdentity) !== JSON.stringify(startIdentity)
  )
    throw new InvalidExecutionIdentityError('admission');
  const contextRunId = application.runId;
  const contextWorkflow = application['breakwater.workflowScope'];
  if (
    (contextRunId !== undefined && contextRunId !== execution.runId) ||
    (contextWorkflow !== undefined && contextWorkflow !== execution.workflowId)
  )
    throw new InvalidExecutionIdentityError('admission');
  const context = record(JSON.parse(JSON.stringify(application)));
  if (
    context.runId !== contextRunId ||
    context['breakwater.workflowScope'] !== contextWorkflow
  )
    throw new InvalidExecutionIdentityError('admission');
  return Object.freeze({
    execution,
    attemptToken,
    mutationEpoch,
    startIdentity,
    requestContext: Object.freeze({ ...context, [PROVENANCE]: provenance }),
    fence,
    reservationStore,
    reservation,
    proof,
    runOwnerGuard,
    onInitialWriteAttempt,
  });
}

function sameReservation(
  actual: StartReservationReading | undefined,
  expected: StartReservationReading,
  execution?: D1RunExecutionIdentity,
): boolean {
  return (
    actual !== undefined &&
    sameReservationIdentity(actual, expected) &&
    actual.state === expected.state &&
    actual.updatedAt === expected.updatedAt &&
    (execution === undefined
      ? actual.binding.kind === expected.binding.kind
      : actual.binding.kind === 'bound' &&
        sameFields({ ...actual.binding.execution }, { ...execution }))
  );
}

function sameFields(
  actual: Record<string, unknown>,
  expected: Record<string, unknown>,
): boolean {
  return Object.keys(expected).every(
    (key) => Object.hasOwn(actual, key) && actual[key] === expected[key],
  );
}

function reservationPredicate(
  reservation: StartReservationReading,
  bind: (value: unknown) => string,
  qualifier = '',
  runReference?: string,
): string {
  return `${qualifier}key = ${bind(reservation.key)} AND ${qualifier}run_id = ${runReference ?? bind(reservation.runId)}
    AND ${qualifier}owner_kind = ${bind(reservation.owner.kind)} AND ${qualifier}owner_id = ${bind(reservation.owner.id)}
    AND ${qualifier}target_kind = ${bind(reservation.targetKind)} AND ${qualifier}target_id = ${bind(reservation.targetId)} AND ${qualifier}thread_id IS ${bind(reservation.threadId ?? null)}
    AND ${qualifier}created_at = ${bind(reservation.createdAt)} AND ${qualifier}updated_at = ${bind(reservation.updatedAt)} AND ${qualifier}state = 'started'
    AND ${qualifier}start_token = '' AND ${qualifier}start_table_prefix IS NULL AND ${qualifier}start_workflow_id IS NULL`;
}

function captureBatchResults(value: unknown, length: number) {
  if (!Array.isArray(value) || value.length !== length)
    throw new Error('initial batch cardinality is invalid');
  return Array.from({ length }, (_, index) => {
    if (!Object.hasOwn(value, index))
      throw new Error('initial batch is missing a result');
    return captureStatementResult(value[index]);
  });
}

function captureStatementResult(result: unknown) {
  const results = snapshotResultRows(result).map((row) =>
    Object.freeze(
      Object.fromEntries(
        Object.getOwnPropertyNames(row).map((key) => [key, row[key]]),
      ),
    ),
  );
  const meta: unknown = (result as { meta?: unknown }).meta;
  if (
    meta !== undefined &&
    (meta === null || typeof meta !== 'object' || Array.isArray(meta))
  )
    throw new Error('workflow snapshot result is malformed');
  return {
    results,
    ...(meta !== undefined && 'changes' in meta
      ? { meta: { changes: (meta as { changes: unknown }).changes } }
      : {}),
  };
}

/** Rows one write statement returned, which `meta.changes` must agree with. */
function writtenRowCount({
  results,
  meta,
}: ReturnType<typeof captureStatementResult>): number {
  if (
    results.length > 1 ||
    (meta &&
      (!Number.isSafeInteger(meta.changes) || meta.changes !== results.length))
  )
    throw new Error('statement changes disagree with returned rows');
  return results.length;
}

/**
 * Exact-row compare-and-set of a snapshot row's `snapshot` and `updatedAt`. It
 * matches nothing for a replacement SQLite cannot parse over a row it can.
 */
function prepareSnapshotReplace(
  database: InitialAdmissionDatabase,
  expected: RawWorkflowSnapshot,
  replacement: { snapshot: string; updatedAt: string },
): SnapshotStatement {
  return database
    .prepare(`UPDATE "${expected.tablePrefix}mastra_workflow_snapshot"
    SET snapshot = ?1, updatedAt = ?2
    WHERE workflow_name = ?3 AND run_id = ?4
      AND snapshot = ?5 AND createdAt IS ?6 AND updatedAt IS ?7
      AND resourceId IS ?8
      AND (json_valid(?1) OR NOT json_valid(?5))
    RETURNING workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt`)
    .bind(
      replacement.snapshot,
      replacement.updatedAt,
      expected.workflowId,
      expected.runId,
      expected.snapshot,
      expected.createdAt,
      expected.updatedAt,
      expected.resourceId,
    );
}

/** The replaced row, or undefined when the compare-and-set matched nothing. */
function decodeSnapshotReplace(
  result: unknown,
  expected: RawWorkflowSnapshot,
): RawWorkflowSnapshot | undefined {
  const captured = captureStatementResult(result);
  writtenRowCount(captured);
  return decodeRawWorkflowSnapshotResult(captured, expected);
}

/**
 * Run retention compares a row's stored `updatedAt` with its cutoff as text, so
 * only the `Date.prototype.toISOString()` form orders correctly.
 */
function isCanonicalIsoTime(text: string): boolean {
  const ms = Date.parse(text);
  return Number.isFinite(ms) && new Date(ms).toISOString() === text;
}

async function replaceSnapshotRow(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  expected: RawWorkflowSnapshot,
  replacement: { snapshot: string; updatedAt: string },
): Promise<boolean> {
  const { snapshot, updatedAt } = replacement;
  if (
    expected.tablePrefix !== tablePrefix ||
    !isPathSafeId(expected.workflowId) ||
    !isPathSafeId(expected.runId) ||
    typeof snapshot !== 'string' ||
    typeof updatedAt !== 'string' ||
    parseSnapshotObject(snapshot) === undefined ||
    !isCanonicalIsoTime(updatedAt)
  )
    throw new Error('workflow snapshot replacement is malformed');
  const result = await prepareSnapshotReplace(database, expected, {
    snapshot,
    updatedAt,
  }).all();
  if (decodeSnapshotReplace(result, expected) !== undefined) return true;
  // A miss is a refusal when the bound texts alone decide it, and a row that
  // changed otherwise.
  const verdict = await decisionRow(
    database,
    'SELECT json_valid(?1) AS storable, json_valid(?2) AS expected_readable',
    [snapshot, expected.snapshot],
  );
  if (
    !decisionFlag(verdict, 'storable') &&
    decisionFlag(verdict, 'expected_readable')
  )
    throw new RunStateNotStorableError(expected.workflowId, expected.runId);
  return false;
}

type PatchRunLifecycleArgs = Parameters<
  NonNullable<FencedWorkflowAdmissionCapability['patchRunLifecycle']>
>;

/**
 * The timestamp is bound as JSON text because a bound JS number would be
 * stored as a REAL (`456.0`). A request context that is not an object makes
 * json_set write nothing yet still count a row, so the predicate refuses it.
 * The patch replaces the whole lifecycle, so, when the caller passes
 * `expected.lifecycle`, it matches that lifecycle and not only its revision,
 * which a resume on another instance reaches as well; a stored dispute refuses
 * it as it refuses recording an intent.
 */
async function patchLifecycleRow(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  address: PatchRunLifecycleArgs[0],
  expected: PatchRunLifecycleArgs[1],
  patch: PatchRunLifecycleArgs[2],
): Promise<boolean> {
  const { workflowId, runId } = address;
  const { status, lifecycleRevision, lifecycle: expectedLifecycle } = expected;
  const { lifecycle, timestamp, updatedAt } = patch;
  if (
    !isPathSafeId(workflowId) ||
    !isPathSafeId(runId) ||
    typeof status !== 'string' ||
    (lifecycleRevision !== undefined &&
      !Number.isSafeInteger(lifecycleRevision)) ||
    (expectedLifecycle !== undefined &&
      (expectedLifecycle === null ||
        typeof expectedLifecycle !== 'object' ||
        Array.isArray(expectedLifecycle))) ||
    lifecycle === null ||
    typeof lifecycle !== 'object' ||
    Array.isArray(lifecycle) ||
    !Number.isSafeInteger(timestamp) ||
    timestamp < 0 ||
    typeof updatedAt !== 'string' ||
    !isCanonicalIsoTime(updatedAt)
  )
    throw new Error('workflow lifecycle patch is malformed');
  const result = await database
    .prepare(`UPDATE "${tablePrefix}mastra_workflow_snapshot"
    SET snapshot = json_set(snapshot,
        ${lifecyclePath()}, json(?1),
        '$.timestamp', json(?2)),
      updatedAt = ?3
    WHERE workflow_name = ?4 AND run_id = ?5
      AND CASE
        WHEN NOT json_valid(snapshot) THEN 0
        WHEN coalesce(json_type(snapshot, '$.requestContext'), 'object') <> 'object' THEN 0
        ELSE json_extract(snapshot, '$.status') = ?6
          AND ${lifecycleSql('snapshot', 'revision')} IS ?7
          AND ${STORED_UNSETTLED_SQL}
          AND NOT ${disputedSql('snapshot')}
          AND (?8 IS NULL OR (snapshot -> ${lifecyclePath()}) IS json(?8))
        END
    RETURNING workflow_name`)
    .bind(
      JSON.stringify(lifecycle),
      String(timestamp),
      updatedAt,
      workflowId,
      runId,
      status,
      lifecycleRevision ?? null,
      expectedLifecycle === undefined
        ? null
        : JSON.stringify(expectedLifecycle),
    )
    .all();
  return writtenRowCount(captureStatementResult(result)) === 1;
}

/**
 * The columns @mastra/cloudflare-d1's `persistWorkflowSnapshot` writes for a
 * new row (its `serializeValue` rules for `resourceId`). On an existing row it
 * changes only `snapshot` and `updatedAt`.
 */
function mastraSnapshotRow(args: PersistInput, nowIso: string) {
  const { workflowName, runId, resourceId, snapshot, createdAt, updatedAt } = {
    workflowName: args.workflowName,
    runId: args.runId,
    resourceId: args.resourceId,
    snapshot: args.snapshot,
    createdAt: args.createdAt,
    updatedAt: args.updatedAt,
  } satisfies Record<keyof PersistInput, unknown>;
  const resource: unknown = resourceId;
  return {
    workflowName,
    runId,
    resourceId:
      resource == null
        ? null
        : resource instanceof Date
          ? resource.toISOString()
          : typeof resource === 'object'
            ? JSON.stringify(resource)
            : resource,
    snapshot: JSON.stringify(snapshot),
    createdAt: createdAt ? createdAt.toISOString() : nowIso,
    updatedAt: updatedAt ? updatedAt.toISOString() : nowIso,
  };
}

const LIFECYCLE_JSON_PATH = `$.requestContext."${RUN_LIFECYCLE_CONTEXT_KEY}"`;

/** The run lifecycle's JSON path, or one of its fields', as an SQL literal. */
function lifecyclePath(path?: string): string {
  return `'${LIFECYCLE_JSON_PATH}${path === undefined ? '' : `.${path}`}'`;
}

function lifecycleSql(column: string, path: string): string {
  return `json_extract(${column}, ${lifecyclePath(path)})`;
}

function unsettledSql(stored: string): string {
  return RUN_SETTLING_MARKERS.map(
    (marker) => `${lifecycleSql(stored, marker)} IS NULL`,
  ).join(' AND ');
}

const STORED_UNSETTLED_SQL = unsettledSql('snapshot');

/** Whether the lifecycle in `column` records a disputed economic operation. */
function disputedSql(column: string): string {
  return `EXISTS (
    SELECT 1 FROM json_each(${column}, ${lifecyclePath('economicOperations')})
    WHERE json_extract(value, '$.settlementState') = 'disputed'
  )`;
}

/** A rule read as 1 or 0, as a WHERE or a CASE reads it: NULL reads 0. */
function decisionSql(rule: string): string {
  return `CASE WHEN ${rule} THEN 1 ELSE 0 END`;
}

/**
 * Upsert condition over a stored row: unsettled, or the incoming write
 * advances the revision and keeps the settlement. A stored row SQLite cannot
 * read (malformed, or nested past its depth limit) fails it, and
 * #persistUnlessSettled then evaluates it on that row's lifecycle. CASE,
 * unlike OR, never evaluates json_extract on JSON its guard found unreadable.
 */
function settledRowGuardSql(stored: string, incoming: string): string {
  return `CASE
    WHEN NOT json_valid(${stored}) THEN 0
    WHEN NOT json_valid(${incoming}) THEN ${unsettledSql(stored)}
    ELSE (${unsettledSql(stored)})
      OR (json_type(${incoming}, ${lifecyclePath('revision')}) = 'integer'
        AND ${lifecycleSql(incoming, 'revision')} > ${lifecycleSql(stored, 'revision')}
        AND ${RUN_SETTLED_IDENTITY_PATHS.map(
          (path) =>
            `${lifecycleSql(incoming, path)} IS ${lifecycleSql(stored, path)}`,
        ).join(' AND ')})
    END`;
}

const SETTLED_ROW_GUARD_SQL = settledRowGuardSql(
  'snapshot',
  'excluded.snapshot',
);

type LifecycleMerge = 'as written' | 'stored lifecycle' | 'stored intent';

/**
 * How an admitted write over an unsettled row treats the stored run
 * lifecycle, following effectiveLifecycle's revision order in the runtime: a
 * write whose revision is lower or absent takes the stored lifecycle (a
 * writer's revisions only grow, so the write was serialized before the stored
 * lifecycle landed; only the dispute arm below lowers a stored revision); a
 * write of the same revision without a transitionIntent
 * takes the stored intent; any other write is stored as written. A stored
 * intent never joins a write it was not checked against: a write that records
 * a disputed economic operation is stored as written over an intent, as the
 * dispute would have refused it, and a run-deadline intent does not join a
 * write that moved its deadline. CASE reads no field of text an earlier branch
 * found unreadable, and a NULL comparison falls through to 'as written'.
 */
function lifecycleMergeSql(stored: string, incoming: string): string {
  const storedIntent = `json_type(${stored}, ${lifecyclePath('transitionIntent')}) = 'object'`;
  return `CASE
    WHEN NOT json_valid(${stored}) OR NOT json_valid(${incoming}) THEN 'as written'
    WHEN NOT (${unsettledSql(stored)}) THEN 'as written'
    WHEN json_type(${stored}, ${lifecyclePath()}) IS NOT 'object' THEN 'as written'
    WHEN coalesce(json_type(${incoming}, '$.requestContext'), 'object') <> 'object'
      THEN 'as written'
    WHEN ${storedIntent}
      AND json_type(${incoming}, ${lifecyclePath('revision')}) = 'integer'
      AND ${disputedSql(incoming)}
      THEN 'as written'
    WHEN ${lifecycleSql(incoming, 'revision')} IS NULL
      OR ${lifecycleSql(incoming, 'revision')} < ${lifecycleSql(stored, 'revision')}
      THEN 'stored lifecycle'
    WHEN ${lifecycleSql(incoming, 'revision')} = ${lifecycleSql(stored, 'revision')}
      AND ${storedIntent}
      AND ${lifecycleSql(incoming, 'transitionIntent')} IS NULL
      AND (${lifecycleSql(stored, 'transitionIntent.expectedDeadlineAt')} IS NULL
        OR ${lifecycleSql(incoming, 'deadlineAt')}
          IS ${lifecycleSql(stored, 'transitionIntent.expectedDeadlineAt')})
      THEN 'stored intent'
    ELSE 'as written' END`;
}

const UPSERT_SNAPSHOT_SQL = `CASE (${lifecycleMergeSql('snapshot', 'excluded.snapshot')})
    WHEN 'stored lifecycle' THEN json_set(excluded.snapshot, ${lifecyclePath()},
      snapshot -> ${lifecyclePath()})
    WHEN 'stored intent' THEN json_set(excluded.snapshot,
      ${lifecyclePath('transitionIntent')},
      snapshot -> ${lifecyclePath('transitionIntent')})
    ELSE excluded.snapshot END`;

/**
 * A stored row's liveness for a leg's touch: 1 unsettled, 0 settled, NULL for
 * JSON SQLite cannot read, which touchRunRow decides on the row's lifecycle.
 */
function storedLiveSql(stored: string): string {
  return `CASE WHEN json_valid(${stored}) THEN (${unsettledSql(stored)}) END`;
}

const STORED_LIVE_SQL = storedLiveSql('snapshot');

/**
 * Compare-and-set attempts a write over a row SQLite cannot read makes before
 * answering that the row keeps changing.
 */
const UNREADABLE_ROW_WRITE_ATTEMPTS = 3;

/**
 * A snapshot's text parsed once for a decision over a row SQLite cannot read:
 * the object, and the part the guard reads (its request context's run
 * lifecycle) as JSON SQLite can parse. Text that is not a JSON object projects
 * to `{}`, which holds no settlement; text `JSON.parse` fails on for a reason
 * other than its syntax is unreadable. `lifecycle` is the run lifecycle when
 * it is an object.
 */
interface DecisionInput {
  readonly snapshot?: Record<string, unknown>;
  readonly lifecycle?: Record<string, unknown>;
  readonly projection: string;
}

function decisionInput(text: string): DecisionInput {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (cause) {
    if (cause instanceof SyntaxError) return { projection: '{}' };
    throw new ExecutionFenceUnreadableError(
      'workflow snapshot is not readable',
      { cause },
    );
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed))
    return { projection: '{}' };
  const snapshot = parsed as Record<string, unknown>;
  if (!Object.hasOwn(snapshot, 'requestContext'))
    return { snapshot, projection: '{}' };
  const context: unknown = snapshot.requestContext;
  if (context === null || typeof context !== 'object' || Array.isArray(context))
    return { snapshot, projection: JSON.stringify({ requestContext: null }) };
  if (!Object.hasOwn(context, RUN_LIFECYCLE_CONTEXT_KEY))
    return { snapshot, projection: JSON.stringify({ requestContext: {} }) };
  const lifecycle: unknown = (context as Record<string, unknown>)[
    RUN_LIFECYCLE_CONTEXT_KEY
  ];
  return {
    snapshot,
    ...(lifecycle !== null &&
    typeof lifecycle === 'object' &&
    !Array.isArray(lifecycle)
      ? { lifecycle: lifecycle as Record<string, unknown> }
      : {}),
    projection: JSON.stringify({
      requestContext: { [RUN_LIFECYCLE_CONTEXT_KEY]: lifecycle },
    }),
  };
}

/**
 * The single row of a statement that decides a refused write or a touch. A
 * failure reads as an unreadable row, as readRawWorkflowSnapshot reports one.
 */
async function decisionRow(
  database: InitialAdmissionDatabase,
  sql: string,
  values: readonly unknown[],
): Promise<Record<string, unknown>> {
  try {
    const rows = snapshotResultRows(
      await database
        .prepare(sql)
        .bind(...values)
        .all(),
    );
    const row = rows[0];
    if (rows.length !== 1 || row === undefined)
      throw new Error('workflow snapshot decision is malformed');
    return row;
  } catch (cause) {
    throw new ExecutionFenceUnreadableError(
      'workflow snapshot is not readable',
      { cause },
    );
  }
}

function decisionFlag(row: Record<string, unknown>, column: string): boolean {
  const value = row[column];
  if (value !== 0 && value !== 1)
    throw new Error('workflow snapshot decision is malformed');
  return value === 1;
}

/**
 * Whether SQLite can read the row a write was refused over (undefined when it
 * is gone), and whether it can read the refused write.
 */
async function probeRefusedWrite(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  address: { workflowId: string; runId: string },
  incoming: string,
): Promise<{ readable: boolean | undefined; storable: boolean }> {
  const row = await decisionRow(
    database,
    `SELECT (SELECT json_valid(snapshot) FROM "${tablePrefix}mastra_workflow_snapshot"
      WHERE workflow_name = ?1 AND run_id = ?2) AS readable,
      json_valid(?3) AS storable`,
    [address.workflowId, address.runId, incoming],
  );
  return {
    readable: row.readable === null ? undefined : decisionFlag(row, 'readable'),
    storable: decisionFlag(row, 'storable'),
  };
}

/**
 * The guard's verdict on a write over a stored row SQLite cannot read, and how
 * the upsert would merge the stored lifecycle into it.
 */
async function decideOverUnreadableRow(
  database: InitialAdmissionDatabase,
  stored: DecisionInput,
  incoming: DecisionInput,
): Promise<{ admitted: boolean; merge: LifecycleMerge }> {
  const row = await decisionRow(
    database,
    `SELECT ${decisionSql(settledRowGuardSql('?1', '?2'))} AS admitted,
      ${lifecycleMergeSql('?1', '?2')} AS merge`,
    [stored.projection, incoming.projection],
  );
  const merge = row.merge;
  if (
    merge !== 'as written' &&
    merge !== 'stored lifecycle' &&
    merge !== 'stored intent'
  )
    throw new Error('workflow snapshot decision is malformed');
  return { admitted: decisionFlag(row, 'admitted'), merge };
}

/**
 * The bytes an admitted write over a row SQLite cannot read stores, placing
 * the stored lifecycle or its transitionIntent where the upsert's json_set
 * places it: an object spread keeps an existing key's position and appends a
 * new one.
 */
function mergedSnapshot(
  stored: DecisionInput,
  incoming: DecisionInput,
  incomingText: string,
  merge: LifecycleMerge,
): string {
  if (merge === 'as written' || !incoming.snapshot || !stored.lifecycle)
    return incomingText;
  const context = (incoming.snapshot.requestContext ?? {}) as Record<
    string,
    unknown
  >;
  const lifecycle =
    merge === 'stored lifecycle'
      ? stored.lifecycle
      : {
          ...incoming.lifecycle,
          transitionIntent: stored.lifecycle.transitionIntent,
        };
  return JSON.stringify({
    ...incoming.snapshot,
    requestContext: { ...context, [RUN_LIFECYCLE_CONTEXT_KEY]: lifecycle },
  });
}

/**
 * A snapshot written over a row SQLite cannot read: the guard's rule and the
 * lifecycle merge decide it on the lifecycle parsed from the row just read,
 * and an admitted write is a compare-and-set against that row. A miss reads
 * the row again, so a row purged meanwhile is refused rather than inserted
 * afresh.
 */
async function writeOverUnreadableRow(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  row: {
    workflowName: string;
    runId: string;
    snapshot: string;
    updatedAt: string;
  },
): Promise<void> {
  const address = { workflowId: row.workflowName, runId: row.runId };
  const incoming = decisionInput(row.snapshot);
  for (let attempt = 0; attempt < UNREADABLE_ROW_WRITE_ATTEMPTS; attempt++) {
    const stored = await readRawWorkflowSnapshot(
      database,
      { tablePrefix, ...address },
      { missingTable: 'error' },
    );
    if (stored === undefined)
      throw new RunSettledConflictError(row.workflowName, row.runId);
    const storedInput = decisionInput(stored.snapshot);
    const decision = await decideOverUnreadableRow(
      database,
      storedInput,
      incoming,
    );
    if (!decision.admitted)
      throw new RunSettledConflictError(row.workflowName, row.runId);
    if (
      await replaceSnapshotRow(database, tablePrefix, stored, {
        snapshot: mergedSnapshot(
          storedInput,
          incoming,
          row.snapshot,
          decision.merge,
        ),
        updatedAt: row.updatedAt,
      })
    )
      return;
  }
  throw new ExecutionFenceUnreadableError(
    'workflow snapshot write kept missing a changing row SQLite cannot parse',
  );
}

/**
 * The touch of a row SQLite cannot read: its parsed lifecycle decides, and
 * `updatedAt` moves only on the exact row decided on. A row that changed in
 * between reads live; the next touch decides again.
 */
async function touchUnreadableRow(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  address: { workflowId: string; runId: string },
  nowMs: number,
): Promise<'live' | 'settled' | 'absent'> {
  const stored = await readRawWorkflowSnapshot(
    database,
    { tablePrefix, ...address },
    { missingTable: 'error' },
  );
  if (stored === undefined) return 'absent';
  const live = decisionFlag(
    await decisionRow(
      database,
      `SELECT ${decisionSql(storedLiveSql('?1'))} AS live`,
      [decisionInput(stored.snapshot).projection],
    ),
    'live',
  );
  if (!live) return 'settled';
  // Not replaceSnapshotRow, which refuses stored bytes that are not JSON.
  decodeSnapshotReplace(
    await prepareSnapshotReplace(database, stored, {
      snapshot: stored.snapshot,
      updatedAt: new Date(nowMs).toISOString(),
    }).all(),
    stored,
  );
  return 'live';
}

/**
 * SQLite counts a matched row as changed even when `updatedAt` is set to
 * itself, so on a settled row `meta.changes` and the returned row still agree.
 * `SET` reads the row before the update and `RETURNING` after it, and the
 * predicate reads only `snapshot`, which the statement leaves alone.
 */
async function touchRunRow(
  database: InitialAdmissionDatabase,
  tablePrefix: string,
  address: { workflowId: string; runId: string },
  nowMs: number,
): Promise<'live' | 'settled' | 'absent'> {
  const { workflowId, runId } = address;
  if (
    !isPathSafeId(workflowId) ||
    !isPathSafeId(runId) ||
    !Number.isSafeInteger(nowMs) ||
    nowMs < 0
  )
    throw new Error('workflow snapshot address is malformed');
  const result = await database
    .prepare(`UPDATE "${tablePrefix}mastra_workflow_snapshot"
    SET updatedAt = CASE WHEN ${STORED_LIVE_SQL} THEN ?1 ELSE updatedAt END
    WHERE workflow_name = ?2 AND run_id = ?3
    RETURNING ${STORED_LIVE_SQL} AS live`)
    .bind(new Date(nowMs).toISOString(), workflowId, runId)
    .all();
  const captured = captureStatementResult(result);
  if (writtenRowCount(captured) === 0) return 'absent';
  const live: unknown = captured.results[0]?.live;
  if (live === 1) return 'live';
  if (live === 0) return 'settled';
  if (live === null)
    return touchUnreadableRow(database, tablePrefix, address, nowMs);
  throw new Error('workflow snapshot touch result is malformed');
}

function terminalizationUnreadable(
  cause: unknown,
): ExecutionFenceUnreadableError {
  return new ExecutionFenceUnreadableError(
    'initial admission cannot be terminalized',
    { cause },
  );
}

function terminalizationSnapshot(row: RawWorkflowSnapshot) {
  const snapshot = record(JSON.parse(row.snapshot));
  if (
    !isRunStatus(snapshot.status) ||
    typeof snapshot.runId !== 'string' ||
    snapshot.runId !== row.runId
  )
    throw new Error('stored workflow snapshot is malformed');
  const context =
    snapshot.requestContext === undefined
      ? {}
      : record(snapshot.requestContext);
  const rawProvenance = context[PROVENANCE];
  if (rawProvenance === undefined || record(rawProvenance).version === 1)
    return { snapshot, context, provenance: undefined, lifecycle: undefined };
  const provenance = decodeProgressRunProvenance(rawProvenance);
  const lifecycle = parseRunLifecycle(context[RUN_LIFECYCLE_CONTEXT_KEY]);
  return { snapshot, context, provenance, lifecycle };
}

function sameStart(
  actual: ProgressRunProvenance,
  expected: ProgressRunProvenance,
): boolean {
  return (
    actual.startToken === expected.startToken &&
    actual.mutationEpoch === expected.mutationEpoch &&
    actual.agentStart?.threaded === expected.agentStart?.threaded &&
    actual.startIdentity?.owner.kind === expected.startIdentity?.owner.kind &&
    actual.startIdentity?.owner.id === expected.startIdentity?.owner.id &&
    actual.startIdentity?.target.kind === expected.startIdentity?.target.kind &&
    actual.startIdentity?.target.id === expected.startIdentity?.target.id &&
    (actual.startIdentity?.target.kind === 'agent'
      ? actual.startIdentity.target.threadId
      : undefined) ===
      (expected.startIdentity?.target.kind === 'agent'
        ? expected.startIdentity.target.threadId
        : undefined)
  );
}

function prepareTerminalization(
  source: InitialTerminalizationRequest,
  tablePrefix: string,
) {
  const {
    expected: rawExpected,
    execution: rawExecution,
    attemptToken,
    nowMs,
    markOutcomeUnknown,
  } = record(source);
  const execution = normalizeD1RunExecutionIdentity(rawExecution);
  const {
    tablePrefix: expectedPrefix,
    workflowId,
    runId,
    resourceId,
    snapshot,
    createdAt,
    updatedAt,
  } = record(rawExpected);
  if (
    expectedPrefix !== tablePrefix ||
    execution.tablePrefix !== tablePrefix ||
    workflowId !== execution.workflowId ||
    runId !== execution.runId ||
    (resourceId !== null && typeof resourceId !== 'string') ||
    typeof snapshot !== 'string' ||
    typeof createdAt !== 'string' ||
    typeof updatedAt !== 'string' ||
    !isPathSafeId(attemptToken) ||
    typeof nowMs !== 'number' ||
    !Number.isSafeInteger(nowMs) ||
    nowMs < 0 ||
    (markOutcomeUnknown !== undefined && markOutcomeUnknown !== true)
  )
    throw new InvalidExecutionIdentityError('admission');
  let nowIso: string;
  try {
    nowIso = new Date(nowMs).toISOString();
  } catch {
    throw new InvalidExecutionIdentityError('admission');
  }
  const expected: RawWorkflowSnapshot = Object.freeze({
    tablePrefix,
    workflowId,
    runId,
    resourceId,
    snapshot,
    createdAt,
    updatedAt,
  });
  let parsed: ReturnType<typeof terminalizationSnapshot>;
  try {
    parsed = terminalizationSnapshot(expected);
  } catch (error) {
    throw terminalizationUnreadable(error);
  }
  const { provenance, lifecycle, context } = parsed;
  if (
    !provenance ||
    provenance.startToken !== execution.startToken ||
    provenance.attemptToken !== attemptToken ||
    provenance.initialAdmission !== true ||
    provenance.resumeCounts.length !== 0 ||
    lifecycle?.terminal ||
    provenance.requestedBy !== provenance.startIdentity?.owner.id ||
    provenance.requestedByKind !== provenance.startIdentity?.owner.kind ||
    (context.runId !== undefined && context.runId !== runId) ||
    (context['breakwater.workflowScope'] !== undefined &&
      context['breakwater.workflowScope'] !== workflowId) ||
    (provenance.startIdentity?.target.kind === 'workflow' &&
      provenance.startIdentity.target.id !== workflowId)
  )
    throw new InvalidExecutionIdentityError('admission');
  assertInitialSnapshot(parsed.snapshot, runId);
  try {
    decodeInitialRunProvenance(context[PROVENANCE], 'present');
  } catch (error) {
    throw terminalizationUnreadable(error);
  }
  const nextProvenance = { ...record(context[PROVENANCE]) };
  delete nextProvenance.initialAdmission;
  const nextContext: Record<string, unknown> = {
    ...context,
    [PROVENANCE]: nextProvenance,
  };
  let fields = terminalStateUpdate({
    status: 'failed',
    error: {
      name: 'StartOutcomeUnknown',
      message:
        'Start interrupted before a durable execution outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.',
    },
  });
  let cleanup: ReturnType<typeof terminalCleanupFor>;
  const intent = lifecycle?.transitionIntent;
  if (intent) {
    if (hasDisputedSettlement(lifecycle))
      throw new RunLifecycleBlockedError({
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      });
    try {
      const next = projectTerminalLifecycle(
        lifecycle,
        intent.status,
        nowMs,
        intent.replayPrincipals,
      );
      nextContext[RUN_LIFECYCLE_CONTEXT_KEY] = next;
      fields = {
        ...terminalStateFields(intent.status),
        error: runTerminalError(intent.status),
      };
      cleanup = terminalCleanupFor(next);
    } catch (error) {
      throw terminalizationUnreadable(error);
    }
  } else if (markOutcomeUnknown) {
    try {
      nextContext[RUN_LIFECYCLE_CONTEXT_KEY] = advanceLifecycle(lifecycle, {
        startOutcomeUnknownAt: nowMs,
      });
    } catch (error) {
      throw terminalizationUnreadable(error);
    }
  }
  const replacement = Object.freeze({
    ...expected,
    updatedAt: nowIso,
    snapshot: JSON.stringify({
      ...parsed.snapshot,
      ...fields,
      requestContext: nextContext,
      timestamp: nowMs,
    }),
  });
  return { expected, replacement, provenance, cleanup };
}

/**
 * Owned initial admission and repair. On a binding with `batch()`, unscoped
 * persistence is an owned upsert that refuses to overwrite a settled run row
 * (RunSettledConflictError); otherwise it delegates to the adapter.
 */
export class FencedWorkflowsStorageD1 extends WorkflowsStorageD1 {
  readonly [FENCED_WORKFLOW_STORAGE]?: FencedWorkflowAdmissionCapability;
  readonly #admission?: FencedWorkflowAdmissionCapability;
  readonly #scopes = new AsyncLocalStorage<AdmissionScope>();
  readonly #unreadableRowTails = new Map<string, Promise<unknown>>();

  constructor(config: D1DomainConfig) {
    const captured = captureD1DomainConfig(config);
    super(captured);
    if ('binding' in captured && admissionDatabase(captured.binding)) {
      const database = captured.binding;
      const tablePrefix = (captured.tablePrefix ?? '').toLowerCase();
      this.#admission = Object.freeze({
        database,
        tablePrefix,
        withInitialAdmission: <T>(
          input: InitialRunAdmission,
          createRun: () => Promise<T>,
        ) => this.#withInitialAdmission(input, createRun),
        readSnapshot: (address: { workflowId: string; runId: string }) =>
          readRawWorkflowSnapshot(
            database,
            {
              tablePrefix,
              workflowId: address.workflowId,
              runId: address.runId,
            },
            { missingTable: 'empty' },
          ),
        terminalizeInitialAdmission: (request: InitialTerminalizationRequest) =>
          this.#terminalizeInitialAdmission(request),
        touchRun: (
          address: { workflowId: string; runId: string },
          nowMs: number,
        ) => touchRunRow(database, tablePrefix, address, nowMs),
        replaceSnapshot: (
          expected: RawWorkflowSnapshot,
          replacement: { snapshot: string; updatedAt: string },
        ) => replaceSnapshotRow(database, tablePrefix, expected, replacement),
        patchRunLifecycle: (...args: PatchRunLifecycleArgs) =>
          patchLifecycleRow(database, tablePrefix, ...args),
      });
      this[FENCED_WORKFLOW_STORAGE] = this.#admission;
    }
  }

  protected withInitialTerminalizationLock<T>(
    _workflowName: string,
    _runId: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    return operation();
  }

  async #terminalizeInitialAdmission(
    source: InitialTerminalizationRequest,
  ): Promise<InitialTerminalizationResult> {
    const capability = this.#admission;
    if (!capability) throw new InvalidExecutionIdentityError('admission');
    const frame = prepareTerminalization(source, capability.tablePrefix);
    const { expected, replacement, cleanup, provenance } = frame;
    const { database } = capability;
    return this.withInitialTerminalizationLock(
      expected.workflowId,
      expected.runId,
      async () => {
        const readback = async (): Promise<InitialTerminalizationResult> => {
          const row = await readRawWorkflowSnapshot(database, expected, {
            missingTable: 'error',
          });
          if (!row) return { kind: 'conflict' };
          if (sameFields({ ...row }, { ...replacement }))
            return {
              kind: 'already-terminalized',
              row,
              ...(cleanup ? { cleanup } : {}),
            };
          const current = terminalizationSnapshot(row);
          if (
            current.provenance &&
            sameStart(current.provenance, provenance) &&
            current.snapshot.status !== 'pending'
          ) {
            const currentCleanup = terminalCleanupFor(current.lifecycle);
            return {
              kind: 'progressed',
              row,
              ...(currentCleanup ? { cleanup: currentCleanup } : {}),
            };
          }
          return { kind: 'conflict', row };
        };
        let result: unknown;
        try {
          result = await prepareSnapshotReplace(
            database,
            expected,
            replacement,
          ).all();
        } catch (error) {
          try {
            const recovered = await readback();
            if (recovered.kind !== 'conflict') return recovered;
          } catch {
            /* The write's original uncertainty remains authoritative. */
          }
          throw terminalizationUnreadable(error);
        }
        try {
          const row = decodeSnapshotReplace(result, expected);
          if (!row) return await readback();
          if (!sameFields({ ...row }, { ...replacement }))
            throw new Error('terminalization returned a different row');
          return { kind: 'terminalized', row, ...(cleanup ? { cleanup } : {}) };
        } catch (error) {
          throw terminalizationUnreadable(error);
        }
      },
    );
  }

  async #withInitialAdmission<T>(
    source: InitialRunAdmission,
    createRun: () => Promise<T>,
  ): Promise<{ value: T; witness: InitialAdmissionWitness }> {
    const parent = this.#scopes.getStore();
    if (parent) {
      parent.failed = true;
      parent.failure = new InvalidExecutionIdentityError('admission');
      throw parent.failure;
    }
    const input = captureAdmission(source);
    const capability = this.#admission;
    if (
      !capability ||
      typeof createRun !== 'function' ||
      input.execution.tablePrefix !== capability.tablePrefix ||
      !input.fence.usesDatabase(capability.database) ||
      (input.reservationStore &&
        !input.reservationStore.usesDatabase(capability.database))
    )
      throw new InvalidExecutionIdentityError('admission');
    const scope: AdmissionScope = {
      input,
      open: true,
      attempted: false,
      failed: false,
    };
    try {
      return await this.#scopes.run(scope, async () => {
        const value = await Reflect.apply(createRun, undefined, []);
        if (scope.failed) throw scope.failure;
        if (!scope.witness) {
          throw new ExecutionFenceUnreadableError(
            'initial run admission has no persistence witness',
          );
        }
        return { value, witness: scope.witness };
      });
    } finally {
      scope.open = false;
    }
  }

  override async persistWorkflowSnapshot(args: PersistInput): Promise<void> {
    const scope = this.#scopes.getStore();
    if (!scope) {
      const capability = this.#admission;
      if (!capability) return super.persistWorkflowSnapshot(args);
      return this.#persistUnlessSettled(capability, args);
    }
    if (
      !scope.open ||
      scope.attempted ||
      args.workflowName !== scope.input.execution.workflowId ||
      args.runId !== scope.input.execution.runId
    ) {
      scope.failed = true;
      scope.failure = new InvalidExecutionIdentityError('admission');
      throw scope.failure;
    }
    scope.attempted = true;
    try {
      Reflect.apply(scope.input.onInitialWriteAttempt, undefined, []);
      const { row, nowMs } = this.#initialRow(args, scope.input);
      scope.witness = await this.#insert(scope.input, row, nowMs);
    } catch (error) {
      scope.failed = true;
      scope.failure = error;
      throw error;
    }
  }

  /**
   * The upsert refuses a snapshot SQLite cannot parse, and any write over a row
   * SQLite cannot parse; the probe then sends such a row to
   * writeOverUnreadableRow, so a row stored earlier stays writable.
   */
  async #persistUnlessSettled(
    capability: FencedWorkflowAdmissionCapability,
    args: PersistInput,
  ): Promise<void> {
    const row = mastraSnapshotRow(args, new Date().toISOString());
    const { database, tablePrefix } = capability;
    const result = await database
      .prepare(`INSERT INTO "${tablePrefix}mastra_workflow_snapshot"
      (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
      SELECT ?1, ?2, ?3, ?4, ?5, ?6
      WHERE json_valid(?4)
      ON CONFLICT (workflow_name, run_id) DO UPDATE
        SET snapshot = ${UPSERT_SNAPSHOT_SQL}, updatedAt = excluded.updatedAt
        WHERE ${SETTLED_ROW_GUARD_SQL}
      RETURNING workflow_name`)
      .bind(
        row.workflowName,
        row.runId,
        row.resourceId,
        row.snapshot,
        row.createdAt,
        row.updatedAt,
      )
      .all();
    if (writtenRowCount(captureStatementResult(result)) === 1) return;
    // The guard refused a readable row, or the write is not storable; a row
    // gone since the upsert met it is refused too, so a purged settlement is
    // not written over.
    const refused = await probeRefusedWrite(
      database,
      tablePrefix,
      { workflowId: row.workflowName, runId: row.runId },
      row.snapshot,
    );
    if (refused.readable !== false) {
      if (!refused.storable)
        throw new RunStateNotStorableError(row.workflowName, row.runId);
      throw new RunSettledConflictError(row.workflowName, row.runId);
    }
    // Persists through this storage over one run's unreadable row run one at a
    // time, so they do not make each other's compare-and-set miss.
    await serializedByKey(
      this.#unreadableRowTails,
      `${row.workflowName}\0${row.runId}`,
      () => writeOverUnreadableRow(database, tablePrefix, row),
    );
  }

  #initialRow(
    args: PersistInput,
    input: InitialRunAdmission,
  ): { row: RawWorkflowSnapshot; nowMs: number } {
    const snapshot = { ...args.snapshot };
    assertInitialSnapshot(snapshot, input.execution.runId);
    const provenance = decodeInitialRunProvenance(
      { ...record(input.requestContext[PROVENANCE]), initialAdmission: true },
      'present',
    );
    snapshot.requestContext = {
      ...input.requestContext,
      [PROVENANCE]: provenance,
    };
    const nowMs = Date.now();
    if (!Number.isSafeInteger(nowMs) || nowMs < 0)
      throw new InvalidExecutionIdentityError('admission');
    const nowIso = new Date(nowMs).toISOString();
    // Before the snapshot serializes: a toJSON that rewrites the context then
    // fails the comparison below.
    const contextBytes = JSON.stringify(snapshot.requestContext);
    const {
      resourceId: serializedResource,
      snapshot: bytes,
      createdAt: created,
      updatedAt: updated,
    } = mastraSnapshotRow({ ...args, snapshot }, nowIso);
    if (serializedResource !== null && typeof serializedResource !== 'string')
      throw new InvalidExecutionIdentityError('admission');
    const serialized = record(JSON.parse(bytes));
    assertInitialSnapshot(serialized, input.execution.runId);
    if (JSON.stringify(serialized.requestContext) !== contextBytes)
      throw new InvalidExecutionIdentityError('admission');
    const serializedContext = record(serialized.requestContext);
    decodeInitialRunProvenance(serializedContext[PROVENANCE], 'present');
    if (
      (serializedContext.runId !== undefined &&
        serializedContext.runId !== input.execution.runId) ||
      (serializedContext['breakwater.workflowScope'] !== undefined &&
        serializedContext['breakwater.workflowScope'] !==
          input.execution.workflowId)
    )
      throw new InvalidExecutionIdentityError('admission');
    if (typeof created !== 'string' || typeof updated !== 'string')
      throw new InvalidExecutionIdentityError('admission');
    const row: RawWorkflowSnapshot = Object.freeze({
      tablePrefix: input.execution.tablePrefix,
      workflowId: input.execution.workflowId,
      runId: input.execution.runId,
      resourceId: serializedResource,
      snapshot: bytes,
      createdAt: created,
      updatedAt: updated,
    });
    return { row, nowMs };
  }

  async #insert(
    input: InitialRunAdmission,
    row: RawWorkflowSnapshot,
    nowMs: number,
  ): Promise<InitialAdmissionWitness> {
    const capability = this.#admission;
    if (!capability) throw new InvalidExecutionIdentityError('admission');
    const { database } = capability;
    await input.fence.seed('open');
    const observed = await input.fence.readForAdmission();
    const semanticValues = executionFenceAdmissionValues(observed);
    let schema: string;
    try {
      schema = await captureExecutionFenceAdmissionSchema(
        await database
          .prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`)
          .all(),
      );
    } catch (cause) {
      throw new ExecutionFenceUnreadableError(
        'initial admission requires current fence schema',
        { cause },
      );
    }
    if (
      input.reservation &&
      !sameReservation(
        await input.reservationStore?.readForAdmission(input.reservation.key),
        input.reservation,
      )
    )
      throw new RunAdmissionConflictError('reservation-changed');
    const values: unknown[] = [];
    const bind = (value: unknown) => {
      values.push(value);
      return `?${values.length}`;
    };
    const fields = [
      row.workflowId,
      row.runId,
      row.resourceId,
      row.snapshot,
      row.createdAt,
      row.updatedAt,
    ].map(bind);
    const epoch = bind(input.mutationEpoch ?? null);
    const semantic = semanticValues.map(bind);
    const schemaParameter = bind(schema);
    const proofRevision = bind(input.proof?.transitionRevision ?? null);
    const proofKey = bind(input.proof?.key ?? null);
    const reservation = input.reservation
      ? `AND EXISTS (SELECT 1 FROM ${START_IDEMPOTENCY_TABLE} AS r WHERE ${reservationPredicate(input.reservation, bind, 'r.', fields[1])})`
      : '';
    const proofEpoch = bind(input.proof?.mutationEpoch ?? null);
    const owner = input.runOwnerGuard
      ? `AND EXISTS (SELECT 1 FROM ${RESOURCE_OWNER_TABLE} AS o WHERE o.resource_kind = 'run' AND o.resource_id = ${fields[1]}
      AND o.owner_kind = ${bind(input.runOwnerGuard.owner.kind)} AND o.owner_id = ${bind(input.runOwnerGuard.owner.id)}
      AND (o.reservation_token IS NULL OR o.reservation_token = ${bind(input.runOwnerGuard.reservationToken)}))`
      : '';
    const fencePredicate = executionFenceAdmissionSql({
      callerEpoch: epoch,
      semantic,
      schema: schemaParameter,
      statePredicate: `f.state COLLATE BINARY = 'open' OR
        (f.state COLLATE BINARY = 'proof-only' AND f.proof_key COLLATE BINARY = ${proofKey}
          AND f.transition_revision = ${proofRevision} AND f.mutation_epoch = ${proofEpoch}
          AND f.proof_run_id IS NULL AND f.proof_table_prefix IS NULL
          AND f.proof_workflow_id IS NULL AND f.proof_start_token IS NULL)`,
    });
    const statements = [
      database
        .prepare(`INSERT INTO "${row.tablePrefix}mastra_workflow_snapshot"
      (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
      SELECT ${fields.join(', ')}
      WHERE ${fencePredicate} AND json_valid(${fields[3]}) ${reservation} ${owner}
      ON CONFLICT (workflow_name, run_id) DO NOTHING
      RETURNING workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt`)
        .bind(...values),
    ];
    if (input.reservation) {
      const keyValues: unknown[] = [
        input.execution.startToken,
        row.tablePrefix,
        row.workflowId,
      ];
      const predicate = reservationPredicate(input.reservation, (value) => {
        keyValues.push(value);
        return `?${keyValues.length}`;
      });
      statements.push(
        database
          .prepare(`UPDATE ${START_IDEMPOTENCY_TABLE}
        SET start_token = ?1, start_table_prefix = ?2, start_workflow_id = ?3
        WHERE changes() = 1 AND ${predicate} RETURNING *`)
          .bind(...keyValues),
      );
    }
    statements.push(
      database
        .prepare(`UPDATE ${EXECUTION_FENCE_TABLE}
      SET proof_run_id = ?1, proof_table_prefix = ?2, proof_workflow_id = ?3, proof_start_token = ?4, updated_at = ?5
      WHERE changes() = 1 AND id = 'deployment' AND state = 'proof-only' AND proof_key = ?6
        AND mutation_epoch = ?7 AND transition_revision = ?8
        AND proof_run_id IS NULL AND proof_table_prefix IS NULL AND proof_workflow_id IS NULL AND proof_start_token IS NULL RETURNING *`)
        .bind(
          row.runId,
          row.tablePrefix,
          row.workflowId,
          input.execution.startToken,
          nowMs,
          input.proof?.key ?? null,
          input.proof?.mutationEpoch ?? null,
          input.proof?.transitionRevision ?? null,
        ),
    );
    let result: unknown;
    try {
      result = await database.batch(statements);
    } catch (error) {
      try {
        if (await this.#converged(database, input, row, observed, nowMs))
          return Object.freeze({ execution: input.execution, row });
      } catch {
        /* The write failure remains the cause of an uncertain outcome. */
      }
      throw new ExecutionFenceUnreadableError(
        'initial run admission outcome is not readable',
        { cause: error },
      );
    }
    const positive = this.#decodeBatch(result, input, row, observed, nowMs);
    if (positive) return Object.freeze({ execution: input.execution, row });
    const refusal = await this.#diagnoseZero(
      database,
      input,
      observed,
      row.snapshot,
    );
    throw definitiveInitialAdmissionRefusal(input.execution, refusal);
  }

  #decodeBatch(
    result: unknown,
    input: InitialRunAdmission,
    row: RawWorkflowSnapshot,
    observed: ExecutionFenceAdmissionObservation,
    nowMs: number,
  ): boolean {
    try {
      const captured = captureBatchResults(result, input.reservation ? 3 : 2);
      const counts = captured.map(writtenRowCount);
      if (counts[0] === 0) {
        if (counts.some((count) => count !== 0))
          throw new Error('zero INSERT changed a participant');
        return false;
      }
      const actual = decodeRawWorkflowSnapshotResult(
        captured[0],
        input.execution,
      );
      if (!actual || !sameFields({ ...actual }, { ...row }))
        throw new Error('initial INSERT returned different bytes');
      if (
        input.reservation &&
        !sameReservation(
          decodeStartReservationAdmissionResult(captured[1]),
          input.reservation,
          input.execution,
        )
      )
        throw new Error('initial binding is inconsistent');
      const proofRows = snapshotResultRows(captured[captured.length - 1]);
      const proofRow = proofRows[0];
      if (observed.reading.state === 'proof-only') {
        if (
          proofRows.length !== 1 ||
          proofRow === undefined ||
          !sameFields(
            { ...decodeExecutionFenceAdmissionRow(proofRow).raw },
            {
              ...observed.raw,
              proof_run_id: row.runId,
              proof_table_prefix: row.tablePrefix,
              proof_workflow_id: row.workflowId,
              proof_start_token: input.execution.startToken,
              updated_at: nowMs,
            },
          )
        )
          throw new Error('initial proof binding is inconsistent');
      } else if (proofRows.length !== 0)
        throw new Error('open admission changed proof');
      return true;
    } catch (error) {
      throw new ExecutionFenceUnreadableError(
        'initial run admission batch result is not readable',
        { cause: error },
      );
    }
  }

  async #converged(
    database: InitialAdmissionDatabase,
    input: InitialRunAdmission,
    row: RawWorkflowSnapshot,
    observed: ExecutionFenceAdmissionObservation,
    nowMs: number,
  ): Promise<boolean> {
    const raw = prepareRawWorkflowSnapshotRead(database, input.execution);
    const statements: SnapshotStatement[] = [raw.statement];
    if (input.reservation)
      statements.push(
        database
          .prepare(
            `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE key = ? LIMIT 2`,
          )
          .bind(input.reservation.key),
      );
    if (observed.reading.state === 'proof-only')
      statements.push(
        database.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE} LIMIT 2`),
      );
    if (input.reservation)
      statements.push(
        database.prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`),
      );
    if (observed.reading.state === 'proof-only')
      statements.push(
        database.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`),
      );
    const results = captureBatchResults(
      await database.batch(statements),
      statements.length,
    );
    let index = 0;
    const actual = decodeRawWorkflowSnapshotResult(
      results[index++],
      raw.address,
    );
    const reservation = input.reservation
      ? decodeStartReservationAdmissionResult(results[index++])
      : undefined;
    const proofRows =
      observed.reading.state === 'proof-only'
        ? snapshotResultRows(results[index++])
        : undefined;
    if (input.reservation)
      validateStartReservationAdmissionSchema(results[index++]);
    if (proofRows)
      await validateExecutionFenceAdmissionSchema(results[index++]);
    if (
      !actual ||
      !sameFields({ ...actual }, { ...row }) ||
      (input.reservation &&
        !sameReservation(reservation, input.reservation, input.execution))
    )
      return false;
    const snapshot = record(JSON.parse(actual.snapshot));
    const provenance = decodeInitialRunProvenance(
      record(snapshot.requestContext)[PROVENANCE],
      'present',
    );
    if (
      provenance.startToken !== input.execution.startToken ||
      provenance.attemptToken !== input.attemptToken
    )
      return false;
    const proofRow = proofRows?.[0];
    return (
      !proofRows ||
      (proofRows.length === 1 &&
        proofRow !== undefined &&
        sameFields(
          { ...decodeExecutionFenceAdmissionRow(proofRow).raw },
          {
            ...observed.raw,
            proof_run_id: row.runId,
            proof_table_prefix: row.tablePrefix,
            proof_workflow_id: row.workflowId,
            proof_start_token: input.execution.startToken,
            updated_at: nowMs,
          },
        ))
    );
  }

  async #diagnoseZero(
    database: InitialAdmissionDatabase,
    input: InitialRunAdmission,
    observed: ExecutionFenceAdmissionObservation,
    admissionSnapshot: string,
  ): Promise<DoStatusError> {
    try {
      const admission = await decisionRow(
        database,
        'SELECT json_valid(?1) AS storable',
        [admissionSnapshot],
      );
      if (!decisionFlag(admission, 'storable'))
        return new RunStateNotStorableError(
          input.execution.workflowId,
          input.execution.runId,
        );
      const [current, snapshot, reservation, owner] = await Promise.all([
        input.fence.readForAdmission(),
        readRawWorkflowSnapshot(database, input.execution, {
          missingTable: 'error',
        }),
        input.reservation
          ? input.reservationStore?.readForAdmission(input.reservation.key)
          : undefined,
        input.runOwnerGuard
          ? this.#readOwner(database, input.execution.runId)
          : undefined,
      ]);
      if (reservation && input.reservation) {
        if (
          reservation.owner.kind !== input.reservation.owner.kind ||
          reservation.owner.id !== input.reservation.owner.id
        )
          return new StartReservationOwnerMismatchError(input.reservation.key);
        if (
          reservation.targetKind !== input.reservation.targetKind ||
          reservation.targetId !== input.reservation.targetId
        )
          return new StartReservationTargetMismatchError(
            input.reservation.key,
            reservation,
          );
      }
      if (
        input.runOwnerGuard &&
        (!owner ||
          owner.owner_kind !== input.runOwnerGuard.owner.kind ||
          owner.owner_id !== input.runOwnerGuard.owner.id ||
          (owner.reservation_token !== null &&
            owner.reservation_token !== input.attemptToken))
      )
        return new RunAdmissionConflictError('run-owner-changed');
      if (input.reservation && !sameReservation(reservation, input.reservation))
        return new RunAdmissionConflictError('reservation-changed');
      assertMutationEpoch(current.reading, input.mutationEpoch);
      const stateAllowsStart = admitsRunStart(
        current.reading,
        input.proof?.key,
      );
      const proofRoundMatches =
        current.reading.state !== 'proof-only' ||
        (input.proof !== undefined &&
          current.reading.mutationEpoch === input.proof.mutationEpoch &&
          current.reading.transitionRevision ===
            input.proof.transitionRevision);
      const proofSlotUnbound =
        current.raw.proof_run_id === null &&
        current.raw.proof_table_prefix === null &&
        current.raw.proof_workflow_id === null &&
        current.raw.proof_start_token === null;
      if (
        !stateAllowsStart ||
        !proofRoundMatches ||
        (current.reading.state === 'proof-only' && !proofSlotUnbound)
      )
        return new ExecutionFencedError(
          current.reading.state,
          'initial run admission',
        );
      if (snapshot) return new RunAdmissionConflictError('run-exists');
      const { updated_at: _previousTime, ...previous } = observed.raw;
      return new RunAdmissionConflictError(
        sameFields({ ...current.raw }, previous)
          ? 'admission-raced'
          : 'fence-changed',
      );
    } catch (error) {
      if (
        error instanceof DoStatusError &&
        error.reason?.code === 'MUTATION_EPOCH_MISMATCH'
      )
        return error;
      return new ExecutionFenceUnreadableError(
        'initial run admission readback is not readable',
        { cause: error },
      );
    }
  }

  async #readOwner(
    database: InitialAdmissionDatabase,
    runId: string,
  ): Promise<Record<string, unknown> | undefined> {
    const rows = snapshotResultRows(
      await database
        .prepare(
          `SELECT resource_kind, resource_id, owner_kind, owner_id, reservation_token FROM ${RESOURCE_OWNER_TABLE} WHERE resource_kind = 'run' AND resource_id = ? LIMIT 2`,
        )
        .bind(runId)
        .all(),
    );
    if (rows.length > 1) throw new Error('run owner is not a singleton');
    const row = rows[0];
    if (!row) return undefined;
    const captured = {
      resource_kind: row.resource_kind,
      resource_id: row.resource_id,
      owner_kind: row.owner_kind,
      owner_id: row.owner_id,
      reservation_token: row.reservation_token,
    };
    if (
      captured.resource_kind !== 'run' ||
      captured.resource_id !== runId ||
      !isExecutionPrincipalKind(captured.owner_kind) ||
      !isExecutionPrincipalId(captured.owner_id) ||
      (captured.reservation_token !== null &&
        !isPathSafeId(captured.reservation_token))
    )
      throw new Error('run owner row is malformed');
    return captured;
  }
}
