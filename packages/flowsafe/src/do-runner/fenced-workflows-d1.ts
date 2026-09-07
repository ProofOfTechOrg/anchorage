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
  decodeExecutionFenceAdmissionRow,
  type ExecutionFenceAdmissionObservation,
  ExecutionFencedError,
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
  hasDisputedSettlement,
  parseRunLifecycle,
  projectTerminalLifecycle,
  RUN_LIFECYCLE_CONTEXT_KEY,
  RunLifecycleBlockedError,
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
  const meta = record(result).meta;
  const hasChanges = meta !== undefined && 'changes' in record(meta);
  return {
    results,
    ...(hasChanges ? { meta: { changes: record(meta).changes } } : {}),
  };
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
    nowMs < 0
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
        error: {
          name:
            intent.status === 'cancelled'
              ? 'RunCancelledError'
              : 'RunTimedOutError',
          message: next.terminal.error.message,
        },
      };
      cleanup = terminalCleanupFor(next);
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

/** Owned initial admission and repair; unscoped persistence delegates to the adapter. */
export class FencedWorkflowsStorageD1 extends WorkflowsStorageD1 {
  readonly [FENCED_WORKFLOW_STORAGE]?: FencedWorkflowAdmissionCapability;
  readonly #admission?: FencedWorkflowAdmissionCapability;
  readonly #scopes = new AsyncLocalStorage<AdmissionScope>();

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
          result = await database
            .prepare(`UPDATE "${expected.tablePrefix}mastra_workflow_snapshot"
          SET snapshot = ?1, updatedAt = ?2
          WHERE workflow_name = ?3 AND run_id = ?4
            AND snapshot = ?5 AND createdAt IS ?6 AND updatedAt IS ?7
            AND resourceId IS ?8
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
            )
            .all();
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
          const captured = captureStatementResult(result);
          const row = decodeRawWorkflowSnapshotResult(captured, expected);
          const changes = captured.meta?.changes;
          if (
            captured.meta &&
            (!Number.isSafeInteger(changes) ||
              changes !== captured.results.length)
          )
            throw new Error(
              'terminalization changes disagree with returned rows',
            );
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
    if (!scope) return super.persistWorkflowSnapshot(args);
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

  #initialRow(
    args: PersistInput,
    input: InitialRunAdmission,
  ): { row: RawWorkflowSnapshot; nowMs: number } {
    const { snapshot: original, resourceId, createdAt, updatedAt } = args;
    const snapshot = { ...original };
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
    const resource: unknown = resourceId;
    const serializedResource =
      resource == null
        ? null
        : resource instanceof Date
          ? resource.toISOString()
          : typeof resource === 'object'
            ? JSON.stringify(resource)
            : resource;
    if (serializedResource !== null && typeof serializedResource !== 'string')
      throw new InvalidExecutionIdentityError('admission');
    const contextBytes = JSON.stringify(snapshot.requestContext);
    const bytes = JSON.stringify(snapshot);
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
    const created = createdAt ? createdAt.toISOString() : nowIso;
    const updated = updatedAt ? updatedAt.toISOString() : nowIso;
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
    const fence = observed.raw;
    const semantic = [
      'state',
      'mutation_epoch',
      'require_mutation_epoch',
      'transition_revision',
      'last_transition_request',
      'proof_key',
      'proof_run_id',
      'proof_table_prefix',
      'proof_workflow_id',
      'proof_start_token',
    ].map((key) => bind(fence[key]));
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
    const statements = [
      database
        .prepare(`INSERT INTO "${row.tablePrefix}mastra_workflow_snapshot"
      (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
      SELECT ${fields.join(', ')}
      WHERE EXISTS (
        SELECT 1 FROM ${EXECUTION_FENCE_TABLE} AS f
        WHERE f.id = 'deployment' AND f.state = ${semantic[0]}
          AND f.mutation_epoch = ${semantic[1]} AND f.require_mutation_epoch = ${semantic[2]}
          AND f.transition_revision = ${semantic[3]} AND f.last_transition_request IS ${semantic[4]}
          AND f.proof_key IS ${semantic[5]} AND f.proof_run_id IS ${semantic[6]}
          AND f.proof_table_prefix IS ${semantic[7]} AND f.proof_workflow_id IS ${semantic[8]} AND f.proof_start_token IS ${semantic[9]}
          AND (f.require_mutation_epoch = 0 OR f.mutation_epoch = ${epoch})
          AND (f.state = 'open' OR (f.state = 'proof-only' AND f.proof_key = ${proofKey}
            AND f.transition_revision = ${proofRevision} AND f.mutation_epoch = ${proofEpoch}
            AND f.proof_run_id IS NULL AND f.proof_table_prefix IS NULL AND f.proof_workflow_id IS NULL AND f.proof_start_token IS NULL))
      ) ${reservation} ${owner}
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
    const refusal = await this.#diagnoseZero(database, input, observed);
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
      const counts = captured.map(({ results, meta }) => {
        if (
          results.length > 1 ||
          (meta &&
            (!Number.isSafeInteger(meta.changes) ||
              meta.changes !== results.length))
        )
          throw new Error('initial statement changes contradict returned rows');
        return results.length;
      });
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
  ): Promise<DoStatusError> {
    try {
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
