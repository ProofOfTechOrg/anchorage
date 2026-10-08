// SPDX-License-Identifier: Apache-2.0
// RunnerRuntime hosts Mastra workflow execution against injected storage.
// It is environment-free: the Durable Object shell feeds it
// D1-backed storage; tests feed it InMemoryStore. Durability comes from
// Mastra's own snapshot persistence — createRun() writes the initial
// snapshot, the engine persists after each step boundary, and resume()
// loads the snapshot — so a run started in one process resumes in any
// other process that shares the same database. Persistence only happens
// for workflows registered on a Mastra instance that has storage (core
// silently skips it otherwise), which is why this class owns the Mastra
// instance instead of running standalone workflows.
//
// Concurrency: start/resume and terminal transitions are serialized per run —
// keyed by the run's full identity (workflowId + runId) — via distinct
// execution and lifecycle FIFO locks.
// Without the execution lock, two concurrent resumes both pass the
// 'suspended' pre-check and the gated step's side effects execute twice — the
// exact failure an approval product must not have. Both locks are per runtime instance;
// cross-instance serialization comes from routing one DO instance per run
// (see durable-object.ts).

import type { Agent, ToolsInput } from '@mastra/core/agent';
import type { IMastraLogger } from '@mastra/core/logger';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import type {
  MastraCompositeStore,
  WorkflowsStorage,
} from '@mastra/core/storage';
import {
  type AnyWorkflow,
  cleanStepResult,
  type WorkflowRunState,
  type WorkflowRunStatus,
  type WorkflowState,
  type WorkflowStateField,
} from '@mastra/core/workflows';
import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';

import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from './breakwater-keys.js';
import { errorMessageOf, findInCauseChain } from './cause-chain.js';
import { DoStatusError } from './do-status-error.js';
import {
  assertMutationEpoch,
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  normalizeD1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeStartIdentity,
  type ProofEntryExpectation,
  type RunExecutionIdentity,
  RunStartPendingError,
  type StartIdentity,
} from './execution-admission.js';
import {
  isReservedExecutionContextKey,
  RUN_PROVENANCE_CONTEXT_KEY,
  stripReservedExecutionContext,
} from './execution-context.js';
import {
  admitsExistingRun,
  admitsRunStart,
  ExecutionFencedError,
  type ExecutionFenceStore,
} from './execution-fence.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
} from './fenced-workflow-capability.js';
import { isDefinitiveInitialAdmissionRefusal } from './initial-admission-refusal.js';
import { mastraRegistryEntries } from './mastra-registry.js';
import { isPathSafeId } from './path-safe-id.js';
import type { HostPubSub } from './pubsub.js';
import {
  exceedsRunInputDepth,
  runInputDepthMessage,
} from './run-input-depth.js';
import {
  advanceLifecycle,
  canonicalEconomicOperations,
  canonicalReplayPrincipals,
  canonicalScheduleDispatch,
  failureEnvelope,
  hasDisputedSettlement,
  legAbortReason,
  lifecycleFromRequestContext,
  projectTerminalLifecycle,
  RUN_FAILURE_MARKERS,
  RUN_LIFECYCLE_CONTEXT_KEY,
  RUN_SETTLING_MARKERS,
  type RunEconomicOperation,
  RunLifecycleBlockedError,
  type RunLifecyclePrincipal,
  type RunLifecycleState,
  type RunScheduleDispatch,
  RunSettledConflictError,
  RunStateNotStorableError,
  type RunTerminalCleanup,
  type RunTerminalErrorEnvelope,
  type RunTerminalStatus,
  runTerminalError,
  terminalCleanupFor,
  terminalLegAbortReason,
} from './run-lifecycle.js';
import {
  decodeProgressRunProvenance,
  decodeResumeCounts,
  nextResumeCount,
  type ProgressRunProvenance,
  runExecutionIdentityFor,
} from './run-provenance.js';
import {
  type CoreRunResult,
  errorText,
  isDurableRunStatus,
  isRunStatus,
  isTerminalRunStatus,
  type RunStatus,
  terminalStateFields,
  terminalStateUpdate,
} from './run-terminal-state.js';
import {
  captureReservation,
  type StartReservationReading,
} from './start-reservation-contract.js';
import { validateTablePrefix } from './table-prefix.js';
import {
  parseSnapshotObject,
  type RawWorkflowSnapshot,
} from './workflow-snapshot-row.js';

export {
  RunLifecycleBlockedError,
  type RunLifecycleBlockedReason,
  RunSettledConflictError,
  RunStateNotStorableError,
} from './run-lifecycle.js';
export type { RunStatus } from './run-terminal-state.js';

import type { StartIdempotencyStore } from './start-idempotency.js';

export class UnknownWorkflowError extends Error {
  constructor(workflowId: string) {
    super(`unknown workflow '${workflowId}'`);
    this.name = 'UnknownWorkflowError';
  }
}

export class UnknownRunError extends Error {
  constructor(workflowId: string, runId: string) {
    super(`no run '${runId}' found for workflow '${workflowId}'`);
    this.name = 'UnknownRunError';
  }
}

export class RunNotSuspendedError extends Error {
  constructor(workflowId: string, runId: string, status: WorkflowRunStatus) {
    super(
      `run '${runId}' of workflow '${workflowId}' is '${status}', not 'suspended'`,
    );
    this.name = 'RunNotSuspendedError';
  }
}

/**
 * A resume that names the suspension it is for, of a step that has since left
 * it: resumed and suspended again, or no longer suspended at all.
 */
export class SuspensionChangedError extends DoStatusError {
  readonly status = 409;
  readonly reason = { code: 'SUSPENSION_CHANGED' } as const;

  constructor(workflowId: string, runId: string, step: string) {
    super(
      `step '${step}' of run '${runId}' of workflow '${workflowId}' is no longer at the suspension the resume names`,
    );
    this.name = 'SuspensionChangedError';
  }
}

/**
 * An authoritative run-state read did not succeed, so nothing that read
 * returned is evidence about the run. One cause is Mastra answering from its
 * in-memory fallback instead of from storage — what comes back then describes
 * the Run object this isolate happens to hold rather than what is persisted.
 * Distinct from UnknownRunError: the run may
 * well exist and be suspended — nothing about it could be READ. The message
 * therefore names no cause: it is minted where the read failed, not where the
 * reason is known.
 */
export class RunStateUnreadableError extends Error {
  constructor(workflowId: string, runId: string, options?: ErrorOptions) {
    super(
      `run '${runId}' of workflow '${workflowId}' state is not readable`,
      options,
    );
    this.name = 'RunStateUnreadableError';
  }
}

export class RunAlreadyExistsError extends Error {
  constructor(workflowId: string, runId: string, status: RunStatus) {
    super(
      `run '${runId}' of workflow '${workflowId}' already exists (status '${status}')`,
    );
    this.name = 'RunAlreadyExistsError';
  }
}

export class RunTerminalConflictError extends Error {
  constructor(workflowId: string, runId: string, status: RunStatus) {
    super(
      `run '${runId}' of workflow '${workflowId}' is already terminal with status '${status}'`,
    );
    this.name = 'RunTerminalConflictError';
  }
}

/** A request the caller can fix: bad input/resume data or step selection. */
export class InvalidRunRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidRunRequestError';
  }
}

// Core surfaces input/resume-schema violations and wrong-step selections as
// untyped Errors. Message matching is brittle, so it is scoped to choosing
// the HTTP-facing error class only; anything unmatched propagates unchanged
// (a 500 at the DO boundary).
const CLIENT_ERROR_MARKERS = [
  'Invalid input',
  'Invalid resume data',
  'was not suspended',
  'No snapshot found',
];

function asClientError(error: unknown): InvalidRunRequestError | undefined {
  if (!(error instanceof Error)) return undefined;
  return CLIENT_ERROR_MARKERS.some((marker) => error.message.includes(marker))
    ? new InvalidRunRequestError(error.message)
    : undefined;
}

/** JSON-safe projection of a workflow run outcome for HTTP transport. */
export interface RunSummary {
  runId: string;
  status: RunStatus;
  /** Durable execution provenance used by approval reconciliation. */
  requestedBy?: string;
  /** Principal kind paired with `requestedBy`; absent on legacy snapshots. */
  requestedByKind?: ExecutionPrincipalKind;
  result?: unknown;
  error?: string;
  /** Structured terminal reason for Flowsafe-owned lifecycle transitions. */
  errorEnvelope?: RunTerminalErrorEnvelope;
  /** Epoch milliseconds. Persisted inside the authoritative request context. */
  deadlineAt?: number;
  /** Suspended step paths, e.g. [['approval']]. Present when status is 'suspended'. */
  suspended?: string[][];
  /** Keyed by suspended step id, e.g. { approval: { reason } }. */
  suspendPayload?: unknown;
  /**
   * Epoch-ms suspension time per dot-joined suspended step key (core clock),
   * e.g. { approval: 1751882400000 }. Present when status is 'suspended' and
   * the snapshot recorded step timestamps. Approval bridges copy the resumed
   * step's entry into CreateApprovalInput.suspendedAt so grant minting can
   * bind the decision to this exact suspension (clock-free).
   */
  suspendedAt?: Record<string, number>;
  /**
   * Epoch-ms resume time per dot-joined suspended step key (core clock).
   * INFORMATIONAL audit metadata only — NOT the grant-binding tie-breaker
   * (that is `resumeCount`). Mastra stamps it only on a payload-bearing
   * resume, so it is absent for a first suspension AND for any re-suspension
   * reached via a falsy resume; do not use its presence to tell a first
   * suspension from a re-suspension.
   */
  resumedAt?: Record<string, number>;
  /**
   * Runtime-owned monotonic per-step resume ordinal (dot-joined step key ->
   * count). ABSENT for a step's first suspension (never resumed), `1` after
   * the first resume, `2` after the second, and so on. This is the grant
   * binding tie-breaker paired with `suspendedAt`: unlike `resumedAt` the
   * runtime increments it on EVERY resume regardless of payload, so it is
   * collision-free and cannot be erased by a same-ms suspendedAt collision or
   * a no-payload resume. Approval bridges copy it into
   * CreateApprovalInput.resumeCount.
   */
  resumeCount?: Record<string, number>;
  /**
   * Dot-joined suspended step keys that wait as timers: the run's Durable
   * Object resumes each one itself when its deadline expires, so approval
   * bridges file no approval for it. Only `DurableObjectRunner` fills it.
   */
  suspensionTimers?: string[];
  /** ISO 8601. Present on status() projections (read from the stored snapshot). */
  createdAt?: string;
  /** ISO 8601. Present on status() projections (read from the stored snapshot). */
  updatedAt?: string;
}

const RUN_STATE_FIELDS: WorkflowStateField[] = [
  'result',
  'error',
  'steps',
  'suspendedPaths',
  'requestContext',
];

const RESUME_STATE_FIELDS: WorkflowStateField[] = [
  ...RUN_STATE_FIELDS,
  'payload',
];

/** @internal One physical observation; this does not certify a logical root. */
export type AuthoritativeStartState = {
  readonly provenance: ProgressRunProvenance;
  readonly snapshot: WorkflowRunState;
} & (
  | {
      readonly storage: 'd1';
      readonly execution: D1RunExecutionIdentity;
      readonly raw: RawWorkflowSnapshot;
    }
  | {
      readonly storage: 'unfenced';
      readonly execution: RunExecutionIdentity & { readonly tablePrefix: null };
      readonly raw?: never;
    }
) &
  (
    | { readonly kind: 'initial'; readonly summary?: never }
    | { readonly kind: 'result'; readonly summary: RunSummary }
  );

/** @internal Ordinary compatibility data, never execution-generation authority. */
export interface LegacyRunState {
  readonly kind: 'legacy';
  readonly provenanceVersion: 1 | undefined;
  readonly address: Pick<
    RunExecutionIdentity,
    'tablePrefix' | 'workflowId' | 'runId'
  >;
  readonly snapshot: WorkflowRunState;
  readonly summary: RunSummary;
}

interface LegacyRunProvenance {
  version: 1;
  /** Absent on unattributed runs; may be unpaired only on legacy snapshots. */
  requestedBy?: string;
  /** Absent on unattributed runs and snapshots written before principal kinds. */
  requestedByKind?: ExecutionPrincipalKind;
  /** Token of the start that created the run; stable across resume legs. */
  startToken: string;
  /** Token of the current execution leg. */
  attemptToken: string;
  resumeCounts: Array<[string, number]>;
}

type RunProvenance = LegacyRunProvenance | ProgressRunProvenance;

function runProvenance(
  state: Pick<WorkflowState | WorkflowRunState, 'requestContext'>,
): RunProvenance | undefined {
  const value = state.requestContext?.[RUN_PROVENANCE_CONTEXT_KEY];
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object') {
    throw new Error('stored run provenance is malformed');
  }
  if ((value as { version?: unknown }).version === 2)
    return decodeProgressRunProvenance(value);
  const candidate = value as Partial<LegacyRunProvenance>;
  if (
    candidate.version !== 1 ||
    (candidate.requestedBy !== undefined &&
      !isExecutionPrincipalId(candidate.requestedBy)) ||
    (candidate.requestedByKind !== undefined &&
      !isExecutionPrincipalKind(candidate.requestedByKind)) ||
    (candidate.requestedBy === undefined &&
      candidate.requestedByKind !== undefined) ||
    !isPathSafeId(candidate.attemptToken) ||
    (candidate.startToken !== undefined &&
      !isPathSafeId(candidate.startToken)) ||
    !Array.isArray(candidate.resumeCounts)
  ) {
    throw new Error('stored run provenance is malformed');
  }
  const counts = decodeResumeCounts(candidate.resumeCounts);
  return {
    version: 1,
    ...(candidate.requestedBy === undefined
      ? {}
      : { requestedBy: candidate.requestedBy }),
    ...(candidate.requestedByKind === undefined
      ? {}
      : { requestedByKind: candidate.requestedByKind }),
    attemptToken: candidate.attemptToken,
    startToken: candidate.startToken ?? candidate.attemptToken,
    resumeCounts: counts,
  };
}

// Registered agents may carry incompatible tool/output generics. The public
// method preserves each concrete type; this erased form is only for handing
// the heterogeneous registry to Mastra.
type ErasedRuntimeAgent = Agent<string, ToolsInput, unknown>;

function summarize(
  runId: string,
  result: CoreRunResult,
  counts?: ReadonlyMap<string, number>,
  requestedBy?: string,
  requestedByKind?: ExecutionPrincipalKind,
  deadlineAt?: number,
): RunSummary {
  let summary: RunSummary;
  switch (result.status) {
    case 'success':
      summary = { runId, status: result.status, result: result.result };
      break;
    case 'failed':
      summary = {
        runId,
        status: result.status,
        error: errorText(result.error),
      };
      break;
    case 'suspended': {
      summary = {
        runId,
        status: result.status,
        suspended: result.suspended,
        suspendPayload: result.suspendPayload,
      };
      const suspendedKeys = result.suspended.map((path) => path.join('.'));
      const suspendedAt = byStep(suspendedKeys, (key) =>
        suspendedAtOf(result.steps, key),
      );
      if (suspendedAt !== undefined) summary.suspendedAt = suspendedAt;
      const resumedAt = byStep(suspendedKeys, (key) =>
        resumedAtOf(result.steps, key),
      );
      if (resumedAt !== undefined) summary.resumedAt = resumedAt;
      // resumeCount is runtime-owned snapshot provenance, not core step state.
      const resumeCount = byStep(suspendedKeys, (key) => counts?.get(key));
      if (resumeCount !== undefined) summary.resumeCount = resumeCount;
      break;
    }
    case 'tripwire':
      summary = {
        runId,
        status: result.status,
        error: result.tripwire.reason,
      };
      break;
    default:
      summary = { runId, status: result.status };
  }
  if (requestedBy !== undefined) summary.requestedBy = requestedBy;
  if (requestedByKind !== undefined) {
    summary.requestedByKind = requestedByKind;
  }
  if (deadlineAt !== undefined) summary.deadlineAt = deadlineAt;
  return summary;
}

// Storage may hand timestamps back as Date or as a serialized string
// depending on the adapter's snapshot round-trip; normalize defensively.
function toIso(value: Date | string): string {
  return value instanceof Date
    ? value.toISOString()
    : new Date(value).toISOString();
}

// The resumed step for RunLeg: the caller's selection (dot-joined strings
// are nested paths, matching suspendedPaths key convention), else the single
// suspended step from the snapshot, else undefined (ambiguous multi-step
// resume without a selection — providers treat that as "no step-scoped
// capabilities", fail closed).
function resolveResumeStep(
  step: string | string[] | undefined,
  state: { suspendedPaths?: Record<string, number[]> },
): string[] | undefined {
  if (Array.isArray(step)) return [...step];
  if (typeof step === 'string') return step.split('.');
  const suspended = Object.keys(state.suspendedPaths ?? {});
  return suspended.length === 1 ? suspended[0]?.split('.') : undefined;
}

// The live attempt for a step. Repeated steps (foreach) store an array of
// attempts, so the current suspension/resume is the latest; a single-run step
// stores one entry.
function latestAttempt(
  steps: WorkflowState['steps'] | undefined,
  stepKey: string,
) {
  const entry = steps?.[stepKey];
  return Array.isArray(entry) ? entry[entry.length - 1] : entry;
}

/** Epoch-ms suspension time of the step's latest attempt, if recorded. */
function suspendedAtOf(
  steps: WorkflowState['steps'] | undefined,
  stepKey: string,
): number | undefined {
  return latestAttempt(steps, stepKey)?.suspendedAt;
}

/**
 * Epoch-ms resume time of the step's latest attempt, if recorded. Feeds the
 * INFORMATIONAL RunSummary.resumedAt only — Mastra stamps it solely on a
 * payload-bearing resume, so it is unreliable as a first-vs-re-suspension
 * signal. The grant binding uses snapshot provenance `resumeCount` instead.
 */
function resumedAtOf(
  steps: WorkflowState['steps'] | undefined,
  stepKey: string,
): number | undefined {
  return latestAttempt(steps, stepKey)?.resumedAt;
}

/**
 * One home for the per-step projection convention: collect
 * `stepKey -> value` over the suspended step keys, projecting an empty result
 * as undefined so JSON summaries omit the field. The accumulator is
 * null-prototype because step keys are author-chosen strings: on a plain
 * object literal a step named '__proto__' would route into the
 * Object.prototype setter and silently vanish (or, for an object-valued
 * payload, rewire the accumulator's prototype).
 */
function byStep<T>(
  suspendedKeys: readonly string[],
  lookup: (stepKey: string) => T | undefined,
): Record<string, T> | undefined {
  const map: Record<string, T> = Object.create(null);
  let size = 0;
  for (const key of suspendedKeys) {
    const value = lookup(key);
    if (value !== undefined) {
      map[key] = value;
      size += 1;
    }
  }
  return size > 0 ? map : undefined;
}

// Projection of the persisted WorkflowState for status(). Unlike summarize()
// (which shapes a just-finished WorkflowResult), this reads the stored
// snapshot: suspended paths come from suspendedPaths keys (dot-joined for
// nested steps) and suspend payloads from per-step results — the same keyed
// shape start/resume return. A tripwire status carries no reason here (the
// snapshot does not persist it). When the state is an in-memory approximation
// (isFromInMemory), steps are empty and timestamps are current-time; the
// projection truthfully degrades to status-only rather than fabricating
// detail.
type SummaryState = Pick<
  WorkflowState,
  'status' | 'result' | 'error' | 'steps' | 'requestContext' | 'suspendedPaths'
> & { createdAt: Date | string; updatedAt: Date | string };

function summarizeState(
  runId: string,
  state: SummaryState,
  counts?: ReadonlyMap<string, number>,
  requestedBy?: string,
  requestedByKind?: ExecutionPrincipalKind,
): RunSummary {
  const lifecycle = lifecycleFromRequestContext(state.requestContext);
  const summary: RunSummary = {
    runId,
    status: lifecycle?.terminal?.status ?? state.status,
    createdAt: toIso(state.createdAt),
    updatedAt: toIso(state.updatedAt),
  };
  if (lifecycle?.deadlineAt !== undefined) {
    summary.deadlineAt = lifecycle.deadlineAt;
  }
  if (lifecycle?.terminal) {
    summary.errorEnvelope = lifecycle.terminal.error;
    return summaryWithRequester(summary, requestedBy, requestedByKind);
  }
  summaryWithRequester(summary, requestedBy, requestedByKind);
  if (state.status === 'success') {
    summary.result = state.result;
  } else if (state.status === 'failed') {
    if (state.error) summary.error = errorText(state.error);
    const envelope = failureEnvelope(lifecycle);
    if (envelope) summary.errorEnvelope = envelope;
  } else if (state.status === 'suspended') {
    const suspendedKeys = Object.keys(state.suspendedPaths ?? {});
    summary.suspended = suspendedKeys.map((key) => key.split('.'));
    const suspendPayload = byStep(
      suspendedKeys,
      (key) => latestAttempt(state.steps, key)?.suspendPayload,
    );
    if (suspendPayload !== undefined) {
      summary.suspendPayload = suspendPayload;
    }
    const suspendedAt = byStep(suspendedKeys, (key) =>
      suspendedAtOf(state.steps, key),
    );
    if (suspendedAt !== undefined) summary.suspendedAt = suspendedAt;
    const resumedAt = byStep(suspendedKeys, (key) =>
      resumedAtOf(state.steps, key),
    );
    if (resumedAt !== undefined) summary.resumedAt = resumedAt;
    const resumeCount = byStep(suspendedKeys, (key) => counts?.get(key));
    if (resumeCount !== undefined) summary.resumeCount = resumeCount;
  }
  return summary;
}

function summaryFromSelectedSnapshot(
  runId: string,
  snapshot: WorkflowRunState,
  timestamps: { createdAt: string; updatedAt: string },
  provenance: RunProvenance | undefined,
): RunSummary {
  const steps = Object.fromEntries(
    Object.entries(snapshot.context ?? {})
      .filter(([key]) => key !== 'input' && key !== '__state')
      .map(([key, value]) => [key, cleanStepResult(value)]),
  ) as WorkflowState['steps'];
  return summarizeState(
    runId,
    {
      status: snapshot.status,
      result: snapshot.result,
      error: snapshot.error,
      requestContext: snapshot.requestContext,
      suspendedPaths: snapshot.suspendedPaths,
      steps,
      ...timestamps,
    },
    provenance ? new Map(provenance.resumeCounts) : undefined,
    provenance?.requestedBy,
    provenance?.requestedByKind,
  );
}

function summaryWithRequester(
  summary: RunSummary,
  requestedBy?: string,
  requestedByKind?: ExecutionPrincipalKind,
): RunSummary {
  if (requestedBy !== undefined) summary.requestedBy = requestedBy;
  if (requestedByKind !== undefined) summary.requestedByKind = requestedByKind;
  return summary;
}

/**
 * Which execution leg the provider is minting for. On resume, `step` is the
 * resumed step's normalized path — taken from the caller's step selection,
 * or resolved from the snapshot when exactly one step is suspended;
 * undefined when the target step cannot be determined (ambiguous multi-step
 * resume without an explicit selection). `suspendedAt` is the epoch-ms
 * timestamp of that step's CURRENT suspension (from the persisted snapshot):
 * providers bind capabilities to the specific suspension they were granted
 * for — an approval decided before this suspension began belongs to an
 * earlier incarnation of the gate and must not mint again (see flowsafe's
 * approvalGrantProvider). `resumeCount` pairs with `suspendedAt` — the
 * runtime-owned monotonic resume ordinal (undefined on a step's first
 * suspension, `1,2,…` on successive re-suspensions) — so a provider can tell
 * two same-step suspensions apart even when their `suspendedAt` stamps collide
 * within a millisecond. Unlike the payload-conditional `resumedAt`, it is the
 * tie-breaker: the runtime increments it on every resume, so no-payload
 * resumes cannot erase the first-vs-re-suspension distinction.
 */
export type RunLeg =
  | { kind: 'start' }
  | {
      kind: 'resume';
      step?: string[];
      suspendedAt?: number;
      resumeCount?: number;
    };

/**
 * Server-side requestContext supplied during start and resume preparation.
 * Trusted host starts and verified schedule targets may supply non-reserved
 * application context through storedRequestContext. Capability values,
 * including approval grants, derive from trusted server-side state through
 * this provider (security-threat-model.md, trust boundary 6). Use trusted
 * sources such as the flowsafe approval store; never derive capabilities
 * from client input, model output, or tool results.
 *
 * Resume values overwrite matching persisted keys. Omitting a key retains
 * its persisted value. A provider that scopes a capability per leg must
 * return its key for each leg, using an empty grant list to revoke persisted
 * connector grants.
 */
export type RequestContextProvider = (
  workflowId: string,
  runId: string,
  leg: RunLeg,
) =>
  | Record<string, unknown>
  | undefined
  | Promise<Record<string, unknown> | undefined>;

const TRUSTED_IDENTITY_CONTEXT_KEYS = new Set([
  BREAKWATER_ACTOR_KEY,
  'breakwater.auditContext',
  'threadId',
  'resourceId',
]);

function orderedRequestContext(
  runtimeContext: Record<string, unknown>,
  provided: Record<string, unknown> | undefined,
  scheduled: Record<string, unknown> | undefined,
): RequestContext {
  const application: Array<[string, unknown]> = [];
  const capabilities: Array<[string, unknown]> = [];
  const identity: Array<[string, unknown]> = [];
  for (const entry of Object.entries(provided ?? {})) {
    const [key] = entry;
    if (
      key === BREAKWATER_WORKFLOW_SCOPE_KEY ||
      key === BREAKWATER_ISOLATION_SCOPE_KEY ||
      key === BREAKWATER_CONNECTOR_EXECUTION_KEY ||
      key === RUN_PROVENANCE_CONTEXT_KEY ||
      key === RUN_LIFECYCLE_CONTEXT_KEY ||
      key === 'runId'
    ) {
      continue;
    }
    if (TRUSTED_IDENTITY_CONTEXT_KEYS.has(key)) {
      identity.push(entry);
    } else if (isReservedExecutionContextKey(key)) {
      capabilities.push(entry);
    } else {
      application.push(entry);
    }
  }
  return new RequestContext([
    ...Object.entries(stripReservedExecutionContext(scheduled)),
    ...application,
    ...Object.entries(runtimeContext),
    ...capabilities,
    ...identity,
  ]);
}

export interface RunnerRuntimeOptions {
  storage: MastraCompositeStore;
  logger?: IMastraLogger | false;
  /** Consulted on every start/resume — see RequestContextProvider. */
  requestContextForRun?: RequestContextProvider;
  /**
   * The host Durable Object's single pubsub identity from do-runner/pubsub.ts, threaded
   * here by init() alongside storage so a host that configures it
   * reaches the runtime with no host change.
   *
   * Passed to core at `workflow.createRun({ runId, pubsub })` and to Mastra
   * at construction. The `pubsub` getter exposes the held identity so an
   * agent runner sharing this isolate uses the same feed. When absent, core
   * defaults a fresh emitter per run.
   */
  pubsub?: HostPubSub;
  /**
   * The deployment execution fence (execution-fence.ts), consulted on EVERY
   * start and resume. THIS is the closure guarantee for runs: a check here
   * cannot be routed around by a surface that forgot to gate itself.
   *
   * Absent ⇒ unfenced.
   */
  executionFence?: ExecutionFenceStore;
  /**
   * The deployment's start reservations (start-idempotency.ts). The runtime
   * neither creates nor claims one — the surfaces above it do — but it owns
   * the terminal reconcile that marks a key spent.
   *
   * A reservation that never settles is not a correctness bug (replay still
   * answers from the snapshot) but it never leaves the drain inventory and
   * never becomes purgeable, so a deployment would eventually be unable to
   * prove itself empty.
   *
   * Absent ⇒ no reconcile.
   */
  startIdempotency?: StartIdempotencyStore;
}

/** @inline */
type OptionalRunRequester =
  | {
      /** Trusted requester id persisted with the authoritative run snapshot. */
      requestedBy: string;
      /** Trusted requester kind persisted alongside `requestedBy`. */
      requestedByKind: ExecutionPrincipalKind;
    }
  | {
      requestedBy?: never;
      requestedByKind?: never;
    };

export type StartRunOptions = {
  /**
   * Required: the runtime never generates a runId. Hosts mint it server-side
   * (createRunRouter) so a client can never choose the identity a run is
   * keyed by everywhere it lands. A generation fallback here would let any caller
   * that forgets to mint create a run under an id the host never issued.
   */
  runId: string;
  inputData?: unknown;
  /** Schedule-target or ordinary trusted-start state. */
  initialState?: unknown;
  /**
   * Non-reserved context supplied by a trusted host start or a verified
   * schedule target.
   */
  storedRequestContext?: Record<string, unknown>;
  /** Host correlation token for this execution leg. */
  attemptToken?: string;
  /** Relative run deadline, measured from this start. */
  deadlineMs?: number;
  /** Trusted settlement projection; never accepted by the public run router. */
  economicOperations?: readonly RunEconomicOperation[];
  /** Trusted schedule source; never accepted directly from a public request. */
  scheduleDispatch?: RunScheduleDispatch;
  /**
   * The start's idempotency key. The execution fence's proof-only state admits
   * the start whose key matches its nominated proof key.
   *
   * INTERNAL: it reaches the runtime from a trusted host seam, never from a
   * request body and never through an open request-context key — a tenant able
   * to name the proof key could start a run on a deployment that is supposed to
   * be executing exactly one.
   * @internal
   */
  idempotencyKey?: string;
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly startReservation?: StartReservationReading;
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly mutationEpoch?: number;
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly startIdentity?: StartIdentity;
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly agentStart?: { readonly threaded: boolean };
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly onPreparedStartIdentity?: (
    execution: RunExecutionIdentity,
  ) => void | Promise<void>;
  /** @internal Captured infrastructure authority; never request-context data. */
  readonly runOwnerGuard?: {
    readonly owner: StartIdentity['owner'];
    readonly reservationToken: string;
  };
  /**
   * @internal An abort controller the runtime aborts wherever it aborts the
   * leg: when the liveness touch finds the run settled by another instance,
   * which can happen before the leg's engine run exists, and when a terminate
   * or a deadline handled in this isolate cancels the leg, after the engine
   * run's own abort, with a {@link terminalLegAbortReason}. Never
   * request-context data.
   */
  readonly legAbort?: AbortController;
} & OptionalRunRequester;

export type ResumeRunOptions = {
  /** Suspended step id (or nested path). Optional when only one step is suspended. */
  step?: string | string[];
  resumeData?: unknown;
  /**
   * Host preparation that must consume the exact trusted context this resume
   * will execute with. Runs once, inside the per-run lock, before createRun.
   * Its second argument is the run's persisted workflow input, `undefined`
   * when the snapshot holds none. Treat it as read-only.
   * @internal
   */
  prepareExecution?: (
    requestContext: RequestContext,
    workflowInput: unknown,
  ) => Promise<void>;
  /** @internal See {@link StartRunOptions.legAbort}. */
  readonly legAbort?: AbortController;
  /** Replace the persisted deadline relative to this resume. */
  deadlineMs?: number;
  /** Trusted settlement projection for the resumed execution leg. */
  economicOperations?: readonly RunEconomicOperation[];
  /**
   * The suspension this resume is for, as a decided approval records it: the
   * step's `suspendedAt` and its `resumeCount`, absent for a step's first
   * suspension. When the resumed step is at another suspension the resume
   * throws SuspensionChangedError before the step runs. The resumed step must
   * be resolvable: named by `step`, or the only suspended step.
   */
  expectedSuspension?: {
    readonly suspendedAt: number;
    readonly resumeCount?: number;
  };
} & OptionalRunRequester;

export interface RunLifecycleCas {
  expectedRevision: number;
  expectedDeadlineAt?: number;
}

export interface RunLifecycleTransitionResult {
  summary: RunSummary;
  transitioned: boolean;
  casMatched: boolean;
  cleanup: RunTerminalCleanup;
}

/** How often an executing leg marks its run row live. */
const RUN_LEG_TOUCH_MS = 30_000;
// Longer than the largest Workers CPU limit (five minutes), which is the most
// a busy step can delay a touch timer: a row untouched this long has no leg
// left marking it, on this instance or one being replaced.
const RUN_LEG_SILENT_MS = 360_000;

/** Has no leg marked this row live within RUN_LEG_SILENT_MS? */
function rowSilent(raw: RawWorkflowSnapshot, now: number): boolean {
  const touchedAt = Date.parse(raw.updatedAt);
  if (!Number.isFinite(touchedAt))
    throw new RunStateUnreadableError(raw.workflowId, raw.runId);
  return now - touchedAt >= RUN_LEG_SILENT_MS;
}

/** A compare-and-set write found the run's row changed since the read it decided from. */
class RowChangedError extends Error {
  constructor() {
    super('the run row changed since it was read');
    this.name = 'RowChangedError';
  }
}

/**
 * Compare-and-set passes a lifecycle transition makes before it answers that
 * the run's row keeps changing. A touch moves the row at most once per
 * RUN_LEG_TOUCH_MS, so a miss it causes clears on the next pass; a leg writing
 * snapshots back to back misses every time when it runs on another instance,
 * or, over a row SQLite cannot parse, in this object too.
 */
const LIFECYCLE_WRITE_ATTEMPTS = 5;

function snapshotOfRow(row: RawWorkflowSnapshot): WorkflowRunState {
  const state = parseSnapshotObject(row.snapshot) as
    | WorkflowRunState
    | undefined;
  if (state === undefined || state.runId !== row.runId)
    throw new RunStateUnreadableError(row.workflowId, row.runId);
  return state;
}

const TERMINABLE_RUN_STATUSES = new Set<RunStatus>([
  'running',
  'waiting',
  'pending',
  'paused',
  'waiting_callback',
  'waiting_signal',
  'retry_wait',
  'suspended',
]);

function relativeDeadline(
  deadlineMs: unknown,
  now = Date.now(),
): number | undefined {
  if (deadlineMs === undefined) return undefined;
  if (!Number.isSafeInteger(deadlineMs) || (deadlineMs as number) < 0) {
    throw new InvalidRunRequestError(
      'deadlineMs must be a nonnegative safe integer',
    );
  }
  const deadlineAt = now + (deadlineMs as number);
  if (!Number.isSafeInteger(deadlineAt)) {
    throw new InvalidRunRequestError('deadlineMs exceeds the supported range');
  }
  return deadlineAt;
}

function assertLegAbort(legAbort: unknown): void {
  if (legAbort !== undefined && !(legAbort instanceof AbortController))
    throw new InvalidRunRequestError('legAbort is malformed');
}

function assertRunInputDepth(field: string, value: unknown): void {
  if (exceedsRunInputDepth(value))
    throw new InvalidRunRequestError(runInputDepthMessage(field));
}

/**
 * `resumeCount` is absent on a first suspension and 1, 2, … after it, so 0 is
 * malformed rather than a suspension the step can be at.
 */
function captureExpectedSuspension(
  value: unknown,
): ResumeRunOptions['expectedSuspension'] {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new InvalidRunRequestError('expectedSuspension is malformed');
  const { suspendedAt, resumeCount } = value as Record<string, unknown>;
  if (
    !Number.isSafeInteger(suspendedAt) ||
    (suspendedAt as number) < 0 ||
    (resumeCount !== undefined &&
      (!Number.isSafeInteger(resumeCount) || (resumeCount as number) < 1))
  )
    throw new InvalidRunRequestError('expectedSuspension is malformed');
  return Object.freeze({
    suspendedAt: suspendedAt as number,
    ...(resumeCount === undefined
      ? {}
      : { resumeCount: resumeCount as number }),
  });
}

function isRunStateNotStorable(error: unknown): boolean {
  return findInCauseChain(
    error,
    (link) => link instanceof RunStateNotStorableError,
    { rootOnly: false },
  );
}

function captureStartRunOptions(source: StartRunOptions): StartRunOptions {
  const {
    runId,
    inputData,
    initialState,
    storedRequestContext,
    attemptToken,
    deadlineMs,
    economicOperations: rawOperations,
    scheduleDispatch: rawDispatch,
    idempotencyKey,
    requestedBy,
    requestedByKind,
    mutationEpoch: rawEpoch,
    startIdentity: rawIdentity,
    agentStart: rawAgentStart,
    onPreparedStartIdentity,
    runOwnerGuard: rawGuard,
    startReservation: rawReservation,
    legAbort,
  } = source;
  assertLegAbort(legAbort);
  if (requestedBy !== undefined && !isExecutionPrincipalId(requestedBy)) {
    throw new InvalidRunRequestError('requestedBy is malformed');
  }
  if (
    requestedByKind !== undefined &&
    !isExecutionPrincipalKind(requestedByKind)
  ) {
    throw new InvalidRunRequestError('requestedByKind is malformed');
  }
  if ((requestedBy === undefined) !== (requestedByKind === undefined)) {
    throw new InvalidRunRequestError(
      'requestedBy and requestedByKind must be provided together',
    );
  }
  const mutationEpoch = normalizeMutationEpoch(rawEpoch);
  const startIdentity =
    rawIdentity === undefined ? undefined : normalizeStartIdentity(rawIdentity);
  const startReservation =
    rawReservation === undefined
      ? undefined
      : captureReservation(rawReservation, 'started');
  let agentStart: StartRunOptions['agentStart'];
  if (rawAgentStart !== undefined) {
    if (
      rawAgentStart === null ||
      typeof rawAgentStart !== 'object' ||
      Array.isArray(rawAgentStart)
    ) {
      throw new InvalidRunRequestError('agentStart is malformed');
    }
    const { threaded } = rawAgentStart;
    if (typeof threaded !== 'boolean') {
      throw new InvalidRunRequestError('agentStart is malformed');
    }
    agentStart = Object.freeze({ threaded });
  }
  if (agentStart !== undefined && startIdentity?.target.kind !== 'agent')
    throw new InvalidRunRequestError('agentStart has no agent target');
  if (startIdentity?.target.kind === 'agent' && agentStart === undefined) {
    throw new InvalidRunRequestError(
      'agentStart is required for an agent target',
    );
  }
  if (
    startIdentity &&
    (startIdentity.owner.id !== requestedBy ||
      startIdentity.owner.kind !== requestedByKind)
  ) {
    throw new InvalidRunRequestError(
      'startIdentity owner does not match requester',
    );
  }
  if (
    onPreparedStartIdentity !== undefined &&
    typeof onPreparedStartIdentity !== 'function'
  ) {
    throw new InvalidRunRequestError('onPreparedStartIdentity is malformed');
  }
  let runOwnerGuard: StartRunOptions['runOwnerGuard'];
  if (rawGuard !== undefined) {
    if (
      rawGuard === null ||
      typeof rawGuard !== 'object' ||
      Array.isArray(rawGuard)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    const { owner: rawOwner, reservationToken } = rawGuard;
    if (
      rawOwner === null ||
      typeof rawOwner !== 'object' ||
      Array.isArray(rawOwner)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    const { kind, id } = rawOwner;
    if (
      !isExecutionPrincipalKind(kind) ||
      !isExecutionPrincipalId(id) ||
      !isPathSafeId(reservationToken)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    runOwnerGuard = Object.freeze({
      owner: Object.freeze({ kind, id }),
      reservationToken,
    });
  }
  if (
    deadlineMs !== undefined &&
    (!Number.isSafeInteger(deadlineMs) || deadlineMs < 0)
  ) {
    throw new InvalidRunRequestError(
      'deadlineMs must be a nonnegative safe integer',
    );
  }
  let dispatch: RunScheduleDispatch | undefined;
  if (rawDispatch !== undefined) {
    if (
      rawDispatch === null ||
      typeof rawDispatch !== 'object' ||
      Array.isArray(rawDispatch)
    ) {
      throw new Error('stored run lifecycle is malformed');
    }
    const { scheduleId, dispatchId } = rawDispatch;
    dispatch = { scheduleId, dispatchId };
  }
  let operations: RunEconomicOperation[] | undefined;
  if (rawOperations !== undefined) {
    if (!Array.isArray(rawOperations)) {
      throw new Error('stored run lifecycle is malformed');
    }
    const length = rawOperations.length;
    if (!Number.isSafeInteger(length) || length < 0 || length > 0xffff_ffff) {
      throw new Error('stored run lifecycle is malformed');
    }
    operations = new Array<RunEconomicOperation>(length);
    for (let index = 0; index < length; index++) {
      if (!(index in rawOperations))
        throw new Error('stored run lifecycle is malformed');
      const entry = rawOperations[index];
      if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
        throw new Error('stored run lifecycle is malformed');
      }
      const { id, settlementState } = entry;
      operations[index] = { id, settlementState };
    }
  }
  const captured = {
    runId,
    inputData,
    initialState,
    storedRequestContext,
    attemptToken,
    deadlineMs,
    economicOperations: canonicalEconomicOperations(operations),
    scheduleDispatch: canonicalScheduleDispatch(dispatch),
    idempotencyKey,
    mutationEpoch,
    startIdentity,
    agentStart,
    onPreparedStartIdentity,
    runOwnerGuard,
    startReservation,
    legAbort,
  };
  return Object.freeze(
    requestedBy !== undefined && requestedByKind !== undefined
      ? { ...captured, requestedBy, requestedByKind }
      : captured,
  );
}

function lifecycleForStart(
  options: StartRunOptions,
): RunLifecycleState | undefined {
  const deadlineAt = relativeDeadline(options.deadlineMs);
  const economicOperations = canonicalEconomicOperations(
    options.economicOperations,
  );
  const scheduleDispatch = canonicalScheduleDispatch(options.scheduleDispatch);
  if (
    deadlineAt === undefined &&
    economicOperations === undefined &&
    scheduleDispatch === undefined
  ) {
    return undefined;
  }
  return {
    version: 1,
    revision: 1,
    ...(deadlineAt === undefined ? {} : { deadlineAt }),
    ...(economicOperations === undefined ? {} : { economicOperations }),
    ...(scheduleDispatch === undefined ? {} : { scheduleDispatch }),
  };
}

function effectiveLifecycle(
  persisted: RunLifecycleState | undefined,
  active: RunLifecycleState | undefined,
): RunLifecycleState | undefined {
  if (!persisted) return active;
  if (!active) return persisted;
  // As lifecycleMergeSql decides: a stored intent never joins a lifecycle it
  // was not checked against.
  if (persisted.transitionIntent && hasDisputedSettlement(active))
    return active;
  if (persisted.revision > active.revision) return persisted;
  if (active.revision > persisted.revision) return active;
  const { transitionIntent: storedIntent, ...persistedWithoutIntent } =
    persisted;
  const transitionIntent =
    storedIntent &&
    (storedIntent.expectedDeadlineAt === undefined ||
      storedIntent.expectedDeadlineAt === active.deadlineAt)
      ? storedIntent
      : active.transitionIntent;
  return {
    ...persistedWithoutIntent,
    ...active,
    ...(transitionIntent ? { transitionIntent } : {}),
    ...((persisted.terminal ?? active.terminal)
      ? { terminal: persisted.terminal ?? active.terminal }
      : {}),
  };
}

/**
 * A stored request context with a leg's values laid over it, and the lifecycle
 * that results. Summary selection reads the attempt token in the context that
 * results, so every writer of a leg's outcome builds it here.
 */
function overlayLegContext(stored: unknown, overlay: Record<string, unknown>) {
  const persisted =
    stored !== null && typeof stored === 'object' && !Array.isArray(stored)
      ? (stored as Record<string, unknown>)
      : {};
  const context: Record<string, unknown> = { ...persisted, ...overlay };
  const persistedLifecycle = lifecycleFromRequestContext(persisted);
  return {
    context,
    persistedLifecycle,
    lifecycle: effectiveLifecycle(
      persistedLifecycle,
      lifecycleFromRequestContext(context),
    ),
  };
}

/** The request-context keys summary selection reads from a run's stored context. */
const SELECTION_CONTEXT_KEYS = [
  RUN_PROVENANCE_CONTEXT_KEY,
  RUN_LIFECYCLE_CONTEXT_KEY,
] as const;

/** The state with which the runtime fails a run by one of its failure markers. */
function failedByMarker(marker: keyof typeof RUN_FAILURE_MARKERS, now: number) {
  const { errorName, envelope } = RUN_FAILURE_MARKERS[marker];
  return {
    ...terminalStateFields('failed'),
    error: { name: errorName, message: envelope.message },
    timestamp: now,
  };
}

type StoredRunScope = Parameters<
  NonNullable<FencedWorkflowAdmissionCapability['withStoredRun']>
>[0];

type CapturedWorkflowStorage = {
  readonly workflows: WorkflowsStorage;
  readonly read: WorkflowsStorage['getWorkflowRunById'];
  readonly load: WorkflowsStorage['loadWorkflowSnapshot'];
  readonly persist: WorkflowsStorage['persistWorkflowSnapshot'];
} & (
  | { readonly storage: 'unfenced'; readonly tablePrefix: null }
  | {
      readonly storage: 'd1';
      readonly tablePrefix: string;
      readonly capability: FencedWorkflowAdmissionCapability;
      readonly database: FencedWorkflowAdmissionCapability['database'];
      readonly readSnapshot: FencedWorkflowAdmissionCapability['readSnapshot'];
      readonly admit: FencedWorkflowAdmissionCapability['withInitialAdmission'];
      readonly terminalize: FencedWorkflowAdmissionCapability['terminalizeInitialAdmission'];
      readonly touch?: FencedWorkflowAdmissionCapability['touchRun'];
      readonly replace?: FencedWorkflowAdmissionCapability['replaceSnapshot'];
      readonly patch?: FencedWorkflowAdmissionCapability['patchRunLifecycle'];
      readonly enterStoredRun?: FencedWorkflowAdmissionCapability['withStoredRun'];
    }
);

type ActiveRun = {
  run?: { cancel(): Promise<void>; readonly abortController: AbortController };
  legAbort?: AbortController;
  lifecycle?: RunLifecycleState;
  requestContext?: RequestContext;
  source?: CapturedWorkflowStorage;
  storedRunScope?: StoredRunScope;
};

type TerminalTransitionOptions = {
  cas?: RunLifecycleCas;
  replayPrincipals?: readonly RunLifecyclePrincipal[];
  replayingPrincipal?: RunLifecyclePrincipal;
};

type TransitionRead = {
  state: WorkflowRunState;
  row?: RawWorkflowSnapshot;
};

/** @internal An owning recovery's selected durable outcome. */
export type RecoveredStart =
  | { kind: 'ordinary'; summary: RunSummary }
  | { kind: 'lifecycle'; transition: RunLifecycleTransitionResult };

/** @internal What settling a run with no live execution leg found or wrote. */
export type InterruptedRunSettlement =
  | { kind: 'absent' }
  | { kind: 'durable' }
  /** A leg may still drive the run here or on an instance being replaced. */
  | { kind: 'live' }
  | {
      kind: 'transition';
      /** The recorded cancellation or timeout, completed; cleanup is the host's. */
      transition: RunLifecycleTransitionResult;
      /** Its first replay principal, for an owner already released. */
      replayPrincipal: RunLifecyclePrincipal;
    }
  | { kind: 'interrupted'; summary: RunSummary };

type RecoveryTargetExpectation =
  | { readonly kind: 'workflow' }
  | {
      readonly kind: 'agent';
      readonly id: string;
      readonly threadId: string;
      readonly owner: StartIdentity['owner'];
      readonly threaded: boolean;
    };

function sameExecution(
  a: RunExecutionIdentity,
  b: RunExecutionIdentity,
): boolean {
  return (
    a.tablePrefix === b.tablePrefix &&
    a.workflowId === b.workflowId &&
    a.runId === b.runId &&
    a.startToken === b.startToken
  );
}

function assertClaimIdentity(
  claim: StartReservationReading,
  execution: StartIdentity & { runId: string },
  key = claim.key,
): void {
  if (
    claim.key !== key ||
    claim.runId !== execution.runId ||
    claim.owner.id !== execution.owner.id ||
    claim.owner.kind !== execution.owner.kind ||
    claim.targetKind !== execution.target.kind ||
    claim.targetId !== execution.target.id ||
    claim.threadId !==
      (execution.target.kind === 'agent'
        ? execution.target.threadId
        : undefined)
  ) {
    throw new InvalidRunRequestError(
      'start reservation disagrees with execution identity',
    );
  }
}

export class RunnerRuntime {
  readonly #storage: MastraCompositeStore;
  readonly #logger: IMastraLogger | false;
  readonly #requestContextForRun?: RequestContextProvider;
  readonly #agents = new Map<string, ErasedRuntimeAgent>();
  readonly #workflows = new Map<string, AnyWorkflow>();
  readonly #runLocks = new Map<string, Promise<unknown>>();
  readonly #activeRuns = new Map<string, ActiveRun>();
  readonly #terminalAbortIntents = new Map<string, RunTerminalStatus>();
  readonly #lifecycleLocks = new Map<string, Promise<unknown>>();
  // The host DO's pubsub identity (RunnerRuntimeOptions.pubsub) is passed to
  // Mastra and each createRun so a configured host shares one feed.
  // Undefined ⇒ core defaults a fresh emitter per run.
  readonly #pubsub?: HostPubSub;
  readonly #executionFence?: ExecutionFenceStore;
  readonly #startIdempotency?: StartIdempotencyStore;
  #mastra?: Mastra;

  constructor(options: RunnerRuntimeOptions) {
    this.#storage = options.storage;
    this.#logger = options.logger ?? false;
    this.#requestContextForRun = options.requestContextForRun;
    this.#pubsub = options.pubsub;
    this.#executionFence = options.executionFence;
    this.#startIdempotency = options.startIdempotency;
  }

  /**
   * The deployment execution fence this runtime enforces, or undefined when
   * the host built an unfenced runtime. Exposed so the surfaces ABOVE the
   * runtime gate on the same store rather than constructing a second one.
   */
  get executionFence(): ExecutionFenceStore | undefined {
    return this.#executionFence;
  }

  /**
   * The deployment's start reservations, or undefined for a host with no
   * database to reserve against. Exposed for the same reason the fence is: the
   * surfaces ABOVE this runtime reserve and claim against it, and two stores
   * over two bindings would be two different tables answering the same key.
   */
  get startIdempotency(): StartIdempotencyStore | undefined {
    return this.#startIdempotency;
  }

  /**
   * Is this run EXECUTING in this isolate right now?
   *
   * The liveness half of the idempotent-start replay decision, answered from
   * the same `#activeRuns` map the cancel path uses — the runtime's own record
   * of runs it is currently driving. It is in-memory ON PURPOSE: liveness is a
   * property of an isolate that is running code, and any durable proxy for it
   * (a journal, a heartbeat, a timestamp) would keep saying "live" after the
   * isolate that wrote it was evicted, which is precisely the case the probe
   * exists to detect.
   *
   * A `false` here therefore means "not running HERE", which is authoritative
   * only where the run has exactly one possible host — a run Durable Object
   * addressed by `idFromName(workflowId:runId)`, or the thread object an agent
   * run is bound to. Callers that probe across an object boundary must ask the
   * object that owns the run, never their own runtime.
   */
  isRunActive(workflowId: string, runId: string): boolean {
    return this.#activeRuns.has(this.#runKey(workflowId, runId));
  }

  /**
   * The host pubsub identity this runtime was built with, or undefined when the
   * host configured none. Exposed so callers can verify the identity and so
   * an agent runner sharing this runtime's isolate takes THIS instance rather
   * than building a second feed.
   */
  get pubsub(): HostPubSub | undefined {
    return this.#pubsub;
  }

  /**
   * Register an agent that a runtime-owned workflow resolves by id.
   *
   * Durable-agent workflows persist only the agent id. Keeping the raw agent
   * on the same Mastra instance as the workflow lets a resumed run resolve it
   * after isolate eviction, when the original DurableAgent instance is gone.
   */
  registerAgent<TAgentId extends string, TTools extends ToolsInput, TOutput>(
    agent: Agent<TAgentId, TTools, TOutput>,
  ): void {
    if (this.#mastra) {
      throw new Error(
        'RunnerRuntime: register all agents before the first run — the Mastra instance is frozen once runs start',
      );
    }
    if (this.#agents.has(agent.id)) {
      throw new Error(`RunnerRuntime: duplicate agent id '${agent.id}'`);
    }
    this.#agents.set(agent.id, agent as unknown as ErasedRuntimeAgent);
  }

  register(workflow: AnyWorkflow): void {
    if (this.#mastra) {
      throw new Error(
        'RunnerRuntime: register all workflows before the first run — the Mastra instance is frozen once runs start',
      );
    }
    if (!isPathSafeId(workflow.id)) {
      throw new Error(
        `RunnerRuntime: workflow id '${workflow.id}' must be URL-path-safe — it feeds the DO name join and the /runs/:workflowId/:runId path`,
      );
    }
    if (this.#workflows.has(workflow.id)) {
      throw new Error(`RunnerRuntime: duplicate workflow id '${workflow.id}'`);
    }
    this.#workflows.set(workflow.id, workflow);
  }

  workflowIds(): string[] {
    return [...this.#workflows.keys()];
  }

  async #assertStartFence(
    idempotencyKey: string | undefined,
    mutationEpoch: number | undefined,
  ): Promise<ProofEntryExpectation | undefined> {
    const fence = this.#executionFence;
    if (!fence) return;
    const reading = await fence.read();
    assertMutationEpoch(reading, mutationEpoch);
    if (!admitsRunStart(reading, idempotencyKey))
      throw new ExecutionFencedError(reading.state, 'run start');
    if (reading.state === 'proof-only' && reading.proofKey !== undefined)
      return Object.freeze({
        key: reading.proofKey,
        mutationEpoch: reading.mutationEpoch,
        transitionRevision: reading.transitionRevision,
      });
  }

  async #settleStartReservation(state: AuthoritativeStartState): Promise<void> {
    if (state.kind !== 'result' || !isTerminalRunStatus(state.summary.status))
      return;
    try {
      await this.settleStartExecution(state);
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'start-reservation-settle-failed',
          runId: state.execution.runId,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  }

  async #assertResumeFence(
    workflowId: string,
    runId: string,
  ): Promise<D1RunExecutionIdentity | undefined> {
    const fence = this.#executionFence;
    if (!fence) return;
    const reading = await fence.read();
    if (reading.state === 'open' || reading.state === 'draining') return;
    if (reading.state === 'migration-locked')
      throw new ExecutionFencedError(reading.state, 'run resume');
    const state = await this.authoritativeStartState(workflowId, runId);
    if (state?.storage !== 'd1' || !admitsExistingRun(reading, state.execution))
      throw new ExecutionFencedError(reading.state, 'run resume');
    return state.execution;
  }

  /** @internal Retain this physical proof expectation across preparation waits. */
  assertExistingRunAllowed(
    workflowId: string,
    runId: string,
  ): Promise<D1RunExecutionIdentity | undefined> {
    return this.#assertResumeFence(workflowId, runId);
  }

  async #assertRetainedResume(
    source: CapturedWorkflowStorage,
    workflowId: string,
    runId: string,
    expected: RunProvenance | undefined,
    proof: D1RunExecutionIdentity | undefined,
  ): Promise<WorkflowRunState> {
    const reading = this.#executionFence
      ? await this.#executionFence.read()
      : undefined;
    const snapshot = await source.load.call(source.workflows, {
      workflowName: workflowId,
      runId,
    });
    if (!snapshot) throw new UnknownRunError(workflowId, runId);
    if (snapshot.runId !== runId)
      throw new RunStateUnreadableError(workflowId, runId);
    const current = runProvenance(snapshot);
    if (
      expected?.version === 2 &&
      (current?.version !== 2 || current.startToken !== expected.startToken)
    )
      throw new RunStateUnreadableError(workflowId, runId);
    if (
      proof &&
      (source.tablePrefix !== proof.tablePrefix ||
        current?.version !== 2 ||
        current.startToken !== proof.startToken)
    )
      throw new RunStateUnreadableError(workflowId, runId);
    if (reading) {
      const execution =
        current?.version === 2
          ? runExecutionIdentityFor(
              { tablePrefix: source.tablePrefix, workflowId, runId },
              current,
            )
          : undefined;
      if (
        !admitsExistingRun(reading, execution) ||
        (proof && (!execution || !sameExecution(proof, execution)))
      )
        throw new ExecutionFencedError(reading.state, 'run resume');
    }
    return snapshot;
  }

  async #completedStartState(
    source: CapturedWorkflowStorage,
    execution: RunExecutionIdentity,
    expected: Pick<RunProvenance, 'version' | 'startToken' | 'attemptToken'>,
  ): Promise<
    AuthoritativeStartState & { kind: 'result'; summary: RunSummary }
  > {
    const state = await this.#readStartState(
      source,
      execution.workflowId,
      execution.runId,
    );
    if (
      !state ||
      state.kind === 'legacy' ||
      !sameExecution(state.execution, execution) ||
      state.provenance.version !== expected.version ||
      state.provenance.attemptToken !== expected.attemptToken
    )
      throw new RunStateUnreadableError(execution.workflowId, execution.runId);
    if (state.kind === 'initial') throw new RunStartPendingError();
    return state;
  }

  async start(
    workflowId: string,
    sourceOptions: StartRunOptions,
  ): Promise<RunSummary> {
    const options = captureStartRunOptions(sourceOptions);
    const runId = options.runId;
    if (!isPathSafeId(runId))
      throw new InvalidRunRequestError(
        'runId is required and must be URL-path-safe',
      );
    if (
      options.attemptToken !== undefined &&
      !isPathSafeId(options.attemptToken)
    )
      throw new InvalidRunRequestError('attemptToken is malformed');
    const startIdentity =
      options.startIdentity ??
      (options.requestedBy === undefined
        ? undefined
        : normalizeStartIdentity({
            owner: { id: options.requestedBy, kind: options.requestedByKind },
            target: { kind: 'workflow', id: workflowId },
          }));
    const claim = options.startReservation;
    if (claim) {
      if (!startIdentity || !this.#startIdempotency)
        throw new InvalidRunRequestError(
          'start reservation requires configured store and identity',
        );
      if (options.idempotencyKey !== claim.key)
        throw new InvalidRunRequestError(
          'start reservation key disagrees with execution',
        );
      assertClaimIdentity(
        claim,
        { ...startIdentity, runId },
        options.idempotencyKey,
      );
    }
    let preflight = true;
    const releasePreflight = async () => {
      if (preflight && claim && this.#startIdempotency)
        await this.#startIdempotency.releaseReservation(claim);
    };
    try {
      // An agent's loop input also carries the thread history memory recalls,
      // which the caller did not send; the agent host bounds the caller's part.
      if (options.agentStart === undefined)
        assertRunInputDepth('inputData', options.inputData);
      assertRunInputDepth('initialState', options.initialState);
      for (const value of Object.values(options.storedRequestContext ?? {}))
        assertRunInputDepth('a requestContext value', value);
      const workflow = this.#getWorkflow(workflowId);
      if (
        startIdentity?.target.kind === 'workflow' &&
        startIdentity.target.id !== workflow.id
      )
        throw new InvalidRunRequestError(
          'startIdentity target does not match workflow',
        );
      const proof = await this.#assertStartFence(
        options.idempotencyKey,
        options.mutationEpoch,
      );
      return await this.#withRunLock(workflowId, runId, async () => {
        const activeKey = this.#runKey(workflowId, runId);
        const active: ActiveRun = options.legAbort
          ? { legAbort: options.legAbort }
          : {};
        if (this.#activeRuns.has(activeKey))
          throw new RunAlreadyExistsError(workflowId, runId, 'running');
        const leaveLeg = this.#registerLeg(
          activeKey,
          active,
          workflowId,
          runId,
        );
        let execution: RunExecutionIdentity | undefined;
        let engineEntered = false;
        let outcomeReadStarted = false;
        let admissionEntered = false;
        let candidate:
          | Awaited<ReturnType<AnyWorkflow['createRun']>>
          | undefined;
        const originalCached = workflow.runs.get(runId);
        let provenance: ProgressRunProvenance | undefined;
        try {
          const existing =
            await this.#getWorkflow(workflowId).getWorkflowRunById(runId);
          if (existing || originalCached)
            throw new RunAlreadyExistsError(
              workflowId,
              runId,
              existing?.status ?? 'pending',
            );
          const source = await this.#captureWorkflowStorage(workflowId);
          active.source = source;
          if (this.#withStoredRunOf(source))
            active.storedRunScope = { workflowId, runId, rowStored: false };
          provenance = {
            version: 2,
            startToken: crypto.randomUUID(),
            attemptToken: options.attemptToken ?? crypto.randomUUID(),
            resumeCounts: [],
            ...(options.requestedBy === undefined
              ? {}
              : {
                  requestedBy: options.requestedBy,
                  requestedByKind: options.requestedByKind,
                }),
            ...(startIdentity ? { startIdentity } : {}),
            ...(options.agentStart ? { agentStart: options.agentStart } : {}),
            ...(options.mutationEpoch === undefined
              ? {}
              : { mutationEpoch: options.mutationEpoch }),
          };
          execution = runExecutionIdentityFor(
            { tablePrefix: source.tablePrefix, workflowId, runId },
            provenance,
          );
          const lifecycle = lifecycleForStart(options);
          active.lifecycle = lifecycle;
          preflight = false;
          const requestContext = await this.#requestContextFor(
            workflowId,
            runId,
            { kind: 'start' },
            provenance,
            options.storedRequestContext,
            lifecycle,
          );
          active.requestContext = requestContext;
          if (options.onPreparedStartIdentity)
            await Reflect.apply(options.onPreparedStartIdentity, undefined, [
              execution,
            ]);
          const capturedExecution = execution;
          const capturedProvenance = provenance;
          await this.#inStoredRunScope(active, async () => {
            const { executionPromise } = await this.#withLifecycleLock(
              workflowId,
              runId,
              async () => {
                let run: Awaited<ReturnType<AnyWorkflow['createRun']>>;
                if (this.#executionFence) {
                  if (source.storage !== 'd1')
                    throw new Error(
                      'fenced workflow storage capability is unavailable',
                    );
                  const d1Execution =
                    normalizeD1RunExecutionIdentity(capturedExecution);
                  admissionEntered = true;
                  const admitted = await source.admit.call(
                    source.capability,
                    {
                      execution: d1Execution,
                      attemptToken: capturedProvenance.attemptToken,
                      mutationEpoch: options.mutationEpoch,
                      startIdentity,
                      requestContext: Object.fromEntries(
                        requestContext.entries(),
                      ),
                      fence: this.#executionFence,
                      reservationStore: claim
                        ? this.#startIdempotency
                        : undefined,
                      reservation: claim,
                      proof,
                      runOwnerGuard: options.runOwnerGuard,
                      onInitialWriteAttempt: () => {
                        candidate = workflow.runs.get(runId);
                      },
                    },
                    () => this.#createRun(workflowId, runId),
                  );
                  if (
                    !admitted?.witness ||
                    !sameExecution(admitted.witness.execution, d1Execution)
                  )
                    throw new RunStateUnreadableError(workflowId, runId);
                  const witnessed = this.#projectD1StartState(
                    source,
                    workflowId,
                    runId,
                    admitted.witness.row,
                  );
                  if (
                    witnessed.kind !== 'initial' ||
                    !sameExecution(witnessed.execution, d1Execution) ||
                    witnessed.provenance.initialAdmission !== true ||
                    witnessed.provenance.attemptToken !==
                      capturedProvenance.attemptToken
                  )
                    throw new RunStateUnreadableError(workflowId, runId);
                  if (active.storedRunScope)
                    active.storedRunScope.rowStored = true;
                  run = admitted.value as Awaited<
                    ReturnType<AnyWorkflow['createRun']>
                  >;
                } else {
                  if (claim && startIdentity && this.#startIdempotency)
                    await this.#startIdempotency.bindPreparedStart(claim, {
                      ...capturedExecution,
                      ...startIdentity,
                    });
                  run = await this.#createRun(workflowId, runId);
                }
                active.run = run;
                engineEntered = true;
                return {
                  executionPromise: run.start({
                    inputData: options.inputData,
                    initialState: options.initialState,
                    requestContext,
                  }),
                };
              },
            );
            const result = await executionPromise;
            await this.#reconcileTerminalState(
              workflowId,
              runId,
              result,
              requestContext,
              source,
              active.storedRunScope?.rowStored === true,
            );
          });
          outcomeReadStarted = true;
          const selected = await this.#completedStartState(
            source,
            capturedExecution,
            provenance,
          );
          await this.#settleStartReservation(selected);
          return selected.summary;
        } catch (error) {
          if (
            admissionEntered &&
            !engineEntered &&
            execution?.tablePrefix !== null &&
            execution !== undefined &&
            isDefinitiveInitialAdmissionRefusal(
              error,
              normalizeD1RunExecutionIdentity(execution),
            )
          ) {
            if (
              candidate &&
              candidate !== originalCached &&
              workflow.runs.get(runId) === candidate
            )
              workflow.runs.delete(runId);
            if (claim && this.#startIdempotency)
              await this.#startIdempotency.releaseReservation(claim);
          }
          if (engineEntered && provenance && !outcomeReadStarted) {
            await this.#failUnstorableLeg(workflowId, runId, active, error);
            const recovered = await this.#summaryForAttempt(
              workflowId,
              runId,
              provenance,
            );
            if (recovered) return recovered;
          }
          throw asClientError(error) ?? error;
        } finally {
          await leaveLeg();
        }
      });
    } catch (error) {
      await releasePreflight();
      throw error;
    }
  }

  /**
   * Reattach to a suspended run — the designed fresh-process pattern:
   * createRun({ runId }) does not clobber an existing snapshot; resume()
   * loads it and re-enters the engine at the suspended step.
   */
  async resume(
    workflowId: string,
    runId: string,
    options: ResumeRunOptions = {},
  ): Promise<RunSummary> {
    const { legAbort } = options;
    assertLegAbort(legAbort);
    assertRunInputDepth('resumeData', options.resumeData);
    const expectedSuspension = captureExpectedSuspension(
      options.expectedSuspension,
    );
    this.#getWorkflow(workflowId);
    const proof = await this.#assertResumeFence(workflowId, runId);
    return this.#withRunLock(workflowId, runId, async () => {
      const activeKey = this.#runKey(workflowId, runId);
      const active: ActiveRun = legAbort ? { legAbort } : {};
      if (this.#activeRuns.has(activeKey))
        throw new RunTerminalConflictError(workflowId, runId, 'running');
      const leaveLeg = this.#registerLeg(activeKey, active, workflowId, runId);
      let provenance: RunProvenance | undefined;
      let engineEntered = false;
      let outcomeReadStarted = false;
      try {
        const source = await this.#captureWorkflowStorage(workflowId);
        active.source = source;
        const state = await this.#workflowState(
          workflowId,
          runId,
          true,
          RESUME_STATE_FIELDS,
        );
        if (!state) throw new UnknownRunError(workflowId, runId);
        if (state.isFromInMemory)
          throw new RunStateUnreadableError(workflowId, runId);
        if (this.#withStoredRunOf(source))
          active.storedRunScope = { workflowId, runId, rowStored: true };
        const prior = runProvenance(state);
        if (
          proof &&
          (prior?.version !== 2 ||
            prior.startToken !== proof.startToken ||
            source.tablePrefix !== proof.tablePrefix)
        )
          throw new RunStateUnreadableError(workflowId, runId);
        if (state.status !== 'suspended')
          throw new RunNotSuspendedError(workflowId, runId, state.status);
        const prepared = await this.#trustedResumePreparation(
          workflowId,
          runId,
          state,
          { ...options, expectedSuspension },
        );
        provenance = prepared.provenance;
        const { requestContext, lifecycle, nextCounts } = prepared;
        active.requestContext = requestContext;
        active.lifecycle = lifecycle;
        const check = async () => {
          const current = await this.#assertRetainedResume(
            source,
            workflowId,
            runId,
            prior,
            proof,
          );
          const currentLifecycle = lifecycleFromRequestContext(
            current.requestContext,
          );
          if (
            currentLifecycle?.terminal ||
            currentLifecycle?.transitionIntent ||
            this.#activeRuns.get(activeKey) !== active
          )
            throw new RunTerminalConflictError(
              workflowId,
              runId,
              current.status as RunStatus,
            );
          active.lifecycle = effectiveLifecycle(
            currentLifecycle,
            active.lifecycle,
          );
        };
        await this.#withLifecycleLock(workflowId, runId, check);
        if (options.prepareExecution) {
          const values = structuredClone(
            Object.fromEntries(requestContext.entries()),
          );
          await options.prepareExecution(
            new RequestContext(Object.entries(values)),
            state.payload,
          );
          await this.#withLifecycleLock(workflowId, runId, check);
        }
        const result = await this.#inStoredRunScope(active, async () => {
          const run = await this.#createRun(workflowId, runId);
          const { executionPromise } = await this.#withLifecycleLock(
            workflowId,
            runId,
            async () => {
              await check();
              active.run = run;
              engineEntered = true;
              return {
                executionPromise: run.resume({
                  step: options.step,
                  resumeData: options.resumeData,
                  requestContext,
                }),
              };
            },
          );
          const resumed = await executionPromise;
          await this.#reconcileTerminalState(
            workflowId,
            runId,
            resumed,
            requestContext,
            source,
            active.storedRunScope?.rowStored === true,
            proof,
          );
          return resumed;
        });
        if (provenance.version === 2) {
          const execution = runExecutionIdentityFor(
            { tablePrefix: source.tablePrefix, workflowId, runId },
            provenance,
          );
          outcomeReadStarted = true;
          const selected = await this.#completedStartState(
            source,
            execution,
            provenance,
          );
          await this.#settleStartReservation(selected);
          return selected.summary;
        }
        return summarize(
          runId,
          result,
          nextCounts,
          provenance.requestedBy,
          provenance.requestedByKind,
          lifecycle?.deadlineAt,
        );
      } catch (error) {
        if (engineEntered && provenance && !outcomeReadStarted) {
          await this.#failUnstorableLeg(workflowId, runId, active, error);
          const recovered = await this.#summaryForAttempt(
            workflowId,
            runId,
            provenance,
          );
          if (recovered) return recovered;
        }
        throw asClientError(error) ?? error;
      } finally {
        await leaveLeg();
      }
    });
  }

  /** Idempotently cancel a live run from its persisted snapshot. */
  terminate(
    workflowId: string,
    runId: string,
    now = Date.now(),
  ): Promise<RunLifecycleTransitionResult> {
    return this.#transitionTerminal(workflowId, runId, 'cancelled', now);
  }

  /**
   * Interrupt the in-isolate Mastra execution before waiting on the run lock.
   * A deadline CAS is checked from persisted state first, so a stale sweep can
   * never cancel a live run whose deadline was extended.
   */
  async cancelActiveExecution(
    workflowId: string,
    runId: string,
    intendedStatus: RunTerminalStatus,
    replayPrincipals: readonly RunLifecyclePrincipal[],
    cas?: RunLifecycleCas,
    now = Date.now(),
  ): Promise<boolean> {
    this.#getWorkflow(workflowId);
    const prepared = await this.#withLifecycleLock(workflowId, runId, () =>
      this.#retryOnRowChange(workflowId, runId, async () => {
        const source =
          this.#activeRuns.get(this.#runKey(workflowId, runId))?.source ??
          (await this.#captureWorkflowStorage(workflowId));
        const { state, row } = await this.#readForTransition(
          source,
          workflowId,
          runId,
        );
        const key = this.#runKey(workflowId, runId);
        const active = this.#activeRuns.get(key);
        const cancelTarget = () => {
          if (!active?.run) return undefined;
          this.#terminalAbortIntents.set(key, intendedStatus);
          return { run: active.run, legAbort: active.legAbort, key };
        };
        const lifecycle = effectiveLifecycle(
          lifecycleFromRequestContext(state.requestContext),
          active?.lifecycle,
        );
        if (lifecycle?.terminal) return undefined;
        if (hasDisputedSettlement(lifecycle)) {
          throw new RunLifecycleBlockedError({
            code: 'DISPUTED_SETTLEMENT',
            message:
              'run termination is blocked while an economic operation is disputed',
          });
        }
        const existingIntent = lifecycle?.transitionIntent;
        if (
          existingIntent?.status === intendedStatus &&
          (cas === undefined ||
            (existingIntent.expectedRevision === cas.expectedRevision &&
              existingIntent.expectedDeadlineAt === cas.expectedDeadlineAt))
        ) {
          return cancelTarget();
        }
        if (
          cas &&
          (lifecycle?.revision !== cas.expectedRevision ||
            lifecycle.deadlineAt !== cas.expectedDeadlineAt ||
            lifecycle.deadlineAt === undefined ||
            lifecycle.deadlineAt > now)
        ) {
          return undefined;
        }
        if (!TERMINABLE_RUN_STATUSES.has(state.status as RunStatus)) {
          throw new RunTerminalConflictError(
            workflowId,
            runId,
            state.status as RunStatus,
          );
        }
        if (
          cas &&
          active &&
          (active.lifecycle?.revision !== cas.expectedRevision ||
            active.lifecycle.deadlineAt !== cas.expectedDeadlineAt)
        ) {
          return undefined;
        }
        const principals = canonicalReplayPrincipals(replayPrincipals);
        const intent = advanceLifecycle(lifecycle, {
          transitionIntent: {
            status: intendedStatus,
            requestedAt: now,
            replayPrincipals: principals,
            ...(cas
              ? {
                  expectedRevision: cas.expectedRevision,
                  expectedDeadlineAt: cas.expectedDeadlineAt,
                }
              : {}),
          },
        });
        await this.#persistIntent(
          workflowId,
          runId,
          state,
          intent,
          now,
          source,
          row,
        );
        if (active) {
          active.lifecycle = intent;
          active.requestContext?.set(RUN_LIFECYCLE_CONTEXT_KEY, intent);
        }
        return cancelTarget();
      }),
    );
    if (!prepared) return false;
    try {
      await prepared.run.cancel();
    } catch (error) {
      this.#terminalAbortIntents.delete(prepared.key);
      throw error;
    }
    // After `run.cancel()` has aborted the engine run, so the engine's
    // cancellation precedes any abort error from a call in flight.
    prepared.legAbort?.abort(terminalLegAbortReason(intendedStatus));
    return true;
  }

  /** Trusted host variant that persists the exact identities allowed to retry. */
  terminateAsPrincipal(
    workflowId: string,
    runId: string,
    principal: RunLifecyclePrincipal,
    owner: RunLifecyclePrincipal,
    now = Date.now(),
  ): Promise<RunLifecycleTransitionResult> {
    return this.#transitionTerminal(
      workflowId,
      runId,
      'cancelled',
      now,
      undefined,
      [principal, owner],
      principal,
    );
  }

  /** CAS-guarded deadline transition driven through the owner Durable Object. */
  timeOut(
    workflowId: string,
    runId: string,
    cas: RunLifecycleCas,
    now = Date.now(),
  ): Promise<RunLifecycleTransitionResult> {
    if (
      !Number.isSafeInteger(cas.expectedRevision) ||
      cas.expectedRevision < 1 ||
      (cas.expectedDeadlineAt !== undefined &&
        (!Number.isSafeInteger(cas.expectedDeadlineAt) ||
          cas.expectedDeadlineAt < 0)) ||
      !Number.isSafeInteger(now) ||
      now < 0
    ) {
      throw new InvalidRunRequestError('deadline transition CAS is malformed');
    }
    return this.#transitionTerminal(workflowId, runId, 'timed_out', now, cas);
  }

  /** Trusted host deadline transition with retry principals persisted atomically. */
  timeOutAsPrincipal(
    workflowId: string,
    runId: string,
    cas: RunLifecycleCas,
    principal: RunLifecyclePrincipal,
    owner: RunLifecyclePrincipal,
    now = Date.now(),
  ): Promise<RunLifecycleTransitionResult> {
    return this.#transitionTerminal(
      workflowId,
      runId,
      'timed_out',
      now,
      cas,
      [principal, owner],
      principal,
    );
  }

  async #transitionTerminal(
    workflowId: string,
    runId: string,
    status: RunTerminalStatus,
    now: number,
    cas?: RunLifecycleCas,
    replayPrincipals?: readonly RunLifecyclePrincipal[],
    replayingPrincipal?: RunLifecyclePrincipal,
  ): Promise<RunLifecycleTransitionResult> {
    this.#getWorkflow(workflowId);
    return this.#withRunLock(workflowId, runId, async () => {
      const source = await this.#captureWorkflowStorage(workflowId);
      return this.#retryOnRowChange(workflowId, runId, async () =>
        this.#transitionTerminalLocked(
          workflowId,
          runId,
          status,
          now,
          { cas, replayPrincipals, replayingPrincipal },
          source,
          await this.#readForTransition(source, workflowId, runId),
        ),
      );
    });
  }

  /**
   * Decide a terminal transition from one read of the run's row and write it,
   * for a caller holding the run lock. Where the read carries the row, the
   * write is a compare-and-set against it and a miss throws RowChangedError.
   */
  async #transitionTerminalLocked(
    workflowId: string,
    runId: string,
    status: RunTerminalStatus,
    now: number,
    { cas, replayPrincipals, replayingPrincipal }: TerminalTransitionOptions,
    source: CapturedWorkflowStorage,
    { state, row }: TransitionRead,
  ): Promise<RunLifecycleTransitionResult> {
    const lifecycle = lifecycleFromRequestContext(state.requestContext);
    if (lifecycle?.terminal) {
      if (
        replayingPrincipal &&
        !lifecycle.terminal.replayPrincipals.some(
          (principal) =>
            principal.kind === replayingPrincipal.kind &&
            principal.id === replayingPrincipal.id,
        )
      ) {
        throw new UnknownRunError(workflowId, runId);
      }
      return {
        summary: await this.#summaryAfterPersist(
          workflowId,
          runId,
          source,
          runProvenance(state),
        ),
        transitioned: false,
        casMatched:
          cas === undefined ||
          (lifecycle.terminal.status === 'timed_out' &&
            lifecycle.deadlineAt === cas.expectedDeadlineAt),
        cleanup: terminalCleanupFor(lifecycle) as RunTerminalCleanup,
      };
    }
    const transitionIntent = lifecycle?.transitionIntent;
    const intentMatchesTransition =
      transitionIntent?.status === status &&
      (cas === undefined ||
        (transitionIntent.expectedRevision === cas.expectedRevision &&
          transitionIntent.expectedDeadlineAt === cas.expectedDeadlineAt));
    const intentMatchesCas = cas !== undefined && intentMatchesTransition;
    if (
      cas !== undefined &&
      !intentMatchesCas &&
      (lifecycle?.revision !== cas.expectedRevision ||
        lifecycle.deadlineAt !== cas.expectedDeadlineAt ||
        lifecycle.deadlineAt === undefined ||
        lifecycle.deadlineAt > now)
    ) {
      return {
        summary: await this.#summaryAfterPersist(
          workflowId,
          runId,
          source,
          runProvenance(state),
        ),
        transitioned: false,
        casMatched: false,
        cleanup: {
          revision: lifecycle?.revision ?? 0,
          status,
          cleanupCompleted: false,
          ...(lifecycle?.scheduleDispatch
            ? { scheduleDispatch: lifecycle.scheduleDispatch }
            : {}),
        },
      };
    }
    if (hasDisputedSettlement(lifecycle)) {
      throw new RunLifecycleBlockedError({
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      });
    }
    const currentStatus = state.status as RunStatus;
    const abortIntent = this.#terminalAbortIntents.get(
      this.#runKey(workflowId, runId),
    );
    if (
      !TERMINABLE_RUN_STATUSES.has(currentStatus) &&
      !intentMatchesTransition &&
      !(currentStatus === 'canceled' && abortIntent === status)
    ) {
      throw new RunTerminalConflictError(workflowId, runId, currentStatus);
    }
    const provenance = runProvenance(state);
    const fallbackPrincipal: RunLifecyclePrincipal =
      provenance?.requestedBy && provenance.requestedByKind
        ? {
            kind: provenance.requestedByKind,
            id: provenance.requestedBy,
          }
        : { kind: 'system', id: 'flowsafe-system' };
    const principals = canonicalReplayPrincipals(
      replayPrincipals ??
        transitionIntent?.replayPrincipals ?? [fallbackPrincipal],
    );
    const next = projectTerminalLifecycle(lifecycle, status, now, principals);
    await this.#persistLifecycle(
      workflowId,
      runId,
      {
        ...state,
        ...terminalStateFields(status),
        error: runTerminalError(status),
        timestamp: now,
      },
      next,
      now,
      source,
      row,
    );
    this.#terminalAbortIntents.delete(this.#runKey(workflowId, runId));
    return {
      summary: await this.#summaryAfterPersist(
        workflowId,
        runId,
        source,
        runProvenance(state),
      ),
      transitioned: true,
      casMatched: true,
      cleanup: terminalCleanupFor(next) as RunTerminalCleanup,
    };
  }

  /** Marks idempotent terminal side-effect cleanup after every hook succeeds. */
  async completeTerminalCleanup(
    workflowId: string,
    runId: string,
    expectedRevision: number,
    now = Date.now(),
  ): Promise<RunSummary> {
    this.#getWorkflow(workflowId);
    return this.#withRunLock(workflowId, runId, async () => {
      const source = await this.#captureWorkflowStorage(workflowId);
      const state = await source.load.call(source.workflows, {
        workflowName: workflowId,
        runId,
      });
      if (!state) throw new UnknownRunError(workflowId, runId);
      if (state.runId !== runId)
        throw new RunStateUnreadableError(workflowId, runId);
      const lifecycle = lifecycleFromRequestContext(state.requestContext);
      if (!lifecycle?.terminal) {
        throw new RunTerminalConflictError(
          workflowId,
          runId,
          state.status as RunStatus,
        );
      }
      if (lifecycle.terminal.cleanupCompletedAt !== undefined) {
        return this.#summaryAfterPersist(
          workflowId,
          runId,
          source,
          runProvenance(state),
        );
      }
      if (lifecycle.revision !== expectedRevision) {
        throw new Error('run terminal cleanup CAS no longer matches');
      }
      const next = advanceLifecycle(lifecycle, {
        terminal: { ...lifecycle.terminal, cleanupCompletedAt: now },
      });
      await this.#persistLifecycle(workflowId, runId, state, next, now, source);
      return this.#summaryAfterPersist(
        workflowId,
        runId,
        source,
        runProvenance(state),
      );
    });
  }

  /**
   * The PROJECTION read: the best answer available about a run, which is what
   * an HTTP status route, a broadcast frame or an existence check wants.
   *
   * It can answer from Mastra's in-memory fallback, so a caller about to
   * conclude something IRREVERSIBLE from what it reads — deleting wake state,
   * spending an abandonment budget, resuming a run, deleting a row — must use
   * {@link RunnerRuntime.authoritativeStatus} instead, which refuses a read
   * that did not reach storage.
   */
  async status(workflowId: string, runId: string): Promise<RunSummary | null> {
    const state = await this.#workflowState(workflowId, runId);
    if (!state) return null;
    return this.#summaryFromState(runId, state);
  }

  /**
   * status() for a caller about to CONCLUDE something irreversible from what it
   * reads — delete a wake record, spend an abandonment budget, resume a run.
   *
   * Workflow.getInMemoryRunAsWorkflowState reports the retained Run's lifecycle
   * status with isFromInMemory: true, without suspended paths or requestContext.
   * Refusing that marker prevents an approximation from authorizing an
   * irreversible action; after non-suspended completion the Run leaves the
   * in-memory map, so the fallback is null.
   *
   * A valid v2 pending row is refused before lifecycle projection; it has no
   * durable execution outcome yet.
   */
  async authoritativeStatus(
    workflowId: string,
    runId: string,
  ): Promise<RunSummary | null> {
    const state = await this.#workflowState(workflowId, runId);
    if (!state) return null;
    if (state.isFromInMemory === true) {
      throw new RunStateUnreadableError(workflowId, runId);
    }
    return this.#summaryFromState(runId, state);
  }

  /** @internal Include ordinary v1 and unversioned data from the selected row. */
  authoritativeStartState(
    workflowId: string,
    runId: string,
    options: { readonly includeLegacy: true },
  ): Promise<AuthoritativeStartState | LegacyRunState | null>;
  /** @internal Read only modern generation and root-local result data. */
  authoritativeStartState(
    workflowId: string,
    runId: string,
  ): Promise<AuthoritativeStartState | null>;
  async authoritativeStartState(
    workflowId: string,
    runId: string,
    options?: { readonly includeLegacy: true },
  ): Promise<AuthoritativeStartState | LegacyRunState | null> {
    const includeLegacy = options?.includeLegacy === true;
    const { state } = await this.#observeStartState(workflowId, runId);
    if (state?.kind === 'legacy' && !includeLegacy)
      throw new RunStateUnreadableError(workflowId, runId);
    return state;
  }

  async #captureWorkflowStorage(
    workflowId: string,
  ): Promise<CapturedWorkflowStorage> {
    this.#getWorkflow(workflowId);
    const workflows = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflows) throw new Error('workflow storage is unavailable');
    const methods = {
      workflows,
      read: workflows.getWorkflowRunById,
      load: workflows.loadWorkflowSnapshot,
      persist: workflows.persistWorkflowSnapshot,
    };
    const capability = (
      workflows as WorkflowsStorage & {
        [FENCED_WORKFLOW_STORAGE]?: FencedWorkflowAdmissionCapability;
      }
    )[FENCED_WORKFLOW_STORAGE];
    if (capability === undefined) {
      if (this.#executionFence)
        throw new Error('fenced workflow storage capability is unavailable');
      return { ...methods, storage: 'unfenced', tablePrefix: null };
    }
    if (capability === null || typeof capability !== 'object')
      throw new Error('workflow capability is malformed');
    const {
      database,
      tablePrefix: prefix,
      readSnapshot,
      withInitialAdmission: admit,
      terminalizeInitialAdmission: terminalize,
      touchRun: touch,
      replaceSnapshot: replace,
      patchRunLifecycle: patch,
      withStoredRun: enterStoredRun,
    } = capability;
    if (
      typeof prefix !== 'string' ||
      typeof readSnapshot !== 'function' ||
      typeof admit !== 'function' ||
      typeof terminalize !== 'function' ||
      (touch !== undefined && typeof touch !== 'function') ||
      (replace !== undefined && typeof replace !== 'function') ||
      (patch !== undefined && typeof patch !== 'function') ||
      (enterStoredRun !== undefined && typeof enterStoredRun !== 'function') ||
      !database ||
      typeof database.prepare !== 'function' ||
      typeof database.batch !== 'function'
    )
      throw new Error('workflow capability is malformed');
    validateTablePrefix(prefix);
    if (
      (this.#executionFence && !this.#executionFence.usesDatabase(database)) ||
      (this.#startIdempotency && !this.#startIdempotency.usesDatabase(database))
    )
      throw new Error('workflow storage binding disagrees with runtime stores');
    return {
      ...methods,
      storage: 'd1',
      tablePrefix: prefix.toLowerCase(),
      capability,
      database,
      readSnapshot,
      admit,
      terminalize,
      ...(touch === undefined ? {} : { touch }),
      ...(replace === undefined ? {} : { replace }),
      ...(patch === undefined ? {} : { patch }),
      ...(enterStoredRun === undefined ? {} : { enterStoredRun }),
    };
  }

  async #observeStartState(
    workflowId: string,
    runId: string,
  ): Promise<{
    source: CapturedWorkflowStorage;
    state: AuthoritativeStartState | LegacyRunState | null;
  }> {
    if (!isPathSafeId(workflowId))
      throw new InvalidRunRequestError('workflowId is malformed');
    if (!isPathSafeId(runId))
      throw new InvalidRunRequestError('runId is malformed');
    this.#getWorkflow(workflowId);
    try {
      const source = await this.#captureWorkflowStorage(workflowId);
      return {
        source,
        state: await this.#readStartState(source, workflowId, runId),
      };
    } catch (cause) {
      if (cause instanceof RunStateUnreadableError) throw cause;
      throw new RunStateUnreadableError(workflowId, runId, { cause });
    }
  }

  #projectD1StartState(
    source: Extract<CapturedWorkflowStorage, { storage: 'd1' }>,
    workflowId: string,
    runId: string,
    observed: RawWorkflowSnapshot,
  ): AuthoritativeStartState | LegacyRunState {
    for (const key of [
      'tablePrefix',
      'workflowId',
      'runId',
      'resourceId',
      'snapshot',
      'createdAt',
      'updatedAt',
    ]) {
      if (!Object.hasOwn(observed, key))
        throw new Error('workflow snapshot field is missing');
    }
    const {
      tablePrefix,
      workflowId: storedWorkflow,
      runId: storedRun,
      resourceId,
      snapshot,
      createdAt,
      updatedAt,
    } = observed;
    if (
      tablePrefix !== source.tablePrefix ||
      storedWorkflow !== workflowId ||
      storedRun !== runId ||
      (resourceId !== null && typeof resourceId !== 'string') ||
      typeof snapshot !== 'string' ||
      typeof createdAt !== 'string' ||
      typeof updatedAt !== 'string'
    )
      throw new Error('workflow snapshot fields are malformed');
    const raw = Object.freeze({
      tablePrefix,
      workflowId,
      runId,
      resourceId,
      snapshot,
      createdAt,
      updatedAt,
    });
    return this.#projectStartState(
      { storage: 'd1', tablePrefix, raw },
      workflowId,
      runId,
      JSON.parse(snapshot),
      createdAt,
      updatedAt,
    );
  }

  async #readStartState(
    source: CapturedWorkflowStorage,
    workflowId: string,
    runId: string,
  ): Promise<AuthoritativeStartState | LegacyRunState | null> {
    try {
      let state: AuthoritativeStartState | LegacyRunState | null;
      if (source.storage === 'd1') {
        const row = await source.readSnapshot.call(source.capability, {
          workflowId,
          runId,
        });
        state =
          row === undefined
            ? null
            : this.#projectD1StartState(source, workflowId, runId, row);
      } else {
        const row = await source.read.call(source.workflows, {
          workflowName: workflowId,
          runId,
        });
        if (row === null) return null;
        const {
          workflowName,
          runId: storedRun,
          snapshot,
          createdAt,
          updatedAt,
        } = row;
        if (workflowName !== workflowId || storedRun !== runId)
          throw new Error(
            'workflow snapshot selector disagrees with the request',
          );
        state = this.#projectStartState(
          source,
          workflowId,
          runId,
          typeof snapshot === 'string'
            ? JSON.parse(snapshot)
            : structuredClone(snapshot),
          createdAt,
          updatedAt,
        );
      }
      return state;
    } catch (cause) {
      throw new RunStateUnreadableError(workflowId, runId, { cause });
    }
  }

  #projectStartState(
    physical:
      | { storage: 'd1'; tablePrefix: string; raw: RawWorkflowSnapshot }
      | { storage: 'unfenced'; tablePrefix: null },
    workflowId: string,
    runId: string,
    decoded: unknown,
    createdAt: Date | string,
    updatedAt: Date | string,
  ): AuthoritativeStartState | LegacyRunState {
    if (
      decoded === null ||
      typeof decoded !== 'object' ||
      Array.isArray(decoded)
    )
      throw new Error('workflow snapshot is malformed');
    const snapshot = decoded as WorkflowRunState;
    if (snapshot.runId !== runId || !isRunStatus(snapshot.status))
      throw new Error('workflow snapshot identity or status is malformed');
    for (const value of [
      snapshot.requestContext,
      snapshot.context,
      snapshot.suspendedPaths,
    ]) {
      if (
        value !== undefined &&
        (value === null || typeof value !== 'object' || Array.isArray(value))
      )
        throw new Error('workflow snapshot container is malformed');
    }
    const provenance = runProvenance(snapshot);
    lifecycleFromRequestContext(snapshot.requestContext);
    for (const value of [createdAt, updatedAt]) {
      if (!(value instanceof Date) && typeof value !== 'string')
        throw new Error('workflow snapshot timestamp is malformed');
      if (!Number.isFinite(new Date(value).getTime()))
        throw new Error('workflow snapshot timestamp is malformed');
    }
    if (provenance?.version !== 2)
      return {
        kind: 'legacy',
        provenanceVersion: provenance?.version,
        address: Object.freeze({
          tablePrefix: physical.tablePrefix,
          workflowId,
          runId,
        }),
        snapshot,
        summary: summaryFromSelectedSnapshot(
          runId,
          snapshot,
          { createdAt: toIso(createdAt), updatedAt: toIso(updatedAt) },
          provenance,
        ),
      };
    const execution = runExecutionIdentityFor(
      { tablePrefix: physical.tablePrefix, workflowId, runId },
      provenance,
    );
    const state =
      snapshot.status === 'pending'
        ? { kind: 'initial' as const, snapshot, provenance }
        : {
            kind: 'result' as const,
            snapshot,
            provenance,
            summary: summaryFromSelectedSnapshot(
              runId,
              snapshot,
              { createdAt: toIso(createdAt), updatedAt: toIso(updatedAt) },
              provenance,
            ),
          };
    return physical.storage === 'd1'
      ? {
          ...state,
          storage: 'd1',
          execution: Object.freeze({
            ...execution,
            tablePrefix: physical.tablePrefix,
          }),
          raw: physical.raw,
        }
      : {
          ...state,
          storage: 'unfenced',
          execution: Object.freeze({ ...execution, tablePrefix: null }),
        };
  }

  /** @internal Recover only after the exact owning execution has unwound. */
  async recoverStartAttempt(
    value: D1RunExecutionIdentity,
    recovery: {
      attemptToken: string;
      isOwnerQuiescent: () => boolean | Promise<boolean>;
      startReservation?: StartReservationReading;
      expectedTarget?: RecoveryTargetExpectation;
      /**
       * What the caller knows of the start leg. `unwound`: it ended in this
       * isolate. `touched`: it marks the row live while it runs (RUN_LEG_TOUCH_MS),
       * so an initial row is repaired only once silent for RUN_LEG_SILENT_MS.
       * Both stamp the repaired row so the leg's later writes are refused,
       * except `touched` on storage without the touch, which has no silence to
       * wait for.
       */
      startLeg: 'unwound' | 'touched';
    },
  ): Promise<RecoveredStart | null> {
    const execution = normalizeD1RunExecutionIdentity(value);
    const {
      attemptToken,
      isOwnerQuiescent,
      startReservation: suppliedClaim,
      expectedTarget: suppliedTarget,
      startLeg,
    } = recovery;
    let expectedTarget: RecoveryTargetExpectation | undefined;
    if (suppliedTarget !== undefined) {
      if (
        suppliedTarget === null ||
        typeof suppliedTarget !== 'object' ||
        Array.isArray(suppliedTarget)
      )
        throw new InvalidRunRequestError('start recovery target is malformed');
      const { kind } = suppliedTarget;
      if (kind === 'workflow') expectedTarget = Object.freeze({ kind });
      else if (kind === 'agent') {
        const { owner, id, threadId, threaded } = suppliedTarget;
        const identity = normalizeStartIdentity({
          owner,
          target: { kind, id, threadId },
        });
        if (typeof threaded !== 'boolean')
          throw new InvalidRunRequestError(
            'start recovery target is malformed',
          );
        expectedTarget = Object.freeze({
          kind,
          id: identity.target.id,
          threadId,
          owner: identity.owner,
          threaded,
        });
      } else
        throw new InvalidRunRequestError('start recovery target is malformed');
    }
    const assertExpectedTarget = (state: AuthoritativeStartState): void => {
      if (!expectedTarget) return;
      const { startIdentity, agentStart } = state.provenance;
      if (expectedTarget.kind === 'workflow') {
        if (
          startIdentity?.target.kind !== 'workflow' ||
          startIdentity.target.id !== state.execution.workflowId ||
          agentStart !== undefined
        )
          throw new Error('recovery workflow target disagrees with execution');
      } else if (
        startIdentity?.target.kind !== 'agent' ||
        startIdentity.target.id !== expectedTarget.id ||
        startIdentity.target.threadId !== expectedTarget.threadId ||
        startIdentity.owner.kind !== expectedTarget.owner.kind ||
        startIdentity.owner.id !== expectedTarget.owner.id ||
        agentStart?.threaded !== expectedTarget.threaded
      )
        throw new Error('recovery agent target disagrees with execution');
    };
    const claim =
      suppliedClaim === undefined
        ? undefined
        : captureReservation(suppliedClaim, 'started');
    if (
      !isPathSafeId(attemptToken) ||
      typeof isOwnerQuiescent !== 'function' ||
      (startLeg !== 'unwound' && startLeg !== 'touched')
    )
      throw new InvalidRunRequestError('start recovery authority is malformed');
    if (claim && !this.#startIdempotency)
      throw new ExecutionFenceUnreadableError(
        'run start recovery is unresolved',
      );
    const { workflowId, runId } = execution;
    this.#getWorkflow(workflowId);
    return this.#withRunLock(workflowId, runId, () =>
      this.#withLifecycleLock(workflowId, runId, async () => {
        try {
          if (this.isRunActive(workflowId, runId))
            throw new Error('run owner is not quiescent');
          const source = await this.#captureWorkflowStorage(workflowId);
          if (
            this.isRunActive(workflowId, runId) ||
            (await Reflect.apply(isOwnerQuiescent, undefined, [])) !== true ||
            this.isRunActive(workflowId, runId)
          )
            throw new Error('run owner is not quiescent');
          const state = await this.#readStartState(source, workflowId, runId);
          if (state?.kind === 'legacy')
            throw new RunStateUnreadableError(workflowId, runId);
          if (
            source.storage !== 'd1' ||
            source.tablePrefix !== execution.tablePrefix
          )
            throw new Error('recovery source disagrees with execution');
          if (!state) return null;
          if (!sameExecution(state.execution, execution))
            throw new Error('recovery generation disagrees with execution');
          assertExpectedTarget(state);
          if (claim) {
            if (!state.provenance.startIdentity)
              throw new Error('recovery lacks logical identity');
            assertClaimIdentity(claim, {
              ...state.provenance.startIdentity,
              runId,
            });
          }
          let selected = state;
          let transitioned = false;
          let cleanup = terminalCleanupFor(
            lifecycleFromRequestContext(state.snapshot.requestContext),
          );
          if (state.kind === 'initial') {
            if (
              !this.#executionFence ||
              state.storage !== 'd1' ||
              state.provenance.initialAdmission !== true ||
              state.provenance.attemptToken !== attemptToken ||
              state.provenance.resumeCounts.length !== 0
            )
              throw new RunStartPendingError();
            const nowMs = Date.now();
            // A source without the touch keeps no silence evidence: its start
            // leg may still run, so it is repaired at once and unstamped.
            const waitsForSilence =
              startLeg === 'touched' && source.touch !== undefined;
            if (waitsForSilence && !rowSilent(state.raw, nowMs))
              throw new RunStartPendingError();
            const legStopped = startLeg === 'unwound' || waitsForSilence;
            const result = await source.terminalize.call(source.capability, {
              expected: state.raw,
              execution,
              attemptToken,
              nowMs,
              ...(legStopped ? { markOutcomeUnknown: true as const } : {}),
            });
            if (result.kind === 'conflict')
              throw new Error('initial terminalization conflicts');
            const projected = this.#projectD1StartState(
              source,
              workflowId,
              runId,
              result.row,
            );
            if (
              projected.kind !== 'result' ||
              !sameExecution(projected.execution, execution)
            )
              throw new Error('terminalization result is unresolved');
            selected = projected;
            assertExpectedTarget(selected);
            transitioned = result.kind === 'terminalized';
            cleanup = result.cleanup;
          }
          if (selected.kind !== 'result') throw new RunStartPendingError();
          if (isTerminalRunStatus(selected.summary.status))
            await this.settleStartExecution(selected, claim);
          return cleanup
            ? {
                kind: 'lifecycle',
                transition: {
                  summary: selected.summary,
                  transitioned,
                  casMatched: true,
                  cleanup,
                },
              }
            : { kind: 'ordinary', summary: selected.summary };
        } catch (cause) {
          if (
            cause instanceof RunStartPendingError ||
            cause instanceof RunStateUnreadableError
          )
            throw cause;
          throw new ExecutionFenceUnreadableError(
            'run start recovery is unresolved',
            { cause },
          );
        }
      }),
    );
  }

  /**
   * @internal Mark this run's row live for an executing leg, from whichever
   * instance runs it; the evidence #silentRow reads. When the row reads
   * settled, or, on storage with `withStoredRun`, absent after the leg stored
   * it, the leg this runtime drives for the run is aborted: its engine starts
   * no further step, and its step in flight sees its `abortSignal` aborted. A
   * storage source without the touch leaves nothing to read, so its runs are
   * never settled here.
   */
  async touchRun(
    workflowId: string,
    runId: string,
    now = Date.now(),
  ): Promise<void> {
    const source = await this.#captureWorkflowStorage(workflowId);
    if (source.storage !== 'd1' || !source.touch) return;
    const touched = await source.touch.call(
      source.capability,
      { workflowId, runId },
      now,
    );
    const active = this.#activeRuns.get(this.#runKey(workflowId, runId));
    if (
      touched === 'settled' ||
      (touched === 'absent' && active?.storedRunScope?.rowStored === true)
    )
      this.#abortLeg(
        active,
        new RunSettledConflictError(workflowId, runId),
        'run-leg-settled-abort',
        workflowId,
        runId,
      );
  }

  /**
   * A leg's registration and its touch share one span, because recovery reads a
   * row nobody touches as abandoned. The release waits for a touch in flight:
   * its late write would make a following repair's exact-row compare-and-set
   * miss.
   */
  #registerLeg(
    activeKey: string,
    active: ActiveRun,
    workflowId: string,
    runId: string,
  ): () => Promise<void> {
    this.#activeRuns.set(activeKey, active);
    const touches = new Set<Promise<void>>();
    const timer = setInterval(() => {
      const touch: Promise<void> = this.touchRun(workflowId, runId)
        .catch((error: unknown) =>
          console.error('run leg liveness touch failed', error),
        )
        .finally(() => touches.delete(touch));
      touches.add(touch);
    }, RUN_LEG_TOUCH_MS);
    return async () => {
      clearInterval(timer);
      await Promise.all(touches);
      if (this.#activeRuns.get(activeKey) === active)
        this.#activeRuns.delete(activeKey);
    };
  }

  /**
   * Abort the engine and the `legAbort` of a leg this runtime drives, with
   * `cause` as the cause of one shared reason, so a model or tool call in
   * flight stops with the engine. Never `run.cancel()`: it reads the stored
   * status first and returns without aborting on `failed`, which an
   * interruption or a start repair writes.
   */
  #abortLeg(
    active: ActiveRun | undefined,
    cause: Error,
    logType: string,
    workflowId: string,
    runId: string,
  ): void {
    const unaborted = [active?.run?.abortController, active?.legAbort].filter(
      (controller): controller is AbortController =>
        controller !== undefined && !controller.signal.aborted,
    );
    if (unaborted.length === 0) return;
    console.error(JSON.stringify({ type: logType, workflowId, runId }));
    const reason = legAbortReason(cause);
    for (const controller of unaborted) controller.abort(reason);
  }

  #withStoredRunOf(
    source: CapturedWorkflowStorage | undefined,
  ): FencedWorkflowAdmissionCapability['withStoredRun'] {
    return source?.storage === 'd1'
      ? source.enterStoredRun?.bind(source.capability)
      : undefined;
  }

  /** Run a leg's writes of its own run inside the storage's stored-run scope. */
  #inStoredRunScope<T>(
    active: ActiveRun,
    operation: () => Promise<T>,
  ): Promise<T> {
    const withStoredRun = this.#withStoredRunOf(active.source);
    const { storedRunScope } = active;
    return withStoredRun && storedRunScope
      ? withStoredRun(storedRunScope, operation)
      : operation();
  }

  /**
   * @internal Settle a run whose execution leg ended without a durable outcome.
   *
   * The caller is the run's own Durable Object after it has established that no
   * leg of its own drives the run; this runtime's `#activeRuns` covers the
   * in-process drivers. An object can be replaced while its leg keeps running
   * on the outgoing instance, so a run is written only after its row has gone
   * untouched for RUN_LEG_SILENT_MS (`live` otherwise).
   *
   * `durable` (suspended, or terminal with its cleanup done) and `absent` leave
   * the snapshot untouched. A recorded cancellation or timeout owns the
   * outcome: `transition` completes it, or replays a terminal one whose cleanup
   * is unfinished, and leaves the cleanup to the caller as the terminate and
   * deadline routes do. The pending initial row stays with start recovery,
   * which repairs it as an unknown outcome.
   *
   * Any other run becomes `failed` (`interrupted`), never re-executed: the step
   * that was in flight may already have had effects.
   */
  async settleInterruptedRun(
    workflowId: string,
    runId: string,
    now = Date.now(),
  ): Promise<InterruptedRunSettlement> {
    this.#getWorkflow(workflowId);
    // Checked before queueing: a live leg holds the run lock for its whole
    // length, and a caller waiting behind it would learn nothing.
    if (this.isRunActive(workflowId, runId)) return { kind: 'live' };
    return this.#withRunLock(workflowId, runId, () =>
      this.#withLifecycleLock(workflowId, runId, async () => {
        const source = await this.#captureWorkflowStorage(workflowId);
        const state = await source.load.call(source.workflows, {
          workflowName: workflowId,
          runId,
        });
        if (!state) return { kind: 'absent' };
        if (state.runId !== runId)
          throw new RunStateUnreadableError(workflowId, runId);
        const lifecycle = lifecycleFromRequestContext(state.requestContext);
        // Ahead of the status checks: a cancel that took effect leaves Mastra's
        // terminal `canceled` precursor, which only the recorded transition
        // turns into `cancelled` with its cleanup.
        const recorded = lifecycle?.terminal ?? lifecycle?.transitionIntent;
        if (lifecycle?.terminal?.cleanupCompletedAt !== undefined)
          return { kind: 'durable' };
        if (!recorded) {
          if (isDurableRunStatus(state.status)) return { kind: 'durable' };
          if (state.status === 'pending') throw new RunStartPendingError();
          if (!TERMINABLE_RUN_STATUSES.has(state.status as RunStatus))
            throw new RunStateUnreadableError(workflowId, runId);
        }
        // Every settlement write below is a compare-and-set against this
        // silent row: a write that lands after the read makes it miss (`live`).
        const silent = await this.#silentRow(source, workflowId, runId, now);
        if (!silent) return { kind: 'live' };
        // The write replaces the silent row, so it is built from that row; one
        // that moved since the decision above waits for the next wake.
        const current = snapshotOfRow(silent);
        try {
          if (recorded)
            return {
              kind: 'transition',
              replayPrincipal: recorded
                .replayPrincipals[0] as RunLifecyclePrincipal,
              transition: await this.#transitionTerminalLocked(
                workflowId,
                runId,
                recorded.status,
                now,
                {},
                source,
                { state: current, row: silent },
              ),
            };
          const currentLifecycle = lifecycleFromRequestContext(
            current.requestContext,
          );
          if (
            current.status !== state.status ||
            currentLifecycle?.revision !== lifecycle?.revision ||
            currentLifecycle?.terminal ||
            currentLifecycle?.transitionIntent
          )
            return { kind: 'live' };
          await this.#persistLifecycle(
            workflowId,
            runId,
            { ...current, ...failedByMarker('interruptedAt', now) },
            advanceLifecycle(currentLifecycle, { interruptedAt: now }),
            now,
            source,
            silent,
          );
        } catch (error) {
          if (error instanceof RowChangedError) return { kind: 'live' };
          throw error;
        }
        return {
          kind: 'interrupted',
          summary: await this.#summaryAfterPersist(
            workflowId,
            runId,
            source,
            runProvenance(state),
          ),
        };
      }),
    );
  }

  /**
   * The run's row, when no leg has marked it live within RUN_LEG_SILENT_MS and
   * the storage can compare-and-set it; `undefined` otherwise.
   */
  async #silentRow(
    source: CapturedWorkflowStorage,
    workflowId: string,
    runId: string,
    now: number,
  ): Promise<RawWorkflowSnapshot | undefined> {
    if (source.storage !== 'd1' || !source.touch || !source.replace)
      return undefined;
    const raw = await source.readSnapshot.call(source.capability, {
      workflowId,
      runId,
    });
    return raw && rowSilent(raw, now) ? raw : undefined;
  }

  /**
   * The run's snapshot for a lifecycle transition, with the exact row it was
   * parsed from where the storage can compare-and-set that row, so the write
   * lands only on the row the decision read. Other storage answers the
   * snapshot alone and keeps its unconditional write.
   */
  async #readForTransition(
    source: CapturedWorkflowStorage,
    workflowId: string,
    runId: string,
  ): Promise<TransitionRead> {
    if (source.storage === 'd1' && source.replace) {
      const row = await source.readSnapshot.call(source.capability, {
        workflowId,
        runId,
      });
      if (!row) throw new UnknownRunError(workflowId, runId);
      return { state: snapshotOfRow(row), row };
    }
    const state = await source.load.call(source.workflows, {
      workflowName: workflowId,
      runId,
    });
    if (!state) throw new UnknownRunError(workflowId, runId);
    if (state.runId !== runId)
      throw new RunStateUnreadableError(workflowId, runId);
    return { state };
  }

  /**
   * Repeat one read-decide-write pass until its compare-and-set lands or it
   * decides without writing, for at most LIFECYCLE_WRITE_ATTEMPTS passes. A
   * miss means the row changed after the read, so the next pass decides again
   * from the row as it now stands.
   */
  async #retryOnRowChange<T>(
    workflowId: string,
    runId: string,
    pass: () => Promise<T>,
  ): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      try {
        return await pass();
      } catch (error) {
        if (!(error instanceof RowChangedError)) throw error;
        if (attempt >= LIFECYCLE_WRITE_ATTEMPTS)
          throw new RunStateUnreadableError(workflowId, runId, {
            cause: new Error(
              `the run row changed on each of ${LIFECYCLE_WRITE_ATTEMPTS} compare-and-set attempts`,
              { cause: error },
            ),
          });
      }
    }
  }

  /** @internal Settle the exact selected terminal generation before managed cleanup. */
  async settleStartExecution(
    state: AuthoritativeStartState,
    originalClaim?: StartReservationReading,
  ): Promise<void> {
    const claim =
      originalClaim === undefined
        ? undefined
        : captureReservation(originalClaim, 'started');
    if (state.kind !== 'result' || !isTerminalRunStatus(state.summary.status))
      throw new RunStartPendingError();
    const identity = state.provenance.startIdentity;
    if (claim) {
      if (!identity || !this.#startIdempotency)
        throw new ExecutionFenceUnreadableError(
          'run start settlement is unresolved',
        );
      assertClaimIdentity(claim, { ...identity, runId: state.execution.runId });
    }
    if (!identity || !this.#startIdempotency) return;
    await this.#startIdempotency.settleExecution({
      ...state.execution,
      ...identity,
    });
  }

  // A provider crash propagates (fail loud): silently starting the leg with
  // fewer capabilities than intended would mask the fault. Missing grants can
  // only ever deny downstream (fail closed), so loud propagation is safe.
  //
  // Every leg — provider or not — carries a base context minting the
  // workflow-scope key: the runtime is the trusted authority for "which
  // workflow is executing", so the scope is never client-suppliable. Provider values are partitioned into
  // stored application context, capabilities, and trusted identity so every
  // layer has an explicit order and a provider cannot replace runtime scope.
  async #requestContextFor(
    workflowId: string,
    runId: string,
    leg: RunLeg,
    provenance?: RunProvenance,
    storedRequestContext?: Record<string, unknown>,
    lifecycle?: RunLifecycleState,
  ): Promise<RequestContext> {
    const base: Record<string, unknown> = {
      [BREAKWATER_WORKFLOW_SCOPE_KEY]: workflowId,
      runId,
    };
    if (provenance !== undefined) {
      base[RUN_PROVENANCE_CONTEXT_KEY] = provenance;
    }
    if (lifecycle !== undefined) {
      base[RUN_LIFECYCLE_CONTEXT_KEY] = lifecycle;
    }
    // No isolation scope is minted here: a deployment serves exactly one
    // organization. The isolation-scope context key is RESERVED, so a provider
    // can never mint a scope that desyncs from the execution identity below.
    if (leg.kind === 'start') {
      base[BREAKWATER_CONNECTOR_EXECUTION_KEY] = {
        kind: 'start',
        workflowId,
        runId,
      };
    } else if (leg.step !== undefined && leg.suspendedAt !== undefined) {
      base[BREAKWATER_CONNECTOR_EXECUTION_KEY] = {
        kind: 'resume',
        workflowId,
        runId,
        suspension: {
          stepPath: [...leg.step],
          suspendedAt: leg.suspendedAt,
          ...(leg.resumeCount === undefined
            ? {}
            : { resumeCount: leg.resumeCount }),
        },
      };
    } else {
      base[BREAKWATER_CONNECTOR_EXECUTION_KEY] = null;
    }
    const values = this.#requestContextForRun
      ? await this.#requestContextForRun(workflowId, runId, leg)
      : undefined;
    return orderedRequestContext(base, values, storedRequestContext);
  }

  async #trustedResumePreparation(
    workflowId: string,
    runId: string,
    state: WorkflowState,
    options: Pick<
      ResumeRunOptions,
      | 'step'
      | 'requestedBy'
      | 'requestedByKind'
      | 'deadlineMs'
      | 'economicOperations'
      | 'expectedSuspension'
    >,
  ): Promise<{
    nextCounts: ReadonlyMap<string, number>;
    provenance: RunProvenance;
    requestContext: RequestContext;
    lifecycle?: RunLifecycleState;
  }> {
    const {
      step: selectedStep,
      requestedBy,
      requestedByKind,
      deadlineMs,
      economicOperations,
      expectedSuspension,
    } = options;
    if (requestedBy !== undefined && !isExecutionPrincipalId(requestedBy)) {
      throw new InvalidRunRequestError('requestedBy is malformed');
    }
    if (
      requestedByKind !== undefined &&
      !isExecutionPrincipalKind(requestedByKind)
    ) {
      throw new InvalidRunRequestError('requestedByKind is malformed');
    }
    if ((requestedBy === undefined) !== (requestedByKind === undefined)) {
      throw new InvalidRunRequestError(
        'requestedBy and requestedByKind must be provided together',
      );
    }
    const step = resolveResumeStep(selectedStep, state);
    const stepKey = step?.join('.');
    // resumeCount is read BEFORE this resume increments it: it is the count
    // of prior resumes = the ordinal of the CURRENT suspension being resumed
    // (undefined for a first suspension), which the minting approval captured.
    const storedProvenance = runProvenance(state);
    if (
      requestedBy === undefined &&
      storedProvenance?.requestedBy !== undefined &&
      storedProvenance.requestedByKind === undefined
    ) {
      throw new InvalidRunRequestError(
        'legacy requestedBy provenance requires an explicit requestedBy and requestedByKind to resume',
      );
    }
    const requester = requestedBy ?? storedProvenance?.requestedBy;
    const requesterKind =
      requestedBy === undefined
        ? storedProvenance?.requestedByKind
        : requestedByKind;
    const priorCounts = new Map(storedProvenance?.resumeCounts ?? []);
    const leg: RunLeg = {
      kind: 'resume',
      step,
      suspendedAt:
        stepKey !== undefined ? suspendedAtOf(state.steps, stepKey) : undefined,
      resumeCount: stepKey !== undefined ? priorCounts.get(stepKey) : undefined,
    };
    // The pair the leg's grant minting reads, compared as
    // boundToCurrentSuspension compares it.
    if (expectedSuspension !== undefined) {
      if (stepKey === undefined)
        throw new InvalidRunRequestError(
          'expectedSuspension requires a resolvable resumed step',
        );
      if (
        leg.suspendedAt !== expectedSuspension.suspendedAt ||
        leg.resumeCount !== expectedSuspension.resumeCount
      )
        throw new SuspensionChangedError(workflowId, runId, stepKey);
    }
    const nextCounts = new Map(priorCounts);
    if (stepKey !== undefined) {
      nextCounts.set(stepKey, nextResumeCount(nextCounts.get(stepKey) ?? 0));
    }
    const provenance: RunProvenance = {
      ...(storedProvenance?.version === 2
        ? storedProvenance
        : { version: 1 as const }),
      ...(requester === undefined
        ? {}
        : { requestedBy: requester, requestedByKind: requesterKind }),
      startToken: storedProvenance?.startToken ?? crypto.randomUUID(),
      attemptToken: crypto.randomUUID(),
      resumeCounts: [...nextCounts],
    };
    const storedLifecycle = lifecycleFromRequestContext(state.requestContext);
    const replacementDeadline = relativeDeadline(deadlineMs);
    const replacementOperations =
      canonicalEconomicOperations(economicOperations);
    const lifecycle: RunLifecycleState | undefined =
      replacementDeadline === undefined && replacementOperations === undefined
        ? storedLifecycle
        : advanceLifecycle(storedLifecycle, {
            ...(replacementDeadline === undefined
              ? {}
              : { deadlineAt: replacementDeadline }),
            ...(replacementOperations === undefined
              ? {}
              : { economicOperations: replacementOperations }),
          });
    const requestContext = await this.#requestContextFor(
      workflowId,
      runId,
      leg,
      provenance,
      undefined,
      lifecycle,
    );
    return {
      nextCounts,
      provenance,
      requestContext,
      lifecycle,
    };
  }

  #workflowState(
    workflowId: string,
    runId: string,
    withNestedWorkflows = false,
    fields: WorkflowStateField[] = RUN_STATE_FIELDS,
  ): Promise<WorkflowState | null> {
    return this.#getWorkflow(workflowId).getWorkflowRunById(runId, {
      fields,
      withNestedWorkflows,
    });
  }

  /**
   * With `expected`, a compare-and-set against that exact row, serialized as
   * Mastra's own persist does; a miss throws RowChangedError.
   */
  async #persistLifecycle(
    workflowId: string,
    runId: string,
    state: WorkflowRunState,
    lifecycle: RunLifecycleState,
    now: number,
    source: CapturedWorkflowStorage,
    expected?: RawWorkflowSnapshot,
  ): Promise<WorkflowRunState> {
    const workflows = source.workflows;
    const persisted: WorkflowRunState = {
      ...state,
      requestContext: {
        ...(state.requestContext ?? {}),
        [RUN_LIFECYCLE_CONTEXT_KEY]: lifecycle,
      },
      timestamp: now,
    };
    if (expected) {
      if (source.storage !== 'd1' || !source.replace)
        throw new Error('storage cannot compare-and-set the run row');
      const replaced = await source.replace.call(source.capability, expected, {
        snapshot: JSON.stringify(persisted),
        updatedAt: new Date(now).toISOString(),
      });
      if (!replaced) throw new RowChangedError();
      return persisted;
    }
    await source.persist.call(workflows, {
      workflowName: workflowId,
      runId,
      snapshot: persisted,
      updatedAt: new Date(now),
    });
    return persisted;
  }

  /**
   * Persist a cancellation intent through `patchRunLifecycle` where the storage
   * has it, otherwise as #persistLifecycle does; a miss throws RowChangedError.
   * The patch also misses on a snapshot SQLite cannot parse, which the
   * whole-row compare-and-set still replaces, so that write is tried before
   * the miss counts as a row change.
   */
  async #persistIntent(
    workflowId: string,
    runId: string,
    state: WorkflowRunState,
    intent: RunLifecycleState,
    now: number,
    source: CapturedWorkflowStorage,
    row: RawWorkflowSnapshot | undefined,
  ): Promise<void> {
    if (source.storage === 'd1' && source.patch) {
      const patched = await source.patch.call(
        source.capability,
        { workflowId, runId },
        {
          status: state.status,
          lifecycleRevision: lifecycleFromRequestContext(state.requestContext)
            ?.revision,
          // As read from the row: the patch compares stored JSON text, and the
          // parsed lifecycle orders its keys differently.
          lifecycle: state.requestContext?.[RUN_LIFECYCLE_CONTEXT_KEY],
        },
        {
          lifecycle: intent,
          timestamp: now,
          updatedAt: new Date(now).toISOString(),
        },
      );
      if (patched) return;
      if (!row) throw new RowChangedError();
    }
    await this.#persistLifecycle(
      workflowId,
      runId,
      state,
      intent,
      now,
      source,
      row,
    );
  }

  async #summaryAfterPersist(
    workflowId: string,
    runId: string,
    source: CapturedWorkflowStorage,
    expected: RunProvenance | undefined,
  ): Promise<RunSummary> {
    if (expected?.version === 2) {
      const selected = await this.#completedStartState(
        source,
        runExecutionIdentityFor(
          { tablePrefix: source.tablePrefix, workflowId, runId },
          expected,
        ),
        expected,
      );
      if (isTerminalRunStatus(selected.summary.status))
        await this.settleStartExecution(selected);
      return selected.summary;
    }
    const state = await this.#workflowState(workflowId, runId);
    if (!state) throw new UnknownRunError(workflowId, runId);
    return this.#summaryFromState(runId, state);
  }

  #summaryFromState(runId: string, state: WorkflowState): RunSummary {
    const provenance = runProvenance(state);
    if (state.status === 'pending' && provenance?.version === 2)
      throw new RunStartPendingError();
    return summarizeState(
      runId,
      state,
      provenance ? new Map(provenance.resumeCounts) : undefined,
      provenance?.requestedBy,
      provenance?.requestedByKind,
    );
  }

  async #summaryForAttempt(
    workflowId: string,
    runId: string,
    expected: Pick<RunProvenance, 'version' | 'startToken' | 'attemptToken'>,
  ): Promise<RunSummary | undefined> {
    try {
      if (expected.version === 2) {
        const source = this.#activeRuns.get(
          this.#runKey(workflowId, runId),
        )?.source;
        if (!source) return undefined;
        const state = await this.#completedStartState(
          source,
          {
            tablePrefix: source.tablePrefix,
            workflowId,
            runId,
            startToken: expected.startToken,
          },
          expected,
        );
        await this.#settleStartReservation(state);
        return state.summary;
      }
      const persisted = await this.#workflowState(workflowId, runId);
      if (
        !persisted ||
        persisted.isFromInMemory ||
        persisted.status === 'pending'
      )
        return undefined;
      const provenance = runProvenance(persisted);
      if (
        provenance?.version !== expected.version ||
        provenance.attemptToken !== expected.attemptToken
      )
        return undefined;
      return summarizeState(
        runId,
        persisted,
        new Map(provenance.resumeCounts),
        provenance.requestedBy,
        provenance.requestedByKind,
      );
    } catch {
      return undefined;
    }
  }

  /**
   * When `error` is a storage refusal of state that cannot be stored, record
   * the leg's run as failed and stop the leg. The row still holds the last
   * state the run stored, and only the keys summary selection reads are laid
   * over it from the leg's context: the other values may be the ones SQLite
   * cannot parse. The marker makes the settled-row guard refuse the leg's later
   * writes. Never throws: the caller's summary read follows.
   */
  async #failUnstorableLeg(
    workflowId: string,
    runId: string,
    active: ActiveRun,
    error: unknown,
  ): Promise<void> {
    if (!isRunStateNotStorable(error)) return;
    const { source, requestContext } = active;
    if (source?.storage === 'd1' && source.replace && requestContext) {
      const live = Object.fromEntries(requestContext.entries());
      const overlay = Object.fromEntries(
        SELECTION_CONTEXT_KEYS.filter((key) => key in live).map((key) => [
          key,
          live[key],
        ]),
      );
      try {
        await this.#withLifecycleLock(workflowId, runId, () =>
          this.#retryOnRowChange(workflowId, runId, async () => {
            const { state, row } = await this.#readForTransition(
              source,
              workflowId,
              runId,
            );
            const { context, lifecycle } = overlayLegContext(
              state.requestContext,
              overlay,
            );
            if (
              !TERMINABLE_RUN_STATUSES.has(state.status as RunStatus) ||
              lifecycle?.transitionIntent !== undefined ||
              RUN_SETTLING_MARKERS.some(
                (marker) => lifecycle?.[marker] !== undefined,
              )
            )
              return;
            const now = Date.now();
            await this.#persistLifecycle(
              workflowId,
              runId,
              {
                ...state,
                ...failedByMarker('stateNotStorableAt', now),
                requestContext: context,
              },
              advanceLifecycle(lifecycle, { stateNotStorableAt: now }),
              now,
              source,
              row,
            );
          }),
        );
      } catch (settleError) {
        console.error(
          JSON.stringify({
            type: 'run-state-not-storable-settle-failed',
            workflowId,
            runId,
            error: errorMessageOf(settleError),
          }),
        );
      }
    }
    this.#abortLeg(
      active,
      new RunStateNotStorableError(workflowId, runId),
      'run-leg-state-not-storable-abort',
      workflowId,
      runId,
    );
  }

  async #reconcileTerminalState(
    workflowId: string,
    runId: string,
    result: CoreRunResult,
    requestContext: RequestContext,
    source: CapturedWorkflowStorage,
    rowStored: boolean,
    proof?: D1RunExecutionIdentity,
  ): Promise<void> {
    const opts = terminalStateUpdate(result);
    if (!opts) return;
    await this.#withLifecycleLock(workflowId, runId, async () => {
      const workflows = source.workflows;
      const snapshot = await source.load.call(workflows, {
        workflowName: workflowId,
        runId,
      });
      if (!snapshot) {
        // A row gone after this leg stored it was removed by retention, either
        // after another instance settled the run or after this leg's own
        // terminal write (`success` or `failed`).
        if (rowStored) throw new RunSettledConflictError(workflowId, runId);
        throw new Error(
          `RunnerRuntime: run '${runId}' of workflow '${workflowId}' completed without a durable snapshot`,
        );
      }
      if (snapshot.runId !== runId)
        throw new RunStateUnreadableError(workflowId, runId);
      const expected = runProvenance({
        requestContext: Object.fromEntries(requestContext.entries()),
      });
      const persisted = runProvenance(snapshot);
      if (
        expected?.version === 2 &&
        ((persisted !== undefined &&
          (persisted.version !== 2 ||
            persisted.startToken !== expected.startToken)) ||
          (this.#executionFence && persisted?.version !== 2))
      )
        throw new RunStateUnreadableError(workflowId, runId);
      if (proof)
        await this.#assertRetainedResume(
          source,
          workflowId,
          runId,
          expected,
          proof,
        );
      // Core merges resume context over the prior snapshot. Terminal-only
      // repair must persist that same effective context, including application
      // keys the current provider intentionally omitted.
      const {
        context: authoritativeContext,
        persistedLifecycle,
        lifecycle: reconciledLifecycle,
      } = overlayLegContext(
        snapshot.requestContext,
        Object.fromEntries(requestContext.entries()),
      );
      if (reconciledLifecycle) {
        authoritativeContext[RUN_LIFECYCLE_CONTEXT_KEY] = reconciledLifecycle;
      }
      const persistedCoversLifecycle =
        reconciledLifecycle === undefined ||
        (persistedLifecycle !== undefined &&
          JSON.stringify(persistedLifecycle) ===
            JSON.stringify(reconciledLifecycle));
      const persistedToken = runProvenance(snapshot)?.attemptToken;
      const authoritativeToken = runProvenance({
        ...snapshot,
        requestContext: authoritativeContext,
      })?.attemptToken;
      if (
        snapshot.status === result.status &&
        persistedToken === authoritativeToken &&
        persistedCoversLifecycle
      ) {
        return;
      }
      await source.persist.call(workflows, {
        workflowName: workflowId,
        runId,
        snapshot: {
          ...snapshot,
          ...opts,
          requestContext: authoritativeContext,
          timestamp: Date.now(),
        },
      });
    });
  }

  #getWorkflow(workflowId: string): AnyWorkflow {
    this.#ensureMastra();
    const workflow = this.#workflows.get(workflowId);
    if (!workflow) throw new UnknownWorkflowError(workflowId);
    // Run-state reads and run creation go through the workflow's own Mastra,
    // so a workflow another Mastra registered would reach that Mastra's storage.
    // Another Mastra can register the object during any await, so callers look
    // the workflow up again where they reach storage, not only at entry.
    if (workflow.mastra !== this.#mastra) {
      throw new Error(
        `RunnerRuntime: workflow '${workflowId}' is not registered on this runtime's Mastra — another runtime or Mastra has registered the same workflow object`,
      );
    }
    return workflow;
  }

  #createRun(
    workflowId: string,
    runId: string,
  ): ReturnType<AnyWorkflow['createRun']> {
    return this.#getWorkflow(workflowId).createRun({
      runId,
      pubsub: this.#pubsub,
    });
  }

  #ensureMastra(): void {
    if (this.#mastra) return;
    this.#mastra = new Mastra({
      // Mastra uses plain-object registries. Remap inherited object keys only,
      // preserving normal getAgent(id) lookups; collision IDs use intrinsic-id lookup.
      agents: Object.fromEntries(
        mastraRegistryEntries(this.#agents, 'runtime-agent'),
      ),
      // The same rule keeps normal getWorkflow(id) behavior while making
      // `__proto__`/`constructor` workflows available via intrinsic id.
      workflows: Object.fromEntries(
        mastraRegistryEntries(this.#workflows, 'runtime-workflow'),
      ),
      storage: this.#storage,
      logger: this.#logger,
      ...(this.#pubsub !== undefined ? { pubsub: this.#pubsub } : {}),
    });
  }

  // The run's full identity as a single map key: workflowId + runId, never runId
  // alone. The same caller-supplied runId under two workflows are DISTINCT
  // persisted runs (Mastra snapshots key on workflowName+runId) and must never
  // share a per-run FIFO entry. Composing the key in ONE place keeps every
  // per-run map keyed identically, so no map can key by runId alone and cross
  // workflow boundaries.
  // This is the exact string the DO name join produces
  // (idFromName(`${workflowId}:${runId}`)); PATH_SAFE_ID_PATTERN excludes ':'
  // from both ids, so the join is unambiguous.
  #runKey(workflowId: string, runId: string): string {
    return `${workflowId}:${runId}`;
  }

  async #withLifecycleLock<T>(
    workflowId: string,
    runId: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.#withLock(
      this.#lifecycleLocks,
      this.#runKey(workflowId, runId),
      fn,
    );
  }

  // FIFO per-run lock: callers for the same run execute strictly in arrival
  // order; distinct runs do not contend. The map entry is removed when the last
  // waiter settles, so idle runs hold no memory. Keyed by the run's full identity
  // via #runKey (workflowId + runId), not runId alone — the in-process lock
  // granularity thus matches the cross-instance DO routing granularity.
  async #withRunLock<T>(
    workflowId: string,
    runId: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    return this.#withLock(this.#runLocks, this.#runKey(workflowId, runId), fn);
  }

  async #withLock<T>(
    locks: Map<string, Promise<unknown>>,
    key: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const previous = locks.get(key) ?? Promise.resolve();
    const task = previous.then(fn);
    const tail = task.then(
      () => undefined,
      () => undefined,
    );
    locks.set(key, tail);
    void tail.then(() => {
      if (locks.get(key) === tail) locks.delete(key);
    });
    return task;
  }
}
