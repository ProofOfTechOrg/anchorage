// SPDX-License-Identifier: Apache-2.0
// RunnerRuntime hosts Mastra workflow execution against injected storage.
// It is deliberately environment-free: the Durable Object shell feeds it
// D1-backed storage; tests feed it InMemoryStore. Durability comes from
// Mastra's own snapshot persistence — createRun() writes the initial
// snapshot, the engine persists after each step boundary, and resume()
// loads the snapshot — so a run started in one process resumes in any
// other process that shares the same database. Persistence only happens
// for workflows registered on a Mastra instance that has storage (core
// silently skips it otherwise), which is why this class owns the Mastra
// instance instead of running standalone workflows.
//
// Concurrency: start/resume are serialized per run — keyed by the run's
// full identity (workflowId + runId) — via an in-instance FIFO lock.
// Without it, two concurrent resumes both pass the 'suspended' pre-check
// and the gated step's side effects execute twice — the exact failure an
// approval product must not have. The lock is per runtime instance;
// cross-instance serialization comes from routing one DO instance per run
// (see durable-object.ts).

import type { Agent, ToolsInput } from '@mastra/core/agent';
import type { IMastraLogger } from '@mastra/core/logger';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import type {
  MastraCompositeStore,
  UpdateWorkflowStateOptions,
} from '@mastra/core/storage';
import type {
  AnyWorkflow,
  WorkflowRunStatus,
  WorkflowState,
  WorkflowStateField,
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
import {
  isReservedExecutionContextKey,
  RUN_PROVENANCE_CONTEXT_KEY,
  stripReservedExecutionContext,
} from './execution-context.js';
import { mastraRegistryEntries } from './mastra-registry.js';
import { isPathSafeId } from './path-safe-id.js';
import type { HostPubSub } from './pubsub.js';

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

export class RunAlreadyExistsError extends Error {
  constructor(workflowId: string, runId: string, status: WorkflowRunStatus) {
    super(
      `run '${runId}' of workflow '${workflowId}' already exists (status '${status}')`,
    );
    this.name = 'RunAlreadyExistsError';
  }
}

/** A request the caller can fix: bad input/resume data or step selection. */
export class InvalidRunRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidRunRequestError';
  }
}

// PATH_SAFE_ID_PATTERN validates runId and workflowId (full rationale lives on
// the leaf module). runId is validated at start() (client input →
// InvalidRunRequestError); workflowId is developer-controlled and validated at
// register() (programming error → plain Error).

// Core surfaces input/resume-schema violations and wrong-step selections as
// untyped Errors. Message matching is brittle, so it is scoped to choosing
// the HTTP-facing error class only; anything unmatched propagates unchanged
// (a 500 at the DO boundary, same as before classification existed).
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
  status: WorkflowRunStatus;
  /** Durable execution provenance used by approval reconciliation. */
  requestedBy?: string;
  /** Principal kind paired with `requestedBy`; absent on legacy snapshots. */
  requestedByKind?: ExecutionPrincipalKind;
  result?: unknown;
  error?: string;
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

interface RunProvenance {
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

function runProvenance(
  state: Pick<WorkflowState, 'requestContext'>,
): RunProvenance | undefined {
  const value = state.requestContext?.[RUN_PROVENANCE_CONTEXT_KEY];
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object') {
    throw new Error('stored run provenance is malformed');
  }
  const candidate = value as Partial<RunProvenance>;
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
  const counts: Array<[string, number]> = [];
  for (const entry of candidate.resumeCounts) {
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof entry[0] !== 'string' ||
      !Number.isSafeInteger(entry[1]) ||
      entry[1] < 1
    ) {
      throw new Error('stored run provenance is malformed');
    }
    counts.push([entry[0], entry[1]]);
  }
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

// Structural view of core's WorkflowResult union — only the fields the
// summary transports. AnyWorkflow erases the generics, so narrowing happens
// here on the status discriminant.
type CoreRunResult =
  | { status: 'success'; result: unknown }
  | { status: 'failed'; error: unknown }
  | {
      status: 'suspended';
      suspended: [string[], ...string[][]];
      suspendPayload?: unknown;
      /** Per-step state incl. suspendedAt (workflows/types, suspended arm). */
      steps?: WorkflowState['steps'];
    }
  | { status: 'tripwire'; tripwire: { reason: string } }
  | {
      status: Exclude<
        WorkflowRunStatus,
        'success' | 'failed' | 'suspended' | 'tripwire'
      >;
    };

const NONTERMINAL_RUN_STATUSES = new Set<WorkflowRunStatus>([
  'running',
  'suspended',
  'waiting',
  'pending',
  'paused',
]);

// Registered agents may carry incompatible tool/output generics. The public
// method preserves each concrete type; this erased form is only for handing
// the heterogeneous registry to Mastra.
type ErasedRuntimeAgent = Agent<string, ToolsInput, unknown>;

function terminalStateUpdate(
  result: CoreRunResult,
): UpdateWorkflowStateOptions | undefined {
  if (NONTERMINAL_RUN_STATUSES.has(result.status)) return undefined;
  const common = {
    status: result.status,
    result: undefined,
    error: undefined,
    suspendedPaths: {},
    waitingPaths: {},
    resumeLabels: {},
    activePaths: [],
    activeStepsPath: {},
  };
  if (result.status === 'success') {
    return {
      ...common,
      result: result.result as UpdateWorkflowStateOptions['result'],
    };
  }
  if (result.status === 'failed') {
    const error = result.error;
    const name =
      error instanceof Error
        ? error.name
        : error !== null &&
            typeof error === 'object' &&
            'name' in error &&
            typeof (error as { name: unknown }).name === 'string'
          ? (error as { name: string }).name
          : 'Error';
    const stack =
      error instanceof Error
        ? error.stack
        : error !== null &&
            typeof error === 'object' &&
            'stack' in error &&
            typeof (error as { stack: unknown }).stack === 'string'
          ? (error as { stack: string }).stack
          : undefined;
    return {
      ...common,
      error: {
        name,
        message: errorText(error),
        ...(stack !== undefined ? { stack } : {}),
      },
    };
  }
  return common;
}

// Failed runs carry the step's thrown error as an Error instance, a string,
// or — once it crossed an engine/persistence boundary — a serialized
// { name, message, stack } object; String() on the last reads
// '[object Object]', so extract the message wherever it lives.
function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (
    error !== null &&
    typeof error === 'object' &&
    'message' in error &&
    typeof (error as { message: unknown }).message === 'string'
  ) {
    return (error as { message: string }).message;
  }
  return String(error);
}

function summarize(
  runId: string,
  result: CoreRunResult,
  counts?: ReadonlyMap<string, number>,
  requestedBy?: string,
  requestedByKind?: ExecutionPrincipalKind,
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
  state: WorkflowState,
): string[] | undefined {
  if (Array.isArray(step)) return [...step];
  if (typeof step === 'string') return step.split('.');
  const suspended = Object.keys(state.suspendedPaths ?? {});
  return suspended.length === 1 ? suspended[0]?.split('.') : undefined;
}

// The live attempt for a step. Repeated steps (foreach) store an array of
// attempts, so the current suspension/resume is the latest; a single-run step
// stores one entry. Shared by the suspendedAt/resumedAt/suspendPayload readers.
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
 * One home for the per-step projection convention behind every RunSummary map
 * (suspendedAt / resumedAt / resumeCount / suspendPayload): collect
 * `stepKey -> value` over the suspended step keys, projecting an empty result
 * as undefined so JSON summaries omit the field. The accumulator is
 * null-prototype because step keys are author-chosen strings: on a plain
 * object literal a step named '__proto__' would route into the
 * Object.prototype setter and silently vanish (or, for an object-valued
 * payload, rewire the accumulator's prototype) — the same hazard
 * snapshot provenance stores [stepKey, count] pairs to avoid.
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
function summarizeState(
  runId: string,
  state: WorkflowState,
  counts?: ReadonlyMap<string, number>,
  requestedBy?: string,
  requestedByKind?: ExecutionPrincipalKind,
): RunSummary {
  const summary: RunSummary = {
    runId,
    status: state.status,
    createdAt: toIso(state.createdAt),
    updatedAt: toIso(state.updatedAt),
  };
  if (requestedBy !== undefined) summary.requestedBy = requestedBy;
  if (requestedByKind !== undefined) {
    summary.requestedByKind = requestedByKind;
  }
  if (state.status === 'success') {
    summary.result = state.result;
  } else if (state.status === 'failed' && state.error) {
    summary.error = errorText(state.error);
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
 * within a millisecond. It replaces the payload-conditional `resumedAt` as the
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
 * Server-side requestContext source, consulted on EVERY start and resume.
 * This is the trusted-computing-base seam (security-threat-model.md, trust
 * boundary 6): the DO HTTP boundary never maps requestContext from request
 * bodies, so capability keys — e.g. breakwater's approval grants
 * ('breakwater.connectorGrants') — can only enter a run through this
 * provider. Wire it to derive values from trusted server-side state (the
 * flowsafe approval store), never from client input, model output, or tool
 * results.
 *
 * Merge semantics are pinned by tests against the installed Mastra core: the context provided
 * at resume merges OVER the run's persisted context — provided keys win,
 * persisted start-time keys survive. Omitting a key therefore does not
 * revoke it; a provider that scopes a capability per leg must return the key
 * on EVERY leg (an empty value when nothing applies) so the overwrite
 * retires stale grants.
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
   * reaches the runtime with no host change: every DO subclass already returns
   * init()'s runtime from build(), and nothing else in the isolate can hand this
   * object a pubsub.
   *
   * PASSED TO CORE at the two `workflow.createRun({ runId, pubsub })` sites in
   * start()/resume() — the only place core accepts one; the
   * `pubsub` getter still exposes the held identity so an agent runner sharing
   * this isolate takes THIS instance rather than building a second feed. Absent
   * ⇒ undefined ⇒ core defaults a fresh emitter per run ⇒ byte-identical to
   * before this seam existed.
   */
  pubsub?: HostPubSub;
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
   * keyed by everywhere it lands (D1 snapshot row, DO name, R2 segment,
   * grant-list predicate). A generation fallback here would let any caller
   * that forgets to mint create a run under an id the host never issued.
   */
  runId: string;
  inputData?: unknown;
  /** Initial workflow state from an infrastructure-verified schedule target. */
  initialState?: unknown;
  /**
   * Non-reserved application context from an infrastructure-verified schedule
   * target. Runtime-owned keys are stripped again before execution.
   */
  storedRequestContext?: Record<string, unknown>;
  /** Host recovery token persisted with the first executed snapshot. */
  attemptToken?: string;
} & OptionalRunRequester;

export type ResumeRunOptions = {
  /** Suspended step id (or nested path). Optional when only one step is suspended. */
  step?: string | string[];
  resumeData?: unknown;
  /**
   * Host preparation that must consume the exact trusted context this resume
   * will execute with. Runs once, inside the per-run lock, before createRun.
   * @internal
   */
  prepareExecution?: (requestContext: RequestContext) => Promise<void>;
} & OptionalRunRequester;

export class RunnerRuntime {
  readonly #storage: MastraCompositeStore;
  readonly #logger: IMastraLogger | false;
  readonly #requestContextForRun?: RequestContextProvider;
  readonly #agents = new Map<string, ErasedRuntimeAgent>();
  readonly #workflows = new Map<string, AnyWorkflow>();
  readonly #runLocks = new Map<string, Promise<unknown>>();
  // The host DO's pubsub identity (RunnerRuntimeOptions.pubsub), threaded into
  // both createRun sites below (CI-M-002-002) so a configured host publishes and
  // replays on ONE shared feed. Undefined ⇒ core defaults a fresh emitter per
  // run ⇒ byte-identical to before this seam existed.
  readonly #pubsub?: HostPubSub;
  #mastra?: Mastra;

  constructor(options: RunnerRuntimeOptions) {
    this.#storage = options.storage;
    this.#logger = options.logger ?? false;
    this.#requestContextForRun = options.requestContextForRun;
    this.#pubsub = options.pubsub;
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
        `RunnerRuntime: workflow id '${workflow.id}' must be URL-path-safe (letters, digits, '.', '_', '~', '-'; 1-200 chars) — it feeds the DO name join and the /runs/:workflowId/:runId path`,
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

  async start(
    workflowId: string,
    options: StartRunOptions,
  ): Promise<RunSummary> {
    const workflow = this.#getWorkflow(workflowId);
    // Reject non-path-safe ids at the mint boundary so the runId is unambiguous
    // everywhere it addresses the run (D1 key, DO name, URL path) — see
    // PATH_SAFE_ID_PATTERN. Fail fast, before the lock and any createRun work.
    // The typeof guard is load-bearing, not redundant with the string type:
    // this value can arrive from JSON.parse through an unchecked `as` cast
    // (durable-object.ts readJson), and RegExp.test() coerces its argument to a
    // String — so a numeric runId like 123 would pass the pattern as "123" yet
    // mint a run keyed by the number 123, unreachable by the string "123" the
    // URL path later carries. There is NO generation fallback (INV-1): a
    // missing/null runId is a client error, not a request for one.
    if (!isPathSafeId(options.runId)) {
      throw new InvalidRunRequestError(
        "runId is required and must be URL-path-safe (letters, digits, '.', '_', '~', '-'; 1–200 chars)",
      );
    }
    const runId = options.runId;
    if (
      options.requestedBy !== undefined &&
      !isExecutionPrincipalId(options.requestedBy)
    ) {
      throw new InvalidRunRequestError('requestedBy is malformed');
    }
    if (
      options.requestedByKind !== undefined &&
      !isExecutionPrincipalKind(options.requestedByKind)
    ) {
      throw new InvalidRunRequestError('requestedByKind is malformed');
    }
    if (
      (options.requestedBy === undefined) !==
      (options.requestedByKind === undefined)
    ) {
      throw new InvalidRunRequestError(
        'requestedBy and requestedByKind must be provided together',
      );
    }
    if (
      options.attemptToken !== undefined &&
      !isPathSafeId(options.attemptToken)
    ) {
      throw new InvalidRunRequestError('attemptToken is malformed');
    }
    return this.#withRunLock(workflowId, runId, async () => {
      // Supplied ids can collide with an existing run; starting it
      // again would re-execute already-executed steps.
      const existing = await workflow.getWorkflowRunById(runId);
      if (existing) {
        throw new RunAlreadyExistsError(workflowId, runId, existing.status);
      }
      // Resolve the leg's context BEFORE createRun: createRun persists the
      // initial snapshot, so a provider failure after it would strand a
      // pending-but-never-started run (a supplied runId would then be locked
      // out by RunAlreadyExistsError on retry). Failing here leaves no state.
      const startAttemptToken = options.attemptToken ?? crypto.randomUUID();
      const provenance: RunProvenance = {
        version: 1,
        ...(options.requestedBy === undefined
          ? {}
          : {
              requestedBy: options.requestedBy,
              requestedByKind: options.requestedByKind,
            }),
        startToken: startAttemptToken,
        attemptToken: startAttemptToken,
        resumeCounts: [],
      };
      const requestContext = await this.#requestContextFor(
        workflowId,
        runId,
        { kind: 'start' },
        provenance,
        options.storedRequestContext,
      );
      // Thread the host DO's pubsub identity into the run (CI-M-002-002). Core
      // accepts `createRun({ runId, pubsub })` at every one of its OWN call
      // sites (agent/durable index.js:5224/5541) and stamps it straight onto
      // `new Run({ ..., pubsub: options?.pubsub })`, defaulting a FRESH
      // EventEmitterPubSub when it is undefined. So an unconfigured host
      // (#pubsub undefined) reaches the identical `new Run({ pubsub: undefined })`
      // the prior `createRun({ runId })` produced — byte-identical, polling
      // stays the fallback. A configured host gets ONE shared feed so publish
      // and observe()/replay agree (do-runner/pubsub.ts).
      const run = await workflow.createRun({ runId, pubsub: this.#pubsub });
      let result: CoreRunResult;
      try {
        result = await run.start({
          inputData: options.inputData,
          initialState: options.initialState,
          requestContext,
        });
      } catch (error) {
        const recovered = await this.#summaryForAttempt(
          workflow,
          runId,
          provenance.attemptToken,
        );
        if (recovered) return recovered;
        throw asClientError(error) ?? error;
      }
      try {
        await this.#reconcileTerminalState(
          workflowId,
          run.runId,
          result,
          requestContext,
        );
      } catch (error) {
        const recovered = await this.#summaryForAttempt(
          workflow,
          runId,
          provenance.attemptToken,
        );
        if (recovered) return recovered;
        throw error;
      }
      return summarize(
        run.runId,
        result,
        undefined,
        provenance.requestedBy,
        provenance.requestedByKind,
      );
    });
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
    const workflow = this.#getWorkflow(workflowId);
    return this.#withRunLock(workflowId, runId, async () => {
      const state = await this.#workflowState(workflow, runId);
      if (!state) throw new UnknownRunError(workflowId, runId);
      if (state.status !== 'suspended') {
        throw new RunNotSuspendedError(workflowId, runId, state.status);
      }
      // Provider before createRun for symmetry with start(): a resume-time
      // createRun only reattaches (no snapshot write), but failing before it
      // still does the least work and keeps the ordering invariant uniform.
      const { nextCounts, provenance, requestContext } =
        await this.#trustedResumePreparation(
          workflowId,
          runId,
          state,
          options.step,
          options.requestedBy,
          options.requestedByKind,
        );
      if (options.prepareExecution) {
        const preparationValues = structuredClone(
          Object.fromEntries(requestContext.entries()),
        );
        await options.prepareExecution(
          new RequestContext(Object.entries(preparationValues)),
        );
      }
      // Same host pubsub identity as start() (CI-M-002-002) — see the note
      // there; undefined stays byte-identical to `createRun({ runId })`.
      const run = await workflow.createRun({ runId, pubsub: this.#pubsub });
      let result: CoreRunResult;
      try {
        result = await run.resume({
          step: options.step,
          resumeData: options.resumeData,
          requestContext,
        });
      } catch (error) {
        const recovered = await this.#summaryForAttempt(
          workflow,
          runId,
          provenance.attemptToken,
        );
        if (recovered) return recovered;
        // No authoritative snapshot carries this attempt token: the run stayed
        // on its prior suspension, so neither requester nor ordinal advances.
        throw asClientError(error) ?? error;
      }
      // A re-suspension produced by this resume carries the incremented ordinal
      // in the same authoritative snapshot as the new workflow state.
      const summary = summarize(
        run.runId,
        result,
        nextCounts,
        provenance.requestedBy,
        provenance.requestedByKind,
      );
      try {
        await this.#reconcileTerminalState(
          workflowId,
          run.runId,
          result,
          requestContext,
        );
      } catch (error) {
        const recovered = await this.#summaryForAttempt(
          workflow,
          runId,
          provenance.attemptToken,
        );
        if (recovered) return recovered;
        throw error;
      }
      return summary;
    });
  }

  async status(workflowId: string, runId: string): Promise<RunSummary | null> {
    const workflow = this.#getWorkflow(workflowId);
    const state = await this.#workflowState(workflow, runId);
    if (!state) return null;
    const provenance = runProvenance(state);
    // Snapshot provenance projects requester and resume ordinal alongside the
    // authoritative workflow state, including after isolate eviction.
    const counts = provenance ? new Map(provenance.resumeCounts) : undefined;
    return summarizeState(
      runId,
      state,
      counts,
      provenance?.requestedBy,
      provenance?.requestedByKind,
    );
  }

  /**
   * Reconcile an interrupted start against the token stored in the
   * authoritative workflow snapshot. Mastra's `createRun()` first persists a
   * tokenless pending shell; if no executed snapshot replaced it, the shell is
   * abandoned and must not become a successful FlowSafe run.
   */
  async recoverStartAttempt(
    workflowId: string,
    runId: string,
    attemptToken: string,
  ): Promise<RunSummary | null> {
    if (!isPathSafeId(attemptToken)) {
      throw new InvalidRunRequestError('attemptToken is malformed');
    }
    const workflow = this.#getWorkflow(workflowId);
    return this.#withRunLock(workflowId, runId, async () => {
      const state = await this.#workflowState(workflow, runId);
      if (!state) return null;
      const provenance = runProvenance(state);
      if (provenance?.startToken === attemptToken) {
        return summarizeState(
          runId,
          state,
          new Map(provenance.resumeCounts),
          provenance.requestedBy,
          provenance.requestedByKind,
        );
      }
      if (state.status === 'pending' && provenance === undefined) {
        await workflow.deleteWorkflowRunById(runId);
        return null;
      }
      throw new Error(
        `run '${runId}' snapshot belongs to another start attempt`,
      );
    });
  }

  // A provider crash propagates (fail loud): silently starting the leg with
  // fewer capabilities than intended would mask the fault. Missing grants can
  // only ever deny downstream (fail closed), so loud propagation is safe.
  //
  // Every leg — provider or not — carries a base context minting the
  // workflow-scope key (breakwater's crossWorkflowIsolation reads it): the
  // runtime is the trusted authority for "which workflow is executing", so
  // the scope is never client-suppliable. Provider values are partitioned into
  // stored application context, capabilities, and trusted identity so every
  // layer has an explicit order and a provider cannot replace runtime scope.
  async #requestContextFor(
    workflowId: string,
    runId: string,
    leg: RunLeg,
    provenance?: RunProvenance,
    storedRequestContext?: Record<string, unknown>,
  ): Promise<RequestContext> {
    const base: Record<string, unknown> = {
      [BREAKWATER_WORKFLOW_SCOPE_KEY]: workflowId,
      runId,
    };
    if (provenance !== undefined) {
      base[RUN_PROVENANCE_CONTEXT_KEY] = provenance;
    }
    // No isolation scope is minted here: a deployment serves exactly one
    // organization, so breakwater's connector idempotency and rate-limit keys
    // are deployment-wide by construction. Budget partitioning within a
    // deployment stays available
    // through breakwater's crossWorkflowIsolation, which reads the workflow
    // scope minted above. The isolation-scope context key remains RESERVED —
    // orderedRequestContext drops it from provider values — so a provider can
    // never mint a scope that desyncs from the execution identity below.
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
    selectedStep: string | string[] | undefined,
    requestedBy?: string,
    requestedByKind?: ExecutionPrincipalKind,
  ): Promise<{
    nextCounts: ReadonlyMap<string, number>;
    provenance: RunProvenance;
    requestContext: RequestContext;
  }> {
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
    const nextCounts = new Map(priorCounts);
    if (stepKey !== undefined) {
      nextCounts.set(stepKey, (nextCounts.get(stepKey) ?? 0) + 1);
    }
    const provenance: RunProvenance = {
      version: 1,
      ...(requester === undefined
        ? {}
        : { requestedBy: requester, requestedByKind: requesterKind }),
      startToken: storedProvenance?.startToken ?? crypto.randomUUID(),
      attemptToken: crypto.randomUUID(),
      resumeCounts: [...nextCounts],
    };
    const requestContext = await this.#requestContextFor(
      workflowId,
      runId,
      {
        kind: 'resume',
        step,
        suspendedAt:
          stepKey !== undefined
            ? suspendedAtOf(state.steps, stepKey)
            : undefined,
        resumeCount:
          stepKey !== undefined ? priorCounts?.get(stepKey) : undefined,
      },
      provenance,
    );
    return {
      nextCounts,
      provenance,
      requestContext,
    };
  }

  #workflowState(
    workflow: AnyWorkflow,
    runId: string,
  ): Promise<WorkflowState | null> {
    return workflow.getWorkflowRunById(runId, { fields: RUN_STATE_FIELDS });
  }

  async #summaryForAttempt(
    workflow: AnyWorkflow,
    runId: string,
    attemptToken: string,
  ): Promise<RunSummary | undefined> {
    const persisted = await this.#workflowState(workflow, runId);
    if (!persisted) return undefined;
    const provenance = runProvenance(persisted);
    if (provenance?.attemptToken !== attemptToken) return undefined;
    return summarizeState(
      runId,
      persisted,
      new Map(provenance.resumeCounts),
      provenance.requestedBy,
      provenance.requestedByKind,
    );
  }

  async #reconcileTerminalState(
    workflowId: string,
    runId: string,
    result: CoreRunResult,
    requestContext: RequestContext,
  ): Promise<void> {
    const opts = terminalStateUpdate(result);
    if (!opts) return;
    const workflows = await this.#storage.getStore('workflows');
    if (!workflows) {
      throw new Error(
        'RunnerRuntime: workflows storage is unavailable while persisting terminal state',
      );
    }
    const snapshot = await workflows.loadWorkflowSnapshot({
      workflowName: workflowId,
      runId,
    });
    if (!snapshot) {
      throw new Error(
        `RunnerRuntime: run '${runId}' of workflow '${workflowId}' completed without a durable snapshot`,
      );
    }
    const persistedContext =
      snapshot.requestContext !== null &&
      typeof snapshot.requestContext === 'object' &&
      !Array.isArray(snapshot.requestContext)
        ? snapshot.requestContext
        : {};
    // Core merges resume context over the prior snapshot. Terminal-only repair
    // must persist that same effective context, including application keys the
    // current provider intentionally omitted.
    const authoritativeContext = {
      ...persistedContext,
      ...Object.fromEntries(requestContext.entries()),
    };
    const persistedToken = runProvenance(snapshot)?.attemptToken;
    const authoritativeToken = runProvenance({
      ...snapshot,
      requestContext: authoritativeContext,
    })?.attemptToken;
    if (
      snapshot.status === result.status &&
      persistedToken === authoritativeToken
    ) {
      return;
    }
    await workflows.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId,
      snapshot: {
        ...snapshot,
        ...opts,
        requestContext: authoritativeContext,
        timestamp: Date.now(),
      },
    });
  }

  #getWorkflow(workflowId: string): AnyWorkflow {
    this.#ensureMastra();
    const workflow = this.#workflows.get(workflowId);
    if (!workflow) throw new UnknownWorkflowError(workflowId);
    return workflow;
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
  // per-run map keyed identically, so a future map cannot reintroduce a
  // runId-only key and cross workflow boundaries.
  // This is the exact string the DO name join produces
  // (idFromName(`${workflowId}:${runId}`)); PATH_SAFE_ID_PATTERN excludes ':'
  // from both ids, so the join is unambiguous.
  #runKey(workflowId: string, runId: string): string {
    return `${workflowId}:${runId}`;
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
    const key = this.#runKey(workflowId, runId);
    const prev = this.#runLocks.get(key) ?? Promise.resolve();
    const task = prev.then(fn);
    const tail = task.then(
      () => undefined,
      () => undefined,
    );
    this.#runLocks.set(key, tail);
    void tail.then(() => {
      if (this.#runLocks.get(key) === tail) this.#runLocks.delete(key);
    });
    return task;
  }
}
