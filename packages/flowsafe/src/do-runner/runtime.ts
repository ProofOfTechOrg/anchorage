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
} from '@mastra/core/workflows';

import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from './breakwater-keys.js';
import { PATH_SAFE_ID_PATTERN, tenantOfRunId } from './path-safe-id.js';
import type { HostPubSub } from './pubsub.js';
import { InMemoryResumeLedger, type ResumeLedger } from './resume-ledger.js';

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
): RunSummary {
  switch (result.status) {
    case 'success':
      return { runId, status: result.status, result: result.result };
    case 'failed':
      return {
        runId,
        status: result.status,
        error: errorText(result.error),
      };
    case 'suspended': {
      const summary: RunSummary = {
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
      // resumeCount is runtime-owned (the ledger), NOT read from the snapshot.
      const resumeCount = byStep(suspendedKeys, (key) => counts?.get(key));
      if (resumeCount !== undefined) summary.resumeCount = resumeCount;
      return summary;
    }
    case 'tripwire':
      return { runId, status: result.status, error: result.tripwire.reason };
    default:
      return { runId, status: result.status };
  }
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
 * signal. The grant binding uses `resumeCount` (the runtime ledger) instead.
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
 * resume-ledger.ts stores [stepKey, count] pairs to avoid.
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
): RunSummary {
  const summary: RunSummary = {
    runId,
    status: state.status,
    createdAt: toIso(state.createdAt),
    updatedAt: toIso(state.updatedAt),
  };
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
 * Merge semantics (pinned by test against core 1.49.0): the context provided
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
): RequestContext {
  const stored: Array<[string, unknown]> = [];
  const capabilities: Array<[string, unknown]> = [];
  const identity: Array<[string, unknown]> = [];
  for (const entry of Object.entries(provided ?? {})) {
    const [key] = entry;
    if (
      key === BREAKWATER_WORKFLOW_SCOPE_KEY ||
      key === BREAKWATER_ISOLATION_SCOPE_KEY ||
      key === BREAKWATER_CONNECTOR_EXECUTION_KEY ||
      key === 'runId'
    ) {
      continue;
    }
    if (TRUSTED_IDENTITY_CONTEXT_KEYS.has(key)) {
      identity.push(entry);
    } else if (
      key === BREAKWATER_CONNECTOR_GRANTS_KEY ||
      key.startsWith('breakwater.') ||
      key === 'mastra:goal'
    ) {
      capabilities.push(entry);
    } else {
      stored.push(entry);
    }
  }
  return new RequestContext([
    ...stored,
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
   * Per-run resume ledger backing RunSummary.resumeCount / RunLeg.resumeCount.
   * Default: in-memory. The DO shell adopts a ctx.storage-backed ledger via
   * adoptDefaultResumeLedger() so the ordinal survives eviction — an explicit
   * ledger here wins over that adoption.
   */
  resumeLedger?: ResumeLedger;
  /**
   * The host Durable Object's single pubsub identity from do-runner/pubsub.ts, threaded
   * here by init() alongside storage and the ledger so a host that configures it
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

export interface StartRunOptions {
  /**
   * Required: the runtime never generates a runId. Multi-tenant
   * hosts mint `${tenantId}_${uuid}` server-side (createRunRouter) so the
   * runId carries its tenant everywhere it becomes a key (D1 snapshot row,
   * DO name, R2 segment, grant-list predicate). A generation fallback here
   * would let any caller that forgets to mint create a bare, tenant-less
   * run — unreachable by tenant purge and un-ownable by every actor.
   */
  runId: string;
  inputData?: unknown;
}

export interface ResumeRunOptions {
  /** Suspended step id (or nested path). Optional when only one step is suspended. */
  step?: string | string[];
  resumeData?: unknown;
}

export class RunnerRuntime {
  readonly #storage: MastraCompositeStore;
  readonly #logger: IMastraLogger | false;
  readonly #requestContextForRun?: RequestContextProvider;
  readonly #workflows = new Map<string, AnyWorkflow>();
  readonly #runLocks = new Map<string, Promise<unknown>>();
  // Per-run resume ledger: #runKey(workflowId, runId) -> (stepKey -> times that
  // step has been resumed). Keyed by the run's FULL identity, never runId alone
  // (see #runKey): a shared caller runId under two workflows are distinct runs.
  // The runtime increments it on every resume (regardless of
  // payload) and projects it as RunSummary.resumeCount / RunLeg.resumeCount —
  // the collision-free grant-binding tie-breaker that Mastra's
  // payload-conditional resumedAt cannot provide.
  //
  // Durability: a lost ledger is fail-closed for grants (leg.resumeCount
  // undefined vs an approval's captured ordinal -> mismatch -> deny, see
  // grants.ts) — never a leak, but an AVAILABILITY bug: the approved action
  // silently no-ops. DO eviction (~70-140s idle), hibernation, and code
  // deploys all wipe in-memory state while ctx.storage survives, so the DO
  // shell adopts a DurableStorageResumeLedger (durable-object.ts); the
  // in-memory default covers node tests and non-DO hosts. Entries are
  // deleted on terminal status below; a resumed-then-abandoned suspended run
  // keeps one small entry until the run is purged.
  #resumeLedger: ResumeLedger;
  readonly #resumeLedgerExplicit: boolean;
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
    this.#resumeLedgerExplicit = options.resumeLedger !== undefined;
    this.#resumeLedger = options.resumeLedger ?? new InMemoryResumeLedger();
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
   * Host-shell seam: replace the DEFAULT in-memory ledger with a durable one.
   * The DO shell calls this with a ctx.storage-backed ledger right after
   * build(), so every DO host gets eviction-proof ordinals without threading
   * a parameter (a forgettable thread is how the durability guarantee rots).
   * A ledger explicitly injected via options wins; adoption is rejected once
   * runs may have consulted the ledger.
   */
  adoptDefaultResumeLedger(ledger: ResumeLedger): void {
    if (this.#mastra) {
      throw new Error(
        'RunnerRuntime: adopt the resume ledger before the first run — ordinals already read from the default ledger would be lost',
      );
    }
    if (this.#resumeLedgerExplicit) return;
    this.#resumeLedger = ledger;
  }

  register(workflow: AnyWorkflow): void {
    if (this.#mastra) {
      throw new Error(
        'RunnerRuntime: register all workflows before the first run — the Mastra instance is frozen once runs start',
      );
    }
    if (!PATH_SAFE_ID_PATTERN.test(workflow.id)) {
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
    if (
      typeof options.runId !== 'string' ||
      !PATH_SAFE_ID_PATTERN.test(options.runId)
    ) {
      throw new InvalidRunRequestError(
        "runId is required and must be URL-path-safe (letters, digits, '.', '_', '~', '-'; 1–200 chars)",
      );
    }
    const runId = options.runId;
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
      const requestContext = await this.#requestContextFor(workflowId, runId, {
        kind: 'start',
      });
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
          requestContext,
        });
      } catch (error) {
        throw asClientError(error) ?? error;
      }
      await this.#reconcileTerminalState(workflowId, run.runId, result);
      return summarize(run.runId, result);
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
      const runKey = this.#runKey(workflowId, runId);
      const state = await workflow.getWorkflowRunById(runId);
      if (!state) throw new UnknownRunError(workflowId, runId);
      if (state.status !== 'suspended') {
        throw new RunNotSuspendedError(workflowId, runId, state.status);
      }
      // Provider before createRun for symmetry with start(): a resume-time
      // createRun only reattaches (no snapshot write), but failing before it
      // still does the least work and keeps the ordering invariant uniform.
      const { stepKey, priorCounts, requestContext } =
        await this.#trustedResumePreparation(
          workflowId,
          runId,
          state,
          options.step,
        );
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
        // The run stayed suspended (resume threw) — do NOT increment; the
        // prior count must persist for a later retry of this same suspension.
        throw asClientError(error) ?? error;
      }
      // Record THIS resume AFTER the leg fingerprint read above and BEFORE
      // summarize: a re-suspension produced by this resume must capture the
      // incremented ordinal so its approval binds to the new suspension.
      // priorCounts rides along so a durable ledger need not re-read what the
      // per-run FIFO lock guarantees unchanged since the read above.
      let counts = priorCounts;
      if (stepKey !== undefined) {
        counts = await this.#resumeLedger.increment(
          runKey,
          stepKey,
          priorCounts ?? new Map(),
        );
      }
      const summary = summarize(run.runId, result, counts);
      await this.#reconcileTerminalState(workflowId, run.runId, result);
      // Terminal run: drop the ledger (no further suspension can occur).
      if (summary.status !== 'suspended') {
        await this.#resumeLedger.delete(runKey);
      }
      return summary;
    });
  }

  async status(workflowId: string, runId: string): Promise<RunSummary | null> {
    const workflow = this.#getWorkflow(workflowId);
    const state = await workflow.getWorkflowRunById(runId);
    if (!state) return null;
    // Ledger read only for SUSPENDED runs — the one branch that projects
    // resumeCount (defense in depth: a bridge that ever mints off status()
    // must not see a stale/absent ordinal). Every other status would discard
    // the result, and under the DO shell each read is a billed
    // ctx.storage.get the SPA's polling would pay on every terminal run.
    const counts =
      state.status === 'suspended'
        ? await this.#resumeLedger.counts(this.#runKey(workflowId, runId))
        : undefined;
    return summarizeState(runId, state, counts);
  }

  /**
   * Derive the exact trusted context the next resume leg will receive without
   * advancing the run. Durable-agent hosts use it while rehydrating the
   * in-process registry, then call resume(); resume independently re-derives
   * the same suspension fingerprint before execution.
   */
  async trustedRequestContextForResume(
    workflowId: string,
    runId: string,
    options: Pick<ResumeRunOptions, 'step'> = {},
  ): Promise<RequestContext> {
    const workflow = this.#getWorkflow(workflowId);
    const state = await workflow.getWorkflowRunById(runId);
    if (!state) throw new UnknownRunError(workflowId, runId);
    if (state.status !== 'suspended') {
      throw new RunNotSuspendedError(workflowId, runId, state.status);
    }
    return (
      await this.#trustedResumePreparation(
        workflowId,
        runId,
        state,
        options.step,
      )
    ).requestContext;
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
  ): Promise<RequestContext> {
    const base: Record<string, unknown> = {
      [BREAKWATER_WORKFLOW_SCOPE_KEY]: workflowId,
      runId,
    };
    // Tenant-salted runs (INV-1: `${tenantId}_${uuid}`) also mint the OPAQUE
    // isolation scope, segmenting breakwater's connector idempotency and
    // rate-limit keys per tenant. Server-authoritative like the workflow
    // scope above — the runId was minted from the AUTHENTICATED tenant, so
    // its prefix is trustworthy here.
    //
    // Structural, not flagged: a runId shaped `<inv3-slug>_<rest>` mints a
    // scope. For this repo's hosts that is exactly the tenant (they all mint
    // via TenantContext.newRunId). CONSTRAINT for standalone OSS consumers
    // minting their own runIds: the scope segments breakwater's CROSS-RUN
    // idempotency and rate-limit keys, so one logical account must keep ONE
    // stable pre-`_` prefix (or avoid `_`-prefixed runIds entirely) — ids
    // like `daily_<uuid>` vs `weekly_<uuid>` would split that account's key
    // namespace per prefix, and a cross-run business key ("invoice-2026-07")
    // would fire once per prefix instead of once. Never a cross-tenant leak
    // (splitting only narrows sharing), but a real duplication hazard.
    // Non-matching runIds mint nothing: the single-tenant keys stay
    // byte-identical, and deployments that must never run scope-less enforce
    // that with breakwater's tenantIsolation evaluator (which binds dry-run
    // too, unlike the idempotency path).
    const tenantId = tenantOfRunId(runId);
    if (tenantId !== undefined) {
      base[BREAKWATER_ISOLATION_SCOPE_KEY] = tenantId;
    }
    if (leg.kind === 'start') {
      base[BREAKWATER_CONNECTOR_EXECUTION_KEY] = {
        kind: 'start',
        workflowId,
        runId,
        ...(tenantId === undefined ? {} : { isolationScope: tenantId }),
      };
    } else if (leg.step !== undefined && leg.suspendedAt !== undefined) {
      base[BREAKWATER_CONNECTOR_EXECUTION_KEY] = {
        kind: 'resume',
        workflowId,
        runId,
        ...(tenantId === undefined ? {} : { isolationScope: tenantId }),
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
    return orderedRequestContext(base, values);
  }

  async #trustedResumePreparation(
    workflowId: string,
    runId: string,
    state: WorkflowState,
    selectedStep: string | string[] | undefined,
  ): Promise<{
    stepKey: string | undefined;
    priorCounts: ReadonlyMap<string, number> | undefined;
    requestContext: RequestContext;
  }> {
    const step = resolveResumeStep(selectedStep, state);
    const stepKey = step?.join('.');
    // resumeCount is read BEFORE this resume increments it: it is the count
    // of prior resumes = the ordinal of the CURRENT suspension being resumed
    // (undefined for a first suspension), which the minting approval captured.
    const priorCounts = await this.#resumeLedger.counts(
      this.#runKey(workflowId, runId),
    );
    const requestContext = await this.#requestContextFor(workflowId, runId, {
      kind: 'resume',
      step,
      suspendedAt:
        stepKey !== undefined ? suspendedAtOf(state.steps, stepKey) : undefined,
      resumeCount:
        stepKey !== undefined ? priorCounts?.get(stepKey) : undefined,
    });
    return { stepKey, priorCounts, requestContext };
  }

  async #reconcileTerminalState(
    workflowId: string,
    runId: string,
    result: CoreRunResult,
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
    if (snapshot.status === result.status) return;
    await workflows.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId,
      snapshot: {
        ...snapshot,
        ...opts,
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
      workflows: Object.fromEntries(this.#workflows),
      storage: this.#storage,
      logger: this.#logger,
    });
  }

  // The run's full identity as a single map key: workflowId + runId, never runId
  // alone. The same caller-supplied runId under two workflows are DISTINCT
  // persisted runs (Mastra snapshots key on workflowName+runId) and must never
  // share a per-run entry — the FIFO lock OR the resume ledger. Composing the key
  // in ONE place keeps every per-run map keyed identically, so a future per-run
  // map cannot reintroduce a runId-only key (the grant leak the ledger guards).
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
