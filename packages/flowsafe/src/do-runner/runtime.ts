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
import type { MastraCompositeStore } from '@mastra/core/storage';
import type {
  AnyWorkflow,
  WorkflowRunStatus,
  WorkflowState,
} from '@mastra/core/workflows';

import {
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from './breakwater-keys.js';
import { PATH_SAFE_ID_PATTERN, tenantOfRunId } from './path-safe-id.js';
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
      const suspendedAt = suspendedAtByStep(result.steps, suspendedKeys);
      if (suspendedAt !== undefined) summary.suspendedAt = suspendedAt;
      const resumedAt = resumedAtByStep(result.steps, suspendedKeys);
      if (resumedAt !== undefined) summary.resumedAt = resumedAt;
      // resumeCount is runtime-owned (the ledger), NOT read from the snapshot.
      const resumeCount = resumeCountByStep(counts, suspendedKeys);
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
function latestAttempt(steps: WorkflowState['steps'], stepKey: string) {
  const entry = steps?.[stepKey];
  return Array.isArray(entry) ? entry[entry.length - 1] : entry;
}

/** Epoch-ms suspension time of the step's latest attempt, if recorded. */
function suspendedAtOf(
  steps: WorkflowState['steps'],
  stepKey: string,
): number | undefined {
  return latestAttempt(steps, stepKey)?.suspendedAt;
}

/** RunSummary.suspendedAt: dot-joined step key -> epoch-ms suspension time. */
function suspendedAtByStep(
  steps: WorkflowState['steps'] | undefined,
  suspendedKeys: readonly string[],
): Record<string, number> | undefined {
  if (!steps) return undefined;
  const map: Record<string, number> = {};
  for (const key of suspendedKeys) {
    const at = suspendedAtOf(steps, key);
    if (at !== undefined) map[key] = at;
  }
  return Object.keys(map).length > 0 ? map : undefined;
}

/**
 * Epoch-ms resume time of the step's latest attempt, if recorded. Feeds the
 * INFORMATIONAL RunSummary.resumedAt only — Mastra stamps it solely on a
 * payload-bearing resume, so it is unreliable as a first-vs-re-suspension
 * signal. The grant binding uses `resumeCount` (the runtime ledger) instead.
 */
function resumedAtOf(
  steps: WorkflowState['steps'],
  stepKey: string,
): number | undefined {
  return latestAttempt(steps, stepKey)?.resumedAt;
}

/** RunSummary.resumedAt: dot-joined step key -> epoch-ms resume time. */
function resumedAtByStep(
  steps: WorkflowState['steps'] | undefined,
  suspendedKeys: readonly string[],
): Record<string, number> | undefined {
  if (!steps) return undefined;
  const map: Record<string, number> = {};
  for (const key of suspendedKeys) {
    const at = resumedAtOf(steps, key);
    if (at !== undefined) map[key] = at;
  }
  return Object.keys(map).length > 0 ? map : undefined;
}

/**
 * RunSummary.resumeCount: dot-joined step key -> runtime resume ordinal.
 * Sourced from the runtime's per-run resume ledger (NOT the Mastra snapshot),
 * so it is present for every re-suspension regardless of resume payload —
 * the collision-free grant-binding tie-breaker. Absent for a step with no
 * ledger entry (a first suspension).
 */
function resumeCountByStep(
  counts: ReadonlyMap<string, number> | undefined,
  suspendedKeys: readonly string[],
): Record<string, number> | undefined {
  if (!counts) return undefined;
  const map: Record<string, number> = {};
  for (const key of suspendedKeys) {
    const count = counts.get(key);
    if (count !== undefined) map[key] = count;
  }
  return Object.keys(map).length > 0 ? map : undefined;
}

function suspendPayloadByStep(
  steps: WorkflowState['steps'],
  suspendedKeys: string[],
): Record<string, unknown> | undefined {
  if (!steps) return undefined;
  const payloads: Record<string, unknown> = {};
  for (const key of suspendedKeys) {
    const latest = latestAttempt(steps, key);
    if (latest && latest.suspendPayload !== undefined) {
      payloads[key] = latest.suspendPayload;
    }
  }
  return Object.keys(payloads).length > 0 ? payloads : undefined;
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
    const suspendPayload = suspendPayloadByStep(state.steps, suspendedKeys);
    if (suspendPayload !== undefined) {
      summary.suspendPayload = suspendPayload;
    }
    const suspendedAt = suspendedAtByStep(state.steps, suspendedKeys);
    if (suspendedAt !== undefined) summary.suspendedAt = suspendedAt;
    const resumedAt = resumedAtByStep(state.steps, suspendedKeys);
    if (resumedAt !== undefined) summary.resumedAt = resumedAt;
    const resumeCount = resumeCountByStep(counts, suspendedKeys);
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
 * ('breakwater.approvedConnectors') — can only enter a run through this
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
}

export interface StartRunOptions {
  /**
   * REQUIRED — the runtime never generates a runId (INV-1). Multi-tenant
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
  #mastra?: Mastra;

  constructor(options: RunnerRuntimeOptions) {
    this.#storage = options.storage;
    this.#logger = options.logger ?? false;
    this.#requestContextForRun = options.requestContextForRun;
    this.#resumeLedgerExplicit = options.resumeLedger !== undefined;
    this.#resumeLedger = options.resumeLedger ?? new InMemoryResumeLedger();
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
      const run = await workflow.createRun({ runId });
      let result: CoreRunResult;
      try {
        result = await run.start({
          inputData: options.inputData,
          requestContext,
        });
      } catch (error) {
        throw asClientError(error) ?? error;
      }
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
      const step = resolveResumeStep(options.step, state);
      const stepKey = step?.join('.');
      // resumeCount is read BEFORE this resume increments it: it is the count
      // of prior resumes = the ordinal of the CURRENT suspension being resumed
      // (undefined for a first suspension), which the minting approval captured
      // at that suspension. suspendedAt still comes from the snapshot.
      const priorCounts = await this.#resumeLedger.counts(runKey);
      const requestContext = await this.#requestContextFor(workflowId, runId, {
        kind: 'resume',
        step,
        suspendedAt:
          stepKey !== undefined
            ? suspendedAtOf(state.steps, stepKey)
            : undefined,
        resumeCount:
          stepKey !== undefined ? priorCounts?.get(stepKey) : undefined,
      });
      const run = await workflow.createRun({ runId });
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
      let counts = priorCounts;
      if (stepKey !== undefined) {
        counts = await this.#resumeLedger.increment(runKey, stepKey);
      }
      const summary = summarize(run.runId, result, counts);
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
    // Project resumeCount from the ledger too (defense in depth: a bridge that
    // ever mints off status() must not see a stale/absent ordinal).
    const counts = await this.#resumeLedger.counts(
      this.#runKey(workflowId, runId),
    );
    return summarizeState(runId, state, counts);
  }

  // A provider crash propagates (fail loud): silently starting the leg with
  // fewer capabilities than intended would mask the fault. Missing grants can
  // only ever deny downstream (fail closed), so loud propagation is safe.
  //
  // Every leg — provider or not — carries a base context minting the
  // workflow-scope key (breakwater's crossWorkflowIsolation reads it): the
  // runtime is the trusted authority for "which workflow is executing", so
  // the scope is never client-suppliable. Provider values merge OVER the
  // base, so a provider can override the scope deliberately.
  async #requestContextFor(
    workflowId: string,
    runId: string,
    leg: RunLeg,
  ): Promise<RequestContext> {
    const base: Record<string, unknown> = {
      [BREAKWATER_WORKFLOW_SCOPE_KEY]: workflowId,
    };
    // Tenant-salted runs (INV-1: `${tenantId}_${uuid}`) also mint the OPAQUE
    // isolation scope, segmenting breakwater's connector idempotency and
    // rate-limit keys per tenant. Server-authoritative like the workflow
    // scope above — the runId was minted from the AUTHENTICATED tenant, so
    // its prefix is trustworthy here.
    //
    // Structural, not flagged: a runId shaped `<inv3-slug>_<rest>` mints a
    // scope. For this repo's hosts that is exactly the tenant (they all mint
    // via TenantContext.newRunId); a standalone OSS consumer whose custom
    // runIds happen to look like `batch_042` gets incidental — and harmless —
    // key segmentation, never a leak (there is no tenant boundary there).
    // Non-matching runIds mint nothing: the single-tenant keys stay
    // byte-identical, and deployments that must never run scope-less enforce
    // that with breakwater's tenantIsolation evaluator (which binds dry-run
    // too, unlike the idempotency path).
    const tenantId = tenantOfRunId(runId);
    if (tenantId !== undefined) {
      base[BREAKWATER_ISOLATION_SCOPE_KEY] = tenantId;
    }
    const values = this.#requestContextForRun
      ? await this.#requestContextForRun(workflowId, runId, leg)
      : undefined;
    return new RequestContext(Object.entries({ ...base, ...values }));
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
