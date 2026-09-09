// SPDX-License-Identifier: Apache-2.0
// Classic fetch-contract Durable Object (constructor(state, env) + fetch).
// Deliberately NOT `extends DurableObject` from 'cloudflare:workers': the
// classic contract needs no workers-only runtime import, so this module and
// everything under it load in node/vitest. Run state lives in D1, not DO
// storage — that is what lets a run started before a restart resume after
// one. Serialization of execution and lifecycle mutations on a single run is
// enforced by RunnerRuntime's per-run FIFO locks; routing one DO instance per run
// (idFromName(`${workflowId}:${runId}`)) makes those locks authoritative,
// since all traffic for a run lands on one instance. The object's alarm is
// reserved for recovering run-owner claims that outlive an interrupted start
// and for the per-suspension deadlines of this run (suspension-deadline.ts);
// both duties share one wake, armed at the earliest of the two due times.

import {
  decodeExecutionPrincipal,
  type ExecutionPrincipal,
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import type { DurableObjectRunnerState, WebSocketLike } from './cf-types.js';
import { newWebSocketPair, safeSend } from './cf-types.js';
import {
  isDatabaseBinding,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from './deployment-identity.js';
import { DoStatusError, doErrorResponse } from './do-error-response.js';
import {
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  isRunStartPendingError,
  MUTATION_EPOCH_HEADER,
  mutationEpochFromHeader,
  normalizeD1RunExecutionIdentity,
  normalizeRunExecutionIdentity,
  normalizeStartExecutionIdentity,
  normalizeStartIdentity,
  type RunExecutionIdentity,
  RunStartPendingError,
} from './execution-admission.js';
import {
  admitsRunStart,
  ExecutionFencedError,
  type ExecutionFenceReading,
  isExecutionFenceRefusal,
  readExecutionFence,
} from './execution-fence.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import { isDefinitiveInitialAdmissionRefusal } from './initial-admission-refusal.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  lifecycleFromRequestContext,
  terminalCleanupFor,
} from './run-lifecycle.js';
import { isTerminalRunStatus } from './run-terminal-state.js';
import {
  type AuthoritativeStartState,
  InvalidRunRequestError,
  type RecoveredStart,
  RunAlreadyExistsError,
  type RunLifecycleCas,
  type RunLifecycleTransitionResult,
  RunnerRuntime,
  RunStateUnreadableError,
  type RunSummary,
  UnknownRunError,
} from './runtime.js';
import {
  resolveScheduleStartOwner,
  type ScheduleSourceStore,
  type ScheduleSourceWorkflowTarget,
} from './schedule-source.js';
import {
  captureReservation,
  type StartReservationReading,
  sameReservationIdentity,
} from './start-reservation-contract.js';
import {
  dueSuspensionDeadline,
  isReadableRunSummary,
  isSuspensionDeadlineDue,
  MAX_SUSPENSION_DEADLINE_ATTEMPTS,
  MAX_SUSPENSION_DEADLINES_PER_RUN,
  mergeSuspensionDeadlines,
  nextSuspensionDeadlineAt,
  parseSuspensionDeadlineRecord,
  type RejectedSuspensionDeadline,
  SUSPENSION_DEADLINE_PRINCIPAL_ID,
  SUSPENSION_DEADLINE_STORAGE_KEY,
  SUSPENSION_TIMEOUT_RESUME_KEY,
  type SuspensionDeadlineEntry,
  type SuspensionDeadlineRecord,
  suspensionDeadlinesOf,
  suspensionTimeoutResumeData,
  tombstoned,
} from './suspension-deadline.js';

export interface DurableObjectRunOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

export interface DurableObjectRunOwnershipStore {
  owner(
    kind: 'run' | 'schedule' | 'thread' | 'resource',
    resourceId: string,
  ): Promise<DurableObjectRunOwner | undefined>;
  reserveAll(
    claims: readonly { kind: 'run'; resourceId: string }[],
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<boolean>;
  settleReservation(
    token: string,
    release: readonly { kind: 'run'; resourceId: string }[],
  ): Promise<void>;
  release?(
    kind: 'run',
    resourceId: string,
    owner: DurableObjectRunOwner,
  ): Promise<boolean>;
}

export interface DurableObjectRunLifecycleHooks {
  abandonApprovals(
    workflowId: string,
    runId: string,
    status: 'cancelled' | 'timed_out',
  ): Promise<void>;
  discardScheduleDispatch?(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<void>;
}

const RUN_OWNER_RECOVERY_KEY = 'flowsafe:run-owner-recovery:v1';
export const RUN_OWNER_RECOVERY_DELAY_MS = 60_000;
const SUSPENSION_DEADLINE_RETRY_MS = 60_000;
// How long a run's state may stay unreadable before the entries due under it
// are given up on. A read that never succeeds is never charged, so without this
// bound one run whose workflow registration a deploy dropped for good would
// keep a 60 s heartbeat forever. A day is four orders of magnitude past any
// credible storage incident, so nothing but a permanent fault reaches it.
//
// The bound is per DUE BATCH, not per entry: one failed read stamps every entry
// due at that wake, so a record of any size clears about a day after its last
// entry falls due. An entry armed far in the future starts its own day only
// when it comes due — until then it is not this wake's business and is left
// untouched.
const SUSPENSION_DEADLINE_UNREADABLE_LIMIT_MS = 86_400_000;
// Cloudflare runs an alarm scheduled in the past immediately. An entry that is
// already due therefore cannot be armed at its own deadline: the wake would
// re-arm at the same past time and spin. Every arm is floored this far out, so
// a wake that fails to consume its entry costs one wake per second at worst
// until the retry ledger backs it off or drops it. The ledger is what ends
// that, so the floor only bounds a wake that CHARGES one. The three classes
// that never do converge nothing and keep the 60 s recovery cadence instead: a
// wake whose authoritative read did not succeed (uncharged by design — it is
// no evidence about the entry), a wake that could not BUILD its runtime (a
// misconfigured binding is a fault of the host, and says nothing about any
// entry either), and a wake that could not read its record or write its ledger
// at all. The first has one exception: a read that has not succeeded for a day
// abandons the entries due under it, and abandoning IS a convergence — that
// wake arms from what is left, which for a record holding nothing else is the
// delete that ends the heartbeat. Otherwise an unrelated entry's wake is
// deferred to the watchdog — up to a minute late, and only while the fault
// lasts.
const SUSPENSION_DEADLINE_ARM_FLOOR_MS = 1_000;

type RunOwnerRecovery = {
  version: 2;
  workflowId: string;
  runId: string;
  token: string;
  owner: DurableObjectRunOwner;
  startReservation?: StartReservationReading;
} & (
  | { phase: 'preparing'; execution?: never }
  | { phase: 'prepared'; execution: D1RunExecutionIdentity }
  | { phase: 'prepared-unfenced'; execution: RunExecutionIdentity }
);

interface RunStartFrame {
  token: string;
  unwound: boolean;
}

function recoveryObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Error('stored run owner recovery is malformed');
  const copy: Record<string, unknown> = Object.create(null);
  for (const [key, descriptor] of Object.entries(
    Object.getOwnPropertyDescriptors(value),
  )) {
    if (!('value' in descriptor))
      throw new Error('stored run owner recovery is malformed');
    copy[key] = descriptor.value;
  }
  return copy;
}

function sameRunRecovery(
  actual: RunOwnerRecovery,
  expected: RunOwnerRecovery,
): boolean {
  const claim = actual.startReservation;
  const wantedClaim = expected.startReservation;
  return (
    actual.version === expected.version &&
    actual.phase === expected.phase &&
    actual.token === expected.token &&
    actual.workflowId === expected.workflowId &&
    actual.runId === expected.runId &&
    actual.owner.kind === expected.owner.kind &&
    actual.owner.id === expected.owner.id &&
    actual.execution?.tablePrefix === expected.execution?.tablePrefix &&
    actual.execution?.workflowId === expected.execution?.workflowId &&
    actual.execution?.runId === expected.execution?.runId &&
    actual.execution?.startToken === expected.execution?.startToken &&
    (claim === undefined
      ? wantedClaim === undefined
      : wantedClaim !== undefined &&
        sameReservationIdentity(claim, wantedClaim) &&
        claim.state === wantedClaim.state &&
        claim.updatedAt === wantedClaim.updatedAt &&
        claim.binding.kind === wantedClaim.binding.kind)
  );
}

interface StartBody {
  startReservation?: unknown;
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
  initialState?: unknown;
  requestContext?: Record<string, unknown>;
  scheduleId?: unknown;
  dispatchId?: unknown;
  deadlineMs?: unknown;
  /**
   * The start's idempotency key, arriving on the INTERNAL Worker-to-DO channel
   * only. Every request that reaches this route carries the deployment-identity
   * header (a tenant request bearing one is refused before routing), so a key
   * here is one the trusted run router reserved — which is what lets the
   * proof-only fence match it. It is deliberately not an open request-context
   * key and never a field of the public POST /runs body.
   * @internal
   */
  idempotencyKey?: unknown;
}

interface ResumeBody {
  step?: string | string[];
  resumeData?: unknown;
  requestedBy?: unknown;
  requestedByKind?: unknown;
  deadlineMs?: unknown;
}

interface DeadlineBody {
  expectedRevision?: unknown;
  expectedDeadlineAt?: unknown;
}

class DurableObjectRunIdentityError extends DoStatusError {
  readonly status = 403;
}

/**
 * Does this env carry a database to fence against?
 *
 * `isDatabaseBinding` is deployment-identity's, imported rather than restated:
 * it is the SAME question about the SAME binding (an RPC binding — a service
 * binding with a named entrypoint, a Durable Object stub — is a proxy that
 * answers every property with a callable, so `fetch` is what separates that
 * family from a D1Database). Two copies would be two places for that
 * discrimination to be revised, and a deployment where they disagreed would
 * pass the identity check and skip the fence assert, or the reverse.
 */
function hasDatabaseBinding(env: unknown): boolean {
  return isDatabaseBinding((env as { DB?: unknown } | null | undefined)?.DB);
}

/**
 * A run object bound to a real database must never serve from a fence-less
 * runtime. `init()` makes that true for every host that hands it the binding —
 * its `{ DB }` branch has no opt-out — so what is left for this assert is the
 * host that builds a RunnerRuntime by hand inside `build()` and forgets: that
 * deployment would then execute straight through a migration lock, silently,
 * and the fence would look wired because every OTHER surface reports it.
 *
 * Scoped to runtimes THIS PACKAGE built. A test double cast to the type is not
 * a RunnerRuntime and carries no fence by construction; asserting on it would
 * indict every stub for a property it was never meant to have, and would say
 * nothing about the production wiring this guard exists for.
 */
function assertFencedRuntime(runtime: RunnerRuntime, env: unknown): void {
  if (!(runtime instanceof RunnerRuntime)) return;
  if (!hasDatabaseBinding(env)) return;
  if (runtime.executionFence === undefined) {
    throw new Error(
      'DurableObjectRunner: build() returned a runtime with no execution fence while this deployment carries a DB binding — wire it through init({ DB }) (which builds one) or pass an ExecutionFenceStore, so a migration-locked deployment cannot execute',
    );
  }
  // The same guard, for the same failure. A hand-built runtime with no
  // reservation store still EXECUTES idempotent starts — the router reserves
  // and claims above it — but never marks their reservations spent, so every
  // key this deployment ever honoured stays in the drain inventory and never
  // becomes purgeable. That is invisible until the day someone tries to prove
  // the deployment empty, which is the day it matters most.
  if (runtime.startIdempotency === undefined) {
    throw new Error(
      'DurableObjectRunner: build() returned a runtime with no start-reservation store while this deployment carries a DB binding — wire it through init({ DB }) (which builds one), so an idempotent start can be marked settled when its run ends',
    );
  }
}

export abstract class DurableObjectRunner<TEnv = unknown> {
  protected readonly env: TEnv;
  /** Absent in Node tests; present under workerd for storage and the alarm. */
  protected readonly state?: DurableObjectRunnerState;
  #runtime?: RunnerRuntime;
  #operationTail = Promise.resolve();
  /** `step\0reason` of every suspension deadline this object has reported. */
  #reportedSuspensionRejections = new Set<string>();
  /** Persisted claims outlive an isolate and cannot establish run liveness. */
  readonly #startsInFlight = new Map<string, RunStartFrame>();

  constructor(state: DurableObjectRunnerState | undefined, env: TEnv) {
    this.state = state;
    this.env = env;
  }

  /** Define workflows via init() and return its runtime. Called once, lazily. */
  protected abstract build(env: TEnv): RunnerRuntime;

  /** The deployment-local ownership registry used for recoverable run starts. */
  protected abstract runOwnership(env: TEnv): DurableObjectRunOwnershipStore;

  /** Existing schedules domain used only for target-verifiable schedule fires. */
  protected scheduleSource(_env: TEnv): ScheduleSourceStore | undefined {
    return undefined;
  }

  /** Required when the host exposes terminate or deadline lifecycle routes. */
  protected runLifecycle(
    _env: TEnv,
  ): DurableObjectRunLifecycleHooks | undefined {
    return undefined;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const encodedPrincipal = request.headers.get(EXECUTION_PRINCIPAL_HEADER);
      const encodedEpoch = request.headers.get(MUTATION_EPOCH_HEADER);
      // Deployment-identity check BEFORE any routing or storage work: under
      // workerd this instance refuses to serve until its env tag matches the
      // database sentinel (fail closed on a mis-provisioned binding); off
      // workerd (node tests, state undefined) it is a no-op. Memoized after
      // the first success, so steady-state requests pay nothing.
      await verifyDurableObjectDeploymentRequest(request, this.state, this.env);
      const mutationEpoch = mutationEpochFromHeader(encodedEpoch);
      return await this.#route(request, encodedPrincipal, mutationEpoch);
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  #ensureRuntime(): RunnerRuntime {
    if (!this.#runtime) {
      const runtime = this.build(this.env);
      assertFencedRuntime(runtime, this.env);
      this.#runtime = runtime;
    }
    return this.#runtime;
  }

  /**
   * This object's execution fence, or the open reading when the host built an
   * unfenced runtime. Taken from the RUNTIME rather than constructed here, so
   * the routes below and the start/resume backstop inside the runtime can
   * never be gating two different databases.
   */
  async #readExecutionFence(): Promise<ExecutionFenceReading> {
    return readExecutionFence(this.#ensureRuntime().executionFence);
  }

  // Enforce host-owned run ids at the DO boundary: this instance was addressed
  // as idFromName(`${workflowId}:${runId}`) by the trusted Worker, and id.name
  // is unforgeable at this boundary. If a request asks the instance to act on
  // a DIFFERENT (workflowId, runId), someone routed around the name join —
  // acting on the request's ids would run outside this instance's identity
  // (and outside its per-run serialization). Refuse loudly. `id.name` is only
  // populated for idFromName-created ids; it is absent under node tests
  // (state undefined / minimal stubs), where the runtime's own validation
  // still applies.
  #assertRunIdentity(workflowId: string, runId: string): void {
    const name = this.state?.id?.name;
    if (name === undefined) return;
    if (name !== `${workflowId}:${runId}`) {
      throw new Error(
        `DO identity mismatch: instance is '${name}' but the request names '${workflowId}:${runId}' — refusing`,
      );
    }
  }

  /**
   * The same `workflowId:runId` join the DO name and the runtime's own run key
   * use — composed here so the in-flight set cannot be keyed by runId alone,
   * which would make two workflows' runs of the same id one another's liveness.
   */
  #inFlightKey(workflowId: string, runId: string): string {
    return `${workflowId}:${runId}`;
  }

  /**
   * Is a start for this run executing in THIS object right now?
   *
   * Two sources, ORed, because they cover different halves of the window: the
   * route's own set covers from the recovery journal to the response (including
   * the gap before core persists anything), and the runtime's `#activeRuns`
   * covers a run driven through this isolate's runtime by any other path — an
   * in-process host, a resume, an agent loop sharing the runtime. Neither alone
   * is the whole window, and a false negative here is the expensive direction:
   * it turns a live run into an UNRESOLVABLE refusal.
   */
  #isStartLive(workflowId: string, runId: string): boolean {
    if (this.#startsInFlight.has(this.#inFlightKey(workflowId, runId))) {
      return true;
    }
    return this.#ensureRuntime().isRunActive(workflowId, runId);
  }

  /**
   * Validate an internal-channel idempotency key. Absent stays absent; anything
   * present must be path-safe, because the same string is compared against the
   * fence's proof key and stored as a reservation's primary key.
   */
  #startIdempotencyKey(value: unknown): string | undefined {
    if (value === undefined || value === null) return undefined;
    if (!isPathSafeId(value)) {
      throw new InvalidRunRequestError(
        'idempotencyKey must be a URL-path-safe identifier',
      );
    }
    return value;
  }

  #requestedBy(value: unknown): string {
    if (!isExecutionPrincipalId(value)) {
      throw new InvalidRunRequestError('run requester is missing or malformed');
    }
    return value;
  }

  #requestedByKind(value: unknown): DurableObjectRunOwner['kind'] {
    if (!isExecutionPrincipalKind(value)) {
      throw new InvalidRunRequestError('run requester kind is malformed');
    }
    return value;
  }

  /**
   * The reserved timeout envelope is minted by the alarm and by nothing else.
   * A caller allowed to resume could otherwise drive a step's timeout branch
   * while provenance still names them as the requester, which would make the
   * one contract this feature sells — a timeout resume is distinguishable from
   * a real signal — untrue. The KEY is refused, not just a well-formed
   * envelope, so a step that reads the key directly cannot be fooled either.
   */
  #resumeData(value: unknown): unknown {
    if (
      value !== null &&
      typeof value === 'object' &&
      SUSPENSION_TIMEOUT_RESUME_KEY in value
    ) {
      throw new InvalidRunRequestError(
        `resume data must not carry the reserved '${SUSPENSION_TIMEOUT_RESUME_KEY}' key`,
      );
    }
    return value;
  }

  #trustedExecutionPrincipal(encoded: string | null): ExecutionPrincipal {
    const principal = encoded ? decodeExecutionPrincipal(encoded) : undefined;
    if (!principal) {
      throw new DurableObjectRunIdentityError(
        'run request carries no valid trusted execution principal',
      );
    }
    return principal;
  }

  async #finalizeTerminal(
    runtime: RunnerRuntime,
    workflowId: string,
    runId: string,
    owner: DurableObjectRunOwner,
    result: RunLifecycleTransitionResult,
    selected?: AuthoritativeStartState,
    originalClaim?: StartReservationReading,
  ): Promise<RunSummary> {
    const claim =
      originalClaim === undefined
        ? undefined
        : captureReservation(originalClaim, 'started');
    const assertQuiescent = (): void => {
      if (
        this.#startsInFlight.has(this.#inFlightKey(workflowId, runId)) ||
        runtime.isRunActive(workflowId, runId)
      )
        throw new RunStartPendingError();
    };
    assertQuiescent();
    const state =
      selected ??
      (await runtime.authoritativeStartState(workflowId, runId, {
        includeLegacy: true,
      }));
    if (
      !state ||
      state.kind === 'initial' ||
      !isTerminalRunStatus(state.snapshot.status) ||
      !isTerminalRunStatus(state.summary.status)
    )
      throw new RunStateUnreadableError(workflowId, runId);
    const cleanup = terminalCleanupFor(
      lifecycleFromRequestContext(state.snapshot.requestContext),
    );
    if (
      !cleanup ||
      state.summary.runId !== result.summary.runId ||
      state.summary.status !== result.summary.status ||
      cleanup.status !== result.cleanup.status ||
      cleanup.revision !== result.cleanup.revision ||
      cleanup.scheduleDispatch?.scheduleId !==
        result.cleanup.scheduleDispatch?.scheduleId ||
      cleanup.scheduleDispatch?.dispatchId !==
        result.cleanup.scheduleDispatch?.dispatchId ||
      (result.cleanup.cleanupCompleted && !cleanup.cleanupCompleted)
    )
      throw new RunStateUnreadableError(workflowId, runId);
    const stored = await this.state?.storage?.get(RUN_OWNER_RECOVERY_KEY);
    assertQuiescent();
    if (state.kind === 'legacy') {
      if (stored !== undefined || claim !== undefined)
        throw new RunStateUnreadableError(workflowId, runId);
    } else if (stored !== undefined) {
      const recovery = this.#runOwnerRecovery(stored);
      this.#assertRunIdentity(recovery.workflowId, recovery.runId);
      if (
        claim &&
        !sameRunRecovery(recovery, { ...recovery, startReservation: claim })
      )
        throw new RunStateUnreadableError(workflowId, runId);
      return this.#finishRunOwner(recovery, state.summary, state);
    } else {
      await runtime.settleStartExecution(state, claim);
    }
    const assertNoJournal = async (): Promise<void> => {
      assertQuiescent();
      if (
        (await this.state?.storage?.get(RUN_OWNER_RECOVERY_KEY)) !== undefined
      )
        throw new RunStateUnreadableError(workflowId, runId);
      assertQuiescent();
    };
    return this.#completeTerminalEffects(
      runtime,
      workflowId,
      runId,
      owner,
      { ...result, summary: state.summary, cleanup },
      assertNoJournal,
    );
  }

  async #completeTerminalEffects(
    runtime: RunnerRuntime,
    workflowId: string,
    runId: string,
    owner: DurableObjectRunOwner,
    result: RunLifecycleTransitionResult,
    validate?: () => Promise<void>,
  ): Promise<RunSummary> {
    if (validate) await validate();
    if (result.cleanup.cleanupCompleted) return result.summary;
    const hooks = this.runLifecycle(this.env);
    if (!hooks) {
      throw new Error('run termination requires lifecycle cleanup hooks');
    }
    await hooks.abandonApprovals(workflowId, runId, result.cleanup.status);
    if (validate) await validate();
    if (result.cleanup.scheduleDispatch) {
      if (!hooks?.discardScheduleDispatch) {
        throw new Error(
          'scheduled run termination requires a dispatch-settlement hook',
        );
      }
      await hooks.discardScheduleDispatch(
        result.cleanup.scheduleDispatch.scheduleId,
        result.cleanup.scheduleDispatch.dispatchId,
        runId,
      );
      if (validate) await validate();
    }
    const ownership = this.runOwnership(this.env);
    if (!ownership.release) {
      throw new Error('run termination requires ownership release support');
    }
    const released = await ownership.release('run', runId, owner);
    if (validate) await validate();
    if (!released) {
      const current = await ownership.owner('run', runId);
      if (validate) await validate();
      if (current) {
        throw new Error(`run '${runId}' ownership could not be released`);
      }
    }
    const summary = await runtime.completeTerminalCleanup(
      workflowId,
      runId,
      result.cleanup.revision,
    );
    if (validate) await validate();
    return summary;
  }

  async #startSource(
    principal: ExecutionPrincipal,
    workflowId: string,
    runId: string,
    scheduleId: unknown,
    dispatchId: unknown,
  ): Promise<{
    owner: DurableObjectRunOwner;
    target?: ScheduleSourceWorkflowTarget;
  }> {
    if (scheduleId === undefined) {
      if (dispatchId !== undefined) {
        throw new InvalidRunRequestError(
          'dispatchId is only valid for a scheduled run',
        );
      }
      return {
        owner: Object.freeze({ kind: principal.kind, id: principal.id }),
      };
    }
    if (!isPathSafeId(scheduleId) || !isPathSafeId(dispatchId)) {
      throw new InvalidRunRequestError(
        'scheduleId and dispatchId are required path-safe identifiers',
      );
    }
    const schedules = this.scheduleSource(this.env);
    const source = schedules
      ? await resolveScheduleStartOwner(
          schedules,
          this.runOwnership(this.env),
          scheduleId,
          dispatchId,
          runId,
          { type: 'workflow', workflowId },
        )
      : undefined;
    if (!source) {
      throw new InvalidRunRequestError(
        'scheduled run source is missing or does not match the prepared workflow target',
      );
    }
    return source;
  }

  async #reserveRunOwner(
    runId: string,
    owner: DurableObjectRunOwner,
    token: string,
  ): Promise<void> {
    if (
      !(await this.runOwnership(this.env).reserveAll(
        [{ kind: 'run', resourceId: runId }],
        owner,
        token,
      ))
    ) {
      throw new Error(
        `run '${runId}' is unavailable or owned by another principal`,
      );
    }
  }

  async #withOperationLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#operationTail;
    let release: () => void = () => undefined;
    this.#operationTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  /**
   * The arming path for this object's one alarm, which two independent duties
   * share: run-owner recovery and the run's suspension deadlines. Every arm
   * that reflects CONVERGED state routes through here, or through the
   * #armAlarmFor it delegates to, because an alarm set for one duty from a
   * site that cannot see the other would silently drop the other's wake — a
   * suspended run's deadline is then lost until something else happens to
   * re-arm it, and nothing else has to. The unconditional watchdog arm
   * (#armAlarmWatchdog, and the retry wake a failed reconcile leaves behind)
   * is the deliberate exception: it is set before anything is read, when no
   * duty's state is known yet, and the body's own final arm replaces it.
   *
   * `recoveryDueAt` is the recovery time a caller is arming right now; without
   * it the stored recovery record supplies one. No duty pending at all is the
   * only case that deletes the alarm.
   */
  async #armNextAlarm(recoveryDueAt?: number): Promise<void> {
    await this.#armAlarmFor(
      await this.#readSuspensionDeadlines(),
      recoveryDueAt,
    );
  }

  /**
   * #armNextAlarm for a caller that already holds the record — the one it just
   * read to derive entries, or the one it just wrote. Re-reading it here would
   * charge every lifecycle boundary a second storage read for state the caller
   * is holding.
   */
  async #armAlarmFor(
    stored: SuspensionDeadlineRecord | undefined,
    recoveryDueAt?: number,
  ): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    if (!storage.setAlarm) {
      throw new Error(
        'run owner recovery and suspension deadlines require Durable Object alarms',
      );
    }
    const suspensionDueAt = nextSuspensionDeadlineAt(stored);
    const recoveryDue =
      recoveryDueAt ??
      ((await storage.get<unknown>(RUN_OWNER_RECOVERY_KEY)) !== undefined
        ? Date.now() + RUN_OWNER_RECOVERY_DELAY_MS
        : undefined);
    // `now` is sampled AFTER the awaited recovery-key read above. Hoisting it
    // before the read would shrink the floor by the read's latency, and the
    // floor is the anti-spin guarantee for an entry that is already due.
    const due = nextDutyAlarmAt(suspensionDueAt, recoveryDue, Date.now());
    if (due === undefined) {
      await storage.deleteAlarm?.();
      return;
    }
    await storage.setAlarm(due);
  }

  /**
   * The unconditional watchdog wake every alarm() body opens with: set BEFORE
   * any read, so a body that dies mid-flight still leaves this object woken,
   * and never a delete, because no branch has converged yet. One recovery delay
   * out because that is the cadence of the duty that can only be retried, and
   * because it is the wake the object keeps if the body cannot get far enough
   * to compute a better one; the body's own final #armNextAlarm() replaces it
   * with the true next due time.
   *
   * A reconcile whose write failed leaves the same wake for the same reason:
   * it too knows only that something must wake this object, and nothing about
   * what is stored — which is precisely what it could not write.
   */
  async #armAlarmWatchdog(): Promise<void> {
    const storage = this.state?.storage;
    if (!storage?.setAlarm) {
      throw new Error(
        'run owner recovery and suspension deadlines require Durable Object alarms',
      );
    }
    await storage.setAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
  }

  async #armRunOwnerRecovery(recovery: RunOwnerRecovery): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    // Arm BEFORE journaling, never after: an isolate that dies between the two
    // leaves a journal with no wake, and nothing else would recover the claim.
    // An alarm with no journal yet is harmless — that wake finds nothing to do.
    // One arm is enough, because a second one after the write would compute the
    // same time from the same two inputs.
    await this.#armNextAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
    await storage.put(RUN_OWNER_RECOVERY_KEY, recovery);
  }

  async #assertRunOwnerRecoveryCurrent(
    recovery: RunOwnerRecovery,
  ): Promise<void> {
    const current = await this.state?.storage?.get(RUN_OWNER_RECOVERY_KEY);
    if (
      current === undefined ||
      !sameRunRecovery(this.#runOwnerRecovery(current), recovery)
    )
      throw new Error('run owner recovery changed');
  }

  /**
   * Failed reconciliation can leave a retry wake without a stored deadline.
   * Keeping that wake avoids losing it when alarms are rebuilt from storage.
   */
  async #clearRunOwnerRecovery(
    recovery: RunOwnerRecovery,
    keepWake: boolean,
  ): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    await storage.delete(RUN_OWNER_RECOVERY_KEY);
    if (keepWake) {
      await this.#armAlarmWatchdog();
      return;
    }
    await this.#armNextAlarm();
  }

  async #prepareRunOwner(
    recovery: RunOwnerRecovery,
    execution: RunExecutionIdentity,
  ): Promise<RunOwnerRecovery> {
    const identity = normalizeRunExecutionIdentity(execution);
    if (
      identity.runId !== recovery.runId ||
      identity.workflowId !== recovery.workflowId
    )
      throw new Error('prepared run identity mismatch');
    const prepared: RunOwnerRecovery = this.#ensureRuntime().executionFence
      ? {
          ...recovery,
          phase: 'prepared',
          execution: normalizeD1RunExecutionIdentity(identity),
        }
      : { ...recovery, phase: 'prepared-unfenced', execution: identity };
    const storage = this.state?.storage;
    if (!storage)
      throw new Error('run owner recovery requires durable storage');
    const current = this.#runOwnerRecovery(
      await storage.get(RUN_OWNER_RECOVERY_KEY),
    );
    if (sameRunRecovery(current, prepared)) return prepared;
    if (current.phase !== 'preparing' || !sameRunRecovery(current, recovery))
      throw new Error('run owner recovery changed');
    try {
      await storage.put(RUN_OWNER_RECOVERY_KEY, prepared);
    } catch (error) {
      const reread = await storage.get(RUN_OWNER_RECOVERY_KEY);
      if (
        reread === undefined ||
        !sameRunRecovery(this.#runOwnerRecovery(reread), prepared)
      )
        throw error;
    }
    return prepared;
  }

  #matchingRunState(
    recovery: RunOwnerRecovery,
    state: AuthoritativeStartState | null,
  ): AuthoritativeStartState {
    const execution = recovery.execution;
    if (
      !state ||
      !execution ||
      state.execution.tablePrefix !== execution.tablePrefix ||
      state.execution.workflowId !== execution.workflowId ||
      state.execution.runId !== execution.runId ||
      state.execution.startToken !== execution.startToken
    )
      throw new RunStateUnreadableError(recovery.workflowId, recovery.runId);
    const identity = state.provenance.startIdentity;
    if (
      identity?.target.kind !== 'workflow' ||
      identity.target.id !== recovery.workflowId ||
      state.provenance.agentStart !== undefined
    )
      throw new RunStateUnreadableError(recovery.workflowId, recovery.runId);
    const claim = recovery.startReservation;
    if (
      claim &&
      (identity.owner.kind !== claim.owner.kind ||
        identity.owner.id !== claim.owner.id)
    )
      throw new RunStateUnreadableError(recovery.workflowId, recovery.runId);
    return state;
  }

  async #finishRunOwner(
    recovery: RunOwnerRecovery,
    summary: RunSummary,
    selected?: AuthoritativeStartState,
  ): Promise<RunSummary> {
    const runtime = this.#ensureRuntime();
    if (recovery.startReservation && !runtime.startIdempotency)
      throw new ExecutionFenceUnreadableError(
        'run start recovery is unresolved',
      );
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    const state = this.#matchingRunState(
      recovery,
      selected ??
        (await runtime.authoritativeStartState(
          recovery.workflowId,
          recovery.runId,
        )),
    );
    if (state.kind === 'initial') throw new RunStartPendingError();
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    if (isTerminalRunStatus(state.summary.status))
      await runtime.settleStartExecution(state, recovery.startReservation);
    await this.runOwnership(this.env).settleReservation(recovery.token, []);
    const cleanup = terminalCleanupFor(
      lifecycleFromRequestContext(state.snapshot.requestContext),
    );
    if (cleanup)
      summary = await this.#completeTerminalEffects(
        runtime,
        recovery.workflowId,
        recovery.runId,
        recovery.owner,
        {
          summary: state.summary,
          transitioned: false,
          casMatched: true,
          cleanup,
        },
      );
    const reconciled = await this.#reconcileSuspensionDeadlinesBestEffort(
      recovery.workflowId,
      recovery.runId,
      summary,
    );
    await this.#clearRunOwnerRecovery(recovery, !reconciled);
    return summary;
  }

  async #rearmRunOwnerRecovery(): Promise<void> {
    const storage = this.state?.storage;
    if (!storage?.setAlarm) {
      throw new Error(
        'run owner recovery and suspension deadlines require Durable Object alarms',
      );
    }
    await this.#armNextAlarm(Date.now() + RUN_OWNER_RECOVERY_DELAY_MS);
  }

  /**
   * Hydrate this run's armed deadlines. A record this instance cannot parse is
   * logged and deleted rather than thrown: the alarm has to converge, and a
   * record that throws on every wake would strand the run's remaining
   * deadlines forever.
   */
  async #readSuspensionDeadlines(): Promise<
    SuspensionDeadlineRecord | undefined
  > {
    const storage = this.state?.storage;
    if (!storage) return undefined;
    const stored = await storage.get<unknown>(SUSPENSION_DEADLINE_STORAGE_KEY);
    try {
      return parseSuspensionDeadlineRecord(stored);
    } catch (error) {
      console.error('stored suspension deadline discarded', error);
      await storage.delete(SUSPENSION_DEADLINE_STORAGE_KEY);
      return undefined;
    }
  }

  /**
   * Persist the run's armed deadlines and re-arm from what was just written.
   * `stored` is the record the caller read to compute `entries`: a boundary
   * with nothing stored that derives nothing writes nothing at all, because
   * every consumer that never arms a deadline would otherwise pay a delete and
   * an alarm write at each start, resume and termination. Nothing changed
   * there, so the alarm the recovery duty owns is left exactly as it was.
   */
  async #writeSuspensionDeadlines(
    workflowId: string,
    runId: string,
    stored: SuspensionDeadlineRecord | undefined,
    entries: readonly SuspensionDeadlineEntry[],
  ): Promise<void> {
    const storage = this.state?.storage;
    if (!storage) return;
    if (stored === undefined && entries.length === 0) return;
    let written: SuspensionDeadlineRecord | undefined;
    if (entries.length === 0) {
      await storage.delete(SUSPENSION_DEADLINE_STORAGE_KEY);
    } else {
      written = { version: 1, workflowId, runId, entries: [...entries] };
      await storage.put<SuspensionDeadlineRecord>(
        SUSPENSION_DEADLINE_STORAGE_KEY,
        written,
      );
    }
    await this.#armAlarmFor(written);
  }

  /**
   * Re-derive this run's deadlines from an authoritative summary and re-arm.
   * Called at every lifecycle boundary that produces one, and at every wake
   * that ends without a resume: derivation is idempotent (an entry is keyed by
   * the suspension that created it), so a boundary that changed nothing
   * rewrites the same record, a run that is no longer suspended derives
   * nothing and clears it, and a suspension nothing has armed yet is armed
   * here. That last case is why this is the wake's own no-resume exit: a run
   * whose only remaining event is its own alarm has no other boundary coming.
   */
  async #reconcileSuspensionDeadlines(
    workflowId: string,
    runId: string,
    summary: RunSummary,
    stored?: SuspensionDeadlineRecord | null,
  ): Promise<void> {
    if (!this.state?.storage) return;
    const { entries: derived, rejected } = suspensionDeadlinesOf(summary);
    for (const rejection of rejected) {
      this.#logSuspensionDeadlineRejection(runId, rejection);
    }
    // `stored` is the record a wake already read this alarm; re-reading it
    // here would be the second storage read #armAlarmFor's JSDoc refuses to
    // pay. `null` is a wake that read and found NOTHING stored, which `??`
    // could not tell from a lifecycle boundary holding no record at all
    // (undefined) — the one that must read.
    const previous =
      stored === undefined
        ? await this.#readSuspensionDeadlines()
        : (stored ?? undefined);
    // The ledger belongs to a suspension of THIS run: a record stamped for
    // another run carries nothing this run may inherit. The merge matches on
    // the suspension's own keys and not on the record's ids, so without this a
    // foreign entry whose step, fence and deadline happened to coincide would
    // hand its spent budget to this run's live deadline, which would then be
    // born abandoned. `previous` still reaches the write below, whose
    // write-or-skip decision is about the key that is stored, not about whose
    // run it names.
    const inherited =
      previous?.workflowId === workflowId && previous.runId === runId
        ? previous
        : undefined;
    await this.#writeSuspensionDeadlines(
      workflowId,
      runId,
      previous,
      mergeSuspensionDeadlines(inherited, derived),
    );
  }

  /**
   * Report a deadline the run asked for and did not get, once. Derivation runs
   * at every boundary, so an unchanged suspension re-derives the identical
   * rejection: logging each one every time would bury the rest of the log
   * under a single author mistake. Bounded by the per-run entry cap so a
   * long-lived object with a churning set of steps cannot grow this set.
   */
  #logSuspensionDeadlineRejection(
    runId: string,
    rejection: RejectedSuspensionDeadline,
  ): void {
    const seen = `${rejection.step}\u0000${rejection.reason}`;
    if (this.#reportedSuspensionRejections.has(seen)) return;
    if (
      this.#reportedSuspensionRejections.size >=
      MAX_SUSPENSION_DEADLINES_PER_RUN
    ) {
      this.#reportedSuspensionRejections.clear();
    }
    this.#reportedSuspensionRejections.add(seen);
    console.error(
      `suspension deadline not armed for step '${rejection.step}' of run '${runId}': ${rejection.reason}`,
    );
  }

  /**
   * Reconciliation is bookkeeping ABOUT a transition that has already been
   * persisted by the runtime. Failing the caller's start, resume, or
   * termination over it would report a lifecycle failure that did not happen,
   * so the failure is logged instead — and, when there was something to arm,
   * answered with a retry wake, because a suspended run is guaranteed no other
   * boundary. That wake re-derives from `authoritativeStatus()` whether or not
   * a record exists (#reconcileFromAuthoritativeStatus); the record it holds is
   * only the identity fallback, with its ledger carried by the merge. Returns
   * whether the deadlines converged, so a caller that would otherwise re-arm
   * from storage can leave the retry wake alone (#clearRunOwnerRecovery's
   * `keepWake`).
   *
   * A failure to arm the retry wake itself loses the deadline. That is the
   * lost-alarm failure mode the design already owns, now narrowed to it.
   */
  async #reconcileSuspensionDeadlinesBestEffort(
    workflowId: string,
    runId: string,
    summary: RunSummary,
  ): Promise<boolean> {
    try {
      await this.#reconcileSuspensionDeadlines(workflowId, runId, summary);
      return true;
    } catch (error) {
      console.error('suspension deadline reconciliation failed', error);
      // Derivation is pure and never throws, so asking it what was at stake
      // costs nothing. Nothing derivable means nothing was lost: a stale
      // record that could not be deleted is healed by its own alarm.
      if (suspensionDeadlinesOf(summary).entries.length === 0) return true;
      try {
        await this.#armAlarmWatchdog();
      } catch (alarmError) {
        console.error('suspension deadline retry wake failed', alarmError);
      }
      return false;
    }
  }

  // An entry is keyed by the DOT-JOINED suspended path, exactly as the summary
  // keys the two fence fields read below, so the path is joined here too. The
  // wake reads the rehydrated projection, which splits a stored key on every
  // dot: comparing a single segment instead would fail to recognize a top-level
  // step id containing a dot ([['a','b']] against 'a.b') and silently discard
  // its deadline. Nested paths never reach this point (suspension-deadline.ts
  // refuses them, having no fence of their own), so a joined match cannot be a
  // nested suspension wearing a top-level entry's key.
  #suspensionFenceMatches(
    summary: RunSummary,
    entry: SuspensionDeadlineEntry,
  ): boolean {
    return (
      summary.status === 'suspended' &&
      (summary.suspended ?? []).some((path) => path.join('.') === entry.step) &&
      summary.suspendedAt?.[entry.step] === entry.suspendedAt &&
      (summary.resumeCount?.[entry.step] ?? 0) === entry.resumeCount
    );
  }

  /**
   * Charge one failed wake to the retry ledger of the entry that wake was
   * working on, and abandon that entry once the budget is spent. Every failure
   * INSIDE the deadline duty that leaves its entry unconsumed comes through
   * here, because an entry that stays due would otherwise re-arm the alarm at
   * an already-past time, which Cloudflare fires immediately: the ledger is
   * what turns a persistent failure into a backoff rather than a wake loop.
   * A wake that fails before the duty runs at all cannot converge a ledger and
   * does not try to — it keeps the watchdog's recovery cadence instead.
   *
   * Abandonment keeps the entry as a TOMBSTONE (`attempts` at the budget, no
   * retry floor) rather than removing it: every reconcile re-derives the
   * still-armed suspend payload, so a removed entry would come back with a
   * clean ledger on the next boundary or wake — past due, armed at the floor —
   * and the budget would bound one burst instead of the suspension. The merge
   * carries the tombstone while the fence is unchanged and drops it when the
   * suspension moves on, so a later suspension of the same step starts fresh.
   */
  async #chargeSuspensionDeadlineAttempt(
    stored: SuspensionDeadlineRecord,
    entry: SuspensionDeadlineEntry,
    now: number,
    error: unknown,
  ): Promise<void> {
    const { workflowId, runId } = stored;
    const remaining = entriesWithoutStep(stored.entries, entry.step);
    const attempts = (entry.attempts ?? 0) + 1;
    if (attempts >= MAX_SUSPENSION_DEADLINE_ATTEMPTS) {
      console.error(
        `suspension deadline for step '${entry.step}' of run '${runId}' dropped after ${attempts} failed wakes`,
        error,
      );
      await this.#writeSuspensionDeadlines(workflowId, runId, stored, [
        ...remaining,
        tombstoned(entry),
      ]);
      return;
    }
    console.error(
      `suspension deadline wake for step '${entry.step}' of run '${runId}' failed (attempt ${attempts})`,
      error,
    );
    // Rebuilt field by field rather than spread from `entry`: the REBUILD is
    // what drops the unreadable-state stamp. Two wakes reach a charge: one
    // whose authoritative read SUCCEEDED — and a successful read is exactly
    // what clears that stamp — and the identity assert, the one charged
    // failure that read nothing.
    // Clearing the stamp there costs nothing: an entry a foreign record put in
    // front of this object's own name is charged every wake and tombstoned by
    // the fifth, which is how that record goes quiet.
    await this.#writeSuspensionDeadlines(workflowId, runId, stored, [
      ...remaining,
      {
        step: entry.step,
        deadlineAt: entry.deadlineAt,
        suspendedAt: entry.suspendedAt,
        resumeCount: entry.resumeCount,
        attempts,
        // attempts caps at 4 here, so the backoff tops out at 480s and needs
        // no ceiling of its own.
        nextAttemptAt: now + SUSPENSION_DEADLINE_RETRY_MS * 2 ** (attempts - 1),
      },
    ]);
  }

  /**
   * Record that this wake could not read the run's authoritative state, and
   * abandon an entry once that has been true of it for a day.
   *
   * Stamps EVERY entry due at this wake, not just the one the wake selected: a
   * read that did not succeed is one fact about the whole run, and one clock
   * per due batch is what keeps an N-entry record's bound at a day rather than
   * N days (a serial clock would only start the second entry's day once the
   * first was abandoned). Entries not yet due are left byte-identical — their
   * day begins when they fall due, not when a sibling's did.
   *
   * Never a charge: a read that did not succeed is no evidence about any entry,
   * and the abandonment budget exists for failed resumes — spending it on a
   * fifteen-minute storage incident would permanently abandon every deadline
   * falling due inside one. The stamp is what still bounds the other side: a
   * run whose state became permanently unreadable (a deploy that dropped the
   * workflow registration for good) would otherwise keep a 60 s heartbeat
   * forever. Any successful read clears it — the merge rebuilds the entry from
   * the derived one, and a charge rebuilds it field by field. The bound is
   * therefore conditional on this write LANDING: it is best effort, and a
   * storage layer that cannot write this key converges nothing anyway, so the
   * failure is logged and the wake keeps its cadence.
   *
   * Writes the record and NOTHING else, deliberately not through
   * #writeSuspensionDeadlines: while the clock is merely running this wake has
   * converged nothing, so the watchdog armed at the top of alarm() is the
   * cadence it must keep. Arming from a still-due entry that no ledger backed
   * off would land on the one-second floor and spin this object for the length
   * of the incident.
   *
   * Answers whether anything was ABANDONED, which is a convergence: giving up
   * on an entry and then waking every minute for it forever is incoherent, so
   * the caller reports it as converged and the body's final arm re-reads what
   * is left — for a record holding nothing else, the delete that finally ends
   * the heartbeat this bound exists to end.
   */
  async #markSuspensionDeadlineUnreadable(
    stored: SuspensionDeadlineRecord,
    now: number,
  ): Promise<boolean> {
    const storage = this.state?.storage;
    if (!storage) return false;
    try {
      let abandoned = false;
      let changed = false;
      const entries = stored.entries.map((entry) => {
        if (!isSuspensionDeadlineDue(entry, now)) return entry;
        // Clamped to this wake: a stamp in the FUTURE — a clock that stepped
        // backwards between two wakes, or a hand-written record — would put
        // `now - since` permanently below the limit and pin an uncharged
        // heartbeat forever. Clamping alone would not end it either (the next
        // wake would clamp the same untouched stamp again), so the corrected
        // value is what gets written below: the day starts at this wake.
        const since = Math.min(entry.unreadableSince ?? now, now);
        if (now - since > SUSPENSION_DEADLINE_UNREADABLE_LIMIT_MS) {
          console.error(
            `suspension deadline for step '${entry.step}' of run '${stored.runId}' abandoned after 24 h of unreadable run state`,
          );
          abandoned = true;
          changed = true;
          return tombstoned(entry);
        }
        // Stamped once: the first unsuccessful read is what starts the clock,
        // and rewriting it every wake would push the bound out forever. A
        // stamp already at its clamped value is that first one; anything else
        // is the future stamp being corrected.
        if (entry.unreadableSince === since) return entry;
        changed = true;
        return { ...entry, unreadableSince: since };
      });
      if (!changed) return false;
      // The SECOND writer of a tombstone, and the one that deliberately does
      // not arm: the charge writer goes through #writeSuspensionDeadlines and
      // re-arms from what it wrote, while this one has converged nothing worth
      // arming for and leaves the alarm to alarm()'s own final arm on the
      // wakes that did converge.
      await storage.put<SuspensionDeadlineRecord>(
        SUSPENSION_DEADLINE_STORAGE_KEY,
        { ...stored, entries },
      );
      return abandoned;
    } catch (error) {
      console.error(
        'suspension deadline unreadable-state marker failed',
        error,
      );
      return false;
    }
  }

  /**
   * The authoritative read every alarm path makes, carrying the one conclusion
   * an alarm may draw from its failure: every way this read can fail —
   * Mastra's in-memory fallback, an unregistered workflow, a storage fault —
   * is a read that did not succeed, and nothing downstream of it may conclude
   * anything else. One helper rather than the same try/catch at each site so
   * that rule is structural: an alarm-path authoritative read that is NOT
   * classified is one that did not come through here.
   */
  async #authoritativeStatusOrUnreadable(
    runtime: RunnerRuntime,
    workflowId: string,
    runId: string,
  ): Promise<RunSummary | null> {
    try {
      return await runtime.authoritativeStatus(workflowId, runId);
    } catch (error) {
      if (isRunStartPendingError(error)) throw error;
      throw error instanceof RunStateUnreadableError
        ? error
        : new RunStateUnreadableError(workflowId, runId, { cause: error });
    }
  }

  /**
   * Resume the run for the ONE entry this wake selected. Bounded work per wake
   * keeps a run with many armed steps from executing an unbounded number of
   * workflow bodies in a single alarm; #armNextAlarm() schedules the next.
   *
   * Throws on every failure that leaves the entry unconsumed, so its caller can
   * charge the ledger of the entry it selected and no other. The runtime comes
   * from the caller because BUILDING one is not such a failure: a misconfigured
   * binding throws on every wake and says nothing about this entry, so that
   * throw must land before an entry is in hand to charge.
   */
  async #resumeDueSuspensionDeadline(
    runtime: RunnerRuntime,
    stored: SuspensionDeadlineRecord,
    entry: SuspensionDeadlineEntry,
    now: number,
  ): Promise<void> {
    const { workflowId, runId } = stored;
    // Enforce host-owned run ids at the boundary whose ids come from storage,
    // rather than from the trusted Worker. This wake is about to execute a
    // workflow body, so it must be this object's own run and serialization.
    // Charge it like any other duty failure: it is the one charged failure that
    // read nothing, and charging is what quiets a foreign record's entry
    // (five wakes to a tombstone). It stays here rather than moving up with the
    // runtime because the nothing-due path below deliberately prefers `id.name`
    // over a foreign record, which asserting early would break.
    this.#assertRunIdentity(workflowId, runId);
    // The fence is re-read from authoritative state inside the operation lock:
    // a real signal may have resumed this suspension since the entry was
    // armed, and resuming again would run the step twice.
    //
    // ONLY A READ THAT SUCCEEDED MAY CONCLUDE ANYTHING, which is why this read
    // goes through the classifier above rather than being taken at face value:
    // the caller can then keep the watchdog instead of spending an abandonment
    // budget on an incident, and nothing AFTER this fence can masquerade as
    // one.
    const summary = await this.#authoritativeStatusOrUnreadable(
      runtime,
      workflowId,
      runId,
    );
    // A read that succeeded and found NOTHING is not evidence that a signal got
    // there first either — a read replica may not yet show a snapshot this
    // object itself wrote — but it is evidence the store answered, and D1
    // staleness windows are sub-second against a 900 s budget. So it is charged
    // as a failed wake and terminates as a tombstone, which is what quietly
    // ends the record of a run whose state is genuinely gone. The predicate is
    // the second line of defence, for a self-inconsistent 'suspended'
    // projection from any cause; it must be applied BEFORE the fence check,
    // which such a summary would fail, sending the moved-on reconcile below on
    // to wipe every entry of a run that is still suspended.
    if (!summary || !isReadableRunSummary(summary)) {
      throw new Error(
        `run '${runId}' of workflow '${workflowId}' read back absent or self-inconsistent`,
      );
    }
    if (!this.#suspensionFenceMatches(summary, entry)) {
      // A discarded deadline must be visible: this is the branch a real signal
      // reaches, and also the one a projection disagreement would reach.
      console.error(
        `suspension deadline for step '${entry.step}' of run '${runId}' dropped: the suspension it was armed against has moved on`,
      );
      // Reconcile rather than merely drop the entry. The summary in hand is
      // authoritative, and it may show a NEW suspension — one this run's own
      // failed bookkeeping never armed. Dropping alone would leave a suspended
      // run with a derivable deadline, no record, and no further wake. The
      // record this wake read goes with it: re-reading it would be the second
      // storage read #armAlarmFor's JSDoc refuses to pay.
      await this.#reconcileSuspensionDeadlines(
        workflowId,
        runId,
        summary,
        stored,
      );
      return;
    }
    const next = await runtime.resume(workflowId, runId, {
      step: [entry.step],
      resumeData: suspensionTimeoutResumeData(entry, now),
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });
    // EVERYTHING after this point is best effort, because the resume has
    // already run the step: charging a bookkeeping failure to the retry ledger
    // would record a failed resume that in fact succeeded, and could resume the
    // run a second time. The broadcast is one of those failures — its frame is
    // built by JSON.stringify OUTSIDE safeSend's per-socket tolerance, so a
    // step result JSON cannot encode (a bigint, a cycle, a throwing toJSON)
    // throws here rather than in the send it was meant to survive.
    try {
      this.#broadcastRunSummary(next);
    } catch (error) {
      console.error('suspension deadline broadcast failed', error);
    }
    await this.#reconcileSuspensionDeadlinesBestEffort(workflowId, runId, next);
  }

  /**
   * The shared end of every wake that will not resume AND holds no
   * authoritative summary of its own (the fence-moved branch above re-derives
   * directly from the one it read): re-derive this run's deadlines from
   * authoritative state. The set of wakes that land here with nothing due is
   * ordinary, not anomalous — the watchdog survivor whose duty already ran,
   * H1's retry wake after a failed reconcile, a recovery-armed wake whose
   * start body outlived the 60 s recovery delay (a healthy slow workflow), a
   * workerd alarm redelivery or at-least-once retry after a rethrown recovery
   * error (up to 6 retries), and an entry still inside the 1 s arm floor. The
   * `authoritativeStatus()` read lands off the lifecycle hot path on exactly
   * those wakes — and it is the authoritative read, not `status()`, because
   * this path DELETES a record it derives nothing from, so Mastra's in-memory
   * fallback reaching it would silently wipe the wake state of a run that is
   * still suspended. It cannot be skipped when a record exists: the boundary
   * that should have armed this run's CURRENT suspension may have failed to
   * write, and the record in hand is then stale — its own fence check only
   * runs when its entry falls due, so re-arming to it would let a short
   * re-suspension deadline hide behind a far-future stale one. Accepted cost:
   * an interrupted-start recovery wake that DID arm pays a second
   * `authoritativeStatus()` and record write here after #recoverRunOwner's own
   * reconcile — a rare path.
   *
   * Identity, three ways: a valid `id.name` is the single source under
   * workerd — the trusted Worker addressed this instance as
   * idFromName(`${workflowId}:${runId}`), PATH_SAFE_ID_PATTERN excludes ':'
   * from both ids, and stored ids are never asserted against it (a foreign
   * record's entries are simply dropped by the merge). A name that is present
   * but does not split into two path-safe ids skips the step entirely — a
   * record written from an unvalidated name would discard itself on read-back,
   * and a foreign record must not steer a status read either. Without a name
   * (Node stubs) the stored record supplies the ids; without either, the wake
   * converges to no alarm and a deadline whose record was never written is
   * lost. Production is safe from that last case because the exported topology
   * (host-kit/do-run-topology.ts) always addresses this object by idFromName.
   */
  async #reconcileFromAuthoritativeStatus(
    stored?: SuspensionDeadlineRecord | null,
  ): Promise<void> {
    const name = this.state?.id?.name;
    let workflowId: string;
    let runId: string;
    if (name !== undefined) {
      const parts = name.split(':');
      if (parts.length !== 2) return;
      const [namedWorkflowId, namedRunId] = parts;
      // isPathSafeId rejects the empty string and narrows both halves, so a
      // separate truthiness check would only restate it.
      if (!isPathSafeId(namedWorkflowId) || !isPathSafeId(namedRunId)) return;
      workflowId = namedWorkflowId;
      runId = namedRunId;
    } else if (stored) {
      workflowId = stored.workflowId;
      runId = stored.runId;
    } else {
      return;
    }
    // Built outside the wrap below on purpose: a runtime that cannot be BUILT
    // is a binding fault, not an unreadable run, and its throw belongs to the
    // caller's generic branch (which has no entry in hand here, so it keeps the
    // watchdog either way).
    const runtime = this.#ensureRuntime();
    // Through the same classifier the resume path uses, so a read that did not
    // succeed reaches the caller as one fact with one log line instead of the
    // charged-failure wording for a wake that charges nothing.
    const summary = await this.#authoritativeStatusOrUnreadable(
      runtime,
      workflowId,
      runId,
    );
    if (!summary) return;
    // The second line of defence, for a self-inconsistent 'suspended'
    // projection the marker does not cover: it converges exactly like an
    // absent read — without reconciling, so the record in hand stays and the
    // caller's re-arm keeps its wake — but it is logged, because unlike a
    // truly unknown run it means a suspended run's state could not be read.
    if (!isReadableRunSummary(summary)) {
      // Worded apart from the resume path's identical-looking refusal on
      // purpose: that one is CHARGED to the entry's budget and this one never
      // is, so an operator grepping the log can tell which consequence a line
      // carried.
      console.error(
        `run '${runId}' of workflow '${workflowId}' state read back self-inconsistent; keeping the record`,
      );
      return;
    }
    await this.#reconcileSuspensionDeadlines(
      workflowId,
      runId,
      summary,
      stored,
    );
  }

  /**
   * Never rethrows. workerd retries a thrown alarm(), which would re-run this
   * whole wake — including a resume that may have executed workflow steps — so
   * this branch owns its own retry ledger and reports failures instead.
   *
   * Answers whether the deadline duty CONVERGED, which is what tells the
   * caller its final arm is safe. It did not converge when nothing could be
   * charged or settled for a failure — the record unreadable, the runtime
   * unbuildable, the ledger write itself failing, or an authoritative read
   * that did not succeed (which is never charged at all) — because an arm
   * computed from a still-due entry that no ledger backed off lands on the
   * floor and wakes again a second later, forever. The one exception is the
   * 24 h abandonment of a continuously unreadable entry: that settles the
   * entry, so it converges.
   */
  async #runDueSuspensionDeadline(now: number): Promise<boolean> {
    let stored: SuspensionDeadlineRecord | undefined;
    let entry: SuspensionDeadlineEntry | undefined;
    try {
      stored = await this.#readSuspensionDeadlines();
      if (!stored) {
        // `null`, not nothing: this wake HAS read the record key and found it
        // empty, so the reconcile below must not read it a second time.
        await this.#reconcileFromAuthoritativeStatus(null);
        return true;
      }
      // Built BEFORE an entry is selected, and passed down rather than built
      // where it is used. `build(env)` is the host's, and a misconfigured
      // binding throws from it on every wake — a fault that says nothing about
      // any entry, so charging it would tombstone a live deadline in five
      // wakes. Landing the throw with no entry in hand keeps it uncharged, on
      // the watchdog cadence, exactly like the identity failure one layer up.
      const runtime = this.#ensureRuntime();
      entry = dueSuspensionDeadline(stored, now);
      if (!entry) {
        // Same end as the no-record wake: a wake that will not resume
        // re-derives from authoritative state, because the boundary that
        // should have armed this run's current suspension may have failed to
        // write, and the record in hand is then stale (its own fence check
        // only runs when it falls due).
        await this.#reconcileFromAuthoritativeStatus(stored);
        return true;
      }
      await this.#resumeDueSuspensionDeadline(runtime, stored, entry, now);
      return true;
    } catch (error) {
      if (isRunStartPendingError(error)) {
        if (
          stored?.entries.some((entry) => entry.unreadableSince !== undefined)
        ) {
          try {
            await this.state?.storage?.put(SUSPENSION_DEADLINE_STORAGE_KEY, {
              ...stored,
              entries: stored.entries.map(
                ({ unreadableSince: _unreadableSince, ...entry }) => entry,
              ),
            });
          } catch (resetError) {
            console.error(
              'suspension deadline pending stamp reset failed',
              resetError,
            );
          }
        }
        return false;
      }
      // The deployment is fenced (or its fence could not be read). Classified
      // with the same THREE outcomes as an unreadable read — uncharged,
      // unconverged, watchdog cadence — because a wake refused by an
      // operational control is no evidence about the entry it was working on.
      // Charging it would spend the abandonment budget in about sixteen
      // minutes of lock and tombstone a live deadline; converging would arm
      // from a still-due entry no ledger backed off, which lands on the
      // one-second floor and spins.
      //
      // What it deliberately does NOT do is stamp `unreadableSince`. That
      // clock exists to bound a run whose state became PERMANENTLY unreadable,
      // and abandons its entries after a day; a fence is a deliberate,
      // operator-visible, bounded state, and answering a long migration by
      // discarding every deadline due inside it would be the same fault the
      // no-charge rule above exists to prevent, one order of magnitude later.
      if (isExecutionFenceRefusal(error)) {
        console.error(
          'suspension deadline wake refused by the deployment execution fence',
          error,
        );
        return false;
      }
      // Classified FIRST, or every degraded wake would also log the generic
      // failure below. A read that did not succeed converges nothing and
      // charges nothing: it keeps the watchdog cadence until it heals, and the
      // entry it was working on carries the clock that bounds that.
      if (error instanceof RunStateUnreadableError) {
        console.error(
          'suspension deadline wake could not read authoritative state',
          error,
        );
        // Converged only where the marker ABANDONS an entry: that is the one
        // outcome here that settles anything, and leaving the alarm armed for
        // an entry just given up on would keep the heartbeat forever.
        //
        // `entry` is the gate, not the target: it is what proves this wake had
        // duty work due at all (a wake whose nothing-due re-derivation could
        // not read has no clock to run), and the marker then covers every entry
        // due at the same moment, because one failed read is one fact about
        // them all.
        return stored && entry
          ? await this.#markSuspensionDeadlineUnreadable(stored, now)
          : false;
      }
      console.error('suspension deadline wake failed', error);
      // The charge names the entry this wake had in hand, never one re-selected
      // afterwards: with two entries due, re-selecting would push the second
      // one out and spend a fifth of a budget it never used. A failure with no
      // entry in hand — the record itself unreadable, or the re-derivation
      // above — has nothing to charge, so it keeps the watchdog cadence.
      if (!stored || !entry) return false;
      try {
        await this.#chargeSuspensionDeadlineAttempt(stored, entry, now, error);
        return true;
      } catch (ledgerError) {
        console.error('suspension deadline ledger update failed', ledgerError);
        return false;
      }
    }
  }

  async #recoverRunOwner(
    recovery: RunOwnerRecovery,
    ownFrame?: RunStartFrame,
    ownFailure?: unknown,
  ): Promise<RunSummary | null> {
    this.#assertRunIdentity(recovery.workflowId, recovery.runId);
    const runtime = this.#ensureRuntime();
    if (recovery.startReservation && !runtime.startIdempotency)
      throw new ExecutionFenceUnreadableError(
        'run start recovery is unresolved',
      );
    const key = this.#inFlightKey(recovery.workflowId, recovery.runId);
    const quiescent = (): boolean => {
      const active = this.#startsInFlight.get(key);
      return (
        active === undefined ||
        (active === ownFrame &&
          ownFrame.unwound &&
          ownFrame.token === recovery.token)
      );
    };
    if (
      !quiescent() ||
      runtime.isRunActive(recovery.workflowId, recovery.runId)
    )
      throw new RunStartPendingError();
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    let recovered: RecoveredStart | null = null;
    if (recovery.phase === 'prepared') {
      recovered = await runtime.recoverStartAttempt(recovery.execution, {
        attemptToken: recovery.token,
        isOwnerQuiescent: quiescent,
        startReservation: recovery.startReservation,
        expectedTarget: { kind: 'workflow' },
      });
    } else if (recovery.phase === 'prepared-unfenced') {
      const state = this.#matchingRunState(
        recovery,
        await runtime.authoritativeStartState(
          recovery.workflowId,
          recovery.runId,
        ),
      );
      if (state.kind === 'initial') throw new RunStartPendingError();
      return this.#finishRunOwner(recovery, state.summary, state);
    }
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    if (recovered) {
      const summary =
        recovered.kind === 'ordinary'
          ? recovered.summary
          : recovered.transition.summary;
      if (recovered.kind === 'lifecycle') {
        // Runtime already strictly settled the single selected recovery observation.
        await this.runOwnership(this.env).settleReservation(recovery.token, []);
        const completed = recovered.transition.cleanup.cleanupCompleted
          ? summary
          : await this.#completeTerminalEffects(
              runtime,
              recovery.workflowId,
              recovery.runId,
              recovery.owner,
              recovered.transition,
            );
        const reconciled = await this.#reconcileSuspensionDeadlinesBestEffort(
          recovery.workflowId,
          recovery.runId,
          completed,
        );
        await this.#clearRunOwnerRecovery(recovery, !reconciled);
        return completed;
      }
      await this.runOwnership(this.env).settleReservation(recovery.token, []);
      const reconciled = await this.#reconcileSuspensionDeadlinesBestEffort(
        recovery.workflowId,
        recovery.runId,
        summary,
      );
      await this.#clearRunOwnerRecovery(recovery, !reconciled);
      return summary;
    }
    const localZero = (): boolean =>
      recovery.phase === 'prepared' &&
      ownFrame !== undefined &&
      this.#startsInFlight.get(key) === ownFrame &&
      ownFrame.unwound &&
      ownFrame.token === recovery.token &&
      isDefinitiveInitialAdmissionRefusal(ownFailure, recovery.execution);
    await this.runOwnership(this.env).settleReservation(recovery.token, [
      { kind: 'run', resourceId: recovery.runId },
    ]);
    if (recovery.phase === 'prepared' && !localZero()) {
      await this.#rearmRunOwnerRecovery();
      throw new ExecutionFenceUnreadableError(
        'run start recovery is unresolved',
      );
    }
    await this.#assertRunOwnerRecoveryCurrent(recovery);
    await this.#clearRunOwnerRecovery(recovery, false);
    return null;
  }

  #runOwnerRecovery(value: unknown): RunOwnerRecovery {
    const stored = recoveryObject(value);
    const owner = recoveryObject(stored.owner);
    if (
      stored.version !== 2 ||
      !isPathSafeId(stored.workflowId) ||
      !isPathSafeId(stored.runId) ||
      !isPathSafeId(stored.token) ||
      !isExecutionPrincipalKind(owner.kind) ||
      !isExecutionPrincipalId(owner.id)
    )
      throw new Error('stored run owner recovery is malformed');
    const rawClaim =
      stored.startReservation === undefined
        ? undefined
        : recoveryObject(stored.startReservation);
    const claim =
      rawClaim === undefined
        ? undefined
        : captureReservation(
            {
              ...rawClaim,
              owner: recoveryObject(rawClaim.owner),
              binding: recoveryObject(rawClaim.binding),
            } as unknown as StartReservationReading,
            'started',
          );
    if (
      claim &&
      (claim.targetKind !== 'workflow' ||
        claim.targetId !== stored.workflowId ||
        claim.runId !== stored.runId ||
        claim.threadId !== undefined)
    )
      throw new Error('stored run owner recovery is malformed');
    const base = {
      version: 2 as const,
      workflowId: stored.workflowId,
      runId: stored.runId,
      token: stored.token,
      owner: { kind: owner.kind, id: owner.id },
      ...(claim ? { startReservation: claim } : {}),
    };
    if (stored.phase === 'preparing' && !Object.hasOwn(stored, 'execution'))
      return { ...base, phase: 'preparing' };
    if (stored.phase !== 'prepared' && stored.phase !== 'prepared-unfenced')
      throw new Error('stored run owner recovery is malformed');
    const raw = recoveryObject(stored.execution);
    const execution =
      stored.phase === 'prepared'
        ? normalizeD1RunExecutionIdentity(raw)
        : normalizeRunExecutionIdentity(raw);
    if (
      execution.tablePrefix !== raw.tablePrefix ||
      execution.workflowId !== base.workflowId ||
      execution.runId !== base.runId
    )
      throw new Error('stored run owner recovery is malformed');
    return stored.phase === 'prepared'
      ? {
          ...base,
          phase: 'prepared',
          execution: normalizeD1RunExecutionIdentity(execution),
        }
      : { ...base, phase: 'prepared-unfenced', execution };
  }

  async #recoverPendingRunOwner(): Promise<void> {
    const stored = await this.state?.storage?.get<unknown>(
      RUN_OWNER_RECOVERY_KEY,
    );
    if (stored !== undefined) {
      await this.#recoverRunOwner(this.#runOwnerRecovery(stored));
    }
  }

  async alarm(): Promise<void> {
    await this.#withOperationLock(async () => {
      await this.#armAlarmWatchdog();
      // The watchdog arm above is deliberately the ONLY arm on the path where
      // this throws. A misbound namespace fails verification on every wake, so
      // the deadline duty below never runs and its retry ledger can never
      // converge; arming at an already-due deadline here (floored one second
      // out) would then wake every suspended run's object every second for as
      // long as the misbinding lasts. Failing with the watchdog in place keeps
      // the recovery cadence this object woke at before it served deadlines.
      await verifyDurableObjectDeploymentIdentity(this.state, this.env);
      // The two duties are failure-isolated: a run-owner recovery that throws
      // must not skip a due suspension deadline (and its re-arm), and neither
      // must be able to leave the object with no wake at all.
      let recoveryError: unknown;
      try {
        await this.#recoverPendingRunOwner();
      } catch (error) {
        recoveryError = error;
      }
      const converged = await this.#runDueSuspensionDeadline(Date.now());
      // A duty that converged nothing has no arm to compute: the watchdog set
      // at the top of this body IS the cadence it falls back to, and re-arming
      // here would min it against an entry no ledger could back off — the same
      // treatment, for the same reason, as the identity failure above.
      if (recoveryError !== undefined) {
        if (recoveryError instanceof RunStateUnreadableError) {
          // Never rethrown: workerd retries a thrown alarm() up to six times,
          // which would be a retry storm during the exact storage incident
          // that caused it. Nothing was settled, so the journal survives and
          // #armNextAlarm below reads it and arms the 60 s recovery cadence —
          // the published rule for a wake that cannot read what it needs. The
          // held reservation degrades owner() reads until the read heals or
          // the isolate is evicted, which clears the in-memory run the marker
          // comes from.
          console.error(
            'run owner recovery could not read authoritative state',
            recoveryError,
          );
        } else {
          if (converged) await this.#rearmRunOwnerRecovery();
          throw recoveryError;
        }
      }
      if (converged) await this.#armNextAlarm();
    });
  }

  async #route(
    request: Request,
    encodedPrincipal: string | null,
    mutationEpoch: number | undefined,
  ): Promise<Response> {
    const segments = new URL(request.url).pathname.split('/').filter(Boolean);
    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);
    const [, workflowId, runId, action] = segments;

    if (request.method === 'POST' && segments.length === 1) {
      const principal = this.#trustedExecutionPrincipal(encodedPrincipal);
      return this.#withOperationLock(async () => {
        const body = await readJson<StartBody>(request);
        if (
          body &&
          [
            'mutationEpoch',
            'startIdentity',
            'agentStart',
            'execution',
            'tablePrefix',
            'startToken',
            'attemptToken',
            'runOwnerGuard',
            'onPreparedStartIdentity',
          ].some((key) => Object.hasOwn(body, key))
        ) {
          throw new InvalidRunRequestError(
            'start authority is derived from trusted provenance',
          );
        }
        const {
          workflowId,
          runId,
          inputData,
          initialState,
          requestContext,
          scheduleId,
          dispatchId,
          deadlineMs,
          idempotencyKey: rawIdempotencyKey,
          startReservation: suppliedReservation,
        } = body ?? {};
        if (typeof workflowId !== 'string') {
          return json({ error: 'workflowId is required' }, 400);
        }
        // The DO never generates a runId: the trusted Worker mints the id and
        // addresses this instance with it. A start without one is a caller bug,
        // not a request for generation.
        if (typeof runId !== 'string') {
          return json(
            { error: 'runId is required (server-minted by the run router)' },
            400,
          );
        }
        if (!isPathSafeId(workflowId) || !isPathSafeId(runId)) {
          throw new InvalidRunRequestError(
            'workflowId and runId must be URL-path-safe identifiers',
          );
        }
        this.#assertRunIdentity(workflowId, runId);
        // The key rides the internal channel and nothing else. Validated here
        // rather than trusted because this body is JSON: an unvalidated value
        // would reach the fence's proof-only comparison and the runtime's
        // reservation as whatever the parser produced.
        const idempotencyKey = this.#startIdempotencyKey(rawIdempotencyKey);
        const startReservation =
          suppliedReservation === undefined
            ? undefined
            : captureReservation(
                suppliedReservation as StartReservationReading,
                'started',
              );
        if (
          startReservation &&
          (startReservation.key !== idempotencyKey ||
            startReservation.runId !== runId ||
            startReservation.targetKind !== 'workflow' ||
            startReservation.targetId !== workflowId ||
            startReservation.threadId !== undefined ||
            startReservation.owner.kind !== principal.kind ||
            startReservation.owner.id !== principal.id ||
            !this.#ensureRuntime().startIdempotency)
        )
          throw new InvalidRunRequestError(
            'start reservation does not match the trusted start',
          );
        const startIdentity = normalizeStartIdentity({
          owner: { kind: principal.kind, id: principal.id },
          target: { kind: 'workflow', id: workflowId },
        });
        // Only this local preflight can release the captured claim. Admission
        // and proof binding belong to Runtime after durable preparation.
        try {
          const startFence = await this.#readExecutionFence();
          if (!admitsRunStart(startFence, idempotencyKey))
            throw new ExecutionFencedError(startFence.state, 'run start');
        } catch (error) {
          if (startReservation) {
            try {
              await this.#ensureRuntime().startIdempotency?.releaseReservation(
                startReservation,
              );
            } catch (releaseError) {
              console.error(
                'start reservation preflight release failed',
                releaseError,
              );
            }
          }
          throw error;
        }
        const source = await this.#startSource(
          principal,
          workflowId,
          runId,
          scheduleId,
          dispatchId,
        );
        const rawOwner = source.owner;
        const owner = Object.freeze({ kind: rawOwner.kind, id: rawOwner.id });
        const target = source.target;
        const resolvedInput = target ? target.inputData : inputData;
        const resolvedState = target ? target.initialState : initialState;
        const storedRequestContext = target
          ? target.requestContext
          : requestContext;
        const runtime = this.#ensureRuntime();
        await this.#recoverPendingRunOwner();
        const existing = await runtime.status(workflowId, runId);
        if (existing) {
          const registered = await this.runOwnership(this.env).owner(
            'run',
            runId,
          );
          if (
            !registered ||
            registered.kind !== owner.kind ||
            registered.id !== owner.id
          ) {
            throw new Error(
              `existing run '${runId}' has no matching committed owner`,
            );
          }
          throw new RunAlreadyExistsError(workflowId, runId, existing.status);
        }
        let recovery: RunOwnerRecovery = {
          version: 2,
          phase: 'preparing',
          workflowId,
          runId,
          token: crypto.randomUUID(),
          owner,
          ...(startReservation ? { startReservation } : {}),
        };
        const frame: RunStartFrame = { token: recovery.token, unwound: false };
        const frameKey = this.#inFlightKey(workflowId, runId);
        this.#startsInFlight.set(frameKey, frame);
        try {
          await this.#armRunOwnerRecovery(recovery);
          await this.#reserveRunOwner(runId, owner, recovery.token);
          let summary: RunSummary;
          try {
            summary = await runtime.start(workflowId, {
              runId,
              inputData: resolvedInput,
              initialState: resolvedState,
              ...(storedRequestContext !== undefined
                ? { storedRequestContext }
                : {}),
              requestedBy: principal.id,
              requestedByKind: principal.kind,
              attemptToken: recovery.token,
              ...(mutationEpoch === undefined ? {} : { mutationEpoch }),
              startIdentity,
              startReservation,
              runOwnerGuard: { owner, reservationToken: recovery.token },
              onPreparedStartIdentity: async (execution) => {
                recovery = await this.#prepareRunOwner(recovery, execution);
              },
              ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
              ...(deadlineMs === undefined
                ? {}
                : { deadlineMs: deadlineMs as number }),
            });
          } finally {
            frame.unwound = true;
          }
          summary = await this.#finishRunOwner(recovery, summary);
          this.#broadcastRunSummary(summary);
          return json(summary);
        } catch (error) {
          frame.unwound = true;
          try {
            const stored = await this.state?.storage?.get(
              RUN_OWNER_RECOVERY_KEY,
            );
            if (stored !== undefined) {
              const current = this.#runOwnerRecovery(stored);
              if (!sameRunRecovery(current, recovery))
                throw new Error('run owner recovery changed');
              const recovered = await this.#recoverRunOwner(
                current,
                frame,
                error,
              );
              if (recovered) return json(recovered);
            }
          } catch (recoveryError) {
            console.error('interrupted start recovery failed', recoveryError);
            await this.#rearmRunOwnerRecovery();
          }
          throw error;
        } finally {
          if (this.#startsInFlight.get(frameKey) === frame)
            this.#startsInFlight.delete(frameKey);
        }
      });
    }
    // The liveness probe, answered OUTSIDE #withOperationLock on purpose: the
    // start it is asking about holds that lock for the whole first leg, so a
    // probe that queued behind it would block for exactly as long as the run it
    // was trying to describe — and time out reporting nothing.
    if (
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'start-liveness' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      return json({ live: this.#isStartLive(workflowId, runId) });
    }
    if (
      request.method === 'GET' &&
      segments.length === 3 &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const runtime = this.#ensureRuntime();
      if (new URL(request.url).searchParams.get('replay') === '1') {
        const state = await runtime.authoritativeStartState(workflowId, runId);
        if (!state) return json({ error: 'run not found' }, 404);
        const identity = state.provenance.startIdentity;
        if (
          identity?.target.kind !== 'workflow' ||
          identity.target.id !== workflowId
        )
          throw new RunStateUnreadableError(workflowId, runId);
        const execution = normalizeStartExecutionIdentity({
          ...state.execution,
          ...identity,
        });
        return json(
          state.kind === 'initial'
            ? { kind: 'initial', execution }
            : { kind: 'result', execution, value: state.summary },
        );
      }
      const summary = await runtime.status(workflowId, runId);
      if (!summary) return json({ error: 'run not found' }, 404);
      return json(summary);
    }
    if (
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'dispatch-status' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      return this.#withOperationLock(async () => {
        const runtime = this.#ensureRuntime();
        await this.#recoverPendingRunOwner();
        const summary = await runtime.status(workflowId, runId);
        if (!summary) return json({ error: 'run not found' }, 404);
        return json(summary);
      });
    }
    if (
      request.method === 'GET' &&
      segments.length === 4 &&
      action === 'stream' &&
      workflowId &&
      runId
    ) {
      const isUpgrade =
        request.headers.get('upgrade')?.toLowerCase() === 'websocket';
      const state = this.state;
      if (!isUpgrade || !state?.acceptWebSocket) {
        // The per-run WS stream needs an Upgrade handshake AND the workerd
        // Hibernatable-WebSocket API; off workerd (node/vitest) or on a plain
        // GET, poll `GET /runs/:workflowId/:runId` instead. Fail with 426
        // (Upgrade Required), never a 500 — the WS path is proven by the spike.
        return json(
          {
            error:
              'websocket upgrade required for run streaming (workerd-only; poll GET /runs/:workflowId/:runId as the fallback)',
          },
          426,
        );
      }
      // The trusted Worker already verified the run ticket and routed by
      // ticket.runId to idFromName; re-bind to this instance's identity
      // before accepting so a mis-routed upgrade is refused.
      this.#assertRunIdentity(workflowId, runId);
      const runtime = this.#ensureRuntime();
      const snapshot = await runtime.status(workflowId, runId);
      if (!snapshot) return json({ error: 'run not found' }, 404);
      const { 0: client, 1: server } = newWebSocketPair();
      state.acceptWebSocket(server);
      // On-connect snapshot: seed the new subscriber with the current
      // authoritative summary so it need not wait for the next
      // lifecycle transition. Nothing to send if the run is not yet queryable.
      safeSend(server, runFrame(snapshot));
      return new Response(null, {
        status: 101,
        webSocket: client,
      } as unknown as ResponseInit);
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      action === 'resume' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const body = (await readJson<ResumeBody>(request)) ?? {};
      const requestedBy = this.#requestedBy(body.requestedBy);
      const requestedByKind = this.#requestedByKind(body.requestedByKind);
      const resumeData = this.#resumeData(body.resumeData);
      return this.#withOperationLock(async () => {
        const runtime = this.#ensureRuntime();
        // Refused here as well as inside resume(), so a fenced deployment
        // answers before it takes the per-run lock. A drain still admits
        // resumes — the suspended runs it is draining are waiting for exactly
        // these — and proof-only admits its one nominated run.
        await runtime.assertExistingRunAllowed(workflowId, runId);
        const summary = await runtime.resume(workflowId, runId, {
          step: body.step,
          resumeData,
          requestedBy,
          requestedByKind,
          ...(body.deadlineMs === undefined
            ? {}
            : { deadlineMs: body.deadlineMs as number }),
        });
        await this.#reconcileSuspensionDeadlinesBestEffort(
          workflowId,
          runId,
          summary,
        );
        // Broadcast the post-resume authoritative summary.
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      (action === 'terminate' || action === 'terminate-replay') &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const principal = this.#trustedExecutionPrincipal(encodedPrincipal);
      const runtime = this.#ensureRuntime();
      const preflightOwner = await this.runOwnership(this.env).owner(
        'run',
        runId,
      );
      if (action === 'terminate') {
        await runtime.cancelActiveExecution(workflowId, runId, 'cancelled', [
          principal,
          preflightOwner ?? principal,
        ]);
      }
      return this.#withOperationLock(async () => {
        const owner = await this.runOwnership(this.env).owner('run', runId);
        if (action === 'terminate-replay') {
          // Degrades CLOSED: the in-memory fallback reports 'pending', which
          // is neither terminal status, so a replay during a degraded read is
          // refused with UnknownRunError rather than replayed against a run
          // whose terminal state could not be read.
          const summary = await runtime.status(workflowId, runId);
          if (
            summary?.status !== 'cancelled' &&
            summary?.status !== 'timed_out'
          ) {
            throw new UnknownRunError(workflowId, runId);
          }
        }
        const result = await runtime.terminateAsPrincipal(
          workflowId,
          runId,
          principal,
          owner ?? principal,
        );
        const summary = await this.#finalizeTerminal(
          runtime,
          workflowId,
          runId,
          owner ?? principal,
          result,
        );
        await this.#reconcileSuspensionDeadlinesBestEffort(
          workflowId,
          runId,
          summary,
        );
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    if (
      request.method === 'POST' &&
      segments.length === 4 &&
      action === 'deadline' &&
      workflowId &&
      runId
    ) {
      this.#assertRunIdentity(workflowId, runId);
      const principal = this.#trustedExecutionPrincipal(encodedPrincipal);
      const body = (await readJson<DeadlineBody>(request)) ?? {};
      const cas: RunLifecycleCas = {
        expectedRevision: body.expectedRevision as number,
        ...(body.expectedDeadlineAt === undefined
          ? {}
          : { expectedDeadlineAt: body.expectedDeadlineAt as number }),
      };
      const runtime = this.#ensureRuntime();
      const preflightOwner = await this.runOwnership(this.env).owner(
        'run',
        runId,
      );
      await runtime.cancelActiveExecution(
        workflowId,
        runId,
        'timed_out',
        [principal, preflightOwner ?? principal],
        cas,
      );
      return this.#withOperationLock(async () => {
        const owner = await this.runOwnership(this.env).owner('run', runId);
        if (!owner) {
          // Degrades CLOSED, same as the replay guard: 'pending' is not
          // 'timed_out', so the throw stands. It costs more here — this route
          // is driven by the maintenance sweep, and the throw fails the whole
          // sweep pass for that interval — but the next pass retries it, so a
          // degraded read delays the sweep rather than timing out a run whose
          // state nothing could read.
          const summary = await runtime.status(workflowId, runId);
          if (summary?.status !== 'timed_out') {
            throw new UnknownRunError(workflowId, runId);
          }
        }
        const result = await runtime.timeOutAsPrincipal(
          workflowId,
          runId,
          cas,
          principal,
          owner ?? principal,
        );
        if (!result.casMatched || result.cleanup.cleanupCompleted) {
          // The only terminal path that returns without finalizing, so it is
          // also the only one that would leave a record armed for a run that
          // can never suspend again. It reconciles from `result.summary` — a
          // post-transition read of a run this call has just settled or found
          // already settled or CAS-stale, taken after #loadSnapshot succeeded
          // on the same store — so it is an ordinary re-derivation, not a
          // conclusion drawn from a read that might not have reached storage.
          await this.#reconcileSuspensionDeadlinesBestEffort(
            workflowId,
            runId,
            result.summary,
          );
          return json(result.summary);
        }
        const summary = await this.#finalizeTerminal(
          runtime,
          workflowId,
          runId,
          owner ?? principal,
          result,
        );
        await this.#reconcileSuspensionDeadlinesBestEffort(
          workflowId,
          runId,
          summary,
        );
        this.#broadcastRunSummary(summary);
        return json(summary);
      });
    }
    return json({ error: 'not found' }, 404);
  }

  /**
   * Fan the authoritative RunSummary out to every subscribed run-channel
   * socket after start, resume, terminate, or deadline expiry, plus the
   * on-connect snapshot. No-op when the DO exposes no getWebSockets
   * (node/vitest, or any host without the Hibernatable-WebSocket API), so the
   * HTTP surface is unchanged off workerd.
   */
  #broadcastRunSummary(summary: RunSummary): void {
    const sockets = this.state?.getWebSockets?.();
    if (!sockets) return;
    const frame = runFrame(summary);
    for (const ws of sockets) {
      safeSend(ws, frame);
    }
  }

  // Hibernation wake handlers — workerd invokes these BY NAME on the instance
  // when a hibernated run-channel socket receives a frame, closes, or errors,
  // so they must exist for a live socket to survive DO eviction. The run
  // channel is broadcast-only (progress flows server->client via
  // #broadcastRunSummary), so there is no client->server protocol beyond a
  // keepalive and no per-socket state to release — a closed socket simply
  // drops out of getWebSockets(). Only exercised under workerd (the spike).
  webSocketMessage(ws: WebSocketLike, message: string | ArrayBuffer): void {
    // Lightweight keepalive: answer a client 'ping' with 'pong'.
    if (message === 'ping') {
      ws.send('pong');
    }
  }

  webSocketClose(_ws: WebSocketLike, _code: number, _reason: string): void {
    // Nothing to reconcile; the closed socket leaves getWebSockets() on its own.
  }

  webSocketError(_ws: WebSocketLike, _error: unknown): void {
    // Nothing to reconcile on a broadcast-only channel.
  }
}

/**
 * The one decision behind every converged-state arm: the earlier of the two
 * duties' due times, floored SUSPENSION_DEADLINE_ARM_FLOOR_MS out so an
 * already-due entry cannot re-arm at its own past time (which Cloudflare
 * fires immediately). `undefined` means no duty is pending and the caller
 * deletes the alarm. Pure so it can be pinned in isolation; exported from
 * this module only, never from the package barrel. Callers must sample `now`
 * AFTER their last await — under workerd's I/O-frozen clock an earlier sample
 * would shrink the floor by that await's latency and weaken the anti-spin
 * guarantee.
 */
export function nextDutyAlarmAt(
  suspensionDueAt: number | undefined,
  recoveryDueAt: number | undefined,
  now: number,
): number | undefined {
  const due =
    suspensionDueAt === undefined
      ? recoveryDueAt
      : recoveryDueAt === undefined
        ? suspensionDueAt
        : Math.min(suspensionDueAt, recoveryDueAt);
  if (due === undefined) return undefined;
  return Math.max(due, now + SUSPENSION_DEADLINE_ARM_FLOOR_MS);
}

// One entry per suspended step is the stored record's key, so settling an entry
// is "drop this step". Matching on the step id rather than on object identity
// lets a caller settle an entry it read back separately from the record.
function entriesWithoutStep(
  entries: readonly SuspensionDeadlineEntry[],
  step: string,
): SuspensionDeadlineEntry[] {
  return entries.filter((entry) => entry.step !== step);
}

/** The run-channel wire frame containing the authoritative RunSummary. */
function runFrame(summary: RunSummary): string {
  return JSON.stringify({ type: 'run', summary });
}

async function readJson<T>(request: Request): Promise<T | null> {
  try {
    return (await request.json()) as T;
  } catch {
    return null;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}
