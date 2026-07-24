// SPDX-License-Identifier: Apache-2.0
// Track D (M-006), CI-M-006-002 — createScheduleTick. WE OWN THE TICK (DL-012):
// a cron-triggered scheduled() (or DO alarm) drives listDueSchedules -> CAS
// updateScheduleNextFire claim -> fire, bypassing core's pubsub worker loop
// entirely (the P1 "one chokepoint, no second execution path" rule). Core's own
// consumer (SchedulerWorker + AgentScheduleWorker) is a pubsub-driven loop we do
// NOT adopt.
//
// TARGET KINDS:
//  - WORKFLOW targets: mint a fresh INV-1 runId `${tenantId}_${uuid}` and fire
//    through the host's run-start seam (topology.start / RunnerRuntime.start),
//    so the run inherits INV-1, the per-leg requestContext derivation, and the
//    resume ledger. This is the fully-owned path.
//  - AGENT targets: `startAgent` routes the claimed target through the host's
//    runtime-driven thread DO. Without that seam, the tick retains the audited
//    `agent-target-unsupported` skip and never adopts core's worker.
//
// CAP (DL-007, P7): every unattended workflow start consults an INJECTABLE
// run-cap seam (host-agnostic; the showcase's demo caps are one implementation).
// A capped tenant yields an audited skip and the schedule stays healthy — the
// CAS claim already advanced nextFireAt, so a capped fire is CONSUMED, not
// retried hot (spike D-S4).
//
// STORED-CONTEXT BARRIER (b) (DL-004/R-004): a schedule's stored
// WorkflowSchedule.requestContext is NEVER forwarded verbatim into a fired leg.
// The tick STRIPS every reserved key (the whole `breakwater.` namespace + core's
// goal key) before handing the remainder to the start seam. The reserved set IS
// exactly the keys #requestContextFor derives (the two scope keys + the grant
// key), so a stripped context shares NO key with the runtime-derived context —
// there is normally nothing to collide. buildScheduledLegContext keeps the
// R-004 order (stored FIRST, runtime-derived LAST) as defense-in-depth for a host
// that applies the stored context, so a reserved key that ever slipped the strip
// still LOSES to the runtime value. NOTE the flowsafe DO topology's start seam
// carries only inputData (the frozen /runs route), so on the DO path stored
// context is DROPPED entirely (strictly more fail-closed); a local-runtime host
// can apply the tick-provided requestContext.

import { computeNextFireAt } from '@mastra/core/workflows';
import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from '../do-runner/breakwater-keys.js';
import {
  assertMintableTenantId,
  mintSaltedId,
  tenantOwnsSaltedId,
} from '../do-runner/index.js';
import { GOAL_REQUEST_CONTEXT_KEY } from '../goals/objective-routes.js';
import type { Schedule, ScheduleTrigger } from './schedules-d1.js';

/** The `breakwater.` namespace prefix — every runtime-owned key starts with it. */
const BREAKWATER_KEY_PREFIX = 'breakwater.';

/**
 * The explicit reserved requestContext keys (DL-004): the four runtime base keys
 * (all `breakwater.`-prefixed) + core's goal key. This is the canonical LIST for
 * introspection and the drift-guard test — NOT the runtime matcher. The strip and
 * the router both use `isReservedScheduleContextKey` (the `breakwater.` PREFIX +
 * the goal key), so a FUTURE breakwater key is covered without editing this list;
 * a test pins that every key here is matched by the predicate, so the explicit
 * list and the prefix matcher cannot diverge.
 */
export const RESERVED_SCHEDULE_CONTEXT_KEYS: readonly string[] = [
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  GOAL_REQUEST_CONTEXT_KEY,
];

/**
 * A key is reserved iff it is in the `breakwater.` namespace (covers all four
 * base keys AND any future breakwater key) or is core's goal key. A stored
 * schedule context carrying any of these would inject a standing capability /
 * objective into every woken run — the same stored-capability class as the
 * approval create-route leak.
 */
export function isReservedScheduleContextKey(key: string): boolean {
  return (
    key.startsWith(BREAKWATER_KEY_PREFIX) || key === GOAL_REQUEST_CONTEXT_KEY
  );
}

/** Drop every reserved key from a stored schedule context (barrier b's core job). */
export function stripReservedScheduleContext(
  stored: Record<string, unknown> | undefined,
): Record<string, unknown> {
  if (!stored) return {};
  const safe: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(stored)) {
    if (!isReservedScheduleContextKey(key)) safe[key] = value;
  }
  return safe;
}

/**
 * Build a fired leg's context: the stored NON-reserved keys UNDER the
 * runtime-derived context. R-004: spread stored FIRST, runtime-derived LAST so
 * the runtime value wins on any collision — the same last-spread-wins ordering
 * #requestContextFor uses (runtime.ts:701). After stripping, the stored keys
 * share no key with the runtime-derived set (the reserved set IS the
 * runtime-owned set), so the order is defense-in-depth: a reserved key that ever
 * slipped the strip still loses. A pure helper a host applies when it seeds a
 * fired run's context from the tick-provided stored context.
 */
export function buildScheduledLegContext(
  stored: Record<string, unknown> | undefined,
  runtimeDerived: Record<string, unknown>,
): Record<string, unknown> {
  return { ...stripReservedScheduleContext(stored), ...runtimeDerived };
}

/** The storage subset the tick reads/writes (a subset of D1SchedulesStorage). */
export interface ScheduleTickStore {
  listDueSchedules(now: number, limit?: number): Promise<Schedule[]>;
  getSchedule(id: string): Promise<Schedule | null>;
  updateScheduleNextFire(
    id: string,
    expectedNextFireAt: number,
    newNextFireAt: number,
    lastFireAt: number,
    lastRunId: string,
  ): Promise<boolean>;
  recordTrigger(trigger: ScheduleTrigger): Promise<void>;
}

/**
 * The run-start seam. A host passes `topology.start` (the DO round-trip — a 3-arg
 * function is assignable, and it drops the 4th `requestContext` arg, the
 * documented fail-closed DO-path behavior) or a local `RunnerRuntime.start`
 * wrapper that applies the stored context. Returns anything carrying the started
 * `runId` (RunSummary satisfies it).
 */
export type ScheduleTickStart = (
  workflowId: string,
  runId: string,
  inputData: unknown,
  requestContext?: Record<string, unknown>,
) => Promise<{ runId: string }>;

export type AgentScheduleTarget = Extract<
  Schedule['target'],
  { type: 'agent' }
>;

export interface ScheduleTickStartAgentInput {
  target: AgentScheduleTarget;
  tenantId: string;
  runId: string;
  /** DO address; ephemeral and memoryless when `threaded` is false. */
  topologyThreadId: string;
  threaded: boolean;
  requestContext: Record<string, unknown>;
  streamRequestContext: Record<string, unknown>;
}

export type ScheduleTickStartAgent = (
  input: ScheduleTickStartAgentInput,
) => Promise<{ runId: string }>;

/**
 * The injectable per-tenant run cap (DL-007): return false to REFUSE an
 * unattended start (a capped fire is skipped + audited, the schedule stays
 * healthy). Async so a D1/KV-backed cap fits. Absent ⇒ uncapped.
 */
export type ScheduleTickRunCap = (
  tenantId: string,
) => boolean | Promise<boolean>;

/** Outcome of one due-schedule fire attempt in a tick pass. */
export type ScheduleFireOutcome =
  | 'published' // a workflow run was dispatched
  | 'skipped' // agent start unavailable OR run-capped (deliberate non-fire)
  | 'failed' // invalid tenant OR the start seam threw
  | 'lost'; // the row advanced, paused, or disappeared before the claim

/** The structured audit event a tick emits per fire attempt (accepted OR not). */
export interface ScheduleTickAuditEvent {
  type: 'schedule.fire';
  scheduleId: string;
  /** The resolved tenant, or undefined when the row's metadata.tenantId is invalid. */
  tenantId?: string;
  target: 'workflow' | 'agent';
  outcome: ScheduleFireOutcome;
  /** The minted runId for a published fire. */
  runId?: string;
  /** Present for non-published outcomes: the structured reason. */
  reason?: string;
  timestamp: string;
}

/** The audit seam — a host bridges this to its AuditLogger / SIEM sink. */
export type ScheduleTickAuditSink = (
  event: ScheduleTickAuditEvent,
) => void | Promise<void>;

export interface ScheduleTickOptions {
  /** The schedules store (a D1SchedulesStorage) the tick claims + records over. */
  store: ScheduleTickStore;
  /** The run-start seam (topology.start / a local runtime start wrapper). */
  start: ScheduleTickStart;
  /** Runtime-driven agent start. Absent preserves the guarded skip. */
  startAgent?: ScheduleTickStartAgent;
  /** The per-tenant run cap (DL-007). Absent ⇒ uncapped. */
  runCap?: ScheduleTickRunCap;
  /** Every fire attempt is audited through this (accepted OR skipped/failed). Absent ⇒ no audit. */
  audit?: ScheduleTickAuditSink;
  /** Max due schedules processed per tick pass. Default 100. */
  limit?: number;
  /** Clock override for tests. */
  now?: () => number;
}

/** Per-pass tally the tick returns (for the cron's maintenance log line). */
export interface ScheduleTickResult {
  /** Due schedules the pass considered. */
  due: number;
  /** Workflow or agent runs dispatched. */
  fired: number;
  /** Deliberate non-fires (agent start unavailable or run-capped). */
  skipped: number;
  /** Errors (invalid tenant + start threw); the schedule was still advanced. */
  failed: number;
  /** CAS claims lost because the row advanced, paused, or disappeared. */
  lost: number;
}

function isMintableTenant(tenantId: unknown): tenantId is string {
  if (typeof tenantId !== 'string') return false;
  try {
    assertMintableTenantId(tenantId, 'scheduleTick');
    return true;
  } catch {
    return false;
  }
}

/**
 * Build the schedule tick: a `() => Promise<ScheduleTickResult>` a host slots
 * into its cron dispatch as its OWN failure-isolated duty (own try/catch, own
 * log line — the purge-availability lesson). Each due schedule is processed
 * independently: one bad row (a start throw, a corrupt cron) is contained and the
 * rest still fire.
 */
export function createScheduleTick(
  options: ScheduleTickOptions,
): () => Promise<ScheduleTickResult> {
  const { store, start } = options;
  const now = options.now ?? Date.now;
  const limit = options.limit ?? 100;

  const audit = async (
    event: Omit<ScheduleTickAuditEvent, 'type' | 'timestamp'>,
  ): Promise<void> => {
    if (!options.audit) return;
    await options.audit({
      type: 'schedule.fire',
      timestamp: new Date().toISOString(),
      ...event,
    });
  };

  const fireOne = async (
    schedule: Schedule,
    at: number,
    result: ScheduleTickResult,
  ): Promise<void> => {
    const targetType = schedule.target.type;
    // 1. Compute the next fire time. A corrupt cron cannot be advanced — the
    // facade validates cron at create, so this is a defense-in-depth edge. We do
    // NOT CAS-claim it (leaving nextFireAt would hot-loop, but a permanently
    // corrupt row is an ops data-integrity issue, not a hot-loop the tick should
    // mask by advancing to an arbitrary time); audit the failure and move on.
    let newNextFireAt: number;
    try {
      newNextFireAt = computeNextFireAt(schedule.cron, {
        ...(schedule.timezone !== undefined
          ? { timezone: schedule.timezone }
          : {}),
        after: at,
      });
    } catch {
      result.failed += 1;
      await audit({
        scheduleId: schedule.id,
        target: targetType,
        outcome: 'failed',
        reason: 'invalid-cron',
      });
      return;
    }

    // 2. Resolve the tenant from metadata (re-validated INV-3 at fire time).
    const rawTenant = (schedule.metadata as Record<string, unknown> | undefined)
      ?.tenantId;
    const tenantId = isMintableTenant(rawTenant) ? rawTenant : undefined;

    // 3. Provisional runId — only a workflow target with a valid tenant mints one
    // (the CAS lastRunId). Agent / capped / invalid fires carry no run.
    const runId =
      tenantId !== undefined
        ? mintSaltedId(tenantId, () => crypto.randomUUID(), 'scheduleTick')
        : undefined;

    // 4. CAS claim — the exactly-once gate (spike D-S1). The LOSER of two
    // concurrent ticks gets false here and fires nothing / records no trigger.
    // The cap is consulted AFTER the claim (DL-007), so `runId` is minted before
    // we know whether the fire will actually dispatch: on a capped fire the row's
    // `lastRunId` records this CLAIM's id though no run started. The trigger row
    // is the authoritative record of what happened (outcome + its own runId,
    // null for a skip); `lastRunId` is a best-effort last-claim marker, not a
    // guarantee the run was dispatched.
    const claimed = await store.updateScheduleNextFire(
      schedule.id,
      schedule.nextFireAt,
      newNextFireAt,
      at,
      runId ?? '',
    );
    if (!claimed) {
      result.lost += 1;
      const current = await store.getSchedule(schedule.id);
      const reason = !current
        ? 'disappeared'
        : current.status === 'paused'
          ? 'paused'
          : 'concurrent-claim';
      const lostReason = `lost: ${reason}`;
      await store.recordTrigger({
        scheduleId: schedule.id,
        runId: null,
        scheduledFireAt: schedule.nextFireAt,
        actualFireAt: at,
        outcome: 'skipped',
        metadata: { ...(tenantId ? { tenantId } : {}), reason: lostReason },
      });
      await audit({
        scheduleId: schedule.id,
        ...(tenantId ? { tenantId } : {}),
        target: targetType,
        outcome: 'lost',
        reason: lostReason,
      });
      return;
    }

    // 5. We OWN this fire — nextFireAt is already advanced (consumed, never
    // hot-looped), whatever happens below.
    const scheduledFireAt = schedule.nextFireAt;

    if (tenantId === undefined) {
      // Defense-in-depth: the facade always stamps a valid metadata.tenantId, so
      // this is a tampered/legacy row. Fail closed — no runId can be minted.
      result.failed += 1;
      await store.recordTrigger({
        scheduleId: schedule.id,
        runId: null,
        scheduledFireAt,
        actualFireAt: at,
        outcome: 'failed',
        metadata: { reason: 'invalid-tenant' },
      });
      await audit({
        scheduleId: schedule.id,
        target: targetType,
        outcome: 'failed',
        reason: 'invalid-tenant',
      });
      return;
    }

    if (targetType === 'agent') {
      if (!options.startAgent) {
        result.skipped += 1;
        await store.recordTrigger({
          scheduleId: schedule.id,
          runId: null,
          scheduledFireAt,
          actualFireAt: at,
          outcome: 'skipped',
          metadata: { tenantId, reason: 'agent-target-unsupported' },
        });
        await audit({
          scheduleId: schedule.id,
          tenantId,
          target: 'agent',
          outcome: 'skipped',
          reason: 'agent-target-unsupported',
        });
        return;
      }

      const target = schedule.target;
      if (
        (target.threadId !== undefined &&
          !tenantOwnsSaltedId(tenantId, target.threadId)) ||
        (target.resourceId !== undefined &&
          !tenantOwnsSaltedId(tenantId, target.resourceId)) ||
        (target.threadId !== undefined && target.resourceId === undefined)
      ) {
        result.failed += 1;
        await store.recordTrigger({
          scheduleId: schedule.id,
          runId: null,
          scheduledFireAt,
          actualFireAt: at,
          outcome: 'failed',
          metadata: { tenantId, reason: 'foreign-memory-id' },
        });
        await audit({
          scheduleId: schedule.id,
          tenantId,
          target: 'agent',
          outcome: 'failed',
          reason: 'foreign-memory-id',
        });
        return;
      }

      const allowed = options.runCap ? await options.runCap(tenantId) : true;
      if (!allowed) {
        result.skipped += 1;
        await store.recordTrigger({
          scheduleId: schedule.id,
          runId: null,
          scheduledFireAt,
          actualFireAt: at,
          outcome: 'skipped',
          metadata: { tenantId, reason: 'run-capped' },
        });
        await audit({
          scheduleId: schedule.id,
          tenantId,
          target: 'agent',
          outcome: 'skipped',
          reason: 'run-capped',
        });
        return;
      }

      const firedRunId = runId as string;
      const threaded = target.threadId !== undefined;
      const topologyThreadId =
        target.threadId ??
        mintSaltedId(
          tenantId,
          () => crypto.randomUUID(),
          'schedule agent thread',
        );
      let summary: { runId: string };
      try {
        summary = await options.startAgent({
          target,
          tenantId,
          runId: firedRunId,
          topologyThreadId,
          threaded,
          requestContext: stripReservedScheduleContext(target.requestContext),
          streamRequestContext: stripReservedScheduleContext(
            target.ifIdle?.streamOptions?.requestContext,
          ),
        });
      } catch (error) {
        result.failed += 1;
        await store.recordTrigger({
          scheduleId: schedule.id,
          runId: firedRunId,
          scheduledFireAt,
          actualFireAt: at,
          outcome: 'failed',
          error: error instanceof Error ? error.message : String(error),
          metadata: { tenantId },
        });
        await audit({
          scheduleId: schedule.id,
          tenantId,
          target: 'agent',
          outcome: 'failed',
          runId: firedRunId,
          reason: 'start-error',
        });
        return;
      }
      const dispatchedRunId = summary.runId ?? firedRunId;
      result.fired += 1;
      try {
        await store.recordTrigger({
          scheduleId: schedule.id,
          runId: dispatchedRunId,
          scheduledFireAt,
          actualFireAt: at,
          outcome: 'published',
          metadata: { tenantId },
        });
        await audit({
          scheduleId: schedule.id,
          tenantId,
          target: 'agent',
          outcome: 'published',
          runId: dispatchedRunId,
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'schedule-tick-bookkeeping-error',
            scheduleId: schedule.id,
            runId: dispatchedRunId,
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }
      return;
    }

    // 6. Workflow target, valid tenant: consult the run cap (DL-007).
    const allowed = options.runCap ? await options.runCap(tenantId) : true;
    if (!allowed) {
      // Capped: the schedule stays healthy (already advanced), audited (D-S4).
      result.skipped += 1;
      await store.recordTrigger({
        scheduleId: schedule.id,
        runId: null,
        scheduledFireAt,
        actualFireAt: at,
        outcome: 'skipped',
        metadata: { tenantId, reason: 'run-capped' },
      });
      await audit({
        scheduleId: schedule.id,
        tenantId,
        target: 'workflow',
        outcome: 'skipped',
        reason: 'run-capped',
      });
      return;
    }

    // 7. Fire the workflow run (runId is defined by construction here).
    const firedRunId = runId as string;
    const target = schedule.target;
    // Barrier (b): hand the start seam only the NON-reserved stored context.
    const safeContext = stripReservedScheduleContext(
      target.type === 'workflow' ? target.requestContext : undefined,
    );
    // The try wraps ONLY the start() dispatch. A post-dispatch bookkeeping
    // failure (a transient recordTrigger/audit throw) must NOT be reclassified as
    // a start failure — that would double-count and audit a run that actually ran
    // as `failed`/`start-error`, corrupting the very trail operators rely on.
    let summary: { runId: string };
    try {
      summary = await start(
        target.type === 'workflow' ? target.workflowId : '',
        firedRunId,
        target.type === 'workflow' ? target.inputData : undefined,
        safeContext,
      );
    } catch (error) {
      // The dispatch itself threw — no run started.
      result.failed += 1;
      await store.recordTrigger({
        scheduleId: schedule.id,
        runId: firedRunId,
        scheduledFireAt,
        actualFireAt: at,
        outcome: 'failed',
        error: error instanceof Error ? error.message : String(error),
        metadata: { tenantId },
      });
      await audit({
        scheduleId: schedule.id,
        tenantId,
        target: 'workflow',
        outcome: 'failed',
        runId: firedRunId,
        reason: 'start-error',
      });
      return;
    }
    // The run WAS dispatched. Record it as `published` (write-once at dispatch,
    // per core's ScheduleTriggerOutcome doc). Bookkeeping failures are logged
    // locally and do not reclassify the successful dispatch as a start failure.
    const dispatchedRunId = summary.runId ?? firedRunId;
    result.fired += 1;
    try {
      await store.recordTrigger({
        scheduleId: schedule.id,
        runId: dispatchedRunId,
        scheduledFireAt,
        actualFireAt: at,
        outcome: 'published',
        metadata: { tenantId },
      });
      await audit({
        scheduleId: schedule.id,
        tenantId,
        target: 'workflow',
        outcome: 'published',
        runId: dispatchedRunId,
      });
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'schedule-tick-bookkeeping-error',
          scheduleId: schedule.id,
          runId: dispatchedRunId,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  };

  return async () => {
    const at = now();
    const due = await store.listDueSchedules(at, limit);
    const result: ScheduleTickResult = {
      due: due.length,
      fired: 0,
      skipped: 0,
      failed: 0,
      lost: 0,
    };
    for (const schedule of due) {
      // Per-schedule isolation: one wedged row (an unexpected store/audit throw)
      // must not abort the pass and strand every due schedule behind it.
      try {
        await fireOne(schedule, at, result);
      } catch (error) {
        result.failed += 1;
        console.error(
          JSON.stringify({
            type: 'schedule-tick-error',
            scheduleId: schedule.id,
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    }
    return result;
  };
}
