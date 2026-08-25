// SPDX-License-Identifier: Apache-2.0
// createScheduleTick. WE OWN THE TICK: a Durable Object alarm drives
// listDueSchedules -> CAS updateScheduleNextFire claim -> fire, bypassing
// core's pubsub worker loop entirely under the "one chokepoint, no second
// execution path" rule. It does not adopt core's pubsub-driven schedule worker.
//
// TARGET KINDS:
//  - WORKFLOW targets: mint a fresh path-safe runId and fire
//    through the host's run-start seam (topology.start / RunnerRuntime.start),
//    so the run inherits host-owned run ids, per-leg requestContext derivation,
//    and snapshot provenance. This is the fully-owned path.
//  - AGENT targets: `startAgent` routes the claimed target through the host's
//    runtime-driven thread DO. Without that seam, the tick retains the audited
//    `agent-target-unsupported` skip and never adopts core's worker.
//
// CAP: every unattended workflow start consults an INJECTABLE run-cap seam.
// The showcase's demo caps are one implementation. A capped deployment yields
// an audited skip and the schedule stays healthy: the CAS claim already
// advanced nextFireAt, so the capped fire is consumed without a hot retry.
//
// STORED-CONTEXT BARRIER: a schedule's stored WorkflowSchedule.requestContext
// is NEVER forwarded verbatim into a fired leg. The tick STRIPS every reserved
// key: the whole `breakwater.` namespace and core's goal key, before handing
// the remainder to the start seam. The reserved set exactly matches
// #requestContextFor's keys: two scope keys and the grant key. A stripped
// context shares NO key with the runtime-derived context. There is normally
// nothing to collide. buildScheduledLegContext keeps the stored context FIRST
// and the runtime-derived context LAST. For a host applying stored context, a
// reserved key that slips the strip still LOSES to the runtime value. The DO
// target resolves this sanitized context from the exact prepared trigger
// snapshot. RunnerRuntime merges it below provider/runtime-derived values. It
// never trusts a forwarded body copy.

import { ScheduleInputSchema } from '@mastra/core/schedules';
import { computeNextFireAt } from '@mastra/core/workflows';
import type { ResourceOwnershipStore } from '../approval-api/index.js';
import {
  admitsWorkAuthoring,
  type ExecutionFenceWiring,
  isPathSafeId,
  isReservedExecutionContextKey,
  RESERVED_EXECUTION_CONTEXT_KEYS,
  readExecutionFence,
  resolveScheduleStartOwner,
  stripReservedExecutionContext,
} from '../do-runner/index.js';
import { nonnegativeSafeInteger } from '../numeric-config.js';
import type {
  Schedule,
  ScheduleAgentDispatchReceipt,
  ScheduleFireClaim,
  ScheduleTrigger,
} from './schedules-d1.js';
import {
  type ScheduleTargetPolicy,
  scheduleCreatorRole,
} from './target-policy.js';

/**
 * Compatibility alias for the former schedule-specific inventory. New code
 * should use RESERVED_EXECUTION_CONTEXT_KEYS from do-runner.
 */
export const RESERVED_SCHEDULE_CONTEXT_KEYS = RESERVED_EXECUTION_CONTEXT_KEYS;

/**
 * A key is reserved iff it is in the `breakwater.` namespace (covers all four
 * base keys AND any future breakwater key) or is core's goal key. A stored
 * schedule context carrying any of these would inject a standing capability /
 * objective into every woken run — the same stored-capability class as the
 * approval create-route leak. The three object meta-keys are also reserved so
 * parsed JSON cannot mutate a copied context's prototype.
 */
export function isReservedScheduleContextKey(key: string): boolean {
  return isReservedExecutionContextKey(key);
}

/** Drop every reserved key from a stored schedule context (barrier b's core job). */
export function stripReservedScheduleContext(
  stored: Record<string, unknown> | undefined,
): Record<string, unknown> {
  return stripReservedExecutionContext(stored);
}

/**
 * Build a fired leg's context: the stored NON-reserved keys UNDER the
 * runtime-derived context. Stored values are spread first and runtime-derived
 * values last so the runtime value wins on any collision — the same
 * last-spread-wins ordering
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
  claimScheduleFire(claim: ScheduleFireClaim): Promise<boolean>;
  recordTrigger(trigger: ScheduleTrigger): Promise<void>;
  /** Merge reconciliation metadata without replacing target-owned dispatch state. */
  touchDeferredTrigger(
    id: string,
    scheduleId: string,
    error: string | undefined,
    metadata: Record<string, unknown>,
  ): Promise<void>;
  listDeferredTriggers(limit?: number): Promise<ScheduleTrigger[]>;
}

/** Ownership-bearing workflow dispatch input. A schedule-aware host adapter
 * must resolve `scheduleId` to its registered owner before starting `runId`.
 */
export interface ScheduleTickStartInput {
  scheduleId: string;
  /** Prepared trigger that authorizes this one fire. */
  dispatchId: string;
  workflowId: string;
  runId: string;
  inputData: unknown;
  initialState: unknown;
  requestContext: Record<string, unknown>;
}

/** The required schedule-aware workflow run-start seam. */
export type ScheduleTickStart = (
  input: ScheduleTickStartInput,
) => Promise<{ runId: string; status?: string }>;

export type AgentScheduleTarget = Extract<
  Schedule['target'],
  { type: 'agent' }
>;
export type WorkflowScheduleTarget = Extract<
  Schedule['target'],
  { type: 'workflow' }
>;
export type ScheduleStartSourceTarget =
  | WorkflowScheduleTarget
  | AgentScheduleTarget;

export interface ScheduleTickStartAgentInput {
  /** The schedule whose owner must own all resources minted by this fire. */
  scheduleId: string;
  /** Prepared trigger that authorizes this one fire. */
  dispatchId: string;
  target: AgentScheduleTarget;
  /** Infrastructure-verified tag for audit attribution. */
  deploymentTag?: string;
  runId: string;
  /** Stable target DO address, including for an unthreaded fire. */
  topologyThreadId: string;
  threaded: boolean;
  entryPath: 'schedule.fire';
  requestContext: Record<string, unknown>;
  streamRequestContext: Record<string, unknown>;
  /** JSON-safe provider options with authoritative schedule correlation. */
  providerOptions: Record<string, unknown>;
}

export type ScheduleTickStartAgent = (
  input: ScheduleTickStartAgentInput,
) => Promise<{ runId: string; status?: string }>;

export interface ScheduleTickSignalAgentInput
  extends ScheduleTickStartAgentInput {
  threaded: true;
}

/** Target-thread signal dispatch. Direct starts are reserved for threadless targets. */
export type ScheduleTickSignalAgent = (
  input: ScheduleTickSignalAgentInput,
) => Promise<ScheduleAgentDispatchReceipt>;

/**
 * Authorize durable persistence requested by a schedule while keeping the
 * executing principal as SYSTEM. The schedule and its fixed thread/resource
 * must share one committed registered owner.
 */
export async function canPersistScheduledAgentSignal(
  store: ScheduleStartSource,
  resources: Pick<ResourceOwnershipStore, 'owner'>,
  input: {
    scheduleId: string;
    dispatchId: string;
    runId: string;
    agentId: string;
    threadId: string;
    resourceId: string;
  },
): Promise<boolean> {
  return (
    (await resolveScheduleStartOwner(
      store,
      resources,
      input.scheduleId,
      input.dispatchId,
      input.runId,
      {
        type: 'agent',
        mode: 'threaded-signal',
        agentId: input.agentId,
        threadId: input.threadId,
        resourceId: input.resourceId,
      },
    )) !== undefined
  );
}

export type ScheduleTickDispatchRef =
  | {
      scheduleId: string;
      dispatchId: string;
      target: 'workflow';
      workflowId: string;
      runId: string;
      /** Canonical target snapshot claimed for this exact fire. */
      workflowTarget: WorkflowScheduleTarget;
    }
  | {
      scheduleId: string;
      dispatchId: string;
      target: 'agent';
      agentId: string;
      threadId: string;
      runId: string;
      mode: 'start';
      /** Canonical target snapshot claimed for this exact fire. */
      agentTarget: AgentScheduleTarget;
    }
  | {
      scheduleId: string;
      dispatchId: string;
      target: 'agent';
      agentId: string;
      threadId: string;
      runId: string;
      mode: 'signal';
      /** Canonical target needed to retry an indeterminate at-least-once dispatch. */
      agentTarget: AgentScheduleTarget;
    };

export interface ScheduleTickStatusResult {
  runId?: string;
  status?: string;
  dispatchReceipt?: ScheduleAgentDispatchReceipt;
}

export type ScheduleTickStatus = (
  input: ScheduleTickDispatchRef,
) => Promise<ScheduleTickStatusResult | undefined>;

/**
 * The injectable deployment run cap: return false to refuse an
 * unattended start (a capped fire is skipped + audited, the schedule stays
 * healthy). Async so a D1/KV-backed cap fits. Absent ⇒ uncapped.
 */
export type ScheduleTickRunCap = () => boolean | Promise<boolean>;

/** Outcome of one due-schedule fire attempt in a tick pass. */
export type ScheduleFireOutcome =
  | 'published' // a workflow run was dispatched
  | 'succeeded' // a threaded schedule woke an idle run
  | 'delivered' // a threaded schedule joined an active run
  | 'persisted' // a threaded schedule durably queued its signal
  | 'discarded' // a threaded schedule deliberately dropped its signal
  | 'deferred' // dispatch may have committed; status reconciliation will decide
  | 'skipped' // agent start unavailable OR run-capped (deliberate non-fire)
  | 'failed' // invalid stored target OR the start seam threw
  | 'lost'; // the row advanced, paused, or disappeared before the claim

/** The structured audit event a tick emits per fire attempt (accepted OR not). */
export interface ScheduleTickAuditEvent {
  type: 'schedule.fire';
  scheduleId: string;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
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
  /** The same catalog policy enforced at create/update, rechecked per fire. */
  targetPolicy: ScheduleTargetPolicy;
  /** A schedule-aware run-start adapter that preserves registered ownership. */
  start: ScheduleTickStart;
  /** Runtime-driven agent start. Absent preserves the guarded skip. */
  startAgent?: ScheduleTickStartAgent;
  /** Thread-targeted agent signal dispatch with a durable target receipt. */
  signalAgent?: ScheduleTickSignalAgent;
  /** Authoritative target status used after a lost/failed dispatch response. */
  status: ScheduleTickStatus;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /** The deployment run cap. Absent means uncapped. */
  runCap?: ScheduleTickRunCap;
  /** Every fire attempt is audited through this (accepted OR skipped/failed). Absent ⇒ no audit. */
  audit?: ScheduleTickAuditSink;
  /**
   * The deployment execution fence, read ONCE per pass and BEFORE any CAS
   * claim, or `'none'` for a tick with no database behind it.
   *
   * REQUIRED, and this is the option the requirement exists for: a fenced pass
   * must do nothing at all, because claiming a fire it will not run CONSUMES it
   * — the claim advances `nextFireAt` — and the fenced runtime then refuses the
   * start, so the fire is lost rather than deferred. A host that wires the
   * runtime's fence but forgets this one gets exactly that, and nothing reports
   * it. See ExecutionFenceWiring.
   */
  executionFence: ExecutionFenceWiring;
  /**
   * Max due schedules processed per tick pass. Must be a nonnegative safe
   * integer; zero is an intentional no-op. Default 100.
   */
  limit?: number;
  /** Clock override for tests. */
  now?: () => number;
}

/** Per-pass tally the tick returns for its maintenance log line. */
export interface ScheduleTickResult {
  /** Due schedules the pass considered. */
  due: number;
  /** Workflow or agent runs dispatched. */
  fired: number;
  /** Deliberate non-fires (agent start unavailable or run-capped). */
  skipped: number;
  /** Errors (invalid target + start threw); the schedule was still advanced. */
  failed: number;
  /** Dispatches whose commit state is still unknown and will be reconciled. */
  deferred: number;
  /** Earlier deferred triggers resolved during this pass. */
  reconciled: number;
  /** CAS claims lost because the row advanced, paused, or disappeared. */
  lost: number;
}

function mintPathSafeId(label: string): string {
  const id = crypto.randomUUID();
  if (!isPathSafeId(id)) {
    throw new Error(`${label}: generated id must be URL-path-safe`);
  }
  return id;
}

function contextObject(
  value: unknown,
): Record<string, unknown> | undefined | null {
  if (value === undefined) return undefined;
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function normalizeAndSanitizeWorkflowTarget(
  stored: WorkflowScheduleTarget,
): WorkflowScheduleTarget | undefined {
  if (!isPathSafeId(stored.workflowId)) return undefined;
  const requestContext = contextObject(stored.requestContext);
  if (requestContext === null) return undefined;
  return {
    type: 'workflow',
    workflowId: stored.workflowId,
    ...(stored.inputData !== undefined ? { inputData: stored.inputData } : {}),
    ...(stored.initialState !== undefined
      ? { initialState: stored.initialState }
      : {}),
    ...(requestContext !== undefined
      ? { requestContext: stripReservedScheduleContext(requestContext) }
      : {}),
  };
}

export function normalizeAndSanitizeAgentTarget(
  scheduleId: string,
  stored: AgentScheduleTarget,
): AgentScheduleTarget | undefined {
  if (
    !isPathSafeId(stored.agentId) ||
    typeof stored.prompt !== 'string' ||
    stored.prompt.length === 0 ||
    (stored.name !== undefined && typeof stored.name !== 'string')
  ) {
    return undefined;
  }
  const legacyContext = contextObject(stored.requestContext);
  if (legacyContext === null) return undefined;

  const rawIdle = stored.ifIdle;
  let normalizedIdle = rawIdle;
  if (
    rawIdle !== undefined &&
    typeof rawIdle === 'object' &&
    rawIdle !== null &&
    !Array.isArray(rawIdle) &&
    rawIdle.streamOptions !== undefined
  ) {
    const rawStreamOptions = rawIdle.streamOptions;
    if (
      typeof rawStreamOptions !== 'object' ||
      rawStreamOptions === null ||
      Array.isArray(rawStreamOptions)
    ) {
      return undefined;
    }
    const streamContext = contextObject(rawStreamOptions.requestContext);
    if (streamContext === null) return undefined;
    normalizedIdle = {
      ...rawIdle,
      streamOptions: {
        ...rawStreamOptions,
        ...(streamContext !== undefined
          ? {
              requestContext: stripReservedScheduleContext(streamContext),
            }
          : {}),
      },
    };
  }

  const parsed = ScheduleInputSchema.safeParse({
    scheduleId,
    agentId: stored.agentId,
    prompt: stored.prompt,
    threadId: stored.threadId,
    resourceId: stored.resourceId,
    signalType: stored.signalType,
    tagName: stored.tagName,
    attributes: stored.attributes,
    providerOptions: stored.providerOptions,
    ifActive: stored.ifActive,
    ifIdle: normalizedIdle,
  });
  if (!parsed.success) return undefined;

  const {
    scheduleId: _scheduleId,
    agentId,
    prompt,
    ...runtimeFields
  } = parsed.data;
  if (
    (runtimeFields.threadId !== undefined &&
      !isPathSafeId(runtimeFields.threadId)) ||
    (runtimeFields.resourceId !== undefined &&
      !isPathSafeId(runtimeFields.resourceId)) ||
    (runtimeFields.threadId !== undefined &&
      runtimeFields.resourceId === undefined) ||
    (runtimeFields.threadId === undefined &&
      (runtimeFields.resourceId !== undefined ||
        runtimeFields.signalType !== undefined ||
        runtimeFields.ifActive !== undefined ||
        runtimeFields.ifIdle !== undefined))
  ) {
    return undefined;
  }
  return {
    type: 'agent',
    agentId,
    prompt,
    ...(stored.name !== undefined ? { name: stored.name } : {}),
    ...runtimeFields,
    ...(legacyContext !== undefined
      ? { requestContext: stripReservedScheduleContext(legacyContext) }
      : {}),
  };
}

function agentScheduleProviderOptions(
  scheduleId: string,
  target: AgentScheduleTarget,
): Record<string, unknown> {
  const base = target.providerOptions ?? {};
  const mastra =
    typeof base.mastra === 'object' &&
    base.mastra !== null &&
    !Array.isArray(base.mastra)
      ? (base.mastra as Record<string, unknown>)
      : {};
  return {
    ...base,
    mastra: {
      ...mastra,
      schedule: {
        scheduleId,
        ...(target.threadId !== undefined ? { threadId: target.threadId } : {}),
      },
    },
  };
}

function scheduleTickDispatchRef(
  value: unknown,
): ScheduleTickDispatchRef | undefined {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined;
  }
  const candidate = value as Record<string, unknown>;
  if (
    !isPathSafeId(candidate.scheduleId) ||
    !isPathSafeId(candidate.dispatchId) ||
    !isPathSafeId(candidate.runId)
  ) {
    return undefined;
  }
  if (candidate.target === 'workflow' && isPathSafeId(candidate.workflowId)) {
    const stored = candidate.workflowTarget;
    if (
      stored === null ||
      typeof stored !== 'object' ||
      Array.isArray(stored) ||
      (stored as { type?: unknown }).type !== 'workflow'
    ) {
      return undefined;
    }
    const workflowTarget = normalizeAndSanitizeWorkflowTarget(
      stored as WorkflowScheduleTarget,
    );
    if (!workflowTarget || workflowTarget.workflowId !== candidate.workflowId) {
      return undefined;
    }
    return {
      scheduleId: candidate.scheduleId,
      dispatchId: candidate.dispatchId,
      target: 'workflow',
      workflowId: candidate.workflowId,
      runId: candidate.runId,
      workflowTarget,
    };
  }
  if (
    candidate.target !== 'agent' ||
    !isPathSafeId(candidate.agentId) ||
    !isPathSafeId(candidate.threadId) ||
    (candidate.mode !== 'start' && candidate.mode !== 'signal')
  ) {
    return undefined;
  }
  const stored = candidate.agentTarget;
  if (
    stored === null ||
    typeof stored !== 'object' ||
    Array.isArray(stored) ||
    (stored as { type?: unknown }).type !== 'agent'
  ) {
    return undefined;
  }
  const agentTarget = normalizeAndSanitizeAgentTarget(
    candidate.scheduleId,
    stored as AgentScheduleTarget,
  );
  if (
    !agentTarget ||
    agentTarget.agentId !== candidate.agentId ||
    (candidate.mode === 'start' && agentTarget.threadId !== undefined) ||
    (candidate.mode === 'signal' && agentTarget.threadId !== candidate.threadId)
  ) {
    return undefined;
  }
  return {
    scheduleId: candidate.scheduleId,
    dispatchId: candidate.dispatchId,
    target: 'agent',
    agentId: candidate.agentId,
    threadId: candidate.threadId,
    runId: candidate.runId,
    mode: candidate.mode,
    agentTarget,
  };
}

/** Narrow prepared-trigger read required by target-side schedule dispatch. */
export interface ScheduleStartSourceStore {
  getClaimedScheduleDispatch(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<ScheduleTrigger | null>;
}

/** Target-side source consumed structurally by run and thread Durable Objects. */
export interface ScheduleStartSource {
  resolveScheduleTarget(
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ): Promise<ScheduleStartSourceTarget | undefined>;
}

/**
 * Resolve the canonical target snapshot bound to one atomically claimed fire.
 * A reusable schedule id alone never authorizes a start.
 */
export function createScheduleStartSource(
  store: ScheduleStartSourceStore,
): ScheduleStartSource {
  return {
    async resolveScheduleTarget(scheduleId, dispatchId, runId) {
      const trigger = await store.getClaimedScheduleDispatch(
        scheduleId,
        dispatchId,
        runId,
      );
      const metadata = trigger?.metadata;
      if (
        trigger?.id !== dispatchId ||
        trigger.scheduleId !== scheduleId ||
        trigger.runId !== runId ||
        trigger.outcome !== 'deferred' ||
        !metadata ||
        !['prepared', 'executing', 'settled'].includes(
          String(metadata.dispatchState),
        )
      ) {
        return undefined;
      }
      const ref = scheduleTickDispatchRef(metadata.dispatchRef);
      if (
        !ref ||
        ref.scheduleId !== scheduleId ||
        ref.dispatchId !== dispatchId ||
        ref.runId !== runId
      ) {
        return undefined;
      }
      return ref.target === 'workflow'
        ? ref.workflowTarget
        : {
            ...ref.agentTarget,
            providerOptions: agentScheduleProviderOptions(
              scheduleId,
              ref.agentTarget,
            ),
          };
    },
  };
}

/**
 * Build the schedule tick: a `() => Promise<ScheduleTickResult>` a host slots
 * into its alarm dispatch as its OWN failure-isolated duty (own try/catch, own
 * log line — the purge-availability lesson). Each due schedule is processed
 * independently: one bad row (a start throw, a corrupt cron) is contained and the
 * rest still fire.
 */
export function createScheduleTick(
  options: ScheduleTickOptions,
): () => Promise<ScheduleTickResult> {
  const { store, start } = options;
  const now = options.now ?? Date.now;
  const limit = nonnegativeSafeInteger(
    options.limit ?? 100,
    'schedule tick limit',
  );

  const audit = async (
    event: Omit<ScheduleTickAuditEvent, 'type' | 'timestamp'>,
  ): Promise<void> => {
    if (!options.audit) return;
    await options.audit({
      type: 'schedule.fire',
      timestamp: new Date().toISOString(),
      ...(options.deploymentTag !== undefined
        ? { deploymentTag: options.deploymentTag }
        : {}),
      ...event,
    });
  };

  const triggerMetadata = (
    extra: Record<string, unknown> = {},
  ): Record<string, unknown> => ({
    ...(options.deploymentTag !== undefined
      ? { deploymentTag: options.deploymentTag }
      : {}),
    ...extra,
  });

  const recordResolvedDispatch = async (
    ref: ScheduleTickDispatchRef,
    trigger: ScheduleTrigger,
    summary: ScheduleTickStatusResult | undefined,
    result: ScheduleTickResult,
  ): Promise<void> => {
    const published = summary !== undefined;
    const receipt = summary?.dispatchReceipt;
    const outcome = receipt?.outcome ?? (published ? 'published' : 'failed');
    const resolvedRunId = receipt?.runId ?? summary?.runId ?? ref.runId;
    const { error: priorError, ...resolvedTrigger } = trigger;
    if (!published) result.failed += 1;
    else if (
      receipt &&
      (receipt.outcome === 'persisted' ||
        receipt.outcome === 'discarded' ||
        receipt.outcome === 'skipped')
    ) {
      result.skipped += 1;
    } else {
      result.fired += 1;
    }
    try {
      await store.recordTrigger({
        ...resolvedTrigger,
        runId: published ? resolvedRunId : ref.runId,
        outcome,
        ...(published ? {} : { error: priorError ?? 'start failed' }),
        metadata: triggerMetadata({
          ...(trigger.metadata ?? {}),
          dispatchRef: ref,
          reason: published ? 'dispatch-reconciled' : 'start-error-confirmed',
        }),
      });
      await audit({
        scheduleId: ref.scheduleId,
        target: ref.target,
        outcome,
        runId: resolvedRunId,
        ...(published ? {} : { reason: 'start-error-confirmed' }),
      });
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'schedule-tick-bookkeeping-error',
          scheduleId: ref.scheduleId,
          runId: ref.runId,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  };

  const reconcileDeferred = async (
    result: ScheduleTickResult,
  ): Promise<void> => {
    const pending = await store.listDeferredTriggers(limit);
    for (const trigger of pending) {
      const ref = scheduleTickDispatchRef(trigger.metadata?.dispatchRef);
      if (!ref) {
        result.failed += 1;
        result.reconciled += 1;
        await store.recordTrigger({
          ...trigger,
          outcome: 'failed',
          error: 'stored deferred dispatch is malformed',
          metadata: triggerMetadata({ reason: 'invalid-deferred-dispatch' }),
        });
        continue;
      }
      try {
        const summary = await options.status(ref);
        result.reconciled += 1;
        await recordResolvedDispatch(ref, trigger, summary, result);
      } catch (error) {
        let pendingError = error;
        if (
          ref.target === 'agent' &&
          ref.mode === 'signal' &&
          options.signalAgent
        ) {
          try {
            const receipt = await options.signalAgent({
              scheduleId: ref.scheduleId,
              target: ref.agentTarget,
              ...(options.deploymentTag !== undefined
                ? { deploymentTag: options.deploymentTag }
                : {}),
              dispatchId: ref.dispatchId,
              runId: ref.runId,
              topologyThreadId: ref.threadId,
              threaded: true,
              entryPath: 'schedule.fire',
              requestContext: ref.agentTarget.requestContext ?? {},
              streamRequestContext:
                ref.agentTarget.ifIdle?.streamOptions?.requestContext ?? {},
              providerOptions: agentScheduleProviderOptions(
                ref.scheduleId,
                ref.agentTarget,
              ),
            });
            result.reconciled += 1;
            await recordResolvedDispatch(
              ref,
              trigger,
              {
                ...(receipt.runId !== undefined
                  ? { runId: receipt.runId }
                  : {}),
                dispatchReceipt: receipt,
              },
              result,
            );
            continue;
          } catch (retryError) {
            pendingError = retryError;
          }
        }
        result.deferred += 1;
        const priorAttempts = trigger.metadata?.reconcileAttempts;
        const attempts =
          typeof priorAttempts === 'number' &&
          Number.isSafeInteger(priorAttempts) &&
          priorAttempts >= 0
            ? priorAttempts
            : 0;
        const priorAfter = trigger.metadata?.reconcileAfter;
        const reconcileAfter = Math.max(
          now(),
          typeof priorAfter === 'number' && Number.isSafeInteger(priorAfter)
            ? priorAfter + 1
            : trigger.actualFireAt + 1,
        );
        try {
          await store.touchDeferredTrigger(
            trigger.id ?? '',
            trigger.scheduleId,
            trigger.error,
            triggerMetadata({
              reconcileAttempts: attempts + 1,
              reconcileAfter,
              statusError:
                pendingError instanceof Error
                  ? pendingError.message
                  : String(pendingError),
            }),
          );
        } catch (bookkeepingError) {
          console.error(
            JSON.stringify({
              type: 'schedule-tick-bookkeeping-error',
              scheduleId: ref.scheduleId,
              runId: ref.runId,
              error:
                bookkeepingError instanceof Error
                  ? bookkeepingError.message
                  : String(bookkeepingError),
            }),
          );
        }
      }
    }
  };

  const reconcileStartError = async (
    ref: ScheduleTickDispatchRef,
    trigger: ScheduleTrigger,
    error: unknown,
    result: ScheduleTickResult,
  ): Promise<void> => {
    try {
      const summary = await options.status(ref);
      await recordResolvedDispatch(ref, trigger, summary, result);
    } catch (statusError) {
      result.deferred += 1;
      try {
        await store.touchDeferredTrigger(
          trigger.id ?? '',
          trigger.scheduleId,
          error instanceof Error ? error.message : String(error),
          triggerMetadata({
            dispatchRef: ref,
            reason: 'dispatch-indeterminate',
            reconcileAttempts: 0,
            reconcileAfter: trigger.actualFireAt,
            statusError:
              statusError instanceof Error
                ? statusError.message
                : String(statusError),
          }),
        );
        await audit({
          scheduleId: ref.scheduleId,
          target: ref.target,
          outcome: 'deferred',
          runId: ref.runId,
          reason: 'dispatch-indeterminate',
        });
      } catch (bookkeepingError) {
        console.error(
          JSON.stringify({
            type: 'schedule-tick-bookkeeping-error',
            scheduleId: ref.scheduleId,
            runId: ref.runId,
            error:
              bookkeepingError instanceof Error
                ? bookkeepingError.message
                : String(bookkeepingError),
          }),
        );
      }
    }
  };

  const prepareDispatch = async (
    ref: ScheduleTickDispatchRef,
    trigger: ScheduleFireClaim['trigger'],
    result: ScheduleTickResult,
  ): Promise<boolean> => {
    try {
      await store.recordTrigger({
        ...trigger,
        metadata: triggerMetadata({
          ...(trigger.metadata ?? {}),
          dispatchRef: ref,
          dispatchState: 'prepared',
          reason: 'dispatch-indeterminate',
        }),
      });
      return true;
    } catch (error) {
      result.deferred += 1;
      console.error(
        JSON.stringify({
          type: 'schedule-tick-bookkeeping-error',
          scheduleId: ref.scheduleId,
          runId: ref.runId,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
      return false;
    }
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

    // 2. Mint the provisional runId before the CAS so lastRunId identifies this
    // claim even when a cap later consumes it without dispatch.
    const runId = mintPathSafeId('scheduleTick');

    // 3. CAS claim — the single-claim gate. The LOSER of two
    // concurrent ticks gets false here and dispatches nothing. This serializes
    // the claim only; downstream dispatch remains recoverable/at-least-once.
    // The cap is consulted AFTER the claim, so `runId` is minted before
    // we know whether the fire will actually dispatch: on a capped fire the row's
    // `lastRunId` records this CLAIM's id though no run started. The trigger row
    // is the authoritative record of what happened (outcome + its own runId,
    // null for a skip); `lastRunId` is a best-effort last-claim marker, not a
    // guarantee the run was dispatched.
    const claimTrigger: ScheduleFireClaim['trigger'] = {
      id: mintPathSafeId('scheduleTrigger'),
      scheduleId: schedule.id,
      runId,
      scheduledFireAt: schedule.nextFireAt,
      actualFireAt: at,
      outcome: 'deferred',
      error: 'dispatch has not reached an authoritative target state',
      metadata: triggerMetadata({
        reason: 'dispatch-preparing',
        reconcileAttempts: 0,
        reconcileAfter: at,
      }),
    };
    const claimed = await store.claimScheduleFire({
      scheduleId: schedule.id,
      expectedNextFireAt: schedule.nextFireAt,
      newNextFireAt,
      actualFireAt: at,
      runId,
      trigger: claimTrigger,
    });
    if (!claimed) {
      result.lost += 1;
      const current = await store.getSchedule(schedule.id);
      const reason = !current
        ? 'disappeared'
        : current.status === 'paused'
          ? 'paused'
          : 'concurrent-claim';
      const lostReason = `lost: ${reason}`;
      await audit({
        scheduleId: schedule.id,
        target: targetType,
        outcome: 'lost',
        reason: lostReason,
      });
      return;
    }

    // 4. We own this fire — nextFireAt is already advanced (consumed, never
    // hot-looped), whatever happens below.
    let targetDecision:
      | ReturnType<ScheduleTargetPolicy['authorize']>
      | undefined;
    try {
      targetDecision = options.targetPolicy.authorize(
        schedule.target,
        scheduleCreatorRole(schedule),
      );
    } catch {
      // A legacy/corrupt row with no creator role is never dispatched.
    }
    if (!targetDecision?.allowed) {
      const reason = targetDecision?.reason ?? 'invalid-creator-role';
      result.failed += 1;
      await store.recordTrigger({
        ...claimTrigger,
        runId: null,
        outcome: 'failed',
        error: reason,
        metadata: triggerMetadata({ reason }),
      });
      await audit({
        scheduleId: schedule.id,
        target: targetType,
        outcome: 'failed',
        reason,
      });
      return;
    }

    if (targetType === 'agent') {
      const target = normalizeAndSanitizeAgentTarget(
        schedule.id,
        schedule.target,
      );
      if (!target) {
        result.failed += 1;
        await store.recordTrigger({
          ...claimTrigger,
          runId: null,
          outcome: 'failed',
          error: 'invalid agent target',
          metadata: triggerMetadata({ reason: 'invalid-agent-target' }),
        });
        await audit({
          scheduleId: schedule.id,
          target: 'agent',
          outcome: 'failed',
          reason: 'invalid-agent-target',
        });
        return;
      }
      if (
        (target.threadId !== undefined && !isPathSafeId(target.threadId)) ||
        (target.resourceId !== undefined && !isPathSafeId(target.resourceId)) ||
        (target.threadId !== undefined && target.resourceId === undefined)
      ) {
        result.failed += 1;
        await store.recordTrigger({
          ...claimTrigger,
          runId: null,
          outcome: 'failed',
          error: 'invalid memory id',
          metadata: triggerMetadata({ reason: 'invalid-memory-id' }),
        });
        await audit({
          scheduleId: schedule.id,
          target: 'agent',
          outcome: 'failed',
          reason: 'invalid-memory-id',
        });
        return;
      }
      if (
        target.threadId === undefined &&
        (target.resourceId !== undefined ||
          target.signalType !== undefined ||
          target.ifActive !== undefined ||
          target.ifIdle !== undefined)
      ) {
        result.failed += 1;
        await store.recordTrigger({
          ...claimTrigger,
          runId: null,
          outcome: 'failed',
          error: 'invalid agent target',
          metadata: triggerMetadata({ reason: 'invalid-agent-target' }),
        });
        await audit({
          scheduleId: schedule.id,
          target: 'agent',
          outcome: 'failed',
          reason: 'invalid-agent-target',
        });
        return;
      }

      const firedRunId = runId;
      const threaded = target.threadId !== undefined;
      const topologyThreadId =
        target.threadId ?? mintPathSafeId('scheduleThread');
      const providerOptions = agentScheduleProviderOptions(schedule.id, target);

      if (threaded) {
        if (!options.signalAgent) {
          result.skipped += 1;
          await store.recordTrigger({
            ...claimTrigger,
            runId: null,
            outcome: 'skipped',
            error: undefined,
            metadata: triggerMetadata({ reason: 'agent-target-unsupported' }),
          });
          await audit({
            scheduleId: schedule.id,
            target: 'agent',
            outcome: 'skipped',
            reason: 'agent-target-unsupported',
          });
          return;
        }
        const dispatch: ScheduleTickDispatchRef = {
          scheduleId: schedule.id,
          target: 'agent',
          agentId: target.agentId,
          threadId: topologyThreadId,
          runId: firedRunId,
          mode: 'signal',
          dispatchId: claimTrigger.id,
          agentTarget: target,
        };
        if (!(await prepareDispatch(dispatch, claimTrigger, result))) return;
        let receipt: ScheduleAgentDispatchReceipt;
        try {
          receipt = await options.signalAgent({
            scheduleId: schedule.id,
            target,
            ...(options.deploymentTag !== undefined
              ? { deploymentTag: options.deploymentTag }
              : {}),
            dispatchId: claimTrigger.id,
            runId: firedRunId,
            topologyThreadId,
            threaded: true,
            entryPath: 'schedule.fire',
            requestContext: target.requestContext ?? {},
            streamRequestContext:
              target.ifIdle?.streamOptions?.requestContext ?? {},
            providerOptions,
          });
        } catch (error) {
          await reconcileStartError(
            dispatch,
            {
              ...claimTrigger,
              error: error instanceof Error ? error.message : String(error),
              metadata: triggerMetadata({ dispatchRef: dispatch }),
            },
            error,
            result,
          );
          return;
        }
        if (
          receipt.outcome === 'persisted' ||
          receipt.outcome === 'discarded' ||
          receipt.outcome === 'skipped'
        ) {
          result.skipped += 1;
        } else {
          result.fired += 1;
        }
        try {
          await store.recordTrigger({
            ...claimTrigger,
            runId: receipt.runId ?? null,
            outcome: receipt.outcome,
            error: undefined,
            metadata: triggerMetadata({
              action: receipt.action,
              ...(receipt.signalId !== undefined
                ? { signalId: receipt.signalId }
                : {}),
            }),
          });
          await audit({
            scheduleId: schedule.id,
            target: 'agent',
            outcome: receipt.outcome,
            ...(receipt.runId !== undefined ? { runId: receipt.runId } : {}),
          });
        } catch (error) {
          console.error(
            JSON.stringify({
              type: 'schedule-tick-bookkeeping-error',
              scheduleId: schedule.id,
              runId: receipt.runId ?? firedRunId,
              error: error instanceof Error ? error.message : String(error),
            }),
          );
        }
        return;
      }

      if (!options.startAgent) {
        result.skipped += 1;
        await store.recordTrigger({
          ...claimTrigger,
          runId: null,
          outcome: 'skipped',
          error: undefined,
          metadata: triggerMetadata({ reason: 'agent-target-unsupported' }),
        });
        await audit({
          scheduleId: schedule.id,
          target: 'agent',
          outcome: 'skipped',
          reason: 'agent-target-unsupported',
        });
        return;
      }
      const allowed = options.runCap ? await options.runCap() : true;
      if (!allowed) {
        result.skipped += 1;
        await store.recordTrigger({
          ...claimTrigger,
          runId: null,
          outcome: 'skipped',
          error: undefined,
          metadata: triggerMetadata({ reason: 'run-capped' }),
        });
        await audit({
          scheduleId: schedule.id,
          target: 'agent',
          outcome: 'skipped',
          reason: 'run-capped',
        });
        return;
      }
      const dispatch: ScheduleTickDispatchRef = {
        scheduleId: schedule.id,
        dispatchId: claimTrigger.id,
        target: 'agent',
        agentId: target.agentId,
        threadId: topologyThreadId,
        runId: firedRunId,
        mode: 'start',
        agentTarget: target,
      };
      if (!(await prepareDispatch(dispatch, claimTrigger, result))) return;
      let summary: { runId: string };
      try {
        summary = await options.startAgent({
          scheduleId: schedule.id,
          dispatchId: claimTrigger.id,
          target,
          ...(options.deploymentTag !== undefined
            ? { deploymentTag: options.deploymentTag }
            : {}),
          runId: firedRunId,
          topologyThreadId,
          threaded,
          entryPath: 'schedule.fire',
          requestContext: target.requestContext ?? {},
          streamRequestContext:
            target.ifIdle?.streamOptions?.requestContext ?? {},
          providerOptions,
        });
      } catch (error) {
        await reconcileStartError(
          dispatch,
          {
            ...claimTrigger,
            error: error instanceof Error ? error.message : String(error),
            metadata: triggerMetadata({ dispatchRef: dispatch }),
          },
          error,
          result,
        );
        return;
      }
      const dispatchedRunId = summary.runId ?? firedRunId;
      result.fired += 1;
      try {
        await store.recordTrigger({
          ...claimTrigger,
          runId: dispatchedRunId,
          outcome: 'published',
          error: undefined,
          metadata: triggerMetadata(),
        });
        await audit({
          scheduleId: schedule.id,
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

    const target = normalizeAndSanitizeWorkflowTarget(schedule.target);
    if (!target) {
      result.failed += 1;
      await store.recordTrigger({
        ...claimTrigger,
        runId: null,
        outcome: 'failed',
        error: 'invalid workflow target',
        metadata: triggerMetadata({ reason: 'invalid-workflow-target' }),
      });
      await audit({
        scheduleId: schedule.id,
        target: 'workflow',
        outcome: 'failed',
        reason: 'invalid-workflow-target',
      });
      return;
    }

    // 5. Workflow target: consult the run cap.
    const allowed = options.runCap ? await options.runCap() : true;
    if (!allowed) {
      // Capped: the schedule stays healthy (already advanced) and is audited.
      result.skipped += 1;
      await store.recordTrigger({
        ...claimTrigger,
        runId: null,
        outcome: 'skipped',
        error: undefined,
        metadata: triggerMetadata({ reason: 'run-capped' }),
      });
      await audit({
        scheduleId: schedule.id,
        target: 'workflow',
        outcome: 'skipped',
        reason: 'run-capped',
      });
      return;
    }

    // 7. Fire the workflow run (runId is defined by construction here).
    const firedRunId = runId;
    const dispatch: ScheduleTickDispatchRef = {
      scheduleId: schedule.id,
      dispatchId: claimTrigger.id,
      target: 'workflow',
      workflowId: target.workflowId,
      runId: firedRunId,
      workflowTarget: target,
    };
    if (!(await prepareDispatch(dispatch, claimTrigger, result))) return;
    // The try wraps ONLY the start() dispatch. A post-dispatch bookkeeping
    // failure (a transient recordTrigger/audit throw) must NOT be reclassified as
    // a start failure — that would double-count and audit a run that actually ran
    // as `failed`/`start-error`, corrupting the very trail operators rely on.
    let summary: { runId: string };
    try {
      summary = await start({
        scheduleId: schedule.id,
        dispatchId: claimTrigger.id,
        workflowId: target.workflowId,
        runId: firedRunId,
        inputData: target.inputData,
        initialState: target.initialState,
        requestContext: target.requestContext ?? {},
      });
    } catch (error) {
      await reconcileStartError(
        dispatch,
        {
          ...claimTrigger,
          error: error instanceof Error ? error.message : String(error),
          metadata: triggerMetadata({ dispatchRef: dispatch }),
        },
        error,
        result,
      );
      return;
    }
    // The run WAS dispatched. Record it as `published` (write-once at dispatch,
    // per core's ScheduleTriggerOutcome doc). Bookkeeping failures are logged
    // locally and do not reclassify the successful dispatch as a start failure.
    const dispatchedRunId = summary.runId ?? firedRunId;
    result.fired += 1;
    try {
      await store.recordTrigger({
        ...claimTrigger,
        runId: dispatchedRunId,
        outcome: 'published',
        error: undefined,
        metadata: triggerMetadata(),
      });
      await audit({
        scheduleId: schedule.id,
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

  const idlePass = (): ScheduleTickResult => ({
    due: 0,
    fired: 0,
    skipped: 0,
    failed: 0,
    deferred: 0,
    reconciled: 0,
    lost: 0,
  });

  return async () => {
    if (limit === 0) return idlePass();
    // ONE fence read per pass, before the due list and before any claim.
    // Nothing at all runs on a fenced pass — not the deferred reconciliation
    // either, since its agent-signal retry can dispatch. Every due fire stays
    // due, so it fires when the fence reopens: neither lost nor duplicated.
    //
    // A fence that cannot be read skips the pass too. This is a cron/alarm
    // path, so it degrades closed by DOING NOTHING and logging: throwing would
    // fail the maintenance duty, and proceeding would claim fires on a
    // deployment whose state is unknown.
    let admitted: boolean;
    try {
      admitted = admitsWorkAuthoring(
        await readExecutionFence(options.executionFence),
      );
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'schedule-tick-fence-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
      return idlePass();
    }
    if (!admitted) return idlePass();
    const at = now();
    const due = await store.listDueSchedules(at, limit);
    const result: ScheduleTickResult = {
      due: due.length,
      fired: 0,
      skipped: 0,
      failed: 0,
      deferred: 0,
      reconciled: 0,
      lost: 0,
    };
    await reconcileDeferred(result);
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
