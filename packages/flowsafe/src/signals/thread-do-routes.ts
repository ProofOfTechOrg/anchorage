// SPDX-License-Identifier: Apache-2.0
// The signal routes hosted ON the thread DO. A thread's agent loop and every
// signal both address `idFromName(threadId)`, so the platform serializes them
// onto ONE isolate. The DO IS the serialization lease Mastra otherwise wants
// Redis for. These routes run AFTER ThreadDurableObject.fetch verifies the
// deployment identity and decodes the server-stamped principal. The scope's
// threadId and principal are trusted here; the ingestion gate is the
// Worker-side createSignalRouter's job, the same split createRunRouter (Worker
// gate) → DurableObjectRunner (execution) uses.
//
// Core keys its in-process signal registry by pub/sub identity. A runtime-driven
// wrapper carries the thread Durable Object's pub/sub from construction; these
// routes refuse a configured identity mismatch. Other agents receive the
// thread's pub/sub before each call. Without a host pub/sub, other agents fall
// back to core's module default and runtime-driven wrappers use their own stream bus.
//
// core's `agentThreadStreamRuntime` is NOT on the package exports map, so these
// routes drive only the PUBLIC Agent methods, never a deep dist import across
// the export boundary.

import type {
  Agent,
  AgentMessageInput,
  AgentSignal,
  AgentSignalAttributes,
  AgentSignalIfIdleOptions,
  AgentStateSignalInput,
  SendAgentSignalOptions,
} from '@mastra/core/agent';
import {
  createMessageSignal,
  createSignal,
  resolveDeliveryAttributes,
  signalToXmlMarkup,
} from '@mastra/core/agent';
import {
  createNotificationSignal,
  createNotificationSummarySignal,
  type NotificationRecord,
  type NotificationsStorage,
  type SendNotificationSignalInput,
  summarizeNotifications,
  type UpdateNotificationInput,
} from '@mastra/core/notifications';
import { RequestContext } from '@mastra/core/request-context';
import { FLOWSAFE_PERSISTENCE_FORBIDDEN } from '../agent-runner/durable-agent-runner.js';
import {
  type AgentEntryPath,
  isRuntimeDrivenAgent,
} from '../agent-runner/index.js';
import {
  type ExecutionPrincipal,
  samePrincipal,
} from '../approval-api/index.js';
import {
  admitsExistingRun,
  type D1RunExecutionIdentity,
  DoStatusError,
  ExecutionFencedError,
  type ExecutionFenceReading,
  executionFencedResponse,
  isExecutionFenceRefusal,
  isPathSafeId,
  type RunStatus,
  readExecutionFence,
  type ThreadScope,
} from '../do-runner/index.js';
import { internalErrorResponse } from '../internal-error-response.js';
import { positiveSafeInteger } from '../numeric-config.js';
import {
  createScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchAction,
  type ScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchState,
} from '../schedules/schedules-d1.js';
import type { AgentScheduleTarget } from '../schedules/tick.js';
import {
  captureNotificationDeliverySelection,
  captureNotificationDeliveryStorage,
  DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
  MAX_NOTIFICATION_DISPATCH_IDS,
  type NotificationDeliveryObservation,
  planNotificationDispatch,
  recordNotificationDeliveryFailure,
  reportNotificationDeliveryError,
} from './notification-dispatch.js';

// The schedule tick settles a threaded fire as failed when this route answers
// one of the permanent statuses `ScheduleTickSignalAgent` documents, so a
// transient condition must not answer with one of them.
const SIGNAL_ROUTE_REFUSAL_MESSAGES: Partial<Record<number, string>> = {
  400: 'bad request',
  403: 'forbidden',
  404: 'not found',
  409: 'conflict',
};

/**
 * The idle-thread delivery behavior a send may ask for. `wake` starts a run
 * after consulting the run cap; `persist` writes the signal to the durable
 * inbox for the next host-started turn; `discard` drops it. Mirrors core
 * `AgentSignalIdleBehavior`, re-declared so the route body validates the wire
 * value rather than trusting an `as`.
 */
export const IDLE_BEHAVIORS = ['wake', 'persist', 'discard'] as const;
export type IdleBehavior = (typeof IDLE_BEHAVIORS)[number];

/** The active-thread delivery behavior. Mirrors core `AgentSignalActiveBehavior`. */
export const ACTIVE_BEHAVIORS = ['deliver', 'persist', 'discard'] as const;
export type ActiveBehavior = (typeof ACTIVE_BEHAVIORS)[number];

/**
 * Put the persistence prohibition in signal metadata so core preserves it
 * through the database round trip without showing it to the model. Attributes
 * are rendered into model-visible markup by `signalToXmlMarkup`; metadata is
 * not.
 */
function markPersistenceForbidden<
  T extends { metadata?: Record<string, unknown> },
>(input: T): T {
  return {
    ...input,
    metadata: {
      ...input.metadata,
      [FLOWSAFE_PERSISTENCE_FORBIDDEN]: true,
    },
  };
}

function activeThreadRunIdOf(
  agent: Agent,
  threadId: string,
  resourceId: string,
): string | undefined {
  return typeof agent.getActiveThreadRunId === 'function'
    ? agent.getActiveThreadRunId({ threadId, resourceId })
    : undefined;
}

/**
 * A run-cap consult for an idle-thread wake: starting a run with nobody
 * watching must charge the same deployment budget an unattended
 * schedule fire does, or a signal storm bills Cloudflare instead of exhausting a
 * quota. Returns false to REFUSE the wake (over cap). A refused or capped wake,
 * or an absent start seam, degrades to a durable persist when the principal may
 * persist and the agent has memory; otherwise the route answers
 * `persistence-forbidden` or `memory-unavailable`. Absent ⇒ wake is unmetered
 * (hosts with no budget).
 */
export type RunCapConsult = () => Promise<boolean> | boolean;

export interface StartIdleRunInput {
  agent: Agent;
  runId: string;
  threadId: string;
  resourceId?: string;
  /**
   * WHO is waking the run. Carries the KIND, so a host synthesizing a
   * ThreadScope for the idle run cannot downgrade automation to a human — which
   * would let it past the agent host's role branch without its agent ever
   * declaring the entry.
   */
  principal: ExecutionPrincipal;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  entryPath: AgentEntryPath;
  /** Authoritative source for a schedule.fire wake. */
  scheduleId?: string;
  /** Prepared trigger authorizing the scheduled wake's exact run id. */
  dispatchId?: string;
  message?: AgentMessageInput;
  signal?: AgentSignal;
  /** Sanitized context applied to a schedule-woken execution. */
  safeContext?: Record<string, unknown>;
}

export interface StartIdleRunResult {
  /** Authoritative run id; may differ when the start joined an active run. */
  runId: string;
  /** Authoritative start status; canceled lifecycle terminals become discard receipts. */
  status?: RunStatus;
  signalId?: string;
}

/**
 * Host-owned runtime-driven start on the thread DO. The host resolves the
 * registered owner of `input.threadId` and passes that owner to the target
 * agent host, which reserves the thread/resource/run claims before execution.
 * The automated principal that delivered a signal remains the requester and
 * does not replace the thread's human or service owner.
 */
export type StartIdleRun = (
  input: StartIdleRunInput,
) => Promise<StartIdleRunResult>;

/** Durable receipt protocol for target-side threaded schedule dispatch. */
export interface ScheduleSignalDispatchStore {
  begin(
    scheduleId: string,
    dispatchId: string,
  ): Promise<ScheduleAgentDispatchState>;
  settle(
    scheduleId: string,
    dispatchId: string,
    receipt: ScheduleAgentDispatchReceipt,
  ): Promise<void>;
}

export interface SignalContentPolicyInput {
  text: string;
  agentId: string;
  threadId: string;
  resourceId?: string;
  runId?: string;
  deploymentTag?: string;
  entryPath: AgentEntryPath;
  principal: ExecutionPrincipal;
}

export type SignalContentPolicyResult =
  | { allowed: true }
  | { allowed: false; outcome: 'denied' | 'error' };

export type SignalContentPolicy = (
  input: SignalContentPolicyInput,
) => SignalContentPolicyResult | Promise<SignalContentPolicyResult>;

/** Stored options and trusted target identity for a threaded schedule fire. */
export interface ScheduleProviderOptionsPolicyInput {
  providerOptions: unknown;
  agentId: string;
  threadId: string;
  resourceId: string;
}

/** Decides whether stored options may reach a threaded schedule signal. */
export type ScheduleProviderOptionsPolicy = (
  input: ScheduleProviderOptionsPolicyInput,
) => SignalContentPolicyResult | Promise<SignalContentPolicyResult>;

export interface ThreadSignalRoutesOptions {
  /**
   * The per-thread agent whose public signal methods these routes drive. Built
   * once per DO instance by the host (its model/memory/tools are the host's
   * concern); these routes only need its identity and pubsub.
   *
   * MUST be a runtime-driven durable agent for an idle wake. The host-provided
   * `startIdleRun` seam starts it through RunnerRuntime with a host-minted,
   * path-safe run id. Without either requirement, wake degrades to durable
   * persistence only when the principal may persist and the agent has memory;
   * otherwise the route returns `persistence-forbidden` or
   * `memory-unavailable`, and never escapes onto core's default execution
   * engine. Trusted notification dispatch supplies the persisted agent id.
   * When the thread's init() has a pub/sub, a runtime-driven agent must return
   * that same instance from getPubSub() or the routes answer 503. The routes
   * install the thread's pub/sub on a non-runtime-driven agent.
   */
  resolveAgent: (
    scope: ThreadScope,
    agentId: string | undefined,
    entryPath: AgentEntryPath,
  ) => Agent | Promise<Agent>;
  /**
   * The thread's host-owned memory resourceId — part of core's `(resourceId,
   * threadId)` signal key, so it MUST match whatever the loop registered under
   * or a send never finds the active run. Server-derived (a memory id is
   * TCB-only — never a client field); the host mints it from the authenticated
   * host. Delivery routes refuse with 409 when no resourceId resolves.
   */
  resolveResourceId?: (scope: ThreadScope) => string | undefined;
  /** Run-cap seam for idle-thread wakes. Absent means wakes are unmetered. */
  consultRunCap?: RunCapConsult;
  /**
   * Runtime-driven start seam. When absent, a requested wake degrades to a
   * durable persist when the principal may persist and the agent has memory;
   * otherwise the route answers `persistence-forbidden` or
   * `memory-unavailable`.
   */
  startIdleRun?: StartIdleRun;
  /**
   * Storage-backed thread occupancy, including runs surviving DO eviction.
   * `status` is the run's stored status when the host knows it. A delivery
   * into a run that is not executing persists to memory because a queued
   * delivery lives only in this isolate until the run drains it. A run that
   * reports no status is treated as running.
   */
  resolveBlockingRun?: (
    scope: ThreadScope,
  ) =>
    | Promise<
        | { runId: string; principal: ExecutionPrincipal; status?: RunStatus }
        | undefined
      >
    | { runId: string; principal: ExecutionPrincipal; status?: RunStatus }
    | undefined;
  /**
   * Host-shared target-thread critical section. Required with
   * `resolveBlockingRun`: the occupancy/principal check and signal action must
   * serialize with public agent start/resume routes.
   */
  serializeDispatch?: <T>(
    scope: ThreadScope,
    operation: () => Promise<T>,
  ) => Promise<T>;
  /** Recover a stable schedule-wake run after its HTTP receipt was lost. */
  resolveScheduleRunStatus?: (
    scope: ThreadScope,
    input: { agentId: string; resourceId: string; runId: string },
  ) => Promise<{ runId: string; status?: string } | undefined>;
  /** Resolve the canonical target bound to one prepared schedule fire. */
  resolveScheduleTarget?: (
    scope: ThreadScope,
    input: { scheduleId: string; dispatchId: string; runId: string },
  ) => Promise<AgentScheduleTarget | undefined>;
  /**
   * Whether this principal owns the thread and may persist input for a later
   * turn. Absent, every principal may, signal providers included. A principal
   * that may not persist is answered `persistence-forbidden` where a route
   * would persist its input, and its deliveries carry a marker that keeps the
   * runner's terminal refusal from persisting them.
   *
   * For `/signal/notification`, an owner's notification to a runtime-driven
   * agent is delivered at ingestion under the owner's principal, and a
   * non-owner's is recorded for the notification dispatch tick, which this
   * callback then judges as the dispatch principal. An unbranded agent's owner
   * notification follows Mastra's delivery policy.
   */
  canPersist?: (scope: ThreadScope) => boolean | Promise<boolean>;
  /**
   * Whether the registered schedule owner may persist to its fixed target.
   * When set, its answer is used in place of `canPersist`'s answer for schedule fires.
   */
  canPersistSchedule?: (
    scope: ThreadScope,
    input: {
      scheduleId: string;
      dispatchId: string;
      runId: string;
      agentId: string;
      threadId: string;
      resourceId: string;
    },
  ) => boolean | Promise<boolean>;
  /**
   * Durable inbox used by the trusted due-notification dispatch route. Required
   * for `/signal/notification` ingestion to a runtime-driven agent or from a
   * non-owner, which returns 409 without it.
   */
  resolveNotificationsStorage?: (
    scope: ThreadScope,
  ) => NotificationsStorage | Promise<NotificationsStorage>;
  /**
   * Consulted only for a non-owner `/signal/notification`. A false result
   * answers 409 with `notification-dispatch-forbidden` before the inbox write.
   * When absent, every non-owner notification is recorded for the dispatch
   * tick. A true result does not guarantee dispatch: the host's automated-entry
   * authorizer and required permissions still apply at the tick.
   */
  notificationDispatchAllowed?: (
    scope: ThreadScope,
    agentId: string,
  ) => boolean | Promise<boolean>;
  /** Failed delivery rounds before a pending notification is discarded. */
  maxDeliveryAttempts?: number;
  /** Target-side lease and receipt store for at-least-once schedule fires. */
  resolveScheduleDispatchStore?: (
    scope: ThreadScope,
  ) => ScheduleSignalDispatchStore | Promise<ScheduleSignalDispatchStore>;
  /**
   * Optional model-visible content gate. FlowSafe supplies only trusted route
   * identity plus Mastra's canonical escaped XML; the callback must return the
   * opaque structural result and is never given a request body or storage row.
   */
  contentPolicy?: SignalContentPolicy;
  /**
   * Receives stored options and target identity on every threaded agent-schedule
   * fire before signal creation. A denied result settles a discard; an error,
   * throw, or malformed result leaves the lease for a later tick. Without the
   * callback, stored options reach the signal unchecked.
   */
  scheduleProviderOptionsPolicy?: ScheduleProviderOptionsPolicy;
}

/**
 * A thread-DO signal router: `(request, scope) => Response | null`. `null` means
 * the path is not one of ours, so the subclass's `route()` can compose it ahead
 * of its own durable-agent routes. `scope` is the already-asserted
 * ThreadScope the template-method `fetch` hands down.
 */
export type ThreadSignalRouter = (
  request: Request,
  scope: ThreadScope,
) => Promise<Response | null>;

function entryPathForSignalRoute(path: string): AgentEntryPath | undefined {
  switch (path) {
    case '/signal/message':
      return 'signal.message';
    case '/signal/queue':
      return 'signal.queue';
    case '/signal':
      return 'signal.reactive';
    case '/signal/schedule':
      return 'schedule.fire';
    case '/signal/state':
      return 'signal.state';
    case '/signal/notification':
      return 'signal.notification';
    case '/signal/notifications/dispatch':
      return 'notification.dispatch';
    default:
      return undefined;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

async function readJson(
  request: Request,
): Promise<Record<string, unknown> | null> {
  try {
    const parsed = await request.json();
    return typeof parsed === 'object' && parsed !== null
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

/**
 * The wire surface accepts STRING contents only — the common case, and the one
 * the ingestion gate can size-cap and escape uniformly as defense in depth.
 * Core's multimodal `AgentSignalContents` array form (TextPart/FilePart) is a
 * documented residual, not exposed over this untrusted channel.
 */
function isContents(value: unknown): value is string {
  return typeof value === 'string';
}

/**
 * The XML-name rule core's `assertXmlName` applies to a signal's `tagName`
 * (and attribute names) inside `signalToXmlMarkup` — MIRRORED here, not
 * deep-imported: `XML_NAME_PATTERN` / `assertXmlName` are not on core's exports
 * map. Validating the caller-supplied `tagName` at INGEST turns an invalid one
 * into a 400 at the route, and an invalid attribute name into a dropped
 * attributes object (`isAttributes`), rather than a
 * throw at render time inside the agent turn. Kept byte-identical to core
 * (chunk `signalToXmlMarkup`: `/^[A-Za-z_][A-Za-z0-9_.-]*$/`).
 */
const XML_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_.-]*$/;

/**
 * Attributes must be a flat record of primitives (core `AgentSignalAttributes`)
 * whose rendered KEYS are XML names. Core's `signalAttributesToXml` drops
 * null/undefined entries and then asserts the name of every remaining key, so
 * an unrenderable key would otherwise throw at render time — inside the agent
 * turn, or here at the content gate — long after the route accepted it. A
 * malformed attributes object is dropped whole, exactly as a malformed
 * attribute VALUE already is.
 */
function isAttributes(value: unknown): value is AgentSignalAttributes {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  return Object.entries(value).every(
    ([key, v]) =>
      v === null ||
      v === undefined ||
      (XML_NAME_PATTERN.test(key) &&
        (typeof v === 'string' ||
          typeof v === 'number' ||
          typeof v === 'boolean')),
  );
}

/**
 * Whether already-typed attributes can be rendered — the key half of the
 * `isAttributes` rule, for server-owned records that arrive typed and so need
 * no value check. Absent attributes render fine.
 */
function renderableAttributes(
  attributes: AgentSignalAttributes | undefined,
): boolean {
  if (attributes === undefined) return true;
  return Object.entries(attributes).every(
    ([key, value]) =>
      value === null || value === undefined || XML_NAME_PATTERN.test(key),
  );
}

type SignalContentInspection = 'allowed' | 'denied' | 'error';
type RenderableSignal = Pick<
  AgentSignal,
  'type' | 'tagName' | 'attributes' | 'contents'
>;
type InspectSignalContent = (
  signal: RenderableSignal,
  runId?: string,
) => Promise<SignalContentInspection>;

async function inspectPolicyResult(
  invoke: () => SignalContentPolicyResult | Promise<SignalContentPolicyResult>,
): Promise<SignalContentInspection> {
  try {
    const result = await invoke();
    if (result?.allowed === true) return 'allowed';
    if (
      result?.allowed === false &&
      (result.outcome === 'denied' || result.outcome === 'error')
    ) {
      return result.outcome;
    }
  } catch {
    return 'error';
  }
  return 'error';
}

async function inspectSignalContent(
  policy: SignalContentPolicy,
  input: Omit<SignalContentPolicyInput, 'text' | 'runId'>,
  signal: RenderableSignal,
  runId?: string,
): Promise<SignalContentInspection> {
  if (typeof signal.contents !== 'string') return 'error';
  let text: string;
  try {
    text = signalToXmlMarkup({
      type: signal.type,
      tagName: signal.tagName,
      attributes: signal.attributes,
      contents: signal.contents,
    });
  } catch {
    return 'error';
  }
  return inspectPolicyResult(() =>
    policy({
      ...input,
      text,
      ...(runId !== undefined ? { runId } : {}),
    }),
  );
}

function signalContentPolicyResponse(
  inspection: Exclude<SignalContentInspection, 'allowed'>,
): Response {
  return inspection === 'denied'
    ? json({ error: 'signal content denied' }, 422)
    : json({ error: 'signal content policy unavailable' }, 503);
}

export function createThreadSignalRoutes(
  options: ThreadSignalRoutesOptions,
): ThreadSignalRouter {
  const maxDeliveryAttempts = positiveSafeInteger(
    options.maxDeliveryAttempts ?? DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
    'notification maximum delivery attempts',
  );
  const {
    resolveAgent,
    resolveResourceId,
    consultRunCap,
    startIdleRun,
    resolveBlockingRun,
    serializeDispatch,
    resolveScheduleRunStatus,
    resolveScheduleTarget,
    canPersist,
    canPersistSchedule,
    resolveNotificationsStorage,
    notificationDispatchAllowed,
    resolveScheduleDispatchStore,
    contentPolicy,
    scheduleProviderOptionsPolicy,
  } = options;
  if (resolveBlockingRun && !serializeDispatch) {
    throw new Error(
      'signal routes require serializeDispatch with resolveBlockingRun',
    );
  }
  let wakeTail: Promise<unknown> = Promise.resolve();
  const serializeWake = <T>(operation: () => Promise<T>): Promise<T> => {
    const next = wakeTail.then(operation, operation);
    wakeTail = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };
  let notificationTail: Promise<unknown> = Promise.resolve();
  const completedScheduleDispatches = new Map<
    string,
    ScheduleAgentDispatchReceipt
  >();
  const serializeNotification = <T>(
    operation: () => Promise<T>,
  ): Promise<T> => {
    const next = notificationTail.then(operation, operation);
    notificationTail = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  };

  const route = async (
    url: URL,
    scope: ThreadScope,
    entryPath: AgentEntryPath,
    body: Record<string, unknown>,
  ): Promise<Response> => {
    const path = url.pathname;
    try {
      // The execution fence, read ONCE for this request and before any store
      // lookup. `migration-locked` refuses every signal route outright — the
      // persist lanes included, because their thread-state write is part of
      // what the migration is copying. `proof-only` is decided further down,
      // once the thread's active run is knowable.
      //
      // A read that fails throws ExecutionFenceUnreadableError, which the
      // catch below answers — degrade closed, never a silent open.
      const executionFence = await readExecutionFence(
        scope.init.executionFence,
      );
      if (executionFence.state === 'migration-locked') {
        return executionFencedResponse(executionFence.state, entryPath);
      }
      let scheduleTarget: AgentScheduleTarget | undefined;
      if (path === '/signal/schedule') {
        if (!resolveScheduleTarget) {
          return json(
            { error: 'schedule target resolution is not configured' },
            503,
          );
        }
        const { scheduleId, dispatchId, runId } = body;
        if (
          !isPathSafeId(scheduleId) ||
          !isPathSafeId(dispatchId) ||
          !isPathSafeId(runId)
        ) {
          return json({ error: 'invalid schedule signal dispatch' }, 400);
        }
        scheduleTarget = await resolveScheduleTarget(scope, {
          scheduleId,
          dispatchId,
          runId,
        });
        if (!scheduleTarget) {
          return json({ error: 'schedule dispatch not found' }, 404);
        }
      }
      const requestedAgentId =
        path === '/signal/schedule'
          ? scheduleTarget?.agentId
          : path === '/signal/notifications/dispatch'
            ? typeof body.agentId === 'string' && body.agentId.trim().length > 0
              ? body.agentId
              : undefined
            : undefined;
      if (
        path === '/signal/notifications/dispatch' &&
        requestedAgentId === undefined
      ) {
        return json(
          { error: 'agentId is required for notification dispatch' },
          400,
        );
      }

      // Host resolution stays inside this catch-all so construction and storage
      // failures use the route's internal error response.
      const agent = await resolveAgent(scope, requestedAgentId, entryPath);
      if (requestedAgentId !== undefined && agent.id !== requestedAgentId) {
        return json({ error: 'agent binding does not match' }, 404);
      }
      const runtimeDriven = isRuntimeDrivenAgent(agent);
      const pubsub = scope.init.pubsub;
      if (pubsub && !runtimeDriven) agent.__setPubSub(pubsub);
      if (
        runtimeDriven &&
        pubsub !== undefined &&
        (typeof agent.getPubSub !== 'function' || agent.getPubSub() !== pubsub)
      ) {
        return json(
          { error: "agent pub/sub does not match this thread's" },
          503,
        );
      }

      const resourceId = resolveResourceId?.(scope);
      const threadId = scope.threadId;
      // proof-only admits work on ONE run, so the gate lives here — the first
      // point where the run a signal would reach is knowable, and the only one
      // every route passes through. Deciding it inside handleWake would leave
      // the lanes that never reach it ungated; handleWake keeps its own check
      // for the wake path it owns.
      let proof: SignalProofGuard | undefined;
      if (executionFence.state === 'proof-only') {
        const runtime = scope.init.runtime;
        const reader = (
          agent as Agent & {
            proofExecutionFor?: (
              runtime: typeof scope.init.runtime,
              threadId: string,
              runId: string,
            ) => Promise<D1RunExecutionIdentity | undefined>;
          }
        ).proofExecutionFor;
        if (
          !runtimeDriven ||
          typeof reader !== 'function' ||
          runtime.executionFence !== scope.init.executionFence
        )
          throw new ExecutionFencedError(executionFence.state, entryPath);
        const capture = async (runId: string | undefined) => {
          if (runId === undefined)
            throw new ExecutionFencedError(executionFence.state, entryPath);
          const execution = await Reflect.apply(reader, agent, [
            runtime,
            threadId,
            runId,
          ]);
          if (
            execution === undefined ||
            !admitsExistingRun(executionFence, execution)
          )
            throw new ExecutionFencedError(executionFence.state, entryPath);
          return Object.freeze({ ...execution });
        };
        const admitted = await capture(
          activeThreadRunIdOf(agent, threadId, resourceId ?? ''),
        );
        const assertActive = (expected = admitted) => {
          if (
            activeThreadRunIdOf(agent, threadId, resourceId ?? '') !==
            expected.runId
          )
            throw new ExecutionFencedError(executionFence.state, entryPath);
        };
        proof = {
          capture,
          async check(expected = admitted) {
            const current = await capture(expected.runId);
            if (
              !admitsExistingRun(
                { state: 'proof-only', proofExecution: expected },
                current,
              )
            )
              throw new ExecutionFencedError(executionFence.state, entryPath);
          },
          assertActive,
        };
        assertActive();
      }
      let memoryResolution: Promise<boolean> | undefined;
      const memoryAvailable: MemoryAvailable = () => {
        memoryResolution ??= (async () => {
          try {
            return (
              typeof agent.getMemory === 'function' &&
              Boolean(await agent.getMemory())
            );
          } catch {
            try {
              console.error(
                JSON.stringify({
                  type: 'signal-memory-resolution-failed',
                  threadId,
                }),
              );
            } catch {
              // Diagnostics cannot change the memory fallback.
            }
            return false;
          }
        })();
        return memoryResolution;
      };
      const deliveredResourceId = url.searchParams.get('resourceId');
      if (
        path === '/signal/notification' &&
        deliveredResourceId !== null &&
        deliveredResourceId !== resourceId
      ) {
        return json(
          { error: 'notification resource binding does not match' },
          404,
        );
      }
      const blockingRun = resolveBlockingRun
        ? () => resolveBlockingRun(scope)
        : undefined;
      const persistenceAllowed = canPersist ? await canPersist(scope) : true;
      const inspectContent: InspectSignalContent | undefined = contentPolicy
        ? (signal, runId) =>
            inspectSignalContent(
              contentPolicy,
              {
                agentId: agent.id,
                threadId,
                ...(resourceId !== undefined ? { resourceId } : {}),
                ...(scope.deploymentTag !== undefined
                  ? { deploymentTag: scope.deploymentTag }
                  : {}),
                entryPath,
                principal: scope.principal,
              },
              signal,
              runId,
            )
        : undefined;

      // POST /signal/message — immediate user message (joins the active loop or,
      // idle, wakes/persists per ifIdle).
      if (path === '/signal/message') {
        return await handleMessage(
          agent,
          body,
          threadId,
          resourceId,
          consultRunCap,
          scope.deploymentTag,
          entryPath,
          startIdleRun,
          serializeWake,
          {
            runtimeDriven,
            principal: scope.principal,
            blockingRun,
            persistenceAllowed,
            memoryAvailable,
            inspectContent,
            proof,
            executionFence,
          },
        );
      }
      // POST /signal/queue — persisted for the next host-started turn; never
      // wakes, in either state.
      if (path === '/signal/queue') {
        return await handleQueue(agent, body, threadId, resourceId, {
          principal: scope.principal,
          blockingRun,
          persistenceAllowed,
          memoryAvailable,
          inspectContent,
          proof,
        });
      }
      // POST /signal — a system signal with its ifActive/ifIdle behaviors.
      if (path === '/signal') {
        return await handleSignal(
          agent,
          body,
          threadId,
          resourceId,
          consultRunCap,
          scope.deploymentTag,
          entryPath,
          startIdleRun,
          serializeWake,
          {
            runtimeDriven,
            principal: scope.principal,
            blockingRun,
            persistenceAllowed,
            memoryAvailable,
            inspectContent,
            proof,
            executionFence,
          },
        );
      }
      if (path === '/signal/schedule') {
        if (!resolveScheduleDispatchStore) {
          return json({ error: 'schedule dispatch is not configured' }, 503);
        }
        if (!scheduleTarget) {
          return json({ error: 'schedule dispatch not found' }, 404);
        }
        return await handleScheduleSignal({
          agent,
          body,
          executionFence,
          target: scheduleTarget,
          threadId,
          resourceId,
          principal: scope.principal,
          deploymentTag: scope.deploymentTag,
          runtimeDriven,
          consultRunCap,
          startIdleRun,
          serializeWake,
          blockingRun,
          persistenceAllowed,
          memoryAvailable,
          schedulePersistenceAllowed: canPersistSchedule
            ? (input) => canPersistSchedule(scope, input)
            : undefined,
          resolveRunStatus: resolveScheduleRunStatus
            ? (input) => resolveScheduleRunStatus(scope, input)
            : undefined,
          store: await resolveScheduleDispatchStore(scope),
          completed: completedScheduleDispatches,
          inspectContent,
          scheduleProviderOptionsPolicy,
          proof,
        });
      }
      // POST /signal/state — a durable thread-state lane (snapshot/delta).
      if (path === '/signal/state') {
        return await handleState(agent, body, threadId, resourceId, {
          principal: scope.principal,
          blockingRun,
          persistenceAllowed,
          runtimeDriven,
          memoryAvailable,
          inspectContent,
          proof,
        });
      }
      if (requestedAgentId !== undefined) {
        if (!resolveNotificationsStorage) {
          return json(
            { error: 'notification dispatch is not configured' },
            404,
          );
        }
        return await serializeNotification(async () =>
          handleNotificationDispatch({
            agent,
            body,
            executionFence,
            threadId,
            resourceId,
            deploymentTag: scope.deploymentTag,
            principal: scope.principal,
            entryPath,
            runtimeDriven,
            consultRunCap,
            startIdleRun,
            serializeWake,
            blockingRun,
            persistenceAllowed,
            memoryAvailable,
            storage: await resolveNotificationsStorage(scope),
            maxDeliveryAttempts,
            agentId: requestedAgentId,
            inspectContent,
            proof,
          }),
        );
      }
      // POST /signal/notification — the durable AGENT inbox.
      if (path === '/signal/notification') {
        return await serializeNotification(async () =>
          handleNotification(
            agent,
            body,
            threadId,
            resourceId,
            resolveNotificationsStorage
              ? () => resolveNotificationsStorage(scope)
              : undefined,
            {
              principal: scope.principal,
              blockingRun,
              persistenceAllowed,
              runtimeDriven,
              memoryAvailable,
              inspectContent,
              notificationDispatchAllowed: notificationDispatchAllowed
                ? () => notificationDispatchAllowed(scope, agent.id)
                : undefined,
              proof,
            },
          ),
        );
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      // The fence refusing, or failing to answer. Surfaced with its own status
      // and reason rather than the 502 below: a caller must be able to tell "this
      // deployment is deliberately not executing" (retry after the migration)
      // from "the model or a route is broken".
      if (isExecutionFenceRefusal(error)) {
        return json(
          { error: error.message, reason: error.reason },
          error.status,
        );
      }
      if (error instanceof DoStatusError) {
        const message = SIGNAL_ROUTE_REFUSAL_MESSAGES[error.status];
        if (message !== undefined) {
          return json({ error: message }, error.status);
        }
      }
      // A send that cannot be routed at all (e.g. an idle wake whose stream setup
      // throws — no model) rejects `accepted`; surface it as a 502 rather than a
      // 500 so a model/config fault reads distinctly from a route bug.
      return internalErrorResponse('signals.thread', error, 502);
    }
  };
  return async (request, scope) => {
    if (request.method !== 'POST') return null;
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== '/signal' && !path.startsWith('/signal/')) return null;
    const entryPath = entryPathForSignalRoute(path);
    if (entryPath === undefined) return json({ error: 'not found' }, 404);
    // A queued request keeps its parsed body if the sender disconnects.
    const body = await readJson(request);
    if (!body) return json({ error: 'a JSON body is required' }, 400);
    return serializeDispatch
      ? serializeDispatch(scope, () => route(url, scope, entryPath, body))
      : route(url, scope, entryPath, body);
  };
}

interface SignalProofGuard {
  capture(runId: string | undefined): Promise<D1RunExecutionIdentity>;
  check(execution?: D1RunExecutionIdentity): Promise<void>;
  assertActive(execution?: D1RunExecutionIdentity): void;
}

async function handleNotificationDispatch(options: {
  agent: Agent;
  body: Record<string, unknown>;
  threadId: string;
  resourceId: string | undefined;
  deploymentTag: string | undefined;
  principal: ExecutionPrincipal;
  entryPath: AgentEntryPath;
  runtimeDriven: boolean;
  consultRunCap?: RunCapConsult;
  startIdleRun?: StartIdleRun;
  serializeWake<T>(operation: () => Promise<T>): Promise<T>;
  blockingRun?: BlockingRunResolver;
  persistenceAllowed: boolean;
  memoryAvailable: MemoryAvailable;
  storage: NotificationsStorage;
  maxDeliveryAttempts: number;
  agentId: string;
  inspectContent?: InspectSignalContent;
  proof?: SignalProofGuard;
  /** The ONE fence reading this request took — see handleWake. */
  executionFence: ExecutionFenceReading;
}): Promise<Response> {
  const ids = options.body.notificationIds;
  if (
    !Array.isArray(ids) ||
    ids.length === 0 ||
    ids.length > MAX_NOTIFICATION_DISPATCH_IDS ||
    !ids.every((id): id is string => typeof id === 'string' && id.length > 0)
  ) {
    return json(
      {
        error: `notificationIds must contain 1-${MAX_NOTIFICATION_DISPATCH_IDS} strings`,
      },
      400,
    );
  }
  const uniqueIds = [...new Set(ids)];
  if (
    options.resourceId === undefined ||
    options.body.resourceId !== options.resourceId
  ) {
    return json({ error: 'notification resource binding does not match' }, 404);
  }
  const resourceId = options.resourceId;
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return notificationPrincipalMismatchResponse(durableBlockingRun.runId);
  }
  const now =
    typeof options.body.now === 'string'
      ? new Date(options.body.now)
      : new Date();
  if (Number.isNaN(now.getTime())) {
    return json({ error: 'now must be an ISO timestamp' }, 400);
  }
  const nowMs = now.getTime();
  const carriesBatchThreadState = Object.hasOwn(
    options.body,
    'batchThreadState',
  );
  const requestedBatchThreadState = options.body.batchThreadState;
  if (
    carriesBatchThreadState &&
    requestedBatchThreadState !== null &&
    requestedBatchThreadState !== 'active' &&
    requestedBatchThreadState !== 'idle'
  ) {
    return json(
      { error: 'batchThreadState must be null, active, or idle' },
      400,
    );
  }

  const deliveryStorage = captureNotificationDeliveryStorage(options.storage);
  const selections: ReturnType<typeof captureNotificationDeliverySelection>[] =
    [];
  // A record settles once: an already-discarded record must not be counted
  // twice, nor have its content-policy reason overwritten by an unrelated
  // storage error. `recordFailure` returns early for the same reason.
  const settled = new Set<string>();
  let delivered = 0;
  let failed = 0;
  let discarded = 0;
  let skipped = 0;
  const settle = (
    id: string,
    outcome: 'delivered' | 'failed' | 'discarded' | 'skipped',
  ) => {
    if (settled.has(id)) return;
    settled.add(id);
    if (outcome === 'delivered') delivered += 1;
    else if (outcome === 'failed') failed += 1;
    else if (outcome === 'discarded') discarded += 1;
    else skipped += 1;
  };
  for (const id of uniqueIds) {
    const current = await deliveryStorage.getNotification({
      threadId: options.threadId,
      id,
    });
    if (current?.status !== 'pending' || current.deliveredSignalId) {
      settle(id, 'skipped');
      continue;
    }
    if (
      current.id !== id ||
      current.threadId !== options.threadId ||
      current.resourceId !== options.resourceId ||
      current.agentId !== options.agentId
    ) {
      return json({ error: 'notification binding does not match' }, 404);
    }
    try {
      const selection = captureNotificationDeliverySelection(current);
      const { record } = selection;
      const due =
        (record.deliverAt !== undefined &&
          record.deliverAt.getTime() <= nowMs) ||
        (record.summaryAt !== undefined && record.summaryAt.getTime() <= nowMs);
      if (!due) {
        settle(id, 'skipped');
        continue;
      }
      selections.push(selection);
    } catch (error) {
      settle(id, 'failed');
      reportNotificationDeliveryError(error);
    }
  }

  const batchThreadState: 'active' | 'idle' =
    requestedBatchThreadState === 'active' ||
    requestedBatchThreadState === 'idle'
      ? requestedBatchThreadState
      : durableBlockingRun ||
          activeThreadRunIdOf(options.agent, options.threadId, resourceId)
        ? 'active'
        : 'idle';

  const recordFailure = async (
    expected: NotificationDeliveryObservation | undefined,
    action: { type: 'failure'; error: unknown } | { type: 'exhausted' },
  ) => {
    if (action.type === 'failure' && isExecutionFenceRefusal(action.error))
      throw action.error;
    if (!expected) throw new Error('notification observation is unavailable');
    if (settled.has(expected.id)) return;
    await options.proof?.check();
    options.proof?.assertActive();
    const outcome = await recordNotificationDeliveryFailure(
      deliveryStorage,
      expected,
      new Date(nowMs),
      options.maxDeliveryAttempts,
      action,
    );
    settle(expected.id, outcome === 'discarded' ? 'discarded' : 'failed');
  };
  const discardAfterDenial = async (record: NotificationRecord) => {
    await options.proof?.check();
    options.proof?.assertActive();
    await options.storage.updateNotification({
      id: record.id,
      threadId: record.threadId,
      status: 'discarded',
      deliveryReason: 'content-policy-denied',
      lastDeliveryAttemptAt: new Date(nowMs),
    });
    settle(record.id, 'discarded');
  };
  const inspect = async (
    signal: AgentSignal,
  ): Promise<SignalContentInspection> => {
    if (!options.inspectContent) return 'allowed';
    return options.inspectContent(
      signal,
      durableBlockingRun?.runId ??
        activeThreadRunIdOf(options.agent, options.threadId, resourceId),
    );
  };
  const send = async (signal: AgentSignal): Promise<string> => {
    const deliverableSignal = options.persistenceAllowed
      ? signal
      : markPersistenceForbidden(signal);
    const response = await handleWake({
      agent: options.agent,
      deploymentTag: options.deploymentTag,
      threadId: options.threadId,
      resourceId,
      principal: options.principal,
      entryPath: options.entryPath,
      runtimeDriven: options.runtimeDriven,
      consultRunCap: options.consultRunCap,
      startIdleRun: options.startIdleRun,
      serializeWake: options.serializeWake,
      executionFence: options.executionFence,
      proof: options.proof,
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed: options.persistenceAllowed,
      memoryAvailable: options.memoryAvailable,
      signal: deliverableSignal,
      deliverActive: (runId, activeMemoryAvailable, resolvedActiveBehavior) =>
        options.agent.sendSignal(deliverableSignal, {
          runId,
          threadId: options.threadId,
          resourceId,
          ifActive: { behavior: resolvedActiveBehavior },
          ifIdle: {
            behavior:
              activeMemoryAvailable && options.persistenceAllowed
                ? 'persist'
                : 'discard',
          },
        }),
      persist: () =>
        options.agent.sendSignal(deliverableSignal, {
          threadId: options.threadId,
          resourceId,
          ifIdle: { behavior: 'persist' },
        }),
    });
    if (!response.ok) {
      if (options.executionFence.state === 'proof-only')
        throw new ExecutionFencedError('proof-only', 'notification signal');
      throw new Error(`notification signal returned ${response.status}`);
    }
    const result = (await response.json()) as {
      signalId?: string;
      decision?: unknown;
    };
    const action = recordValue(result.decision)?.action;
    // A stale active id can fall through to idle. Persistence-forbidden and
    // memory-unavailable discards are failed delivery, never a fresh wake.
    if (
      action !== 'wake' &&
      action !== 'deliver' &&
      !(action === 'persist' && options.persistenceAllowed)
    ) {
      throw new Error(
        `notification signal was not executed (${String(action ?? 'unknown')})`,
      );
    }
    if (!result.signalId) throw new Error('notification signal has no id');
    return result.signalId;
  };
  const persistWithoutWake = async (signal: AgentSignal): Promise<string> => {
    if (!options.persistenceAllowed) {
      throw new Error('signal persistence is forbidden for this principal');
    }
    if (!(await options.memoryAvailable())) {
      throw new Error('signal persistence requires agent memory');
    }
    await options.proof?.check();
    options.proof?.assertActive();
    const result = options.agent.sendSignal(signal, {
      threadId: options.threadId,
      resourceId,
      ifActive: {
        behavior: activeBehaviorForRun('deliver', durableBlockingRun),
      },
      ifIdle: { behavior: 'persist' },
    });
    await result.accepted;
    if (result.persisted) await result.persisted;
    return result.signal.id;
  };

  const records: NotificationRecord[] = [];
  const observations = new Map<string, NotificationDeliveryObservation>();
  for (const { record, expected } of selections) {
    if (expected.deliveryAttempts >= options.maxDeliveryAttempts) {
      await recordFailure(expected, { type: 'exhausted' });
      continue;
    }
    records.push(record);
    observations.set(record.id, expected);
  }

  for (const item of planNotificationDispatch(records, now)) {
    if (item.type === 'summary') {
      // A summary group's failure, from rendering through bookkeeping, is
      // contained to this group rather than escaping and abandoning the rest
      // of the plan.
      try {
        const signal = createNotificationSummarySignal(
          summarizeNotifications(item.records),
        );
        const inspection = await inspect(signal);
        if (inspection === 'denied') {
          for (const record of item.records) await discardAfterDenial(record);
          continue;
        }
        if (inspection === 'error') {
          for (const record of item.records) {
            await recordFailure(observations.get(record.id), {
              type: 'failure',
              error: new Error('signal content policy failed'),
            });
          }
          continue;
        }
        const lowPriority = item.records.every(
          (record) => record.priority === 'low',
        );
        const signalId =
          lowPriority && options.persistenceAllowed
            ? await persistWithoutWake(signal)
            : await send(signal);
        for (const record of item.records) {
          await options.proof?.check();
          options.proof?.assertActive();
          await options.storage.updateNotification({
            id: record.id,
            threadId: record.threadId,
            summaryAt: null,
            summarySignalId: signalId,
            lastDeliveryAttemptAt: new Date(nowMs),
          });
          settle(record.id, 'delivered');
        }
      } catch (error) {
        for (const record of item.records) {
          await recordFailure(observations.get(record.id), {
            type: 'failure',
            error,
          });
        }
      }
      continue;
    }

    const selected = item.record;
    let expected = observations.get(selected.id);
    try {
      const current = await deliveryStorage.getNotification({
        threadId: selected.threadId,
        id: selected.id,
      });
      if (
        current?.status !== 'pending' ||
        current.deliveredSignalId ||
        current.id !== selected.id ||
        current.threadId !== options.threadId ||
        current.resourceId !== resourceId ||
        current.agentId !== options.agentId
      ) {
        settle(selected.id, 'skipped');
        continue;
      }
      // Clearing the pre-read observation keeps a failure between here and the
      // re-capture from being recorded against stale state.
      expected = undefined;
      const selection = captureNotificationDeliverySelection(current);
      const { record } = selection;
      expected = selection.expected;
      const summaryDue = Boolean(
        record.summaryAt && record.summaryAt.getTime() <= nowMs,
      );
      const deliveryDue = Boolean(
        record.deliverAt && record.deliverAt.getTime() <= nowMs,
      );
      if (summaryDue || !deliveryDue) {
        settle(selected.id, 'skipped');
        continue;
      }
      if (expected.deliveryAttempts >= options.maxDeliveryAttempts) {
        await recordFailure(expected, { type: 'exhausted' });
        continue;
      }
      if (
        record.priority === 'high' &&
        record.summarySignalId &&
        batchThreadState === 'active'
      ) {
        settle(selected.id, 'skipped');
        continue;
      }
      const signal = createNotificationSignal({
        ...record,
        status: 'delivered',
        deliveredAt: new Date(nowMs),
      });
      const inspection = await inspect(signal);
      if (inspection === 'denied') {
        await discardAfterDenial(record);
        continue;
      }
      if (inspection === 'error') {
        await recordFailure(expected, {
          type: 'failure',
          error: new Error('signal content policy failed'),
        });
        continue;
      }
      const signalId = await send(signal);
      await options.proof?.check();
      options.proof?.assertActive();
      await options.storage.updateNotification({
        id: record.id,
        threadId: record.threadId,
        status: 'delivered',
        deliveredSignalId: signalId,
        lastDeliveryAttemptAt: new Date(nowMs),
      });
      settle(record.id, 'delivered');
    } catch (error) {
      if (isExecutionFenceRefusal(error)) throw error;
      if (expected) await recordFailure(expected, { type: 'failure', error });
      else {
        settle(selected.id, 'failed');
        reportNotificationDeliveryError(error);
      }
    }
  }

  return json({
    delivered,
    failed,
    ...(discarded > 0 ? { discarded } : {}),
    ...(skipped > 0 ? { skipped } : {}),
    ...(carriesBatchThreadState ? { batchThreadState } : {}),
  });
}

/**
 * Why a requested wake was refused.
 *
 * `execution-draining` is the fence's (do-runner/execution-fence.ts): a
 * draining deployment must mint no new run, and a drain cannot answer a signal
 * by refusing — the sender has nowhere to put it and the migration would lose
 * it. Degrading to the SAME persist branch the other refusals use keeps it
 * durable for the deployment that takes over.
 */
type WakeRefusal =
  | 'not-runtime-driven'
  | 'no-start-idle-run'
  | 'execution-draining';
/** Marker returned when an unbranded route cannot guarantee runtime execution. */
type RouteDegradation = 'not-runtime-driven';

/**
 * Resolve the idle behavior a body asked for. A `wake` STARTS a run, so
 * handleWake gates it.
 */
function requestedIdle(body: Record<string, unknown>): IdleBehavior {
  const requested = body.ifIdle;
  return typeof requested === 'string' &&
    (IDLE_BEHAVIORS as readonly string[]).includes(requested)
    ? (requested as IdleBehavior)
    : 'persist';
}

interface WakeDelivery {
  accepted: Promise<unknown>;
  signal: { id: string };
  persisted?: Promise<void>;
}

type BlockingRunResolver = () =>
  | Promise<
      | { runId: string; principal: ExecutionPrincipal; status?: RunStatus }
      | undefined
    >
  | { runId: string; principal: ExecutionPrincipal; status?: RunStatus }
  | undefined;
type MemoryAvailable = () => Promise<boolean>;

/** See resolveBlockingRun for why a run that is not executing gets persist. */
function activeBehaviorForRun(
  requested: ActiveBehavior,
  blockingRun: Awaited<ReturnType<BlockingRunResolver>>,
): ActiveBehavior {
  return requested === 'deliver' &&
    blockingRun?.status !== undefined &&
    blockingRun.status !== 'running'
    ? 'persist'
    : requested;
}

function deliverableActiveBehavior(
  behavior: ActiveBehavior,
  persistenceAllowed: boolean,
): ActiveBehavior {
  return behavior === 'persist' && !persistenceAllowed ? 'discard' : behavior;
}

function notificationPrincipalMismatchResponse(runId: string): Response {
  return json(
    {
      error: 'notification principal does not match the active run',
      reason: 'principal-mismatch',
      runId,
      retry: true,
    },
    409,
  );
}

const OWNER_NOTIFICATION_REFUSALS = {
  'memory-unavailable': 'notification delivery requires agent memory',
  'notification-pending': 'a matching notification is already pending',
  'notification-dispatch-forbidden':
    'the target agent does not accept notification dispatch',
  'thread-blocked': 'the thread is blocked by another run',
} as const;

function ownerNotificationRefusal(
  reason: keyof typeof OWNER_NOTIFICATION_REFUSALS,
): Response {
  return json({ error: OWNER_NOTIFICATION_REFUSALS[reason], reason }, 409);
}

function principalMismatchResponse(runId: string): Response {
  return json({
    decision: {
      action: 'blocked',
      reason: 'principal-mismatch',
      runId,
    },
    capped: false,
  });
}

/**
 * Report a lazily computed missing-memory gate after content inspection, only
 * where a memory write would otherwise disappear.
 */
function memoryUnavailableResponse(): Response {
  return json({
    decision: { action: 'discard', reason: 'memory-unavailable' },
  });
}

/** Report route-specific persistence authorization failures. */
function persistenceForbiddenResponse(options?: {
  capped?: boolean;
  wakeRefused?: WakeRefusal;
  signalId?: string;
}): Response {
  return json({
    decision: { action: 'discard', reason: 'persistence-forbidden' },
    ...(options?.capped !== undefined ? { capped: options.capped } : {}),
    ...(options?.wakeRefused !== undefined
      ? { wakeRefused: options.wakeRefused }
      : {}),
    ...(options?.signalId !== undefined ? { signalId: options.signalId } : {}),
  });
}

async function handleWake(options: {
  agent: Agent;
  deploymentTag: string | undefined;
  threadId: string;
  resourceId: string;
  principal: ExecutionPrincipal;
  entryPath: AgentEntryPath;
  runtimeDriven: boolean;
  consultRunCap?: RunCapConsult;
  startIdleRun?: StartIdleRun;
  serializeWake<T>(operation: () => Promise<T>): Promise<T>;
  blockingRun?: BlockingRunResolver;
  persistenceAllowed: boolean;
  memoryAvailable: MemoryAvailable;
  message?: AgentMessageInput;
  signal?: AgentSignal;
  runId?: string;
  scheduleId?: string;
  dispatchId?: string;
  safeContext?: Record<string, unknown>;
  /**
   * The ONE fence reading this request took (never re-read per branch). The
   * route resolves it before dispatch.
   */
  executionFence: ExecutionFenceReading;
  proof?: SignalProofGuard;
  /**
   * Treat an explicit active-branch discard as the caller's own outcome: it
   * suppresses both the memory-unavailable and the persistence-forbidden
   * attribution. A stale active id can still read a substituted discard as
   * the caller's, which core's bare discard result cannot distinguish.
   */
  activeBehavior?: ActiveBehavior;
  deliverActive(
    runId: string,
    activeMemoryAvailable: boolean,
    resolvedActiveBehavior: ActiveBehavior,
  ): WakeDelivery;
  persist(): WakeDelivery;
}): Promise<Response> {
  return options.serializeWake(async () => {
    const fence = options.executionFence;
    const durableBlockingRun = await options.blockingRun?.();
    const activeRunId = activeThreadRunIdOf(
      options.agent,
      options.threadId,
      options.resourceId,
    );
    if (
      durableBlockingRun &&
      !samePrincipal(durableBlockingRun.principal, options.principal)
    ) {
      return principalMismatchResponse(durableBlockingRun.runId);
    }
    // Delivery into a run that ALREADY exists survives a drain — the run is
    // what the drain is waiting for — and in proof-only it is admitted only
    // for the nominated run. The check sits after the principal gate so a
    // fenced deployment leaks nothing a permitted caller could not see.
    const admitted =
      fence.state === 'proof-only'
        ? await options.proof?.capture(activeRunId)
        : undefined;
    if (fence.state === 'proof-only' && admitted === undefined)
      throw new ExecutionFencedError(fence.state, 'signal delivery');
    if (activeRunId) {
      if (durableBlockingRun && durableBlockingRun.runId !== activeRunId) {
        return json({
          decision: {
            action: 'blocked',
            reason: 'thread-blocked',
            runId: durableBlockingRun.runId,
          },
          capped: false,
        });
      }
      const memoryAvailable = await options.memoryAvailable();
      await options.proof?.check(admitted);
      options.proof?.assertActive(admitted);
      const activeBehavior = activeBehaviorForRun(
        options.activeBehavior ?? 'deliver',
        durableBlockingRun,
      );
      const deliveredActiveBehavior = deliverableActiveBehavior(
        activeBehavior,
        options.persistenceAllowed,
      );
      const delivered = options.deliverActive(
        activeRunId,
        memoryAvailable,
        deliveredActiveBehavior,
      );
      const decision = await delivered.accepted;
      if (delivered.persisted) await delivered.persisted;
      const action = recordValue(decision)?.action;
      if (action === 'discard' && options.activeBehavior !== 'discard') {
        return options.persistenceAllowed
          ? memoryUnavailableResponse()
          : persistenceForbiddenResponse({ capped: false });
      }
      if (action === 'persist' && !memoryAvailable) {
        return memoryUnavailableResponse();
      }
      return json({
        decision,
        capped: false,
        signalId: delivered.signal.id,
      });
    }

    if (durableBlockingRun) {
      return json({
        decision: {
          action: 'blocked',
          reason: 'thread-blocked',
          runId: durableBlockingRun.runId,
        },
        capped: false,
        ...(options.signal?.id !== undefined
          ? { signalId: options.signal.id }
          : {}),
      });
    }

    // An idle thread has no run to deliver into, so from here on the only way
    // to serve the signal is to MINT one. `migration-locked` and `proof-only`
    // forbid that outright and have no lossless alternative to offer (the
    // proof run is a specific run, not this thread's next one), so they refuse
    // and the caller retries after the migration. `draining` degrades to the
    // persist branch below instead — see WakeRefusal.
    if (fence.state === 'migration-locked' || fence.state === 'proof-only') {
      return executionFencedResponse(fence.state, 'signal wake');
    }
    const startIdleRun = options.startIdleRun;
    const refusal: WakeRefusal | undefined =
      fence.state === 'draining'
        ? 'execution-draining'
        : !options.runtimeDriven
          ? 'not-runtime-driven'
          : !startIdleRun
            ? 'no-start-idle-run'
            : undefined;
    const capped =
      options.runtimeDriven && options.consultRunCap
        ? !(await options.consultRunCap())
        : false;
    if (refusal || capped) {
      // A drain can lose a signal here, and it is the CALLER'S choice, not the
      // fence's. `execution-draining` degrades a wake into a persist so
      // nothing is lost while a deployment finishes its work — but a caller who
      // said persistence is forbidden has already declared it does not want its
      // signal parked, and honouring the fence by parking it anyway would
      // override an authorization decision with an operational one. So it is
      // discarded, and the response SAYS SO: `wakeRefused:
      // 'execution-draining'` alongside `action: 'discard'` tells the caller
      // exactly which condition dropped it, so a sender that would rather wait
      // out the migration can retry instead of assuming delivery.
      if (!options.persistenceAllowed) {
        return persistenceForbiddenResponse({
          capped,
          ...(refusal ? { wakeRefused: refusal } : {}),
          ...(options.signal?.id !== undefined
            ? { signalId: options.signal.id }
            : {}),
        });
      }
      if (!(await options.memoryAvailable()))
        return memoryUnavailableResponse();
      const persisted = options.persist();
      const decision = await persisted.accepted;
      if (persisted.persisted) await persisted.persisted;
      return json({
        decision,
        capped,
        ...(refusal ? { wakeRefused: refusal } : {}),
        signalId: persisted.signal.id,
      });
    }

    const runId = options.runId ?? crypto.randomUUID();
    if (!isPathSafeId(runId)) {
      throw new Error('thread signal wake generated a non-path-safe run id');
    }
    if (!startIdleRun) {
      throw new Error('idle-start gate allowed a missing startIdleRun seam');
    }
    const started = await startIdleRun({
      agent: options.agent,
      runId,
      threadId: options.threadId,
      resourceId: options.resourceId,
      principal: options.principal,
      ...(options.deploymentTag !== undefined
        ? { deploymentTag: options.deploymentTag }
        : {}),
      entryPath: options.entryPath,
      ...(options.scheduleId !== undefined
        ? { scheduleId: options.scheduleId }
        : {}),
      ...(options.dispatchId !== undefined
        ? { dispatchId: options.dispatchId }
        : {}),
      ...(options.message !== undefined ? { message: options.message } : {}),
      ...(options.signal !== undefined ? { signal: options.signal } : {}),
      ...(options.safeContext !== undefined
        ? { safeContext: options.safeContext }
        : {}),
    });
    if (
      started.status === 'canceled' ||
      started.status === 'cancelled' ||
      started.status === 'timed_out'
    ) {
      return json({
        decision: { action: 'discard', runId: started.runId },
        capped: false,
      });
    }
    const signalId = started.signalId ?? options.signal?.id;
    return json({
      decision: { action: 'wake', runId: started.runId },
      capped: false,
      ...(signalId !== undefined ? { signalId } : {}),
    });
  });
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function scheduleReceipt(
  decision: unknown,
  signalId: string | undefined,
): ScheduleAgentDispatchReceipt | undefined {
  const candidate = recordValue(decision);
  const action = candidate?.action;
  if (
    action !== 'wake' &&
    action !== 'deliver' &&
    action !== 'persist' &&
    action !== 'discard' &&
    action !== 'blocked'
  ) {
    return undefined;
  }
  const runId = candidate?.runId;
  if (
    (action === 'wake' || action === 'deliver' || action === 'blocked') &&
    !isPathSafeId(runId)
  ) {
    return undefined;
  }
  const canonicalAction = action as ScheduleAgentDispatchAction;
  return createScheduleAgentDispatchReceipt(canonicalAction, {
    ...(typeof runId === 'string' ? { runId } : {}),
    ...(signalId !== undefined ? { signalId } : {}),
  });
}

async function handleScheduleSignal(options: {
  agent: Agent;
  body: Record<string, unknown>;
  target: AgentScheduleTarget;
  threadId: string;
  resourceId: string | undefined;
  principal: ExecutionPrincipal;
  deploymentTag: string | undefined;
  runtimeDriven: boolean;
  consultRunCap: RunCapConsult | undefined;
  startIdleRun: StartIdleRun | undefined;
  serializeWake<T>(operation: () => Promise<T>): Promise<T>;
  blockingRun?: BlockingRunResolver;
  persistenceAllowed: boolean;
  memoryAvailable: MemoryAvailable;
  schedulePersistenceAllowed?: (input: {
    scheduleId: string;
    dispatchId: string;
    runId: string;
    agentId: string;
    threadId: string;
    resourceId: string;
  }) => boolean | Promise<boolean>;
  resolveRunStatus?: (input: {
    agentId: string;
    resourceId: string;
    runId: string;
  }) => Promise<{ runId: string; status?: string } | undefined>;
  store: ScheduleSignalDispatchStore;
  completed: Map<string, ScheduleAgentDispatchReceipt>;
  inspectContent?: InspectSignalContent;
  scheduleProviderOptionsPolicy?: ThreadSignalRoutesOptions['scheduleProviderOptionsPolicy'];
  proof?: SignalProofGuard;
  /** The ONE fence reading this request took — see handleWake. */
  executionFence: ExecutionFenceReading;
}): Promise<Response> {
  const scheduleId = options.body.scheduleId;
  const dispatchId = options.body.dispatchId;
  const runId = options.body.runId;
  const target = options.target;
  const requestContext = target.requestContext ?? {};
  const streamRequestContext =
    target.ifIdle?.streamOptions?.requestContext ?? {};
  if (
    !isPathSafeId(scheduleId) ||
    !isPathSafeId(dispatchId) ||
    !isPathSafeId(runId) ||
    typeof options.resourceId !== 'string' ||
    target.threadId !== options.threadId ||
    target.resourceId !== options.resourceId
  ) {
    return json({ error: 'invalid schedule signal dispatch' }, 400);
  }
  const resourceId = options.resourceId;

  // A terminal "this fire will not be delivered" outcome settles a canonical
  // receipt, so a replay of the dispatch returns it instead of re-deciding.
  // Settling is what lets the schedule advance; leaving the lease
  // unsettled is reserved for outcomes a later tick could still resolve.
  const settleDiscard = async (): Promise<Response> => {
    const receipt = createScheduleAgentDispatchReceipt('discard', {
      signalId: dispatchId,
    });
    await options.proof?.check();
    options.proof?.assertActive();
    options.completed.set(dispatchId, receipt);
    await options.store.settle(scheduleId, dispatchId, receipt);
    options.completed.delete(dispatchId);
    return json({ receipt });
  };

  const completed = options.completed.get(dispatchId);
  if (completed) {
    await options.proof?.check();
    options.proof?.assertActive();
    await options.store.settle(scheduleId, dispatchId, completed);
    options.completed.delete(dispatchId);
    return json({ receipt: completed });
  }
  await options.proof?.check();
  options.proof?.assertActive();
  const state = await options.store.begin(scheduleId, dispatchId);
  if (state.state === 'missing') {
    return json({ error: 'schedule dispatch not found' }, 404);
  }
  if (state.state === 'pending') {
    return json({ pending: true }, 202);
  }
  if (state.state === 'settled') {
    return json({ receipt: state.receipt });
  }

  const recoveredRun = await options.resolveRunStatus?.({
    agentId: target.agentId,
    resourceId,
    runId,
  });
  if (recoveredRun) {
    const receipt: ScheduleAgentDispatchReceipt = {
      action: 'wake',
      outcome: 'succeeded',
      runId: recoveredRun.runId,
      signalId: dispatchId,
    };
    await options.proof?.check();
    options.proof?.assertActive();
    options.completed.set(dispatchId, receipt);
    await options.store.settle(scheduleId, dispatchId, receipt);
    options.completed.delete(dispatchId);
    return json({ receipt });
  }

  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    const receipt: ScheduleAgentDispatchReceipt = {
      action: 'blocked',
      outcome: 'skipped',
      runId: durableBlockingRun.runId,
      signalId: dispatchId,
    };
    await options.proof?.check();
    options.proof?.assertActive();
    options.completed.set(dispatchId, receipt);
    await options.store.settle(scheduleId, dispatchId, receipt);
    options.completed.delete(dispatchId);
    return json({ receipt });
  }

  // A stored target that cannot be rendered as XML can never be delivered: core
  // asserts the tag name and every surviving attribute name inside
  // signalToXmlMarkup, and a schedule's attributes never pass through
  // `isAttributes` (core types them as a bare string record). Returning an error
  // would leave the lease unsettled and hand the same permanently broken target
  // to every later tick, so settle it terminally and let the schedule advance.
  const effectiveTagName = target.tagName ?? 'schedule';
  if (
    !XML_NAME_PATTERN.test(effectiveTagName) ||
    !renderableAttributes(target.attributes) ||
    !renderableAttributes(target.ifActive?.attributes) ||
    !renderableAttributes(target.ifIdle?.attributes)
  ) {
    // The operator has to be able to find the broken schedule; the offending
    // name itself stays out of the log.
    try {
      console.error(
        JSON.stringify({
          type: 'schedule-target-unrenderable',
          scheduleId,
          dispatchId,
          agentId: target.agentId,
        }),
      );
    } catch {
      // Diagnostics cannot prevent terminal settlement.
    }
    return await settleDiscard();
  }

  const storedProviderOptions = target.providerOptions;
  const scheduleProviderOptionsPolicy = options.scheduleProviderOptionsPolicy;
  if (scheduleProviderOptionsPolicy) {
    const inspection = await inspectPolicyResult(() =>
      scheduleProviderOptionsPolicy({
        // The policy cannot change the options later delivered in the signal.
        providerOptions: structuredClone(storedProviderOptions),
        agentId: target.agentId,
        threadId: options.threadId,
        resourceId,
      }),
    );
    if (inspection === 'denied') {
      return await settleDiscard();
    }
    if (inspection === 'error') {
      return json(
        { error: 'schedule provider options policy unavailable' },
        503,
      );
    }
  }

  const baseProviderOptions = storedProviderOptions ?? {};
  const baseMastra = recordValue(baseProviderOptions.mastra) ?? {};
  const signal: AgentSignal = {
    id: dispatchId,
    type: target.signalType ?? 'notification',
    tagName: effectiveTagName,
    contents: target.prompt,
    ...(target.attributes !== undefined
      ? { attributes: target.attributes }
      : {}),
    providerOptions: {
      ...baseProviderOptions,
      mastra: {
        ...baseMastra,
        schedule: {
          scheduleId,
          threadId: options.threadId,
        },
      },
    } as AgentSignal['providerOptions'],
  };
  const ifActive = target.ifActive ?? { behavior: 'deliver' as const };
  const requestedActiveBehavior = ifActive.behavior ?? 'deliver';
  const effectiveIfActive = {
    ...ifActive,
    behavior: activeBehaviorForRun(requestedActiveBehavior, durableBlockingRun),
  };
  const ifIdle = target.ifIdle ?? { behavior: 'wake' as const };
  const localActiveRunId = activeThreadRunIdOf(
    options.agent,
    options.threadId,
    resourceId,
  );
  const persistenceRequested = localActiveRunId
    ? effectiveIfActive.behavior === 'persist'
    : (ifIdle.behavior ?? 'wake') === 'persist';
  // A wake or a delivery can still end in persistence, so the schedule's
  // authorization governs every fire.
  const persistenceAllowed = options.schedulePersistenceAllowed
    ? await options.schedulePersistenceAllowed({
        scheduleId,
        dispatchId,
        runId,
        agentId: target.agentId,
        threadId: options.threadId,
        resourceId,
      })
    : options.persistenceAllowed;
  if (persistenceRequested && !persistenceAllowed) {
    return await settleDiscard();
  }
  const deliverableSignal = persistenceAllowed
    ? signal
    : markPersistenceForbidden(signal);
  if (options.inspectContent) {
    // Mastra chooses the active or idle branch after an async boundary, so both
    // renderings are inspected. `resolveDeliveryAttributes` returns the SAME
    // object when a branch declares no attributes — the default — so the common
    // case is one inspection, not two identical ones (a host policy backed by a
    // classifier would otherwise pay twice per fire and audit twice).
    const createdSignal = createSignal(deliverableSignal);
    const activeSignal = resolveDeliveryAttributes(
      createdSignal,
      ifActive.attributes,
    );
    const idleSignal = resolveDeliveryAttributes(
      createdSignal,
      ifIdle.attributes,
    );
    const executingRunId = localActiveRunId ?? durableBlockingRun?.runId;
    const [activeInspection, idleInspection] =
      activeSignal === idleSignal
        ? [
            await options.inspectContent(activeSignal, executingRunId ?? runId),
            undefined,
          ]
        : await Promise.all([
            options.inspectContent(activeSignal, executingRunId),
            options.inspectContent(idleSignal, runId),
          ]);
    // Denial wins over an evaluator failure on the other branch: only one branch
    // ever delivers, but which one is not known here, so refusing terminally is
    // the fail-closed answer.
    if (activeInspection === 'denied' || idleInspection === 'denied') {
      return await settleDiscard();
    }
    if (activeInspection === 'error' || idleInspection === 'error') {
      return signalContentPolicyResponse('error');
    }
  }
  const idleRequestContext =
    ifIdle.streamOptions?.requestContext ?? streamRequestContext;
  const signalIfIdle: AgentSignalIfIdleOptions = {
    ...(ifIdle.behavior !== undefined ? { behavior: ifIdle.behavior } : {}),
    ...(ifIdle.attributes !== undefined
      ? { attributes: ifIdle.attributes }
      : {}),
    ...(ifIdle.streamOptions !== undefined
      ? {
          streamOptions: {
            ...ifIdle.streamOptions,
            requestContext:
              idleRequestContext === undefined
                ? undefined
                : new RequestContext(Object.entries(idleRequestContext)),
          } as NonNullable<AgentSignalIfIdleOptions['streamOptions']>,
        }
      : {}),
  };
  const signalTarget: SendAgentSignalOptions = {
    threadId: options.threadId,
    resourceId,
    ifActive: {
      ...effectiveIfActive,
      behavior: deliverableActiveBehavior(
        effectiveIfActive.behavior,
        persistenceAllowed,
      ),
    },
    ifIdle: {
      ...signalIfIdle,
      behavior:
        signalIfIdle.behavior === 'persist' && !persistenceAllowed
          ? 'discard'
          : signalIfIdle.behavior,
    },
  };

  let decision: unknown;
  let signalId: string | undefined;
  if ((ifIdle.behavior ?? 'wake') === 'wake') {
    const idleSignal: AgentSignal = {
      ...deliverableSignal,
      ...(ifIdle.attributes !== undefined
        ? {
            attributes: {
              ...deliverableSignal.attributes,
              ...ifIdle.attributes,
            },
          }
        : {}),
    };
    const response = await handleWake({
      agent: options.agent,
      deploymentTag: options.deploymentTag,
      threadId: options.threadId,
      resourceId,
      principal: options.principal,
      entryPath: 'schedule.fire',
      runtimeDriven: options.runtimeDriven,
      consultRunCap: options.consultRunCap,
      startIdleRun: options.startIdleRun,
      serializeWake: options.serializeWake,
      executionFence: options.executionFence,
      proof: options.proof,
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed,
      memoryAvailable: options.memoryAvailable,
      signal: idleSignal,
      activeBehavior: requestedActiveBehavior,
      runId,
      scheduleId,
      dispatchId,
      safeContext: { ...requestContext, ...idleRequestContext },
      deliverActive: (
        activeRunId,
        activeMemoryAvailable,
        resolvedActiveBehavior,
      ) =>
        options.agent.sendSignal(deliverableSignal, {
          runId: activeRunId,
          threadId: options.threadId,
          resourceId,
          ifActive: { ...ifActive, behavior: resolvedActiveBehavior },
          ifIdle: {
            behavior:
              activeMemoryAvailable && persistenceAllowed
                ? 'persist'
                : 'discard',
          },
        }),
      persist: () =>
        options.agent.sendSignal(deliverableSignal, {
          threadId: options.threadId,
          resourceId,
          ifActive,
          ifIdle: { ...signalIfIdle, behavior: 'persist' },
        }),
    });
    if (!response.ok) return response;
    const payload = (await response.json()) as Record<string, unknown>;
    decision = payload.decision;
    signalId =
      typeof payload.signalId === 'string' ? payload.signalId : undefined;
  } else {
    await options.proof?.check();
    options.proof?.assertActive();
    const sent = options.agent.sendSignal(deliverableSignal, signalTarget);
    decision = await sent.accepted;
    const action = recordValue(decision)?.action;
    if (action === 'persist' && !(await options.memoryAvailable())) {
      if (sent.persisted) await sent.persisted;
      // A memory-less host cannot persist this fire; an unsettled lease would
      // replay it every tick.
      return await settleDiscard();
    }
    if (action === 'persist' && sent.persisted) {
      await sent.persisted;
    }
    signalId = sent.signal.id;
  }
  const receipt = scheduleReceipt(decision, signalId);
  if (!receipt) {
    throw new Error('agent schedule returned an invalid signal decision');
  }
  await options.proof?.check();
  options.proof?.assertActive();
  options.completed.set(dispatchId, receipt);
  await options.store.settle(scheduleId, dispatchId, receipt);
  options.completed.delete(dispatchId);
  return json({ receipt });
}

async function contentPolicyRefusal(
  inspectContent: InspectSignalContent | undefined,
  signal: RenderableSignal,
  runId?: string,
): Promise<Response | undefined> {
  if (!inspectContent) return undefined;
  const inspection = await inspectContent(signal, runId);
  return inspection === 'allowed'
    ? undefined
    : signalContentPolicyResponse(inspection);
}

async function handleMessage(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  consultRunCap: RunCapConsult | undefined,
  deploymentTag: string | undefined,
  entryPath: AgentEntryPath,
  startIdleRun: StartIdleRun | undefined,
  serializeWake: <T>(operation: () => Promise<T>) => Promise<T>,
  options: {
    runtimeDriven: boolean;
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    persistenceAllowed: boolean;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    proof?: SignalProofGuard;
    /** The ONE fence reading this request took — see handleWake. */
    executionFence: ExecutionFenceReading;
  },
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  const baseMessage: AgentMessageInput = {
    contents: body.contents,
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const message = options.persistenceAllowed
    ? baseMessage
    : markPersistenceForbidden(baseMessage);
  // sendMessage requires a resourceId+threadId target for its idle branch.
  if (resourceId === undefined) {
    return json(
      {
        error:
          'this thread has no resourceId wired; message delivery needs one',
      },
      409,
    );
  }
  const behavior = requestedIdle(body);
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  const policyRefusal = await contentPolicyRefusal(
    options.inspectContent,
    createMessageSignal(message),
    durableBlockingRun?.runId,
  );
  if (policyRefusal) return policyRefusal;
  if (behavior === 'wake') {
    return handleWake({
      agent,
      deploymentTag,
      threadId,
      resourceId,
      principal: options.principal,
      entryPath,
      runtimeDriven: options.runtimeDriven,
      consultRunCap,
      startIdleRun,
      serializeWake,
      executionFence: options.executionFence,
      proof: options.proof,
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed: options.persistenceAllowed,
      memoryAvailable: options.memoryAvailable,
      message,
      deliverActive: (runId, activeMemoryAvailable, resolvedActiveBehavior) =>
        agent.sendMessage(message, {
          runId,
          threadId,
          resourceId,
          ifActive: { behavior: resolvedActiveBehavior },
          // A stale active id can disappear before core sends. Persist only
          // when both the memory and authorization gates allow the write.
          ifIdle: {
            behavior:
              activeMemoryAvailable && options.persistenceAllowed
                ? 'persist'
                : 'discard',
          },
        }),
      persist: () =>
        agent.sendMessage(message, {
          threadId,
          resourceId,
          ifIdle: { behavior: 'persist' },
        }),
    });
  }
  if (behavior === 'persist' && !options.persistenceAllowed) {
    return persistenceForbiddenResponse({ capped: false });
  }
  const activeBehavior = activeBehaviorForRun('deliver', durableBlockingRun);
  const activePersistenceForbidden =
    activeBehavior === 'persist' && !options.persistenceAllowed;
  const deliveredActiveBehavior = deliverableActiveBehavior(
    activeBehavior,
    options.persistenceAllowed,
  );
  const sent = await sendWithoutWake(
    agent,
    (sendOptions) => agent.sendMessage(message, sendOptions),
    {
      threadId,
      resourceId,
      ifActive: deliveredActiveBehavior,
      ifIdle: behavior,
    },
    options,
  );
  return noWakeResponse(
    sent,
    behavior,
    deliveredActiveBehavior,
    activePersistenceForbidden,
  );
}

async function handleQueue(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  options: {
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    persistenceAllowed: boolean;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    proof?: SignalProofGuard;
  },
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  if (resourceId === undefined) {
    return json(
      { error: 'this thread has no resourceId wired; queue needs one' },
      409,
    );
  }
  const message: AgentMessageInput = {
    contents: body.contents,
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  if (!options.persistenceAllowed) {
    return persistenceForbiddenResponse();
  }
  const policyRefusal = await contentPolicyRefusal(
    options.inspectContent,
    createMessageSignal(message),
    durableBlockingRun?.runId,
  );
  if (policyRefusal) return policyRefusal;
  if (!(await options.memoryAvailable())) return memoryUnavailableResponse();
  await options.proof?.check();
  options.proof?.assertActive();
  const result = agent.sendMessage(message, {
    threadId,
    resourceId,
    ifActive: { behavior: 'persist' },
    ifIdle: { behavior: 'persist' },
  });
  const decision = await result.accepted;
  if (result.persisted) await result.persisted;
  return json({ decision, signalId: result.signal.id });
}

async function handleSignal(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  consultRunCap: RunCapConsult | undefined,
  deploymentTag: string | undefined,
  entryPath: AgentEntryPath,
  startIdleRun: StartIdleRun | undefined,
  serializeWake: <T>(operation: () => Promise<T>) => Promise<T>,
  options: {
    runtimeDriven: boolean;
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    persistenceAllowed: boolean;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    proof?: SignalProofGuard;
    /** The ONE fence reading this request took — see handleWake. */
    executionFence: ExecutionFenceReading;
  },
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  // Route-level tagName defense: reject a non-XML-name tagName HERE with a
  // 400, rather than letting core's signalToXmlMarkup throw at render time inside
  // the agent turn. Core still escapes contents/attribute values and re-validates
  // names.
  if (
    typeof body.tagName === 'string' &&
    !XML_NAME_PATTERN.test(body.tagName)
  ) {
    return json({ error: 'tagName is not a valid XML name' }, 400);
  }
  if (resourceId === undefined) {
    return json(
      {
        error: 'this thread has no resourceId wired; signal delivery needs one',
      },
      409,
    );
  }
  const baseSignal: AgentSignal = {
    type: 'reactive',
    contents: body.contents,
    ...(typeof body.tagName === 'string' ? { tagName: body.tagName } : {}),
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const signal = options.persistenceAllowed
    ? baseSignal
    : markPersistenceForbidden(baseSignal);
  const activeBehavior: ActiveBehavior =
    typeof body.ifActive === 'string' &&
    (ACTIVE_BEHAVIORS as readonly string[]).includes(body.ifActive)
      ? (body.ifActive as ActiveBehavior)
      : 'deliver';
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  const policyRefusal = await contentPolicyRefusal(
    options.inspectContent,
    createSignal(signal),
    durableBlockingRun?.runId,
  );
  if (policyRefusal) return policyRefusal;
  const effectiveActiveBehavior = activeBehaviorForRun(
    activeBehavior,
    durableBlockingRun,
  );
  const activePersistenceForbidden =
    effectiveActiveBehavior === 'persist' && !options.persistenceAllowed;
  const deliveredActiveBehavior = deliverableActiveBehavior(
    effectiveActiveBehavior,
    options.persistenceAllowed,
  );
  const behavior = requestedIdle(body);
  if (behavior === 'wake') {
    return handleWake({
      agent,
      deploymentTag,
      threadId,
      resourceId,
      principal: options.principal,
      entryPath,
      runtimeDriven: options.runtimeDriven,
      consultRunCap,
      startIdleRun,
      serializeWake,
      executionFence: options.executionFence,
      proof: options.proof,
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed: options.persistenceAllowed,
      memoryAvailable: options.memoryAvailable,
      signal,
      activeBehavior,
      deliverActive: (runId, activeMemoryAvailable, resolvedActiveBehavior) =>
        agent.sendSignal(signal, {
          runId,
          threadId,
          resourceId,
          ifActive: { behavior: resolvedActiveBehavior },
          ifIdle: {
            behavior:
              activeMemoryAvailable && options.persistenceAllowed
                ? 'persist'
                : 'discard',
          },
        }),
      persist: () =>
        agent.sendSignal(signal, {
          threadId,
          resourceId,
          ifActive: { behavior: deliveredActiveBehavior },
          ifIdle: { behavior: 'persist' },
        }),
    });
  }
  if (behavior === 'persist' && !options.persistenceAllowed) {
    return persistenceForbiddenResponse({ capped: false });
  }
  const sent = await sendWithoutWake(
    agent,
    (sendOptions) => agent.sendSignal(signal, sendOptions),
    {
      threadId,
      resourceId,
      ifActive: deliveredActiveBehavior,
      ifIdle: behavior,
    },
    options,
  );
  return noWakeResponse(
    sent,
    behavior,
    deliveredActiveBehavior,
    activePersistenceForbidden,
  );
}

function noWakeResponse(
  sent: Awaited<ReturnType<typeof sendWithoutWake>>,
  behavior: Exclude<IdleBehavior, 'wake'>,
  deliveredActiveBehavior: ActiveBehavior,
  activePersistenceForbidden: boolean,
): Response {
  const { decision, signal, wasActive, memoryAvailable } = sent;
  const action = recordValue(decision)?.action;
  if (action === 'persist' && !memoryAvailable) {
    return memoryUnavailableResponse();
  }
  // An idle discard is either the caller's `ifIdle: 'discard'` or the memory
  // gate's substitution, handled by the next block.
  if (
    action === 'discard' &&
    behavior === 'persist' &&
    !memoryAvailable &&
    !(wasActive && deliveredActiveBehavior === 'discard')
  ) {
    return memoryUnavailableResponse();
  }
  if (activePersistenceForbidden && wasActive && action === 'discard') {
    return persistenceForbiddenResponse({ capped: false });
  }
  return json({
    decision,
    capped: false,
    signalId: signal.id,
  });
}

/**
 * Send a signal or message that never wakes a run, behind the proof gate.
 * Without agent memory the idle behavior becomes `discard`, because core's
 * idle persist reports `persist` even when no memory wrote the signal.
 */
async function sendWithoutWake(
  agent: Agent,
  send: (sendOptions: {
    threadId: string;
    resourceId: string;
    ifActive: { behavior: ActiveBehavior };
    ifIdle: { behavior: Exclude<IdleBehavior, 'wake'> };
  }) => WakeDelivery,
  target: {
    threadId: string;
    resourceId: string;
    ifActive: ActiveBehavior;
    ifIdle: Exclude<IdleBehavior, 'wake'>;
  },
  options: { memoryAvailable: MemoryAvailable; proof?: SignalProofGuard },
): Promise<{
  decision: unknown;
  signal: { id: string };
  wasActive: boolean;
  memoryAvailable: boolean;
}> {
  const memoryAvailable = await options.memoryAvailable();
  const wasActive =
    activeThreadRunIdOf(agent, target.threadId, target.resourceId) !==
    undefined;
  await options.proof?.check();
  options.proof?.assertActive();
  const result = send({
    threadId: target.threadId,
    resourceId: target.resourceId,
    ifActive: { behavior: target.ifActive },
    ifIdle: { behavior: memoryAvailable ? target.ifIdle : 'discard' },
  });
  const decision = await result.accepted;
  if (result.persisted) await result.persisted;
  return { decision, signal: result.signal, wasActive, memoryAvailable };
}

async function handleState(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  options: {
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    persistenceAllowed: boolean;
    runtimeDriven: boolean;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    proof?: SignalProofGuard;
  },
): Promise<Response> {
  if (typeof body.id !== 'string' || typeof body.cacheKey !== 'string') {
    return json(
      { error: 'id and cacheKey are required for a state signal' },
      400,
    );
  }
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  if (resourceId === undefined) {
    return json(
      { error: 'this thread has no resourceId wired; state needs one' },
      409,
    );
  }
  const mode = body.mode === 'delta' ? 'delta' : 'snapshot';
  const state: AgentStateSignalInput = {
    id: body.id,
    cacheKey: body.cacheKey,
    contents: body.contents,
    mode,
    ...(mode === 'snapshot' ? { value: body.value } : { delta: body.delta }),
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  if (!options.persistenceAllowed) {
    return persistenceForbiddenResponse();
  }
  // Core's createStateSignalInput strips id/cacheKey/mode/value/delta out of the
  // signal it renders (they ride in metadata, which signalToXmlMarkup never
  // emits) and defaults tagName to 'state'. Mirror exactly that, so the gate
  // inspects the markup the model will see and nothing else.
  const policyRefusal = await contentPolicyRefusal(
    options.inspectContent,
    createSignal({
      type: 'state',
      tagName: 'state',
      contents: state.contents,
      ...(state.attributes !== undefined
        ? { attributes: state.attributes }
        : {}),
    }),
    durableBlockingRun?.runId,
  );
  if (policyRefusal) return policyRefusal;
  if (!(await options.memoryAvailable())) return memoryUnavailableResponse();
  await options.proof?.check();
  options.proof?.assertActive();
  const result = await agent.sendStateSignal(state, {
    threadId,
    resourceId,
    ...(!options.runtimeDriven
      ? { ifActive: { behavior: 'persist' as const } }
      : {}),
    ifIdle: { behavior: 'persist' },
  });
  // A snapshot whose cacheKey value is unchanged is de-duped (skipped) — no run
  // touched, no signal minted. Surface that distinctly rather than pretend a
  // delivery happened.
  if (result.skipped) {
    return json({ skipped: true, reason: result.reason });
  }
  const decision = await result.accepted;
  if (result.persisted) await result.persisted;
  const degraded: RouteDegradation = 'not-runtime-driven';
  return json({
    decision,
    signalId: result.signal.id,
    ...(!options.runtimeDriven ? { degraded } : {}),
  });
}

function hasDueTime(row: NotificationRecord): boolean {
  return row.deliverAt !== undefined || row.summaryAt !== undefined;
}

async function matchingPendingNotifications(
  storage: NotificationsStorage,
  notification: SendNotificationSignalInput,
  threadId: string,
  resourceId: string,
  agentId: string,
): Promise<NotificationRecord[]> {
  if (!notification.dedupeKey && !notification.coalesceKey) return [];
  const pending = await storage.listNotifications({
    threadId,
    status: 'pending',
    source: notification.source,
    agentId,
    resourceId,
  });
  // Storage coalesces into a pending row with the same source, kind, agent,
  // resource and key. D1 can match an empty key when the other key is truthy.
  return pending.filter(
    (row) =>
      row.kind === notification.kind &&
      ((notification.dedupeKey !== undefined &&
        row.dedupeKey === notification.dedupeKey) ||
        (notification.coalesceKey !== undefined &&
          row.coalesceKey === notification.coalesceKey)),
  );
}

async function handleNotification(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  resolveNotificationsStorage:
    | (() => NotificationsStorage | Promise<NotificationsStorage>)
    | undefined,
  options: {
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    persistenceAllowed: boolean;
    runtimeDriven: boolean;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    notificationDispatchAllowed?: () => boolean | Promise<boolean>;
    proof?: SignalProofGuard;
  },
): Promise<Response> {
  if (
    typeof body.source !== 'string' ||
    typeof body.kind !== 'string' ||
    typeof body.summary !== 'string'
  ) {
    return json(
      { error: 'source, kind and summary are required for a notification' },
      400,
    );
  }
  const notification: SendNotificationSignalInput = {
    source: body.source,
    kind: body.kind,
    summary: body.summary,
    ...(typeof body.priority === 'string' &&
    ['low', 'medium', 'high', 'urgent'].includes(body.priority)
      ? { priority: body.priority as SendNotificationSignalInput['priority'] }
      : {}),
    ...(body.payload !== undefined ? { payload: body.payload } : {}),
    ...(typeof body.dedupeKey === 'string'
      ? { dedupeKey: body.dedupeKey }
      : {}),
    ...(typeof body.coalesceKey === 'string'
      ? { coalesceKey: body.coalesceKey }
      : {}),
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  // The target requires a resourceId because the inbox is keyed by its owner.
  if (resourceId === undefined) {
    return json(
      { error: 'this thread has no resourceId wired; notifications need one' },
      409,
    );
  }
  // An owner's notification to a runtime-driven agent is recorded, then
  // delivered at ingestion under the owner's principal: into the owner's
  // running run, otherwise persisted to memory. It never wakes a run, and its
  // row is never due, so the dispatcher, whose deliveries and wakes belong to
  // the dispatch principal, never selects it. Core's sendNotificationSignal
  // cannot serve it: its in-process delivery reads the notifications store
  // from the agent's own Mastra, and the FlowSafe wrapper is registered on
  // none. The branch keys on the brand rather than on Mastra registration
  // because a registered wrapper still carries no delivery policy (see
  // "DurableAgent does not forward the wrapped Agent's `notifications`" in
  // agent-runner/durable-agent-runner.ts).
  if (options.persistenceAllowed && options.runtimeDriven) {
    return handleOwnerNotification(
      agent,
      notification,
      threadId,
      resourceId,
      resolveNotificationsStorage,
      options,
    );
  }
  // On core's delivery path this gate is AUTHORITATIVE, not a preview: core can
  // send an individual or summary signal before the record reaches the
  // dispatcher's second gate. On the record-only branch it refuses the row
  // before it is written. Storage owns the id, timestamps, and coalescing, so
  // inspect a prospective record carrying every untrusted model-visible field
  // instead.
  if (options.inspectContent) {
    const prospectiveRecord: NotificationRecord = {
      id: 'prospective',
      threadId,
      resourceId,
      agentId: agent.id,
      source: notification.source,
      kind: notification.kind,
      summary: notification.summary,
      priority: notification.priority ?? 'medium',
      status: 'pending',
      coalescedCount: 1,
      ...(notification.attributes !== undefined
        ? { attributes: notification.attributes }
        : {}),
      createdAt: new Date(0),
      updatedAt: new Date(0),
    };
    // Core's delivery policy, or the dispatcher on a later tick, picks between
    // two renderings, so inspect both. The individual signal renders
    // `status="delivered"` because that is the status its sender stamps on it;
    // the summary renders from the row as stored, which is why it keeps
    // `status:'pending'`.
    for (const candidate of [
      createNotificationSignal({ ...prospectiveRecord, status: 'delivered' }),
      createNotificationSummarySignal(
        summarizeNotifications([prospectiveRecord]),
      ),
    ]) {
      const policyRefusal = await contentPolicyRefusal(
        options.inspectContent,
        candidate,
      );
      if (policyRefusal) return policyRefusal;
    }
  }
  // A non-owner's notification is record-only for any agent: the row is due
  // now, and the host's createNotificationDispatchTick delivers it as the
  // dispatch principal. A host without that tick records it but never
  // delivers it. The record-only branch bypasses an agent-level
  // notifications.deliveryPolicy and __ensureNotificationDispatchReady.
  if (!options.persistenceAllowed) {
    const storage = await resolveNotificationsStorage?.();
    if (!storage) {
      return json({ error: 'notifications storage unavailable' }, 409);
    }
    if (
      options.notificationDispatchAllowed &&
      !(await options.notificationDispatchAllowed())
    ) {
      return ownerNotificationRefusal('notification-dispatch-forbidden');
    }
    // Owner residue cannot become due under the dispatch principal.
    if (
      (
        await matchingPendingNotifications(
          storage,
          notification,
          threadId,
          resourceId,
          agent.id,
        )
      ).some((row) => !hasDueTime(row))
    ) {
      return ownerNotificationRefusal('notification-pending');
    }
    await options.proof?.check();
    options.proof?.assertActive();
    const record = await storage.createNotification({
      ...notification,
      threadId,
      resourceId,
      agentId: agent.id,
      deliverAt: new Date(),
    });
    return json({
      record,
      delivery: { action: 'deferred', reason: 'dispatcher' },
    });
  }
  // An unbranded agent's owner notification uses core's delivery policy, whose
  // summary wake override can start a run below this boundary, outside
  // RunnerRuntime, the host entry gate and the run cap (@mastra/core 1.67.0,
  // agent-Dk0N0Nlg.js:38431, :38447). The inbox row is the durable artifact, so
  // that path has no memory gate: without agent memory, a model-visible
  // persist is best-effort and the row stays pending.
  const notificationsStore = await agent
    .getMastraInstance?.()
    ?.getStorage()
    ?.getStore('notifications');
  if (!notificationsStore) {
    return json({ error: 'notifications storage unavailable' }, 409);
  }
  await options.proof?.check();
  options.proof?.assertActive();
  const result = await agent.sendNotificationSignal(notification, {
    threadId,
    resourceId,
    ifActive: { behavior: 'persist' },
    ifIdle: { behavior: 'persist' },
  });
  if (result.persisted) await result.persisted;
  const response: {
    record: typeof result;
    delivery?: Awaited<NonNullable<typeof result.accepted>>;
    degraded: RouteDegradation;
  } = {
    record: result,
    degraded: 'not-runtime-driven',
  };
  if (result.accepted) response.delivery = await result.accepted;
  return json(response);
}

async function handleOwnerNotification(
  agent: Agent,
  notification: SendNotificationSignalInput,
  threadId: string,
  resourceId: string,
  resolveNotificationsStorage:
    | (() => NotificationsStorage | Promise<NotificationsStorage>)
    | undefined,
  options: {
    principal: ExecutionPrincipal;
    blockingRun: BlockingRunResolver | undefined;
    memoryAvailable: MemoryAvailable;
    inspectContent: InspectSignalContent | undefined;
    proof?: SignalProofGuard;
  },
): Promise<Response> {
  const durableBlockingRun = await options.blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, options.principal)
  ) {
    return notificationPrincipalMismatchResponse(durableBlockingRun.runId);
  }
  const ifActive = activeBehaviorForRun('deliver', durableBlockingRun);
  const memoryAvailable = await options.memoryAvailable();
  if (
    !memoryAvailable &&
    (ifActive === 'persist' ||
      activeThreadRunIdOf(agent, threadId, resourceId) === undefined)
  ) {
    return ownerNotificationRefusal('memory-unavailable');
  }
  const storage = await resolveNotificationsStorage?.();
  if (!storage) {
    return json({ error: 'notifications storage unavailable' }, 409);
  }
  if (
    (
      await matchingPendingNotifications(
        storage,
        notification,
        threadId,
        resourceId,
        agent.id,
      )
    ).some(hasDueTime)
  ) {
    return ownerNotificationRefusal('notification-pending');
  }
  await options.proof?.check();
  options.proof?.assertActive();
  // No deliverAt or summaryAt, so no dispatch pass ever selects the row.
  const stored = await storage.createNotification({
    ...notification,
    id: crypto.randomUUID(),
    threadId,
    resourceId,
    agentId: agent.id,
  });
  const now = new Date();
  let settled = false;
  let accepted:
    | { signalId: string; delivery: Record<string, unknown> }
    | undefined;
  // Every settle write passes the proof gate first. When the gate refuses, the
  // row stays pending with no due time rather than bypass it.
  const settle = async (
    patch: Pick<
      UpdateNotificationInput,
      'status' | 'deliveredSignalId' | 'deliveryReason'
    >,
  ): Promise<NotificationRecord> => {
    await options.proof?.check();
    options.proof?.assertActive();
    const record = await storage.updateNotification({
      ...patch,
      id: stored.id,
      threadId: stored.threadId,
      lastDeliveryAttemptAt: now,
    });
    settled = true;
    return record;
  };
  try {
    // Built from the stored row, which a coalesce may have merged, so the
    // policy inspects exactly the signal that is sent.
    const signal = createNotificationSignal({
      ...stored,
      status: 'delivered',
      deliveredAt: now,
    });
    const inspection = options.inspectContent
      ? await options.inspectContent(signal, durableBlockingRun?.runId)
      : 'allowed';
    if (inspection !== 'allowed') {
      await settle({
        status: 'discarded',
        deliveryReason:
          inspection === 'denied'
            ? 'content-policy-denied'
            : 'content-policy-error',
      });
      return signalContentPolicyResponse(inspection);
    }
    const sent = await sendWithoutWake(
      agent,
      (sendOptions) => agent.sendSignal(signal, sendOptions),
      { threadId, resourceId, ifActive, ifIdle: 'persist' },
      options,
    );
    const decision = recordValue(sent.decision);
    const action = decision?.action;
    if (
      action === 'deliver' ||
      (action === 'persist' && sent.memoryAvailable)
    ) {
      accepted = {
        signalId: sent.signal.id,
        delivery: { ...decision, signalId: sent.signal.id },
      };
      const record = await settle({
        status: 'delivered',
        deliveredSignalId: accepted.signalId,
      });
      return json({ record, delivery: accepted.delivery });
    }
    const cause =
      (action === 'persist' || action === 'discard') && !sent.memoryAvailable
        ? 'memory-unavailable'
        : action === 'blocked'
          ? 'thread-blocked'
          : 'delivery-failed';
    await settle({ status: 'discarded', deliveryReason: cause });
    if (cause === 'delivery-failed') {
      throw new Error(
        `owner notification was not delivered (${String(action ?? 'unknown')})`,
      );
    }
    return ownerNotificationRefusal(cause);
  } catch (error) {
    if (isExecutionFenceRefusal(error)) throw error;
    if (accepted !== undefined) {
      const record = await settle({
        status: 'delivered',
        deliveredSignalId: accepted.signalId,
      });
      return json({ record, delivery: accepted.delivery });
    }
    if (!settled) {
      await settle({ status: 'discarded', deliveryReason: 'delivery-failed' });
    }
    throw error;
  }
}
