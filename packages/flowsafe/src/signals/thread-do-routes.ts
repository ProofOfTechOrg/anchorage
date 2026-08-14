// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) — the signal routes hosted ON the thread DO (CI-M-004-001,
// DL-002). A thread's agent loop and every signal for that thread both address
// `idFromName(threadId)`, so the platform serializes them onto ONE isolate — the
// DO IS the serialization lease Mastra otherwise wants Redis for (DL-002). These
// routes run AFTER ThreadDurableObject.fetch has already verified the deployment
// identity and decoded the server-stamped principal, so `scope.threadId` and
// `scope.principal` are trusted here; the P6 ingestion gate (allowlist / size
// cap / rate cap / audit) is the Worker-side createSignalRouter's job, the same
// split createRunRouter (Worker gate) → DurableObjectRunner (execution) uses.
//
// AFFINITY IS THE PUBSUB. Core keys its in-process signal registry by the pubsub
// instance passed to each agent method (`#statesByPubSub`, falling back to a
// module-level `defaultAgentThreadPubSub`), so a send only drains into an active
// loop when BOTH run in one isolate (the DO gives this) AND both use the SAME
// pubsub. The agent resolves its pubsub from `agent.getPubSub()`, so these routes
// stamp the DO's ONE identity (`scope.init.pubsub`, Track 0 / DL-001) onto the
// agent before every call — the exact reason Track A threads that same identity
// into createRun. Absent (host opted out) ⇒ core's module default, still one per
// isolate, so affinity holds either way; a wired pubsub additionally makes
// observe()/replay align (pubsub.ts).
//
// core's `agentThreadStreamRuntime` is NOT on the package exports map, so these
// routes drive the PUBLIC Agent methods only (never a deep dist import — R-001).

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
  createNotificationSignal,
  createNotificationSummarySignal,
  type NotificationRecord,
  type NotificationsStorage,
  type SendNotificationSignalInput,
  summarizeNotifications,
} from '@mastra/core/notifications';
import { RequestContext } from '@mastra/core/request-context';
import {
  type AgentEntryPath,
  isRuntimeDrivenAgent,
} from '../agent-runner/index.js';
import {
  type ExecutionPrincipal,
  samePrincipal,
} from '../approval-api/index.js';
import {
  DoStatusError,
  isPathSafeId,
  type RunStatus,
  type ThreadScope,
} from '../do-runner/index.js';
import { internalErrorResponse } from '../internal-error-response.js';
import {
  createScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchAction,
  type ScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchState,
} from '../schedules/schedules-d1.js';
import type { AgentScheduleTarget } from '../schedules/tick.js';
import {
  deferNotificationAfterFailure,
  MAX_NOTIFICATION_DISPATCH_IDS,
  planNotificationDispatch,
} from './notification-dispatch.js';

/**
 * The idle-thread delivery behavior a send may ask for. `wake` starts a run
 * after consulting the run cap; `persist` writes the signal to the durable
 * inbox for the next turn; `discard` drops it. Mirrors core
 * `AgentSignalIdleBehavior`, re-declared so the route body validates the wire
 * value rather than trusting an `as`.
 */
export const IDLE_BEHAVIORS = ['wake', 'persist', 'discard'] as const;
export type IdleBehavior = (typeof IDLE_BEHAVIORS)[number];

/** The active-thread delivery behavior. Mirrors core `AgentSignalActiveBehavior`. */
export const ACTIVE_BEHAVIORS = ['deliver', 'persist', 'discard'] as const;
export type ActiveBehavior = (typeof ACTIVE_BEHAVIORS)[number];

/**
 * A run-cap consult for an idle-thread wake: starting a run with nobody
 * watching must charge the same deployment budget an unattended
 * schedule fire does, or a signal storm bills Cloudflare instead of exhausting a
 * quota. Returns false to REFUSE the wake (over cap) — the route then falls back
 * to `persist` (durable, no run) rather than dropping the signal. Absent ⇒ wake
 * is unmetered (hosts with no budget).
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

export interface ThreadSignalRoutesOptions {
  /**
   * The per-thread agent whose public signal methods these routes drive. Built
   * once per DO instance by the host (its model/memory/tools are the host's
   * concern); these routes only need its identity and pubsub.
   *
   * MUST be a runtime-driven durable agent for an idle wake. The host-provided
   * `startIdleRun` seam starts it through RunnerRuntime with a host-minted,
   * path-safe run id. Without either requirement, wake degrades to durable
   * persistence and never escapes onto core's default execution engine. Trusted
   * notification dispatch supplies the persisted agent id; other routes pass
   * `undefined`.
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
   * host. Absent ⇒ threadId-only keying (resourceId ''), which is consistent
   * within this DO but only interoperates with a loop that also omits it — the
   * binding expected by the registered durable run.
   */
  resolveResourceId?: (scope: ThreadScope) => string | undefined;
  /** Run-cap seam for idle-thread wakes. Absent means wakes are unmetered. */
  consultRunCap?: RunCapConsult;
  /** Runtime-driven start seam. Absent wakes degrade to durable persistence. */
  startIdleRun?: StartIdleRun;
  /** Storage-backed thread occupancy, including runs surviving DO eviction. */
  resolveBlockingRun?: (
    scope: ThreadScope,
  ) =>
    | Promise<{ runId: string; principal: ExecutionPrincipal } | undefined>
    | { runId: string; principal: ExecutionPrincipal }
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
  /** Whether this principal owns the thread and may persist future input. */
  canPersist?: (scope: ThreadScope) => boolean | Promise<boolean>;
  /** Whether the registered schedule owner may persist to its fixed target. */
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
  /** Durable inbox used by the trusted due-notification dispatch route. */
  resolveNotificationsStorage?: (
    scope: ThreadScope,
  ) => NotificationsStorage | Promise<NotificationsStorage>;
  /** Target-side lease and receipt store for at-least-once schedule fires. */
  resolveScheduleDispatchStore?: (
    scope: ThreadScope,
  ) => ScheduleSignalDispatchStore | Promise<ScheduleSignalDispatchStore>;
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
 * documented residual, not exposed over this untrusted channel in v1.
 */
function isContents(value: unknown): value is string {
  return typeof value === 'string';
}

/**
 * The XML-name rule core's `assertXmlName` applies to a signal's `tagName`
 * (and attribute names) inside `signalToXmlMarkup` — MIRRORED here, not
 * deep-imported: `XML_NAME_PATTERN` / `assertXmlName` are not on core's exports
 * map. Validating the caller-supplied `tagName` at INGEST turns an invalid one
 * into a 400 at the route rather than a
 * throw at render time inside the agent turn. Kept byte-identical to core
 * (chunk `signalToXmlMarkup`: `/^[A-Za-z_][A-Za-z0-9_.-]*$/`); the render test
 * pins core's own neutralization of contents/attribute values so this
 * mirror and that layer are checked together.
 */
const XML_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_.-]*$/;

/** Attributes must be a flat record of primitives (core `AgentSignalAttributes`). */
function isAttributes(value: unknown): value is AgentSignalAttributes {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return false;
  }
  return Object.values(value).every(
    (v) =>
      v === null ||
      v === undefined ||
      typeof v === 'string' ||
      typeof v === 'number' ||
      typeof v === 'boolean',
  );
}

export function createThreadSignalRoutes(
  options: ThreadSignalRoutesOptions,
): ThreadSignalRouter {
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
    resolveScheduleDispatchStore,
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

  const route: ThreadSignalRouter = async (request, scope) => {
    if (request.method !== 'POST') return null;
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== '/signal' && !path.startsWith('/signal/')) return null;
    const entryPath = entryPathForSignalRoute(path);
    if (entryPath === undefined) return json({ error: 'not found' }, 404);

    const body = await readJson(request);
    if (!body) return json({ error: 'a JSON body is required' }, 400);

    try {
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

      // Affinity: stamp the DO's ONE pubsub identity onto the agent so its signal
      // methods share the registry state the loop registered under. Keep host
      // resolution inside this catch-all: construction/storage failures are
      // internal and must not escape through the outer DO error response.
      const agent = await resolveAgent(scope, requestedAgentId, entryPath);
      if (requestedAgentId !== undefined && agent.id !== requestedAgentId) {
        return json({ error: 'agent binding does not match' }, 404);
      }
      const runtimeDriven = isRuntimeDrivenAgent(agent);
      const pubsub = scope.init.pubsub;
      if (pubsub) agent.__setPubSub(pubsub);

      const resourceId = resolveResourceId?.(scope);
      const threadId = scope.threadId;
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
          runtimeDriven,
          scope.principal,
          entryPath,
          startIdleRun,
          serializeWake,
          blockingRun,
          persistenceAllowed,
        );
      }
      // POST /signal/queue — deliver on the NEXT turn (never wakes).
      if (path === '/signal/queue') {
        return await handleQueue(
          agent,
          body,
          threadId,
          resourceId,
          scope.principal,
          blockingRun,
          persistenceAllowed,
        );
      }
      // POST /signal — a system signal (ifActive/ifIdle deliver/persist/discard/wake).
      if (path === '/signal') {
        return await handleSignal(
          agent,
          body,
          threadId,
          resourceId,
          consultRunCap,
          scope.deploymentTag,
          runtimeDriven,
          scope.principal,
          entryPath,
          startIdleRun,
          serializeWake,
          blockingRun,
          persistenceAllowed,
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
          schedulePersistenceAllowed: canPersistSchedule
            ? (input) => canPersistSchedule(scope, input)
            : undefined,
          resolveRunStatus: resolveScheduleRunStatus
            ? (input) => resolveScheduleRunStatus(scope, input)
            : undefined,
          store: await resolveScheduleDispatchStore(scope),
          completed: completedScheduleDispatches,
        });
      }
      // POST /signal/state — a durable thread-state lane (snapshot/delta).
      if (path === '/signal/state') {
        return await handleState(agent, body, threadId, resourceId);
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
            storage: await resolveNotificationsStorage(scope),
            agentId: requestedAgentId,
          }),
        );
      }
      // POST /signal/notification — the durable AGENT inbox (mastra_notifications).
      if (path === '/signal/notification') {
        return await serializeNotification(() =>
          handleNotification(agent, body, threadId, resourceId),
        );
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      if (
        error instanceof DoStatusError &&
        (error.status === 403 || error.status === 404 || error.status === 409)
      ) {
        const message =
          error.status === 403
            ? 'forbidden'
            : error.status === 404
              ? 'not found'
              : 'conflict';
        return json({ error: message }, error.status);
      }
      // A send that cannot be routed at all (e.g. an idle wake whose stream setup
      // throws — no model) rejects `accepted`; surface it as a 502 rather than a
      // 500 so a model/config fault reads distinctly from a route bug.
      return internalErrorResponse('signals.thread', error, 502);
    }
  };
  return (request, scope) => {
    if (request.method !== 'POST') return Promise.resolve(null);
    const path = new URL(request.url).pathname;
    if (path !== '/signal' && !path.startsWith('/signal/')) {
      return Promise.resolve(null);
    }
    return serializeDispatch
      ? serializeDispatch(scope, () => route(request, scope))
      : route(request, scope);
  };
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
  storage: NotificationsStorage;
  agentId: string;
}): Promise<Response> {
  const ids = options.body.notificationIds;
  if (
    !Array.isArray(ids) ||
    ids.length === 0 ||
    ids.length > MAX_NOTIFICATION_DISPATCH_IDS ||
    !ids.every((id): id is string => typeof id === 'string' && id.length > 0)
  ) {
    return json({ error: 'notificationIds must contain 1-100 strings' }, 400);
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
    return json(
      {
        error: 'notification principal does not match the active run',
        reason: 'principal-mismatch',
        runId: durableBlockingRun.runId,
        retry: true,
      },
      409,
    );
  }
  const now =
    typeof options.body.now === 'string'
      ? new Date(options.body.now)
      : new Date();
  if (Number.isNaN(now.getTime())) {
    return json({ error: 'now must be an ISO timestamp' }, 400);
  }
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

  const records: NotificationRecord[] = [];
  let skipped = 0;
  for (const id of uniqueIds) {
    const current = await options.storage.getNotification({
      threadId: options.threadId,
      id,
    });
    if (current?.status !== 'pending' || current.deliveredSignalId) {
      skipped += 1;
      continue;
    }
    if (
      current.resourceId !== options.resourceId ||
      current.agentId !== options.agentId
    ) {
      return json({ error: 'notification binding does not match' }, 404);
    }
    const due =
      (current.deliverAt !== undefined &&
        current.deliverAt.getTime() <= now.getTime()) ||
      (current.summaryAt !== undefined &&
        current.summaryAt.getTime() <= now.getTime());
    if (!due) {
      skipped += 1;
      continue;
    }
    records.push(current);
  }

  const batchThreadState: 'active' | 'idle' =
    requestedBatchThreadState === 'active' ||
    requestedBatchThreadState === 'idle'
      ? requestedBatchThreadState
      : durableBlockingRun ||
          (typeof options.agent.getActiveThreadRunId === 'function' &&
            options.agent.getActiveThreadRunId({
              threadId: options.threadId,
              resourceId,
            }))
        ? 'active'
        : 'idle';

  let delivered = 0;
  let failed = 0;
  const updateFailure = async (record: NotificationRecord, error: unknown) => {
    failed += 1;
    await deferNotificationAfterFailure(options.storage, record, now, error);
  };
  const send = async (signal: AgentSignal): Promise<string> => {
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
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed: options.persistenceAllowed,
      signal,
      deliverActive: (runId) =>
        options.agent.sendSignal(signal, {
          runId,
          threadId: options.threadId,
          resourceId,
          ifActive: { behavior: 'deliver' },
        }),
      persist: () =>
        options.agent.sendSignal(signal, {
          threadId: options.threadId,
          resourceId,
          ifIdle: { behavior: 'persist' },
        }),
    });
    if (!response.ok)
      throw new Error(`notification signal returned ${response.status}`);
    const result = (await response.json()) as {
      signalId?: string;
      decision?: unknown;
    };
    const action = recordValue(result.decision)?.action;
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
    const result = options.agent.sendSignal(signal, {
      threadId: options.threadId,
      resourceId,
      ifActive: { behavior: 'deliver' },
      ifIdle: { behavior: 'persist' },
    });
    await result.accepted;
    if (result.persisted) await result.persisted;
    return result.signal.id;
  };

  for (const item of planNotificationDispatch(records, now)) {
    if (item.type === 'summary') {
      try {
        const signal = createNotificationSummarySignal(
          summarizeNotifications(item.records),
        );
        const lowPriority = item.records.every(
          (record) => record.priority === 'low',
        );
        const signalId =
          lowPriority && options.persistenceAllowed
            ? await persistWithoutWake(signal)
            : await send(signal);
        for (const record of item.records) {
          await options.storage.updateNotification({
            id: record.id,
            threadId: record.threadId,
            summaryAt: null,
            summarySignalId: signalId,
            lastDeliveryAttemptAt: now,
          });
          delivered += 1;
        }
      } catch (error) {
        for (const record of item.records) await updateFailure(record, error);
      }
      continue;
    }

    const selected = item.record;
    try {
      const record = await options.storage.getNotification({
        threadId: selected.threadId,
        id: selected.id,
      });
      const summaryDue = Boolean(
        record?.summaryAt && record.summaryAt.getTime() <= now.getTime(),
      );
      const deliveryDue = Boolean(
        record?.deliverAt && record.deliverAt.getTime() <= now.getTime(),
      );
      if (
        record?.status !== 'pending' ||
        record.deliveredSignalId ||
        record.resourceId !== resourceId ||
        record.agentId !== options.agentId ||
        summaryDue ||
        !deliveryDue
      ) {
        skipped += 1;
        continue;
      }
      if (
        record.priority === 'high' &&
        record.summarySignalId &&
        batchThreadState === 'active'
      ) {
        skipped += 1;
        continue;
      }
      const signalId = await send(
        createNotificationSignal({
          ...record,
          status: 'delivered',
          deliveredAt: now,
        }),
      );
      await options.storage.updateNotification({
        id: record.id,
        threadId: record.threadId,
        status: 'delivered',
        deliveredSignalId: signalId,
        lastDeliveryAttemptAt: now,
      });
      delivered += 1;
    } catch (error) {
      await updateFailure(selected, error);
    }
  }

  return json({
    delivered,
    failed,
    ...(skipped > 0 ? { skipped } : {}),
    ...(carriesBatchThreadState ? { batchThreadState } : {}),
  });
}

/** The reason a requested wake was refused and degraded to a durable persist. */
type WakeRefusal = 'not-runtime-driven' | 'no-start-idle-run';

/**
 * Resolve the idle behavior a body asked for. A `wake` STARTS a run, so it is
 * gated twice, both fail-closed to a durable persist (the signal survives the
 * next turn rather than dropping or billing):
 *   - the agent must be RUNTIME-DRIVEN (its stream re-enters RunnerRuntime, not
 *     the default engine) — else `wakeRefused:'not-runtime-driven'`;
 *   - the deployment run cap must allow it — otherwise `capped:true`.
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
  | Promise<{ runId: string; principal: ExecutionPrincipal } | undefined>
  | { runId: string; principal: ExecutionPrincipal }
  | undefined;

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
  message?: AgentMessageInput;
  signal?: AgentSignal;
  runId?: string;
  scheduleId?: string;
  dispatchId?: string;
  safeContext?: Record<string, unknown>;
  deliverActive(runId: string): WakeDelivery;
  persist(): WakeDelivery;
}): Promise<Response> {
  return options.serializeWake(async () => {
    const durableBlockingRun = await options.blockingRun?.();
    const activeRunId =
      typeof options.agent.getActiveThreadRunId === 'function'
        ? options.agent.getActiveThreadRunId({
            threadId: options.threadId,
            resourceId: options.resourceId,
          })
        : undefined;
    if (
      durableBlockingRun &&
      !samePrincipal(durableBlockingRun.principal, options.principal)
    ) {
      return principalMismatchResponse(durableBlockingRun.runId);
    }
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
      const delivered = options.deliverActive(activeRunId);
      return json({
        decision: await delivered.accepted,
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

    const startIdleRun = options.startIdleRun;
    const refusal: WakeRefusal | undefined = !options.runtimeDriven
      ? 'not-runtime-driven'
      : !startIdleRun
        ? 'no-start-idle-run'
        : undefined;
    const capped =
      options.runtimeDriven && options.consultRunCap
        ? !(await options.consultRunCap())
        : false;
    if (refusal || capped) {
      if (!options.persistenceAllowed) {
        return json({
          decision: {
            action: 'discard',
            reason: 'persistence-forbidden',
          },
          capped,
          ...(refusal ? { wakeRefused: refusal } : {}),
          ...(options.signal?.id !== undefined
            ? { signalId: options.signal.id }
            : {}),
        });
      }
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
  if (target.tagName !== undefined && !XML_NAME_PATTERN.test(target.tagName)) {
    return json({ error: 'tagName is not a valid XML name' }, 400);
  }

  const completed = options.completed.get(dispatchId);
  if (completed) {
    await options.store.settle(scheduleId, dispatchId, completed);
    options.completed.delete(dispatchId);
    return json({ receipt: completed });
  }
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
    options.completed.set(dispatchId, receipt);
    await options.store.settle(scheduleId, dispatchId, receipt);
    options.completed.delete(dispatchId);
    return json({ receipt });
  }

  const baseProviderOptions = target.providerOptions ?? {};
  const baseMastra = recordValue(baseProviderOptions.mastra) ?? {};
  const signal: AgentSignal = {
    id: dispatchId,
    type: target.signalType ?? 'notification',
    tagName: target.tagName ?? 'schedule',
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
  const ifIdle = target.ifIdle ?? { behavior: 'wake' as const };
  const localActiveRunId =
    typeof options.agent.getActiveThreadRunId === 'function'
      ? options.agent.getActiveThreadRunId({
          threadId: options.threadId,
          resourceId,
        })
      : undefined;
  const persistenceRequested = localActiveRunId
    ? (ifActive.behavior ?? 'deliver') === 'persist'
    : (ifIdle.behavior ?? 'wake') === 'persist';
  const persistenceAllowed =
    persistenceRequested && options.schedulePersistenceAllowed
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
    const receipt: ScheduleAgentDispatchReceipt = {
      action: 'discard',
      outcome: 'discarded',
      signalId: dispatchId,
    };
    options.completed.set(dispatchId, receipt);
    await options.store.settle(scheduleId, dispatchId, receipt);
    options.completed.delete(dispatchId);
    return json({ receipt });
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
    ifActive,
    ifIdle: signalIfIdle,
  };

  let decision: unknown;
  let signalId: string | undefined;
  if ((ifIdle.behavior ?? 'wake') === 'wake') {
    const idleSignal: AgentSignal = {
      ...signal,
      ...(ifIdle.attributes !== undefined
        ? { attributes: { ...signal.attributes, ...ifIdle.attributes } }
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
      blockingRun: durableBlockingRun
        ? () => durableBlockingRun
        : options.blockingRun,
      persistenceAllowed,
      signal: idleSignal,
      runId,
      scheduleId,
      dispatchId,
      safeContext: { ...requestContext, ...idleRequestContext },
      deliverActive: (activeRunId) =>
        options.agent.sendSignal(signal, {
          runId: activeRunId,
          threadId: options.threadId,
          resourceId,
          ifActive,
        }),
      persist: () =>
        options.agent.sendSignal(signal, {
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
    const sent = options.agent.sendSignal(signal, signalTarget);
    decision = await sent.accepted;
    if (recordValue(decision)?.action === 'persist' && sent.persisted) {
      await sent.persisted;
    }
    signalId = sent.signal.id;
  }
  const receipt = scheduleReceipt(decision, signalId);
  if (!receipt) {
    throw new Error('agent schedule returned an invalid signal decision');
  }
  options.completed.set(dispatchId, receipt);
  await options.store.settle(scheduleId, dispatchId, receipt);
  options.completed.delete(dispatchId);
  return json({ receipt });
}

async function handleMessage(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  consultRunCap: RunCapConsult | undefined,
  deploymentTag: string | undefined,
  runtimeDriven: boolean,
  principal: ExecutionPrincipal,
  entryPath: AgentEntryPath,
  startIdleRun: StartIdleRun | undefined,
  serializeWake: <T>(operation: () => Promise<T>) => Promise<T>,
  blockingRun: BlockingRunResolver | undefined,
  persistenceAllowed: boolean,
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  const message: AgentMessageInput = {
    contents: body.contents,
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  // sendMessage requires a resourceId+threadId target for its idle branch; when a
  // host has not wired a resourceId, only the active/queue path is reachable.
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
  const durableBlockingRun = await blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  if (behavior === 'wake') {
    return handleWake({
      agent,
      deploymentTag,
      threadId,
      resourceId,
      principal,
      entryPath,
      runtimeDriven,
      consultRunCap,
      startIdleRun,
      serializeWake,
      blockingRun: durableBlockingRun ? () => durableBlockingRun : blockingRun,
      persistenceAllowed,
      message,
      deliverActive: (runId) =>
        agent.sendMessage(message, {
          runId,
          threadId,
          resourceId,
          ifActive: { behavior: 'deliver' },
        }),
      persist: () =>
        agent.sendMessage(message, {
          threadId,
          resourceId,
          ifIdle: { behavior: 'persist' },
        }),
    });
  }
  if (behavior === 'persist' && !persistenceAllowed) {
    return json({
      decision: { action: 'discard', reason: 'persistence-forbidden' },
      capped: false,
    });
  }
  const result = agent.sendMessage(message, {
    threadId,
    resourceId,
    ifIdle: { behavior },
  });
  const decision = await result.accepted;
  return json({
    decision,
    capped: false,
    signalId: result.signal.id,
  });
}

async function handleQueue(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  principal: ExecutionPrincipal,
  blockingRun: BlockingRunResolver | undefined,
  persistenceAllowed: boolean,
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
  const durableBlockingRun = await blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  if (!persistenceAllowed) {
    return json({
      decision: { action: 'discard', reason: 'persistence-forbidden' },
    });
  }
  const result = agent.queueMessage(message, { threadId, resourceId });
  const decision = await result.accepted;
  return json({ decision, signalId: result.signal.id });
}

async function handleSignal(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  consultRunCap: RunCapConsult | undefined,
  deploymentTag: string | undefined,
  runtimeDriven: boolean,
  principal: ExecutionPrincipal,
  entryPath: AgentEntryPath,
  startIdleRun: StartIdleRun | undefined,
  serializeWake: <T>(operation: () => Promise<T>) => Promise<T>,
  blockingRun: BlockingRunResolver | undefined,
  persistenceAllowed: boolean,
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  // Route-level tagName defense (C-S5): reject a non-XML-name tagName HERE with a
  // 400, rather than letting core's signalToXmlMarkup throw at render time inside
  // the agent turn. Core still escapes contents/attribute values and re-validates
  // names; this is the ingest-time half the plan calls "route-level defense".
  if (
    typeof body.tagName === 'string' &&
    !XML_NAME_PATTERN.test(body.tagName)
  ) {
    return json({ error: 'tagName is not a valid XML name' }, 400);
  }
  const signal: AgentSignal = {
    type: 'reactive',
    contents: body.contents,
    ...(typeof body.tagName === 'string' ? { tagName: body.tagName } : {}),
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const activeBehavior: ActiveBehavior =
    typeof body.ifActive === 'string' &&
    (ACTIVE_BEHAVIORS as readonly string[]).includes(body.ifActive)
      ? (body.ifActive as ActiveBehavior)
      : 'deliver';
  const durableBlockingRun = await blockingRun?.();
  if (
    durableBlockingRun &&
    !samePrincipal(durableBlockingRun.principal, principal)
  ) {
    return principalMismatchResponse(durableBlockingRun.runId);
  }
  if (resourceId === undefined) {
    // Active-only target: no idle branch available without a resourceId.
    const runId = crypto.randomUUID();
    if (!isPathSafeId(runId)) {
      throw new Error('thread signal generated a non-path-safe run id');
    }
    const result = agent.sendSignal(signal, {
      threadId,
      runId,
      ifActive: { behavior: activeBehavior },
    });
    const decision = await result.accepted;
    return json({ decision, signalId: result.signal.id });
  }
  const behavior = requestedIdle(body);
  if (behavior === 'wake') {
    return handleWake({
      agent,
      deploymentTag,
      threadId,
      resourceId,
      principal,
      entryPath,
      runtimeDriven,
      consultRunCap,
      startIdleRun,
      serializeWake,
      blockingRun: durableBlockingRun ? () => durableBlockingRun : blockingRun,
      persistenceAllowed,
      signal,
      deliverActive: (runId) =>
        agent.sendSignal(signal, {
          runId,
          threadId,
          resourceId,
          ifActive: { behavior: activeBehavior },
        }),
      persist: () =>
        agent.sendSignal(signal, {
          threadId,
          resourceId,
          ifActive: { behavior: activeBehavior },
          ifIdle: { behavior: 'persist' },
        }),
    });
  }
  if (behavior === 'persist' && !persistenceAllowed) {
    return json({
      decision: { action: 'discard', reason: 'persistence-forbidden' },
      capped: false,
    });
  }
  const result = agent.sendSignal(signal, {
    threadId,
    resourceId,
    ifActive: { behavior: activeBehavior },
    ifIdle: { behavior },
  });
  const decision = await result.accepted;
  return json({
    decision,
    capped: false,
    signalId: result.signal.id,
  });
}

async function handleState(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
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
  const result = await agent.sendStateSignal(state, { threadId, resourceId });
  // A snapshot whose cacheKey value is unchanged is de-duped (skipped) — no run
  // touched, no signal minted. Surface that distinctly rather than pretend a
  // delivery happened.
  if (result.skipped) {
    return json({ skipped: true, reason: result.reason });
  }
  const decision = await result.accepted;
  return json({ decision, signalId: result.signal.id });
}

async function handleNotification(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
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
  // agent.sendNotificationSignal persists via the Mastra storage's notifications
  // domain (mastra_notifications) — D1-backed once createD1Storage composes
  // D1NotificationsStorage — and returns the created record. The durable inbox
  // surfaces on the next turn (the dispatcher reads it at run start); nothing
  // wakes here (P6/P8: an inbound notification is untrusted context, never a
  // capability). Core's notification target REQUIRES a resourceId (it keys the
  // inbox on the owner), so a thread with none wired cannot take one.
  if (resourceId === undefined) {
    return json(
      { error: 'this thread has no resourceId wired; notifications need one' },
      409,
    );
  }
  const result = await agent.sendNotificationSignal(notification, {
    threadId,
    resourceId,
  });
  if (result.persisted) await result.persisted;
  return json({ record: result });
}
