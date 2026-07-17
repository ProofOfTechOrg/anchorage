// SPDX-License-Identifier: Apache-2.0
// Track C (M-004) — the signal routes hosted ON the thread DO (CI-M-004-001,
// DL-002). A thread's agent loop and every signal for that thread both address
// `idFromName(threadId)`, so the platform serializes them onto ONE isolate — the
// DO IS the serialization lease Mastra otherwise wants Redis for (DL-002). These
// routes run AFTER ThreadDurableObject.fetch has already asserted the request's
// authenticated tenant against the DO name's tenant prefix, so `scope.threadId`
// and `scope.tenantId` are trusted here; the P6 ingestion gate (allowlist / size
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
  AgentStateSignalInput,
} from '@mastra/core/agent';
import type { SendNotificationSignalInput } from '@mastra/core/notifications';

import { isRuntimeDrivenAgent } from '../agent-runner/index.js';
import type { ThreadScope } from '../do-runner/index.js';

/**
 * The idle-thread delivery behavior a send may ask for. `wake` starts a run
 * (run cap consulted first, DL-007); `persist` writes the signal to the durable
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
 * A run-cap consult for an idle-thread WAKE (DL-007): starting a run with nobody
 * watching must charge the same per-tenant + global budget an unattended
 * schedule fire does, or a signal storm bills Cloudflare instead of exhausting a
 * quota. Returns false to REFUSE the wake (over cap) — the route then falls back
 * to `persist` (durable, no run) rather than dropping the signal. Absent ⇒ wake
 * is unmetered (single-tenant hosts with no budget).
 */
export type RunCapConsult = (tenantId: string) => Promise<boolean> | boolean;

export interface ThreadSignalRoutesOptions {
  /**
   * The per-thread agent whose public signal methods these routes drive. Built
   * once per DO instance by the host (its model/memory/tools are the host's
   * concern); these routes only need its identity and pubsub. The scope carries
   * the asserted tenant so a host can refuse to build an agent for a thread it
   * does not recognize.
   *
   * MUST be a RUNTIME-DRIVEN durable agent (`createFlowsafeDurableAgent`, which
   * carries the `RUNTIME_DRIVEN_AGENT` brand) for the idle-WAKE path to run a
   * loop through RunnerRuntime. The wake is the only signal path that STARTS a
   * run: it calls `agent.stream(…, { ifIdle:'wake' })`, and for a plain core
   * `Agent` (or a stock `DurableAgent`) that stream runs the loop on the DEFAULT
   * engine, OUTSIDE RunnerRuntime — a second execution path DL-001 forbids,
   * tenant-unscoped and grant-underivable. So a wake requested on a
   * non-runtime-driven agent is refused fail-closed (degraded to a durable
   * persist, `wakeRefused:'not-runtime-driven'` in the response) rather than
   * allowed to escape. The persist/queue/state/notification/active-deliver paths
   * never start a run, so they work on any agent; only the wake needs the brand.
   *
   * RESIDUAL (owned by Track A's real-loop wiring, not this seam): even a
   * runtime-driven agent's woken run gets a bare `crypto.randomUUID()` runId —
   * core mints it inside `agentThreadStreamRuntime.sendSignal` and the public
   * send API cannot override it — so full `${tenantId}_${uuid}` INV-1 scoping of
   * idle-wake runs (purge, grant derivation) awaits Track A supplying the wake's
   * tenant→runId seam. This brand closes only the immediate off-runtime escape.
   */
  resolveAgent: (scope: ThreadScope) => Agent;
  /**
   * The thread's tenant-owned memory resourceId — part of core's `(resourceId,
   * threadId)` signal key, so it MUST match whatever the loop registered under
   * or a send never finds the active run. Server-derived (a memory id is
   * TCB-only — never a client field); the host mints it from the authenticated
   * tenant. Absent ⇒ threadId-only keying (resourceId ''), which is consistent
   * within this DO but only interoperates with a loop that also omits it — the
   * documented residual until Track A's real loop pins the binding.
   */
  resolveResourceId?: (scope: ThreadScope) => string | undefined;
  /** Run-cap seam for idle-thread wakes (DL-007). Absent ⇒ wakes are unmetered. */
  consultRunCap?: RunCapConsult;
}

/**
 * A thread-DO signal router: `(request, scope) => Response | null`. `null` means
 * the path is not one of ours, so the subclass's `route()` can compose it ahead
 * of its own routes (Track A's agent-loop drive). `scope` is the already-asserted
 * ThreadScope (threadId, tenantId, init) the template-method `fetch` hands down.
 */
export type ThreadSignalRouter = (
  request: Request,
  scope: ThreadScope,
) => Promise<Response | null>;

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
 * the P6 ingestion gate can size-cap and (defense-in-depth) escape uniformly.
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
 * into a 400 at the route (the "route-level defense" C-S5 names) rather than a
 * throw at render time inside the agent turn. Kept byte-identical to core
 * (chunk `signalToXmlMarkup`: `/^[A-Za-z_][A-Za-z0-9_.-]*$/`); the C-S5 render
 * test pins core's own neutralization of contents/attribute values so this
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
  const { resolveAgent, resolveResourceId, consultRunCap } = options;

  return async (request, scope) => {
    if (request.method !== 'POST') return null;
    const url = new URL(request.url);
    const path = url.pathname;
    if (path !== '/signal' && !path.startsWith('/signal/')) return null;

    const body = await readJson(request);
    if (!body) return json({ error: 'a JSON body is required' }, 400);

    // Affinity: stamp the DO's ONE pubsub identity onto the agent so its signal
    // methods share the registry state the loop registered under. Idempotent —
    // safe to re-apply per request. Absent ⇒ agent keeps core's module default,
    // still one per isolate.
    const agent = resolveAgent(scope);
    // The wake path starts a run, safe only through a runtime-driven agent
    // (resolveAgent's contract). Computed ONCE and threaded into the two idle
    // paths, so a plain agent's wake degrades to a durable persist rather than
    // escaping onto the default engine.
    const runtimeDriven = isRuntimeDrivenAgent(agent);
    const pubsub = scope.init.pubsub;
    if (pubsub) agent.__setPubSub(pubsub);

    const resourceId = resolveResourceId?.(scope);
    const threadId = scope.threadId;

    try {
      // POST /signal/message — immediate user message (joins the active loop or,
      // idle, wakes/persists per ifIdle).
      if (path === '/signal/message') {
        return await handleMessage(
          agent,
          body,
          threadId,
          resourceId,
          consultRunCap,
          scope.tenantId,
          runtimeDriven,
        );
      }
      // POST /signal/queue — deliver on the NEXT turn (never wakes).
      if (path === '/signal/queue') {
        return await handleQueue(agent, body, threadId, resourceId);
      }
      // POST /signal — a system signal (ifActive/ifIdle deliver/persist/discard/wake).
      if (path === '/signal') {
        return await handleSignal(
          agent,
          body,
          threadId,
          resourceId,
          consultRunCap,
          scope.tenantId,
          runtimeDriven,
        );
      }
      // POST /signal/state — a durable thread-state lane (snapshot/delta).
      if (path === '/signal/state') {
        return await handleState(agent, body, threadId, resourceId);
      }
      // POST /signal/notification — the durable AGENT inbox (mastra_notifications).
      if (path === '/signal/notification') {
        return await handleNotification(agent, body, threadId, resourceId);
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      // A send that cannot be routed at all (e.g. an idle wake whose stream setup
      // throws — no model) rejects `accepted`; surface it as a 502 rather than a
      // 500 so a model/config fault reads distinctly from a route bug.
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        502,
      );
    }
  };
}

/** The reason a requested wake was refused and degraded to a durable persist. */
type WakeRefusal = 'not-runtime-driven';

/**
 * Resolve the idle behavior a body asked for. A `wake` STARTS a run, so it is
 * gated twice, both fail-closed to a durable persist (the signal survives the
 * next turn rather than dropping or billing):
 *   - the agent must be RUNTIME-DRIVEN (its stream re-enters RunnerRuntime, not
 *     the default engine) — else `wakeRefused:'not-runtime-driven'`;
 *   - the per-tenant run cap must allow it (DL-007) — else `capped:true`.
 */
async function resolveIdle(
  body: Record<string, unknown>,
  consultRunCap: RunCapConsult | undefined,
  tenantId: string,
  runtimeDriven: boolean,
): Promise<{
  behavior: IdleBehavior;
  capped: boolean;
  wakeRefused?: WakeRefusal;
}> {
  const requested = body.ifIdle;
  const behavior: IdleBehavior =
    typeof requested === 'string' &&
    (IDLE_BEHAVIORS as readonly string[]).includes(requested)
      ? (requested as IdleBehavior)
      : 'persist';
  if (behavior === 'wake') {
    // A plain/ephemeral agent's wake would run the loop OFF RunnerRuntime (a
    // second execution path, DL-001; tenant-unscoped). Refuse it BEFORE the
    // send — persist instead, so the signal is kept, not escaped or dropped.
    if (!runtimeDriven) {
      return {
        behavior: 'persist',
        capped: false,
        wakeRefused: 'not-runtime-driven',
      };
    }
    // Over cap: degrade to a durable persist so an unattended wake storm bills a
    // quota, not Cloudflare (DL-007).
    if (consultRunCap && !(await consultRunCap(tenantId))) {
      return { behavior: 'persist', capped: true };
    }
  }
  return { behavior, capped: false };
}

async function handleMessage(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
  consultRunCap: RunCapConsult | undefined,
  tenantId: string,
  runtimeDriven: boolean,
): Promise<Response> {
  if (!isContents(body.contents)) {
    return json({ error: 'contents (string) is required' }, 400);
  }
  const message: AgentMessageInput = {
    contents: body.contents,
    ...(isAttributes(body.attributes) ? { attributes: body.attributes } : {}),
  };
  const { behavior, capped, wakeRefused } = await resolveIdle(
    body,
    consultRunCap,
    tenantId,
    runtimeDriven,
  );
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
  const result = agent.sendMessage(message, {
    threadId,
    resourceId,
    ifIdle: { behavior },
  });
  const decision = await result.accepted;
  return json({
    decision,
    capped,
    ...(wakeRefused ? { wakeRefused } : {}),
    signalId: result.signal.id,
  });
}

async function handleQueue(
  agent: Agent,
  body: Record<string, unknown>,
  threadId: string,
  resourceId: string | undefined,
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
  tenantId: string,
  runtimeDriven: boolean,
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
  if (resourceId === undefined) {
    // Active-only target: no idle branch available without a resourceId.
    const result = agent.sendSignal(signal, {
      threadId,
      runId: `${tenantId}_signal`,
      ifActive: { behavior: activeBehavior },
    });
    const decision = await result.accepted;
    return json({ decision, signalId: result.signal.id });
  }
  const { behavior, capped, wakeRefused } = await resolveIdle(
    body,
    consultRunCap,
    tenantId,
    runtimeDriven,
  );
  const result = agent.sendSignal(signal, {
    threadId,
    resourceId,
    ifActive: { behavior: activeBehavior },
    ifIdle: { behavior },
  });
  const decision = await result.accepted;
  return json({
    decision,
    capped,
    ...(wakeRefused ? { wakeRefused } : {}),
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
  const record = await agent.sendNotificationSignal(notification, {
    threadId,
    resourceId,
  });
  return json({ record });
}
