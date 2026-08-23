// SPDX-License-Identifier: Apache-2.0
// HTTP surface for the run catalog + run lifecycle, shared by every host.
//
// Mirrors createApprovalRouter's contract — plain fetch routing, an injected
// `authenticate`, and `null` for paths outside its ownership so a host Worker
// can compose it after the approval router. (Its two paths are fixed rather
// than configurable: unlike the approval surface, a host mounts exactly one run
// surface.) What it owns that the hosts used to triplicate is the route-specific
// AUTHORIZATION order:
//
//   start:   authenticate -> coarse role -> workflow role -> host policy
//   resume:    authenticate -> catalog -> ownership -> coarse role -> workflow
//              role -> host policy
//   terminate: authenticate -> catalog -> ownership -> coarse role -> workflow
//              role
//
// and the suspension bridge: a start that suspends queues its approval
// attributed to the STARTING actor, so that actor cannot later decide their own
// run (separation of duties).
//
// The resume route deliberately carries NO grants. A forged `resumeData.approved`
// can flip a workflow boolean, but capability comes only from the server-derived
// grant the runtime mints per leg (approval-api/grants.ts), so a side-effecting
// step re-checks and fails closed. Approve through the queue, not this route.

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRecord,
  type ExecutionPrincipal,
  type ExecutionPrincipalKind,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import {
  beginIdempotentStart,
  DoStatusError,
  type ExecutionFenceWiring,
  InvalidRunRequestError,
  RunAlreadyExistsError,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  type RunSummary,
  RunTerminalConflictError,
  requireStartIdempotency,
  rollbackFencedStart,
  type StartIdempotencyWiring,
  type StartReservation,
  UnknownRunError,
  UnknownWorkflowError,
} from '../do-runner/index.js';
import { readBoundedBody } from '../http-body.js';
import { queueApprovalForSuspension } from './approval-bridge.js';
import { requireResourceAccess } from './resource-access.js';
import { RunRouteError } from './run-route-error.js';
import type { WorkflowMeta } from './workflow-meta.js';

export interface RunRouterOptions {
  /**
   * The catalog: GET /workflows lists these; POST /runs resolves against them
   * and enforces each one's `allowedRoles`.
   *
   * Metadata, not WorkflowModules: the router registers nothing, so depending
   * on the registration machinery would force every host to build a
   * WorkflowModuleContext (and its AuditLogger) just to expose a route. Hosts
   * pass `modules.map((m) => m.meta)`; that the ids match what was actually
   * committed is asserted at registration (see buildShowcaseRuntime).
   */
  workflows: ReadonlyArray<WorkflowMeta>;
  /**
   * Authenticates the request and resolves the actor-scoped approval service
   * and server-owned id minters. undefined yields 401.
   */
  resolve: ActorResolver;
  /**
   * System-principal id used to authorize bridge bookkeeping. Approval
   * requester provenance comes from the run summary, so this id need not be
   * globally disjoint from human ids. Default: 'flowsafe-system'.
   */
  systemPrincipalId?: string;
  /**
   * How this host honours `idempotencyKey` on POST /runs — the reservation
   * store plus the liveness probe that resolves a claimed-but-unpersisted run.
   *
   * REQUIRED, and with no `undefined` in the type, for the reason
   * ExecutionFenceWiring spells out: an option a host may omit is one a host
   * will omit, and the failure mode of omitting THIS one is silent. A router
   * that ignored an unwired key would answer an exactly-once request with
   * at-least-once behaviour, and the caller would have no way to find out. The
   * typed opt-out (`'none'`) is honest — it makes every keyed start refuse with
   * IDEMPOTENT_START_UNSUPPORTED — and unkeyed starts are unaffected either way.
   *
   * The probe travels WITH the store rather than beside it because a store
   * without one cannot answer the only question the reservation cannot settle
   * on its own, and a host that wired the first and forgot the second would
   * fall back to guessing.
   */
  startIdempotency: RunRouterStartIdempotency;
  /** Host topology: in-process runtime, or a DO stub fetch. */
  start: (input: RunStartInput) => Promise<RunSummary>;
  status: (
    workflowId: string,
    runId: string,
  ) => Promise<RunSummary | undefined>;
  resume: (
    workflowId: string,
    runId: string,
    body: unknown,
    requestedBy: string,
    requestedByKind: ExecutionPrincipalKind,
  ) => Promise<RunSummary>;
  /** Optional for compatibility; when supplied, mounts POST .../terminate. */
  terminate?: (
    workflowId: string,
    runId: string,
    principal: ExecutionPrincipal,
    replayOnly: boolean,
  ) => Promise<RunSummary>;
  /** Host policy that must pass immediately before a validated run start. */
  beforeStart?: (
    context: ActorContext,
    workflowId: string,
    inputData: unknown,
  ) => Promise<void>;
  /** Host policy that must pass immediately before a validated raw resume. */
  beforeResume?: (
    context: ActorContext,
    workflowId: string,
    runId: string,
    body: unknown,
  ) => Promise<void>;
  /**
   * Self-healing hook invoked after a status() read reports the run
   * suspended, so every status poll of a stuck run doubles as a check for a
   * gate whose approval never made it into the queue (see
   * reconcileApprovalsForSummary in approval-bridge.ts). Awaited rather than
   * fire-and-forget by default: this host-agnostic layer has no ctx.waitUntil
   * of its own to keep a detached promise alive past the response, so a
   * plain awaited call is what it can offer on its own. Reconciliation pages
   * the run's full approval history and may supersede stale open records
   * before filing a fresh one, so the two ctx-capable hosts
   * (deploy/worker.ts and the showcase worker) hand this hook a wrapper that
   * detaches the real work
   * via ctx.waitUntil and resolves immediately — this option's contract (an
   * awaited function of this exact shape) is unchanged either way, only
   * what a given host's function actually blocks on. A throw is caught and
   * logged here, never surfaced to the caller: a broken reconcile must not
   * turn a working status read into a 500; the next poll simply retries.
   * Absent => today's behavior (no reconciliation). Hosts wire
   * reconcileApprovalsOnStatus(systemPrincipalId) here, optionally wrapped for
   * waitUntil-detachment.
   */
  reconcileApprovals?: (
    context: ActorContext,
    workflowId: string,
    summary: RunSummary,
  ) => Promise<void>;
}

/**
 * The router's idempotent-start wiring: either the typed opt-out, or a store
 * paired with the liveness probe its replay decision depends on.
 *
 * `'none'` is written out as its own arm rather than allowed as a missing
 * `store` field, so a host declares the absence instead of arriving at it.
 */
export type RunRouterStartIdempotency =
  | 'none'
  | {
      store: Exclude<StartIdempotencyWiring, 'none'>;
      /**
       * Is a start for this run executing right now? Hosts wire
       * `createDoRunTopology(...).startLiveness`; an in-process host wires its
       * runtime's own `isRunActive`.
       */
      live: (workflowId: string, runId: string) => Promise<boolean>;
      /**
       * The deployment execution fence, so a REPLAY can re-assert a proof-only
       * fence's binding to the run this key already made.
       *
       * Named here rather than looked up, and required rather than optional,
       * because it must be the fence over the SAME database the reservation
       * lives in — a second store over a different binding would silently
       * re-bind nothing. `'none'` is the honest answer for a host with no fence.
       */
      fence: ExecutionFenceWiring;
    };

export interface RunStartInput {
  workflowId: string;
  runId: string;
  inputData: unknown;
  /** Full trusted identity stamped onto the target Durable Object request. */
  principal: ExecutionPrincipal;
  /** Present only for a target-verifiable schedule fire. */
  scheduleId?: string;
  /** Relative deadline measured from the accepted start. */
  deadlineMs?: number;
  /**
   * The reserved idempotency key, forwarded to the runtime on the trusted
   * channel so the execution fence's proof-only state can match it. Set by this
   * router from a reservation it already took — never copied straight from a
   * request body, which is what keeps a tenant from naming the proof key.
   * @internal
   */
  idempotencyKey?: string;
}

export type RunRouter = (request: Request) => Promise<Response | null>;

interface StartBody {
  workflowId?: string;
  runId?: string;
  inputData?: unknown;
  deadlineMs?: unknown;
  /**
   * A caller-chosen key that makes this start exactly-once for this caller.
   *
   * The one identifier a client MAY supply, and the reason the runId stays
   * refused two fields up: a key names a REQUEST, so the worst a caller can do
   * with a bad one is converge onto its own earlier run, while a runId names a
   * SLOT in every store this deployment has and a caller that could choose one
   * could collide with anybody's.
   */
  idempotencyKey?: unknown;
}

// no-store is defense in depth: this is an authenticated API served from the
// same origin as (and under) the SPA's `assets` block, and a run projection
// names the actors and payloads of an approval.
function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

function errorResponse(error: unknown): Response {
  if (error instanceof RunRouteError) {
    return json(
      {
        error: error.message,
        ...(error.reason === undefined ? {} : { reason: error.reason }),
      },
      error.status,
    );
  }
  if (error instanceof ActorResolutionError) {
    // Authenticated but malformed claims are a verifier bug, surfaced as
    // forbidden rather than a retryable 500.
    return json({ error: 'forbidden' }, 403);
  }
  if (
    error instanceof UnknownWorkflowError ||
    error instanceof UnknownRunError
  ) {
    return json({ error: error.message }, 404);
  }
  if (
    error instanceof RunNotSuspendedError ||
    error instanceof RunAlreadyExistsError ||
    error instanceof RunTerminalConflictError
  ) {
    return json({ error: error.message }, 409);
  }
  if (error instanceof RunLifecycleBlockedError) {
    return json({ error: error.message, reason: error.reason }, 409);
  }
  if (error instanceof InvalidRunRequestError) {
    return json({ error: error.message }, 400);
  }
  // Every refusal this package authors on the taxonomy's own base renders with
  // its declared status and reason — the reservation family among them. Placed
  // LAST so the named branches above keep their exact shapes, and typed against
  // the base rather than against each reservation class so a refusal added
  // later cannot arrive here as an anonymous 500.
  if (error instanceof DoStatusError) {
    const { status } = error;
    if (Number.isInteger(status) && status >= 400 && status <= 599) {
      return json(
        {
          error: error.message,
          ...(error.reason === undefined ? {} : { reason: error.reason }),
        },
        status,
      );
    }
  }
  return json(
    { error: error instanceof Error ? error.message : String(error) },
    500,
  );
}

const MAX_RUN_BODY_BYTES = 1_048_576;

async function readJson(request: Request): Promise<unknown> {
  const raw = await readBoundedBody(
    request,
    MAX_RUN_BODY_BYTES,
    'run body exceeds limit',
  );
  if (!raw.ok && raw.reason === 'payload-too-large') {
    throw new RunRouteError(413, 'payload too large');
  }
  if (!raw.ok) return null;
  try {
    return raw.text === '' ? null : JSON.parse(raw.text);
  } catch {
    return null;
  }
}

/**
 * What a keyed POST /runs resolved to: a run this request started, or one an
 * earlier request already started.
 *
 * The distinction is not cosmetic. Filing an approval for a suspension is a
 * SIDE EFFECT of starting a run, and the whole promise of an idempotency key is
 * that a retry repeats none of the first request's side effects — so a replay
 * must not re-file. `replayed` is what carries that fact back to the route.
 */
interface IdempotentStartResult {
  summary: RunSummary;
  replayed: boolean;
}

/**
 * Run a keyed start through the reservation.
 *
 * The reservation decides; this function only supplies the two surface reads it
 * needs (a run's persisted summary, and whether its object is executing it) and
 * then does what it is told. The run id comes from the reservation in BOTH
 * branches — freshly minted through `context.newRunId()` on the branch that
 * creates the row, read back from the winner's row on every other — so the
 * server-minted-run-id rule holds unchanged: the router still mints, and the
 * reservation still only ever stores what it was handed.
 */
async function startIdempotently(
  options: RunRouterOptions,
  context: ActorContext,
  workflowId: string,
  body: StartBody,
  rawKey: unknown,
): Promise<IdempotentStartResult> {
  const wiring = options.startIdempotency;
  // `requireStartIdempotency` turns the opt-out into the published refusal. A
  // key on an unwired host is never ignored: honouring it silently would be an
  // exactly-once promise this deployment cannot keep.
  const store = requireStartIdempotency(
    wiring === 'none' ? 'none' : wiring.store,
  );
  const live = wiring === 'none' ? undefined : wiring.live;
  const decision = await beginIdempotentStart<RunSummary>(
    store,
    {
      key: rawKey as string,
      owner: {
        kind: context.principal.kind,
        id: context.principal.id,
      },
      targetKind: 'workflow',
      targetId: workflowId,
      mintRunId: () => context.newRunId(),
    },
    {
      persisted: async (reservation: StartReservation) =>
        options.status(workflowId, reservation.runId),
      live: async (reservation: StartReservation) =>
        live ? live(workflowId, reservation.runId) : false,
    },
    wiring === 'none' ? undefined : wiring.fence,
  );
  if (decision.kind === 'replay') {
    return { summary: decision.persisted, replayed: true };
  }
  const { runId, key } = decision.reservation;
  try {
    return {
      summary: await options.start({
        workflowId,
        runId,
        inputData: body.inputData,
        principal: context.principal,
        idempotencyKey: key,
        ...(body.deadlineMs === undefined
          ? {}
          : { deadlineMs: body.deadlineMs as number }),
      }),
      replayed: false,
    };
  } catch (error) {
    // Only a fence refusal gives the claim back — see rollbackFencedStart. A
    // deployment that closed its fence between the claim and the start executed
    // nothing, so holding the claim would turn an operator's drain into a
    // permanently poisoned key; anything else may have executed, and giving the
    // claim back there would hand the next retry a second run.
    return rollbackFencedStart(store, key, runId, error);
  }
}

export function createRunRouter(options: RunRouterOptions): RunRouter {
  const { workflows, resolve } = options;
  const systemPrincipalId = options.systemPrincipalId ?? 'flowsafe-system';
  const metaById = new Map(workflows.map((meta) => [meta.id, meta]));

  function metaFor(workflowId: string): WorkflowMeta | undefined {
    return metaById.get(workflowId);
  }

  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    if (url.pathname !== '/workflows' && segments[0] !== 'runs') return null;

    try {
      const context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);
      const actor = context.actor;

      if (request.method === 'GET' && url.pathname === '/workflows') {
        // The catalog echoes the AUTHENTICATED identity so the SPA renders
        // role gates from the server's view of the actor — a client that
        // guesses its own role from a local token table is fail-open (an
        // unknown token must not default to admin).
        return json({
          workflows,
          actor: {
            id: actor.id,
            role: actor.role,
            // Display hint: whether THIS caller may decide its own request.
            // The resolver built it — the DECIDER_ROLES guard plus the
            // deployment's SoD policy, fed the SAME allowSelfDecision the
            // service enforces — so the echo can only reflect the server's
            // decide() verdict. The SPA uses it to suppress a now-false "you
            // will be refused" hint. Enforced server-side regardless
            // (ApprovalService.decide).
            canSelfDecide: context.canSelfDecide(actor.role),
          },
        });
      }
      if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

      const [, workflowId, runId, action] = segments;

      if (request.method === 'POST' && segments.length === 1) {
        if (!RUN_START_ROLES.includes(actor.role)) {
          return json({ error: 'forbidden' }, 403);
        }
        const body = (await readJson(request)) as StartBody | null;
        if (!body || typeof body.workflowId !== 'string') {
          return json({ error: 'workflowId is required' }, 400);
        }
        // A client may never choose the runId. 400 (not silent override) so a
        // caller pinning ids finds out.
        if (body.runId !== undefined) {
          return json({ error: 'runId is server-assigned' }, 400);
        }
        const meta = metaFor(body.workflowId);
        if (!meta) {
          return json({ error: `unknown workflow '${body.workflowId}'` }, 404);
        }
        // Per-workflow RBAC: a workflow may restrict who can START it — a finer
        // gate than the coarse "can start any run" check above.
        const { allowedRoles } = meta;
        if (allowedRoles && !allowedRoles.includes(actor.role)) {
          return json(
            {
              error: `role '${actor.role}' may not start '${body.workflowId}'`,
            },
            403,
          );
        }
        await options.beforeStart?.(context, body.workflowId, body.inputData);
        const startTarget = body.workflowId;
        // Unkeyed starts take the path they always took: mint, start, answer.
        // Keyed starts route through the reservation, which decides whether
        // this request starts a run or reports one that already exists.
        const { summary, replayed } =
          body.idempotencyKey === undefined
            ? {
                summary: await options.start({
                  workflowId: startTarget,
                  runId: context.newRunId(),
                  inputData: body.inputData,
                  principal: context.principal,
                  ...(body.deadlineMs === undefined
                    ? {}
                    : { deadlineMs: body.deadlineMs as number }),
                }),
                replayed: false,
              }
            : await startIdempotently(
                options,
                context,
                startTarget,
                body,
                body.idempotencyKey,
              );
        if (summary.status !== 'suspended') return json(summary);
        if (replayed) {
          // A replay answers with the run's persisted state and files nothing.
          // Re-running the start's approval filing here would create a SECOND
          // approval record for the same gate on every retry — the exact
          // duplication the key was bought to prevent — because
          // queueApprovalForSuspension files unconditionally (its deduplicating
          // sibling is the reconcile below, which the status route also uses).
          // So the replay takes the reconcile instead: it files only a gate
          // that genuinely has no open record, and is a no-op for the normal
          // case where the original start already filed one.
          if (options.reconcileApprovals) {
            try {
              await options.reconcileApprovals(context, startTarget, summary);
            } catch (error) {
              console.error(
                JSON.stringify({
                  type: 'reconcile-error',
                  workflowId: startTarget,
                  runId: summary.runId,
                  error: error instanceof Error ? error.message : String(error),
                }),
              );
            }
          }
          return json(summary);
        }
        let approvals: ApprovalRecord[] = [];
        try {
          approvals = await queueApprovalForSuspension(
            context.service(),
            body.workflowId,
            summary,
            actor.id,
            systemPrincipalId,
          );
        } catch (error) {
          console.error(
            JSON.stringify({
              type: 'approval-filing-error',
              workflowId: body.workflowId,
              runId: summary.runId,
              error: error instanceof Error ? error.message : String(error),
            }),
          );
        }
        // `approval` remains the single-gate response contract (what the SPA
        // links); `approvals` carries every record a parallel multi-step
        // suspension filed, so no gate is invisible to the caller.
        return json({ ...summary, approval: approvals[0], approvals });
      }

      // Status, resume, and terminate validate the workflow against the
      // catalog: the start route already did, and a passthrough that did not
      // would answer for workflow ids this host never registered.
      if (workflowId && runId && !metaFor(workflowId)) {
        return json({ error: `unknown workflow '${workflowId}'` }, 404);
      }

      // Ownership is resolved before role gates or runner access. A missing id
      // and a resource owned by another principal are the same 404.
      const isTerminate =
        request.method === 'POST' &&
        segments.length === 4 &&
        action === 'terminate' &&
        options.terminate !== undefined;
      let replayOnly = false;
      if (workflowId && runId && segments.length >= 3) {
        try {
          await requireResourceAccess(
            context,
            'run',
            runId,
            request.method === 'GET' ? 'read' : 'write',
            'run',
          );
        } catch (error) {
          if (
            !isTerminate ||
            !(error instanceof RunRouteError) ||
            error.status !== 404
          ) {
            throw error;
          }
          // Ownership is deliberately gone after terminal cleanup. Only the
          // owner DO may authorize this replay against persisted exact
          // principals; every other case remains the same opaque 404.
          replayOnly = true;
        }
      }

      if (request.method === 'POST' && !RUN_START_ROLES.includes(actor.role)) {
        return json({ error: 'forbidden' }, 403);
      }

      // Per-workflow RBAC, mirrored from the start route: a module that
      // narrows who may START a workflow also narrows who may ADVANCE it —
      // resume drives the same state machine. Hoisted above dispatch so a
      // future mutating route cannot forget it; GETs stay coarse
      // (reviewer/viewer inspect runs they may not drive). Placed after the
      // catalog 404 above so an unknown workflow does not become a role oracle.
      if (request.method !== 'GET' && workflowId) {
        const allowedRoles = metaFor(workflowId)?.allowedRoles;
        if (allowedRoles && !allowedRoles.includes(actor.role)) {
          return json(
            { error: `role '${actor.role}' may not advance '${workflowId}'` },
            403,
          );
        }
      }

      if (
        request.method === 'GET' &&
        segments.length === 3 &&
        workflowId &&
        runId
      ) {
        const summary = await options.status(workflowId, runId);
        if (!summary) return json({ error: 'run not found' }, 404);
        if (summary.status === 'suspended' && options.reconcileApprovals) {
          try {
            await options.reconcileApprovals(context, workflowId, summary);
          } catch (error) {
            console.error(
              JSON.stringify({
                type: 'reconcile-error',
                workflowId,
                runId,
                error: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        }
        return json(summary);
      }

      if (isTerminate && workflowId && runId && options.terminate) {
        return json(
          await options.terminate(
            workflowId,
            runId,
            context.principal,
            replayOnly,
          ),
        );
      }

      if (
        request.method === 'POST' &&
        segments.length === 4 &&
        action === 'resume' &&
        workflowId &&
        runId
      ) {
        const body = (await readJson(request)) ?? {};
        await options.beforeResume?.(context, workflowId, runId, body);
        return json(
          await options.resume(
            workflowId,
            runId,
            body,
            context.principal.id,
            'human',
          ),
        );
      }

      return json({ error: 'not found' }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  };
}
