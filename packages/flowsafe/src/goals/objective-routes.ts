// SPDX-License-Identifier: Apache-2.0
// Track F (M-005), CI-M-005-001 — the goal objective HTTP surface (DL-018).
//
// A goal is a durable, thread-scoped OBJECTIVE record (@mastra/core's
// GoalObjectiveRecord, stored in the mastra_thread_state domain under type
// 'goal') that a Mastra agent's in-loop judge reads to decide whether to keep
// working toward it. In the DURABLE path the goal step rebuilds RequestContext
// from initData and reads the objective from D1 via resolveGoalStore ->
// readObjective, NOT any in-process registry (DL-018). Verified against the
// on-disk @mastra/core 1.50.0 dist: resolveGoalStore(mastra) is
// `mastra.getStorage().getStore('threadState')` and readObjective is
// `getState({ threadId, type: 'goal' })`. So this surface writes the D1 domain
// DIRECTLY at the Worker level — no thread-DO affinity is needed for the record
// write, and a record written here is what the loop later reads: the write goes
// through core's OWN writeObjective/readObjective/clearObjective over the SAME
// (threadId, 'goal') key, so the stored shape can never drift from the reader's.
//
// An objective is a STANDING INSTRUCTION injected into every future model turn,
// so the write path is an ingestion trust boundary (P6-lite, DL-006). It uses
// the same resource-first authorization rule as the signal router:
//
//   1. resolve (authenticate and validate actor)         -> 401 / 403
//   2. registry-backed thread ownership                   -> 404
//   3. coarse role (RUN_START_ROLES) on MUTATIONS        -> 403 (reads stay coarse)
//   4. size cap on the raw body, THEN JSON parse          -> 413 / 400
//   5. body names NO client memory id (assertNoClientMemoryIds) -> 400
//   6. field allowlist (objective/maxRuns/judge/prompt; status on update) -> 400
//   7. maxRuns host cap (DL-007)                          -> 400
//   8. audit (goal.objective) + persist
//
// Every MUTATION (set/update/clear) is audited on ACCEPT and on EVERY post-auth
// denial (role 403, malformed target 404, size/body/field/cap 400) — the Track C lesson.
// A GET is audited only on a post-auth denial; a
// benign successful read is not a standing-instruction write and is not logged.
// Pre-auth failures (401 / a resolver throw -> 403) are NOT audited: an
// unauthenticated flood must never be able to write the log.
//
// maxRuns (DL-007): a requested maxRuns above the host cap is REJECTED (400),
// not silently clamped. A caller that asked for 200 evaluations and got 50 would
// see mysterious early-stopping — exactly the "your value was quietly replaced"
// footgun the run router 400s a client runId to avoid — so the boundary fails
// loud instead. The stored record therefore never carries maxRuns above the cap
// (default the core DEFAULT_GOAL_MAX_RUNS, 50). Track F itself starts NO runs:
// the deployment run-start budget stays enforced at the existing run-start seam;
// bounding maxRuns only bounds how many of those already-budgeted runs one goal
// can drive.
//
// P8 (goals never mint capability): no route reads or writes requestContext and
// none names a connector/grant/step. writeObjective is called WITHOUT a
// requestContext, so the within-turn GOAL_REQUEST_CONTEXT_KEY surface is never
// touched here — the objective payload is content only.
//
// Write serialization: update is a read-modify-write and clear an unconditional
// delete, both at the Worker with NO per-thread serialization — deliberately
// weaker than signals (whose sends ride the thread-DO lease), because DL-018
// writes D1 directly. Core's own
// Agent.updateObjectiveOptions has the identical non-atomic read-then-write, so
// concurrent mutations are last-write-wins within the deployment; the maxRuns cap is
// enforced on every write, so no interleaving can persist an over-cap value or
// cross an authorization/capability line.

import type { GoalObjectiveRecord } from '@mastra/core/storage';
import {
  clearObjective,
  DEFAULT_GOAL_MAX_RUNS,
  readObjective,
  writeObjective,
} from '@mastra/core/tools';

import {
  type ActorContext,
  ActorResolutionError,
  type ActorResolver,
  type ApprovalRole,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import {
  admitsWorkAuthoring,
  type ExecutionFenceStore,
  executionFencedResponse,
  isExecutionFenceRefusal,
  OPEN_EXECUTION_FENCE,
} from '../do-runner/index.js';
import {
  assertNoClientMemoryIds,
  type BoundThreadTargetValidator,
  RunRouteError,
  requireResourceAccess,
} from '../host-kit/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';
import { readBoundedBody } from '../http-body.js';
import { internalErrorResponse } from '../internal-error-response.js';
import {
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';

/**
 * requestContext key @mastra/core surfaces the current objective under WITHIN a
 * turn. MIRRORED by value: 'mastra:goal' lives in core's
 * dist/agent/goal/objective.js, which the package `exports` map does NOT expose
 * (there is no `@mastra/core/agent/goal` subpath, and neither `@mastra/core/tools`
 * nor `@mastra/core/agent` re-exports it), so importing it would mean a forbidden
 * deep `dist/` import. Signals take the same mirror-not-deep-import stance
 * for core's XML-name validation. This surface never writes this key; it is
 * reserved here only so the no-collision test pins that it does not collide with
 * the runtime's #requestContextFor base keys. The mirror is defensive-only
 * (nothing here ever writes the key), and the test suite pins it BOTH ways: the
 * no-collision pin against the runtime base keys, and a dist drift-pin reading
 * core's objective.d.ts declaration — so a core bump that changes the value
 * fails a test loudly instead of silently diverging.
 */
export const GOAL_REQUEST_CONTEXT_KEY = 'mastra:goal';

/**
 * The thread-scoped state store the goal record lives in — structurally core's
 * ResolvedGoalStore (getState/setState/deleteState over a (threadId, type)
 * pair), which core does not export through a mapped subpath, so it is mirrored.
 * A host injects `createSignalStorageDomains(binding).threadState` (a
 * D1ThreadStateStorage) — the SAME domain the durable goal step resolves through
 * resolveGoalStore, so a record this surface writes is what the loop reads.
 */
export interface ObjectiveStore {
  getState<T = unknown>(args: {
    threadId: string;
    type: string;
  }): Promise<T | undefined>;
  setState(args: {
    threadId: string;
    type: string;
    value: GoalObjectiveRecord;
  }): Promise<void>;
  deleteState(args: { threadId: string; type: string }): Promise<void>;
}

/** The objective operations, one per HTTP verb on `/:threadId/goal`. */
export type ObjectiveOperation = 'set' | 'get' | 'update' | 'clear';

const OPERATION_BY_METHOD: Record<string, ObjectiveOperation | undefined> = {
  PUT: 'set',
  GET: 'get',
  PATCH: 'update',
  DELETE: 'clear',
};

/** The structured audit event a mutation (and a denied read) emits. */
export interface ObjectiveAuditEvent {
  type: 'goal.objective';
  deploymentTag?: string;
  actorId: string;
  threadId: string;
  operation: ObjectiveOperation;
  outcome: 'accepted' | 'rejected';
  /** Present for rejected outcomes — WHY the write was refused. */
  reason?: string;
  timestamp: string;
}

/** The audit seam — a host bridges this to its AuditLogger / SIEM sink. */
export type ObjectiveAuditSink = (
  event: ObjectiveAuditEvent,
) => void | Promise<void>;

export interface ObjectiveRouterOptions {
  /** Authenticate and validate the actor; undefined means 401. */
  resolve: ActorResolver;
  /** The thread-state domain in which the goal record lives. */
  store: ObjectiveStore;
  /** Prove mutations target durable bound memory, not an ephemeral run id. */
  validateThreadTarget: BoundThreadTargetValidator;
  /**
   * Who may SET/UPDATE/CLEAR an objective. Default RUN_START_ROLES
   * (operator/admin) — reviewers/viewers cannot author standing instructions.
   * Reads (GET) are not role-gated beyond ownership.
   */
  roles?: readonly ApprovalRole[];
  /** Every mutation (and denied read) is audited through this. Absent ⇒ no audit. */
  audit?: ObjectiveAuditSink;
  /**
   * The ceiling on a per-objective maxRuns. A request above it is
   * REJECTED; the stored record never carries a higher value. Default the core
   * DEFAULT_GOAL_MAX_RUNS (50). Must be a positive safe integer.
   */
  maxRunsCap?: number;
  /**
   * Max raw request-body size in bytes. Must be a nonnegative safe integer;
   * zero denies every non-empty body. Default 16384.
   */
  maxContentBytes?: number;
  /**
   * The deployment execution fence. A standing objective is authored work — it
   * is what the agent loop re-reads to decide it should run again — so SET,
   * UPDATE, and CLEAR are refused past `open` while GET stays available in
   * every state. Absent ⇒ this router is unfenced; see
   * ScheduleRouterOptions.executionFence for why a router's is optional while
   * init()'s is required.
   */
  executionFence?: ExecutionFenceStore;
  /** Route prefix. Default '/api/threads' (goals mount at `/:threadId/goal`). */
  basePath?: string;
}

export type ObjectiveRouter = (request: Request) => Promise<Response | null>;

/** The goal record statuses core's judge and this surface both use. */
const GOAL_STATUSES = ['active', 'paused', 'done'] as const;

// Fields a client may name in a SET body (create/replace the objective). status
// is always 'active' and runsUsed always 0 on a fresh set, so both are omitted —
// naming them (or any other key) is a 400.
const SET_FIELDS = new Set(['objective', 'maxRuns', 'judgeModelId', 'prompt']);
// Fields a client may name in an UPDATE body — OPTIONS only, never the prose
// (mirrors core's Agent.updateObjectiveOptions: judge/maxRuns/prompt/status). To
// change the objective text, replace the record with a fresh set.
const UPDATE_FIELDS = new Set(['maxRuns', 'judgeModelId', 'prompt', 'status']);

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

interface FieldError {
  reason: string;
  message: string;
  status: number;
}
type FieldResult<T> = { ok: true; value: T } | { ok: false; error: FieldError };

function rejectField(
  reason: string,
  message: string,
): { ok: false; error: FieldError } {
  return { ok: false, error: { reason, message, status: 400 } };
}

function validateMaxRuns(
  value: unknown,
  cap: number,
): FieldResult<number | undefined> {
  if (value === undefined) return { ok: true, value: undefined };
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1) {
    return rejectField('maxruns-invalid', 'maxRuns must be a positive integer');
  }
  if (value > cap) {
    return rejectField(
      'maxruns-over-cap',
      `maxRuns ${value} exceeds the host cap of ${cap}`,
    );
  }
  return { ok: true, value };
}

// rejectBlank: an identifier field (judgeModelId) with a whitespace-only value
// would persist and only fail later at model resolution, so it is refused at
// the boundary; prompt stays untrimmed — leading/trailing whitespace can be
// legitimate prompt content.
function validateOptionalString(
  value: unknown,
  field: string,
  rejectBlank = false,
): FieldResult<string | undefined> {
  if (value === undefined) return { ok: true, value: undefined };
  if (
    typeof value !== 'string' ||
    value.length === 0 ||
    (rejectBlank && value.trim().length === 0)
  ) {
    return rejectField(
      `${field}-invalid`,
      `${field} must be a non-empty string`,
    );
  }
  return { ok: true, value };
}

/** Reject any body key outside the operation's allowlist (unknown or reserved). */
function firstDisallowedField(
  body: Record<string, unknown>,
  allowed: Set<string>,
  verb: string,
): FieldError | undefined {
  const offending = Object.keys(body).find((key) => !allowed.has(key));
  return offending === undefined
    ? undefined
    : {
        reason: `field-not-allowed:${offending}`,
        message: `field '${offending}' is not ${verb} on an objective`,
        status: 400,
      };
}

/**
 * Build a fresh objective record from a SET body, byte-identical to core's
 * `Agent.setObjective` (id = randomUUID, status 'active', runsUsed 0, numeric
 * startedAt/updatedAt, only explicitly-provided optionals persisted). The `id`
 * is the in-record correlation tag core always mints for a goal — NOT an
 * addressing/memory id (it never keys a store or addresses a DO), so
 * a randomUUID here is faithful to core and needs no addressing prefix.
 */
function buildSetRecord(
  body: Record<string, unknown>,
  cap: number,
): FieldResult<GoalObjectiveRecord> {
  const disallowed = firstDisallowedField(body, SET_FIELDS, 'settable');
  if (disallowed) return { ok: false, error: disallowed };

  const objective = body.objective;
  if (typeof objective !== 'string' || objective.trim().length === 0) {
    return rejectField(
      'objective-invalid',
      'objective must be a non-empty string',
    );
  }
  const maxRuns = validateMaxRuns(body.maxRuns, cap);
  if (!maxRuns.ok) return maxRuns;
  const judgeModelId = validateOptionalString(
    body.judgeModelId,
    'judgeModelId',
    true,
  );
  if (!judgeModelId.ok) return judgeModelId;
  const prompt = validateOptionalString(body.prompt, 'prompt');
  if (!prompt.ok) return prompt;

  const now = Date.now();
  const record: GoalObjectiveRecord = {
    id: crypto.randomUUID(),
    objective,
    status: 'active',
    runsUsed: 0,
    startedAt: now,
    updatedAt: now,
    ...(maxRuns.value !== undefined ? { maxRuns: maxRuns.value } : {}),
    ...(judgeModelId.value !== undefined
      ? { judgeModelId: judgeModelId.value }
      : {}),
    ...(prompt.value !== undefined ? { prompt: prompt.value } : {}),
  };
  return { ok: true, value: record };
}

/**
 * Merge an UPDATE body over the existing record, byte-identical to core's
 * `Agent.updateObjectiveOptions`: only judge/maxRuns/prompt/status are settable
 * (never the prose), runsUsed/startedAt/id are preserved, updatedAt is bumped.
 */
function buildUpdateRecord(
  existing: GoalObjectiveRecord,
  body: Record<string, unknown>,
  cap: number,
): FieldResult<GoalObjectiveRecord> {
  const disallowed = firstDisallowedField(body, UPDATE_FIELDS, 'updatable');
  if (disallowed) return { ok: false, error: disallowed };

  const maxRuns = validateMaxRuns(body.maxRuns, cap);
  if (!maxRuns.ok) return maxRuns;
  const judgeModelId = validateOptionalString(
    body.judgeModelId,
    'judgeModelId',
    true,
  );
  if (!judgeModelId.ok) return judgeModelId;
  const prompt = validateOptionalString(body.prompt, 'prompt');
  if (!prompt.ok) return prompt;

  let status: GoalObjectiveRecord['status'] | undefined;
  if (body.status !== undefined) {
    const candidate = body.status;
    if (
      typeof candidate !== 'string' ||
      !(GOAL_STATUSES as readonly string[]).includes(candidate)
    ) {
      return rejectField(
        'status-invalid',
        `status must be one of ${GOAL_STATUSES.join(', ')}`,
      );
    }
    status = candidate as GoalObjectiveRecord['status'];
  }

  const updated: GoalObjectiveRecord = {
    ...existing,
    updatedAt: Date.now(),
    ...(judgeModelId.value !== undefined
      ? { judgeModelId: judgeModelId.value }
      : {}),
    ...(maxRuns.value !== undefined ? { maxRuns: maxRuns.value } : {}),
    ...(prompt.value !== undefined ? { prompt: prompt.value } : {}),
    ...(status !== undefined ? { status } : {}),
  };
  return { ok: true, value: updated };
}

export function createObjectiveRouter(
  options: ObjectiveRouterOptions,
): ObjectiveRouter {
  const { executionFence, resolve, store } = options;
  const roles = options.roles ?? RUN_START_ROLES;
  const maxRunsCap = positiveSafeInteger(
    options.maxRunsCap ?? DEFAULT_GOAL_MAX_RUNS,
    'maxRunsCap',
  );
  const maxContentBytes = nonnegativeSafeInteger(
    options.maxContentBytes ?? 16_384,
    'objective maxContentBytes',
  );
  const base = options.basePath ?? '/api/threads';
  const baseSegments = base.split('/').filter(Boolean);

  return async (request) => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    // /api/threads/:threadId/goal — the basePath segments, the threadId, then
    // the literal 'goal'. Any other shape composes ahead of the next router.
    if (
      segments.length !== baseSegments.length + 2 ||
      baseSegments.some((seg, i) => segments[i] !== seg) ||
      segments[baseSegments.length + 1] !== 'goal'
    ) {
      return null;
    }
    // Malformed percent-encoding in the threadId is not a real route target —
    // route-absent, never a pre-auth decodeURIComponent throw out of the handler.
    const threadId = safeDecodeSegment(segments[baseSegments.length]);
    if (threadId === undefined) return null;
    const operation = OPERATION_BY_METHOD[request.method];
    if (operation === undefined) {
      return json({ error: 'method not allowed' }, 405);
    }
    const isMutation = operation !== 'get';

    // Hoisted above the try so the catch audits the post-auth denials that
    // surface as thrown RunRouteErrors (the ownership 404, the memory-id 400).
    // `context` is undefined until resolve succeeds and the closure no-ops while
    // it is, so a pre-auth throw is never audited; a benign GET is not audited.
    let context: ActorContext | undefined;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      reason?: string,
    ): Promise<void> => {
      if (!options.audit || !context) return;
      if (operation === 'get' && outcome === 'accepted') return;
      await options.audit({
        type: 'goal.objective',
        ...(context.deploymentTag !== undefined
          ? { deploymentTag: context.deploymentTag }
          : {}),
        actorId: context.actor.id,
        threadId,
        operation,
        outcome,
        ...(reason !== undefined ? { reason } : {}),
        timestamp: new Date().toISOString(),
      });
    };
    const auditCommittedMutation = async (): Promise<void> => {
      try {
        await audit('accepted');
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'goal.objective-audit-error',
            threadId,
            operation,
            reason: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    };

    try {
      // 1. Resolve and authenticate.
      context = await resolve(request);
      if (!context) return json({ error: 'authentication required' }, 401);

      // 2. Resolve ownership before role checks or reading thread state.
      await requireResourceAccess(
        context,
        'thread',
        threadId,
        isMutation ? 'write' : 'read',
        'thread',
      );

      // 3. Coarse role on mutations: authoring a standing instruction is an
      // operator/admin act.
      if (isMutation && !roles.includes(context.actor.role)) {
        await audit('rejected', 'forbidden-role');
        return json({ error: 'forbidden' }, 403);
      }

      // 3b. The execution fence, after the role gate so a refusal is auditable
      // and tells an unauthorized caller nothing. Reads pass in every state.
      if (isMutation) {
        const reading = executionFence
          ? await executionFence.read()
          : OPEN_EXECUTION_FENCE;
        if (!admitsWorkAuthoring(reading)) {
          await audit('rejected', 'execution-fenced');
          return executionFencedResponse(
            reading.state,
            `objective ${operation}`,
          );
        }
      }

      // 4. GET / CLEAR carry no validated body.
      if (operation === 'get') {
        const record = await readObjective(store, threadId);
        await audit('accepted');
        return json({ objective: record ?? null });
      }
      if (operation === 'clear') {
        await clearObjective(store, threadId);
        await auditCommittedMutation();
        return json({ ok: true });
      }

      // 5. SET / UPDATE: size-cap the raw body at the wire, THEN parse.
      const rawBody = await readBoundedBody(
        request,
        maxContentBytes,
        'objective body exceeds limit',
      );
      if (!rawBody.ok && rawBody.reason === 'payload-too-large') {
        await audit('rejected', 'payload-too-large');
        return json(
          { error: `objective payload exceeds ${maxContentBytes} bytes` },
          413,
        );
      }
      if (!rawBody.ok) {
        await audit('rejected', 'malformed-body');
        return json({ error: 'a JSON object body is required' }, 400);
      }
      let body: Record<string, unknown>;
      try {
        const parsed = rawBody.text === '' ? {} : JSON.parse(rawBody.text);
        if (
          typeof parsed !== 'object' ||
          parsed === null ||
          Array.isArray(parsed)
        ) {
          await audit('rejected', 'malformed-body');
          return json({ error: 'a JSON object body is required' }, 400);
        }
        body = parsed as Record<string, unknown>;
      } catch {
        await audit('rejected', 'malformed-body');
        return json({ error: 'a JSON object body is required' }, 400);
      }

      // 6. No client memory id anywhere in the body (assertNoClientMemoryIds 400s).
      assertNoClientMemoryIds(body);

      // 7/8. Field allowlist + maxRuns cap, then persist through core's OWN
      // writer so the stored shape matches the reader's exactly.
      if (operation === 'set') {
        const built = buildSetRecord(body, maxRunsCap);
        if (!built.ok) {
          await audit('rejected', built.error.reason);
          return json({ error: built.error.message }, built.error.status);
        }
        await options.validateThreadTarget(context, { threadId });
        await writeObjective(store, threadId, built.value);
        await auditCommittedMutation();
        return json({ objective: built.value });
      }

      // operation === 'update': merge over the existing record (404 if none —
      // the OWN thread has nothing to update, distinct from the foreign 404).
      const existing = await readObjective(store, threadId);
      if (!existing) {
        await audit('rejected', 'no-objective');
        return json({ error: 'no objective set for this thread' }, 404);
      }
      const built = buildUpdateRecord(existing, body, maxRunsCap);
      if (!built.ok) {
        await audit('rejected', built.error.reason);
        return json({ error: built.error.message }, built.error.status);
      }
      await options.validateThreadTarget(context, { threadId });
      await writeObjective(store, threadId, built.value);
      await auditCommittedMutation();
      return json({ objective: built.value });
    } catch (error) {
      // A fence that could not be READ is not evidence the deployment is open,
      // so it degrades closed with its own retryable 503 rather than the
      // generic 500 below — an operator must be able to tell a deployment
      // that is being migrated from one that is broken.
      if (isExecutionFenceRefusal(error)) {
        await audit('rejected', 'execution-fence-unreadable');
        return json(
          { error: error.message, reason: error.reason },
          error.status,
        );
      }
      if (error instanceof RunRouteError) {
        // A post-auth denial: the target 404 or a smuggled memory-id 400.
        await audit(
          'rejected',
          error.status === 404
            ? 'invalid-thread'
            : error.status === 400
              ? 'client-memory-id'
              : `route-error-${error.status}`,
        );
        return json({ error: error.message }, error.status);
      }
      if (error instanceof ActorResolutionError) {
        // Pre-auth (the resolver itself threw): unauthenticated, so not audited.
        return json({ error: 'forbidden' }, 403);
      }
      return internalErrorResponse('goals.objective', error);
    }
  };
}
