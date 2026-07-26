// SPDX-License-Identifier: Apache-2.0
// Track D (M-006), CI-M-006-003 — createScheduleRouter, the tenant facade over
// the schedules domain. Schedule rows have NO tenant column (DL-013): the tenant
// is stamped into metadata.tenantId at create (server-controlled, never a client
// value) and every read/list/mutation is tenant-scoped on it. Ids are
// SERVER-MINTED `${prefix}${uuid}` (agent_/schedule_) — a client cannot name the
// id, so it cannot collide with (or probe for) another tenant's schedule, the
// INV-1 posture applied to schedule ids; core's slugified client-id path is
// avoided (a slugify-drift + existence-oracle vector).
//
// The write path is an ingestion trust boundary (P6-lite, DL-006) — the same gate
// order createSignalRouter / createObjectiveRouter enforce:
//
//   1. resolve (authenticate -> INV-3 -> bind)             -> 401 / 403
//   2. coarse role (RUN_START_ROLES) on MUTATIONS           -> 403 (reads stay coarse)
//   3. per-resource ownership (metadata.tenantId match)      -> 404 (no oracle)
//   4. size cap on the raw body, THEN JSON parse             -> 413 / 400
//   5. field validation + per-tenant COUNT cap (create) + fire-RATE cap (DL-007)
//   6. P4 reserved requestContext-key rejection (DL-004)     -> 400
//   7. audit (schedule.route) + persist
//
// P4 STORED-CONTEXT BARRIER (a) (DL-004): a stored WorkflowSchedule.requestContext
// / agent ScheduleStreamOptions.requestContext replays into a future run, so it is
// a stored-capability channel — the same class as the approval create-route leak.
// Create/update REJECT any requestContext naming a reserved key (the whole
// `breakwater.` namespace + core's 'mastra:goal'), matching the tick's strip
// (barrier b). Both kinds are guarded (workflow requestContext + agent
// ifIdle.streamOptions.requestContext).
//
// Every MUTATION is audited on ACCEPT and on EVERY post-auth denial (role 403,
// foreign 404, cap/field/reserved-key 400). A GET/list is audited only on a
// post-auth denial (a cross-tenant probe); a benign successful read is not. A
// pre-auth failure (401 / resolver throw -> 403) is NOT audited: an
// unauthenticated flood must never be able to write the log.
//
// AGENT schedules are creatable/manageable here. Their firing belongs solely to
// the tick's optional runtime-driven `startAgent` seam; without it, the tick
// retains the audited fail-closed skip. This surface mints no capability and
// starts no runs.

import {
  AGENT_SCHEDULE_PREFIX,
  type AnySchedule,
  type CreateAgentScheduleInput,
  type CreateWorkflowScheduleInput,
  ScheduleInputSchema,
  toScheduleView,
  type UpdateAgentScheduleInput,
  type UpdateWorkflowScheduleInput,
  WORKFLOW_SCHEDULE_PREFIX,
} from '@mastra/core/schedules';
import type {
  Schedule,
  ScheduleFilter,
  ScheduleTarget,
  ScheduleTrigger,
  ScheduleTriggerListOptions,
  ScheduleUpdate,
} from '@mastra/core/storage';
import { computeNextFireAt, validateCron } from '@mastra/core/workflows';
import {
  type ApprovalRole,
  RUN_START_ROLES,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '../approval-api/index.js';
import { RunRouteError, requireOwnedMemoryId } from '../host-kit/index.js';
import { safeDecodeSegment } from '../host-kit/route-path.js';
import { internalErrorResponse } from '../internal-error-response.js';
import {
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';
import { isReservedScheduleContextKey } from './tick.js';

/** The storage subset the facade reads/writes (a subset of D1SchedulesStorage). */
export interface ScheduleFacadeStore {
  createSchedule(schedule: Schedule): Promise<Schedule>;
  getSchedule(id: string): Promise<Schedule | null>;
  listSchedules(filter?: ScheduleFilter): Promise<Schedule[]>;
  updateSchedule(id: string, patch: ScheduleUpdate): Promise<Schedule>;
  deleteSchedule(id: string): Promise<void>;
  listTriggers(
    scheduleId: string,
    opts?: ScheduleTriggerListOptions,
  ): Promise<ScheduleTrigger[]>;
}

/** The operations, one per route. */
export type ScheduleOperation =
  | 'create'
  | 'get'
  | 'list'
  | 'update'
  | 'delete'
  | 'pause'
  | 'resume'
  | 'triggers';

/** The structured audit event a mutation (and a denied read) emits. */
export interface ScheduleRouteAuditEvent {
  type: 'schedule.route';
  tenantId: string;
  actorId: string;
  operation: ScheduleOperation;
  /** Present once an id is in play (all but create/list). */
  scheduleId?: string;
  outcome: 'accepted' | 'rejected';
  /** Present for rejected outcomes — WHY. */
  reason?: string;
  timestamp: string;
}

/** The audit seam — a host bridges this to its AuditLogger / SIEM sink. */
export type ScheduleRouteAuditSink = (
  event: ScheduleRouteAuditEvent,
) => void | Promise<void>;

export interface ScheduleRouterOptions {
  /** Authenticate, validate the tenant ID, and bind it; undefined means 401. */
  resolve: TenantResolver;
  /** The schedules domain the facade reads/writes. */
  store: ScheduleFacadeStore;
  /**
   * Who may create/update/delete/pause/resume. Default RUN_START_ROLES
   * (operator/admin) — reviewers/viewers cannot author schedules. Reads (get/
   * list/triggers) are not role-gated beyond ownership.
   */
  roles?: readonly ApprovalRole[];
  /** Every mutation (and denied read) is audited through this. Absent ⇒ no audit. */
  audit?: ScheduleRouteAuditSink;
  /**
   * Per-tenant count cap: the maximum number of schedules a tenant may own. A create
   * at or over it is REJECTED. Must be a nonnegative safe integer; zero denies
   * every create. Default 100.
   */
  maxSchedulesPerTenant?: number;
  /**
   * Per-schedule fire-rate cap: the minimum interval between two
   * consecutive fires of a schedule's cron, in ms. A cron whose interval is
   * shorter is REJECTED at create/update — bounding the aggregate fire rate a
   * tenant can schedule (with the count cap). Must be a positive safe integer.
   * Default 60000 (1 minute).
   */
  minFireIntervalMs?: number;
  /**
   * Max raw request-body size in bytes. Must be a nonnegative safe integer;
   * zero denies every non-empty body. Default 16384 (16 KiB).
   */
  maxContentBytes?: number;
  /** Route prefix. Default '/api/schedules'. */
  basePath?: string;
}

export type ScheduleRouter = (request: Request) => Promise<Response | null>;

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

interface Rejection {
  reason: string;
  message: string;
  status: number;
}
type Validated<T> = { ok: true; value: T } | { ok: false; error: Rejection };

function reject(reason: string, message: string, status = 400): Rejection {
  return { reason, message, status };
}

/** A schedule is owned by the tenant iff its metadata.tenantId matches. */
function ownsSchedule(schedule: Schedule, tenantId: string): boolean {
  const owner = (schedule.metadata as Record<string, unknown> | undefined)
    ?.tenantId;
  return owner === tenantId;
}

/**
 * The stored-context barrier over both requestContext-carrying surfaces of a create/update
 * body: the top-level `requestContext` (workflow + non-standard agent) and the
 * agent `ifIdle.streamOptions.requestContext` (core's only other stored-context
 * location — `ScheduleIfActive` has none). Returns a Rejection naming the offending
 * key, or undefined when both surfaces are clean. One helper so the create and
 * update paths cannot drift on which surfaces are guarded.
 */
function reservedContextRejection(
  body: Record<string, unknown>,
  target: 'workflow' | 'agent',
): Rejection | undefined {
  const idle = body.ifIdle as
    | { streamOptions?: { requestContext?: unknown } }
    | undefined;
  const surfaces: Array<[string, unknown]> =
    target === 'workflow'
      ? [['requestContext', body.requestContext]]
      : [
          [
            'ifIdle.streamOptions.requestContext',
            idle?.streamOptions?.requestContext,
          ],
        ];
  for (const [label, ctx] of surfaces) {
    if (ctx === undefined) continue;
    if (typeof ctx !== 'object' || ctx === null || Array.isArray(ctx)) {
      return reject('reserved-context-key', `${label} must be an object`);
    }
    const offending = Object.keys(ctx).find(isReservedScheduleContextKey);
    if (offending !== undefined) {
      return reject(
        'reserved-context-key',
        `${label} may not carry the reserved key '${offending}'`,
      );
    }
  }
  return undefined;
}

/**
 * computeNextFireAt, but a calendrically-impossible cron — one `validateCron`
 * accepts (syntactically legal) yet that has NO future occurrence, e.g.
 * `0 0 30 2 *` — yields a `cron-invalid` Rejection instead of a raw throw. Without
 * this the throw would escape to the router's outer catch and surface as an
 * UNAUDITED 500 with the raw error, where every other cron failure is a clean,
 * audited 400.
 */
function nextFireOrReject(
  cron: string,
  timezone: string | undefined,
  after: number,
): Validated<number> {
  try {
    return {
      ok: true,
      value: computeNextFireAt(cron, {
        ...(timezone !== undefined ? { timezone } : {}),
        after,
      }),
    };
  } catch (error) {
    return {
      ok: false,
      error: reject(
        'cron-invalid',
        error instanceof Error
          ? error.message
          : 'cron has no future occurrence',
      ),
    };
  }
}

/** Reject a cron whose interval between two consecutive fires is under the floor. */
function checkFireRate(
  cron: string,
  timezone: string | undefined,
  minFireIntervalMs: number,
  now: number,
): Rejection | undefined {
  const first = nextFireOrReject(cron, timezone, now);
  if (!first.ok) return first.error;
  const second = nextFireOrReject(cron, timezone, first.value);
  if (!second.ok) return second.error;
  // NOTE (DL-007 limitation): this samples only the NEXT two fires at request
  // time. A non-uniform cron (e.g. a dense minute cluster + a long gap) can pass
  // when sampled during the gap yet still fire the dense cluster later; the
  // per-tenant COUNT cap and the tick's run-cap seam are the real aggregate
  // backstops. A precise cycle-wide bound is deliberately not attempted here.
  if (second.value - first.value < minFireIntervalMs) {
    return reject(
      'fire-rate-too-high',
      `cron fires more often than the ${minFireIntervalMs}ms floor`,
    );
  }
  return undefined;
}

// A body may only carry the keys of the kind it declares — an unknown key is a
// 400 (defense-in-depth, the goals/signals allowlist posture). tenantId is never
// settable (server-stamped), and id/nextFireAt/lastFireAt/lastRunId/createdAt/
// updatedAt/ownerType/ownerId are server-owned.
type WorkflowCreateField = Exclude<keyof CreateWorkflowScheduleInput, 'id'>;
type AgentCreateField = Exclude<keyof CreateAgentScheduleInput, 'id'>;

const WORKFLOW_CREATE_FIELDS = new Set<WorkflowCreateField>([
  'workflowId',
  'cron',
  'timezone',
  'inputData',
  'initialState',
  'requestContext',
  'metadata',
  'status',
]);
const AGENT_CREATE_FIELDS = new Set<AgentCreateField>([
  'agentId',
  'cron',
  'prompt',
  'name',
  'timezone',
  'threadId',
  'resourceId',
  'signalType',
  'tagName',
  'attributes',
  'providerOptions',
  'ifActive',
  'ifIdle',
  'metadata',
  'status',
]);
const WORKFLOW_UPDATE_FIELDS = new Set<keyof UpdateWorkflowScheduleInput>([
  'cron',
  'timezone',
  'status',
  'inputData',
  'initialState',
  'requestContext',
  'metadata',
]);
const AGENT_UPDATE_FIELDS = new Set<keyof UpdateAgentScheduleInput>([
  'cron',
  'timezone',
  'status',
  'prompt',
  'name',
  'signalType',
  'tagName',
  'attributes',
  'providerOptions',
  'ifActive',
  'ifIdle',
  'metadata',
]);

function firstUnknownField(
  body: Record<string, unknown>,
  allowed: ReadonlySet<string>,
): string | undefined {
  return Object.keys(body).find((key) => !allowed.has(key));
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function metadataOrReject(value: unknown): Validated<Record<string, unknown>> {
  if (
    value === undefined ||
    typeof value !== 'object' ||
    value === null ||
    Array.isArray(value)
  ) {
    return {
      ok: false,
      error: reject('metadata-invalid', 'metadata must be an object'),
    };
  }
  return { ok: true, value: value as Record<string, unknown> };
}

function validateMemoryIds(
  body: Record<string, unknown>,
  tenant: TenantContext,
): Rejection | undefined {
  const hasThread = body.threadId !== undefined;
  const hasResource = body.resourceId !== undefined;
  if (hasThread && !nonEmptyString(body.threadId)) {
    return reject('threadId-invalid', 'threadId must be a non-empty string');
  }
  if (hasResource && !nonEmptyString(body.resourceId)) {
    return reject(
      'resourceId-invalid',
      'resourceId must be a non-empty string',
    );
  }
  if (!hasThread) {
    const offender = ['resourceId', 'signalType', 'ifActive', 'ifIdle'].find(
      (field) => body[field] !== undefined,
    );
    if (offender !== undefined) {
      return reject(
        `threadless-field:${offender}`,
        `${offender} requires threadId`,
      );
    }
    return undefined;
  }
  if (!hasResource) {
    return reject(
      'resourceId-required',
      'resourceId is required when threadId is set',
    );
  }
  requireOwnedMemoryId(tenant, body.threadId as string, 'threadId');
  requireOwnedMemoryId(tenant, body.resourceId as string, 'resourceId');
  return undefined;
}

function normalizeAgentTarget(
  scheduleId: string,
  values: Record<string, unknown>,
): Validated<ScheduleTarget> {
  const parsed = ScheduleInputSchema.safeParse({
    scheduleId,
    agentId: values.agentId,
    prompt: values.prompt,
    threadId: values.threadId,
    resourceId: values.resourceId,
    signalType: values.signalType,
    tagName: values.tagName,
    attributes: values.attributes,
    providerOptions: values.providerOptions,
    ifActive: values.ifActive,
    ifIdle: values.ifIdle,
  });
  if (!parsed.success) {
    return {
      ok: false,
      error: reject(
        'agent-options-invalid',
        parsed.error.issues[0]?.message ?? 'invalid agent schedule options',
      ),
    };
  }
  const {
    scheduleId: _scheduleId,
    agentId,
    prompt,
    ...runtimeFields
  } = parsed.data;
  return {
    ok: true,
    value: {
      type: 'agent',
      agentId,
      prompt,
      ...(values.name !== undefined ? { name: values.name as string } : {}),
      ...runtimeFields,
    },
  };
}

/** Build a fresh Schedule row from a create body, tenant-stamped + server-id'd. */
function buildCreateRow(
  body: Record<string, unknown>,
  tenant: TenantContext,
  now: number,
  minFireIntervalMs: number,
): Validated<Schedule> {
  const tenantId = tenant.tenantId;
  const hasWorkflow = 'workflowId' in body && body.workflowId !== undefined;
  const hasAgent = 'agentId' in body && body.agentId !== undefined;
  if (hasWorkflow === hasAgent) {
    return {
      ok: false,
      error: reject(
        'target-ambiguous',
        'exactly one of workflowId or agentId is required',
      ),
    };
  }
  if (!nonEmptyString(body.cron)) {
    return {
      ok: false,
      error: reject('cron-invalid', 'cron must be a string'),
    };
  }
  const cron = body.cron;
  const timezone =
    body.timezone === undefined ? undefined : (body.timezone as string);
  if (timezone !== undefined && typeof timezone !== 'string') {
    return {
      ok: false,
      error: reject('timezone-invalid', 'timezone must be a string'),
    };
  }
  try {
    validateCron(cron, timezone);
  } catch (error) {
    return {
      ok: false,
      error: reject(
        'cron-invalid',
        error instanceof Error ? error.message : 'invalid cron',
      ),
    };
  }
  const fireRate = checkFireRate(cron, timezone, minFireIntervalMs, now);
  if (fireRate) return { ok: false, error: fireRate };

  const status =
    body.status === undefined ? 'active' : (body.status as Schedule['status']);
  if (status !== 'active' && status !== 'paused') {
    return {
      ok: false,
      error: reject('status-invalid', "status must be 'active' or 'paused'"),
    };
  }

  let clientMetadata: Record<string, unknown> = {};
  if (body.metadata !== undefined) {
    const parsedMetadata = metadataOrReject(body.metadata);
    if (!parsedMetadata.ok) return parsedMetadata;
    clientMetadata = parsedMetadata.value;
  }
  // tenantId is stamped LAST so a client value can never override it (DL-013).
  const metadata: Record<string, unknown> = {
    ...clientMetadata,
    tenantId,
  };

  // checkFireRate above already proved the cron has a future occurrence, so this
  // cannot throw; nextFireOrReject keeps it defensive without a bare computeNextFireAt.
  const nextFire = nextFireOrReject(cron, timezone, now);
  if (!nextFire.ok) return { ok: false, error: nextFire.error };
  const nextFireAt = nextFire.value;
  const common = {
    cron,
    ...(timezone !== undefined ? { timezone } : {}),
    status,
    nextFireAt,
    createdAt: now,
    updatedAt: now,
    metadata,
  };

  if (hasWorkflow) {
    const unknown = firstUnknownField(body, WORKFLOW_CREATE_FIELDS);
    if (unknown !== undefined) {
      return {
        ok: false,
        error: reject(
          `field-not-allowed:${unknown}`,
          `field '${unknown}' is not settable on a workflow schedule`,
        ),
      };
    }
    if (!nonEmptyString(body.workflowId)) {
      return {
        ok: false,
        error: reject(
          'workflowId-invalid',
          'workflowId must be a non-empty string',
        ),
      };
    }
    const reserved = reservedContextRejection(body, 'workflow');
    if (reserved) return { ok: false, error: reserved };
    const target: ScheduleTarget = {
      type: 'workflow',
      workflowId: body.workflowId,
      ...(body.inputData !== undefined ? { inputData: body.inputData } : {}),
      ...(body.initialState !== undefined
        ? { initialState: body.initialState }
        : {}),
      ...(body.requestContext !== undefined
        ? { requestContext: body.requestContext as Record<string, unknown> }
        : {}),
    };
    return {
      ok: true,
      value: {
        id: `${WORKFLOW_SCHEDULE_PREFIX}${crypto.randomUUID()}`,
        target,
        ...common,
      },
    };
  }

  // Agent schedule.
  const unknown = firstUnknownField(body, AGENT_CREATE_FIELDS);
  if (unknown !== undefined) {
    return {
      ok: false,
      error: reject(
        `field-not-allowed:${unknown}`,
        `field '${unknown}' is not settable on an agent schedule`,
      ),
    };
  }
  if (!nonEmptyString(body.agentId)) {
    return {
      ok: false,
      error: reject('agentId-invalid', 'agentId must be a non-empty string'),
    };
  }
  if (!nonEmptyString(body.prompt)) {
    return {
      ok: false,
      error: reject('prompt-invalid', 'prompt must be a non-empty string'),
    };
  }
  if (body.name !== undefined && typeof body.name !== 'string') {
    return {
      ok: false,
      error: reject('name-invalid', 'name must be a string'),
    };
  }
  const memory = validateMemoryIds(body, tenant);
  if (memory) return { ok: false, error: memory };
  const reserved = reservedContextRejection(body, 'agent');
  if (reserved) return { ok: false, error: reserved };
  const agentTarget = normalizeAgentTarget('pending', body);
  if (!agentTarget.ok) return agentTarget;
  return {
    ok: true,
    value: {
      id: `${AGENT_SCHEDULE_PREFIX}${crypto.randomUUID()}`,
      target: agentTarget.value,
      ...common,
      ownerType: 'agent',
      ownerId: body.agentId,
    },
  };
}

export function createScheduleRouter(
  options: ScheduleRouterOptions,
): ScheduleRouter {
  const { resolve, store } = options;
  const roles = options.roles ?? RUN_START_ROLES;
  const maxSchedules = nonnegativeSafeInteger(
    options.maxSchedulesPerTenant ?? 100,
    'maxSchedulesPerTenant',
  );
  const minFireIntervalMs = positiveSafeInteger(
    options.minFireIntervalMs ?? 60_000,
    'minFireIntervalMs',
  );
  const maxContentBytes = nonnegativeSafeInteger(
    options.maxContentBytes ?? 16_384,
    'schedule maxContentBytes',
  );
  const base = options.basePath ?? '/api/schedules';
  const baseSegments = base.split('/').filter(Boolean);

  return async (request) => {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);
    if (
      segments.length < baseSegments.length ||
      segments.length > baseSegments.length + 2 ||
      baseSegments.some((seg, i) => segments[i] !== seg)
    ) {
      return null;
    }
    // Malformed percent-encoding in the id is not a real route target —
    // route-absent, never a pre-auth decodeURIComponent throw out of the handler.
    let id: string | undefined;
    if (segments.length > baseSegments.length) {
      id = safeDecodeSegment(segments[baseSegments.length]);
      if (id === undefined) return null;
    }
    const sub =
      segments.length === baseSegments.length + 2
        ? segments[baseSegments.length + 1]
        : undefined;

    // Resolve the (method, path shape) into an operation, or fall through / 405.
    const operation = resolveOperation(request.method, id, sub);
    if (operation === null) return null;
    if (operation === undefined) {
      return json({ error: 'method not allowed' }, 405);
    }
    const isMutation =
      operation !== 'get' && operation !== 'list' && operation !== 'triggers';

    // `tenant` is a hoisted `let` so the `audit` closure (defined before the try)
    // can read it. It stays undefined until resolve succeeds, and the closure
    // no-ops while it is — so a pre-auth failure never writes the log (an
    // unauthenticated flood cannot spam the audit sink).
    let tenant: TenantContext | undefined;
    const audit = async (
      outcome: 'accepted' | 'rejected',
      reason?: string,
    ): Promise<void> => {
      if (!options.audit || !tenant) return;
      // A benign read is not audited; only its denial is.
      if (!isMutation && outcome === 'accepted') return;
      await options.audit({
        type: 'schedule.route',
        tenantId: tenant.tenantId,
        actorId: tenant.actor.id,
        operation,
        ...(id !== undefined ? { scheduleId: id } : {}),
        outcome,
        ...(reason !== undefined ? { reason } : {}),
        timestamp: new Date().toISOString(),
      });
    };

    try {
      // 1. Resolve.
      tenant = await resolve(request);
      if (!tenant) return json({ error: 'authentication required' }, 401);
      // Captured after the guard so the closures below don't need a non-null
      // assertion on the hoisted `tenant` let.
      const ownerTenant = tenant.tenantId;

      // 2. Coarse role on mutations.
      if (isMutation && !roles.includes(tenant.actor.role)) {
        await audit('rejected', 'forbidden-role');
        return json({ error: 'forbidden' }, 403);
      }

      // LIST — tenant-filtered (DL-013 post-filter over the domain list).
      if (operation === 'list') {
        const all = await store.listSchedules();
        const mine = all.filter((s) => ownsSchedule(s, ownerTenant));
        return json({ schedules: mine.map(toView) });
      }

      // CREATE — count cap, field/cron/fire-rate/reserved-key checks, persist.
      if (operation === 'create') {
        const parsed = await readBody(request, maxContentBytes);
        if (parsed instanceof Response) {
          await audit(
            'rejected',
            parsed.status === 413 ? 'payload-too-large' : 'malformed-body',
          );
          return parsed;
        }
        // COUNT cap (DL-007): a tenant may not exceed its schedule budget. This
        // is check-then-act (no atomic increment through the domain seam), so N
        // concurrent creates at count = cap-1 can each read the same count and
        // all insert, overshooting by up to N — self-limited to the requester's
        // own tenant. The tick's run-cap seam is the real cost backstop; a hard
        // atomic cap would need a transaction D1 does not expose here.
        const owned = (await store.listSchedules()).filter((s) =>
          ownsSchedule(s, ownerTenant),
        );
        if (owned.length >= maxSchedules) {
          await audit('rejected', 'schedule-count-cap');
          return json(
            { error: `tenant schedule cap of ${maxSchedules} reached` },
            400,
          );
        }
        const built = buildCreateRow(
          parsed,
          tenant,
          Date.now(),
          minFireIntervalMs,
        );
        if (!built.ok) {
          await audit('rejected', built.error.reason);
          return json({ error: built.error.message }, built.error.status);
        }
        const created = await store.createSchedule(built.value);
        await audit('accepted');
        return json({ schedule: toView(created) }, 201);
      }

      // Everything below addresses ONE schedule by id — 404 (no oracle) on a
      // missing OR foreign one, BEFORE any work, both audited the same.
      const scheduleId = id as string;
      const existing = await store.getSchedule(scheduleId);
      if (!existing || !ownsSchedule(existing, tenant.tenantId)) {
        await audit('rejected', 'not-found');
        return json({ error: 'not found' }, 404);
      }

      if (operation === 'get') {
        return json({ schedule: toView(existing) });
      }

      if (operation === 'triggers') {
        // Default the limit so an omitted/garbage `?limit` never returns an
        // UNBOUNDED history (trigger rows grow per fire; SCHEDULE_TRIGGER_
        // RETENTION_DAYS is opt-in/unset by default).
        const limit = clampLimit(url.searchParams.get('limit')) ?? 100;
        const triggers = await store.listTriggers(scheduleId, { limit });
        return json({ triggers });
      }

      if (operation === 'delete') {
        await store.deleteSchedule(scheduleId);
        await audit('accepted');
        return json({ ok: true });
      }

      if (operation === 'pause') {
        if (existing.status === 'paused') {
          await audit('accepted');
          return json({ schedule: toView(existing) });
        }
        const updated = await store.updateSchedule(scheduleId, {
          status: 'paused',
        });
        await audit('accepted');
        return json({ schedule: toView(updated) });
      }

      if (operation === 'resume') {
        if (existing.status === 'active') {
          await audit('accepted');
          return json({ schedule: toView(existing) });
        }
        // Re-activating recomputes nextFireAt from now (core's resume semantics).
        // Guarded: a legacy row with a calendrically-impossible cron would else
        // throw an unaudited 500 here (create now rejects such crons, so this is
        // defense-in-depth for pre-existing rows).
        const nextFire = nextFireOrReject(
          existing.cron,
          existing.timezone,
          Date.now(),
        );
        if (!nextFire.ok) {
          await audit('rejected', nextFire.error.reason);
          return json({ error: nextFire.error.message }, nextFire.error.status);
        }
        const nextFireAt = nextFire.value;
        const updated = await store.updateSchedule(scheduleId, {
          status: 'active',
          nextFireAt,
        });
        await audit('accepted');
        return json({ schedule: toView(updated) });
      }

      // UPDATE (PATCH).
      const parsed = await readBody(request, maxContentBytes);
      if (parsed instanceof Response) {
        await audit(
          'rejected',
          parsed.status === 413 ? 'payload-too-large' : 'malformed-body',
        );
        return parsed;
      }
      const patch = buildUpdatePatch(
        existing,
        parsed,
        minFireIntervalMs,
        Date.now(),
      );
      if (!patch.ok) {
        await audit('rejected', patch.error.reason);
        return json({ error: patch.error.message }, patch.error.status);
      }
      const updated = await store.updateSchedule(scheduleId, patch.value);
      await audit('accepted');
      return json({ schedule: toView(updated) });
    } catch (error) {
      if (error instanceof RunRouteError) {
        await audit(
          'rejected',
          error.status === 404
            ? 'foreign-memory-id'
            : `route-error-${error.status}`,
        );
        return json({ error: error.message }, error.status);
      }
      if (error instanceof TenantResolutionError) {
        // Pre-auth: unauthenticated, so not audited.
        return json({ error: 'forbidden' }, 403);
      }
      return internalErrorResponse('schedules', error);
    }
  };
}

/**
 * Map (method, path shape) to an operation. `null` = not our route (fall
 * through); `undefined` = our path but a wrong method (405).
 */
function resolveOperation(
  method: string,
  id: string | undefined,
  sub: string | undefined,
): ScheduleOperation | null | undefined {
  if (id === undefined) {
    if (method === 'POST') return 'create';
    if (method === 'GET') return 'list';
    return undefined;
  }
  if (sub === undefined) {
    if (method === 'GET') return 'get';
    if (method === 'PATCH') return 'update';
    if (method === 'DELETE') return 'delete';
    return undefined;
  }
  if (sub === 'pause') return method === 'POST' ? 'pause' : undefined;
  if (sub === 'resume') return method === 'POST' ? 'resume' : undefined;
  if (sub === 'triggers') return method === 'GET' ? 'triggers' : undefined;
  return null;
}

function buildUpdatePatch(
  existing: Schedule,
  body: Record<string, unknown>,
  minFireIntervalMs: number,
  now: number,
): Validated<ScheduleUpdate> {
  const allowed =
    existing.target.type === 'workflow'
      ? WORKFLOW_UPDATE_FIELDS
      : AGENT_UPDATE_FIELDS;
  const unknown = firstUnknownField(body, allowed);
  if (unknown !== undefined) {
    return {
      ok: false,
      error: reject(
        `field-not-allowed:${unknown}`,
        `field '${unknown}' is not updatable`,
      ),
    };
  }

  const patch: ScheduleUpdate = {};
  const nextCron =
    body.cron !== undefined ? (body.cron as string) : existing.cron;
  const nextTimezone =
    body.timezone !== undefined ? (body.timezone as string) : existing.timezone;
  if (body.cron !== undefined || body.timezone !== undefined) {
    if (body.cron !== undefined && !nonEmptyString(body.cron)) {
      return {
        ok: false,
        error: reject('cron-invalid', 'cron must be a string'),
      };
    }
    // The same non-string timezone guard create enforces — a number/boolean
    // timezone else slips past validateCron (which does not throw on it) and
    // persists into the timezone TEXT column.
    if (body.timezone !== undefined && typeof body.timezone !== 'string') {
      return {
        ok: false,
        error: reject('timezone-invalid', 'timezone must be a string'),
      };
    }
    try {
      validateCron(nextCron, nextTimezone);
    } catch (error) {
      return {
        ok: false,
        error: reject(
          'cron-invalid',
          error instanceof Error ? error.message : 'invalid cron',
        ),
      };
    }
    const fireRate = checkFireRate(
      nextCron,
      nextTimezone,
      minFireIntervalMs,
      now,
    );
    if (fireRate) return { ok: false, error: fireRate };
    patch.cron = nextCron;
    if (body.timezone !== undefined) patch.timezone = nextTimezone;
    // Recompute the next fire on a cron/timezone change (checkFireRate above
    // already proved computability, so this cannot throw).
    const nextFire = nextFireOrReject(nextCron, nextTimezone, now);
    if (!nextFire.ok) return { ok: false, error: nextFire.error };
    patch.nextFireAt = nextFire.value;
  }

  if (body.status !== undefined) {
    if (body.status !== 'active' && body.status !== 'paused') {
      return {
        ok: false,
        error: reject('status-invalid', "status must be 'active' or 'paused'"),
      };
    }
    patch.status = body.status;
  }

  const reserved = reservedContextRejection(body, existing.target.type);
  if (reserved) return { ok: false, error: reserved };

  // Patch the target in place (kind is fixed at create — a workflow schedule
  // stays a workflow schedule). Only the settable target fields are merged.
  const nextTarget = patchTarget(existing, body);
  if (!nextTarget.ok) return nextTarget;
  if (nextTarget.value !== undefined) patch.target = nextTarget.value;

  if (body.metadata !== undefined) {
    // A client may not change tenantId — re-stamp the existing owner.
    const owner = (existing.metadata as Record<string, unknown> | undefined)
      ?.tenantId;
    const clientMeta = metadataOrReject(body.metadata);
    if (!clientMeta.ok) return clientMeta;
    patch.metadata = { ...clientMeta.value, tenantId: owner };
  }

  return { ok: true, value: patch };
}

/** Merge the settable target fields; returns undefined when nothing target-side changed. */
function patchTarget(
  existing: Schedule,
  body: Record<string, unknown>,
): Validated<ScheduleTarget | undefined> {
  const target = existing.target;
  if (target.type === 'workflow') {
    const changed =
      body.inputData !== undefined ||
      body.initialState !== undefined ||
      body.requestContext !== undefined;
    if (!changed) return { ok: true, value: undefined };
    return {
      ok: true,
      value: {
        ...target,
        ...(body.inputData !== undefined ? { inputData: body.inputData } : {}),
        ...(body.initialState !== undefined
          ? { initialState: body.initialState }
          : {}),
        ...(body.requestContext !== undefined
          ? { requestContext: body.requestContext as Record<string, unknown> }
          : {}),
      },
    };
  }
  const agentKeys = [
    'prompt',
    'name',
    'signalType',
    'tagName',
    'attributes',
    'providerOptions',
    'ifActive',
    'ifIdle',
  ];
  if (!agentKeys.some((key) => body[key] !== undefined)) {
    return { ok: true, value: undefined };
  }
  const merged = { ...target } as Record<string, unknown>;
  for (const key of agentKeys) {
    if (body[key] !== undefined) merged[key] = body[key];
  }
  if (!nonEmptyString(merged.prompt)) {
    return {
      ok: false,
      error: reject('prompt-invalid', 'prompt must be a non-empty string'),
    };
  }
  if (merged.name !== undefined && typeof merged.name !== 'string') {
    return {
      ok: false,
      error: reject('name-invalid', 'name must be a string'),
    };
  }
  if (
    merged.threadId === undefined &&
    ['resourceId', 'signalType', 'ifActive', 'ifIdle'].some(
      (field) => merged[field] !== undefined,
    )
  ) {
    return {
      ok: false,
      error: reject(
        'threadless-options',
        'resourceId, signalType, ifActive, and ifIdle require threadId',
      ),
    };
  }
  if (merged.threadId !== undefined && merged.resourceId === undefined) {
    return {
      ok: false,
      error: reject(
        'resourceId-required',
        'resourceId is required when threadId is set',
      ),
    };
  }
  const normalized = normalizeAgentTarget(existing.id, merged);
  return normalized.ok ? { ok: true, value: normalized.value } : normalized;
}

function toView(schedule: Schedule): AnySchedule {
  // Core's projection — the response shape can never drift from mastra.schedules.
  const view = toScheduleView(schedule);
  if (!view) {
    // Unreachable for a well-formed row; return a minimal fallback rather than
    // throw (a corrupt row must not 500 a whole list).
    return { id: schedule.id } as AnySchedule;
  }
  return view;
}

function clampLimit(raw: string | null): number | undefined {
  if (raw === null) return undefined;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1) return undefined;
  return Math.min(parsed, 200);
}

/** Size-cap the raw body at the wire, THEN parse it to a JSON object. */
async function readBody(
  request: Request,
  maxContentBytes: number,
): Promise<Record<string, unknown> | Response> {
  const raw = await request.text();
  const bytes = new TextEncoder().encode(raw).length;
  if (bytes > maxContentBytes) {
    return json({ error: `payload exceeds ${maxContentBytes} bytes` }, 413);
  }
  try {
    const parsed = raw === '' ? {} : JSON.parse(raw);
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      return json({ error: 'a JSON object body is required' }, 400);
    }
    return parsed as Record<string, unknown>;
  } catch {
    return json({ error: 'a JSON object body is required' }, 400);
  }
}
