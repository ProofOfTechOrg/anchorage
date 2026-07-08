// Anchorage Showcase — the runnable multi-workflow host.
//
// The five design-sketch workflows grown into an actual deployed service: all
// five (gtm-outbound, content-pipeline, lead-generation, product-launch,
// access-request) running on a real Cloudflare Worker + Durable Object + D1,
// behind the approval queue, bearer auth, and cron maintenance. This file is
// host wiring only — it mirrors `deploy/worker.ts` (bearer auth seam, multi-gate
// approval bridging, cron SLA sweep + retention purge, optional Queues audit
// export) and delegates every workflow to buildShowcaseRuntime().
//
// Side effects are binding-gated in each connector (see showcase/workflows/):
// with no binding a connector simulates its side effect (envelope/preview
// logged) while still exercising the approval-grant gate. Bind `EMAIL` (and the
// CRM/deploy egress, wired in defineWorkflows) to go live; a forged resume fails
// closed regardless.
//
// Routes:
//   GET  /workflows                       -> module catalog (id/title/sampleInput/
//                                            allowedRoles) for the launcher
//   POST /runs { workflowId, inputData }  -> start; a suspension auto-queues
//                                            an approval (response.approval).
//                                            A module's meta.allowedRoles gates
//                                            who may start it (per-workflow RBAC)
//   GET  /runs/:workflowId/:runId         -> status projection
//   POST /runs/:workflowId/:runId/resume  -> raw resume (no grants; the gated
//                                            step fails closed — approve via
//                                            the queue instead)
//   *    /api/approvals[...]              -> approval queue REST surface
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz require `Authorization: Bearer <token>` mapped to
// an actor via APPROVAL_ACTOR_TOKENS (baked as a wrangler var for the demo; use
// a secret for real deployments).

import type {
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  MessageBatch,
  Queue,
  Request as CfRequest,
  ScheduledController,
} from '@cloudflare/workers-types';
import { AuditLogger } from '@proofoftech/breakwater';

import {
  APPROVAL_ROLES,
  type ApprovalActor,
  type ApprovalRecord,
  ApprovalService,
  createApprovalRouter,
  D1ApprovalStore,
  defaultResumeData,
} from '../src/approval-api/index.js';
import {
  createAuditQueueConsumer,
  queueAuditSink,
} from '../src/audit-export/index.js';
import {
  DurableObjectRunner,
  purgeExpiredWorkflowRuns,
  type RunnerRuntime,
  type RunSummary,
} from '../src/do-runner/index.js';
import {
  queueApprovalForSuspension,
  resumeRunWithRequeue,
} from '../src/host-kit/index.js';
import {
  buildShowcaseRuntime,
  SHOWCASE_MODULES,
  type EmailServiceBinding,
} from './runtime.js';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  /**
   * Optional Cloudflare Email Service binding. Absent (Phase A) => the outreach
   * connector simulates the send (logs the envelope, sends nothing). Bound =>
   * real transactional send from an onboarded domain.
   */
  EMAIL?: EmailServiceBinding;
  /** Envelope sender identity for the outreach connector (vars). */
  OUTREACH_FROM_ADDRESS?: string;
  OUTREACH_FROM_NAME?: string;
  /**
   * Secret (`wrangler secret put APPROVAL_ACTOR_TOKENS`): JSON map of bearer
   * token -> actor, e.g. {"<random-token>": {"id": "ray", "role":
   * "reviewer"}}. Swap authenticateActor() for your SSO/JWT verification to
   * replace it — actor mapping stays inside the trusted computing base either
   * way.
   */
  APPROVAL_ACTOR_TOKENS?: string;
  /** Default SLA seconds for new approvals (var; default 14400 = 4h). */
  APPROVAL_SLA_SECONDS?: string;
  /** Cron purges terminal run snapshots older than this (var; default 30). */
  RUN_RETENTION_DAYS?: string;
  /**
   * Optional audit export to a SIEM: bind a queue producer (wrangler.jsonc
   * `queues` block) and audit events flow producer -> queue -> the `queue`
   * consumer below -> HTTP POST to SIEM_ENDPOINT (auth via the SIEM_AUTH_HEADER
   * secret). Without the binding, audit stays on structured Workers Logs only.
   */
  AUDIT_QUEUE?: Queue;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
}

/** Identity for system-created records and the cron SLA sweep. */
const SYSTEM_ACTOR: ApprovalActor = { id: 'flowsafe-worker', role: 'operator' };

function defineWorkflows(env: Env): RunnerRuntime {
  return buildShowcaseRuntime({
    initInput: env,
    approvalStore: new D1ApprovalStore(env.DB),
    email: env.EMAIL,
    fromAddress: env.OUTREACH_FROM_ADDRESS,
    fromName: env.OUTREACH_FROM_NAME,
    // Connector audit streams to Workers Logs alongside the approval audit.
    audit: new AuditLogger({
      sink: (event) =>
        console.log(JSON.stringify({ type: 'connector-audit', ...event })),
    }),
    // crm/deploy egress bindings + artifact bucket are left unset: the showcase
    // runs offline (R2 via InMemoryArtifactBucket, CRM/deploy simulated). Bind
    // them in wrangler.jsonc + wire them here to go live per connector.
  });
}

export class ShowcaseRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env);
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function runStub(
  env: Env,
  workflowId: string,
  runId: string,
): ReturnType<DurableObjectNamespace['get']> {
  return env.RUNNER.get(env.RUNNER.idFromName(`${workflowId}:${runId}`));
}

function numberVar(
  raw: string | undefined,
  fallback: number,
  name: string,
): number {
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    // Fall back rather than fail: maintenance must keep running on a typo'd
    // var, and the log line is the operator's tripwire.
    console.error(
      JSON.stringify({ type: 'config-error', var: name, raw, fallback }),
    );
    return fallback;
  }
  return value;
}

function parseActorTokens(raw: string | undefined): Map<string, ApprovalActor> {
  const actors = new Map<string, ApprovalActor>();
  if (!raw) return actors;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    console.error(
      JSON.stringify({
        type: 'config-error',
        var: 'APPROVAL_ACTOR_TOKENS',
        reason: 'not valid JSON — all authenticated routes will 401',
      }),
    );
    return actors;
  }
  if (parsed === null || typeof parsed !== 'object') return actors;
  for (const [token, actor] of Object.entries(parsed)) {
    const candidate = actor as { id?: unknown; role?: unknown };
    if (
      typeof candidate?.id === 'string' &&
      candidate.id.length > 0 &&
      typeof candidate.role === 'string' &&
      (APPROVAL_ROLES as readonly string[]).includes(candidate.role)
    ) {
      actors.set(token, candidate as ApprovalActor);
    }
  }
  return actors;
}

// The auth seam. Bearer tokens from a Worker secret are the minimal real
// deployment; production SSO/JWT verification replaces only this function.
function authenticateActor(
  request: Request,
  env: Env,
): ApprovalActor | undefined {
  const token = request.headers
    .get('authorization')
    ?.match(/^Bearer\s+(.+)$/i)?.[1];
  if (!token) return undefined;
  return parseActorTokens(env.APPROVAL_ACTOR_TOKENS).get(token);
}

// requestedConnectors + queueApprovalForSuspension + the SoD re-queue live in
// src/host-kit (imported above), shared verbatim with the dev backend so the
// security-sensitive (suspendedAt, resumeCount) capture and self-decision guard
// have a single home. This host supplies only its topology: the DO-stub resume.

// Worker-level approval service sharing the DO's D1 database. Decisions resume
// the run through its DO stub (grants come from the store via the DO-side
// provider, never from this request); if the resumed run suspends again at a
// later gate, the next approval is queued right here, so multi-gate workflows
// keep flowing through the queue.
function buildApprovalService(
  env: Env,
  waitUntil?: (promise: Promise<unknown>) => void,
): ApprovalService {
  const store = new D1ApprovalStore(env.DB);
  const queueSink = env.AUDIT_QUEUE
    ? queueAuditSink(env.AUDIT_QUEUE)
    : undefined;
  const service: ApprovalService = new ApprovalService({
    store,
    defaultSlaSeconds: numberVar(
      env.APPROVAL_SLA_SECONDS,
      4 * 60 * 60,
      'APPROVAL_SLA_SECONDS',
    ),
    audit: (event) => {
      console.log(JSON.stringify({ type: 'audit', ...event }));
      if (queueSink) {
        const send = queueSink(event).catch((error: unknown) =>
          console.error(
            JSON.stringify({
              type: 'audit-queue-error',
              reason: String(error),
            }),
          ),
        );
        waitUntil?.(send);
      }
    },
    onEscalation: (record) =>
      console.log(
        JSON.stringify({
          type: 'sla-escalation',
          id: record.id,
          workflowId: record.workflowId,
          runId: record.runId,
          slaDeadlineAt: record.slaDeadlineAt,
        }),
      ),
    // This host's only topology-specific piece: resume the run through its DO
    // stub. resumeRunWithRequeue (host-kit) wraps it with the SoD-guarded
    // re-queue so a run that re-suspends at a later gate keeps flowing through
    // the queue — the same wrapper the dev backend uses over its in-process base.
    resumeRun: resumeRunWithRequeue(
      async (record, decision) => {
        const response = await runStub(
          env,
          record.workflowId,
          record.runId,
        ).fetch(`http://do/runs/${record.workflowId}/${record.runId}/resume`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            step: record.stepPath,
            resumeData: defaultResumeData(record, decision),
          }),
        });
        const summary = (await response.json()) as RunSummary;
        if (!response.ok) {
          throw new Error(
            `resume failed (${response.status}): ${JSON.stringify(summary)}`,
          );
        }
        return summary;
      },
      () => service,
      SYSTEM_ACTOR,
    ),
  });
  return service;
}

/**
 * Cron owns both enforcement surfaces: sweepSLA() (SLA breaches escalate) and
 * purgeExpiredWorkflowRuns() (terminal snapshots are reclaimed). Each is
 * isolated so one failing never masks the other; counts go to structured logs.
 */
async function runMaintenance(env: Env, cron: string): Promise<void> {
  let escalated: number | undefined;
  let purged: number | undefined;
  const pendingSends: Promise<unknown>[] = [];
  try {
    const service = buildApprovalService(env, (send) =>
      pendingSends.push(send),
    );
    escalated = (await service.sweepSLA(SYSTEM_ACTOR)).length;
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'sla-sweep',
        cron,
        error: String(error),
      }),
    );
  }
  try {
    purged = await purgeExpiredWorkflowRuns(env.DB, {
      ttlMs:
        numberVar(env.RUN_RETENTION_DAYS, 30, 'RUN_RETENTION_DAYS') *
        24 *
        60 *
        60 *
        1000,
    });
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'maintenance-error',
        surface: 'retention-purge',
        cron,
        error: String(error),
      }),
    );
  }
  await Promise.all(pendingSends);
  console.log(JSON.stringify({ type: 'maintenance', cron, escalated, purged }));
}

// POST /runs — start a run through its DO; a suspension auto-queues its
// approval, attributed to the starting actor so they cannot later decide their
// own run.
async function startRun(
  env: Env,
  request: CfRequest,
  approvalService: ApprovalService,
  actor: ApprovalActor,
): Promise<Response> {
  const body = await request
    .json<{ workflowId?: string; runId?: string; inputData?: unknown }>()
    .catch(() => null);
  if (!body || typeof body.workflowId !== 'string') {
    return json({ error: 'workflowId is required' }, 400);
  }
  // Per-workflow RBAC: a module may restrict who can START it
  // (meta.allowedRoles) — a finer gate than the coarse "can start any run"
  // check the fetch handler already applied.
  const workflowModule = SHOWCASE_MODULES.find(
    (candidate) => candidate.meta.id === body.workflowId,
  );
  if (!workflowModule) {
    return json({ error: `unknown workflow '${body.workflowId}'` }, 404);
  }
  const { allowedRoles } = workflowModule.meta;
  if (allowedRoles && !allowedRoles.includes(actor.role)) {
    return json(
      { error: `role '${actor.role}' may not start '${body.workflowId}'` },
      403,
    );
  }
  // Worker owns runId generation so the DO instance key exists up front.
  const runId = body.runId ?? crypto.randomUUID();
  const response = await runStub(env, body.workflowId, runId).fetch(
    'http://do/runs',
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ ...body, runId }),
    },
  );
  if (!response.ok) return response as unknown as Response;
  const summary = (await response.json()) as RunSummary;
  if (summary.status !== 'suspended') return json(summary);
  const record = await queueApprovalForSuspension(
    approvalService,
    body.workflowId,
    summary,
    actor.id,
    SYSTEM_ACTOR,
  );
  return json({ ...summary, approval: record });
}

const handler: ExportedHandler<Env> = {
  async fetch(
    request: CfRequest,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);

    if (request.method === 'GET' && url.pathname === '/healthz') {
      return json({ ok: true });
    }

    const waitUntil = (promise: Promise<unknown>) => ctx.waitUntil(promise);
    const approvalService = buildApprovalService(env, waitUntil);
    const approvalRouter = createApprovalRouter({
      service: approvalService,
      authenticate: (routed) => authenticateActor(routed, env),
    });
    const approvalResponse = await approvalRouter(
      request as unknown as Request,
    );
    if (approvalResponse) return approvalResponse;

    // GET /workflows — the launcher catalog: each module's public meta (id,
    // title, description, sampleInput, allowedRoles). Any authenticated actor
    // may read it.
    if (request.method === 'GET' && url.pathname === '/workflows') {
      if (!authenticateActor(request as unknown as Request, env)) {
        return json({ error: 'authentication required' }, 401);
      }
      return json({
        workflows: SHOWCASE_MODULES.map((candidate) => candidate.meta),
      });
    }

    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

    // The run surface shares the approval bearer map: any authenticated actor
    // may inspect runs; starting or raw-resuming one is operator work.
    const actor = authenticateActor(request as unknown as Request, env);
    if (!actor) return json({ error: 'authentication required' }, 401);
    if (
      request.method === 'POST' &&
      actor.role !== 'admin' &&
      actor.role !== 'operator' &&
      actor.role !== 'builder'
    ) {
      return json({ error: 'forbidden' }, 403);
    }

    if (request.method === 'POST' && segments.length === 1) {
      return startRun(env, request, approvalService, actor);
    }

    // GET status and POST /resume forward straight to the run's DO. The raw
    // resume route carries NO grants: a forged `resumeData.approved` can flip a
    // workflow boolean but grants no connector capability — the send step
    // re-checks the server-derived grant and fails closed. Approve through the
    // queue (which mints grants), not this route.
    const [, workflowId, runId] = segments;
    if (workflowId && runId) {
      return runStub(env, workflowId, runId).fetch(
        new Request(
          `http://do${url.pathname}`,
          request as unknown as Request,
        ) as unknown as CfRequest,
      ) as unknown as Response;
    }
    return json({ error: 'not found' }, 404);
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    ctx.waitUntil(runMaintenance(env, controller.cron));
  },

  // Audit-export consumer (active only when the wrangler.jsonc `queues` block is
  // uncommented): ships each batch to the SIEM collector; a failed export
  // retries the batch, so nothing is acked unconfirmed.
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    if (!env.SIEM_ENDPOINT) {
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'SIEM_ENDPOINT',
          reason:
            'audit consumer bound without an export endpoint — retrying batch',
        }),
      );
      batch.retryAll();
      return;
    }
    await createAuditQueueConsumer({
      endpoint: env.SIEM_ENDPOINT,
      headers: env.SIEM_AUTH_HEADER
        ? { authorization: env.SIEM_AUTH_HEADER }
        : undefined,
    })(batch);
  },
};

export default handler;
