// Reference production deployment for the flowsafe DO runner + approval
// queue. Copy this directory as the starting point for a real Worker: the
// wiring is production-shaped (bearer-token auth seam, cron-driven SLA sweep
// and retention purge, structured audit logs, multi-gate approval bridging);
// replace the example workflow with your own and swap authenticateActor()
// for your identity provider.
//
// Routes:
//   POST /runs { workflowId, inputData }  -> start; a suspension auto-queues
//                                            an approval (response.approval)
//   GET  /runs/:workflowId/:runId         -> status projection
//   POST /runs/:workflowId/:runId/resume  -> raw resume (no grants; gated
//                                            steps fail closed — approve via
//                                            the queue instead)
//   *    /api/approvals[...]              -> approval queue REST surface
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz require `Authorization: Bearer <token>` mapped
// to an actor via the APPROVAL_ACTOR_TOKENS secret.
//
// Scheduled (wrangler.jsonc `triggers.crons`): every firing sweeps SLA
// breaches (escalates open approvals past their deadline) and purges
// terminal run snapshots older than RUN_RETENTION_DAYS.
//
// Deploy checklist: README.md next to this file.

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
import { z } from 'zod';

import {
  APPROVAL_ROLES,
  type ApprovalActor,
  type ApprovalRecord,
  ApprovalService,
  approvalGrantProvider,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  D1ApprovalStore,
  defaultResumeData,
} from '@proofoftech/flowsafe/approval-api';
import {
  createAuditQueueConsumer,
  queueAuditSink,
} from '@proofoftech/flowsafe/audit-export';
import {
  DurableObjectRunner,
  init,
  purgeExpiredWorkflowRuns,
  type RunnerRuntime,
  type RunSummary,
} from '@proofoftech/flowsafe/do-runner';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  /**
   * Secret (`wrangler secret put APPROVAL_ACTOR_TOKENS`): JSON map of bearer
   * token -> actor, e.g. {"<random-token>": {"id": "ray", "role":
   * "reviewer"}}. Swap authenticateActor() for your SSO/JWT verification to
   * replace it — actor mapping stays inside the trusted computing base
   * either way.
   */
  APPROVAL_ACTOR_TOKENS?: string;
  /** Default SLA seconds for new approvals (var; default 14400 = 4h). */
  APPROVAL_SLA_SECONDS?: string;
  /** Cron purges terminal run snapshots older than this (var; default 30). */
  RUN_RETENTION_DAYS?: string;
  /**
   * Optional audit export to a SIEM: bind a queue producer (wrangler.jsonc
   * `queues` block) and audit events flow producer -> queue -> the `queue`
   * consumer below -> HTTP POST to SIEM_ENDPOINT (auth via the
   * SIEM_AUTH_HEADER secret, sent as the `authorization` header). Without
   * the binding, audit stays on structured Workers Logs only.
   */
  AUDIT_QUEUE?: Queue;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
}

/** Identity for system-created records and the cron SLA sweep. */
const SYSTEM_ACTOR: ApprovalActor = { id: 'flowsafe-worker', role: 'operator' };

/** The connector the example publish step demands a grant for. */
const EXAMPLE_CONNECTOR = 'example-publisher';

function defineWorkflows(env: Env): RunnerRuntime {
  const approvals = new D1ApprovalStore(env.DB);
  const { createWorkflow, createStep, runtime } = init(env, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records in D1 — decisions become
    // capabilities without any grant crossing a request body.
    requestContextForRun: approvalGrantProvider(approvals),
  });

  // Replace from here down with your workflows. Conventions to keep:
  //  - a gate step suspends with { reason, connectors }: the bridges copy
  //    `connectors` into the queued approval, so a decision mints exactly
  //    the grants that suspension asked for;
  //  - resumeSchema matches approval-api's defaultResumeData contract.
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      approved: z.boolean(),
      decidedBy: z.string().optional(),
    }),
    suspendSchema: z.object({
      reason: z.string(),
      connectors: z.array(z.string()),
    }),
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'human approval required before publish',
          connectors: [EXAMPLE_CONNECTOR],
        });
      }
      return {
        topic: inputData.topic,
        approved: resumeData.approved,
        decidedBy: resumeData.decidedBy,
      };
    },
  });

  const publish = createStep({
    id: 'publish',
    inputSchema: z.object({
      topic: z.string(),
      approved: z.boolean(),
      decidedBy: z.string().optional(),
    }),
    outputSchema: z.object({
      topic: z.string(),
      published: z.boolean(),
      approvedBy: z.string().optional(),
    }),
    execute: async ({ inputData, requestContext }) => {
      if (!inputData.approved) {
        return { topic: inputData.topic, published: false };
      }
      // Stand-in for a breakwater write-gated connector: demand the grant
      // the approval minted. Real deployments call a createConnector() tool
      // here and the wrapper enforces this same key.
      const grants = requestContext.get(BREAKWATER_APPROVED_CONNECTORS_KEY);
      if (!Array.isArray(grants) || !grants.includes(EXAMPLE_CONNECTOR)) {
        throw new Error(
          `publish: approval required and not granted for '${EXAMPLE_CONNECTOR}'`,
        );
      }
      return {
        topic: inputData.topic,
        published: true,
        approvedBy: inputData.decidedBy,
      };
    },
  });

  createWorkflow({
    id: 'example-approval',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      published: z.boolean(),
      approvedBy: z.string().optional(),
    }),
  })
    .then(gate)
    .then(publish)
    .commit();

  return runtime;
}

export class FlowsafeRunner extends DurableObjectRunner<Env> {
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

/** Steps declare the grants they need in their suspend payload. */
function requestedConnectors(stepPayload: unknown): string[] {
  if (stepPayload === null || typeof stepPayload !== 'object') return [];
  const connectors = (stepPayload as Record<string, unknown>).connectors;
  return Array.isArray(connectors) &&
    connectors.every((c): c is string => typeof c === 'string')
    ? connectors
    : [];
}

/**
 * A suspension IS an approval request: queue it (idempotently — the store's
 * partial unique index collapses duplicates). Capturing the step's
 * (suspendedAt, resumedAt) pair binds the approval to THIS suspension exactly
 * (clock-free grant minting), and the suspend payload's `connectors` declares
 * what a decision should mint.
 *
 * `requestedBy` is the HUMAN who advanced the run to this suspension — the
 * actor who started it, or the reviewer whose decision caused a re-suspension
 * at the next gate. It must NOT be the system bridge: the library's
 * self-decision separation-of-duties check compares `requestedBy` to the
 * deciding actor, so attributing every request to `SYSTEM_ACTOR` would make
 * that check unfireable and let a start actor approve their own run. The
 * bridge still creates as `SYSTEM_ACTOR` (which holds CAN_CREATE); only the
 * attribution carries the real identity.
 */
async function queueApprovalForSuspension(
  service: ApprovalService,
  workflowId: string,
  summary: RunSummary,
  requestedBy: string,
): Promise<ApprovalRecord> {
  const stepPath = summary.suspended?.[0];
  const stepKey = stepPath?.join('.');
  const stepPayload =
    stepKey !== undefined &&
    summary.suspendPayload !== null &&
    typeof summary.suspendPayload === 'object'
      ? (summary.suspendPayload as Record<string, unknown>)[stepKey]
      : undefined;
  const connectors = requestedConnectors(stepPayload);
  const { record } = await service.create(
    {
      workflowId,
      runId: summary.runId,
      stepPath,
      suspendedAt:
        stepKey !== undefined ? summary.suspendedAt?.[stepKey] : undefined,
      resumedAt:
        stepKey !== undefined ? summary.resumedAt?.[stepKey] : undefined,
      title: `Approve '${workflowId}' run`,
      payload: summary.suspendPayload,
      connectors: connectors.length > 0 ? connectors : undefined,
      requestedBy,
    },
    SYSTEM_ACTOR,
  );
  return record;
}

// Worker-level approval service sharing the DO's D1 database. Decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request); if the resumed run suspends
// again at a later gate, the next approval is queued right here, so
// multi-gate workflows keep flowing through the queue.
//
// waitUntil keeps queue sends alive past the invocation (ctx.waitUntil in
// fetch; the maintenance runner collects and awaits them itself).
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
    // Structured audit trail into Workers Logs, plus the SIEM queue when
    // bound. The sink must not throw and must not block the approval path:
    // a failed send is logged, never propagated.
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
    // Notification seam: page/Slack/queue SLA breaches from here.
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
    resumeRun: async (record, decision) => {
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
      if (summary.status === 'suspended') {
        // The reviewer whose decision advanced the run is the requester of
        // the next gate's approval — so they cannot also decide it (SoD
        // carries across gates). decide() always sets decidedBy before
        // resumeRun fires; guard fail-CLOSED rather than fall back to the
        // bridge id, which would silently disable SoD for the next gate.
        if (!record.decidedBy) {
          throw new Error(
            'resumeRun: decidedBy unset — refusing to re-queue an approval without a requester',
          );
        }
        await queueApprovalForSuspension(
          service,
          record.workflowId,
          summary,
          record.decidedBy,
        );
      }
      return summary;
    },
  });
  return service;
}

/**
 * Cron owns both enforcement surfaces: sweepSLA() (SLA breaches escalate)
 * and purgeExpiredWorkflowRuns() (terminal snapshots are reclaimed). Each is
 * isolated so one failing never masks the other; counts go to structured
 * logs.
 */
async function runMaintenance(env: Env, cron: string): Promise<void> {
  let escalated: number | undefined;
  let purged: number | undefined;
  // The maintenance promise itself runs under ctx.waitUntil, so queue sends
  // fired by sweep audit events are collected and awaited here instead.
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
// approval, attributed to `requestedBy` (the starting actor) so they cannot
// later decide their own run. Split out of fetch() so the handler stays a
// pure dispatcher.
async function startRun(
  env: Env,
  request: CfRequest,
  approvalService: ApprovalService,
  requestedBy: string,
): Promise<Response> {
  const body = await request
    .json<{ workflowId?: string; runId?: string; inputData?: unknown }>()
    .catch(() => null);
  if (!body || typeof body.workflowId !== 'string') {
    return json({ error: 'workflowId is required' }, 400);
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
    requestedBy,
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

    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

    // The run surface shares the approval bearer map: any authenticated
    // actor may inspect runs; starting or raw-resuming one is operator
    // work. Workflow-level authorization beyond this belongs to your
    // deployment (breakwater RBAC at the agent boundary).
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
      return startRun(env, request, approvalService, actor.id);
    }

    // GET status and POST /resume forward straight to the run's DO. The raw
    // resume route carries NO grants: a forged `resumeData.approved` can flip
    // a workflow boolean but grants no connector capability — the
    // side-effecting step re-checks the server-derived grant and fails closed.
    // Approve through the queue (which mints grants), not this route.
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

  // Audit-export consumer (active only when the wrangler.jsonc `queues`
  // block is uncommented): ships each batch to the SIEM collector; a failed
  // export retries the batch, so nothing is acked unconfirmed.
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
