// Anchorage Showcase — the runnable multi-workflow host.
//
// The five design-sketch workflows grown into an actual deployed service: all
// five (gtm-outbound, content-pipeline, lead-generation, product-launch,
// access-request) running on a real Cloudflare Worker + Durable Object + D1,
// behind the approval queue, bearer auth, and cron maintenance. This file is
// host wiring only: it delegates every workflow to buildShowcaseRuntime(), and
// auth + the run routes + the approval bridge to src/host-kit (shared with
// `deploy/worker.ts`). What stays here is this host's topology — the DO-stub
// start/status/resume thunks — plus cron maintenance and the Queues audit
// export.
//
// Side effects are binding-gated in each connector (see showcase/workflows/):
// with no binding a connector simulates its side effect (envelope/preview
// logged) while still exercising the approval-grant gate. Bind `EMAIL` (and the
// CRM/deploy egress, wired in defineWorkflows) to go live; a forged resume fails
// closed regardless.
//
// Routes (the /workflows + /runs surface is host-kit's createRunRouter; the
// /api/approvals surface is approval-api's createApprovalRouter):
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
//   *    /api/approvals[...]              -> approval queue REST surface. The
//                                            create route stays OFF (allowCreate
//                                            defaults false): approval records
//                                            are minted in-process from an
//                                            observed suspension, never from a
//                                            request body.
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz require `Authorization: Bearer <token>` mapped to
// an actor via the APPROVAL_ACTOR_TOKENS secret. There is no baked-in token
// map: a deploy without `wrangler secret put APPROVAL_ACTOR_TOKENS` 401s
// everywhere (fail closed by construction). Local dev reads showcase/.dev.vars.

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
  type ApprovalActor,
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
  bearerActorAuthenticator,
  createRunRouter,
  doSummary,
  parseActorTokens,
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
   * "reviewer"}}. Absent => an empty map => every authenticated route 401s.
   * Swap bearerActorAuthenticator for your SSO/JWT verification to replace it —
   * actor mapping stays inside the trusted computing base either way.
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

// The auth seam (parseActorTokens + bearerActorAuthenticator), the run routes
// with their RBAC gate order, and the DO-response reader (doSummary) live in
// src/host-kit, shared with the reference deploy template and the dev backend
// so the security-sensitive pieces — the (suspendedAt, resumeCount) capture,
// the self-decision guard, and the coarse start-role check — have a single
// tested home. This host supplies only its topology: every run leg travels
// through the run's DO stub.

function runRouterFor(
  env: Env,
  approvalService: ApprovalService,
  authenticate: (request: Request) => ApprovalActor | undefined,
) {
  return createRunRouter({
    workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
    service: approvalService,
    systemActor: SYSTEM_ACTOR,
    authenticate,
    start: async (workflowId, runId, inputData) =>
      doSummary(
        await runStub(env, workflowId, runId).fetch('http://do/runs', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ workflowId, runId, inputData }),
        }),
      ),
    // The DO answers 404 for a run it has never seen; the router turns the
    // undefined into its own 404 rather than leaking the DO's body.
    status: async (workflowId, runId) => {
      const response = await runStub(env, workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}`,
      );
      if (response.status === 404) return undefined;
      return doSummary(response);
    },
    resume: async (workflowId, runId, body) =>
      doSummary(
        await runStub(env, workflowId, runId).fetch(
          `http://do/runs/${workflowId}/${runId}/resume`,
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(body),
          },
        ),
      ),
  });
}

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
      async (record, decision) =>
        doSummary(
          await runStub(env, record.workflowId, record.runId).fetch(
            `http://do/runs/${record.workflowId}/${record.runId}/resume`,
            {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                step: record.stepPath,
                resumeData: defaultResumeData(record, decision),
              }),
            },
          ),
        ),
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

const handler: ExportedHandler<Env> = {
  async fetch(
    request: CfRequest,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'GET' && url.pathname === '/healthz') {
      return json({ ok: true });
    }

    const waitUntil = (promise: Promise<unknown>) => ctx.waitUntil(promise);
    const approvalService = buildApprovalService(env, waitUntil);
    // Parsed once per request rather than once per authenticate() call — this
    // handler authenticates in both routers.
    const authenticate = bearerActorAuthenticator(
      parseActorTokens(env.APPROVAL_ACTOR_TOKENS),
    );

    const routed = request as unknown as Request;
    const approvalResponse = await createApprovalRouter({
      service: approvalService,
      authenticate,
    })(routed);
    if (approvalResponse) return approvalResponse;

    const runResponse = await runRouterFor(
      env,
      approvalService,
      authenticate,
    )(routed);
    if (runResponse) return runResponse;

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
