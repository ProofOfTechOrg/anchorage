// Reference production deployment for the flowsafe DO runner + approval
// queue. Copy this directory as the starting point for a real Worker: the
// wiring is production-shaped (bearer-token auth seam, cron-driven SLA sweep
// and retention purge, structured audit logs, multi-gate approval bridging);
// replace the example workflow with your own and swap bearerActorAuthenticator
// for your identity provider.
//
// The security-critical pieces are NOT copied here — auth, the run routes with
// their RBAC gate order, and the suspension→approval bridge all come from
// `@proofoftech/flowsafe/host-kit`, where they are tested. This file supplies
// the workflows and this deployment's DO-stub topology.
//
// Routes (the /workflows + /runs surface is host-kit's createRunRouter; the
// /api/approvals surface is approval-api's createApprovalRouter):
//   GET  /workflows                       -> catalog of the WORKFLOWS metas
//   POST /runs { workflowId, inputData }  -> start; a suspension auto-queues
//                                            an approval (response.approval).
//                                            A meta's allowedRoles gates who
//                                            may start it (per-workflow RBAC)
//   GET  /runs/:workflowId/:runId         -> status projection
//   POST /runs/:workflowId/:runId/resume  -> raw resume (no grants; gated
//                                            steps fail closed — approve via
//                                            the queue instead)
//   *    /api/approvals[...]              -> approval queue REST surface. The
//                                            create route stays OFF
//                                            (allowCreate defaults false):
//                                            approval records are minted
//                                            in-process from an observed
//                                            suspension, never from a body
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz require `Authorization: Bearer <token>` mapped
// to an actor via the APPROVAL_ACTOR_TOKENS secret. No secret => every
// authenticated route 401s (fail closed).
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
  type ApprovalActor,
  ApprovalService,
  approvalGrantProvider,
  createTenantResolver,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  D1ApprovalStoreFactory,
  defaultResumeData,
  sweepSLA,
  type TenantBoundApprovalStore,
  type TenantResolver,
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
import {
  assertWorkflowsRegistered,
  bearerActorAuthenticator,
  createRunRouter,
  doSummary,
  parseActorTokens,
  resumeRunWithRequeue,
  staticTokenVerifier,
  withSubdomainCrossCheck,
  type WorkflowMeta,
} from '@proofoftech/flowsafe/host-kit';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
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
   * consumer below -> HTTP POST to SIEM_ENDPOINT (auth via the
   * SIEM_AUTH_HEADER secret, sent as the `authorization` header). Without
   * the binding, audit stays on structured Workers Logs only.
   */
  AUDIT_QUEUE?: Queue;
  SIEM_ENDPOINT?: string;
  SIEM_AUTH_HEADER?: string;
  /**
   * Client-per-subdomain apex, e.g. 'example.com' (var). When set, a request
   * to <tenant>.<apex> is denied unless the token's verified tenant IS that
   * tenant — defense in depth over INV-2, closing the pasted-token
   * confused-deputy UX (reserved infra subdomains and hosts outside the apex
   * skip the check). Unset => no cross-check (single-host deployments).
   */
  TENANT_APEX_DOMAIN?: string;
}

/** Id for system-created records; the tenant is bound per request/instance. */
const SYSTEM_ACTOR_ID = 'flowsafe-worker';

/**
 * Attribution identity for the cron SLA sweep (audit only — the sweep is TCB
 * code over the system store; per-record tenants ride in the audit detail).
 */
const MAINTENANCE_ACTOR: ApprovalActor = {
  id: SYSTEM_ACTOR_ID,
  role: 'operator',
  tenantId: 'system',
};

/** The connector the example publish step demands a grant for. */
const EXAMPLE_CONNECTOR = 'example-publisher';

/**
 * Every workflow this deployment hosts, as the run router sees it: the catalog
 * for GET /workflows, and the per-workflow start gate (`allowedRoles`, omitted
 * here so the coarse RUN_START_ROLES check is the only one). Each `id` MUST
 * equal the createWorkflow id committed below — defineWorkflows asserts it.
 */
const WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
  {
    id: 'example-approval',
    title: 'Example approval',
    description:
      'A gated publish: suspend for approval, then demand the grant.',
    sampleInput: { topic: 'launch' },
  },
];

// One factory per isolate, not per request: it owns the memoized schema-init
// promise, so rebuilding it inside fetch() would re-run the whole DDL pass on
// every request. Keyed by the D1 binding, stable for an isolate's lifetime.
const approvalFactories = new WeakMap<D1Database, D1ApprovalStoreFactory>();

function approvalStoreFactory(db: D1Database): D1ApprovalStoreFactory {
  let factory = approvalFactories.get(db);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db);
    approvalFactories.set(db, factory);
  }
  return factory;
}

function defineWorkflows(env: Env, tenantId: string): RunnerRuntime {
  // Bound to THIS DO instance's tenant, recovered from its idFromName
  // identity (INV-1 -> INV-2): the grant mint can only ever read the runs'
  // own tenant, even though the runId predicate already scopes it.
  const approvals = approvalStoreFactory(env.DB).forTenant(tenantId);
  const { createWorkflow, createStep, runtime } = init(env, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records in D1 — decisions become
    // capabilities without any grant crossing a request body.
    requestContextForRun: approvalGrantProvider(approvals),
  });

  // Replace from here down with your workflows. Conventions to keep:
  //  - a gate step suspends with { reason, connectors }, where `connectors` is
  //    a server-authored STATIC literal: the bridge copies it into the queued
  //    approval, so a decision mints exactly the grants that suspension asked
  //    for. Deriving it from run input would let client input choose its own
  //    capability;
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

  // Fail fast on a catalog/runtime mismatch: the router looks workflows up by
  // WORKFLOWS[].id, but start/resume route by the committed createWorkflow id.
  // A typo would otherwise surface as a mysterious 404 on a hosted workflow.
  assertWorkflowsRegistered(runtime, WORKFLOWS);

  return runtime;
}

export class FlowsafeRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env, this.tenantId);
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
// with their RBAC gate order, and the approval bridge all live in
// @proofoftech/flowsafe/host-kit. They are security-critical and tested there —
// the (suspendedAt, resumeCount) capture that binds a decision to one exact
// suspension, the separation-of-duties re-queue, and the coarse start-role
// check. Do not re-derive them here; production SSO/JWT verification replaces
// only the `authenticate` seam.

/** The run surface: shared routes + this deployment's DO-stub topology. */
function runRouterFor(env: Env, resolve: TenantResolver) {
  return createRunRouter({
    workflows: WORKFLOWS,
    resolve,
    systemActorId: SYSTEM_ACTOR_ID,
    start: async (workflowId, runId, inputData) =>
      doSummary(
        await runStub(env, workflowId, runId).fetch('http://do/runs', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ workflowId, runId, inputData }),
        }),
      ),
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

// Worker-level approval service sharing the DO's D1 database. Decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request); if the resumed run suspends
// again at a later gate, the next approval is queued right here, so
// multi-gate workflows keep flowing through the queue.
//
// waitUntil keeps queue sends alive past the invocation (ctx.waitUntil in
// fetch; the maintenance runner collects and awaits them itself).
function buildApprovalService(
  store: TenantBoundApprovalStore,
  env: Env,
  waitUntil?: (promise: Promise<unknown>) => void,
): ApprovalService {
  const queueSink = env.AUDIT_QUEUE
    ? queueAuditSink(env.AUDIT_QUEUE)
    : undefined;
  const systemActor: ApprovalActor = {
    id: SYSTEM_ACTOR_ID,
    role: 'operator',
    tenantId: store.tenantId,
  };
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
    // This host's only topology-specific piece: resume the run through its DO
    // stub. resumeRunWithRequeue (host-kit) wraps it with the SoD-guarded
    // re-queue so a run that re-suspends at a later gate keeps flowing through
    // the queue — the reviewer whose decision advanced the run becomes the next
    // gate's requester, and therefore cannot decide it.
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
      systemActor,
    ),
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
    // Cron-owned TCB sweep over the SYSTEM store: the ONLY legitimate
    // cross-tenant read+write, and it is not reachable over HTTP.
    const queueSink = env.AUDIT_QUEUE
      ? queueAuditSink(env.AUDIT_QUEUE)
      : undefined;
    escalated = (
      await sweepSLA(approvalStoreFactory(env.DB).system(), {
        systemActor: MAINTENANCE_ACTOR,
        audit: (event) => {
          console.log(JSON.stringify({ type: 'audit', ...event }));
          if (queueSink) {
            pendingSends.push(
              queueSink(event).catch((error: unknown) =>
                console.error(
                  JSON.stringify({
                    type: 'audit-queue-error',
                    reason: String(error),
                  }),
                ),
              ),
            );
          }
        },
        onEscalation: (record) =>
          console.log(
            JSON.stringify({
              type: 'sla-escalation',
              id: record.id,
              tenantId: record.tenantId,
              workflowId: record.workflowId,
              runId: record.runId,
              slaDeadlineAt: record.slaDeadlineAt,
            }),
          ),
      })
    ).length;
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
    // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
    // approval store to the verified actor's tenant before any service
    // exists — there is no pre-auth store for a rushed fix to reach.
    const baseResolve = createTenantResolver({
      authenticate: bearerActorAuthenticator(
        staticTokenVerifier(parseActorTokens(env.APPROVAL_ACTOR_TOKENS)),
      ),
      storeFactory: approvalStoreFactory(env.DB),
      buildService: (store) => buildApprovalService(store, env, waitUntil),
    });
    const resolve = env.TENANT_APEX_DOMAIN
      ? withSubdomainCrossCheck(baseResolve, {
          apexDomain: env.TENANT_APEX_DOMAIN,
        })
      : baseResolve;

    const routed = request as unknown as Request;
    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    const runResponse = await runRouterFor(env, resolve)(routed);
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
