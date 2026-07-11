// Reference production deployment for the flowsafe DO runner + approval
// queue. Copy this directory as the starting point for a real Worker: the
// wiring is production-shaped (bearer-token auth seam, cron-driven SLA sweep
// and retention purge, structured audit logs, multi-gate approval bridging);
// replace the example workflow with your own and swap bearerActorAuthenticator
// for your identity provider.
//
// The security-critical pieces are NOT copied here — auth, the run routes with
// their RBAC gate order, the suspension→approval bridge, the DO-stub topology,
// and the approval-service assembly all come from
// `@proofoftech/flowsafe/host-kit`, where they are tested. This file supplies
// the workflows and this deployment's bindings.
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
// Scheduled (wrangler.jsonc `triggers.crons`): TWO cron expressions,
// dispatched on controller.cron, so the SLA sweep and the retention purge
// never share an invocation — a Workers CPU-limit termination kills the
// isolate and is NOT a catchable JS error, so a slow sweep sharing an
// invocation would permanently starve the purge (and vice versa) no matter
// how many try/catches wrap them.
//
// Deploy checklist: README.md next to this file.

import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  MessageBatch,
  Queue,
  ScheduledController,
} from '@cloudflare/workers-types';
import {
  approvalGrantProvider,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  createTenantResolver,
  type TenantResolver,
} from '@proofoftech/flowsafe/approval-api';
import { createAuditQueueHandler } from '@proofoftech/flowsafe/audit-export';
import {
  DurableObjectRunner,
  init,
  purgeExpiredWorkflowRuns,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  assertWorkflowsRegistered,
  bearerActorAuthenticator,
  buildHostApprovalService,
  createDoRunTopology,
  createRunRouter,
  type DoRunTopology,
  maintenanceActor,
  numberVar,
  parseActorTokens,
  reconcileApprovalsOnStatusDetached,
  runApprovalRetentionPurge,
  runSlaSweepMaintenance,
  staticTokenVerifier,
  type TokenVerifier,
  type WorkflowMeta,
  withSubdomainCrossCheck,
} from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

import { PURGE_CRON, SWEEP_CRON } from './crons.js';

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
  /** Cron purges terminal run snapshots older than this (var; default 30; 0 = immediately). */
  RUN_RETENTION_DAYS?: string;
  /** Cron purges DECIDED (approved/rejected) approval records older than this (var; default 30; 0 = immediately). */
  APPROVAL_RETENTION_DAYS?: string;
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

/** The connector the example publish step demands a grant for. */
const EXAMPLE_CONNECTOR = 'example-publisher';

/**
 * Every workflow this deployment hosts, as the run router sees it: the catalog
 * for GET /workflows, and the per-workflow gate (`allowedRoles`, enforced on
 * start AND resume; omitted here so the coarse RUN_START_ROLES check is the
 * only one). Each `id` MUST equal the createWorkflow id committed below —
 * defineWorkflows asserts it.
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

function defineWorkflows(env: Env, tenantId: string): RunnerRuntime {
  // Bound to THIS DO instance's tenant, recovered from its idFromName
  // identity (INV-1 -> INV-2): the grant mint can only ever read the runs'
  // own tenant, even though the runId predicate already scopes it.
  const approvals = approvalStoreFactoryFor(env.DB).forTenant(tenantId);
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
  //  - resumeSchema matches approval-api's defaultResumeData contract;
  //  - real breakwater connectors (createConnector) in a MULTI-TENANT
  //    deployment must register breakwater's `tenantIsolation()` evaluator in
  //    their `policies.evaluators`: the runtime mints the isolation scope
  //    from every INV-1 runId, and the evaluator turns "scope somehow absent"
  //    from silently-shared idempotency/rate-limit keys into a denial. Wire
  //    durable stores too (D1IdempotencyStore / D1RateLimitStore) — an
  //    in-memory budget under DO-per-run routing is a per-RUN budget.
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

// The auth seam (parseActorTokens + bearerActorAuthenticator), the run routes
// with their RBAC gate order, the approval bridge, and the service assembly
// all live in @proofoftech/flowsafe/host-kit. They are security-critical and
// tested there — the (suspendedAt, resumeCount) capture that binds a decision
// to one exact suspension, the separation-of-duties re-queue, and the role
// gates. Do not re-derive them here; production SSO/JWT verification replaces
// only the `authenticate` seam.

/**
 * The bearer verifier, memoized per isolate: parseActorTokens re-parses and
 * re-validates the whole secret map, and the env is stable for the isolate's
 * lifetime — rebuilding it on every request is pure waste. Swap the body for
 * your SSO/JWT verification; keep the memo if construction stays non-trivial.
 */
let verifierMemo: { key: string; verifier: TokenVerifier } | undefined;

function buildVerifier(env: Env): TokenVerifier {
  const key = env.APPROVAL_ACTOR_TOKENS ?? '';
  if (verifierMemo?.key === key) return verifierMemo.verifier;
  const verifier = staticTokenVerifier(
    parseActorTokens(env.APPROVAL_ACTOR_TOKENS),
  );
  verifierMemo = { key, verifier };
  return verifier;
}

/**
 * The run surface: shared routes + this deployment's DO-stub topology.
 * `waitUntil` detaches the D4 reconcile hook (below) from the response path.
 */
function runRouterFor(
  resolve: TenantResolver,
  topology: DoRunTopology,
  waitUntil: (promise: Promise<unknown>) => void,
) {
  return createRunRouter({
    workflows: WORKFLOWS,
    resolve,
    systemActorId: SYSTEM_ACTOR_ID,
    start: topology.start,
    status: topology.status,
    resume: topology.resume,
    // D4 self-healing, waitUntil-detached — the shared host-kit wrapper owns
    // the detach + reconcile-error logging.
    reconcileApprovals: reconcileApprovalsOnStatusDetached(
      SYSTEM_ACTOR_ID,
      waitUntil,
    ),
  });
}

// The two-cron dispatch rationale and the wrangler.jsonc byte-equality
// contract live with the constants in crons.ts.

async function runPurgeMaintenance(env: Env, cron: string): Promise<void> {
  let purged: number | undefined;
  try {
    // Storing run artifacts in R2? Pass your R2ArtifactStore here as
    // `artifactStore` — and the same store to purgeTenant, if you add
    // tenant offboarding (this template does not call it): the snapshot row
    // is the only record of a run's artifact keys, so a retention purge
    // without the pairing strands the purged runs' artifacts. `limit`
    // batches both purge paths per firing.
    purged = await purgeExpiredWorkflowRuns(env.DB, {
      // allowZero: RUN_RETENTION_DAYS=0 means "purge terminal runs now".
      ttlMs:
        numberVar(env.RUN_RETENTION_DAYS, 30, 'RUN_RETENTION_DAYS', {
          allowZero: true,
        }) *
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
  // Own try/catch, same isolation as the snapshot purge above: a failure in
  // either must never stop the other (D3, 2026-07-11 audit). The purge
  // itself now lives in host-kit (2026-07-11 audit follow-up, FIX 5) —
  // verbatim-shared with the showcase worker instead of hand-copied.
  const approvalsPurged = await runApprovalRetentionPurge({
    store: approvalStoreFactoryFor(env.DB).system(),
    retentionDays: env.APPROVAL_RETENTION_DAYS,
    cron,
  });
  console.log(
    JSON.stringify({ type: 'maintenance', cron, purged, approvalsPurged }),
  );
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
    const topology = createDoRunTopology(env.RUNNER);
    // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
    // approval store to the verified actor's tenant before any service
    // exists — there is no pre-auth store for a rushed fix to reach.
    const baseResolve = createTenantResolver({
      authenticate: bearerActorAuthenticator(buildVerifier(env)),
      storeFactory: approvalStoreFactoryFor(env.DB),
      buildService: (store) =>
        buildHostApprovalService(store, {
          systemActorId: SYSTEM_ACTOR_ID,
          defaultSlaSeconds: numberVar(
            env.APPROVAL_SLA_SECONDS,
            4 * 60 * 60,
            'APPROVAL_SLA_SECONDS',
          ),
          // This deployment's only topology-specific piece: decisions resume
          // the run through its DO stub.
          resumeRun: topology.resumeRecord,
          queue: env.AUDIT_QUEUE,
          waitUntil,
        }),
    });
    const resolve = env.TENANT_APEX_DOMAIN
      ? withSubdomainCrossCheck(baseResolve, {
          apexDomain: env.TENANT_APEX_DOMAIN,
        })
      : baseResolve;

    const routed = request as unknown as Request;
    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    const runResponse = await runRouterFor(
      resolve,
      topology,
      waitUntil,
    )(routed);
    if (runResponse) return runResponse;

    return json({ error: 'not found' }, 404);
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    // Dispatch on WHICH cron fired; an unrecognized expression (ops edited
    // wrangler without updating the constants) runs both sequentially and
    // logs — availability of both duties beats purity on a misconfig.
    const sweep = () =>
      runSlaSweepMaintenance({
        store: approvalStoreFactoryFor(env.DB).system(),
        systemActor: maintenanceActor(SYSTEM_ACTOR_ID),
        queue: env.AUDIT_QUEUE,
        cron: controller.cron,
      });
    if (controller.cron === SWEEP_CRON) {
      ctx.waitUntil(sweep());
    } else if (controller.cron === PURGE_CRON) {
      ctx.waitUntil(runPurgeMaintenance(env, controller.cron));
    } else {
      console.error(
        JSON.stringify({
          type: 'config-error',
          var: 'triggers.crons',
          raw: controller.cron,
          reason: 'unknown cron expression — running both maintenance surfaces',
        }),
      );
      ctx.waitUntil(
        sweep().then(() => runPurgeMaintenance(env, controller.cron)),
      );
    }
  },

  // Audit-export consumer (active only when the wrangler.jsonc `queues`
  // block is uncommented): ships each batch to the SIEM collector; a failed
  // export retries the batch, so nothing is acked unconfirmed.
  async queue(batch: MessageBatch, env: Env): Promise<void> {
    await createAuditQueueHandler({
      endpoint: env.SIEM_ENDPOINT,
      authHeader: env.SIEM_AUTH_HEADER,
    })(batch);
  },
};

export default handler;
