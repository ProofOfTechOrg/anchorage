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
} from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  assertWorkflowsRegistered,
  createFlowsafeWorker,
  parseActorTokens,
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
  /**
   * Separation-of-duties exemption (var). Unset or a `false` spelling keeps SoD
   * on (default); `true` lets every decider self-decide; a CSV of roles
   * (e.g. `admin`) exempts only those — a single-operator deployment sets
   * `admin`. Any invalid value falls back to OFF.
   */
  APPROVAL_ALLOW_SELF_DECISION?: string;
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

// The auth seam (parseActorTokens + bearerActorAuthenticator), the run routes
// with their RBAC gate order, the approval bridge, the service assembly, and
// the whole Worker pipeline (createFlowsafeWorker) all live in
// @proofoftech/flowsafe/host-kit. They are security-critical and tested
// there — the (suspendedAt, resumeCount) capture that binds a decision to one
// exact suspension, the separation-of-duties re-queue, and the role gates. Do
// not re-derive them here; production SSO/JWT verification replaces only the
// `buildVerifier` seam.

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

// The composed Worker: /healthz → /api/approvals → /workflows + /runs → 404,
// two-cron maintenance (sweep vs purge never share an invocation; the
// byte-equality contract with wrangler.jsonc lives in crons.ts), and the
// audit-export queue consumer. This deployment supplies its workflows, its
// verifier, and the optional client-per-subdomain cross-check; add run
// artifacts (R2ArtifactStore) by copying the purge pairing notes in
// host-kit's runPurgeMaintenance into an `extraPurgeDuties` hook.
const worker = createFlowsafeWorker<Env>({
  workflows: WORKFLOWS,
  systemActorId: SYSTEM_ACTOR_ID,
  buildVerifier,
  crons: { sweep: SWEEP_CRON, purge: PURGE_CRON },
  // Defense in depth over INV-2 (see Env.TENANT_APEX_DOMAIN), only when set.
  wrapResolve: (resolve, env) =>
    env.TENANT_APEX_DOMAIN
      ? withSubdomainCrossCheck(resolve, {
          apexDomain: env.TENANT_APEX_DOMAIN,
        })
      : resolve,
});

const handler: ExportedHandler<Env> = {
  fetch: (request: CfRequest, env: Env, ctx: ExecutionContext) =>
    worker.fetch(request as unknown as Request, env, ctx),
  scheduled: (
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ) => worker.scheduled(controller, env, ctx),
  queue: (batch: MessageBatch, env: Env) => worker.queue(batch, env),
};

export default handler;
