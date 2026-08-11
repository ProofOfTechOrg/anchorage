// Reference production deployment for the flowsafe DO runner + approval
// queue. Copy this directory as the starting point for a real Worker: the
// wiring is production-shaped (bearer-token auth seam, alarm-driven SLA sweep
// and retention purge, structured audit logs, multi-gate approval bridging,
// opt-in live streaming); replace the example workflow with your own and swap
// bearerActorAuthenticator for your identity provider.
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
//   POST /api/stream/ticket               -> mint a short-lived (~60s) WS
//                                            stream ticket (authenticated).
//                                            Opt-in: the whole /api/stream/*
//                                            stage mounts only when the HUB
//                                            binding AND STREAM_TICKET_SECRET
//                                            are both present (else poll-only)
//   GET  /api/stream/hub?ticket=          -> live approval-queue WebSocket
//                                            (deployment hub DO fan-out)
//   GET  /api/stream/run/:wf/:runId?ticket= -> live run-progress WebSocket
//                                            (per-run WS on the runner DO)
//   GET  /healthz                         -> liveness (unauthenticated)
//
// All routes except /healthz require `Authorization: Bearer <token>` mapped
// to an actor via the APPROVAL_ACTOR_TOKENS secret. No secret => every
// authenticated route 401s (fail closed).
//
// Live streaming is OPT-IN: `wrangler secret put STREAM_TICKET_SECRET` (a
// DEDICATED HMAC key, kept distinct from any session-JWT secret) turns on the
// /api/stream/* stage (the HUB DO binding below is always declared). The
// browser cannot set Authorization on a WebSocket, so a client mints a ~60s
// HMAC ticket over authenticated REST and presents it in the WS URL query; the
// Worker is the SOLE ticket authority and the ticket carries ADDRESSING only
// (channel/runId/actor/exp), never a grant. Absent the secret, every
// dashboard stays on its poll fallback and nothing else changes.
//
// The fixed-name maintenance Durable Object gives the SLA sweep and retention
// purge separate alarm invocations, so an uncatchable CPU termination in one
// cannot starve the other.
//
// Deploy checklist: README.md next to this file.

import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
  Queue,
} from '@cloudflare/workers-types';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { createConnector } from '@proofoftech/breakwater/connector-sdk';
import { approvalGrantProvider } from '@proofoftech/flowsafe/approval-api';
import {
  DurableObjectRunner,
  HubDurableObject,
  init,
  type RunnerRuntime,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  assertWorkflowsRegistered,
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeWorker,
  type FlowsafeWorkerConfig,
  parseActorTokens,
  staticTokenVerifier,
  type TokenVerifier,
  type WorkflowMeta,
} from '@proofoftech/flowsafe/host-kit';
import { z } from 'zod';

interface Env {
  DB: D1Database;
  /** Provisioning-stamped tag; must match the sentinel row in DB. */
  DEPLOYMENT_TENANT: string;
  /** Secret credential shared only by this deployment's Worker and DOs. */
  DEPLOYMENT_IDENTITY_SECRET: string;
  RUNNER: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
  /**
   * Deployment live-stream hub Durable Object namespace (DL-009). Declared by
   * the `HUB` binding in wrangler.jsonc (see its v2 migration), so it is always
   * present at runtime. createFlowsafeWorker mounts the /api/stream/* stage —
   * ticket mint + the hub/run WebSocket upgrades — ONLY when this binding AND
   * STREAM_TICKET_SECRET are both present; either absent leaves every dashboard
   * on its poll fallback (DL-019). Addressed as one fixed deployment singleton.
   */
  HUB: DurableObjectNamespace;
  /**
   * Secret (`wrangler secret put STREAM_TICKET_SECRET`): the dedicated HMAC key
   * that signs the short-lived (~60s) WebSocket stream tickets (DL-010/DL-019).
   * A ticket is ADDRESSING only — channel + runId + actor + exp —
   * never a grant. Keep it DISTINCT from any session-JWT secret so a stream
   * ticket and a session token can never be confused under one key. Absent =>
   * the stream stage stays unmounted (streaming is opt-in; poll-only still
   * works).
   */
  STREAM_TICKET_SECRET?: string;
  /**
   * Secret (`wrangler secret put APPROVAL_ACTOR_TOKENS`): JSON map of bearer
   * token -> actor, e.g. {"<random-token>": {"id": "ray", "role":
   * "reviewer"}}. Absent => an empty map => every authenticated route 401s.
   * Swap bearerActorAuthenticator for your SSO/JWT verification to replace it —
   * actor mapping stays inside the trusted computing base either way.
   */
  APPROVAL_ACTOR_TOKENS?: string;
  MAINTENANCE_ADMIN_SECRET?: string;
  /** Default SLA seconds for new approvals (var; default 14400 = 4h). */
  APPROVAL_SLA_SECONDS?: string;
  /**
   * Separation-of-duties exemption (var). Unset or a `false` spelling keeps SoD
   * on (default); `true` lets every decider self-decide; a CSV of roles
   * (e.g. `admin`) exempts only those — a single-operator deployment sets
   * `admin`. Any invalid value falls back to OFF.
   */
  APPROVAL_ALLOW_SELF_DECISION?: string;
  /** Alarm maintenance purges terminal run snapshots older than this (var; default 30; 0 = immediately). */
  RUN_RETENTION_DAYS?: string;
  /** Alarm maintenance purges DECIDED approvals older than this (var; default 30; 0 = immediately). */
  APPROVAL_RETENTION_DAYS?: string;
  /**
   * Agent-memory thread TTL in days (var; docs/agent-memory-isolation.md): the
   * purge duty deletes threads untouched for longer than this, with their
   * messages. UNSET (the default) => no thread ever expires — a conversation is
   * something a host means to keep, unlike a terminal run snapshot, so this
   * deployment only starts expiring memory when an operator names a number.
   */
  THREAD_RETENTION_DAYS?: string;
  /**
   * Optional audit export: this tenant Worker only produces to the shared
   * queue. The control plane owns consumption and SIEM delivery.
   */
  AUDIT_QUEUE?: Queue;
}

/** Id for system-created records. */
const SYSTEM_PRINCIPAL_ID = 'flowsafe-worker';

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

function defineWorkflows(env: Env): RunnerRuntime {
  const approvals = approvalStoreFactoryFor(env.DB).store();
  const { createWorkflow, createStep, runtime } = init(env, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records in D1 — decisions become
    // capabilities without any grant crossing a request body.
    requestContextForRun: approvalGrantProvider(approvals),
  });
  const publisher = createConnector<{ topic: string }, { published: boolean }>({
    id: EXAMPLE_CONNECTOR,
    description: 'Publishes the approved example topic',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ published: z.boolean() }),
    permissions: { sideEffect: 'write', requiresApproval: true },
    execute: async () => ({ published: true }),
  });

  // Replace from here down with your workflows. Conventions to keep:
  //  - a gate step suspends with { reason, connectors }, where `connectors` is
  //    a server-authored STATIC literal: the bridge copies it into the queued
  //    approval, so a decision mints exactly the grants that suspension asked
  //    for. Deriving it from run input would let client input choose its own
  //    capability;
  //  - resumeSchema matches approval-api's defaultResumeData contract;
  //  - wire durable breakwater stores (D1IdempotencyStore /
  //    D1RateLimitStore) when budgets must survive isolate replacement. An
  //    in-memory budget under DO-per-run routing is a per-run budget.
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
      if (!publisher.execute) throw new Error('publisher has no execute');
      const result = (await publisher.execute({ topic: inputData.topic }, {
        requestContext,
      } as unknown as ToolExecutionContext)) as { published: boolean };
      return {
        topic: inputData.topic,
        published: result.published,
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
    return defineWorkflows(env);
  }

  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }
}

/**
 * The deployment live-stream hub Durable Object (DL-009). The wrangler `HUB`
 * binding + the append-only `v2` migration resolve this named export; the base
 * class does all the work (fan-out over hibernatable WebSockets + a presence
 * roster), so the subclass body is empty. Addressed under a fixed singleton
 * name by the composer's stream router; no ticket verification happens here (the
 * Worker is the sole ticket authority). Fan-out activates once
 * STREAM_TICKET_SECRET is set; until then the DO is bound but idle.
 */
export class FlowsafeHub extends HubDurableObject<Env> {}

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
// alarm maintenance (sweep and purge never share an invocation). This
// deployment supplies its workflows and its
// verifier. To add run
// artifacts (R2ArtifactStore), set the `artifactStore` field on this config:
// createFlowsafeWorker pairs artifact deletion INSIDE the retention purge, so
// each expired run's artifacts are deleted BEFORE its snapshot row (the row is
// the only enumerable record of the run's artifact keys). An `extraPurgeDuties`
// hook cannot do this — it runs AFTER the rows are deleted, when the keys are
// already unenumerable.
const workerConfig = {
  workflows: WORKFLOWS,
  systemPrincipalId: SYSTEM_PRINCIPAL_ID,
  buildVerifier,
  maintenance: {
    sweepIntervalMs: 15 * 60 * 1_000,
    purgeIntervalMs: 60 * 60 * 1_000,
  },
} satisfies FlowsafeWorkerConfig<Env>;

export class FlowsafeMaintenance extends createFlowsafeMaintenanceDurableObject(
  workerConfig,
) {}

const worker = createFlowsafeWorker<Env>(workerConfig);

const handler: ExportedHandler<Env> = {
  fetch: (request: CfRequest, env: Env, ctx: ExecutionContext) =>
    worker.fetch(request as unknown as Request, env, ctx),
};

export default handler;
