// Phase 1+3 spike worker: proves on workerd (wrangler dev) that
//  - a Mastra workflow runs on Workers + DO with D1-backed suspend/resume
//    that survives dev-server restarts (Phase 1), and
//  - the approval queue closes the loop (Phase 3): a suspend auto-creates a
//    D1-backed approval request, deciding it resumes the run through the
//    run's DO, and the grant provider derives the breakwater grant key from
//    APPROVED store records — the gated publish step sees the grant in its
//    requestContext without any grant ever crossing an HTTP body.
//
//   GET  /workflows                        -> catalog + server-derived actor
//   POST /runs { workflowId, inputData }   -> starts (runId server-minted as
//                                             `spike_<uuid>` per INV-1); on
//                                             suspend, also queues an approval
//   GET  /runs/:workflowId/:runId          -> widened status projection
//   POST /runs/:workflowId/:runId/resume   -> raw resume (no grants — the
//                                             gated step fails closed)
//   /api/approvals[...]                    -> approval queue REST surface
//   POST /api/stream/ticket                -> mint a ~60s WS stream ticket
//   GET  /api/stream/hub?ticket=           -> live approval-queue WebSocket
//   GET  /api/stream/run/:wf/:runId?ticket= -> live run-progress WebSocket
//
// Live streaming (DL-009/DL-010): STREAM_TICKET_SECRET is set as a LOCAL-ONLY
// spike var so the stream stage MOUNTS; DemoHub is the per-tenant fan-out hub.
// The workerd WS proof (suspend/resume/decide fan-out, hibernation persistence,
// expired/cross-tenant ticket fail-closed) is automated in
// scripts/spike-verify.mjs.
//
// Auth: bearer tokens over the SAME host-kit seam every deployed host uses
// (staticTokenVerifier + bearerActorAuthenticator + createRunRouter) — a
// second unguarded routing path is how INV-1 rots, so even this local spike
// mounts the shared run router instead of hand-rolled /runs routing. The
// SPIKE_ACTORS tokens are LOCAL-ONLY fixtures for wrangler dev; this worker
// is never deployed.
//
// Spike script (restart between steps 1 and 2 to prove persistence):
//   1. curl -sX POST localhost:8787/runs -H 'authorization: Bearer spike-operator' \
//        -H 'content-type: application/json' \
//        -d '{"workflowId":"demo-approval","inputData":{"topic":"launch"}}'
//   2. curl -s localhost:8787/api/approvals -H 'authorization: Bearer spike-viewer'
//   3. curl -sX POST localhost:8787/api/approvals/<id>/decide \
//        -H 'authorization: Bearer spike-reviewer' \
//        -H 'content-type: application/json' -d '{"decision":"approve"}'
//   4. curl -s localhost:8787/runs/demo-approval/<runId> \
//        -H 'authorization: Bearer spike-viewer'
//
// One DO instance per run (idFromName(workflowId:runId)) serializes
// start/resume for that run; all instances share the same D1 database.

import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  ExecutionContext,
  ExportedHandler,
} from '@cloudflare/workers-types';
import { z } from 'zod';

import {
  type ApprovalActor,
  ApprovalService,
  type ApprovalStreamSink,
  approvalGrantProvider,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  createTenantResolver,
  D1ApprovalStoreFactory,
  defaultResumeData,
  type TenantBoundApprovalStore,
} from '../src/approval-api/index.js';
import {
  DurableObjectRunner,
  HubDurableObject,
  init,
  type RunnerRuntime,
} from '../src/do-runner/index.js';
import {
  bearerActorAuthenticator,
  createHubTopology,
  createRunRouter,
  createStreamRouter,
  doSummary,
  staticTokenVerifier,
  type WorkflowMeta,
} from '../src/host-kit/index.js';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  /** Per-tenant live-stream hub DO (idFromName(tenantId)); see DemoHub. */
  HUB: DurableObjectNamespace;
  /**
   * HMAC key signing the ~60s WebSocket stream tickets. A LOCAL-ONLY spike
   * fixture set in spike/wrangler.jsonc `vars` (and re-passed by
   * spike-verify.mjs via `--var` so its forged-ticket probes share the exact
   * key). Never a real secret; this worker is never deployed. Present here =>
   * the stream stage always MOUNTS on the spike so the workerd WS proof can
   * exercise it.
   */
  STREAM_TICKET_SECRET: string;
}

/** The connector id an approval grants; the publish step demands it. */
const PUBLISH_CONNECTOR = 'demo-publisher';

/**
 * Id for bridge-queued approval records. The run router builds the full actor
 * (operator role, the request's resolved tenant) — matching showcase/deploy.
 */
const SYSTEM_ACTOR_ID = 'demo-worker';

// LOCAL-ONLY spike identities (wrangler dev; never deployed). One tenant so
// every probe sees the same queue; ids/roles chosen so spike-verify.mjs can
// exercise RBAC, the grant loop, and separation of duties:
//   spike-admin    — the ONLY role in RUN_START_ROLES ∩ decide-capable, so it
//                    can request AND be denied deciding its own request (SoD)
//   spike-operator — starts runs, fires the forged-resume probe
//   spike-reviewer — decides the happy-path approval
//   spike-viewer   — read-only listing
const SPIKE_ACTORS = new Map<string, ApprovalActor>([
  ['spike-admin', { id: 'ada', role: 'admin', tenantId: 'spike' }],
  ['spike-operator', { id: 'opal', role: 'operator', tenantId: 'spike' }],
  ['spike-reviewer', { id: 'ray', role: 'reviewer', tenantId: 'spike' }],
  ['spike-viewer', { id: 'vic', role: 'viewer', tenantId: 'spike' }],
]);

const WORKFLOWS: ReadonlyArray<WorkflowMeta> = [
  {
    id: 'demo-approval',
    title: 'Demo approval',
    description:
      'research -> human approval gate -> grant-gated publish (the spike workflow)',
    sampleInput: { topic: 'launch' },
  },
  {
    id: 'demo-agent-gate',
    title: 'Demo agent gate',
    description:
      'agent tool-call approval gate (R-003 suspend shape) -> grant-gated publish (Track A)',
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
  // identity (INV-1 -> INV-2).
  const approvals = approvalStoreFactory(env.DB).forTenant(tenantId);
  const { createWorkflow, createStep, runtime } = init(env, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records in D1 — decisions become
    // capabilities without any grant crossing a request body.
    requestContextForRun: approvalGrantProvider(approvals),
  });

  const research = createStep({
    id: 'research',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), notes: z.string() }),
    execute: async ({ inputData }) => ({
      topic: inputData.topic,
      notes: `research notes for ${inputData.topic}`,
    }),
  });

  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({ topic: z.string(), notes: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
      approved: z.boolean(),
      decidedBy: z.string().optional(),
    }),
    // `connectors` is the convention every host-kit bridge reads: a
    // server-authored static literal naming the grants a decision should mint.
    // It must never be derived from run input.
    suspendSchema: z.object({
      reason: z.string(),
      connectors: z.array(z.string()),
    }),
    // Matches approval-api's defaultResumeData contract.
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'human approval required before publish',
          connectors: [PUBLISH_CONNECTOR],
        });
      }
      return {
        ...inputData,
        approved: resumeData.approved,
        decidedBy: resumeData.decidedBy,
      };
    },
  });

  const publish = createStep({
    id: 'publish',
    inputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
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
      // Stand-in for a breakwater write-gated connector (the real wrapper is
      // exercised in approval-api/end-to-end.test.ts): demand the documented
      // grant key so the spike proves the provider path on workerd.
      const grants = requestContext.get(BREAKWATER_APPROVED_CONNECTORS_KEY);
      if (!Array.isArray(grants) || !grants.includes(PUBLISH_CONNECTOR)) {
        throw new Error(
          `publish: approval required and not granted for '${PUBLISH_CONNECTOR}'`,
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
    id: 'demo-approval',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      published: z.boolean(),
      approvedBy: z.string().optional(),
    }),
  })
    .then(research)
    .then(approval)
    .then(publish)
    .commit();

  // Track A agent-gate probe: the SAME grant-only loop, but the gate suspends
  // with the durable-agent tool-call shape (R-003) — { type:'approval',
  // toolName, ... } with NO explicit `connectors` array. host-kit's
  // requestedConnectors derives connectors:[toolName] from it, so an approved
  // agent gate mints the grant the publish step below demands, on workerd + D1.
  // This mirrors the durable-agentic-loop's tool-call gate without needing an
  // LLM; the full DurableAgent -> runtime.start drive is unit-tested in
  // agent-runner/.
  const agentGate = createStep({
    id: 'agent-gate',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
    suspendSchema: z.object({
      type: z.literal('approval'),
      toolCallId: z.string(),
      toolName: z.string(),
      args: z.record(z.string(), z.unknown()),
    }),
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          type: 'approval',
          toolCallId: 'call-1',
          toolName: PUBLISH_CONNECTOR,
          args: { topic: inputData.topic },
        });
      }
      return { topic: inputData.topic, approved: resumeData.approved };
    },
  });

  const agentPublish = createStep({
    id: 'agent-publish',
    inputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
    outputSchema: z.object({ topic: z.string(), published: z.boolean() }),
    execute: async ({ inputData, requestContext }) => {
      if (!inputData.approved) {
        return { topic: inputData.topic, published: false };
      }
      const grants = requestContext.get(BREAKWATER_APPROVED_CONNECTORS_KEY);
      if (!Array.isArray(grants) || !grants.includes(PUBLISH_CONNECTOR)) {
        throw new Error(
          `agent-publish: approval required and not granted for '${PUBLISH_CONNECTOR}'`,
        );
      }
      return { topic: inputData.topic, published: true };
    },
  });

  createWorkflow({
    id: 'demo-agent-gate',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), published: z.boolean() }),
  })
    .then(agentGate)
    .then(agentPublish)
    .commit();

  return runtime;
}

export class DemoRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env, this.tenantId);
  }
}

/**
 * The per-tenant live-stream hub DO (DL-009). The wrangler `HUB` binding + the
 * append-only `v2` migration resolve this named export; the base class does all
 * the work (fan-out over hibernatable WebSockets + presence), so the body is
 * empty. Addressed idFromName(tenantId), so id.name IS the tenant and the
 * fan-out is tenant-disjoint by construction. The workerd spike
 * (scripts/spike-verify.mjs) drives this over a real WebSocket to prove
 * fan-out, hibernation persistence, and ticket fail-closed.
 */
export class DemoHub extends HubDurableObject<Env> {}

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

// Worker-level approval service: shares the DO's D1 database; decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request).
function buildApprovalService(
  store: TenantBoundApprovalStore,
  env: Env,
  stream: ApprovalStreamSink | undefined,
): ApprovalService {
  return new ApprovalService({
    store,
    defaultSlaSeconds: 15 * 60,
    // Live fan-out: every SUCCESSFUL mutation forwards to the tenant hub, which
    // fans it out to that tenant's open dashboard sockets. Fire-and-forget; the
    // caller keeps the publish alive with ctx.waitUntil (see fetch below).
    stream,
    // doSummary (host-kit) reads the DO's answer and rethrows a non-ok one as a
    // RunRouteError carrying the DO's own status — the same reader the showcase
    // and deploy hosts use. The spike keeps its own topology, not its own parsing.
    resumeRun: async (record, decision) =>
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
  });
}

const handler: ExportedHandler<Env> = {
  async fetch(
    request: CfRequest,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    // Fetch-scope live fan-out sink (mirrors createFlowsafeWorker): each
    // successful approval mutation forwards to the tenant hub, kept alive by
    // ctx.waitUntil so the publish completes AFTER the mutation's response (a
    // fire-and-forget publish would be cancelled). Contained — a failed fan-out
    // logs and never fails the mutation.
    const hubTopology = createHubTopology(env.HUB);
    const streamSink: ApprovalStreamSink = (event) =>
      ctx.waitUntil(
        hubTopology.publish(event).catch((error: unknown) => {
          console.error('stream publish failed', error);
        }),
      );

    // AUTHENTICATE FIRST, then construct (INV-2): the resolver binds the
    // approval store to the verified actor's tenant.
    const resolve = createTenantResolver({
      authenticate: bearerActorAuthenticator(staticTokenVerifier(SPIKE_ACTORS)),
      storeFactory: approvalStoreFactory(env.DB),
      buildService: (store) => buildApprovalService(store, env, streamSink),
    });
    const routed = request as unknown as Request;

    // Stream stage BEFORE approvals (same order as the composer). The Worker is
    // the SOLE ticket authority; the hub/runner DOs re-bind by their own
    // idFromName identity. Every route is under /api/stream/, so it composes
    // ahead of the approval router without overlapping it.
    const streamResponse = await createStreamRouter({
      resolve,
      ticketSecret: env.STREAM_TICKET_SECRET,
      hub: env.HUB,
      runner: env.RUNNER,
    })(routed);
    if (streamResponse) return streamResponse;

    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    // The shared run surface: server-minted `${tenantId}_${uuid}` runIds,
    // the 401 -> coarse-role -> per-workflow gate order, ownership checks,
    // and the suspension->approval bridge — all host-kit, zero spike-local
    // routing. Topology (the only host-specific part): every leg goes
    // through the run's DO stub.
    const runResponse = await createRunRouter({
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
              body: JSON.stringify(body ?? {}),
            },
          ),
        ),
    })(routed);
    if (runResponse) return runResponse;

    return json({ error: 'not found' }, 404);
  },
};

export default handler;
