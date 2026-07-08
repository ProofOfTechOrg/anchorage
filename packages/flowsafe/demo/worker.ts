// Phase 1+3 spike worker: proves on workerd (wrangler dev) that
//  - a Mastra workflow runs on Workers + DO with D1-backed suspend/resume
//    that survives dev-server restarts (Phase 1), and
//  - the approval queue closes the loop (Phase 3): a suspend auto-creates a
//    D1-backed approval request, deciding it resumes the run through the
//    run's DO, and the grant provider derives the breakwater grant key from
//    APPROVED store records — the gated publish step sees the grant in its
//    requestContext without any grant ever crossing an HTTP body.
//
//   POST /runs { workflowId, inputData }   -> starts; on suspend, also queues
//                                             an approval (response.approval)
//   GET  /runs/:workflowId/:runId          -> widened status projection
//   POST /runs/:workflowId/:runId/resume   -> raw resume (no grants — the
//                                             gated step fails closed)
//   /api/approvals[...]                    -> approval queue REST surface;
//                                             demo auth: x-actor-id/x-actor-role
//                                             headers (production wires real
//                                             authentication here)
//
// One DO instance per run (idFromName(workflowId:runId)) serializes
// start/resume for that run; all instances share the same D1 database.
//
// Spike script (restart between steps 1 and 2 to prove persistence):
//   1. curl -sX POST localhost:8787/runs -H 'content-type: application/json' \
//        -d '{"workflowId":"demo-approval","inputData":{"topic":"launch"}}'
//   2. curl -s localhost:8787/api/approvals -H 'x-actor-id: vic' -H 'x-actor-role: viewer'
//   3. curl -sX POST localhost:8787/api/approvals/<id>/decide \
//        -H 'x-actor-id: ray' -H 'x-actor-role: reviewer' \
//        -H 'content-type: application/json' -d '{"decision":"approve"}'
//   4. curl -s localhost:8787/runs/demo-approval/<runId>

import type {
  D1Database,
  DurableObjectNamespace,
  ExportedHandler,
  Request as CfRequest,
} from '@cloudflare/workers-types';
import { z } from 'zod';

import {
  approvalGrantProvider,
  type ApprovalRole,
  ApprovalService,
  APPROVAL_ROLES,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  D1ApprovalStore,
  defaultResumeData,
} from '../src/approval-api/index.js';
import {
  DurableObjectRunner,
  init,
  type RunnerRuntime,
  type RunSummary,
} from '../src/do-runner/index.js';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
}

/** The connector id an approval grants; the publish step demands it. */
const PUBLISH_CONNECTOR = 'demo-publisher';

function defineWorkflows(env: Env): RunnerRuntime {
  const approvals = new D1ApprovalStore(env.DB);
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
    suspendSchema: z.object({ reason: z.string() }),
    // Matches approval-api's defaultResumeData contract.
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({ reason: 'human approval required before publish' });
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

  return runtime;
}

export class DemoRunner extends DurableObjectRunner<Env> {
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

// Worker-level approval service: shares the DO's D1 database; decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request).
function buildApprovalService(env: Env): ApprovalService {
  const store = new D1ApprovalStore(env.DB);
  return new ApprovalService({
    store,
    defaultSlaSeconds: 15 * 60,
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
      const summary = await response.json();
      if (!response.ok) {
        throw new Error(
          `resume failed (${response.status}): ${JSON.stringify(summary)}`,
        );
      }
      return summary;
    },
  });
}

const handler: ExportedHandler<Env> = {
  async fetch(request: CfRequest, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const segments = url.pathname.split('/').filter(Boolean);

    // Demo-only authentication: trust two headers. A real deployment maps
    // its session/JWT to the actor here — this stays inside the trusted
    // computing base either way.
    const approvalRouter = createApprovalRouter({
      service: buildApprovalService(env),
      authenticate: (routed) => {
        const id = routed.headers.get('x-actor-id');
        const role = routed.headers.get('x-actor-role');
        return id &&
          role &&
          (APPROVAL_ROLES as readonly string[]).includes(role)
          ? { id, role: role as ApprovalRole }
          : undefined;
      },
    });
    const approvalResponse = await approvalRouter(
      request as unknown as Request,
    );
    if (approvalResponse) return approvalResponse;

    if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);

    if (request.method === 'POST' && segments.length === 1) {
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
      // A suspension IS an approval request: queue it (idempotently — the
      // partial unique index collapses duplicates) so reviewers see it.
      // Capturing the step's (suspendedAt, resumedAt) pair binds the approval
      // to THIS suspension exactly (clock-free grant minting).
      const service = buildApprovalService(env);
      const stepPath = summary.suspended?.[0];
      const stepKey = stepPath?.join('.');
      // Attribute the request to the human who started the run (the
      // x-actor-id header), not the system bridge — otherwise the
      // self-decision separation-of-duties check can never fire. Falls back
      // to 'demo-worker' when the start carries no identity.
      const requestedBy = request.headers.get('x-actor-id') ?? 'demo-worker';
      const { record } = await service.create(
        {
          workflowId: body.workflowId,
          runId: summary.runId,
          stepPath,
          suspendedAt:
            stepKey !== undefined ? summary.suspendedAt?.[stepKey] : undefined,
          resumedAt:
            stepKey !== undefined ? summary.resumedAt?.[stepKey] : undefined,
          title: `Approve '${body.workflowId}' run`,
          payload: summary.suspendPayload,
          connectors: [PUBLISH_CONNECTOR],
          requestedBy,
        },
        { id: 'demo-worker', role: 'operator' },
      );
      return json({ ...summary, approval: record });
    }

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
};

export default handler;
