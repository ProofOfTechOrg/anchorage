// SPDX-License-Identifier: Apache-2.0

import {
  WorkflowEntrypoint,
  type WorkflowEvent,
  type WorkflowStep,
} from 'cloudflare:workers';
import type {
  D1Database,
  DurableObjectNamespace,
  ExportedHandler,
  Workflow,
} from '@cloudflare/workers-types';
import {
  createConnector,
  invokeConnector,
} from '@proofoftech/breakwater/connector-sdk';
import { z } from 'zod';
import {
  ApprovalService,
  approvalGrantProvider,
  D1ApprovalStoreFactory,
} from '../src/approval-api/index.js';
import {
  DurableObjectRunner,
  ensureDeploymentIdentityBindings,
  init,
  isPathSafeId,
  type RunnerRuntime,
} from '../src/do-runner/index.js';
import {
  abandonApprovalsForRun,
  createDoRunTopology,
  doSummary,
  queueApprovalForSuspension,
} from '../src/host-kit/index.js';

interface BenchmarkInput {
  deliveryId: string;
  topic: string;
}

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  FLOWSAFE_RUNNER: DurableObjectNamespace;
  NATIVE_WORKFLOW: Workflow<BenchmarkInput>;
}

const WORKFLOW_ID = 'approval-effect-benchmark';
const CONNECTOR_ID = 'benchmark-publisher';
const OPERATOR_ID = 'benchmark-operator';
const REVIEWER = { id: 'benchmark-reviewer', role: 'reviewer' } as const;
const SYSTEM_PRINCIPAL_ID = 'benchmark-system';

const schemaReady = new WeakMap<D1Database, Promise<void>>();
const approvalFactories = new WeakMap<D1Database, D1ApprovalStoreFactory>();

function ensureBenchmarkSchema(db: D1Database): Promise<void> {
  let pending = schemaReady.get(db);
  if (!pending) {
    pending = db
      .batch([
        db.prepare(`CREATE TABLE IF NOT EXISTS durability_benchmark_deliveries (
          authority TEXT NOT NULL,
          delivery_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (authority, delivery_id)
        )`),
        db.prepare(`CREATE TABLE IF NOT EXISTS durability_benchmark_effects (
          authority TEXT NOT NULL,
          run_id TEXT NOT NULL,
          effect_key TEXT NOT NULL,
          delivery_id TEXT NOT NULL,
          topic TEXT NOT NULL,
          created_at TEXT NOT NULL,
          PRIMARY KEY (authority, run_id, effect_key)
        )`),
        db.prepare(`CREATE TABLE IF NOT EXISTS durability_benchmark_native_approvals (
          run_id TEXT PRIMARY KEY,
          status TEXT NOT NULL CHECK (status IN ('pending', 'approved')),
          decided_by TEXT,
          created_at TEXT NOT NULL,
          decided_at TEXT
        )`),
      ])
      .then(() => undefined)
      .catch((error: unknown) => {
        schemaReady.delete(db);
        throw error;
      });
    schemaReady.set(db, pending);
  }
  return pending;
}

function approvalFactory(db: D1Database): D1ApprovalStoreFactory {
  let factory = approvalFactories.get(db);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db, {
      workflowSnapshotTable: 'mastra_workflow_snapshot',
    });
    approvalFactories.set(db, factory);
  }
  return factory;
}

async function recordEffect(
  db: D1Database,
  authority: 'flowsafe' | 'native',
  runId: string,
  input: BenchmarkInput,
): Promise<{ effectCount: number }> {
  await db
    .prepare(
      `INSERT OR IGNORE INTO durability_benchmark_effects
         (authority, run_id, effect_key, delivery_id, topic, created_at)
       VALUES (?, ?, 'publish', ?, ?, datetime('now'))`,
    )
    .bind(authority, runId, input.deliveryId, input.topic)
    .run();
  const count = await db
    .prepare(
      `SELECT COUNT(*) AS count
       FROM durability_benchmark_effects
       WHERE authority = ? AND run_id = ?`,
    )
    .bind(authority, runId)
    .first<{ count: number }>();
  return {
    effectCount: count?.count ?? 0,
  };
}

function containsConnectorGrant(value: unknown): boolean {
  return (
    Array.isArray(value) &&
    value.some(
      (grant) =>
        typeof grant === 'object' &&
        grant !== null &&
        (grant as { connectorId?: unknown }).connectorId === CONNECTOR_ID,
    )
  );
}

function defineFlowsafeWorkflow(env: Env): RunnerRuntime {
  const { createStep, createWorkflow, runtime } = init(env, {
    requestContextForRun: approvalGrantProvider(
      approvalFactory(env.DB).store(),
    ),
  });
  const publisher = createConnector<
    BenchmarkInput & { runId: string },
    { effectCount: number }
  >({
    id: CONNECTOR_ID,
    description: 'Records the benchmark approval-gated effect',
    inputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      runId: z.string(),
    }),
    outputSchema: z.object({ effectCount: z.number() }),
    permissions: { sideEffect: 'write', requiresApproval: true },
    execute: async (input) => ({
      effectCount: (await recordEffect(env.DB, 'flowsafe', input.runId, input))
        .effectCount,
    }),
  });

  const research = createStep({
    id: 'research',
    inputSchema: z.object({ deliveryId: z.string(), topic: z.string() }),
    outputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      notes: z.string(),
    }),
    execute: async ({ inputData }) => ({
      ...inputData,
      notes: `research:${inputData.topic}`,
    }),
  });
  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      notes: z.string(),
    }),
    outputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      notes: z.string(),
      approved: z.boolean(),
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
    execute: async ({ inputData, resumeData, suspend }) =>
      resumeData
        ? { ...inputData, approved: resumeData.approved }
        : suspend({
            reason: 'approval required before the benchmark effect',
            connectors: [CONNECTOR_ID],
          }),
  });
  const publish = createStep({
    id: 'publish',
    inputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      notes: z.string(),
      approved: z.boolean(),
    }),
    outputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      published: z.boolean(),
      effectCount: z.number(),
      grantReconstructed: z.boolean(),
    }),
    execute: async ({ inputData, requestContext, runId }) => {
      if (!inputData.approved) {
        return {
          deliveryId: inputData.deliveryId,
          topic: inputData.topic,
          published: false,
          effectCount: 0,
          grantReconstructed: false,
        };
      }
      const grants = requestContext.get('breakwater.connectorGrants');
      const output = await invokeConnector(
        publisher,
        { deliveryId: inputData.deliveryId, topic: inputData.topic, runId },
        { requestContext },
      );
      return {
        deliveryId: inputData.deliveryId,
        topic: inputData.topic,
        published: true,
        effectCount: output.effectCount,
        grantReconstructed: containsConnectorGrant(grants),
      };
    },
  });

  createWorkflow({
    id: WORKFLOW_ID,
    inputSchema: z.object({ deliveryId: z.string(), topic: z.string() }),
    outputSchema: z.object({
      deliveryId: z.string(),
      topic: z.string(),
      published: z.boolean(),
      effectCount: z.number(),
      grantReconstructed: z.boolean(),
    }),
  })
    .then(research)
    .then(approval)
    .then(publish)
    .commit();
  return runtime;
}

export class BenchmarkFlowsafeRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return defineFlowsafeWorkflow(env);
  }

  protected runOwnership(env: Env) {
    return approvalFactory(env.DB).resources();
  }

  protected runLifecycle(env: Env) {
    const service = new ApprovalService({
      store: approvalFactory(env.DB).store(),
    });
    return {
      abandonApprovals: (
        workflowId: string,
        runId: string,
        status: 'cancelled' | 'timed_out',
      ) =>
        abandonApprovalsForRun(
          service,
          workflowId,
          runId,
          status,
          SYSTEM_PRINCIPAL_ID,
        ).then(() => undefined),
    };
  }
}

export class BenchmarkNativeWorkflow extends WorkflowEntrypoint<
  Env,
  BenchmarkInput
> {
  async run(
    event: Readonly<WorkflowEvent<BenchmarkInput>>,
    step: WorkflowStep,
  ): Promise<unknown> {
    const research = await step.do('research', async () => ({
      ...event.payload,
      notes: `research:${event.payload.topic}`,
    }));
    await step.waitForEvent('approval', {
      type: 'approval',
      timeout: '1 hour',
    });
    const grant = await step.do('reconstruct approval grant', async () => {
      const approval = await this.env.DB.prepare(
        `SELECT status, decided_by
         FROM durability_benchmark_native_approvals
         WHERE run_id = ?`,
      )
        .bind(event.instanceId)
        .first<{ status: string; decided_by: string | null }>();
      if (approval?.status !== 'approved' || !approval.decided_by) {
        throw new Error('no approved native grant exists for this run');
      }
      return { connectorId: CONNECTOR_ID, decidedBy: approval.decided_by };
    });
    return step.do(
      'publish',
      { retries: { limit: 3, delay: '1 second', backoff: 'constant' } },
      async () => ({
        deliveryId: research.deliveryId,
        topic: research.topic,
        published: true,
        effectCount: (
          await recordEffect(this.env.DB, 'native', event.instanceId, research)
        ).effectCount,
        grantReconstructed: grant.connectorId === CONNECTOR_ID,
      }),
    );
  }
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
    },
  });
}

async function bodyOf(request: Request): Promise<Record<string, unknown>> {
  const value = await request.json<unknown>();
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('JSON object required');
  }
  return value as Record<string, unknown>;
}

function stringField(body: Record<string, unknown>, name: string): string {
  const value = body[name];
  if (typeof value !== 'string' || !isPathSafeId(value)) {
    throw new Error(`${name} must be a path-safe identifier`);
  }
  return value;
}

function textField(body: Record<string, unknown>, name: string): string {
  const value = body[name];
  if (typeof value !== 'string' || value.length === 0 || value.length > 256) {
    throw new Error(`${name} must contain 1-256 characters`);
  }
  return value;
}

async function delivery(
  db: D1Database,
  authority: 'flowsafe' | 'native',
  deliveryId: string,
  runId: string,
): Promise<{ inserted: boolean; runId: string }> {
  const result = await db
    .prepare(
      `INSERT OR IGNORE INTO durability_benchmark_deliveries
         (authority, delivery_id, run_id, created_at)
       VALUES (?, ?, ?, datetime('now'))`,
    )
    .bind(authority, deliveryId, runId)
    .run();
  const row = await db
    .prepare(
      `SELECT run_id FROM durability_benchmark_deliveries
       WHERE authority = ? AND delivery_id = ?`,
    )
    .bind(authority, deliveryId)
    .first<{ run_id: string }>();
  if (!row) throw new Error('provider delivery ledger write was lost');
  return { inserted: result.meta.changes === 1, runId: row.run_id };
}

function flowsafeService(env: Env): ApprovalService {
  const topology = createDoRunTopology(
    env.FLOWSAFE_RUNNER,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );
  return new ApprovalService({
    store: approvalFactory(env.DB).store(),
    resumeRun: topology.resumeRecord,
  });
}

async function flowsafeStatus(env: Env, runId: string): Promise<Response> {
  const topology = createDoRunTopology(
    env.FLOWSAFE_RUNNER,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );
  const summary = await topology.status(WORKFLOW_ID, runId);
  if (!summary) return json({ error: 'run not found' }, 404);
  const approvals = await approvalFactory(env.DB).store().list({
    workflowId: WORKFLOW_ID,
    runId,
  });
  return json({ authority: 'flowsafe', summary, approvals });
}

async function handleFlowsafe(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const segments = url.pathname.split('/').filter(Boolean);
  const topology = createDoRunTopology(
    env.FLOWSAFE_RUNNER,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );

  if (request.method === 'POST' && url.pathname === '/flowsafe/deliveries') {
    const body = await bodyOf(request);
    const deliveryId = stringField(body, 'deliveryId');
    const requestedRunId = stringField(body, 'runId');
    const topic = textField(body, 'topic');
    const accepted = await delivery(
      env.DB,
      'flowsafe',
      deliveryId,
      requestedRunId,
    );
    if (!accepted.inserted) {
      return json({
        authority: 'flowsafe',
        deduplicated: true,
        runId: accepted.runId,
      });
    }
    const summary = await topology.start({
      workflowId: WORKFLOW_ID,
      runId: requestedRunId,
      inputData: { deliveryId, topic },
      principal: { kind: 'human', id: OPERATOR_ID, role: 'operator' },
    });
    const approvals = await queueApprovalForSuspension(
      flowsafeService(env),
      WORKFLOW_ID,
      summary,
      OPERATOR_ID,
      SYSTEM_PRINCIPAL_ID,
    );
    return json({ authority: 'flowsafe', summary, approvals }, 201);
  }

  if (
    request.method === 'GET' &&
    segments[0] === 'flowsafe' &&
    segments[1] === 'runs' &&
    segments.length === 3
  ) {
    return flowsafeStatus(env, stringField({ runId: segments[2] }, 'runId'));
  }

  if (
    request.method === 'POST' &&
    segments[0] === 'flowsafe' &&
    segments[1] === 'runs' &&
    segments[3] === 'approve' &&
    segments.length === 4
  ) {
    const runId = stringField({ runId: segments[2] }, 'runId');
    const body = await bodyOf(request);
    const approvalId = stringField(body, 'approvalId');
    const service = flowsafeService(env);
    const record = await service.get(approvalId, REVIEWER);
    if (record.runId !== runId || record.workflowId !== WORKFLOW_ID) {
      return json({ error: 'approval does not belong to this run' }, 409);
    }
    const decided = await service.decide(
      approvalId,
      { decision: 'approve' },
      REVIEWER,
    );
    return json({ authority: 'flowsafe', ...decided });
  }

  if (
    request.method === 'POST' &&
    segments[0] === 'flowsafe' &&
    segments[1] === 'runs' &&
    segments[3] === 'resume' &&
    segments.length === 4
  ) {
    const runId = stringField({ runId: segments[2] }, 'runId');
    const summary = await topology.resume(
      WORKFLOW_ID,
      runId,
      {
        step: ['approval'],
        resumeData: { approved: true, decidedBy: REVIEWER.id },
      },
      REVIEWER.id,
      'human',
    );
    return json({ authority: 'flowsafe', summary });
  }

  if (
    request.method === 'POST' &&
    url.pathname === '/flowsafe/probes/cross-run'
  ) {
    const body = await bodyOf(request);
    const objectRunId = stringField(body, 'objectRunId');
    const requestRunId = stringField(body, 'requestRunId');
    const stub = env.FLOWSAFE_RUNNER.get(
      env.FLOWSAFE_RUNNER.idFromName(`${WORKFLOW_ID}:${objectRunId}`),
    );
    const response = await stub.fetch(
      `http://do/runs/${WORKFLOW_ID}/${requestRunId}`,
      {
        headers: {
          'x-flowsafe-deployment-identity': env.DEPLOYMENT_IDENTITY_SECRET,
        },
      },
    );
    let detail: unknown;
    try {
      detail = await doSummary(response);
    } catch (error) {
      detail = error instanceof Error ? error.message : String(error);
    }
    return json({ status: response.status, detail });
  }

  return json({ error: 'not found' }, 404);
}

async function nativeStatus(env: Env, runId: string): Promise<Response> {
  const [status, approval] = await Promise.all([
    env.NATIVE_WORKFLOW.get(runId).then((instance) => instance.status()),
    env.DB.prepare(
      `SELECT run_id, status, decided_by, created_at, decided_at
       FROM durability_benchmark_native_approvals WHERE run_id = ?`,
    )
      .bind(runId)
      .first(),
  ]);
  return json({ authority: 'native', summary: status, approval });
}

async function handleNative(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const segments = url.pathname.split('/').filter(Boolean);
  if (request.method === 'POST' && url.pathname === '/native/deliveries') {
    const body = await bodyOf(request);
    const deliveryId = stringField(body, 'deliveryId');
    const requestedRunId = stringField(body, 'runId');
    const topic = textField(body, 'topic');
    const accepted = await delivery(
      env.DB,
      'native',
      deliveryId,
      requestedRunId,
    );
    if (!accepted.inserted) {
      return json({
        authority: 'native',
        deduplicated: true,
        runId: accepted.runId,
      });
    }
    const instance = await env.NATIVE_WORKFLOW.create({
      id: requestedRunId,
      params: { deliveryId, topic },
    });
    await env.DB.prepare(
      `INSERT INTO durability_benchmark_native_approvals
         (run_id, status, created_at) VALUES (?, 'pending', datetime('now'))`,
    )
      .bind(requestedRunId)
      .run();
    return json(
      { authority: 'native', runId: instance.id, deduplicated: false },
      201,
    );
  }

  if (
    request.method === 'GET' &&
    segments[0] === 'native' &&
    segments[1] === 'runs' &&
    segments.length === 3
  ) {
    return nativeStatus(env, stringField({ runId: segments[2] }, 'runId'));
  }

  if (
    request.method === 'POST' &&
    segments[0] === 'native' &&
    segments[1] === 'runs' &&
    segments[3] === 'approve' &&
    segments.length === 4
  ) {
    const runId = stringField({ runId: segments[2] }, 'runId');
    const result = await env.DB.prepare(
      `UPDATE durability_benchmark_native_approvals
       SET status = 'approved', decided_by = ?, decided_at = datetime('now')
       WHERE run_id = ? AND status = 'pending'`,
    )
      .bind(REVIEWER.id, runId)
      .run();
    if (result.meta.changes !== 1) {
      return json(
        { error: 'native approval is absent or already decided' },
        409,
      );
    }
    await (await env.NATIVE_WORKFLOW.get(runId)).sendEvent({
      type: 'approval',
      payload: { approved: true, decidedBy: REVIEWER.id },
    });
    return json({ authority: 'native', approved: true });
  }

  if (
    request.method === 'POST' &&
    segments[0] === 'native' &&
    segments[1] === 'runs' &&
    segments[3] === 'event' &&
    segments.length === 4
  ) {
    const runId = stringField({ runId: segments[2] }, 'runId');
    await (await env.NATIVE_WORKFLOW.get(runId)).sendEvent({
      type: 'approval',
      payload: { approved: true, decidedBy: REVIEWER.id },
    });
    return json({ authority: 'native', delivered: true });
  }

  return json({ error: 'not found' }, 404);
}

async function effects(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const authority = url.searchParams.get('authority');
  const runId = url.searchParams.get('runId');
  if (
    (authority !== 'flowsafe' && authority !== 'native') ||
    !runId ||
    !isPathSafeId(runId)
  ) {
    return json({ error: 'authority and path-safe runId are required' }, 400);
  }
  const rows = await env.DB.prepare(
    `SELECT authority, run_id, effect_key, delivery_id, topic, created_at
     FROM durability_benchmark_effects
     WHERE authority = ? AND run_id = ? ORDER BY created_at`,
  )
    .bind(authority, runId)
    .all();
  return json({ effects: rows.results });
}

function errorResponse(error: unknown): Response {
  const name = error instanceof Error ? error.name : '';
  const message = error instanceof Error ? error.message : String(error);
  if (
    name === 'ApprovalConflictError' ||
    name === 'RunNotSuspendedError' ||
    name === 'RunAlreadyExistsError' ||
    message.includes("not 'suspended'")
  ) {
    return json({ error: message }, 409);
  }
  if (name === 'UnknownApprovalError' || name === 'UnknownRunError') {
    return json({ error: message }, 404);
  }
  return json({ error: message }, 500);
}

const handler: ExportedHandler<Env> = {
  async fetch(request, env): Promise<Response> {
    try {
      await ensureDeploymentIdentityBindings(env);
      await ensureBenchmarkSchema(env.DB);
      const url = new URL(request.url);
      if (url.pathname === '/healthz') return json({ ok: true });
      if (url.pathname === '/effects') {
        return await effects(request, env);
      }
      if (url.pathname.startsWith('/flowsafe/')) {
        return await handleFlowsafe(request, env);
      }
      if (url.pathname.startsWith('/native/')) {
        return await handleNative(request, env);
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      return errorResponse(error);
    }
  },
};

export default handler;
