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
  DurableObjectState,
  ExecutionContext,
  ExportedHandler,
} from '@cloudflare/workers-types';
import { Agent } from '@mastra/core/agent';
import type {
  MastraModelConfig,
  OpenAICompatibleConfig,
} from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import { readObjective, resolveGoalStore } from '@mastra/core/tools';
import { createGuardedAgent } from '@proofoftech/breakwater/agent';
import {
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
} from '@proofoftech/breakwater/audit';
import {
  ConnectorPolicyError,
  createConnector,
} from '@proofoftech/breakwater/connector-sdk';
import { ACTOR_CONTEXT_KEY } from '@proofoftech/breakwater/rbac';
import { z } from 'zod';
import {
  type AgentMeta,
  type AgentModule,
  createAgentApprovalResumer,
  createAgentRouter,
  createAgentThreadTopology,
  createThreadAgentHost,
  type ThreadAgentHost,
} from '../src/agent-host/index.js';
import { readAgentThreadBinding } from '../src/agent-runner/index.js';
import {
  type ApprovalActor,
  type ApprovalDecision,
  type ApprovalRecord,
  ApprovalService,
  type ApprovalStreamSink,
  approvalGrantProvider,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  createApprovalRouter,
  createTenantResolver,
  D1ApprovalStoreFactory,
  defaultResumeData,
  type TenantBoundApprovalStore,
  type TenantContext,
} from '../src/approval-api/index.js';

import {
  BackgroundTaskHost,
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
} from '../src/background-tasks/index.js';
import {
  createD1Storage,
  createHostPubSub,
  DurableObjectRunner,
  HubDurableObject,
  type InitResult,
  init,
  mintResourceId,
  mintSaltedId,
  mintThreadId,
  type RunnerRuntime,
  ThreadDurableObject,
  type ThreadScope,
  tenantOwnsMemoryId,
  tenantOwnsSaltedId,
} from '../src/do-runner/index.js';
import {
  createObjectiveRouter,
  type ObjectiveAuditEvent,
} from '../src/goals/index.js';
import {
  bearerActorAuthenticator,
  createHubTopology,
  createRunRouter,
  createStreamRouter,
  createThreadTopology,
  doSummary,
  staticTokenVerifier,
  type WorkflowMeta,
} from '../src/host-kit/index.js';
import {
  createScheduleTick,
  D1SchedulesStorage,
} from '../src/schedules/index.js';
import {
  createWebhookRouter,
  createWebhookSignalProvider,
  D1SubscriptionStoreFactory,
  githubSignalProvider,
  type SignalProviderAdapter,
  type SignalProviderAuditEvent,
  SignalProviderHost,
  type SignalProviderHostWiring,
} from '../src/signal-providers/index.js';
import {
  createSignalStorageDomains,
  createThreadSignalRoutes,
  D1NotificationsStorage,
  D1ThreadStateStorage,
} from '../src/signals/index.js';

interface Env {
  DB: D1Database;
  RUNNER: DurableObjectNamespace;
  /** Per-tenant live-stream hub DO (idFromName(tenantId)); see DemoHub. */
  HUB: DurableObjectNamespace;
  /** Per-thread agent-loop / signal DO (idFromName(threadId)); see DemoThread (Track C). */
  THREAD: DurableObjectNamespace;
  /** Per-tenant signal-provider host DO (idFromName(tenantId)); see DemoSignalProviderHost (Track E). */
  SIGNAL_PROVIDER_HOST: DurableObjectNamespace;
  BACKGROUND_TASKS: DurableObjectNamespace;
  SPIKE_LLM_MODEL_ID?: string;
  SPIKE_LLM_API_KEY?: string;
  SPIKE_LLM_BASE_URL?: string;
  /**
   * GitHub webhook signing secret (Track E). A LOCAL-ONLY spike fixture set in
   * spike/wrangler.jsonc `vars`; spike-verify.mjs re-passes it via `--var` and
   * signs its webhook probes with it. Absent ⇒ the github webhook route is absent
   * (byte-identical). Never a real secret; this worker is never deployed.
   */
  GITHUB_WEBHOOK_SECRET?: string;
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

const SPIKE_AGENT_ID = 'spike-guarded-agent';
const SPIKE_WRITE_CONNECTOR_ID = 'spike_recordWrite';
const SPIKE_AGENT_META = {
  id: SPIKE_AGENT_ID,
  title: 'Spike guarded agent',
  description:
    'Calls one approval-gated write connector through the catalog-driven durable host.',
  allowedRoles: ['admin', 'operator'],
} as const satisfies AgentMeta;

const modelUsage = {
  inputTokens: 1,
  outputTokens: 1,
  totalTokens: 2,
};

const agentProbeTableInitializers = new WeakMap<D1Database, Promise<void>>();

function ensureAgentProbeTables(db: D1Database): Promise<void> {
  let initialized = agentProbeTableInitializers.get(db);
  if (!initialized) {
    initialized = db
      .batch([
        db.prepare(
          `CREATE TABLE IF NOT EXISTS spike_agent_model_calls (
             call_id TEXT PRIMARY KEY,
             created_at TEXT NOT NULL
           )`,
        ),
        db.prepare(
          `CREATE TABLE IF NOT EXISTS spike_agent_connector_calls (
             call_id TEXT PRIMARY KEY,
             tool_call_id TEXT NOT NULL,
             run_id TEXT NOT NULL,
             thread_id TEXT NOT NULL,
             resource_id TEXT NOT NULL,
             actor_id TEXT NOT NULL,
             actor_role TEXT NOT NULL,
             tenant_id TEXT NOT NULL,
             entry_path TEXT NOT NULL,
             created_at TEXT NOT NULL
           )`,
        ),
        db.prepare(
          `CREATE TABLE IF NOT EXISTS spike_agent_side_effects (
             tool_call_id TEXT PRIMARY KEY,
             thread_id TEXT NOT NULL,
             resource_id TEXT NOT NULL,
             created_at TEXT NOT NULL
           )`,
        ),
      ])
      .then(() => undefined)
      .catch((error: unknown) => {
        agentProbeTableInitializers.delete(db);
        throw error;
      });
    agentProbeTableInitializers.set(db, initialized);
  }
  return initialized;
}

function deterministicToolModel(db: D1Database): MastraModelConfig {
  const toolCall = () => ({
    type: 'tool-call' as const,
    toolCallId: `call_${crypto.randomUUID()}`,
    toolName: SPIKE_WRITE_CONNECTOR_ID,
    input: JSON.stringify({ value: 'phase-a' }),
  });
  const recordModelCall = async () => {
    await ensureAgentProbeTables(db);
    await db
      .prepare(
        `INSERT INTO spike_agent_model_calls (call_id, created_at)
         VALUES (?, ?)`,
      )
      .bind(crypto.randomUUID(), new Date().toISOString())
      .run();
  };
  return {
    specificationVersion: 'v2',
    provider: 'flowsafe-spike',
    modelId: 'deterministic-tool-call',
    supportedUrls: {},
    doGenerate: async () => {
      await recordModelCall();
      return {
        content: [toolCall()],
        finishReason: 'tool-calls',
        usage: modelUsage,
        warnings: [],
      };
    },
    doStream: async () => {
      await recordModelCall();
      const call = toolCall();
      return {
        stream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'stream-start', warnings: [] });
            controller.enqueue(call);
            controller.enqueue({
              type: 'finish',
              finishReason: 'tool-calls',
              usage: modelUsage,
            });
            controller.close();
          },
        }),
      };
    },
  };
}

function agentModel(env: Env): MastraModelConfig {
  const modelId = env.SPIKE_LLM_MODEL_ID;
  const apiKey = env.SPIKE_LLM_API_KEY;
  if (!modelId || !apiKey) return deterministicToolModel(env.DB);
  if (!modelId.includes('/')) {
    throw new Error('SPIKE_LLM_MODEL_ID must use provider/model form');
  }
  const model: OpenAICompatibleConfig = {
    id: modelId as `${string}/${string}`,
    apiKey,
    ...(env.SPIKE_LLM_BASE_URL ? { url: env.SPIKE_LLM_BASE_URL } : {}),
  };
  return model;
}

function createSpikeAgentModule(env: Env, audit: AuditLogger): AgentModule {
  const write = createConnector({
    id: SPIKE_WRITE_CONNECTOR_ID,
    description: 'Record the required guarded-agent write exactly once',
    inputSchema: z.object({ value: z.string().optional() }),
    outputSchema: z.object({ recorded: z.boolean() }),
    execute: async (_input, context) => {
      const toolCallId = context.agent?.toolCallId;
      const threadId = context.agent?.threadId;
      const resourceId = context.agent?.resourceId;
      const actor = context.requestContext?.get(ACTOR_CONTEXT_KEY) as
        | { id?: unknown; role?: unknown }
        | undefined;
      const correlation = context.requestContext?.get(
        AGENT_AUDIT_CONTEXT_KEY,
      ) as
        | {
            tenantId?: unknown;
            runId?: unknown;
            threadId?: unknown;
            resourceId?: unknown;
            entryPath?: unknown;
          }
        | undefined;
      if (
        !toolCallId ||
        !threadId ||
        !resourceId ||
        typeof actor?.id !== 'string' ||
        typeof actor.role !== 'string' ||
        typeof correlation?.tenantId !== 'string' ||
        typeof correlation.runId !== 'string' ||
        correlation.threadId !== threadId ||
        correlation.resourceId !== resourceId ||
        typeof correlation.entryPath !== 'string'
      ) {
        throw new Error(
          'guarded connector is missing its trusted principal or durable binding',
        );
      }
      await ensureAgentProbeTables(env.DB);
      const createdAt = new Date().toISOString();
      await env.DB.batch([
        env.DB.prepare(
          `INSERT INTO spike_agent_connector_calls
             (call_id, tool_call_id, run_id, thread_id, resource_id,
              actor_id, actor_role, tenant_id, entry_path, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).bind(
          crypto.randomUUID(),
          toolCallId,
          correlation.runId,
          threadId,
          resourceId,
          actor.id,
          actor.role,
          correlation.tenantId,
          correlation.entryPath,
          createdAt,
        ),
        env.DB.prepare(
          `INSERT OR IGNORE INTO spike_agent_side_effects
             (tool_call_id, thread_id, resource_id, created_at)
             VALUES (?, ?, ?, ?)`,
        ).bind(toolCallId, threadId, resourceId, createdAt),
      ]);
      return { recorded: true };
    },
    permissions: { sideEffect: 'write' },
    policies: {
      writePermissions: { requireApproval: [SPIKE_WRITE_CONNECTOR_ID] },
      audit,
    },
  });
  return {
    meta: SPIKE_AGENT_META,
    agent: createGuardedAgent({
      id: SPIKE_AGENT_ID,
      name: 'Flowsafe Phase A spike agent',
      instructions:
        'Call spike_recordWrite exactly once, then stop after the tool result.',
      model: agentModel(env),
      tools: { [SPIKE_WRITE_CONNECTOR_ID]: write },
      allowedRoles: SPIKE_AGENT_META.allowedRoles,
      policies: [],
      audit,
      maxSteps: 1,
      toolChoice: 'required',
    }),
  };
}

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
  ['other-operator', { id: 'oliver', role: 'operator', tenantId: 'other' }],
  ['other-reviewer', { id: 'ruth', role: 'reviewer', tenantId: 'other' }],
  ['other-viewer', { id: 'vera', role: 'viewer', tenantId: 'other' }],
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

  // Track D (M-006) D-S2 probe: a workflow the schedule tick fires through the DO.
  // Its ONE step ECHOES what reached the leg, so the spike can assert the
  // stored-context barrier. The FORGED connector id planted in the schedule ROW's
  // stored requestContext must NOT appear in the leg's grant list: the topology
  // start carries only inputData (stored context dropped), and the DO's own
  // approvalGrantProvider mints `breakwater.approvedConnectors` from APPROVED
  // records (an EMPTY [] for this fresh run) which WINS via #requestContextFor's
  // last-spread — two independent reasons the forged value can never become a
  // grant. `reservedLeaked` checks the VALUE (does the grant include the forged
  // id), NOT mere key presence (the key is legitimately present as []). The
  // runtime-derived scope keys ARE present (it ran through RunnerRuntime), and the
  // benign stored key is absent (stored context dropped on the DO path).
  const schedEcho = createStep({
    id: 'echo',
    inputSchema: z.object({}),
    outputSchema: z.object({
      reservedLeaked: z.boolean(),
      workflowScopePresent: z.boolean(),
      isolationScopePresent: z.boolean(),
      customPresent: z.boolean(),
    }),
    execute: async ({ requestContext }) => {
      const grants = requestContext.get(BREAKWATER_APPROVED_CONNECTORS_KEY);
      return {
        reservedLeaked:
          Array.isArray(grants) && grants.includes('forged-connector'),
        workflowScopePresent:
          requestContext.get('breakwater.workflowScope') !== undefined,
        isolationScopePresent:
          requestContext.get('breakwater.isolationScope') !== undefined,
        customPresent: requestContext.get('sched.note') !== undefined,
      };
    },
  });
  createWorkflow({
    id: 'sched-echo',
    inputSchema: z.object({}),
    outputSchema: z.object({
      reservedLeaked: z.boolean(),
      workflowScopePresent: z.boolean(),
      isolationScopePresent: z.boolean(),
      customPresent: z.boolean(),
    }),
  })
    .then(schedEcho)
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

/** One execution-capable background-task manager per tenant DO. */
export class DemoBackgroundTasks {
  readonly #state: DurableObjectState;
  readonly #env: Env;
  #host?: Promise<BackgroundTaskHost>;

  constructor(state: DurableObjectState, env: Env) {
    this.#state = state;
    this.#env = env;
  }

  #boot(): Promise<BackgroundTaskHost> {
    if (!this.#host) {
      this.#host = (async () => {
        const tenantId = this.#state.id.name;
        if (!tenantId)
          throw new Error('background-task DO requires idFromName(tenantId)');
        const pubsub = createHostPubSub();
        const storage = createD1Storage({
          binding: this.#env.DB as unknown as never,
          domains: createBackgroundTaskD1Domains({
            binding: this.#env.DB as unknown as never,
            tenantId,
          }),
        });
        await storage.init();
        const mastra = new Mastra({ storage, pubsub });
        const host = new BackgroundTaskHost({
          mastra,
          pubsub,
          execution: { tenantId },
          executors: {
            bgProbe: {
              execute: async (args) => {
                if (typeof args.delayMs === 'number' && args.delayMs > 0) {
                  await new Promise((resolve) =>
                    setTimeout(resolve, args.delayMs as number),
                  );
                }
                return { executed: true, args };
              },
            },
          },
        });
        await host.boot();
        return host;
      })();
    }
    return this.#host;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const host = await this.#boot();
      const url = new URL(request.url);
      if (request.method === 'POST' && url.pathname === '/enqueue') {
        const body = (await request.json()) as {
          runId?: string;
          delayMs?: number;
        };
        const tenantId = this.#state.id.name;
        if (
          !tenantId ||
          !body.runId ||
          !tenantOwnsMemoryId(tenantId, body.runId)
        ) {
          return json({ error: 'tenant-owned parent runId required' }, 404);
        }
        const queued = await host.manager.enqueue({
          runId: body.runId,
          toolName: 'bgProbe',
          toolCallId: `call-${crypto.randomUUID()}`,
          args: {
            value: 'durable',
            ...(body.delayMs ? { delayMs: body.delayMs } : {}),
          },
          agentId: 'background-probe-agent',
          maxRetries: body.delayMs ? 1 : 0,
        });
        return json({ taskId: queued.task.id, status: queued.task.status });
      }
      if (request.method === 'GET' && url.pathname.startsWith('/task/')) {
        const task = await host.manager.getTask(
          decodeURIComponent(url.pathname.slice('/task/'.length)),
        );
        return task ? json(task) : json({ error: 'not found' }, 404);
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      return json(
        { error: error instanceof Error ? error.message : String(error) },
        500,
      );
    }
  }

  async alarm(): Promise<void> {
    await (await this.#boot()).onAlarm();
  }
}

/**
 * Track C (M-004): the per-thread signal DO (idFromName(threadId), tenant-minted,
 * so id.name carries the tenant). `build()` wires init() with the ONE host pubsub
 * identity (createHostPubSub) — the affinity carrier: core keys its in-process
 * signal registry by the pubsub instance, so a send only drains into an active
 * loop when both share this isolate's ONE pubsub (DL-002). `route()` composes the
 * production Track C signal routes plus a spike-only C-S2 affinity probe. The
 * base class asserts the request's authenticated tenant against the name's prefix
 * BEFORE route() runs, so a forged cross-tenant request never reaches here (C-S4).
 */
export class DemoThread extends ThreadDurableObject<Env> {
  #signalAgent?: Agent;
  #storage?: ReturnType<typeof createD1Storage>;
  #agentHost?: ThreadAgentHost;
  #approvalService?: ApprovalService;
  #threadInit?: InitResult;

  protected build(env: Env): InitResult {
    const storage = createD1Storage({
      binding: env.DB as unknown as never,
      domains: createSignalStorageDomains(env.DB as unknown as never),
    });
    this.#storage = storage;
    const approvals = approvalStoreFactory(env.DB).forTenant(this.tenantId);
    const audit = new AuditLogger({
      sink: (event) => {
        console.log(JSON.stringify(event));
      },
    });
    const agentHost = createThreadAgentHost({
      buildModules: () => [createSpikeAgentModule(this.env, audit)],
      storage: () => storage,
      stateStorage: () => {
        if (!this.state?.storage) {
          throw new Error('thread Durable Object storage is unavailable');
        }
        return this.state.storage;
      },
      approvalService: () => {
        this.#approvalService ??= new ApprovalService({
          store: approvals,
          stream: (event) => createHubTopology(this.env.HUB).publish(event),
        });
        return this.#approvalService;
      },
      systemActorId: SYSTEM_ACTOR_ID,
      audit: (event) => audit.record(event),
    });
    this.#agentHost = agentHost;
    const threadInit = init(
      { storage },
      {
        pubsub: createHostPubSub(),
        requestContextForRun: agentHost.requestContextForRun(
          approvalGrantProvider(approvals),
        ),
      },
    );
    this.#threadInit = threadInit;
    return threadInit;
  }

  #host(): ThreadAgentHost {
    if (!this.#agentHost) {
      throw new Error('thread agent host is not initialized');
    }
    return this.#agentHost;
  }

  #initResult(): InitResult {
    if (!this.#threadInit) {
      throw new Error('thread agent host is not initialized');
    }
    return this.#threadInit;
  }

  // Compatibility-only affinity and notification probes retain a raw,
  // non-driven agent on unbound threads. Catalog-bound threads always resolve
  // through the guarded host below.
  #getSignalAgent(scope: ThreadScope): Agent {
    if (!this.#signalAgent) {
      const bare = new Agent({
        id: 'demo-thread-signal-agent',
        name: 'demo-thread-signal-agent',
        instructions: 'signal affinity + notification delivery target',
        model: 'openai/gpt-4o-mini',
      });
      const mastra = new Mastra({
        storage: this.#storage,
        agents: { 'demo-thread-signal-agent': bare },
      });
      this.#signalAgent = mastra.getAgent('demo-thread-signal-agent');
    }
    if (scope.init.pubsub) {
      this.#signalAgent.__setPubSub(scope.init.pubsub);
    }
    return this.#signalAgent;
  }

  #signalRoutes = createThreadSignalRoutes({
    resolveAgent: async (scope, agentId, entryPath) => {
      if (!this.state?.storage) {
        throw new Error('thread Durable Object storage is unavailable');
      }
      const binding = await readAgentThreadBinding(this.state.storage);
      if (binding || agentId) {
        return (
          await this.#host().resolveBoundAgent(scope, {
            agentId,
            entryPath,
          })
        ).durableAgent as unknown as Agent;
      }
      return this.#getSignalAgent(scope);
    },
    resolveResourceId: (scope) => mintResourceId(scope.tenantId, 'demo-thread'),
    resolveNotificationsStorage: async () => {
      const store = await this.#storage?.getStore('notifications');
      if (!store) throw new Error('notifications storage unavailable');
      return store;
    },
    startIdleRun: async (input) => {
      const scope = {
        threadId: input.threadId,
        tenantId: this.tenantId,
        actor: input.actor,
        requestedBy: input.requestedBy,
        init: this.#initResult(),
      };
      const result = await this.#host().start(scope, {
        agentId: input.agent.id,
        threadId: input.threadId,
        runId: input.runId,
        resourceId:
          input.resourceId ?? mintResourceId(scope.tenantId, scope.threadId),
        messages:
          input.message !== undefined
            ? typeof input.message === 'string'
              ? input.message
              : Array.isArray(input.message)
                ? [{ role: 'user', content: input.message }]
                : [{ role: 'user', content: input.message.contents }]
            : input.signal !== undefined
              ? [{ role: 'user', content: input.signal.contents }]
              : 'Perform the required write now.',
        entryPath: input.entryPath,
      });
      return { runId: result.runId };
    },
  });

  protected async route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response> {
    const url = new URL(request.url);
    const agentResponse = await this.#host().route(request, scope);
    if (agentResponse) return agentResponse;
    if (request.method === 'POST' && url.pathname === '/probe/affinity') {
      return this.#affinityProbe(scope);
    }
    const signalResponse = await this.#signalRoutes(request, scope);
    if (signalResponse) return signalResponse;
    return json({ error: 'not found' }, 404);
  }

  /**
   * C-S2 structural proof (no LLM): a sendSignal into an ACTIVE (reserved) run on
   * THIS isolate drains IN-PROCESS via core's pubsub-keyed registry.
   *   1. idle-wake RESERVES a run synchronously (core sets activeThreadRunIds
   *      before the async stream setup); contain the async setup rejection.
   *   2. the thread now reads ACTIVE in this isolate.
   *   3. a second signal resolves to action 'deliver' with the reserved runId —
   *      it JOINED the in-process run, not persisted to storage (idle) or
   *      discarded. That distinction IS the affinity.
   */
  async #affinityProbe(scope: ThreadScope): Promise<Response> {
    const agent = this.#getSignalAgent(scope);
    const resourceId = mintResourceId(scope.tenantId, 'affinity-probe');
    const target = { threadId: scope.threadId, resourceId };
    const reserve = agent.sendSignal(
      { type: 'user', contents: 'wake' },
      { ...target, ifIdle: { behavior: 'wake' } },
    );
    void reserve.accepted.catch(() => {});
    const activeRunId = agent.getActiveThreadRunId(target);
    const deliver = agent.sendSignal(
      { type: 'reactive', contents: 'follow-up' },
      { ...target, ifActive: { behavior: 'deliver' } },
    );
    const decision = await deliver.accepted;
    return json({
      threadId: scope.threadId,
      tenantId: scope.tenantId,
      activeRunId,
      decision,
    });
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

// --- Track E (M-007) signal providers --------------------------------------
// The GitHub webhook reference provider + a DETERMINISTIC poll provider (no
// external HTTP — it emits one notification per subscription, for the E-S3
// alarm/rehydration proof). One subscription-store factory + one webhook router
// per isolate (the router's forgery-audit window and the DDL memo persist).

const subscriptionFactories = new WeakMap<
  D1Database,
  D1SubscriptionStoreFactory
>();
function subscriptionFactory(db: D1Database): D1SubscriptionStoreFactory {
  let factory = subscriptionFactories.get(db);
  if (!factory) {
    factory = new D1SubscriptionStoreFactory(db as unknown as never);
    subscriptionFactories.set(db, factory);
  }
  return factory;
}

const spikePollProvider: SignalProviderAdapter = createWebhookSignalProvider({
  id: 'spike-poller',
  pollInterval: 60_000,
  buildNotification: (_payload, subscription) => ({
    source: 'spike-poller',
    kind: 'poll',
    summary: `polled ${subscription.externalResourceId}`,
  }),
  pollForDeliveries: async (subscriptions) =>
    subscriptions.map((subscription) => ({
      subscription,
      notification: {
        source: 'spike-poller',
        kind: 'poll',
        summary: `polled ${subscription.externalResourceId}`,
      },
    })),
});

function spikeProviders(): SignalProviderAdapter[] {
  return [githubSignalProvider(), spikePollProvider];
}

/**
 * The per-tenant provider host DO (idFromName(tenantId), Track E). `build()`
 * binds the subscription store to this instance's tenant, the thread topology
 * (delivery), and the spike's providers. The base class drives the alarm poll +
 * the `/poll` probe route.
 */
export class DemoSignalProviderHost extends SignalProviderHost<Env> {
  protected build(env: Env, tenantId: string): SignalProviderHostWiring {
    return {
      store: subscriptionFactory(env.DB).forTenant(tenantId),
      topology: createThreadTopology(env.THREAD),
      providers: spikeProviders(),
    };
  }
}

// The webhook audit stream the /sigp/audit probe reads (module-level so the
// forgery-audit bound persists across requests in the isolate).
const sigpAudit: SignalProviderAuditEvent[] = [];
const webhookRouters = new WeakMap<
  D1Database,
  ReturnType<typeof createWebhookRouter>
>();
function webhookRouter(env: Env): ReturnType<typeof createWebhookRouter> {
  let router = webhookRouters.get(env.DB);
  if (!router) {
    router = createWebhookRouter({
      providers: Object.fromEntries(spikeProviders().map((p) => [p.id, p])),
      subscriptions: subscriptionFactory(env.DB).system(),
      topology: createThreadTopology(env.THREAD),
      // Only GitHub is a real webhook provider here; the poll provider signs nothing.
      secretForProvider: (id) =>
        id === 'github' ? env.GITHUB_WEBHOOK_SECRET : undefined,
      audit: (event) => {
        sigpAudit.push(event);
      },
    });
    webhookRouters.set(env.DB, router);
  }
  return router;
}

const SIGP_THREAD_ID = mintThreadId('spike', () => 'sigp'); // 'spike_sigp'
// Matches DemoThread.resolveResourceId (mintResourceId(tenant,'demo-thread')), so
// delivery keys the inbox on the thread's owner.
const SIGP_RESOURCE_ID = mintResourceId('spike', 'demo-thread');
const SIGP_FOREIGN_THREAD_ID = 'other_victim'; // NOT owned by tenant 'spike'

// LOCAL-ONLY Track E probes (tenant 'spike'), driven by spike-verify.mjs:
//  E-S2 (forged): a bad-signature webhook is rejected BEFORE parse + audited.
//  E-S1 (delivery): subscribe -> signed webhook -> notification lands in the inbox.
//  E-S3 (rehydration): after a kill+restart, the host DO's /poll rehydrates
//    subscriptions from D1 and fires poll delivery (the in-memory-lost proof).
//  cross-tenant: a valid webhook for a TAMPERED row (foreign thread) delivers to none.
async function handleSignalProviderProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/sigp/')) return null;
  const store = subscriptionFactory(env.DB).forTenant('spike');

  if (request.method === 'POST' && path === '/sigp/subscribe') {
    await store.subscribe({
      providerId: 'github',
      externalResourceId: 'github:acme/repo',
      threadId: SIGP_THREAD_ID,
      resourceId: SIGP_RESOURCE_ID,
    });
    await store.subscribe({
      providerId: 'spike-poller',
      externalResourceId: 'poller:demo',
      threadId: SIGP_THREAD_ID,
      resourceId: SIGP_RESOURCE_ID,
    });
    return json({ subscribed: true, threadId: SIGP_THREAD_ID });
  }

  if (request.method === 'POST' && path === '/sigp/subscribe-foreign') {
    // A tampered row: tenant 'spike' but a thread it does not own — the topology
    // must 404 any delivery to it (cross-tenant fail-closed).
    await store.subscribe({
      providerId: 'github',
      externalResourceId: 'github:cross/repo',
      threadId: SIGP_FOREIGN_THREAD_ID,
      resourceId: SIGP_RESOURCE_ID,
    });
    return json({ tampered: true });
  }

  if (request.method === 'GET' && path === '/sigp/audit') {
    return json({ events: sigpAudit });
  }

  if (request.method === 'POST' && path === '/sigp/poll') {
    // E-S3 direct-alarm probe: drive the host DO's /poll (deterministic — no
    // dependency on wrangler's alarm timer). A FRESH post-restart host rehydrates
    // its subscriptions from D1 here.
    const stub = env.SIGNAL_PROVIDER_HOST.get(
      env.SIGNAL_PROVIDER_HOST.idFromName('spike'),
    );
    const response = await stub.fetch('http://host/poll', { method: 'POST' });
    return json({ status: response.status, result: await response.json() });
  }

  if (request.method === 'GET' && path === '/sigp/notifications') {
    const threadId =
      new URL(request.url).searchParams.get('threadId') ?? SIGP_THREAD_ID;
    const notifications = new D1NotificationsStorage(
      env.DB as unknown as never,
    );
    const inbox = await notifications.listNotifications({ threadId });
    return json({ count: inbox.length, sources: inbox.map((n) => n.source) });
  }

  return null;
}

function runStub(
  env: Env,
  workflowId: string,
  runId: string,
): ReturnType<DurableObjectNamespace['get']> {
  return env.RUNNER.get(env.RUNNER.idFromName(`${workflowId}:${runId}`));
}

async function handleLiveAgentRoute(
  request: Request,
  env: Env,
  resolve: ReturnType<typeof createTenantResolver>,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/agent/live/effects') return null;
  const tenant = await resolve(request);
  if (!tenant) return json({ error: 'unauthorized' }, 401);
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  await ensureAgentProbeTables(env.DB);
  const requestedRunId = url.searchParams.get('runId');
  if (requestedRunId && !tenant.ownsRun(requestedRunId)) {
    return json({ error: 'run not found' }, 404);
  }
  const statement = requestedRunId
    ? env.DB.prepare(
        `SELECT c.call_id, c.tool_call_id, c.run_id, c.thread_id,
                  c.resource_id, c.actor_id, c.actor_role, c.tenant_id,
                  c.entry_path,
                  CASE WHEN EXISTS (
                    SELECT 1 FROM spike_agent_side_effects s
                    WHERE s.tool_call_id = c.tool_call_id
                  ) THEN 1 ELSE 0 END AS recorded
           FROM spike_agent_connector_calls c
           WHERE c.tenant_id = ? AND c.run_id = ?
           ORDER BY c.created_at`,
      ).bind(tenant.tenantId, requestedRunId)
    : env.DB.prepare(
        `SELECT c.call_id, c.tool_call_id, c.run_id, c.thread_id,
                  c.resource_id, c.actor_id, c.actor_role, c.tenant_id,
                  c.entry_path,
                  CASE WHEN EXISTS (
                    SELECT 1 FROM spike_agent_side_effects s
                    WHERE s.tool_call_id = c.tool_call_id
                  ) THEN 1 ELSE 0 END AS recorded
           FROM spike_agent_connector_calls c
           WHERE c.tenant_id = ?
           ORDER BY c.created_at`,
      ).bind(tenant.tenantId);
  const calls = await statement.all<{
    call_id: string;
    tool_call_id: string;
    run_id: string;
    thread_id: string;
    resource_id: string;
    actor_id: string;
    actor_role: string;
    tenant_id: string;
    entry_path: string;
    recorded: number;
  }>();
  const modelCalls = await env.DB.prepare(
    'SELECT COUNT(*) AS count FROM spike_agent_model_calls',
  ).first<{ count: number }>();
  return json({
    connectorCalls: calls.results.length,
    effects: calls.results.filter((call) => call.recorded === 1).length,
    modelCalls: modelCalls?.count ?? 0,
    calls: calls.results,
  });
}

function tenantContextForActor(actor: ApprovalActor, env: Env): TenantContext {
  let service: ApprovalService | undefined;
  return {
    actor,
    tenantId: actor.tenantId,
    service: () => {
      service ??= new ApprovalService({
        store: approvalStoreFactory(env.DB).forTenant(actor.tenantId),
      });
      return service;
    },
    newRunId: () =>
      mintSaltedId(
        actor.tenantId,
        () => crypto.randomUUID(),
        'tenantContextForActor.newRunId',
      ),
    ownsRun: (runId) => tenantOwnsSaltedId(actor.tenantId, runId),
    newThreadId: () => mintThreadId(actor.tenantId),
    newResourceId: (resourceKey) => mintResourceId(actor.tenantId, resourceKey),
    ownsMemoryId: (id) => tenantOwnsMemoryId(actor.tenantId, id),
    canSelfDecide: () => false,
  };
}

// Worker-level approval service: shares the DO's D1 database; decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request).
function buildApprovalService(
  store: TenantBoundApprovalStore,
  env: Env,
  stream: ApprovalStreamSink | undefined,
): ApprovalService {
  const fallback = async (record: ApprovalRecord, decision: ApprovalDecision) =>
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
    );
  const resumeRun = createAgentApprovalResumer({
    fallback,
    agents: [SPIKE_AGENT_META],
    topology: createAgentThreadTopology(env.THREAD),
    tenantForActor: (actor) => tenantContextForActor(actor, env),
  });
  return new ApprovalService({
    store,
    defaultSlaSeconds: 15 * 60,
    // Live fan-out: every SUCCESSFUL mutation forwards to the tenant hub, which
    // fans it out to that tenant's open dashboard sockets. Fire-and-forget; the
    // caller keeps the publish alive with ctx.waitUntil (see fetch below).
    stream,
    resumeRun,
  });
}

// --- Track B (M-003) background-task probes --------------------------------
// LOCAL-ONLY worker-level probes (spike tenant 'spike'), driven by
// scripts/spike-verify.mjs:
//  - B-S3: createConnector rejects a smuggled `_background` arg and audits it.
//  - B-S2: the persistence-only compatibility path recovers a task through
//    public init() with the stock D1 domains.
//  - H2: DemoBackgroundTasks composes the serialized and tenant-scoped domains,
//    executes a body, and recovers a killed in-flight task after restart.
const BG_TASK_ID = 'spike_bs2';
const BG_RUN_ID = 'spike_bg-run';

function bgMastra(env: Env): Mastra {
  return new Mastra({
    storage: createD1Storage({ binding: env.DB as unknown as never }),
  });
}

async function handleBackgroundTaskProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;

  const executionStub = () =>
    env.BACKGROUND_TASKS.get(env.BACKGROUND_TASKS.idFromName('spike'));

  if (
    request.method === 'POST' &&
    (path === '/bg/execute' || path === '/bg/execute-recover')
  ) {
    const response = await executionStub().fetch('http://background/enqueue', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        runId: `spike_${crypto.randomUUID()}`,
        ...(path.endsWith('execute-recover') ? { delayMs: 5000 } : {}),
      }),
    });
    return new Response(response.body, response);
  }

  if (request.method === 'GET' && path.startsWith('/bg/execution-task/')) {
    const taskId = encodeURIComponent(
      decodeURIComponent(path.slice('/bg/execution-task/'.length)),
    );
    const response = await executionStub().fetch(
      `http://background/task/${taskId}`,
    );
    return new Response(response.body, response);
  }

  if (request.method === 'POST' && path === '/bg/background-reject') {
    const audit = new AuditLogger();
    const writer = createConnector({
      id: 'bg-probe-writer',
      description:
        'B-S3: a write connector must reject a smuggled _background arg',
      execute: async () => ({ ok: true }),
      permissions: { sideEffect: 'write' },
      policies: { audit },
    });
    let denied = false;
    let policy = '';
    try {
      await (writer.execute as (i: unknown, c: unknown) => Promise<unknown>)(
        { topic: 'x', _background: { enabled: true } },
        { requestContext: new RequestContext() },
      );
    } catch (error) {
      if (error instanceof ConnectorPolicyError) {
        denied = true;
        policy = error.policy;
      }
    }
    const audited = audit
      .events()
      .some(
        (event) =>
          event.decision === 'denied' &&
          (event.detail as { policy?: string } | undefined)?.policy ===
            'background',
      );
    return json({ denied, policy, audited });
  }

  if (request.method === 'POST' && path === '/bg/seed-stranded') {
    const store = await backgroundTasksStore(bgMastra(env));
    const now = new Date();
    await store.createTask({
      id: BG_TASK_ID,
      status: 'running',
      toolName: 'bgProbe',
      toolCallId: 'call-1',
      args: {},
      agentId: 'agent-1',
      runId: BG_RUN_ID,
      createdAt: now,
      startedAt: now,
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 300_000,
    });
    return json({ seeded: true });
  }

  if (request.method === 'POST' && path === '/bg/recover') {
    // A FRESH host: its init() fires the manager's recoverStaleTasks (the R-002
    // seam) over the SAME durable D1 the seed wrote to.
    const host = new BackgroundTaskHost({
      mastra: bgMastra(env),
      pubsub: createHostPubSub(),
      executors: {},
    });
    await host.boot();
    return json({ recovered: true });
  }

  if (request.method === 'GET' && path.startsWith('/bg/task/')) {
    const taskId = decodeURIComponent(path.slice('/bg/task/'.length));
    const store = await backgroundTasksStore(bgMastra(env));
    const task = await store.getTask(taskId);
    return json({ status: task?.status ?? null });
  }

  return null;
}

// --- Track C (M-004) signal probes -----------------------------------------
// LOCAL-ONLY worker-level probes (spike tenant 'spike'), driven by
// scripts/spike-verify.mjs:
//  - C-S2 (affinity): a signal into an ACTIVE loop on the thread DO drains
//    IN-PROCESS. Driven THROUGH createThreadTopology (the sanctioned reach — it
//    stamps the tenant header the DO authenticates on), so the send lands in the
//    isolate hosting the (reserved) loop and resolves to action 'deliver'.
//  - C-S4 (isolation): a cross-tenant foreign-threadId send fails CLOSED at BOTH
//    barriers — the topology ownership 404 (before the DO is addressed) and the
//    DO's own header 403 (a direct forged-header fetch).
async function handleSignalProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;

  // Local probe context; the topology overwrites both trusted headers from it.
  const ctxFor = (tenantId: string): TenantContext =>
    ({
      tenantId,
      actor: { id: 'signal-probe', role: 'operator', tenantId },
      ownsMemoryId: (id: string) => tenantOwnsMemoryId(tenantId, id),
    }) as unknown as TenantContext;

  const topology = createThreadTopology(env.THREAD);

  if (request.method === 'POST' && path === '/sig/affinity') {
    const threadId = mintThreadId('spike');
    const response = await topology.send(
      ctxFor('spike'),
      threadId,
      '/probe/affinity',
      { method: 'POST', body: '{}' },
    );
    const probe = (await response.json()) as Record<string, unknown>;
    return json({ status: response.status, ...probe });
  }

  if (request.method === 'POST' && path === '/sig/cross-tenant') {
    const ownerThreadId = mintThreadId('spike');
    // Barrier 1: tenant 'other' driving 'spike's threadId through the topology is
    // 404'd (ownership) BEFORE the DO is addressed — no wake, no oracle.
    let ownershipStatus = 0;
    try {
      const r = await topology.send(
        ctxFor('other'),
        ownerThreadId,
        '/probe/affinity',
        { method: 'POST', body: '{}' },
      );
      ownershipStatus = r.status;
    } catch (error) {
      ownershipStatus = (error as { status?: number }).status ?? -1;
    }
    // Barrier 2: a DIRECT forged-header fetch (bypassing the topology) is 403'd by
    // the DO's own #assertTenantIdentity (name tenant 'spike' != header 'other').
    const stub = env.THREAD.get(env.THREAD.idFromName(ownerThreadId));
    const forged = await stub.fetch('http://thread/probe/affinity', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-flowsafe-tenant': 'other',
      },
      body: '{}',
    });
    return json({ ownershipStatus, headerStatus: forged.status });
  }

  return null;
}

// --- Track F (M-005) goal probes -------------------------------------------
// LOCAL-ONLY worker-level probes (spike tenant 'spike'), driven by
// scripts/spike-verify.mjs:
//  - F-S1 (read path): an objective SET via the real createObjectiveRouter lands
//    in mastra_thread_state; the DURABLE goal-step read path (resolveGoalStore
//    -> readObjective over our COMPOSED storage) returns exactly what was written.
//  - F-S2 (eviction): the record is in D1, so it survives a dev-server restart —
//    spike-verify kills+restarts and reads it back (a DO-evicted goal still sees
//    it, because it lives in D1, not an in-process registry).
//  - F-S3 (fail-closed): a cross-tenant write to a foreign threadId is 404 +
//    audited; an over-cap maxRuns is rejected + audited.
const GOAL_THREAD_ID = 'spike_fsgoal';
const GOAL_OBJECTIVE = 'ship the launch checklist';

// The composed storage a durable host builds: the D1Store default with the Track
// C signal domains (notifications + thread-state) over it, so
// resolveGoalStore(mastra).getStore('threadState') resolves our D1ThreadStateStorage
// — the EXACT store the goal step reads through.
function goalMastra(env: Env): Mastra {
  return new Mastra({
    storage: createD1Storage({
      binding: env.DB as unknown as never,
      domains: createSignalStorageDomains(env.DB as unknown as never),
    }),
  });
}

async function handleGoalProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/goal/')) return null;

  // A partial resolver createObjectiveRouter accepts: the probe controls the
  // tenant + role (the real host uses createTenantResolver).
  const ctxFor = (
    tenantId: string,
    role: ApprovalActor['role'],
  ): TenantContext =>
    ({
      tenantId,
      actor: { id: 'goal-probe', role, tenantId },
      ownsMemoryId: (id: string) => tenantOwnsMemoryId(tenantId, id),
    }) as unknown as TenantContext;

  // The SAME D1 domain a durable host wires (createSignalStorageDomains uses it).
  const store = new D1ThreadStateStorage(env.DB as unknown as never);

  // Drive the REAL objective router (full P6-lite gate), capturing the audit.
  const driveSet = async (
    tenantId: string,
    body: Record<string, unknown>,
    maxRunsCap?: number,
  ): Promise<{ status: number; record: unknown; audited: string[] }> => {
    const events: ObjectiveAuditEvent[] = [];
    const router = createObjectiveRouter({
      resolve: async () => ctxFor(tenantId, 'operator'),
      store,
      audit: (event) => {
        events.push(event);
      },
      ...(maxRunsCap !== undefined ? { maxRunsCap } : {}),
    });
    const res = await router(
      new Request(`http://do/api/threads/${GOAL_THREAD_ID}/goal`, {
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      }),
    );
    const record =
      res && res.status === 200
        ? ((await res.json()) as { objective: unknown }).objective
        : null;
    return {
      status: res?.status ?? 0,
      record,
      audited: events.map((e) => `${e.outcome}:${e.reason ?? ''}`),
    };
  };

  if (request.method === 'POST' && path === '/goal/set') {
    return json(
      await driveSet('spike', { objective: GOAL_OBJECTIVE, maxRuns: 5 }),
    );
  }

  // The DURABLE goal-step read path over our COMPOSED storage: resolveGoalStore
  // must resolve the thread-state domain, and readObjective return the record.
  if (request.method === 'GET' && path === '/goal/read') {
    const resolved = await resolveGoalStore(goalMastra(env));
    const record = await readObjective(resolved, GOAL_THREAD_ID);
    return json({
      storeResolved: resolved !== undefined,
      record: record ?? null,
    });
  }

  // F-S3: tenant 'other' writing spike's threadId -> 404 + audited (no oracle).
  if (request.method === 'POST' && path === '/goal/cross-tenant') {
    return json(await driveSet('other', { objective: 'foreign write' }));
  }

  // F-S3: a maxRuns above the host cap -> 400 + audited.
  if (request.method === 'POST' && path === '/goal/over-cap') {
    return json(
      await driveSet('spike', { objective: 'too big', maxRuns: 999 }, 10),
    );
  }

  return null;
}

// --- Track D (M-006) schedule probes ---------------------------------------
// LOCAL-ONLY worker-level probes (spike tenant 'spike'), driven by
// scripts/spike-verify.mjs, over the flowsafe-owned D1 schedules domain:
//  - D-S1 (exactly-once): two CONCURRENT ticks over one due schedule fire EXACTLY
//    once (the CAS updateScheduleNextFire), one 'published' trigger row, nextFireAt
//    advanced once — on real workerd + D1.
//  - D-S2 (barrier + INV-1): a workflow-target schedule fires through the DO's
//    RunnerRuntime with a fresh INV-1 runId (`spike_<uuid>`); a reserved key
//    planted directly in the ROW's stored requestContext (a compromised row) is
//    ABSENT from the executing leg — the DO derives the leg context solely via
//    #requestContextFor (the topology start carries only inputData), so the DO's
//    own scope keys ARE present and the stored context is dropped fail-closed.
//  - D-S3: an agent target reaches the same per-thread runtime-driven loop and
//    approval bridge as an interactive start.
async function handleScheduleProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/sched/')) return null;
  const store = new D1SchedulesStorage(env.DB as unknown as never);
  const now = Date.now();

  if (request.method === 'POST' && path === '/sched/exactly-once') {
    const id = `schedule_${crypto.randomUUID()}`;
    await store.createSchedule({
      id,
      target: { type: 'workflow', workflowId: 'sched-echo', inputData: {} },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: now - 1000,
      createdAt: now,
      updatedAt: now,
      metadata: { tenantId: 'spike' },
    });
    // A recording start seam — D-S1 proves the CAS, not the run itself.
    let fireCount = 0;
    const tick = createScheduleTick({
      store,
      start: async (_wf, runId) => {
        fireCount += 1;
        return { runId };
      },
    });
    const [a, b] = await Promise.all([tick(), tick()]);
    const triggers = await store.listTriggers(id);
    const sched = await store.getSchedule(id);
    return json({
      fires: a.fired + b.fired,
      lost: a.lost + b.lost,
      fireCount,
      published: triggers.filter((t) => t.outcome === 'published').length,
      advanced: (sched?.nextFireAt ?? 0) > now - 1000,
    });
  }

  if (request.method === 'POST' && path === '/sched/barrier') {
    const id = `schedule_${crypto.randomUUID()}`;
    await store.createSchedule({
      id,
      target: {
        type: 'workflow',
        workflowId: 'sched-echo',
        inputData: {},
        // A reserved key planted in the ROW (simulating a compromised row that
        // bypassed the facade's create-time rejection) + a benign one.
        requestContext: {
          [BREAKWATER_APPROVED_CONNECTORS_KEY]: ['forged-connector'],
          'sched.note': 'benign',
        },
      },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: now - 1000,
      createdAt: now,
      updatedAt: now,
      metadata: { tenantId: 'spike' },
    });
    let firedRunId: string | undefined;
    let firedStatus: string | undefined;
    let firedLeg: unknown;
    const tick = createScheduleTick({
      store,
      // Fire through the DO topology (the production path) — the tick mints the
      // INV-1 runId and the DO runs sched-echo, echoing its leg context keys.
      start: async (workflowId, runId, inputData) => {
        firedRunId = runId;
        const summary = await doSummary(
          await runStub(env, workflowId, runId).fetch('http://do/runs', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ workflowId, runId, inputData }),
          }),
        );
        firedStatus = summary.status;
        firedLeg = summary.result;
        return summary;
      },
    });
    const result = await tick();
    return json({
      fired: result.fired,
      runId: firedRunId ?? null,
      status: firedStatus ?? null,
      leg: firedLeg ?? null,
    });
  }

  if (request.method === 'POST' && path === '/sched/agent') {
    const id = `schedule_${crypto.randomUUID()}`;
    const threadId = mintThreadId('spike');
    const resourceId = mintResourceId('spike', threadId);
    await store.createSchedule({
      id,
      target: {
        type: 'agent',
        agentId: SPIKE_AGENT_ID,
        prompt: 'Use the required write tool exactly once.',
        threadId,
        resourceId,
      },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: now - 1000,
      createdAt: now,
      updatedAt: now,
      metadata: { tenantId: 'spike' },
    });
    const actor: ApprovalActor = {
      id: SYSTEM_ACTOR_ID,
      role: 'operator',
      tenantId: 'spike',
    };
    const tenant = tenantContextForActor(actor, env);
    const topology = createAgentThreadTopology(env.THREAD);
    const tick = createScheduleTick({
      store,
      start: async (_workflowId, runId) => ({ runId }),
      startAgent: async ({
        target,
        runId,
        topologyThreadId,
        threaded,
        entryPath,
        requestContext,
        streamRequestContext,
      }) => {
        const started = await topology.start(tenant, {
          agentId: target.agentId,
          runId,
          prompt: target.prompt,
          entryPath,
          threaded,
          requestContext,
          streamRequestContext,
          ...(threaded
            ? {
                threadId: topologyThreadId,
                resourceId: target.resourceId,
              }
            : {}),
        });
        return { runId: started.runId };
      },
    });
    const result = await tick();
    const trigger = (await store.listTriggers(id))[0];
    return json({
      result,
      scheduleId: id,
      threadId,
      resourceId,
      runId: trigger?.runId ?? null,
    });
  }

  return null;
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

    const agentProbeResponse = await handleLiveAgentRoute(routed, env, resolve);
    if (agentProbeResponse) return agentProbeResponse;

    // Track B probes (B-S2 recovery seam, B-S3 _background rejection) — local,
    // unauthenticated, ahead of the routers.
    const bgProbe = await handleBackgroundTaskProbe(routed, env);
    if (bgProbe) return bgProbe;

    // Track C probes (C-S2 affinity, C-S4 cross-tenant fail-closed) — local,
    // unauthenticated, ahead of the routers.
    const sigProbe = await handleSignalProbe(routed, env);
    if (sigProbe) return sigProbe;

    // Track F probes (F-S1 read path, F-S2 eviction read, F-S3 fail-closed) —
    // local, unauthenticated, ahead of the routers.
    const goalProbe = await handleGoalProbe(routed, env);
    if (goalProbe) return goalProbe;

    // Track D probes (D-S1 exactly-once, D-S2 barrier + INV-1) — local,
    // unauthenticated, ahead of the routers.
    const schedProbe = await handleScheduleProbe(routed, env);
    if (schedProbe) return schedProbe;

    // Track E probes (E-S2 forged, E-S1 delivery, E-S3 rehydration, cross-tenant)
    // — local, unauthenticated, ahead of the routers.
    const sigpProbe = await handleSignalProviderProbe(routed, env);
    if (sigpProbe) return sigpProbe;

    // Track E webhook ingress: the github webhook route TERMINATES on the Worker,
    // signature-authed (not bearer), route-absent when its secret is unset.
    const webhookResponse = await webhookRouter(env)(routed);
    if (webhookResponse) return webhookResponse;

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

    const agentResponse = await createAgentRouter({
      agents: [SPIKE_AGENT_META],
      resolve,
      topology: createAgentThreadTopology(env.THREAD),
    })(routed);
    if (agentResponse) return agentResponse;

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
