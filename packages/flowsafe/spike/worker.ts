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
//                                             an opaque UUID); on
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
// spike var so the stream stage MOUNTS; DemoHub is the deployment fan-out hub.
// The workerd WS proof (suspend/resume/decide fan-out, hibernation persistence,
// expired/malformed ticket fail-closed) is automated in
// scripts/spike-verify.mjs.
//
// Auth: bearer tokens over the SAME host-kit seam every deployed host uses
// (staticTokenVerifier + bearerActorAuthenticator + createRunRouter) — a
// second unguarded routing path is how server-owned ID policy rots, so this spike
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
import { Agent, createMessageSignal, createSignal } from '@mastra/core/agent';
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
  invokeConnector,
} from '@proofoftech/breakwater/connector-sdk';
import {
  createContentPolicyGate,
  denyPatterns,
} from '@proofoftech/breakwater/policy-engine';
import { ACTOR_CONTEXT_KEY } from '@proofoftech/breakwater/rbac';
import { z } from 'zod';
import {
  type AgentEntryPath,
  type AgentMeta,
  type AgentModule,
  type AgentThreadStateStorage,
  createAgentApprovalResumer,
  createAgentRouter,
  createAgentThreadTopology,
  createThreadAgentHost,
  type ThreadAgentHost,
} from '../src/agent-host/index.js';
import { readAgentThreadBinding } from '../src/agent-runner/index.js';
import {
  type ActorContext,
  type ActorResolver,
  type ApprovalActor,
  type ApprovalDecision,
  type ApprovalRecord,
  ApprovalService,
  type ApprovalStore,
  type ApprovalStreamSink,
  approvalGrantProvider,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  breakwaterActorFor,
  createActorResolver,
  createApprovalRouter,
  createPrincipalActorContext,
  D1ApprovalStoreFactory,
  defaultResumeData,
  type ExecutionPrincipal,
  principalAuditFields,
  type ResourceClaim,
  type ResourceKind,
  withRegisteredResourceOwner,
} from '../src/approval-api/index.js';

import {
  BackgroundTaskHost,
  backgroundTasksStore,
  createBackgroundTaskD1Domains,
} from '../src/background-tasks/index.js';
import {
  assertExecutionFenceState,
  createD1Storage,
  createHostPubSub,
  DeploymentInventory,
  DurableObjectRunner,
  doErrorResponse,
  type ExecutionFenceStore,
  ensureDeploymentIdentityBindings,
  executionFenceFor,
  executionFenceReadingPayload,
  HubDurableObject,
  type InitResult,
  init,
  isInventoryCategory,
  isPathSafeId,
  isSuspensionTimeoutResumeData,
  mintThreadId,
  type RunnerRuntime,
  resourceIdFromKey,
  type StartIdempotencyStore,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_TIMEOUT_RESUME_KEY,
  stampDeploymentIdentityRequest,
  startIdempotencyFor,
  ThreadDurableObject,
  type ThreadScope,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from '../src/do-runner/index.js';
import {
  createObjectiveRouter,
  type ObjectiveAuditEvent,
} from '../src/goals/index.js';
import {
  abandonApprovalsForRun,
  bearerActorAuthenticator,
  createDoRunTopology,
  createHubTopology,
  createRunRouter,
  createStreamRouter,
  createThreadTopology,
  doSummary,
  RunRouteError,
  staticTokenVerifier,
  type WorkflowMeta,
} from '../src/host-kit/index.js';
import {
  canPersistScheduledAgentSignal,
  createScheduleStartSource,
  createScheduleTargetPolicy,
  createScheduleTick,
  D1SchedulesStorage,
  parseScheduleAgentDispatchReceipt,
  type ScheduleAgentDispatchReceipt,
  scheduleWithCreatorRole,
} from '../src/schedules/index.js';
import {
  createWebhookRouter,
  createWebhookSignalProvider,
  D1SubscriptionStoreFactory,
  githubSignalProvider,
  SIGNAL_PROVIDER_HOST_INSTANCE_NAME,
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
  type SignalContentPolicyInput,
} from '../src/signals/index.js';

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  RUNNER: DurableObjectNamespace;
  /** Singleton deployment live-stream hub DO; see DemoHub. */
  HUB: DurableObjectNamespace;
  /** Per-thread agent-loop / signal DO (idFromName(threadId)); see DemoThread (Track C). */
  THREAD: DurableObjectNamespace;
  /** Singleton deployment signal-provider host DO; see DemoSignalProviderHost (Track E). */
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

/**
 * ONE execution-fence store per D1 binding, for every surface in this worker.
 *
 * The spike composes its routers, ticks, and Durable Objects by hand rather
 * than through createFlowsafeWorker, so nothing else would keep the admin probe
 * that MOVES the fence, the schedule ticks that read it before claiming, and
 * the runtimes that obey it pointed at the same database. The memo itself is
 * the package's (`executionFenceFor`), keyed on the binding for the same reason
 * host-kit's composer is: the fence belongs to the database, not to the request
 * that reached it. This wrapper only unwraps `env` — which is what every call
 * site here holds — and carries the workers-types-to-structural cast the rest
 * of this worker makes on the same binding.
 */
function executionFenceForEnv(env: Env): ExecutionFenceStore {
  return executionFenceFor(env.DB as unknown as never);
}

/**
 * The same one-store-per-binding rule for start reservations: the run router
 * and every agent topology below reserve here, and the runtimes inside the run
 * and thread objects settle there, and both must be the store built from THIS
 * binding.
 */
function startIdempotencyForEnv(env: Env): StartIdempotencyStore {
  return startIdempotencyFor(env.DB as unknown as never);
}

function fetchDeploymentObject(
  stub: ReturnType<DurableObjectNamespace['get']>,
  env: Env,
  input: string,
  init?: RequestInit,
): Promise<Response> {
  return stub.fetch(
    stampDeploymentIdentityRequest(
      new Request(input, init),
      env.DEPLOYMENT_IDENTITY_SECRET,
    ),
  );
}

/** The connector id an approval grants; the publish step demands it. */
const PUBLISH_CONNECTOR = 'demo-publisher';

function containsConnectorId(value: unknown, connectorId: string): boolean {
  return (
    Array.isArray(value) &&
    value.some(
      (grant) =>
        typeof grant === 'object' &&
        grant !== null &&
        (grant as { connectorId?: unknown }).connectorId === connectorId,
    )
  );
}

/**
 * Id for bridge-queued approval records. The run router builds the full actor
 * (operator role, the authenticated request) — matching showcase/deploy.
 */
const SYSTEM_PRINCIPAL_ID = 'demo-worker';

const SPIKE_AGENT_ID = 'spike-guarded-agent';
const SPIKE_WRITE_CONNECTOR_ID = 'spike_recordWrite';
const SPIKE_AGENT_META = {
  id: SPIKE_AGENT_ID,
  title: 'Spike guarded agent',
  description:
    'Calls one approval-gated write connector through the catalog-driven durable host.',
  allowedRoles: ['admin', 'operator'],
  // The automated entries the spike drives. 'approval.resume' is implied by the
  // kind, so a scheduled run that suspends for approval still resumes.
  allowedAutomation: [
    { kind: 'system', entryPaths: ['schedule.fire', 'notification.dispatch'] },
    { kind: 'service', entryPaths: ['signal.notification'] },
  ],
} as const satisfies AgentMeta;

// The signal content policy, wired the way the FlowSafe README documents it: a
// REAL Breakwater gate behind FlowSafe's structural callback. The marker keeps
// the boundary deterministic — every other spike signal renders clean and flows
// unchanged, so this proves the gate under real workerd without gating the rest
// of the run.
const SPIKE_DENIED_CONTENT = 'spike-denied-content';

const inspectSpikeSignalContent = createContentPolicyGate({
  policies: [
    denyPatterns([SPIKE_DENIED_CONTENT], { name: 'spike-signal-content' }),
  ],
  resource: 'spike-signal-content',
});

function spikeSignalPolicyContext(
  input: SignalContentPolicyInput,
): RequestContext {
  const requestContext = new RequestContext();
  requestContext.set(ACTOR_CONTEXT_KEY, breakwaterActorFor(input.principal));
  requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
    agentId: input.agentId,
    ...(input.deploymentTag === undefined
      ? {}
      : { tenantId: input.deploymentTag }),
    ...(input.runId === undefined ? {} : { runId: input.runId }),
    threadId: input.threadId,
    ...(input.resourceId === undefined ? {} : { resourceId: input.resourceId }),
    entryPath: input.entryPath,
    ...principalAuditFields(input.principal),
  });
  return requestContext;
}

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
             deployment_tag TEXT NOT NULL,
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
        db.prepare(
          `CREATE TABLE IF NOT EXISTS spike_agent_input_processor_calls (
             call_id TEXT PRIMARY KEY,
             run_id TEXT NOT NULL,
             deployment_tag TEXT NOT NULL,
             message_count INTEGER NOT NULL,
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
      // Breakwater's generic audit-context field remains named `tenantId`;
      // Flowsafe fills it only from the verified deployment tag and stores it
      // under that meaning here.
      const deploymentTag = correlation.tenantId;
      await env.DB.batch([
        env.DB.prepare(
          `INSERT INTO spike_agent_connector_calls
             (call_id, tool_call_id, run_id, thread_id, resource_id,
              actor_id, actor_role, deployment_tag, entry_path, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        ).bind(
          crypto.randomUUID(),
          toolCallId,
          correlation.runId,
          threadId,
          resourceId,
          actor.id,
          actor.role,
          deploymentTag,
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
      allowedPrincipalKinds: ['human', 'system', 'service'],
      policies: [],
      audit,
      maxSteps: 1,
      toolChoice: 'required',
      applicationInputProcessors: [
        {
          id: 'spike-input-probe',
          processInput: async (args) => {
            const correlation = args.requestContext?.get(
              AGENT_AUDIT_CONTEXT_KEY,
            ) as
              | {
                  tenantId?: unknown;
                  runId?: unknown;
                }
              | undefined;
            if (
              typeof correlation?.tenantId !== 'string' ||
              typeof correlation.runId !== 'string'
            ) {
              throw new Error(
                'guarded input processor is missing its durable correlation',
              );
            }
            await ensureAgentProbeTables(env.DB);
            // See the connector probe above: this legacy Breakwater field is
            // the verified deployment tag, not a request tenant selector.
            const deploymentTag = correlation.tenantId;
            await env.DB.prepare(
              `INSERT INTO spike_agent_input_processor_calls
                 (call_id, run_id, deployment_tag, message_count, created_at)
                 VALUES (?, ?, ?, ?, ?)`,
            )
              .bind(
                crypto.randomUUID(),
                correlation.runId,
                deploymentTag,
                args.messages.length,
                new Date().toISOString(),
              )
              .run();
            if (args.messages.length === 0) {
              args.abort(
                'spike application input processor received empty messages',
              );
            }
            return args.messages;
          },
        },
      ],
    }),
  };
}

// LOCAL-ONLY spike identities (wrangler dev; never deployed). One deployment so
// every probe sees the same queue; ids/roles chosen so spike-verify.mjs can
// exercise RBAC, the grant loop, and separation of duties:
//   spike-admin    — the ONLY role in RUN_START_ROLES ∩ decide-capable, so it
//                    can request AND be denied deciding its own request (SoD)
//   spike-operator — starts runs, fires the forged-resume probe
//   spike-reviewer — decides the happy-path approval
//   spike-viewer   — read-only listing
const SPIKE_ACTORS = new Map<string, ApprovalActor>([
  ['spike-admin', { id: 'ada', role: 'admin' }],
  ['spike-operator', { id: 'opal', role: 'operator' }],
  ['spike-reviewer', { id: 'ray', role: 'reviewer' }],
  ['spike-viewer', { id: 'vic', role: 'viewer' }],
  ['other-operator', { id: 'oliver', role: 'operator' }],
  ['other-reviewer', { id: 'ruth', role: 'reviewer' }],
  ['other-viewer', { id: 'vera', role: 'viewer' }],
]);

// --- Idempotent-start execution counter (FI1/FI2) ---------------------------
// The reservation's claim is not "one run id came back twice" — it is that the
// paid first step of a keyed start EXECUTED ONCE. A repeated run id is only
// evidence of that if nothing else could have run, which is an inference. This
// workflow's first step writes a durable D1 row instead, so the spike can count
// executions directly across a process death and across a concurrent burst.
const COUNTED_WORKFLOW_ID = 'demo-idempotent';
const EXECUTION_COUNT_TABLE = 'spike_execution_count';
const EXECUTION_COUNT_DDL = `CREATE TABLE IF NOT EXISTS ${EXECUTION_COUNT_TABLE} (
    id TEXT PRIMARY KEY,
    executions INTEGER NOT NULL
  )`;

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
  {
    id: 'demo-deadline',
    title: 'Demo suspension deadline',
    description:
      'one step that suspends with a per-suspension deadline, so the run resumes itself when the awaited signal never arrives',
    sampleInput: { topic: 'launch', deadlineMs: 10_000 },
  },
  {
    id: COUNTED_WORKFLOW_ID,
    title: 'Demo idempotent start',
    description:
      'demo-approval with a counting first step, so an idempotent start can be proved by EXECUTIONS rather than by run ids',
    sampleInput: { topic: 'launch', counterId: 'probe' },
  },
];
const scheduleTargetPolicy = createScheduleTargetPolicy({
  workflows: [...WORKFLOWS, { id: 'sched-echo' }],
  agents: [SPIKE_AGENT_META],
});

// One factory per isolate, not per request: it owns the memoized schema-init
// promise, so rebuilding it inside fetch() would re-run the whole DDL pass on
// every request. Keyed by the D1 binding, stable for an isolate's lifetime.
const approvalFactories = new WeakMap<D1Database, D1ApprovalStoreFactory>();

function approvalStoreFactory(db: D1Database): D1ApprovalStoreFactory {
  let factory = approvalFactories.get(db);
  if (!factory) {
    factory = new D1ApprovalStoreFactory(db, {
      workflowSnapshotTable: 'mastra_workflow_snapshot',
    });
    approvalFactories.set(db, factory);
  }
  return factory;
}

function defineWorkflows(env: Env): RunnerRuntime {
  const approvals = approvalStoreFactory(env.DB).store();
  const { createWorkflow, createStep, runtime } = init(env, {
    // The grant-minting seam: on every start/resume the runtime derives the
    // breakwater grant key from APPROVED records in D1 — decisions become
    // capabilities without any grant crossing a request body.
    requestContextForRun: approvalGrantProvider(approvals),
  });
  const publisher = createConnector<{ topic: string }, { published: boolean }>({
    id: PUBLISH_CONNECTOR,
    description: 'Publishes the approved workerd probe',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ published: z.boolean() }),
    permissions: { sideEffect: 'write', requiresApproval: true },
    execute: async () => ({ published: true }),
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
      const result = await invokeConnector(
        publisher,
        { topic: inputData.topic },
        { requestContext },
      );
      return {
        topic: inputData.topic,
        published: result.published,
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

  // FI1/FI2: demo-approval's shape with a COUNTING first step. The step is
  // ordinary paid work as far as the runtime is concerned — it writes to D1
  // before the run suspends — so a second execution of this key would show up
  // as a second row increment whatever the run ids said.
  const countedResearch = createStep({
    id: 'counted-research',
    inputSchema: z.object({ topic: z.string(), counterId: z.string() }),
    outputSchema: z.object({ topic: z.string(), notes: z.string() }),
    execute: async ({ inputData }) => {
      // Lazy DDL rather than a provisioning step: this table belongs to the
      // probe, not to the deployment, and creating it here keeps its one
      // definition beside its one writer.
      await env.DB.prepare(EXECUTION_COUNT_DDL).run();
      await env.DB.prepare(
        `INSERT INTO ${EXECUTION_COUNT_TABLE} (id, executions) VALUES (?, 1)
           ON CONFLICT(id) DO UPDATE SET executions = executions + 1`,
      )
        .bind(inputData.counterId)
        .run();
      return {
        topic: inputData.topic,
        notes: `research notes for ${inputData.topic}`,
      };
    },
  });

  createWorkflow({
    id: COUNTED_WORKFLOW_ID,
    inputSchema: z.object({ topic: z.string(), counterId: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      published: z.boolean(),
      approvedBy: z.string().optional(),
    }),
  })
    .then(countedResearch)
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
      const result = await invokeConnector(
        publisher,
        { topic: inputData.topic },
        { requestContext, toolCallId: 'call-1' },
      );
      return { topic: inputData.topic, published: result.published };
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
  // stored requestContext must NOT appear in the leg's grant list: the
  // schedule-source adapter strips reserved fields, and the DO's own
  // approvalGrantProvider mints `breakwater.connectorGrants` from APPROVED
  // records (an EMPTY [] for this fresh run). `reservedLeaked` checks the VALUE
  // (does the grant include the forged
  // id), NOT mere key presence (the key is legitimately present as []). The
  // runtime-derived workflow scope is present, no isolation scope is fabricated,
  // and the benign stored key plus initial workflow state reach the leg through
  // the verified target.
  const scheduleStateSchema = z.object({
    fromSchedule: z.boolean().optional(),
  });
  const schedEcho = createStep({
    id: 'echo',
    stateSchema: scheduleStateSchema,
    inputSchema: z.object({}),
    outputSchema: z.object({
      reservedLeaked: z.boolean(),
      workflowScopePresent: z.boolean(),
      isolationScopePresent: z.boolean(),
      customPresent: z.boolean(),
      initialStatePresent: z.boolean(),
    }),
    execute: async ({ requestContext, state }) => {
      const grants = requestContext.get(BREAKWATER_CONNECTOR_GRANTS_KEY);
      return {
        // Introspection only: actual authorization above uses createConnector.
        reservedLeaked: containsConnectorId(grants, 'forged-connector'),
        workflowScopePresent:
          requestContext.get('breakwater.workflowScope') !== undefined,
        isolationScopePresent:
          requestContext.get('breakwater.isolationScope') !== undefined,
        customPresent: requestContext.get('sched.note') !== undefined,
        initialStatePresent: state.fromSchedule === true,
      };
    },
  });
  createWorkflow({
    id: 'sched-echo',
    stateSchema: scheduleStateSchema,
    inputSchema: z.object({}),
    outputSchema: z.object({
      reservedLeaked: z.boolean(),
      workflowScopePresent: z.boolean(),
      isolationScopePresent: z.boolean(),
      customPresent: z.boolean(),
      initialStatePresent: z.boolean(),
    }),
  })
    .then(schedEcho)
    .commit();

  // Per-suspension deadline probe (T1-T3): a step arms its own durable wake by
  // putting SUSPENSION_DEADLINE_PAYLOAD_KEY in the payload it hands Mastra's
  // suspend(); the run's OWN Durable Object derives a fenced entry from the
  // authoritative summary, persists it in DO storage, and arms its single alarm
  // for it. When the wake fires it resumes THIS step with the reserved timeout
  // envelope under the system principal. The step therefore records WHICH kind
  // of resume reached it, through the exported guard rather than a string
  // literal, so spike-verify can assert the ENVELOPE arrived — not merely that
  // the run advanced.
  //
  // It declares NEITHER a suspendSchema (which would have to declare the
  // reserved key, or Mastra's parse substitution strips it) NOR a resumeSchema
  // (which would have to accept the timeout envelope, or the resume is
  // refused) — the simplest correct arming shape. `deadlineMs` rides the run
  // input so ONE workflow drives both sides of the fence: a short deadline that
  // must fire itself, and a longer one whose real signal must settle the entry
  // first.
  const deadlineInputSchema = z.object({
    topic: z.string(),
    deadlineMs: z.number().int(),
  });
  const deadlineOutputSchema = z.object({
    topic: z.string(),
    resumedBy: z.enum(['timeout', 'signal']),
    timeoutStep: z.string().optional(),
    deadlineAt: z.number().optional(),
    expiredAt: z.number().optional(),
  });
  const deadlineWait = createStep({
    id: 'wait-signal',
    inputSchema: deadlineInputSchema,
    outputSchema: deadlineOutputSchema,
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          [SUSPENSION_DEADLINE_PAYLOAD_KEY]: inputData.deadlineMs,
          awaiting: 'external launch signal',
        });
      }
      if (isSuspensionTimeoutResumeData(resumeData)) {
        const timeout = resumeData[SUSPENSION_TIMEOUT_RESUME_KEY];
        return {
          topic: inputData.topic,
          resumedBy: 'timeout' as const,
          timeoutStep: timeout.step,
          deadlineAt: timeout.deadlineAt,
          expiredAt: timeout.expiredAt,
        };
      }
      return { topic: inputData.topic, resumedBy: 'signal' as const };
    },
  });
  createWorkflow({
    id: 'demo-deadline',
    inputSchema: deadlineInputSchema,
    outputSchema: deadlineOutputSchema,
  })
    .then(deadlineWait)
    .commit();

  return runtime;
}

/**
 * DO storage key holding one run's armed suspension deadlines. Deliberately NOT
 * on the package's public surface — it is the alarm's own bookkeeping — so the
 * spike mirrors the constant to introspect it.
 *
 * The trade, stated plainly: a duplicated literal can drift from the module's
 * own, and exporting the key to stop that would put a Durable Object's private
 * wake state on the public surface for every consumer, where reading it proves
 * nothing and writing it corrupts the alarm. Drift is also the cheaper failure:
 * this key is read in ONE place, the route below, and spike-verify's T2
 * assertion then observes no armed record where it requires one and fails
 * loudly. Silent success is not reachable.
 */
const SPIKE_SUSPENSION_DEADLINE_STORAGE_KEY = 'flowsafe:suspension-deadline:v1';

/** The run-DO route the T1-T3 probes read that armed state through. */
const SPIKE_SUSPENSION_DEADLINE_PATH = '/spike/suspension-deadline';

export class DemoRunner extends DurableObjectRunner<Env> {
  /**
   * Spike-only introspection of the state the suspension-deadline duty owns:
   * the armed record in THIS object's storage and the single alarm both DO
   * duties share. Nothing on the run surface exposes either (correctly — a
   * client has no business reading an object's wake schedule), so spike-verify
   * would otherwise have to infer "armed" from the resume that follows it. The
   * runner's own routes all live under /runs, so a /spike/ path cannot collide
   * with one, and deployment identity is verified exactly as the inherited
   * fetch does.
   */
  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname !== SPIKE_SUSPENSION_DEADLINE_PATH) {
      return super.fetch(request);
    }
    try {
      await verifyDurableObjectDeploymentRequest(request, this.state, this.env);
      const storage = this.state?.storage;
      // getAlarm is not part of the structural storage subset do-runner
      // declares (it never reads the alarm back); workerd always provides it.
      const alarms = storage as unknown as
        | { getAlarm(): Promise<number | null> }
        | undefined;
      return json({
        alarmAt: (await alarms?.getAlarm()) ?? null,
        record:
          (await storage?.get(SPIKE_SUSPENSION_DEADLINE_STORAGE_KEY)) ?? null,
      });
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  protected build(env: Env): RunnerRuntime {
    return defineWorkflows(env);
  }

  protected runOwnership(env: Env) {
    return approvalStoreFactory(env.DB).resources();
  }

  protected scheduleSource(env: Env) {
    return createScheduleStartSource(
      new D1SchedulesStorage(env.DB as unknown as never),
    );
  }

  protected runLifecycle(env: Env) {
    const service = new ApprovalService({
      store: approvalStoreFactory(env.DB).store(),
      // Deliberately unfenced: the only thing this service is used for is
      // abandonApprovalsForRun below, and abandoning is a terminate-path
      // operation that stays allowed in every fence state — it removes future
      // work, which is the direction a drain is going. It never decides, so it
      // never commits a decision a locked deployment could not resume.
      executionFence: 'none',
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

/**
 * The deployment live-stream hub DO (DL-009). The wrangler `HUB` binding + the
 * append-only `v2` migration resolve this named export; the base class does all
 * the work (fan-out over hibernatable WebSockets + presence), so the body is
 * empty. Addressed through the singleton deployment topology. The workerd spike
 * (scripts/spike-verify.mjs) drives this over a real WebSocket to prove
 * fan-out, hibernation persistence, and ticket fail-closed.
 */
export class DemoHub extends HubDurableObject<Env> {}

const BACKGROUND_TASKS_INSTANCE_NAME = 'deployment-background-tasks';
const BACKGROUND_TASKS_ALARM_MS = 60_000;

/** One execution-capable background-task manager per deployment. */
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
        await this.#state.storage.setAlarm(
          Date.now() + BACKGROUND_TASKS_ALARM_MS,
        );
        const pubsub = createHostPubSub();
        const storage = createD1Storage({
          binding: this.#env.DB as unknown as never,
          domains: createBackgroundTaskD1Domains({
            binding: this.#env.DB as unknown as never,
          }),
        });
        await storage.init();
        const mastra = new Mastra({ storage, pubsub });
        const host = new BackgroundTaskHost({
          mastra,
          pubsub,
          execution: true,
          executionFence: executionFenceForEnv(this.#env),
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
        try {
          await host.boot();
        } catch (error) {
          try {
            await host.shutdown();
          } catch (shutdownError) {
            console.error(
              'spike background-task boot rollback failed',
              shutdownError,
            );
          }
          throw error;
        }
        return host;
      })().catch((error: unknown) => {
        this.#host = undefined;
        throw error;
      });
    }
    return this.#host;
  }

  async #verifyIdentity(): Promise<void> {
    await verifyDurableObjectDeploymentIdentity(this.#state, this.#env);
    this.#assertInstanceName();
  }

  #assertInstanceName(): void {
    if (this.#state.id.name !== BACKGROUND_TASKS_INSTANCE_NAME) {
      throw new Error(
        `background-task DO must be addressed as '${BACKGROUND_TASKS_INSTANCE_NAME}'`,
      );
    }
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await verifyDurableObjectDeploymentRequest(
        request,
        this.#state,
        this.#env,
      );
      if (this.#state.id.name !== BACKGROUND_TASKS_INSTANCE_NAME) {
        throw new Error(
          `background-task DO must be addressed as '${BACKGROUND_TASKS_INSTANCE_NAME}'`,
        );
      }
      const host = await this.#boot();
      const url = new URL(request.url);
      if (request.method === 'POST' && url.pathname === '/enqueue') {
        const body = (await request.json()) as {
          runId?: string;
          delayMs?: number;
        };
        if (!isPathSafeId(body.runId)) {
          return json({ error: 'path-safe parent runId required' }, 404);
        }
        const queued = await host.enqueue({
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
        const task = await host.getTask(
          decodeURIComponent(url.pathname.slice('/task/'.length)),
        );
        return task ? json(task) : json({ error: 'not found' }, 404);
      }
      return json({ error: 'not found' }, 404);
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  async alarm(): Promise<void> {
    this.#assertInstanceName();
    await this.#state.storage.setAlarm(Date.now() + BACKGROUND_TASKS_ALARM_MS);
    await this.#verifyIdentity();
    await (await this.#boot()).onAlarm();
  }
}

/**
 * Track C (M-004): the per-thread signal DO (idFromName(threadId)). `build()`
 * wires init() with the ONE host pubsub
 * identity (createHostPubSub) — the affinity carrier: core keys its in-process
 * signal registry by the pubsub instance, so a send only drains into an active
 * loop when both share this isolate's ONE pubsub (DL-002). `route()` composes the
 * production Track C signal routes plus a spike-only C-S2 affinity probe. The
 * base class asserts deployment identity and a trusted execution principal
 * BEFORE route() runs.
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
    const approvals = approvalStoreFactory(env.DB).store();
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
        return this.state.storage as unknown as AgentThreadStateStorage;
      },
      resourceAccess: () => approvalStoreFactory(env.DB).resources(),
      scheduleSource: () =>
        createScheduleStartSource(
          new D1SchedulesStorage(env.DB as unknown as never),
        ),
      discardScheduleDispatch: (scheduleId, dispatchId, runId) =>
        new D1SchedulesStorage(
          env.DB as unknown as never,
        ).discardAgentScheduleDispatch(scheduleId, dispatchId, runId),
      approvalService: () => {
        this.#approvalService ??= new ApprovalService({
          store: approvals,
          // The thread's own agent-approval service decides, and decide()
          // COMMITS before it resumes, so it gates on the same store as the
          // thread runtime beside it.
          executionFence: executionFenceForEnv(env),
          stream: (event) =>
            createHubTopology(
              this.env.HUB,
              this.env.DEPLOYMENT_IDENTITY_SECRET,
            ).publish(event),
        });
        return this.#approvalService;
      },
      systemPrincipalId: SYSTEM_PRINCIPAL_ID,
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
        // The composite store hides the binding init would have fenced from,
        // so this thread DO names it: the fence must live in the SAME database
        // as the state it fences.
        executionFence: executionFenceForEnv(env),
        // Same reasoning, and the same binding: the agent topology reserves
        // keyed starts into THIS store, so the runtime that sees an agent run
        // reach terminal has to be the one that can mark them spent. Wiring
        // 'none' here would reserve and claim normally and then never settle.
        startIdempotency: startIdempotencyForEnv(env),
      },
    );
    this.#threadInit = threadInit;
    return threadInit;
  }

  protected async onAlarm(
    _env: Env,
    threadId: string,
    initResult: InitResult,
  ): Promise<void> {
    if (!this.#agentHost) {
      throw new Error('thread agent host is unavailable');
    }
    await this.#agentHost.recoverOwnership(initResult.runtime, threadId);
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
    contentPolicy: (input) =>
      inspectSpikeSignalContent({
        text: input.text,
        requestContext: spikeSignalPolicyContext(input),
      }),
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
    resolveResourceId: (scope) => resourceIdFromKey(scope.threadId),
    resolveBlockingRun: (scope) => this.#host().blockingRun(scope),
    serializeDispatch: (_scope, operation) =>
      this.#host().serializeDispatch(operation),
    resolveScheduleRunStatus: (scope, input) =>
      this.#host().scheduleDispatchStatus(scope, input),
    resolveScheduleTarget: async (_scope, input) => {
      const target = await createScheduleStartSource(
        new D1SchedulesStorage(this.env.DB as unknown as never),
      ).resolveScheduleTarget(input.scheduleId, input.dispatchId, input.runId);
      return target?.type === 'agent' ? target : undefined;
    },
    canPersist: async (scope) => {
      const owner = await approvalStoreFactory(this.env.DB)
        .resources()
        .owner('thread', scope.threadId);
      return (
        owner?.kind === scope.principal.kind && owner.id === scope.principal.id
      );
    },
    canPersistSchedule: (scope, input) =>
      canPersistScheduledAgentSignal(
        createScheduleStartSource(
          new D1SchedulesStorage(this.env.DB as unknown as never),
        ),
        approvalStoreFactory(this.env.DB).resources(),
        { ...input, threadId: scope.threadId },
      ),
    resolveNotificationsStorage: async () => {
      const store = await this.#storage?.getStore('notifications');
      if (!store) throw new Error('notifications storage unavailable');
      return store;
    },
    resolveScheduleDispatchStore: () => {
      const store = new D1SchedulesStorage(this.env.DB as unknown as never);
      return {
        begin: async (scheduleId, dispatchId) => {
          const key = `flowsafe:schedule-dispatch-receipt:v1:${dispatchId}`;
          const local =
            await this.state?.storage.get<ScheduleAgentDispatchReceipt>(key);
          if (local) {
            await store.settleAgentScheduleDispatch(
              scheduleId,
              dispatchId,
              local,
            );
            await this.state?.storage.delete(key);
            return { state: 'settled' as const, receipt: local };
          }
          return store.beginAgentScheduleDispatch(scheduleId, dispatchId);
        },
        settle: async (scheduleId, dispatchId, receipt) => {
          const key = `flowsafe:schedule-dispatch-receipt:v1:${dispatchId}`;
          await this.state?.storage.put(key, receipt);
          await store.settleAgentScheduleDispatch(
            scheduleId,
            dispatchId,
            receipt,
          );
          await this.state?.storage.delete(key);
        },
      };
    },
    startIdleRun: async (input) => {
      const scope = {
        threadId: input.threadId,
        deploymentTag: this.env.DEPLOYMENT_TENANT,
        principal: input.principal,
        init: this.#initResult(),
      };
      const result = await this.#host().start(scope, {
        agentId: input.agent.id,
        threadId: input.threadId,
        runId: input.runId,
        resourceId: input.resourceId ?? resourceIdFromKey(scope.threadId),
        messages:
          input.message !== undefined
            ? createMessageSignal(input.message)
            : input.signal !== undefined
              ? createSignal(input.signal)
              : 'Perform the required write now.',
        entryPath: input.entryPath,
        ...(input.scheduleId !== undefined
          ? { scheduleId: input.scheduleId }
          : {}),
        ...(input.dispatchId !== undefined
          ? { dispatchId: input.dispatchId }
          : {}),
        ...(input.scheduleId !== undefined && input.dispatchId !== undefined
          ? { scheduleDispatchLease: 'executing' as const }
          : {}),
        safeContext: input.safeContext,
      });
      return { runId: result.runId, status: result.summary.status };
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
    const resourceId = resourceIdFromKey('affinity-probe');
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
      deploymentTag: scope.deploymentTag,
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
 * The singleton deployment provider host DO (Track E). `build()` binds the
 * deployment subscription store, the thread topology (delivery), and the
 * spike's providers. The base class drives the alarm poll + `/poll` probe route.
 */
export class DemoSignalProviderHost extends SignalProviderHost<Env> {
  protected build(env: Env): SignalProviderHostWiring {
    return {
      store: subscriptionFactory(env.DB).store(),
      topology: createThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      providers: spikeProviders(),
      executionFence: executionFenceForEnv(env),
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
      subscriptions: subscriptionFactory(env.DB).store(),
      topology: createThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      deploymentTag: env.DEPLOYMENT_TENANT,
      // Only GitHub is a real webhook provider here; the poll provider signs nothing.
      secretForProvider: (id) =>
        id === 'github' ? env.GITHUB_WEBHOOK_SECRET : undefined,
      audit: (event) => {
        sigpAudit.push(event);
      },
      executionFence: executionFenceForEnv(env),
    });
    webhookRouters.set(env.DB, router);
  }
  return router;
}

const SIGP_THREAD_ID = mintThreadId(() => 'sigp');
// Matches DemoThread.resolveResourceId, so delivery keys the inbox on the
// bound thread's resource owner.
const SIGP_RESOURCE_ID = resourceIdFromKey(SIGP_THREAD_ID);

// LOCAL-ONLY Track E probes, driven by spike-verify.mjs:
//  E-S2 (forged): a bad-signature webhook is rejected BEFORE parse + audited.
//  E-S1 (delivery): subscribe -> signed webhook -> notification lands in the inbox.
//  E-S3 (rehydration): after a kill+restart, the host DO's /poll rehydrates
//    subscriptions from D1 and fires poll delivery (the in-memory-lost proof).
async function handleSignalProviderProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  if (!path.startsWith('/sigp/')) return null;
  const store = subscriptionFactory(env.DB).store();

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

  if (request.method === 'GET' && path === '/sigp/audit') {
    return json({ events: sigpAudit });
  }

  if (request.method === 'POST' && path === '/sigp/poll') {
    // E-S3 direct-alarm probe: drive the host DO's /poll (deterministic — no
    // dependency on wrangler's alarm timer). A FRESH post-restart host rehydrates
    // its subscriptions from D1 here.
    const stub = env.SIGNAL_PROVIDER_HOST.get(
      env.SIGNAL_PROVIDER_HOST.idFromName(SIGNAL_PROVIDER_HOST_INSTANCE_NAME),
    );
    const response = await fetchDeploymentObject(
      stub,
      env,
      'http://host/poll',
      {
        method: 'POST',
      },
    );
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
  resolve: ActorResolver,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/agent/live/effects') return null;
  const context = await resolve(request);
  if (!context) return json({ error: 'unauthorized' }, 401);
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  await ensureAgentProbeTables(env.DB);
  const requestedRunId = url.searchParams.get('runId');
  if (requestedRunId && !isPathSafeId(requestedRunId)) {
    return json({ error: 'run not found' }, 404);
  }
  const statement = requestedRunId
    ? env.DB.prepare(
        `SELECT c.call_id, c.tool_call_id, c.run_id, c.thread_id,
                  c.resource_id, c.actor_id, c.actor_role, c.deployment_tag,
                  c.entry_path,
                  CASE WHEN EXISTS (
                    SELECT 1 FROM spike_agent_side_effects s
                    WHERE s.tool_call_id = c.tool_call_id
                  ) THEN 1 ELSE 0 END AS recorded
           FROM spike_agent_connector_calls c
           WHERE c.deployment_tag = ? AND c.run_id = ?
           ORDER BY c.created_at`,
      ).bind(context.deploymentTag, requestedRunId)
    : env.DB.prepare(
        `SELECT c.call_id, c.tool_call_id, c.run_id, c.thread_id,
                  c.resource_id, c.actor_id, c.actor_role, c.deployment_tag,
                  c.entry_path,
                  CASE WHEN EXISTS (
                    SELECT 1 FROM spike_agent_side_effects s
                    WHERE s.tool_call_id = c.tool_call_id
                  ) THEN 1 ELSE 0 END AS recorded
           FROM spike_agent_connector_calls c
           WHERE c.deployment_tag = ?
           ORDER BY c.created_at`,
      ).bind(context.deploymentTag);
  const calls = await statement.all<{
    call_id: string;
    tool_call_id: string;
    run_id: string;
    thread_id: string;
    resource_id: string;
    actor_id: string;
    actor_role: string;
    deployment_tag: string;
    entry_path: string;
    recorded: number;
  }>();
  const inputProcessorStatement = requestedRunId
    ? env.DB.prepare(
        `SELECT message_count
           FROM spike_agent_input_processor_calls
           WHERE deployment_tag = ? AND run_id = ?
           ORDER BY created_at`,
      ).bind(context.deploymentTag, requestedRunId)
    : env.DB.prepare(
        `SELECT message_count
           FROM spike_agent_input_processor_calls
           WHERE deployment_tag = ?
           ORDER BY created_at`,
      ).bind(context.deploymentTag);
  const inputProcessorCalls = await inputProcessorStatement.all<{
    message_count: number;
  }>();
  const modelCalls = await env.DB.prepare(
    'SELECT COUNT(*) AS count FROM spike_agent_model_calls',
  ).first<{ count: number }>();
  return json({
    connectorCalls: calls.results.length,
    effects: calls.results.filter((call) => call.recorded === 1).length,
    inputProcessorCalls: inputProcessorCalls.results.length,
    inputProcessorMessageCounts: inputProcessorCalls.results.map(
      (call) => call.message_count,
    ),
    modelCalls: modelCalls?.count ?? 0,
    calls: calls.results,
  });
}

function actorContextForPrincipal(
  principal: ExecutionPrincipal,
  env: Env,
): ActorContext {
  const factory = approvalStoreFactory(env.DB);
  return createPrincipalActorContext({
    principal,
    storeFactory: factory,
    deploymentTag: env.DEPLOYMENT_TENANT,
    buildService: (store) =>
      new ApprovalService({
        store,
        // The twin of buildApprovalService below, and fenced for the same
        // reason: these contexts decide approvals, and decide() COMMITS before
        // it resumes. Same database, same store.
        executionFence: executionFenceForEnv(env),
      }),
  });
}

async function actorContextForRegisteredOwner(
  principal: ExecutionPrincipal,
  env: Env,
  sourceKind: ResourceKind,
  sourceId: string,
): Promise<ActorContext> {
  return actorContextForRegisteredResources(principal, env, [
    { kind: sourceKind, resourceId: sourceId },
  ]);
}

async function actorContextForRegisteredResources(
  principal: ExecutionPrincipal,
  env: Env,
  claims: readonly ResourceClaim[],
): Promise<ActorContext> {
  const context = actorContextForPrincipal(principal, env);
  const resources = approvalStoreFactory(env.DB).resources();
  return withRegisteredResourceOwner(context, resources, claims);
}

// Worker-level approval service: shares the DO's D1 database; decisions
// resume the run through its DO stub (grants come from the store via the
// DO-side provider, never from this request).
function buildApprovalService(
  store: ApprovalStore,
  env: Env,
  stream: ApprovalStreamSink | undefined,
): ApprovalService {
  const fallback = async (
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ) => {
    if (!record.decidedBy) {
      throw new Error('decided approval is missing its reviewer identity');
    }
    return doSummary(
      await fetchDeploymentObject(
        runStub(env, record.workflowId, record.runId),
        env,
        `http://do/runs/${record.workflowId}/${record.runId}/resume`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            step: record.stepPath,
            resumeData: defaultResumeData(record, decision),
            requestedBy: record.decidedBy,
            requestedByKind: 'human',
          }),
        },
      ),
    );
  };
  const resumeRun = createAgentApprovalResumer({
    fallback,
    agents: [SPIKE_AGENT_META],
    topology: createAgentThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
      {
        startIdempotency: startIdempotencyForEnv(env),
        executionFence: executionFenceForEnv(env),
      },
    ),
    contextForPrincipal: (principal, record) => {
      const target = record.resumeTarget;
      if (target?.kind !== 'agent-thread') {
        throw new Error('agent approval has no registered thread target');
      }
      return actorContextForRegisteredResources(principal, env, [
        { kind: 'thread', resourceId: target.threadId },
        { kind: 'resource', resourceId: target.resourceId },
        { kind: 'run', resourceId: record.runId },
      ]);
    },
  });
  return new ApprovalService({
    store,
    defaultSlaSeconds: 15 * 60,
    // Live fan-out: every SUCCESSFUL mutation forwards to the deployment hub,
    // which fans it out to open dashboard sockets. Fire-and-forget; the
    // caller keeps the publish alive with ctx.waitUntil (see fetch below).
    stream,
    resumeRun,
    // DECIDE commits and then resumes, so the fence has to be consulted at the
    // service — before the CAS — not left to the run DO's own resume gate. A
    // decision that committed against a locked deployment would be durable with
    // nothing behind it. Same database as the runs it gates.
    executionFence: executionFenceForEnv(env),
  });
}

// --- Track B (M-003) background-task probes --------------------------------
// LOCAL-ONLY worker-level probes, driven by
// scripts/spike-verify.mjs:
//  - B-S3: createConnector rejects a smuggled `_background` arg and audits it.
//  - B-S2: the persistence-only compatibility path recovers a task through
//    public init() with the stock D1 domains.
//  - H2: DemoBackgroundTasks composes the serialized deployment domains,
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
    env.BACKGROUND_TASKS.get(
      env.BACKGROUND_TASKS.idFromName(BACKGROUND_TASKS_INSTANCE_NAME),
    );

  if (
    request.method === 'POST' &&
    (path === '/bg/execute' || path === '/bg/execute-recover')
  ) {
    const response = await fetchDeploymentObject(
      executionStub(),
      env,
      'http://background/enqueue',
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          runId: crypto.randomUUID(),
          ...(path.endsWith('execute-recover') ? { delayMs: 5000 } : {}),
        }),
      },
    );
    return new Response(response.body, response);
  }

  if (request.method === 'GET' && path.startsWith('/bg/execution-task/')) {
    const taskId = encodeURIComponent(
      decodeURIComponent(path.slice('/bg/execution-task/'.length)),
    );
    const response = await fetchDeploymentObject(
      executionStub(),
      env,
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
      await invokeConnector(
        writer,
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
      executionFence: executionFenceForEnv(env),
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
// LOCAL-ONLY worker-level probes, driven by
// scripts/spike-verify.mjs:
//  - C-S2 (affinity): a signal into an ACTIVE loop on the thread DO drains
//    IN-PROCESS. Driven THROUGH createThreadTopology (the sanctioned reach — it
//    stamps the trusted principal header), so the send lands in the isolate
//    hosting the reserved loop and resolves to action 'deliver'.
async function handleSignalProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  const path = url.pathname;

  const context = actorContextForPrincipal(
    { kind: 'human', id: 'signal-probe', role: 'operator' },
    env,
  );

  const topology = createThreadTopology(
    env.THREAD,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );

  if (request.method === 'POST' && path === '/sig/affinity') {
    const threadId = mintThreadId();
    const response = await topology.send(context, threadId, '/probe/affinity', {
      method: 'POST',
      body: '{}',
    });
    const probe = (await response.json()) as Record<string, unknown>;
    return json({ status: response.status, ...probe });
  }

  // C-S6 (content policy): the SAME thread-DO boundary every signal lane
  // converges on refuses denied model-visible text and lets clean text through,
  // under real workerd with a real Breakwater gate.
  if (request.method === 'POST' && path === '/sig/content-policy') {
    const threadId = mintThreadId();
    const send = async (contents: string) => {
      const response = await topology.send(
        context,
        threadId,
        '/signal/message',
        {
          method: 'POST',
          body: JSON.stringify({ contents, ifIdle: 'persist' }),
        },
      );
      return { status: response.status, body: await response.text() };
    };
    const denied = await send(`please ${SPIKE_DENIED_CONTENT} now`);
    const allowed = await send('an ordinary operator message');
    return json({ denied, allowed });
  }

  if (request.method === 'POST' && path === '/sig/malformed-thread') {
    let status = 0;
    try {
      const response = await topology.send(
        context,
        'bad/thread',
        '/probe/affinity',
        { method: 'POST', body: '{}' },
      );
      status = response.status;
    } catch (error) {
      status = (error as { status?: number }).status ?? -1;
    }
    return json({ status });
  }

  return null;
}

// --- Track F (M-005) goal probes -------------------------------------------
// LOCAL-ONLY worker-level probes, driven by
// scripts/spike-verify.mjs:
//  - F-S1 (read path): an objective SET via the real createObjectiveRouter lands
//    in mastra_thread_state; the DURABLE goal-step read path (resolveGoalStore
//    -> readObjective over our COMPOSED storage) returns exactly what was written.
//  - F-S2 (eviction): the record is in D1, so it survives a dev-server restart —
//    spike-verify kills+restarts and reads it back (a DO-evicted goal still sees
//    it, because it lives in D1, not an in-process registry).
//  - F-S3 (fail-closed): a malformed thread target is 404 + audited; an over-cap
//    maxRuns is rejected + audited.
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

  const ctxFor = (role: ApprovalActor['role']): ActorContext =>
    actorContextForPrincipal({ kind: 'human', id: 'goal-probe', role }, env);

  // The SAME D1 domain a durable host wires (createSignalStorageDomains uses it).
  const store = new D1ThreadStateStorage(env.DB as unknown as never);

  // Drive the REAL objective router (full P6-lite gate), capturing the audit.
  const driveSet = async (
    body: Record<string, unknown>,
    maxRunsCap?: number,
    threadId = GOAL_THREAD_ID,
  ): Promise<{ status: number; record: unknown; audited: string[] }> => {
    const events: ObjectiveAuditEvent[] = [];
    const context = ctxFor('operator');
    if (threadId === GOAL_THREAD_ID) {
      await context.claimResource('thread', threadId);
    }
    const router = createObjectiveRouter({
      resolve: async () => context,
      store,
      validateThreadTarget: async (_context, target) => {
        if (target.threadId !== GOAL_THREAD_ID) {
          throw new RunRouteError(404, 'agent not found');
        }
      },
      audit: (event) => {
        events.push(event);
      },
      executionFence: executionFenceForEnv(env),
      ...(maxRunsCap !== undefined ? { maxRunsCap } : {}),
    });
    const res = await router(
      new Request(
        `http://do/api/threads/${encodeURIComponent(threadId)}/goal`,
        {
          method: 'PUT',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body),
        },
      ),
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
    return json(await driveSet({ objective: GOAL_OBJECTIVE, maxRuns: 5 }));
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

  // F-S3: malformed memory identity -> 404 + audited.
  if (request.method === 'POST' && path === '/goal/malformed-target') {
    return json(
      await driveSet({ objective: 'invalid target' }, undefined, 'bad/thread'),
    );
  }

  // F-S3: a maxRuns above the host cap -> 400 + audited.
  if (request.method === 'POST' && path === '/goal/over-cap') {
    return json(await driveSet({ objective: 'too big', maxRuns: 999 }, 10));
  }

  return null;
}

// --- Track D (M-006) schedule probes ---------------------------------------
// LOCAL-ONLY worker-level probes, driven by
// scripts/spike-verify.mjs, over the flowsafe-owned D1 schedules domain:
//  - D-S1 (single claim): two CONCURRENT ticks over one due schedule allow one
//    CAS claimant, one 'published' trigger row, and one nextFireAt advance on
//    real workerd + D1. Dispatch crash recovery has separate semantics.
//  - D-S2 (barrier + opaque IDs): a workflow-target schedule fires through the
//    DO's RunnerRuntime with a fresh UUID runId; a reserved key
//    planted directly in the ROW's stored requestContext (a compromised row) is
//    ABSENT from the executing leg — the verified source strips reserved keys
//    before the DO layers benign stored context below its runtime-derived scope;
//    the same verified target supplies core's initial workflow state.
//  - D-S3: an agent target reaches the same per-thread runtime-driven loop and
//    approval bridge as an interactive start.

// Every schedule probe leaves its recurring row active so its trigger evidence
// survives. If a later stage crosses a cron-minute boundary that row falls due
// again, and the next probe's tick legitimately claims it — which is how O2
// observed `due:2, fired:2`. Pausing predecessors at the probe ENTRY (rather
// than inside one route, where it depended on step order) keeps each probe's
// counters scoped to the schedule it creates, and preserves the earlier rows'
// trigger history.
async function pausePriorProbeSchedules(
  store: D1SchedulesStorage,
): Promise<void> {
  for (const existing of await store.listSchedules({ status: 'active' })) {
    await store.updateSchedule(existing.id, { status: 'paused' });
  }
}

const SCHED_PROBE_ROUTES = new Set([
  '/sched/concurrent-claim',
  '/sched/barrier',
  '/sched/agent',
]);

async function handleScheduleProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const path = new URL(request.url).pathname;
  // Decide the route BEFORE any side effect: an unmatched /sched/* path must
  // not build the store or pause the probe's schedules on its way to a 404.
  if (request.method !== 'POST' || !SCHED_PROBE_ROUTES.has(path)) return null;
  const store = new D1SchedulesStorage(env.DB as unknown as never);
  const runTopology = createDoRunTopology(
    env.RUNNER,
    env.DEPLOYMENT_IDENTITY_SECRET,
  );
  const now = Date.now();
  await pausePriorProbeSchedules(store);

  if (path === '/sched/concurrent-claim') {
    const id = `schedule_${crypto.randomUUID()}`;
    const ownerContext = actorContextForPrincipal(
      { kind: 'human', id: 'schedule-probe-owner', role: 'operator' },
      env,
    );
    await ownerContext.claimResource('schedule', id);
    await store.createSchedule(
      scheduleWithCreatorRole(
        {
          id,
          target: { type: 'workflow', workflowId: 'sched-echo', inputData: {} },
          cron: '* * * * *',
          status: 'active',
          nextFireAt: now - 1000,
          createdAt: now,
          updatedAt: now,
        },
        'operator',
      ),
    );
    let fireCount = 0;
    const tick = createScheduleTick({
      store,
      targetPolicy: scheduleTargetPolicy,
      deploymentTag: env.DEPLOYMENT_TENANT,
      executionFence: executionFenceForEnv(env),
      start: async ({
        scheduleId,
        dispatchId,
        workflowId,
        runId,
        inputData,
      }) => {
        const context = await actorContextForRegisteredOwner(
          {
            kind: 'system',
            id: SYSTEM_PRINCIPAL_ID,
            purpose: 'scheduled-workflow-execution',
          },
          env,
          'schedule',
          scheduleId,
        );
        fireCount += 1;
        return runTopology.start({
          workflowId,
          runId,
          inputData,
          principal: context.principal,
          scheduleId,
          dispatchId,
        });
      },
      status: (ref) =>
        ref.target === 'workflow'
          ? runTopology.dispatchStatus(ref.workflowId, ref.runId)
          : Promise.resolve(undefined),
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

  if (path === '/sched/barrier') {
    const id = `schedule_${crypto.randomUUID()}`;
    const ownerContext = actorContextForPrincipal(
      { kind: 'human', id: 'schedule-probe-owner', role: 'operator' },
      env,
    );
    await ownerContext.claimResource('schedule', id);
    await store.createSchedule(
      scheduleWithCreatorRole(
        {
          id,
          target: {
            type: 'workflow',
            workflowId: 'sched-echo',
            inputData: {},
            initialState: { fromSchedule: true },
            // A reserved key planted in the ROW (simulating a compromised row that
            // bypassed the facade's create-time rejection) + a benign one.
            requestContext: {
              [BREAKWATER_CONNECTOR_GRANTS_KEY]: [
                {
                  scope: 'run',
                  connectorId: 'forged-connector',
                  isolationScope: 'spike',
                  workflowId: 'sched-echo',
                  runId: 'spike_forged',
                },
              ],
              'sched.note': 'benign',
            },
          },
          cron: '* * * * *',
          status: 'active',
          nextFireAt: now - 1000,
          createdAt: now,
          updatedAt: now,
        },
        'operator',
      ),
    );
    let firedRunId: string | undefined;
    let firedStatus: string | undefined;
    let firedLeg: unknown;
    const tick = createScheduleTick({
      store,
      targetPolicy: scheduleTargetPolicy,
      deploymentTag: env.DEPLOYMENT_TENANT,
      executionFence: executionFenceForEnv(env),
      // Fire through the DO topology (the production path) — the tick mints the
      // opaque runId and the DO runs sched-echo, echoing its leg context keys.
      start: async ({
        scheduleId,
        dispatchId,
        workflowId,
        runId,
        inputData,
      }) => {
        const context = await actorContextForRegisteredOwner(
          {
            kind: 'system',
            id: SYSTEM_PRINCIPAL_ID,
            purpose: 'scheduled-workflow-execution',
          },
          env,
          'schedule',
          scheduleId,
        );
        firedRunId = runId;
        const summary = await runTopology.start({
          workflowId,
          runId,
          inputData,
          principal: context.principal,
          scheduleId,
          dispatchId,
        });
        firedStatus = summary.status;
        firedLeg = summary.result;
        return summary;
      },
      status: (ref) =>
        ref.target === 'workflow'
          ? runTopology.dispatchStatus(ref.workflowId, ref.runId)
          : Promise.resolve(undefined),
    });
    const result = await tick();
    return json({
      fired: result.fired,
      runId: firedRunId ?? null,
      status: firedStatus ?? null,
      leg: firedLeg ?? null,
    });
  }

  if (path === '/sched/agent') {
    // `?entryPath=` drives the NEGATIVE half: the same SYSTEM principal on an
    // entry path SPIKE_AGENT_META never declared must be refused at the host.
    const requestedEntry = new URL(request.url).searchParams.get('entryPath');
    const id = `schedule_${crypto.randomUUID()}`;
    const threadId = mintThreadId();
    const resourceId = resourceIdFromKey(threadId);
    // A threaded schedule fires only through an EXISTING binding, so bind the
    // thread with a human start first. That is also what makes the probe
    // meaningful: the thread belongs to a person, and the later unattended fire
    // must still arrive as SYSTEM rather than inheriting that person's role.
    const ownerContext = actorContextForPrincipal(
      {
        kind: 'human',
        id: 'sched-owner',
        role: 'operator',
      },
      env,
    );
    const topology = createAgentThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
      {
        startIdempotency: startIdempotencyForEnv(env),
        executionFence: executionFenceForEnv(env),
      },
    );
    await ownerContext.claimResource('thread', threadId);
    await ownerContext.claimResource('resource', resourceId);
    const bindingRun = await topology.start(ownerContext, {
      agentId: SPIKE_AGENT_ID,
      prompt: 'Bind this thread.',
      entryPath: 'http.start',
      threadId,
      resourceId,
    });
    if (!bindingRun.approval) {
      throw new Error('thread binding run did not suspend for approval');
    }
    await buildApprovalService(
      approvalStoreFactory(env.DB).store(),
      env,
      undefined,
    ).decide(
      bindingRun.approval.id,
      { decision: 'approve' },
      { id: 'schedule-probe-reviewer', role: 'reviewer' },
    );
    const bindingStatus = await topology.status(ownerContext, {
      agentId: SPIKE_AGENT_ID,
      threadId,
      runId: bindingRun.runId,
    });
    if (bindingStatus?.summary.status !== 'success') {
      throw new Error('thread binding run did not complete after approval');
    }
    await ownerContext.claimResource('schedule', id);
    await store.createSchedule(
      scheduleWithCreatorRole(
        {
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
        },
        'operator',
      ),
    );
    // The schedule tick fires as SYSTEM automation, not as a synthetic human
    // operator — the agent it targets must have declared this entry.
    const principal: ExecutionPrincipal = {
      kind: 'system',
      id: SYSTEM_PRINCIPAL_ID,
      purpose: 'scheduled-agent-execution',
    };
    const context = await actorContextForRegisteredOwner(
      principal,
      env,
      'schedule',
      id,
    );
    if (requestedEntry && requestedEntry !== 'schedule.fire') {
      try {
        await topology.start(context, {
          agentId: SPIKE_AGENT_ID,
          prompt: 'This entry path must be refused.',
          entryPath: requestedEntry as AgentEntryPath,
          threadId,
          resourceId,
        });
      } catch (error) {
        if (error instanceof RunRouteError && error.status === 403) {
          return json({
            result: {
              due: 1,
              fired: 0,
              skipped: 0,
              failed: 1,
              deferred: 0,
              reconciled: 0,
              lost: 0,
            },
            scheduleId: id,
            threadId,
            resourceId,
            runId: null,
            runOwner: null,
            error: 'forbidden',
          });
        }
        throw error;
      }
      throw new Error('undeclared automated entry path was admitted');
    }
    const signalTopology = createThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    // A box, not a `let`: TypeScript's control-flow analysis cannot see an
    // assignment made inside a closure, so a `let` narrows to `null` at every
    // read below and types the response branch dead. Behavior is identical.
    const dispatched: { runId: string | null } = { runId: null };
    const tick = createScheduleTick({
      store,
      targetPolicy: scheduleTargetPolicy,
      deploymentTag: env.DEPLOYMENT_TENANT,
      executionFence: executionFenceForEnv(env),
      // Tripwire, the same shape as the scheduleId guards below: this probe
      // creates only an agent-target schedule, so a workflow fire means the
      // entry-level isolation broke. It is a tripwire, not the guarantee — the
      // tick classifies a start throw as `failed` only when the status seam
      // answers 404 for the unknown run; if that hop throws instead it counts
      // `deferred`. The due/trigger assertions in spike-verify are what
      // actually scope the result.
      start: async () => {
        throw new Error('agent probe: no workflow schedule may be due');
      },
      startAgent: async ({
        scheduleId,
        dispatchId,
        target,
        runId,
        topologyThreadId,
        threaded,
        entryPath,
        requestContext,
        streamRequestContext,
        providerOptions,
      }) => {
        if (scheduleId !== id) throw new Error('schedule identity mismatch');
        // Record the id the TICK minted, from this seam's own parameter and
        // BEFORE the topology hop — the same way signalAgent does. Reading it
        // back off the hop's RESULT would make the two sources below one
        // source, and the spike-verify assertion that ties the persisted
        // trigger row to the dispatched run would compare a value to itself.
        dispatched.runId = runId;
        const started = await topology.start(context, {
          agentId: target.agentId,
          runId,
          prompt: target.prompt,
          entryPath: (requestedEntry ?? entryPath) as typeof entryPath,
          scheduleId,
          dispatchId,
          threaded,
          requestContext,
          streamRequestContext,
          providerOptions,
          ...(!threaded ? { topologyThreadId } : {}),
          ...(threaded
            ? {
                threadId: target.threadId,
                resourceId: target.resourceId,
              }
            : {}),
        });
        return { runId: started.runId };
      },
      signalAgent: async ({ scheduleId, target, dispatchId, runId }) => {
        if (scheduleId !== id) throw new Error('schedule identity mismatch');
        dispatched.runId = runId;
        if (!target.threadId || !target.resourceId) {
          throw new Error('threaded schedule signal requires memory ids');
        }
        const response = await signalTopology.send(
          context,
          target.threadId,
          '/signal/schedule',
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              scheduleId,
              dispatchId,
              runId,
            }),
          },
        );
        if (!response.ok) {
          throw new Error(
            `agent schedule signal failed with status ${response.status}`,
          );
        }
        const payload = (await response.json()) as { receipt?: unknown };
        const receipt = parseScheduleAgentDispatchReceipt(payload.receipt);
        if (!receipt)
          throw new Error('agent schedule returned no valid receipt');
        return receipt;
      },
      status: async (ref) => {
        if (ref.target === 'workflow') {
          return runTopology.dispatchStatus(ref.workflowId, ref.runId);
        }
        if (ref.mode === 'signal') {
          const state = await store.agentScheduleDispatchState(
            ref.scheduleId,
            ref.dispatchId,
          );
          if (state.state === 'settled') {
            return {
              ...(state.receipt.runId !== undefined
                ? { runId: state.receipt.runId }
                : {}),
              dispatchReceipt: state.receipt,
            };
          }
          if (state.state === 'pending') {
            throw new Error('agent schedule dispatch remains pending');
          }
          return undefined;
        }
        return topology.dispatchStatus(context, {
          agentId: ref.agentId,
          threadId: ref.threadId,
          runId: ref.runId,
        });
      },
    });
    const result = await tick();
    // Two INDEPENDENT sources, deliberately: `runId`/`runOwner` come from the
    // in-memory dispatch seam (the id this handler watched the tick mint and
    // deliver to the thread DO), while `triggers` comes from the D1 trigger
    // rows the tick wrote. spike-verify asserting they name the same run is
    // what ties the persisted row to the run that was actually dispatched —
    // reading both from the trigger row would only compare a value to itself.
    const triggers = await store.listTriggers(id);
    const trigger = triggers[0];
    const runOwner = dispatched.runId
      ? await approvalStoreFactory(env.DB)
          .resources()
          .owner('run', dispatched.runId)
      : undefined;
    return json({
      result,
      scheduleId: id,
      threadId,
      resourceId,
      runId: dispatched.runId,
      runOwner: runOwner ?? null,
      error: trigger?.error ?? null,
      triggers: triggers.map((row) => ({
        outcome: row.outcome,
        runId: row.runId,
      })),
    });
  }

  return null;
}

// --- Suspension deadline probe (T1-T3) -------------------------------------
// LOCAL-ONLY worker-level probe reading the wake state a run's OWN DO holds for
// its suspension deadlines: the persisted fenced entry and the single alarm the
// object's two duties share. spike-verify asserts a suspension ARMS one before
// the process is killed, and that a run resumed by a real signal keeps none —
// neither is observable on the run surface, and both are exactly what the
// hand-written DurableObjectState stub in the unit tests cannot prove.
async function handleSuspensionDeadlineProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.pathname !== '/deadline/armed') {
    return null;
  }
  const workflowId = url.searchParams.get('workflowId');
  const runId = url.searchParams.get('runId');
  if (!isPathSafeId(workflowId) || !isPathSafeId(runId)) {
    return json({ error: 'path-safe workflowId and runId required' }, 404);
  }
  // The SAME address the run topology uses (idFromName(`${workflowId}:${runId}`)),
  // so this reads the storage of the very object that serves the run.
  const response = await fetchDeploymentObject(
    runStub(env, workflowId, runId),
    env,
    `http://do${SPIKE_SUSPENSION_DEADLINE_PATH}`,
  );
  return json({ status: response.status, armed: await response.json() });
}

// --- Idempotent-start execution count probe (FI1/FI2) ----------------------
// Reads the counter `counted-research` writes. LOCAL-ONLY and unauthenticated,
// like every other spike probe: it exposes nothing a run's own status does not,
// and exists because no published surface reports how many times a step ran.
async function handleExecutionCountProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (request.method !== 'GET' || url.pathname !== '/idempotent/executions') {
    return null;
  }
  const counterId = url.searchParams.get('counterId');
  if (!isPathSafeId(counterId)) {
    return json({ error: 'a path-safe counterId is required' }, 404);
  }
  try {
    const { results } = await env.DB.prepare(
      `SELECT executions FROM ${EXECUTION_COUNT_TABLE} WHERE id = ?`,
    )
      .bind(counterId)
      .all<{ executions: number }>();
    return json({ executions: results[0]?.executions ?? 0 });
  } catch (error) {
    // The table is created by the first counted step, so before any counted
    // run has executed, ZERO executions is the truth rather than a fault.
    if (!/no such table/i.test(String(error))) throw error;
    return json({ executions: 0 });
  }
}

// --- Execution fence control probe (F1) ------------------------------------
// LOCAL-ONLY worker-level fence control channel, in the same shape the
// published admin route serves (`GET`/`POST /admin/execution-fence`, CAS on
// `expected`).
//
// It is spike-local rather than the host-kit route because this worker composes
// its routers by hand and never calls createFlowsafeWorker, so it configures no
// MAINTENANCE_ADMIN_SECRET and mounts no /admin surface at all. What the spike
// exists to prove is the part unit tests cannot: that the fence state is
// DURABLE across process death and that the enforcement points refuse real HTTP
// requests on real workerd. Both go through the same ExecutionFenceStore the
// published route drives; only the authentication in front of it differs, and
// that is covered by flowsafe-worker.test.ts.
async function handleExecutionFenceProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/admin/execution-fence') return null;
  const fence = executionFenceForEnv(env);
  try {
    if (request.method === 'GET') {
      return json(executionFenceReadingPayload(await fence.read()));
    }
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405);
    }
    const body = (await request.json()) as {
      expected?: unknown;
      next?: unknown;
      proofKey?: unknown;
    };
    const reading = await fence.transition({
      expected: assertExecutionFenceState(body.expected, 'expected'),
      next: assertExecutionFenceState(body.next, 'next'),
      ...(body.proofKey === undefined ? {} : { proofKey: body.proofKey }),
    });
    return json({ state: reading.state });
  } catch (error) {
    return doErrorResponse(error);
  }
}

// --- Drain inventory probe (F2) --------------------------------------------
// LOCAL-ONLY worker-level read of the deployment's outstanding work, in the
// same shape the published `GET /admin/inventory` route serves (index with no
// category, keyset page with one). Spike-local for the same reason the fence
// probe is: this worker composes its routers by hand and mounts no
// authenticated /admin surface.
//
// What the spike proves that unit tests cannot: that the inventory reads the
// SAME D1 the runs, notifications, tasks, and reservations were really written
// to by real workerd traffic, that its counts move as that work drains, and
// that it stays answerable while the fence is refusing everything else.
async function handleInventoryProbe(
  request: Request,
  env: Env,
): Promise<Response | null> {
  const url = new URL(request.url);
  if (url.pathname !== '/admin/inventory') return null;
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  const inventory = new DeploymentInventory(env.DB as unknown as never);
  try {
    const category = url.searchParams.get('category');
    if (category === null || category === '') return json(inventory.index());
    if (!isInventoryCategory(category)) {
      return json({ error: `unknown inventory category '${category}'` }, 400);
    }
    const cursor = url.searchParams.get('cursor');
    const limit = url.searchParams.get('limit');
    return json(
      await inventory.read(category, {
        ...(cursor === null ? {} : { cursor }),
        ...(limit === null ? {} : { limit: Number(limit) }),
      }),
    );
  } catch (error) {
    return doErrorResponse(error);
  }
}

const handler: ExportedHandler<Env> = {
  async fetch(
    request: CfRequest,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    try {
      await ensureDeploymentIdentityBindings(env);
    } catch (error) {
      return doErrorResponse(error);
    }
    // Fetch-scope live fan-out sink (mirrors createFlowsafeWorker): each
    // successful approval mutation forwards to the deployment hub, kept alive by
    // ctx.waitUntil so the publish completes AFTER the mutation's response (a
    // fire-and-forget publish would be cancelled). Contained — a failed fan-out
    // logs and never fails the mutation.
    const hubTopology = createHubTopology(
      env.HUB,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    const streamSink: ApprovalStreamSink = (event) =>
      ctx.waitUntil(
        hubTopology.publish(event).catch((error: unknown) => {
          console.error('stream publish failed', error);
        }),
      );

    // AUTHENTICATE FIRST, then construct the actor-scoped service over the
    // deployment store.
    const resolve = createActorResolver({
      authenticate: bearerActorAuthenticator(staticTokenVerifier(SPIKE_ACTORS)),
      storeFactory: approvalStoreFactory(env.DB),
      deploymentTag: env.DEPLOYMENT_TENANT,
      buildService: (store) => buildApprovalService(store, env, streamSink),
    });
    const routed = request as unknown as Request;
    const runTopology = createDoRunTopology(
      env.RUNNER,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );

    // The fence control channel, ahead of every router: an operator has to be
    // able to move the fence on a deployment the fence is already refusing.
    const fenceProbe = await handleExecutionFenceProbe(routed, env);
    if (fenceProbe) return fenceProbe;

    // The drain proof, beside the control that holds the drain open: an
    // operator has to be able to READ what remains on a deployment that is
    // refusing everything else.
    const inventoryProbe = await handleInventoryProbe(routed, env);
    if (inventoryProbe) return inventoryProbe;

    const agentProbeResponse = await handleLiveAgentRoute(routed, env, resolve);
    if (agentProbeResponse) return agentProbeResponse;

    // Track B probes (B-S2 recovery seam, B-S3 _background rejection) — local,
    // unauthenticated, ahead of the routers.
    const bgProbe = await handleBackgroundTaskProbe(routed, env);
    if (bgProbe) return bgProbe;

    // Track C probes (C-S2 affinity, malformed-id fail-closed) — local,
    // unauthenticated, ahead of the routers.
    const sigProbe = await handleSignalProbe(routed, env);
    if (sigProbe) return sigProbe;

    // Track F probes (F-S1 read path, F-S2 eviction read, F-S3 fail-closed) —
    // local, unauthenticated, ahead of the routers.
    const goalProbe = await handleGoalProbe(routed, env);
    if (goalProbe) return goalProbe;

    // Track D probes (D-S1 single claim, D-S2 barrier + opaque IDs) — local,
    // unauthenticated, ahead of the routers.
    const schedProbe = await handleScheduleProbe(routed, env);
    if (schedProbe) return schedProbe;

    // Track E probes (E-S2 forged, E-S1 delivery, E-S3 rehydration)
    // — local, unauthenticated, ahead of the routers.
    const sigpProbe = await handleSignalProviderProbe(routed, env);
    if (sigpProbe) return sigpProbe;

    // Suspension deadline probe (T1-T3 armed-wake introspection) — local,
    // unauthenticated, ahead of the routers.
    const deadlineProbe = await handleSuspensionDeadlineProbe(routed, env);
    if (deadlineProbe) return deadlineProbe;

    // Idempotent-start execution count (FI1/FI2) — local, unauthenticated,
    // ahead of the routers.
    const executionCountProbe = await handleExecutionCountProbe(routed, env);
    if (executionCountProbe) return executionCountProbe;

    // Track E webhook ingress: the github webhook route TERMINATES on the Worker,
    // signature-authed (not bearer), route-absent when its secret is unset.
    const webhookResponse = await webhookRouter(env)(routed);
    if (webhookResponse) return webhookResponse;

    // Stream stage BEFORE approvals (same order as the composer). The Worker is
    // the SOLE ticket authority; the hub/runner DOs re-bind to their own
    // address identity. Every route is under /api/stream/, so it composes
    // ahead of the approval router without overlapping it.
    const streamResponse = await createStreamRouter({
      resolve,
      ticketSecret: env.STREAM_TICKET_SECRET,
      hub: env.HUB,
      runner: env.RUNNER,
      runStatus: runTopology.status,
      deploymentIdentitySecret: env.DEPLOYMENT_IDENTITY_SECRET,
    })(routed);
    if (streamResponse) return streamResponse;

    const agentResponse = await createAgentRouter({
      agents: [SPIKE_AGENT_META],
      resolve,
      topology: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotencyForEnv(env),
          executionFence: executionFenceForEnv(env),
        },
      ),
    })(routed);
    if (agentResponse) return agentResponse;

    const approvalResponse = await createApprovalRouter({ resolve })(routed);
    if (approvalResponse) return approvalResponse;

    // The shared run surface: server-minted opaque runIds and the start route's
    // 401 -> coarse-role -> per-workflow gate order,
    // and the suspension->approval bridge — all host-kit, zero spike-local
    // routing. Topology (the only host-specific part): every leg goes
    // through the run's DO stub.
    const runResponse = await createRunRouter({
      workflows: WORKFLOWS,
      resolve,
      systemPrincipalId: SYSTEM_PRINCIPAL_ID,
      start: runTopology.start,
      status: runTopology.status,
      resume: runTopology.resume,
      terminate: runTopology.terminate,
      startIdempotency: {
        store: startIdempotencyForEnv(env),
        live: runTopology.startLiveness,
        executionFence: executionFenceForEnv(env),
      },
    })(routed);
    if (runResponse) return runResponse;

    return json({ error: 'not found' }, 404);
  },
};

export default handler;
