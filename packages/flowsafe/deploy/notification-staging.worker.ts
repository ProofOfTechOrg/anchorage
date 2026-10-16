import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  DurableObjectState,
  ExecutionContext,
} from '@cloudflare/workers-types';
import type { Agent } from '@mastra/core/agent';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { createTool } from '@mastra/core/tools';
import { createGuardedAgent } from '@proofoftech/breakwater/agent';
import { AuditLogger } from '@proofoftech/breakwater/audit';
import { z } from 'zod';
import {
  type AgentModule,
  type AgentThreadStateStorage,
  createAgentThreadTopology,
  createRemoteStepNotificationRecoveryTick,
  createThreadAgentHost,
  type ThreadAgentHost,
} from '../src/agent-host/index.js';
import { createFlowsafeDurableAgent } from '../src/agent-runner/index.js';
import {
  ApprovalService,
  createPrincipalActorContext,
} from '../src/approval-api/index.js';
import { credentialsMatch } from '../src/do-runner/deployment-identity.js';
import {
  assertDeploymentIdentitySecret,
  createD1Storage,
  createHostPubSub,
  DurableObjectRunner,
  doErrorResponse,
  ensureDeploymentIdentityBindings,
  executionFenceFor,
  type InitResult,
  init,
  mutationEpochFromHeader,
  type RunnerRuntime,
  startIdempotencyFor,
  ThreadDurableObject,
  type ThreadScope,
} from '../src/do-runner/index.js';
import {
  REMOTE_STEP_MARKER,
  RemoteStepJournal,
} from '../src/do-runner/remote-step-journal.js';
import {
  approvalStoreFactoryFor,
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeWorker,
  ensureRemoteStepNotificationRecoveryDuty,
  type FlowsafeWorkerConfig,
  staticTokenVerifier,
} from '../src/host-kit/index.js';

interface Env {
  DB: D1Database;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  MAINTENANCE_ADMIN_SECRET: string;
  STAGING_PROBE_SECRET: string;
  STAGING_MUTATION_EPOCH: string;
  NOTIFICATION_PROOF_ENABLED?: string;
  CF_VERSION_METADATA: { id: string };
  RUNNER: DurableObjectNamespace;
  THREAD: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
}

const OPERATION_ID = 'staging-notification-enqueue-v1';
const AGENT_ID = 'staging-notification-agent';
const TOOL_ID = 'staging_notification';
const SYSTEM_ID = 'staging-notification-worker';
const RESULT_JSON = '{"completed":true}';
const EVIDENCE_JSON = '{"operation":"no-side-effect-staging-dispatch-v1"}';

function stagingAgent(): AgentModule {
  const audit = new AuditLogger({
    sink: (event) => console.log(JSON.stringify(event)),
  });
  const tool = createTool({
    id: TOOL_ID,
    description: 'Suspend for the host-owned staging notification',
    inputSchema: z.object({}),
    outputSchema: z.object({ completed: z.boolean() }),
    resumeSchema: z.object({ completed: z.boolean() }),
    execute: async (_input, context) => {
      if (context.agent?.resumeData) return { completed: true };
      if (!context.agent?.suspend)
        throw new Error('staging tool suspension is unavailable');
      await context.agent.suspend({
        remoteStep: REMOTE_STEP_MARKER,
        request: { notificationOperationId: OPERATION_ID },
      });
      throw new Error('staging tool suspension returned');
    },
  });
  const model: MastraModelConfig = {
    specificationVersion: 'v2',
    provider: 'flowsafe-staging',
    modelId: 'fixed-notification-tool',
    supportedUrls: {},
    doGenerate: async () => ({
      content: [
        {
          type: 'tool-call',
          toolCallId: `call_${crypto.randomUUID()}`,
          toolName: TOOL_ID,
          input: '{}',
        },
      ],
      finishReason: 'tool-calls',
      usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      warnings: [],
    }),
    doStream: async () => ({
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: 'stream-start', warnings: [] });
          controller.enqueue({
            type: 'tool-call',
            toolCallId: `call_${crypto.randomUUID()}`,
            toolName: TOOL_ID,
            input: '{}',
          });
          controller.enqueue({
            type: 'finish',
            finishReason: 'tool-calls',
            usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          });
          controller.close();
        },
      }),
    }),
  };
  return {
    meta: {
      id: AGENT_ID,
      title: 'Staging notification agent',
      description: 'Produces a selected host-owned notification suspension',
      allowedRoles: ['operator'],
      allowedAutomation: [
        { kind: 'system', entryPaths: ['notification.dispatch'] },
      ],
    },
    agent: createGuardedAgent({
      id: AGENT_ID,
      name: 'Staging notification agent',
      instructions: 'Call staging_notification once.',
      model,
      tools: { [TOOL_ID]: tool },
      allowedRoles: ['operator'],
      allowedPrincipalKinds: ['system'],
      policies: [],
      audit,
      maxSteps: 1,
      toolChoice: 'required',
    }),
  };
}

export class NotificationStagingRunner extends DurableObjectRunner<Env> {
  protected build(env: Env): RunnerRuntime {
    return init(env as never).runtime;
  }

  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }
}

function journalFor(env: Env, runtime: ReturnType<typeof init>['runtime']) {
  return new RemoteStepJournal({
    db: env.DB as unknown as never,
    fence: executionFenceFor(env.DB as unknown as never),
    runtime,
    deploymentId: env.DEPLOYMENT_TENANT,
    tablePrefix: '',
    allowedToolIds: [TOOL_ID],
    verifyCompletion: async ({ invocation, resultJson, evidenceJson }) => {
      if (
        invocation.request === null ||
        typeof invocation.request !== 'object' ||
        (invocation.request as { notificationOperationId?: unknown })
          .notificationOperationId !== OPERATION_ID ||
        resultJson !== RESULT_JSON ||
        evidenceJson !== EVIDENCE_JSON
      )
        throw new Error(
          'staging no-op completion does not match the operation',
        );
    },
  });
}

function stagingRuntime(env: Env): {
  runtime: ReturnType<typeof init>['runtime'];
  workflowId: string;
} {
  const storage = createD1Storage({ binding: env.DB as unknown as never });
  const app = init(
    { storage },
    {
      executionFence: executionFenceFor(env.DB as unknown as never),
      startIdempotency: startIdempotencyFor(env.DB as unknown as never),
    },
  );
  const agents = {
    [AGENT_ID]: stagingAgent().agent as unknown as Agent,
  } as Record<typeof AGENT_ID, Agent>;
  const mastra = new Mastra({ storage, agents });
  const durable = createFlowsafeDurableAgent({
    agent: mastra.getAgentById(AGENT_ID),
    runtime: app.runtime,
    threadRuntime: mastra.agentThreadStreamRuntime,
  });
  return { runtime: app.runtime, workflowId: durable.getWorkflow().id };
}

export class NotificationStagingThread extends ThreadDurableObject<Env> {
  #host?: ThreadAgentHost;

  constructor(state: DurableObjectState, env: Env) {
    super(state as never, env);
  }

  protected build(env: Env): InitResult {
    const storage = createD1Storage({ binding: env.DB as unknown as never });
    const app = init(
      { storage },
      {
        executionFence: executionFenceFor(env.DB as unknown as never),
        startIdempotency: startIdempotencyFor(env.DB as unknown as never),
        pubsub: createHostPubSub(),
        requestContextForRun: (workflowId, runId, leg) => {
          if (!this.#host) throw new Error('thread agent host is unavailable');
          return this.#host.requestContextForRun()(workflowId, runId, leg);
        },
      },
    );
    const journal = journalFor(env, app.runtime);
    this.#host = createThreadAgentHost({
      buildModules: () => [stagingAgent()],
      storage: () => storage,
      stateStorage: () => {
        if (!this.state?.storage)
          throw new Error('thread storage is unavailable');
        return this.state.storage as AgentThreadStateStorage;
      },
      resourceAccess: () => approvalStoreFactoryFor(env.DB).resources(),
      approvalService: () =>
        new ApprovalService({
          store: approvalStoreFactoryFor(env.DB).store(),
          executionFence: executionFenceFor(env.DB as unknown as never),
          workflowTablePrefix: '',
        }),
      ...(env.NOTIFICATION_PROOF_ENABLED === 'true'
        ? {
            remoteSteps: journal,
            remoteStepNotification: {
              journal,
              agentId: AGENT_ID,
              operationId: OPERATION_ID,
              source: 'flowsafe-staging',
              kind: 'host-owned-notification',
              summary: 'Staging notification recovery',
              ensureScheduledRecovery: () =>
                ensureRemoteStepNotificationRecoveryDuty(env),
            },
          }
        : {}),
    });
    return app;
  }

  protected async route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response> {
    return (
      (await this.#host?.route(request, scope)) ??
      new Response(null, { status: 404 })
    );
  }

  protected async onAlarm(
    _env: Env,
    threadId: string,
    app: InitResult,
    deploymentTag?: string,
  ): Promise<void> {
    if (!this.#host) throw new Error('thread host is unavailable');
    await this.#host.recoverOwnership({ threadId, init: app, deploymentTag });
  }
}

const config = {
  workflows: [],
  mutationEpoch: (env: Env) =>
    mutationEpochFromHeader(env.STAGING_MUTATION_EPOCH),
  systemPrincipalId: SYSTEM_ID,
  buildVerifier: () => staticTokenVerifier(new Map()),
  maintenance: {
    sweepIntervalMs: 60_000,
    purgeIntervalMs: 60_000,
    remoteStepRecoveryIntervalMs: 60_000,
  },
  remoteStepRecoveryTick: (env: Env) => async () => {
    if (env.NOTIFICATION_PROOF_ENABLED !== 'true')
      return { scanned: 0, recovered: 0, unresolved: 0 };
    const journal = journalFor(env, stagingRuntime(env).runtime);
    await journal.initialize();
    return createRemoteStepNotificationRecoveryTick({
      journal,
      namespace: env.THREAD,
      deploymentIdentitySecret: env.DEPLOYMENT_IDENTITY_SECRET,
    })();
  },
  preRoutes: async (request: Request, env: Env, _ctx, kit) => {
    const url = new URL(request.url);
    if (!url.pathname.startsWith('/__staging/')) return null;
    if (env.NOTIFICATION_PROOF_ENABLED !== 'true')
      return new Response(null, { status: 404 });
    assertDeploymentIdentitySecret(
      env.STAGING_PROBE_SECRET,
      'staging probe credential',
    );
    const supplied = request.headers.get('x-flowsafe-staging-probe');
    if (
      !supplied ||
      !(await credentialsMatch(supplied, env.STAGING_PROBE_SECRET))
    )
      return new Response(null, { status: 403 });
    if (request.method === 'GET' && url.pathname === '/__staging/identity') {
      if (
        !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(
          env.CF_VERSION_METADATA?.id,
        )
      )
        return new Response(null, { status: 503 });
      return Response.json({
        versionId: env.CF_VERSION_METADATA.id,
        deploymentTenant: env.DEPLOYMENT_TENANT,
      });
    }
    if (request.method === 'POST' && url.pathname === '/__staging/ensure') {
      await ensureRemoteStepNotificationRecoveryDuty(env);
      return Response.json({ armed: true });
    }
    if (
      request.method !== 'POST' ||
      (url.pathname !== '/__staging/start' &&
        url.pathname !== '/__staging/complete')
    )
      return new Response(null, { status: 404 });
    const { runtime, workflowId } = stagingRuntime(env);
    const journal = journalFor(env, runtime);
    await journal.initialize();
    const storeFactory = approvalStoreFactoryFor(env.DB);
    const context = createPrincipalActorContext({
      principal: {
        kind: 'system',
        id: env.DEPLOYMENT_TENANT,
        purpose: 'private-notification-staging-proof',
      },
      storeFactory,
      deploymentTag: env.DEPLOYMENT_TENANT,
      mutationEpoch: mutationEpochFromHeader(env.STAGING_MUTATION_EPOCH),
      buildService: (store) =>
        new ApprovalService({
          store,
          executionFence: executionFenceFor(env.DB as unknown as never),
          workflowTablePrefix: '',
        }),
    });
    const topology = createAgentThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
      {
        startIdempotency: startIdempotencyFor(env.DB as unknown as never),
        executionFence: executionFenceFor(env.DB as unknown as never),
        remoteStepDispatch: journal,
        remoteSteps: journal,
      },
    );
    if (url.pathname === '/__staging/start') {
      const threadId = context.newThreadId();
      const resourceId = context.resourceIdFromKey(threadId);
      await context.claimResource('thread', threadId);
      await context.claimResource('resource', resourceId);
      const started = await topology.start(context, {
        agentId: AGENT_ID,
        prompt: 'Produce the host-owned staging notification.',
        entryPath: 'notification.dispatch',
        threadId,
        resourceId,
        threaded: true,
      });
      if (started.summary.status !== 'suspended')
        throw new Error('staging agent did not suspend');
      const invocation = await journal.register(
        {
          deploymentId: env.DEPLOYMENT_TENANT,
          tablePrefix: '',
          workflowId,
          agentId: started.agentId,
          threadId: started.threadId,
          resourceId: started.resourceId,
          runId: started.runId,
        },
        Date.now() + 120_000,
        context.principal,
      );
      return Response.json({
        invocationId: invocation.invocationId,
        runId: started.runId,
        threadId: started.threadId,
        status: started.summary.status,
      });
    }
    const body = (await request.json()) as { invocationId?: unknown };
    if (typeof body.invocationId !== 'string')
      return new Response(null, { status: 400 });
    const stored = await kit.contextForStoredInvocation(
      journal,
      body.invocationId,
    );
    if (
      stored?.principal.kind !== 'system' ||
      stored.principal.id !== context.principal.id
    )
      return new Response(null, { status: 404 });
    const ticket = await topology.issueRemoteStepDispatch(
      stored,
      body.invocationId,
      30_000,
    );
    const consumed = await topology.consumeRemoteStepDispatch(
      stored,
      ticket.permit,
    );
    if (consumed.kind !== 'accepted')
      throw new Error('staging no-op dispatch was already consumed');
    await journal.recordResult(
      body.invocationId,
      consumed.rawPermit,
      RESULT_JSON,
      EVIDENCE_JSON,
    );
    const resumed = await topology.resumeRemoteStep(stored, body.invocationId);
    return Response.json({
      invocationId: body.invocationId,
      status: resumed.summary.status,
    });
  },
} satisfies FlowsafeWorkerConfig<Env>;

export class NotificationStagingMaintenance extends createFlowsafeMaintenanceDurableObject(
  config,
) {}

const worker = createFlowsafeWorker<Env>(config);

export default {
  async fetch(request: CfRequest, env: Env, ctx: ExecutionContext) {
    try {
      await ensureDeploymentIdentityBindings(env);
      return await worker.fetch(request as unknown as Request, env, ctx);
    } catch (error) {
      return doErrorResponse(error);
    }
  },
};
