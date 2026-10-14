import type {
  Request as CfRequest,
  D1Database,
  DurableObjectNamespace,
  DurableObjectState,
  ExecutionContext,
} from '@cloudflare/workers-types';
import {
  type AgentThreadStateStorage,
  createRemoteStepNotificationRecoveryTick,
  createThreadAgentHost,
  type ThreadAgentHost,
} from '../src/agent-host/index.js';
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
  type RunnerRuntime,
  startIdempotencyFor,
  ThreadDurableObject,
  type ThreadScope,
} from '../src/do-runner/index.js';
import { RemoteStepJournal } from '../src/do-runner/remote-step-journal.js';
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
  NOTIFICATION_PROOF_ENABLED?: string;
  RUNNER: DurableObjectNamespace;
  THREAD: DurableObjectNamespace;
  MAINTENANCE: DurableObjectNamespace;
}

const OPERATION_ID = 'staging-notification-enqueue-v1';
const AGENT_ID = 'staging-notification-agent';

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
    allowedToolIds: ['staging_notification'],
    verifyCompletion: async () => {
      throw new Error('staging notification completion is not configured');
    },
  });
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
      },
    );
    const journal = journalFor(env, app.runtime);
    this.#host = createThreadAgentHost({
      buildModules: () => [],
      storage: () => storage,
      stateStorage: () => {
        if (!this.state?.storage)
          throw new Error('thread storage is unavailable');
        return this.state.storage as AgentThreadStateStorage;
      },
      resourceAccess: () => approvalStoreFactoryFor(env.DB).resources(),
      approvalService: () => {
        throw new Error('staging agent continuation is unavailable');
      },
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
  systemPrincipalId: 'staging-notification-worker',
  buildVerifier: () => staticTokenVerifier(new Map()),
  maintenance: {
    sweepIntervalMs: 60_000,
    purgeIntervalMs: 60_000,
    remoteStepRecoveryIntervalMs: 60_000,
  },
  remoteStepRecoveryTick: (env: Env) => async () => {
    if (env.NOTIFICATION_PROOF_ENABLED !== 'true')
      return { scanned: 0, recovered: 0, unresolved: 0 };
    const journal = journalFor(env, init(env as never).runtime);
    await journal.initialize();
    return createRemoteStepNotificationRecoveryTick({
      journal,
      namespace: env.THREAD,
      deploymentIdentitySecret: env.DEPLOYMENT_IDENTITY_SECRET,
    })();
  },
  preRoutes: async (request: Request, env: Env) => {
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
    if (request.method === 'POST' && url.pathname === '/__staging/ensure') {
      await ensureRemoteStepNotificationRecoveryDuty(env);
      return Response.json({ armed: true });
    }
    return new Response(null, { status: 404 });
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
