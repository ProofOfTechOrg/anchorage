// SPDX-License-Identifier: Apache-2.0

import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  type Agent,
  createMessageSignal,
  createSignal,
} from '@mastra/core/agent';
import type { MessageListInput } from '@mastra/core/agent/message-list';
import { Mastra } from '@mastra/core/mastra';
import { AuditLogger } from '@proofoftech/breakwater';
import {
  type AgentThreadStateStorage,
  createThreadAgentHost,
  type ThreadAgentHost,
  type ThreadAgentStartInput,
} from '@proofoftech/flowsafe/agent-host';
import {
  ApprovalService,
  approvalGrantProvider,
  decodeExecutionPrincipal,
  type ExecutionPrincipal,
  principalMayAccess,
} from '@proofoftech/flowsafe/approval-api';
import {
  BackgroundTaskHost,
  createBackgroundTaskD1Domains,
  createBackgroundTaskRoutes,
} from '@proofoftech/flowsafe/background-tasks';
import {
  createD1Storage,
  createHostPubSub,
  DoStatusError,
  type DurableObjectRunLifecycleHooks,
  DurableObjectRunner,
  doErrorResponse,
  EXECUTION_PRINCIPAL_HEADER,
  HubDurableObject,
  type InitResult,
  init,
  resourceIdFromKey,
  ThreadDurableObject,
  type ThreadScope,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  createFlowsafeRunnerLifecycle,
  createHubTopology,
  createThreadTopology,
} from '@proofoftech/flowsafe/host-kit';
import {
  canPersistScheduledAgentSignal,
  createScheduleStartSource,
  type ScheduleAgentDispatchReceipt,
} from '@proofoftech/flowsafe/schedules';
import {
  githubSignalProvider,
  SignalProviderHost,
  type SignalProviderHostWiring,
} from '@proofoftech/flowsafe/signal-providers';
import {
  createThreadSignalRoutes,
  type StartIdleRunInput,
} from '@proofoftech/flowsafe/signals';

import { createStarterAgentModule } from './agent.js';
import { modelConfig, SYSTEM_PRINCIPAL_ID } from './config.js';
import { starterRunnerLifecycleConfig } from './principal-context.js';
import {
  createComposedStorage,
  schedulesStore,
  subscriptionStoreFactory,
} from './storage.js';
import { defineWorkflows } from './workflows.js';

const github = githubSignalProvider();
const BACKGROUND_ALARM_MS = 60_000;

function json(payload: unknown, status = 200): Response {
  return Response.json(payload, { status });
}

function idleRunMessages(input: StartIdleRunInput): MessageListInput {
  if (input.message !== undefined) {
    return createMessageSignal(input.message);
  }
  if (input.signal !== undefined) {
    return createSignal(input.signal);
  }
  return 'Record the requested operation through the approved tool.';
}

export function discardStarterScheduleDispatch(
  env: Env,
  scheduleId: string,
  dispatchId: string,
  runId: string,
): Promise<void> {
  return schedulesStore(env.DB).discardAgentScheduleDispatch(
    scheduleId,
    dispatchId,
    runId,
  );
}

export function idleRunScheduleDispatch(input: {
  scheduleId?: string;
  dispatchId?: string;
}): Pick<
  ThreadAgentStartInput,
  'scheduleId' | 'dispatchId' | 'scheduleDispatchLease'
> {
  return {
    ...(input.scheduleId !== undefined ? { scheduleId: input.scheduleId } : {}),
    ...(input.dispatchId !== undefined ? { dispatchId: input.dispatchId } : {}),
    ...(input.scheduleId !== undefined && input.dispatchId !== undefined
      ? { scheduleDispatchLease: 'executing' as const }
      : {}),
  };
}

export class StarterRunner extends DurableObjectRunner<Env> {
  protected build(env: Env) {
    return defineWorkflows(env);
  }

  protected runOwnership(env: Env) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected scheduleSource(env: Env) {
    return createScheduleStartSource(schedulesStore(env.DB));
  }

  protected runLifecycle(env: Env): DurableObjectRunLifecycleHooks {
    return createFlowsafeRunnerLifecycle(starterRunnerLifecycleConfig, env, {
      waitUntil: this.state?.waitUntil?.bind(this.state),
    });
  }
}

export class StarterHub extends HubDurableObject<Env> {}

export class StarterSignalProviderHost extends SignalProviderHost<Env> {
  protected build(env: Env): SignalProviderHostWiring {
    return {
      store: subscriptionStoreFactory(env.DB).store(),
      topology: createThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      providers: [github],
    };
  }
}

export class StarterThread extends ThreadDurableObject<Env> {
  #storage?: ReturnType<typeof createComposedStorage>;
  #agentHost?: ThreadAgentHost;
  #approvalService?: ApprovalService;
  #threadInit?: InitResult;

  protected build(env: Env): InitResult {
    const storage = createComposedStorage(env.DB);
    this.#storage = storage;
    const audit = new AuditLogger({
      sink: (event) => {
        console.log(JSON.stringify(event));
      },
    });
    const approvals = approvalStoreFactoryFor(env.DB).store();
    const agentHost = createThreadAgentHost({
      buildModules: () => [
        createStarterAgentModule({
          model: modelConfig(this.env),
          db: this.env.DB,
          audit,
        }),
      ],
      storage: () => storage,
      stateStorage: () => {
        if (!this.state?.storage) {
          throw new Error('thread Durable Object storage is unavailable');
        }
        return this.state.storage as unknown as AgentThreadStateStorage;
      },
      resourceAccess: () => approvalStoreFactoryFor(env.DB).resources(),
      scheduleSource: () => createScheduleStartSource(schedulesStore(env.DB)),
      discardScheduleDispatch: (scheduleId, dispatchId, runId) =>
        discardStarterScheduleDispatch(env, scheduleId, dispatchId, runId),
      approvalService: () => {
        this.#approvalService ??= new ApprovalService({
          store: approvals,
          ...(this.env.STREAM_TICKET_SECRET
            ? {
                stream: (event) =>
                  createHubTopology(
                    this.env.HUB,
                    this.env.DEPLOYMENT_IDENTITY_SECRET,
                  ).publish(event),
              }
            : {}),
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

  #resourceId(scope: ThreadScope): string {
    return resourceIdFromKey(scope.threadId);
  }

  #signalRoutes = createThreadSignalRoutes({
    resolveAgent: async (scope, agentId, entryPath): Promise<Agent> =>
      (await this.#host().resolveBoundAgent(scope, { agentId, entryPath }))
        .durableAgent as unknown as Agent,
    resolveResourceId: (scope) => this.#resourceId(scope),
    resolveBlockingRun: (scope) => this.#host().blockingRun(scope),
    serializeDispatch: (_scope, operation) =>
      this.#host().serializeDispatch(operation),
    resolveScheduleRunStatus: (scope, input) =>
      this.#host().scheduleDispatchStatus(scope, input),
    resolveScheduleTarget: async (_scope, input) => {
      const target = await createScheduleStartSource(
        schedulesStore(this.env.DB),
      ).resolveScheduleTarget(input.scheduleId, input.dispatchId, input.runId);
      return target?.type === 'agent' ? target : undefined;
    },
    canPersist: async (scope) => {
      const owner = await approvalStoreFactoryFor(this.env.DB)
        .resources()
        .owner('thread', scope.threadId);
      return (
        owner?.kind === scope.principal.kind && owner.id === scope.principal.id
      );
    },
    canPersistSchedule: (scope, input) =>
      canPersistScheduledAgentSignal(
        createScheduleStartSource(schedulesStore(this.env.DB)),
        approvalStoreFactoryFor(this.env.DB).resources(),
        { ...input, threadId: scope.threadId },
      ),
    resolveNotificationsStorage: async () => {
      const storage = await this.#storage?.getStore('notifications');
      if (!storage) throw new Error('notifications storage is unavailable');
      return storage;
    },
    resolveScheduleDispatchStore: () => {
      const store = schedulesStore(this.env.DB);
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
      const scope: ThreadScope = {
        threadId: input.threadId,
        deploymentTag: this.env.DEPLOYMENT_TENANT,
        principal: input.principal,
        init: this.#initResult(),
      };
      const result = await this.#host().start(scope, {
        agentId: input.agent.id,
        threadId: input.threadId,
        runId: input.runId,
        resourceId: input.resourceId ?? this.#resourceId(scope),
        messages: idleRunMessages(input),
        entryPath: input.entryPath,
        ...idleRunScheduleDispatch(input),
        safeContext: input.safeContext,
      });
      return { runId: result.runId, status: result.summary.status };
    },
  });

  #initResult(): InitResult {
    if (!this.#threadInit) {
      throw new Error('thread agent host is not initialized');
    }
    return this.#threadInit;
  }

  protected async route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response> {
    const agent = await this.#host().route(request, scope);
    if (agent) return agent;
    const signal = await this.#signalRoutes(request, scope);
    return signal ?? json({ error: 'not found' }, 404);
  }
}

export const BACKGROUND_TASKS_INSTANCE_NAME = 'deployment-background-tasks';

class BackgroundTaskIdentityError extends DoStatusError {
  readonly status = 403;
}

export class StarterBackgroundTasks {
  protected readonly ctx: DurableObjectState;
  protected readonly env: Env;
  #host?: Promise<BackgroundTaskHost>;

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
  }

  #assertInstanceName(): void {
    if (this.ctx.id.name !== BACKGROUND_TASKS_INSTANCE_NAME) {
      throw new BackgroundTaskIdentityError(
        `background-task host must be addressed as '${BACKGROUND_TASKS_INSTANCE_NAME}'`,
      );
    }
  }

  #ensureHost(): Promise<BackgroundTaskHost> {
    this.#host ??= this.#boot().catch((error: unknown) => {
      this.#host = undefined;
      throw error;
    });
    return this.#host;
  }

  async #boot(): Promise<BackgroundTaskHost> {
    await this.ctx.storage.setAlarm(Date.now() + BACKGROUND_ALARM_MS);
    const pubsub = createHostPubSub();
    const storage = createD1Storage({
      binding: this.env.DB,
      domains: createBackgroundTaskD1Domains({
        binding: this.env.DB,
      }),
    });
    await storage.init();
    const mastra = new Mastra({ storage, pubsub });
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      executors: {
        starter_echo: {
          execute: async (args) => ({ args }),
        },
      },
      execution: true,
      manager: {
        globalConcurrency: 10,
        perAgentConcurrency: 3,
        backpressure: 'queue',
        cleanup: {
          cleanupIntervalMs: BACKGROUND_ALARM_MS,
        },
      },
    });
    try {
      await host.boot();
    } catch (error) {
      try {
        await host.shutdown();
      } catch (shutdownError) {
        console.error('background-task boot rollback failed', shutdownError);
      }
      throw error;
    }
    return host;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await verifyDurableObjectDeploymentRequest(request, this.ctx, this.env);
      this.#assertInstanceName();
      const encodedPrincipal = request.headers.get(EXECUTION_PRINCIPAL_HEADER);
      let principal: ExecutionPrincipal | undefined;
      try {
        principal = encodedPrincipal
          ? decodeExecutionPrincipal(encodedPrincipal)
          : undefined;
      } catch {
        throw new BackgroundTaskIdentityError(
          'background-task request has no trusted principal',
        );
      }
      if (!principal) {
        throw new BackgroundTaskIdentityError(
          'background-task request has no trusted principal',
        );
      }
      const resources = approvalStoreFactoryFor(this.env.DB).resources();
      const host = await this.#ensureHost();
      const route = createBackgroundTaskRoutes({
        manager: host.manager,
        authorize: async (scope) => {
          const checks: Promise<boolean>[] = [];
          if (scope.runId !== undefined) {
            checks.push(
              resources
                .owner('run', scope.runId)
                .then(
                  (owner) =>
                    owner !== undefined &&
                    principalMayAccess(principal, owner, 'read'),
                ),
            );
          }
          if (scope.threadId !== undefined) {
            checks.push(
              resources
                .owner('thread', scope.threadId)
                .then(
                  (owner) =>
                    owner !== undefined &&
                    principalMayAccess(principal, owner, 'read'),
                ),
            );
          }
          return (
            checks.length > 0 && (await Promise.all(checks)).every(Boolean)
          );
        },
      });
      return (await route(request)) ?? json({ error: 'not found' }, 404);
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'starter.background-task-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
      return doErrorResponse(error);
    }
  }

  async alarm(): Promise<void> {
    this.#assertInstanceName();
    await this.ctx.storage.setAlarm(Date.now() + BACKGROUND_ALARM_MS);
    await verifyDurableObjectDeploymentIdentity(this.ctx, this.env);
    const host = await this.#ensureHost();
    await host.onAlarm();
  }
}
