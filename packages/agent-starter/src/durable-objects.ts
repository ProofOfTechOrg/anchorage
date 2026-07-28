// SPDX-License-Identifier: Apache-2.0

import { DurableObject } from 'cloudflare:workers';
import type { Agent } from '@mastra/core/agent';
import type { MessageListInput } from '@mastra/core/agent/message-list';
import { Mastra } from '@mastra/core/mastra';
import { AuditLogger } from '@proofoftech/breakwater';
import {
  createThreadAgentHost,
  type ThreadAgentHost,
} from '@proofoftech/flowsafe/agent-host';
import {
  ApprovalService,
  approvalGrantProvider,
  humanPrincipal,
} from '@proofoftech/flowsafe/approval-api';
import {
  BackgroundTaskHost,
  createBackgroundTaskD1Domains,
  createBackgroundTaskRoutes,
} from '@proofoftech/flowsafe/background-tasks';
import {
  createD1Storage,
  createHostPubSub,
  DurableObjectRunner,
  HubDurableObject,
  type InitResult,
  init,
  mintResourceId,
  RESERVED_TENANT_IDS,
  TENANT_ID_PATTERN,
  ThreadDurableObject,
  type ThreadScope,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  createHubTopology,
  createThreadTopology,
} from '@proofoftech/flowsafe/host-kit';
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
import { modelConfig, SYSTEM_ACTOR_ID } from './config.js';
import { createComposedStorage, subscriptionStoreFactory } from './storage.js';
import { defineWorkflows } from './workflows.js';

const github = githubSignalProvider();
const BACKGROUND_ALARM_MS = 60_000;

function json(payload: unknown, status = 200): Response {
  return Response.json(payload, { status });
}

function idleRunMessages(input: StartIdleRunInput): MessageListInput {
  if (input.message !== undefined) {
    if (typeof input.message === 'string') return input.message;
    if (Array.isArray(input.message)) {
      return [{ role: 'user', content: input.message }];
    }
    return [{ role: 'user', content: input.message.contents }];
  }
  if (input.signal !== undefined) {
    return [{ role: 'user', content: input.signal.contents }];
  }
  return 'Record the requested operation through the approved tool.';
}

export class StarterRunner extends DurableObjectRunner<Env> {
  protected build(env: Env) {
    return defineWorkflows(env, this.tenantId);
  }
}

export class StarterHub extends HubDurableObject<Env> {}

export class StarterSignalProviderHost extends SignalProviderHost<Env> {
  protected build(env: Env, tenantId: string): SignalProviderHostWiring {
    return {
      store: subscriptionStoreFactory(env.DB).forTenant(tenantId),
      topology: createThreadTopology(env.THREAD),
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
    const approvals = approvalStoreFactoryFor(env.DB).forTenant(this.tenantId);
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
        return this.state.storage;
      },
      approvalService: () => {
        this.#approvalService ??= new ApprovalService({
          store: approvals,
          ...(this.env.STREAM_TICKET_SECRET
            ? {
                stream: (event) =>
                  createHubTopology(this.env.HUB).publish(event),
              }
            : {}),
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

  #resourceId(scope: ThreadScope): string {
    return mintResourceId(scope.tenantId, scope.threadId);
  }

  #signalRoutes = createThreadSignalRoutes({
    resolveAgent: async (scope, agentId, entryPath): Promise<Agent> =>
      (await this.#host().resolveBoundAgent(scope, { agentId, entryPath }))
        .durableAgent as unknown as Agent,
    resolveResourceId: (scope) => this.#resourceId(scope),
    resolveNotificationsStorage: async () => {
      const storage = await this.#storage?.getStore('notifications');
      if (!storage) throw new Error('notifications storage is unavailable');
      return storage;
    },
    startIdleRun: async (input) => {
      const scope: ThreadScope = {
        threadId: input.threadId,
        tenantId: input.actor.tenantId,
        actor: input.actor,
        principal: humanPrincipal(input.actor),
        requestedBy: input.actor.id,
        init: this.#initResult(),
      };
      const result = await this.#host().start(scope, {
        agentId: input.agent.id,
        threadId: input.threadId,
        runId: input.runId,
        resourceId: input.resourceId ?? this.#resourceId(scope),
        messages: idleRunMessages(input),
        entryPath: input.entryPath,
      });
      return { runId: result.runId };
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

export class StarterBackgroundTasks extends DurableObject<Env> {
  #host?: Promise<BackgroundTaskHost>;

  #tenantId(): string {
    const name = this.ctx.id.name;
    if (
      !name ||
      !TENANT_ID_PATTERN.test(name) ||
      RESERVED_TENANT_IDS.includes(name)
    ) {
      throw new Error(
        'background-task hosts must be addressed with a valid, non-reserved tenantId',
      );
    }
    return name;
  }

  #ensureHost(): Promise<BackgroundTaskHost> {
    this.#host ??= this.#boot().catch((error: unknown) => {
      this.#host = undefined;
      throw error;
    });
    return this.#host;
  }

  async #boot(): Promise<BackgroundTaskHost> {
    const tenantId = this.#tenantId();
    const pubsub = createHostPubSub();
    const storage = createD1Storage({
      binding: this.env.DB,
      domains: createBackgroundTaskD1Domains({
        binding: this.env.DB,
        tenantId,
      }),
    });
    const mastra = new Mastra({ storage, pubsub });
    const host = new BackgroundTaskHost({
      mastra,
      pubsub,
      executors: {
        starter_echo: {
          execute: async (args) => ({ args }),
        },
      },
      execution: { tenantId },
      manager: {
        globalConcurrency: 10,
        perAgentConcurrency: 3,
        backpressure: 'queue',
        cleanup: {
          cleanupIntervalMs: BACKGROUND_ALARM_MS,
        },
      },
    });
    await host.boot();
    await this.ctx.storage.setAlarm(Date.now() + BACKGROUND_ALARM_MS);
    return host;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      const host = await this.#ensureHost();
      const route = createBackgroundTaskRoutes({
        manager: host.manager,
      });
      return (
        (await route(request, this.#tenantId())) ??
        json({ error: 'not found' }, 404)
      );
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'starter.background-task-error',
          reason: error instanceof Error ? error.message : String(error),
        }),
      );
      return json({ error: 'background task host unavailable' }, 500);
    }
  }

  async alarm(): Promise<void> {
    try {
      await (await this.#ensureHost()).onAlarm();
    } finally {
      await this.ctx.storage.setAlarm(Date.now() + BACKGROUND_ALARM_MS);
    }
  }
}
