// SPDX-License-Identifier: Apache-2.0

import { DurableObject } from 'cloudflare:workers';
import type { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import {
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
  type FlowsafeDurableAgent,
} from '@proofoftech/flowsafe/agent-runner';
import {
  type ApprovalActor,
  ApprovalService,
  approvalGrantProvider,
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
  type RunSummary,
  TENANT_ID_PATTERN,
  ThreadDurableObject,
  type ThreadScope,
  tenantOwnsMemoryId,
  tenantOwnsSaltedId,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  createHubTopology,
  createThreadTopology,
  reconcileApprovalsForSummary,
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

import { createStarterAgent, STARTER_AGENT_ID } from './agent.js';
import { modelConfig, SYSTEM_ACTOR_ID } from './config.js';
import { createComposedStorage, subscriptionStoreFactory } from './storage.js';
import { defineWorkflows } from './workflows.js';

const github = githubSignalProvider();
const BACKGROUND_ALARM_MS = 60_000;

function json(payload: unknown, status = 200): Response {
  return Response.json(payload, { status });
}

function readObject(request: Request): Promise<Record<string, unknown> | null> {
  return request
    .json<unknown>()
    .then((value) =>
      typeof value === 'object' && value !== null && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null,
    )
    .catch(() => null);
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
  #mastra?: Mastra;
  #agent?: FlowsafeDurableAgent;
  #currentScope?: ThreadScope;

  protected build(env: Env): InitResult {
    const storage = createComposedStorage(env.DB);
    this.#storage = storage;
    return init(
      { storage },
      {
        pubsub: createHostPubSub(),
        requestContextForRun: approvalGrantProvider(
          approvalStoreFactoryFor(env.DB).forTenant(this.tenantId),
        ),
      },
    );
  }

  #resourceId(scope: ThreadScope): string {
    return mintResourceId(scope.tenantId, scope.threadId);
  }

  #getAgent(scope: ThreadScope): FlowsafeDurableAgent {
    if (this.#agent) return this.#agent;
    if (!this.#storage) {
      throw new Error('thread storage is not initialized');
    }
    const bare = createStarterAgent({
      model: modelConfig(this.env),
      db: this.env.DB,
    });
    this.#mastra = new Mastra({
      storage: this.#storage,
      agents: { [STARTER_AGENT_ID]: bare },
      ...(scope.init.pubsub ? { pubsub: scope.init.pubsub } : {}),
    });
    const agent = this.#mastra.getAgent(STARTER_AGENT_ID);
    this.#agent = createFlowsafeDurableAgent({
      agent,
      runtime: scope.init.runtime,
      pubsub: scope.init.pubsub,
      threadRuntime: this.#mastra.agentThreadStreamRuntime,
      maxSteps: 1,
    });
    return this.#agent;
  }

  async #bridge(
    scope: ThreadScope,
    runId: string,
    resourceId?: string,
  ): Promise<RunSummary | null> {
    const summary = await scope.init.runtime.status(
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      runId,
    );
    if (!summary) return null;
    if (summary.status === 'suspended') {
      const service = new ApprovalService({
        store: approvalStoreFactoryFor(this.env.DB).forTenant(scope.tenantId),
        ...(this.env.STREAM_TICKET_SECRET
          ? {
              stream: (event) => createHubTopology(this.env.HUB).publish(event),
            }
          : {}),
      });
      const actor: ApprovalActor = {
        id: SYSTEM_ACTOR_ID,
        role: 'operator',
        tenantId: scope.tenantId,
      };
      await reconcileApprovalsForSummary(
        service,
        DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
        summary,
        actor,
        {
          kind: 'thread',
          threadId: scope.threadId,
          ...(resourceId ? { resourceId } : {}),
        },
      );
    }
    return summary;
  }

  async #startAgent(
    scope: ThreadScope,
    input: {
      runId?: unknown;
      resourceId?: unknown;
      prompt?: unknown;
      message?: StartIdleRunInput['message'];
      signal?: StartIdleRunInput['signal'];
      threaded?: unknown;
    },
  ): Promise<Response> {
    if (
      typeof input.runId !== 'string' ||
      !tenantOwnsSaltedId(scope.tenantId, input.runId)
    ) {
      return json({ error: 'run not found' }, 404);
    }
    const threaded = input.threaded !== false;
    const expectedResourceId = threaded ? this.#resourceId(scope) : undefined;
    if (input.resourceId !== expectedResourceId) {
      return json({ error: 'resource not found' }, 404);
    }

    const agent = this.#getAgent(scope);
    if (threaded && expectedResourceId) {
      const activeRunId = agent.getActiveThreadRunId({
        threadId: scope.threadId,
        resourceId: expectedResourceId,
      });
      if (activeRunId) {
        return json({ runId: activeRunId, joined: true });
      }
    }

    const content =
      typeof input.prompt === 'string'
        ? input.prompt
        : (input.message ??
          input.signal ??
          'Record the requested operation through the approved tool.');
    const memory =
      threaded && expectedResourceId
        ? { thread: scope.threadId, resource: expectedResourceId }
        : undefined;
    await agent.stream(
      content as Parameters<FlowsafeDurableAgent['stream']>[0],
      {
        runId: input.runId,
        toolChoice: 'required',
        ...(memory ? { memory } : {}),
      },
    );
    const summary = await this.#bridge(scope, input.runId, expectedResourceId);
    return json({ runId: input.runId, summary });
  }

  async #snapshotBindingMatches(
    scope: ThreadScope,
    runId: string,
    resourceId?: string,
  ): Promise<boolean> {
    if (!tenantOwnsSaltedId(scope.tenantId, runId)) {
      return false;
    }
    const workflows = await this.#storage?.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      runId,
    });
    const input = snapshot?.context.input as
      | {
          messageListState?: {
            memoryInfo?: {
              threadId?: unknown;
              resourceId?: unknown;
            } | null;
          };
        }
      | undefined;
    const memory = input?.messageListState?.memoryInfo;
    const matches = resourceId
      ? memory?.threadId === scope.threadId && memory.resourceId === resourceId
      : memory === null;
    return matches;
  }

  #signalRoutes = createThreadSignalRoutes({
    // DurableAgent narrows generate() options, so core's generic Agent type is
    // invariant even though the runtime object is an Agent subclass.
    resolveAgent: (scope): Agent => this.#getAgent(scope) as unknown as Agent,
    resolveResourceId: (scope) => this.#resourceId(scope),
    resolveNotificationsStorage: async () => {
      const storage = await this.#storage?.getStore('notifications');
      if (!storage) throw new Error('notifications storage is unavailable');
      return storage;
    },
    startIdleRun: async (input) => {
      const scope = this.#currentScope;
      if (!scope) throw new Error('thread scope is not initialized');
      const response = await this.#startAgent(scope, {
        runId: input.runId,
        resourceId: input.resourceId,
        message: input.message,
        signal: input.signal,
        threaded: true,
      });
      if (!response.ok) {
        throw new Error(`idle agent start failed with ${response.status}`);
      }
      const result = (await response.json()) as { runId?: unknown };
      return {
        runId: typeof result.runId === 'string' ? result.runId : input.runId,
      };
    },
  });

  protected async route(
    request: Request,
    scope: ThreadScope,
  ): Promise<Response> {
    this.#currentScope = scope;
    const url = new URL(request.url);

    if (request.method === 'POST' && url.pathname === '/agent/start') {
      const body = await readObject(request);
      return body
        ? this.#startAgent(scope, body)
        : json({ error: 'a JSON object body is required' }, 400);
    }

    if (request.method === 'GET' && url.pathname === '/agent/status') {
      const runId = url.searchParams.get('runId');
      const resourceId = url.searchParams.get('resourceId');
      if (
        !runId ||
        resourceId !== this.#resourceId(scope) ||
        !tenantOwnsMemoryId(scope.tenantId, resourceId)
      ) {
        return json({ error: 'not found' }, 404);
      }
      if (!(await this.#snapshotBindingMatches(scope, runId, resourceId))) {
        return json({ error: 'not found' }, 404);
      }
      const summary = await this.#bridge(scope, runId, resourceId);
      return summary ? json(summary) : json({ error: 'not found' }, 404);
    }

    if (request.method === 'POST' && url.pathname === '/agent/resume') {
      const body = await readObject(request);
      const runId = body?.runId;
      const resourceId = body?.resourceId;
      if (
        typeof runId !== 'string' ||
        (resourceId !== undefined && resourceId !== this.#resourceId(scope))
      ) {
        return json({ error: 'not found' }, 404);
      }
      if (
        !(await this.#snapshotBindingMatches(
          scope,
          runId,
          typeof resourceId === 'string' ? resourceId : undefined,
        ))
      ) {
        throw new Error(
          'agent snapshot memory binding does not match this thread',
        );
      }
      const step =
        typeof body?.step === 'string' ||
        (Array.isArray(body?.step) &&
          body.step.every((part) => typeof part === 'string'))
          ? body.step
          : undefined;
      const summary = await this.#getAgent(scope).resumeViaRuntime({
        runId,
        ...(step !== undefined ? { step } : {}),
        ...(body && 'resumeData' in body
          ? { resumeData: body.resumeData }
          : {}),
        ...(typeof resourceId === 'string'
          ? {
              memory: {
                thread: scope.threadId,
                resource: resourceId,
              },
            }
          : {}),
      });
      await this.#bridge(
        scope,
        runId,
        typeof resourceId === 'string' ? resourceId : undefined,
      );
      return json(summary);
    }

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
