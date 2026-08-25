// SPDX-License-Identifier: Apache-2.0

import {
  createAgentRouter,
  createAgentThreadTopology,
} from '@proofoftech/flowsafe/agent-host';
import {
  type ActorResolver,
  encodeExecutionPrincipal,
} from '@proofoftech/flowsafe/approval-api';
import {
  deploymentIdentityHeaders,
  EXECUTION_PRINCIPAL_HEADER,
} from '@proofoftech/flowsafe/do-runner';
import { createObjectiveRouter } from '@proofoftech/flowsafe/goals';
import {
  createFlowsafeMaintenanceDurableObject,
  createFlowsafeWorker,
  createThreadTopology,
  type FlowsafeWorkerConfig,
  RunRouteError,
} from '@proofoftech/flowsafe/host-kit';
import { createScheduleRouter } from '@proofoftech/flowsafe/schedules';
import {
  createSignalProviderHostTopology,
  createSubscriptionRouter,
  createWebhookRouter,
  githubSignalProvider,
} from '@proofoftech/flowsafe/signal-providers';
import {
  createInMemorySignalRateLimiter,
  createSignalRouter,
} from '@proofoftech/flowsafe/signals';

import { STARTER_AGENT_META } from './agent.js';
import {
  audit,
  buildVerifier,
  githubResourceAllowed,
  signalAttributeAllowlist,
} from './config.js';
import {
  BACKGROUND_TASKS_INSTANCE_NAME,
  StarterBackgroundTasks,
  StarterHub,
  StarterRunner,
  StarterSignalProviderHost,
  StarterThread,
} from './durable-objects.js';
import { scheduleTargetPolicy, starterMaintenanceTick } from './maintenance.js';
import { starterRunnerLifecycleConfig } from './principal-context.js';
import {
  executionFence,
  schedulesStore,
  startIdempotency,
  subscriptionStoreFactory,
  threadStateStore,
} from './storage.js';
import { WORKFLOWS } from './workflows.js';

export {
  StarterBackgroundTasks,
  StarterHub,
  StarterRunner,
  StarterSignalProviderHost,
  StarterThread,
};

const github = githubSignalProvider();
const signalRateLimit = createInMemorySignalRateLimiter({
  limit: 120,
  windowMs: 60_000,
});
const webhookRouters = new WeakMap<
  Env['DB'],
  ReturnType<typeof createWebhookRouter>
>();

function json(payload: unknown, status = 200): Response {
  return Response.json(payload, { status });
}

function webhookRouter(env: Env) {
  let router = webhookRouters.get(env.DB);
  if (!router) {
    router = createWebhookRouter({
      providers: { github },
      subscriptions: subscriptionStoreFactory(env.DB).store(),
      topology: createThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      deploymentTag: env.DEPLOYMENT_TENANT,
      secretForProvider: (providerId) =>
        providerId === 'github' ? env.GITHUB_WEBHOOK_SECRET : undefined,
      audit,
      executionFence: executionFence(env.DB),
    });
    webhookRouters.set(env.DB, router);
  }
  return router;
}

async function handleBackgroundRoutes(
  request: Request,
  env: Env,
  resolve: ActorResolver,
): Promise<Response | null> {
  const url = new URL(request.url);
  const prefix = '/api/background-tasks';
  if (url.pathname !== prefix && !url.pathname.startsWith(`${prefix}/`)) {
    return null;
  }
  const context = await resolve(request);
  if (!context) return json({ error: 'authentication required' }, 401);
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  const id = env.BACKGROUND_TASKS.idFromName(BACKGROUND_TASKS_INSTANCE_NAME);
  const target = new URL(request.url);
  target.pathname = `/background-tasks${url.pathname.slice(prefix.length)}`;
  const headers = new Headers(request.headers);
  headers.delete('authorization');
  headers.set(
    EXECUTION_PRINCIPAL_HEADER,
    encodeExecutionPrincipal(context.principal),
  );
  const stampedHeaders = deploymentIdentityHeaders(
    env.DEPLOYMENT_IDENTITY_SECRET,
    headers,
  );
  const forwarded = new Request(target, {
    method: 'GET',
    headers: stampedHeaders,
    redirect: request.redirect,
  });
  return env.BACKGROUND_TASKS.get(id).fetch(forwarded);
}

const workerConfig = {
  ...starterRunnerLifecycleConfig,
  workflows: WORKFLOWS,
  buildVerifier,
  maintenance: {
    sweepIntervalMs: 5 * 60 * 1_000,
    purgeIntervalMs: 60 * 60 * 1_000,
    tickIntervalMs: 60 * 1_000,
  },
  preRoutes: async (request, env, _ctx, kit) => {
    const webhook = await webhookRouter(env)(request);
    if (webhook) return webhook;

    const subscriptions = await createSubscriptionRouter({
      resolve: kit.resolve,
      subscriptions: subscriptionStoreFactory(env.DB),
      validateThreadTarget: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotency(env.DB),
          executionFence: executionFence(env.DB),
        },
      ).requireBoundThread,
      knownProviders: ['github'],
      authorizeMutation: ({
        method,
        threadId,
        externalResourceId,
        resourceKey,
      }) => {
        if (method !== 'POST') return;
        if (resourceKey !== threadId) {
          throw new RunRouteError(
            400,
            'resourceKey must equal the threadId in this starter',
          );
        }
        if (!githubResourceAllowed(env, externalResourceId)) {
          throw new RunRouteError(
            403,
            'the external resource is not provisioned for this deployment',
          );
        }
      },
      reconcilePolling: createSignalProviderHostTopology(
        env.SIGNAL_PROVIDER_HOST,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ).reconcilePolling,
      audit,
    })(request);
    if (subscriptions) return subscriptions;

    const background = await handleBackgroundRoutes(request, env, kit.resolve);
    if (background) return background;
    return null;
  },
  buildAgentRouter: (resolve, env) =>
    createAgentRouter({
      agents: [STARTER_AGENT_META],
      resolve,
      topology: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotency(env.DB),
          executionFence: executionFence(env.DB),
        },
      ),
    }),
  buildSignalRouter: (resolve, env) =>
    createSignalRouter({
      resolve,
      topology: createThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      attributeAllowlist: signalAttributeAllowlist(env),
      rateLimit: signalRateLimit,
      audit,
    }),
  buildObjectiveRouter: (resolve, env) =>
    createObjectiveRouter({
      resolve,
      store: threadStateStore(env.DB),
      validateThreadTarget: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotency(env.DB),
          executionFence: executionFence(env.DB),
        },
      ).requireBoundThread,
      maxRunsCap: 50,
      audit,
      executionFence: executionFence(env.DB),
    }),
  buildScheduleRouter: (resolve, env) =>
    createScheduleRouter({
      resolve,
      store: schedulesStore(env.DB),
      targetPolicy: scheduleTargetPolicy,
      validateThreadTarget: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotency(env.DB),
          executionFence: executionFence(env.DB),
        },
      ).requireBoundThread,
      maxSchedules: 100,
      minFireIntervalMs: 60_000,
      audit,
      executionFence: executionFence(env.DB),
    }),
  scheduleTick: starterMaintenanceTick,
  backgroundTasks: {
    completedTtlMs: 60 * 60 * 1_000,
    failedTtlMs: 24 * 60 * 60 * 1_000,
  },
} satisfies FlowsafeWorkerConfig<Env>;

export class StarterMaintenance extends createFlowsafeMaintenanceDurableObject(
  workerConfig,
) {}

const worker = createFlowsafeWorker<Env>(workerConfig);

export default {
  fetch: (request, env, ctx) => worker.fetch(request, env, ctx),
} satisfies ExportedHandler<Env>;
