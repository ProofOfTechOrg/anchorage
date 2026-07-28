// SPDX-License-Identifier: Apache-2.0

import {
  createAgentApprovalResumer,
  createAgentRouter,
  createAgentThreadTopology,
} from '@proofoftech/flowsafe/agent-host';
import {
  RUN_START_ROLES,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '@proofoftech/flowsafe/approval-api';
import { createObjectiveRouter } from '@proofoftech/flowsafe/goals';
import {
  createDoRunTopology,
  createFlowsafeWorker,
  createThreadTopology,
  RunRouteError,
  withSubdomainCrossCheck,
} from '@proofoftech/flowsafe/host-kit';
import {
  createScheduleRouter,
  createScheduleTick,
} from '@proofoftech/flowsafe/schedules';
import {
  createSignalProviderHostTopology,
  createSubscriptionRouter,
  createWebhookRouter,
  githubSignalProvider,
} from '@proofoftech/flowsafe/signal-providers';
import {
  createInMemorySignalRateLimiter,
  createNotificationDispatchTick,
  createSignalRouter,
} from '@proofoftech/flowsafe/signals';

import { STARTER_AGENT_META } from './agent.js';
import {
  buildVerifier,
  githubResourceAllowed,
  PURGE_CRON,
  SWEEP_CRON,
  SYSTEM_ACTOR_ID,
  signalAttributeAllowlist,
  TICK_CRON,
} from './config.js';
import {
  StarterBackgroundTasks,
  StarterHub,
  StarterRunner,
  StarterSignalProviderHost,
  StarterThread,
} from './durable-objects.js';
import {
  notificationsStore,
  schedulesStore,
  subscriptionStoreFactory,
  threadStateStore,
} from './storage.js';
import { systemTenant, tenantForPrincipal } from './system-tenant.js';
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

function decoded(value: string): string | undefined {
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
}

function audit(event: unknown): void {
  console.log(JSON.stringify(event));
}

function webhookRouter(env: Env) {
  let router = webhookRouters.get(env.DB);
  if (!router) {
    router = createWebhookRouter({
      providers: { github },
      subscriptions: subscriptionStoreFactory(env.DB).system(),
      topology: createThreadTopology(env.THREAD),
      secretForProvider: (providerId) =>
        providerId === 'github' ? env.GITHUB_WEBHOOK_SECRET : undefined,
      audit,
    });
    webhookRouters.set(env.DB, router);
  }
  return router;
}

async function handleBackgroundRoutes(
  request: Request,
  env: Env,
  resolve: TenantResolver,
): Promise<Response | null> {
  const url = new URL(request.url);
  const prefix = '/api/background-tasks';
  if (url.pathname !== prefix && !url.pathname.startsWith(`${prefix}/`)) {
    return null;
  }
  const tenant = await resolve(request);
  if (!tenant) return json({ error: 'authentication required' }, 401);
  if (request.method !== 'GET') {
    return json({ error: 'method not allowed' }, 405);
  }
  const id = env.BACKGROUND_TASKS.idFromName(tenant.tenantId);
  const target = new URL(request.url);
  target.pathname = `/background-tasks${url.pathname.slice(prefix.length)}`;
  const headers = new Headers(request.headers);
  headers.delete('authorization');
  const forwarded = new Request(target, {
    method: 'GET',
    headers,
    redirect: request.redirect,
  });
  return env.BACKGROUND_TASKS.get(id).fetch(forwarded);
}

function subscriptionResolver(
  env: Env,
  resolve: TenantResolver,
): TenantResolver {
  return async (request) => {
    const tenant = await resolve(request);
    if (!tenant || request.method !== 'POST') return tenant;

    const segments = new URL(request.url).pathname.split('/').filter(Boolean);
    if (
      segments.length !== 4 ||
      segments[0] !== 'api' ||
      segments[1] !== 'threads' ||
      segments[3] !== 'subscriptions'
    ) {
      return tenant;
    }
    const threadId = decoded(segments[2] ?? '');
    if (
      !threadId ||
      !RUN_START_ROLES.includes(tenant.actor.role) ||
      !tenant.ownsMemoryId(threadId)
    ) {
      return tenant;
    }
    const constrainedTenant: TenantContext = {
      ...tenant,
      newResourceId: (resourceKey) => {
        if (resourceKey !== threadId) {
          throw new RunRouteError(
            400,
            'resourceKey must equal the threadId in this starter',
          );
        }
        return tenant.newResourceId(resourceKey);
      },
    };

    const body = await request
      .clone()
      .json<unknown>()
      .catch(() => undefined);
    if (
      typeof body !== 'object' ||
      body === null ||
      Array.isArray(body) ||
      !('providerId' in body) ||
      body.providerId !== 'github' ||
      !('externalResourceId' in body) ||
      typeof body.externalResourceId !== 'string'
    ) {
      return constrainedTenant;
    }
    if (!githubResourceAllowed(env, tenant.tenantId, body.externalResourceId)) {
      audit({
        type: 'signal-provider.subscription',
        tenantId: tenant.tenantId,
        actorId: tenant.actor.id,
        threadId,
        action: 'subscribe',
        outcome: 'rejected',
        providerId: 'github',
        externalResourceId: body.externalResourceId,
        reason: 'external-resource-not-owned',
        timestamp: new Date().toISOString(),
      });
      throw new TenantResolutionError(
        'the external resource is not provisioned for this tenant',
      );
    }
    return constrainedTenant;
  };
}

const worker = createFlowsafeWorker<Env>({
  workflows: WORKFLOWS,
  systemActorId: SYSTEM_ACTOR_ID,
  buildVerifier,
  crons: {
    sweep: SWEEP_CRON,
    purge: PURGE_CRON,
    tick: TICK_CRON,
  },
  wrapResolve: (resolve, env) =>
    env.TENANT_APEX_DOMAIN
      ? withSubdomainCrossCheck(resolve, {
          apexDomain: env.TENANT_APEX_DOMAIN,
        })
      : resolve,
  preRoutes: async (request, env, _ctx, kit) => {
    const webhook = await webhookRouter(env)(request);
    if (webhook) return webhook;

    const subscriptions = await createSubscriptionRouter({
      resolve: subscriptionResolver(env, kit.resolve),
      subscriptions: subscriptionStoreFactory(env.DB),
      knownProviders: ['github'],
      reconcilePolling: createSignalProviderHostTopology(
        env.SIGNAL_PROVIDER_HOST,
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
      topology: createAgentThreadTopology(env.THREAD),
    }),
  buildResumeRun: (fallback, env) =>
    createAgentApprovalResumer({
      fallback,
      agents: [STARTER_AGENT_META],
      topology: createAgentThreadTopology(env.THREAD),
      tenantForPrincipal: (principal) => tenantForPrincipal(env, principal),
    }),
  buildSignalRouter: (resolve, env) =>
    createSignalRouter({
      resolve,
      topology: createThreadTopology(env.THREAD),
      attributeAllowlist: signalAttributeAllowlist(env),
      rateLimit: signalRateLimit,
      audit,
    }),
  buildObjectiveRouter: (resolve, env) =>
    createObjectiveRouter({
      resolve,
      store: threadStateStore(env.DB),
      maxRunsCap: 50,
      audit,
    }),
  buildScheduleRouter: (resolve, env) =>
    createScheduleRouter({
      resolve,
      store: schedulesStore(env.DB),
      maxSchedulesPerTenant: 100,
      minFireIntervalMs: 60_000,
      audit,
    }),
  scheduleTick: (env) => {
    const runTopology = createDoRunTopology(env.RUNNER);
    const threadTopology = createThreadTopology(env.THREAD);
    const agentTopology = createAgentThreadTopology(env.THREAD);
    const schedules = createScheduleTick({
      store: schedulesStore(env.DB),
      start: runTopology.start,
      startAgent: async ({
        target,
        tenantId,
        runId,
        topologyThreadId,
        threaded,
        entryPath,
        requestContext,
        streamRequestContext,
      }) => {
        const started = await agentTopology.start(systemTenant(env, tenantId), {
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
      audit,
    });
    const notifications = createNotificationDispatchTick({
      storage: notificationsStore(env.DB),
      topology: threadTopology,
      resolveTenant: (tenantId) => systemTenant(env, tenantId),
      limit: 100,
    });
    return async () => ({
      schedules: await schedules(),
      notifications: await notifications(),
    });
  },
  backgroundTasks: {
    completedTtlMs: 60 * 60 * 1_000,
    failedTtlMs: 24 * 60 * 60 * 1_000,
  },
});

export default {
  fetch: (request, env, ctx) => worker.fetch(request, env, ctx),
  scheduled: (controller, env, ctx) => worker.scheduled(controller, env, ctx),
} satisfies ExportedHandler<Env>;
