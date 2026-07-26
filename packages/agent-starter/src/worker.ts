// SPDX-License-Identifier: Apache-2.0

import {
  createApprovalRouter,
  defaultResumeData,
  RUN_START_ROLES,
  type TenantContext,
  TenantResolutionError,
  type TenantResolver,
} from '@proofoftech/flowsafe/approval-api';
import { createObjectiveRouter } from '@proofoftech/flowsafe/goals';
import {
  approvalStoreFactoryFor,
  assertNoClientMemoryIds,
  buildHostApprovalService,
  createDoRunTopology,
  createFlowsafeWorker,
  createHubTopology,
  createThreadTopology,
  type DoRunTopology,
  doSummary,
  type FlowsafeWorkerContext,
  numberVar,
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

import { STARTER_AGENT_ID } from './agent.js';
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
import { systemTenant } from './system-tenant.js';
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

async function bodyObject(
  request: Request,
  maxBytes = 16_384,
): Promise<Record<string, unknown> | Response> {
  const text = await request.text();
  if (new TextEncoder().encode(text).length > maxBytes) {
    return json({ error: 'payload too large' }, 413);
  }
  try {
    const parsed = text === '' ? {} : JSON.parse(text);
    return typeof parsed === 'object' &&
      parsed !== null &&
      !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : json({ error: 'a JSON object body is required' }, 400);
  } catch {
    return json({ error: 'a JSON object body is required' }, 400);
  }
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

async function handleAgentRoutes(
  request: Request,
  env: Env,
  resolve: TenantResolver,
): Promise<Response | null> {
  const url = new URL(request.url);
  const segments = url.pathname.split('/').filter(Boolean);
  if (
    segments[0] !== 'api' ||
    segments[1] !== 'agent' ||
    segments[2] !== 'runs'
  ) {
    return null;
  }

  const tenant = await resolve(request);
  if (!tenant) return json({ error: 'authentication required' }, 401);

  if (request.method === 'POST' && segments.length === 3) {
    if (!RUN_START_ROLES.includes(tenant.actor.role)) {
      return json({ error: 'forbidden' }, 403);
    }
    const body = await bodyObject(request);
    if (body instanceof Response) return body;
    try {
      assertNoClientMemoryIds(body);
    } catch {
      return json(
        {
          error:
            'threadId and resourceId are server-assigned and may not appear in the body',
        },
        400,
      );
    }
    const unknownField = Object.keys(body).find((field) => field !== 'prompt');
    if (unknownField) {
      return json({ error: `field '${unknownField}' is not allowed` }, 400);
    }
    if (
      body.prompt !== undefined &&
      (typeof body.prompt !== 'string' || body.prompt.length > 10_000)
    ) {
      return json(
        { error: 'prompt must be a string of at most 10000 chars' },
        400,
      );
    }

    const threadId = tenant.newThreadId();
    const resourceId = tenant.newResourceId(threadId);
    const runId = tenant.newRunId();
    const response = await createThreadTopology(env.THREAD).send(
      tenant,
      threadId,
      '/agent/start',
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          runId,
          resourceId,
          prompt:
            typeof body.prompt === 'string'
              ? body.prompt
              : 'Record a starter action through the approved connector.',
          threaded: true,
        }),
      },
    );
    const result = await response.json();
    return json(
      {
        threadId,
        resourceId,
        runId,
        result,
      },
      response.status,
    );
  }

  if (request.method === 'GET' && segments.length === 5) {
    const threadId = decoded(segments[3] ?? '');
    const runId = decoded(segments[4] ?? '');
    const resourceId = url.searchParams.get('resourceId');
    if (!threadId || !runId || !resourceId) {
      return json({ error: 'not found' }, 404);
    }
    const response = await createThreadTopology(env.THREAD).send(
      tenant,
      threadId,
      `/agent/status?runId=${encodeURIComponent(runId)}&resourceId=${encodeURIComponent(resourceId)}`,
    );
    return new Response(response.body, response);
  }

  return json({ error: 'method not allowed' }, 405);
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

function customApprovalResolver(
  env: Env,
  ctx: FlowsafeWorkerContext,
  baseResolve: TenantResolver,
  topology: DoRunTopology,
): TenantResolver {
  return async (request) => {
    const tenant = await baseResolve(request);
    if (!tenant) return undefined;
    let custom: ReturnType<typeof buildHostApprovalService> | undefined;
    const scoped: TenantContext = {
      ...tenant,
      service: () => {
        custom ??= buildHostApprovalService(
          approvalStoreFactoryFor(env.DB).forTenant(tenant.tenantId),
          {
            systemActorId: SYSTEM_ACTOR_ID,
            defaultSlaSeconds: numberVar(
              env.APPROVAL_SLA_SECONDS,
              4 * 60 * 60,
              'APPROVAL_SLA_SECONDS',
            ),
            waitUntil: (promise) => ctx.waitUntil(promise),
            stream: env.STREAM_TICKET_SECRET
              ? (event) =>
                  ctx.waitUntil(
                    createHubTopology(env.HUB)
                      .publish(event)
                      .catch((error: unknown) => {
                        console.error(
                          JSON.stringify({
                            type: 'starter.stream-publish-error',
                            reason:
                              error instanceof Error
                                ? error.message
                                : String(error),
                          }),
                        );
                      }),
                  )
              : undefined,
            resumeRun: async (record, decision) => {
              if (record.resumeTarget?.kind !== 'thread') {
                return topology.resumeRecord(record, decision);
              }
              const response = await createThreadTopology(env.THREAD).send(
                systemTenant(env, record.tenantId),
                record.resumeTarget.threadId,
                '/agent/resume',
                {
                  method: 'POST',
                  headers: { 'content-type': 'application/json' },
                  body: JSON.stringify({
                    runId: record.runId,
                    resourceId: record.resumeTarget.resourceId,
                    step: record.stepPath,
                    resumeData: defaultResumeData(record, decision),
                  }),
                },
              );
              return doSummary(response);
            },
          },
        );
        return custom;
      },
    };
    return scoped;
  };
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
  preRoutes: async (request, env, ctx, kit) => {
    const webhook = await webhookRouter(env)(request);
    if (webhook) return webhook;

    const agent = await handleAgentRoutes(request, env, kit.resolve);
    if (agent) return agent;

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

    const approval = await createApprovalRouter({
      resolve: customApprovalResolver(env, ctx, kit.resolve, kit.topology),
    })(request);
    return approval;
  },
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
    const schedules = createScheduleTick({
      store: schedulesStore(env.DB),
      start: runTopology.start,
      startAgent: async ({
        target,
        tenantId,
        runId,
        topologyThreadId,
        threaded,
      }) => {
        if (target.agentId !== STARTER_AGENT_ID) {
          throw new Error(`unknown scheduled agent '${target.agentId}'`);
        }
        const response = await threadTopology.send(
          systemTenant(env, tenantId),
          topologyThreadId,
          '/agent/start',
          {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              runId,
              resourceId: threaded ? target.resourceId : undefined,
              prompt: target.prompt,
              threaded,
            }),
          },
        );
        if (!response.ok) {
          throw new Error(
            `scheduled agent start failed with ${response.status}`,
          );
        }
        const started = (await response.json()) as { runId?: unknown };
        return {
          runId: typeof started.runId === 'string' ? started.runId : runId,
        };
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
