// SPDX-License-Identifier: Apache-2.0

import {
  createAgentApprovalResumer,
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
  createDoRunTopology,
  createFlowsafeWorker,
  createThreadTopology,
  queueApprovalForSuspension,
  RunRouteError,
} from '@proofoftech/flowsafe/host-kit';
import {
  createScheduleRouter,
  createScheduleTargetPolicy,
  createScheduleTick,
  parseScheduleAgentDispatchReceipt,
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
  SYSTEM_PRINCIPAL_ID,
  signalAttributeAllowlist,
  TICK_CRON,
} from './config.js';
import {
  BACKGROUND_TASKS_INSTANCE_NAME,
  StarterBackgroundTasks,
  StarterHub,
  StarterRunner,
  StarterSignalProviderHost,
  StarterThread,
} from './durable-objects.js';
import {
  contextForRegisteredResources,
  contextForResourceOwner,
  systemContext,
} from './principal-context.js';
import {
  notificationsStore,
  schedulesStore,
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
const scheduleTargetPolicy = createScheduleTargetPolicy({
  workflows: WORKFLOWS,
  agents: [STARTER_AGENT_META],
});
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

function audit(event: unknown): void {
  console.log(JSON.stringify(event));
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

const worker = createFlowsafeWorker<Env>({
  workflows: WORKFLOWS,
  systemPrincipalId: SYSTEM_PRINCIPAL_ID,
  buildVerifier,
  crons: {
    sweep: SWEEP_CRON,
    purge: PURGE_CRON,
    tick: TICK_CRON,
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
      ),
    }),
  buildResumeRun: (fallback, env) =>
    createAgentApprovalResumer({
      fallback,
      agents: [STARTER_AGENT_META],
      topology: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ),
      contextForPrincipal: (principal, record) => {
        const target = record.resumeTarget;
        if (target?.kind !== 'agent-thread') {
          throw new Error('agent approval has no registered thread target');
        }
        return contextForRegisteredResources(env, principal, [
          { kind: 'thread', resourceId: target.threadId },
          { kind: 'resource', resourceId: target.resourceId },
          { kind: 'run', resourceId: record.runId },
        ]);
      },
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
      ).requireBoundThread,
      maxRunsCap: 50,
      audit,
    }),
  buildScheduleRouter: (resolve, env) =>
    createScheduleRouter({
      resolve,
      store: schedulesStore(env.DB),
      targetPolicy: scheduleTargetPolicy,
      validateThreadTarget: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
      ).requireBoundThread,
      maxSchedules: 100,
      minFireIntervalMs: 60_000,
      audit,
    }),
  scheduleTick: (env) => {
    const runTopology = createDoRunTopology(
      env.RUNNER,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    const threadTopology = createThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    const agentTopology = createAgentThreadTopology(
      env.THREAD,
      env.DEPLOYMENT_IDENTITY_SECRET,
    );
    const schedules = createScheduleTick({
      store: schedulesStore(env.DB),
      targetPolicy: scheduleTargetPolicy,
      start: async ({
        workflowId,
        runId,
        inputData,
        scheduleId,
        dispatchId,
      }) => {
        const context = await contextForResourceOwner(
          env,
          'schedule',
          scheduleId,
          'schedule.fire',
        );
        const summary = await runTopology.start({
          workflowId,
          runId,
          inputData,
          principal: context.principal,
          scheduleId,
          dispatchId,
        });
        if (summary.status === 'suspended') {
          try {
            await queueApprovalForSuspension(
              context.service(),
              workflowId,
              summary,
              context.principal.id,
              SYSTEM_PRINCIPAL_ID,
            );
          } catch (error) {
            console.error(
              JSON.stringify({
                type: 'scheduled-approval-filing-error',
                workflowId,
                runId,
                error: error instanceof Error ? error.message : String(error),
              }),
            );
          }
        }
        return summary;
      },
      deploymentTag: env.DEPLOYMENT_TENANT,
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
        const context = await contextForResourceOwner(
          env,
          'schedule',
          scheduleId,
          'schedule.fire',
        );
        const started = await agentTopology.start(context, {
          agentId: target.agentId,
          runId,
          prompt: target.prompt,
          entryPath,
          scheduleId,
          dispatchId,
          threaded,
          requestContext,
          streamRequestContext,
          providerOptions,
          ...(threaded
            ? {
                threadId: target.threadId,
                resourceId: target.resourceId,
              }
            : { topologyThreadId }),
        });
        return { runId: started.runId };
      },
      signalAgent: async ({ scheduleId, target, dispatchId, runId }) => {
        if (!target.threadId || !target.resourceId) {
          throw new Error('threaded schedule signal requires memory ids');
        }
        const context = await contextForResourceOwner(
          env,
          'schedule',
          scheduleId,
          'schedule.fire',
        );
        const response = await threadTopology.send(
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
          throw new RunRouteError(
            response.status,
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
        const context = await contextForResourceOwner(
          env,
          'schedule',
          ref.scheduleId,
          'schedule.fire',
        );
        if (ref.target === 'workflow') {
          const summary = await runTopology.dispatchStatus(
            ref.workflowId,
            ref.runId,
          );
          if (summary?.status === 'suspended') {
            try {
              await queueApprovalForSuspension(
                context.service(),
                ref.workflowId,
                summary,
                context.principal.id,
                SYSTEM_PRINCIPAL_ID,
              );
            } catch (error) {
              console.error(
                JSON.stringify({
                  type: 'scheduled-approval-filing-error',
                  workflowId: ref.workflowId,
                  runId: ref.runId,
                  error: error instanceof Error ? error.message : String(error),
                }),
              );
            }
          }
          return summary;
        }
        if (ref.mode === 'signal') {
          const state = await schedulesStore(env.DB).agentScheduleDispatchState(
            ref.scheduleId,
            ref.dispatchId,
          );
          if (state.state === 'settled') {
            return {
              runId: state.receipt.runId,
              dispatchReceipt: state.receipt,
            };
          }
          if (state.state === 'pending') {
            throw new Error('agent schedule dispatch remains pending');
          }
          return undefined;
        }
        return agentTopology.dispatchStatus(context, {
          agentId: ref.agentId,
          threadId: ref.threadId,
          runId: ref.runId,
        });
      },
      audit,
    });
    const notifications = createNotificationDispatchTick({
      storage: notificationsStore(env.DB),
      topology: threadTopology,
      resolveContext: () => systemContext(env, 'notification-dispatch'),
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
