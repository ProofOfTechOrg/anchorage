// SPDX-License-Identifier: Apache-2.0
// The host's unattended maintenance duties, in a module of their own rather
// than in the Worker entry: workerd rejects a non-handler export from an entry
// module, so anything a test must reach has to live beside it (the same shape
// flowsafe's own deploy/crons.ts uses).
//
// What is worth testing here is the WIRING, not the closures: every surface in
// this host takes its execution fence from `executionFence(env.DB)`, and the
// one that must not be missed is the schedule tick — an unfenced tick claims a
// due fire through the schedules CAS (which advances `nextFireAt`) and the
// fenced runtime then refuses the start, so the fire is consumed and never
// runs.

import { createAgentThreadTopology } from '@proofoftech/flowsafe/agent-host';
import {
  createDoRunTopology,
  createThreadTopology,
  queueApprovalForSuspension,
  RunRouteError,
} from '@proofoftech/flowsafe/host-kit';
import {
  createScheduleTargetPolicy,
  createScheduleTick,
  parseScheduleAgentDispatchReceipt,
  type ScheduleTickResult,
} from '@proofoftech/flowsafe/schedules';
import {
  createNotificationDispatchTick,
  type NotificationDispatchTickResult,
} from '@proofoftech/flowsafe/signals';

import { STARTER_AGENT_META } from './agent.js';
import { audit, SYSTEM_PRINCIPAL_ID } from './config.js';
import { contextForResourceOwner, systemContext } from './principal-context.js';
import {
  executionFence,
  notificationsStore,
  schedulesStore,
  startIdempotency,
} from './storage.js';
import { WORKFLOWS } from './workflows.js';

/** The catalog every schedule target is rechecked against, at create AND at fire. */
export const scheduleTargetPolicy = createScheduleTargetPolicy({
  workflows: WORKFLOWS,
  agents: [STARTER_AGENT_META],
});

/**
 * The deployment's schedule duty: claim and fire due schedules and reconcile
 * deferred fires. It reads the SAME fence store as the runtime and the
 * routers, so a fenced deployment never claims a fire.
 */
export function starterScheduleTick(
  env: Env,
): () => Promise<ScheduleTickResult> {
  // ONE store, named once: the file header's claim that the tick gates the
  // same fence as the runtime is a claim about identity, and two calls make a
  // reader check the memo to believe it.
  const fence = executionFence(env.DB);
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
    {
      startIdempotency: startIdempotency(env.DB),
      executionFence: fence,
    },
  );
  return createScheduleTick({
    store: schedulesStore(env.DB),
    targetPolicy: scheduleTargetPolicy,
    executionFence: fence,
    start: async ({ workflowId, runId, inputData, scheduleId, dispatchId }) => {
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
      if (!receipt) throw new Error('agent schedule returned no valid receipt');
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
}

/**
 * The deployment's notification duty: dispatch due notifications through the
 * owning thread Durable Object. The maintenance singleton runs it in its own
 * invocation, so a schedule pass that throws or is terminated does not hold
 * delivery back, and it reads the same fence store as the runtime, so a
 * fenced deployment does not burn a notification's delivery attempts.
 */
export function starterNotificationTick(
  env: Env,
): () => Promise<NotificationDispatchTickResult> {
  return createNotificationDispatchTick({
    storage: notificationsStore(env.DB),
    topology: createThreadTopology(env.THREAD, env.DEPLOYMENT_IDENTITY_SECRET),
    resolveContext: () => systemContext(env, 'notification-dispatch'),
    limit: 100,
    executionFence: executionFence(env.DB),
  });
}
