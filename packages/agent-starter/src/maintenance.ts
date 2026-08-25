// SPDX-License-Identifier: Apache-2.0
// The host's unattended maintenance duty, in a module of its own rather than in
// the Worker entry: workerd rejects a non-handler export from an entry module,
// so anything a test must reach has to live beside it (the same shape
// flowsafe's own deploy/crons.ts uses).
//
// What is worth testing here is the WIRING, not the closures: every surface in
// this host takes its execution fence from `executionFence(env.DB)`, and the
// one that must not be missed is this tick — an unfenced tick claims a due fire
// through the schedules CAS (which advances `nextFireAt`) and the fenced
// runtime then refuses the start, so the fire is consumed and never runs.

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
} from '@proofoftech/flowsafe/schedules';
import { createNotificationDispatchTick } from '@proofoftech/flowsafe/signals';

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
 * The deployment's cron duty: claim and fire due schedules, then dispatch due
 * notifications. Both passes read the SAME fence store as the runtime and the
 * routers, so a fenced deployment neither claims a fire nor burns a
 * notification's delivery attempts.
 */
export function starterMaintenanceTick(env: Env): () => Promise<unknown> {
  // ONE store, named once: the file header's claim that both passes gate the
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
  const schedules = createScheduleTick({
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
  const notifications = createNotificationDispatchTick({
    storage: notificationsStore(env.DB),
    topology: threadTopology,
    resolveContext: () => systemContext(env, 'notification-dispatch'),
    limit: 100,
    executionFence: fence,
  });
  return async () => ({
    schedules: await schedules(),
    notifications: await notifications(),
  });
}
