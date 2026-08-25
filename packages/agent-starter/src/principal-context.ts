// SPDX-License-Identifier: Apache-2.0

import {
  createAgentApprovalResumer,
  createAgentThreadTopology,
} from '@proofoftech/flowsafe/agent-host';
import {
  type ActorContext,
  ApprovalService,
  createPrincipalActorContext,
  type ExecutionPrincipal,
  type ResourceClaim,
  type ResourceKind,
  withRegisteredResourceOwner,
} from '@proofoftech/flowsafe/approval-api';
import {
  approvalStoreFactoryFor,
  type FlowsafeRunnerLifecycleConfig,
} from '@proofoftech/flowsafe/host-kit';

import { STARTER_AGENT_META } from './agent.js';
import { SYSTEM_PRINCIPAL_ID } from './config.js';
import { executionFence, startIdempotency } from './storage.js';

export const starterRunnerLifecycleConfig = {
  systemPrincipalId: SYSTEM_PRINCIPAL_ID,
  buildResumeRun: (fallback, env) =>
    createAgentApprovalResumer({
      fallback,
      agents: [STARTER_AGENT_META],
      topology: createAgentThreadTopology(
        env.THREAD,
        env.DEPLOYMENT_IDENTITY_SECRET,
        {
          startIdempotency: startIdempotency(env.DB),
          executionFence: executionFence(env.DB),
        },
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
} satisfies FlowsafeRunnerLifecycleConfig<Env>;

/**
 * The unattended scheduler identity. It is SYSTEM automation, not a synthetic
 * human operator: an agent it fires must declare `system` on the
 * `schedule.fire` entry path, or the host denies the start.
 */
export function systemContext(
  env: Env,
  purpose = 'scheduled-agent-execution',
): ActorContext {
  return contextForPrincipal(env, {
    kind: 'system',
    id: SYSTEM_PRINCIPAL_ID,
    purpose,
  });
}

export function contextForPrincipal(
  env: Env,
  principal: ExecutionPrincipal,
): ActorContext {
  const factory = approvalStoreFactoryFor(env.DB);
  return createPrincipalActorContext({
    principal,
    storeFactory: factory,
    deploymentTag: env.DEPLOYMENT_TENANT,
    buildService: (store) =>
      new ApprovalService({
        // Same database, same fence as every other surface here: this service
        // files and decides approvals for scheduled and system-driven runs, and
        // decide() COMMITS before it resumes.
        store,
        executionFence: executionFence(env.DB),
      }),
  });
}

export async function contextForResourceOwner(
  env: Env,
  resourceKind: ResourceKind,
  resourceId: string,
  purpose: string,
): Promise<ActorContext> {
  return contextForRegisteredResources(
    env,
    {
      kind: 'system',
      id: SYSTEM_PRINCIPAL_ID,
      purpose,
    },
    [{ kind: resourceKind, resourceId }],
  );
}

export async function contextForRegisteredResources(
  env: Env,
  principal: ExecutionPrincipal,
  claims: readonly ResourceClaim[],
): Promise<ActorContext> {
  const context = contextForPrincipal(env, principal);
  const resources = approvalStoreFactoryFor(env.DB).resources();
  return withRegisteredResourceOwner(context, resources, claims);
}
