// SPDX-License-Identifier: Apache-2.0

import {
  type ActorContext,
  ApprovalService,
  createPrincipalActorContext,
  type ExecutionPrincipal,
  type ResourceClaim,
  type ResourceKind,
  withRegisteredResourceOwner,
} from '@proofoftech/flowsafe/approval-api';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';

import { SYSTEM_PRINCIPAL_ID } from './config.js';

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
    buildService: (store) => new ApprovalService({ store }),
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
