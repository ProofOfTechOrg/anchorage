// SPDX-License-Identifier: Apache-2.0

import {
  ApprovalService,
  type ExecutionPrincipal,
  principalActor,
  type TenantContext,
} from '@proofoftech/flowsafe/approval-api';
import {
  mintResourceId,
  mintSaltedId,
  mintThreadId,
  tenantOwnsMemoryId,
  tenantOwnsSaltedId,
} from '@proofoftech/flowsafe/do-runner';
import { approvalStoreFactoryFor } from '@proofoftech/flowsafe/host-kit';

import { SYSTEM_ACTOR_ID } from './config.js';

/**
 * The unattended scheduler identity. It is SYSTEM automation, not a synthetic
 * human operator: an agent it fires must have declared `system` on the
 * `schedule.fire` entry path, or the host denies the start.
 */
export function systemTenant(
  env: Env,
  tenantId: string,
  purpose = 'scheduled-agent-execution',
): TenantContext {
  return tenantForPrincipal(env, {
    kind: 'system',
    id: SYSTEM_ACTOR_ID,
    tenantId,
    purpose,
  });
}

export function tenantForPrincipal(
  env: Env,
  principal: ExecutionPrincipal,
): TenantContext {
  const { tenantId } = principal;
  const actor = principalActor(principal);
  let service: ApprovalService | undefined;
  return {
    actor,
    principal,
    tenantId,
    service: () => {
      service ??= new ApprovalService({
        store: approvalStoreFactoryFor(env.DB).forTenant(tenantId),
      });
      return service;
    },
    newRunId: () =>
      mintSaltedId(tenantId, () => crypto.randomUUID(), 'starter agent run'),
    ownsRun: (runId) => tenantOwnsSaltedId(tenantId, runId),
    newThreadId: () => mintThreadId(tenantId),
    newResourceId: (resourceKey) => mintResourceId(tenantId, resourceKey),
    ownsMemoryId: (id) => tenantOwnsMemoryId(tenantId, id),
    canSelfDecide: () => false,
  };
}
