// SPDX-License-Identifier: Apache-2.0

import {
  type ApprovalActor,
  ApprovalService,
  type ExecutionPrincipal,
  humanPrincipal,
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
export function systemTenant(env: Env, tenantId: string): TenantContext {
  return tenantForPrincipal(env, {
    kind: 'system',
    id: SYSTEM_ACTOR_ID,
    tenantId,
    purpose: 'scheduled-agent-execution',
  });
}

export function tenantForActor(env: Env, actor: ApprovalActor): TenantContext {
  return tenantForPrincipal(env, humanPrincipal(actor));
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
