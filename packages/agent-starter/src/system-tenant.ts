// SPDX-License-Identifier: Apache-2.0

import {
  type ApprovalActor,
  ApprovalService,
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

export function systemTenant(env: Env, tenantId: string): TenantContext {
  return tenantForActor(env, {
    id: SYSTEM_ACTOR_ID,
    role: 'operator' as const,
    tenantId,
  });
}

export function tenantForActor(env: Env, actor: ApprovalActor): TenantContext {
  const { tenantId } = actor;
  let service: ApprovalService | undefined;
  return {
    actor,
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
