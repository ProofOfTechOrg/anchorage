// SPDX-License-Identifier: Apache-2.0

import {
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
  const actor = {
    id: SYSTEM_ACTOR_ID,
    role: 'operator' as const,
    tenantId,
  };
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
      mintSaltedId(tenantId, () => crypto.randomUUID(), 'starter system run'),
    ownsRun: (runId) => tenantOwnsSaltedId(tenantId, runId),
    newThreadId: () => mintThreadId(tenantId),
    newResourceId: (resourceKey) => mintResourceId(tenantId, resourceKey),
    ownsMemoryId: (id) => tenantOwnsMemoryId(tenantId, id),
    canSelfDecide: () => false,
  };
}
