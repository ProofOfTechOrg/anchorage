// SPDX-License-Identifier: Apache-2.0

import { provisionTenant } from '@proofoftech/flowsafe/host-kit';

export async function provisionCommercialTenant(
  env: Env,
  tenantId: string,
): Promise<void> {
  await provisionTenant(env.DB, { tenantId, kind: 'commercial' });
}
