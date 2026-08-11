// SPDX-License-Identifier: Apache-2.0

import {
  type AuditMessageBatch,
  createAuditQueueHandler,
} from '@proofoftech/flowsafe/audit-export';

export interface FleetAuditConsumerEnv {
  readonly SIEM_ENDPOINT?: string;
  readonly SIEM_AUTH_HEADER?: string;
}

export default {
  async queue(
    batch: AuditMessageBatch,
    env: FleetAuditConsumerEnv,
  ): Promise<void> {
    await createAuditQueueHandler({
      endpoint: env.SIEM_ENDPOINT,
      authHeader: env.SIEM_AUTH_HEADER,
    })(batch);
  },
};
