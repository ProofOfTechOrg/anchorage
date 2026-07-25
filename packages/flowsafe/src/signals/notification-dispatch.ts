// SPDX-License-Identifier: Apache-2.0

import type {
  NotificationRecord,
  NotificationsStorage,
} from '@mastra/core/notifications';

import type { TenantContext } from '../approval-api/index.js';
import { tenantOfMemoryId } from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import { nonnegativeSafeInteger } from '../numeric-config.js';

/** Maximum route-valid ids in one trusted thread-DO dispatch request. */
export const MAX_NOTIFICATION_DISPATCH_IDS = 100;

export interface NotificationDispatchTickOptions {
  storage: NotificationsStorage;
  topology: ThreadTopology;
  /** Builds the system-authorized context used only after row tenancy validates. */
  resolveTenant(tenantId: string): TenantContext;
  now?: () => Date;
  /**
   * Max due rows read per pass. Must be a nonnegative safe integer; zero is an
   * intentional no-op. Values above 100 are split into route-valid chunks.
   */
  limit?: number;
}

export interface NotificationDispatchTickResult {
  due: number;
  delivered: number;
  failed: number;
}

interface DeliveryGroup {
  tenantId: string;
  threadId: string;
  resourceId: string;
  agentId: string;
  records: NotificationRecord[];
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function recordFailure(
  storage: NotificationsStorage,
  record: NotificationRecord,
  now: Date,
  error: unknown,
): Promise<void> {
  try {
    await storage.updateNotification({
      id: record.id,
      threadId: record.threadId,
      deliveryAttempts: (record.deliveryAttempts ?? 0) + 1,
      lastDeliveryAttemptAt: now,
      lastDeliveryError: errorMessage(error),
    });
  } catch (updateError) {
    console.error(
      JSON.stringify({
        type: 'notification-dispatch-bookkeeping-error',
        notificationId: record.id,
        error: errorMessage(updateError),
      }),
    );
  }
}

/**
 * Dispatch due rows through tenant-addressed thread DOs. The global due read is
 * a system-only TCB operation; both memory ids independently establish the
 * tenant before any topology address is resolved.
 */
export function createNotificationDispatchTick(
  options: NotificationDispatchTickOptions,
): () => Promise<NotificationDispatchTickResult> {
  const limit = nonnegativeSafeInteger(
    options.limit ?? 100,
    'notification dispatch tick limit',
  );
  return async () => {
    if (limit === 0) return { due: 0, delivered: 0, failed: 0 };
    const now = options.now?.() ?? new Date();
    const due = await options.storage.listDueNotifications({
      now,
      limit,
    });
    const result: NotificationDispatchTickResult = {
      due: due.length,
      delivered: 0,
      failed: 0,
    };
    const groups = new Map<string, DeliveryGroup>();

    for (const record of due) {
      const threadTenant = tenantOfMemoryId(record.threadId);
      const resourceTenant = record.resourceId
        ? tenantOfMemoryId(record.resourceId)
        : undefined;
      if (!threadTenant || !resourceTenant || threadTenant !== resourceTenant) {
        result.failed += 1;
        await recordFailure(
          options.storage,
          record,
          now,
          new Error('notification has malformed or mixed-tenant memory ids'),
        );
        continue;
      }
      if (typeof record.agentId !== 'string' || record.agentId.length === 0) {
        result.failed += 1;
        await recordFailure(
          options.storage,
          record,
          now,
          new Error('notification has no agent id'),
        );
        continue;
      }
      const resourceId = record.resourceId;
      if (!resourceId) continue;
      const key = `${threadTenant}\0${record.threadId}\0${resourceId}\0${record.agentId}`;
      const group = groups.get(key) ?? {
        tenantId: threadTenant,
        threadId: record.threadId,
        resourceId,
        agentId: record.agentId,
        records: [] as NotificationRecord[],
      };
      group.records.push(record);
      groups.set(key, group);
    }

    for (const group of groups.values()) {
      for (
        let offset = 0;
        offset < group.records.length;
        offset += MAX_NOTIFICATION_DISPATCH_IDS
      ) {
        const records = group.records.slice(
          offset,
          offset + MAX_NOTIFICATION_DISPATCH_IDS,
        );
        try {
          const tenant = options.resolveTenant(group.tenantId);
          const response = await options.topology.send(
            tenant,
            group.threadId,
            '/signal/notifications/dispatch',
            {
              method: 'POST',
              headers: { 'content-type': 'application/json' },
              body: JSON.stringify({
                notificationIds: records.map((record) => record.id),
                resourceId: group.resourceId,
                agentId: group.agentId,
                now: now.toISOString(),
              }),
            },
          );
          if (!response.ok) {
            throw new Error(
              `thread notification dispatch returned ${response.status}`,
            );
          }
          const body = (await response.json()) as {
            delivered?: number;
            failed?: number;
          };
          result.delivered += body.delivered ?? 0;
          result.failed += body.failed ?? 0;
        } catch (error) {
          result.failed += records.length;
          for (const record of records) {
            await recordFailure(options.storage, record, now, error);
          }
        }
      }
    }

    return result;
  };
}
