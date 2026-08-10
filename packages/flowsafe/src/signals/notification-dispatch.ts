// SPDX-License-Identifier: Apache-2.0

import type {
  NotificationRecord,
  NotificationsStorage,
} from '@mastra/core/notifications';

import type { ActorContext } from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import { nonnegativeSafeInteger } from '../numeric-config.js';

/** Maximum route-valid ids in one trusted thread-DO dispatch request. */
export const MAX_NOTIFICATION_DISPATCH_IDS = 100;

export interface NotificationDispatchTickOptions {
  storage: NotificationsStorage;
  topology: ThreadTopology;
  /** Builds the system-authorized context used after row bindings validate. */
  resolveContext(): ActorContext;
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
  threadId: string;
  resourceId: string;
  agentId: string;
  records: NotificationRecord[];
}

export type NotificationDispatchItem =
  | {
      type: 'individual';
      record: NotificationRecord;
      priority: NotificationRecord['priority'];
      createdAt: Date;
    }
  | {
      type: 'summary';
      records: NotificationRecord[];
      priority: NotificationRecord['priority'];
      createdAt: Date;
    };

const DELIVERY_PRIORITY: Record<NotificationRecord['priority'], number> = {
  urgent: 0,
  high: 1,
  medium: 2,
  low: 3,
};

function isSummaryDue(record: NotificationRecord, now: Date): boolean {
  return Boolean(
    record.summaryAt && record.summaryAt.getTime() <= now.getTime(),
  );
}

/**
 * Build the same logical delivery items as Mastra's dispatcher. Individuals
 * are constructed before the aggregate summary so a stable equal-key sort
 * preserves Mastra's individual-before-summary tie behavior.
 */
export function planNotificationDispatch(
  records: NotificationRecord[],
  now: Date,
): NotificationDispatchItem[] {
  const summaryRecords: NotificationRecord[] = [];
  const individualRecords: NotificationRecord[] = [];
  for (const record of records) {
    (isSummaryDue(record, now) ? summaryRecords : individualRecords).push(
      record,
    );
  }

  const items: NotificationDispatchItem[] = individualRecords.map((record) => ({
    type: 'individual',
    record,
    priority: record.priority,
    createdAt: record.createdAt,
  }));
  const firstSummary = summaryRecords[0];
  if (firstSummary) {
    const priority = summaryRecords.reduce(
      (highest, record) =>
        DELIVERY_PRIORITY[record.priority] < DELIVERY_PRIORITY[highest]
          ? record.priority
          : highest,
      'low' as NotificationRecord['priority'],
    );
    const createdAt = summaryRecords.reduce(
      (earliest, record) =>
        record.createdAt.getTime() < earliest.getTime()
          ? record.createdAt
          : earliest,
      firstSummary.createdAt,
    );
    items.push({
      type: 'summary',
      records: summaryRecords,
      priority,
      createdAt,
    });
  }

  items.sort(
    (left, right) =>
      DELIVERY_PRIORITY[left.priority] - DELIVERY_PRIORITY[right.priority] ||
      left.createdAt.getTime() - right.createdAt.getTime(),
  );
  return items;
}

/**
 * Pack logical items into route-valid requests without splitting a summary
 * that fits the route limit. An oversized summary is emitted as consecutive,
 * summary-only fragments because the trusted route has a strict 100-id bound.
 */
export function packNotificationDispatchItems(
  items: NotificationDispatchItem[],
): NotificationRecord[][] {
  const batches: NotificationRecord[][] = [];
  let current: NotificationRecord[] = [];
  const flush = () => {
    if (current.length === 0) return;
    batches.push(current);
    current = [];
  };

  for (const item of items) {
    if (item.type === 'individual') {
      if (current.length === MAX_NOTIFICATION_DISPATCH_IDS) flush();
      current.push(item.record);
      continue;
    }

    if (item.records.length > MAX_NOTIFICATION_DISPATCH_IDS) {
      flush();
      for (
        let offset = 0;
        offset < item.records.length;
        offset += MAX_NOTIFICATION_DISPATCH_IDS
      ) {
        batches.push(
          item.records.slice(offset, offset + MAX_NOTIFICATION_DISPATCH_IDS),
        );
      }
      continue;
    }

    if (current.length + item.records.length > MAX_NOTIFICATION_DISPATCH_IDS) {
      flush();
    }
    current.push(...item.records);
  }
  flush();
  return batches;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

const NOTIFICATION_RETRY_BASE_MS = 1_000;
const NOTIFICATION_RETRY_MAX_MS = 5 * 60_000;

/**
 * Record one delivery failure and move every currently-due cursor forward.
 * Without the cursor move, one permanently blocked row can monopolize a
 * bounded deployment-wide due scan forever.
 */
export async function deferNotificationAfterFailure(
  storage: NotificationsStorage,
  record: NotificationRecord,
  now: Date,
  error: unknown,
): Promise<void> {
  const attempts = (record.deliveryAttempts ?? 0) + 1;
  const delay = Math.min(
    NOTIFICATION_RETRY_MAX_MS,
    NOTIFICATION_RETRY_BASE_MS * 2 ** Math.min(attempts - 1, 8),
  );
  const retryAt = new Date(now.getTime() + delay);
  await storage.updateNotification({
    id: record.id,
    threadId: record.threadId,
    deliveryAttempts: attempts,
    lastDeliveryAttemptAt: now,
    lastDeliveryError: errorMessage(error),
    ...(record.deliverAt && record.deliverAt.getTime() <= now.getTime()
      ? { deliverAt: retryAt }
      : {}),
    ...(record.summaryAt && record.summaryAt.getTime() <= now.getTime()
      ? { summaryAt: retryAt }
      : {}),
  });
}

async function recordFailure(
  storage: NotificationsStorage,
  record: NotificationRecord,
  now: Date,
  error: unknown,
): Promise<void> {
  try {
    await deferNotificationAfterFailure(storage, record, now, error);
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
 * Dispatch due rows through thread DOs. The deployment-wide due read is a
 * system-only TCB operation; both memory ids are validated before any topology
 * address is resolved.
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
      if (
        !isPathSafeId(record.threadId) ||
        !record.resourceId ||
        !isPathSafeId(record.resourceId)
      ) {
        result.failed += 1;
        await recordFailure(
          options.storage,
          record,
          now,
          new Error('notification has malformed memory ids'),
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
      const key = `${record.threadId}\0${resourceId}\0${record.agentId}`;
      const group = groups.get(key) ?? {
        threadId: record.threadId,
        resourceId,
        agentId: record.agentId,
        records: [] as NotificationRecord[],
      };
      group.records.push(record);
      groups.set(key, group);
    }

    for (const group of groups.values()) {
      const batches = packNotificationDispatchItems(
        planNotificationDispatch(group.records, now),
      );
      let batchThreadState: 'active' | 'idle' | null = null;
      for (const records of batches) {
        try {
          const context = options.resolveContext();
          const response = await options.topology.send(
            context,
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
                batchThreadState,
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
            batchThreadState?: unknown;
          };
          result.delivered += body.delivered ?? 0;
          result.failed += body.failed ?? 0;
          if (
            batchThreadState === null &&
            (body.batchThreadState === 'active' ||
              body.batchThreadState === 'idle')
          ) {
            batchThreadState = body.batchThreadState;
          }
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
