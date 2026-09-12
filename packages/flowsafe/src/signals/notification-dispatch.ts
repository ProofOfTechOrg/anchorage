// SPDX-License-Identifier: Apache-2.0

import {
  type NotificationRecord,
  type NotificationsStorage,
  summarizeNotifications,
} from '@mastra/core/notifications';

import type { ActorContext } from '../approval-api/index.js';
import {
  admitsDrainableExecution,
  type ExecutionFenceWiring,
  isPathSafeId,
  readExecutionFence,
} from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import {
  nonnegativeSafeInteger,
  positiveSafeInteger,
} from '../numeric-config.js';
import { jsonOrNull } from './d1-shared.js';

/** Scalar values preserve the attempted observation across asynchronous writes. */
export type NotificationDeliveryObservation = Readonly<{
  id: string;
  threadId: string;
  source: string;
  kind: string;
  priority: NotificationRecord['priority'];
  status: NotificationRecord['status'];
  summary: string;
  payload: string | null;
  resourceId: string | null;
  agentId: string | null;
  sourceId: string | null;
  dedupeKey: string | null;
  coalesceKey: string | null;
  coalescedCount: number;
  attributes: string | null;
  createdAt: string;
  updatedAt: string;
  deliverAt: string | null;
  summaryAt: string | null;
  deliveryReason: string | null;
  deliveryAttempts: number;
  lastDeliveryAttemptAt: string | null;
  lastDeliveryError: string | null;
  deliveredSignalId: string | null;
  summarySignalId: string | null;
  deliveredAt: string | null;
  seenAt: string | null;
  dismissedAt: string | null;
  archivedAt: string | null;
  discardedAt: string | null;
  metadata: string | null;
}>;

export type NotificationDeliveryFailure =
  | Readonly<{
      type: 'retry';
      updatedAt: string;
      deliveryAttempts: number;
      lastDeliveryAttemptAt: string;
      lastDeliveryError: string;
      deliverAt?: string;
      summaryAt?: string;
    }>
  | Readonly<{
      type: 'discard';
      updatedAt: string;
      deliveryAttempts: number;
      lastDeliveryAttemptAt: string;
      lastDeliveryError: string;
    }>
  | Readonly<{ type: 'exhausted'; updatedAt: string }>;

export type NotificationDeliveryUpdateResult =
  | { applied: true; record: NotificationRecord }
  | { applied: false };

export interface NotificationDeliveryStorage extends NotificationsStorage {
  updateNotificationDeliveryIfUnchanged(input: {
    expected: NotificationDeliveryObservation;
    failure: NotificationDeliveryFailure;
  }): Promise<NotificationDeliveryUpdateResult>;
}

/** Maximum route-valid ids in one trusted thread-DO dispatch request. */
export const MAX_NOTIFICATION_DISPATCH_IDS = 100;

export const DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS = 10;

export interface NotificationDispatchTickOptions {
  storage: NotificationDeliveryStorage;
  topology: ThreadTopology;
  /** Builds the system-authorized context used after row bindings validate. */
  resolveContext(): ActorContext;
  now?: () => Date;
  /**
   * Max due rows read per pass. Must be a nonnegative safe integer; zero is an
   * intentional no-op. Values above 100 are split into route-valid chunks.
   */
  limit?: number;
  /** Failed rounds before terminal discard. Must be a positive safe integer. */
  maxDeliveryAttempts?: number;
  /**
   * The deployment execution fence, read ONCE per pass, or `'none'` for a tick
   * with no database behind it. A drain still dispatches — the thread routes
   * degrade a wake to a persist there, so the inbox drains without minting —
   * while migration-locked and proof-only skip the pass entirely: a due row
   * stays due, so the deployment that takes over dispatches it.
   *
   * REQUIRED: an unfenced tick keeps delivering into thread DOs a locked
   * deployment is refusing, which burns the notification's delivery attempts on
   * a deployment that cannot act on it. See ExecutionFenceWiring.
   */
  executionFence: ExecutionFenceWiring;
}

export interface NotificationDispatchTickResult {
  due: number;
  delivered: number;
  failed: number;
  /** Confirmed terminal discards. Omitted when zero for wire compatibility. */
  discarded?: number;
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

function textValue(value: unknown): string {
  if (typeof value !== 'string') {
    throw new TypeError('notification text must be a string');
  }
  return value;
}

function optionalText(value: unknown): string | null {
  return value === undefined || value === null ? null : textValue(value);
}

function dateValue(value: unknown): string {
  if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
    throw new TypeError('notification timestamp must be a finite Date');
  }
  return value.toISOString();
}

function optionalDate(value: unknown): string | null {
  return value === undefined || value === null ? null : dateValue(value);
}

function parsedJson(value: string): unknown {
  try {
    return JSON.parse(value, (_key, parsed: unknown) => {
      if (typeof parsed === 'number' && !Number.isFinite(parsed)) {
        throw new TypeError('notification JSON contains a nonfinite number');
      }
      return parsed;
    });
  } catch {
    throw new TypeError('notification JSON is malformed');
  }
}

function jsonValue(value: unknown): string | null {
  try {
    const encoded = jsonOrNull(value);
    if (encoded === null) return null;
    if (typeof encoded !== 'string') {
      throw new TypeError('notification value is not JSON serializable');
    }
    parsedJson(encoded);
    return encoded;
  } catch {
    throw new TypeError('notification value is not JSON serializable');
  }
}

export function captureNotificationDeliveryObservation(
  record: NotificationRecord,
): NotificationDeliveryObservation {
  const priority = record.priority;
  const status = record.status;
  if (!['low', 'medium', 'high', 'urgent'].includes(priority)) {
    throw new TypeError('notification priority is invalid');
  }
  if (
    ![
      'pending',
      'delivered',
      'seen',
      'dismissed',
      'archived',
      'discarded',
    ].includes(status)
  ) {
    throw new TypeError('notification status is invalid');
  }
  const coalescedCount =
    record.coalescedCount === undefined ? 1 : record.coalescedCount;
  if (typeof coalescedCount !== 'number' || !Number.isFinite(coalescedCount)) {
    throw new TypeError('notification coalesced count must be finite');
  }
  return Object.freeze({
    id: textValue(record.id),
    threadId: textValue(record.threadId),
    source: textValue(record.source),
    kind: textValue(record.kind),
    priority,
    status,
    summary: textValue(record.summary),
    payload: jsonValue(record.payload),
    resourceId: optionalText(record.resourceId),
    agentId: optionalText(record.agentId),
    sourceId: optionalText(record.sourceId),
    dedupeKey: optionalText(record.dedupeKey),
    coalesceKey: optionalText(record.coalesceKey),
    coalescedCount,
    attributes: jsonValue(record.attributes),
    createdAt: dateValue(record.createdAt),
    updatedAt: dateValue(record.updatedAt),
    deliverAt: optionalDate(record.deliverAt),
    summaryAt: optionalDate(record.summaryAt),
    deliveryReason: optionalText(record.deliveryReason),
    deliveryAttempts: nonnegativeSafeInteger(
      record.deliveryAttempts === undefined ? 0 : record.deliveryAttempts,
      'notification delivery attempts',
    ),
    lastDeliveryAttemptAt: optionalDate(record.lastDeliveryAttemptAt),
    lastDeliveryError: optionalText(record.lastDeliveryError),
    deliveredSignalId: optionalText(record.deliveredSignalId),
    summarySignalId: optionalText(record.summarySignalId),
    deliveredAt: optionalDate(record.deliveredAt),
    seenAt: optionalDate(record.seenAt),
    dismissedAt: optionalDate(record.dismissedAt),
    archivedAt: optionalDate(record.archivedAt),
    discardedAt: optionalDate(record.discardedAt),
    metadata: jsonValue(record.metadata),
  });
}

function decodedDate(value: string): Date {
  const date = new Date(textValue(value));
  if (dateValue(date) !== value) {
    throw new TypeError('notification observation timestamp must be ISO');
  }
  return date;
}

function decodedOptionalDate(value: string | null): Date | undefined {
  return value === null ? undefined : decodedDate(value);
}

function decodedJson(value: string | null): unknown {
  return value === null ? undefined : parsedJson(textValue(value));
}

function recordFromObservation(
  expected: NotificationDeliveryObservation,
): NotificationRecord {
  return {
    ...expected,
    payload: decodedJson(expected.payload),
    resourceId: expected.resourceId ?? undefined,
    agentId: expected.agentId ?? undefined,
    sourceId: expected.sourceId ?? undefined,
    dedupeKey: expected.dedupeKey ?? undefined,
    coalesceKey: expected.coalesceKey ?? undefined,
    attributes: decodedJson(
      expected.attributes,
    ) as NotificationRecord['attributes'],
    createdAt: decodedDate(expected.createdAt),
    updatedAt: decodedDate(expected.updatedAt),
    deliverAt: decodedOptionalDate(expected.deliverAt),
    summaryAt: decodedOptionalDate(expected.summaryAt),
    deliveryReason: expected.deliveryReason ?? undefined,
    lastDeliveryAttemptAt: decodedOptionalDate(expected.lastDeliveryAttemptAt),
    lastDeliveryError: expected.lastDeliveryError ?? undefined,
    deliveredSignalId: expected.deliveredSignalId ?? undefined,
    summarySignalId: expected.summarySignalId ?? undefined,
    deliveredAt: decodedOptionalDate(expected.deliveredAt),
    seenAt: decodedOptionalDate(expected.seenAt),
    dismissedAt: decodedOptionalDate(expected.dismissedAt),
    archivedAt: decodedOptionalDate(expected.archivedAt),
    discardedAt: decodedOptionalDate(expected.discardedAt),
    metadata: decodedJson(expected.metadata) as NotificationRecord['metadata'],
  };
}

export function captureNotificationDeliverySelection(
  record: NotificationRecord,
): {
  record: NotificationRecord;
  expected: NotificationDeliveryObservation;
} {
  const expected = captureNotificationDeliveryObservation(record);
  return { record: recordFromObservation(expected), expected };
}

export function captureNotificationDeliveryStorage(
  storage: NotificationsStorage,
): Pick<
  NotificationDeliveryStorage,
  'getNotification' | 'updateNotificationDeliveryIfUnchanged'
> {
  const update = (storage as Partial<NotificationDeliveryStorage>)
    .updateNotificationDeliveryIfUnchanged;
  const get = storage.getNotification;
  if (typeof update !== 'function' || typeof get !== 'function') {
    throw new TypeError(
      'notification dispatch requires conditional delivery storage',
    );
  }
  return {
    getNotification: get.bind(storage),
    updateNotificationDeliveryIfUnchanged: update.bind(storage),
  };
}

// Core's inline summary sender and the thread-DO summary route both reach
// Core's summarizeNotifications; unpatched, a source named after an
// Object.prototype member is miscounted in the summary a receipt is recorded
// against, and Core's own source-policy lookup resolves an inherited entry
// instead of the configured action. A behaviour probe rather than a prototype
// check lets any correct upstream fix pass. The record is the getting-started
// guide's confirmation record.
const SOURCE_KEY_PROBE: NotificationRecord = {
  id: 'n',
  threadId: 't',
  source: 'constructor',
  kind: 'k',
  priority: 'low',
  status: 'pending',
  summary: 's',
  createdAt: new Date(0),
  updatedAt: new Date(0),
};
let sourceKeysPatched: boolean | undefined;

export function assertNotificationSourceKeysPatched(): void {
  sourceKeysPatched ??=
    typeof summarizeNotifications([SOURCE_KEY_PROBE]).bySource.constructor ===
    'number';
  if (!sourceKeysPatched) {
    throw new TypeError(
      'notification dispatch requires the @mastra/core patch flowsafe ships; apply it at the application root (getting started: "Apply the flowsafe patch to @mastra/core")',
    );
  }
}

function errorMessage(error: unknown): string {
  try {
    return String(error instanceof Error ? error.message : error);
  } catch {
    return 'unreadable error';
  }
}

function logFailure(
  type: string,
  error: unknown,
  field: 'error' | 'reason' = 'error',
): void {
  try {
    console.error(JSON.stringify({ type, [field]: errorMessage(error) }));
  } catch {
    // Diagnostic failures cannot interrupt notification bookkeeping.
  }
}

export function reportNotificationDeliveryError(error: unknown): void {
  logFailure('notification-dispatch-bookkeeping-error', error);
}

function observationsMatch(
  left: NotificationDeliveryObservation,
  right: NotificationDeliveryObservation,
): boolean {
  return (Object.keys(left) as (keyof NotificationDeliveryObservation)[]).every(
    (key) => left[key] === right[key],
  );
}

const NOTIFICATION_RETRY_BASE_MS = 1_000;
const NOTIFICATION_RETRY_MAX_MS = 5 * 60_000;

/**
 * Record one delivery failure and move every currently-due cursor forward.
 * Without the cursor move, one permanently blocked row can monopolize a
 * bounded deployment-wide due scan. The attempt bound is the other half of the
 * same guarantee: a row that keeps failing leaves the due scan.
 */
export async function recordNotificationDeliveryFailure(
  storage: Pick<
    NotificationDeliveryStorage,
    'getNotification' | 'updateNotificationDeliveryIfUnchanged'
  >,
  expected: NotificationDeliveryObservation,
  now: Date,
  maxDeliveryAttempts: number,
  action: { type: 'failure'; error: unknown } | { type: 'exhausted' },
): Promise<'deferred' | 'discarded' | 'unchanged' | 'uncertain'> {
  try {
    positiveSafeInteger(
      maxDeliveryAttempts,
      'notification maximum delivery attempts',
    );
    // Re-capturing the caller-supplied observation from its own record checks
    // its canonical form before the conditional write: a custom store does not
    // run D1's value validation, so a non-canonical timestamp or non-JSON
    // payload text would otherwise reach the write unchecked.
    const observed = captureNotificationDeliveryObservation(
      recordFromObservation(expected),
    );
    if (!observationsMatch(observed, expected)) {
      throw new TypeError('notification delivery observation is invalid');
    }
    if (observed.status !== 'pending' || observed.deliveredSignalId)
      return 'unchanged';
    const attemptAt = dateValue(now);
    const attemptTime = Date.parse(attemptAt);
    const updatedAt = new Date().toISOString();
    let failure: NotificationDeliveryFailure;
    if (observed.deliveryAttempts >= maxDeliveryAttempts) {
      failure = { type: 'exhausted', updatedAt };
    } else {
      if (action.type !== 'failure') {
        throw new RangeError(
          'notification delivery attempts are not exhausted',
        );
      }
      const attempts = observed.deliveryAttempts + 1;
      const receipt = {
        updatedAt,
        deliveryAttempts: attempts,
        lastDeliveryAttemptAt: attemptAt,
        lastDeliveryError: errorMessage(action.error),
      };
      if (attempts >= maxDeliveryAttempts) {
        failure = { type: 'discard', ...receipt };
      } else {
        const delay = Math.min(
          NOTIFICATION_RETRY_MAX_MS,
          NOTIFICATION_RETRY_BASE_MS * 2 ** Math.min(attempts - 1, 8),
        );
        const retryAt = new Date(attemptTime + delay).toISOString();
        failure = {
          type: 'retry',
          ...receipt,
          ...(observed.deliverAt !== null &&
          Date.parse(observed.deliverAt) <= attemptTime
            ? { deliverAt: retryAt }
            : {}),
          ...(observed.summaryAt !== null &&
          Date.parse(observed.summaryAt) <= attemptTime
            ? { summaryAt: retryAt }
            : {}),
        };
      }
    }
    Object.freeze(failure);
    const target: NotificationDeliveryObservation = Object.freeze({
      ...observed,
      updatedAt,
      ...(failure.type === 'exhausted'
        ? {}
        : {
            deliveryAttempts: failure.deliveryAttempts,
            lastDeliveryAttemptAt: failure.lastDeliveryAttemptAt,
            lastDeliveryError: failure.lastDeliveryError,
          }),
      ...(failure.type === 'retry'
        ? {
            ...(failure.deliverAt === undefined
              ? {}
              : { deliverAt: failure.deliverAt }),
            ...(failure.summaryAt === undefined
              ? {}
              : { summaryAt: failure.summaryAt }),
          }
        : {
            status: 'discarded',
            deliveryReason: 'delivery-attempts-exhausted',
            discardedAt: updatedAt,
            deliverAt: null,
            summaryAt: null,
          }),
    });
    const confirmed = failure.type === 'retry' ? 'deferred' : 'discarded';
    try {
      const result = await storage.updateNotificationDeliveryIfUnchanged({
        expected: observed,
        failure,
      });
      if (!result || typeof result !== 'object' || Array.isArray(result)) {
        throw new TypeError(
          'notification delivery update returned an invalid result',
        );
      }
      const keys = Reflect.ownKeys(result);
      if (
        result.applied === false &&
        keys.length === 1 &&
        keys[0] === 'applied'
      ) {
        return 'unchanged';
      }
      if (
        result.applied !== true ||
        keys.length !== 2 ||
        !keys.includes('applied') ||
        !keys.includes('record') ||
        !observationsMatch(
          target,
          captureNotificationDeliveryObservation(result.record),
        )
      ) {
        throw new TypeError(
          'notification delivery update did not confirm its receipt',
        );
      }
      return confirmed;
    } catch (error) {
      try {
        const current = await storage.getNotification({
          threadId: observed.threadId,
          id: observed.id,
        });
        if (
          current &&
          observationsMatch(
            target,
            captureNotificationDeliveryObservation(current),
          )
        ) {
          return confirmed;
        }
      } catch (readError) {
        logFailure('notification-dispatch-bookkeeping-read-error', readError);
      }
      logFailure('notification-dispatch-bookkeeping-error', error);
      return 'uncertain';
    }
  } catch (error) {
    logFailure('notification-dispatch-bookkeeping-error', error);
    return 'uncertain';
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
  const maxDeliveryAttempts = positiveSafeInteger(
    options.maxDeliveryAttempts ?? DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
    'notification maximum delivery attempts',
  );
  assertNotificationSourceKeysPatched();
  if (limit === 0) return async () => ({ due: 0, delivered: 0, failed: 0 });
  const {
    storage,
    topology,
    resolveContext,
    now: clock,
    executionFence,
  } = options;
  const deliveryStorage = captureNotificationDeliveryStorage(storage);
  return async () => {
    // The fence, before the due read and before any delivery. This runs on a
    // maintenance alarm, so a fence that cannot be READ degrades closed by
    // skipping the pass and logging: throwing would fail the duty, and
    // proceeding would dispatch on a deployment whose state is unknown.
    let admitted: boolean;
    try {
      admitted = admitsDrainableExecution(
        await readExecutionFence(executionFence),
      );
    } catch (error) {
      // `reason` matches schedule-tick-fence-error, so the two alarm lanes
      // degrade closed identically and an operator greps one field.
      logFailure('notification-dispatch-fence-error', error, 'reason');
      return { due: 0, delivered: 0, failed: 0 };
    }
    if (!admitted) return { due: 0, delivered: 0, failed: 0 };
    const nowMs = (clock?.() ?? new Date()).getTime();
    const due = await storage.listDueNotifications({
      now: new Date(nowMs),
      limit,
    });
    const result: NotificationDispatchTickResult = {
      due: due.length,
      delivered: 0,
      failed: 0,
    };
    const groups = new Map<string, DeliveryGroup>();
    const selected = new Map<
      NotificationRecord,
      NotificationDeliveryObservation
    >();
    const seen = new Map<string, Set<string>>();
    for (const record of due) {
      try {
        const threadId = textValue(record.threadId);
        const id = textValue(record.id);
        const ids = seen.get(threadId) ?? new Set<string>();
        if (ids.has(id)) continue;
        ids.add(id);
        seen.set(threadId, ids);
        const selection = captureNotificationDeliverySelection(record);
        selected.set(selection.record, selection.expected);
      } catch (error) {
        result.failed += 1;
        reportNotificationDeliveryError(error);
      }
    }
    const recordFailure = async (
      expected: NotificationDeliveryObservation,
      action: { type: 'failure'; error: unknown } | { type: 'exhausted' },
    ) => {
      const outcome = await recordNotificationDeliveryFailure(
        deliveryStorage,
        expected,
        new Date(nowMs),
        maxDeliveryAttempts,
        action,
      );
      if (outcome === 'discarded') {
        result.discarded = (result.discarded ?? 0) + 1;
      } else {
        result.failed += 1;
      }
    };

    for (const [record, expected] of selected) {
      if (expected.status !== 'pending' || expected.deliveredSignalId) continue;
      if (expected.deliveryAttempts >= maxDeliveryAttempts) {
        await recordFailure(expected, { type: 'exhausted' });
        continue;
      }
      if (
        !isPathSafeId(record.threadId) ||
        !record.resourceId ||
        !isPathSafeId(record.resourceId)
      ) {
        await recordFailure(expected, {
          type: 'failure',
          error: new Error('notification has malformed memory ids'),
        });
        continue;
      }
      if (typeof record.agentId !== 'string' || record.agentId.length === 0) {
        await recordFailure(expected, {
          type: 'failure',
          error: new Error('notification has no agent id'),
        });
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
        planNotificationDispatch(group.records, new Date(nowMs)),
      );
      let batchThreadState: 'active' | 'idle' | null = null;
      for (const records of batches) {
        try {
          const context = resolveContext();
          const response = await topology.send(
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
                now: new Date(nowMs).toISOString(),
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
            discarded?: number;
            batchThreadState?: unknown;
          };
          result.delivered += body.delivered ?? 0;
          result.failed += body.failed ?? 0;
          if (body.discarded && body.discarded > 0) {
            result.discarded = (result.discarded ?? 0) + body.discarded;
          }
          if (
            batchThreadState === null &&
            (body.batchThreadState === 'active' ||
              body.batchThreadState === 'idle')
          ) {
            batchThreadState = body.batchThreadState;
          }
        } catch (error) {
          for (const record of records) {
            const expected = selected.get(record);
            if (expected)
              await recordFailure(expected, { type: 'failure', error });
          }
        }
      }
    }

    return result;
  };
}
