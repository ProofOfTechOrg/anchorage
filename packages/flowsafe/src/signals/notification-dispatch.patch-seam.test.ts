// SPDX-License-Identifier: Apache-2.0
// The @mastra/core patch seam, against an unpatched Core. The mock is
// file-scoped, so these cases live apart from the rest of the dispatch suites.

import type { Agent } from '@mastra/core/agent';
import type {
  NotificationDeliveryDecision,
  NotificationDeliveryPolicyConfig,
  NotificationDeliveryPolicyDecision,
  NotificationDeliveryPolicyInput,
  NotificationRecord,
  NotificationsStorage,
} from '@mastra/core/notifications';
import { describe, expect, it, vi } from 'vitest';

import type { ThreadScope } from '../do-runner/index.js';
import {
  assertNotificationDeliveryPolicyPatched,
  createNotificationDispatchTick,
  type NotificationDispatchTickOptions,
} from './notification-dispatch.js';
import { createThreadSignalRoutes } from './thread-do-routes.js';

// The module shape both mock registrations build: `summarizeNotifications`
// unpatched, or patched for the delivery-policy case, which reaches the
// asynchronous probe only when the synchronous one passes.
const unpatchedNotifications = vi.hoisted(
  () =>
    (
      actual: typeof import('@mastra/core/notifications'),
      patchedAccumulator: boolean,
    ) => {
      const normalize = (
        decision: NotificationDeliveryPolicyDecision,
      ): NotificationDeliveryDecision =>
        typeof decision === 'string' ? { action: decision } : decision;
      return {
        ...actual,
        // 1.53.0's accumulator is an ordinary object literal, so a source named
        // after an Object.prototype member resolves the inherited member instead
        // of its own count. The patched shape is built here rather than read
        // from the installed core, so both shapes hold whichever core the
        // workspace installs.
        summarizeNotifications: (records: NotificationRecord[]) => {
          const summary = actual.summarizeNotifications(records);
          const bySource: Record<string, number> = patchedAccumulator
            ? Object.create(null)
            : {};
          for (const notification of records) {
            if (notification.status !== 'pending') continue;
            bySource[notification.source] =
              (bySource[notification.source] ?? 0) + 1;
          }
          return { ...summary, bySource };
        },
        // 1.53.0's source lookup, reproduced from the shipped patch's `-` lines:
        // the bare index read resolves an Object.prototype member for a source
        // named after one, and normalizeDecision hands that member back as the
        // decision, which therefore carries no action.
        resolveNotificationDeliveryDecision: async ({
          config,
          ...input
        }: NotificationDeliveryPolicyInput & {
          config?: NotificationDeliveryPolicyConfig;
        }): Promise<NotificationDeliveryDecision> => {
          const custom = await config?.decide?.(input);
          if (custom) return normalize(custom);
          const sourceDecision = config?.sources?.[input.record.source];
          if (sourceDecision) return normalize(sourceDecision);
          const priorityDecision = config?.priorities?.[input.record.priority];
          if (priorityDecision) return normalize(priorityDecision);
          if (config?.default) return normalize(config.default);
          return actual.defaultNotificationDeliveryDecision(input);
        },
      };
    },
);

vi.mock('@mastra/core/notifications', async (importOriginal) =>
  unpatchedNotifications(
    await importOriginal<typeof import('@mastra/core/notifications')>(),
    false,
  ),
);

// Vitest caches a factory's result per registration, so a case that needs the
// other accumulator registers its own factory rather than resetting modules
// around a switch the cached result would keep ignoring.
const mockNotifications = (patchedAccumulator: boolean): void => {
  vi.doMock('@mastra/core/notifications', async (importOriginal) =>
    unpatchedNotifications(
      await importOriginal<typeof import('@mastra/core/notifications')>(),
      patchedAccumulator,
    ),
  );
  vi.resetModules();
};

const PATCH_MESSAGE = /Apply the flowsafe patch to @mastra\/core/;

function tickOptions(
  overrides: Record<string, unknown> = {},
): NotificationDispatchTickOptions {
  return {
    // The tick captures the conditional-delivery capability ahead of the patch
    // probe, so storage lacking it refuses for the other reason.
    storage: {
      getNotification: async () => null,
      updateNotificationDeliveryIfUnchanged: async () => ({
        outcome: 'unchanged',
      }),
    },
    topology: { send: async () => new Response(null, { status: 404 }) },
    resolveContext: () => ({}),
    executionFence: 'none',
    ...overrides,
  } as unknown as NotificationDispatchTickOptions;
}

function scope(): ThreadScope {
  return {
    threadId: 'acme_t1',
    actor: { id: 'operator', role: 'operator' },
    principal: { kind: 'human', id: 'operator', role: 'operator' },
    requestedBy: 'operator',
    init: { pubsub: undefined },
  } as unknown as ThreadScope;
}

function post(path: string, body: unknown): Request {
  return new Request(`http://thread${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** The route catch surfaces its refusal through `console.error`. */
async function withCapturedErrors(
  run: () => Promise<Response | null | undefined>,
): Promise<{ response: Response | null | undefined; logged: string[] }> {
  const logged: string[] = [];
  const consoleError = console.error;
  let response: Response | null | undefined;
  try {
    console.error = (...args: unknown[]) => {
      logged.push(args.map(String).join(' '));
    };
    response = await run();
  } finally {
    console.error = consoleError;
  }
  return { response, logged };
}

describe('notification dispatch against an unpatched @mastra/core', () => {
  it('refuses tick construction, naming the patch', () => {
    expect(() => createNotificationDispatchTick(tickOptions())).toThrow(
      TypeError,
    );
    expect(() => createNotificationDispatchTick(tickOptions())).toThrow(
      PATCH_MESSAGE,
    );
  });

  it('builds a zero-limit tick, which does no notification work', async () => {
    const tick = createNotificationDispatchTick(tickOptions({ limit: 0 }));

    await expect(tick()).resolves.toEqual({
      due: 0,
      delivered: 0,
      failed: 0,
    });
  });

  it('refuses a dispatch request without reading a notification', async () => {
    const storage = {
      getNotification: vi.fn(async () => null),
      updateNotificationDeliveryIfUnchanged: vi.fn(async () => ({
        outcome: 'unchanged',
      })),
    };
    // Construction stays available: the refusal belongs to notification
    // dispatch, not to the whole /signal surface.
    const routes = createThreadSignalRoutes({
      resolveAgent: () => ({ id: 'agent' }) as unknown as Agent,
      resolveResourceId: () => 'acme_res',
      resolveNotificationsStorage: () =>
        storage as unknown as NotificationsStorage,
    });

    const { response, logged } = await withCapturedErrors(() =>
      routes(
        post('/signal/notifications/dispatch', {
          notificationIds: ['n1'],
          resourceId: 'acme_res',
          agentId: 'agent',
          now: '2026-07-20T12:00:00.000Z',
        }),
        scope(),
      ),
    );

    // Status and body match the conditional-storage capability refusal, which
    // takes the same route catch; the logged message is what tells them apart.
    expect(response?.status).toBe(502);
    expect(await response?.json()).toEqual({ error: 'internal error' });
    expect(logged.some((line) => PATCH_MESSAGE.test(line))).toBe(true);
    expect(storage.getNotification).not.toHaveBeenCalled();
  });

  it("refuses an ingestion request without reaching core's sender", async () => {
    const sendNotificationSignal = vi.fn();
    const routes = createThreadSignalRoutes({
      resolveAgent: () =>
        ({ id: 'agent', sendNotificationSignal }) as unknown as Agent,
      resolveResourceId: () => 'acme_res',
    });

    const { response, logged } = await withCapturedErrors(() =>
      routes(
        post('/signal/notification', {
          source: 'constructor',
          kind: 'changed',
          summary: 's',
        }),
        scope(),
      ),
    );

    // The agent stub carries the method: an absent one throws its own TypeError
    // into the same catch, for the same status and body, so the uncalled spy is
    // what tells the patch refusal from a stub-shape fault.
    expect(response?.status).toBe(502);
    expect(await response?.json()).toEqual({ error: 'internal error' });
    expect(logged.some((line) => PATCH_MESSAGE.test(line))).toBe(true);
    expect(sendNotificationSignal).not.toHaveBeenCalled();
  });

  it('refuses the delivery policy probe, naming the patch', async () => {
    await expect(assertNotificationDeliveryPolicyPatched()).rejects.toThrow(
      PATCH_MESSAGE,
    );
  });
});

describe('notification ingestion against a core patched for summaries alone', () => {
  it("refuses without reaching core's sender", async () => {
    const sendNotificationSignal = vi.fn();
    mockNotifications(true);
    try {
      // Both probes memoize per module instance, and the instance the cases
      // above hold has recorded the summary half as unpatched, so this case
      // reads the delivery half through a module graph of its own.
      const { assertNotificationSourceKeysPatched } = await import(
        './notification-dispatch.js'
      );
      const { createThreadSignalRoutes } = await import(
        './thread-do-routes.js'
      );
      expect(() => assertNotificationSourceKeysPatched()).not.toThrow();

      const routes = createThreadSignalRoutes({
        resolveAgent: () =>
          ({ id: 'agent', sendNotificationSignal }) as unknown as Agent,
        resolveResourceId: () => 'acme_res',
      });
      const { response, logged } = await withCapturedErrors(() =>
        routes(
          post('/signal/notification', {
            source: 'constructor',
            kind: 'changed',
            summary: 's',
          }),
          scope(),
        ),
      );

      // The summary half passing is what leaves the source delivery policy
      // lookup as the only subject this refusal can belong to.
      expect(response?.status).toBe(502);
      expect(await response?.json()).toEqual({ error: 'internal error' });
      expect(logged.some((line) => PATCH_MESSAGE.test(line))).toBe(true);
      expect(sendNotificationSignal).not.toHaveBeenCalled();
    } finally {
      mockNotifications(false);
    }
  });
});
