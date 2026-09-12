// SPDX-License-Identifier: Apache-2.0
// The @mastra/core patch seam, against an unpatched Core. The mock is
// file-scoped, so these cases live apart from the rest of the dispatch suites.

import type { Agent } from '@mastra/core/agent';
import type {
  NotificationRecord,
  NotificationsStorage,
} from '@mastra/core/notifications';
import { describe, expect, it, vi } from 'vitest';

import type { ThreadScope } from '../do-runner/index.js';
import {
  createNotificationDispatchTick,
  type NotificationDispatchTickOptions,
} from './notification-dispatch.js';
import { createThreadSignalRoutes } from './thread-do-routes.js';

vi.mock('@mastra/core/notifications', async (importOriginal) => {
  const actual =
    await importOriginal<typeof import('@mastra/core/notifications')>();
  return {
    ...actual,
    // 1.53.0's accumulator is an ordinary object literal, so a source named
    // after an Object.prototype member resolves the inherited member instead of
    // its own count.
    summarizeNotifications: (records: NotificationRecord[]) => {
      const summary = actual.summarizeNotifications(records);
      const bySource: Record<string, number> = {};
      for (const notification of records) {
        if (notification.status !== 'pending') continue;
        bySource[notification.source] =
          (bySource[notification.source] ?? 0) + 1;
      }
      return { ...summary, bySource };
    },
  };
});

const PATCH_MESSAGE = /Apply the flowsafe patch to @mastra\/core/;

function tickOptions(
  overrides: Record<string, unknown> = {},
): NotificationDispatchTickOptions {
  return {
    storage: {},
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
});
