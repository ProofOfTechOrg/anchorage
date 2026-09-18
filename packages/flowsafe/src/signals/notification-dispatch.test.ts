// SPDX-License-Identifier: Apache-2.0

import {
  type CreateNotificationInput,
  InMemoryNotificationsStorage,
  type NotificationRecord,
} from '@mastra/core/notifications';
import { describe, expect, it, vi } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type { ActorContext } from '../approval-api/index.js';
import {
  type ExecutionFenceDatabase,
  type ExecutionFenceState,
  ExecutionFenceStore,
} from '../do-runner/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import type { SignalDatabase } from './d1-shared.js';
import {
  captureNotificationDeliveryObservation,
  captureNotificationDeliverySelection,
  captureNotificationDeliveryStorage,
  createNotificationDispatchTick as createNotificationDispatchTickImpl,
  DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
  type NotificationDeliveryStorage,
  type NotificationDeliveryUpdateResult,
  type NotificationDispatchTickOptions,
  recordNotificationDeliveryFailure,
} from './notification-dispatch.js';

import { D1NotificationsStorage } from './notifications-d1.js';

const NOW = new Date('2026-07-20T12:00:00.000Z');

function notificationStorage(): D1NotificationsStorage {
  return new D1NotificationsStorage(
    sqliteUnitDatabase(openSqlite()) as SignalDatabase,
  );
}

function pending(
  storage: NotificationDeliveryStorage,
  overrides: Partial<CreateNotificationInput> = {},
): Promise<NotificationRecord> {
  return storage.createNotification({
    id: 'pending',
    threadId: 'acme_thread',
    resourceId: 'acme_resource',
    agentId: 'agent',
    source: 'test',
    kind: 'ready',
    summary: 'ready',
    deliverAt: new Date(NOW.getTime() - 1),
    ...overrides,
  });
}

function failure(
  storage: NotificationDeliveryStorage,
  record: NotificationRecord,
  maxDeliveryAttempts = DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS,
  now = NOW,
) {
  return recordNotificationDeliveryFailure(
    captureNotificationDeliveryStorage(storage),
    captureNotificationDeliveryObservation(record),
    now,
    maxDeliveryAttempts,
    { type: 'failure', error: new Error('target refused') },
  );
}

/** SQLite fixtures without an execution fence use the explicit no-fence wiring. */
function createNotificationDispatchTick(
  options: Omit<NotificationDispatchTickOptions, 'executionFence'> &
    Partial<Pick<NotificationDispatchTickOptions, 'executionFence'>>,
) {
  return createNotificationDispatchTickImpl({
    ...options,
    executionFence: options.executionFence ?? 'none',
  });
}

function actorContext(groupId = 'deployment'): ActorContext {
  return {
    actor: { id: 'maintenance', role: 'admin' },
    principal: { kind: 'human', id: 'maintenance', role: 'admin' },
    resourceOwner: { kind: 'human', id: 'maintenance' },
    service: () => {
      throw new Error('approval service is not used in dispatch tests');
    },
    newRunId: () => `${groupId}-run`,
    newThreadId: () => `${groupId}-thread`,
    resourceIdFromKey: (key) => key,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  };
}

describe('createNotificationDispatchTick', () => {
  it('rejects malformed rows before addressing a thread DO', async () => {
    const storage = notificationStorage();
    const valid = await storage.createNotification({
      id: 'valid',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'valid',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    const malformed = await storage.createNotification({
      id: 'malformed',
      threadId: 'acme_thread',
      resourceId: 'bad/resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'malformed',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    const send = vi.fn(
      async (
        _context: ActorContext,
        _threadId: string,
        _path: string,
        _init: RequestInit,
      ) => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 2, delivered: 1, failed: 1 });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({}),
      valid.threadId,
      '/signal/notifications/dispatch',
      expect.any(Object),
    );
    expect(
      await storage.getNotification({
        threadId: malformed.threadId,
        id: malformed.id,
      }),
    ).toMatchObject({
      deliveryAttempts: 1,
      lastDeliveryError: 'notification has malformed memory ids',
    });
  });

  it('surfaces terminal content-policy discards separately from failures', async () => {
    // #given — two thread groups, one whose DO discarded both of its rows
    const storage = notificationStorage();
    for (const groupId of ['acme', 'globex']) {
      await storage.createNotification({
        id: groupId,
        threadId: `${groupId}_thread`,
        resourceId: `${groupId}_resource`,
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: groupId,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    const send = vi.fn(async (_context, threadId: string) =>
      threadId.startsWith('acme_')
        ? new Response(
            JSON.stringify({ delivered: 0, failed: 0, discarded: 2 }),
          )
        : new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    // #when / #then — a discard is neither a delivery nor a retryable failure
    expect(await tick()).toEqual({
      due: 2,
      delivered: 1,
      failed: 0,
      discarded: 2,
    });
  });

  it('omits the discard counter when no route discarded anything', async () => {
    // #given — the pre-existing wire shape, which callers assert exactly
    const storage = notificationStorage();
    await storage.createNotification({
      id: 'acme',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'acme',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    const send = vi.fn(
      async () =>
        new Response(JSON.stringify({ delivered: 1, failed: 0, discarded: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    // #when / #then
    expect(await tick()).toEqual({ due: 1, delivered: 1, failed: 0 });
  });

  it('isolates a failed thread group from its neighbors', async () => {
    const storage = notificationStorage();
    for (const groupId of ['acme', 'globex']) {
      await storage.createNotification({
        id: groupId,
        threadId: `${groupId}_thread`,
        resourceId: `${groupId}_resource`,
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: groupId,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    const send = vi.fn(async (_context, threadId: string) =>
      threadId.startsWith('acme_')
        ? new Response('down', { status: 503 })
        : new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 2, delivered: 1, failed: 1 });
    expect(send).toHaveBeenCalledTimes(2);
  });

  it('backs off a blocked row so a bounded scan reaches later notifications', async () => {
    const storage = notificationStorage();
    const blocked = await storage.createNotification({
      id: 'blocked',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'blocked',
      deliverAt: new Date(NOW.getTime() - 2_000),
    });
    await storage.createNotification({
      id: 'later',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'later',
      deliverAt: new Date(NOW.getTime() - 1_000),
    });
    const send = vi
      .fn(
        async (
          _context: ActorContext,
          _threadId: string,
          _path: string,
          _init: RequestInit,
        ) => new Response('principal mismatch', { status: 409 }),
      )
      .mockResolvedValueOnce(
        new Response('principal mismatch', { status: 409 }),
      )
      .mockResolvedValue(
        new Response(JSON.stringify({ delivered: 1, failed: 0 })),
      );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 1,
    });

    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    expect(
      await storage.getNotification({
        threadId: blocked.threadId,
        id: blocked.id,
      }),
    ).toMatchObject({
      deliveryAttempts: 1,
      deliverAt: new Date(NOW.getTime() + 1_000),
    });
    expect(await tick()).toEqual({ due: 1, delivered: 1, failed: 0 });
    const secondBody = JSON.parse(String(send.mock.calls[1]?.[3]?.body)) as {
      notificationIds: string[];
    };
    expect(secondBody.notificationIds).toEqual(['later']);
  });

  it('groups the same thread/resource separately by persisted agent id', async () => {
    const storage = notificationStorage();
    for (const agentId of ['agent-a', 'agent-b']) {
      await storage.createNotification({
        id: agentId,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId,
        source: 'test',
        kind: 'ready',
        summary: agentId,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    const send = vi.fn(
      async (
        _context: ActorContext,
        _threadId: string,
        _path: string,
        _init: RequestInit,
      ) => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 2, delivered: 2, failed: 0 });
    expect(send).toHaveBeenCalledTimes(2);
    const bodies = send.mock.calls.map((call) =>
      JSON.parse(String(call[3]?.body)),
    );
    expect(bodies).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          agentId: 'agent-a',
          notificationIds: ['agent-a'],
        }),
        expect.objectContaining({
          agentId: 'agent-b',
          notificationIds: ['agent-b'],
        }),
      ]),
    );
  });

  it('fails a due row with no agent id before addressing a thread DO', async () => {
    const storage = notificationStorage();
    const record = await storage.createNotification({
      id: 'no-agent',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      source: 'test',
      kind: 'ready',
      summary: 'missing agent',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    const send = vi.fn();
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    expect(send).not.toHaveBeenCalled();
    expect(
      await storage.getNotification({
        threadId: record.threadId,
        id: record.id,
      }),
    ).toMatchObject({
      deliveryAttempts: 1,
      lastDeliveryError: 'notification has no agent id',
    });
  });

  it('plans the full group before packing so urgent rows cross the 100-id boundary', async () => {
    const storage = notificationStorage();
    for (let index = 0; index < 100; index += 1) {
      await storage.createNotification({
        id: `low-${index}`,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: `low ${index}`,
        priority: 'low',
        deliverAt: new Date(NOW.getTime() - 2_000),
      });
    }
    await storage.createNotification({
      id: 'urgent',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'urgent',
      priority: 'urgent',
      deliverAt: new Date(NOW.getTime() - 1_000),
    });
    const bodies: Array<{ notificationIds: string[] }> = [];
    const send = vi.fn(async (_context, _thread, _path, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as {
        notificationIds: string[];
      };
      bodies.push(body);
      return new Response(
        JSON.stringify({
          delivered: body.notificationIds.length,
          failed: 0,
        }),
      );
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 101,
    });

    expect(await tick()).toEqual({ due: 101, delivered: 101, failed: 0 });
    expect(bodies.map((body) => body.notificationIds.length)).toEqual([100, 1]);
    expect(bodies[0]?.notificationIds[0]).toBe('urgent');
  });

  it('keeps a route-sized summary intact while packing higher-priority individuals first', async () => {
    const storage = notificationStorage();
    const summaryIds: string[] = [];
    const createSummary = async (index: number) => {
      const id = `summary-${index}`;
      summaryIds.push(id);
      await storage.createNotification({
        id,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'digest',
        summary: id,
        priority: 'medium',
        summaryAt: new Date(NOW.getTime() - 3_000),
      });
    };
    for (let index = 0; index < 30; index += 1) await createSummary(index);
    for (let index = 0; index < 60; index += 1) {
      await storage.createNotification({
        id: `urgent-${index}`,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: `urgent ${index}`,
        priority: 'urgent',
        deliverAt: new Date(NOW.getTime() - 2_000),
      });
    }
    for (let index = 30; index < 60; index += 1) await createSummary(index);
    const batches: string[][] = [];
    const send = vi.fn(async (_context, _thread, _path, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as {
        notificationIds: string[];
      };
      batches.push(body.notificationIds);
      return new Response(
        JSON.stringify({
          delivered: body.notificationIds.length,
          failed: 0,
        }),
      );
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 120,
    });

    expect(await tick()).toEqual({ due: 120, delivered: 120, failed: 0 });
    expect(batches.map((batch) => batch.length)).toEqual([60, 60]);
    expect(batches[0]).toEqual(
      Array.from({ length: 60 }, (_, index) => `urgent-${index}`),
    );
    expect(batches[1]).toEqual(summaryIds);
  });

  it('fragments an oversized summary into consecutive summary-only requests', async () => {
    const storage = notificationStorage();
    for (let index = 0; index < 205; index += 1) {
      await storage.createNotification({
        id: `summary-${index}`,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'digest',
        summary: `notification ${index}`,
        summaryAt: new Date(NOW.getTime() - 1),
      });
    }
    const batches: string[][] = [];
    const send = vi.fn(async (_context, _thread, _path, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as {
        notificationIds: string[];
      };
      batches.push(body.notificationIds);
      return new Response(
        JSON.stringify({
          delivered: body.notificationIds.length,
          failed: 0,
        }),
      );
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 205,
    });

    expect(await tick()).toEqual({ due: 205, delivered: 205, failed: 0 });
    expect(batches.map((batch) => batch.length)).toEqual([100, 100, 5]);
    expect(batches.flat()).toEqual(
      Array.from({ length: 205 }, (_, index) => `summary-${index}`),
    );
  });

  it('carries one echoed thread-state snapshot within a group and resets it for the next group', async () => {
    const storage = notificationStorage();
    for (const groupId of ['acme', 'globex']) {
      for (let index = 0; index < 101; index += 1) {
        await storage.createNotification({
          id: `${groupId}-${index}`,
          threadId: `${groupId}_thread`,
          resourceId: `${groupId}_resource`,
          agentId: 'agent',
          source: 'test',
          kind: 'ready',
          summary: `${groupId} ${index}`,
          deliverAt: new Date(NOW.getTime() - 1),
        });
      }
    }
    const bodies: Array<{
      threadId: string;
      batchThreadState: 'active' | 'idle' | null;
    }> = [];
    const send = vi.fn(
      async (
        _context: ActorContext,
        threadId: string,
        _path: string,
        init: RequestInit,
      ) => {
        const body = JSON.parse(String(init.body)) as {
          notificationIds: string[];
          batchThreadState: 'active' | 'idle' | null;
        };
        bodies.push({ threadId, batchThreadState: body.batchThreadState });
        return new Response(
          JSON.stringify({
            delivered: body.notificationIds.length,
            failed: 0,
            batchThreadState: threadId.startsWith('acme_') ? 'active' : 'idle',
          }),
        );
      },
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 202,
    });

    expect(await tick()).toEqual({ due: 202, delivered: 202, failed: 0 });
    expect(bodies).toEqual([
      { threadId: 'acme_thread', batchThreadState: null },
      { threadId: 'acme_thread', batchThreadState: 'active' },
      { threadId: 'globex_thread', batchThreadState: null },
      { threadId: 'globex_thread', batchThreadState: 'idle' },
    ]);
  });

  it('chunks 205 records into route-valid batches of 100, 100, and 5', async () => {
    const storage = notificationStorage();
    for (let index = 0; index < 205; index += 1) {
      await storage.createNotification({
        id: `n-${index}`,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: `notification ${index}`,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    const sizes: number[] = [];
    const send = vi.fn(async (_context, _thread, _path, init: RequestInit) => {
      const body = JSON.parse(String(init.body)) as {
        notificationIds: string[];
      };
      sizes.push(body.notificationIds.length);
      return new Response(
        JSON.stringify({
          delivered: body.notificationIds.length,
          failed: 0,
        }),
      );
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 205,
    });

    expect(await tick()).toEqual({ due: 205, delivered: 205, failed: 0 });
    expect(sizes).toEqual([100, 100, 5]);
  });

  it('isolates a failed middle chunk and continues with later chunks', async () => {
    const storage = notificationStorage();
    for (let index = 0; index < 205; index += 1) {
      await storage.createNotification({
        id: `n-${index}`,
        threadId: 'acme_thread',
        resourceId: 'acme_resource',
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: `notification ${index}`,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    let call = 0;
    const send = vi.fn(async (_context, _thread, _path, init: RequestInit) => {
      call += 1;
      const body = JSON.parse(String(init.body)) as {
        notificationIds: string[];
      };
      if (call === 2) return new Response('down', { status: 503 });
      return new Response(
        JSON.stringify({
          delivered: body.notificationIds.length,
          failed: 0,
        }),
      );
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      limit: 205,
    });

    expect(await tick()).toEqual({ due: 205, delivered: 105, failed: 100 });
    expect(send).toHaveBeenCalledTimes(3);
    expect(
      await storage.getNotification({
        threadId: 'acme_thread',
        id: 'n-100',
      }),
    ).toMatchObject({
      deliveryAttempts: 1,
      lastDeliveryError: 'thread notification dispatch returned 503',
    });
  });

  it('rejects invalid limits synchronously and treats zero as an intentional no-op', async () => {
    const storage = notificationStorage();
    const send = vi.fn();
    for (const limit of [
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(() =>
        createNotificationDispatchTick({
          storage,
          topology: { send } as unknown as ThreadTopology,
          resolveContext: actorContext,
          limit,
        }),
      ).toThrow(RangeError);
    }
    const list = vi.spyOn(storage, 'listDueNotifications');
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      limit: 0,
    });
    await expect(tick()).resolves.toEqual({
      due: 0,
      delivered: 0,
      failed: 0,
    });
    expect(list).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });
});

describe('createNotificationDispatchTick and the deployment execution fence', () => {
  async function fenceAt(
    state: ExecutionFenceState,
  ): Promise<ExecutionFenceStore> {
    const fence = new ExecutionFenceStore(
      sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
    );
    await fence.seed(state);
    return fence;
  }

  async function dueRow(): Promise<D1NotificationsStorage> {
    const storage = notificationStorage();
    await storage.createNotification({
      id: 'due-1',
      threadId: 'acme_thread',
      resourceId: 'acme_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'due',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    return storage;
  }

  it('skips the whole pass once locked, leaving the row due', async () => {
    // #given
    const storage = await dueRow();
    const send = vi.fn(
      async () => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const executionFence = await fenceAt('migration-locked');
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      executionFence,
    });

    // #when / #then — no thread DO addressed, and the row is untouched: the
    // deployment taking over dispatches it.
    expect(await tick()).toEqual({ due: 0, delivered: 0, failed: 0 });
    expect(send).not.toHaveBeenCalled();
    const row = await storage.getNotification({
      threadId: 'acme_thread',
      id: 'due-1',
    });
    expect(row?.deliveryAttempts ?? 0).toBe(0);

    // #when — reopened
    await executionFence.transition({
      expected: 'migration-locked',
      next: 'open',
    });

    // #then — the same row dispatches, exactly once.
    expect(await tick()).toEqual({ due: 1, delivered: 1, failed: 0 });
    expect(send).toHaveBeenCalledTimes(1);
  });

  it('keeps dispatching while draining', async () => {
    // #given — the thread routes degrade a wake to a persist under a drain, so
    // the inbox drains without minting.
    const storage = await dueRow();
    const send = vi.fn(
      async () => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      executionFence: await fenceAt('draining'),
    });

    // #then
    expect(await tick()).toEqual({ due: 1, delivered: 1, failed: 0 });
  });
});

describe('notification delivery observations', () => {
  it('detaches dates and JSON while preserving storage normalization', async () => {
    const record = await pending(notificationStorage());
    record.deliveryAttempts = undefined;
    record.coalescedCount = undefined;
    record.payload = { values: [Number.NaN, undefined], absent: undefined };
    record.attributes = { present: true, missing: undefined };
    record.metadata = { nested: { name: 'before' } };
    const { record: detached, expected } =
      captureNotificationDeliverySelection(record);
    record.deliverAt?.setTime(0);
    (record.metadata.nested as { name: string }).name = 'after';
    detached.createdAt.setTime(0);
    expect(expected.deliveryAttempts).toBe(0);
    expect(expected.coalescedCount).toBe(1);
    expect(expected.payload).toBe('{"values":[null,null]}');
    expect(expected.attributes).toBe('{"present":true}');
    expect(detached.metadata).toEqual({ nested: { name: 'before' } });
    expect(expected.deliverAt).toBe(new Date(NOW.getTime() - 1).toISOString());
    expect(expected.createdAt).not.toBe(new Date(0).toISOString());
    expect(Object.isFrozen(expected)).toBe(true);
  });

  it.each([
    null,
    -1,
    0.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
    '1',
  ])('rejects a malformed public counter %s', async (deliveryAttempts) => {
    const record = await pending(notificationStorage());
    Object.assign(record, { deliveryAttempts });
    expect(() => captureNotificationDeliveryObservation(record)).toThrow();
  });

  it.each([
    { payload: () => undefined },
    { payload: 1n },
    { createdAt: new Date(Number.NaN) },
    { summaryAt: new Date(Number.NaN) },
    { status: 'unknown' },
    { priority: 'unknown' },
    { resourceId: 1 },
    { coalescedCount: Number.POSITIVE_INFINITY },
  ])('rejects an unreadable observation %s', async (patch) => {
    const record = await pending(notificationStorage());
    Object.assign(record, patch);
    expect(() => captureNotificationDeliverySelection(record)).toThrow();
  });
});

describe('conditional notification failure bookkeeping', () => {
  it('keeps the exact backoff and a future individual cursor', async () => {
    const storage = notificationStorage();
    let now = new Date(NOW);
    const future = new Date('2030-01-01T00:00:00Z');
    let record = await pending(storage, {
      summaryAt: new Date(0),
      deliverAt: future,
    });
    for (const delay of [1, 2, 4, 8, 16, 32, 64, 128, 256, 256, 256]) {
      expect(await failure(storage, record, 20, now)).toBe('deferred');
      const next = await storage.getNotification(record);
      expect(next?.summaryAt).toEqual(new Date(now.getTime() + delay * 1000));
      expect(next?.deliverAt).toEqual(future);
      expect(next?.lastDeliveryAttemptAt).toEqual(now);
      expect(next?.lastDeliveryError).toBe('target refused');
      if (!next?.summaryAt)
        throw new Error('retry did not retain summary cursor');
      record = next;
      now = next.summaryAt;
    }
  });

  it('preserves wall-clock status time with an injected dispatch clock', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const before = Date.now();
    expect(await failure(storage, record, 1)).toBe('discarded');
    const after = Date.now();
    const current = await storage.getNotification(record);
    expect(current?.lastDeliveryAttemptAt).toEqual(NOW);
    expect(current?.discardedAt).toEqual(current?.updatedAt);
    expect(current?.updatedAt.getTime()).toBeGreaterThanOrEqual(before);
    expect(current?.updatedAt.getTime()).toBeLessThanOrEqual(after);
    expect(current?.deliveryAttempts).toBe(1);
    expect(current?.deliveryReason).toBe('delivery-attempts-exhausted');
    expect(current?.deliverAt).toBeUndefined();
    expect(current?.summaryAt).toBeUndefined();
  });

  it.each([
    'retry',
    'discard',
    'exhausted',
  ] as const)('recovers the exact committed %s receipt after response loss without replay', async (mode) => {
    const storage = notificationStorage();
    let record = await pending(storage, { summaryAt: new Date(0) });
    if (mode === 'exhausted') {
      record = await storage.updateNotification({
        ...record,
        deliveryAttempts: 10,
        lastDeliveryError: 'first refusal',
        lastDeliveryAttemptAt: new Date(1000),
      });
    }
    const write = storage.updateNotificationDeliveryIfUnchanged.bind(storage);
    const lost = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockImplementation(async (input) => {
        await write(input);
        throw new Error('response lost');
      });
    const read = vi.spyOn(storage, 'getNotification');
    expect(
      await recordNotificationDeliveryFailure(
        captureNotificationDeliveryStorage(storage),
        captureNotificationDeliveryObservation(record),
        NOW,
        mode === 'discard' ? 1 : 10,
        mode === 'exhausted'
          ? { type: 'exhausted' }
          : { type: 'failure', error: 'target refused' },
      ),
    ).toBe(mode === 'retry' ? 'deferred' : 'discarded');
    expect(lost).toHaveBeenCalledTimes(1);
    expect(read).toHaveBeenCalledTimes(1);
    const current = await storage.getNotification(record);
    expect(current?.deliveryAttempts).toBe(mode === 'exhausted' ? 10 : 1);
    expect(current?.lastDeliveryError).toBe(
      mode === 'exhausted' ? 'first refusal' : 'target refused',
    );
  });

  it('leaves a before-commit failure uncertain and never claims a discard', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const write = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockRejectedValue(new Error('before commit'));
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(await failure(storage, record, 1)).toBe('uncertain');
      expect(await storage.getNotification(record)).toEqual(record);
      expect(write).toHaveBeenCalledTimes(1);
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    null,
    [],
    {},
    { applied: 1 },
    { applied: true },
    { applied: false, record: {} },
    { applied: false, extra: true },
    { applied: true, record: {} },
  ])('does not trust a malformed custom result %j', async (result) => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const write = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockResolvedValue(result as NotificationDeliveryUpdateResult);
    const read = vi.spyOn(storage, 'getNotification');
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(await failure(storage, record, 1)).toBe('uncertain');
      expect(write).toHaveBeenCalledTimes(1);
      expect(read).toHaveBeenCalledTimes(1);
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    'content',
    'signal',
    'reason',
    'count',
    'time',
    'missing',
    'unreadable',
  ] as const)('does not attribute a changed %s readback after committed response loss', async (change) => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const write = storage.updateNotificationDeliveryIfUnchanged.bind(storage);
    vi.spyOn(
      storage,
      'updateNotificationDeliveryIfUnchanged',
    ).mockImplementation(async (input) => {
      await write(input);
      throw new Error('response lost');
    });
    const read = storage.getNotification.bind(storage);
    vi.spyOn(storage, 'getNotification').mockImplementation(async (input) => {
      if (change === 'missing') return null;
      if (change === 'unreadable') throw new Error('read unavailable');
      const current = await read(input);
      if (!current) throw new Error('missing fixture');
      const patches = {
        content: { summary: 'replacement' },
        signal: { summarySignalId: 'other-summary' },
        reason: { deliveryReason: 'content-policy-denied' },
        count: { deliveryAttempts: 2 },
        time: { discardedAt: new Date(0) },
      };
      return { ...current, ...patches[change] };
    });
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(await failure(storage, record, 1)).toBe('uncertain');
    } finally {
      log.mockRestore();
    }
  });

  it('validates encoded JSON without treating overflow as normalized null', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const expected = {
      ...captureNotificationDeliveryObservation(record),
      payload: '{"overflow":1e400}',
    };
    const write = vi.spyOn(storage, 'updateNotificationDeliveryIfUnchanged');
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(
        await recordNotificationDeliveryFailure(
          captureNotificationDeliveryStorage(storage),
          expected,
          NOW,
          10,
          { type: 'failure', error: 'refused' },
        ),
      ).toBe('uncertain');
      expect(write).not.toHaveBeenCalled();
    } finally {
      log.mockRestore();
    }
  });

  it('rejects an exhausted action below the bound without writing', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const write = vi.spyOn(storage, 'updateNotificationDeliveryIfUnchanged');
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(
        await recordNotificationDeliveryFailure(
          captureNotificationDeliveryStorage(storage),
          captureNotificationDeliveryObservation(record),
          NOW,
          10,
          { type: 'exhausted' },
        ),
      ).toBe('uncertain');
      expect(write).not.toHaveBeenCalled();
    } finally {
      log.mockRestore();
    }
  });

  it('does not read back a conditional mismatch or rewrite a successful receipt', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const delivered = await storage.updateNotification({
      ...record,
      status: 'delivered',
      deliveredSignalId: 'delivered',
    });
    const read = vi.spyOn(storage, 'getNotification');
    expect(await failure(storage, record, 1)).toBe('unchanged');
    expect(read).not.toHaveBeenCalled();
    expect(await storage.getNotification(record)).toEqual(delivered);
  });
});

describe('bounded notification dispatch tick', () => {
  it.each([
    0,
    -1,
    0.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ])('rejects invalid attempt bounds %s synchronously before dependencies', (maxDeliveryAttempts) => {
    const options = {
      maxDeliveryAttempts,
      get storage() {
        throw new Error('dependency accessed');
      },
    } as unknown as NotificationDispatchTickOptions;
    expect(() => createNotificationDispatchTickImpl(options)).toThrow(
      RangeError,
    );
  });

  it('captures the delivery capability before the zero-limit no-op, leaving the other dependencies unread', async () => {
    const storage = notificationStorage();
    let storageReads = 0;
    const options = {
      limit: 0,
      get storage() {
        storageReads += 1;
        return storage;
      },
      get topology() {
        throw new Error('topology accessed');
      },
      get resolveContext() {
        throw new Error('context accessed');
      },
      get now() {
        throw new Error('clock accessed');
      },
      get executionFence() {
        throw new Error('fence accessed');
      },
    } as unknown as NotificationDispatchTickOptions;
    const list = vi.spyOn(storage, 'listDueNotifications');

    const tick = createNotificationDispatchTickImpl(options);
    expect(storageReads).toBe(1);

    expect(await tick()).toEqual({ due: 0, delivered: 0, failed: 0 });
    expect(storageReads).toBe(1);
    expect(list).not.toHaveBeenCalled();
  });

  it('refuses a zero-limit tick on storage without conditional delivery', () => {
    const storage = new InMemoryNotificationsStorage();
    const read = vi.spyOn(storage, 'listDueNotifications');
    const build = () =>
      createNotificationDispatchTick({
        storage: storage as unknown as NotificationDeliveryStorage,
        topology: { send: vi.fn() } as unknown as ThreadTopology,
        resolveContext: actorContext,
        limit: 0,
      });

    expect(build).toThrow(TypeError);
    expect(build).toThrow(
      'notification dispatch requires conditional delivery storage',
    );
    expect(read).not.toHaveBeenCalled();
  });

  it('resolves the zero-limit no-op with the captured capability unused', async () => {
    const storage = notificationStorage();
    const list = vi.spyOn(storage, 'listDueNotifications');
    const get = vi.spyOn(storage, 'getNotification');
    const update = vi.spyOn(storage, 'updateNotificationDeliveryIfUnchanged');
    const send = vi.fn();
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      limit: 0,
    });

    expect(await tick()).toEqual({ due: 0, delivered: 0, failed: 0 });
    expect(list).not.toHaveBeenCalled();
    expect(get).not.toHaveBeenCalled();
    expect(update).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it('refuses ordinary Core storage before reading or sending', () => {
    const storage = new InMemoryNotificationsStorage();
    const read = vi.spyOn(storage, 'listDueNotifications');
    const send = vi.fn();
    expect(() =>
      createNotificationDispatchTick({
        storage: storage as unknown as NotificationDeliveryStorage,
        topology: { send } as unknown as ThreadTopology,
        resolveContext: actorContext,
      }),
    ).toThrow(TypeError);
    expect(read).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();
  });

  it('uses the approved default to retire a refused oldest row and advance the due window', async () => {
    const storage = notificationStorage();
    const poison = await pending(storage, {
      id: 'poison',
      deliverAt: new Date(0),
    });
    const next = await pending(storage, {
      id: 'next',
      deliverAt: new Date('2030-01-01T00:00:00Z'),
    });
    let now = new Date(NOW);
    const send = vi.fn(async () => new Response(null, { status: 404 }));
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => now,
      limit: 1,
    });
    expect(DEFAULT_MAX_NOTIFICATION_DELIVERY_ATTEMPTS).toBe(10);
    for (let attempt = 1; attempt <= 10; attempt += 1) {
      expect(await tick()).toEqual(
        attempt === 10
          ? { due: 1, delivered: 0, failed: 0, discarded: 1 }
          : { due: 1, delivered: 0, failed: 1 },
      );
      const current = await storage.getNotification(poison);
      expect(current?.deliveryAttempts).toBe(attempt);
      if (current?.deliverAt) now = current.deliverAt;
    }
    const terminal = await storage.getNotification(poison);
    expect(terminal).toMatchObject({
      status: 'discarded',
      deliveryAttempts: 10,
      lastDeliveryError: 'thread notification dispatch returned 404',
      deliveryReason: 'delivery-attempts-exhausted',
    });
    expect(terminal?.deliverAt).toBeUndefined();
    expect(terminal?.summaryAt).toBeUndefined();
    expect(
      (await storage.listNotifications({ threadId: poison.threadId })).find(
        (record) => record.id === poison.id,
      ),
    ).toEqual(terminal);
    now = new Date('2030-01-01T00:00:00Z');
    expect(await storage.listDueNotifications({ now, limit: 1 })).toEqual([
      next,
    ]);
    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    expect(send).toHaveBeenCalledTimes(11);
  });

  it.each([
    { deliveryAttempts: 10, resourceId: 'acme_resource' },
    { deliveryAttempts: 10, resourceId: 'bad/resource' },
    {
      deliveryAttempts: Number.MAX_SAFE_INTEGER,
      resourceId: 'acme_resource',
    },
    {
      deliveryAttempts: Number.MAX_SAFE_INTEGER,
      resourceId: 'bad/resource',
    },
  ])('discards pre-exhausted count $deliveryAttempts for $resourceId without another target call', async ({
    deliveryAttempts,
    resourceId,
  }) => {
    const storage = notificationStorage();
    const record = await pending(storage, {
      resourceId,
      summaryAt: new Date(0),
    });
    const before = await storage.updateNotification({
      ...record,
      deliveryAttempts,
      lastDeliveryError: 'original',
      lastDeliveryAttemptAt: new Date(1000),
      summarySignalId: 'prior-summary',
    });
    const send = vi.fn();
    const context = vi.fn(actorContext);
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: context,
      now: () => NOW,
    });
    expect(await tick()).toEqual({
      due: 1,
      delivered: 0,
      failed: 0,
      discarded: 1,
    });
    const current = await storage.getNotification(record);
    expect(current).toMatchObject({
      deliveryAttempts,
      lastDeliveryError: before.lastDeliveryError,
      lastDeliveryAttemptAt: before.lastDeliveryAttemptAt,
      summarySignalId: 'prior-summary',
      status: 'discarded',
    });
    expect(send).not.toHaveBeenCalled();
    expect(context).not.toHaveBeenCalled();
    expect(current?.deliverAt).toBeUndefined();
    expect(current?.summaryAt).toBeUndefined();
  });

  it.each([
    403,
    404,
    'transport',
  ] as const)('bounds repeated %s refusal', async (refusal) => {
    const storage = notificationStorage();
    const record = await pending(storage);
    let now = new Date(NOW);
    const send = vi.fn(async () => {
      if (refusal === 'transport') throw new Error('transport unavailable');
      return new Response(null, { status: refusal });
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => now,
      maxDeliveryAttempts: 2,
    });
    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    const deferred = await storage.getNotification(record);
    if (!deferred?.deliverAt) throw new Error('missing retry');
    now = deferred.deliverAt;
    expect(await tick()).toEqual({
      due: 1,
      delivered: 0,
      failed: 0,
      discarded: 1,
    });
    expect((await storage.getNotification(record))?.deliveryAttempts).toBe(2);
  });

  it('captures the bound, dependencies, writer, and attempt time before waits', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const now = new Date(NOW);
    let listed!: () => void;
    let release!: () => void;
    const waiting = new Promise<void>((resolve) => {
      listed = resolve;
    });
    const barrier = new Promise<void>((resolve) => {
      release = resolve;
    });
    const list = storage.listDueNotifications.bind(storage);
    vi.spyOn(storage, 'listDueNotifications').mockImplementation(
      async (input) => {
        const records = await list(input);
        input.now.setTime(0);
        listed();
        await barrier;
        return records;
      },
    );
    const send = vi.fn(async () => {
      throw new Error('captured target');
    });
    const options: NotificationDispatchTickOptions = {
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => now,
      maxDeliveryAttempts: 1,
      executionFence: 'none',
    };
    const tick = createNotificationDispatchTickImpl(options);
    const result = tick();
    await waiting;
    options.maxDeliveryAttempts = 20;
    options.storage = notificationStorage();
    options.resolveContext = () => {
      throw new Error('replacement context');
    };
    options.topology = { send: vi.fn() } as unknown as ThreadTopology;
    const replaced = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockRejectedValue(new Error('replacement writer'));
    now.setTime(0);
    release();
    expect(await result).toEqual({
      due: 1,
      delivered: 0,
      failed: 0,
      discarded: 1,
    });
    expect(replaced).not.toHaveBeenCalled();
    expect(send).toHaveBeenCalledTimes(1);
    expect(await storage.getNotification(record)).toMatchObject({
      deliveryAttempts: 1,
      lastDeliveryAttemptAt: NOW,
      lastDeliveryError: 'captured target',
    });
  });

  it('keeps detached batch observations after a topology await mutates source records', async () => {
    const storage = notificationStorage();
    const record = await pending(storage, { payload: { name: 'before' } });
    const listed = [record];
    vi.spyOn(storage, 'listDueNotifications').mockResolvedValue(listed);
    const send = vi.fn(async () => {
      record.threadId = 'replacement';
      record.deliveryAttempts = 9;
      record.deliverAt?.setTime(0);
      (record.payload as { name: string }).name = 'after';
      throw new Error('target refused');
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });
    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    expect(
      await storage.getNotification({ threadId: 'acme_thread', id: 'pending' }),
    ).toMatchObject({
      deliveryAttempts: 1,
      payload: { name: 'before' },
      lastDeliveryError: 'target refused',
    });
  });

  it.each([
    'summary',
    'delivered',
    'denied',
    'failed',
  ] as const)('preserves downstream %s bookkeeping after response loss', async (mode) => {
    const storage = notificationStorage();
    const record = await pending(storage, { summaryAt: new Date(0) });
    let downstream: NotificationRecord | null = null;
    const send = vi.fn(async () => {
      if (mode === 'failed') await failure(storage, record);
      else
        await storage.updateNotification({
          id: record.id,
          threadId: record.threadId,
          ...(mode === 'summary'
            ? {
                summaryAt: null,
                summarySignalId: 'summary',
                lastDeliveryAttemptAt: NOW,
              }
            : mode === 'delivered'
              ? {
                  status: 'delivered',
                  deliveredSignalId: 'signal',
                  lastDeliveryAttemptAt: NOW,
                }
              : {
                  status: 'discarded',
                  deliveryReason: 'content-policy-denied',
                  lastDeliveryAttemptAt: NOW,
                }),
        });
      downstream = await storage.getNotification(record);
      throw new Error('HTTP response lost');
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      maxDeliveryAttempts: 1,
    });
    expect(await tick()).toEqual({ due: 1, delivered: 0, failed: 1 });
    expect(await storage.getNotification(record)).toEqual(downstream);
    if (mode === 'failed')
      expect(downstream).toMatchObject({
        deliveryAttempts: 1,
        lastDeliveryError: 'target refused',
      });
  });

  it('skips a pending delivered receipt but permits a summarized individual', async () => {
    const storage = notificationStorage();
    const delivered = await pending(storage, { id: 'delivered' });
    await storage.updateNotification({
      ...delivered,
      deliveredSignalId: 'signal',
      deliveryAttempts: 10,
    });
    const summarized = await pending(storage, { id: 'summarized' });
    await storage.updateNotification({
      ...summarized,
      summarySignalId: 'summary',
    });
    const bodies: unknown[] = [];
    const send = vi.fn(async (_context, _thread, _path, input: RequestInit) => {
      bodies.push(JSON.parse(String(input.body)));
      return new Response(JSON.stringify({ delivered: 1, failed: 0 }));
    });
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
    });
    expect(await tick()).toEqual({ due: 2, delivered: 1, failed: 0 });
    expect(bodies).toEqual([
      expect.objectContaining({ notificationIds: ['summarized'] }),
    ]);
    expect((await storage.getNotification(delivered))?.status).toBe('pending');
  });

  it('contains malformed records and diagnostic failures while settling distinct physical keys', async () => {
    const storage = notificationStorage();
    const first = await pending(storage, { id: 'same', threadId: 'first' });
    const second = await pending(storage, { id: 'same', threadId: 'second' });
    const malformed = await pending(storage, { id: 'malformed' });
    Object.assign(malformed, { deliveryAttempts: null });
    vi.spyOn(storage, 'listDueNotifications').mockResolvedValue([
      first,
      second,
      first,
      malformed,
    ]);
    const write = storage.updateNotificationDeliveryIfUnchanged.bind(storage);
    vi.spyOn(
      storage,
      'updateNotificationDeliveryIfUnchanged',
    ).mockImplementation((input) => {
      if (input.expected.threadId === 'first') throw new Error('before commit');
      return write(input);
    });
    const send = vi.fn(async () => {
      throw {
        toString() {
          throw new Error('coercion failed');
        },
      };
    });
    const logger = vi.spyOn(console, 'error').mockImplementation(() => {
      throw new Error('logger failed');
    });
    try {
      const tick = createNotificationDispatchTick({
        storage,
        topology: { send } as unknown as ThreadTopology,
        resolveContext: actorContext,
        now: () => NOW,
        maxDeliveryAttempts: 1,
      });
      expect(await tick()).toEqual({
        due: 4,
        delivered: 0,
        failed: 2,
        discarded: 1,
      });
      expect((await storage.getNotification(first))?.deliveryAttempts).toBe(0);
      expect(await storage.getNotification(second)).toMatchObject({
        status: 'discarded',
        deliveryAttempts: 1,
        lastDeliveryError: 'unreadable error',
      });
      expect((await storage.getNotification(malformed))?.deliveryAttempts).toBe(
        0,
      );
      expect(send).toHaveBeenCalledTimes(2);
    } finally {
      logger.mockRestore();
    }
  });
});

describe('notification delivery boundary cases', () => {
  it('records the maximum safe attempt at an equal maximum safe bound', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const observed = await storage.updateNotification({
      ...record,
      deliveryAttempts: Number.MAX_SAFE_INTEGER - 1,
    });
    expect(await failure(storage, observed, Number.MAX_SAFE_INTEGER)).toBe(
      'discarded',
    );
    expect(await storage.getNotification(record)).toMatchObject({
      deliveryAttempts: Number.MAX_SAFE_INTEGER,
      status: 'discarded',
    });
  });

  it('keeps malformed composite keys distinct without requiring route-safe storage IDs', async () => {
    const storage = notificationStorage();
    const template = await pending(storage);
    const first = { ...template, threadId: 'a\0b', id: 'c' };
    const second = {
      ...template,
      threadId: 'a',
      id: 'b\0c',
      resourceId: 'bad/resource',
    };
    vi.spyOn(storage, 'listDueNotifications').mockResolvedValue([
      first,
      second,
    ]);
    const write = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockResolvedValue({ applied: false });
    const send = vi.fn();
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveContext: actorContext,
      now: () => NOW,
      maxDeliveryAttempts: 1,
    });
    expect(await tick()).toEqual({
      due: 2,
      delivered: 0,
      failed: 2,
    });
    expect(send).not.toHaveBeenCalled();
    expect(
      write.mock.calls.map(([input]) => [
        input.expected.threadId,
        input.expected.id,
      ]),
    ).toEqual([
      ['a\0b', 'c'],
      ['a', 'b\0c'],
    ]);
  });

  it.each([
    'wrong-key',
    'wrong-receipt',
  ] as const)('requires exact persisted state after a valid-shaped %s applied result', async (mode) => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const read = vi.spyOn(storage, 'getNotification');
    vi.spyOn(
      storage,
      'updateNotificationDeliveryIfUnchanged',
    ).mockResolvedValue({
      applied: true,
      record: {
        ...record,
        ...(mode === 'wrong-key'
          ? { id: 'different' }
          : { status: 'discarded', deliveryReason: 'content-policy-denied' }),
      },
    });
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(await failure(storage, record, 1)).toBe('uncertain');
      expect(read).toHaveBeenCalledTimes(1);
      expect(await storage.getNotification(record)).toEqual(record);
    } finally {
      log.mockRestore();
    }
  });

  it('recovers a committed exact target after a malformed storage result', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const write = storage.updateNotificationDeliveryIfUnchanged.bind(storage);
    const call = vi
      .spyOn(storage, 'updateNotificationDeliveryIfUnchanged')
      .mockImplementation(async (input) => {
        await write(input);
        return { applied: true } as NotificationDeliveryUpdateResult;
      });
    const read = vi.spyOn(storage, 'getNotification');
    expect(await failure(storage, record, 1)).toBe('discarded');
    expect(call).toHaveBeenCalledTimes(1);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it('preserves date ordering across extended ISO years', async () => {
    const storage = notificationStorage();
    const record = await pending(storage, {
      summaryAt: new Date('+010000-01-01T00:00:00.000Z'),
    });
    const now = new Date('+010000-01-01T00:00:00.000Z');
    expect(await failure(storage, record, 10, now)).toBe('deferred');
    const current = await storage.getNotification(record);
    const retryAt = new Date(now.getTime() + 1000);
    expect(current?.deliverAt).toEqual(retryAt);
    expect(current?.summaryAt).toEqual(retryAt);
  });
});

describe('notification bookkeeping diagnostics', () => {
  it('does not log unreadable JSON content or serializer exceptions', async () => {
    const storage = notificationStorage();
    const record = await pending(storage);
    const expected = {
      ...captureNotificationDeliveryObservation(record),
      payload: '{"token":"private-json"',
    };
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});
    try {
      expect(
        await recordNotificationDeliveryFailure(
          captureNotificationDeliveryStorage(storage),
          expected,
          NOW,
          10,
          { type: 'failure', error: 'refused' },
        ),
      ).toBe('uncertain');
      expect(JSON.stringify(log.mock.calls)).not.toContain('private-json');
      record.payload = {
        toJSON() {
          throw new Error('private-serializer');
        },
      };
      expect(() => captureNotificationDeliveryObservation(record)).toThrow(
        'notification value is not JSON serializable',
      );
    } finally {
      log.mockRestore();
    }
  });
});
