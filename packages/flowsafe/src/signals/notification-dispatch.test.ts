// SPDX-License-Identifier: Apache-2.0

import { InMemoryNotificationsStorage } from '@mastra/core/notifications';
import { describe, expect, it, vi } from 'vitest';

import type { TenantContext } from '../approval-api/index.js';
import type { ThreadTopology } from '../host-kit/index.js';
import { createNotificationDispatchTick } from './notification-dispatch.js';

const NOW = new Date('2026-07-20T12:00:00.000Z');

function tenant(tenantId: string): TenantContext {
  return {
    tenantId,
    actor: { id: 'maintenance', role: 'admin', tenantId },
    ownsMemoryId: (id: string) => id.startsWith(`${tenantId}_`),
  } as unknown as TenantContext;
}

describe('createNotificationDispatchTick', () => {
  it('rejects malformed and mixed-tenant rows before addressing a thread DO', async () => {
    const storage = new InMemoryNotificationsStorage();
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
    const mixed = await storage.createNotification({
      id: 'mixed',
      threadId: 'acme_thread',
      resourceId: 'globex_resource',
      agentId: 'agent',
      source: 'test',
      kind: 'ready',
      summary: 'mixed',
      deliverAt: new Date(NOW.getTime() - 1),
    });
    const send = vi.fn(
      async (
        _tenant: TenantContext,
        _threadId: string,
        _path: string,
        _init: RequestInit,
      ) => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveTenant: tenant,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 2, delivered: 1, failed: 1 });
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith(
      expect.objectContaining({ tenantId: 'acme' }),
      valid.threadId,
      '/signal/notifications/dispatch',
      expect.any(Object),
    );
    expect(
      await storage.getNotification({
        threadId: mixed.threadId,
        id: mixed.id,
      }),
    ).toMatchObject({
      deliveryAttempts: 1,
      lastDeliveryError:
        'notification has malformed or mixed-tenant memory ids',
    });
  });

  it('isolates a failed tenant/thread group from its neighbors', async () => {
    const storage = new InMemoryNotificationsStorage();
    for (const tenantId of ['acme', 'globex']) {
      await storage.createNotification({
        id: tenantId,
        threadId: `${tenantId}_thread`,
        resourceId: `${tenantId}_resource`,
        agentId: 'agent',
        source: 'test',
        kind: 'ready',
        summary: tenantId,
        deliverAt: new Date(NOW.getTime() - 1),
      });
    }
    const send = vi.fn(async (_tenant, threadId: string) =>
      threadId.startsWith('acme_')
        ? new Response('down', { status: 503 })
        : new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveTenant: tenant,
      now: () => NOW,
    });

    expect(await tick()).toEqual({ due: 2, delivered: 1, failed: 1 });
    expect(send).toHaveBeenCalledTimes(2);
  });

  it('groups the same tenant/thread/resource separately by persisted agent id', async () => {
    const storage = new InMemoryNotificationsStorage();
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
        _tenant: TenantContext,
        _threadId: string,
        _path: string,
        _init: RequestInit,
      ) => new Response(JSON.stringify({ delivered: 1, failed: 0 })),
    );
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveTenant: tenant,
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
    const storage = new InMemoryNotificationsStorage();
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
      resolveTenant: tenant,
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

  it('chunks 205 records into route-valid batches of 100, 100, and 5', async () => {
    const storage = new InMemoryNotificationsStorage();
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
    const send = vi.fn(async (_tenant, _thread, _path, init: RequestInit) => {
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
      resolveTenant: tenant,
      now: () => NOW,
      limit: 205,
    });

    expect(await tick()).toEqual({ due: 205, delivered: 205, failed: 0 });
    expect(sizes).toEqual([100, 100, 5]);
  });

  it('isolates a failed middle chunk and continues with later chunks', async () => {
    const storage = new InMemoryNotificationsStorage();
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
    const send = vi.fn(async (_tenant, _thread, _path, init: RequestInit) => {
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
      resolveTenant: tenant,
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
    const storage = new InMemoryNotificationsStorage();
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
          resolveTenant: tenant,
          limit,
        }),
      ).toThrow(RangeError);
    }
    const list = vi.spyOn(storage, 'listDueNotifications');
    const tick = createNotificationDispatchTick({
      storage,
      topology: { send } as unknown as ThreadTopology,
      resolveTenant: tenant,
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
