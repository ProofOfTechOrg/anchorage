// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import type { TenantContext } from '../approval-api/index.js';
import {
  createSignalProviderHostTopology,
  type SignalProviderHostNamespaceLike,
} from './host-topology.js';

function tenant(tenantId = 'acme', actorTenantId = tenantId): TenantContext {
  return {
    tenantId,
    actor: { id: 'operator-1', role: 'operator', tenantId: actorTenantId },
  } as TenantContext;
}

describe('createSignalProviderHostTopology', () => {
  it('addresses only the authenticated tenant and sends the internal arm command', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response('not parsed', { status: 200 }));
    const idFromName = vi.fn((name: string) => `id:${name}`);
    const get = vi.fn(() => ({ fetch }));
    const namespace: SignalProviderHostNamespaceLike<string> = {
      idFromName,
      get,
    };

    await createSignalProviderHostTopology(namespace).reconcilePolling(
      tenant(),
    );

    expect(idFromName).toHaveBeenCalledWith('acme');
    expect(get).toHaveBeenCalledWith('id:acme');
    expect(fetch).toHaveBeenCalledWith('http://provider-host/arm', {
      method: 'POST',
    });
  });

  it.each([
    ['malformed tenant', tenant('Bad_Name')],
    ['reserved tenant', tenant('system')],
    ['actor mismatch', tenant('acme', 'globex')],
  ])('refuses %s before addressing a namespace', async (_label, context) => {
    const idFromName = vi.fn((name: string) => name);
    const get = vi.fn(() => ({
      fetch: () => Promise.resolve(new Response(null, { status: 204 })),
    }));
    const topology = createSignalProviderHostTopology({ idFromName, get });

    await expect(topology.reconcilePolling(context)).rejects.toThrow();
    expect(idFromName).not.toHaveBeenCalled();
    expect(get).not.toHaveBeenCalled();
  });

  it('rejects a non-success host response without parsing its body', async () => {
    const topology = createSignalProviderHostTopology({
      idFromName: (name: string) => name,
      get: () => ({
        fetch: () =>
          Promise.resolve(new Response('private host detail', { status: 503 })),
      }),
    });

    await expect(topology.reconcilePolling(tenant())).rejects.toThrow(
      'status 503',
    );
  });

  it('propagates a host fetch failure', async () => {
    const topology = createSignalProviderHostTopology({
      idFromName: (name: string) => name,
      get: () => ({
        fetch: () => Promise.reject(new Error('host unavailable')),
      }),
    });

    await expect(topology.reconcilePolling(tenant())).rejects.toThrow(
      'host unavailable',
    );
  });
});
