// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  createSignalProviderHostTopology,
  SIGNAL_PROVIDER_HOST_INSTANCE_NAME,
  type SignalProviderHostNamespaceLike,
} from './host-topology.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

describe('createSignalProviderHostTopology', () => {
  it('addresses the deployment singleton and sends the internal arm command', async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response('not parsed', { status: 200 }));
    const idFromName = vi.fn((name: string) => `id:${name}`);
    const get = vi.fn(() => ({ fetch }));
    const namespace: SignalProviderHostNamespaceLike<string> = {
      idFromName,
      get,
    };

    await createSignalProviderHostTopology(
      namespace,
      DEPLOYMENT_IDENTITY_SECRET,
    ).reconcilePolling();

    expect(idFromName).toHaveBeenCalledWith(SIGNAL_PROVIDER_HOST_INSTANCE_NAME);
    expect(get).toHaveBeenCalledWith(
      `id:${SIGNAL_PROVIDER_HOST_INSTANCE_NAME}`,
    );
    expect(fetch).toHaveBeenCalledWith(
      'http://provider-host/arm',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          'x-flowsafe-deployment-identity': DEPLOYMENT_IDENTITY_SECRET,
        }),
      }),
    );
  });

  it('rejects a non-success host response without parsing its body', async () => {
    const topology = createSignalProviderHostTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: () =>
            Promise.resolve(
              new Response('private host detail', { status: 503 }),
            ),
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
    );

    await expect(topology.reconcilePolling()).rejects.toThrow('status 503');
  });

  it('propagates a host fetch failure', async () => {
    const topology = createSignalProviderHostTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: () => Promise.reject(new Error('host unavailable')),
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
    );

    await expect(topology.reconcilePolling()).rejects.toThrow(
      'host unavailable',
    );
  });
});
