// SPDX-License-Identifier: Apache-2.0
// SignalClient wire-format + error-mapping (plain node, injected fetch).

import { describe, expect, it } from 'vitest';

import {
  SignalApiError,
  SignalClient,
  type SignalFetchLike,
} from './client.js';

function recorder(
  status = 200,
  payload: unknown = { decision: { action: 'deliver' } },
) {
  const calls: Array<{ url: string; init?: Parameters<SignalFetchLike>[1] }> =
    [];
  const fetch: SignalFetchLike = async (url, init) => {
    calls.push({ url, init });
    return {
      ok: status >= 200 && status < 300,
      status,
      json: async () => payload,
    };
  };
  return { fetch, calls };
}

describe('SignalClient', () => {
  it('POSTs a message to the thread channel with the auth headers', async () => {
    const { fetch, calls } = recorder();
    const client = new SignalClient({
      fetch,
      headers: { authorization: 'Bearer t' },
    });
    await client.sendMessage('acme_t1', {
      contents: 'hello',
      ifIdle: 'persist',
    });
    expect(calls[0]?.url).toBe('/api/threads/acme_t1/message');
    expect(calls[0]?.init?.method).toBe('POST');
    expect(calls[0]?.init?.headers?.authorization).toBe('Bearer t');
    expect(JSON.parse(calls[0]?.init?.body ?? '{}')).toEqual({
      contents: 'hello',
      ifIdle: 'persist',
    });
  });

  it('routes each channel to its path', async () => {
    const { fetch, calls } = recorder();
    const client = new SignalClient({ fetch });
    await client.queueMessage('acme_t1', { contents: 'q' });
    await client.sendSignal('acme_t1', { contents: 's' });
    await client.sendStateSignal('acme_t1', {
      id: 'g',
      cacheKey: 'k',
      contents: 'c',
    });
    await client.sendNotificationSignal('acme_t1', {
      source: 'x',
      kind: 'y',
      summary: 'z',
    });
    expect(calls.map((c) => c.url)).toEqual([
      '/api/threads/acme_t1/queue',
      '/api/threads/acme_t1/signal',
      '/api/threads/acme_t1/state',
      '/api/threads/acme_t1/notification',
    ]);
  });

  it('encodes the threadId in the path', async () => {
    const { fetch, calls } = recorder();
    const client = new SignalClient({ fetch });
    await client.sendMessage('acme_t/1', { contents: 'x' });
    expect(calls[0]?.url).toBe('/api/threads/acme_t%2F1/message');
  });

  it('maps a non-ok response to a SignalApiError carrying status + message', async () => {
    const { fetch } = recorder(403, { error: 'forbidden' });
    const client = new SignalClient({ fetch });
    await expect(
      client.sendMessage('acme_t1', { contents: 'x' }),
    ).rejects.toMatchObject({
      name: 'SignalApiError',
      status: 403,
      message: 'forbidden',
    });
    await expect(
      client.sendMessage('acme_t1', { contents: 'x' }),
    ).rejects.toBeInstanceOf(SignalApiError);
  });
});
