// SPDX-License-Identifier: Apache-2.0
import { SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import {
  type EgressRequestInit,
  egressFetch,
} from '../src/connector-sdk/egress-fetch.js';

const guarded = egressFetch(['api.example.com'], {
  // SELF uses Workers RequestInit types; the guard forwards their runtime values.
  fetch: (url: string, init?: EgressRequestInit) =>
    SELF.fetch(url, init as RequestInit),
});

describe('guarded fetch error mode inside workerd', () => {
  it('rejects a redirect with the fetch error contract', async () => {
    const error = await guarded('https://api.example.com/redirect', {
      redirect: 'error',
    }).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(TypeError);
    expect(error).toMatchObject({
      message: 'fetch failed',
      cause: new Error('unexpected redirect'),
    });
  });

  it('returns a non-redirect response', async () => {
    const response = await guarded('https://api.example.com/start', {
      redirect: 'error',
    });

    expect(response.status).toBe(200);
  });
});
