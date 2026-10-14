import type { Fetcher } from '@cloudflare/workers-types';
import { expect, it, vi } from 'vitest';
import handler from './notification-staging-probe.worker.js';

it('forwards a staging request only after the local caller supplies the probe credential', async () => {
  const secret = 'staging-local-probe-secret-32-bytes-minimum';
  const fetch = vi.fn(async () => new Response(null, { status: 204 }));
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: secret,
  };
  const request = (credential?: string) =>
    new Request('http://127.0.0.1/ensure', {
      method: 'POST',
      ...(credential
        ? { headers: { authorization: `Bearer ${credential}` } }
        : {}),
    });

  expect((await handler.fetch(request(), env)).status).toBe(403);
  expect((await handler.fetch(request('wrong'), env)).status).toBe(403);
  expect(fetch).not.toHaveBeenCalled();

  expect((await handler.fetch(request(secret), env)).status).toBe(204);
  expect(fetch).toHaveBeenCalledOnce();
  const forwarded = fetch.mock.calls[0];
  expect(forwarded?.[0]).toBe('http://subject/__staging/ensure');
  expect(forwarded?.[1]).toMatchObject({ method: 'POST' });
  expect(forwarded?.[1]?.headers).toMatchObject({
    'x-flowsafe-staging-probe': secret,
  });
});
