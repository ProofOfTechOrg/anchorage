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

it('returns the authenticated subject version through the private binding', async () => {
  const secret = 'staging-local-probe-secret-32-bytes-minimum';
  const fetch = vi.fn(async () =>
    Response.json({
      versionId: 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa',
      deploymentTenant: 'flowsafenotificationfresh',
    }),
  );
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: secret,
  };
  const request = (credential: string) =>
    new Request('http://127.0.0.1/identity', {
      headers: { authorization: `Bearer ${credential}` },
    });
  expect((await handler.fetch(request('wrong'), env)).status).toBe(403);
  expect(fetch).not.toHaveBeenCalled();
  const response = await handler.fetch(request(secret), env);
  expect(response.status).toBe(200);
  expect(await response.json()).toEqual({
    versionId: 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa',
    deploymentTenant: 'flowsafenotificationfresh',
  });
  expect(fetch.mock.calls[0]?.[0]).toBe('http://subject/__staging/identity');
});

it('keeps start and completion on the authenticated private service binding', async () => {
  const secret = 'staging-local-probe-secret-32-bytes-minimum';
  const fetch = vi.fn(async () => new Response(null, { status: 204 }));
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: secret,
  };
  for (const path of ['/start', '/complete']) {
    const body =
      path === '/complete' ? '{"invocationId":"invocation-1"}' : undefined;
    const request = (credential?: string) =>
      new Request(`http://127.0.0.1${path}`, {
        method: 'POST',
        ...(credential
          ? { headers: { authorization: `Bearer ${credential}` } }
          : {}),
        ...(body ? { body } : {}),
      });
    expect((await handler.fetch(request(), env)).status).toBe(403);
    expect(fetch).not.toHaveBeenCalled();
    expect((await handler.fetch(request(secret), env)).status).toBe(204);
    const forwarded = fetch.mock.calls.at(-1);
    expect(forwarded?.[0]).toBe(`http://subject/__staging${path}`);
    if (body) expect(forwarded?.[1]?.body).toBe(body);
    fetch.mockClear();
  }
});

it('authenticates local admin requests and supplies only the proxy-owned admin credential', async () => {
  const probeSecret = 'staging-local-probe-secret-32-bytes-minimum';
  const adminSecret = 'staging-admin-secret-32-bytes-minimum';
  const fetch = vi.fn(async () => new Response(null, { status: 200 }));
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: probeSecret,
    MAINTENANCE_ADMIN_SECRET: adminSecret,
  };
  for (const [method, path, body] of [
    ['GET', '/admin/execution-fence', undefined],
    ['POST', '/admin/execution-fence', '{"expected":"open","next":"draining"}'],
    ['GET', '/admin/maintenance-status', undefined],
  ] as const) {
    const request = (credential: string) =>
      new Request(`http://127.0.0.1${path}`, {
        method,
        headers: { authorization: `Bearer ${credential}` },
        ...(body ? { body } : {}),
      });
    expect((await handler.fetch(request('wrong'), env)).status).toBe(403);
    expect(fetch).not.toHaveBeenCalled();
    expect((await handler.fetch(request(probeSecret), env)).status).toBe(200);
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[0]).toBe(`http://subject${path}`);
    expect(fetch.mock.calls[0]?.[1]).toMatchObject({
      method,
      headers: { authorization: `Bearer ${adminSecret}` },
    });
    expect(fetch.mock.calls[0]?.[1]?.body).toBe(body);
    fetch.mockClear();
  }
});

it('rejects unlisted admin operations, query strings, and oversized writes', async () => {
  const secret = 'staging-local-probe-secret-32-bytes-minimum';
  const fetch = vi.fn(async () => new Response(null, { status: 200 }));
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: secret,
    MAINTENANCE_ADMIN_SECRET: 'staging-admin-secret-32-bytes-minimum',
  };
  const headers = { authorization: `Bearer ${secret}` };
  for (const request of [
    new Request('http://127.0.0.1/admin/inventory', { headers }),
    new Request('http://127.0.0.1/admin/execution-fence', {
      method: 'DELETE',
      headers,
    }),
    new Request('http://127.0.0.1/admin/execution-fence?state=open', {
      headers,
    }),
  ]) {
    expect((await handler.fetch(request, env)).status).toBe(404);
  }
  expect(
    (
      await handler.fetch(
        new Request('http://127.0.0.1/admin/execution-fence', {
          method: 'POST',
          headers,
          body: 'x'.repeat(4_097),
        }),
        env,
      )
    ).status,
  ).toBe(413);
  expect(fetch).not.toHaveBeenCalled();
});

it('fails closed when the admin credential is missing or reused as the caller credential', async () => {
  const secret = 'staging-local-probe-secret-32-bytes-minimum';
  const fetch = vi.fn(async () => new Response(null, { status: 200 }));
  const env = {
    SUBJECT: { fetch } as unknown as Fetcher,
    STAGING_PROBE_SECRET: secret,
    MAINTENANCE_ADMIN_SECRET: undefined as string | undefined,
  };
  const request = new Request('http://127.0.0.1/admin/execution-fence', {
    headers: { authorization: `Bearer ${secret}` },
  });
  expect((await handler.fetch(request, env)).status).toBe(503);
  env.MAINTENANCE_ADMIN_SECRET = secret;
  expect((await handler.fetch(request, env)).status).toBe(503);
  expect(fetch).not.toHaveBeenCalled();
});
