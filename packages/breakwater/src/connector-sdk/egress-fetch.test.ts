// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  type EgressDenial,
  EgressDeniedError,
  egressFetch,
} from './egress-fetch.js';

// Plain structural response — breakwater's test tsconfig is lib-ES2022-only,
// so mocks model the fetch surface the same way the guard's own types do.
function stubResponse(status: number, headers: Record<string, string> = {}) {
  const lower = new Map(
    Object.entries(headers).map(([name, value]) => [name.toLowerCase(), value]),
  );
  return {
    status,
    headers: { get: (name: string) => lower.get(name.toLowerCase()) ?? null },
  };
}

type StubResponse = ReturnType<typeof stubResponse>;

interface BaseCall {
  url: string;
  init: Record<string, unknown> | undefined;
}

// Queued base fetch: each call consumes the next response; the last one
// repeats (covers the redirect-loop case without pre-counting hops).
function baseFetch(...responses: StubResponse[]) {
  const calls: BaseCall[] = [];
  const queue = [...responses];
  const fn = async (url: string, init?: Record<string, unknown>) => {
    calls.push({ url, init });
    const next = queue.length > 1 ? queue.shift() : queue[0];
    return next ?? stubResponse(200);
  };
  return { fn, calls };
}

function hopHeaders(call: BaseCall): { get(name: string): string | null } {
  return call.init?.headers as { get(name: string): string | null };
}

describe('egressFetch construction', () => {
  it('rejects allowlist entries that are not bare hostnames', () => {
    // #given / #when / #then
    expect(() => egressFetch(['https://api.example.com'])).toThrow(TypeError);
    expect(() => egressFetch(['api.example.com/path'])).toThrow(
      /bare hostname/,
    );
  });
});

describe('egressFetch host checks', () => {
  it('passes an allowed host through to the base fetch', async () => {
    // #given
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/v1/things', {
      method: 'POST',
      body: '{}',
    });
    // #then
    expect(response.status).toBe(200);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.url).toBe('https://api.example.com/v1/things');
    // follow mode drives redirects manually so no hop can escape the check
    expect(calls[0]?.init).toMatchObject({
      method: 'POST',
      body: '{}',
      redirect: 'manual',
    });
  });

  it('denies an undeclared host before the base fetch runs', async () => {
    // #given
    const { fn, calls } = baseFetch();
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const failure = await guarded('https://evil.example.org/x').catch(
      (error: unknown) => error,
    );
    // #then
    expect(failure).toBeInstanceOf(EgressDeniedError);
    expect((failure as EgressDeniedError).host).toBe('evil.example.org');
    expect((failure as EgressDeniedError).hop).toBe(0);
    expect(calls).toHaveLength(0);
  });

  it('denies everything when the allowlist is empty', async () => {
    // #given — no declared egress means no network
    const { fn, calls } = baseFetch();
    const guarded = egressFetch([], { fetch: fn });
    // #when / #then
    await expect(guarded('https://api.example.com/')).rejects.toBeInstanceOf(
      EgressDeniedError,
    );
    expect(calls).toHaveLength(0);
  });

  it('matches wildcards on label boundaries and excludes the apex', async () => {
    // #given
    const { fn } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['*.example.com'], { fetch: fn });
    // #when / #then
    await expect(guarded('https://api.example.com/')).resolves.toMatchObject({
      status: 200,
    });
    await expect(guarded('https://example.com/')).rejects.toBeInstanceOf(
      EgressDeniedError,
    );
    await expect(guarded('https://evil-example.com/')).rejects.toBeInstanceOf(
      EgressDeniedError,
    );
  });

  it('normalizes case and trailing dots like the declaration gate', async () => {
    // #given — 'API.EXAMPLE.COM.' is the same DNS name as 'api.example.com'
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://API.EXAMPLE.COM./v1');
    // #then
    expect(response.status).toBe(200);
    expect(calls).toHaveLength(1);
  });

  it('denies non-http(s) schemes', async () => {
    // #given
    const { fn, calls } = baseFetch();
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when / #then
    await expect(guarded('ftp://api.example.com/file')).rejects.toThrow(
      /scheme 'ftp:' is not http\(s\)/,
    );
    expect(calls).toHaveLength(0);
  });

  it('denies relative and unparseable URLs', async () => {
    // #given
    const { fn, calls } = baseFetch();
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when / #then
    await expect(guarded('/v1/things')).rejects.toThrow(
      /not an absolute, parseable URL/,
    );
    expect(calls).toHaveLength(0);
  });

  it('accepts URL objects and rejects Request-shaped input', async () => {
    // #given
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when / #then — {href} is a URL; {url} is a Request, which smuggles
    // body/redirect state the guard cannot see
    await expect(
      guarded({ href: 'https://api.example.com/v1' }),
    ).resolves.toMatchObject({ status: 200 });
    await expect(
      guarded({ url: 'https://api.example.com/v1' } as unknown as string),
    ).rejects.toThrow(/pass \(url, init\), not a Request/);
    expect(calls).toHaveLength(1);
  });
});

describe('egressFetch redirect following', () => {
  it('follows an allowed redirect and resolves a relative Location', async () => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: '/moved' }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start');
    // #then
    expect(response.status).toBe(200);
    expect(calls.map((call) => call.url)).toEqual([
      'https://api.example.com/start',
      'https://api.example.com/moved',
    ]);
  });

  it('denies a redirect hop to an undeclared host', async () => {
    // #given — the case platform 'follow' would silently allow
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://exfil.example.org/collect' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const failure = await guarded('https://api.example.com/start').catch(
      (error: unknown) => error,
    );
    // #then
    expect(failure).toBeInstanceOf(EgressDeniedError);
    expect((failure as EgressDeniedError).host).toBe('exfil.example.org');
    expect((failure as EgressDeniedError).hop).toBe(1);
    expect(calls).toHaveLength(1);
  });

  it('strips credential headers on a cross-origin hop and keeps them same-origin', async () => {
    // #given — two allowed hosts; the fetch spec strips Authorization when
    // the origin changes, and a manual follower must do the same
    const crossOrigin = baseFetch(
      stubResponse(302, { location: 'https://other.example.com/next' }),
      stubResponse(200),
    );
    const sameOrigin = baseFetch(
      stubResponse(302, { location: '/next' }),
      stubResponse(200),
    );
    const hosts = ['api.example.com', 'other.example.com'];
    const init = {
      headers: { authorization: 'Bearer secret', 'x-vendor': 'keep' },
    };
    // #when
    await egressFetch(hosts, { fetch: crossOrigin.fn })(
      'https://api.example.com/start',
      init,
    );
    await egressFetch(hosts, { fetch: sameOrigin.fn })(
      'https://api.example.com/start',
      init,
    );
    // #then
    const crossHop = hopHeaders(crossOrigin.calls[1] as BaseCall);
    expect(crossHop.get('authorization')).toBeNull();
    expect(crossHop.get('x-vendor')).toBe('keep');
    const sameHop = hopHeaders(sameOrigin.calls[1] as BaseCall);
    expect(sameHop.get('authorization')).toBe('Bearer secret');
  });

  it('rewrites 303 to a bodiless GET and drops content headers', async () => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(303, { location: '/created' }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    await guarded('https://api.example.com/things', {
      method: 'POST',
      body: '{"a":1}',
      headers: { 'content-type': 'application/json', 'x-vendor': 'keep' },
    });
    // #then
    expect(calls[1]?.init).toMatchObject({ method: 'GET', body: null });
    const headers = hopHeaders(calls[1] as BaseCall);
    expect(headers.get('content-type')).toBeNull();
    expect(headers.get('x-vendor')).toBe('keep');
  });

  it('preserves method and a re-sendable body across 307', async () => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(307, { location: '/retry' }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    await guarded('https://api.example.com/things', {
      method: 'PUT',
      body: 'payload',
    });
    // #then
    expect(calls[1]?.init).toMatchObject({ method: 'PUT', body: 'payload' });
  });

  it('refuses to follow a 307 that would re-send a one-shot stream body', async () => {
    // #given — re-sending a consumed stream would silently transmit nothing
    const { fn } = baseFetch(stubResponse(307, { location: '/retry' }));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    const streamBody = { getReader: () => ({}) };
    // #when / #then
    await expect(
      guarded('https://api.example.com/things', {
        method: 'POST',
        body: streamBody,
      }),
    ).rejects.toThrow(/one-shot \(stream\) body/);
  });

  it('throws after maxRedirects hops', async () => {
    // #given — an endless redirect loop
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://api.example.com/loop' }),
    );
    const guarded = egressFetch(['api.example.com'], {
      fetch: fn,
      maxRedirects: 2,
    });
    // #when / #then
    await expect(guarded('https://api.example.com/start')).rejects.toThrow(
      /exceeded 2 redirects/,
    );
    expect(calls).toHaveLength(3);
  });

  it('passes redirect: "manual" through and returns the 3xx to the caller', async () => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://exfil.example.org/x' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start', {
      redirect: 'manual',
    });
    // #then — no hop happens, so the disallowed Location never gets fetched
    expect(response.status).toBe(302);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.init).toMatchObject({ redirect: 'manual' });
  });

  it('returns a 3xx without a Location header as-is', async () => {
    // #given
    const { fn, calls } = baseFetch(stubResponse(302));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start');
    // #then
    expect(response.status).toBe(302);
    expect(calls).toHaveLength(1);
  });
});

describe('egressFetch seams', () => {
  it('maps denials through the denied() seam', async () => {
    // #given
    const denials: EgressDenial[] = [];
    const guarded = egressFetch(['api.example.com'], {
      fetch: baseFetch().fn,
      denied: (denial) => {
        denials.push(denial);
        return new Error(`mapped:${denial.host}:${denial.hop}`);
      },
    });
    // #when / #then
    await expect(guarded('https://evil.example.org/')).rejects.toThrow(
      'mapped:evil.example.org:0',
    );
    expect(denials).toEqual([
      {
        host: 'evil.example.org',
        reason: "host 'evil.example.org' is not in the allowed egress hosts",
        hop: 0,
      },
    ]);
  });

  it('defaults the base to the global fetch and fails loudly without one', async () => {
    // #given
    const { fn, calls } = baseFetch(stubResponse(200));
    vi.stubGlobal('fetch', fn);
    try {
      // #when
      const response = await egressFetch(['api.example.com'])(
        'https://api.example.com/v1',
      );
      // #then
      expect(response.status).toBe(200);
      expect(calls).toHaveLength(1);
      // #when the runtime has no fetch at all
      vi.stubGlobal('fetch', undefined);
      await expect(
        egressFetch(['api.example.com'])('https://api.example.com/v1'),
      ).rejects.toThrow(/no global fetch and none was injected/);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
