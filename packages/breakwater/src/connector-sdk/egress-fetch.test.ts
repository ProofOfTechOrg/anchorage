// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it, vi } from 'vitest';

import {
  type EgressDenial,
  EgressDeniedError,
  EgressGuardError,
  type EgressRequestInit,
  egressFetch,
} from './egress-fetch.js';

// `body` models the runtime Response body stream the guard cancels on
// discarded redirect hops; omitted (undefined) for the common case.
function stubResponse(
  status: number,
  headers: Record<string, string> = {},
  body?: unknown,
) {
  const lower = new Map(
    Object.entries(headers).map(([name, value]) => [name.toLowerCase(), value]),
  );
  return {
    status,
    headers: { get: (name: string) => lower.get(name.toLowerCase()) ?? null },
    body,
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

const matchesEveryHost = {
  startsWith: () => true,
  slice: () => '',
  toString: () => 'x',
};
const stringMethodsEntry = {
  toString: () => 'api.example.com',
  toLowerCase: () => ({ replace: () => matchesEveryHost }),
};

function hostsWith(entry: unknown): unknown[] {
  return ['api.example.com', entry];
}

function hostsWithHole(): unknown[] {
  const hosts: unknown[] = ['api.example.com'];
  hosts.length = 2;
  return hosts;
}

const nonStringHostEntries: [string, () => unknown[], string][] = [
  ['null', () => hostsWith(null), 'null'],
  ['undefined', () => hostsWith(undefined), 'undefined'],
  ['a hole', hostsWithHole, 'undefined'],
  ['a number', () => hostsWith(123), 'number'],
  ['a Symbol', () => hostsWith(Symbol('api.example.com')), 'symbol'],
  ['a String object', () => hostsWith(new String('api.example.com')), 'object'],
  [
    'a plain object with toString',
    () => hostsWith({ toString: () => 'api.example.com' }),
    'object',
  ],
  [
    'an object with its own string methods',
    () => hostsWith(stringMethodsEntry),
    'object',
  ],
];

describe('egress decision metadata', () => {
  it.each([
    {
      input: { url: 'https://api.example.com/private?secret=hidden' },
      code: 'EGRESS_INPUT_INVALID',
      host: null,
    },
    { input: '/private?secret=hidden', code: 'EGRESS_URL_INVALID', host: null },
    {
      input: 'ftp://api.example.com/private?secret=hidden',
      code: 'EGRESS_SCHEME_NOT_ALLOWED',
      host: 'api.example.com',
    },
    {
      input: 'data:text/plain,hidden',
      code: 'EGRESS_SCHEME_NOT_ALLOWED',
      host: '',
    },
    {
      input: 'https://EVIL.EXAMPLE.ORG./private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: 'evil.example.org',
      legacyHost: 'evil.example.org.',
    },
    {
      input: 'https://[::1]/private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: '[::1]',
    },
    {
      input: 'https://_service.example.com/private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: '_service.example.com',
    },
    {
      input: 'https://a!b.example.com/private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: 'a!b.example.com',
    },
    {
      input: 'https://-host.example.com/private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: '-host.example.com',
    },
    {
      input: 'https://foo../private?secret=hidden',
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: 'foo.',
      legacyHost: 'foo..',
    },
    {
      input: 'custom://%F0%9F%8C%90/private?secret=hidden',
      code: 'EGRESS_SCHEME_NOT_ALLOWED',
      host: '%f0%9f%8c%90',
      legacyHost: '%F0%9F%8C%90',
    },
  ])('classifies $input as $code without fetching', async ({
    input,
    code,
    host,
    legacyHost,
  }) => {
    const { fn, calls } = baseFetch();
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    const failure = await guarded(input as string).catch(
      (error: unknown) => error,
    );

    expect(failure).toBeInstanceOf(EgressDeniedError);
    expect(failure).toMatchObject({
      kind: 'egress-denied',
      host: legacyHost ?? host,
      code,
      policyKind: 'egress-fetch',
      retryable: false,
      details: { host, hop: 0 },
    });
    expect(JSON.stringify(failure)).not.toContain('hidden');
    expect(calls).toHaveLength(0);
  });

  it.each([
    {
      location: 'https://[invalid/private?secret=hidden',
      code: 'EGRESS_REDIRECT_URL_INVALID',
      host: null,
    },
    {
      location: 'ftp://api.example.com/private?secret=hidden',
      code: 'EGRESS_REDIRECT_SCHEME_NOT_ALLOWED',
      host: 'api.example.com',
    },
    {
      location: 'https://evil.example.org/private?secret=hidden',
      code: 'EGRESS_REDIRECT_HOST_DENIED',
      host: 'evil.example.org',
    },
  ])('classifies $code and releases the redirect before refusing its request', async ({
    location,
    code,
    host,
  }) => {
    const cancel = vi.fn(() => Promise.resolve());
    const { fn, calls } = baseFetch(
      stubResponse(302, { location }, { cancel }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    const failure = await guarded('https://api.example.com/start').catch(
      (error: unknown) => error,
    );

    expect(failure).toBeInstanceOf(EgressDeniedError);
    expect(failure).toMatchObject({
      code,
      policyKind: 'egress-fetch',
      retryable: false,
      details: { host, hop: 1 },
    });
    expect(JSON.stringify(failure)).not.toContain('hidden');
    expect(calls).toHaveLength(1);
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it.each([
    {
      status: 0,
      maxRedirects: 20,
      body: undefined,
      code: 'EGRESS_REDIRECT_UNVERIFIABLE',
      message:
        'egressFetch: received an opaque redirect (status 0) whose Location cannot be read — this guard cannot verify the hop, so it fails closed; on a browser use redirect: "manual" and handle the 3xx yourself',
    },
    {
      status: 302,
      maxRedirects: 0,
      body: undefined,
      code: 'EGRESS_REDIRECT_LIMIT_EXCEEDED',
      message: 'egressFetch: exceeded 0 redirects',
    },
    {
      status: 307,
      maxRedirects: 20,
      body: { getReader: () => ({}) },
      code: 'EGRESS_REDIRECT_BODY_UNREPLAYABLE',
      message:
        'egressFetch: cannot follow a redirect that re-sends a one-shot (stream) body — buffer the body or handle the 3xx with redirect: "manual"',
    },
  ])('keeps the TypeError message for $code with no next request', async ({
    status,
    maxRedirects,
    body,
    code,
    message,
  }) => {
    const cancel = vi.fn(() => Promise.resolve());
    const { fn, calls } = baseFetch(
      stubResponse(status, { location: '/next?secret=hidden' }, { cancel }),
    );
    const guarded = egressFetch(['api.example.com'], {
      fetch: fn,
      maxRedirects,
    });

    const failure = await guarded('https://api.example.com/start', {
      method: 'POST',
      body,
    }).catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(TypeError);
    expect(failure).toBeInstanceOf(EgressGuardError);
    expect(failure).toMatchObject({
      kind: 'egress-guard',
      message,
      code,
      policyKind: 'egress-fetch',
      retryable: false,
      details: { host: 'api.example.com', hop: 1 },
    });
    expect(JSON.stringify(failure)).not.toContain('hidden');
    expect(calls).toHaveLength(1);
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('classifies opaque redirects after an allowed hop and disposes both responses', async () => {
    const cancel = vi.fn(() => Promise.resolve());
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: '/next' }, { cancel }),
      stubResponse(0, {}, { cancel }),
    );

    await expect(
      egressFetch(['api.example.com'], { fetch: fn })(
        'https://api.example.com/start',
      ),
    ).rejects.toMatchObject({
      code: 'EGRESS_REDIRECT_UNVERIFIABLE',
      retryable: false,
      details: { host: 'api.example.com', hop: 2 },
    });
    expect(calls).toHaveLength(2);
    expect(cancel).toHaveBeenCalledTimes(2);
  });

  it('supports construction without a code and copies safe details', () => {
    const denial = { host: 'api.example.com', hop: 0, reason: 'custom reason' };
    const error = new EgressDeniedError(denial);
    denial.host = 'changed.example.com';

    expect(error).toMatchObject({
      message: 'egress denied: custom reason',
      host: 'api.example.com',
      hop: 0,
      reason: 'custom reason',
      code: 'EGRESS_DENIED',
      policyKind: 'egress-fetch',
      retryable: false,
      details: { host: 'api.example.com', hop: 0 },
    });
    expect(Object.isFrozen(error.details)).toBe(true);
  });
});

describe('egressFetch construction', () => {
  it('rejects allowlist entries that are not bare hostnames', () => {
    // #given / #when / #then
    expect(() => egressFetch(['https://api.example.com'])).toThrow(TypeError);
    expect(() => egressFetch(['api.example.com/path'])).toThrow(
      /bare hostname/,
    );
  });

  it.each(
    nonStringHostEntries,
  )('refuses %s as an allowlist entry', (_label, allowedHosts, got) => {
    // #when / #then
    expect(() =>
      egressFetch(allowedHosts() as unknown as readonly string[]),
    ).toThrow(
      new TypeError(
        `egressFetch: allowedHosts entry 1 must be a string (got ${got})`,
      ),
    );
  });

  it('refuses an allowlist that is not an array', () => {
    // #when / #then
    expect(() =>
      egressFetch('api.example.com' as unknown as readonly string[]),
    ).toThrow(new TypeError('egressFetch: allowedHosts must be an array'));
  });

  it('enforces the allowlist entries it validated', async () => {
    // #given
    let reads = 0;
    const allowedHosts = new Proxy(['api.example.com'], {
      get(target, key, receiver) {
        if (key === '0') {
          reads += 1;
          return reads === 1 ? 'api.example.com' : stringMethodsEntry;
        }
        return Reflect.get(target, key, receiver);
      },
    });
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(allowedHosts, { fetch: fn });
    // #when / #then
    await expect(guarded('https://exfil.example/private')).rejects.toThrow(
      EgressDeniedError,
    );
    expect(calls).toHaveLength(0);
    await guarded('https://api.example.com/v1');
    expect(calls.map((call) => call.url)).toEqual([
      'https://api.example.com/v1',
    ]);
    expect(reads).toBe(1);
  });

  it('rejects a non-integer or negative maxRedirects at construction', () => {
    // #when / #then
    expect(() =>
      egressFetch(['api.example.com'], { maxRedirects: -1 }),
    ).toThrow(TypeError);
    expect(() =>
      egressFetch(['api.example.com'], { maxRedirects: 1.5 }),
    ).toThrow(TypeError);
    expect(() =>
      egressFetch(['api.example.com'], { maxRedirects: Number.NaN }),
    ).toThrow(TypeError);
    // #then — 0 is valid
    expect(() =>
      egressFetch(['api.example.com'], { maxRedirects: 0 }),
    ).not.toThrow();
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
    // follow mode drives redirects manually
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
    // #given
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

  it('normalizes case and trailing dots', async () => {
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
    // #when / #then — {href} is a URL; {url} is a Request
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
    // #given — the fetch spec strips Authorization when
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

  it('refuses to follow a 307 that would re-send a one-shot async-iterable (Node Readable) body', async () => {
    // #given — a Node Readable / async-iterable body is one-shot but has no
    // getReader; re-sending it across a 307 would silently transmit nothing
    const { fn } = baseFetch(stubResponse(307, { location: '/retry' }));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    const streamBody = { [Symbol.asyncIterator]: () => ({}) };
    // #when / #then
    await expect(
      guarded('https://api.example.com/things', {
        method: 'POST',
        body: streamBody,
      }),
    ).rejects.toThrow(/one-shot \(stream\) body/);
  });

  it('fails closed on an opaque status-0 response', async () => {
    // #given — a browser's redirect: 'manual' returns an opaque status-0
    // response (Workers/Node return a real 3xx); the guard cannot inspect the
    // hop, so it must refuse rather than resolve to the unfollowed response
    const { fn } = baseFetch(
      stubResponse(0, { location: 'https://api.example.com/next' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when / #then
    await expect(guarded('https://api.example.com/start')).rejects.toThrow(
      TypeError,
    );
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

  it('returns the 3xx to the caller in manual mode', async () => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://exfil.example.org/x' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start', {
      method: 'POST',
      redirect: 'manual',
    });
    // #then — no hop happens, so the disallowed Location never gets fetched
    expect(response.status).toBe(302);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.init).toMatchObject({
      method: 'POST',
      redirect: 'manual',
    });
  });

  it.each([
    {
      name: '302 with an absolute Location',
      status: 302,
      location: 'https://exfil.example.org/x',
    },
    { name: '307 with a relative Location', status: 307, location: '/retry' },
    { name: '302 without Location', status: 302 },
    { name: 'an opaque redirect (status 0)', status: 0 },
  ])('rejects $name in error mode', async ({ status, location }) => {
    const cancel = vi.fn(() => Promise.resolve());
    const { fn, calls } = baseFetch(
      stubResponse(status, location === undefined ? {} : { location }, {
        cancel,
      }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    const error = await guarded('https://api.example.com/start', {
      method: 'POST',
      redirect: 'error',
    }).catch((failure: unknown) => failure);

    expect(error).toBeInstanceOf(TypeError);
    expect(error).not.toBeInstanceOf(EgressGuardError);
    expect(error).toMatchObject({
      message: 'fetch failed',
      cause: new Error('unexpected redirect'),
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]?.init).toMatchObject({
      method: 'POST',
      redirect: 'manual',
    });
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it.each([
    200, 304, 404,
  ])('returns status %s unchanged in error mode', async (status) => {
    const cancel = vi.fn(() => Promise.resolve());
    const response = stubResponse(status, {}, { cancel });
    const { fn, calls } = baseFetch(response);
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    await expect(
      guarded('https://api.example.com/start', { redirect: 'error' }),
    ).resolves.toBe(response);

    expect(calls).toHaveLength(1);
    expect(calls[0]?.init).toMatchObject({ redirect: 'manual' });
    expect(cancel).not.toHaveBeenCalled();
  });

  function inheritedInit(redirect?: 'manual' | 'error') {
    const controller = new AbortController();
    const headers = {
      'content-type': 'application/json',
      authorization: 'Bearer t',
    };
    const cf = { cacheTtl: 5 };
    const init = Object.create({
      method: 'POST',
      headers,
      body: '{"a":1}',
      signal: controller.signal,
      cf,
      cache: 'no-store',
      credentials: 'include',
      ...(redirect === undefined ? {} : { redirect }),
    }) as EgressRequestInit;
    return { init, headers, signal: controller.signal, cf };
  }

  it.each([
    'manual',
    'error',
  ] as const)('forwards request init inherited members in %s mode', async (redirect) => {
    const { init, headers, signal, cf } = inheritedInit(redirect);
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    await guarded('https://api.example.com/things', init);

    expect(calls).toHaveLength(1);
    expect(calls[0]?.init).toMatchObject({
      method: 'POST',
      body: '{"a":1}',
      cache: 'no-store',
      credentials: 'include',
      redirect: 'manual',
    });
    expect(calls[0]?.init?.headers).toBe(headers);
    expect(calls[0]?.init?.signal).toBe(signal);
    expect(calls[0]?.init?.cf).toBe(cf);
  });

  it('forwards request init inherited members across a 307 redirect', async () => {
    const { init, headers, signal, cf } = inheritedInit();
    const { fn, calls } = baseFetch(
      stubResponse(307, { location: '/retry' }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    await guarded('https://api.example.com/things', init);

    expect(calls).toHaveLength(2);
    for (const call of calls) {
      expect(call.init).toMatchObject({
        method: 'POST',
        body: '{"a":1}',
        cache: 'no-store',
        credentials: 'include',
        redirect: 'manual',
      });
      expect(call.init?.signal).toBe(signal);
      expect(call.init?.cf).toBe(cf);
    }
    expect(calls[0]?.init?.headers).toBe(headers);
    expect(hopHeaders(calls[1] as BaseCall).get('content-type')).toBe(
      'application/json',
    );
  });

  it('forwards request init members from a Request', async () => {
    const request = new Request('https://api.example.com/things', {
      method: 'POST',
      headers: { 'x-test': '1' },
      body: 'payload',
    });
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    // Request provides RequestInit members through prototype accessors.
    await guarded(
      'https://api.example.com/things',
      request as unknown as EgressRequestInit,
    );

    expect(calls).toHaveLength(1);
    expect(calls[0]?.init?.method).toBe('POST');
    expect(hopHeaders(calls[0] as BaseCall).get('x-test')).toBe('1');
    expect(request.body).not.toBeNull();
    expect(calls[0]?.init?.body).toBe(request.body);
  });

  it.each<[string, unknown]>([
    ['an array', ['follow']],
    ['a String object', new String('follow')],
    ['an object with toString', { toString: () => 'follow' }],
    [
      'an object with Symbol.toPrimitive',
      { [Symbol.toPrimitive]: () => 'follow' },
    ],
    ['a Symbol', Symbol('follow')],
  ])('refuses %s as init.redirect before the base fetch runs', async (_label, redirect) => {
    // #given
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://exfil.example.org/x' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const failure = await guarded('https://api.example.com/start', {
      redirect: redirect as never,
    }).catch((error: unknown) => error);
    // #then
    expect(failure).toBeInstanceOf(EgressDeniedError);
    expect(failure).toMatchObject({
      code: 'EGRESS_INPUT_INVALID',
      host: 'api.example.com',
      hop: 0,
      reason: "init.redirect must be 'follow', 'manual' or 'error'",
      details: { host: 'api.example.com', hop: 0 },
    });
    expect(calls).toHaveLength(0);
  });

  it.each([
    undefined,
    'follow',
  ] as const)('refuses a non-string method before any request in %s mode', async (redirect) => {
    const { fn, calls } = baseFetch(stubResponse(200));
    const guarded = egressFetch(['api.example.com'], { fetch: fn });

    await expect(
      guarded('https://api.example.com/x', {
        method: 123 as unknown as string,
        ...(redirect === undefined ? {} : { redirect }),
      }),
    ).rejects.toBeInstanceOf(TypeError);
    expect(calls).toHaveLength(0);
  });

  it('sends the base the redirect mode it checked when init.redirect is a getter', async () => {
    // #given
    let reads = 0;
    const init = {
      get redirect(): 'follow' | 'manual' {
        reads += 1;
        return reads === 1 ? 'manual' : 'follow';
      },
    };
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: 'https://exfil.example.org/x' }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start', init);
    // #then
    expect(response.status).toBe(302);
    expect(calls).toHaveLength(1);
    const descriptor = Object.getOwnPropertyDescriptor(
      calls[0]?.init,
      'redirect',
    );
    expect(descriptor).toEqual({
      value: 'manual',
      writable: true,
      enumerable: true,
      configurable: true,
    });
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

  it('cancels each intermediate redirect body but never the returned response', async () => {
    // #given — an allowed hop then the final response.
    // Following must release the discarded 3xx's stream but leave the caller's
    // response intact (the caller still reads it).
    const hopCancel = vi.fn(() => Promise.resolve());
    const finalCancel = vi.fn(() => Promise.resolve());
    const { fn } = baseFetch(
      stubResponse(302, { location: '/moved' }, { cancel: hopCancel }),
      stubResponse(200, {}, { cancel: finalCancel }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start');
    // #then
    expect(response.status).toBe(200);
    expect(hopCancel).toHaveBeenCalledTimes(1);
    expect(finalCancel).not.toHaveBeenCalled();
  });

  it('cancels the redirect body when the hop cap is exceeded', async () => {
    // #given — an endless loop; the throw path must still release the body it
    // was about to discard (one release per 3xx read, the repeating stub)
    const cancel = vi.fn(() => Promise.resolve());
    const { fn } = baseFetch(
      stubResponse(
        302,
        { location: 'https://api.example.com/loop' },
        { cancel },
      ),
    );
    const guarded = egressFetch(['api.example.com'], {
      fetch: fn,
      maxRedirects: 2,
    });
    // #when / #then
    await expect(guarded('https://api.example.com/start')).rejects.toThrow(
      /exceeded 2 redirects/,
    );
    expect(cancel).toHaveBeenCalledTimes(3);
  });

  it('cancels the redirect body when the next hop is denied', async () => {
    // #given — a 302 to an undeclared host; the denial path must release the
    // 3xx body it read the Location from
    const cancel = vi.fn(() => Promise.resolve());
    const { fn } = baseFetch(
      stubResponse(
        302,
        { location: 'https://exfil.example.org/collect' },
        { cancel },
      ),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    await expect(
      guarded('https://api.example.com/start'),
    ).rejects.toBeInstanceOf(EgressDeniedError);
    // #then
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('cancels the redirect body when it refuses a one-shot stream re-send', async () => {
    // #given — a 307 that would re-send a consumed stream; the refusal path
    // must still release the 3xx body
    const cancel = vi.fn(() => Promise.resolve());
    const { fn } = baseFetch(
      stubResponse(307, { location: '/retry' }, { cancel }),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    const streamBody = { getReader: () => ({}) };
    // #when / #then
    await expect(
      guarded('https://api.example.com/things', {
        method: 'POST',
        body: streamBody,
      }),
    ).rejects.toThrow(/one-shot \(stream\) body/);
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('follows the redirect even if releasing the intermediate body rejects', async () => {
    // #given — cancel() rejects (stream already errored); disposal is
    // best-effort and must not surface as an unhandled rejection or fail the hop
    const cancel = vi.fn(() => Promise.reject(new Error('already errored')));
    const { fn } = baseFetch(
      stubResponse(302, { location: '/moved' }, { cancel }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start');
    // #then — the rejected cancel is swallowed; the follow still succeeds
    expect(response.status).toBe(200);
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('follows the redirect even if releasing the intermediate body throws synchronously', async () => {
    // #given — an injected vendor fetch whose 3xx body has a cancel() that
    // throws synchronously (a conformant stream would reject, not throw). The
    // sync throw must be swallowed, never pre-empt the follow-through — else it
    // would mask the real result, here aborting an otherwise-successful follow.
    const cancel = vi.fn(() => {
      throw new Error('already errored');
    });
    const { fn, calls } = baseFetch(
      stubResponse(302, { location: '/moved' }, { cancel }),
      stubResponse(200),
    );
    const guarded = egressFetch(['api.example.com'], { fetch: fn });
    // #when
    const response = await guarded('https://api.example.com/start');
    // #then — swallowed; the second hop fired and the follow reached 200
    expect(response.status).toBe(200);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(calls).toHaveLength(2);
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
        code: 'EGRESS_HOST_NOT_DECLARED',
        host: 'evil.example.org',
        reason: "host 'evil.example.org' is not in the allowed egress hosts",
        hop: 0,
      },
    ]);
  });

  it('preserves a frozen custom mapper error without adding metadata', async () => {
    const mapped = Object.freeze(new Error('mapper-owned error'));
    const { fn, calls } = baseFetch();
    const denied = vi.fn((_denial: EgressDenial) => mapped);
    const guarded = egressFetch(['api.example.com'], { fetch: fn, denied });

    await expect(guarded('https://evil.example.org/')).rejects.toBe(mapped);

    expect(denied).toHaveBeenCalledWith({
      code: 'EGRESS_HOST_NOT_DECLARED',
      host: 'evil.example.org',
      reason: "host 'evil.example.org' is not in the allowed egress hosts",
      hop: 0,
    });
    expect(mapped).not.toHaveProperty('code');
    expect(mapped).not.toHaveProperty('details');
    expect(calls).toHaveLength(0);
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
