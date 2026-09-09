// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Cloudflare from 'cloudflare';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';
import { handleDirectReferenceHttpRequest } from '../scripts/direct-reference-http.js';
import {
  DIRECT_REFERENCE_LEASE,
  DirectReferenceTransport,
  type DirectReferenceTransportOptions,
} from '../scripts/direct-reference-transport.js';

const runtime = {
  requestTimeoutMs: 1000,
  invocationTimeoutMs: 5000,
  maxProviderRequests: 9,
};

function fixture(overrides: Partial<DirectReferenceTransportOptions> = {}) {
  const nativeFetch = vi.fn<typeof fetch>(async () => new Response('fixture'));
  const abort = new AbortController();
  const transport = new DirectReferenceTransport({
    runtime,
    startedAt: performance.now(),
    signal: abort.signal,
    fetch: nativeFetch,
    ...overrides,
  });
  return { transport, nativeFetch, abort };
}

describe('direct reference transport', () => {
  it('shares concurrent provider/maintenance counts and permits the last allowed attempt', async () => {
    const { transport, nativeFetch } = fixture();
    await Promise.all(
      Array.from({ length: 9 }, (_, i) =>
        (i % 2 ? transport.maintenanceFetch : transport.providerFetch)(
          'https://fixture.test',
        ),
      ),
    );
    expect(nativeFetch).toHaveBeenCalledTimes(9);
    expect(transport.snapshot()).toMatchObject({
      providerAttempts: 5,
      maintenanceAttempts: 4,
      failure: null,
    });
    expect(() => transport.assertWithinBudget()).not.toThrow();
    await expect(
      transport.maintenanceFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'budget-exhausted' });
    await expect(
      transport.providerFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'budget-exhausted' });
    expect(nativeFetch).toHaveBeenCalledTimes(9);
    expect(transport.snapshot().failure).toBe('attempts');
    expect(() => transport.assertWithinBudget()).toThrow('budget-exhausted');
  });

  it('counts actual Cloudflare SDK retries separately', async () => {
    const { transport, nativeFetch } = fixture();
    nativeFetch.mockImplementation(async () =>
      nativeFetch.mock.calls.length < 3
        ? Response.json(
            {
              success: false,
              errors: [{ code: 1000, message: 'fixture retry' }],
            },
            { status: 429, headers: { 'retry-after-ms': '1' } },
          )
        : Response.json({
            success: true,
            errors: [],
            result: { subdomain: 'fixture' },
          }),
    );
    const sdk = new Cloudflare({
      apiToken: 'inert-test-token',
      baseURL: 'https://api.cloudflare.com/client/v4',
      fetch: transport.providerFetch,
      maxRetries: 2,
      logLevel: 'off',
    });
    expect(
      await sdk.workers.subdomains.get({ account_id: 'account' }),
    ).toMatchObject({ subdomain: 'fixture' });
    expect(nativeFetch).toHaveBeenCalledTimes(3);
    expect(transport.snapshot()).toMatchObject({
      providerAttempts: 3,
      maintenanceAttempts: 0,
      failure: null,
    });
  });

  it('does not charge the SDK local FormData probe as HTTP', async () => {
    const { transport, nativeFetch } = fixture();
    await transport.providerFetch('data:,');
    expect(nativeFetch).toHaveBeenCalledTimes(1);
    expect(transport.snapshot()).toMatchObject({
      providerAttempts: 0,
      maintenanceAttempts: 0,
      failure: null,
    });
    await expect(
      transport.providerFetch('file:///fixture'),
    ).rejects.toMatchObject({ code: 'operation-refused' });
    expect(nativeFetch).toHaveBeenCalledTimes(1);
  });

  it('preserves request inputs and rejects implicit redirects', async () => {
    const { transport, nativeFetch } = fixture();
    nativeFetch.mockImplementation(async (input, init) => {
      const normalized = new Request(input, init);
      expect(normalized.method).toBe('POST');
      expect(normalized.headers.get('authorization')).toBe(
        'Bearer fixture-token',
      );
      expect(normalized.redirect).toBe('error');
      expect(await normalized.text()).toBe('fixture-body');
      return new Response('done');
    });
    await transport.providerFetch(
      new Request('https://fixture.test', {
        method: 'POST',
        headers: { authorization: 'Bearer fixture-token' },
        body: 'fixture-body',
        redirect: 'follow',
      }),
    );
    expect(transport.snapshot().providerAttempts).toBe(1);
  });

  it('checks elapsed time before delegation even when no timer has fired', async () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(0);
    try {
      const { transport, nativeFetch } = fixture({ startedAt: 0 });
      now.mockReturnValue(5000);
      await expect(
        transport.providerFetch('https://fixture.test'),
      ).rejects.toMatchObject({ code: 'budget-exhausted' });
      expect(nativeFetch).not.toHaveBeenCalled();
      expect(transport.snapshot().failure).toBe('deadline');
    } finally {
      now.mockRestore();
    }
  });

  it('rechecks time after preparing the native request signal', async () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(0);
    const nativeTimeout = AbortSignal.timeout.bind(AbortSignal);
    const timeout = vi
      .spyOn(AbortSignal, 'timeout')
      .mockImplementation((ms) => {
        now.mockReturnValue(5000);
        return nativeTimeout(ms);
      });
    try {
      const { transport, nativeFetch } = fixture({ startedAt: 0 });
      await expect(
        transport.providerFetch('https://fixture.test'),
      ).rejects.toMatchObject({ code: 'budget-exhausted' });
      expect(nativeFetch).not.toHaveBeenCalled();
    } finally {
      timeout.mockRestore();
      now.mockRestore();
    }
  });

  it('keeps operator ceilings below the real lease TTL', () => {
    const { transport } = fixture({
      runtime: {
        requestTimeoutMs: 2_147_483_647,
        invocationTimeoutMs: 2_147_483_647,
        maxProviderRequests: 9,
      },
    });
    expect(transport.effectiveRequestTimeoutMs).toBe(
      DIRECT_REFERENCE_LEASE.leaseTtlMs - 1,
    );
    expect(DIRECT_REFERENCE_LEASE.leaseRenewalIntervalMs).toBeLessThan(
      DIRECT_REFERENCE_LEASE.leaseTtlMs,
    );
    expect(
      fixture({ runtime: { ...runtime, invocationTimeoutMs: 30 } }).transport
        .effectiveRequestTimeoutMs,
    ).toBe(30);
  });

  it('retains per-request timeout through response body reads', async () => {
    const { transport, nativeFetch } = fixture({
      runtime: { ...runtime, requestTimeoutMs: 25 },
    });
    nativeFetch.mockImplementation(
      async (_input, init) =>
        new Response(
          new ReadableStream({
            start(controller) {
              init?.signal?.addEventListener(
                'abort',
                () => controller.error(new Error('body-aborted')),
                { once: true },
              );
            },
          }),
        ),
    );
    const response = await transport.maintenanceFetch('https://fixture.test');
    await expect(response.text()).rejects.toThrow('body-aborted');
    expect(transport.snapshot()).toMatchObject({
      maintenanceAttempts: 1,
      failure: null,
    });
  });

  it('propagates the original request signal', async () => {
    const { transport, nativeFetch } = fixture();
    const controller = new AbortController();
    nativeFetch.mockImplementation(
      async (_input, init) =>
        new Response(
          new ReadableStream({
            start(body) {
              init?.signal?.addEventListener(
                'abort',
                () => body.error(new Error('original-abort')),
                { once: true },
              );
            },
          }),
        ),
    );
    const response = await transport.providerFetch('https://fixture.test', {
      signal: controller.signal,
    });
    controller.abort();
    await expect(response.text()).rejects.toThrow('original-abort');
    expect(transport.snapshot().failure).toBeNull();
  });

  it('aborts active fetches and refuses more on invocation cancellation', async () => {
    const { transport, nativeFetch, abort } = fixture();
    nativeFetch.mockImplementation(
      async (_input, init) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener(
            'abort',
            () => reject(new Error('invocation-aborted')),
            { once: true },
          );
        }),
    );
    const pending = transport.providerFetch('https://fixture.test');
    abort.abort();
    await expect(pending).rejects.toThrow('invocation-aborted');
    await expect(
      transport.maintenanceFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'budget-exhausted' });
    expect(nativeFetch).toHaveBeenCalledTimes(1);
    expect(transport.snapshot().failure).toBe('aborted');
  });

  it('keeps attempt exhaustion sticky after a caught transport error', async () => {
    let retained = false;
    const nativeFetch = vi.fn<typeof fetch>(
      async () => new Response('fixture'),
    );
    const body = JSON.stringify({
      contractVersion: 1,
      configSha256: 'a'.repeat(64),
      action: { kind: 'control-read' },
    });
    const response = await handleDirectReferenceHttpRequest(
      new Request(`https://fixture.test${DIRECT_REFERENCE_PATH}`, {
        method: 'POST',
        headers: { authorization: 'Bearer test' },
        body,
      }),
      {
        invokeSecret: 'test',
        configSha256: 'a'.repeat(64),
        invocationTimeoutMs: 5000,
        dispatch: async (_action, signal) => {
          const transport = new DirectReferenceTransport({
            runtime,
            startedAt: performance.now(),
            signal,
            fetch: nativeFetch,
          });
          for (let i = 0; i < 10; i++)
            await transport
              .providerFetch('https://fixture.test')
              .catch(() => undefined);
          retained = true;
          transport.assertWithinBudget();
          return { status: 'complete' };
        },
      },
    );
    expect(retained).toBe(true);
    expect(nativeFetch).toHaveBeenCalledTimes(9);
    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({
      ok: false,
      error: { code: 'budget-exhausted' },
    });
  });
});

describe('direct reference transport inside workerd', () => {
  let directory: string;
  let server: TestHarness;
  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'direct-transport-'));
    const main = join(directory, 'worker.ts');
    await writeFile(
      main,
      `import {DirectReferenceTransport} from ${JSON.stringify(fileURLToPath(new URL('../scripts/direct-reference-transport.ts', import.meta.url)))};
    export default {async fetch(request){
      const mode=new URL(request.url).searchParams.get('mode');let calls=0,aborted=false;
      const transport=new DirectReferenceTransport({runtime:{requestTimeoutMs:25,invocationTimeoutMs:5000,maxProviderRequests:9},startedAt:performance.now(),signal:request.signal,fetch:async(_input,init)=>{
        calls++;
        return mode==='body'?new Response(new ReadableStream({start(c){init.signal.addEventListener('abort',()=>{aborted=true;c.error(new Error('fixture-abort'));},{once:true});}})):new Response('fixture');
      }});
      if(mode==='body'){const response=await transport.maintenanceFetch('https://fixture.test');await response.text().catch(()=>{});}
      else{await transport.providerFetch('data:,');for(let i=0;i<10;i++)await (i%2?transport.maintenanceFetch:transport.providerFetch)('https://fixture.test').catch(()=>{});}
      return Response.json({calls,aborted,metrics:transport.snapshot()});
    }};`,
    );
    server = createTestHarness({
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-transport-harness',
            main,
            compatibility_date: '2026-08-06',
            compatibility_flags: ['nodejs_compat'],
          },
        },
      ],
    });
    await server.listen();
  }, 30_000);
  afterAll(async () => {
    try {
      await server?.close();
    } finally {
      if (directory) await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);
  it('shares actual HTTP counts without counting the data probe', async () => {
    const response = await server.getWorker().fetch('https://fixture.test');
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      calls: 10,
      metrics: {
        providerAttempts: 5,
        maintenanceAttempts: 4,
        failure: 'attempts',
      },
    });
  });
  it('aborts a maintenance body after headers', async () => {
    const response = await server
      .getWorker()
      .fetch('https://fixture.test?mode=body');
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({
      calls: 1,
      aborted: true,
      metrics: { maintenanceAttempts: 1, failure: null },
    });
  });
});
