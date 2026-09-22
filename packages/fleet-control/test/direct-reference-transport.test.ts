// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Cloudflare from 'cloudflare';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import {
  directDeploymentSpec,
  generateDirectDeploymentSecrets,
} from '../scripts/direct-credentialed-spec.js';
import {
  DIRECT_REFERENCE_PATH,
  directReferenceRequestSha256,
} from '../scripts/direct-reference-contract.mjs';
import { handleDirectReferenceHttpRequest } from '../scripts/direct-reference-http.js';
import {
  DIRECT_REFERENCE_LEASE,
  DirectReferenceTransport,
  type DirectReferenceTransportOptions,
} from '../scripts/direct-reference-transport.js';
import { probeDirectTenant } from '../scripts/direct-reference-worker.js';
import { deploymentSpecDigest } from '../src/spec-digest.js';
import type { FleetRecord } from '../src/types.js';
import {
  DIRECT_FIXTURE_PROVIDER,
  directFixtureManifest,
} from './fixtures/direct-credentialed-config.js';

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
  it('shares concurrent provider, maintenance and application attempts', async () => {
    const { transport, nativeFetch } = fixture();
    await Promise.all(
      Array.from({ length: 9 }, (_, i) =>
        (i % 3 === 0
          ? transport.providerFetch
          : i % 3 === 1
            ? transport.maintenanceFetch
            : transport.applicationFetch)('https://fixture.test'),
      ),
    );
    expect(nativeFetch).toHaveBeenCalledTimes(9);
    expect(transport.snapshot()).toMatchObject({
      providerAttempts: 3,
      maintenanceAttempts: 3,
      applicationAttempts: 3,
      failure: null,
    });
    expect(() => transport.assertWithinBudget()).not.toThrow();
    await expect(
      transport.maintenanceFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'budget-exhausted' });
    await expect(
      transport.providerFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'budget-exhausted' });
    await expect(
      transport.applicationFetch('https://fixture.test'),
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
      expect(normalized.redirect).toBe('manual');
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

  it.each([
    301, 302, 303, 307, 308,
  ])('refuses redirect status %s without another delegation', async (status) => {
    const { transport, nativeFetch } = fixture();
    const canceled = vi.fn(() => new Promise<void>(() => {}));
    nativeFetch.mockImplementation(
      async () =>
        new Response(new ReadableStream({ cancel: canceled }), {
          status,
          headers: { location: 'https://unvisited.example.test' },
        }),
    );
    await expect(
      transport.providerFetch('https://fixture.test'),
    ).rejects.toMatchObject({ code: 'operation-refused' });
    expect(nativeFetch).toHaveBeenCalledTimes(1);
    expect(canceled).toHaveBeenCalledTimes(1);
  });

  it('preserves a nonredirect 304 response', async () => {
    const { transport, nativeFetch } = fixture();
    nativeFetch.mockResolvedValue(new Response(null, { status: 304 }));
    expect((await transport.providerFetch('https://fixture.test')).status).toBe(
      304,
    );
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
    const core = {
      contractVersion: 2,
      configSha256: 'a'.repeat(64),
      action: { kind: 'control-read' },
    };
    const body = JSON.stringify({
      ...core,
      reservation: {
        ordinal: 1,
        requestSha256: directReferenceRequestSha256(core),
      },
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
        invocationJournal: async () => ({
          receiveInvocation: async () => {},
          settleReceivedInvocation: async () => {},
          reconcileInvocation: async () => 'cancelled',
        }),
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
        const normalized=new Request(_input,init);
        if(normalized.redirect!=='manual')throw new Error('unexpected redirect mode');
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

describe('fixed tenant probe transport', () => {
  const manifest = directFixtureManifest();
  const secrets = generateDirectDeploymentSecrets();
  const spec = directDeploymentSpec(
    manifest,
    'a',
    'initial',
    secrets,
    DIRECT_FIXTURE_PROVIDER,
  );
  const record: FleetRecord = {
    tenantTag: spec.tenantTag,
    environment: spec.environment,
    backend: 'plain-worker',
    scriptName: spec.scriptName,
    databaseId: 'fixture-database',
    databaseName: spec.databaseName,
    schemaVersion: spec.schemaVersion,
    desiredSpecDigest: deploymentSpecDigest(spec),
    artifactVersion: 'fixture-version',
    durableObjectBindings: [],
    routeHostname: spec.routeHostname,
    phase: 'ready',
    updatedAt: '2026-09-10T00:00:00.000Z',
  };
  function probe(
    response: Response,
    options: Partial<DirectReferenceTransportOptions> = {},
  ) {
    const state = fixture(options);
    state.nativeFetch.mockResolvedValue(response);
    const context: Parameters<typeof probeDirectTenant>[0] = {
      transport: state.transport,
      control: { getDeployment: async () => record },
      roleFor: () => 'a',
      specFor: () => spec,
      secrets: () => secrets,
    };
    return {
      ...state,
      context,
      run: (
        operation: 'health' | 'object-put' | 'object-read' | 'object-delete',
      ) =>
        probeDirectTenant(
          context,
          manifest,
          { kind: 'tenant-probe', role: 'a', operation },
          state.abort.signal,
        ),
    };
  }
  it.each([
    {
      operation: 'health' as const,
      facts: { release: '1', marker: 'initial' },
    },
    { operation: 'health' as const, facts: { release: '2', marker: null } },
    { operation: 'object-read' as const, facts: { present: false } },
    {
      operation: 'object-read' as const,
      facts: { present: true, size: 29, sha256: 'a'.repeat(64) },
    },
  ])('projects bounded $operation facts without exposing its token', async ({
    operation,
    facts,
  }) => {
    const state = probe(Response.json(facts));
    const result = await state.run(operation);
    expect(result).toEqual({ role: 'a', operation, ...facts });
    expect(JSON.stringify(result)).not.toContain(
      secrets.application?.APP_PROBE_TOKEN,
    );
    const input = state.nativeFetch.mock.calls[0]?.[0];
    if (!(input instanceof Request)) throw new Error('missing probe request');
    expect(input.url).toBe(
      `https://${spec.routeHostname}/__direct/${operation === 'health' ? 'health' : 'object'}`,
    );
    expect(input.method).toBe('GET');
    expect(input.headers.get('authorization')).toBe(
      `Bearer ${secrets.application?.APP_PROBE_TOKEN}`,
    );
    expect(state.transport.snapshot()).toMatchObject({
      providerAttempts: 0,
      maintenanceAttempts: 0,
      applicationAttempts: 1,
    });
  });
  it.each([
    'object-put',
    'object-delete',
  ] as const)('uses the fixed %s request and empty acknowledgement', async (operation) => {
    const state = probe(new Response(null, { status: 204 }));
    expect(await state.run(operation)).toEqual({
      role: 'a',
      operation,
      returned: true,
    });
    const input = state.nativeFetch.mock.calls[0]?.[0];
    if (!(input instanceof Request)) throw new Error('missing probe request');
    expect(input.method).toBe(operation === 'object-put' ? 'POST' : 'DELETE');
    expect(input.body).toBeNull();
    const incorrect = probe(Response.json({ returned: true }));
    await expect(incorrect.run(operation)).rejects.toMatchObject({
      code: 'operation-refused',
    });
    const unexpectedBytes = probe(
      new Response(null, {
        status: 204,
        headers: { 'content-length': '1' },
      }),
    );
    await expect(unexpectedBytes.run(operation)).rejects.toMatchObject({
      code: 'operation-refused',
    });
  });
  it.each([
    () => Response.json({ release: 1, marker: 'initial' }),
    () =>
      Response.json({
        release: '1',
        marker: 'initial',
        extra: 'private-sentinel',
      }),
    () => Response.json({ release: '1', marker: 'private-sentinel' }),
    () =>
      new Response('{', { headers: { 'content-type': 'application/json' } }),
    () =>
      new Response(new Uint8Array([255]), {
        headers: { 'content-type': 'application/json' },
      }),
    () =>
      new Response('x'.repeat(1025), {
        headers: { 'content-type': 'application/json' },
      }),
    () => new Response('{}', { headers: { 'content-type': 'text/plain' } }),
    () => Response.json({ secret: 'private-sentinel' }, { status: 503 }),
  ])('refuses malformed or incomplete health responses %#', async (response) => {
    const state = probe(response());
    await expect(state.run('health')).rejects.toMatchObject({
      message: 'operation-refused',
    });
  });
  it.each([
    { present: true },
    { present: true, size: 0, sha256: 'a'.repeat(64) },
    { present: true, size: 29, sha256: 'A'.repeat(64) },
    { present: false, size: 0 },
  ])('refuses malformed object observations %#', async (facts) => {
    const state = probe(Response.json(facts));
    await expect(state.run('object-read')).rejects.toMatchObject({
      code: 'operation-refused',
    });
  });
  it('requires the selected current record and retained token before dispatch', async () => {
    const state = probe(Response.json({ release: '1', marker: 'initial' }));
    state.context.control.getDeployment = async () => undefined;
    await expect(state.run('health')).rejects.toMatchObject({
      code: 'operation-refused',
    });
    state.context.control.getDeployment = async () => record;
    vi.spyOn(state.context, 'secrets').mockReturnValue({
      ...secrets,
      application: {},
    });
    await expect(state.run('health')).rejects.toMatchObject({
      code: 'operation-refused',
    });
    expect(state.nativeFetch).not.toHaveBeenCalled();
  });
  it('keeps timeout cancellation attached until the response body settles', async () => {
    let cancelled = false;
    let release: (() => void) | undefined;
    const cancellation = new Promise<void>((resolve) => {
      release = resolve;
    });
    const body = new ReadableStream<Uint8Array>({
      cancel() {
        cancelled = true;
        return cancellation;
      },
    });
    const state = probe(
      new Response(body, { headers: { 'content-type': 'application/json' } }),
      {
        runtime: { ...runtime, requestTimeoutMs: 15 },
      },
    );
    let settled = false;
    const pending = state.run('health').then(
      (result) => {
        settled = true;
        return { result };
      },
      (error: unknown) => {
        settled = true;
        return { error };
      },
    );
    try {
      await vi.waitFor(() => expect(cancelled).toBe(true));
      expect(settled).toBe(false);
    } finally {
      release?.();
    }
    expect(await pending).toHaveProperty('error');
    expect(state.transport.snapshot().applicationAttempts).toBe(1);
  });
});
