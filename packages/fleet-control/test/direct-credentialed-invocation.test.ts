// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import http from 'node:http';
import https from 'node:https';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { preflightDirectConformance } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  awaitReferenceIngress,
  createDirectInvocationClient,
  DIRECT_INVOCATION_FAILURE_DETAILS,
  DIRECT_RECONCILIATION_MAX_INTERVAL_MS,
  DIRECT_RECONCILIATION_MAX_REQUESTS,
  DirectInvocationError,
  reconcileDirectInvocation,
} from '../scripts/direct-credentialed-invocation.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
  directReferenceRequestSha256,
  serializeDirectReferenceCore,
} from '../scripts/direct-reference-contract.mjs';
import { handleDirectReferenceHttpRequest } from '../scripts/direct-reference-http.js';

const SECRET = 'invocation-secret-sentinel';
const CLAIM = 'opaque-claim-sentinel';
const directories: string[] = [];
const journals = new Set<DirectRunJournal>();
const servers = new Set<http.Server>();
const hash = (value: string) =>
  createHash('sha256').update(value).digest('hex');
const attempts = { provider: 1, maintenance: 2, application: 3 };
const responseHeaders = {
  'Cache-Control': 'no-store',
  'Content-Type': 'application/json',
  'X-Direct-Provider-Attempts': '1',
  'X-Direct-Maintenance-Attempts': '2',
  'X-Direct-Application-Attempts': '3',
};
const invocationJournal = () => ({
  receiveInvocation: vi.fn(async () => {}),
  settleReceivedInvocation: vi.fn(async () => {}),
  reconcileInvocation: vi.fn(async () => 'cancelled' as const),
});

async function fixture(limit = 3, timeoutMs = 1000) {
  const directory = await mkdtemp(join(tmpdir(), 'direct-invocation-'));
  directories.push(directory);
  const config = JSON.parse(
    await readFile(
      new URL(
        '../scripts/direct-credentialed-conformance.example.json',
        import.meta.url,
      ),
      'utf8',
    ),
  );
  const reference =
    "import manifest from './direct-run-manifest.js'; export default {fetch(){return Response.json(manifest.contractVersion)}};";
  const tenant =
    'export class Maintenance {} export class Runner {} export default {};';
  config.referenceWorker.artifact = {
    bundle: './reference.mjs',
    mainModule: 'worker.js',
    sha256: hash(reference),
  };
  config.referenceWorker.maxInvocations = limit;
  config.referenceWorker.invocationTimeoutMs = timeoutMs;
  config.deployment.artifact = {
    bundle: './tenant.mjs',
    mainModule: 'worker.js',
    sha256: hash(tenant),
  };
  const configPath = join(directory, 'config.json');
  await writeFile(configPath, JSON.stringify(config));
  await writeFile(join(directory, 'reference.mjs'), reference);
  await writeFile(join(directory, 'tenant.mjs'), tenant);
  const prepared = await preflightDirectConformance({ configPath });
  const input = { configPath, prepared, accountId: 'account' };
  const journal = await opened({ ...input, mode: 'run' });
  const options = {
    prepared,
    journal,
    accountWorkersDevSubdomain: 'attested-account',
    invokeSecret: SECRET,
  };
  const success = (
    action = 'control-read',
    result: unknown = { token: CLAIM },
  ) => ({
    contractVersion: 2,
    configSha256: prepared.configSha256,
    action,
    ok: true,
    result,
  });
  const response = (value: unknown = success(), status = 200) =>
    Response.json(value, { status, headers: responseHeaders });
  return { configPath, prepared, input, journal, options, success, response };
}

async function opened(input: Parameters<typeof openDirectRunState>[0]) {
  const journal = await openDirectRunState(input);
  journals.add(journal);
  return journal;
}

async function closed(journal: DirectRunJournal) {
  await journal.close();
  journals.delete(journal);
}

async function disk(journal: DirectRunJournal) {
  return readFile(join(journal.directory, 'journal.json'), 'utf8');
}

async function localReference(listener: http.RequestListener) {
  const server = http.createServer(listener);
  servers.add(server);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('Local reference address absent');
  return vi.spyOn(https, 'request').mockImplementation((url, options, cb) => {
    const target = new URL(String(url));
    expect(target.protocol).toBe('https:');
    expect(target.pathname).toBe(DIRECT_REFERENCE_PATH);
    return http.request(
      `http://127.0.0.1:${address.port}${target.pathname}`,
      options,
      cb,
    );
  });
}

async function expectUnknown(
  f: Awaited<ReturnType<typeof fixture>>,
  fetchRequest: typeof fetch,
  action: DirectReferenceAction = {
    kind: 'provision',
    role: 'a',
    release: 'initial',
  },
) {
  const fetchMock = vi.fn(fetchRequest);
  const client = createDirectInvocationClient({
    ...f.options,
    fetch: fetchMock,
  });
  const error = await client
    .invoke(action)
    .catch((failure: unknown) => failure);
  expect(error).toBeInstanceOf(DirectInvocationError);
  expect(error).toMatchObject({ code: 'outcome-unknown' });
  expect(String(error)).not.toContain(SECRET);
  expect(error).not.toHaveProperty('cause');
  expect(f.journal.snapshot()).toMatchObject({
    invocationCount: 1,
    lastInvocation: { state: 'pending' },
  });
  await expect(client.invoke(action)).rejects.toMatchObject({
    code: 'outcome-unknown',
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
  await closed(f.journal);
  await expect(
    openDirectRunState({ ...f.input, mode: 'resume' }),
  ).rejects.toMatchObject({ code: 'outcome-unknown' });
}

afterEach(async () => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  await Promise.all(
    [...servers].map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.closeAllConnections();
          server.close((error) => (error ? reject(error) : resolve()));
        }),
    ),
  );
  servers.clear();
  try {
    await Promise.all([...journals].map((journal) => journal.close()));
  } finally {
    journals.clear();
    await Promise.all(
      directories
        .splice(0)
        .map((directory) => rm(directory, { recursive: true, force: true })),
    );
  }
});

const describeLinux =
  process.platform === 'linux' ? describe.sequential : describe.skip;

function ingressRefusal() {
  return Response.json(
    { contractVersion: 2, ok: false, error: { code: 'unauthorized' } },
    {
      status: 401,
      headers: { 'Cache-Control': 'no-store', 'WWW-Authenticate': 'Bearer' },
    },
  );
}

function reconciliationResponse(
  configSha256: string,
  state: 'received' | 'executed',
) {
  return Response.json(
    {
      contractVersion: 2,
      configSha256,
      action: 'reconcile-invocation',
      ok: true,
      result: { state },
    },
    { headers: { 'Cache-Control': 'no-store' } },
  );
}

describeLinux('Node authenticated direct invocation', () => {
  it.each([
    'cancelled',
    'executed',
    'failed',
  ] as const)('returns terminal invocation reconciliation %s without a local reservation', async (state) => {
    const f = await fixture();
    const fetchRequest = vi.fn<typeof fetch>(async (_input, init) => {
      expect(JSON.parse(init?.body as string)).toMatchObject({
        contractVersion: 2,
        configSha256: f.prepared.configSha256,
        action: {
          kind: 'reconcile-invocation',
          ordinal: 1,
          requestSha256: 'a'.repeat(64),
        },
        reservation: null,
      });
      return Response.json(
        {
          contractVersion: 2,
          configSha256: f.prepared.configSha256,
          action: 'reconcile-invocation',
          ok: true,
          result: { state },
        },
        { headers: { 'Cache-Control': 'no-store' } },
      );
    });
    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 1000,
        fetch: fetchRequest,
      }),
    ).resolves.toBe(state);
    expect(f.journal.snapshot().invocationCount).toBe(0);
  });

  it('bounds persistent received reconciliation and backs off to the interval ceiling', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const waits: number[] = [];
    const sleep = vi.fn(async (ms: number) => {
      waits.push(ms);
      await vi.advanceTimersByTimeAsync(ms);
    });
    const fetchRequest = vi.fn<typeof fetch>(async () =>
      reconciliationResponse(f.prepared.configSha256, 'received'),
    );
    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        // This deadline keeps the time bound beyond the capped schedule.
        workerDeadlineMs: 2_147_483_647,
        deadlineMs: 2_147_483_647,
        intervalMs: 1,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('unreachable');
    const expectedWaits = Array.from(
      { length: DIRECT_RECONCILIATION_MAX_REQUESTS - 1 },
      (_, index) => Math.min(2 ** index, DIRECT_RECONCILIATION_MAX_INTERVAL_MS),
    );
    expect(fetchRequest).toHaveBeenCalledTimes(
      DIRECT_RECONCILIATION_MAX_REQUESTS,
    );
    expect(waits).toEqual(expectedWaits);
  });

  it('bounds the production tail by the request cap', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const requestTimes: number[] = [];
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      requestTimes.push(performance.now());
      return reconciliationResponse(f.prepared.configSha256, 'received');
    });
    const sleep = async (ms: number) => {
      await vi.advanceTimersByTimeAsync(ms);
    };

    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 600_000,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('unreachable');
    expect(fetchRequest).toHaveBeenCalledTimes(32);
    expect(requestTimes).toEqual([
      0, 250, 750, 1_750, 3_750, 7_750, 15_750, 31_750, 63_750, 95_750, 127_750,
      159_750, 191_750, 223_750, 255_750, 287_750, 319_750, 351_750, 383_750,
      415_750, 447_750, 479_750, 511_750, 543_750, 575_750, 600_000, 600_250,
      600_500, 600_750, 601_000, 601_250, 601_500,
    ]);
    expect(requestTimes[25]).toBe(600_000);
    expect(requestTimes.at(-1)).toBeLessThan(605_000);
    expect(DIRECT_RECONCILIATION_MAX_REQUESTS).toBe(32);
  });

  it('observes a terminal answer at the Worker deadline', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const requestTimes: number[] = [];
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      requestTimes.push(performance.now());
      return reconciliationResponse(
        f.prepared.configSha256,
        performance.now() === 600_000 ? 'executed' : 'received',
      );
    });
    const sleep = async (ms: number) => {
      await vi.advanceTimersByTimeAsync(ms);
    };

    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 600_000,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('executed');
    expect(fetchRequest).toHaveBeenCalledTimes(26);
    expect(requestTimes.at(-1)).toBe(600_000);
  });

  it('observes a terminal write on the first tail request', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const requestTimes: number[] = [];
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      requestTimes.push(performance.now());
      return reconciliationResponse(
        f.prepared.configSha256,
        performance.now() > 600_000 ? 'executed' : 'received',
      );
    });
    const sleep = async (ms: number) => {
      await vi.advanceTimersByTimeAsync(ms);
    };

    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 600_000,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('executed');
    expect(fetchRequest).toHaveBeenCalledTimes(27);
    expect(requestTimes[25]).toBe(600_000);
    expect(requestTimes[26]).toBe(600_250);
  });

  it('returns a terminal answer after received reconciliation with exponential waits', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const waits: number[] = [];
    const sleep = vi.fn(async (ms: number) => {
      waits.push(ms);
      await vi.advanceTimersByTimeAsync(ms);
    });
    let requestCount = 0;
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      requestCount += 1;
      return reconciliationResponse(
        f.prepared.configSha256,
        requestCount < 3 ? 'received' : 'executed',
      );
    });
    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 1000,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('executed');
    expect(fetchRequest).toHaveBeenCalledTimes(3);
    expect(waits).toEqual([250, 500]);
  });

  it.each([
    [
      'after a wait clamped to it',
      (_sentAt: number): number => 0,
      [
        1_000, 2_000, 4_000, 8_000, 16_000, 29_000, 1_000, 1_000, 1_000, 1_000,
        1_000,
      ],
    ],
    [
      'when a response lands at it',
      (sentAt: number): number => (sentAt === 64_000 ? 1_000 : 0),
      [1_000, 2_000, 4_000, 8_000, 16_000, 29_000, 1_000, 1_000, 1_000, 1_000],
    ],
  ] as const)('ends received reconciliation at the time bound before the request cap %s', async (_name, latencyMs, expectedWaits) => {
    const f = await fixture();
    // Only `performance` is fake: the abort timer runs on the real clock and
    // cannot fire while the poll runs, so the loop's own time checks end it.
    vi.useFakeTimers({ toFake: ['performance'] });
    const requestTimes: number[] = [];
    const waits: number[] = [];
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      const sentAt = performance.now();
      requestTimes.push(sentAt);
      vi.advanceTimersByTime(latencyMs(sentAt));
      return reconciliationResponse(f.prepared.configSha256, 'received');
    });
    const sleep = async (ms: number) => {
      waits.push(ms);
      vi.advanceTimersByTime(ms);
    };
    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 60_000,
        deadlineMs: 65_000,
        intervalMs: 1_000,
        fetch: fetchRequest,
        sleep,
      }),
    ).resolves.toBe('unreachable');
    expect(requestTimes).toEqual([
      0, 1_000, 3_000, 7_000, 15_000, 31_000, 60_000, 61_000, 62_000, 63_000,
      64_000,
    ]);
    expect(requestTimes.length).toBeLessThan(
      DIRECT_RECONCILIATION_MAX_REQUESTS,
    );
    expect(waits).toEqual(expectedWaits);
  });

  it('rejects a reconciliation interval above its ceiling', async () => {
    const f = await fixture();
    const fetchRequest = vi.fn<typeof fetch>();

    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 600_000,
        intervalMs: DIRECT_RECONCILIATION_MAX_INTERVAL_MS + 1,
        fetch: fetchRequest,
      }),
    ).rejects.toMatchObject({ code: 'invalid-input' });
    expect(fetchRequest).not.toHaveBeenCalled();
  });

  it('maps reconciliation transport failure to unreachable', async () => {
    const f = await fixture();
    await expect(
      reconcileDirectInvocation({
        endpoint: `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
        secret: SECRET,
        configSha256: f.prepared.configSha256,
        ordinal: 1,
        requestSha256: 'a'.repeat(64),
        workerDeadlineMs: 1,
        deadlineMs: 100,
        intervalMs: 1,
        fetch: async () => {
          throw new Error(SECRET);
        },
      }),
    ).resolves.toBe('unreachable');
  });

  it.each([
    'an unmarked platform 500 page',
    'a thrown fetch',
    'a fetch timeout',
    'a non-contract 200 media type',
    'a non-contract 200 cache header',
    'a malformed contract body',
    'invalid attempt counts',
  ])('re-sends a read-only action after %s within the delivery window: two requests, one reservation', async (kind) => {
    const f = await fixture(3, 10_000);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    const fetchRequest = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(async () => {
        if (kind === 'a thrown fetch') throw new Error(SECRET);
        if (kind === 'a fetch timeout')
          throw new DOMException(SECRET, 'TimeoutError');
        if (kind === 'an unmarked platform 500 page')
          return new Response('error code: 1104', {
            status: 500,
            headers: { 'content-type': 'text/plain' },
          });
        if (kind === 'a malformed contract body')
          return f.response({ invalid: true });
        const response = f.response();
        if (kind === 'invalid attempt counts')
          response.headers.delete('X-Direct-Provider-Attempts');
        else if (kind === 'a non-contract 200 cache header')
          response.headers.delete('cache-control');
        else response.headers.set('content-type', 'text/html');
        return response;
      })
      .mockImplementation(async () => f.response());
    const result = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
      fetch: fetchRequest,
    }).invoke({ kind: 'control-read' });
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(2_000);
    await expect(result).resolves.toEqual({
      result: { token: CLAIM },
      attempts,
    });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
    expect(fetchRequest.mock.calls[1]).toEqual(fetchRequest.mock.calls[0]);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it.each(
    DIRECT_INVOCATION_FAILURE_DETAILS,
  )('produces every listed outcome-unknown detail through the client: %s', async (detail) => {
    const f = await fixture(3, 3_500);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      switch (detail) {
        case 'platform-page':
          return new Response('error code: 1104', {
            status: 500,
            headers: { 'content-type': 'text/plain' },
          });
        case 'transport-failure':
        case 'delivery-window-expired':
          throw new Error(SECRET);
        case 'non-contract-answer':
          return f.response({ invalid: true });
        default:
          throw new Error(`Missing client failure case: ${detail}`);
      }
    });
    const result = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
      fetch: fetchRequest,
    }).invoke(
      detail === 'delivery-window-expired'
        ? { kind: 'control-read' }
        : { kind: 'provision', role: 'a', release: 'initial' },
    );
    const refused = expect(result).rejects.toMatchObject({
      code: 'outcome-unknown',
      detail,
    });
    if (detail === 'delivery-window-expired') {
      await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
      await vi.advanceTimersByTimeAsync(3_500);
    }
    await refused;
    expect(fetchRequest).toHaveBeenCalledTimes(
      detail === 'delivery-window-expired' ? 2 : 1,
    );
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
  });

  it.each([
    3_500, 600_000,
  ])('bounds read-only re-delivery by the delivery window: ceil(min(120000, %i)/2000) requests, one reservation', async (timeoutMs) => {
    const f = await fixture(3, timeoutMs);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    let startedAt = 0;
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      if (fetchRequest.mock.calls.length === 1) startedAt = performance.now();
      return new Response('error code: 1104', {
        status: 500,
        headers: { 'content-type': 'text/plain' },
      });
    });
    const result = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
      fetch: fetchRequest,
    })
      .invoke({ kind: 'control-read' })
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    const windowMs = Math.min(120_000, timeoutMs);
    await vi.advanceTimersByTimeAsync(
      windowMs - (performance.now() - startedAt),
    );
    await expect(result).resolves.toMatchObject({
      code: 'outcome-unknown',
      detail: 'delivery-window-expired',
    });
    expect(fetchRequest).toHaveBeenCalledTimes(Math.ceil(windowMs / 2_000));
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
    for (const call of fetchRequest.mock.calls)
      expect(call).toEqual(fetchRequest.mock.calls[0]);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
  });

  it('stops re-delivering a read-only action at the delivery window without a final send', async () => {
    const f = await fixture(3, 3_500);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const deliveries: number[] = [];
    let startedAt = 0;
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      if (deliveries.length === 0) startedAt = performance.now();
      deliveries.push(performance.now());
      throw new Error('synthetic transport failure');
    });
    const result = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    })
      .invoke({ kind: 'control-read' })
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(3_499 - (performance.now() - startedAt));
    expect(deliveries.map((time) => time - startedAt)).toEqual([0, 2_000]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await expect(result).resolves.toMatchObject({
      code: 'outcome-unknown',
      detail: 'delivery-window-expired',
    });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    expect(f.journal.snapshot()).toMatchObject({
      invocationCount: 1,
      lastInvocation: { state: 'pending' },
    });
  });

  it.each([
    { kind: 'inventory-read', slot: 'inventory-before' },
    { kind: 'audit-page', slot: 'audit-before', limit: 1 },
    { kind: 'migration-page', limit: 1 },
    { kind: 'cleanup-receipt', role: 'a' },
    { kind: 'decommission-export', role: 'a' },
    { kind: 'tenant-probe', role: 'a', operation: 'health' },
    { kind: 'tenant-probe', role: 'a', operation: 'object-read' },
    { kind: 'tenant-fence', role: 'a', operation: 'read' },
    { kind: 'tenant-fence', role: 'a', operation: 'inventory' },
  ] as const)('re-delivers read-only $kind/$operation: two requests, one reservation', async (action) => {
    const f = await fixture(3, 10_000);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    const fetchRequest = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(new Error(SECRET))
      .mockImplementation(async () => f.response(f.success(action.kind)));
    const result = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
      fetch: fetchRequest,
    }).invoke(action);
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(2_000);
    await expect(result).resolves.toMatchObject({ attempts });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
  });

  it.each([
    { kind: 'force-observe' },
    { kind: 'tenant-probe', role: 'a', operation: 'object-put' },
    { kind: 'tenant-probe', role: 'a', operation: 'object-delete' },
    { kind: 'tenant-fence', role: 'a', operation: 'mutate-current' },
    { kind: 'decommission-continue', role: 'a' },
  ] as const)('keeps journal or provider mutation $kind/$operation unknown: one request, one reservation', async (action) => {
    const f = await fixture();
    await expectUnknown(
      f,
      async () => new Response('error code: 1104', { status: 500 }),
      action,
    );
  });

  it.each(
    DIRECT_INVOCATION_FAILURE_DETAILS,
  )('constructs outcome-unknown with a listed detail and drops an unlisted one: %s', (detail) => {
    expect(
      new DirectInvocationError(
        'outcome-unknown',
        undefined,
        undefined,
        detail,
      ),
    ).toMatchObject({ code: 'outcome-unknown', detail });
    expect(
      new DirectInvocationError(
        'outcome-unknown',
        undefined,
        undefined,
        SECRET as typeof detail,
      ).detail,
    ).toBeUndefined();
  });

  it.each([
    'text/plain; charset=UTF-8',
    'text/html',
  ])('retries a platform 404 %s with one reservation and identical request bytes', async (contentType) => {
    const f = await fixture(3, 10_000);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    const cancel = vi.fn();
    const page = new Response(new ReadableStream({ cancel }), {
      status: 404,
      headers: { 'content-type': contentType },
    });
    const fetchRequest = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(page)
      .mockImplementation(async () => f.response());
    const client = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
      fetch: fetchRequest,
    });
    const result = client.invoke({ kind: 'control-read' });
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
    expect(cancel).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(2_000);
    await expect(result).resolves.toEqual({
      result: { token: CLAIM },
      attempts,
    });
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    expect(fetchRequest.mock.calls[1]).toEqual(fetchRequest.mock.calls[0]);
    const reservedCore = reserveInvocation.mock.calls[0]?.[0];
    const transmitted = JSON.parse(
      fetchRequest.mock.calls[1]?.[1]?.body as string,
    );
    const { reservation, ...transmittedCore } = transmitted;
    expect(serializeDirectReferenceCore(transmittedCore)).toBe(reservedCore);
    expect(reservation).toMatchObject({
      ordinal: 1,
      requestSha256: hash(reservedCore as string),
    });
    expect(f.journal.snapshot()).toMatchObject({
      invocationCount: 1,
      lastInvocation: { state: 'settled' },
    });
  });

  it.each([
    600_000, 3_500,
  ])('leaves one reservation pending when platform 404s exhaust the delivery window within a %i ms invocation', async (timeoutMs) => {
    const f = await fixture(3, timeoutMs);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    let startedAt = 0;
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      if (fetchRequest.mock.calls.length === 1) startedAt = performance.now();
      return new Response('unavailable', {
        status: 404,
        headers: { 'content-type': 'text/plain' },
      });
    });
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    });
    let settled = false;
    const result = client
      .invoke({ kind: 'control-read' })
      .catch((error: unknown) => {
        settled = true;
        return error;
      });
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    const windowMs = Math.min(120_000, timeoutMs);
    await vi.advanceTimersByTimeAsync(
      windowMs - (performance.now() - startedAt) - 1,
    );
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await expect(result).resolves.toMatchObject({ code: 'outcome-unknown' });
    expect(fetchRequest).toHaveBeenCalledTimes(Math.ceil(windowMs / 2_000));
    expect(f.journal.snapshot()).toMatchObject({
      invocationCount: 1,
      lastInvocation: { state: 'pending' },
    });
    await expect(client.invoke({ kind: 'control-read' })).rejects.toMatchObject(
      { code: 'outcome-unknown' },
    );
    await closed(f.journal);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('bounds a stalled re-delivery by the first attempt delivery window', async () => {
    const f = await fixture(3, 600_000);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    let startedAt = 0;
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      if (fetchRequest.mock.calls.length > 1)
        return new Promise<Response>(() => {});
      startedAt = performance.now();
      return new Response('unavailable', {
        status: 404,
        headers: { 'content-type': 'text/html' },
      });
    });
    const result = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    })
      .invoke({ kind: 'control-read' })
      .catch((error: unknown) => error);
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(
      120_000 - (performance.now() - startedAt),
    );
    await expect(result).resolves.toMatchObject({ code: 'outcome-unknown' });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    expect(fetchRequest.mock.calls[1]?.[1]?.signal?.aborted).toBe(true);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
  });

  it.each([
    [404, 'application/json', {}],
    [500, 'application/json', {}],
    [500, 'text/plain', {}],
    [500, 'text/html', {}],
    [200, 'text/plain', {}],
    [302, 'text/html', {}],
    [404, 'text/plain', { 'cache-control': 'no-store' }],
    [404, 'text/html', { 'www-authenticate': 'Bearer' }],
    [404, 'application/problem+json', {}],
  ] as const)('does not re-deliver HTTP %i %s with markers %j', async (status, contentType, markers) => {
    const f = await fixture(3, 5_000);
    await expectUnknown(
      f,
      async () =>
        new Response(
          JSON.stringify({
            contractVersion: 2,
            ok: false,
            error: { code: status === 404 ? 'not-found' : 'operation-refused' },
          }),
          { status, headers: { 'content-type': contentType, ...markers } },
        ),
    );
  });

  it('sends the default transport POST with the invocation headers and serialized body', async () => {
    const f = await fixture();
    let received: unknown;
    const request = await localReference((incoming, outgoing) => {
      let body = '';
      incoming.setEncoding('utf8');
      incoming.on('data', (chunk) => {
        body += chunk;
      });
      incoming.on('end', () => {
        received = {
          method: incoming.method,
          path: incoming.url,
          headers: incoming.headers,
          body,
        };
        outgoing.writeHead(200, responseHeaders);
        outgoing.end(JSON.stringify(f.success()));
      });
    });
    const fetchRequest = vi.fn<typeof fetch>();
    vi.stubGlobal('fetch', fetchRequest);
    const client = createDirectInvocationClient(f.options);
    await expect(client.invoke({ kind: 'control-read' })).resolves.toEqual({
      result: { token: CLAIM },
      attempts,
    });
    expect(received).toMatchObject({
      method: 'POST',
      path: DIRECT_REFERENCE_PATH,
      headers: {
        authorization: `Bearer ${SECRET}`,
        'content-type': 'application/json',
        accept: 'application/json',
        'cache-control': 'no-store',
      },
    });
    const body = JSON.parse((received as { body: string }).body);
    const core = {
      contractVersion: 2,
      configSha256: f.prepared.configSha256,
      action: { kind: 'control-read' },
    };
    expect(body).toEqual({
      ...core,
      reservation: {
        ordinal: 1,
        requestSha256: directReferenceRequestSha256(core),
      },
    });
    expect(request).toHaveBeenCalledTimes(1);
    expect(fetchRequest).not.toHaveBeenCalled();
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it('disables transport idle timeouts and accepts mutation headers after 300 seconds within the invocation deadline', async () => {
    const f = await fixture(3, 600_000);
    let receive!: (response: http.ServerResponse) => void;
    const received = new Promise<http.ServerResponse>((resolve) => {
      receive = resolve;
    });
    const request = await localReference((_incoming, outgoingResponse) => {
      receive(outgoingResponse);
    });
    vi.useFakeTimers();
    const client = createDirectInvocationClient(f.options);
    const invocation = client.invoke({
      kind: 'provision',
      role: 'a',
      release: 'initial',
    });
    const accepted = expect(invocation).resolves.toMatchObject({ attempts });
    const outgoing = await received;
    const options = request.mock.calls[0]?.[1] as https.RequestOptions;
    expect(options).toEqual({
      method: 'POST',
      headers: expect.any(Object),
      signal: expect.any(AbortSignal),
      timeout: 0,
      agent: false,
    });
    await vi.advanceTimersByTimeAsync(300_001);
    expect(options.signal?.aborted).toBe(false);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
    outgoing.writeHead(200, responseHeaders);
    outgoing.end(JSON.stringify(f.success('provision')));
    await accepted;
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it('accepts read-only headers after 300 seconds within the invocation deadline', async () => {
    const f = await fixture(3, 600_000);
    let receive!: (response: http.ServerResponse) => void;
    const received = new Promise<http.ServerResponse>((resolve) => {
      receive = resolve;
    });
    const request = await localReference((_incoming, outgoingResponse) => {
      receive(outgoingResponse);
    });
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const invocation = createDirectInvocationClient(f.options).invoke({
      kind: 'control-read',
    });
    const accepted = expect(invocation).resolves.toMatchObject({ attempts });
    const outgoing = await received;
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(300_001);
    outgoing.writeHead(200, responseHeaders);
    outgoing.end(JSON.stringify(f.success()));
    await accepted;
    expect(request).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it.each([
    ['read-only', { kind: 'control-read' }],
    ['mutation', { kind: 'provision', role: 'a', release: 'initial' }],
  ] as const)('keeps a re-delivered %s answer streaming past the delivery window', async (_kind, action) => {
    const f = await fixture(3, 600_000);
    const reserveInvocation = vi.fn((body: string) =>
      f.journal.reserveInvocation(body),
    );
    const bodies: string[] = [];
    let receive!: (response: http.ServerResponse) => void;
    const received = new Promise<http.ServerResponse>((resolve) => {
      receive = resolve;
    });
    const request = await localReference((incoming, outgoingResponse) => {
      let body = '';
      incoming.setEncoding('utf8');
      incoming.on('data', (chunk) => {
        body += chunk;
      });
      incoming.on('end', () => {
        bodies.push(body);
        if (bodies.length === 1) {
          if (action.kind === 'control-read') outgoingResponse.destroy();
          else {
            outgoingResponse.writeHead(404, { 'content-type': 'text/html' });
            outgoingResponse.end('<html>not ready</html>');
          }
          return;
        }
        outgoingResponse.writeHead(200, responseHeaders);
        outgoingResponse.write('{');
        receive(outgoingResponse);
      });
    });
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const invocation = createDirectInvocationClient({
      ...f.options,
      journal: { ...f.journal, reserveInvocation },
    }).invoke(action);
    const accepted = expect(invocation).resolves.toMatchObject({ attempts });
    await vi.waitFor(() => {
      expect(request).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(3);
    });
    await vi.advanceTimersByTimeAsync(2_000);
    const outgoing = await received;
    await vi.waitFor(() => expect(vi.getTimerCount()).toBe(1));
    await vi.advanceTimersByTimeAsync(120_001);
    outgoing.end(JSON.stringify(f.success(action.kind)).slice(1));
    await accepted;
    expect(request).toHaveBeenCalledTimes(2);
    expect(reserveInvocation).toHaveBeenCalledTimes(1);
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toBe(bodies[0]);
    const transmitted = JSON.parse(bodies[0] as string);
    const { reservation, ...core } = transmitted;
    expect(serializeDirectReferenceCore(core)).toBe(
      reserveInvocation.mock.calls[0]?.[0],
    );
    expect(reservation.requestSha256).toBe(
      hash(reserveInvocation.mock.calls[0]?.[0] as string),
    );
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it('reports the answer class, not the delivery window, when the invocation deadline ends a first read-only attempt', async () => {
    const f = await fixture(3, 10_000);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const fetchRequest = vi.fn<typeof fetch>(
      () => new Promise<Response>(() => {}),
    );
    const invocation = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    }).invoke({ kind: 'control-read' });
    const refused = expect(invocation).rejects.toMatchObject({
      code: 'outcome-unknown',
      detail: 'transport-failure',
    });
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(10_000);
    await refused;
    expect(fetchRequest).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
  });

  it.each([
    'headers',
    'body',
  ])('aborts a hanging default transport %s exchange as outcome-unknown', async (phase) => {
    const f = await fixture(3, 100);
    let disconnect!: () => void;
    const disconnected = new Promise<void>((resolve) => {
      disconnect = resolve;
    });
    const request = await localReference((_incoming, outgoing) => {
      outgoing.on('close', disconnect);
      if (phase === 'body') {
        outgoing.writeHead(200, responseHeaders);
        outgoing.write('{');
      }
    });
    const client = createDirectInvocationClient(f.options);
    await expect(client.invoke({ kind: 'control-read' })).rejects.toMatchObject(
      { code: 'outcome-unknown' },
    );
    await disconnected;
    const options = request.mock.calls[0]?.[1] as https.RequestOptions;
    expect(options.signal?.aborted).toBe(true);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
    await expect(client.invoke({ kind: 'control-read' })).rejects.toMatchObject(
      { code: 'outcome-unknown' },
    );
    expect(request).toHaveBeenCalledTimes(1);
    await closed(f.journal);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it('refuses a default transport redirect without following its location', async () => {
    const f = await fixture(3, 3_500);
    const paths: (string | undefined)[] = [];
    const request = await localReference((incoming, outgoing) => {
      paths.push(incoming.url);
      outgoing.writeHead(302, {
        ...responseHeaders,
        Location: `http://${incoming.headers.host}/redirect-target`,
      });
      outgoing.end();
    });
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const client = createDirectInvocationClient(f.options);
    const result = client
      .invoke({ kind: 'control-read' })
      .catch((error: unknown) => error);
    await vi.waitFor(() => {
      expect(paths).toHaveLength(1);
      expect(vi.getTimerCount()).toBe(2);
    });
    await vi.advanceTimersToNextTimerAsync();
    await vi.waitFor(() => expect(paths).toHaveLength(2));
    await vi.advanceTimersToNextTimerAsync();
    await expect(result).resolves.toMatchObject({
      code: 'outcome-unknown',
      detail: 'delivery-window-expired',
    });
    expect(paths).toEqual([DIRECT_REFERENCE_PATH, DIRECT_REFERENCE_PATH]);
    expect(paths).not.toContain('/redirect-target');
    expect(request).toHaveBeenCalledTimes(2);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
  });

  it('accepts three consecutive exact ingress contract refusals with unauthenticated requests', async () => {
    const f = await fixture();
    const dispatch = vi.fn();
    const fetchRequest = vi.fn<typeof fetch>(async (url, init) => {
      const request = new Request(url, init);
      expect(request.url).toBe(
        `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
      );
      expect(request.method).toBe('POST');
      expect(Object.fromEntries(request.headers)).toEqual({
        accept: 'application/json',
        'cache-control': 'no-store',
        'content-type': 'application/json',
      });
      expect(init?.body).toBe('{}');
      expect(request.redirect).toBe('manual');
      expect(request.cache).toBe('no-store');
      return handleDirectReferenceHttpRequest(request, {
        configSha256: f.prepared.configSha256,
        invokeSecret: SECRET,
        invocationTimeoutMs: 1000,
        invocationJournal: async () => invocationJournal(),
        dispatch,
      });
    });
    await expect(
      awaitReferenceIngress({
        ...f.options,
        fetch: fetchRequest,
        sleep: async () => {},
      }),
    ).resolves.toBe(true);
    expect(fetchRequest).toHaveBeenCalledTimes(3);
    expect(dispatch).not.toHaveBeenCalled();
    expect(f.journal.snapshot()).toMatchObject({
      invocationCount: 0,
      lastInvocation: null,
    });
  });

  it('retries HTML 404, thrown fetch, and non-contract 200 before ingress readiness', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const sleep = vi.fn(async (ms: number) => {
      await vi.advanceTimersByTimeAsync(ms);
    });
    const fetchRequest = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(
        new Response('<html>missing</html>', { status: 404 }),
      )
      .mockRejectedValueOnce(new Error(SECRET))
      .mockResolvedValueOnce(Response.json({}))
      .mockImplementation(async () => ingressRefusal());
    await expect(
      awaitReferenceIngress({
        ...f.options,
        fetch: fetchRequest,
        deadlineMs: 100,
        intervalMs: 10,
        sleep,
      }),
    ).resolves.toBe(true);
    expect(fetchRequest).toHaveBeenCalledTimes(6);
    expect(sleep.mock.calls).toEqual([[10], [10], [10], [10], [10]]);
    for (const [url, init] of fetchRequest.mock.calls)
      expect(new Request(url, init).headers.has('authorization')).toBe(false);
  });

  it('returns not ready at the deadline after non-contract responses', async () => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const sleep = vi.fn(
      (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
    );
    const fetchRequest = vi.fn<typeof fetch>(async () => Response.json({}));
    const result = awaitReferenceIngress({
      ...f.options,
      fetch: fetchRequest,
      deadlineMs: 25,
      intervalMs: 10,
      sleep,
    });
    await vi.advanceTimersByTimeAsync(24);
    expect(fetchRequest).toHaveBeenCalledTimes(3);
    expect(sleep.mock.calls).toEqual([[10], [10]]);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await expect(result).resolves.toBe(false);
    expect(fetchRequest).toHaveBeenCalledTimes(3);
  });

  it.each([
    'cache-control',
    'error-code',
    'content-type',
    'www-authenticate',
    'extra-field',
    'oversize',
  ])('does not accept a deviating ingress refusal: %s', async (kind) => {
    const f = await fixture();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] });
    const response = ingressRefusal();
    if (kind === 'cache-control' || kind === 'www-authenticate')
      response.headers.delete(kind);
    if (kind === 'content-type')
      response.headers.set('content-type', 'text/html');
    if (kind === 'oversize')
      response.headers.set('content-length', String(4 * 1024 * 1024 + 1));
    const altered =
      kind === 'error-code' || kind === 'extra-field'
        ? new Response(
            JSON.stringify({
              contractVersion: 2,
              ok: false,
              error: { code: kind === 'error-code' ? SECRET : 'unauthorized' },
              ...(kind === 'extra-field' ? { extra: true } : {}),
            }),
            { status: 401, headers: response.headers },
          )
        : response;
    const fetchRequest = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(altered)
      .mockImplementation(async () => ingressRefusal());
    await expect(
      awaitReferenceIngress({
        ...f.options,
        fetch: fetchRequest,
        deadlineMs: 40,
        intervalMs: 10,
        sleep: async (ms) => {
          await vi.advanceTimersByTimeAsync(ms);
        },
      }),
    ).resolves.toBe(true);
    expect(fetchRequest).toHaveBeenCalledTimes(4);
  });

  it.each([
    'fetch',
    'body',
  ])('bounds a stalled ingress %s by the deadline', async (kind) => {
    const f = await fixture();
    const response = ingressRefusal();
    const stalled = new Response(new ReadableStream(), {
      status: 401,
      headers: response.headers,
    });
    const fetchRequest = vi.fn<typeof fetch>(async () =>
      kind === 'fetch' ? new Promise<Response>(() => {}) : stalled,
    );
    await expect(
      awaitReferenceIngress({
        ...f.options,
        fetch: fetchRequest,
        deadlineMs: 20,
      }),
    ).resolves.toBe(false);
    expect(fetchRequest).toHaveBeenCalledTimes(1);
    expect(fetchRequest.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
  });

  it.each([
    'operator:token',
    'operator token',
    'operator\ttoken',
  ])('accepts a header-safe credential supported by the receiver: %s', async (invokeSecret) => {
    const f = await fixture();
    const client = createDirectInvocationClient({
      ...f.options,
      invokeSecret,
      fetch: async (url, init) => {
        const response = await handleDirectReferenceHttpRequest(
          new Request(url, init),
          {
            configSha256: f.prepared.configSha256,
            invokeSecret,
            invocationTimeoutMs: 1000,
            invocationJournal: async () => invocationJournal(),
            dispatch: async () => ({ accepted: true }),
          },
        );
        for (const [key, value] of Object.entries(responseHeaders))
          response.headers.set(key, value);
        return response;
      },
    });
    await expect(
      client.invoke({ kind: 'control-read' }),
    ).resolves.toMatchObject({ result: { accepted: true } });
  });
  it.each([
    1, 11, 12, 31, 127,
  ])('rejects HTTP control byte %i before reservation', async (code) => {
    const f = await fixture();
    const fetchRequest = vi.fn<typeof fetch>();
    const reserveInvocation = vi.fn(f.journal.reserveInvocation);
    expect(() =>
      createDirectInvocationClient({
        ...f.options,
        journal: { ...f.journal, reserveInvocation },
        invokeSecret: `operator${String.fromCharCode(code)}token`,
        fetch: fetchRequest,
      }),
    ).toThrow('invalid-input');
    expect(reserveInvocation).not.toHaveBeenCalled();
    expect(fetchRequest).not.toHaveBeenCalled();
    expect(f.journal.snapshot().invocationCount).toBe(0);
  });

  it('reserves the exact once-serialized body before sending to attested ingress and settles the real HTTP envelope', async () => {
    const f = await fixture();
    const toJSON = vi.fn(() => ({
      kind: 'migration-continue',
      token: { private: CLAIM },
    }));
    const result = { status: 'pending', token: { private: CLAIM } };
    const fetchRequest = vi.fn<typeof fetch>(async (url, init) => {
      expect(url).toBe(
        `https://${f.prepared.names.referenceWorker}.attested-account.workers.dev${DIRECT_REFERENCE_PATH}`,
      );
      expect(init).toMatchObject({
        method: 'POST',
        cache: 'no-store',
        redirect: 'manual',
      });
      const request = new Request(url, init);
      expect(request.headers.get('authorization')).toBe(`Bearer ${SECRET}`);
      expect(request.headers.get('content-type')).toBe('application/json');
      expect(request.headers.get('cache-control')).toBe('no-store');
      expect(request.headers.get('accept')).toBe('application/json');
      expect(typeof init?.body).toBe('string');
      const transmitted = JSON.parse(init?.body as string);
      const pending = JSON.parse(await disk(f.journal));
      expect(pending).toMatchObject({
        invocationCount: 1,
        lastInvocation: {
          state: 'pending',
          action: { kind: 'migration-continue' },
          requestSha256: transmitted.reservation.requestSha256,
        },
      });
      expect(transmitted.reservation.requestSha256).toBe(
        directReferenceRequestSha256({
          contractVersion: transmitted.contractVersion,
          configSha256: transmitted.configSha256,
          action: transmitted.action,
        }),
      );
      const response = await handleDirectReferenceHttpRequest(request, {
        configSha256: f.prepared.configSha256,
        invokeSecret: SECRET,
        invocationTimeoutMs: 1000,
        invocationJournal: async () => invocationJournal(),
        dispatch: async (action) => {
          expect(action).toEqual({
            kind: 'migration-continue',
            token: { private: CLAIM },
          });
          return result;
        },
      });
      for (const [key, value] of Object.entries(responseHeaders))
        response.headers.set(key, value);
      return response;
    });
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    });
    await expect(
      client.invoke({ toJSON } as unknown as DirectReferenceAction),
    ).resolves.toEqual({ result, attempts });
    expect(toJSON).toHaveBeenCalledTimes(1);
    expect(fetchRequest).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
    const bytes = await disk(f.journal);
    expect(bytes).not.toContain(CLAIM);
    expect(bytes).not.toContain(SECRET);
    expect(bytes).not.toContain('"token"');
  });

  it('binds the response to the serialized action despite caller mutation while reserving', async () => {
    const f = await fixture();
    const action = { kind: 'migration-continue' } as { kind: string };
    let reserved!: () => void;
    const ready = new Promise<void>((resolve) => {
      reserved = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const journal: DirectRunJournal = {
      ...f.journal,
      reserveInvocation: async (body) => {
        const reservation = await f.journal.reserveInvocation(body);
        reserved();
        await gate;
        return reservation;
      },
    };
    const fetchRequest = vi.fn<typeof fetch>(async () =>
      f.response(f.success('migration-continue')),
    );
    const client = createDirectInvocationClient({
      ...f.options,
      journal,
      fetch: fetchRequest,
    });
    const invocation = client.invoke(action as DirectReferenceAction);
    await ready;
    action.kind = 'force-recovery';
    release();
    await expect(invocation).resolves.toMatchObject({ attempts });
    expect(
      JSON.parse(fetchRequest.mock.calls[0]?.[1]?.body as string).action.kind,
    ).toBe('migration-continue');
  });

  it('settles controlled migration response loss and resumes the spent budget without storing secrets', async () => {
    const f = await fixture(2);
    const fetchRequest = vi.fn<typeof fetch>(async () =>
      f.response(
        {
          contractVersion: 2,
          ok: false,
          error: { code: 'injected-response-loss' },
        },
        503,
      ),
    );
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    });
    await expect(
      client.invoke({ kind: 'migration-continue', token: CLAIM }),
    ).rejects.toMatchObject({ code: 'injected-response-loss', attempts });
    await closed(f.journal);
    const journal = await opened({ ...f.input, mode: 'resume' });
    expect(journal.snapshot()).toMatchObject({
      invocationCount: 1,
      lastInvocation: { state: 'settled' },
    });
    fetchRequest.mockImplementation(async () => f.response());
    const resumed = createDirectInvocationClient({
      ...f.options,
      journal,
      fetch: fetchRequest,
    });
    await expect(
      resumed.invoke({ kind: 'control-read' }),
    ).resolves.toMatchObject({ attempts });
    await expect(
      resumed.invoke({ kind: 'control-read' }),
    ).rejects.toMatchObject({ code: 'invocation-budget-exhausted' });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
    const bytes = await disk(journal);
    expect(bytes).not.toContain(CLAIM);
    expect(bytes).not.toContain(SECRET);
  });

  it.each([
    'subdomain-url',
    'subdomain-port',
    'subdomain-path',
    'subdomain-newline',
    'subdomain-label',
    'secret-newline',
    'secret-nul',
    'secret-space',
    'secret-empty',
    'secret-unicode',
    'worker-name',
    'config-hash',
    'module-hash',
    'budget',
    'timeout',
  ])('rejects invalid or mismatched construction before reservation: %s', async (kind) => {
    const f = await fixture();
    const fetchRequest = vi.fn<typeof fetch>();
    const options = {
      ...f.options,
      prepared: structuredClone(f.prepared),
      fetch: fetchRequest,
    };
    const changed = options as unknown as {
      accountWorkersDevSubdomain: string;
      invokeSecret: string;
      prepared: {
        names: { referenceWorker: string };
        configSha256: string;
        referenceModuleSetSha256: string;
        config: {
          referenceWorker: {
            maxInvocations: number;
            invocationTimeoutMs: number;
          };
        };
      };
    };
    switch (kind) {
      case 'subdomain-url':
        changed.accountWorkersDevSubdomain = 'https://evil.test';
        break;
      case 'subdomain-port':
        changed.accountWorkersDevSubdomain = 'account:8443';
        break;
      case 'subdomain-path':
        changed.accountWorkersDevSubdomain = 'account/path';
        break;
      case 'subdomain-newline':
        changed.accountWorkersDevSubdomain = 'account\n';
        break;
      case 'subdomain-label':
        changed.accountWorkersDevSubdomain = 'a'.repeat(64);
        break;
      case 'secret-newline':
        changed.invokeSecret = `${SECRET}\nInjected: value`;
        break;
      case 'secret-nul':
        changed.invokeSecret = `${SECRET}\0`;
        break;
      case 'secret-space':
        changed.invokeSecret = `${SECRET} `;
        break;
      case 'secret-empty':
        changed.invokeSecret = '';
        break;
      case 'secret-unicode':
        changed.invokeSecret = 'secret\u0100';
        break;
      case 'worker-name':
        changed.prepared.names.referenceWorker = 'other-worker';
        break;
      case 'config-hash':
        changed.prepared.configSha256 = 'b'.repeat(64);
        break;
      case 'module-hash':
        changed.prepared.referenceModuleSetSha256 = 'b'.repeat(64);
        break;
      case 'budget':
        changed.prepared.config.referenceWorker.maxInvocations++;
        break;
      case 'timeout':
        changed.prepared.config.referenceWorker.invocationTimeoutMs = 0;
        break;
    }
    expect(() => createDirectInvocationClient(options)).toThrow(
      'invalid-input',
    );
    expect(fetchRequest).not.toHaveBeenCalled();
    expect(f.journal.snapshot().invocationCount).toBe(0);
  });

  it.each([
    'invalid-action',
    'serialization',
    'reservation-callback',
    'reservation-fsync',
  ])('does not dispatch after rejected reservation: %s', async (kind) => {
    const f = await fixture();
    const fetchRequest = vi.fn<typeof fetch>();
    let journal = f.journal;
    let action: unknown = { kind: 'control-read' };
    if (kind === 'invalid-action')
      action = { kind: 'force-recovery', arbitrary: SECRET };
    if (kind === 'serialization')
      action = {
        toJSON() {
          throw new Error(SECRET);
        },
      };
    if (kind === 'reservation-callback')
      journal = {
        ...journal,
        async reserveInvocation() {
          throw new Error(SECRET);
        },
      };
    if (kind === 'reservation-fsync') {
      const file = await open(f.configPath);
      const prototype = Object.getPrototypeOf(file);
      await file.close();
      vi.spyOn(prototype, 'sync').mockRejectedValueOnce(new Error(SECRET));
    }
    const client = createDirectInvocationClient({
      ...f.options,
      journal,
      fetch: fetchRequest,
    });
    await expect(
      client.invoke(action as DirectReferenceAction),
    ).rejects.toMatchObject({ code: 'invalid-input' });
    expect(fetchRequest).not.toHaveBeenCalled();
    expect(f.journal.snapshot().invocationCount).toBe(0);
  });

  it.each([
    'generic-503',
    'wrong-503-action',
    'extra-503-field',
    'wrong-503-code',
    '503-missing-count',
    '503-malformed-count',
    'generic-500',
    'unknown-409',
    'redirect',
    'redirected-200',
    'wrong-run',
    'wrong-action',
    'extra-field',
    'missing-result',
    'wrong-version',
    'invalid-json',
    'cache-header',
    'content-type',
    'missing-count',
    'negative-count',
    'noncanonical-count',
    'infinite-count',
    'unsafe-count',
    'count-budget',
    'invalid-utf8',
    'oversize-declared',
    'oversize-actual',
    'partial-body',
    'network-error',
    'hostile-rejection',
  ])('retains pending state and refuses resume for an unaccepted exchange: %s', async (kind) => {
    const f = await fixture();
    const body = f.success('provision');
    let response: Response;
    if (kind.includes('503')) {
      const value: Record<string, unknown> = {
        contractVersion: 2,
        ok: false,
        error: {
          code:
            kind === 'wrong-503-code'
              ? 'operation-refused'
              : 'injected-response-loss',
        },
      };
      if (kind === 'extra-503-field') value.extra = true;
      response = f.response(
        kind === 'generic-503' ? { error: SECRET } : value,
        503,
      );
    } else if (kind === 'generic-500' || kind === 'unknown-409')
      response = f.response(
        {
          contractVersion: 2,
          ok: false,
          error: {
            code: kind === 'generic-500' ? 'operation-refused' : SECRET,
          },
        },
        kind === 'generic-500' ? 500 : 409,
      );
    else if (kind === 'redirect')
      response = new Response(null, {
        status: 302,
        headers: { location: `https://evil.test/${SECRET}` },
      });
    else if (kind === 'invalid-utf8')
      response = new Response(new Uint8Array([0xff]), {
        headers: responseHeaders,
      });
    else if (kind === 'oversize-actual')
      response = new Response(new Uint8Array(4 * 1024 * 1024 + 1), {
        headers: { ...responseHeaders, 'Content-Length': '1' },
      });
    else if (kind === 'invalid-json')
      response = new Response(SECRET, { headers: responseHeaders });
    else if (kind === 'partial-body')
      response = new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(JSON.stringify(body)));
            controller.error(new Error(SECRET));
          },
        }),
        { headers: responseHeaders },
      );
    else {
      if (kind === 'wrong-run') body.configSha256 = 'f'.repeat(64);
      if (kind === 'wrong-action') body.action = 'force-recovery';
      if (kind === 'wrong-version') body.contractVersion = 1;
      if (kind === 'extra-field') Object.assign(body, { secret: SECRET });
      if (kind === 'missing-result') Reflect.deleteProperty(body, 'result');
      response = f.response(body);
    }
    if (kind === 'redirected-200')
      Object.defineProperty(response, 'redirected', { value: true });
    if (kind === 'cache-header') response.headers.delete('Cache-Control');
    if (kind === 'content-type')
      response.headers.set('Content-Type', 'text/plain');
    if (kind === 'missing-count' || kind === '503-missing-count')
      response.headers.delete('X-Direct-Provider-Attempts');
    if (kind === '503-malformed-count')
      response.headers.set('X-Direct-Application-Attempts', '0.0');
    if (kind === 'negative-count')
      response.headers.set('X-Direct-Provider-Attempts', '-1');
    if (kind === 'noncanonical-count')
      response.headers.set('X-Direct-Provider-Attempts', '01');
    if (kind === 'infinite-count')
      response.headers.set('X-Direct-Provider-Attempts', 'Infinity');
    if (kind === 'unsafe-count')
      response.headers.set('X-Direct-Provider-Attempts', '9007199254740992');
    if (kind === 'count-budget')
      response.headers.set(
        'X-Direct-Provider-Attempts',
        String(f.prepared.config.referenceWorker.maxProviderRequests),
      );
    if (kind === 'oversize-declared')
      response.headers.set('Content-Length', String(4 * 1024 * 1024 + 1));
    await expectUnknown(
      f,
      async () => {
        if (kind === 'network-error') throw new Error(SECRET, { cause: CLAIM });
        if (kind === 'hostile-rejection') {
          const { proxy, revoke } = Proxy.revocable({}, {});
          revoke();
          throw proxy;
        }
        return response;
      },
      kind.includes('503') && kind !== 'wrong-503-action'
        ? { kind: 'migration-continue' }
        : { kind: 'provision', role: 'a', release: 'initial' },
    );
  });

  it('accepts a response exactly at the byte cap and preserves zero-attempt replay evidence', async () => {
    const f = await fixture();
    const empty = JSON.stringify(f.success('control-read', ''));
    const response = f.response(
      f.success(
        'control-read',
        'a'.repeat(4 * 1024 * 1024 - Buffer.byteLength(empty)),
      ),
    );
    for (const name of ['Provider', 'Maintenance', 'Application'])
      response.headers.set(`X-Direct-${name}-Attempts`, '0');
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: async () => response,
    });
    const result = await client.invoke({ kind: 'control-read' });
    expect(result.attempts).toEqual({
      provider: 0,
      maintenance: 0,
      application: 0,
    });
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
  });

  it.each([
    ['operation-refused', 409],
    ['wrong-operation', 409],
    ['missing-continuation', 409],
    ['budget-exhausted', 503],
  ] as const)('settles the exact execution refusal %s/%s before permitting another budgeted call', async (code, status) => {
    const f = await fixture(2);
    const fetchRequest = vi.fn<typeof fetch>(async () =>
      f.response({ contractVersion: 2, ok: false, error: { code } }, status),
    );
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    });
    await expect(
      client.invoke({ kind: 'recover-force-residual' }),
    ).rejects.toMatchObject({
      code: 'reference-refused',
      referenceCode: code,
      attempts,
    });
    expect(f.journal.snapshot().lastInvocation?.state).toBe('settled');
    await closed(f.journal);
    const journal = await opened({ ...f.input, mode: 'resume' });
    fetchRequest.mockImplementation(async () => f.response());
    const resumed = createDirectInvocationClient({
      ...f.options,
      journal,
      fetch: fetchRequest,
    });
    await resumed.invoke({ kind: 'control-read' });
    expect(journal.snapshot().invocationCount).toBe(2);
    expect(fetchRequest).toHaveBeenCalledTimes(2);
  });

  it('refuses overlapping calls without another dispatch or reservation', async () => {
    const f = await fixture();
    let release!: (value: Response) => void;
    const deferred = new Promise<Response>((resolve) => {
      release = resolve;
    });
    const fetchRequest = vi.fn<typeof fetch>(async () => deferred);
    const client = createDirectInvocationClient({
      ...f.options,
      fetch: fetchRequest,
    });
    const first = client.invoke({ kind: 'control-read' });
    await expect(
      client.invoke({ kind: 'force-recovery' }),
    ).rejects.toMatchObject({ code: 'invocation-busy' });
    await vi.waitFor(() => expect(fetchRequest).toHaveBeenCalledTimes(1));
    expect(f.journal.snapshot().invocationCount).toBe(1);
    release(f.response());
    await first;
    fetchRequest.mockImplementation(async () => f.response());
    await client.invoke({ kind: 'control-read' });
    expect(fetchRequest).toHaveBeenCalledTimes(2);
  });

  it.each([
    'callback',
    'fsync',
  ])('refuses later dispatch when durable settlement fails: %s', async (kind) => {
    const f = await fixture();
    let journal = f.journal;
    if (kind === 'callback')
      journal = {
        ...journal,
        async settleInvocation() {
          throw new Error(SECRET);
        },
      };
    const fetchRequest = vi.fn<typeof fetch>(async () => {
      if (kind === 'fsync') {
        const file = await open(f.configPath);
        const prototype = Object.getPrototypeOf(file);
        await file.close();
        vi.spyOn(prototype, 'sync').mockRejectedValueOnce(new Error(SECRET));
      }
      return f.response();
    });
    const client = createDirectInvocationClient({
      ...f.options,
      journal,
      fetch: fetchRequest,
    });
    await expect(client.invoke({ kind: 'control-read' })).rejects.toMatchObject(
      { code: 'outcome-unknown' },
    );
    await expect(client.invoke({ kind: 'control-read' })).rejects.toMatchObject(
      { code: 'outcome-unknown' },
    );
    expect(fetchRequest).toHaveBeenCalledTimes(1);
    expect(f.journal.snapshot().lastInvocation?.state).toBe('pending');
    await closed(f.journal);
    await expect(
      openDirectRunState({ ...f.input, mode: 'resume' }),
    ).rejects.toMatchObject({ code: 'outcome-unknown' });
  });

  it.each([
    'headers',
    'incomplete',
    'cancel-pending',
    'cancel-rejects',
    'oversize-cancel-pending',
  ])('bounds abort-insensitive %s without settling or unhandled rejection', async (kind) => {
    const f = await fixture(3, 50);
    let releaseHeaders!: (value: Response) => void;
    const late = new Promise<Response>((resolve) => {
      releaseHeaders = resolve;
    });
    const cancel = vi.fn(() =>
      kind === 'cancel-rejects'
        ? Promise.reject(new Error(SECRET))
        : kind === 'incomplete'
          ? Promise.resolve()
          : new Promise<void>(() => {}),
    );
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(
            kind === 'oversize-cancel-pending'
              ? new Uint8Array(4 * 1024 * 1024 + 1)
              : new TextEncoder().encode(JSON.stringify(f.success())),
          );
        },
        cancel,
      }),
      { headers: responseHeaders },
    );
    const unhandled: unknown[] = [];
    const listener = (error: unknown) => {
      unhandled.push(error);
    };
    process.on('unhandledRejection', listener);
    const began = performance.now();
    try {
      await expectUnknown(f, async () =>
        kind === 'headers' ? late : response,
      );
      expect(performance.now() - began).toBeLessThan(1000);
      if (kind === 'headers') releaseHeaders(response);
      await vi.waitFor(() => expect(cancel).toHaveBeenCalled());
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', listener);
      releaseHeaders(response);
    }
  });
});
