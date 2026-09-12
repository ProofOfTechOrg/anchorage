// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { mkdtemp, open, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { preflightDirectConformance } from '../scripts/direct-credentialed-conformance-preflight.mjs';
import {
  createDirectInvocationClient,
  DirectInvocationError,
} from '../scripts/direct-credentialed-invocation.mjs';
import {
  type DirectRunJournal,
  openDirectRunState,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  DIRECT_REFERENCE_PATH,
  type DirectReferenceAction,
} from '../scripts/direct-reference-contract.mjs';
import { handleDirectReferenceHttpRequest } from '../scripts/direct-reference-http.js';

const SECRET = 'invocation-secret-sentinel';
const CLAIM = 'opaque-claim-sentinel';
const directories: string[] = [];
const journals = new Set<DirectRunJournal>();
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
    contractVersion: 1,
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

async function expectUnknown(
  f: Awaited<ReturnType<typeof fixture>>,
  fetchRequest: typeof fetch,
  action: DirectReferenceAction = { kind: 'control-read' },
) {
  const fetchMock = vi.fn(fetchRequest);
  const client = createDirectInvocationClient({
    ...f.options,
    fetch: fetchMock,
  });
  const error = await client.invoke(action).catch((error: unknown) => error);
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

describeLinux('Node authenticated direct invocation', () => {
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
      const pending = JSON.parse(await disk(f.journal));
      expect(pending).toMatchObject({
        invocationCount: 1,
        lastInvocation: {
          state: 'pending',
          action: { kind: 'migration-continue' },
          requestSha256: hash(init?.body as string),
        },
      });
      const response = await handleDirectReferenceHttpRequest(request, {
        configSha256: f.prepared.configSha256,
        invokeSecret: SECRET,
        invocationTimeoutMs: 1000,
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
          contractVersion: 1,
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
    const body = f.success();
    let response: Response;
    if (kind.includes('503')) {
      const value: Record<string, unknown> = {
        contractVersion: 1,
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
          contractVersion: 1,
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
      if (kind === 'wrong-version') body.contractVersion = 2;
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
      {
        kind:
          kind.includes('503') && kind !== 'wrong-503-action'
            ? 'migration-continue'
            : 'control-read',
      },
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
      f.response({ contractVersion: 1, ok: false, error: { code } }, status),
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
