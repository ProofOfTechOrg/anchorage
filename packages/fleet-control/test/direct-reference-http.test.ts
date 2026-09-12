// SPDX-License-Identifier: Apache-2.0

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestHarness, type TestHarness } from 'wrangler';
import { DIRECT_REFERENCE_PATH } from '../scripts/direct-reference-contract.mjs';
import {
  DirectReferenceExecutionError,
  type DirectReferenceHttpOptions,
  handleDirectReferenceHttpRequest,
} from '../scripts/direct-reference-http.js';
import { DirectReferenceJournalError } from '../scripts/direct-reference-journal.js';

const configSha256 = 'a'.repeat(64);
const invokeSecret = 'direct-test-secret';
const endpoint = `https://reference.test${DIRECT_REFERENCE_PATH}`;
const envelope = (action: unknown = { kind: 'control-read' }) =>
  JSON.stringify({ contractVersion: 1, configSha256, action });

function request(body = envelope(), authorization = `Bearer ${invokeSecret}`) {
  return new Request(endpoint, {
    method: 'POST',
    headers: { authorization },
    body,
  });
}

function fixture(overrides: Partial<DirectReferenceHttpOptions> = {}) {
  const dispatch = vi.fn<DirectReferenceHttpOptions['dispatch']>(async () => ({
    status: 'pending',
  }));
  const options = {
    invokeSecret,
    configSha256,
    invocationTimeoutMs: 5_000,
    dispatch,
    ...overrides,
  };
  return {
    dispatch,
    handle: (input: Request) =>
      handleDirectReferenceHttpRequest(input, options),
  };
}

describe('direct reference HTTP boundary', () => {
  it('uses a trusted shared start time through response serialization', async () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(10);
    try {
      const { handle, dispatch } = fixture({
        invocationTimeoutMs: 1000,
        startedAt: 0,
      });
      dispatch.mockResolvedValue({
        toJSON() {
          now.mockReturnValue(1001);
          return { status: 'complete' };
        },
      });
      const response = await handle(request());
      expect(response.status).toBe(504);
      expect(await response.json()).toMatchObject({
        ok: false,
        error: { code: 'invocation-timeout' },
      });
    } finally {
      now.mockRestore();
    }
  });

  it.each([
    'revoked',
    'prototype-trap',
    'code-getter',
    'unknown-code',
  ] as const)('normalizes hostile rejection inspection: %s', async (kind) => {
    let rejection: unknown;
    if (kind === 'revoked') {
      const { proxy, revoke } = Proxy.revocable({}, {});
      revoke();
      rejection = proxy;
    } else if (kind === 'prototype-trap')
      rejection = new Proxy(
        {},
        {
          getPrototypeOf() {
            throw new Error('prototype-secret-sentinel');
          },
        },
      );
    else {
      rejection = new DirectReferenceExecutionError();
      Object.defineProperty(
        rejection,
        'code',
        kind === 'code-getter'
          ? {
              get() {
                throw new Error('code-secret-sentinel');
              },
            }
          : { value: 'unrecognized-secret-sentinel' },
      );
    }
    const { handle, dispatch } = fixture();
    dispatch.mockRejectedValue(rejection);
    const response = await handle(request());
    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({
      contractVersion: 1,
      ok: false,
      error: { code: 'operation-refused' },
    });
    expect(response.headers.get('cache-control')).toBe('no-store');
  });

  it.each([
    '',
    'Basic direct-test-secret',
    'Bearer other',
  ])('refuses unauthorized input before body and dispatch: %s', async (authorization) => {
    const { handle, dispatch } = fixture();
    const input = request('invalid-body-secret-sentinel', authorization);
    const body = vi.spyOn(input, 'body', 'get');
    const response = await handle(input);
    expect(response.status).toBe(401);
    expect(response.headers.get('www-authenticate')).toBe('Bearer');
    expect(response.headers.get('cache-control')).toBe('no-store');
    expect(await response.json()).toEqual({
      contractVersion: 1,
      ok: false,
      error: { code: 'unauthorized' },
    });
    expect(body).not.toHaveBeenCalled();
    expect(dispatch).not.toHaveBeenCalled();
    body.mockRestore();
  });

  it.each([
    undefined,
    '',
    '   ',
  ])('refuses an absent or blank invocation secret: %s', async (secret) => {
    const { handle, dispatch } = fixture({ invokeSecret: secret });
    expect((await handle(request())).status).toBe(401);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it.each([
    ['https://reference.test/other', 'POST', 404],
    [endpoint, 'GET', 405],
  ] as const)('refuses %s %s', async (url, method, status) => {
    const { handle, dispatch } = fixture();
    const response = await handle(new Request(url, { method }));
    expect(response.status).toBe(status);
    expect(response.headers.get('cache-control')).toBe('no-store');
    if (status === 405) expect(response.headers.get('allow')).toBe('POST');
    expect(dispatch).not.toHaveBeenCalled();
  });

  it.each([
    ['malformed JSON', 'invalid-body-secret-sentinel', 400, 'invalid-request'],
    [
      'unknown key',
      envelope({ kind: 'control-read', secret: 'sentinel' }),
      400,
      'invalid-request',
    ],
    [
      'wrong run',
      envelope().replace(configSha256, 'b'.repeat(64)),
      409,
      'run-binding-mismatch',
    ],
    ['size', 'x'.repeat(17_000), 413, 'payload-too-large'],
  ] as const)('refuses invalid control input (%s)', async (_label, body, status, code) => {
    const { handle, dispatch } = fixture();
    const response = await handle(request(body));
    expect(response.status).toBe(status);
    expect(await response.json()).toEqual({
      contractVersion: 1,
      ok: false,
      error: { code },
    });
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('refuses invalid UTF-8 without dispatch', async () => {
    const { handle, dispatch } = fixture();
    const input = new Request(endpoint, {
      method: 'POST',
      headers: { authorization: `Bearer ${invokeSecret}` },
      body: new Uint8Array([0xff]),
    });
    const response = await handle(input);
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({
      error: { code: 'invalid-utf8' },
    });
    expect(dispatch).not.toHaveBeenCalled();
  });

  it.each([
    'blocked',
    'failed',
  ])('retains the authoritative %s result and explicit token', async (status) => {
    const result = {
      status,
      token: { operationId: 'actual-operation', revision: 8 },
      detail: 'fixed-result',
    };
    const { handle, dispatch } = fixture();
    dispatch.mockResolvedValue(result);
    const response = await handle(
      request(envelope({ kind: 'migration-continue', token: null })),
    );
    expect(dispatch).toHaveBeenCalledTimes(1);
    expect(dispatch.mock.calls[0]?.[0]).toEqual({
      kind: 'migration-continue',
      token: null,
    });
    expect(dispatch.mock.calls[0]?.[1]).toBeInstanceOf(AbortSignal);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      contractVersion: 1,
      configSha256,
      action: 'migration-continue',
      ok: true,
      result,
    });
    expect(response.headers.get('cache-control')).toBe('no-store');
  });

  it.each([
    [
      new Error('provider-secret-sentinel', { cause: 'secret-cause' }),
      500,
      'operation-refused',
    ],
    [new DirectReferenceJournalError(), 500, 'journal-state'],
    [
      new DirectReferenceJournalError('operation-mismatch'),
      409,
      'operation-mismatch',
    ],
    [
      new DirectReferenceExecutionError('wrong-operation'),
      409,
      'wrong-operation',
    ],
    [
      new DirectReferenceExecutionError('injected-response-loss'),
      503,
      'injected-response-loss',
    ],
    [
      new DirectReferenceExecutionError('budget-exhausted'),
      503,
      'budget-exhausted',
    ],
  ] as const)('returns a fixed exception envelope: %s', async (error, status, code) => {
    const { handle, dispatch } = fixture();
    dispatch.mockRejectedValue(error);
    const response = await handle(request());
    expect(response.status).toBe(status);
    expect(await response.json()).toEqual({
      contractVersion: 1,
      ok: false,
      error: { code },
    });
    expect(response.headers.get('cache-control')).toBe('no-store');
  });

  it('refuses already-aborted requests before dispatch', async () => {
    const controller = new AbortController();
    controller.abort('secret-reason');
    const { handle, dispatch } = fixture();
    const response = await handle(
      new Request(request(), { signal: controller.signal }),
    );
    expect(response.status).toBe(499);
    expect(await response.json()).toMatchObject({
      error: { code: 'request-aborted' },
    });
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('checks elapsed time when the timeout callback has not run', async () => {
    const elapsed = vi.spyOn(performance, 'now').mockReturnValue(0);
    try {
      const { handle, dispatch } = fixture({ invocationTimeoutMs: 1000 });
      dispatch.mockImplementation(async () => {
        elapsed.mockReturnValue(2000);
        return { status: 'complete' };
      });
      const response = await handle(request());
      expect(response.status).toBe(504);
    } finally {
      elapsed.mockRestore();
    }
  });

  it('cancels the piped source after an early Content-Length refusal', async () => {
    const { handle, dispatch } = fixture();
    const cancel = vi.fn();
    const init = {
      method: 'POST',
      headers: {
        authorization: `Bearer ${invokeSecret}`,
        'content-length': '17000',
      },
      body: new ReadableStream<Uint8Array>({ cancel }),
      duplex: 'half' as const,
    };
    const response = await handle(new Request(endpoint, init));
    expect(response.status).toBe(413);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('cancels a stalled body despite a nonsettling source cancellation', async () => {
    const { handle, dispatch } = fixture({ invocationTimeoutMs: 25 });
    const cancel = vi.fn(() => new Promise<void>(() => {}));
    const init = {
      method: 'POST',
      headers: { authorization: `Bearer ${invokeSecret}` },
      body: new ReadableStream<Uint8Array>({ cancel }),
      duplex: 'half' as const,
    };
    const response = await handle(new Request(endpoint, init));
    expect(response.status).toBe(504);
    expect(await response.json()).toMatchObject({
      error: { code: 'invocation-timeout' },
    });
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('returns the size refusal without waiting for source cancellation', async () => {
    const { handle, dispatch } = fixture({ invocationTimeoutMs: 1000 });
    const init = {
      method: 'POST',
      headers: { authorization: `Bearer ${invokeSecret}` },
      body: new ReadableStream<Uint8Array>({
        start(c) {
          c.enqueue(new Uint8Array(17000));
        },
        cancel() {
          return new Promise<void>(() => {});
        },
      }),
      duplex: 'half' as const,
    };
    const response = await handle(new Request(endpoint, init));
    expect(response.status).toBe(413);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('awaits dispatch settlement after timeout instead of leaving a detached operation', async () => {
    let release!: () => void;
    let aborted!: () => void;
    const abortObserved = new Promise<void>((resolve) => {
      aborted = resolve;
    });
    const cleanup = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { handle, dispatch } = fixture({ invocationTimeoutMs: 25 });
    dispatch.mockImplementation(async (_action, signal) => {
      signal.addEventListener('abort', aborted, { once: true });
      await cleanup;
      return { status: 'complete' };
    });
    let responseSettled = false;
    const result = handle(request()).then((response) => {
      responseSettled = true;
      return response;
    });
    await abortObserved;
    expect(responseSettled).toBe(false);
    release();
    const response = await result;
    expect(response.status).toBe(504);
    expect(await response.json()).toMatchObject({
      error: { code: 'invocation-timeout' },
    });
    expect(dispatch).toHaveBeenCalledTimes(1);
  });
});

describe('direct reference HTTP boundary inside workerd', () => {
  let directory: string;
  let server: TestHarness;
  beforeAll(async () => {
    directory = await mkdtemp(join(tmpdir(), 'direct-http-'));
    const main = join(directory, 'worker.ts');
    await writeFile(
      main,
      `import {handleDirectReferenceHttpRequest} from ${JSON.stringify(fileURLToPath(new URL('../scripts/direct-reference-http.ts', import.meta.url)))};
    export default {async fetch(request){
      const mode=new URL(request.url).searchParams.get('probe');
      let calls=0;
      if(mode==='stalled')request=new Request(${JSON.stringify(endpoint)},{method:'POST',headers:{authorization:'Bearer ${invokeSecret}'},body:new ReadableStream({cancel(){return new Promise(()=>{});}})});
      if(mode==='oversize')request=new Request(${JSON.stringify(endpoint)},{method:'POST',headers:{authorization:'Bearer ${invokeSecret}'},body:new ReadableStream({start(c){c.enqueue(new Uint8Array(17000));},cancel(){return new Promise(()=>{});}})});
      const response=await handleDirectReferenceHttpRequest(request,{invokeSecret:'${invokeSecret}',configSha256:'${configSha256}',invocationTimeoutMs:100,dispatch:async(action)=>{calls++;return {status:'blocked',action};}});
      response.headers.set('x-fixture-dispatches',String(calls));return response;
    }};`,
    );
    server = createTestHarness({
      root: directory,
      workers: [
        {
          config: {
            name: 'direct-http-harness',
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

  it('authenticates and preserves a blocked result', async () => {
    const response = await server.getWorker().fetch(endpoint, {
      method: 'POST',
      headers: { authorization: `Bearer ${invokeSecret}` },
      body: envelope({ kind: 'migration-continue', token: false }),
    });
    expect(response.status).toBe(200);
    expect(response.headers.get('x-fixture-dispatches')).toBe('1');
    expect(await response.json()).toMatchObject({
      ok: true,
      result: {
        status: 'blocked',
        action: { kind: 'migration-continue', token: false },
      },
    });
  });
  it.each([
    ['stalled', 504],
    ['oversize', 413],
  ] as const)('refuses %s without dispatch', async (mode, status) => {
    const response = await server
      .getWorker()
      .fetch(`${endpoint}?probe=${mode}`);
    expect(response.status, await response.text()).toBe(status);
    expect(response.headers.get('x-fixture-dispatches')).toBe('0');
  });
  it('refuses invalid authentication', async () => {
    const response = await server.getWorker().fetch(endpoint, {
      method: 'POST',
      headers: { authorization: 'Bearer other' },
      body: 'secret-body',
    });
    expect(response.status).toBe(401);
    expect(response.headers.get('x-fixture-dispatches')).toBe('0');
  });
});
