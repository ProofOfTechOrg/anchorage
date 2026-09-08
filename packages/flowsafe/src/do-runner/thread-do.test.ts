// SPDX-License-Identifier: Apache-2.0
import type { DurableObjectState } from '@cloudflare/workers-types';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import {
  deploymentIdentityDatabase,
  deploymentIdentityRequest,
  TEST_DEPLOYMENT_IDENTITY_SECRET,
} from '../../test-support/deployment-identity.js';
import { encodeExecutionPrincipal } from '../approval-api/index.js';
import { MUTATION_EPOCH_HEADER } from './execution-admission.js';
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import { type InitResult, init } from './init.js';
import { RunStateUnreadableError } from './runtime.js';
import { ThreadDurableObject, type ThreadScope } from './thread-do.js';

class TestThread extends ThreadDurableObject {
  builds = 0;
  scopes: ThreadScope[] = [];
  alarmScopes: Array<{
    threadId: string;
    init: InitResult;
    deploymentTag?: string;
  }> = [];
  events?: string[];
  buildError?: Error;
  alarmError?: Error;

  protected build(): InitResult {
    this.builds += 1;
    this.events?.push('build');
    if (this.buildError) throw this.buildError;
    return init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
  }

  protected route(_request: Request, scope: ThreadScope): Promise<Response> {
    this.scopes.push(scope);
    return Promise.resolve(
      Response.json({
        threadId: scope.threadId,
        deploymentTag: scope.deploymentTag,
        principal: scope.principal,
      }),
    );
  }

  protected async onAlarm(
    _env: unknown,
    threadId: string,
    initResult: InitResult,
    deploymentTag?: string,
  ): Promise<void> {
    this.alarmScopes.push({ threadId, init: initResult, deploymentTag });
    this.events?.push('onAlarm');
    if (this.alarmError) throw this.alarmError;
  }
}

function threadWith(
  name: string | undefined,
  options: {
    envTag?: string;
    storedTag?: string;
    omitBindings?: boolean;
    events?: string[];
    buildError?: Error;
    alarmError?: Error;
  } = {},
): TestThread {
  const state = {
    id: { name },
    storage: {
      setAlarm: async () => {
        options.events?.push('setAlarm');
      },
      deleteAlarm: async () => {
        options.events?.push('deleteAlarm');
      },
    },
  } as unknown as DurableObjectState;
  const identity = deploymentIdentityDatabase(options.storedTag ?? 'acme');
  const env = options.omitBindings
    ? {}
    : {
        DEPLOYMENT_TENANT: options.envTag ?? 'acme',
        DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
        DB: {
          prepare(query: string) {
            options.events?.push('identity');
            return identity.prepare(query);
          },
        },
      };
  const thread = new TestThread(state, env);
  thread.events = options.events;
  thread.buildError = options.buildError;
  thread.alarmError = options.alarmError;
  return thread;
}

function request(
  principal = true,
  secret = TEST_DEPLOYMENT_IDENTITY_SECRET,
): Request {
  const headers = new Headers();
  if (principal) {
    headers.set(
      EXECUTION_PRINCIPAL_HEADER,
      encodeExecutionPrincipal({
        kind: 'human',
        id: 'operator',
        role: 'operator',
      }),
    );
  }
  return deploymentIdentityRequest(
    'http://thread/messages',
    {
      method: 'POST',
      headers,
    },
    secret,
  );
}

function cDeferred() {
  let resolve = () => {};
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('FS8 D3 host activation', () => {
  it.each([
    'replacement',
    undefined,
  ])('passes the verified alarm tag after the environment changes to %s', async (replacement) => {
    const entered = cDeferred();
    const release = cDeferred();
    const identity = deploymentIdentityDatabase();
    const setAlarm = vi.fn(async () => {});
    const env = {
      DEPLOYMENT_TENANT: 'acme' as string | undefined,
      DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
      DB: {
        prepare(query: string) {
          const statement = identity.prepare(query);
          return {
            ...statement,
            async all<T>() {
              const result = await statement.all<T>();
              entered.resolve();
              await release.promise;
              return result;
            },
          };
        },
      },
    };
    const thread = new TestThread(
      {
        id: { name: 'thread-alarm' },
        storage: { setAlarm },
      } as unknown as DurableObjectState,
      env,
    );
    const pending = thread.alarm();
    try {
      await entered.promise;
      expect(thread.builds).toBe(0);
      expect(thread.alarmScopes).toEqual([]);
      expect(setAlarm).toHaveBeenCalledOnce();
      env.DEPLOYMENT_TENANT = replacement;
    } finally {
      release.resolve();
      await pending;
    }
    expect(thread.builds).toBe(1);
    expect(thread.alarmScopes).toHaveLength(1);
    expect(thread.alarmScopes[0]).toMatchObject({
      threadId: 'thread-alarm',
      deploymentTag: 'acme',
    });
    expect(thread.alarmScopes[0]?.init.runtime).toBeDefined();
    expect(setAlarm).toHaveBeenCalledOnce();
  });
});

describe('ThreadDurableObject identity boundary', () => {
  it.each([
    undefined,
    0,
    2,
    Number.MAX_SAFE_INTEGER,
  ])('C captures thread headers before deployment verification: %s', async (epoch) => {
    const entered = cDeferred();
    const release = cDeferred();
    const identity = deploymentIdentityDatabase();
    const thread = new TestThread(
      {
        id: { name: 'thread-1' },
        storage: {},
      } as unknown as DurableObjectState,
      {
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET: TEST_DEPLOYMENT_IDENTITY_SECRET,
        DB: {
          prepare(query: string) {
            const statement = identity.prepare(query);
            return {
              ...statement,
              async all<T>() {
                const result = await statement.all<T>();
                entered.resolve();
                await release.promise;
                return result;
              },
            };
          },
        },
      },
    );
    const input = request();
    if (epoch !== undefined)
      input.headers.set(MUTATION_EPOCH_HEADER, String(epoch));
    const pending = thread.fetch(input);
    try {
      await entered.promise;
      expect(thread.builds).toBe(0);
      expect(thread.scopes).toEqual([]);
      input.headers.set(
        EXECUTION_PRINCIPAL_HEADER,
        encodeExecutionPrincipal({
          kind: 'service',
          id: 'replacement',
          purpose: 'replacement start',
        }),
      );
      input.headers.set(MUTATION_EPOCH_HEADER, '3');
    } finally {
      release.resolve();
      await pending;
    }
    expect((await pending).status).toBe(200);
    expect(thread.scopes).toHaveLength(1);
    const scope = thread.scopes[0];
    expect(scope).toMatchObject({
      threadId: 'thread-1',
      deploymentTag: 'acme',
      principal: { kind: 'human', id: 'operator', role: 'operator' },
    });
    expect(scope?.mutationEpoch).toBe(epoch);
    expect(Object.isFrozen(scope)).toBe(true);
    expect(Object.isFrozen(scope?.init)).toBe(false);
    expect(Object.isFrozen(scope?.init.runtime)).toBe(false);
    expect(Object.isFrozen(input)).toBe(false);
  });

  it.each([
    [
      'credential',
      'thread/invalid',
      false,
      'acme',
      'wrong-secret',
      503,
      'credential',
    ],
    [
      'deployment',
      'thread/invalid',
      false,
      'globex',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      503,
      "belongs to 'globex'",
    ],
    [
      'object',
      'thread/invalid',
      false,
      'acme',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      403,
      'path-safe id.name',
    ],
    [
      'missing principal',
      'thread-1',
      false,
      'acme',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      403,
      'no trusted execution principal',
    ],
    [
      'invalid principal',
      'thread-1',
      'malformed',
      'acme',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      403,
      'invalid execution principal',
    ],
    [
      'epoch',
      'thread-1',
      true,
      'acme',
      TEST_DEPLOYMENT_IDENTITY_SECRET,
      400,
      'mutationEpoch must be a nonnegative safe integer or undefined',
    ],
  ] as const)('C thread ingress preserves combined-invalid precedence: %s', async (_label, name, principal, storedTag, secret, status, message) => {
    const events: string[] = [];
    const thread = threadWith(name, { storedTag, events });
    const input = request(principal === true, secret);
    if (principal === 'malformed')
      input.headers.set(EXECUTION_PRINCIPAL_HEADER, 'malformed');
    input.headers.set(MUTATION_EPOCH_HEADER, '01');
    const response = await thread.fetch(input);
    expect(response.status).toBe(status);
    const body = (await response.json()) as { error: string; reason?: unknown };
    expect(body.error).toContain(message);
    if (status === 400)
      expect(body).toEqual({
        error: message,
        reason: { code: 'INVALID_MUTATION_EPOCH' },
      });
    expect(thread.builds).toBe(0);
    expect(thread.scopes).toEqual([]);
    expect(events).not.toContain('setAlarm');
    if (_label === 'credential') expect(events).toEqual([]);
  });

  it('validates its local name before pre-arming alarm storage', async () => {
    const events: string[] = [];
    const thread = threadWith('thread/invalid', { events });

    await expect(thread.alarm()).rejects.toThrow(/path-safe id\.name/);

    expect(events).toEqual([]);
  });

  it('pre-arms before deployment identity failure and preserves the successor', async () => {
    const events: string[] = [];
    const thread = threadWith('thread-1', {
      events,
      storedTag: 'globex',
    });

    await expect(thread.alarm()).rejects.toThrow("belongs to 'globex'");

    expect(events[0]).toBe('setAlarm');
    expect(events.filter((event) => event === 'setAlarm')).toHaveLength(2);
    expect(events).not.toContain('build');
  });

  it.each([
    ['build', { buildError: new Error('build failed') }],
    ['onAlarm', { alarmError: new Error('alarm work failed') }],
  ] as const)('preserves the prearmed successor after a %s failure', async (_label, failure) => {
    const events: string[] = [];
    const thread = threadWith('thread-1', { events, ...failure });

    await expect(thread.alarm()).rejects.toThrow(/failed/);

    expect(events[0]).toBe('setAlarm');
    expect(events.filter((event) => event === 'setAlarm')).toHaveLength(2);
  });

  it('keeps the wake and classifies an alarm whose authoritative read did not succeed', async () => {
    // #given — the owner-recovery duty this alarm drives refuses to conclude
    // anything from a read that did not reach storage, and raises it here.
    const events: string[] = [];
    const thread = threadWith('thread-1', {
      events,
      alarmError: new RunStateUnreadableError(
        'durable-agentic-loop',
        'acme_run',
      ),
    });
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    // #when
    try {
      await expect(thread.alarm()).resolves.toBeUndefined();
    } finally {
      log.mockRestore();
    }

    // #then — never a rethrow: workerd retries a thrown alarm() up to six
    // times, which would answer the storage incident that caused it with a
    // retry storm. The successor is armed and the failure is named once — for
    // this alarm, not for whichever subclass duty raised it.
    expect(events.filter((event) => event === 'setAlarm')).toHaveLength(2);
    expect(logged).toEqual(['thread alarm could not read authoritative state']);
  });

  it('rejects a cross-deployment caller before building named-thread state', async () => {
    const thread = threadWith('thread-1');
    const denied = request(true, 'different-deployment-identity-secret');

    const response = await thread.fetch(denied);

    expect(response.status).toBe(503);
    expect(thread.builds).toBe(0);
  });

  it('serves a path-safe named thread after verifying deployment identity', async () => {
    const thread = threadWith('thread-1');

    const response = await thread.fetch(request());

    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({
      threadId: 'thread-1',
      deploymentTag: 'acme',
      principal: { kind: 'human', id: 'operator', role: 'operator' },
    });
  });

  it('refuses a missing principal before building route wiring', async () => {
    const thread = threadWith('thread-1');

    const response = await thread.fetch(request(false));

    expect(response.status).toBe(403);
    expect(thread.builds).toBe(0);
  });

  it.each([
    undefined,
    '',
    '.',
    '..',
    'thread/1',
  ])('refuses an invalid Durable Object name: %s', async (name) => {
    const thread = threadWith(name);

    const response = await thread.fetch(request());

    expect(response.status).toBe(403);
    expect(thread.builds).toBe(0);
  });

  it('fails closed when the environment tag and D1 sentinel disagree', async () => {
    const thread = threadWith('thread-1', {
      envTag: 'acme',
      storedTag: 'globex',
    });

    const response = await thread.fetch(request());

    expect(response.status).toBe(503);
    expect(thread.builds).toBe(0);
  });

  it('fails closed when production bindings are absent', async () => {
    const thread = threadWith('thread-1', { omitBindings: true });

    const response = await thread.fetch(request());

    expect(response.status).toBe(503);
    expect(thread.builds).toBe(0);
  });

  it('builds its wiring once per Durable Object instance', async () => {
    const thread = threadWith('thread-1');

    await thread.fetch(request());
    await thread.fetch(request());
    await thread.fetch(request());

    expect(thread.builds).toBe(1);
  });
});
