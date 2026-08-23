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
import { EXECUTION_PRINCIPAL_HEADER } from './execution-principal-header.js';
import { type InitResult, init } from './init.js';
import { RunStateUnreadableError } from './runtime.js';
import { ThreadDurableObject, type ThreadScope } from './thread-do.js';

class TestThread extends ThreadDurableObject {
  builds = 0;
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
    return Promise.resolve(
      Response.json({
        threadId: scope.threadId,
        deploymentTag: scope.deploymentTag,
        principal: scope.principal,
      }),
    );
  }

  protected async onAlarm(): Promise<void> {
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

describe('ThreadDurableObject identity boundary', () => {
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
