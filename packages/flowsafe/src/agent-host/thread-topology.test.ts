// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import type { ActorContext, ApprovalRecord } from '../approval-api/index.js';
import {
  type StartIdempotencyDatabase,
  StartIdempotencyStore,
} from '../do-runner/index.js';
import type {
  ThreadNamespaceLike,
  ThreadRequestInit,
} from '../host-kit/index.js';
import { RunRouteError } from '../host-kit/run-route-error.js';

import {
  type AgentThreadDispatchTopology,
  createAgentThreadTopology,
} from './thread-topology.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

interface Hit {
  threadId: string;
  url: string;
  init?: ThreadRequestInit;
}

function harness(): {
  topology: AgentThreadDispatchTopology;
  hits: Hit[];
} {
  const hits: Hit[] = [];
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (threadId) => ({
      fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
        const url = typeof request === 'string' ? request : request.url;
        hits.push({ threadId, url, init });
        if (url.endsWith('/start')) {
          const body = JSON.parse(init?.body ?? '{}') as Record<string, string>;
          return Response.json({
            agentId: body.agentId,
            threadId: body.threadId,
            resourceId: body.resourceId,
            runId: body.runId,
            summary: { runId: body.runId, status: 'success' },
          });
        }
        if (url.endsWith('/resume')) {
          const body = JSON.parse(init?.body ?? '{}') as Record<string, string>;
          return Response.json({
            agentId: body.agentId,
            threadId: body.threadId,
            resourceId: body.resourceId,
            runId: body.runId,
            summary: { runId: body.runId, status: 'success' },
          });
        }
        if (url.includes('/stream?')) {
          return new Response('{"offset":1,"event":{}}\n');
        }
        if (url.includes('/disputed_run/terminate?')) {
          return Response.json(
            {
              error: 'run termination is blocked',
              reason: {
                code: 'DISPUTED_SETTLEMENT',
                message:
                  'run termination is blocked while an economic operation is disputed',
              },
            },
            { status: 409 },
          );
        }
        return Response.json({
          agentId: 'writer',
          threadId,
          resourceId: `acme_resource_${threadId}`,
          runId: 'acme_run',
          summary: { runId: 'acme_run', status: 'success' },
        });
      }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
    }),
  };
  return {
    // No database in this harness, so the opt-out is written down rather
    // than defaulted — see AgentThreadTopologyOptions.
    topology: createAgentThreadTopology(namespace, DEPLOYMENT_IDENTITY_SECRET, {
      startIdempotency: 'none',
      executionFence: 'none',
    }),
    hits,
  };
}

function context() {
  let runMints = 0;
  let threadMints = 0;
  const value: ActorContext = {
    actor: { id: 'operator-1', role: 'operator' },
    principal: {
      kind: 'human',
      id: 'operator-1',
      role: 'operator',
    },
    resourceOwner: { kind: 'human', id: 'operator-1' },
    service: () => {
      throw new Error('unused');
    },
    newRunId: () => `acme_run_${++runMints}`,
    newThreadId: () => `acme_thread_${++threadMints}`,
    resourceIdFromKey: (threadId) => `acme_resource_${threadId}`,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  };
  return {
    value,
    runMints: () => runMints,
    threadMints: () => threadMints,
  };
}

describe('createAgentThreadTopology', () => {
  it('validates a standing target through its owning thread DO binding', async () => {
    const { topology, hits } = harness();
    const scoped = context();

    await topology.requireBoundThread(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: 'acme_resource_acme_thread',
    });

    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'acme_thread',
        url: expect.stringContaining(
          '/_flowsafe/agent-host/binding?resourceId=acme_resource_acme_thread&agentId=writer',
        ),
      }),
    ]);
  });

  it('mints each server-owned HTTP start id once and stamps the principal', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
    });
    expect(result.runId).toBe('acme_run_1');
    expect(scoped.runMints()).toBe(1);
    expect(scoped.threadMints()).toBe(1);
    expect(hits[0]?.init?.headers).toMatchObject({
      // The principal is the sole identity channel; the DO projects the actor
      // from it rather than trusting a second header.
      'x-flowsafe-principal':
        '{"kind":"human","id":"operator-1","role":"operator"}',
    });
  });

  it('accepts an already server-minted run for schedules without minting twice', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      scheduleId: 'acme_schedule',
      dispatchId: 'acme_dispatch',
      runId: 'acme_scheduled-run',
      threadId: 'acme_thread_existing',
      resourceId: 'acme_resource_acme_thread_existing',
      requestContext: {
        inherited: 'request',
        overridden: 'request',
        runId: 'forged',
      },
      streamRequestContext: {
        streamed: 'yes',
        overridden: 'stream',
        'breakwater.actor': { id: 'forged' },
      },
    });
    expect(result.runId).toBe('acme_scheduled-run');
    expect(scoped.runMints()).toBe(0);
    expect(scoped.threadMints()).toBe(0);
    const body = JSON.parse(hits[0]?.init?.body ?? '{}') as {
      safeContext: Record<string, unknown>;
    };
    expect(body.safeContext).toEqual({
      inherited: 'request',
      overridden: 'stream',
      streamed: 'yes',
    });
  });

  it('mints an ephemeral thread for unthreaded schedules and rejects a supplied thread', async () => {
    const { topology } = harness();
    const scoped = context();
    await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      scheduleId: 'acme_schedule',
      dispatchId: 'acme_dispatch',
      threaded: false,
    });
    expect(scoped.threadMints()).toBe(1);
    await expect(
      topology.start(scoped.value, {
        agentId: 'writer',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        scheduleId: 'acme_schedule',
        dispatchId: 'acme_dispatch',
        threaded: false,
        threadId: 'acme_existing',
      }),
    ).rejects.toMatchObject({ status: 409 });
  });

  it('routes status, observation, and approval resume through the owning thread', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      runId: 'acme_run',
    });
    await topology.observe(scoped.value, {
      agentId: 'writer',
      threadId: 'acme_thread',
      runId: 'acme_run',
      offset: 4,
    });
    const record = {
      workflowId: 'durable-agentic-loop',
      runId: 'acme_run',
      decidedBy: 'reviewer-1',
      stepPath: ['tool'],
      resumeTarget: {
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: 'acme_resource_acme_thread',
        principal: scoped.value.actor,
      },
    } as ApprovalRecord;
    await topology.resume(scoped.value, record, 'approve');
    expect(hits.map((hit) => hit.threadId)).toEqual([
      'acme_thread',
      'acme_thread',
      'acme_thread',
    ]);
    expect(hits[1]?.url).toContain('offset=4');
  });

  it('routes path-safe ids after context authorization', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'thread-1',
      runId: 'run-1',
    });
    await topology.status(scoped.value, {
      agentId: 'writer',
      threadId: 'thread-2',
      runId: 'run-2',
    });
    expect(hits.map((hit) => hit.threadId)).toEqual(['thread-1', 'thread-2']);
  });

  it('refuses status before addressing a run the context cannot read', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.status(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('refuses observation before addressing a stream the context cannot read', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.observe(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
        offset: 0,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('recovers a dispatched run after its ephemeral thread ownership is released', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async (kind) => kind === 'run';

    await expect(
      topology.dispatchStatus(scoped.value, {
        agentId: 'writer',
        threadId: 'ephemeral-thread',
        runId: 'owned-run',
      }),
    ).resolves.toMatchObject({ summary: { status: 'success' } });
    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'ephemeral-thread',
        url: expect.stringContaining('&dispatch=1'),
      }),
    ]);
  });

  it('refuses dispatch recovery before addressing a run the context does not own', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;

    await expect(
      topology.dispatchStatus(scoped.value, {
        agentId: 'writer',
        threadId: 'foreign-thread',
        runId: 'foreign-run',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(hits).toHaveLength(0);
  });

  it('replays a terminal run after ephemeral ownership is released', async () => {
    const { topology, hits } = harness();
    const scoped = context();
    scoped.value.canAccessResource = async () => false;
    const terminate = topology.terminate;
    if (!terminate) throw new Error('terminate topology is unavailable');

    await expect(
      terminate(
        scoped.value,
        {
          agentId: 'writer',
          threadId: 'ephemeral-thread',
          runId: 'acme_run',
        },
        true,
      ),
    ).resolves.toMatchObject({ summary: { status: 'success' } });
    expect(hits).toEqual([
      expect.objectContaining({
        threadId: 'ephemeral-thread',
        url: expect.stringContaining('&replay=1'),
      }),
    ]);
  });

  it('preserves the structured disputed-settlement reason from the Thread DO', async () => {
    const { topology } = harness();
    const scoped = context();
    const terminate = topology.terminate;
    if (!terminate) throw new Error('terminate topology is unavailable');

    await expect(
      terminate(scoped.value, {
        agentId: 'writer',
        threadId: 'acme_thread',
        runId: 'disputed_run',
      }),
    ).rejects.toMatchObject({
      status: 409,
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
  });
});

// ---------------------------------------------------------------------------
// Owner-bound idempotent start, on the AGENT surface.
//
// The agent surface is where the reservation earns its keep. A workflow run has
// one possible host (idFromName(workflowId:runId)); an agent run lives in a
// thread object, and an unthreaded retry mints a FRESH thread every time — so
// without a recorded address a retry would be asking an empty object about a
// run it never had, and Durable Object serialization would not help because the
// two calls are two different objects. Every assertion counts STARTS.
// ---------------------------------------------------------------------------

/**
 * A thread namespace that behaves like a real one: each thread object holds its
 * own runs, answers the liveness probe from its own in-flight set, and answers
 * the dispatch status route only for runs it actually started.
 */
function keyedHarness(
  options: { now?: () => number; liveness?: () => Promise<Response> } = {},
) {
  const sqlite = openSqlite();
  const store = new StartIdempotencyStore(
    sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase,
    options.now ? { now: options.now } : {},
  );
  /** runId -> the thread that started it, i.e. where the run actually lives. */
  const runsByThread = new Map<string, string>();
  const starts: Array<{ threadId: string; runId: string; key?: string }> = [];
  const inFlight = new Set<string>();
  const hits: Hit[] = [];
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (threadId) => ({
      fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
        const url = typeof request === 'string' ? request : request.url;
        hits.push({ threadId, url, init });
        if (url.includes('/start-liveness')) {
          if (options.liveness) return options.liveness();
          const runId = url.split('/runs/')[1]?.split('/')[1] ?? '';
          return Response.json({
            live: runsByThread.get(runId) === threadId && inFlight.has(runId),
          });
        }
        if (url.endsWith('/start')) {
          const body = JSON.parse(init?.body ?? '{}') as Record<string, string>;
          runsByThread.set(body.runId as string, threadId);
          inFlight.add(body.runId as string);
          try {
            await Promise.resolve();
            starts.push({
              threadId,
              runId: body.runId as string,
              ...(body.idempotencyKey === undefined
                ? {}
                : { key: body.idempotencyKey }),
            });
            return Response.json({
              agentId: body.agentId,
              threadId: body.threadId,
              resourceId: body.resourceId,
              runId: body.runId,
              summary: { runId: body.runId, status: 'success' },
            });
          } finally {
            inFlight.delete(body.runId as string);
          }
        }
        // The dispatch status route: only the thread that started the run
        // knows it. Every other object answers 404, which is exactly what a
        // replay that guessed the wrong thread would get — and so does the
        // owning thread while the run is still executing, because the first
        // persisted summary lands only at the first suspend or terminal state.
        const runId = url.split('/runs/')[1]?.split(/[/?]/)[1] ?? '';
        if (runsByThread.get(runId) !== threadId || inFlight.has(runId)) {
          return Response.json({ error: 'run not found' }, { status: 404 });
        }
        const value = {
          agentId: 'writer',
          threadId,
          resourceId: `acme_resource_${threadId}`,
          runId,
          summary: { runId, status: 'success' },
        };
        return Response.json(
          url.includes('replay=1')
            ? {
                kind: 'result',
                value,
                execution: {
                  tablePrefix: '',
                  workflowId: 'durable-agentic-loop',
                  runId,
                  startToken: 'test-generation',
                  owner: { kind: 'human', id: 'operator-1' },
                  target: { kind: 'agent', id: 'writer', threadId },
                },
              }
            : value,
        );
      }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
    }),
  };
  return {
    sqlite,
    store,
    starts,
    runsByThread,
    inFlight,
    hits,
    topology: createAgentThreadTopology(namespace, DEPLOYMENT_IDENTITY_SECRET, {
      startIdempotency: store,
      executionFence: 'none',
    }),
  };
}

function cDeferred() {
  let resolve = () => {};
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('C agent topology capture', () => {
  it.each([
    'persisted',
    'live',
    'unclaimed',
  ] as const)('C topology keeps captured authority and methods on the winning reservation: %s', async (state) => {
    const fixture = keyedHarness();
    const scoped = context();
    Object.assign(scoped.value, { mutationEpoch: 2 });
    const principal = scoped.value.principal;
    const resourceIdFromKey = scoped.value.resourceIdFromKey;
    Object.defineProperty(scoped.value, 'resourceIdFromKey', {
      configurable: true,
      writable: true,
      enumerable: false,
      value: function (this: ActorContext, key: string) {
        expect(this).toBe(scoped.value);
        return resourceIdFromKey(key);
      },
    });
    await fixture.store.reserve({
      key: 'original-key',
      owner: { kind: principal.kind, id: principal.id },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'winner-thread',
      mintRunId: () => 'winner-run',
    });
    if (state !== 'unclaimed') {
      const unclaimedReservation =
        await fixture.store.readForAdmission('original-key');
      if (!unclaimedReservation) throw new Error('missing reservation');
      await fixture.store.claimReservation(unclaimedReservation);
      fixture.runsByThread.set('winner-run', 'winner-thread');
    }
    if (state === 'live') fixture.inFlight.add('winner-run');
    const entered = cDeferred();
    const release = cDeferred();
    const reserve = fixture.store.reserve.bind(fixture.store);
    vi.spyOn(fixture.store, 'reserve').mockImplementationOnce(
      async (...args) => {
        entered.resolve();
        await release.promise;
        return reserve(...args);
      },
    );
    const input: Parameters<AgentThreadDispatchTopology['start']>[1] = {
      agentId: 'writer',
      prompt: 'original',
      entryPath: 'http.start',
      threaded: false,
      topologyThreadId: 'candidate-thread',
      runId: 'candidate-run',
      idempotencyKey: 'original-key',
    };
    const replacement = vi.fn(() => 'replacement-resource');
    const pending = fixture.topology.start(scoped.value, input);
    const outcome = pending.then(
      (value) => ({ value }),
      (error: unknown) => ({ error }),
    );
    try {
      expect(
        await Promise.race([
          entered.promise.then(() => true),
          outcome.then(() => false),
        ]),
      ).toBe(true);
      Object.assign(scoped.value, {
        principal: { kind: 'human', id: 'replacement', role: 'admin' },
        mutationEpoch: 3,
        resourceIdFromKey: replacement,
      });
      Object.assign(input, {
        agentId: 'replacement-agent',
        runId: 'replacement-run',
        topologyThreadId: 'replacement-thread',
        idempotencyKey: 'replacement-key',
      });
    } finally {
      release.resolve();
      await outcome;
    }
    const result = await outcome;
    if (state === 'live')
      expect(result).toMatchObject({
        error: {
          status: 503,
          reason: { code: 'IDEMPOTENT_START_PENDING', runId: 'winner-run' },
        },
      });
    else
      expect(result).toMatchObject({
        value: {
          agentId: 'writer',
          threadId: 'winner-thread',
          runId: 'winner-run',
        },
      });
    expect(fixture.hits.length).toBeGreaterThan(0);
    for (const hit of fixture.hits) {
      expect(hit.threadId).toBe('winner-thread');
      expect(hit.init?.headers).toMatchObject({
        'x-flowsafe-mutation-epoch': '2',
        'x-flowsafe-principal': JSON.stringify(principal),
      });
      expect(hit.url).not.toContain('replacement-agent');
    }
    expect(fixture.starts).toEqual(
      state === 'unclaimed'
        ? [
            {
              threadId: 'winner-thread',
              runId: 'winner-run',
              key: 'original-key',
            },
          ]
        : [],
    );
    expect(scoped.runMints()).toBe(0);
    expect(replacement).not.toHaveBeenCalled();
  });

  it.each([
    ['access', false],
    ['access', true],
    ['F3', false],
    ['F3', true],
  ] as const)('C topology captures absent and supplied run IDs before F3: %s supplied=%s', async (boundary, supplied) => {
    const fixture = keyedHarness();
    const scoped = context();
    Object.assign(scoped.value, { mutationEpoch: 2 });
    const principal = scoped.value.principal;
    const entered = cDeferred();
    const release = cDeferred();
    vi.spyOn(scoped.value, 'canAccessResource').mockImplementationOnce(
      async () => {
        if (boundary === 'access') {
          entered.resolve();
          await release.promise;
        }
        return true;
      },
    );
    const reserve = fixture.store.reserve.bind(fixture.store);
    const reserved = vi
      .spyOn(fixture.store, 'reserve')
      .mockImplementationOnce(async (...args) => {
        if (boundary === 'F3') {
          entered.resolve();
          await release.promise;
        }
        return reserve(...args);
      });
    let runId = supplied ? 'original-run' : undefined;
    const readRunId = vi.fn(() => runId);
    const input: Parameters<AgentThreadDispatchTopology['start']>[1] = {
      agentId: 'writer',
      entryPath: 'schedule.fire',
      scheduleId: 'original-schedule',
      dispatchId: 'original-dispatch',
      threadId: 'acme_original',
      resourceId: 'acme_resource_acme_original',
      topologyThreadId: 'acme_original',
      idempotencyKey: 'original-key',
      threaded: true,
      prompt: 'original',
      get runId() {
        return readRunId();
      },
    };
    const replacementMint = vi.fn(() => 'replacement-run');
    const replacementResource = vi.fn(() => 'replacement-resource');
    const pending = fixture.topology.start(scoped.value, input);
    const outcome = pending.then(
      () => false,
      () => false,
    );
    try {
      expect(
        await Promise.race([entered.promise.then(() => true), outcome]),
      ).toBe(true);
      expect(readRunId).toHaveBeenCalledTimes(1);
      expect(scoped.runMints()).toBe(0);
      runId = 'replacement-run';
      Object.assign(input, {
        agentId: 'replacement-agent',
        entryPath: 'signal.wake',
        scheduleId: 'replacement-schedule',
        dispatchId: 'replacement-dispatch',
        threadId: 'replacement-thread',
        resourceId: 'replacement-resource',
        topologyThreadId: 'replacement-topology',
        idempotencyKey: 'replacement-key',
        threaded: false,
        prompt: 'replacement',
      });
      Object.assign(scoped.value, {
        principal: { kind: 'human', id: 'replacement', role: 'admin' },
        mutationEpoch: 3,
        newRunId: replacementMint,
        resourceIdFromKey: replacementResource,
      });
    } finally {
      release.resolve();
      await outcome;
    }
    const expectedRun = supplied ? 'original-run' : 'acme_run_1';
    expect(await pending).toMatchObject({
      agentId: 'writer',
      runId: expectedRun,
      threadId: 'acme_original',
    });
    expect(fixture.starts).toEqual([
      { threadId: 'acme_original', runId: expectedRun, key: 'original-key' },
    ]);
    expect(reserved.mock.calls[0]?.[0]).toMatchObject({
      owner: { kind: principal.kind, id: principal.id },
      targetId: 'writer',
      threadId: 'acme_original',
      key: 'original-key',
    });
    const sent = fixture.hits.find((hit) => hit.url.endsWith('/start'));
    expect(sent?.init?.headers).toMatchObject({
      'x-flowsafe-mutation-epoch': '2',
      'x-flowsafe-principal': JSON.stringify(principal),
    });
    expect(JSON.parse(sent?.init?.body ?? '{}')).toMatchObject({
      agentId: 'writer',
      entryPath: 'schedule.fire',
      threaded: true,
      runId: expectedRun,
      threadId: 'acme_original',
      resourceId: 'acme_resource_acme_original',
      idempotencyKey: 'original-key',
      scheduleId: 'original-schedule',
      dispatchId: 'original-dispatch',
      prompt: 'original',
    });
    expect(readRunId).toHaveBeenCalledTimes(1);
    expect(scoped.runMints()).toBe(supplied ? 0 : 1);
    expect(replacementMint).not.toHaveBeenCalled();
    expect(replacementResource).not.toHaveBeenCalled();
  });
});

describe('createAgentThreadTopology — idempotent start', () => {
  it('refuses a key when the topology wired no reservation store', async () => {
    // #given the typed opt-out
    const { topology } = harness();

    // #when / #then silently ignoring the key would answer an exactly-once
    // request with at-least-once behaviour.
    await expect(
      topology.start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      }),
    ).rejects.toMatchObject({
      status: 503,
      reason: { code: 'IDEMPOTENT_START_UNSUPPORTED' },
    });
  });

  it('converges an UNTHREADED retry onto the original thread and run', async () => {
    // #given the case DO serialization cannot cover: each call mints its own
    // thread, so the two starts are two different objects
    const { topology, starts } = keyedHarness();
    const scoped = context();
    const input = {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start' as const,
      idempotencyKey: 'key-1',
    };

    // #when
    const first = await topology.start(scoped.value, input);
    const retry = await topology.start(scoped.value, input);

    // #then ONE start, and the retry answers with the original run on its
    // original thread — the recorded address is the only thing that could have
    // taken it back there.
    expect(starts).toHaveLength(1);
    expect(retry.runId).toBe(first.runId);
    expect(retry.threadId).toBe(first.threadId);
  });

  it('routes a re-claimed reservation to the RECORDED thread, not the retry’s fresh one', async () => {
    // #given a reservation an earlier caller left un-claimed on thread A —
    // the crash window in which nothing has executed
    const { topology, store, starts } = keyedHarness();
    const scoped = context();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread_original',
      mintRunId: () => 'acme_run_original',
    });

    // #when a retry arrives and mints a thread of its own
    const result = await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
      idempotencyKey: 'key-1',
    });

    // #then it starts on the RECORDED thread under the RESERVED run id.
    // Starting on the freshly minted thread would put the run somewhere the
    // reservation does not point, and the next retry could never find it.
    expect(starts).toEqual([
      {
        threadId: 'acme_thread_original',
        runId: 'acme_run_original',
        key: 'key-1',
      },
    ]);
    expect(result.runId).toBe('acme_run_original');
  });

  it('carries the key on the internal channel so the fence can match it', async () => {
    // #given
    const { topology, starts } = keyedHarness();

    // #when
    await topology.start(context().value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
      idempotencyKey: 'key-1',
    });

    // #then the key reaches the thread object's start body, which is where
    // RunnerRuntime.start reads it from for the proof-only comparison.
    expect(starts[0]?.key).toBe('key-1');
  });

  it('starts ONE run for two same-key calls issued in parallel', async () => {
    // #given two in-flight first calls on one key, neither having seen the
    // other, each minting its own thread
    const { topology, starts } = keyedHarness();
    const scoped = context();
    const input = {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start' as const,
      idempotencyKey: 'key-1',
    };

    // #when
    const outcomes = await Promise.allSettled([
      topology.start(scoped.value, input),
      topology.start(scoped.value, input),
    ]);

    // #then exactly one start. The loser may replay, or be refused as PENDING,
    // or hit the claim-to-dispatch window and be refused as UNRESOLVABLE —
    // what it must never do is produce a second run.
    expect(starts).toHaveLength(1);
    expect(outcomes).toHaveLength(2);
  });

  it('refuses a key reused for a different agent', async () => {
    // #given
    const { topology, starts } = keyedHarness();
    const scoped = context();
    await topology.start(scoped.value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
      idempotencyKey: 'key-1',
    });

    // #when
    const refusal = await topology
      .start(scoped.value, {
        agentId: 'editor',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      })
      .catch((error: unknown) => error);

    // #then a key that meant one agent cannot come to mean another
    expect(refusal).toMatchObject({
      status: 409,
      reason: {
        code: 'IDEMPOTENT_START_TARGET_MISMATCH',
        targetId: 'writer',
      },
    });
    expect(starts).toHaveLength(1);
  });

  it('refuses a key another principal reserved', async () => {
    // #given a reservation held by someone else
    const { topology, store } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: 'operator-2' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread_other',
      mintRunId: () => 'acme_run_other',
    });

    // #when / #then
    await expect(
      topology.start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      }),
    ).rejects.toMatchObject({
      status: 403,
      reason: { code: 'IDEMPOTENT_START_OWNER_MISMATCH' },
    });
  });

  it('probes the RECORDED thread for liveness and reports PENDING', async () => {
    // #given a claimed reservation whose run is executing on its own thread
    const { topology, store, runsByThread, inFlight } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread_live',
      mintRunId: () => 'acme_run_live',
    });
    const unclaimedReservation = await store.readForAdmission('key-1');
    if (!unclaimedReservation) throw new Error('missing reservation');
    await store.claimReservation(unclaimedReservation);
    runsByThread.set('acme_run_live', 'acme_thread_live');
    inFlight.add('acme_run_live');

    // #when a retry arrives, minting a different thread of its own
    const refusal = await topology
      .start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      })
      .catch((error: unknown) => error);

    // #then 503 PENDING — the probe found the run alive on the thread the
    // RESERVATION recorded, which is not the thread this retry minted.
    expect(refusal).toMatchObject({
      status: 503,
      reason: { code: 'IDEMPOTENT_START_PENDING', runId: 'acme_run_live' },
    });
  });

  it('reports UNRESOLVABLE when the recorded thread is not running the claim', async () => {
    // #given a claim held by a thread object that is executing nothing
    const { topology, store } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread_dead',
      mintRunId: () => 'acme_run_dead',
    });
    const unclaimedReservation = await store.readForAdmission('key-1');
    if (!unclaimedReservation) throw new Error('missing reservation');
    await store.claimReservation(unclaimedReservation);

    // #when / #then never re-executed: whether the agent's first tool call
    // already fired is unknowable from here.
    await expect(
      topology.start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      }),
    ).rejects.toMatchObject({
      status: 409,
      reason: { code: 'IDEMPOTENT_START_UNRESOLVABLE' },
    });
  });

  it('reports ALREADY_SETTLED for a spent key whose run aged out', async () => {
    // #given
    const { topology, store } = keyedHarness();
    await store.reserve({
      key: 'key-1',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread_gone',
      mintRunId: () => 'acme_run_gone',
    });
    const unclaimedReservation = await store.readForAdmission('key-1');
    if (!unclaimedReservation) throw new Error('missing reservation');
    await store.claimReservation(unclaimedReservation);
    const terminalExecution = {
      tablePrefix: '',
      workflowId: 'durable-agentic-loop',
      runId: 'acme_run_gone',
      startToken: 'settled-generation',
      owner: { kind: 'human' as const, id: 'operator-1' },
      target: {
        kind: 'agent' as const,
        id: 'writer',
        threadId: 'acme_thread_gone',
      },
    };
    const startedReservation = await store.readForAdmission('key-1');
    if (!startedReservation) throw new Error('missing started reservation');
    await store.bindPreparedStart(startedReservation, terminalExecution);
    await store.settleExecution(terminalExecution);

    // #when / #then
    await expect(
      topology.start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      }),
    ).rejects.toMatchObject({
      status: 409,
      reason: { code: 'IDEMPOTENT_START_ALREADY_SETTLED' },
    });
  });

  it('retains the claim when the thread object reports the fence closed', async () => {
    // #given a thread object refusing the start with the fence's own code —
    // rebuilt from a DO response, so no longer an ExecutionFencedError instance
    const sqlite = openSqlite();
    const store = new StartIdempotencyStore(
      sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase,
    );
    const namespace: ThreadNamespaceLike<string> = {
      idFromName: (name) => name,
      get: () => ({
        fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
          const url = typeof request === 'string' ? request : request.url;
          if (url.includes('/start-liveness')) {
            return Response.json({ live: false });
          }
          if (url.endsWith('/start')) {
            return Response.json(
              {
                error: "deployment execution is fenced ('migration-locked')",
                reason: {
                  code: 'EXECUTION_FENCED',
                  state: 'migration-locked',
                },
              },
              { status: 503 },
            );
          }
          void init;
          return Response.json({ error: 'run not found' }, { status: 404 });
        }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
      }),
    };
    const topology = createAgentThreadTopology(
      namespace,
      DEPLOYMENT_IDENTITY_SECRET,
      { startIdempotency: store, executionFence: 'none' },
    );

    // #when
    const refusal = await topology
      .start(context().value, {
        agentId: 'writer',
        prompt: 'go',
        entryPath: 'http.start',
        idempotencyKey: 'key-1',
      })
      .catch((error: unknown) => error);

    // A protected HTTP refusal is not a local no-admission witness.
    expect((await store.read('key-1'))?.state).toBe('started');
    expect(refusal).toMatchObject({ reason: { code: 'EXECUTION_FENCED' } });
  });

  it('leaves an unkeyed start byte-identical to before the reservation existed', async () => {
    // #given a wired topology
    const { topology, store, starts } = keyedHarness();

    // #when a start arrives with no key
    await topology.start(context().value, {
      agentId: 'writer',
      prompt: 'go',
      entryPath: 'http.start',
    });

    // #then it started, and reserved nothing: a host that wires the store does
    // not thereby make every start pay for a table it never asked for.
    expect(starts).toHaveLength(1);
    expect(starts[0]?.key).toBeUndefined();
    expect(await store.read('key-1')).toBeUndefined();
  });
});

describe('FS8 D3 agent reclaim liveness transport', () => {
  async function retry(
    liveness: () => Promise<Response>,
    state: 'reserved' | 'started' = 'reserved',
  ) {
    const h = keyedHarness({ now: () => 1_000, liveness });
    const { reservation } = await h.store.reserve({
      key: 'retained-key',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'retained-thread',
      mintRunId: () => 'retained-run',
    });
    if (state === 'started') await h.store.claimReservation(reservation);
    const before = h.sqlite
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    const claim = vi.spyOn(h.store, 'claimReservation');
    const outcome = await h.topology
      .start(context().value, {
        agentId: 'writer',
        prompt: 'retry',
        entryPath: 'http.start',
        idempotencyKey: 'retained-key',
      })
      .catch((error: unknown) => error);
    const after = h.sqlite
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    return { ...h, claim, before, after, outcome };
  }

  it.each([
    true,
    false,
  ])('uses an explicit boolean %s from the recorded host', async (live) => {
    const h = await retry(async () => Response.json({ live }));
    const probe = h.hits.find((hit) => hit.url.endsWith('/start-liveness'));
    expect(probe).toMatchObject({
      threadId: 'retained-thread',
      url: expect.stringContaining('/runs/writer/retained-run/start-liveness'),
    });
    if (live) {
      expect(h.outcome).toMatchObject({
        reason: { code: 'IDEMPOTENT_START_PENDING' },
      });
      expect(h.claim).not.toHaveBeenCalled();
      expect(h.after).toEqual(h.before);
      expect(h.starts).toEqual([]);
    } else {
      expect(h.outcome).toMatchObject({
        runId: 'retained-run',
        threadId: 'retained-thread',
      });
      expect(h.claim).toHaveBeenCalledOnce();
      expect(h.after[0]).toMatchObject({ state: 'started', updated_at: 1_001 });
      expect(h.starts).toEqual([
        {
          threadId: 'retained-thread',
          runId: 'retained-run',
          key: 'retained-key',
        },
      ]);
    }
  });

  const malformed: Array<[string, unknown]> = [
    ['missing', {}],
    ['null', null],
    ['primitive', false],
    ['array', Object.assign([], { live: false })],
    ['inherited', Object.create({ live: false })],
    ['undefined', { live: undefined }],
    ['null field', { live: null }],
    ['zero', { live: 0 }],
    ['empty string', { live: '' }],
    ['false string', { live: 'false' }],
    ['true string', { live: 'true' }],
  ];
  it.each(
    malformed.flatMap(([name, payload]) =>
      (['reserved', 'started'] as const).map((state) => ({
        name,
        payload,
        state,
      })),
    ),
  )('refuses $name liveness for a $state row without claiming', async ({
    payload,
    state,
  }) => {
    const h = await retry(async () => {
      const response = Response.json({});
      vi.spyOn(response, 'json').mockResolvedValue(payload);
      return response;
    }, state);
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.starts).toEqual([]);
  });

  it.each([
    201, 400, 404, 503,
  ])('refuses HTTP %s liveness without claiming', async (status) => {
    const h = await retry(async () =>
      Response.json(
        { live: false, error: 'private transport detail' },
        { status },
      ),
    );
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.starts).toEqual([]);
  });

  it('refuses a liveness accessor without invoking it or claiming', async () => {
    const live = vi.fn(() => false);
    const h = await retry(async () => {
      const response = Response.json({});
      vi.spyOn(response, 'json').mockResolvedValue(
        Object.defineProperty({}, 'live', { get: live }),
      );
      return response;
    });
    expect(live).not.toHaveBeenCalled();
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.starts).toEqual([]);
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
  });

  it('refuses invalid liveness JSON without claiming', async () => {
    const h = await retry(async () => new Response('{'));
    expect(h.outcome).toBeInstanceOf(RunRouteError);
    expect(h.outcome).toMatchObject({
      status: 503,
      message: 'run start liveness is not readable',
    });
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.starts).toEqual([]);
  });

  it('propagates a thrown liveness fetch without claiming', async () => {
    const failure = new Error('liveness fetch failed');
    const h = await retry(async () => {
      throw failure;
    });
    expect(h.outcome).toBe(failure);
    expect(h.claim).not.toHaveBeenCalled();
    expect(h.after).toEqual(h.before);
    expect(h.starts).toEqual([]);
  });
});

describe('FS8 D3 protected replay agent wire', () => {
  const execution = {
    tablePrefix: 'private_',
    workflowId: 'actual-agent-workflow',
    runId: 'wire-run',
    startToken: 'wire-generation',
    owner: { kind: 'human', id: 'operator-1' },
    target: { kind: 'agent', id: 'writer', threadId: 'wire-thread' },
  };
  const value = {
    agentId: 'writer',
    threadId: 'wire-thread',
    resourceId: 'acme_resource_wire-thread',
    runId: 'wire-run',
    summary: {
      runId: 'wire-run',
      status: 'success',
      result: { startToken: 'application-payload' },
    },
  };
  async function replay(payload: unknown, status = 200) {
    const store = new StartIdempotencyStore(
      sqliteUnitDatabase(openSqlite()) as StartIdempotencyDatabase,
    );
    await store.reserve({
      key: 'wire-key',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'wire-thread',
      mintRunId: () => 'wire-run',
    });
    const hits: string[] = [];
    const topology = createAgentThreadTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: (async (request: Request | string) => {
            const url = typeof request === 'string' ? request : request.url;
            hits.push(url);
            if (url.endsWith('/start-liveness'))
              return Response.json({ live: true });
            return Response.json(payload, { status });
          }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
        }),
      },
      DEPLOYMENT_IDENTITY_SECRET,
      { startIdempotency: store, executionFence: 'none' },
    );
    const outcome = await topology
      .start(context().value, {
        agentId: 'writer',
        prompt: 'retry',
        entryPath: 'http.start',
        idempotencyKey: 'wire-key',
        threaded: false,
      })
      .then(
        (result) => ({ result }),
        (error: unknown) => ({ error }),
      );
    return { store, hits, outcome };
  }

  it('keeps the selected agent value while omitting private and unknown structural data', async () => {
    const result = await replay({
      kind: 'result',
      execution,
      value: { ...value, unknown: 'omit' },
      snapshot: 'omit',
    });
    expect(result.outcome).toEqual({ result: value });
    expect(result.hits[0]).toContain('dispatch=1&replay=1');
    expect(
      (await result.store.readForAdmission('wire-key'))?.binding,
    ).toMatchObject({
      kind: 'bound',
      execution: { startToken: 'wire-generation' },
    });
  });

  it('does not associate or replay a valid initial agent identity', async () => {
    const result = await replay({ kind: 'initial', execution });
    expect((await result.store.readForAdmission('wire-key'))?.binding).toEqual({
      kind: 'unbound',
    });
    expect(result.hits).toHaveLength(2);
    expect(result.outcome).toMatchObject({
      error: { status: 503, reason: { code: 'IDEMPOTENT_START_PENDING' } },
    });
  });

  it.each([
    ['discriminator', { execution, value }],
    ['initial value', { kind: 'initial', execution, value }],
    ['missing value', { kind: 'result', execution }],
    [
      'owner',
      {
        kind: 'result',
        execution: { ...execution, owner: { kind: 'human', id: 'other' } },
        value,
      },
    ],
    [
      'agent target',
      {
        kind: 'result',
        execution: {
          ...execution,
          target: { ...execution.target, id: 'other' },
        },
        value,
      },
    ],
    [
      'thread target',
      {
        kind: 'result',
        execution: {
          ...execution,
          target: { ...execution.target, threadId: 'other' },
        },
        value,
      },
    ],
    [
      'physical run',
      { kind: 'result', execution: { ...execution, runId: 'other' }, value },
    ],
    [
      'generation',
      {
        kind: 'result',
        execution: { ...execution, startToken: undefined },
        value,
      },
    ],
    [
      'prefix',
      {
        kind: 'result',
        execution: { ...execution, tablePrefix: 'PRIVATE_' },
        value,
      },
    ],
    [
      'resource',
      { kind: 'result', execution, value: { ...value, resourceId: 'other' } },
    ],
    [
      'summary run',
      {
        kind: 'result',
        execution,
        value: { ...value, summary: { ...value.summary, runId: 'other' } },
      },
    ],
    [
      'pending',
      {
        kind: 'result',
        execution,
        value: { ...value, summary: { ...value.summary, status: 'pending' } },
      },
    ],
    [
      'envelope authority',
      { kind: 'result', execution, value: { ...value, startToken: 'private' } },
    ],
    [
      'summary authority',
      {
        kind: 'result',
        execution,
        value: { ...value, summary: { ...value.summary, requestContext: {} } },
      },
    ],
  ])('refuses malformed agent private data before association: %s', async (_field, payload) => {
    const result = await replay(payload);
    expect((await result.store.readForAdmission('wire-key'))?.binding).toEqual({
      kind: 'unbound',
    });
    expect(result.hits).toHaveLength(1);
    expect(result.outcome).toMatchObject({
      error: { status: 503, message: 'persisted start is not readable' },
    });
  });

  it('projects approval fields and rejects foreign workflow or authority structure', async () => {
    const approval = {
      id: 'approval-1',
      workflowId: execution.workflowId,
      runId: execution.runId,
      title: 'Review',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: '2026-09-07T00:00:00Z',
      updatedAt: '2026-09-07T00:00:00Z',
      payload: { startToken: 'application' },
    };
    const accepted = await replay({
      kind: 'result',
      execution,
      value: {
        ...value,
        approval: { ...approval, extra: 'omit' },
        approvals: [approval],
      },
    });
    expect(accepted.outcome).toEqual({
      result: { ...value, approval, approvals: [approval] },
    });
    for (const replacement of [
      { workflowId: 'foreign-workflow' },
      { startToken: 'private' },
    ]) {
      const refused = await replay({
        kind: 'result',
        execution,
        value: { ...value, approval: { ...approval, ...replacement } },
      });
      expect(
        (await refused.store.readForAdmission('wire-key'))?.binding,
      ).toEqual({ kind: 'unbound' });
      expect(refused.outcome).toMatchObject({
        error: { status: 503, message: 'persisted start is not readable' },
      });
    }
  });
});
