// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
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
// F3 — owner-bound idempotent start, on the AGENT surface.
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
function keyedHarness(options: { now?: () => number } = {}) {
  const sqlite = openSqlite();
  const store = new StartIdempotencyStore(
    sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase,
    options.now ? { now: options.now } : {},
  );
  /** runId -> the thread that started it, i.e. where the run actually lives. */
  const runsByThread = new Map<string, string>();
  const starts: Array<{ threadId: string; runId: string; key?: string }> = [];
  const inFlight = new Set<string>();
  const namespace: ThreadNamespaceLike<string> = {
    idFromName: (name) => name,
    get: (threadId) => ({
      fetch: (async (request: Request | string, init?: ThreadRequestInit) => {
        const url = typeof request === 'string' ? request : request.url;
        if (url.includes('/start-liveness')) {
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
        return Response.json({
          agentId: 'writer',
          threadId,
          resourceId: `acme_resource_${threadId}`,
          runId,
          summary: { runId, status: 'success' },
        });
      }) as ReturnType<ThreadNamespaceLike<string>['get']>['fetch'],
    }),
  };
  return {
    store,
    starts,
    runsByThread,
    inFlight,
    topology: createAgentThreadTopology(namespace, DEPLOYMENT_IDENTITY_SECRET, {
      startIdempotency: store,
      executionFence: 'none',
    }),
  };
}

describe('createAgentThreadTopology — idempotent start (F3)', () => {
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
    await store.claim('key-1', 'acme_run_live');
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
    await store.claim('key-1', 'acme_run_dead');

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
    await store.claim('key-1', 'acme_run_gone');
    await store.settleRun('acme_run_gone');

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

  it('gives the claim back when the thread object reports the fence closed', async () => {
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

    // #then the fence's refusal reached the caller AND the claim went back, so
    // a retry after the operator reopens converges on the same run instead of
    // finding a key poisoned by a drain.
    expect(refusal).toMatchObject({ reason: { code: 'EXECUTION_FENCED' } });
    expect((await store.read('key-1'))?.state).toBe('reserved');
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
