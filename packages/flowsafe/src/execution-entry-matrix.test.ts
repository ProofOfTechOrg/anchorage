// SPDX-License-Identifier: Apache-2.0
// THE EXECUTION-ENTRY MATRIX: every way work can begin or continue on this
// deployment, the fence predicate that polices it, and proof — by driving the
// real surface in all four fence states — that it polices it the way the table
// says.
//
// WHY A MATRIX, AND WHY HERE. The fence is not one check; it is a dozen, spread
// across eight modules that never import each other. Each of those modules
// tests its own gate, and every one of those tests passes on a deployment with
// an entry NOBODY gated: a missing check looks exactly like an absent feature
// until an operator closes the fence and a run starts anyway. What no
// per-module suite can hold is the LIST. This file is that list's
// machine-readable home.
//
// ADDING AN ENTRY. Any new surface that mints a run, resumes one, authors
// standing work, or executes queued work belongs in ENTRIES below, in the same
// change that adds it. Pick its predicate from the four:
//
//   admitsRunStart            a MINT. Refused from `draining` on, because a
//                             drain that keeps minting never ends. In
//                             proof-only, admitted only when the start carries
//                             the nominated idempotency key.
//   admitsExistingRun         work on a run that ALREADY exists — resume,
//                             approval decide, signal delivery. Admitted
//                             through a drain, because finishing these is what
//                             the drain is waiting for. In proof-only, only the
//                             nominated run.
//   admitsWorkAuthoring       standing configuration that ARMS future work — a
//                             schedule created or resumed, an objective set, a
//                             due fire claimed. `open` only; nothing nominates
//                             it in proof-only.
//   admitsDrainableExecution  already-owned queued work — a task body, a
//                             dispatch pass, a webhook ingress. Drains, then
//                             stops; nothing nominates it either.
//
//   Reads — status, inventory, list, observe — and the admin routes are UNGATED
//   in every state by design. A surface that only reads does not belong here.
//
// HOW THE PROOF WORKS. The expectation is never written down per entry. For
// each state the table's declared predicate is EVALUATED — the real exported
// function, on the real fence reading — and the surface is then driven and
// required to agree. Declaring the wrong predicate fails, and so does a gate
// that drifts to a different one, because the two sides of the comparison come
// from different places.
//
// Proof-only is driven TWICE where the entry is nominatable: once carrying the
// nomination the fence names, once not. That second probe is what separates the
// predicate PAIRS — across the other three states admitsRunStart is
// indistinguishable from admitsWorkAuthoring, and admitsExistingRun from
// admitsDrainableExecution.

import { Mastra } from '@mastra/core';
import type { Agent } from '@mastra/core/agent';
import type { NotificationsStorage } from '@mastra/core/notifications';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { openSqlite, sqliteUnitDatabase } from '../test-support/sqlite.js';
import type { ActorContext, ApprovalActor } from './approval-api/index.js';
import {
  ApprovalService,
  InMemoryApprovalStore,
  InMemoryResourceOwnershipStore,
} from './approval-api/index.js';
import { BackgroundTaskHost } from './background-tasks/index.js';
import type {
  DurableObjectRunOwnershipStore,
  ExecutionFenceDatabase,
  ExecutionFenceReading,
  ExecutionFenceState,
  RunnerRuntime,
  StartIdempotencyDatabase,
} from './do-runner/index.js';
import {
  admitsDrainableExecution,
  admitsExistingRun,
  admitsRunStart,
  admitsWorkAuthoring,
  createHostPubSub,
  DEPLOYMENT_IDENTITY_HEADER,
  DurableObjectRunner,
  EXECUTION_PRINCIPAL_HEADER,
  ExecutionFenceStore,
  init,
  StartIdempotencyStore,
} from './do-runner/index.js';
import { createObjectiveRouter, type ObjectiveStore } from './goals/index.js';
import {
  createThreadTopology,
  type ThreadNamespaceLike,
} from './host-kit/index.js';
import {
  createScheduleRouter,
  createScheduleTargetPolicy,
  createScheduleTick,
  D1SchedulesStorage,
  type ScheduleDatabase,
  scheduleWithCreatorRole,
} from './schedules/index.js';
import {
  createWebhookRouter,
  InMemorySubscriptionStoreFactory,
  SIGNAL_PROVIDER_HOST_INSTANCE_NAME,
  type SignalProviderAdapter,
  SignalProviderHost,
  type SignalProviderHostState,
  type SignalProviderHostWiring,
  type SubscriptionStoreFactory,
} from './signal-providers/index.js';
import {
  createNotificationDispatchTick,
  createThreadSignalRoutes,
} from './signals/index.js';

const STATES: readonly ExecutionFenceState[] = [
  'open',
  'draining',
  'migration-locked',
  'proof-only',
];

const PROOF_KEY = 'proof-key-1';
const TEST_IDENTITY_SECRET = 'matrix-deployment-identity-secret-0001';
const THREAD_ID = 'acme_t1';

/** Which of the four exported admission predicates an entry is declared under. */
type PredicateName =
  | 'admitsRunStart'
  | 'admitsExistingRun'
  | 'admitsWorkAuthoring'
  | 'admitsDrainableExecution';

/**
 * The declared predicate, evaluated on a real reading.
 *
 * `nomination` is what proof-only would have to name for this entry to be
 * admitted — an idempotency key for a mint, a runId for work on an existing
 * run — and it is `undefined` on the probe that deliberately does not carry it.
 */
function admits(
  predicate: PredicateName,
  reading: ExecutionFenceReading,
  nomination: string | undefined,
): boolean {
  switch (predicate) {
    case 'admitsRunStart':
      return admitsRunStart(reading, nomination);
    case 'admitsExistingRun':
      return admitsExistingRun(reading, nomination);
    case 'admitsWorkAuthoring':
      return admitsWorkAuthoring(reading);
    case 'admitsDrainableExecution':
      return admitsDrainableExecution(reading);
  }
}

type Admission = 'admitted' | 'refused';

/** What a prepared entry can do once the fence has moved. */
interface Prepared {
  /**
   * What proof-only must name for this entry to be admitted, if anything. An
   * entry with no nomination is never admitted in proof-only, and its
   * nominated probe asserts exactly that rather than a duplicate.
   */
  readonly nomination?: string;
  /** Drive the production entry. `carry` supplies the nomination when true. */
  invoke(carry: boolean): Promise<Admission>;
}

/** One execution entry. */
interface Entry {
  /** How it reads in a failure message. */
  readonly name: string;
  /** The module whose gate this is. */
  readonly module: string;
  /** The predicate the gate must behave as. */
  readonly predicate: PredicateName;
  /**
   * Build the surface with the fence still OPEN, so any prerequisite (a
   * suspended run, a filed approval, a due schedule) is created the way
   * production creates it. The fence moves only after this returns.
   */
  prepare(fence: ExecutionFenceStore): Promise<Prepared>;
}

/** A fresh fence store over its own in-memory database, seeded open. */
async function openFence(): Promise<ExecutionFenceStore> {
  const fence = new ExecutionFenceStore(
    sqliteUnitDatabase(openSqlite()) as ExecutionFenceDatabase,
  );
  await fence.seed('open');
  return fence;
}

/**
 * Classify a driven surface.
 *
 * A refusal reaches a caller two ways — a thrown ExecutionFencedError, or a 503
 * carrying `EXECUTION_FENCED` — and both mean the same thing here. Anything
 * else that throws is a broken drive, not a gate, so it is re-thrown and fails
 * loudly rather than being counted as a refusal. That distinction is the whole
 * reason this helper exists: a drive that quietly errored would otherwise
 * "prove" every entry perfectly fenced.
 */
async function classify(run: () => Promise<unknown>): Promise<Admission> {
  let outcome: unknown;
  try {
    outcome = await run();
  } catch (error) {
    const reason = (error as { reason?: { code?: string } } | undefined)
      ?.reason;
    if (reason?.code === 'EXECUTION_FENCED') return 'refused';
    throw error;
  }
  // `null` is a ROUTER SAYING "not my path", never an admission. Counting it
  // as one would let a drive that addressed the wrong URL report every state
  // perfectly open — the exact shape of false pass this matrix exists to
  // prevent — so it fails as a broken drive instead.
  if (outcome === null) {
    throw new Error('the driven router did not handle the request');
  }
  // A surface that returns a value rather than a Response answered normally.
  if (!(outcome instanceof Response)) return 'admitted';
  if (outcome.ok) return 'admitted';
  const body = (await outcome.json()) as { reason?: { code?: string } };
  if (outcome.status === 503 && body.reason?.code === 'EXECUTION_FENCED') {
    return 'refused';
  }
  // Every OTHER non-2xx is a broken drive, not a verdict. Reading a 401 or a
  // 500 as "admitted" is how a matrix comes to certify gates it never reached:
  // the `open` probe expects admission and would pass on the error, leaving
  // only the closed states to fail and no clue why.
  throw new Error(
    `driven surface answered ${String(outcome.status)}, which is neither an admission nor a fence refusal: ${JSON.stringify(body)}`,
  );
}

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

let seq = 0;
function nextRunId(): string {
  seq += 1;
  return `abc_r${String(seq)}`;
}

/** A workflow whose only step suspends, so a run can be left mid-flight. */
function gatedRuntime(
  fence: ExecutionFenceStore,
  storage = new InMemoryStore(),
): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    {
      executionFence: fence,
      // A real reservation store, not `'none'`: the run object refuses to serve
      // a runtime that has none while its env carries a DB binding, so the
      // opt-out would fail every DO drive below with a wiring error instead of
      // a verdict.
      startIdempotency: new StartIdempotencyStore(
        sqliteUnitDatabase(openSqlite()) as StartIdempotencyDatabase,
      ),
    },
  );
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ go: z.boolean() }),
    execute: async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'wait' });
      return {};
    },
  });
  createWorkflow({
    id: 'gated',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(gate)
    .commit();
  return runtime;
}

/** A D1 double carrying the deployment sentinel the DO hosts verify against. */
function deploymentIdentityDatabase(): unknown {
  const sqlite = openSqlite();
  sqlite.exec(
    `CREATE TABLE flowsafe_deployment (
       id INTEGER PRIMARY KEY CHECK (id = 1),
       tenant_tag TEXT NOT NULL,
       provisioned_at TEXT NOT NULL
     )`,
  );
  sqlite
    .prepare(
      'INSERT INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, ?, ?)',
    )
    .run('acme', new Date(0).toISOString());
  return sqliteUnitDatabase(sqlite);
}

interface RunnerEnv {
  storage: InMemoryStore;
  fence: ExecutionFenceStore;
  owners: DurableObjectRunOwnershipStore;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  DB: unknown;
}

/** The production run-object host, over the real in-memory ownership registry. */
class MatrixRunner extends DurableObjectRunner<RunnerEnv> {
  protected runOwnership(env: RunnerEnv): DurableObjectRunOwnershipStore {
    return env.owners;
  }

  protected runLifecycle(): { abandonApprovals: () => Promise<void> } {
    return { abandonApprovals: async () => undefined };
  }

  protected build(env: RunnerEnv): RunnerRuntime {
    return gatedRuntime(env.fence, env.storage);
  }
}

function matrixRunner(fence: ExecutionFenceStore): MatrixRunner {
  return new MatrixRunner(undefined, {
    storage: new InMemoryStore(),
    fence,
    owners: new InMemoryResourceOwnershipStore(),
    DEPLOYMENT_TENANT: 'acme',
    DEPLOYMENT_IDENTITY_SECRET: TEST_IDENTITY_SECRET,
    DB: deploymentIdentityDatabase(),
  });
}

function runnerRequest(path: string, body: Record<string, unknown>): Request {
  return new Request(`http://do${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      [DEPLOYMENT_IDENTITY_HEADER]: TEST_IDENTITY_SECRET,
      [EXECUTION_PRINCIPAL_HEADER]: JSON.stringify({
        kind: 'human',
        id: 'owner-1',
        role: 'operator',
      }),
    },
    body: JSON.stringify(body),
  });
}

/** An actor context sufficient for the routers that resolve one. */
function actorContext(): ActorContext {
  const actor: ApprovalActor = { id: 'opal', role: 'operator' };
  return {
    actor,
    principal: { kind: 'human', id: actor.id, role: actor.role },
    resourceOwner: { kind: 'human', id: actor.id },
    service: () => {
      throw new Error('unused by the fence gates this matrix drives');
    },
    newRunId: () => nextRunId(),
    newThreadId: () => THREAD_ID,
    resourceIdFromKey: (key: string) => key,
    claimResource: async () => undefined,
    releaseResource: async () => undefined,
    resourceOwnerFor: async () => undefined,
    canAccessResource: async () => true,
    canSelfDecide: () => false,
  } as unknown as ActorContext;
}

/** A thread namespace whose delivery always answers 200. */
function stubThreadNamespace(): ThreadNamespaceLike<string> {
  return {
    idFromName: (name: string) => name,
    get: () => ({
      fetch: async () => new Response(JSON.stringify({ record: {} })),
    }),
  } as unknown as ThreadNamespaceLike<string>;
}

function stubTopology(): ReturnType<typeof createThreadTopology> {
  return createThreadTopology(stubThreadNamespace(), TEST_IDENTITY_SECRET);
}

/** The real D1 schedules domain over node:sqlite, with its schema created. */
async function schedulesDomain(): Promise<D1SchedulesStorage> {
  const store = new D1SchedulesStorage(
    sqliteUnitDatabase(openSqlite()) as ScheduleDatabase,
  );
  await store.init();
  return store;
}

const TARGET_POLICY = createScheduleTargetPolicy({
  workflows: [{ id: 'wf' }],
  agents: [],
});

/**
 * The minimum agent the thread signal routes need, with an ACTIVE thread run so
 * proof-only has something to nominate.
 *
 * `Agent` is a @mastra/core class the routes only ever call methods on, so a
 * structural stand-in is the honest fixture here — the alternative is booting a
 * model, which would test the model.
 */
function matrixAgent(activeRunId: string): Agent {
  const delivered = {
    signal: { id: 's' },
    accepted: Promise.resolve({ action: 'deliver', runId: activeRunId }),
  };
  return {
    id: 'agent',
    __setPubSub: () => undefined,
    getMemory: () => ({ saveMessages: async () => undefined }),
    getActiveThreadRunId: () => activeRunId,
    sendSignal: () => delivered,
    sendMessage: () => delivered,
  } as unknown as Agent;
}

/** The thread-DO scope the signal routes run inside. */
function threadScope(fence: ExecutionFenceStore): unknown {
  return {
    threadId: THREAD_ID,
    actor: { id: 'operator', role: 'operator' },
    principal: { kind: 'human', id: 'operator', role: 'operator' },
    requestedBy: 'operator',
    init: { pubsub: createHostPubSub(), executionFence: fence },
  };
}

interface ProviderEnv {
  factory: SubscriptionStoreFactory;
  fence: ExecutionFenceStore;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  DB: unknown;
}

class MatrixProviderHost extends SignalProviderHost<ProviderEnv> {
  protected build(env: ProviderEnv): SignalProviderHostWiring {
    const provider: SignalProviderAdapter = {
      id: 'poller',
      buildNotification: () => ({
        source: 'poller',
        kind: 'poll',
        summary: 'poll',
      }),
      pollForDeliveries: async () => [],
    };
    return {
      store: env.factory.store(),
      topology: stubTopology(),
      providers: [provider],
      executionFence: env.fence,
    };
  }
}

/** An in-memory thread-state domain for the objective router. */
function objectiveStore(): ObjectiveStore {
  const raw = new Map<string, unknown>();
  const key = (threadId: string, type: string) => `${threadId}::${type}`;
  return {
    getState: async <T = unknown>(args: { threadId: string; type: string }) =>
      raw.get(key(args.threadId, args.type)) as T | undefined,
    setState: async (args: {
      threadId: string;
      type: string;
      value: unknown;
    }) => {
      raw.set(key(args.threadId, args.type), args.value);
    },
    deleteState: async (args: { threadId: string; type: string }) => {
      raw.delete(key(args.threadId, args.type));
    },
  };
}

// ---------------------------------------------------------------------------
// THE ENTRIES
// ---------------------------------------------------------------------------

const ENTRIES: readonly Entry[] = [
  {
    name: 'RunnerRuntime.start',
    module: 'do-runner/runtime.ts — the closure guarantee for every mint',
    predicate: 'admitsRunStart',
    prepare: async (fence) => {
      const runtime = gatedRuntime(fence);
      return {
        nomination: PROOF_KEY,
        invoke: (carry) =>
          classify(() =>
            runtime.start('gated', {
              runId: nextRunId(),
              inputData: {},
              ...(carry ? { idempotencyKey: PROOF_KEY } : {}),
            }),
          ),
      };
    },
  },
  {
    name: 'RunnerRuntime.resume',
    module: 'do-runner/runtime.ts — the closure guarantee for every re-entry',
    predicate: 'admitsExistingRun',
    prepare: async (fence) => {
      const runtime = gatedRuntime(fence);
      const runId = nextRunId();
      await runtime.start('gated', { runId, inputData: {} });
      return {
        nomination: runId,
        invoke: () =>
          classify(() =>
            runtime.resume('gated', runId, {
              step: 'gate',
              resumeData: { go: true },
              requestedBy: 'reviewer-1',
              requestedByKind: 'human',
            }),
          ),
      };
    },
  },
  {
    name: 'run object POST /runs',
    module:
      'do-runner/durable-object.ts — ahead of the recovery journal and the owner reservation',
    predicate: 'admitsRunStart',
    prepare: async (fence) => {
      const runner = matrixRunner(fence);
      return {
        nomination: PROOF_KEY,
        invoke: (carry) =>
          classify(() =>
            runner.fetch(
              runnerRequest('/runs', {
                workflowId: 'gated',
                runId: nextRunId(),
                inputData: {},
                ...(carry ? { idempotencyKey: PROOF_KEY } : {}),
              }),
            ),
          ),
      };
    },
  },
  {
    name: 'run object POST /:workflow/:run/resume',
    module: 'do-runner/durable-object.ts — ahead of the per-run operation lock',
    predicate: 'admitsExistingRun',
    prepare: async (fence) => {
      const runner = matrixRunner(fence);
      const runId = nextRunId();
      await runner.fetch(
        runnerRequest('/runs', { workflowId: 'gated', runId, inputData: {} }),
      );
      return {
        nomination: runId,
        invoke: () =>
          classify(() =>
            runner.fetch(
              runnerRequest(`/runs/gated/${runId}/resume`, {
                step: 'gate',
                resumeData: { go: true },
                requestedBy: 'reviewer-1',
                requestedByKind: 'human',
              }),
            ),
          ),
      };
    },
  },
  {
    name: 'ApprovalService.decide',
    module: 'approval-api/service.ts — commits the decision, then resumes',
    predicate: 'admitsExistingRun',
    prepare: async (fence) => {
      const runId = nextRunId();
      const store = new InMemoryApprovalStore();
      const at = new Date(0).toISOString();
      await store.create({
        id: 'apr-matrix',
        workflowId: 'gated',
        runId,
        title: 'matrix',
        connectors: [],
        priority: 'normal',
        status: 'pending',
        createdAt: at,
        updatedAt: at,
      });
      const service = new ApprovalService({ store, executionFence: fence });
      return {
        nomination: runId,
        invoke: () =>
          classify(() =>
            service.decide(
              'apr-matrix',
              { decision: 'approve' },
              { id: 'reviewer-1', role: 'reviewer' },
            ),
          ),
      };
    },
  },
  {
    name: 'thread object POST /signal',
    module: 'signals/thread-do-routes.ts — delivery into an existing run',
    predicate: 'admitsExistingRun',
    prepare: async (fence) => {
      const runId = nextRunId();
      const routes = createThreadSignalRoutes({
        resolveAgent: () => matrixAgent(runId),
        resolveResourceId: () => 'acme_owner',
      });
      return {
        nomination: runId,
        invoke: () =>
          classify(() =>
            routes(
              new Request('http://thread/signal', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ contents: 'nudge' }),
              }),
              threadScope(fence) as never,
            ),
          ),
      };
    },
  },
  {
    name: 'schedule router create',
    module: 'schedules/router.ts — authoring a standing fire',
    predicate: 'admitsWorkAuthoring',
    prepare: async (fence) => {
      const router = createScheduleRouter({
        resolve: async () => actorContext(),
        store: await schedulesDomain(),
        targetPolicy: TARGET_POLICY,
        validateThreadTarget: async () => undefined,
        executionFence: fence,
      });
      return {
        invoke: () =>
          classify(() =>
            router(
              new Request('http://host/api/schedules', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({
                  workflowId: 'wf',
                  cron: '*/5 * * * *',
                  inputData: {},
                }),
              }),
            ),
          ),
      };
    },
  },
  {
    name: 'schedule tick claim',
    module: 'schedules/tick.ts — one fence read per pass, before any CAS claim',
    predicate: 'admitsWorkAuthoring',
    prepare: async (fence) => {
      const store = await schedulesDomain();
      // Through the same helper the router authors with: the tick re-checks
      // the creator's role at fire time, and a schedule with none is skipped
      // rather than fired — which would read as a fence refusal it is not.
      await store.createSchedule(
        scheduleWithCreatorRole(
          {
            id: 'schedule_a',
            target: { type: 'workflow', workflowId: 'wf' },
            cron: '* * * * *',
            status: 'active',
            nextFireAt: 0,
            createdAt: 0,
            updatedAt: 0,
          },
          'operator',
        ),
      );
      let fired = 0;
      const tick = createScheduleTick({
        store,
        targetPolicy: TARGET_POLICY,
        start: async ({ runId }) => {
          fired += 1;
          return { runId };
        },
        status: async () => undefined,
        executionFence: fence,
        now: () => 1_000,
      });
      return {
        invoke: async () => {
          fired = 0;
          await tick();
          // A fenced pass does NOTHING — it never reaches the CAS, because a
          // claim it will not run consumes the fire (the claim advances
          // nextFireAt) and the fenced runtime then refuses the start. The tick
          // runs on an alarm, so it degrades by doing nothing rather than by
          // refusing; the work it did is the only honest signal.
          return fired > 0 ? 'admitted' : 'refused';
        },
      };
    },
  },
  {
    name: 'objective router PUT',
    module: 'goals/objective-routes.ts — authoring a standing instruction',
    predicate: 'admitsWorkAuthoring',
    prepare: async (fence) => {
      const router = createObjectiveRouter({
        resolve: async () => actorContext(),
        store: objectiveStore(),
        validateThreadTarget: async () => undefined,
        executionFence: fence,
      });
      return {
        invoke: () =>
          classify(() =>
            router(
              new Request(`http://host/api/threads/${THREAD_ID}/goal`, {
                method: 'PUT',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ objective: 'ship it' }),
              }),
            ),
          ),
      };
    },
  },
  {
    name: 'webhook ingress',
    module: 'signal-providers/webhook-route.ts — after signature verification',
    predicate: 'admitsDrainableExecution',
    prepare: async (fence) => {
      const router = createWebhookRouter({
        providers: {
          test: {
            id: 'test',
            verifyWebhookSignature: () => true,
            extractResourceIds: () => [],
            buildNotification: () => ({
              source: 'test',
              kind: 'k',
              summary: 's',
            }),
          },
        },
        subscriptions: new InMemorySubscriptionStoreFactory().store(),
        topology: stubTopology(),
        secretForProvider: () => 'webhook-secret',
        executionFence: fence,
      });
      return {
        invoke: () =>
          classify(() =>
            router(
              new Request('http://host/api/signal-providers/test/webhook', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ id: 'evt-1' }),
              }),
            ),
          ),
      };
    },
  },
  {
    name: 'provider host poll',
    module:
      'signal-providers/host-do.ts — one fence read per pass, before any provider runs',
    predicate: 'admitsDrainableExecution',
    prepare: async (fence) => {
      const host = new MatrixProviderHost(
        {
          id: { name: SIGNAL_PROVIDER_HOST_INSTANCE_NAME },
        } as SignalProviderHostState,
        {
          factory: new InMemorySubscriptionStoreFactory(),
          fence,
          DEPLOYMENT_TENANT: 'acme',
          DEPLOYMENT_IDENTITY_SECRET: TEST_IDENTITY_SECRET,
          DB: deploymentIdentityDatabase(),
        },
      );
      return { invoke: () => classify(() => host.poll()) };
    },
  },
  {
    name: 'notification dispatch tick',
    module: 'signals/notification-dispatch.ts — before the due read',
    predicate: 'admitsDrainableExecution',
    prepare: async (fence) => {
      let listed = 0;
      const storage = {
        listDueNotifications: async () => {
          listed += 1;
          return [];
        },
      } as unknown as NotificationsStorage;
      const tick = createNotificationDispatchTick({
        storage,
        topology: stubTopology(),
        resolveContext: () => actorContext(),
        executionFence: fence,
      });
      return {
        invoke: async () => {
          listed = 0;
          await tick();
          // The gate sits BEFORE the due read, so whether the inbox was
          // consulted at all is what the pass admitted or refused. Like the
          // schedule tick this runs on an alarm and never throws.
          return listed > 0 ? 'admitted' : 'refused';
        },
      };
    },
  },
  {
    name: 'background task enqueue',
    module:
      'background-tasks/host.ts — a drain still accepts, a lock refuses new rows',
    predicate: 'admitsDrainableExecution',
    prepare: async (fence) => {
      const pubsub = createHostPubSub();
      const host = new BackgroundTaskHost({
        mastra: new Mastra({ storage: new InMemoryStore(), pubsub }),
        pubsub,
        executors: {},
        executionFence: fence,
      });
      // Booted while the fence is still open, exactly as a host boots before an
      // operator drains it: `boot()` is deliberately NOT fence-gated (a fenced
      // refusal would be memoized forever and would take the read routes down
      // with it), so the gate this entry drives is the enqueue's own.
      await host.boot();
      return {
        invoke: () =>
          classify(async () => {
            seq += 1;
            await host.enqueue(
              {
                toolName: 'longResearch',
                toolCallId: `call-${String(seq)}`,
                args: {},
                agentId: 'agent-1',
                runId: 'abc_r1',
              },
              { executor: { execute: async () => ({ done: true }) } },
            );
          }),
      };
    },
  },
];

// ---------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------

/**
 * Every place in `src/` that consults an admission predicate, and how the four
 * fence states are exercised against it.
 *
 * THIS IS THE LIST'S ENFORCEMENT. The drives above prove that the gates we know
 * about behave correctly; they can say nothing about a gate nobody added and
 * nothing about a gate someone deleted. The census below reads the source, so a
 * new call site fails until it is written down here — with either the matrix
 * entry that drives it, or the suite that already does.
 *
 * `drivenBy` names a matrix entry above wherever one exists. The three that
 * name a test file instead are gates this file cannot reach without a seam that
 * production has no other reason to publish: two of them fire inside the
 * background-task host's private dispatch path, and one is the wake lane of a
 * route whose other arm IS driven here. Each is exercised across all four
 * states in the file named.
 */
const GATE_SITES: ReadonlyArray<{
  file: string;
  predicate: PredicateName;
  drivenBy: string;
}> = [
  {
    file: 'approval-api/service.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'ApprovalService.decide',
  },
  {
    file: 'background-tasks/host.ts',
    predicate: 'admitsDrainableExecution',
    drivenBy: 'background task enqueue',
  },
  {
    file: 'background-tasks/host.ts',
    predicate: 'admitsDrainableExecution',
    // #attemptDispatching: whether this instance subscribes and claims at all.
    drivenBy: 'background-tasks/host.test.ts',
  },
  {
    file: 'background-tasks/host.ts',
    predicate: 'admitsDrainableExecution',
    // The wrapped executor: the task BODY, gated per dispatch so a worker that
    // started while open cannot run one after a transition.
    drivenBy: 'background-tasks/host.test.ts',
  },
  {
    file: 'do-runner/durable-object.ts',
    predicate: 'admitsRunStart',
    drivenBy: 'run object POST /runs',
  },
  {
    file: 'do-runner/durable-object.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'run object POST /:workflow/:run/resume',
  },
  {
    file: 'do-runner/runtime.ts',
    predicate: 'admitsRunStart',
    drivenBy: 'RunnerRuntime.start',
  },
  {
    file: 'do-runner/runtime.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'RunnerRuntime.resume',
  },
  {
    file: 'goals/objective-routes.ts',
    predicate: 'admitsWorkAuthoring',
    drivenBy: 'objective router PUT',
  },
  {
    file: 'schedules/router.ts',
    predicate: 'admitsWorkAuthoring',
    drivenBy: 'schedule router create',
  },
  {
    file: 'schedules/tick.ts',
    predicate: 'admitsWorkAuthoring',
    drivenBy: 'schedule tick claim',
  },
  {
    file: 'signal-providers/host-do.ts',
    predicate: 'admitsDrainableExecution',
    drivenBy: 'provider host poll',
  },
  {
    file: 'signal-providers/webhook-route.ts',
    predicate: 'admitsDrainableExecution',
    drivenBy: 'webhook ingress',
  },
  {
    file: 'signals/notification-dispatch.ts',
    predicate: 'admitsDrainableExecution',
    drivenBy: 'notification dispatch tick',
  },
  {
    file: 'signals/thread-do-routes.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'thread object POST /signal',
  },
  {
    file: 'signals/thread-do-routes.ts',
    predicate: 'admitsExistingRun',
    // handleWake's own check, for the wake path it owns.
    drivenBy: 'signals/thread-do-routes.test.ts',
  },
];

/**
 * The files whose `admits*` mentions are not call sites: the module that
 * DEFINES the predicates, and the barrel that re-exports them.
 */
const NOT_GATE_FILES = ['do-runner/execution-fence.ts', 'do-runner/index.ts'];

/**
 * Every `admits*(` call in the package's source, as (file, predicate) pairs.
 *
 * `process.getBuiltinModule` rather than an import: this package's test
 * tsconfig is workers-typed and carries no `@types/node`, so a static `node:`
 * specifier does not type-check — the same idiom the schema guard uses to reach
 * `node:sqlite`.
 */
function gateCallSites(): Array<{ file: string; predicate: PredicateName }> {
  const fs = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule?.('node:fs') as {
    readdirSync: (
      path: string,
      options: { withFileTypes: true },
    ) => Array<{ name: string; isDirectory: () => boolean }>;
    readFileSync: (path: string, encoding: string) => string;
  };
  // This module's OWN directory, never `process.cwd()`: the working directory
  // is the package under a filtered run and the workspace root under the root
  // `pnpm test`, so a cwd-relative path passes one and throws ENOENT on the
  // other — and the root run is the one CI actually gates on.
  // The cast is for the TYPE, not the value: this package's test tsconfig is
  // workers-typed, and its ambient `ImportMeta` does not declare `url` — which
  // every ESM runtime, vitest's included, provides.
  const here = (import.meta as ImportMeta & { url: string }).url;
  const root = new URL('.', here).pathname.replace(/\/$/, '');
  const found: Array<{ file: string; predicate: PredicateName }> = [];
  const pattern =
    /\badmits(RunStart|ExistingRun|WorkAuthoring|DrainableExecution)\s*\(/g;
  const walk = (directory: string, prefix: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const absolute = `${directory}/${entry.name}`;
      const relative = prefix === '' ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        walk(absolute, relative);
        continue;
      }
      if (!entry.name.endsWith('.ts') || entry.name.endsWith('.test.ts')) {
        continue;
      }
      if (NOT_GATE_FILES.includes(relative)) continue;
      const source = fs.readFileSync(absolute, 'utf8');
      for (const match of source.matchAll(pattern)) {
        found.push({
          file: relative,
          predicate: `admits${match[1] as string}` as PredicateName,
        });
      }
    }
  };
  walk(root, '');
  return found;
}

function siteKey(site: { file: string; predicate: string }): string {
  return `${site.file} :: ${site.predicate}`;
}

describe('execution-entry matrix', () => {
  it('accounts for every admission call site in the source, and for no site that is gone', () => {
    // #given — the drives below can only prove the gates somebody listed. This
    // is what catches the gate nobody listed: a new execution entry, or a check
    // quietly deleted from an existing one.
    const actual = gateCallSites().map(siteKey).sort();
    const declared = GATE_SITES.map(siteKey).sort();

    // #then — multiset equality both ways. An unlisted call site fails (a new
    // entry nobody censused), and so does a listed one that no longer exists (a
    // gate removed while its row stayed behind, claiming protection that is no
    // longer there).
    expect(actual).toEqual(declared);
  });

  it('drives every gate site it claims to, and names a real suite for the rest', () => {
    // #given — `drivenBy` is either a matrix entry above or a test file. A
    // typo in the first would silently turn a driven gate into a delegated one.
    const entryNames = new Set(ENTRIES.map((entry) => entry.name));

    // #then
    for (const site of GATE_SITES) {
      if (site.drivenBy.endsWith('.test.ts')) continue;
      expect(
        entryNames,
        `${siteKey(site)} claims to be driven by '${site.drivenBy}', which is not a matrix entry`,
      ).toContain(site.drivenBy);
    }

    // #and — every matrix entry drives at least one real gate site, so an
    // entry whose surface stopped consulting the fence cannot keep passing on
    // an admission it now grants unconditionally.
    const driven = new Set(GATE_SITES.map((site) => site.drivenBy));
    for (const entry of ENTRIES) {
      expect(
        driven,
        `matrix entry '${entry.name}' drives no censused gate site`,
      ).toContain(entry.name);
    }
  });

  it('names every entry exactly once and exercises all four predicates', () => {
    // #given — the LIST is the product here: a duplicate name would let two
    // rows describe one gate while a third gate went unlisted, and a predicate
    // with no entry left would make its family vacuously satisfied.
    const names = ENTRIES.map((entry) => entry.name);

    // #then
    expect(new Set(names).size).toBe(names.length);
    expect(new Set(ENTRIES.map((entry) => entry.predicate)).size).toBe(4);
  });

  for (const entry of ENTRIES) {
    for (const state of STATES) {
      it(`${entry.name} behaves as ${entry.predicate} under '${state}'`, async () => {
        // #given — the surface built while the fence is still open, so its
        // prerequisites are created the way production creates them.
        const fence = await openFence();
        const prepared = await entry.prepare(fence);

        // #when — the fence moves to the state under test.
        if (state !== 'open') {
          await fence.transition({
            expected: 'open',
            next: state,
            ...(state === 'proof-only' ? { proofKey: PROOF_KEY } : {}),
          });
        }
        const reading = await fence.read();

        // #then — carrying no nomination, the declared predicate decides, and
        // the driven surface must agree with it. The expectation comes from the
        // real exported predicate, never from a hand-written table, so an entry
        // declared under the wrong one fails here.
        expect(await prepared.invoke(false)).toBe(
          admits(entry.predicate, reading, undefined) ? 'admitted' : 'refused',
        );
      });
    }

    it(`${entry.name} answers its proof-only nomination as ${entry.predicate} (${entry.module})`, async () => {
      // #given — the probe that separates the predicate PAIRS. Across open,
      // draining, and migration-locked, admitsRunStart is indistinguishable
      // from admitsWorkAuthoring and admitsExistingRun from
      // admitsDrainableExecution; only the nominated proof-only case tells them
      // apart.
      const fence = await openFence();
      const prepared = await entry.prepare(fence);
      await fence.transition({
        expected: 'open',
        next: 'proof-only',
        proofKey: PROOF_KEY,
      });
      if (
        prepared.nomination !== undefined &&
        entry.predicate === 'admitsExistingRun'
      ) {
        // For work on an EXISTING run the nomination is the run itself, bound
        // the way an admitted proof-only start binds it.
        await fence.recordProofRun(PROOF_KEY, prepared.nomination);
      }
      const reading = await fence.read();
      const expected = admits(entry.predicate, reading, prepared.nomination)
        ? 'admitted'
        : 'refused';

      // #then — an entry with a nomination is admitted when it carries it; one
      // without is refused however it is driven, which is the whole meaning of
      // "nothing nominates authoring or queued execution".
      expect(await prepared.invoke(true)).toBe(expected);
      if (prepared.nomination === undefined) expect(expected).toBe('refused');
    });
  }
});
