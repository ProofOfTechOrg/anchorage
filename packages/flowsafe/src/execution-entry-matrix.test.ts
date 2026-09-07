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
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import { createStep, createWorkflow } from '@mastra/core/workflows';
import ts from 'typescript';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../test-support/sqlite.js';
import type { ActorContext, ApprovalActor } from './approval-api/index.js';
import {
  ApprovalService,
  InMemoryApprovalStore,
  InMemoryResourceOwnershipStore,
} from './approval-api/index.js';
import { BackgroundTaskHost } from './background-tasks/index.js';
import { RUN_PROVENANCE_CONTEXT_KEY } from './do-runner/execution-context.js';
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
  createD1Storage,
  createHostPubSub,
  DEPLOYMENT_IDENTITY_HEADER,
  DurableObjectRunner,
  EXECUTION_PRINCIPAL_HEADER,
  ExecutionFenceStore,
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowsStorageD1,
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
  prepare(
    fence: ExecutionFenceStore,
    database: ExecutionFenceDatabase,
    sqlite: SqliteDatabase,
  ): Promise<Prepared>;
}

/** A fresh fence store over its own in-memory database, seeded open. */
async function openFence(): Promise<{
  fence: ExecutionFenceStore;
  database: ExecutionFenceDatabase;
  sqlite: SqliteDatabase;
}> {
  const sqlite = openSqlite();
  const database = sqliteUnitDatabase(sqlite) as ExecutionFenceDatabase;
  const fence = new ExecutionFenceStore(database);
  await fence.seed('open');
  return { fence, database, sqlite };
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
    name: 'FencedWorkflowsStorageD1.withInitialAdmission',
    module: 'do-runner/fenced-workflows-d1.ts — final initial INSERT guard',
    predicate: 'admitsRunStart',
    prepare: async (fence, database, sqlite) => {
      const workflowId = 'matrix-owned-initial';
      const runId = nextRunId();
      const execution = {
        tablePrefix: '',
        workflowId,
        runId,
        startToken: crypto.randomUUID(),
      };
      const attemptToken = crypto.randomUUID();
      const startIdentity = {
        owner: { kind: 'human' as const, id: 'matrix-owner' },
        target: { kind: 'workflow' as const, id: workflowId },
      };
      const storage = createD1Storage({ binding: database });
      let engineCalls = 0;
      const workflow = createWorkflow({
        id: workflowId,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
        .then(
          createStep({
            id: 'effect',
            inputSchema: z.object({}),
            outputSchema: z.object({}),
            execute: async () => {
              engineCalls += 1;
              return {};
            },
          }),
        )
        .commit();
      const mastra = new Mastra({
        storage,
        workflows: { [workflowId]: workflow },
      });
      await storage.init();
      const domain = (await mastra.getStorage()?.getStore('workflows')) as
        | FencedWorkflowsStorageD1
        | undefined;
      const capability = domain?.[FENCED_WORKFLOW_STORAGE];
      if (!capability) throw new Error('composed owned capability is missing');
      expect(capability.database).toBe(database);

      const reservationStore = new StartIdempotencyStore(database);
      await reservationStore.reserve({
        key: PROOF_KEY,
        owner: startIdentity.owner,
        targetKind: 'workflow',
        targetId: workflowId,
        mintRunId: () => runId,
      });
      expect(await reservationStore.claim(PROOF_KEY, runId)).toBe(true);
      // B1 does not activate modern reserve emission; this is its required
      // already-modern unbound precondition, not a new production minter.
      sqlite
        .prepare(
          "UPDATE flowsafe_start_idempotency SET start_token = '' WHERE key = ?",
        )
        .run(PROOF_KEY);
      const reservation = await reservationStore.readForAdmission(PROOF_KEY);
      if (!reservation) throw new Error('initial reservation is missing');

      return {
        nomination: PROOF_KEY,
        invoke: async (carry) => {
          const reading = await fence.read();
          const expected = admitsRunStart(
            reading,
            carry ? PROOF_KEY : undefined,
          );
          const fenceBefore = sqlite
            .prepare('SELECT * FROM flowsafe_execution_fence')
            .get() as Record<string, unknown>;
          const reservationBefore = sqlite
            .prepare('SELECT * FROM flowsafe_start_idempotency WHERE key = ?')
            .get(PROOF_KEY) as Record<string, unknown>;
          const requestContext = {
            [RUN_PROVENANCE_CONTEXT_KEY]: {
              version: 2,
              startToken: execution.startToken,
              attemptToken,
              startIdentity,
              requestedBy: startIdentity.owner.id,
              requestedByKind: startIdentity.owner.kind,
              resumeCounts: [],
            },
          };
          let attempts = 0;
          try {
            return await classify(async () => {
              const { value, witness } = await capability.withInitialAdmission(
                {
                  execution,
                  attemptToken,
                  startIdentity,
                  requestContext,
                  fence,
                  reservationStore,
                  reservation,
                  ...(carry
                    ? {
                        proof: {
                          key: PROOF_KEY,
                          mutationEpoch: reading.mutationEpoch,
                          transitionRevision: reading.transitionRevision,
                        },
                      }
                    : {}),
                  onInitialWriteAttempt: () => {
                    attempts += 1;
                  },
                },
                () => workflow.createRun({ runId }),
              );
              expect(witness.execution).toEqual(execution);
              expect(
                JSON.parse(witness.row.snapshot).requestContext[
                  RUN_PROVENANCE_CONTEXT_KEY
                ].initialAdmission,
              ).toBe(true);
              expect(engineCalls).toBe(0);
              const result = await value.start({
                inputData: {},
                requestContext: new RequestContext(
                  Object.entries(requestContext),
                ),
              });
              expect(result.status).toBe('success');
            });
          } finally {
            // A post-INSERT decoder refusal must not hide an unauthorized row.
            const snapshots = sqlite
              .prepare(
                'SELECT * FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
              )
              .all(workflowId, runId);
            expect(snapshots, 'final SQL snapshot effects').toHaveLength(
              expected ? 1 : 0,
            );
            expect(
              sqlite
                .prepare(
                  'SELECT * FROM flowsafe_start_idempotency WHERE key = ?',
                )
                .get(PROOF_KEY),
              'final SQL reservation effects',
            ).toEqual(
              expected
                ? {
                    ...reservationBefore,
                    start_token: execution.startToken,
                    start_table_prefix: execution.tablePrefix,
                    start_workflow_id: workflowId,
                  }
                : reservationBefore,
            );
            expect(
              sqlite.prepare('SELECT * FROM flowsafe_execution_fence').get(),
              'final SQL proof effects',
            ).toEqual(
              expected && reading.state === 'proof-only'
                ? {
                    ...fenceBefore,
                    proof_run_id: runId,
                    proof_table_prefix: execution.tablePrefix,
                    proof_workflow_id: workflowId,
                    proof_start_token: execution.startToken,
                    updated_at: expect.any(Number),
                  }
                : fenceBefore,
            );
            expect(attempts).toBe(1);
            expect(engineCalls).toBe(expected ? 1 : 0);
          }
        },
      };
    },
  },
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
 * Every place in `src/` that consults an admission predicate or guards an
 * initial INSERT in SQL, and how the four fence states are exercised against it.
 *
 * THIS IS THE LIST'S ENFORCEMENT. The drives above prove that the gates we know
 * about behave correctly; they can say nothing about a gate nobody added and
 * nothing about a gate someone deleted. The census below reads the source, so a
 * new boundary fails until it is written down here — with either the matrix
 * entry that drives it, or the suite that already does.
 *
 * `drivenBy` names a matrix entry above wherever one exists. The three that
 * name a test file instead are gates this file cannot reach without a seam that
 * production has no other reason to publish: two of them fire inside the
 * background-task host's private dispatch path, and one is the wake lane of a
 * route whose other arm IS driven here. Each is exercised across all four
 * states in the file named.
 */
type GateSite = {
  file: string;
  predicate: PredicateName;
  sql?: 'initial-snapshot-insert';
};

const GATE_SITES: ReadonlyArray<GateSite & { drivenBy: string }> = [
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
    file: 'do-runner/fenced-workflows-d1.ts',
    predicate: 'admitsRunStart',
    sql: 'initial-snapshot-insert',
    drivenBy: 'FencedWorkflowsStorageD1.withInitialAdmission',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    predicate: 'admitsRunStart',
    // Post-zero diagnosis, not the INSERT's final admission authority.
    drivenBy: 'FencedWorkflowsStorageD1.withInitialAdmission',
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

type SourceFileSystem = {
  existsSync(path: string | URL): boolean;
  readdirSync(
    path: string,
    options: { withFileTypes: true },
  ): Array<{ name: string; isDirectory: () => boolean }>;
  readFileSync(path: string, encoding: string): string;
};

function sourceFileSystem(): SourceFileSystem {
  return (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule?.('node:fs') as SourceFileSystem;
}

type SourceFile = { readonly file: string; readonly source: string };

function sourceRoot(): string {
  // This module's OWN directory, never `process.cwd()`: filtered and root test
  // runs use different working directories.
  const here = (import.meta as ImportMeta & { url: string }).url;
  return new URL('.', here).pathname.replace(/\/$/, '');
}

function walkSourceFiles(
  root: string,
  visit: (sourceFile: SourceFile) => void,
): void {
  const fs = sourceFileSystem();
  const walk = (directory: string, prefix: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const absolute = `${directory}/${entry.name}`;
      const relative = prefix === '' ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        walk(absolute, relative);
        continue;
      }
      if (
        !/\.(?:ts|tsx)$/.test(entry.name) ||
        /\.(?:test|stories)\.(?:ts|tsx)$/.test(entry.name)
      ) {
        continue;
      }
      visit({ file: relative, source: fs.readFileSync(absolute, 'utf8') });
    }
  };
  walk(root, '');
}

/**
 * Every direct `admits*(` call, preserving the original lexical census and
 * its declaration/barrel exclusions. SQL discovery has no such exclusions.
 *
 * The filesystem reader keeps the schema guard's getBuiltinModule idiom,
 * without adding a direct Node ambient-type requirement to this test.
 */
function predicateCallSites({ file, source }: SourceFile): GateSite[] {
  const found: GateSite[] = [];
  const pattern =
    /\badmits(RunStart|ExistingRun|WorkAuthoring|DrainableExecution)\s*\(/g;
  if (NOT_GATE_FILES.includes(file)) return found;
  for (const match of source.matchAll(pattern)) {
    found.push({
      file,
      predicate: `admits${match[1] as string}` as PredicateName,
    });
  }
  return found;
}

/** Presence/deletion census of the actual inline initial INSERT guard. */
function sqlAdmissionSites({ file, source }: SourceFile): GateSite[] {
  const parsed = ts.createSourceFile(
    file,
    source,
    ts.ScriptTarget.Latest,
    true,
  );
  const found: GateSite[] = [];
  const visit = (node: ts.Node): void => {
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      node.expression.name.text === 'prepare'
    ) {
      const argument = node.arguments[0];
      if (
        argument &&
        (ts.isStringLiteral(argument) ||
          ts.isNoSubstitutionTemplateLiteral(argument) ||
          ts.isTemplateExpression(argument))
      ) {
        const sql = argument.getText(parsed).slice(1, -1);
        if (
          /^\s*INSERT\s+INTO\b/i.test(sql) &&
          /\bWHERE\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+(?:flowsafe_execution_fence|\$\{EXECUTION_FENCE_TABLE\})\s+AS\s+f\b/i.test(
            sql,
          )
        ) {
          found.push({
            file,
            predicate: 'admitsRunStart',
            sql: 'initial-snapshot-insert',
          });
        }
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(parsed);
  return found;
}

function gateCallSites(): GateSite[] {
  const found: GateSite[] = [];
  walkSourceFiles(sourceRoot(), (sourceFile) => {
    found.push(
      ...predicateCallSites(sourceFile),
      ...sqlAdmissionSites(sourceFile),
    );
  });
  return found;
}

type FenceErrorName = 'ExecutionFencedError' | 'ExecutionFenceUnreadableError';

/**
 * Every production site that AUTHORS a fence refusal or unreadable-store
 * failure. Each row states why the error prevents engine execution. Initial
 * admission may already have persisted rows: those sites must say so, without
 * claiming their refusal proves no durable write. The scan makes a new author
 * fail until its boundary is reviewed and recorded here. It is lexical:
 * a constructor spelling in a comment or string fails loud and asks for review.
 * Aliased class names and namespace imports are forbidden so lexical coverage
 * cannot be bypassed without first changing this test.
 */
const FENCE_ERROR_AUTHORS: ReadonlyArray<{
  file: string;
  error: FenceErrorName;
  anchor: string;
  beforeExecutionEffect: string;
}> = [
  {
    file: 'do-runner/execution-admission.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const current = candidate?.mutationEpoch;',
    beforeExecutionEffect:
      'The public epoch helper rejects malformed reading metadata before returning an admission comparison; it performs no execution.',
  },
  {
    file: 'approval-api/service.ts',
    error: 'ExecutionFencedError',
    anchor: 'async #assertDecidable',
    beforeExecutionEffect:
      'The admission check runs before decide() mutates the approval or resumes its run.',
  },
  {
    file: 'background-tasks/host.ts',
    error: 'ExecutionFencedError',
    anchor: '#gated(executor',
    beforeExecutionEffect:
      'The executor backstop refuses before calling a tool body when core supplies no suspension seam.',
  },
  {
    file: 'background-tasks/host.ts',
    error: 'ExecutionFencedError',
    anchor: 'async enqueue(',
    beforeExecutionEffect:
      'The enqueue admission check runs before the manager creates a queued task row.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFencedError',
    anchor: 'const startFence =',
    beforeExecutionEffect:
      'The start route refuses before source lookup, recovery journalling, owner reservation, or runtime start.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFencedError',
    anchor: 'const resumeFence =',
    beforeExecutionEffect:
      'The resume route refuses before handing the existing run to runtime.resume().',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFencedError',
    anchor: 'export function executionFencedResponse',
    beforeExecutionEffect:
      'The response helper only serializes an already-decided refusal and performs no execution effect.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async readForAdmission():',
    beforeExecutionEffect:
      'The pure current-schema observation rejects missing or malformed metadata before engine entry; diagnostic/readback callers may follow a durable initial admission, so this error alone proves no absence.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (stored.schemaStage > stage)',
    beforeExecutionEffect:
      'Fence-row validation fails closed before any caller can admit execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async #initialize(',
    beforeExecutionEffect:
      'Initialization validates administrative metadata without admitting execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: '#decodeReturned(result:',
    beforeExecutionEffect:
      'A malformed metadata-write result refuses before a caller can admit execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (receipt !== null)',
    beforeExecutionEffect:
      'An uncertain administrative CAS does not execute a run or schedule.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "reading?.state === 'proof-only'",
    beforeExecutionEffect:
      'A failed proof-binding metadata write becomes unreadable before the runtime starts the run.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'function terminalizationUnreadable(',
    beforeExecutionEffect:
      'Explicit initial-row terminalization never enters an engine or grants no-insert authority; malformed input observations or uncertain terminal writes refuse through this fixed operation boundary without replay.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const counts = decodeResumeCounts(resumeCounts);',
    beforeExecutionEffect:
      'The progress decoder validates only owned metadata and performs no I/O or execution. A retained admission stamp never proves unchanged bytes or no effects, and decoding failures cannot grant definitive-zero evidence.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (!scope.witness) {',
    beforeExecutionEffect:
      'A createRun callback without a positive persistence witness cannot enter the engine; another domain or swallowed failure may already have written, so missing witness gives no definitive-zero authority.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (await this.#converged(',
    beforeExecutionEffect:
      'A thrown batch with no exact converged readback blocks engine entry; the batch may already have committed durable admission and this uncertain refusal cannot authorize retry or journal clearing.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: '} else if (proofRows.length !== 0)',
    beforeExecutionEffect:
      'Malformed returned batch data blocks engine entry after a possible committed initial INSERT; no recovery read or missing-result assumption upgrades it to success or definitive zero.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFencedError',
    anchor: 'const proofSlotUnbound =',
    beforeExecutionEffect:
      'After a validated all-zero chained batch, the current state/key/round diagnostic explains refusal before engine entry; the SQL result, not this JavaScript predicate, establishes no initial write.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "error.reason?.code === 'MUTATION_EPOCH_MISMATCH'",
    beforeExecutionEffect:
      'An unreadable post-zero diagnostic blocks engine entry while preserving the already validated all-zero result; failed observation alone would not establish absence of durable admission.',
  },
  {
    file: 'do-runner/workflow-snapshot-row.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'export async function readRawWorkflowSnapshot(',
    beforeExecutionEffect:
      'The exact reader performs no writes and refuses malformed or unavailable rows before its caller enters the engine; admission readback may follow an already committed initial row and does not prove no write.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'export function decodeRunStartIdentity(',
    beforeExecutionEffect:
      'The role-neutral provenance decoder performs no storage or execution and rejects malformed owned identity before callers can use it to authorize engine entry.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'resumeCounts: Object.freeze([]) as readonly [],',
    beforeExecutionEffect:
      'The initial provenance decoder validates before engine entry; callers can use it on a returned or read-back initial row, so decoder failure does not establish absence of durable admission.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: 'if (!admitsRunStart(reading, idempotencyKey))',
    beforeExecutionEffect:
      'The start admission check refuses before proof binding and engine run creation.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: 'if (!(await fence.recordProofRun',
    beforeExecutionEffect:
      'A lost proof-binding compare-and-set refuses while engine run creation has not begun.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: 'async #assertResumeFence',
    beforeExecutionEffect:
      'The resume admission check refuses before the engine continues the existing run.',
  },
  {
    file: 'signal-providers/host-do.ts',
    error: 'ExecutionFencedError',
    anchor: 'async poll(): Promise<PollResult>',
    beforeExecutionEffect:
      'The poll admission check refuses before any provider is polled or notification is delivered.',
  },
];

function fenceErrorAuthorSites(): Array<{
  file: string;
  error: FenceErrorName;
  line: number;
}> {
  const found: Array<{ file: string; error: FenceErrorName; line: number }> =
    [];
  const pattern =
    /\bnew\s+(ExecutionFencedError|ExecutionFenceUnreadableError)\s*\(/g;
  walkSourceFiles(sourceRoot(), ({ file, source }) => {
    for (const match of source.matchAll(pattern)) {
      found.push({
        file,
        error: match[1] as FenceErrorName,
        line: source.slice(0, match.index).split('\n').length,
      });
    }
  });
  return found;
}

const FENCE_ERROR_CENSUS_PATTERNS = [
  {
    pattern:
      /\b(?:ExecutionFencedError|ExecutionFenceUnreadableError)\s+as\s+\w+/g,
    message:
      'import the fence error classes under their own names so the census can see the site, or reword a comment that spells one of these class names followed by `as`',
  },
  {
    pattern:
      /\bnew\s+\w+\.(?:ExecutionFencedError|ExecutionFenceUnreadableError)\s*\(/g,
    message:
      'construct the fence error classes under their own imported names so the census can see the site',
  },
  {
    pattern:
      /\bextends\s+(?:\w+\.)*(?:ExecutionFencedError|ExecutionFenceUnreadableError)\b/g,
    message:
      'subclassing the fence error classes is not census-visible; author fence refusals with the two classes directly',
  },
] as const;

function matchesFenceErrorCensusViolation(source: string): boolean {
  return FENCE_ERROR_CENSUS_PATTERNS.some(({ pattern }) =>
    new RegExp(pattern.source, pattern.flags).test(source),
  );
}

function fenceErrorCensusViolations(): string[] {
  const found: string[] = [];
  walkSourceFiles(sourceRoot(), ({ file, source }) => {
    for (const { pattern, message } of FENCE_ERROR_CENSUS_PATTERNS) {
      for (const match of source.matchAll(pattern)) {
        const line = source.slice(0, match.index).split('\n').length;
        found.push(`${file}:${line}: ${message}: ${match[0]}`);
      }
    }
  });
  return found;
}

const ANCHOR_WINDOW_LINES = 30;
const sourceLinesByFile = new Map<string, readonly string[]>();

function anchorDistanceBeforeAuthor(
  author: (typeof FENCE_ERROR_AUTHORS)[number],
  site: ReturnType<typeof fenceErrorAuthorSites>[number],
): number | undefined {
  let lines = sourceLinesByFile.get(site.file);
  if (!lines) {
    lines = sourceFileSystem()
      .readFileSync(`${sourceRoot()}/${site.file}`, 'utf8')
      .split('\n');
    sourceLinesByFile.set(site.file, lines);
  }
  for (
    let index = site.line - 2;
    index >= Math.max(0, site.line - (ANCHOR_WINDOW_LINES + 1));
    index -= 1
  ) {
    if (lines[index]?.includes(author.anchor)) {
      return site.line - (index + 1);
    }
  }
  return undefined;
}

function siteKey(site: GateSite): string {
  return `${site.file} :: ${site.predicate} :: ${site.sql ?? 'predicate'}`;
}

describe('execution-entry matrix', () => {
  it.each([
    'do-runner/runtime.ts',
    'do-runner/durable-object.ts',
    'do-runner/thread-do.ts',
    'agent-host/thread-host.ts',
    'agent-runner/durable-agent-runner.ts',
  ])('C transport entry keeps activation APIs dormant: %s', (file) => {
    const source = sourceFileSystem().readFileSync(
      `${sourceRoot()}/${file}`,
      'utf8',
    );
    const parsed = ts.createSourceFile(
      file,
      source,
      ts.ScriptTarget.Latest,
      true,
    );
    const calls: string[] = [];
    const forbidden = new Set([
      'assertMutationEpoch',
      'withInitialAdmission',
      'terminalizeInitialAdmission',
      'onPreparedStartIdentity',
    ]);
    const visit = (node: ts.Node): void => {
      if (ts.isCallExpression(node)) {
        const expression = node.expression;
        const name = ts.isIdentifier(expression)
          ? expression.text
          : ts.isPropertyAccessExpression(expression)
            ? expression.name.text
            : undefined;
        if (name && forbidden.has(name)) calls.push(name);
      }
      ts.forEachChild(node, visit);
    };
    visit(parsed);
    expect(calls).toEqual([]);
  });

  describe('SQL admission source census', () => {
    const fenceTable = `\${EXECUTION_FENCE_TABLE}`;
    const insert = `INSERT INTO \${snapshotTable}`;
    const guard = `WHERE EXISTS (SELECT 1 FROM ${fenceTable} AS f WHERE f.state = 'open')`;
    const guardedPrepare = `database.prepare(\`${insert} SELECT 1 ${guard} \${optionalParticipantClauses}\`)`;
    const sqlSite: GateSite = {
      file: 'do-runner/fenced-workflows-d1.ts',
      predicate: 'admitsRunStart',
      sql: 'initial-snapshot-insert',
    };

    it.each([
      ['template with participant interpolation', guardedPrepare],
      [
        'whitespace and a literal table name',
        'database.prepare(`\n INSERT\n INTO snapshot SELECT 1\n' +
          guard.replace(fenceTable, 'flowsafe_execution_fence') +
          '`)',
      ],
      [
        'quoted string argument',
        `database.prepare("INSERT INTO snapshot SELECT 1 ${guard.replace(fenceTable, 'flowsafe_execution_fence')}")`,
      ],
    ])('finds an actual prepare argument: %s', (_name, source) => {
      expect(sqlAdmissionSites({ file: sqlSite.file, source })).toEqual([
        sqlSite,
      ]);
    });

    it.each([
      ['line comment', `// ${guardedPrepare}`],
      ['block comment', `/* ${guardedPrepare} */`],
      [
        'unused template',
        guardedPrepare.replace('database.prepare(', 'void ('),
      ],
      ['different method', guardedPrepare.replace('.prepare(', '.inspect(')],
      [
        'indirect argument',
        `const sql = \`INSERT INTO snapshot SELECT 1 ${guard}\`; database.prepare(sql)`,
      ],
      [
        'diagnostic SELECT',
        guardedPrepare.replace(insert, 'SELECT * FROM snapshot'),
      ],
      [
        'proof UPDATE',
        guardedPrepare.replace(
          `${insert} SELECT 1`,
          'UPDATE flowsafe_execution_fence SET proof_run_id = 1',
        ),
      ],
      ['unguarded INSERT', guardedPrepare.replace(guard, 'WHERE 1 = 1')],
    ])('does not invent a SQL gate from %s', (_name, source) => {
      expect(sqlAdmissionSites({ file: sqlSite.file, source })).toEqual([]);
    });

    it('counts every SQL occurrence and scans files excluded only from JavaScript discovery', () => {
      const excluded = {
        file: NOT_GATE_FILES[0] as string,
        source: `${guardedPrepare}; admitsRunStart(reading, key);`,
      };
      expect(predicateCallSites(excluded)).toEqual([]);
      expect(sqlAdmissionSites(excluded)).toEqual([
        { ...sqlSite, file: excluded.file },
      ]);
      const sites = [
        { file: sqlSite.file, source: `${guardedPrepare}; ${guardedPrepare};` },
        { file: 'other/new-entry.ts', source: guardedPrepare },
      ].flatMap(sqlAdmissionSites);
      expect(sites.map(siteKey).sort()).toEqual(
        [sqlSite, sqlSite, { ...sqlSite, file: 'other/new-entry.ts' }]
          .map(siteKey)
          .sort(),
      );
    });

    it('loses the SQL site when its guard is removed despite unchanged diagnostic calls', () => {
      const original = {
        file: sqlSite.file,
        source: `${guardedPrepare}; admitsRunStart(reading, key);`,
      };
      const mutated = {
        ...original,
        source: original.source.replace(guard, 'WHERE 1 = 1'),
      };
      const calls = predicateCallSites(original);
      expect(predicateCallSites(mutated)).toEqual(calls);
      expect(sqlAdmissionSites(original)).toEqual([sqlSite]);
      expect(sqlAdmissionSites(mutated)).toEqual([]);
      const declared = [...calls, sqlSite].map(siteKey).sort();
      const observed = [
        ...predicateCallSites(mutated),
        ...sqlAdmissionSites(mutated),
      ]
        .map(siteKey)
        .sort();
      expect(observed).not.toEqual(declared);
      expect(siteKey(sqlSite)).not.toBe(siteKey(calls[0] as GateSite));
    });
  });

  it('rejects aliases, qualified construction, and subclassing census escapes', () => {
    const escapes = [
      "import { ExecutionFencedError as HiddenFenceError } from './do-runner/index.js';",
      'new fence.ExecutionFenceUnreadableError()',
      'class HiddenFenceError extends ExecutionFencedError {}',
      'class HiddenFenceError extends fence.ExecutionFencedError {}',
    ];

    expect(escapes.map(matchesFenceErrorCensusViolation)).toEqual([
      true,
      true,
      true,
      true,
    ]);
  });

  it('accounts for every production fence-error author, with a recorded pre-execution justification', () => {
    expect(
      fenceErrorCensusViolations(),
      'fence-error construction must stay visible to the lexical census',
    ).toEqual([]);
    const sites = fenceErrorAuthorSites();
    expect(sites).toHaveLength(FENCE_ERROR_AUTHORS.length);
    const anchored = new Map<
      (typeof sites)[number],
      (typeof FENCE_ERROR_AUTHORS)[number]
    >();
    for (const site of sites) {
      const candidates = FENCE_ERROR_AUTHORS.flatMap((author) => {
        if (author.file !== site.file || author.error !== site.error) return [];
        const distance = anchorDistanceBeforeAuthor(author, site);
        return distance === undefined ? [] : [{ author, distance }];
      });
      const nearest = Math.min(...candidates.map(({ distance }) => distance));
      const authors = candidates.filter(({ distance }) => distance === nearest);
      expect(
        authors.map(({ author }) => author.anchor),
        `${site.file}:${site.line} must have one declared anchor in the preceding ${ANCHOR_WINDOW_LINES} lines`,
      ).toHaveLength(1);
      const author = authors[0]?.author;
      if (author) anchored.set(site, author);
    }
    for (const author of FENCE_ERROR_AUTHORS) {
      const sitesForAnchor = sites.filter(
        (site) => anchored.get(site) === author,
      );
      expect(
        sitesForAnchor.map((site) => site.line),
        `${author.file} :: ${author.error} :: ${author.anchor} must anchor one author site`,
      ).toHaveLength(1);
      expect(
        author.beforeExecutionEffect.length,
        `${author.file} :: ${author.error} :: ${author.anchor} needs a substantive pre-execution justification`,
      ).toBeGreaterThan(40);
    }
  });

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
    const fs = sourceFileSystem();
    const here = (import.meta as ImportMeta & { url: string }).url;

    // #then
    for (const site of GATE_SITES) {
      if (site.drivenBy.endsWith('.test.ts')) {
        expect(
          fs.existsSync(new URL(site.drivenBy, here)),
          `${siteKey(site)} delegates to missing suite '${site.drivenBy}'`,
        ).toBe(true);
        continue;
      }
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
        const { fence, database, sqlite } = await openFence();
        const prepared = await entry.prepare(fence, database, sqlite);

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
      const { fence, database, sqlite } = await openFence();
      const prepared = await entry.prepare(fence, database, sqlite);
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
