// SPDX-License-Identifier: Apache-2.0
import { Mastra } from '@mastra/core';
import { Agent, createSignal } from '@mastra/core/agent';
import { MockMemory } from '@mastra/core/memory';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import {
  createStep,
  createWorkflow,
  type WorkflowRunState,
} from '@mastra/core/workflows';
import ts from 'typescript';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { durableKeyValueStorageFixture } from '../test-support/durable-key-value-storage.js';
import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../test-support/sqlite.js';
import { createFlowsafeDurableAgent } from './agent-runner/index.js';
import type { ActorContext, ApprovalActor } from './approval-api/index.js';
import {
  ApprovalService,
  D1ResourceOwnershipStore,
  InMemoryApprovalStore,
  type ResourceOwnershipDatabase,
} from './approval-api/index.js';
import { BackgroundTaskHost } from './background-tasks/index.js';
import type {
  D1StartExecutionIdentity,
  RunExecutionIdentity,
} from './do-runner/execution-admission.js';
import { RUN_PROVENANCE_CONTEXT_KEY } from './do-runner/execution-context.js';
import type {
  DurableObjectRunOwnershipStore,
  ExecutionFenceDatabase,
  ExecutionFenceReading,
  ExecutionFenceState,
  RunnerRuntime,
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
  type NotificationDeliveryStorage,
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

/** Which exported admission predicate an entry is declared under. */
type PredicateName =
  | 'admitsRunStart'
  | 'admitsExistingRun'
  | 'admitsWorkAuthoring'
  | 'admitsDrainableExecution';

/**
 * The declared predicate, evaluated on a real reading.
 *
 * `nomination` is what proof-only would have to name for this entry to be
 * admitted — an idempotency key for a mint, a complete generation for an existing
 * run — and it is `undefined` on the probe that does not carry it.
 */
function admits(
  predicate: PredicateName,
  reading: ExecutionFenceReading,
  nomination: string | RunExecutionIdentity | undefined,
): boolean {
  switch (predicate) {
    case 'admitsRunStart':
      return admitsRunStart(
        reading,
        typeof nomination === 'string' ? nomination : undefined,
      );
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
  readonly nomination?: string | D1StartExecutionIdentity;
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
  const database = deploymentIdentityDatabase(sqlite);
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
async function gatedRuntime(
  fence: ExecutionFenceStore,
  database: ExecutionFenceDatabase,
): Promise<RunnerRuntime> {
  const storage = createD1Storage({ binding: database });
  await storage.init();
  const {
    createWorkflow: workflowFactory,
    createStep: stepFactory,
    runtime,
  } = init(
    { storage },
    {
      executionFence: fence,
      startIdempotency: new StartIdempotencyStore(database),
    },
  );
  const gate = stepFactory({
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
  workflowFactory({
    id: 'gated',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(gate)
    .commit();
  return runtime;
}

/** A D1 double carrying the deployment sentinel the DO hosts verify against. */
function deploymentIdentityDatabase(
  sqlite = openSqlite(),
): ExecutionFenceDatabase {
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
  return sqliteUnitDatabase(sqlite) as ExecutionFenceDatabase;
}

interface RunnerEnv {
  runtime: RunnerRuntime;
  owners: DurableObjectRunOwnershipStore;
  DEPLOYMENT_TENANT: string;
  DEPLOYMENT_IDENTITY_SECRET: string;
  DB: ExecutionFenceDatabase;
}

/** The production run-object host, over the same D1 ownership and run domains. */
class MatrixRunner extends DurableObjectRunner<RunnerEnv> {
  protected runOwnership(env: RunnerEnv): DurableObjectRunOwnershipStore {
    return env.owners;
  }

  protected runLifecycle(): { abandonApprovals: () => Promise<void> } {
    return { abandonApprovals: async () => undefined };
  }

  protected build(env: RunnerEnv): RunnerRuntime {
    return env.runtime;
  }
}

async function matrixRunner(
  fence: ExecutionFenceStore,
  database: ExecutionFenceDatabase,
  runId: string,
): Promise<{ runner: MatrixRunner; runtime: RunnerRuntime }> {
  const runtime = await gatedRuntime(fence, database);
  const journal = durableKeyValueStorageFixture();
  const runner = new MatrixRunner(
    { id: { name: `gated:${runId}` }, storage: journal.storage },
    {
      runtime,
      owners: new D1ResourceOwnershipStore(
        database as unknown as ResourceOwnershipDatabase,
      ),
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: TEST_IDENTITY_SECRET,
      DB: database,
    },
  );
  expect(journal.alarms).toEqual([]);
  return { runner, runtime };
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

async function schedulesDomain(
  database?: ExecutionFenceDatabase,
): Promise<D1SchedulesStorage> {
  const store = new D1SchedulesStorage(
    (database ?? sqliteUnitDatabase(openSqlite())) as ScheduleDatabase,
  );
  await store.init();
  return store;
}

const TARGET_POLICY = createScheduleTargetPolicy({
  workflows: [{ id: 'wf' }],
  agents: [],
});

async function existingExecution(
  runtime: RunnerRuntime,
  workflowId: string,
  runId: string,
): Promise<D1StartExecutionIdentity> {
  const state = await runtime.authoritativeStartState(workflowId, runId);
  if (
    state?.storage !== 'd1' ||
    state.kind !== 'result' ||
    !state.provenance.startIdentity
  )
    throw new Error('matrix requires an actual owned D1 result');
  return { ...state.execution, ...state.provenance.startIdentity };
}

async function nominateExistingExecution(
  fence: ExecutionFenceStore,
  database: ExecutionFenceDatabase,
  execution: D1StartExecutionIdentity,
): Promise<void> {
  const originalRound = await fence.read();
  const store = new StartIdempotencyStore(database);
  const { reservation } = await store.reserve({
    key: PROOF_KEY,
    owner: execution.owner,
    targetKind: execution.target.kind,
    targetId: execution.target.id,
    ...(execution.target.kind === 'agent'
      ? { threadId: execution.target.threadId }
      : {}),
    mintRunId: () => execution.runId,
  });
  const bound = await store.associateReservation(reservation, execution);
  expect(
    await fence.rebindProofRun({
      reservation: bound,
      execution,
      proof: {
        key: PROOF_KEY,
        mutationEpoch: originalRound.mutationEpoch,
        transitionRevision: originalRound.transitionRevision,
      },
      mutationEpoch: originalRound.mutationEpoch,
      reservationStore: store,
    }),
  ).toBe(true);
  expect((await fence.read()).proofExecution).toEqual({
    tablePrefix: execution.tablePrefix,
    workflowId: execution.workflowId,
    runId: execution.runId,
    startToken: execution.startToken,
  });
}

async function matrixAgent(
  fence: ExecutionFenceStore,
  database: ExecutionFenceDatabase,
  activeRunId: string,
) {
  const storage = createD1Storage({ binding: database });
  await storage.init();
  const pubsub = createHostPubSub();
  const runner = init(
    { storage },
    {
      pubsub,
      executionFence: fence,
      startIdempotency: new StartIdempotencyStore(database),
    },
  );
  const agent = createFlowsafeDurableAgent({
    agent: new Agent({
      id: 'agent',
      name: 'Matrix agent',
      instructions: 'Matrix delivery fixture.',
      model: {
        specificationVersion: 'v2',
        provider: 'matrix',
        modelId: 'unreachable',
        supportedUrls: {},
        doGenerate: async () => {
          throw new Error('matrix must not invoke a model');
        },
        doStream: async () => {
          throw new Error('matrix must not invoke a model');
        },
      },
      memory: new MockMemory(),
    }),
    runtime: runner.runtime,
    pubsub,
    cache: false,
  });
  const workflowId = agent.getWorkflow().id;
  const domain = await storage.getStore('workflows');
  if (!domain) throw new Error('matrix workflow domain is missing');
  await domain.persistWorkflowSnapshot({
    workflowName: workflowId,
    runId: activeRunId,
    snapshot: {
      runId: activeRunId,
      status: 'suspended',
      context: {},
      requestContext: {
        [RUN_PROVENANCE_CONTEXT_KEY]: {
          version: 2,
          startToken: crypto.randomUUID(),
          attemptToken: crypto.randomUUID(),
          requestedBy: 'operator',
          requestedByKind: 'human',
          startIdentity: {
            owner: { kind: 'human', id: 'operator' },
            target: { kind: 'agent', id: 'agent', threadId: THREAD_ID },
          },
          agentStart: { threaded: true },
          resumeCounts: [],
        },
      },
      activePaths: [],
      activeStepsPath: {},
      serializedStepGraph: [],
      suspendedPaths: {},
      waitingPaths: {},
      resumeLabels: {},
      value: {},
      timestamp: Date.now(),
    } as WorkflowRunState,
  });
  const delivered = {
    signal: createSignal({ id: 's', type: 'reactive', contents: 'nudge' }),
    accepted: Promise.resolve({
      action: 'deliver' as const,
      runId: activeRunId,
    }),
  };
  vi.spyOn(agent, 'getActiveThreadRunId').mockReturnValue(activeRunId);
  const delivery = vi.spyOn(agent, 'sendSignal').mockReturnValue(delivered);
  const nomination = await existingExecution(
    runner.runtime,
    workflowId,
    activeRunId,
  );
  expect(
    await agent.proofExecutionFor(runner.runtime, THREAD_ID, activeRunId),
  ).toEqual({
    tablePrefix: nomination.tablePrefix,
    workflowId,
    runId: activeRunId,
    startToken: nomination.startToken,
  });
  return {
    agent,
    nomination,
    delivery,
    scope: {
      threadId: THREAD_ID,
      principal: {
        kind: 'human' as const,
        id: 'operator',
        role: 'operator' as const,
      },
      init: runner,
    },
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
      const reserved = await reservationStore.reserve({
        key: PROOF_KEY,
        owner: startIdentity.owner,
        targetKind: 'workflow',
        targetId: workflowId,
        mintRunId: () => runId,
      });
      const reservation = await reservationStore.claimReservation(
        reserved.reservation,
      );
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
    prepare: async (fence, database) => {
      const runtime = await gatedRuntime(fence, database);
      return {
        nomination: PROOF_KEY,
        invoke: (carry) =>
          classify(() =>
            runtime.start('gated', {
              runId: nextRunId(),
              inputData: {},
              requestedBy: 'owner-1',
              requestedByKind: 'human',
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
    prepare: async (fence, database) => {
      const runtime = await gatedRuntime(fence, database);
      const runId = nextRunId();
      await runtime.start('gated', {
        runId,
        inputData: {},
        requestedBy: 'owner-1',
        requestedByKind: 'human',
      });
      return {
        nomination: await existingExecution(runtime, 'gated', runId),
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
    prepare: async (fence, database) => {
      const runId = nextRunId();
      const { runner } = await matrixRunner(fence, database, runId);
      return {
        nomination: PROOF_KEY,
        invoke: (carry) =>
          classify(() =>
            runner.fetch(
              runnerRequest('/runs', {
                workflowId: 'gated',
                runId,
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
    prepare: async (fence, database) => {
      const runId = nextRunId();
      const { runner, runtime } = await matrixRunner(fence, database, runId);
      expect(
        await classify(() =>
          runner.fetch(
            runnerRequest('/runs', {
              workflowId: 'gated',
              runId,
              inputData: {},
            }),
          ),
        ),
      ).toBe('admitted');
      return {
        nomination: await existingExecution(runtime, 'gated', runId),
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
    prepare: async (fence, database) => {
      const runId = nextRunId();
      const runtime = await gatedRuntime(fence, database);
      await runtime.start('gated', {
        runId,
        inputData: {},
        requestedBy: 'owner-1',
        requestedByKind: 'human',
      });
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
        requestedBy: 'owner-1',
        requestedByKind: 'human',
        createdAt: at,
        updatedAt: at,
      });
      const service = new ApprovalService({
        store,
        executionFence: fence,
        workflowTablePrefix: '',
      });
      return {
        nomination: await existingExecution(runtime, 'gated', runId),
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
    prepare: async (fence, database) => {
      const runId = nextRunId();
      const { agent, nomination, delivery, scope } = await matrixAgent(
        fence,
        database,
        runId,
      );
      const routes = createThreadSignalRoutes({
        resolveAgent: () => agent as unknown as Agent,
        resolveResourceId: () => THREAD_ID,
      });
      return {
        nomination,
        invoke: async () => {
          const outcome = await classify(() =>
            routes(
              new Request('http://thread/signal', {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify({ contents: 'nudge' }),
              }),
              scope,
            ),
          );
          expect(delivery).toHaveBeenCalledTimes(
            outcome === 'admitted' ? 1 : 0,
          );
          return outcome;
        },
      };
    },
  },
  {
    name: 'schedule router create',
    module: 'schedules/router.ts — authoring a standing fire',
    predicate: 'admitsWorkAuthoring',
    prepare: async (fence, database) => {
      const router = createScheduleRouter({
        resolve: async () => actorContext(),
        store: await schedulesDomain(database),
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
          // A fenced pass does NOTHING — it never reaches the CAS. The tick
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
        getNotification: async () => {
          throw new Error('unexpected notification readback');
        },
        updateNotificationDeliveryIfUnchanged: async () => {
          throw new Error('unexpected notification failure write');
        },
      } as unknown as NotificationDeliveryStorage;
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
      // operator drains it, so the gate this entry drives is the enqueue's own.
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

type SourceFileSystem = {
  readFileSync(path: string, encoding: string): string;
};

/**
 * Node builtins load through getBuiltinModule, without adding a direct Node
 * ambient-type requirement to this test.
 */
function sourceFileSystem(): SourceFileSystem {
  return (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule?.('node:fs') as SourceFileSystem;
}

type SourceUrlModule = { fileURLToPath(url: URL): string };

function sourceRoot(): string {
  // This module's OWN directory, never `process.cwd()`: filtered and root test
  // runs use different working directories.
  const here = (import.meta as ImportMeta & { url: string }).url;
  const url = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule?.('node:url') as SourceUrlModule;
  return url.fileURLToPath(new URL('.', here)).replace(/\/$/, '');
}

describe('execution-entry matrix', () => {
  it.each([
    'do-runner/runtime.ts',
    'do-runner/durable-object.ts',
    'do-runner/thread-do.ts',
    'agent-host/thread-host.ts',
    'agent-runner/durable-agent-runner.ts',
  ])('execution entry has no weak reservation or proof calls: %s', (file) => {
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
      'rollbackFencedStart',
      'recordProofRun',
      'settleRun',
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
      // #given — the probe that separates the predicate PAIRS. Outside
      // proof-only, admitsRunStart is indistinguishable from
      // admitsWorkAuthoring and admitsExistingRun from admitsDrainableExecution;
      // only the nominated proof-only case tells them apart.
      const { fence, database, sqlite } = await openFence();
      const prepared = await entry.prepare(fence, database, sqlite);
      await fence.transition({
        expected: 'open',
        next: 'proof-only',
        proofKey: PROOF_KEY,
      });
      if (typeof prepared.nomination === 'object') {
        await nominateExistingExecution(fence, database, prepared.nomination);
      }
      const reading = await fence.read();
      const expected = admits(entry.predicate, reading, prepared.nomination)
        ? 'admitted'
        : 'refused';

      // #then — an entry with a nomination is admitted when it carries it; one
      // without is refused however it is driven, which is the whole meaning of
      // "nothing nominates authoring or queued execution".
      expect(await prepared.invoke(true)).toBe(expected);
      expect(expected).toBe(
        prepared.nomination === undefined ? 'refused' : 'admitted',
      );
    });
  }
});
