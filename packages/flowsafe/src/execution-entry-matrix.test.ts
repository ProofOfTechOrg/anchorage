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
//                             nominated physical generation.
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
import type { DurableKeyValueStorage } from './do-runner/cf-types.js';
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
 * admitted — an idempotency key for a mint, a complete generation for an existing
 * run — and it is `undefined` on the probe that deliberately does not carry it.
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
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    {
      executionFence: fence,
      startIdempotency: new StartIdempotencyStore(database),
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
  const values = new Map<string, unknown>();
  let alarm: number | undefined;
  const storage: DurableKeyValueStorage = {
    get: async <T>(key: string) =>
      structuredClone(values.get(key)) as T | undefined,
    put: async (key, value) => {
      values.set(key, structuredClone(value));
    },
    delete: async (key) => values.delete(key),
    setAlarm: async (at) => {
      alarm = Number(at);
    },
    deleteAlarm: async () => {
      alarm = undefined;
    },
  };
  const runner = new MatrixRunner(
    { id: { name: `gated:${runId}` }, storage },
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
  expect(alarm).toBeUndefined();
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
 * `drivenBy` names a matrix entry above wherever one exists. Delegated suites
 * exercise the background-task host's private dispatch paths, the serialized
 * wake lane, and proof nomination's reservation/snapshot/readback checks.
 * Their boundary-specific states and races live in the named test files.
 */
type GateSite = {
  file: string;
  predicate: PredicateName;
  sql?: 'initial-snapshot-insert';
  delegate?: 'assertExistingRunAllowed' | 'proof.capture';
};

const GATE_SITES: ReadonlyArray<GateSite & { drivenBy: string }> = [
  {
    file: 'do-runner/execution-fence.ts',
    predicate: 'admitsExistingRun',
    // Captured reservation binding agrees with the requested proof generation.
    drivenBy: 'do-runner/execution-fence.test.ts',
  },
  {
    file: 'do-runner/execution-fence.ts',
    predicate: 'admitsExistingRun',
    // Bound current snapshot generation agrees with its reservation.
    drivenBy: 'do-runner/execution-fence.test.ts',
  },
  {
    file: 'do-runner/execution-fence.ts',
    predicate: 'admitsExistingRun',
    // A previously nominated generation agrees with this replay.
    drivenBy: 'do-runner/execution-fence.test.ts',
  },
  {
    file: 'do-runner/execution-fence.ts',
    predicate: 'admitsExistingRun',
    // Nomination RETURNING and response-loss convergence preserve the tuple.
    drivenBy: 'do-runner/execution-fence.test.ts',
  },
  {
    file: 'approval-api/service.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'ApprovalService.decide',
  },
  {
    file: 'approval-api/service.ts',
    predicate: 'admitsExistingRun',
    drivenBy: 'ApprovalService.decide',
  },
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
    delegate: 'assertExistingRunAllowed',
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
    file: 'schedules/schedules-d1.ts',
    predicate: 'admitsWorkAuthoring',
    drivenBy: './schedules/schedules-d1.test.ts',
  },
  {
    file: 'schedules/schedules-d1.ts',
    predicate: 'admitsWorkAuthoring',
    drivenBy: './schedules/schedules-d1.test.ts',
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
    // Retained generation after application awaits.
    drivenBy: 'thread object POST /signal',
  },
  {
    file: 'signals/thread-do-routes.ts',
    predicate: 'admitsExistingRun',
    delegate: 'proof.capture',
    // The serialized wake captures its own current generation.
    drivenBy: 'signals/thread-do-routes.test.ts',
  },
];

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
 * Every actual predicate/delegation call. Definitions, re-exports and comments
 * are not calls; the defining module's own nomination gates remain visible.
 *
 * The filesystem reader keeps the schema guard's getBuiltinModule idiom,
 * without adding a direct Node ambient-type requirement to this test.
 */
function predicateCallSites({ file, source }: SourceFile): GateSite[] {
  const found: GateSite[] = [];
  const parsed = ts.createSourceFile(
    file,
    source,
    ts.ScriptTarget.Latest,
    true,
  );
  const visit = (node: ts.Node): void => {
    if (ts.isCallExpression(node)) {
      const expression = node.expression;
      const name = ts.isIdentifier(expression)
        ? expression.text
        : ts.isPropertyAccessExpression(expression)
          ? expression.name.text
          : undefined;
      if (
        name &&
        /^admits(?:RunStart|ExistingRun|WorkAuthoring|DrainableExecution)$/.test(
          name,
        )
      ) {
        found.push({ file, predicate: name as PredicateName });
      } else if (name === 'assertExistingRunAllowed') {
        found.push({ file, predicate: 'admitsExistingRun', delegate: name });
      } else if (
        name === 'capture' &&
        ts.isPropertyAccessExpression(expression) &&
        ts.isPropertyAccessExpression(expression.expression) &&
        expression.expression.name.text === 'proof'
      ) {
        found.push({
          file,
          predicate: 'admitsExistingRun',
          delegate: 'proof.capture',
        });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(parsed);
  return found;
}

function sqlAdmissionSites({ file, source }: SourceFile): GateSite[] {
  const parsed = ts.createSourceFile(
    file,
    source,
    ts.ScriptTarget.Latest,
    true,
  );
  const found: GateSite[] = [];
  const admissionBindings = new Set<string>();
  const collectBindings = (node: ts.Node): void => {
    if (
      ts.isVariableDeclaration(node) &&
      ts.isIdentifier(node.name) &&
      node.initializer &&
      ts.isCallExpression(node.initializer) &&
      ts.isIdentifier(node.initializer.expression) &&
      node.initializer.expression.text === 'executionFenceAdmissionSql'
    ) {
      admissionBindings.add(node.name.text);
    }
    ts.forEachChild(node, collectBindings);
  };
  collectBindings(parsed);
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
        const sharedGuard = /\bWHERE\s+\$\{\s*([\w$]+)\s*\}/i.exec(sql)?.[1];
        if (
          /^\s*INSERT\s+INTO\b/i.test(sql) &&
          (/\bWHERE\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+(?:flowsafe_execution_fence|\$\{EXECUTION_FENCE_TABLE\})\s+AS\s+f\b/i.test(
            sql,
          ) ||
            (sharedGuard !== undefined && admissionBindings.has(sharedGuard)))
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
 * failure. Each row states what the error refuses and what may already have
 * happened. Reads and cleanup can fail after execution or durable writes;
 * these errors alone establish neither quiescence nor no-insert authority. The scan makes a new author
 * fail until its boundary is reviewed and recorded here. It is lexical:
 * a constructor spelling in a comment or string fails loud and asks for review.
 * Aliased class names and namespace imports are forbidden so lexical coverage
 * cannot be bypassed without first changing this test.
 */
const FENCE_ERROR_AUTHORS: ReadonlyArray<{
  file: string;
  error: FenceErrorName;
  anchor: string;
  effectBoundary: string;
}> = [
  {
    file: 'do-runner/execution-admission.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const current = candidate?.mutationEpoch;',
    effectBoundary:
      'The public epoch helper rejects malformed reading metadata before returning an admission comparison; it performs no execution.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'state.execution.owner.id !== expected.principal.id',
    effectBoundary:
      'An immutable selected owner or target mismatch refuses terminal cleanup before settlement or record deletion; the run may already have executed.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'record(state.snapshot.context?.input)?.agentId !== ref.agentId',
    effectBoundary:
      'A nonterminal or mismatched legacy observation cannot authorize the next cleanup effect or canonical record deletion; earlier authorized execution may already be durable.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const [binding, current, journal] = await Promise.all([',
    effectBoundary:
      'A journal observed before or between legacy cleanup effects blocks further cleanup; earlier completed lifecycle effects are not rolled back and the journal remains authoritative.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor:
      'if (recovery.startReservation && !scope.init.runtime.startIdempotency)',
    effectBoundary:
      'Keyed finalization requires the configured store before owner bookkeeping or journal clearing, including nonterminal results; it grants no new execution authority.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const finalizeTerminalAgentState = async (',
    effectBoundary:
      'A legacy terminal observation cannot clear an existing recovery journal or its canonical record through ordinary cleanup, even after a successful resumed operation.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor:
      'if (stored.startReservation && !scope.init.runtime.startIdempotency)',
    effectBoundary:
      'Captured keyed recovery refuses missing store wiring before any phase can settle H-owned bookkeeping or erase its original claim journal.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const journal = await options',
    effectBoundary:
      'After a legacy termination transition, a present journal blocks ordinary cleanup rather than inventing modern settlement authority; the durable transition may already have completed.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'cleanup.scheduleDispatch?.dispatchId !==',
    effectBoundary:
      'A selected legacy cleanup descriptor must agree with the authorized transition before approval, dispatch, owner or completion effects; a mismatched observation cannot retarget cleanup.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async #finishRunOwner(',
    effectBoundary:
      'Keyed workflow finalization refuses a missing reservation store before settlement, owner bookkeeping and journal clearing, even when the selected outcome is nonterminal.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async #recoverRunOwner(',
    effectBoundary:
      'Every workflow journal phase requires its configured claim store before recovery bookkeeping or Runtime recovery; this refusal retains the journal and grants no rollback.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "if (stored.phase === 'prepared' && !localZero()) {",
    effectBoundary:
      'Prepared absence without local zero evidence retains the journal and record after H-only rollback; it cannot authorize another engine entry or prove earlier effects absent.',
  },
  {
    file: 'agent-host/thread-host.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor:
      'await options.resourceAccess().settleReservation(stored.token, release);',
    effectBoundary:
      'The final local-zero check refuses journal deletion if owning evidence changed during bookkeeping; completed rollback operations do not prove earlier execution absent.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "if (recovery.phase === 'prepared' && !localZero()) {",
    effectBoundary:
      'Prepared absence retains the workflow journal after exact H rollback unless the current own catch proves a matching zero insert; unknown earlier effects never permit a retry.',
  },
  {
    file: 'approval-api/service.ts',
    error: 'ExecutionFencedError',
    anchor: 'async #assertDecidable',
    effectBoundary:
      'The admission check runs before decide() mutates the approval or resumes its run.',
  },
  {
    file: 'approval-api/service.ts',
    error: 'ExecutionFencedError',
    anchor: "if (fence === 'none')",
    effectBoundary:
      'A retained proof expectation without its fence refuses before committing an approval decision or resuming execution.',
  },
  {
    file: 'approval-api/service.ts',
    error: 'ExecutionFencedError',
    anchor: 'const current = await fence.readCurrentRunExecution(execution);',
    effectBoundary:
      'After approval and separation-of-duty reads, the original generation must still match before the decision CAS or resume.',
  },
  {
    file: 'background-tasks/host.ts',
    error: 'ExecutionFencedError',
    anchor: '#gated(executor',
    effectBoundary:
      'The executor backstop refuses before calling a tool body when core supplies no suspension seam.',
  },
  {
    file: 'background-tasks/host.ts',
    error: 'ExecutionFencedError',
    anchor: 'async enqueue(',
    effectBoundary:
      'The enqueue admission check runs before the manager creates a queued task row.',
  },
  {
    file: 'do-runner/durable-object.ts',
    error: 'ExecutionFencedError',
    anchor: 'const startFence =',
    effectBoundary:
      'The authenticated start preflight refuses before recovery journalling, owner reservation, or Runtime execution admission.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFencedError',
    anchor: 'export function executionFencedResponse',
    effectBoundary:
      'The response helper only serializes an already-decided refusal and performs no execution effect.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const execution = normalizeD1RunExecutionIdentity({',
    effectBoundary:
      'Malformed selected proof snapshots cannot supply generation authority; this reader performs no writes or engine entry.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'returned.raw.proof_start_token !== null',
    effectBoundary:
      'A legacy proof setter cannot acknowledge modern generation metadata; this post-write decoder grants no execution or rollback authority.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'returned.raw.updated_at !== expectedTime',
    effectBoundary:
      'Unexpected nomination RETURNING data refuses acknowledgement after a possible metadata write; it never enters an engine or proves no effects.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (validateReturned(converged)) return true;',
    effectBoundary:
      'A lost nomination response without exact single-query convergence remains unreadable; the possible metadata write cannot authorize engine entry.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (cause instanceof ExecutionFenceUnreadableError) throw cause;',
    effectBoundary:
      'The guarded nomination boundary preserves unreadable snapshot, reservation or fence observations without running execution or manufacturing write absence.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async readForAdmission():',
    effectBoundary:
      'The pure current-schema observation rejects missing or malformed metadata before engine entry; diagnostic/readback callers may follow a durable initial admission, so this error alone proves no absence.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (stored.schemaStage > stage)',
    effectBoundary:
      'Fence-row validation fails closed before any caller can admit execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'async #initialize(',
    effectBoundary:
      'Initialization validates administrative metadata without admitting execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: '#decodeReturned(result:',
    effectBoundary:
      'A malformed metadata-write result refuses before a caller can admit execution.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (receipt !== null)',
    effectBoundary:
      'An uncertain administrative CAS does not execute a run or schedule.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "reading?.state === 'proof-only'",
    effectBoundary:
      'A failed proof-binding metadata write becomes unreadable before the runtime starts the run.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'function terminalizationUnreadable(',
    effectBoundary:
      'Explicit initial-row terminalization never enters an engine or grants no-insert authority; malformed input observations or uncertain terminal writes refuse through this fixed operation boundary without replay.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'const counts = decodeResumeCounts(resumeCounts);',
    effectBoundary:
      'The progress decoder validates only owned metadata and performs no I/O or execution. A retained admission stamp never proves unchanged bytes or no effects, and decoding failures cannot grant definitive-zero evidence.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (!scope.witness) {',
    effectBoundary:
      'A createRun callback without a positive persistence witness cannot enter the engine; another domain or swallowed failure may already have written, so missing witness gives no definitive-zero authority.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (await this.#converged(',
    effectBoundary:
      'A thrown batch with no exact converged readback blocks engine entry; the batch may already have committed durable admission and this uncertain refusal cannot authorize retry or journal clearing.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: '} else if (proofRows.length !== 0)',
    effectBoundary:
      'Malformed returned batch data blocks engine entry after a possible committed initial INSERT; no recovery read or missing-result assumption upgrades it to success or definitive zero.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFencedError',
    anchor: 'const proofSlotUnbound =',
    effectBoundary:
      'After a validated all-zero chained batch, the current state/key/round diagnostic explains refusal before engine entry; the SQL result, not this JavaScript predicate, establishes no initial write.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: "error.reason?.code === 'MUTATION_EPOCH_MISMATCH'",
    effectBoundary:
      'An unreadable post-zero diagnostic blocks engine entry while preserving the already validated all-zero result; failed observation alone would not establish absence of durable admission.',
  },
  {
    file: 'do-runner/workflow-snapshot-row.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'export async function readRawWorkflowSnapshot(',
    effectBoundary:
      'The exact reader performs no writes and refuses malformed or unavailable rows before its caller enters the engine; admission readback may follow an already committed initial row and does not prove no write.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'export function decodeRunStartIdentity(',
    effectBoundary:
      'The role-neutral provenance decoder performs no storage or execution and rejects malformed owned identity before callers can use it to authorize engine entry.',
  },
  {
    file: 'do-runner/run-provenance.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'resumeCounts: Object.freeze([]) as readonly [],',
    effectBoundary:
      'The initial provenance decoder validates before engine entry; callers can use it on a returned or read-back initial row, so decoder failure does not establish absence of durable admission.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: 'if (!admitsRunStart(reading, idempotencyKey))',
    effectBoundary:
      'The start admission check refuses before proof binding and engine run creation.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: "if (reading.state === 'migration-locked')",
    effectBoundary:
      'The Runtime refuses every migration-locked resume before engine preparation or execution.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor:
      'const state = await this.authoritativeStartState(workflowId, runId);',
    effectBoundary:
      'The resume admission check refuses before the engine continues the existing run.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFencedError',
    anchor: '(proof && (!execution || !sameExecution(proof, execution)))',
    effectBoundary:
      'After preparation waits, the original generation and current admission must still agree before synchronous engine resume.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (claim && !this.#startIdempotency)',
    effectBoundary:
      'Missing reservation wiring refuses owning recovery before selecting or terminalizing the pending generation; the journal remains unresolved.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'cause instanceof RunStateUnreadableError',
    effectBoundary:
      'Owning recovery failures remain unresolved after possible terminalization or settlement; recovery never runs an engine and this error grants no rollback.',
  },
  {
    file: 'do-runner/runtime.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'if (!identity || !this.#startIdempotency)',
    effectBoundary:
      'Strict terminal settlement requires the original logical owner and configured store before managed hosts can clear journals or ownership.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: 'runtime.executionFence !== scope.init.executionFence',
    effectBoundary:
      'Proof-only signal delivery requires an actual Runtime-driven wrapper and the same fence before any route effect.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: 'if (runId === undefined)',
    effectBoundary:
      'An idle thread cannot claim existing proof execution or persist a new signal through the active-run route.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: '!admitsExistingRun(executionFence, execution)',
    effectBoundary:
      'The actual wrapper generation must match the nominated physical execution before delivery or durable signal mutation.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: 'const assertActive = (expected = admitted) => {',
    effectBoundary:
      'An immediate active-run comparison prevents Core from selecting another run after an awaited generation check and before route effects.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: "{ state: 'proof-only', proofExecution: expected },",
    effectBoundary:
      'A replacement generation after application awaits refuses the next signal effect while retaining the original admitted execution.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: "if (options.executionFence.state === 'proof-only')",
    effectBoundary:
      'A fenced wake response propagates before notification failure bookkeeping; prior admitted effects are not claimed absent or rolled back.',
  },
  {
    file: 'signals/thread-do-routes.ts',
    error: 'ExecutionFencedError',
    anchor: "if (fence.state === 'proof-only' && admitted === undefined)",
    effectBoundary:
      'Serialized wake requires its captured current generation before active delivery or any idle persistence and start path.',
  },
  {
    file: 'signal-providers/host-do.ts',
    error: 'ExecutionFencedError',
    anchor: 'async poll(): Promise<PollResult>',
    effectBoundary:
      'The poll admission check refuses before any provider is polled or notification is delivered.',
  },
  {
    file: 'do-runner/execution-fence.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor:
      'const values = FENCE_ADMISSION_FIELDS.map((key) => observation.raw[key]);',
    effectBoundary:
      'Semantic field types are checked before initial admission and schedule binding; schedule transaction diagnosis preserves unknown outcomes after a positive mutation witness.',
  },
  {
    file: 'do-runner/fenced-workflows-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'schema = await captureExecutionFenceAdmissionSchema(',
    effectBoundary:
      'Unreadable current schema refuses initial admission before its snapshot and dependent reservation or proof writes.',
  },
  {
    file: 'schedules/schedules-d1.ts',
    error: 'ExecutionFencedError',
    anchor: '!admitsWorkAuthoring(observation.reading)',
    effectBoundary:
      'Closed authoring state refuses schedule preparation before the mutation batch; the final SQL predicate independently checks the captured frame.',
  },
  {
    file: 'schedules/schedules-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'results: captured.rows,',
    effectBoundary:
      'Unreadable schema evidence refuses preparation before authoring SQL; the captured valid schema is compared again by the mutation predicate.',
  },
  {
    file: 'schedules/schedules-d1.ts',
    error: 'ExecutionFenceUnreadableError',
    anchor: 'results: schemaResult.rows,',
    effectBoundary:
      'Invalid transactional authority is a refusal with zero write evidence; a positive mutation witness instead reports an unknown outcome without compensation.',
  },
  {
    file: 'schedules/schedules-d1.ts',
    error: 'ExecutionFencedError',
    anchor: '!admitsWorkAuthoring(current.reading)',
    effectBoundary:
      'The transaction observation diagnoses final state refusal when the guarded writes have no witness; contradictory positive writes become an unknown outcome.',
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
  return `${site.file} :: ${site.predicate} :: ${site.sql ?? site.delegate ?? 'predicate'}`;
}

describe('execution-entry matrix', () => {
  it.each([
    'do-runner/runtime.ts',
    'do-runner/durable-object.ts',
    'do-runner/thread-do.ts',
    'agent-host/thread-host.ts',
    'agent-runner/durable-agent-runner.ts',
  ])('D3 execution entry has no weak reservation or proof calls: %s', (file) => {
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

  describe('predicate admission source census', () => {
    const file = 'entry.ts';
    it.each([
      ['admitsExistingRun(reading, execution)', undefined],
      ['fence.admitsExistingRun(reading, execution)', undefined],
      [
        'runtime.assertExistingRunAllowed(workflowId, runId)',
        'assertExistingRunAllowed',
      ],
      ['options.proof?.capture(runId)', 'proof.capture'],
    ] as const)('finds the actual call: %s', (source, delegate) => {
      expect(predicateCallSites({ file, source })).toEqual([
        {
          file,
          predicate: 'admitsExistingRun',
          ...(delegate === undefined ? {} : { delegate }),
        },
      ]);
    });

    it.each([
      '// admitsExistingRun(reading, execution)',
      '/* admitsExistingRun(reading, execution) */',
      '"admitsExistingRun(reading, execution)"',
      'function admitsExistingRun(reading, execution) {}',
      'const gate = admitsExistingRun;',
      'options.capture(runId)',
    ])('does not invent a gate from %s', (source) => {
      expect(predicateCallSites({ file, source })).toEqual([]);
    });
  });

  describe('SQL admission source census', () => {
    const fenceTable = `\${EXECUTION_FENCE_TABLE}`;
    const insert = `INSERT INTO \${snapshotTable}`;
    const guard = `WHERE EXISTS (SELECT 1 FROM ${fenceTable} AS f WHERE f.state = 'open')`;
    const guardedPrepare = `database.prepare(\`${insert} SELECT 1 ${guard} \${optionalParticipantClauses}\`)`;
    const sharedPrepare = `const finalFence = executionFenceAdmissionSql(input); database.prepare(\`${insert} SELECT 1 WHERE \${finalFence} \${optionalParticipantClauses}\`)`;
    const sqlSite: GateSite = {
      file: 'do-runner/fenced-workflows-d1.ts',
      predicate: 'admitsRunStart',
      sql: 'initial-snapshot-insert',
    };

    it.each([
      ['template with participant interpolation', guardedPrepare],
      ['shared admission predicate', sharedPrepare],
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
      [
        'unused shared predicate',
        sharedPrepare.replace(`WHERE \${finalFence}`, 'WHERE 1 = 1'),
      ],
      [
        'unrecognized predicate binding',
        sharedPrepare.replace('executionFenceAdmissionSql', 'otherSql'),
      ],
    ])('does not invent a SQL gate from %s', (_name, source) => {
      expect(sqlAdmissionSites({ file: sqlSite.file, source })).toEqual([]);
    });

    it('counts every SQL occurrence independently of predicate declarations', () => {
      const definition = {
        file: 'do-runner/execution-fence.ts',
        source: `${guardedPrepare}; export function admitsRunStart(reading, key) {}`,
      };
      expect(predicateCallSites(definition)).toEqual([]);
      expect(sqlAdmissionSites(definition)).toEqual([
        { ...sqlSite, file: definition.file },
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

  it('accounts for every production fence-error author, with a recorded effect-boundary justification', () => {
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
        author.effectBoundary.length,
        `${author.file} :: ${author.error} :: ${author.anchor} needs a substantive effect-boundary justification`,
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
