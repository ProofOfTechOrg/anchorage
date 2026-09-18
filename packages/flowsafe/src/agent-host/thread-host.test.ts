// SPDX-License-Identifier: Apache-2.0

import type { MastraCompositeStore } from '@mastra/core/storage';
import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { beforeEach, describe, expect, it, onTestFinished, vi } from 'vitest';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  AgentRunSelectorMismatchError,
  FlowsafeDurableAgent,
} from '../agent-runner/durable-agent-runner.js';
import {
  type ApprovalAuditEvent,
  type ApprovalRecord,
  type ApprovalService,
  D1ResourceOwnershipStore,
  type ExecutionPrincipal,
  InMemoryResourceOwnershipStore,
  type RecoverableResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/index.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
} from '../do-runner/fenced-workflow-capability.js';
import type { FencedWorkflowsStorageD1 } from '../do-runner/fenced-workflows-d1.js';
import type {
  InitResult,
  RequestContextProvider,
  RunnerRuntime,
  RunSummary,
  ScheduleSourceStore,
  ThreadScope,
} from '../do-runner/index.js';
import {
  createD1Storage,
  doErrorResponse,
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
  init,
  RunStateUnreadableError,
  resourceIdFromKey,
  SUSPENSION_TIMEOUT_RESUME_KEY,
} from '../do-runner/index.js';
import {
  D1SchedulesStorage,
  type ScheduleDatabase,
} from '../schedules/schedules-d1.js';
import {
  type AgentThreadInstanceScope,
  type AgentThreadStateStorage,
  type AutomatedEntryAuthorizer,
  createThreadAgentHost,
  type PrincipalPermissionResolver,
  type ThreadAgentStartInput,
} from './thread-host.js';
import { createAgentThreadTopology } from './thread-topology.js';
import type { AgentAutomationRule, Permission } from './types.js';

const mocked = vi.hoisted(() => ({
  mastra: vi.fn(),
  stream: vi.fn(),
  resumeViaRuntime: vi.fn(),
  observe: vi.fn(),
  getHistory: vi.fn(),
  isRunLive: vi.fn(),
  actualFactory: false,
  actualAgent: undefined as unknown,
}));
const RESOURCE_ID = resourceIdFromKey('acme_thread');
const GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

vi.mock('@proofoftech/breakwater/agent', async (importOriginal) => {
  const original =
    await importOriginal<typeof import('@proofoftech/breakwater/agent')>();
  return {
    ...original,
    isGuardedAgentHandle: (value: unknown) =>
      typeof value === 'object' &&
      value !== null &&
      ((value as { guarded?: unknown }).guarded === true ||
        original.isGuardedAgentHandle(value)),
  };
});

vi.mock('@mastra/core/mastra', () => {
  class Mastra {
    readonly agentThreadStreamRuntime = {};
    readonly agents: Record<string, unknown>;

    constructor(options: { agents: Record<string, unknown> }) {
      this.agents = options.agents;
    }

    getAgentById(id: string) {
      return (Object.values(this.agents) as Array<{ id?: string }>).find(
        (agent) => agent.id === id,
      );
    }
  }
  return {
    Mastra: vi.fn(function MastraConstructor(options: {
      agents: Record<string, unknown>;
    }) {
      return mocked.mastra(options) ?? new Mastra(options);
    }),
  };
});

vi.mock('../agent-runner/index.js', async (importOriginal) => {
  const original =
    await importOriginal<typeof import('../agent-runner/index.js')>();
  return {
    ...original,
    createFlowsafeDurableAgent: (
      configuration: Parameters<typeof original.createFlowsafeDurableAgent>[0],
    ) => {
      if (mocked.actualFactory)
        return original.createFlowsafeDurableAgent(configuration);
      const runIds = new Set<string>();
      return {
        getWorkflow: () => ({ id: 'durable-agentic-loop' }),
        isRunLive: (runId: string) =>
          mocked.isRunLive(configuration.agent.id, runId),
        authoritativeAgentStartState: async (
          expected: RunnerRuntime,
          threadId: string,
          runId: string,
        ) => {
          if (expected !== configuration.runtime)
            throw new Error('runtime mismatch');
          const state = await expected.authoritativeStartState(
            'durable-agentic-loop',
            runId,
          );
          if (!state) return null;
          const identity = state.provenance.startIdentity;
          if (identity?.target.kind !== 'agent')
            throw new RunStateUnreadableError('durable-agentic-loop', runId);
          if (
            identity.target.id !== configuration.agent.id ||
            identity.target.threadId !== threadId
          )
            throw new AgentRunSelectorMismatchError(
              'durable-agentic-loop',
              runId,
            );
          return {
            ...state,
            execution: { ...state.execution, ...identity },
            threaded: state.provenance.agentStart?.threaded,
          };
        },
        streamUntilPersisted: async (...args: unknown[]) => {
          const options = args[1] as { runId?: string } | undefined;
          if (options?.runId) runIds.add(options.runId);
          const authority = args[7] as
            | import('../agent-runner/durable-agent-runner.js').AgentStartAuthority
            | undefined;
          if (configuration.runtime.constructor.name !== 'RunnerRuntime')
            await authority?.onPreparedStartIdentity?.({
              tablePrefix: null,
              workflowId: 'durable-agentic-loop',
              runId: options?.runId ?? '',
              startToken: 'test-generation',
            });
          return mocked.stream(...args);
        },
        resumeViaRuntime: async (...args: unknown[]) => {
          const options = args[0] as { runId?: string } | undefined;
          if (options?.runId) runIds.add(options.runId);
          return mocked.resumeViaRuntime(...args);
        },
        observe: mocked.observe,
        runRegistry: {
          has: (runId: string) => runIds.has(runId),
        },
        pubsub: {
          getHistory: mocked.getHistory,
        },
      };
    },
  };
});

interface Harness {
  host: ReturnType<typeof createThreadAgentHost>;
  state: Map<string, unknown>;
  scope: ThreadScope;
  moduleScopes: AgentThreadInstanceScope[];
  storageScopes: AgentThreadInstanceScope[];
  approvalScopes: AgentThreadInstanceScope[];
  auditEvents: ApprovalAuditEvent[];
  resources: InMemoryResourceOwnershipStore;
  resourceAccess: RecoverableResourceOwnershipStore;
  schedules: Map<
    string,
    {
      readonly target: unknown;
      readonly dispatchId: string;
      readonly runId: string;
    }
  >;
  stateStorage: AgentThreadStateStorage;
  alarmAt(): number | Date | undefined;
  setSummary(summary: RunSummary | null, visible?: boolean): void;
  setSnapshot(values?: {
    agentId?: string;
    threadId?: string;
    resourceId?: string;
    memory?: boolean;
    requestContext?: Record<string, unknown>;
  }): void;
}

function guarded(
  id = 'writer',
  automationKinds: readonly string[] = [],
): GuardedAgentHandle {
  if (mocked.actualAgent && id === 'writer')
    return mocked.actualAgent as GuardedAgentHandle;
  return {
    guarded: true,
    [GUARDED_AGENT_HOST_PROTOCOL]: {
      version: 1,
      supportsDurableStructuredOutput: false,
    },
    id,
    allowedRoles: ['operator'],
    allowedPrincipalKinds: ['human', ...automationKinds],
    maxSteps: 1,
  } as unknown as GuardedAgentHandle;
}

function harness(
  agentIds: readonly string[] = ['writer'],
  options: {
    principal?: ExecutionPrincipal;
    allowedAutomation?: readonly AgentAutomationRule[];
    authorizeAutomatedEntry?: AutomatedEntryAuthorizer;
    requiredPermissions?: readonly Permission[];
    resolvePrincipalPermissions?: PrincipalPermissionResolver;
    approvalService?: ApprovalService;
    resourceAccess?: RecoverableResourceOwnershipStore;
    runtime?: Partial<RunnerRuntime>;
    init?: InitResult;
    storage?: MastraCompositeStore;
    discardScheduleDispatch?: (
      scheduleId: string,
      dispatchId: string,
      runId: string,
    ) => Promise<void>;
  } = {},
): Harness {
  const state = new Map<string, unknown>();
  let alarm: number | Date | undefined;
  const stateStorage: AgentThreadStateStorage = {
    get: async <T>(key: string) => state.get(key) as T | undefined,
    put: async (key, value) => {
      state.set(key, structuredClone(value));
    },
    delete: async (key) => state.delete(key),
    list: async <T>({ prefix }: { prefix: string }) =>
      new Map(
        [...state.entries()].filter(([key]) => key.startsWith(prefix)),
      ) as Map<string, T>,
    getAlarm: async () =>
      alarm === undefined
        ? null
        : alarm instanceof Date
          ? alarm.getTime()
          : alarm,
    setAlarm: async (scheduledTime) => {
      alarm = scheduledTime;
    },
    deleteAlarm: async () => {
      alarm = undefined;
    },
  };
  let summary: RunSummary | null = {
    runId: 'acme_run',
    status: 'success',
  };
  let statusVisible = false;
  let snapshot: unknown;
  const setSnapshot: Harness['setSnapshot'] = (values = {}) => {
    const agentId = values.agentId ?? 'writer';
    const threadId = values.threadId ?? 'acme_thread';
    const resourceId = values.resourceId ?? RESOURCE_ID;
    snapshot = {
      requestContext: {
        ...values.requestContext,
        runId: 'acme_run',
        threadId,
        resourceId,
        'breakwater.auditContext': {
          agentId,
          threadId,
          resourceId,
        },
      },
      context: {
        input: {
          agentId,
          messageListState: {
            memoryInfo:
              values.memory === false ? null : { threadId, resourceId },
          },
        },
      },
    };
  };
  setSnapshot();
  const storage =
    options.storage ??
    ({
      getStore: async () => ({
        loadWorkflowSnapshot: async () => snapshot,
      }),
    } as unknown as MastraCompositeStore);
  const runtime = {
    status: vi.fn(async (_workflowId: string, runId: string) => {
      const started = mocked.stream.mock.calls.some(
        (call) => call[1]?.runId === runId,
      );
      if (!statusVisible && !started) return null;
      return summary ? { ...summary, runId } : null;
    }),
    isRunActive: vi.fn(() => false),
    workflowIds: vi.fn(() => []),
    settleStartExecution: vi.fn(async () => {}),
    authoritativeStartState: vi.fn(
      async (_workflowId: string, runId: string) => {
        const call = mocked.stream.mock.calls.find(
          (call) => call[1]?.runId === runId,
        );
        if (options.runtime?.status) {
          const override = await options.runtime.status(_workflowId, runId);
          if (!override) return null;
          summary = override;
        }
        const resumed = mocked.resumeViaRuntime.mock.settledResults.at(-1);
        if (resumed?.type === 'fulfilled' && resumed.value?.runId === runId)
          summary = resumed.value;
        const terminated = vi.isMockFunction(
          options.runtime?.terminateAsPrincipal,
        )
          ? options.runtime.terminateAsPrincipal.mock.settledResults.at(-1)
          : undefined;
        if (terminated?.type === 'fulfilled')
          summary = terminated.value.summary;
        if (!statusVisible && !call && !terminated) return null;
        if (!summary) return null;
        const authority = call?.[7] as
          | import('../agent-runner/durable-agent-runner.js').AgentStartAuthority
          | undefined;
        const identity = authority?.startIdentity ?? {
          owner: {
            kind: options.principal?.kind ?? 'human',
            id: options.principal?.id ?? 'operator-1',
          },
          target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
        };
        const current = snapshot as {
          requestContext: Record<string, unknown>;
          context: {
            input: {
              agentId: string;
              messageListState: { memoryInfo: unknown };
            };
          };
        };
        if (
          (!call && current.context.input.agentId !== identity.target.id) ||
          current.requestContext.threadId !== 'acme_thread' ||
          current.requestContext.resourceId !== RESOURCE_ID
        )
          throw new RunStateUnreadableError('durable-agentic-loop', runId);
        const threaded =
          authority?.agentStart.threaded ??
          current.context.input.messageListState.memoryInfo !== null;
        const provenance = {
          version: 2,
          startToken: 'test-generation',
          attemptToken: call?.[4] ?? `token-${runId}`,
          resumeCounts: [],
          startIdentity: identity,
          agentStart: { threaded },
        };
        const lifecycle =
          terminated?.type === 'fulfilled'
            ? {
                version: 1,
                revision: terminated.value.cleanup.revision,
                terminal: {
                  status: terminated.value.cleanup.status,
                  error: summary.errorEnvelope,
                  transitionedAt: 1,
                  replayPrincipals: [{ kind: 'human', id: 'operator-1' }],
                  ...(terminated.value.cleanup.cleanupCompleted
                    ? { cleanupCompletedAt: 2 }
                    : {}),
                },
                ...(terminated.value.cleanup.scheduleDispatch
                  ? {
                      scheduleDispatch:
                        terminated.value.cleanup.scheduleDispatch,
                    }
                  : {}),
              }
            : undefined;
        return {
          kind: summary.status === 'pending' ? 'initial' : 'result',
          storage: 'unfenced',
          execution: (
            state.get(OWNER_RECOVERY_PREFIX + runId) as
              | { execution?: unknown }
              | undefined
          )?.execution ?? {
            tablePrefix: null,
            workflowId: 'durable-agentic-loop',
            runId,
            startToken: 'test-generation',
          },
          provenance,
          snapshot: {
            ...current,
            requestContext: {
              ...current.requestContext,
              'flowsafe.runProvenance': provenance,
              ...(lifecycle ? { 'flowsafe.runLifecycle': lifecycle } : {}),
            },
          },
          ...(summary.status === 'pending'
            ? {}
            : { summary: { ...summary, runId } }),
        };
      },
    ),
    recoverStartAttempt: vi.fn(async (execution: { runId: string }) => {
      const started = mocked.stream.mock.calls.some(
        (call) => call[1]?.runId === execution.runId,
      );
      if (!statusVisible && !started) return null;
      return summary
        ? { kind: 'ordinary', summary: { ...summary, runId: execution.runId } }
        : null;
    }),
    ...options.runtime,
  } as unknown as RunnerRuntime;
  const scope = {
    threadId: 'acme_thread',
    principal: options.principal ?? {
      kind: 'human',
      id: 'operator-1',
      role: 'operator',
    },
    init:
      options.init ??
      ({
        runtime,
        pubsub: undefined,
      } as unknown as InitResult),
  } satisfies ThreadScope;
  const moduleScopes: AgentThreadInstanceScope[] = [];
  const storageScopes: AgentThreadInstanceScope[] = [];
  const approvalScopes: AgentThreadInstanceScope[] = [];
  const auditEvents: ApprovalAuditEvent[] = [];
  const resources = new InMemoryResourceOwnershipStore();
  const resourceAccess = options.resourceAccess ?? resources;
  const schedules: Harness['schedules'] = new Map();
  const scheduleSource: ScheduleSourceStore = {
    resolveScheduleTarget: async (scheduleId, dispatchId, runId) => {
      const source = schedules.get(scheduleId);
      return source?.dispatchId === dispatchId && source.runId === runId
        ? (source.target as never)
        : undefined;
    },
  };
  const host = createThreadAgentHost({
    ...(options.authorizeAutomatedEntry
      ? { authorizeAutomatedEntry: options.authorizeAutomatedEntry }
      : {}),
    ...(options.resolvePrincipalPermissions
      ? { resolvePrincipalPermissions: options.resolvePrincipalPermissions }
      : {}),
    buildModules: (instanceScope) => {
      moduleScopes.push(instanceScope);
      return agentIds.map((agentId) => ({
        meta: {
          id: agentId,
          title: agentId,
          description: 'Writes an approved record',
          allowedRoles: ['operator'],
          ...(options.allowedAutomation
            ? { allowedAutomation: options.allowedAutomation }
            : {}),
          ...(options.requiredPermissions
            ? { requiredPermissions: options.requiredPermissions }
            : {}),
        },
        agent: guarded(
          agentId,
          (options.allowedAutomation ?? []).map((rule) => rule.kind),
        ),
      }));
    },
    storage: (instanceScope) => {
      storageScopes.push(instanceScope);
      return storage;
    },
    stateStorage: () => stateStorage,
    resourceAccess: () => resourceAccess,
    scheduleSource: () => scheduleSource,
    ...(options.discardScheduleDispatch
      ? { discardScheduleDispatch: options.discardScheduleDispatch }
      : {}),
    approvalService: (instanceScope) => {
      approvalScopes.push(instanceScope);
      if (options.approvalService) return options.approvalService;
      return {
        // The bridge mints its bookkeeping principal against this binding.
        list: async () => [],
        createAsPrincipal: async () => {
          throw new Error('unexpected approval creation');
        },
      } as unknown as import('../approval-api/index.js').ApprovalService;
    },
    audit: (event) => auditEvents.push(event),
  });
  return {
    host,
    state,
    scope,
    moduleScopes,
    storageScopes,
    approvalScopes,
    auditEvents,
    resources,
    resourceAccess,
    schedules,
    stateStorage,
    alarmAt: () => alarm,
    setSummary: (value, visible = true) => {
      summary = value;
      statusVisible = visible;
    },
    setSnapshot,
  };
}

const HUMAN_OWNER = { kind: 'human' as const, id: 'operator-1' };
const SCHEDULE_ID = 'acme_schedule';
const DISPATCH_ID = 'acme_dispatch';
const SCHEDULE_NOW = Date.parse('2026-08-14T00:00:00.000Z');

async function executingAgentSchedule(): Promise<D1SchedulesStorage> {
  const sqlite = openSqlite();
  const schedules = new D1SchedulesStorage(
    sqliteUnitDatabase(sqlite) as ScheduleDatabase,
  );
  await schedules.createSchedule({
    id: SCHEDULE_ID,
    target: {
      type: 'agent',
      agentId: 'writer',
      prompt: 'scheduled',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
    },
    cron: '* * * * *',
    status: 'active',
    nextFireAt: SCHEDULE_NOW,
    createdAt: SCHEDULE_NOW,
    updatedAt: SCHEDULE_NOW,
    metadata: {},
  });
  await schedules.recordTrigger({
    id: DISPATCH_ID,
    scheduleId: SCHEDULE_ID,
    runId: 'acme_run',
    scheduledFireAt: SCHEDULE_NOW,
    actualFireAt: SCHEDULE_NOW,
    outcome: 'deferred',
    metadata: {
      dispatchState: 'prepared',
      dispatchRef: {
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
        runId: 'acme_run',
        target: 'agent',
        mode: 'start',
        agentId: 'writer',
      },
    },
  });
  await expect(
    schedules.beginAgentScheduleDispatch(
      SCHEDULE_ID,
      DISPATCH_ID,
      SCHEDULE_NOW,
      60_000,
    ),
  ).resolves.toEqual({ state: 'ready' });
  return schedules;
}

async function seedThreadOwner(
  fixture: Pick<Harness, 'resources'>,
  owner = HUMAN_OWNER,
): Promise<void> {
  await fixture.resources.claim('thread', 'acme_thread', owner);
  await fixture.resources.claim('resource', RESOURCE_ID, owner);
}

async function seedScheduleOwner(
  fixture: Pick<Harness, 'resources' | 'schedules'>,
  target: unknown,
  owner = HUMAN_OWNER,
  scheduleId = SCHEDULE_ID,
  dispatchId = DISPATCH_ID,
  runId = 'acme_run',
): Promise<void> {
  fixture.schedules.set(scheduleId, { target, dispatchId, runId });
  await fixture.resources.claim('schedule', scheduleId, owner);
}

async function seedThreadedSchedule(
  fixture: Pick<Harness, 'resources' | 'schedules'>,
  owner = HUMAN_OWNER,
  scheduleId = SCHEDULE_ID,
  dispatchId = DISPATCH_ID,
  runId = 'acme_scheduled',
): Promise<void> {
  await seedThreadOwner(fixture, owner);
  await seedScheduleOwner(
    fixture,
    {
      type: 'agent',
      agentId: 'writer',
      prompt: 'scheduled',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
    },
    owner,
    scheduleId,
    dispatchId,
    runId,
  );
}

function seedSuspendedApprovalRun(fixture: Harness): void {
  fixture.setSummary({
    runId: 'acme_run',
    status: 'suspended',
    requestedBy: 'operator-1',
  });
  fixture.state.set('flowsafe:agent-thread-binding:v1', {
    version: 1,
    agentId: 'writer',
    resourceId: RESOURCE_ID,
  });
  fixture.state.set('flowsafe:agent-run:v1:acme_run', {
    version: 2,
    agentId: 'writer',
    principal: fixture.scope.principal,
    originEntryPath: 'http.start',
  });
  mocked.resumeViaRuntime.mockResolvedValue({
    runId: 'acme_run',
    status: 'success',
  });
}

async function seedThreadlessSchedule(
  fixture: Pick<Harness, 'resources' | 'schedules'>,
  owner = HUMAN_OWNER,
  scheduleId = SCHEDULE_ID,
  dispatchId = DISPATCH_ID,
  runId = 'acme_run',
): Promise<void> {
  await seedScheduleOwner(
    fixture,
    { type: 'agent', agentId: 'writer', prompt: 'scheduled' },
    owner,
    scheduleId,
    dispatchId,
    runId,
  );
}

beforeEach(() => {
  mocked.mastra.mockReset();
  mocked.actualFactory = false;
  mocked.actualAgent = undefined;
  mocked.stream.mockReset().mockResolvedValue({});
  mocked.resumeViaRuntime.mockReset();
  mocked.observe.mockReset();
  mocked.getHistory.mockReset().mockResolvedValue([]);
  mocked.isRunLive.mockReset().mockReturnValue(false);
});

const OWNER_RECOVERY_PREFIX = 'flowsafe:agent-owner-recovery:v1:';
const THREAD_BINDING_KEY = 'flowsafe:agent-thread-binding:v1';
const RUN_RECORD_PREFIX = 'flowsafe:agent-run:v1:';
const TEST_OWNER_RECOVERY_KEY = `${OWNER_RECOVERY_PREFIX}acme_run`;
const TEST_RUN_RECORD_KEY = `${RUN_RECORD_PREFIX}acme_run`;

function ownerRecovery(
  runId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    version: 2,
    phase: 'prepared',
    execution: {
      tablePrefix: '',
      workflowId: 'durable-agentic-loop',
      runId,
      startToken: 'test-generation',
    },
    runRecord: {
      version: 2,
      agentId: 'writer',
      principal: { kind: 'human', id: 'operator-1', role: 'operator' },
      originEntryPath: 'http.start',
    },
    agentId: 'writer',
    threadId: 'acme_thread',
    resourceId: RESOURCE_ID,
    runId,
    owner: { kind: 'human', id: 'operator-1' },
    token: `token-${runId}`,
    threaded: true,
    bindingPreexisting: false,
    ...overrides,
  };
}

function seedRecoveryState(
  state: Map<string, unknown>,
  runId: string,
  recovery: Record<string, unknown>,
  binding = true,
): void {
  state.set(`${OWNER_RECOVERY_PREFIX}${runId}`, recovery);
  state.set(`${RUN_RECORD_PREFIX}${runId}`, {
    version: 2,
    agentId: 'writer',
    principal: { kind: 'human', id: 'operator-1', role: 'operator' },
    originEntryPath: 'http.start',
  });
  if (binding) {
    state.set(THREAD_BINDING_KEY, {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
  }
}

function cDeferred() {
  let resolve = () => {};
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const C_START_INPUT: ThreadAgentStartInput = {
  agentId: 'writer',
  threadId: 'acme_thread',
  resourceId: RESOURCE_ID,
  runId: 'acme_run',
  prompt: 'original',
  entryPath: 'http.start',
};

function cObserved<T extends object>(
  values: T,
  mode: 'alternate' | 'second-throw' = 'second-throw',
) {
  const counts = new Map<keyof T, number>();
  const source = {} as T;
  for (const key of Object.keys(values) as Array<keyof T>) {
    Object.defineProperty(source, key, {
      enumerable: true,
      configurable: true,
      get() {
        const count = (counts.get(key) ?? 0) + 1;
        counts.set(key, count);
        if (count > 1) {
          if (mode === 'second-throw')
            throw new Error(`second read: ${String(key)}`);
          return 'replacement';
        }
        return values[key];
      },
    });
  }
  return { source, counts, values };
}

describe('C direct thread host capture', () => {
  it.each(
    [
      'mutationEpoch',
      'startIdentity',
      'agentStart',
      'execution',
      'tablePrefix',
      'startToken',
      'attemptToken',
      'runOwnerGuard',
      'onPreparedStartIdentity',
    ].flatMap((field) => [null, 2].map((value) => ({ field, value }))),
  )('C thread host refuses internal JSON authority before effects ($field, $value)', async ({
    field,
    value,
  }) => {
    const fixture = harness();
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');
    const error = await fixture.host
      .route(
        new Request('https://thread/_flowsafe/agent-host/start', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ ...C_START_INPUT, [field]: value }),
        }),
        fixture.scope,
      )
      .catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(Error);
    const response = doErrorResponse(error);
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'start owner and requester are derived from trusted provenance',
    });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(fixture.state.size).toBe(0);
    expect(fixture.moduleScopes).toEqual([]);
    expect(fixture.storageScopes).toEqual([]);
    expect(
      (
        await fixture.host.route(
          new Request('https://thread/_flowsafe/agent-host/start', {
            method: 'POST',
            body: JSON.stringify(C_START_INPUT),
          }),
          fixture.scope,
        )
      )?.status,
    ).toBe(200);
    expect(mocked.stream).toHaveBeenCalledOnce();
  });

  it.each([
    'normal',
    'failure',
    'recovery',
  ] as const)('FS8 D3 host activation agent preparation and ownership: %s', async (phase) => {
    const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
      '@mastra/core/mastra',
    );
    mocked.mastra.mockImplementation(
      (config: ConstructorParameters<typeof core.Mastra>[0]) =>
        config?.workflows ? new core.Mastra(config) : undefined,
    );
    const sql = openSqlite() as ReturnType<typeof openSqlite> & {
      close(): void;
    };
    const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
      ResourceOwnershipDatabase;
    const storage = createD1Storage({ binding });
    await storage.init();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    for (let index = 0; index < 2; index++) {
      const before = await fence.read();
      const draining = await fence.transition({
        expected: 'open',
        next: 'draining',
        expectedMutationEpoch: before.mutationEpoch,
        expectedRevision: before.transitionRevision,
        advanceMutationEpoch: true,
      });
      await fence.transition({
        expected: 'draining',
        next: 'open',
        expectedMutationEpoch: draining.mutationEpoch,
        expectedRevision: draining.transitionRevision,
      });
    }
    expect(await fence.read()).toMatchObject({
      state: 'open',
      mutationEpoch: 2,
      requireMutationEpoch: true,
    });
    const failure = new Error('C thread provider failed');
    const app = init(
      { storage },
      {
        executionFence: fence,
        startIdempotency: 'none',
        requestContextForRun: () => {
          if (phase === 'failure') throw failure;
          return {};
        },
      },
    );
    const schema = z.object({
      agentId: z.string(),
      runId: z.string(),
      messageListState: z.object({ memoryInfo: z.null() }),
    });
    app
      .createWorkflow({
        id: 'durable-agentic-loop',
        inputSchema: schema,
        outputSchema: schema,
      })
      .then(
        app.createStep({
          id: 'c-host-step',
          inputSchema: schema,
          outputSchema: schema,
          execute: async ({ inputData }) => inputData,
        }),
      )
      .commit();
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing managed owned workflow capability');
    const counts = { admission: 0, terminalization: 0 };
    const capability: FencedWorkflowAdmissionCapability = {
      ...native,
      withInitialAdmission: (input, create) => {
        counts.admission++;
        return native.withInitialAdmission(input, create);
      },
      terminalizeInitialAdmission: (input) => {
        counts.terminalization++;
        return native.terminalizeInitialAdmission(input);
      },
    };
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: capability,
      configurable: true,
    });
    const fixture = harness(['writer'], {
      init: app,
      storage,
      resourceAccess: new D1ResourceOwnershipStore(
        binding as ResourceOwnershipDatabase,
      ),
    });
    Object.assign(fixture.scope, { mutationEpoch: 2 });
    expect(fixture.scope.init.runtime).toBe(app.runtime);
    const writes: Array<[string, unknown]> = [];
    const put = fixture.stateStorage.put.bind(fixture.stateStorage);
    vi.spyOn(fixture.stateStorage, 'put').mockImplementation(
      async (key, value) => {
        writes.push([key, structuredClone(value)]);
        await put(key, value);
      },
    );
    const settle = fixture.resourceAccess.settleReservation.bind(
      fixture.resourceAccess,
    );
    if (phase === 'recovery')
      vi.spyOn(
        fixture.resourceAccess,
        'settleReservation',
      ).mockImplementationOnce(async (...args) => {
        await settle(...args);
        throw new Error('C lost thread settlement receipt');
      });
    mocked.stream.mockImplementation(
      async (
        ...args: Parameters<FlowsafeDurableAgent['streamUntilPersisted']>
      ) => {
        const [
          ,
          options,
          requestedBy,
          requestedByKind,
          attemptToken,
          scheduleDispatch,
          idempotencyKey,
          authority,
        ] = args;
        const runId = options.runId;
        if (typeof runId !== 'string')
          throw new Error('host omitted its runId');
        await app.runtime.start('durable-agentic-loop', {
          runId,
          inputData: {
            agentId: 'writer',
            runId,
            messageListState: { memoryInfo: null },
          },
          storedRequestContext: Object.fromEntries(
            options.requestContext?.entries() ?? [],
          ),
          requestedBy,
          requestedByKind,
          attemptToken,
          scheduleDispatch,
          idempotencyKey,
          mutationEpoch: authority.mutationEpoch,
          startIdentity: authority.startIdentity,
          agentStart: authority.agentStart,
          onPreparedStartIdentity: authority.onPreparedStartIdentity,
          runOwnerGuard: authority.runOwnerGuard,
        });
        return {};
      },
    );
    try {
      const pending = fixture.host.start(fixture.scope, {
        ...C_START_INPUT,
        threaded: false,
      });
      if (phase === 'failure') await expect(pending).rejects.toBe(failure);
      else
        expect(await pending).toMatchObject({ summary: { status: 'success' } });
      expect(mocked.stream).toHaveBeenCalledOnce();
      const args = mocked.stream.mock.calls[0];
      expect(args).toHaveLength(8);
      expect(args?.[7]).toHaveProperty(
        'onPreparedStartIdentity',
        expect.any(Function),
      );
      expect(Object.hasOwn(args?.[7], 'onPreparedStartIdentity')).toBe(true);
      const journals = writes.filter(([key]) =>
        key.startsWith('flowsafe:agent-owner-recovery'),
      );
      expect(journals).toHaveLength(phase === 'failure' ? 1 : 2);
      expect(journals[0]?.[1]).toMatchObject({
        version: 2,
        phase: 'preparing',
        token: args?.[4],
        runRecord: { version: 2, principal: fixture.scope.principal },
      });
      if (phase !== 'failure')
        expect(journals[1]?.[1]).toMatchObject({
          phase: 'prepared',
          execution: { startToken: expect.any(String) },
        });
      const snapshot = await workflows.loadWorkflowSnapshot({
        workflowName: 'durable-agentic-loop',
        runId: 'acme_run',
      });
      if (phase === 'failure') expect(snapshot).toBeNull();
      else {
        expect(
          snapshot?.requestContext?.['flowsafe.runProvenance'],
        ).toMatchObject({
          version: 2,
          requestedBy: 'operator-1',
          requestedByKind: 'human',
          startToken: expect.any(String),
          attemptToken: args?.[4],
          resumeCounts: [],
        });
        for (const key of [
          'mutationEpoch',
          'startIdentity',
          'agentStart',
          'execution',
          'flowsafe.initialAdmission',
        ])
          expect(snapshot?.requestContext).not.toHaveProperty(key);
        expect(writes).toContainEqual([
          TEST_RUN_RECORD_KEY,
          {
            version: 2,
            agentId: 'writer',
            principal: fixture.scope.principal,
            originEntryPath: 'http.start',
          },
        ]);
      }
      if (phase === 'recovery')
        expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
      await fixture.host.recoverOwnership(fixture.scope);
      expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
      expect(
        writes.filter(([key]) =>
          key.startsWith('flowsafe:agent-owner-recovery'),
        ),
      ).toEqual(journals);
    } finally {
      mocked.mastra.mockReset();
      sql.close();
      expect(counts).toEqual({
        admission: phase === 'failure' ? 0 : 1,
        terminalization: 0,
      });
    }
  });

  it.each([
    ['human', true, 'authorize'],
    ['human', false, 'authorize'],
    ['service', false, 'source'],
    ['system', false, 'source'],
    ['service', true, 'authorize'],
    ['system', true, 'authorize'],
  ] as const)('C direct host keeps scope selectors and separate owners: %s threaded=%s at %s', async (kind, threaded, boundary) => {
    const entered = cDeferred();
    const release = cDeferred();
    const principal: ExecutionPrincipal =
      kind === 'human'
        ? { kind, id: 'operator-1', role: 'operator' }
        : { kind, id: `${kind}-starter`, purpose: 'schedule execution' };
    const scheduled = kind !== 'human';
    const entryPath = scheduled ? 'schedule.fire' : 'http.start';
    const authorize = vi.fn(async () => {
      if (boundary === 'authorize') {
        entered.resolve();
        await release.promise;
      }
      return { permissions: [], policyVersion: 'original' };
    });
    const fixture = harness(['writer'], {
      principal,
      allowedAutomation:
        kind !== 'human'
          ? [{ kind, entryPaths: ['schedule.fire'] }]
          : undefined,
      resolvePrincipalPermissions: authorize,
    });
    if (scheduled) {
      if (threaded) {
        await seedThreadedSchedule(
          fixture,
          HUMAN_OWNER,
          SCHEDULE_ID,
          DISPATCH_ID,
          'acme_run',
        );
        fixture.state.set(THREAD_BINDING_KEY, {
          version: 1,
          agentId: 'writer',
          resourceId: RESOURCE_ID,
        });
      } else await seedThreadlessSchedule(fixture);
    }
    const owner = { ...HUMAN_OWNER };
    const nativeOwner = fixture.resourceAccess.owner.bind(
      fixture.resourceAccess,
    );
    vi.spyOn(fixture.resourceAccess, 'owner').mockImplementation(
      async (resourceKind, id) => {
        if (resourceKind === 'schedule') {
          if (boundary === 'source') {
            entered.resolve();
            await release.promise;
          }
          return owner;
        }
        return nativeOwner(resourceKind, id);
      },
    );
    const ownerCopied = cDeferred();
    const ownerRelease = cDeferred();
    const nativeGet = fixture.stateStorage.get.bind(fixture.stateStorage);
    vi.spyOn(fixture.stateStorage, 'get').mockImplementation(
      async <T>(key: string) => {
        if (key === TEST_OWNER_RECOVERY_KEY) {
          ownerCopied.resolve();
          await ownerRelease.promise;
        }
        return nativeGet<T>(key);
      },
    );
    const scope = { ...fixture.scope, mutationEpoch: 2, deploymentTag: 'acme' };
    const originalInit = scope.init;
    const input: ThreadAgentStartInput = {
      ...C_START_INPUT,
      entryPath,
      threaded,
      idempotencyKey: 'original-key',
      scheduleId: scheduled ? SCHEDULE_ID : undefined,
      dispatchId: scheduled ? DISPATCH_ID : undefined,
      scheduleDispatchLease: scheduled ? 'executing' : undefined,
      safeContext: { note: 'original' },
      providerOptions: undefined,
    };
    const writes: Array<[string, unknown]> = [];
    const nativePut = fixture.stateStorage.put.bind(fixture.stateStorage);
    vi.spyOn(fixture.stateStorage, 'put').mockImplementation(
      async (key, value) => {
        writes.push([key, structuredClone(value)]);
        await nativePut(key, value);
      },
    );
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');
    const pending = fixture.host.start(scope, input);
    const outcome = pending.then(
      (result) => ({ result }),
      (error: unknown) => ({ error }),
    );
    try {
      await entered.promise;
      Object.assign(scope, {
        principal: { kind: 'human', id: 'replacement', role: 'admin' },
        mutationEpoch: 3,
        threadId: 'replacement',
        deploymentTag: 'replacement',
        init: {},
      });
      Object.assign(input, {
        agentId: 'replacement',
        threadId: 'replacement',
        resourceId: 'replacement',
        runId: 'replacement',
        prompt: 'replacement',
        messages: ['replacement'],
        entryPath: 'signal.resume',
        threaded: !threaded,
        scheduleId: 'replacement',
        dispatchId: 'replacement',
        scheduleDispatchLease: undefined,
        idempotencyKey: 'replacement',
        safeContext: { note: 'replacement' },
        providerOptions: { replacement: true },
      });
      release.resolve();
      const reached = await Promise.race([
        ownerCopied.promise.then(() => true),
        outcome.then(() => false),
      ]);
      expect(
        reached,
        'captured scope reaches storage after authorization',
      ).toBe(true);
      owner.id = 'replacement-owner';
    } finally {
      release.resolve();
      ownerRelease.resolve();
      await outcome;
    }
    const result = await pending;
    expect(mocked.stream).toHaveBeenCalledOnce();
    const args = mocked.stream.mock.calls[0];
    expect(args).toHaveLength(8);
    const authority = args?.[7];
    expect(args?.[0]).toBe(scheduled ? 'scheduled' : 'original');
    expect(args?.[1]).toMatchObject({
      runId: 'acme_run',
      disableBackgroundTasks: true,
      maxSteps: 1,
    });
    expect(args?.[1].memory).toEqual(
      threaded ? { thread: 'acme_thread', resource: RESOURCE_ID } : undefined,
    );
    expect(args?.slice(2, 4)).toEqual([principal.id, principal.kind]);
    expect(args?.[4]).toEqual(expect.any(String));
    expect(args?.[5]).toEqual(
      scheduled
        ? { scheduleId: SCHEDULE_ID, dispatchId: DISPATCH_ID }
        : undefined,
    );
    expect(args?.[6]).toBe('original-key');
    expect(authority).toEqual({
      mutationEpoch: 2,
      startIdentity: {
        owner: { kind: principal.kind, id: principal.id },
        target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
      },
      agentStart: { threaded },
      onPreparedStartIdentity: expect.any(Function),
      runOwnerGuard: { owner: HUMAN_OWNER, reservationToken: args?.[4] },
    });
    expect(Object.hasOwn(authority, 'onPreparedStartIdentity')).toBe(true);
    expect(reserve.mock.calls[0]).toEqual([
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      HUMAN_OWNER,
      args?.[4],
    ]);
    const journals = writes.filter(([key]) =>
      key.startsWith(OWNER_RECOVERY_PREFIX),
    );
    expect(journals).toHaveLength(2);
    expect(journals[0]?.[1]).toMatchObject({
      version: 2,
      phase: 'preparing',
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      owner: HUMAN_OWNER,
      token: args?.[4],
      threaded,
      bindingPreexisting: scheduled && threaded,
      runRecord: {
        version: 2,
        agentId: 'writer',
        principal,
        originEntryPath: entryPath,
      },
    });
    expect(journals[1]?.[1]).toMatchObject({
      phase: 'prepared-unfenced',
      execution: { tablePrefix: null, startToken: 'test-generation' },
    });
    expect(writes).toContainEqual([
      TEST_RUN_RECORD_KEY,
      { version: 2, agentId: 'writer', principal, originEntryPath: entryPath },
    ]);
    expect(result).toMatchObject({
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
    });
    expect(fixture.moduleScopes[0]).toMatchObject({
      threadId: 'acme_thread',
      deploymentTag: 'acme',
      init: originalInit,
    });
    expect(Object.isFrozen(scope)).toBe(false);
    expect(Object.isFrozen(input)).toBe(false);
    expect(Object.isFrozen(owner)).toBe(false);
  });

  it.each([
    'alternate',
    'second-throw',
  ] as const)('C direct host captures every declared scope and input getter once: %s', async (mode) => {
    const fixture = harness();
    const scope = cObserved(
      { ...fixture.scope, mutationEpoch: 2, deploymentTag: 'acme' },
      mode,
    );
    const input = cObserved<ThreadAgentStartInput>(
      {
        ...C_START_INPUT,
        messages: undefined,
        threaded: false,
        scheduleId: undefined,
        dispatchId: undefined,
        scheduleDispatchLease: undefined,
        safeContext: { note: 'original' },
        providerOptions: undefined,
        idempotencyKey: 'original-key',
      },
      mode,
    );
    await fixture.host.start(scope.source, input.source);
    expect([...scope.counts.values()]).toEqual(
      Object.keys(scope.values).map(() => 1),
    );
    expect([...input.counts.values()]).toEqual(
      Object.keys(input.values).map(() => 1),
    );
    expect(mocked.stream.mock.calls[0]).toHaveLength(8);
    expect(mocked.stream.mock.calls[0]?.[7].mutationEpoch).toBe(2);
    expect(mocked.stream.mock.calls[0]?.[7].agentStart).toEqual({
      threaded: false,
    });
  });

  it.each([
    'principal',
    'mutationEpoch',
    'threadId',
    'deploymentTag',
    'init',
    'agentId',
    'runId',
    'resourceId',
    'prompt',
    'messages',
    'entryPath',
    'threaded',
    'scheduleId',
    'dispatchId',
    'scheduleDispatchLease',
    'safeContext',
    'providerOptions',
    'idempotencyKey',
  ])('C direct host preserves first capture fault without effects: %s', async (key) => {
    const fixture = harness();
    const scope = { ...fixture.scope, mutationEpoch: 2, deploymentTag: 'acme' };
    const input = { ...C_START_INPUT };
    const fault = new Error(`first fault ${key}`);
    Object.defineProperty(
      [
        'principal',
        'mutationEpoch',
        'threadId',
        'deploymentTag',
        'init',
      ].includes(key)
        ? scope
        : input,
      key,
      {
        get() {
          throw fault;
        },
      },
    );
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');
    await expect(fixture.host.start(scope, input)).rejects.toBe(fault);
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(reserve).not.toHaveBeenCalled();
    expect(fixture.state.size).toBe(0);
    expect(fixture.moduleScopes).toEqual([]);
  });
});

describe('createThreadAgentHost owner recovery', () => {
  it('persists and arms the recovery journal before reserving ownership', async () => {
    const { host, scope, state, resourceAccess, alarmAt } = harness();
    const originalReserve = resourceAccess.reserveAll.bind(resourceAccess);
    const reserve = vi.spyOn(resourceAccess, 'reserveAll');

    reserve.mockImplementationOnce(async (claims, owner, token) => {
      expect(state.get(TEST_OWNER_RECOVERY_KEY)).toMatchObject({
        runId: 'acme_run',
        token,
      });
      expect(alarmAt()).toBeDefined();
      return originalReserve(claims, owner, token);
    });

    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    expect(reserve).toHaveBeenCalledOnce();
  });

  it('rejects a malformed journal before runtime or ownership side effects', async () => {
    const { host, scope, state, resourceAccess, alarmAt } = harness();
    state.set(
      TEST_OWNER_RECOVERY_KEY,
      ownerRecovery('acme_run', { token: 'invalid/token' }),
    );
    const recover = vi.spyOn(scope.init.runtime, 'recoverStartAttempt');
    const settle = vi.spyOn(resourceAccess, 'settleReservation');

    await expect(host.recoverOwnership(scope)).rejects.toThrow(
      'stored agent owner recovery is malformed',
    );

    expect(recover).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(alarmAt()).toBeDefined();
  });

  it('ignores a stale listed generation before runtime or ownership side effects', async () => {
    const { host, scope, state, stateStorage, resourceAccess, alarmAt } =
      harness();
    const current = ownerRecovery('acme_run', { token: 'token-current' });
    const stale = ownerRecovery('acme_run', { token: 'token-stale' });
    state.set(TEST_OWNER_RECOVERY_KEY, current);
    vi.spyOn(stateStorage, 'list').mockResolvedValueOnce(
      new Map([[TEST_OWNER_RECOVERY_KEY, stale]]) as never,
    );
    const recover = vi.spyOn(scope.init.runtime, 'recoverStartAttempt');
    const settle = vi.spyOn(resourceAccess, 'settleReservation');

    await expect(host.recoverOwnership(scope)).rejects.toThrow(
      'agent owner recovery changed',
    );

    expect(recover).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(current);
    expect(alarmAt()).toBeDefined();
  });

  it('rolls back a pre-snapshot attempt and its attempt-created metadata', async () => {
    const { host, scope, state, resources, alarmAt } = harness();
    const recovery = ownerRecovery('acme_run', { phase: 'preparing' });
    delete recovery.execution;
    seedRecoveryState(state, 'acme_run', recovery);
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      { kind: 'human', id: 'operator-1' },
      'token-acme_run',
    );

    await host.recoverOwnership(scope);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(state.has(THREAD_BINDING_KEY)).toBe(false);
    expect(await resources.owner('thread', 'acme_thread')).toBeUndefined();
    expect(await resources.owner('resource', RESOURCE_ID)).toBeUndefined();
    expect(await resources.owner('run', 'acme_run')).toBeUndefined();
    expect(alarmAt()).toBeUndefined();
  });

  it('retains a preexisting binding and committed claims during pre-snapshot rollback', async () => {
    const { host, scope, state, resources, alarmAt } = harness();
    const owner = { kind: 'human' as const, id: 'operator-1' };
    await resources.claim('thread', 'acme_thread', owner);
    await resources.claim('resource', RESOURCE_ID, owner);
    const recovery = ownerRecovery('acme_run', {
      bindingPreexisting: true,
      phase: 'preparing',
    });
    delete recovery.execution;
    seedRecoveryState(state, 'acme_run', recovery);
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      owner,
      'token-acme_run',
    );

    await host.recoverOwnership(scope);

    expect(state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await resources.owner('thread', 'acme_thread')).toEqual(owner);
    expect(await resources.owner('resource', RESOURCE_ID)).toEqual(owner);
    expect(await resources.owner('run', 'acme_run')).toBeUndefined();
    expect(alarmAt()).toBeUndefined();
  });

  it('commits ownership when the recovery token has an authoritative snapshot', async () => {
    const { host, scope, state, resources, alarmAt, setSummary } = harness();
    setSummary({ runId: 'acme_run', status: 'suspended' });
    const recovery = ownerRecovery('acme_run');
    seedRecoveryState(state, 'acme_run', recovery);
    const owner = { kind: 'human' as const, id: 'operator-1' };
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      owner,
      'token-acme_run',
    );

    await host.recoverOwnership(scope);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(await resources.owner('thread', 'acme_thread')).toEqual(owner);
    expect(await resources.owner('resource', RESOURCE_ID)).toEqual(owner);
    expect(await resources.owner('run', 'acme_run')).toEqual(owner);
    expect(alarmAt()).toBeUndefined();
  });

  it('retains every recovery record and rearms when the authoritative read does not succeed', async () => {
    const { host, scope, state, resources, resourceAccess, alarmAt } = harness(
      ['writer'],
      {
        // The runner-side guard reaches here: recoverStartAttempt refuses a
        // read that did not reach storage rather than reporting the fabricated
        // 'pending' shell it would otherwise see.
        runtime: {
          recoverStartAttempt: vi.fn(async () => {
            throw new RunStateUnreadableError(
              'durable-agentic-loop',
              'acme_run',
            );
          }),
        },
      },
    );
    const settle = vi.spyOn(resourceAccess, 'settleReservation');
    seedRecoveryState(state, 'acme_run', ownerRecovery('acme_run'));
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      { kind: 'human', id: 'operator-1' },
      'token-acme_run',
    );

    await expect(host.recoverOwnership(scope)).rejects.toBeInstanceOf(
      RunStateUnreadableError,
    );

    // #then — fail closed: an unreadable read is not evidence the attempt was
    // abandoned, so nothing is deleted, nothing is settled, and the journal
    // stays armed for a wake that can read.
    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(settle).not.toHaveBeenCalled();
    expect(alarmAt()).toBeDefined();
  });

  it('keeps an unthreaded nonterminal journal armed, then releases ephemeral claims at terminal state', async () => {
    const { host, scope, state, resources, alarmAt, setSummary, setSnapshot } =
      harness();
    setSnapshot({ memory: false });
    const owner = { kind: 'human' as const, id: 'operator-1' };
    const recovery = ownerRecovery('acme_run', { threaded: false });
    seedRecoveryState(state, 'acme_run', recovery, false);
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      owner,
      'token-acme_run',
    );
    setSummary({ runId: 'acme_run', status: 'suspended' });

    await host.recoverOwnership(scope);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(alarmAt()).toBeDefined();
    expect(await resources.owner('thread', 'acme_thread')).toEqual(owner);
    expect(await resources.owner('resource', RESOURCE_ID)).toEqual(owner);

    setSummary({ runId: 'acme_run', status: 'success' });
    await host.recoverOwnership(scope);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await resources.owner('thread', 'acme_thread')).toBeUndefined();
    expect(await resources.owner('resource', RESOURCE_ID)).toBeUndefined();
    expect(await resources.owner('run', 'acme_run')).toEqual(owner);
    expect(alarmAt()).toBeUndefined();
  });

  it('returns a persisted start after settlement receipt loss and clears recovery on retry', async () => {
    const committed = new InMemoryResourceOwnershipStore();
    let loseReceipt = true;
    const resourceAccess: RecoverableResourceOwnershipStore = {
      claim: (kind, resourceId, owner) =>
        committed.claim(kind, resourceId, owner),
      reserveAll: (claims, owner, token) =>
        committed.reserveAll(claims, owner, token),
      settleReservation: async (token, release) => {
        await committed.settleReservation(token, release);
        if (loseReceipt) {
          loseReceipt = false;
          throw new Error('settlement receipt lost');
        }
      },
      owner: (kind, resourceId) => committed.owner(kind, resourceId),
      release: (kind, resourceId, owner) =>
        committed.release(kind, resourceId, owner),
    };
    const { host, scope, state, alarmAt } = harness(['writer'], {
      resourceAccess,
    });
    const log = vi.spyOn(console, 'error').mockImplementation(() => {});

    try {
      await expect(
        host.start(scope, {
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          prompt: 'go',
          entryPath: 'http.start',
        }),
      ).resolves.toMatchObject({ runId: 'acme_run' });
      expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
      expect(alarmAt()).toBeDefined();
      expect(await committed.owner('run', 'acme_run')).toEqual({
        kind: 'human',
        id: 'operator-1',
      });

      await host.recoverOwnership(scope);

      expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
      expect(alarmAt()).toBeUndefined();
    } finally {
      log.mockRestore();
    }
  });

  it('serializes empty-journal alarm deletion before a concurrent start arms its journal', async () => {
    const { host, scope, state, stateStorage, alarmAt } = harness();
    const originalList = stateStorage.list.bind(stateStorage);
    let listCalls = 0;
    let finalListStarted: () => void = () => undefined;
    let releaseFinalList: () => void = () => undefined;
    const finalStarted = new Promise<void>((resolve) => {
      finalListStarted = resolve;
    });
    const finalBlocked = new Promise<void>((resolve) => {
      releaseFinalList = resolve;
    });
    vi.spyOn(stateStorage, 'list').mockImplementation(async (options) => {
      listCalls += 1;
      if (listCalls === 2) {
        finalListStarted();
        await finalBlocked;
      }
      return originalList(options);
    });
    let releaseStream: () => void = () => undefined;
    let streamStarted: () => void = () => undefined;
    const streamReady = new Promise<void>((resolve) => {
      streamStarted = resolve;
    });
    mocked.stream.mockImplementationOnce(() => {
      streamStarted();
      return new Promise((resolve) => {
        releaseStream = () => resolve({});
      });
    });
    let journalWritten: () => void = () => undefined;
    const journalReady = new Promise<void>((resolve) => {
      journalWritten = resolve;
    });
    const originalPut = stateStorage.put.bind(stateStorage);
    vi.spyOn(stateStorage, 'put').mockImplementation(async (key, value) => {
      await originalPut(key, value);
      if (key.startsWith(OWNER_RECOVERY_PREFIX)) journalWritten();
    });

    const recovery = host.recoverOwnership(scope);
    await finalStarted;
    const start = host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    releaseFinalList();
    await recovery;
    await Promise.race([
      journalReady,
      start.then(() => {
        throw new Error(
          'start completed before its recovery journal was observed',
        );
      }),
    ]);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(alarmAt()).toBeDefined();

    await streamReady;
    releaseStream();
    await start;
  });
});

describe('createThreadAgentHost', () => {
  it('discards the live executing agent-schedule lease during terminal cleanup so a later tick cannot redispatch', async () => {
    const schedules = await executingAgentSchedule();
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    const cancelActiveExecution = vi.fn(async () => true);
    const terminateAsPrincipal = vi.fn(async () => ({
      summary,
      transitioned: true,
      casMatched: true,
      cleanup: {
        revision: 2,
        status: 'cancelled' as const,
        cleanupCompleted: false,
        scheduleDispatch: {
          scheduleId: SCHEDULE_ID,
          dispatchId: DISPATCH_ID,
        },
      },
    }));
    const completeTerminalCleanup = vi.fn(async () => summary);
    const fixture = harness(['writer'], {
      runtime: {
        cancelActiveExecution,
        terminateAsPrincipal,
        completeTerminalCleanup,
      },
      discardScheduleDispatch: (scheduleId, dispatchId, runId) =>
        schedules.discardAgentScheduleDispatch(scheduleId, dispatchId, runId),
    });
    fixture.setSummary(summary);
    fixture.state.set(TEST_RUN_RECORD_KEY, {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'schedule.fire',
    });
    await fixture.resources.claim('run', 'acme_run', HUMAN_OWNER);

    const response = await fixture.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate?resourceId=${RESOURCE_ID}`,
        { method: 'POST' },
      ),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    expect(cancelActiveExecution).toHaveBeenCalledOnce();
    expect(terminateAsPrincipal).toHaveBeenCalledOnce();
    expect(completeTerminalCleanup).toHaveBeenCalledWith(
      'durable-agentic-loop',
      'acme_run',
      2,
    );
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();
    await expect(
      schedules.beginAgentScheduleDispatch(
        SCHEDULE_ID,
        DISPATCH_ID,
        SCHEDULE_NOW + 1,
        60_000,
      ),
    ).resolves.toEqual({
      state: 'settled',
      receipt: {
        action: 'discard',
        outcome: 'discarded',
        runId: 'acme_run',
      },
    });
  });

  it('finishes cleanup when tick bookkeeping finalized the exact run first', async () => {
    const schedules = await executingAgentSchedule();
    await schedules.settleAgentScheduleDispatch(SCHEDULE_ID, DISPATCH_ID, {
      action: 'wake',
      outcome: 'succeeded',
      runId: 'acme_run',
      signalId: DISPATCH_ID,
    });
    await schedules.recordTrigger({
      id: DISPATCH_ID,
      scheduleId: SCHEDULE_ID,
      runId: 'acme_run',
      scheduledFireAt: SCHEDULE_NOW,
      actualFireAt: SCHEDULE_NOW,
      outcome: 'succeeded',
      metadata: { action: 'wake', signalId: DISPATCH_ID },
    });
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    const completeTerminalCleanup = vi.fn(async () => summary);
    const discardScheduleDispatch = vi.fn(
      (scheduleId: string, dispatchId: string, runId: string) =>
        schedules.discardAgentScheduleDispatch(scheduleId, dispatchId, runId),
    );
    const fixture = harness(['writer'], {
      runtime: {
        cancelActiveExecution: vi.fn(async () => false),
        terminateAsPrincipal: vi.fn(async () => ({
          summary,
          transitioned: true,
          casMatched: true,
          cleanup: {
            revision: 2,
            status: 'cancelled' as const,
            cleanupCompleted: false,
            scheduleDispatch: {
              scheduleId: SCHEDULE_ID,
              dispatchId: DISPATCH_ID,
            },
          },
        })),
        completeTerminalCleanup,
      },
      discardScheduleDispatch,
    });
    fixture.setSummary(summary);
    fixture.state.set(TEST_RUN_RECORD_KEY, {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'schedule.fire',
    });
    await fixture.resources.claim('run', 'acme_run', HUMAN_OWNER);

    const response = await fixture.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate?resourceId=${RESOURCE_ID}`,
        { method: 'POST' },
      ),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    expect(discardScheduleDispatch).toHaveBeenCalledWith(
      SCHEDULE_ID,
      DISPATCH_ID,
      'acme_run',
    );
    expect(completeTerminalCleanup).toHaveBeenCalledWith(
      'durable-agentic-loop',
      'acme_run',
      2,
    );
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();
    await expect(schedules.listTriggers(SCHEDULE_ID)).resolves.toEqual([
      expect.objectContaining({
        id: DISPATCH_ID,
        runId: 'acme_run',
        outcome: 'succeeded',
      }),
    ]);
  });

  it('re-drives cleanup after the terminal marker commits but its response is lost', async () => {
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    let cleanupCompleted = false;
    const completeTerminalCleanup = vi.fn(async () => {
      cleanupCompleted = true;
      throw new Error('cleanup marker response lost');
    });
    const terminateAsPrincipal = vi.fn(async () => ({
      summary,
      transitioned: !cleanupCompleted,
      casMatched: true,
      cleanup: {
        revision: 2,
        status: 'cancelled' as const,
        cleanupCompleted,
      },
    }));
    const fixture = harness(['writer'], {
      runtime: {
        cancelActiveExecution: vi.fn(async () => false),
        terminateAsPrincipal,
        completeTerminalCleanup,
      },
    });
    fixture.setSummary(summary);
    fixture.state.set(TEST_RUN_RECORD_KEY, {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'http.start',
    });
    await fixture.resources.claim('run', 'acme_run', HUMAN_OWNER);
    const terminateUrl =
      `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate` +
      `?resourceId=${RESOURCE_ID}`;

    await expect(
      fixture.host.route(
        new Request(terminateUrl, { method: 'POST' }),
        fixture.scope,
      ),
    ).rejects.toThrow('cleanup marker response lost');
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();

    const replay = await fixture.host.route(
      new Request(`${terminateUrl}&replay=1`, { method: 'POST' }),
      fixture.scope,
    );

    expect(replay?.status).toBe(200);
    expect(terminateAsPrincipal).toHaveBeenCalledTimes(2);
    expect(completeTerminalCleanup).toHaveBeenCalledOnce();
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
  });

  it('rejects a malformed terminal recovery journal before journal-driven ownership mutations', async () => {
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    const resourceAccess = new InMemoryResourceOwnershipStore();
    const settleReservation = vi.spyOn(resourceAccess, 'settleReservation');
    const release = vi.spyOn(resourceAccess, 'release');
    const completeTerminalCleanup = vi.fn(async () => summary);
    const fixture = harness(['writer'], {
      resourceAccess,
      runtime: {
        cancelActiveExecution: vi.fn(async () => false),
        terminateAsPrincipal: vi.fn(async () => ({
          summary,
          transitioned: true,
          casMatched: true,
          cleanup: {
            revision: 2,
            status: 'cancelled' as const,
            cleanupCompleted: false,
          },
        })),
        completeTerminalCleanup,
      },
    });
    fixture.setSummary(summary);
    seedRecoveryState(
      fixture.state,
      'acme_run',
      ownerRecovery('acme_run', { token: 'invalid/token', threaded: false }),
      false,
    );
    await resourceAccess.claim('thread', 'acme_thread', HUMAN_OWNER);
    await resourceAccess.claim('resource', RESOURCE_ID, HUMAN_OWNER);
    await resourceAccess.claim('run', 'acme_run', HUMAN_OWNER);

    await expect(
      fixture.host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate?resourceId=${RESOURCE_ID}`,
          { method: 'POST' },
        ),
        fixture.scope,
      ),
    ).rejects.toThrow('stored agent owner recovery is malformed');

    expect(settleReservation).not.toHaveBeenCalled();
    expect(release).not.toHaveBeenCalled();
    await expect(
      resourceAccess.owner('thread', 'acme_thread'),
    ).resolves.toEqual(HUMAN_OWNER);
    await expect(
      resourceAccess.owner('resource', RESOURCE_ID),
    ).resolves.toEqual(HUMAN_OWNER);
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(completeTerminalCleanup).not.toHaveBeenCalled();
  });

  it('cancels a live execution before waiting for the schedule dispatch lock', async () => {
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    let dispatchEntered!: () => void;
    let finishDispatch!: () => void;
    const entered = new Promise<void>((resolve) => {
      dispatchEntered = resolve;
    });
    const held = new Promise<void>((resolve) => {
      finishDispatch = resolve;
    });
    const cancelActiveExecution = vi.fn(async () => {
      finishDispatch();
      return true;
    });
    const fixture = harness(['writer'], {
      runtime: {
        cancelActiveExecution,
        terminateAsPrincipal: vi.fn(async () => ({
          summary,
          transitioned: true,
          casMatched: true,
          cleanup: {
            revision: 2,
            status: 'cancelled' as const,
            cleanupCompleted: false,
          },
        })),
        completeTerminalCleanup: vi.fn(async () => summary),
      },
    });
    fixture.setSummary({ runId: 'acme_run', status: 'running' });
    fixture.state.set(TEST_RUN_RECORD_KEY, {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'schedule.fire',
    });
    await fixture.resources.claim('run', 'acme_run', HUMAN_OWNER);
    const dispatching = fixture.host.serializeDispatch(async () => {
      dispatchEntered();
      await held;
    });
    await entered;

    const response = await fixture.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate?resourceId=${RESOURCE_ID}`,
        { method: 'POST' },
      ),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    expect(cancelActiveExecution).toHaveBeenCalled();
    await expect(dispatching).resolves.toBeUndefined();
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
  });

  it.each([
    '__proto__',
    'constructor',
  ])("resolves prototype-collision agent id '%s'", async (agentId) => {
    const { host, scope } = harness([agentId]);

    await host.start(scope, {
      agentId,
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    expect(mocked.stream).toHaveBeenCalledOnce();
  });

  it('constructs cached dependencies with actor-free instance scope', async () => {
    const { host, scope, moduleScopes, storageScopes } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    expect(Object.keys(moduleScopes[0] ?? {}).sort()).toEqual([
      'deploymentTag',
      'init',
      'threadId',
    ]);
    expect(Object.keys(storageScopes[0] ?? {}).sort()).toEqual([
      'deploymentTag',
      'init',
      'threadId',
    ]);
    expect(moduleScopes[0]).toBe(storageScopes[0]);
    expect(Object.isFrozen(moduleScopes[0])).toBe(true);
  });

  it('derives trusted actor/correlation after the exact-leg grant', async () => {
    const { host, scope } = harness();
    let release: (() => void) | undefined;
    const blocked = new Promise<void>((resolve) => {
      release = resolve;
    });
    mocked.stream.mockImplementation(async () => {
      await blocked;
      return {};
    });
    const provider = host.requestContextForRun((async () => ({
      'breakwater.connectorGrants': [],
      'breakwater.actor': { id: 'forged' },
    })) satisfies RequestContextProvider);
    const started = host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
      safeContext: {
        stored: 'survives',
        runId: 'forged',
        'breakwater.actor': { id: 'forged-from-stored-state' },
      },
    });
    await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalledOnce());
    await expect(
      provider('durable-agentic-loop', 'acme_run', { kind: 'start' }),
    ).resolves.toMatchObject({
      'breakwater.connectorGrants': [],
      stored: 'survives',
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      'breakwater.auditContext': {
        agentId: 'writer',
        entryPath: 'http.start',
      },
    });
    await expect(
      provider('unrelated-workflow', 'acme_run', { kind: 'start' }),
    ).resolves.toEqual({
      'breakwater.connectorGrants': [],
      'breakwater.actor': { id: 'forged' },
    });
    release?.();
    await started;
    const streamOptions = mocked.stream.mock.calls[0]?.[1];
    expect(streamOptions.requestContext.get('stored')).toBe('survives');
    expect(streamOptions.requestContext.get('runId')).toBe('acme_run');
    expect(streamOptions.requestContext.get('breakwater.actor')).toEqual({
      id: 'operator-1',
      role: 'operator',
      kind: 'human',
    });
  });

  it('rejects a simultaneous operation before a second actor can overwrite the persisted principal', async () => {
    const { host, scope, state } = harness();
    let release: (() => void) | undefined;
    mocked.stream.mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () => resolve({});
        }),
    );
    const input = {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start' as const,
    };
    const first = host.start(scope, input);
    await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalledOnce());
    const secondScope: ThreadScope = {
      ...scope,
      principal: { kind: 'human', id: 'operator-2', role: 'operator' },
    };
    await expect(host.start(secondScope, input)).rejects.toMatchObject({
      status: 503,
    });
    expect(state.get('flowsafe:agent-run:v1:acme_run')).toMatchObject({
      principal: { id: 'operator-1' },
    });
    release?.();
    await first;
  });

  it('serializes first binding so concurrent different-agent starts cannot replace it', async () => {
    const { host, scope, state } = harness(['writer', 'reviewer']);
    const starts = [
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_writer',
        prompt: 'writer prompt',
        entryPath: 'http.start',
      }),
      host.start(scope, {
        agentId: 'reviewer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_reviewer',
        prompt: 'reviewer prompt',
        entryPath: 'http.start',
      }),
    ];
    const results = await Promise.allSettled(starts);
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    const rejected = results.find((result) => result.status === 'rejected');
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(rejected).toMatchObject({ reason: { status: 409 } });
    const binding = structuredClone(
      state.get('flowsafe:agent-thread-binding:v1'),
    ) as { agentId: string };
    expect(['writer', 'reviewer']).toContain(binding.agentId);
    const losingAgent = binding.agentId === 'writer' ? 'reviewer' : 'writer';
    await expect(
      host.start(scope, {
        agentId: losingAgent,
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run_loser_retry',
        prompt: 'retry',
        entryPath: 'http.start',
      }),
    ).rejects.toMatchObject({ status: 409 });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
  });

  it('rejects a stored binding whose resource does not belong to the thread', async () => {
    const { host, scope, state } = harness();
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: 'globex_resource',
    });

    await expect(
      host.resolveBoundAgent(scope, {
        agentId: 'writer',
        entryPath: 'signal.message',
      }),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('exposes only a matching durable binding to standing-state adapters', async () => {
    const { host, scope, state } = harness();
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });

    const bound = await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/binding?resourceId=${RESOURCE_ID}&agentId=writer`,
      ),
      scope,
    );
    expect(bound?.status).toBe(200);

    await expect(
      host.route(
        new Request(
          'https://thread/_flowsafe/agent-host/binding?resourceId=other_resource&agentId=writer',
        ),
        scope,
      ),
    ).rejects.toMatchObject({ status: 404 });
  });

  it('rejects replay of an existing durable run before a later actor can recreate metadata', async () => {
    const { host, scope, state } = harness();
    const input = {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start' as const,
    };
    await host.start(scope, input);
    const secondScope: ThreadScope = {
      ...scope,
      principal: { kind: 'human', id: 'operator-2', role: 'operator' },
    };
    await expect(host.start(secondScope, input)).rejects.toMatchObject({
      status: 409,
    });
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it.each([
    'running',
    'waiting',
  ] as const)('retains the execution principal while authoritative status is %s', async (status) => {
    const { host, scope, state, setSummary } = harness();
    setSummary({ runId: 'acme_run', status });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'http.start',
    });
    await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
      ),
      scope,
    );
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(true);
  });

  it('deletes the execution principal after authoritative terminal status', async () => {
    const { host, scope, state, setSummary } = harness();
    setSummary({ runId: 'acme_run', status: 'success' });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'http.start',
    });
    await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
      ),
      scope,
    );
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it('reconciles approvals while recovering a lost unthreaded schedule receipt', async () => {
    const createAsPrincipal = vi.fn(
      async (input: {
        workflowId: string;
        runId: string;
        stepPath?: string[];
      }) => ({
        record: {
          id: 'approval-1',
          ...input,
        } as ApprovalRecord,
        created: true,
      }),
    );
    const approvalService = {
      list: async () => [],
      createAsPrincipal,
    } as unknown as ApprovalService;
    const { host, scope, state, approvalScopes, setSummary, setSnapshot } =
      harness(['writer'], { approvalService });
    setSummary({
      runId: 'acme_run',
      status: 'suspended',
      requestedBy: 'flowsafe-scheduler',
      requestedByKind: 'system',
      suspended: [['tool']],
      suspendedAt: { tool: 123 },
      resumeCount: { tool: 0 },
      suspendPayload: { tool: { connectorId: 'connector-a' } },
    });
    setSnapshot({ memory: false });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: {
        kind: 'system',
        id: 'flowsafe-scheduler',
        purpose: 'scheduled-agent-execution',
      },
      originEntryPath: 'schedule.fire',
    });

    await expect(
      host.scheduleDispatchStatus(scope, {
        agentId: 'writer',
        resourceId: RESOURCE_ID,
        runId: 'acme_run',
      }),
    ).resolves.toMatchObject({ status: 'suspended' });

    expect(createAsPrincipal).toHaveBeenCalledWith(
      expect.objectContaining({
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run',
        stepPath: ['tool'],
        suspendedAt: 123,
        resumeCount: 0,
        requestedBy: 'flowsafe-scheduler',
        requestedByKind: 'system',
      }),
      expect.anything(),
      expect.objectContaining({
        kind: 'agent-thread',
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
      }),
    );
    expect(approvalScopes).toHaveLength(1);
  });

  it('retains prepared-unfenced metadata when failed start has no durable outcome', async () => {
    const { host, scope, state, setSummary } = harness();
    setSummary(null);
    mocked.stream.mockRejectedValue(new Error('model unavailable'));
    await expect(
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_run',
        prompt: 'go',
        entryPath: 'http.start',
      }),
    ).rejects.toThrow('model unavailable');
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(true);
  });

  it('keeps run metadata and names the failure when a failed start cannot read authoritative state', async () => {
    // #given — the same failed start, with the read that would tell an
    // interrupted start apart from a failed one refusing to answer from state
    // it could not reach.
    const { host, scope, state, alarmAt } = harness(['writer'], {
      runtime: {
        authoritativeStartState: vi.fn(async () => {
          throw new RunStateUnreadableError('durable-agentic-loop', 'acme_run');
        }),
      },
    });
    mocked.stream.mockRejectedValue(new Error('model unavailable'));
    const logged: string[] = [];
    const log = vi
      .spyOn(console, 'error')
      .mockImplementation((...args: unknown[]) => {
        logged.push(String(args[0]));
      });

    // #when
    try {
      await expect(
        host.start(scope, {
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          prompt: 'go',
          entryPath: 'http.start',
        }),
        // #then — the caller still sees the ORIGINAL start failure: the read
        // concluded nothing, so it cannot reclassify one.
      ).rejects.toThrow('model unavailable');
    } finally {
      log.mockRestore();
    }

    // #then — and the failure is named rather than swallowed, because it is
    // the reason the metadata above survives and a wake is left to retry it.
    expect(logged).toContain('agent owner recovery failed');
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(true);
    expect(alarmAt()).toBeDefined();
  });

  it('does not create a reusable binding for an unthreaded ephemeral run', async () => {
    const fixture = harness();
    const { host, scope, state } = fixture;
    await seedThreadlessSchedule(fixture);
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      threaded: false,
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    });
    expect([...state.keys()]).not.toContain('flowsafe:agent-thread-binding:v1');
    expect(mocked.stream).toHaveBeenCalledWith(
      'scheduled',
      expect.not.objectContaining({ memory: expect.anything() }),
      'operator-1',
      'human',
      expect.any(String),
      // The schedule dispatch and the reserved idempotency key: passed
      // positionally on every start, and undefined on one that has neither.
      undefined,
      undefined,
      {
        startIdentity: {
          owner: HUMAN_OWNER,
          target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
        },
        agentStart: { threaded: false },
        onPreparedStartIdentity: expect.any(Function),
        runOwnerGuard: {
          owner: HUMAN_OWNER,
          reservationToken: expect.any(String),
        },
      },
    );
  });

  it('terminates a direct scheduled start without settling an unbegun agent-dispatch lease', async () => {
    const summary: RunSummary = {
      runId: 'acme_run',
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    };
    const discardScheduleDispatch = vi.fn(async () => undefined);
    const fixture = harness(['writer'], {
      runtime: {
        cancelActiveExecution: vi.fn(async () => false),
        terminateAsPrincipal: vi.fn(async () => ({
          summary,
          transitioned: true,
          casMatched: true,
          cleanup: {
            revision: 2,
            status: 'cancelled' as const,
            cleanupCompleted: false,
          },
        })),
        completeTerminalCleanup: vi.fn(async () => summary),
      },
      discardScheduleDispatch,
    });
    await seedThreadlessSchedule(fixture);
    fixture.setSummary({ runId: 'acme_run', status: 'suspended' }, false);
    await fixture.host.start(fixture.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      threaded: false,
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    });
    await expect(
      fixture.resources.owner('thread', 'acme_thread'),
    ).resolves.toEqual(HUMAN_OWNER);
    await expect(
      fixture.resources.owner('resource', RESOURCE_ID),
    ).resolves.toEqual(HUMAN_OWNER);
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);

    const response = await fixture.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/terminate?resourceId=${RESOURCE_ID}`,
        { method: 'POST' },
      ),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    // Five host arguments, two undefined optionals (schedule dispatch and
    // reserved idempotency key), then the required captured authority.
    expect(mocked.stream.mock.calls.at(-1)).toHaveLength(8);
    expect(discardScheduleDispatch).not.toHaveBeenCalled();
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();
    await expect(
      fixture.resources.owner('thread', 'acme_thread'),
    ).resolves.toBeUndefined();
    await expect(
      fixture.resources.owner('resource', RESOURCE_ID),
    ).resolves.toBeUndefined();
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
  });

  it('executes stored schedule prompt, context, and provider options instead of body payload', async () => {
    const fixture = harness();
    await seedScheduleOwner(
      fixture,
      {
        type: 'agent',
        agentId: 'writer',
        prompt: 'stored prompt',
        requestContext: { source: 'stored-context' },
        providerOptions: { model: { temperature: 0.2 } },
      },
      HUMAN_OWNER,
      SCHEDULE_ID,
      DISPATCH_ID,
      'acme_stored_payload',
    );

    const response = await fixture.host.route(
      new Request('https://thread/_flowsafe/agent-host/start', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_stored_payload',
          prompt: 'forged prompt',
          entryPath: 'schedule.fire',
          threaded: false,
          scheduleId: SCHEDULE_ID,
          dispatchId: DISPATCH_ID,
          safeContext: { source: 'forged-context' },
          providerOptions: { model: { temperature: 2 } },
        }),
      }),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    expect(mocked.stream).toHaveBeenCalledOnce();
    const [messages, options] = mocked.stream.mock.calls[0] ?? [];
    expect(messages).toBe('stored prompt');
    expect(options?.requestContext.get('source')).toBe('stored-context');
    expect(options?.providerOptions).toEqual({
      model: { temperature: 0.2 },
    });
  });

  it('commits suspended unthreaded ownership and releases only ephemeral ids after resume', async () => {
    const scheduler: ExecutionPrincipal = {
      kind: 'system',
      id: 'system-scheduler',
      purpose: 'scheduled-agent-execution',
    };
    const fixture = harness(['writer'], {
      principal: scheduler,
      allowedAutomation: [{ kind: 'system', entryPaths: ['schedule.fire'] }],
    });
    const { host, scope, resources, setSnapshot, setSummary } = fixture;
    const owner = HUMAN_OWNER;
    await seedThreadlessSchedule(fixture, owner);
    setSnapshot({ memory: false });
    mocked.stream.mockImplementation(async () => {
      setSummary({
        runId: 'acme_run',
        status: 'suspended',
        requestedBy: 'system-scheduler',
      });
      return {};
    });

    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      threaded: false,
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    });

    await expect(resources.owner('thread', 'acme_thread')).resolves.toEqual(
      owner,
    );
    await expect(resources.owner('resource', RESOURCE_ID)).resolves.toEqual(
      owner,
    );
    await expect(resources.owner('run', 'acme_run')).resolves.toEqual(owner);

    mocked.resumeViaRuntime.mockResolvedValue({
      runId: 'acme_run',
      status: 'success',
      requestedBy: 'reviewer-1',
    });
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          requestedBy: 'reviewer-1',
          resumeData: { approved: true },
        }),
      }),
      scope,
    );

    expect(response?.status).toBe(200);
    await expect(
      resources.owner('thread', 'acme_thread'),
    ).resolves.toBeUndefined();
    await expect(
      resources.owner('resource', RESOURCE_ID),
    ).resolves.toBeUndefined();
    await expect(resources.owner('run', 'acme_run')).resolves.toEqual(owner);
  });

  it.each([
    ['absent', undefined],
    [
      'mismatched',
      {
        version: 1,
        agentId: 'reviewer',
        resourceId: RESOURCE_ID,
      },
    ],
  ] as const)('refuses a threaded scheduled start when the stored binding is %s', async (_label, binding) => {
    const fixture = harness(['writer', 'reviewer']);
    const { host, scope, state } = fixture;
    await seedThreadedSchedule(fixture);
    if (binding) {
      state.set('flowsafe:agent-thread-binding:v1', binding);
    }
    await expect(
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_scheduled',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('starts a threaded schedule only through its matching stored binding', async () => {
    const fixture = harness();
    const { host, scope, state } = fixture;
    await seedThreadedSchedule(fixture);
    const binding = {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    };
    state.set('flowsafe:agent-thread-binding:v1', binding);
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_scheduled',
      prompt: 'scheduled',
      entryPath: 'schedule.fire',
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    });
    expect(state.get('flowsafe:agent-thread-binding:v1')).toEqual(binding);
    expect(mocked.stream).toHaveBeenCalledOnce();
  });

  it('rejects caller-supplied start ownership and requester fields before reserving or running', async () => {
    const { host, scope, resourceAccess } = harness();
    const reserve = vi.spyOn(resourceAccess, 'reserveAll');

    await expect(
      host.route(
        new Request('https://thread/_flowsafe/agent-host/start', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            agentId: 'writer',
            threadId: 'acme_thread',
            resourceId: RESOURCE_ID,
            runId: 'acme_forged',
            prompt: 'go',
            entryPath: 'http.start',
            resourceOwner: { kind: 'system', id: 'forged-owner' },
            requestedBy: 'forged-requester',
            requestedByKind: 'system',
          }),
        }),
        scope,
      ),
    ).rejects.toMatchObject({ status: 400 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('derives an ordinary start owner and requester kind from the trusted principal', async () => {
    const principal: ExecutionPrincipal = {
      kind: 'service',
      id: 'webhook-dispatcher',
      purpose: 'trusted-http-start',
    };
    const fixture = harness(['writer'], {
      principal,
      allowedAutomation: [{ kind: 'service', entryPaths: ['http.start'] }],
    });

    await fixture.host.start(fixture.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_service_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    expect(mocked.stream).toHaveBeenCalledWith(
      'go',
      expect.anything(),
      'webhook-dispatcher',
      'service',
      expect.any(String),
      undefined,
      undefined,
      {
        startIdentity: {
          owner: { kind: 'service', id: 'webhook-dispatcher' },
          target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
        },
        agentStart: { threaded: true },
        onPreparedStartIdentity: expect.any(Function),
        runOwnerGuard: {
          owner: { kind: 'service', id: 'webhook-dispatcher' },
          reservationToken: expect.any(String),
        },
      },
    );
    await expect(
      fixture.resources.owner('run', 'acme_service_run'),
    ).resolves.toEqual({ kind: 'service', id: 'webhook-dispatcher' });
  });

  it('rejects a schedule fire without its explicit source id before reserving or running', async () => {
    const { host, scope, resourceAccess } = harness();
    const reserve = vi.spyOn(resourceAccess, 'reserveAll');

    await expect(
      host.start(scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_missing_schedule_source',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        threaded: false,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    ['missing prepared trigger id', 'acme_authorized_run', undefined],
    ['different prepared trigger id', 'acme_authorized_run', 'other_dispatch'],
    ['different run id', 'acme_borrowed_run', DISPATCH_ID],
  ] as const)('rejects a schedule source paired with a %s', async (_label, runId, dispatchId) => {
    const fixture = harness();
    await seedThreadlessSchedule(
      fixture,
      HUMAN_OWNER,
      SCHEDULE_ID,
      DISPATCH_ID,
      'acme_authorized_run',
    );
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');

    await expect(
      fixture.host.start(fixture.scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId,
        prompt: 'forged',
        entryPath: 'schedule.fire',
        threaded: false,
        scheduleId: SCHEDULE_ID,
        ...(dispatchId !== undefined ? { dispatchId } : {}),
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('rejects a deleted schedule source even when its ownership claim remains committed', async () => {
    const fixture = harness();
    await seedThreadlessSchedule(
      fixture,
      HUMAN_OWNER,
      SCHEDULE_ID,
      DISPATCH_ID,
      'acme_deleted_schedule',
    );
    fixture.schedules.delete(SCHEDULE_ID);
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');

    await expect(
      fixture.host.start(fixture.scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_deleted_schedule',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        threaded: false,
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('rejects an uncommitted schedule owner before reserving or running', async () => {
    const fixture = harness();
    fixture.schedules.set(SCHEDULE_ID, {
      target: { type: 'agent', agentId: 'writer', prompt: 'scheduled' },
      dispatchId: DISPATCH_ID,
      runId: 'acme_pending_schedule',
    });
    await fixture.resources.reserveAll(
      [{ kind: 'schedule', resourceId: SCHEDULE_ID }],
      HUMAN_OWNER,
      'pending-schedule-owner',
    );
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');

    await expect(
      fixture.host.start(fixture.scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_pending_schedule',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        threaded: false,
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    [
      'a threadless target replayed as a threaded wake',
      { type: 'agent', agentId: 'writer', prompt: 'scheduled' },
      true,
    ],
    [
      'a threaded target replayed as a threadless start',
      {
        type: 'agent',
        agentId: 'writer',
        prompt: 'scheduled',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
      },
      false,
    ],
    [
      'a different fixed thread',
      {
        type: 'agent',
        agentId: 'writer',
        prompt: 'scheduled',
        threadId: 'acme_other_thread',
        resourceId: RESOURCE_ID,
      },
      true,
    ],
    [
      'an idle-persist target replayed as a wake',
      {
        type: 'agent',
        agentId: 'writer',
        prompt: 'scheduled',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        ifIdle: { behavior: 'persist' },
      },
      true,
    ],
    [
      'an idle-discard target replayed as a wake',
      {
        type: 'agent',
        agentId: 'writer',
        prompt: 'scheduled',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        ifIdle: { behavior: 'discard' },
      },
      true,
    ],
  ] as const)('rejects %s before reserving or running', async (_label, target, threaded) => {
    const fixture = harness();
    await seedThreadOwner(fixture);
    await seedScheduleOwner(
      fixture,
      target,
      HUMAN_OWNER,
      SCHEDULE_ID,
      DISPATCH_ID,
      'acme_schedule_target_mismatch',
    );
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');

    await expect(
      fixture.host.start(fixture.scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_schedule_target_mismatch',
        prompt: 'scheduled',
        entryPath: 'schedule.fire',
        threaded,
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('binds a signal wake to the common committed thread/resource owner while preserving requester kind', async () => {
    const principal: ExecutionPrincipal = {
      kind: 'system',
      id: 'signal-dispatcher',
      purpose: 'signal-wake',
    };
    const fixture = harness(['writer'], {
      principal,
      allowedAutomation: [{ kind: 'system', entryPaths: ['signal.wake'] }],
    });
    await seedThreadOwner(fixture);
    fixture.state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });

    await fixture.host.start(fixture.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_signal_wake',
      prompt: 'wake',
      entryPath: 'signal.wake',
    });

    expect(mocked.stream).toHaveBeenCalledWith(
      'wake',
      expect.anything(),
      'signal-dispatcher',
      'system',
      expect.any(String),
      undefined,
      undefined,
      {
        startIdentity: {
          owner: { kind: 'system', id: 'signal-dispatcher' },
          target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
        },
        agentStart: { threaded: true },
        onPreparedStartIdentity: expect.any(Function),
        runOwnerGuard: {
          owner: HUMAN_OWNER,
          reservationToken: expect.any(String),
        },
      },
    );
    await expect(
      fixture.resources.owner('run', 'acme_signal_wake'),
    ).resolves.toEqual(HUMAN_OWNER);
  });

  it('rejects a signal wake whose committed thread and resource owners differ', async () => {
    const principal: ExecutionPrincipal = {
      kind: 'system',
      id: 'signal-dispatcher',
      purpose: 'signal-wake',
    };
    const fixture = harness(['writer'], {
      principal,
      allowedAutomation: [{ kind: 'system', entryPaths: ['signal.wake'] }],
    });
    await fixture.resources.claim('thread', 'acme_thread', HUMAN_OWNER);
    await fixture.resources.claim('resource', RESOURCE_ID, {
      kind: 'service',
      id: 'different-owner',
    });
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');

    await expect(
      fixture.host.start(fixture.scope, {
        agentId: 'writer',
        threadId: 'acme_thread',
        resourceId: RESOURCE_ID,
        runId: 'acme_mismatched_wake',
        prompt: 'wake',
        entryPath: 'signal.wake',
      }),
    ).rejects.toMatchObject({ status: 404 });
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('serializes dispatch status behind an in-flight target start', async () => {
    const { host, scope } = harness();
    let finishStream: (() => void) | undefined;
    mocked.stream.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishStream = resolve;
        }),
    );
    const body = {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
      threaded: true,
    };
    const start = host.route(
      new Request('https://thread/_flowsafe/agent-host/start', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      }),
      scope,
    );
    await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalledOnce());

    let statusSettled = false;
    const status = host
      .route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}&dispatch=1`,
        ),
        scope,
      )
      .finally(() => {
        statusSettled = true;
      });
    await Promise.resolve();
    expect(statusSettled).toBe(false);

    finishStream?.();
    expect((await start)?.status).toBe(200);
    const statusResponse = await status;
    expect(statusResponse?.status).toBe(200);
    expect(await statusResponse?.json()).toMatchObject({
      runId: 'acme_run',
      summary: { status: 'success' },
    });
  });

  it('fails dispatch status closed when the pending recovery cannot read authoritative state', async () => {
    // #given — a dispatch-status read whose pending owner recovery runs first,
    // with the read that would settle it refusing to answer from state it
    // could not reach.
    const { host, scope, state, resources, resourceAccess } = harness(
      ['writer'],
      {
        runtime: {
          recoverStartAttempt: vi.fn(async () => {
            throw new RunStateUnreadableError(
              'durable-agentic-loop',
              'acme_run',
            );
          }),
        },
      },
    );
    const settle = vi.spyOn(resourceAccess, 'settleReservation');
    seedRecoveryState(state, 'acme_run', ownerRecovery('acme_run'));
    await resources.reserveAll(
      [
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
        { kind: 'run', resourceId: 'acme_run' },
      ],
      { kind: 'human', id: 'operator-1' },
      'token-acme_run',
    );

    // #when
    const raised = await host
      .route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}&dispatch=1`,
        ),
        scope,
      )
      .catch((error: unknown) => error);

    // #then — the route escapes to the Durable Object shell, which answers the
    // retryable 503 this release documents rather than a 200 assembled from a
    // read that never happened. The recovery stays owed: journal, run record
    // and reservation all survive for a wake that can read.
    expect(raised).toBeInstanceOf(RunStateUnreadableError);
    expect(doErrorResponse(raised).status).toBe(503);
    expect(settle).not.toHaveBeenCalled();
    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(await resources.owner('run', 'acme_run')).toBeUndefined();
  });

  it('keeps contradictory snapshot correlation unreadable', async () => {
    const { host, scope, setSnapshot } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    setSnapshot({
      threadId: 'acme_other-thread',
      resourceId: 'acme_other-resource',
    });
    await expect(
      host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
        ),
        scope,
      ),
    ).rejects.toBeInstanceOf(RunStateUnreadableError);
  });

  it('rehydrates a threaded approval resume with the validated memory binding', async () => {
    const { host, scope, state, approvalScopes, setSummary, setSnapshot } =
      harness();
    const taggedScope: ThreadScope = { ...scope, deploymentTag: 'acme' };
    setSummary({
      runId: 'acme_run',
      status: 'suspended',
      requestedBy: 'operator-1',
    });
    setSnapshot({
      requestContext: {
        persistedSafe: 'survives-resume',
        runId: 'forged',
        'breakwater.actor': { id: 'forged' },
        'breakwater.connectorGrants': ['stale'],
      },
    });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'http.start',
    });
    const provider = host.requestContextForRun(async () => ({
      'breakwater.connectorGrants': [],
    }));
    let resumedContext: Record<string, unknown> | undefined;
    mocked.resumeViaRuntime.mockImplementation(async () => {
      resumedContext = await provider('durable-agentic-loop', 'acme_run', {
        kind: 'resume',
        step: ['tool'],
        resumeCount: 1,
      });
      return {
        runId: 'acme_run',
        status: 'success',
      };
    });
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          requestedBy: 'reviewer-1',
          resumeData: { approved: true },
        }),
      }),
      taggedScope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.resumeViaRuntime).toHaveBeenCalledWith(
      expect.objectContaining({
        memory: { thread: 'acme_thread', resource: RESOURCE_ID },
      }),
    );
    expect(resumedContext).toMatchObject({
      persistedSafe: 'survives-resume',
      'breakwater.connectorGrants': [],
      'breakwater.actor': { id: 'operator-1', role: 'operator' },
      runId: 'acme_run',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      'breakwater.auditContext': expect.objectContaining({ tenantId: 'acme' }),
    });
    expect(Object.keys(approvalScopes[0] ?? {}).sort()).toEqual([
      'deploymentTag',
      'init',
      'threadId',
    ]);
  });

  it('accepts an approval-resume requester exactly at the principal-id bound', async () => {
    const fixture = harness();
    seedSuspendedApprovalRun(fixture);
    const requestedBy = 'r'.repeat(200);

    const response = await fixture.host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          requestedBy,
          resumeData: { approved: true },
        }),
      }),
      fixture.scope,
    );

    expect(response?.status).toBe(200);
    expect(mocked.resumeViaRuntime).toHaveBeenCalledWith(
      expect.objectContaining({ requestedBy }),
    );
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u000aforged'],
  ])('rejects an approval resume with %s', async (_label, requestedBy) => {
    const fixture = harness();
    seedSuspendedApprovalRun(fixture);

    await expect(
      fixture.host.route(
        new Request('https://thread/_flowsafe/agent-host/resume', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            agentId: 'writer',
            threadId: 'acme_thread',
            resourceId: RESOURCE_ID,
            runId: 'acme_run',
            entryPath: 'approval.resume',
            requestedBy,
            resumeData: { approved: true },
          }),
        }),
        fixture.scope,
      ),
    ).rejects.toMatchObject({ status: 400 });
    expect(mocked.resumeViaRuntime).not.toHaveBeenCalled();
  });

  it('refuses an approval resume that forges the suspension-timeout envelope', async () => {
    const fixture = harness();
    seedSuspendedApprovalRun(fixture);

    await expect(
      fixture.host.route(
        new Request('https://thread/_flowsafe/agent-host/resume', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            agentId: 'writer',
            threadId: 'acme_thread',
            resourceId: RESOURCE_ID,
            runId: 'acme_run',
            entryPath: 'approval.resume',
            requestedBy: 'reviewer-1',
            resumeData: {
              [SUSPENSION_TIMEOUT_RESUME_KEY]: {
                step: 'tool',
                deadlineAt: 1,
                expiredAt: 2,
              },
            },
          }),
        }),
        fixture.scope,
      ),
      // #then — this route forwards client resume data verbatim as a human
      // requester, so without the guard a caller could drive a step's timeout
      // branch. Only a run object's alarm mints that envelope.
    ).rejects.toMatchObject({
      status: 400,
      message: expect.stringContaining(SUSPENSION_TIMEOUT_RESUME_KEY),
    });
    expect(mocked.resumeViaRuntime).not.toHaveBeenCalled();
  });

  it('resumes an unthreaded suspended run without requiring or inventing memory', async () => {
    const { host, scope, state, setSummary, setSnapshot } = harness();
    setSummary({
      runId: 'acme_run',
      status: 'suspended',
      requestedBy: 'operator-1',
    });
    setSnapshot({ memory: false });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'schedule.fire',
    });
    mocked.resumeViaRuntime.mockResolvedValue({
      runId: 'acme_run',
      status: 'success',
    });
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          requestedBy: 'reviewer-1',
          resumeData: { approved: true },
        }),
      }),
      scope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.resumeViaRuntime).toHaveBeenCalledWith(
      expect.not.objectContaining({ memory: expect.anything() }),
    );
  });

  it('emits next-cursor NDJSON and cancels only the observation reader', async () => {
    const { host, scope } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    let cancelled = false;
    const source = new ReadableStream<unknown>({
      start(controller) {
        controller.enqueue({ type: 'text-delta', text: 'one' });
      },
      cancel() {
        cancelled = true;
      },
    });
    const cleanup = vi.fn();
    mocked.observe.mockResolvedValue({ fullStream: source, cleanup });
    mocked.getHistory.mockResolvedValue(
      Array.from({ length: 6 }, (_, index) => ({ id: String(index) })),
    );
    const response = await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=4`,
      ),
      scope,
    );
    expect(response?.headers.get('content-type')).toBe(
      'application/x-ndjson; charset=utf-8',
    );
    const reader = response?.body?.getReader();
    const first = await reader?.read();
    expect(new TextDecoder().decode(first?.value)).toContain('"offset":5');
    await reader?.cancel();
    expect(cancelled).toBe(true);
    expect(cleanup).not.toHaveBeenCalled();
  });

  it('returns 409 after replay state is lost while durable status remains', async () => {
    const first = harness();
    await first.host.start(first.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    const restarted = harness();
    restarted.state.set(
      'flowsafe:agent-thread-binding:v1',
      first.state.get('flowsafe:agent-thread-binding:v1'),
    );
    await expect(
      restarted.host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=0`,
        ),
        restarted.scope,
      ),
    ).rejects.toMatchObject({
      status: 409,
      message: expect.stringContaining('status endpoint'),
    });
  });

  it('uses external cached history after an isolate restart', async () => {
    const first = harness();
    await first.host.start(first.scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });

    const restarted = harness();
    restarted.state.set(
      'flowsafe:agent-thread-binding:v1',
      first.state.get('flowsafe:agent-thread-binding:v1'),
    );
    mocked.getHistory.mockResolvedValue([
      { id: '0' },
      { id: '1' },
      { id: '2' },
    ]);
    mocked.observe.mockResolvedValue({
      fullStream: new ReadableStream<unknown>({
        start(controller) {
          controller.enqueue({ type: 'finish' });
          controller.close();
        },
      }),
      cleanup: vi.fn(),
    });
    const response = await restarted.host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=1`,
      ),
      restarted.scope,
    );
    expect(response?.status).toBe(200);
    expect(mocked.observe).toHaveBeenCalledWith('acme_run', { offset: 1 });
  });

  it('closes immediately when a terminal reconnect cursor is at cached history end', async () => {
    const { host, scope } = harness();
    await host.start(scope, {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_run',
      prompt: 'go',
      entryPath: 'http.start',
    });
    mocked.getHistory.mockResolvedValue([{ id: '0' }, { id: '1' }]);
    const response = await host.route(
      new Request(
        `https://thread/_flowsafe/agent-host/runs/writer/acme_run/stream?resourceId=${RESOURCE_ID}&offset=2`,
      ),
      scope,
    );
    expect(response?.status).toBe(200);
    await expect(response?.text()).resolves.toBe('');
    expect(mocked.observe).not.toHaveBeenCalled();
  });
});

describe('createThreadAgentHost permission authorization', () => {
  const requiredPermissions = ['agents.run', 'reports.read'] as const;
  const startInput = {
    agentId: 'writer',
    threadId: 'acme_thread',
    resourceId: RESOURCE_ID,
    runId: 'acme_run',
    prompt: 'go',
    entryPath: 'http.start' as const,
  };

  it('requires every declared permission for a human and audits the policy snapshot without effective-authority leakage', async () => {
    const resolver = vi.fn(async (_principal: ExecutionPrincipal) => ({
      permissions: ['agents.run', 'reports.read', 'records.observe'],
      policyVersion: 'permissions-2026-08-08',
    }));
    const { host, scope, auditEvents } = harness(['writer'], {
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await host.start(scope, startInput);

    expect(resolver).toHaveBeenCalledOnce();
    expect(resolver).toHaveBeenCalledWith(scope.principal);
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(auditEvents.at(-1)).toMatchObject({
      action: 'agent.entry.authorize',
      decision: 'allowed',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'permissions-2026-08-08',
        principalKind: 'human',
        principalId: 'operator-1',
      },
    });
    expect(auditEvents.at(-1)?.detail).not.toHaveProperty('permissions');
    expect(auditEvents.at(-1)?.detail).not.toHaveProperty(
      'effectivePermissions',
    );
  });

  it('denies when any one required permission is absent', async () => {
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run'],
      policyVersion: 'permissions-v2',
    }));
    const { host, scope, auditEvents } = harness(['writer'], {
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await expect(host.start(scope, startInput)).rejects.toMatchObject({
      status: 403,
      message: 'forbidden',
    });

    expect(mocked.stream).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'denied',
      reason: 'required permissions are not satisfied',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'permissions-v2',
      },
    });
    expect(auditEvents.at(-1)?.detail).not.toHaveProperty('permissions');
  });

  it('tolerates duplicate identifiers in resolver output because a repeat cannot change an all-of decision', async () => {
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run', 'agents.run', 'reports.read'],
      policyVersion: 'permissions-v2',
    }));
    const { host, scope, auditEvents } = harness(['writer'], {
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await host.start(scope, startInput);

    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'allowed',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'permissions-v2',
      },
    });
  });

  it('fails closed when permissions are required but no resolver is configured', async () => {
    const { host, scope, auditEvents } = harness(['writer'], {
      requiredPermissions,
    });

    await expect(host.start(scope, startInput)).rejects.toMatchObject({
      status: 403,
      message: 'forbidden',
    });

    expect(mocked.stream).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'denied',
      reason: 'permission resolver is not configured',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: null,
      },
    });
  });

  it('re-resolves permissions before approval resume and stops a revoked principal', async () => {
    const resolver = vi.fn(async () => ({
      permissions: [],
      policyVersion: 'permissions-revoked-v3',
    }));
    const { host, scope, state, auditEvents, setSummary } = harness(
      ['writer'],
      {
        requiredPermissions,
        resolvePrincipalPermissions: resolver,
      },
    );
    setSummary({
      runId: 'acme_run',
      status: 'suspended',
      requestedBy: 'operator-1',
    });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'http.start',
    });

    await expect(
      host.route(
        new Request('https://thread/_flowsafe/agent-host/resume', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            agentId: 'writer',
            threadId: 'acme_thread',
            resourceId: RESOURCE_ID,
            runId: 'acme_run',
            entryPath: 'approval.resume',
            requestedBy: 'reviewer-1',
            resumeData: { approved: true },
          }),
        }),
        scope,
      ),
    ).rejects.toMatchObject({ status: 403 });

    expect(resolver).toHaveBeenCalledOnce();
    expect(mocked.resumeViaRuntime).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'denied',
      detail: {
        entryPath: 'approval.resume',
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'permissions-revoked-v3',
      },
    });
  });

  it.each([
    [
      'a thrown error',
      () => {
        throw new Error('private identity-provider failure');
      },
    ],
    [
      'a rejected promise',
      async () => {
        throw new Error('private asynchronous failure');
      },
    ],
    ['a non-object result', () => null],
    [
      'a non-array permission set',
      () => ({ permissions: 'agents.run', policyVersion: 'permissions-v1' }),
    ],
    [
      'a malformed permission identifier',
      () => ({ permissions: ['Agents.run'], policyVersion: 'permissions-v1' }),
    ],
    [
      'a non-string permission entry',
      () => ({
        permissions: ['agents.run', 42],
        policyVersion: 'permissions-v1',
      }),
    ],
    [
      'a malformed policy version',
      () => ({ permissions: requiredPermissions, policyVersion: '\n' }),
    ],
    [
      'a blank policy version',
      () => ({ permissions: requiredPermissions, policyVersion: '   ' }),
    ],
    [
      'a policy version over the 200-character bound',
      () => ({
        permissions: requiredPermissions,
        policyVersion: 'v'.repeat(201),
      }),
    ],
  ])('fails closed and audits resolver error for %s', async (_label, value) => {
    const resolver = vi.fn(value) as unknown as PrincipalPermissionResolver;
    const { host, scope, auditEvents } = harness(['writer'], {
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await expect(host.start(scope, startInput)).rejects.toMatchObject({
      status: 403,
      message: 'forbidden',
    });

    expect(mocked.stream).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'error',
      reason: 'permission resolution failed',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: null,
      },
    });
    expect(JSON.stringify(auditEvents.at(-1))).not.toContain('private');
  });

  it('enforces required permissions for an automated principal without consulting its projected role', async () => {
    const scheduler: ExecutionPrincipal = {
      kind: 'system',
      id: 'flowsafe-scheduler',
      purpose: 'scheduled-agent-execution',
    };
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run'],
      policyVersion: 'automation-v3',
    }));
    const fixture = harness(['writer'], {
      principal: scheduler,
      allowedAutomation: [{ kind: 'system', entryPaths: ['schedule.fire'] }],
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });
    const { host, scope, state, auditEvents } = fixture;
    await seedThreadedSchedule(fixture);
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });

    await expect(
      host.start(scope, {
        ...startInput,
        runId: 'acme_scheduled',
        entryPath: 'schedule.fire',
        scheduleId: SCHEDULE_ID,
        dispatchId: DISPATCH_ID,
      }),
    ).rejects.toMatchObject({ status: 403 });

    expect(resolver).toHaveBeenCalledWith(scheduler);
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'denied',
      detail: {
        principalKind: 'system',
        principalId: 'flowsafe-scheduler',
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'automation-v3',
      },
    });
  });

  it('allows an automated principal that holds every required permission', async () => {
    const scheduler: ExecutionPrincipal = {
      kind: 'system',
      id: 'flowsafe-scheduler',
      purpose: 'scheduled-agent-execution',
    };
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run', 'reports.read'],
      policyVersion: 'automation-v3',
    }));
    const fixture = harness(['writer'], {
      principal: scheduler,
      allowedAutomation: [{ kind: 'system', entryPaths: ['schedule.fire'] }],
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });
    const { host, scope, state, auditEvents } = fixture;
    await seedThreadedSchedule(fixture);
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });

    await host.start(scope, {
      ...startInput,
      runId: 'acme_scheduled',
      entryPath: 'schedule.fire',
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    });

    expect(resolver).toHaveBeenCalledWith(scheduler);
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'allowed',
      detail: {
        principalKind: 'system',
        principalId: 'flowsafe-scheduler',
        entryPath: 'schedule.fire',
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: 'automation-v3',
      },
    });
  });

  it('invokes a configured resolver for a role-only agent, projecting the resolution without requiring it', async () => {
    const resolver = vi.fn(async () => ({
      permissions: ['reports.read'],
      policyVersion: 'permissions-v5',
    }));
    const { host, scope, auditEvents } = harness(['writer'], {
      resolvePrincipalPermissions: resolver,
    });

    await host.start(scope, startInput);

    expect(resolver).toHaveBeenCalledOnce();
    expect(mocked.stream).toHaveBeenCalledOnce();
    const requestContext = mocked.stream.mock.calls[0]?.[1]?.requestContext;
    expect(requestContext?.get('breakwater.principalPermissions')).toEqual({
      permissions: ['reports.read'],
      policyVersion: 'permissions-v5',
    });
    // The entry event keeps its role-only shape: no permission fields.
    expect(auditEvents.at(-1)).toMatchObject({ decision: 'allowed' });
    expect(auditEvents.at(-1)?.detail).not.toHaveProperty(
      'requiredPermissions',
    );
    expect(auditEvents.at(-1)?.detail).not.toHaveProperty(
      'permissionPolicyVersion',
    );
  });

  it('starts a role-only agent without a projection when resolution fails, and audits the failure', async () => {
    const resolver = vi.fn(async () => {
      throw new Error('private identity-provider failure');
    });
    const { host, scope, auditEvents } = harness(['writer'], {
      resolvePrincipalPermissions: resolver,
    });

    await host.start(scope, startInput);

    expect(resolver).toHaveBeenCalledOnce();
    expect(mocked.stream).toHaveBeenCalledOnce();
    const requestContext = mocked.stream.mock.calls[0]?.[1]?.requestContext;
    // An explicit null: a permission-declaring connector inside this run
    // fails closed at breakwater's gate instead of executing unauthorized.
    expect(requestContext?.get('breakwater.principalPermissions')).toBeNull();
    expect(auditEvents.at(-2)).toMatchObject({
      action: 'agent.permissions.resolve',
      decision: 'error',
      reason: 'permission resolution failed',
      detail: {
        agentId: 'writer',
        entryPath: 'http.start',
        principalKind: 'human',
        permissionPolicyVersion: null,
      },
    });
    expect(auditEvents.at(-1)).toMatchObject({
      action: 'agent.entry.authorize',
      decision: 'allowed',
    });
    expect(JSON.stringify(auditEvents)).not.toContain('private');
  });

  it('projects an explicit null when no resolver is configured', async () => {
    const { host, scope } = harness(['writer']);

    await host.start(scope, startInput);

    const requestContext = mocked.stream.mock.calls[0]?.[1]?.requestContext;
    expect(requestContext?.get('breakwater.principalPermissions')).toBeNull();
  });

  it('projects the resolution into the start leg of a permission-requiring agent', async () => {
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run', 'reports.read', 'records.observe'],
      policyVersion: 'permissions-v6',
    }));
    const { host, scope } = harness(['writer'], {
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await host.start(scope, startInput);

    const requestContext = mocked.stream.mock.calls[0]?.[1]?.requestContext;
    expect(requestContext?.get('breakwater.principalPermissions')).toEqual({
      permissions: ['agents.run', 'reports.read', 'records.observe'],
      policyVersion: 'permissions-v6',
    });
  });

  it('re-derives the projection on the approval-resume leg so a policy change retires the stored one', async () => {
    // #given — the persisted snapshot still carries the broader start-time
    // projection, but the CURRENT policy snapshot has narrowed. The resume
    // leg must resolve afresh and overwrite the stored value.
    const resolver = vi.fn(async () => ({
      permissions: ['agents.run', 'reports.read'],
      policyVersion: 'permissions-v2',
    }));
    const { host, scope, state, setSummary, setSnapshot } = harness(
      ['writer'],
      {
        requiredPermissions,
        resolvePrincipalPermissions: resolver,
      },
    );
    setSummary({
      runId: 'acme_run',
      status: 'suspended',
      requestedBy: 'operator-1',
    });
    setSnapshot({
      requestContext: {
        'breakwater.principalPermissions': {
          permissions: ['agents.run', 'reports.read', 'records.observe'],
          policyVersion: 'permissions-v1',
        },
      },
    });
    state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    state.set('flowsafe:agent-run:v1:acme_run', {
      version: 2,
      agentId: 'writer',
      principal: scope.principal,
      originEntryPath: 'http.start',
    });
    const provider = host.requestContextForRun();
    let resumedContext: Record<string, unknown> | undefined;
    mocked.resumeViaRuntime.mockImplementation(async () => {
      resumedContext = await provider('durable-agentic-loop', 'acme_run', {
        kind: 'resume',
        step: ['tool'],
        resumeCount: 1,
      });
      return { runId: 'acme_run', status: 'success' };
    });

    // #when
    const response = await host.route(
      new Request('https://thread/_flowsafe/agent-host/resume', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          agentId: 'writer',
          threadId: 'acme_thread',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
          entryPath: 'approval.resume',
          requestedBy: 'reviewer-1',
          resumeData: { approved: true },
        }),
      }),
      scope,
    );

    // #then — the leg carries the CURRENT snapshot, not the persisted one
    expect(response?.status).toBe(200);
    expect(resolver).toHaveBeenCalledOnce();
    expect(resumedContext?.['breakwater.principalPermissions']).toEqual({
      permissions: ['agents.run', 'reports.read'],
      policyVersion: 'permissions-v2',
    });
  });

  it('does not consult permissions when the existing human-role gate denies first', async () => {
    const viewer: ExecutionPrincipal = {
      kind: 'human',
      id: 'viewer-1',
      role: 'viewer',
    };
    const resolver = vi.fn(async () => ({
      permissions: requiredPermissions,
      policyVersion: 'permissions-v4',
    }));
    const { host, scope, auditEvents } = harness(['writer'], {
      principal: viewer,
      requiredPermissions,
      resolvePrincipalPermissions: resolver,
    });

    await expect(host.start(scope, startInput)).rejects.toMatchObject({
      status: 403,
    });

    expect(resolver).not.toHaveBeenCalled();
    expect(auditEvents.at(-1)).toMatchObject({
      decision: 'denied',
      reason: 'role is not allowed to mutate this agent',
      detail: {
        requiredPermissions: ['agents.run', 'reports.read'],
        permissionPolicyVersion: null,
      },
    });
  });
});

describe('createThreadAgentHost automated entry', () => {
  const SCHEDULER: ExecutionPrincipal = {
    kind: 'system',
    id: 'flowsafe-scheduler',
    purpose: 'scheduled-agent-execution',
  };
  const DECLARED: readonly AgentAutomationRule[] = [
    { kind: 'system', entryPaths: ['schedule.fire'] },
  ];

  function scheduledStart() {
    return {
      agentId: 'writer',
      threadId: 'acme_thread',
      resourceId: RESOURCE_ID,
      runId: 'acme_scheduled',
      prompt: 'scheduled',
      entryPath: 'schedule.fire' as const,
      scheduleId: SCHEDULE_ID,
      dispatchId: DISPATCH_ID,
    };
  }

  async function bind(fixture: Harness): Promise<void> {
    await seedThreadedSchedule(fixture);
    fixture.state.set('flowsafe:agent-thread-binding:v1', {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
  }

  it('denies a scheduled start when the agent declares no automation', async () => {
    // #given — the agent's roles still include 'operator', which is exactly the
    // role a schedule path would fabricate to get in.
    const fixture = harness(['writer'], {
      principal: SCHEDULER,
    });
    const { host, scope } = fixture;
    await bind(fixture);

    // #when / #then
    await expect(host.start(scope, scheduledStart())).rejects.toMatchObject({
      status: 403,
    });
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('denies a declared automated kind arriving on an undeclared entry path', async () => {
    // #given
    const fixture = harness(['writer'], {
      principal: SCHEDULER,
      allowedAutomation: DECLARED,
    });
    const { host, scope } = fixture;
    await bind(fixture);

    // #when / #then
    await expect(
      host.start(scope, {
        ...scheduledStart(),
        entryPath: 'signal.wake',
        scheduleId: undefined,
        dispatchId: undefined,
      }),
    ).rejects.toMatchObject({ status: 403 });
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('runs a declared scheduled start and persists the SYSTEM principal', async () => {
    // #given
    const fixture = harness(['writer'], {
      principal: SCHEDULER,
      allowedAutomation: DECLARED,
    });
    const { host, scope, state } = fixture;
    await bind(fixture);
    // Read the record DURING the run: this harness's runs complete terminally,
    // and a terminal run deletes its own metadata on the way out.
    let persisted: unknown;
    mocked.stream.mockImplementation(async () => {
      persisted = state.get('flowsafe:agent-run:v1:acme_scheduled');
      return {};
    });

    // #when
    await host.start(scope, scheduledStart());

    // #then — the run is attributable to the scheduler, not to a human.
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(persisted).toMatchObject({
      version: 2,
      principal: SCHEDULER,
      originEntryPath: 'schedule.fire',
    });
  });

  it('projects the automated principal into breakwater as a non-human actor', async () => {
    // #given
    const fixture = harness(['writer'], {
      principal: SCHEDULER,
      allowedAutomation: DECLARED,
    });
    const { host, scope } = fixture;
    await bind(fixture);

    // #when
    await host.start(scope, scheduledStart());

    // #then — kind is what breakwater's mandatory gate authorizes on, and the
    // projected role is the least-privileged one, never 'operator'.
    const options = mocked.stream.mock.calls[0]?.[1];
    expect(options?.requestContext.get('breakwater.actor')).toEqual({
      id: 'flowsafe-scheduler',
      role: 'viewer',
      kind: 'system',
    });
    expect(
      options?.requestContext.get('breakwater.auditContext'),
    ).toMatchObject({
      entryPath: 'schedule.fire',
      principalKind: 'system',
      principalId: 'flowsafe-scheduler',
      purpose: 'scheduled-agent-execution',
    });
  });

  it('lets the host authorizer narrow, never widen, the declaration', async () => {
    // #given — the authorizer says yes to everything.
    const permissive = vi.fn(async () => true);
    const undeclared = harness(['writer'], {
      principal: SCHEDULER,
      authorizeAutomatedEntry: permissive as AutomatedEntryAuthorizer,
    });
    await bind(undeclared);

    // #when / #then — still denied: the agent declared nothing.
    await expect(
      undeclared.host.start(undeclared.scope, scheduledStart()),
    ).rejects.toMatchObject({ status: 403 });
    expect(permissive).not.toHaveBeenCalled();

    // #given — declared, but the host refuses this one.
    const denying = vi.fn(async () => false);
    const declared = harness(['writer'], {
      principal: SCHEDULER,
      allowedAutomation: DECLARED,
      authorizeAutomatedEntry: denying as AutomatedEntryAuthorizer,
    });
    await bind(declared);

    // #when / #then
    await expect(
      declared.host.start(declared.scope, scheduledStart()),
    ).rejects.toMatchObject({ status: 403 });
    expect(denying).toHaveBeenCalledWith(
      expect.objectContaining({
        principal: SCHEDULER,
        agentId: 'writer',
        entryPath: 'schedule.fire',
      }),
    );
  });
});

describe('createThreadAgentHost saved owner journal presence', () => {
  it.each(
    ['start', 'schedule status', 'dispatch status'].flatMap((entry) =>
      [null, false, 0, ''].map((journal) => ({ entry, journal })),
    ),
  )('retains a malformed journal before $entry ($journal)', async ({
    entry,
    journal,
  }) => {
    const fixture = harness();
    if (entry !== 'start') {
      fixture.setSummary({ runId: 'acme_run', status: 'suspended' });
      fixture.state.set(THREAD_BINDING_KEY, {
        version: 1,
        agentId: 'writer',
        resourceId: RESOURCE_ID,
      });
      fixture.state.set(TEST_RUN_RECORD_KEY, {
        version: 2,
        agentId: 'writer',
        principal: fixture.scope.principal,
        originEntryPath: 'http.start',
      });
    }
    fixture.state.set(TEST_OWNER_RECOVERY_KEY, journal);
    const before = structuredClone(fixture.state);
    const reserve = vi.spyOn(fixture.resourceAccess, 'reserveAll');
    const observation = vi.spyOn(
      fixture.scope.init.runtime,
      'authoritativeStartState',
    );
    const operation =
      entry === 'start'
        ? fixture.host.start(fixture.scope, C_START_INPUT)
        : entry === 'schedule status'
          ? fixture.host.scheduleDispatchStatus(fixture.scope, {
              agentId: 'writer',
              resourceId: RESOURCE_ID,
              runId: 'acme_run',
            })
          : fixture.host.route(
              new Request(
                `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}&dispatch=1`,
              ),
              fixture.scope,
            );
    const outcome = await operation.catch((error: unknown) => error);
    expect(fixture.state).toEqual(before);
    expect(reserve).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(observation).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
  });

  it('starts when the owner journal is absent', async () => {
    const fixture = harness();
    await expect(
      fixture.host.start(fixture.scope, C_START_INPUT),
    ).resolves.toMatchObject({ summary: { status: 'success' } });
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
  });
});

describe('FS8 D3 host shares cold wrapper initialization', () => {
  async function coldInitializationFixture() {
    const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
      '@mastra/core/mastra',
    );
    const breakwater = await vi.importActual<
      typeof import('@proofoftech/breakwater')
    >('@proofoftech/breakwater');
    mocked.actualFactory = true;
    mocked.actualAgent = breakwater.createGuardedAgent({
      id: 'writer',
      name: 'Writer',
      instructions: 'Unused cold initialization agent.',
      model: 'openai/gpt-4o-mini',
      allowedRoles: ['operator'],
      policies: [],
      audit: new breakwater.AuditLogger(),
      maxSteps: 1,
      toolChoice: 'auto',
    });
    mocked.mastra.mockImplementation(
      (configuration) => new core.Mastra(configuration),
    );
    const sql = openSqlite() as ReturnType<typeof openSqlite> & {
      close(): void;
    };
    onTestFinished(() => sql.close());
    const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
      ResourceOwnershipDatabase;
    const storage = createD1Storage({ binding });
    await storage.init();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('draining');
    const { StartIdempotencyStore, beginIdempotentStart } = await import(
      '../do-runner/start-idempotency.js'
    );
    const reservations = new StartIdempotencyStore(binding);
    const provider = vi.fn(() => {
      throw new Error('unexpected request-context provider entry');
    });
    const app = init(
      { storage },
      {
        executionFence: fence,
        startIdempotency: reservations,
        requestContextForRun: provider,
      },
    );
    const resources = new D1ResourceOwnershipStore(binding);
    const fixture = harness(['writer'], {
      init: app,
      storage,
      resourceAccess: resources,
    });
    const catalogGate = cDeferred();
    const buildModules = vi.fn(async () => {
      await catalogGate.promise;
      return [
        {
          meta: {
            id: 'writer',
            title: 'Writer',
            description: 'Writes records',
            allowedRoles: ['operator'] as const,
          },
          agent: guarded(),
        },
      ];
    });
    const hostStorage = vi.fn(() => storage);
    const createHost = () =>
      createThreadAgentHost({
        buildModules,
        storage: hostStorage,
        stateStorage: () => fixture.stateStorage,
        resourceAccess: () => resources,
        approvalService: () =>
          ({
            list: async () => [],
            createAsPrincipal: async () => {
              throw new Error('unexpected approval creation');
            },
          }) as unknown as ApprovalService,
      });
    const host = createHost();
    const replay = (scope = fixture.scope) =>
      host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}&replay=1`,
        ),
        scope,
      );
    const liveness = (agentId = 'writer', scope = fixture.scope) =>
      host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/${agentId}/acme_run/start-liveness`,
        ),
        scope,
      );
    return {
      ...fixture,
      host,
      app,
      createHost,
      fence,
      resources,
      storage,
      reservations,
      beginIdempotentStart,
      provider,
      catalogGate,
      buildModules,
      hostStorage,
      replay,
      liveness,
      registerAgent: vi.spyOn(app.runtime, 'registerAgent'),
      start: vi.spyOn(app.runtime, 'start'),
    };
  }

  it('queries the addressed cached wrapper for liveness without reading storage', async () => {
    const fixture = harness(['writer', 'reviewer']);
    fixture.state.set(THREAD_BINDING_KEY, {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    await fixture.host.resolveBoundAgent(fixture.scope, {
      agentId: 'writer',
      entryPath: 'http.start',
    });
    const get = vi.spyOn(fixture.stateStorage, 'get');
    const list = vi.spyOn(fixture.stateStorage, 'list');
    mocked.isRunLive.mockImplementation((agentId) => agentId === 'writer');
    for (const agentId of ['writer', 'reviewer']) {
      const response = await fixture.host.route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/${agentId}/acme_live/start-liveness`,
        ),
        fixture.scope,
      );
      expect(await response?.json()).toEqual({ live: agentId === 'writer' });
    }
    expect(mocked.isRunLive.mock.calls).toEqual([
      ['writer', 'acme_live'],
      ['reviewer', 'acme_live'],
    ]);
    expect(get).not.toHaveBeenCalled();
    expect(list).not.toHaveBeenCalled();
    expect(fixture.moduleScopes).toHaveLength(1);
    expect(fixture.storageScopes).toHaveLength(1);
  });

  it('answers cold liveness without waiting for a held catalog or reading storage', async () => {
    const fixture = await coldInitializationFixture();
    const get = vi.spyOn(fixture.stateStorage, 'get');
    const list = vi.spyOn(fixture.stateStorage, 'list');
    const before = new Map(fixture.state);
    const cold = await fixture.liveness();
    expect(await cold?.json()).toEqual({ live: false });
    expect(fixture.buildModules).not.toHaveBeenCalled();
    const replay = fixture.replay();
    try {
      const response = await Promise.race([
        fixture.liveness(),
        new Promise<undefined>((resolve) => setTimeout(resolve, 0)),
      ]);
      expect(fixture.hostStorage).not.toHaveBeenCalled();
      expect(fixture.registerAgent).not.toHaveBeenCalled();
      expect(get).not.toHaveBeenCalled();
      expect(list).not.toHaveBeenCalled();
      expect(response).toBeInstanceOf(Response);
      expect(await response?.json()).toEqual({ live: false });
      await expect(
        fixture.liveness('writer', {
          ...fixture.scope,
          init: { ...fixture.app },
        }),
      ).rejects.toThrow(
        'thread agent host cannot be shared across DO instances',
      );
      expect(fixture.state).toEqual(before);
    } finally {
      fixture.catalogGate.resolve();
      await replay;
    }
  });

  it.each([
    'private replay',
    'bound-agent lookup',
  ])('shares a held catalog between a private replay and %s', async (consumer) => {
    const fixture = await coldInitializationFixture();
    if (consumer === 'bound-agent lookup')
      fixture.state.set(THREAD_BINDING_KEY, {
        version: 1,
        agentId: 'writer',
        resourceId: RESOURCE_ID,
      });
    const before = new Map(fixture.state);
    const replay = fixture.replay();
    const concurrent =
      consumer === 'private replay'
        ? fixture.replay()
        : fixture.host.resolveBoundAgent(fixture.scope, {
            agentId: 'writer',
            entryPath: 'http.start',
          });
    const outcomes = Promise.allSettled([replay, concurrent]);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(fixture.buildModules).toHaveBeenCalledOnce();
    expect(fixture.hostStorage).not.toHaveBeenCalled();
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    fixture.catalogGate.resolve();
    const [first, second] = await outcomes;
    expect(fixture.hostStorage).toHaveBeenCalledOnce();
    expect(fixture.registerAgent).toHaveBeenCalledOnce();
    expect(first).toMatchObject({
      status: 'fulfilled',
      value: { status: 404 },
    });
    expect(second).toMatchObject({
      status: 'fulfilled',
      value:
        consumer === 'private replay'
          ? { status: 404 }
          : { agentId: 'writer', resourceId: RESOURCE_ID },
    });
    expect(fixture.app.runtime.workflowIds()).toEqual(['durable-agentic-loop']);
    expect(fixture.state).toEqual(before);
    expect(fixture.start).not.toHaveBeenCalled();
    expect(fixture.provider).not.toHaveBeenCalled();
  });

  it.each([
    'cached',
    'reconstructed',
  ] as const)('releases and reclaims a keyed start overlapping a cold private replay (%s host)', async (hostKind) => {
    const fixture = await coldInitializationFixture();
    const { globalRunRegistry } = await vi.importActual<
      typeof import('@mastra/core/agent/durable')
    >('@mastra/core/agent/durable');
    const request = {
      key: 'cold-key',
      owner: HUMAN_OWNER,
      targetKind: 'agent' as const,
      targetId: 'writer',
      threadId: fixture.scope.threadId,
      mintRunId: () => 'acme_run',
    };
    let retryHost = fixture.host;
    let retryScope = fixture.scope;
    let retryStart = fixture.start;
    let retryRegisterAgent = fixture.registerAgent;
    const surface = {
      persisted: async () => undefined,
      live: async () => {
        const response = await retryHost.route(
          new Request(
            'https://thread/_flowsafe/agent-host/runs/writer/acme_run/start-liveness',
          ),
          retryScope,
        );
        expect(response?.status).toBe(200);
        const result = (await response?.json()) as { live: boolean };
        expect(typeof result.live).toBe('boolean');
        return result.live;
      },
    };
    const first = await fixture.beginIdempotentStart(
      fixture.reservations,
      request,
      surface,
      'none',
    );
    expect(first.kind).toBe('start');
    expect(await fixture.reservations.readForAdmission(request.key)).toEqual(
      first.reservation,
    );
    expect(first.reservation).toMatchObject({
      state: 'started',
      binding: { kind: 'unbound' },
    });
    const writes = vi.spyOn(fixture.stateStorage, 'put');
    const replay = fixture.replay();
    const started = fixture.host.start(fixture.scope, {
      ...C_START_INPUT,
      idempotencyKey: request.key,
      startReservation: first.reservation,
    });
    const outcomes = Promise.allSettled([replay, started]);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(fixture.buildModules).toHaveBeenCalledOnce();
    expect(fixture.hostStorage).not.toHaveBeenCalled();
    expect(fixture.state.size).toBe(0);
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    onTestFinished(() => {
      vi.useRealTimers();
    });
    fixture.catalogGate.resolve();
    const [read, refused] = await outcomes;
    expect(
      await fixture.reservations.readForAdmission(request.key),
    ).toMatchObject({
      state: 'reserved',
      binding: { kind: 'unbound' },
    });
    expect(fixture.state.size).toBe(0);
    expect(writes).toHaveBeenCalledWith(
      TEST_OWNER_RECOVERY_KEY,
      expect.objectContaining({
        phase: 'preparing',
        startReservation: first.reservation,
      }),
    );
    expect(read).toMatchObject({ status: 'fulfilled', value: { status: 404 } });
    expect(refused).toMatchObject({
      status: 'rejected',
      reason: {
        status: 503,
        reason: { code: 'EXECUTION_FENCED', state: 'draining' },
      },
    });
    expect(fixture.hostStorage).toHaveBeenCalledOnce();
    expect(fixture.registerAgent).toHaveBeenCalledOnce();
    expect(fixture.start).toHaveBeenCalledOnce();
    const released = await fixture.reservations.readForAdmission(request.key);
    if (hostKind === 'reconstructed') {
      const app = init(
        { storage: fixture.storage },
        {
          executionFence: fixture.fence,
          startIdempotency: fixture.reservations,
          requestContextForRun: fixture.provider,
        },
      );
      retryHost = fixture.createHost();
      retryScope = { ...fixture.scope, init: app };
      retryStart = vi.spyOn(app.runtime, 'start');
      retryRegisterAgent = vi.spyOn(app.runtime, 'registerAgent');
    }
    const claim = vi.spyOn(fixture.reservations, 'claimReservation');
    const writesBeforeRetry = writes.mock.calls.length;
    expect(globalRunRegistry.has('acme_run')).toBe(true);
    const immediateRetry = await fixture
      .beginIdempotentStart(fixture.reservations, request, surface, 'none')
      .catch((error: unknown) => error);
    expect(await fixture.reservations.readForAdmission(request.key)).toEqual(
      released,
    );
    expect(claim).not.toHaveBeenCalled();
    expect(writes).toHaveBeenCalledTimes(writesBeforeRetry);
    expect(fixture.state.size).toBe(0);
    expect(fixture.start).toHaveBeenCalledOnce();
    expect(fixture.hostStorage).toHaveBeenCalledOnce();
    expect(fixture.buildModules).toHaveBeenCalledOnce();
    if (hostKind === 'reconstructed') {
      expect(retryStart).not.toHaveBeenCalled();
      expect(retryRegisterAgent).not.toHaveBeenCalled();
    }
    expect(immediateRetry).toMatchObject({
      status: 503,
      reason: { code: 'IDEMPOTENT_START_PENDING' },
    });
    await vi.advanceTimersByTimeAsync(30_000);
    expect(globalRunRegistry.has('acme_run')).toBe(false);
    const retry = await fixture.beginIdempotentStart(
      fixture.reservations,
      request,
      surface,
      'none',
    );
    expect(retry.kind).toBe('start');
    expect(claim).toHaveBeenCalledOnce();
    expect(retry.reservation.runId).toBe(first.reservation.runId);
    expect(retry.reservation.updatedAt).toBeGreaterThan(
      first.reservation.updatedAt,
    );
    expect(released?.updatedAt).toBeLessThan(retry.reservation.updatedAt);
    await expect(
      retryHost.start(retryScope, {
        ...C_START_INPUT,
        idempotencyKey: request.key,
        startReservation: retry.reservation,
      }),
    ).rejects.toMatchObject({
      status: 503,
      reason: { code: 'EXECUTION_FENCED', state: 'draining' },
    });
    expect(retryStart).toHaveBeenCalledTimes(
      hostKind === 'reconstructed' ? 1 : 2,
    );
    expect(fixture.hostStorage).toHaveBeenCalledTimes(
      hostKind === 'reconstructed' ? 2 : 1,
    );
    expect(fixture.registerAgent).toHaveBeenCalledOnce();
    expect(retryRegisterAgent).toHaveBeenCalledOnce();
    expect(fixture.state.size).toBe(0);
    expect(
      await fixture.reservations.readForAdmission(request.key),
    ).toMatchObject({
      state: 'reserved',
      binding: { kind: 'unbound' },
    });
    expect(await fixture.resources.owner('run', 'acme_run')).toBeUndefined();
    expect(
      await fixture.resources.owner('thread', fixture.scope.threadId),
    ).toBeUndefined();
    expect(
      await fixture.resources.owner('resource', RESOURCE_ID),
    ).toBeUndefined();
    const workflows = await fixture.storage.getStore('workflows');
    expect(
      await workflows?.loadWorkflowSnapshot({
        workflowName: 'durable-agentic-loop',
        runId: 'acme_run',
      }),
    ).toBeNull();
    expect(fixture.provider).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(30_000);
    expect(globalRunRegistry.has('acme_run')).toBe(false);
  });

  it('retries a failed catalog and retains the instance boundary', async () => {
    const fixture = await coldInitializationFixture();
    const failure = new Error('catalog unavailable');
    fixture.buildModules.mockRejectedValueOnce(failure);
    await expect(fixture.replay()).rejects.toBe(failure);
    expect(fixture.hostStorage).not.toHaveBeenCalled();
    expect(fixture.registerAgent).not.toHaveBeenCalled();
    const outcomes = Promise.allSettled([fixture.replay(), fixture.replay()]);
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(fixture.buildModules).toHaveBeenCalledTimes(2);
    fixture.catalogGate.resolve();
    const results = await outcomes;
    expect(fixture.hostStorage).toHaveBeenCalledOnce();
    expect(fixture.registerAgent).toHaveBeenCalledOnce();
    expect(results).toMatchObject([
      { status: 'fulfilled', value: { status: 404 } },
      { status: 'fulfilled', value: { status: 404 } },
    ]);
    await expect(
      fixture.replay({ ...fixture.scope, init: { ...fixture.app } }),
    ).rejects.toThrow('thread agent host cannot be shared across DO instances');
    expect(fixture.buildModules).toHaveBeenCalledTimes(2);
    expect(fixture.hostStorage).toHaveBeenCalledOnce();
    expect(fixture.registerAgent).toHaveBeenCalledOnce();
    expect(fixture.state.size).toBe(0);
    expect(fixture.start).not.toHaveBeenCalled();
    expect(fixture.provider).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 host activation cold agent recovery', () => {
  async function coldFixture(
    threaded: boolean,
    mode: 'fenced' | 'unfenced',
    status: 'pending' | 'success' | 'suspended' = 'success',
    keyed = false,
  ) {
    const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
      '@mastra/core/mastra',
    );
    const breakwater = await vi.importActual<
      typeof import('@proofoftech/breakwater')
    >('@proofoftech/breakwater');
    mocked.actualFactory = true;
    mocked.actualAgent = breakwater.createGuardedAgent({
      id: 'writer',
      name: 'Writer',
      instructions: 'Unused cold recovery agent.',
      model: 'openai/gpt-4o-mini',
      allowedRoles: ['operator'],
      policies: [],
      audit: new breakwater.AuditLogger(),
      maxSteps: 1,
      toolChoice: 'auto',
    });
    mocked.mastra.mockImplementation(
      (configuration) => new core.Mastra(configuration),
    );
    const sql = openSqlite();
    const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
      ResourceOwnershipDatabase;
    const storage = createD1Storage({ binding, tablePrefix: 'cold_' });
    await storage.init();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    const { StartIdempotencyStore } = await import(
      '../do-runner/start-idempotency.js'
    );
    const reservations = keyed ? new StartIdempotencyStore(binding) : undefined;
    const app = init(
      { storage },
      {
        executionFence: mode === 'fenced' ? fence : 'none',
        startIdempotency: reservations ?? 'none',
      },
    );
    const resources = new D1ResourceOwnershipStore(
      binding as ResourceOwnershipDatabase,
    );
    const fixture = harness(['writer'], {
      init: app,
      storage,
      resourceAccess: resources,
    });
    const token = 'cold-host-attempt';
    const execution = {
      tablePrefix: 'cold_',
      workflowId: 'durable-agentic-loop',
      runId: 'acme_run',
      startToken: 'cold-generation',
    };
    let claim:
      | import('../do-runner/start-reservation-contract.js').StartReservationReading
      | undefined;
    if (reservations) {
      const reserved = await reservations.reserve({
        key: 'cold-key',
        owner: HUMAN_OWNER,
        targetKind: 'agent',
        targetId: 'writer',
        threadId: 'acme_thread',
        mintRunId: () => 'acme_run',
      });
      claim = await reservations.claimReservation(reserved.reservation);
      if (!claim) throw new Error('missing cold claim');
      await reservations.bindPreparedStart(claim, {
        ...execution,
        owner: HUMAN_OWNER,
        target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
      });
    }
    const provenance = {
      version: 2,
      startToken: execution.startToken,
      attemptToken: status === 'pending' ? token : 'resumed-leg',
      requestedBy: 'operator-1',
      requestedByKind: 'human',
      resumeCounts: [],
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
      },
      agentStart: { threaded },
      ...(status === 'pending' ? { initialAdmission: true } : {}),
    };
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    await workflows.persistWorkflowSnapshot({
      workflowName: execution.workflowId,
      runId: execution.runId,
      snapshot: {
        runId: execution.runId,
        status,
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: 123,
        requestContext: { 'flowsafe.runProvenance': provenance },
      },
    });
    const recovery = ownerRecovery('acme_run', {
      phase: mode === 'fenced' ? 'prepared' : 'prepared-unfenced',
      execution,
      token,
      threaded,
      ...(claim ? { startReservation: claim } : {}),
    });
    seedRecoveryState(fixture.state, 'acme_run', recovery, threaded);
    await resources.reserveAll(
      [
        { kind: 'run', resourceId: 'acme_run' },
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
      ],
      HUMAN_OWNER,
      token,
    );
    const nativeCapability = workflows[FENCED_WORKFLOW_STORAGE];
    if (!nativeCapability) throw new Error('missing cold capability');
    const rawReads = vi.fn(
      nativeCapability.readSnapshot.bind(nativeCapability),
    );
    const terminalization = vi.fn(
      nativeCapability.terminalizeInitialAdmission.bind(nativeCapability),
    );
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...nativeCapability,
        readSnapshot: rawReads,
        terminalizeInitialAdmission: terminalization,
      },
      configurable: true,
    });
    return {
      ...fixture,
      sql,
      reservations,
      claim,
      app,
      workflows,
      execution,
      recovery,
      resources,
      nativeCapability,
      rawReads,
      terminalization,
    };
  }

  it.each([
    true,
    false,
  ])('initializes a fresh actual wrapper before fenced pending recovery (threaded=%s)', async (threaded) => {
    const fixture = await coldFixture(threaded, 'fenced', 'pending');
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    const before = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    expect(before?.status).toBe('pending');
    const start = vi.spyOn(fixture.app.runtime, 'start');
    await expect(
      fixture.host.recoverOwnership(fixture.scope),
    ).resolves.toBeUndefined();
    const after = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    expect(after?.status).toBe('failed');
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(fixture.app.runtime.workflowIds()).toContain(
      fixture.execution.workflowId,
    );
    expect(start).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    true,
    false,
  ])('initializes a fresh actual wrapper for progressed unfenced recovery (threaded=%s)', async (threaded) => {
    const fixture = await coldFixture(threaded, 'unfenced');
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    await expect(
      fixture.host.recoverOwnership(fixture.scope),
    ).resolves.toBeUndefined();
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(b2).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('initializes the cold blocking-run entry before selecting a terminal no-journal record', async () => {
    const fixture = await coldFixture(true, 'fenced');
    fixture.state.delete(TEST_OWNER_RECOVERY_KEY);
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    await expect(
      fixture.host.blockingRun(fixture.scope),
    ).resolves.toBeUndefined();
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('initializes the cold dispatch-status entry before selecting its actual workflow', async () => {
    const fixture = await coldFixture(true, 'fenced');
    fixture.state.delete(TEST_OWNER_RECOVERY_KEY);
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    await expect(
      fixture.host.scheduleDispatchStatus(fixture.scope, {
        agentId: 'writer',
        resourceId: RESOURCE_ID,
        runId: 'acme_run',
      }),
    ).resolves.toMatchObject({ runId: 'acme_run', status: 'success' });
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it('keeps empty recovery and blocking scans lazy', async () => {
    const fixture = harness();
    await expect(
      fixture.host.recoverOwnership(fixture.scope),
    ).resolves.toBeUndefined();
    await expect(
      fixture.host.blockingRun(fixture.scope),
    ).resolves.toBeUndefined();
    expect(fixture.moduleScopes).toEqual([]);
    expect(fixture.storageScopes).toEqual([]);
  });

  it('retains a cold journal for an unknown catalog module without engine entry', async () => {
    const fixture = await coldFixture(true, 'fenced');
    const foreign = ownerRecovery('acme_run', {
      ...fixture.recovery,
      agentId: 'unknown',
      runRecord: {
        version: 2,
        agentId: 'unknown',
        principal: fixture.scope.principal,
        originEntryPath: 'http.start',
      },
    });
    fixture.state.set(TEST_OWNER_RECOVERY_KEY, foreign);
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(foreign);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.alarmAt()).toBeDefined();
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
  });
  it.each([
    true,
    false,
  ])('uses one authoritative recovery observation for the actual cold agent (threaded=%s)', async (threaded) => {
    const fixture = await coldFixture(threaded, 'fenced', 'pending');
    await expect(
      fixture.host.recoverOwnership(fixture.scope),
    ).resolves.toBeUndefined();
    const raw = await fixture.nativeCapability.readSnapshot({
      workflowId: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    expect(JSON.parse(raw?.snapshot ?? '{}').status).toBe('failed');
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.rawReads).toHaveBeenCalledOnce();
    expect(fixture.terminalization).toHaveBeenCalledOnce();
  });

  it.each([
    'agent',
    'owner',
    'thread',
    'mode',
    'workflow role',
    'missing identity',
  ] as const)('retains same-S foreign managed metadata before B2 from one selected row: %s', async (field) => {
    const fixture = await coldFixture(true, 'fenced', 'pending');
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    if (!snapshot?.requestContext) throw new Error('missing cold snapshot');
    const provenance = snapshot.requestContext[
      'flowsafe.runProvenance'
    ] as Record<string, unknown>;
    const identity = provenance.startIdentity as {
      owner: { kind: string; id: string };
      target: { kind: string; id: string; threadId?: string };
    };
    if (field === 'agent') identity.target.id = 'foreign-agent';
    if (field === 'owner') {
      identity.owner.id = 'foreign-owner';
      provenance.requestedBy = 'foreign-owner';
    }
    if (field === 'thread') identity.target.threadId = 'foreign-thread';
    if (field === 'mode') provenance.agentStart = { threaded: false };
    if (field === 'workflow role') {
      identity.target = { kind: 'workflow', id: fixture.execution.workflowId };
      delete provenance.agentStart;
    }
    if (field === 'missing identity') {
      delete provenance.startIdentity;
      delete provenance.agentStart;
    }
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
      snapshot,
    });
    const rawBefore = await fixture.nativeCapability.readSnapshot({
      workflowId: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    const settle = vi.spyOn(fixture.app.runtime, 'settleStartExecution');
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(
      await fixture.nativeCapability.readSnapshot({
        workflowId: fixture.execution.workflowId,
        runId: fixture.execution.runId,
      }),
    ).toEqual(rawBefore);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      fixture.recovery,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.terminalization).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(fixture.rawReads).toHaveBeenCalledOnce();
    expect(outcome).toMatchObject({
      status: 503,
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
  });
  it.each([
    [true, true],
    [true, false],
    [false, true],
    [false, false],
  ] as const)('retains cold prepared absence across repeated same-ID starts (threaded=%s keyed=%s)', async (threaded, keyed) => {
    const fixture = await coldFixture(threaded, 'fenced', 'pending', keyed);
    const beforeClaim =
      await fixture.reservations?.readForAdmission('cold-key');
    fixture.sql
      .prepare(
        'DELETE FROM cold_mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
      )
      .run(fixture.execution.workflowId, fixture.execution.runId);
    const enter = vi
      .spyOn(fixture.app.runtime, 'start')
      .mockRejectedValue(new Error('unexpected new engine entry'));
    const recoveryError = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(
      await fixture.nativeCapability.readSnapshot({
        workflowId: fixture.execution.workflowId,
        runId: fixture.execution.runId,
      }),
    ).toBeUndefined();
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      fixture.recovery,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.state.has(THREAD_BINDING_KEY)).toBe(threaded);
    expect(await fixture.reservations?.readForAdmission('cold-key')).toEqual(
      beforeClaim,
    );
    expect(recoveryError).toMatchObject({ status: 503 });
    for (let retry = 0; retry < 2; retry++) {
      const outcome = await fixture.host
        .start(fixture.scope, {
          ...C_START_INPUT,
          threaded,
          ...(fixture.claim
            ? {
                idempotencyKey: fixture.claim.key,
                startReservation: fixture.claim,
              }
            : {}),
        })
        .catch((error: unknown) => error);
      expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
        fixture.recovery,
      );
      expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
      expect(enter).not.toHaveBeenCalled();
      expect(outcome).toMatchObject({ status: 503 });
    }
    expect(await fixture.host.blockingRun(fixture.scope)).toMatchObject({
      runId: 'acme_run',
    });
    expect(fixture.terminalization).not.toHaveBeenCalled();
  });

  it('retains a no-journal terminal record when its selected immutable owner differs', async () => {
    const fixture = await coldFixture(true, 'fenced');
    fixture.state.delete(TEST_OWNER_RECOVERY_KEY);
    const snapshot = await fixture.workflows.loadWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
    });
    if (!snapshot?.requestContext) throw new Error('missing cold result');
    const provenance = snapshot.requestContext['flowsafe.runProvenance'] as {
      requestedBy: string;
      startIdentity: { owner: { id: string } };
    };
    provenance.startIdentity.owner.id = 'foreign-owner';
    provenance.requestedBy = 'foreign-owner';
    await fixture.workflows.persistWorkflowSnapshot({
      workflowName: fixture.execution.workflowId,
      runId: fixture.execution.runId,
      snapshot,
    });
    const settle = vi.spyOn(fixture.app.runtime, 'settleStartExecution');
    const outcome = await fixture.host
      .blockingRun(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(settle).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ status: 503 });
  });
});

describe('FS8 D3 host activation exact agent journals', () => {
  it.each([
    [
      'phase',
      {
        phase: 'prepared-unfenced',
        execution: {
          tablePrefix: '',
          workflowId: 'durable-agentic-loop',
          runId: 'acme_run',
          startToken: 'test-generation',
        },
      },
    ],
    [
      'generation',
      {
        execution: {
          tablePrefix: '',
          workflowId: 'durable-agentic-loop',
          runId: 'acme_run',
          startToken: 'replacement-generation',
        },
      },
    ],
    [
      'principal',
      {
        runRecord: {
          version: 2,
          agentId: 'writer',
          principal: { kind: 'human', id: 'operator-1', role: 'admin' },
          originEntryPath: 'http.start',
        },
      },
    ],
    [
      'origin',
      {
        runRecord: {
          version: 2,
          agentId: 'writer',
          principal: { kind: 'human', id: 'operator-1', role: 'operator' },
          originEntryPath: 'signal.wake',
        },
      },
    ],
  ])('preserves a replacement with repeated H when the listed journal differs in %s', async (_field, replacement) => {
    const fixture = harness();
    fixture.setSummary({ runId: 'acme_run', status: 'success' });
    const original = ownerRecovery('acme_run');
    const current = ownerRecovery('acme_run', replacement);
    seedRecoveryState(fixture.state, 'acme_run', current);
    vi.spyOn(fixture.stateStorage, 'list').mockResolvedValueOnce(
      new Map([[TEST_OWNER_RECOVERY_KEY, original]]),
    );
    const settle = vi.spyOn(fixture.resourceAccess, 'settleReservation');
    const recover = vi.spyOn(fixture.scope.init.runtime, 'recoverStartAttempt');
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(current);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(settle).not.toHaveBeenCalled();
    expect(recover).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    'principal',
    'origin',
  ] as const)('preserves a replacement run record during preparing rollback: %s', async (field) => {
    const fixture = harness();
    const recovery = ownerRecovery('acme_run', { phase: 'preparing' });
    delete recovery.execution;
    seedRecoveryState(fixture.state, 'acme_run', recovery);
    const record = {
      version: 2,
      agentId: 'writer',
      principal: {
        kind: 'human',
        id: 'operator-1',
        role: field === 'principal' ? 'admin' : 'operator',
      },
      originEntryPath: field === 'origin' ? 'signal.wake' : 'http.start',
    };
    fixture.state.set(TEST_RUN_RECORD_KEY, record);
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_RUN_RECORD_KEY)).toEqual(record);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(recovery);
    expect(fixture.state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    null,
    'actual_',
  ] as const)('never invokes B2 or absence rollback for missing prepared-unfenced state (prefix=%s)', async (tablePrefix) => {
    const fixture = harness();
    fixture.setSummary(null, true);
    const recovery = ownerRecovery('acme_run', {
      phase: 'prepared-unfenced',
      execution: {
        tablePrefix,
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run',
        startToken: 'test-generation',
      },
    });
    seedRecoveryState(fixture.state, 'acme_run', recovery);
    const b2 = vi.spyOn(fixture.scope.init.runtime, 'recoverStartAttempt');
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(recovery);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.alarmAt()).toBeDefined();
    expect(b2).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    true,
    false,
  ])('retains managed bookkeeping after strict terminal settlement failure (threaded=%s)', async (threaded) => {
    const failure = new Error('strict settlement failed');
    const settle = vi.fn(async () => {
      throw failure;
    });
    const fixture = harness(['writer'], {
      runtime: { settleStartExecution: settle },
    });
    const outcome = await fixture.host
      .start(fixture.scope, { ...C_START_INPUT, threaded })
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toMatchObject({
      phase: 'prepared-unfenced',
      threaded,
    });
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(
      await fixture.resourceAccess.reserveAll(
        [{ kind: 'run', resourceId: 'acme_run' }],
        { kind: 'human', id: 'replacement-owner' },
        'replacement-attempt',
      ),
    ).toBe(false);
    expect(fixture.alarmAt()).toBeDefined();
    expect(outcome).toBe(failure);
  });

  it.each([
    true,
    false,
  ])('does not publish normal completion from a pending durable start (threaded=%s)', async (threaded) => {
    const fixture = harness();
    fixture.setSummary({ runId: 'acme_run', status: 'pending' }, false);
    const outcome = await fixture.host
      .start(fixture.scope, { ...C_START_INPUT, threaded })
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toMatchObject({
      phase: 'prepared-unfenced',
      threaded,
    });
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(
      await fixture.resourceAccess.reserveAll(
        [{ kind: 'run', resourceId: 'acme_run' }],
        { kind: 'human', id: 'replacement-owner' },
        'replacement-attempt',
      ),
    ).toBe(false);
    expect(fixture.alarmAt()).toBeDefined();
    expect(outcome).toMatchObject({
      status: 503,
      reason: { code: 'RUN_START_PENDING' },
    });
  });
});

describe('FS8 D3 host activation owning quiescence', () => {
  it('refuses recovery while the exact start frame is still awaiting the engine', async () => {
    const fixture = harness();
    const entered = cDeferred(),
      release = cDeferred();
    mocked.stream.mockImplementation(async () => {
      entered.resolve();
      await release.promise;
      return {};
    });
    const start = fixture.host.start(fixture.scope, C_START_INPUT);
    const outcome = start.catch((error: unknown) => error);
    await entered.promise;
    const journal = structuredClone(fixture.state.get(TEST_OWNER_RECOVERY_KEY));
    try {
      const recovery = await fixture.host
        .recoverOwnership(fixture.scope)
        .catch((error: unknown) => error);
      expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(journal);
      expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
      expect(recovery).toMatchObject({
        status: 503,
        reason: { code: 'RUN_START_PENDING' },
      });
    } finally {
      release.resolve();
      await outcome;
    }
    await expect(start).resolves.toMatchObject({ runId: 'acme_run' });
  });

  it('recovers the previous preparing attempt before installing the next execution frame', async () => {
    const fixture = harness();
    const previous = ownerRecovery('acme_run', { phase: 'preparing' });
    delete previous.execution;
    seedRecoveryState(fixture.state, 'acme_run', previous);
    await fixture.resources.reserveAll(
      [
        { kind: 'run', resourceId: 'acme_run' },
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
      ],
      HUMAN_OWNER,
      previous.token as string,
    );
    const start = fixture.host.start(fixture.scope, C_START_INPUT);
    await expect(start).resolves.toMatchObject({ runId: 'acme_run' });
    const result = await start;
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(mocked.stream).toHaveBeenCalledOnce();
    expect(result).toMatchObject({
      runId: 'acme_run',
      summary: { status: 'success' },
    });
  });
});

describe('FS8 D3 host activation captured original claim', () => {
  it('forwards the captured successful claim through the eighth argument after an authorization wait', async () => {
    const { StartIdempotencyStore } = await import(
      '../do-runner/start-idempotency.js'
    );
    const store = new StartIdempotencyStore(
      sqliteUnitDatabase(
        openSqlite(),
      ) as import('../do-runner/start-idempotency.js').StartIdempotencyDatabase,
    );
    const reserved = await store.reserve({
      key: 'captured-key',
      owner: { kind: 'human', id: 'operator-1' },
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread',
      mintRunId: () => 'acme_run',
    });
    const claim = await store.claimReservation(reserved.reservation);
    if (!claim) throw new Error('missing successful claim');
    const mutable = {
      ...claim,
      owner: { ...claim.owner },
      binding: { ...claim.binding },
    };
    const entered = cDeferred(),
      release = cDeferred();
    const fixture = harness(['writer'], {
      runtime: { startIdempotency: store },
      resolvePrincipalPermissions: async () => {
        entered.resolve();
        await release.promise;
        return { permissions: [], policyVersion: 'test-v1' };
      },
    });
    const pending = fixture.host.start(fixture.scope, {
      ...C_START_INPUT,
      idempotencyKey: claim.key,
      startReservation: mutable,
    });
    const outcome = pending.catch((error: unknown) => error);
    await entered.promise;
    Object.assign(mutable, {
      key: 'replacement-key',
      targetId: 'replacement-agent',
      runId: 'replacement-run',
      updatedAt: claim.updatedAt + 10,
    });
    mutable.owner.id = 'replacement-owner';
    release.resolve();
    await outcome;
    expect(await store.readForAdmission(claim.key)).toEqual(claim);
    expect(mocked.stream.mock.calls[0]?.[7].startReservation).toEqual(claim);
    await expect(pending).resolves.toMatchObject({ runId: 'acme_run' });
  });
});

describe('FS8 D3 host activation custom storage cold recovery', () => {
  it.each([
    true,
    false,
  ])('recovers a real custom-null result without B2 after lazy factory initialization (threaded=%s)', async (threaded) => {
    const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
      '@mastra/core/mastra',
    );
    const { InMemoryStore } = await import('@mastra/core/storage');
    const breakwater = await vi.importActual<
      typeof import('@proofoftech/breakwater')
    >('@proofoftech/breakwater');
    mocked.actualFactory = true;
    mocked.actualAgent = breakwater.createGuardedAgent({
      id: 'writer',
      name: 'Writer',
      instructions: 'Unused custom recovery agent.',
      model: 'openai/gpt-4o-mini',
      allowedRoles: ['operator'],
      policies: [],
      audit: new breakwater.AuditLogger(),
      maxSteps: 1,
      toolChoice: 'auto',
    });
    mocked.mastra.mockImplementation(
      (configuration) => new core.Mastra(configuration),
    );
    const storage = new InMemoryStore();
    await storage.init();
    const app = init(
      { storage },
      { executionFence: 'none', startIdempotency: 'none' },
    );
    const fixture = harness(['writer'], { init: app, storage });
    const execution = {
      tablePrefix: null,
      workflowId: 'durable-agentic-loop',
      runId: 'acme_run',
      startToken: 'custom-generation',
    };
    const recovery = ownerRecovery('acme_run', {
      phase: 'prepared-unfenced',
      execution,
      threaded,
    });
    seedRecoveryState(fixture.state, 'acme_run', recovery, threaded);
    await fixture.resources.reserveAll(
      [
        { kind: 'run', resourceId: 'acme_run' },
        { kind: 'thread', resourceId: 'acme_thread' },
        { kind: 'resource', resourceId: RESOURCE_ID },
      ],
      HUMAN_OWNER,
      recovery.token as string,
    );
    const workflows = await storage.getStore('workflows');
    await workflows?.persistWorkflowSnapshot({
      workflowName: execution.workflowId,
      runId: execution.runId,
      snapshot: {
        runId: execution.runId,
        status: 'success',
        result: { value: 'custom' },
        value: {},
        context: {},
        serializedStepGraph: [],
        activePaths: [],
        activeStepsPath: {},
        suspendedPaths: {},
        resumeLabels: {},
        waitingPaths: {},
        timestamp: 123,
        requestContext: {
          'flowsafe.runProvenance': {
            version: 2,
            startToken: execution.startToken,
            attemptToken: 'resumed-custom-leg',
            resumeCounts: [],
            requestedBy: 'operator-1',
            requestedByKind: 'human',
            startIdentity: {
              owner: HUMAN_OWNER,
              target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
            },
            agentStart: { threaded },
          },
        },
      },
    });
    const b2 = vi.spyOn(app.runtime, 'recoverStartAttempt');
    expect(app.runtime.workflowIds()).toEqual([]);
    await expect(
      fixture.host.recoverOwnership(fixture.scope),
    ).resolves.toBeUndefined();
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(b2).not.toHaveBeenCalled();
    expect(mocked.stream).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 host activation native agent zero admission', () => {
  async function nativeZeroFixture() {
    const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
      '@mastra/core/mastra',
    );
    mocked.mastra.mockImplementation((configuration) =>
      configuration?.workflows ? new core.Mastra(configuration) : undefined,
    );
    const sql = openSqlite();
    const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
      ResourceOwnershipDatabase;
    const storage = createD1Storage({ binding });
    await storage.init();
    const fence = new ExecutionFenceStore(binding);
    await fence.seed('open');
    const { StartIdempotencyStore } = await import(
      '../do-runner/start-idempotency.js'
    );
    const reservations = new StartIdempotencyStore(binding);
    const app = init(
      { storage },
      { executionFence: fence, startIdempotency: reservations },
    );
    let effects = 0;
    app
      .createWorkflow({
        id: 'durable-agentic-loop',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
      .then(
        app.createStep({
          id: 'effect',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async () => {
            effects++;
            return {};
          },
        }),
      )
      .commit();
    const domain = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = domain[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing native capability');
    let closeOnce = true;
    Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        withInitialAdmission: async (
          input: Parameters<typeof native.withInitialAdmission>[0],
          create: Parameters<typeof native.withInitialAdmission>[1],
        ) => {
          if (closeOnce) {
            closeOnce = false;
            await fence.transition({ expected: 'open', next: 'draining' });
          }
          return native.withInitialAdmission(input, create);
        },
      },
      configurable: true,
    });
    const fixture = harness(['writer'], {
      init: app,
      storage,
      resourceAccess: new D1ResourceOwnershipStore(binding),
    });
    mocked.stream.mockImplementation(
      async (
        ...args: Parameters<FlowsafeDurableAgent['streamUntilPersisted']>
      ) => {
        const [
          ,
          options,
          requestedBy,
          requestedByKind,
          attemptToken,
          scheduleDispatch,
          idempotencyKey,
          authority,
        ] = args;
        const runId = options.runId;
        if (typeof runId !== 'string')
          throw new Error('host omitted its runId');
        await app.runtime.start('durable-agentic-loop', {
          runId,
          inputData: {},
          storedRequestContext: Object.fromEntries(
            options.requestContext?.entries() ?? [],
          ),
          requestedBy,
          requestedByKind,
          attemptToken,
          scheduleDispatch,
          idempotencyKey,
          mutationEpoch: authority.mutationEpoch,
          startIdentity: authority.startIdentity,
          agentStart: authority.agentStart,
          onPreparedStartIdentity: authority.onPreparedStartIdentity,
          runOwnerGuard: authority.runOwnerGuard,
          startReservation: authority.startReservation,
        });
        return {};
      },
    );
    const claim = async () => {
      const reserved = await reservations.reserve({
        key: 'agent-zero-key',
        owner: HUMAN_OWNER,
        targetKind: 'agent',
        targetId: 'writer',
        threadId: 'acme_thread',
        mintRunId: () => 'acme_run',
      });
      const result = await reservations.claimReservation(reserved.reservation);
      if (!result) throw new Error('missing agent claim');
      return result;
    };
    return {
      fixture,
      app,
      storage,
      sql,
      domain,
      native,
      fence,
      reservations,
      claim,
      effects: () => effects,
      refuseNext: () => {
        closeOnce = true;
      },
    };
  }

  it('preserves H rows when non-owning recovery races a held native zero frame', async () => {
    const fixture = await nativeZeroFixture();
    const nativeStart = fixture.app.runtime.start.bind(fixture.app.runtime);
    const entered = cDeferred();
    const release = cDeferred();
    vi.spyOn(fixture.app.runtime, 'start').mockImplementationOnce(
      async (...args) => {
        try {
          return await nativeStart(...args);
        } catch (error) {
          entered.resolve();
          await release.promise;
          throw error;
        }
      },
    );
    const starting = fixture.fixture.host.start(
      fixture.fixture.scope,
      C_START_INPUT,
    );
    const settled = starting.catch((error: unknown) => error);
    await entered.promise;
    const owners = fixture.sql
      .prepare(
        'SELECT * FROM flowsafe_resource_owners ORDER BY resource_kind, resource_id',
      )
      .all();
    const state = structuredClone(fixture.fixture.state);
    try {
      const outcome = await fixture.fixture.host
        .recoverOwnership(fixture.fixture.scope)
        .catch((error: unknown) => error);
      expect(
        fixture.sql
          .prepare(
            'SELECT * FROM flowsafe_resource_owners ORDER BY resource_kind, resource_id',
          )
          .all(),
      ).toEqual(owners);
      expect(fixture.fixture.state).toEqual(state);
      expect(fixture.effects()).toBe(0);
      expect(outcome).toMatchObject({
        status: 503,
        reason: { code: 'RUN_START_PENDING' },
      });
    } finally {
      release.resolve();
      await settled;
    }
  });

  it.each([
    'lookalike',
    'serialized',
    'foreign generation',
    'evicted frame',
  ] as const)('retains agent prepared absence without native local zero authority (%s)', async (mode) => {
    const fixture = await nativeZeroFixture();
    const nativeStart = fixture.app.runtime.start.bind(fixture.app.runtime);
    let originalFailure: unknown;
    let foreignExecution:
      | import('../do-runner/execution-admission.js').RunExecutionIdentity
      | undefined;
    let prepared: unknown;
    const put = fixture.fixture.stateStorage.put.bind(
      fixture.fixture.stateStorage,
    );
    vi.spyOn(fixture.fixture.stateStorage, 'put').mockImplementation(
      async (key, value) => {
        await put(key, value);
        if (
          key === TEST_OWNER_RECOVERY_KEY &&
          (value as { phase?: string }).phase === 'prepared'
        )
          prepared = structuredClone(value);
      },
    );
    vi.spyOn(fixture.app.runtime, 'start').mockImplementationOnce(
      async (...args) => {
        try {
          return await nativeStart(...args);
        } catch (error) {
          originalFailure = error;
          if (mode === 'foreign generation') {
            await fixture.fence.transition({
              expected: 'draining',
              next: 'open',
            });
            fixture.refuseNext();
            await nativeStart('durable-agentic-loop', {
              runId: 'acme_run',
              onPreparedStartIdentity: (identity) => {
                foreignExecution = identity;
              },
              inputData: {},
              requestedBy: 'operator-1',
              requestedByKind: 'human',
            });
            throw new Error('foreign native zero unexpectedly succeeded');
          }
          if (mode === 'lookalike') {
            const { ExecutionFencedError } = await import(
              '../do-runner/execution-fence.js'
            );
            throw new ExecutionFencedError('draining', 'run start');
          }
          throw Object.assign(
            new Error('serialized zero receipt'),
            JSON.parse(
              JSON.stringify({
                status: (error as { status?: number }).status,
                reason: (error as { reason?: unknown }).reason,
              }),
            ),
          );
        }
      },
    );
    const claim = await fixture.claim();
    const first = await fixture.fixture.host
      .start(fixture.fixture.scope, {
        ...C_START_INPUT,
        idempotencyKey: claim.key,
        startReservation: claim,
      })
      .catch((error: unknown) => error);
    expect(fixture.fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      prepared,
    );
    expect(fixture.fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.fixture.state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(
      await fixture.native.readSnapshot({
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run',
      }),
    ).toBeUndefined();
    expect(fixture.effects()).toBe(0);
    if (mode === 'foreign generation') {
      expect(foreignExecution).toMatchObject({
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run',
      });
      expect(foreignExecution?.startToken).not.toBe(
        (prepared as { execution: { startToken: string } }).execution
          .startToken,
      );
    }
    expect(first).toBeInstanceOf(Error);
    if (mode === 'evicted frame') {
      const { isDefinitiveInitialAdmissionRefusal } = await import(
        '../do-runner/initial-admission-refusal.js'
      );
      expect(
        isDefinitiveInitialAdmissionRefusal(
          originalFailure,
          (
            prepared as {
              execution: import('../do-runner/execution-admission.js').D1RunExecutionIdentity;
            }
          ).execution,
        ),
      ).toBe(true);
      const coldHost = createThreadAgentHost({
        buildModules: () => [
          {
            meta: {
              id: 'writer',
              title: 'Writer',
              description: 'Cold zero fixture',
              allowedRoles: ['operator'],
            },
            agent: guarded(),
          },
        ],
        storage: () => fixture.storage,
        stateStorage: () => fixture.fixture.stateStorage,
        resourceAccess: () => fixture.fixture.resourceAccess,
        approvalService: () => {
          throw new Error('unexpected cold approval');
        },
      });
      const cold = await coldHost
        .recoverOwnership(fixture.fixture.scope)
        .catch((error: unknown) => error);
      expect(fixture.fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
        prepared,
      );
      expect(fixture.fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
      expect(fixture.fixture.state.has(THREAD_BINDING_KEY)).toBe(true);
      expect(cold).toMatchObject({ status: 503 });
    }
    const beforeClaim = await fixture.reservations.readForAdmission(claim.key);
    await fixture.fence.transition({ expected: 'draining', next: 'open' });
    const retry = await fixture.fixture.host
      .start(fixture.fixture.scope, {
        ...C_START_INPUT,
        idempotencyKey: claim.key,
        startReservation: claim,
      })
      .catch((error: unknown) => error);
    expect(fixture.fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      prepared,
    );
    expect(fixture.fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(await fixture.reservations.readForAdmission(claim.key)).toEqual(
      beforeClaim,
    );
    expect(fixture.effects()).toBe(0);
    expect(retry).toMatchObject({ status: 503 });
  });

  it.each([
    true,
    false,
  ])('clears local agent bookkeeping only after native zero proof and executes one retry (threaded=%s)', async (threaded) => {
    const { fixture, native, fence, reservations, claim, effects } =
      await nativeZeroFixture();
    const firstClaim = await claim();
    const outcome = await fixture.host
      .start(fixture.scope, {
        ...C_START_INPUT,
        threaded,
        idempotencyKey: firstClaim.key,
        startReservation: firstClaim,
      })
      .catch((error: unknown) => error);
    expect(
      await native.readSnapshot({
        workflowId: 'durable-agentic-loop',
        runId: 'acme_run',
      }),
    ).toBeUndefined();
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(fixture.state.has(THREAD_BINDING_KEY)).toBe(false);
    expect(await reservations.readForAdmission('agent-zero-key')).toMatchObject(
      { state: 'reserved', binding: { kind: 'unbound' } },
    );
    expect(effects()).toBe(0);
    expect(outcome).toMatchObject({ status: 503 });
    await fence.transition({ expected: 'draining', next: 'open' });
    const nextClaim = await claim();
    await expect(
      fixture.host.start(fixture.scope, {
        ...C_START_INPUT,
        threaded,
        idempotencyKey: nextClaim.key,
        startReservation: nextClaim,
      }),
    ).resolves.toMatchObject({
      runId: 'acme_run',
      summary: { status: 'success' },
    });
    expect(effects()).toBe(1);
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
  });
});

describe('FS8 D3 host activation explicit legacy observation', () => {
  it.each([
    null,
    3,
    [],
  ])('refuses present malformed provenance before public legacy fallback (%j)', async (provenance) => {
    const fixture = harness(['writer'], {
      runtime: {
        authoritativeStartState: vi.fn(async () => {
          throw new RunStateUnreadableError('durable-agentic-loop', 'acme_run');
        }),
      },
    });
    fixture.setSnapshot({
      requestContext: { 'flowsafe.runProvenance': provenance },
    });
    fixture.setSummary({ runId: 'acme_run', status: 'success' });
    const record = {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'http.start',
    };
    fixture.state.set(TEST_RUN_RECORD_KEY, record);
    const outcome = await fixture.host
      .route(
        new Request(
          `https://thread/_flowsafe/agent-host/runs/writer/acme_run?resourceId=${RESOURCE_ID}`,
        ),
        fixture.scope,
      )
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_RUN_RECORD_KEY)).toEqual(record);
    expect(outcome).toBeInstanceOf(RunStateUnreadableError);
    expect(doErrorResponse(outcome).status).toBe(503);
  });
});

async function hostR1AgentFixture(
  input: {
    mode?: 'custom-null' | 'actual-prefix' | 'fenced';
    provenance?: 'v1' | 'absent' | 'modern';
    status?: 'pending' | 'suspended' | 'success' | 'cancelled';
    threaded?: boolean;
    keyed?: boolean;
    wired?: boolean;
    journal?: boolean;
    lifecycle?: boolean;
    snapshotTarget?: { agentId: string; threadId: string };
  } = {},
) {
  const core = await vi.importActual<typeof import('@mastra/core/mastra')>(
    '@mastra/core/mastra',
  );
  const { InMemoryStore } = await import('@mastra/core/storage');
  const breakwater = await vi.importActual<
    typeof import('@proofoftech/breakwater')
  >('@proofoftech/breakwater');
  mocked.actualFactory = true;
  mocked.actualAgent = breakwater.createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Unused host regression agent.',
    model: 'openai/gpt-4o-mini',
    allowedRoles: ['operator'],
    policies: [],
    audit: new breakwater.AuditLogger(),
    maxSteps: 1,
    toolChoice: 'auto',
  });
  mocked.mastra.mockImplementation(
    (configuration) => new core.Mastra(configuration),
  );
  const mode = input.mode ?? 'actual-prefix';
  const threaded = input.threaded ?? true;
  const status = input.status ?? 'suspended';
  const version = input.provenance ?? 'v1';
  const sql = openSqlite() as ReturnType<typeof openSqlite> & { close(): void };
  const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase &
    ResourceOwnershipDatabase;
  const storage =
    mode === 'custom-null'
      ? new InMemoryStore()
      : createD1Storage({ binding, tablePrefix: 'host_r1_' });
  await storage.init();
  const { StartIdempotencyStore } = await import(
    '../do-runner/start-idempotency.js'
  );
  const reservations = new StartIdempotencyStore(binding);
  const fence = new ExecutionFenceStore(binding);
  await fence.seed('open');
  const app = init(
    { storage },
    {
      executionFence: mode === 'fenced' ? fence : 'none',
      startIdempotency: input.wired === false ? 'none' : reservations,
    },
  );
  const resources = new D1ResourceOwnershipStore(binding);
  const approvals = {
    list: vi.fn(async () => [] as ApprovalRecord[]),
    createAsPrincipal: vi.fn(async () => {
      throw new Error('unexpected approval creation');
    }),
    supersedeStaleAsPrincipal: vi.fn<
      ApprovalService['supersedeStaleAsPrincipal']
    >(async () => null),
  };
  const dispatch = vi.fn(async () => {});
  const fixture = harness(['writer'], {
    storage,
    init: app,
    resourceAccess: resources,
    approvalService: approvals as unknown as ApprovalService,
    discardScheduleDispatch: dispatch,
  });
  const execution = {
    tablePrefix: mode === 'custom-null' ? null : 'host_r1_',
    workflowId: 'durable-agentic-loop',
    runId: 'acme_run',
    startToken: 'host-r1-generation',
  };
  let claim:
    | import('../do-runner/start-reservation-contract.js').StartReservationReading
    | undefined;
  if (input.keyed) {
    const reserved = await reservations.reserve({
      key: 'host-r1-key',
      owner: HUMAN_OWNER,
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread',
      mintRunId: () => 'acme_run',
    });
    claim = await reservations.claimReservation(reserved.reservation);
    if (!claim) throw new Error('missing host claim');
    await reservations.bindPreparedStart(claim, {
      ...execution,
      owner: HUMAN_OWNER,
      target: { kind: 'agent', id: 'writer', threadId: 'acme_thread' },
    });
  }
  const recovery = ownerRecovery('acme_run', {
    phase: mode === 'fenced' ? 'prepared' : 'prepared-unfenced',
    token: 'host-r1-attempt',
    execution,
    threaded,
    ...(claim ? { startReservation: claim } : {}),
  });
  seedRecoveryState(fixture.state, 'acme_run', recovery, threaded);
  if (!input.journal) fixture.state.delete(TEST_OWNER_RECOVERY_KEY);
  await resources.reserveAll(
    [
      { kind: 'run', resourceId: 'acme_run' },
      { kind: 'thread', resourceId: 'acme_thread' },
      { kind: 'resource', resourceId: RESOURCE_ID },
    ],
    HUMAN_OWNER,
    recovery.token as string,
  );
  if (!input.journal)
    await resources.settleReservation(recovery.token as string, []);
  const workflows = await storage.getStore('workflows');
  if (!workflows) throw new Error('missing host workflow domain');
  const snapshotAgentId = input.snapshotTarget?.agentId ?? 'writer';
  const snapshotThreadId = input.snapshotTarget?.threadId ?? 'acme_thread';
  const snapshotResourceId = resourceIdFromKey(snapshotThreadId);
  const snapshot: import('@mastra/core/workflows').WorkflowRunState = {
    runId: 'acme_run',
    status:
      status as import('@mastra/core/workflows').WorkflowRunState['status'],
    value: {},
    context: {
      input: {
        agentId: snapshotAgentId,
        messageListState: {
          memoryInfo: threaded
            ? { threadId: snapshotThreadId, resourceId: snapshotResourceId }
            : null,
        },
      },
    } as unknown as import('@mastra/core/workflows').WorkflowRunState['context'],
    serializedStepGraph: [],
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 123,
    requestContext: {
      runId: 'acme_run',
      threadId: snapshotThreadId,
      resourceId: snapshotResourceId,
      'breakwater.auditContext': {
        agentId: snapshotAgentId,
        threadId: snapshotThreadId,
        resourceId: snapshotResourceId,
      },
      ...(version === 'absent'
        ? {}
        : {
            'flowsafe.runProvenance':
              version === 'v1'
                ? {
                    version: 1,
                    attemptToken: 'host-r1-legacy-leg',
                    requestedBy: 'operator-1',
                    requestedByKind: 'human',
                    resumeCounts: [],
                  }
                : {
                    version: 2,
                    startToken: execution.startToken,
                    attemptToken: recovery.token,
                    requestedBy: 'operator-1',
                    requestedByKind: 'human',
                    resumeCounts: [],
                    startIdentity: {
                      owner: HUMAN_OWNER,
                      target: {
                        kind: 'agent',
                        id: snapshotAgentId,
                        threadId: snapshotThreadId,
                      },
                    },
                    agentStart: { threaded },
                  },
          }),
      ...(input.lifecycle
        ? {
            'flowsafe.runLifecycle': {
              version: 1,
              revision: 2,
              terminal: {
                status: 'cancelled',
                error: { code: 'CANCELLED', message: 'run was cancelled' },
                transitionedAt: 10,
                replayPrincipals: [HUMAN_OWNER],
              },
              scheduleDispatch: {
                scheduleId: SCHEDULE_ID,
                dispatchId: DISPATCH_ID,
              },
            },
          }
        : {}),
    },
  };
  const persist = async (value = snapshot) =>
    workflows.persistWorkflowSnapshot({
      workflowName: execution.workflowId,
      runId: execution.runId,
      snapshot: value,
    });
  await persist();
  const read = () =>
    workflows.loadWorkflowSnapshot({
      workflowName: execution.workflowId,
      runId: execution.runId,
    });
  const owners = () =>
    sql
      .prepare(
        'SELECT * FROM flowsafe_resource_owners ORDER BY resource_kind, resource_id',
      )
      .all();
  return {
    ...fixture,
    storage,
    app,
    sql,
    workflows,
    resources,
    reservations,
    claim,
    fence,
    execution,
    recovery,
    snapshot,
    persist,
    read,
    owners,
    approvals,
    dispatch,
  };
}

function hostR1AgentRequest(suffix = '', query = '') {
  return new Request(
    `https://thread/_flowsafe/agent-host/runs/writer/acme_run${suffix}?resourceId=${RESOURCE_ID}${query}`,
    suffix ? { method: 'POST' } : undefined,
  );
}

describe('agent host selector lookup isolation', () => {
  it.each([
    ['modern', true, 'status'],
    ['modern', false, 'stream'],
    ['modern', true, 'terminate'],
    ['v1', false, 'status'],
    ['v1', true, 'stream'],
    ['v1', false, 'terminate'],
    ['absent', true, 'status'],
    ['absent', false, 'stream'],
    ['absent', true, 'terminate'],
  ] as const)('maps a coherent foreign snapshot to public 404 without effects (%s threaded=%s %s)', async (provenance, threaded, route) => {
    const f = await hostR1AgentFixture({
      provenance,
      threaded,
      keyed: true,
      snapshotTarget: { agentId: 'other-agent', threadId: 'other-thread' },
    });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    const capability = (f.workflows as FencedWorkflowsStorageD1)[
      FENCED_WORKFLOW_STORAGE
    ];
    if (!capability) throw new Error('missing native observation');
    const read = vi.fn(capability.readSnapshot.bind(capability));
    Object.defineProperty(f.workflows, FENCED_WORKFLOW_STORAGE, {
      value: { ...capability, readSnapshot: read },
      configurable: true,
    });
    const cancel = vi.spyOn(f.app.runtime, 'cancelActiveExecution');
    const terminate = vi.spyOn(f.app.runtime, 'terminateAsPrincipal');
    const cleanup = vi.spyOn(f.app.runtime, 'completeTerminalCleanup');
    const settle = vi.spyOn(f.resources, 'settleReservation');
    const resume = vi.spyOn(FlowsafeDurableAgent.prototype, 'resumeViaRuntime');
    const observe = vi.spyOn(FlowsafeDurableAgent.prototype, 'observe');
    const owners = f.owners();
    const state = structuredClone(f.state);
    const claim = await f.reservations.readForAdmission('host-r1-key');
    const suffix = route === 'status' ? '' : `/${route}`;
    const request = new Request(
      `https://thread/_flowsafe/agent-host/runs/writer/acme_run${suffix}?resourceId=${RESOURCE_ID}`,
      {
        method: route === 'terminate' ? 'POST' : 'GET',
      },
    );
    const outcome = await f.host
      .route(request, f.scope)
      .catch((error) => error);
    expect(outcome).toMatchObject({ status: 404, message: 'run not found' });
    expect(doErrorResponse(outcome).status).toBe(404);
    expect(read).toHaveBeenCalledOnce();
    expect(cancel).not.toHaveBeenCalled();
    expect(terminate).not.toHaveBeenCalled();
    expect(cleanup).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(resume).not.toHaveBeenCalled();
    expect(observe).not.toHaveBeenCalled();
    expect(f.approvals.list).not.toHaveBeenCalled();
    expect(f.approvals.createAsPrincipal).not.toHaveBeenCalled();
    expect(f.dispatch).not.toHaveBeenCalled();
    expect(f.owners()).toEqual(owners);
    expect(f.state).toEqual(state);
    expect(await f.reservations.readForAdmission('host-r1-key')).toEqual(claim);
  });

  it.each([
    'modern',
    'v1',
    'absent',
  ] as const)('keeps a corrupt foreign public lookup unreadable: %s', async (provenance) => {
    const f = await hostR1AgentFixture({
      provenance,
      snapshotTarget: { agentId: 'other-agent', threadId: 'other-thread' },
    });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    Object.assign(f.snapshot.requestContext ?? {}, {
      'breakwater.auditContext': { agentId: 'contradiction' },
    });
    await f.persist();
    const selected = vi.spyOn(f.app.runtime, 'authoritativeStartState');
    const before = structuredClone(f.state);
    const outcome = await f.host
      .route(hostR1AgentRequest(), f.scope)
      .catch((error) => error);
    expect(outcome).toBeInstanceOf(RunStateUnreadableError);
    expect(outcome).not.toBeInstanceOf(AgentRunSelectorMismatchError);
    expect(doErrorResponse(outcome).status).toBe(503);
    expect(selected).toHaveBeenCalledOnce();
    expect(f.state).toEqual(before);
    expect(f.approvals.list).not.toHaveBeenCalled();
    expect(f.dispatch).not.toHaveBeenCalled();
  });

  it('checks ordinary termination again inside its dispatch lock after storage changes', async () => {
    const f = await hostR1AgentFixture({ provenance: 'modern' });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    const selected = vi.spyOn(f.app.runtime, 'authoritativeStartState');
    const cancel = vi
      .spyOn(f.app.runtime, 'cancelActiveExecution')
      .mockImplementation(async () => {
        const context = f.snapshot.requestContext;
        if (!context) throw new Error('missing snapshot context');
        context['flowsafe.runProvenance'].startIdentity.target.threadId =
          'other-thread';
        Object.assign(context, {
          threadId: 'other-thread',
          resourceId: 'other-thread',
        });
        Object.assign(context['breakwater.auditContext'], {
          threadId: 'other-thread',
          resourceId: 'other-thread',
        });
        const input = f.snapshot.context.input as unknown as {
          messageListState: { memoryInfo: object };
        };
        Object.assign(input.messageListState.memoryInfo, {
          threadId: 'other-thread',
          resourceId: 'other-thread',
        });
        await f.persist();
        return false;
      });
    const terminate = vi.spyOn(f.app.runtime, 'terminateAsPrincipal');
    const before = structuredClone(f.state);
    const outcome = await f.host
      .route(hostR1AgentRequest('/terminate'), f.scope)
      .catch((error) => error);
    expect(outcome).toMatchObject({ status: 404 });
    expect(selected).toHaveBeenCalledTimes(2);
    expect(cancel).toHaveBeenCalledOnce();
    expect(terminate).not.toHaveBeenCalled();
    expect(f.state).toEqual(before);
    expect(f.approvals.list).not.toHaveBeenCalled();
    expect(f.dispatch).not.toHaveBeenCalled();
  });

  it.each([
    'status',
    'stream',
    'terminate',
  ] as const)('does not reread a selected absent row for public %s', async (route) => {
    const f = await hostR1AgentFixture({ provenance: 'modern' });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    await f.workflows.deleteWorkflowRunById({
      workflowName: f.execution.workflowId,
      runId: f.execution.runId,
    });
    const selected = vi.spyOn(f.app.runtime, 'authoritativeStartState');
    const request = new Request(
      `https://thread/_flowsafe/agent-host/runs/writer/acme_run${route === 'status' ? '' : `/${route}`}?resourceId=${RESOURCE_ID}`,
      { method: route === 'terminate' ? 'POST' : 'GET' },
    );
    const outcome = await f.host
      .route(request, f.scope)
      .catch((error) => error);
    expect(outcome).toMatchObject({ status: 404 });
    expect(selected).toHaveBeenCalledOnce();
  });

  it.each([
    'replay',
    'dispatch',
    'terminate replay',
    'resume',
    'schedule dispatch',
    'blocking',
    'proof',
  ] as const)('keeps a coherent foreign row present on the strict %s path', async (route) => {
    const f = await hostR1AgentFixture({
      provenance: 'modern',
      keyed: true,
      snapshotTarget: { agentId: 'writer', threadId: 'other-thread' },
    });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    const state = structuredClone(f.state);
    const owners = f.owners();
    const claim = await f.reservations.readForAdmission('host-r1-key');
    const terminate = vi.spyOn(f.app.runtime, 'terminateAsPrincipal');
    const resume = vi.spyOn(FlowsafeDurableAgent.prototype, 'resumeViaRuntime');
    const selected = vi.spyOn(f.app.runtime, 'authoritativeStartState');
    const operation = async () => {
      if (route === 'blocking') return f.host.blockingRun(f.scope);
      if (route === 'schedule dispatch')
        return f.host.scheduleDispatchStatus(f.scope, {
          agentId: 'writer',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
        });
      if (route === 'proof') {
        const bound = await f.host.resolveBoundAgent(f.scope, {
          agentId: 'writer',
          entryPath: 'http.start',
        });
        return bound.durableAgent.proofExecutionFor(
          f.app.runtime,
          'acme_thread',
          'acme_run',
        );
      }
      if (route === 'resume')
        return f.host.route(
          new Request('https://thread/_flowsafe/agent-host/resume', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              agentId: 'writer',
              threadId: 'acme_thread',
              resourceId: RESOURCE_ID,
              runId: 'acme_run',
              requestedBy: 'reviewer-2',
              entryPath: 'approval.resume',
              resumeData: {},
            }),
          }),
          f.scope,
        );
      return f.host.route(
        hostR1AgentRequest(
          route === 'terminate replay' ? '/terminate' : '',
          route === 'dispatch' ? '&dispatch=1' : '&dispatch=1&replay=1',
        ),
        f.scope,
      );
    };
    const outcome = await operation().catch((error) => error);
    expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
    expect(doErrorResponse(outcome).status).toBe(503);
    expect(selected).toHaveBeenCalledOnce();
    expect(terminate).not.toHaveBeenCalled();
    expect(resume).not.toHaveBeenCalled();
    expect(f.state).toEqual(state);
    expect(f.owners()).toEqual(owners);
    expect(await f.reservations.readForAdmission('host-r1-key')).toEqual(claim);
    expect(f.approvals.list).not.toHaveBeenCalled();
    expect(f.dispatch).not.toHaveBeenCalled();
  });

  it.each([
    'prepared',
    'prepared-unfenced',
  ] as const)('retains a %s keyed journal and rearms recovery for a coherent foreign row', async (phase) => {
    const f = await hostR1AgentFixture({
      mode: phase === 'prepared' ? 'fenced' : 'actual-prefix',
      provenance: 'modern',
      journal: true,
      keyed: true,
      snapshotTarget: { agentId: 'writer', threadId: 'other-thread' },
    });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    const state = structuredClone(f.state);
    const owners = f.owners();
    const row = await f.read();
    const claim = await f.reservations.readForAdmission('host-r1-key');
    const settle = vi.spyOn(f.resources, 'settleReservation');
    const settleStart = vi.spyOn(f.app.runtime, 'settleStartExecution');
    const recover = vi.spyOn(f.app.runtime, 'recoverStartAttempt');
    const outcome = await f.host
      .recoverOwnership(f.scope)
      .catch((error) => error);
    expect(doErrorResponse(outcome).status).toBe(503);
    if (phase === 'prepared-unfenced')
      expect(outcome).toBeInstanceOf(AgentRunSelectorMismatchError);
    expect(f.state).toEqual(state);
    expect(f.owners()).toEqual(owners);
    expect(await f.read()).toEqual(row);
    expect(await f.reservations.readForAdmission('host-r1-key')).toEqual(claim);
    expect(f.alarmAt()).toBeDefined();
    expect(settle).not.toHaveBeenCalled();
    expect(settleStart).not.toHaveBeenCalled();
    expect(recover).toHaveBeenCalledTimes(phase === 'prepared' ? 1 : 0);
    expect(f.approvals.list).not.toHaveBeenCalled();
    expect(f.dispatch).not.toHaveBeenCalled();
  });

  it('preserves a reserved start when actual host replay observes another thread', async () => {
    const f = await hostR1AgentFixture({
      provenance: 'modern',
      keyed: true,
      snapshotTarget: { agentId: 'writer', threadId: 'other-thread' },
    });
    onTestFinished(() => {
      vi.restoreAllMocks();
      f.sql.close();
    });
    const { createPrincipalActorContext, InMemoryApprovalStoreFactory } =
      await import('../approval-api/index.js');
    const context = createPrincipalActorContext({
      principal: f.scope.principal,
      storeFactory: new InMemoryApprovalStoreFactory(),
      buildService: () => f.approvals as unknown as ApprovalService,
    });
    const hits: string[] = [];
    const topology = createAgentThreadTopology(
      {
        idFromName: (name: string) => name,
        get: () => ({
          fetch: (async (
            request: Request | string,
            init?: import('../host-kit/thread-topology.js').ThreadRequestInit,
          ) => {
            const url = typeof request === 'string' ? request : request.url;
            hits.push(url);
            try {
              return (
                (await f.host.route(new Request(url, init), f.scope)) ??
                new Response(null, { status: 404 })
              );
            } catch (error) {
              return doErrorResponse(error);
            }
          }) as import('../host-kit/thread-topology.js').ThreadStubLike['fetch'],
        }),
      },
      'test-deployment-identity-secret-0001',
      { startIdempotency: f.reservations, executionFence: 'none' },
    );
    const before = await f.reservations.readForAdmission('host-r1-key');
    const outcome = await topology
      .start(context, {
        agentId: 'writer',
        prompt: 'retry',
        entryPath: 'http.start',
        idempotencyKey: 'host-r1-key',
      })
      .catch((error) => error);
    expect(outcome).toMatchObject({
      status: 503,
      message:
        "run 'acme_run' of workflow 'durable-agentic-loop' state is not readable",
    });
    expect(hits).toHaveLength(1);
    expect(hits[0]).toContain('dispatch=1&replay=1');
    expect(await f.reservations.readForAdmission('host-r1-key')).toEqual(
      before,
    );
    expect(mocked.stream).not.toHaveBeenCalled();
    expect(f.approvals.list).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 host R1 agent legacy status guards', () => {
  it.each([
    ['missing binding', true, 404],
    ['wrong binding', true, 404],
    ['unthreaded binding', false, 404],
    ['wrong record agent', true, 404],
    ['missing principal', true, 409],
  ] as const)('refuses before approval effects (%s)', async (guard, threaded, status) => {
    const fixture = await hostR1AgentFixture({ threaded });
    if (guard === 'missing binding') fixture.state.delete(THREAD_BINDING_KEY);
    if (guard === 'wrong binding' || guard === 'unthreaded binding')
      fixture.state.set(THREAD_BINDING_KEY, {
        version: 1,
        agentId: guard === 'wrong binding' ? 'foreign' : 'writer',
        resourceId: RESOURCE_ID,
      });
    if (guard === 'wrong record agent')
      fixture.state.set(TEST_RUN_RECORD_KEY, {
        ...(fixture.state.get(TEST_RUN_RECORD_KEY) as object),
        agentId: 'foreign',
      });
    if (guard === 'missing principal')
      fixture.state.delete(TEST_RUN_RECORD_KEY);
    const before = structuredClone(fixture.state);
    const outcome = await fixture.host
      .route(hostR1AgentRequest(), fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(fixture.approvals.createAsPrincipal).not.toHaveBeenCalled();
    expect(fixture.state).toEqual(before);
    expect(outcome).toMatchObject({ status });
  });

  it.each([
    'v1',
    'absent',
  ] as const)('retains raw pending with terminal projection (%s)', async (provenance) => {
    const fixture = await hostR1AgentFixture({
      provenance,
      status: 'pending',
      lifecycle: true,
    });
    const before = fixture.owners();
    const outcome = await fixture.host
      .blockingRun(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.owners()).toEqual(before);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(fixture.dispatch).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ runId: 'acme_run' });
  });

  it.each([
    'malformed provenance',
    'source error',
  ] as const)('never falls back after actual selected read failure (%s)', async (failure) => {
    const fixture = await hostR1AgentFixture();
    if (failure === 'malformed provenance') {
      fixture.snapshot.requestContext = {
        ...fixture.snapshot.requestContext,
        'flowsafe.runProvenance': { version: 2 },
      };
      await fixture.persist();
    } else {
      const domain = fixture.workflows as FencedWorkflowsStorageD1;
      const native = domain[FENCED_WORKFLOW_STORAGE];
      if (!native) throw new Error('missing capability');
      Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
        value: {
          ...native,
          readSnapshot: async () => {
            throw new Error('actual source failed');
          },
        },
        configurable: true,
      });
    }
    const nominal = vi.spyOn(fixture.workflows, 'loadWorkflowSnapshot');
    const before = structuredClone(fixture.state);
    const outcome = await fixture.host
      .route(hostR1AgentRequest(), fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state).toEqual(before);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(nominal).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(RunStateUnreadableError);
    expect(doErrorResponse(outcome).status).toBe(503);
  });
});

describe('FS8 D3 host R1 agent recovery barriers', () => {
  it.each([
    ['custom-null', 'missing'],
    ['custom-null', 'pending'],
    ['custom-null', 'suspended'],
    ['actual-prefix', 'missing'],
    ['actual-prefix', 'pending'],
    ['actual-prefix', 'suspended'],
  ] as const)('retains keyed nonterminal authority before missing-store refusal (%s %s)', async (mode, status) => {
    const fixture = await hostR1AgentFixture({
      mode,
      status: status === 'missing' ? 'pending' : status,
      provenance: 'modern',
      keyed: true,
      wired: false,
      journal: true,
    });
    if (status === 'missing')
      await fixture.workflows.deleteWorkflowRunById({
        workflowName: fixture.execution.workflowId,
        runId: 'acme_run',
      });
    const before = fixture.owners();
    const bound = await fixture.reservations.readForAdmission('host-r1-key');
    const row = await fixture.read();
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    const settle = vi.spyOn(fixture.resources, 'settleReservation');
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.owners()).toEqual(before);
    expect(settle).not.toHaveBeenCalled();
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      fixture.recovery,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.state.has(THREAD_BINDING_KEY)).toBe(true);
    expect(await fixture.reservations.readForAdmission('host-r1-key')).toEqual(
      bound,
    );
    expect(await fixture.read()).toEqual(row);
    expect(fixture.alarmAt()).toBeDefined();
    expect(b2).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ status: 503 });
  });

  it.each([
    ['custom-null', true],
    ['custom-null', false],
    ['actual-prefix', true],
    ['actual-prefix', false],
  ] as const)('finishes wired or unkeyed suspended recovery (%s keyed=%s)', async (mode, keyed) => {
    const fixture = await hostR1AgentFixture({
      mode,
      provenance: 'modern',
      keyed,
      journal: true,
      status: 'suspended',
    });
    const before = keyed
      ? await fixture.reservations.readForAdmission('host-r1-key')
      : undefined;
    await fixture.host.recoverOwnership(fixture.scope);
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(
      keyed
        ? await fixture.reservations.readForAdmission('host-r1-key')
        : undefined,
    ).toEqual(before);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    'generation',
    'phase',
  ] as const)('preserves all H ownership when the journal changes inside actual absence read (%s)', async (change) => {
    const fixture = await hostR1AgentFixture({
      mode: 'fenced',
      provenance: 'modern',
      journal: true,
    });
    await fixture.workflows.deleteWorkflowRunById({
      workflowName: fixture.execution.workflowId,
      runId: 'acme_run',
    });
    const before = fixture.owners();
    const record = structuredClone(fixture.state.get(TEST_RUN_RECORD_KEY));
    const binding = structuredClone(fixture.state.get(THREAD_BINDING_KEY));
    const replacement = structuredClone(fixture.recovery);
    if (change === 'generation')
      (replacement.execution as { startToken: string }).startToken =
        'replacement-generation';
    else replacement.phase = 'prepared-unfenced';
    const domain = fixture.workflows as FencedWorkflowsStorageD1;
    const native = domain[FENCED_WORKFLOW_STORAGE];
    if (!native) throw new Error('missing capability');
    const read = vi.fn(
      async (...args: Parameters<typeof native.readSnapshot>) => {
        const row = await native.readSnapshot(...args);
        expect(row).toBeUndefined();
        fixture.state.set(TEST_OWNER_RECOVERY_KEY, replacement);
        return row;
      },
    );
    Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
      value: { ...native, readSnapshot: read },
      configurable: true,
    });
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.owners()).toEqual(before);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(replacement);
    expect(fixture.state.get(TEST_RUN_RECORD_KEY)).toEqual(record);
    expect(fixture.state.get(THREAD_BINDING_KEY)).toEqual(binding);
    expect(read).toHaveBeenCalledOnce();
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    ['approvals', 'failure'],
    ['approvals', 'response loss'],
    ['dispatch', 'failure'],
    ['dispatch', 'response loss'],
    ['run owner', 'failure'],
    ['run owner', 'response loss'],
    ['H bookkeeping', 'failure'],
    ['H bookkeeping', 'response loss'],
    ['completion', 'failure'],
    ['completion', 'response loss'],
  ] as const)('retains journal through ordered lifecycle retry (%s %s)', async (boundary, failureMode) => {
    const fixture = await hostR1AgentFixture({
      mode: 'fenced',
      provenance: 'modern',
      status: 'cancelled',
      journal: true,
      lifecycle: true,
    });
    fixture.sql
      .prepare(
        "UPDATE flowsafe_resource_owners SET reservation_token = NULL WHERE resource_kind = 'run' AND resource_id = 'acme_run'",
      )
      .run();
    const failure = new Error('host lifecycle boundary failed');
    const events: string[] = [];
    let armed = true;
    const enter = async <T>(
      name: string,
      action: () => Promise<T>,
    ): Promise<T> => {
      events.push(name);
      if (armed && name === boundary && failureMode === 'failure')
        throw failure;
      const result = await action();
      if (armed && name === boundary) throw failure;
      return result;
    };
    let approval: ApprovalRecord = {
      id: 'host-r1-approval',
      workflowId: fixture.execution.workflowId,
      runId: 'acme_run',
      title: 'Held approval',
      connectors: [],
      priority: 'normal',
      status: 'pending',
      createdAt: '2026-09-08T00:00:00.000Z',
      updatedAt: '2026-09-08T00:00:00.000Z',
    };
    fixture.approvals.list.mockImplementation(async () => [
      structuredClone(approval),
    ]);
    fixture.approvals.supersedeStaleAsPrincipal.mockImplementation(() =>
      enter('approvals', async () => {
        approval = { ...approval, status: 'rejected' };
        return approval;
      }),
    );
    fixture.dispatch.mockImplementation(() =>
      enter('dispatch', async () => {}),
    );
    const release = fixture.resources.release.bind(fixture.resources);
    vi.spyOn(fixture.resources, 'release').mockImplementation((...args) =>
      enter('run owner', () => release(...args)),
    );
    const settle = fixture.resources.settleReservation.bind(fixture.resources);
    vi.spyOn(fixture.resources, 'settleReservation').mockImplementation(
      (...args) => enter('H bookkeeping', () => settle(...args)),
    );
    const complete = fixture.app.runtime.completeTerminalCleanup.bind(
      fixture.app.runtime,
    );
    vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup').mockImplementation(
      (...args) => enter('completion', () => complete(...args)),
    );
    const order = [
      'H bookkeeping',
      'approvals',
      'dispatch',
      'run owner',
      'completion',
    ];
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    const reached = order.indexOf(boundary);
    expect(approval.status).toBe(
      boundary === 'H bookkeeping' ||
        (boundary === 'approvals' && failureMode === 'failure')
        ? 'pending'
        : 'rejected',
    );
    expect(events, String(outcome)).toEqual(order.slice(0, reached + 1));
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(
      fixture.recovery,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(
      boundary !== 'completion',
    );
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      reached < order.indexOf('run owner') ||
        (boundary === 'run owner' && failureMode === 'failure')
        ? HUMAN_OWNER
        : undefined,
    );
    expect(fixture.alarmAt()).toBeDefined();
    expect(outcome).toBe(failure);
    armed = false;
    events.length = 0;
    await fixture.host.recoverOwnership(fixture.scope);
    expect(events).toEqual(
      boundary === 'completion' && failureMode === 'response loss'
        ? ['H bookkeeping']
        : boundary === 'H bookkeeping' ||
            (boundary === 'approvals' && failureMode === 'failure')
          ? order
          : order.filter((step) => step !== 'approvals'),
    );
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toBeUndefined();
    expect(await fixture.read()).toMatchObject({
      requestContext: {
        'flowsafe.runLifecycle': {
          terminal: { cleanupCompletedAt: expect.any(Number) },
        },
      },
    });
    expect(mocked.stream).not.toHaveBeenCalled();
  });
});

describe('FS8 D3 host R1 atomic agent admission', () => {
  it('admits one different run ID on a prebound thread with committed same-owner claims', async () => {
    const fixture = harness();
    fixture.state.set(THREAD_BINDING_KEY, {
      version: 1,
      agentId: 'writer',
      resourceId: RESOURCE_ID,
    });
    await fixture.resources.claim('thread', 'acme_thread', HUMAN_OWNER);
    await fixture.resources.claim('resource', RESOURCE_ID, HUMAN_OWNER);
    fixture.setSummary({ runId: 'acme_run', status: 'suspended' }, false);
    const release = cDeferred();
    mocked.stream.mockImplementation(async () => {
      await release.promise;
      return {};
    });
    const first = fixture.host.start(fixture.scope, C_START_INPUT);
    const second = fixture.host.start(fixture.scope, {
      ...C_START_INPUT,
      runId: 'other-run',
    });
    const settled = Promise.allSettled([first, second]);
    try {
      await vi.waitFor(() => expect(mocked.stream).toHaveBeenCalled());
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      expect(mocked.stream).toHaveBeenCalledOnce();
      expect(
        [...fixture.state.keys()].filter((key) =>
          key.startsWith(RUN_RECORD_PREFIX),
        ),
      ).toHaveLength(1);
    } finally {
      release.resolve();
    }
    const results = await settled;
    expect(
      results.filter((result) => result.status === 'fulfilled'),
    ).toHaveLength(1);
    expect(
      results.filter((result) => result.status === 'rejected'),
    ).toHaveLength(1);
  });
});

describe('FS8 D3 host R1 ordinary legacy agent lifecycle', () => {
  it.each([
    ['v1', true],
    ['v1', false],
    ['absent', true],
    ['absent', false],
  ] as const)('terminates and replays the actual selected legacy row without settling retained key (%s threaded=%s)', async (provenance, threaded) => {
    const fixture = await hostR1AgentFixture({
      provenance,
      threaded,
      keyed: true,
    });
    const bound = await fixture.reservations.readForAdmission('host-r1-key');
    const settle = vi.spyOn(fixture.app.runtime, 'settleStartExecution');
    const b2 = vi.spyOn(fixture.app.runtime, 'recoverStartAttempt');
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const terminate = vi.spyOn(fixture.app.runtime, 'terminateAsPrincipal');
    const response = await fixture.host
      .route(hostR1AgentRequest('/terminate'), fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toBeUndefined();
    expect(await fixture.reservations.readForAdmission('host-r1-key')).toEqual(
      bound,
    );
    expect(settle).not.toHaveBeenCalled();
    expect(b2).not.toHaveBeenCalled();
    expect(complete).toHaveBeenCalledOnce();
    expect(response).toMatchObject({ status: 200 });
    const bytes = JSON.stringify(await fixture.read());
    const replay = await fixture.host
      .route(hostR1AgentRequest('/terminate', '&replay=1'), fixture.scope)
      .catch((error: unknown) => error);
    expect(JSON.stringify(await fixture.read())).toBe(bytes);
    expect(await fixture.reservations.readForAdmission('host-r1-key')).toEqual(
      bound,
    );
    expect(complete).toHaveBeenCalledOnce();
    expect(terminate).toHaveBeenCalledTimes(2);
    expect(replay).toMatchObject({ status: 200 });
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    ['v1', true],
    ['v1', false],
    ['absent', true],
    ['absent', false],
  ] as const)('retires terminal resume using canonical principal and actual owner despite changed requester (%s threaded=%s)', async (provenance, threaded) => {
    const fixture = await hostR1AgentFixture({ provenance, threaded });
    const { FlowsafeDurableAgent } = await import(
      '../agent-runner/durable-agent-runner.js'
    );
    const resume = vi
      .spyOn(FlowsafeDurableAgent.prototype, 'resumeViaRuntime')
      .mockImplementation(async () => {
        fixture.snapshot.status = 'success';
        fixture.snapshot.result = { result: 'resumed' };
        if (provenance === 'v1')
          fixture.snapshot.requestContext = {
            ...fixture.snapshot.requestContext,
            'flowsafe.runProvenance': {
              version: 1,
              attemptToken: 'host-r1-legacy-leg',
              requestedBy: 'reviewer-2',
              requestedByKind: 'human',
              resumeCounts: [],
            },
          };
        await fixture.persist();
        return {
          runId: 'acme_run',
          status: 'success',
          result: fixture.snapshot.result,
          requestedBy: provenance === 'v1' ? 'reviewer-2' : undefined,
        };
      });
    const settle = vi.spyOn(fixture.app.runtime, 'settleStartExecution');
    try {
      const response = await fixture.host
        .route(
          new Request('https://thread/_flowsafe/agent-host/resume', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              agentId: 'writer',
              threadId: 'acme_thread',
              resourceId: RESOURCE_ID,
              runId: 'acme_run',
              requestedBy: 'reviewer-2',
              entryPath: 'approval.resume',
              resumeData: {},
            }),
          }),
          fixture.scope,
        )
        .catch((error: unknown) => error);
      expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
      expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
        HUMAN_OWNER,
      );
      expect(settle).not.toHaveBeenCalled();
      expect(resume).toHaveBeenCalledOnce();
      expect(response).toMatchObject({ status: 200 });
      const bytes = JSON.stringify(await fixture.read());
      await expect(
        fixture.host.blockingRun(fixture.scope),
      ).resolves.toBeUndefined();
      expect(JSON.stringify(await fixture.read())).toBe(bytes);
      expect(resume).toHaveBeenCalledOnce();
    } finally {
      resume.mockRestore();
    }
  });

  it.each([
    ['v1', 'blocker', 'success'],
    ['absent', 'blocker', 'success'],
    ['v1', 'blocker', 'suspended'],
    ['absent', 'blocker', 'suspended'],
    ['v1', 'dispatch', 'success'],
    ['absent', 'dispatch', 'success'],
    ['v1', 'status', 'success'],
    ['absent', 'status', 'success'],
    ['v1', 'protected dispatch', 'success'],
    ['absent', 'protected dispatch', 'success'],
  ] as const)('selects cold actual legacy state (%s %s %s)', async (provenance, path, status) => {
    const fixture = await hostR1AgentFixture({
      provenance,
      status,
      keyed: true,
    });
    const bytes = JSON.stringify(await fixture.read());
    const bound = await fixture.reservations.readForAdmission('host-r1-key');
    expect(fixture.app.runtime.workflowIds()).toEqual([]);
    const operation = (async () => {
      if (path === 'blocker') return fixture.host.blockingRun(fixture.scope);
      if (path === 'dispatch')
        return fixture.host.scheduleDispatchStatus(fixture.scope, {
          agentId: 'writer',
          resourceId: RESOURCE_ID,
          runId: 'acme_run',
        });
      return fixture.host.route(
        hostR1AgentRequest(
          '',
          path === 'protected dispatch' ? '&dispatch=1' : '',
        ),
        fixture.scope,
      );
    })();
    const outcome = await operation.catch((error: unknown) => error);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(status === 'suspended');
    expect(JSON.stringify(await fixture.read())).toBe(bytes);
    expect(await fixture.reservations.readForAdmission('host-r1-key')).toEqual(
      bound,
    );
    expect(fixture.app.runtime.workflowIds()).toContain('durable-agentic-loop');
    expect(mocked.stream).not.toHaveBeenCalled();
    if (path === 'blocker')
      expect(outcome).toEqual(
        status === 'suspended'
          ? expect.objectContaining({ runId: 'acme_run' })
          : undefined,
      );
    else if (path === 'dispatch')
      expect(outcome).toMatchObject({ runId: 'acme_run', status });
    else expect(outcome).toMatchObject({ status: 200 });
  });

  it.each([
    'v1',
    'absent',
  ] as const)('retries legacy hook failure without repeated engine execution (%s)', async (provenance) => {
    const fixture = await hostR1AgentFixture({ provenance });
    const failure = new Error('approval observation failed');
    fixture.approvals.list.mockRejectedValueOnce(failure);
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const result = await fixture.host
      .route(hostR1AgentRequest('/terminate'), fixture.scope)
      .catch((error: unknown) => error);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(complete).not.toHaveBeenCalled();
    expect(result).toBe(failure);
    const retry = await fixture.host.route(
      hostR1AgentRequest('/terminate', '&replay=1'),
      fixture.scope,
    );
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.resources.owner('run', 'acme_run')).toBeUndefined();
    expect(complete).toHaveBeenCalledOnce();
    expect(retry?.status).toBe(200);
    expect(mocked.stream).not.toHaveBeenCalled();
  });

  it.each([
    'old journal',
    'modern journal',
    'malformed journal',
    'active execution',
  ] as const)('retains legacy terminal bookkeeping without cleanup authority (%s)', async (condition) => {
    const fixture = await hostR1AgentFixture({
      provenance: 'absent',
      status: 'cancelled',
      lifecycle: true,
    });
    if (condition === 'old journal')
      fixture.state.set(TEST_OWNER_RECOVERY_KEY, { version: 1 });
    if (condition === 'modern journal')
      fixture.state.set(TEST_OWNER_RECOVERY_KEY, fixture.recovery);
    if (condition === 'malformed journal')
      fixture.state.set(TEST_OWNER_RECOVERY_KEY, null);
    if (condition === 'active execution')
      vi.spyOn(fixture.app.runtime, 'isRunActive').mockReturnValue(true);
    const state = structuredClone(fixture.state);
    const before = fixture.owners();
    const outcome = await fixture.host
      .blockingRun(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.owners()).toEqual(before);
    expect(fixture.state).toEqual(state);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(fixture.dispatch).not.toHaveBeenCalled();
    expect(outcome).toBeDefined();
  });

  it.each([
    'record',
    'binding',
    'journal',
  ] as const)('preserves replacement after legacy cleanup observation waits (%s)', async (replacement) => {
    const fixture = await hostR1AgentFixture({
      status: 'cancelled',
      lifecycle: true,
    });
    const native = fixture.app.runtime.authoritativeStartState.bind(
      fixture.app.runtime,
    );
    async function replaceDuringRead(
      workflowId: string,
      runId: string,
      options: { readonly includeLegacy: true },
    ): Promise<
      | import('../do-runner/runtime.js').AuthoritativeStartState
      | import('../do-runner/runtime.js').LegacyRunState
      | null
    >;
    async function replaceDuringRead(
      workflowId: string,
      runId: string,
    ): Promise<
      import('../do-runner/runtime.js').AuthoritativeStartState | null
    >;
    async function replaceDuringRead(
      workflowId: string,
      runId: string,
      options?: { readonly includeLegacy: true },
    ): Promise<
      | import('../do-runner/runtime.js').AuthoritativeStartState
      | import('../do-runner/runtime.js').LegacyRunState
      | null
    > {
      const selected = options
        ? await native(workflowId, runId, options)
        : await native(workflowId, runId);
      if (replacement === 'record')
        fixture.state.set(TEST_RUN_RECORD_KEY, {
          ...(fixture.state.get(TEST_RUN_RECORD_KEY) as object),
          principal: { ...fixture.scope.principal, role: 'admin' },
        });
      if (replacement === 'binding')
        fixture.state.set(THREAD_BINDING_KEY, {
          version: 1,
          agentId: 'foreign',
          resourceId: RESOURCE_ID,
        });
      if (replacement === 'journal')
        fixture.state.set(TEST_OWNER_RECOVERY_KEY, { version: 1 });
      return selected;
    }
    vi.spyOn(fixture.app.runtime, 'authoritativeStartState').mockImplementation(
      replaceDuringRead,
    );
    const before = fixture.owners();
    const outcome = await fixture.host
      .blockingRun(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.owners()).toEqual(before);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(fixture.dispatch).not.toHaveBeenCalled();
    expect(outcome).toBeDefined();
  });
});

describe('FS8 D3 host R1 reserved lifecycle owner', () => {
  it('preserves the replacement journal before lifecycle effects after H settlement', async () => {
    const fixture = await hostR1AgentFixture({
      mode: 'fenced',
      provenance: 'modern',
      status: 'cancelled',
      journal: true,
      lifecycle: true,
    });
    const replacement = structuredClone(fixture.recovery);
    (replacement.execution as { startToken: string }).startToken =
      'replacement-after-settlement';
    const native = fixture.resources.settleReservation.bind(fixture.resources);
    vi.spyOn(fixture.resources, 'settleReservation').mockImplementation(
      async (...args) => {
        await native(...args);
        fixture.state.set(TEST_OWNER_RECOVERY_KEY, replacement);
      },
    );
    const release = vi.spyOn(fixture.resources, 'release');
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const outcome = await fixture.host
      .recoverOwnership(fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.approvals.list).not.toHaveBeenCalled();
    expect(fixture.dispatch).not.toHaveBeenCalled();
    expect(release).not.toHaveBeenCalled();
    expect(complete).not.toHaveBeenCalled();
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(replacement);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(outcome).toBeInstanceOf(Error);
  });

  it.each([
    true,
    false,
  ])('retires the H-reserved run owner before journal lifecycle completion (threaded=%s)', async (threaded) => {
    const fixture = await hostR1AgentFixture({
      mode: 'fenced',
      threaded,
      provenance: 'modern',
      status: 'cancelled',
      journal: true,
      lifecycle: true,
    });
    await fixture.host.recoverOwnership(fixture.scope);
    expect(
      fixture.sql
        .prepare(
          "SELECT * FROM flowsafe_resource_owners WHERE resource_kind = 'run'",
        )
        .all(),
    ).toEqual([]);
    expect(fixture.state.has(TEST_OWNER_RECOVERY_KEY)).toBe(false);
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(await fixture.read()).toMatchObject({
      requestContext: {
        'flowsafe.runLifecycle': {
          terminal: { cleanupCompletedAt: expect.any(Number) },
        },
      },
    });
  });
});

describe('FS8 D3 host R1 keyed finalization preflight', () => {
  it.each([
    true,
    false,
  ])('retains the prepared nonterminal start when the reservation store disappears at the bridge (threaded=%s)', async (threaded) => {
    const { StartIdempotencyStore } = await import(
      '../do-runner/start-idempotency.js'
    );
    const reservations = new StartIdempotencyStore(
      sqliteUnitDatabase(
        openSqlite(),
      ) as import('../do-runner/start-idempotency.js').StartIdempotencyDatabase,
    );
    const reserved = await reservations.reserve({
      key: 'finalizer-key',
      owner: HUMAN_OWNER,
      targetKind: 'agent',
      targetId: 'writer',
      threadId: 'acme_thread',
      mintRunId: () => 'acme_run',
    });
    const claim = await reservations.claimReservation(reserved.reservation);
    if (!claim) throw new Error('missing finalizer claim');
    const fixture = harness(['writer'], {
      runtime: { startIdempotency: reservations },
    });
    fixture.setSummary(
      {
        runId: 'acme_run',
        status: 'suspended',
        requestedBy: 'operator-1',
        requestedByKind: 'human',
      },
      false,
    );
    mocked.stream.mockImplementation(async () => {
      Object.defineProperty(fixture.scope.init.runtime, 'startIdempotency', {
        value: undefined,
        configurable: true,
      });
      return {};
    });
    const settle = vi.spyOn(fixture.resources, 'settleReservation');
    const outcome = await fixture.host
      .start(fixture.scope, {
        ...C_START_INPUT,
        threaded,
        idempotencyKey: claim.key,
        startReservation: claim,
      })
      .catch((error: unknown) => error);
    expect(settle).not.toHaveBeenCalled();
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toMatchObject({
      phase: 'prepared-unfenced',
      startReservation: claim,
    });
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    expect(fixture.alarmAt()).toBeDefined();
    expect(await reservations.readForAdmission(claim.key)).toEqual(claim);
    expect(outcome).toMatchObject({ status: 503 });
  });
});

describe('FS8 D3 host R1 legacy requester and source owner', () => {
  it('releases the actual owner while retiring the captured canonical record after a v1 requester change', async () => {
    const fixture = await hostR1AgentFixture({ provenance: 'v1' });
    fixture.snapshot.requestContext = {
      ...fixture.snapshot.requestContext,
      'flowsafe.runProvenance': {
        version: 1,
        attemptToken: 'resumed-legacy-leg',
        requestedBy: 'reviewer-3',
        requestedByKind: 'human',
        resumeCounts: [],
      },
    };
    await fixture.persist();
    const owner = { kind: 'human' as const, id: 'source-owner' };
    await fixture.resources.release('run', 'acme_run', HUMAN_OWNER);
    await fixture.resources.claim('run', 'acme_run', owner);
    const release = vi.spyOn(fixture.resources, 'release');
    const response = await fixture.host.route(
      hostR1AgentRequest('/terminate'),
      { ...fixture.scope, principal: { ...owner, role: 'operator' } },
    );
    expect(await fixture.resources.owner('run', 'acme_run')).toBeUndefined();
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    expect(release).toHaveBeenCalledWith('run', 'acme_run', owner);
    expect(response?.status).toBe(200);
  });
});

describe('FS8 D3 host R1 legacy cleanup wait guards', () => {
  it.each([
    'dispatch',
    'owner release',
    'completion',
  ] as const)('retains the canonical record when a journal appears during legacy %s', async (boundary) => {
    const fixture = await hostR1AgentFixture({
      provenance: 'absent',
      status: 'cancelled',
      lifecycle: true,
    });
    const record = structuredClone(fixture.state.get(TEST_RUN_RECORD_KEY));
    const journal = { version: 1 };
    const replace = () => fixture.state.set(TEST_OWNER_RECOVERY_KEY, journal);
    fixture.dispatch.mockImplementation(async () => {
      if (boundary === 'dispatch') replace();
    });
    const nativeRelease = fixture.resources.release.bind(fixture.resources);
    const release = vi
      .spyOn(fixture.resources, 'release')
      .mockImplementation(async (...args) => {
        const result = await nativeRelease(...args);
        if (boundary === 'owner release') replace();
        return result;
      });
    const nativeComplete = fixture.app.runtime.completeTerminalCleanup.bind(
      fixture.app.runtime,
    );
    const complete = vi
      .spyOn(fixture.app.runtime, 'completeTerminalCleanup')
      .mockImplementation(async (...args) => {
        const result = await nativeComplete(...args);
        if (boundary === 'completion') replace();
        return result;
      });
    const outcome = await fixture.host
      .route(hostR1AgentRequest('/terminate'), fixture.scope)
      .catch((error: unknown) => error);
    expect(fixture.state.get(TEST_RUN_RECORD_KEY)).toEqual(record);
    expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(journal);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      boundary === 'dispatch' ? HUMAN_OWNER : undefined,
    );
    expect(fixture.dispatch).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledTimes(boundary === 'dispatch' ? 0 : 1);
    expect(complete).toHaveBeenCalledTimes(boundary === 'completion' ? 1 : 0);
    expect(outcome).toBeInstanceOf(Error);
    expect(doErrorResponse(outcome).status).toBe(503);
  });

  it.each([
    'journal',
    'record',
    'binding',
    'active execution',
  ] as const)('preserves ownership after approval wait changes cleanup authority (%s)', async (change) => {
    const fixture = await hostR1AgentFixture({ provenance: 'absent' });
    let replacement: unknown;
    fixture.approvals.list.mockImplementationOnce(async () => {
      if (change === 'journal') {
        replacement = { version: 1 };
        fixture.state.set(TEST_OWNER_RECOVERY_KEY, replacement);
      }
      if (change === 'record') {
        replacement = {
          ...(fixture.state.get(TEST_RUN_RECORD_KEY) as object),
          principal: { ...fixture.scope.principal, role: 'admin' },
        };
        fixture.state.set(TEST_RUN_RECORD_KEY, replacement);
      }
      if (change === 'binding') {
        replacement = {
          version: 1,
          agentId: 'foreign',
          resourceId: RESOURCE_ID,
        };
        fixture.state.set(THREAD_BINDING_KEY, replacement);
      }
      if (change === 'active execution')
        vi.spyOn(fixture.app.runtime, 'isRunActive').mockReturnValue(true);
      return [];
    });
    const release = vi.spyOn(fixture.resources, 'release');
    const complete = vi.spyOn(fixture.app.runtime, 'completeTerminalCleanup');
    const outcome = await fixture.host
      .route(hostR1AgentRequest('/terminate'), fixture.scope)
      .catch((error: unknown) => error);
    expect(await fixture.resources.owner('run', 'acme_run')).toEqual(
      HUMAN_OWNER,
    );
    expect(release).not.toHaveBeenCalled();
    expect(complete).not.toHaveBeenCalled();
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(true);
    if (change === 'journal')
      expect(fixture.state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(replacement);
    if (change === 'record')
      expect(fixture.state.get(TEST_RUN_RECORD_KEY)).toEqual(replacement);
    if (change === 'binding')
      expect(fixture.state.get(THREAD_BINDING_KEY)).toEqual(replacement);
    expect(fixture.approvals.list).toHaveBeenCalledOnce();
    expect(outcome).toBeInstanceOf(Error);
  });
});
