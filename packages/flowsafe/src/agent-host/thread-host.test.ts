// SPDX-License-Identifier: Apache-2.0

import type { MastraCompositeStore } from '@mastra/core/storage';
import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  type ApprovalAuditEvent,
  type ApprovalRecord,
  type ApprovalService,
  type ExecutionPrincipal,
  InMemoryResourceOwnershipStore,
  type RecoverableResourceOwnershipStore,
} from '../approval-api/index.js';
import type {
  InitResult,
  RequestContextProvider,
  RunnerRuntime,
  RunSummary,
  ScheduleSourceStore,
  ThreadScope,
} from '../do-runner/index.js';
import {
  doErrorResponse,
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
} from './thread-host.js';
import type { AgentAutomationRule, Permission } from './types.js';

const mocked = vi.hoisted(() => ({
  stream: vi.fn(),
  resumeViaRuntime: vi.fn(),
  observe: vi.fn(),
  getHistory: vi.fn(),
}));
const RESOURCE_ID = resourceIdFromKey('acme_thread');
const GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

vi.mock('@proofoftech/breakwater/agent', () => ({
  isGuardedAgentHandle: (value: unknown) =>
    typeof value === 'object' &&
    value !== null &&
    (value as { guarded?: unknown }).guarded === true,
}));

vi.mock('@mastra/core/mastra', () => ({
  Mastra: class {
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
  },
}));

vi.mock('../agent-runner/index.js', async (importOriginal) => {
  const original =
    await importOriginal<typeof import('../agent-runner/index.js')>();
  return {
    ...original,
    createFlowsafeDurableAgent: () => {
      const runIds = new Set<string>();
      return {
        streamUntilPersisted: async (...args: unknown[]) => {
          const options = args[1] as { runId?: string } | undefined;
          if (options?.runId) runIds.add(options.runId);
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
  const storage = {
    getStore: async () => ({
      loadWorkflowSnapshot: async () => snapshot,
    }),
  } as unknown as MastraCompositeStore;
  const runtime = {
    status: vi.fn(async (_workflowId: string, runId: string) => {
      const started = mocked.stream.mock.calls.some(
        (call) => call[1]?.runId === runId,
      );
      if (!statusVisible && !started) return null;
      return summary ? { ...summary, runId } : null;
    }),
    recoverStartAttempt: vi.fn(async (_workflowId: string, runId: string) => {
      const started = mocked.stream.mock.calls.some(
        (call) => call[1]?.runId === runId,
      );
      if (!statusVisible && !started) return null;
      return summary ? { ...summary, runId } : null;
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
    init: {
      runtime,
      pubsub: undefined,
    } as unknown as InitResult,
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
  mocked.stream.mockReset().mockResolvedValue({});
  mocked.resumeViaRuntime.mockReset();
  mocked.observe.mockReset();
  mocked.getHistory.mockReset().mockResolvedValue([]);
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
    version: 1,
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

    await expect(
      host.recoverOwnership(scope.init.runtime, scope.threadId),
    ).rejects.toThrow('stored agent owner recovery is malformed');

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

    await host.recoverOwnership(scope.init.runtime, scope.threadId);

    expect(recover).not.toHaveBeenCalled();
    expect(settle).not.toHaveBeenCalled();
    expect(state.get(TEST_OWNER_RECOVERY_KEY)).toEqual(current);
    expect(alarmAt()).toBeDefined();
  });

  it('rolls back a pre-snapshot attempt and its attempt-created metadata', async () => {
    const { host, scope, state, resources, alarmAt } = harness();
    const recovery = ownerRecovery('acme_run');
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

    await host.recoverOwnership(scope.init.runtime, scope.threadId);

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
    });
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

    await host.recoverOwnership(scope.init.runtime, scope.threadId);

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

    await host.recoverOwnership(scope.init.runtime, scope.threadId);

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

    await expect(
      host.recoverOwnership(scope.init.runtime, scope.threadId),
    ).rejects.toBeInstanceOf(RunStateUnreadableError);

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
    const { host, scope, state, resources, alarmAt, setSummary } = harness();
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

    await host.recoverOwnership(scope.init.runtime, scope.threadId);

    expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
    expect(alarmAt()).toBeDefined();
    expect(await resources.owner('thread', 'acme_thread')).toEqual(owner);
    expect(await resources.owner('resource', RESOURCE_ID)).toEqual(owner);

    setSummary({ runId: 'acme_run', status: 'success' });
    await host.recoverOwnership(scope.init.runtime, scope.threadId);

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
      expect(state.has(TEST_OWNER_RECOVERY_KEY)).toBe(true);
      expect(alarmAt()).toBeDefined();
      expect(await committed.owner('run', 'acme_run')).toEqual({
        kind: 'human',
        id: 'operator-1',
      });

      await host.recoverOwnership(scope.init.runtime, scope.threadId);

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

    const recovery = host.recoverOwnership(scope.init.runtime, scope.threadId);
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
    expect(fixture.state.has(TEST_RUN_RECORD_KEY)).toBe(false);
    await expect(
      fixture.resources.owner('run', 'acme_run'),
    ).resolves.toBeUndefined();

    fixture.state.set(TEST_RUN_RECORD_KEY, {
      version: 2,
      agentId: 'writer',
      principal: fixture.scope.principal,
      originEntryPath: 'http.start',
    });
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
    expect(release).toHaveBeenCalledOnce();
    expect(release).toHaveBeenCalledWith('run', 'acme_run', HUMAN_OWNER);
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
      status: 409,
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
    expect(rejected).toMatchObject({
      reason: { status: 409, message: 'thread is bound to another agent' },
    });
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

  it('removes run metadata when start fails and authoritative state is absent', async () => {
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
    expect(state.has('flowsafe:agent-run:v1:acme_run')).toBe(false);
  });

  it('keeps run metadata and names the failure when a failed start cannot read authoritative state', async () => {
    // #given — the same failed start, with the read that would tell an
    // interrupted start apart from a failed one refusing to answer from state
    // it could not reach.
    const { host, scope, state, alarmAt } = harness(['writer'], {
      runtime: {
        recoverStartAttempt: vi.fn(async () => {
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
    expect(logged).toContain(
      'interrupted start could not read authoritative state',
    );
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
    // Five host arguments plus the two trailing optionals (schedule dispatch,
    // reserved idempotency key), both undefined for this start.
    expect(mocked.stream.mock.calls.at(-1)).toHaveLength(7);
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

  it('rejects a snapshot whose thread correlation does not match the addressed DO', async () => {
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
    ).rejects.toMatchObject({ status: 404 });
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
    // role the schedule path used to fabricate to get in.
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
