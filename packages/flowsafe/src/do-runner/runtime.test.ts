// SPDX-License-Identifier: Apache-2.0
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import type { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore, type MastraCompositeStore } from '@mastra/core/storage';
import type { WorkflowRunState } from '@mastra/core/workflows';
import {
  createConnector,
  invokeConnector,
} from '@proofoftech/breakwater/connector-sdk';
import {
  assert,
  describe,
  expect,
  expectTypeOf,
  it,
  onTestFinished,
  vi,
} from 'vitest';
import { z } from 'zod';
import {
  nestedArray,
  nestedObject,
  PAST_SQLITE_JSON_DEPTH,
  withDeepValue,
} from '../../test-support/deep-json.js';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  D1ResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/resource-ownership.js';
import { createBackgroundTaskD1Domains } from '../background-tasks/d1-storage.js';
import type { D1DatabaseBinding } from './cf-types.js';
import { createD1Storage } from './d1-storage.js';
import {
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  InvalidExecutionIdentityError,
  InvalidMutationEpochError,
  normalizeD1RunExecutionIdentity,
  RunStartPendingError,
} from './execution-admission.js';
import {
  type ExecutionFenceDatabase,
  ExecutionFenceStore,
} from './execution-fence.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type FencedWorkflowAdmissionCapability,
} from './fenced-workflow-capability.js';
import type { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { RunStateUnreadableError as BarrelRunStateUnreadableError } from './index.js';
import { init } from './init.js';
import { createHostPubSub } from './pubsub.js';
import {
  RunLifecycleBlockedError as LeafRunLifecycleBlockedError,
  parseRunLifecycle,
  RunSettledConflictError,
} from './run-lifecycle.js';
import {
  type AuthoritativeStartState,
  InvalidRunRequestError,
  type LegacyRunState,
  type RequestContextProvider,
  RunAlreadyExistsError,
  type RunLeg,
  RunLifecycleBlockedError,
  RunNotSuspendedError,
  type RunnerRuntime,
  RunStateUnreadableError,
  type RunSummary,
  RunTerminalConflictError,
  type StartRunOptions,
  UnknownRunError,
  UnknownWorkflowError,
} from './runtime.js';
import { StartIdempotencyStore } from './start-idempotency.js';
import {
  isReadableRunSummary,
  isSuspensionTimeoutResumeData,
  MASTRA_WORKFLOW_META_KEY,
  SUSPENSION_DEADLINE_PAYLOAD_KEY,
  SUSPENSION_DEADLINE_PRINCIPAL_ID,
  type SuspensionDeadlineEntry,
  suspensionDeadlinesOf,
  suspensionTimeoutResumeData,
} from './suspension-deadline.js';

function expectSnapshotUnreadable(sql: ReturnType<typeof openSqlite>) {
  expect(
    sql
      .prepare(
        'SELECT json_valid(snapshot) AS valid FROM mastra_workflow_snapshot',
      )
      .all(),
  ).toEqual([{ valid: 0 }]);
}

function d1Snapshot(
  status: RunSummary['status'] = 'success',
): WorkflowRunState & {
  requestContext: NonNullable<WorkflowRunState['requestContext']>;
} {
  return {
    runId: 'd1-run',
    status: status as WorkflowRunState['status'],
    result: { source: 'S1' },
    error: { name: 'Error', message: 'S1 failure' },
    context: {
      gate: {
        status: 'suspended',
        payload: {},
        startedAt: 50,
        suspendPayload: { source: 'S1' },
        suspendedAt: 100,
        ...{ resumedAt: 90 },
      },
    },
    requestContext: {
      'flowsafe.runProvenance': {
        version: 2,
        startToken: 'S1',
        attemptToken: 'attempt-1',
        startIdentity: {
          owner: { kind: 'human', id: 'owner' },
          target: { kind: 'workflow', id: 'd1-workflow' },
        },
        requestedBy: 'owner',
        requestedByKind: 'human',
        resumeCounts: [['gate', 2]],
      },
    },
    value: {},
    serializedStepGraph: [],
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: { gate: [0] },
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 100,
  };
}

async function d1Fixture(
  kind: 'default' | 'prefixed' | 'background' | 'unfenced' = 'default',
  bound: 'none' | 'fence' | 'start' = 'none',
) {
  const sql = openSqlite() as ReturnType<typeof openSqlite> & { close(): void };
  const binding = sqliteUnitDatabase(sql) as D1DatabaseBinding;
  const storage =
    kind === 'unfenced'
      ? new InMemoryStore()
      : createD1Storage({
          binding,
          ...(kind === 'prefixed' ? { tablePrefix: 'D1_' } : {}),
          ...(kind === 'background'
            ? { domains: createBackgroundTaskD1Domains({ binding }) }
            : {}),
        });
  await storage.init();
  const app = init(
    { storage },
    {
      executionFence:
        bound === 'fence'
          ? new ExecutionFenceStore(binding as ExecutionFenceDatabase)
          : 'none',
      startIdempotency:
        bound === 'start'
          ? new StartIdempotencyStore(binding as ExecutionFenceDatabase)
          : 'none',
    },
  );
  const schema = z.looseObject({});
  const workflow = app
    .createWorkflow({
      id: 'd1-workflow',
      inputSchema: schema,
      outputSchema: schema,
    })
    .then(
      app.createStep({
        id: 'gate',
        inputSchema: schema,
        outputSchema: schema,
        execute: async ({ inputData }) => inputData,
      }),
    )
    .commit();
  await app.runtime.status('d1-workflow', 'd1-run');
  const workflows = (await storage.getStore(
    'workflows',
  )) as FencedWorkflowsStorageD1;
  const native = workflows[FENCED_WORKFLOW_STORAGE];
  const capability = native ? { ...native } : undefined;
  Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
    value: capability,
    writable: true,
    configurable: true,
  });
  const seed = (snapshot: WorkflowRunState = d1Snapshot()) =>
    workflows.persistWorkflowSnapshot({
      workflowName: 'd1-workflow',
      runId: 'd1-run',
      snapshot,
    });
  return {
    ...app,
    sql,
    storage,
    workflow,
    workflows,
    get capability() {
      if (!capability) throw new Error('D1 fixture capability is missing');
      return capability;
    },
    seed,
    close: () => sql.close(),
  };
}

describe('authoritative start state', () => {
  it.each([
    'default',
    'prefixed',
    'background',
  ] as const)('selects one raw %s snapshot for physical identity and S1 payload', async (kind) => {
    const f = await d1Fixture(kind);
    try {
      for (const status of ['success', 'failed', 'suspended'] as const) {
        const snapshot = d1Snapshot(status);
        await f.seed(snapshot);
        const capability = f.capability;
        const originalRead = capability.readSnapshot.bind(capability);
        const read = vi
          .spyOn(capability, 'readSnapshot')
          .mockImplementation(async (address) => {
            const row = await originalRead(address);
            const replacement = d1Snapshot(status);
            replacement.requestContext['flowsafe.runProvenance'].startToken =
              'S2';
            replacement.result = { source: 'S2' };
            replacement.error = { name: 'Error', message: 'S2 failure' };
            assert(replacement.context.gate);
            replacement.context.gate.suspendPayload = { source: 'S2' };
            await f.seed(replacement);
            return row;
          });
        const publicRead = vi.spyOn(f.workflow, 'getWorkflowRunById');
        const ordinaryRead = vi.spyOn(f.workflows, 'getWorkflowRunById');
        const load = vi.spyOn(f.workflows, 'loadWorkflowSnapshot');
        const selected = await f.runtime.authoritativeStartState(
          'd1-workflow',
          'd1-run',
        );
        expect(selected).toMatchObject({
          storage: 'd1',
          kind: 'result',
          execution: {
            tablePrefix: kind === 'prefixed' ? 'd1_' : '',
            workflowId: 'd1-workflow',
            runId: 'd1-run',
            startToken: 'S1',
          },
        });
        expect(selected?.summary).toMatchObject(
          status === 'success'
            ? { result: { source: 'S1' } }
            : status === 'failed'
              ? { error: 'S1 failure' }
              : {
                  suspendPayload: { gate: { source: 'S1' } },
                  suspendedAt: { gate: 100 },
                  resumedAt: { gate: 90 },
                  resumeCount: { gate: 2 },
                },
        );
        expect(selected?.snapshot).toEqual(snapshot);
        expect(read).toHaveBeenCalledTimes(1);
        expect(publicRead).not.toHaveBeenCalled();
        expect(ordinaryRead).not.toHaveBeenCalled();
        expect(load).not.toHaveBeenCalled();
        expect(
          selected?.storage === 'd1' && Object.isFrozen(selected.raw),
        ).toBe(true);
        if (selected?.storage === 'd1')
          expect(JSON.parse(selected.raw.snapshot)).toEqual(snapshot);
        read.mockRestore();
        publicRead.mockRestore();
        ordinaryRead.mockRestore();
        load.mockRestore();
      }
    } finally {
      f.close();
    }
  });

  it('copies all exact raw fields without reserializing snapshot bytes', async () => {
    const f = await d1Fixture();
    try {
      await f.seed();
      const bytes = `${JSON.stringify(d1Snapshot(), null, 2)}\n`;
      f.sql
        .prepare(
          'UPDATE mastra_workflow_snapshot SET snapshot = ?, resourceId = ?, createdAt = ?, updatedAt = ?',
        )
        .run(
          bytes,
          'resource-1',
          '2026-01-01T04:00:00+04:00',
          '2026-01-02T04:00:00+04:00',
        );
      const row = await f.capability.readSnapshot({
        workflowId: 'd1-workflow',
        runId: 'd1-run',
      });
      assert(row);
      const observed = { ...row };
      vi.spyOn(f.capability, 'readSnapshot').mockResolvedValue(observed);
      const selected = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      assert(selected?.storage === 'd1');
      expect(selected.raw).toEqual(row);
      expect(selected.raw).not.toBe(observed);
      observed.snapshot = '{}';
      observed.resourceId = 'changed';
      expect(selected.raw.snapshot).toBe(bytes);
      expect(selected.raw.resourceId).toBe('resource-1');
      expect(selected.summary).toMatchObject({
        createdAt: '2026-01-01T00:00:00.000Z',
        updatedAt: '2026-01-02T00:00:00.000Z',
      });
    } finally {
      f.close();
    }
  });

  it.each([
    'default',
    'unfenced',
  ] as const)('returns %s absence despite a cached Run', async (kind) => {
    const f = await d1Fixture(kind);
    try {
      await f.workflow.createRun({ runId: 'd1-run' });
      await f.workflows.deleteWorkflowRunById({
        workflowName: 'd1-workflow',
        runId: 'd1-run',
      });
      expect(await f.runtime.status('d1-workflow', 'd1-run')).not.toBeNull();
      expect(
        await f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).toBeNull();
    } finally {
      f.close();
    }
  });

  it.each([
    'string',
    'object',
  ] as const)('detaches one unfenced %s record with an explicit null namespace', async (shape) => {
    const f = await d1Fixture('unfenced');
    try {
      const snapshot = d1Snapshot('suspended');
      const date = new Date('2026-01-01T00:00:00Z');
      const record = {
        workflowName: 'd1-workflow',
        runId: 'd1-run',
        snapshot: shape === 'string' ? JSON.stringify(snapshot) : snapshot,
        createdAt:
          shape === 'string'
            ? ('2026-01-01T00:00:00Z' as unknown as Date)
            : date,
        updatedAt: date,
      };
      const read = vi
        .spyOn(f.workflows, 'getWorkflowRunById')
        .mockResolvedValue(record);
      const selected = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      assert(snapshot.context.gate);
      snapshot.context.gate.suspendPayload = { source: 'S2' };
      snapshot.requestContext['flowsafe.runProvenance'].startToken = 'S2';
      date.setTime(0);
      expect(selected).toMatchObject({
        storage: 'unfenced',
        execution: { tablePrefix: null, startToken: 'S1' },
        summary: {
          suspendPayload: { gate: { source: 'S1' } },
          createdAt: '2026-01-01T00:00:00.000Z',
        },
      });
      expect(selected).not.toHaveProperty('raw');
      expect(selected?.snapshot.context.gate?.suspendPayload).toEqual({
        source: 'S1',
      });
      expect(read).toHaveBeenCalledTimes(1);
    } finally {
      f.close();
    }
  });

  it.each([
    'pending',
    'success',
    'suspended',
  ] as const)('classifies raw %s independently of retained marker and lifecycle', async (status) => {
    const f = await d1Fixture();
    try {
      for (const marker of [undefined, true])
        for (const terminal of [false, true]) {
          const snapshot = d1Snapshot(status);
          snapshot.requestContext['flowsafe.runProvenance'].initialAdmission =
            marker;
          if (terminal)
            snapshot.requestContext['flowsafe.runLifecycle'] = {
              version: 1,
              revision: 2,
              deadlineAt: 50,
              terminal: {
                status: 'timed_out',
                error: { code: 'TIMED_OUT', message: 'run timed out' },
                transitionedAt: 100,
                replayPrincipals: [{ kind: 'human', id: 'owner' }],
              },
            };
          f.sql.exec('DELETE FROM mastra_workflow_snapshot');
          await f.seed(snapshot);
          const selected = await f.runtime.authoritativeStartState(
            'd1-workflow',
            'd1-run',
          );
          expect(selected?.kind).toBe(
            status === 'pending' ? 'initial' : 'result',
          );
          if (status === 'pending')
            expect(selected).not.toHaveProperty('summary');
          else
            expect(selected?.summary?.status).toBe(
              terminal ? 'timed_out' : status,
            );
        }
    } finally {
      f.close();
    }
  });

  it.each([
    'unattributed',
    'inherited child',
    'agent',
  ] as const)('reads role-neutral %s v2 provenance without auxiliary context', async (mode) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot();
      const provenance = snapshot.requestContext['flowsafe.runProvenance'];
      if (mode === 'unattributed') {
        delete provenance.startIdentity;
        delete provenance.requestedBy;
        delete provenance.requestedByKind;
      }
      if (mode === 'inherited child')
        provenance.startIdentity.target.id = 'parent-workflow';
      if (mode === 'agent') {
        provenance.startIdentity.target = {
          kind: 'agent',
          id: 'logical-agent',
          threadId: 'thread-1',
        };
        provenance.agentStart = { threaded: true };
      }
      await f.seed(snapshot);
      const selected = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      expect(selected?.provenance).toEqual(provenance);
      expect(selected?.execution).toEqual({
        tablePrefix: '',
        workflowId: 'd1-workflow',
        runId: 'd1-run',
        startToken: 'S1',
      });
      expect(selected?.summary?.requestedBy).toBe(
        mode === 'unattributed' ? undefined : 'owner',
      );
      expect(selected).not.toHaveProperty('startIdentity');
    } finally {
      f.close();
    }
  });

  it.each([
    ['absent', undefined],
    ['v1', { version: 1 }],
    ['unknown version', { version: 3 }],
    ['marker', { initialAdmission: false }],
    ['start token', { startToken: 'bad/token' }],
    ['attempt token', { attemptToken: '' }],
    ['requester', { requestedByKind: 'robot' }],
    ['counts', { resumeCounts: [['gate', 0]] }],
    ['epoch', { mutationEpoch: -1 }],
    ['agent mode', { agentStart: { threaded: 'true' } }],
  ])('refuses modern association with malformed provenance: %s', async (label, corruption) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot();
      if (label === 'agent mode')
        snapshot.requestContext['flowsafe.runProvenance'].startIdentity.target =
          { kind: 'agent', id: 'logical-agent', threadId: 'thread-1' };
      snapshot.requestContext['flowsafe.runProvenance'] =
        label === 'absent'
          ? undefined
          : {
              ...snapshot.requestContext['flowsafe.runProvenance'],
              ...corruption,
            };
      await f.seed(snapshot);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    ['runId', 'other-run'],
    ['status', 'invented'],
    ['requestContext', []],
    ['context', null],
    ['suspendedPaths', []],
    ['requestContext', { 'flowsafe.runLifecycle': { version: 99 } }],
  ])('refuses malformed consumed snapshot field %s', async (key, value) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot();
      const replacement =
        key === 'requestContext' && !Array.isArray(value)
          ? { ...snapshot.requestContext, ...(value as object) }
          : value;
      await f.seed({ ...snapshot, [key]: replacement } as WorkflowRunState);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    ['tablePrefix', 'other_'],
    ['workflowId', 'other-workflow'],
    ['runId', 'other-run'],
    ['resourceId', 12],
    ['snapshot', '{'],
    ['snapshot', 'null'],
    ['snapshot', '[]'],
    ['createdAt', 'invalid'],
    ['updatedAt', 'invalid'],
  ])('refuses a wrong or malformed raw field: %s', async (key, value) => {
    const f = await d1Fixture();
    try {
      await f.seed();
      const row = await f.capability.readSnapshot({
        workflowId: 'd1-workflow',
        runId: 'd1-run',
      });
      vi.spyOn(f.capability, 'readSnapshot').mockResolvedValue({
        ...row,
        [key]: value,
      } as NonNullable<typeof row>);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'workflowName',
    'runId',
    'timestamp',
    'noncloneable',
  ] as const)('refuses malformed custom record: %s', async (field) => {
    const f = await d1Fixture('unfenced');
    try {
      const snapshot = d1Snapshot();
      if (field === 'noncloneable')
        snapshot.result = { callback: () => undefined };
      vi.spyOn(f.workflows, 'getWorkflowRunById').mockResolvedValue({
        workflowName: field === 'workflowName' ? 'other' : 'd1-workflow',
        runId: field === 'runId' ? 'other' : 'd1-run',
        snapshot,
        createdAt: field === 'timestamp' ? new Date(Number.NaN) : new Date(100),
        updatedAt: new Date(100),
      });
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it('retains input taxonomy and fixed unreadable messages for source failures', async () => {
    const f = await d1Fixture();
    try {
      await expect(
        f.runtime.authoritativeStartState('bad/workflow', 'd1-run'),
      ).rejects.toThrow(InvalidRunRequestError);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'bad/run'),
      ).rejects.toThrow(InvalidRunRequestError);
      await expect(
        f.runtime.authoritativeStartState('unknown', 'd1-run'),
      ).rejects.toThrow(UnknownWorkflowError);
      const cause = new Error('secret-token-storage-failure');
      vi.spyOn(f.capability, 'readSnapshot').mockRejectedValue(cause);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toMatchObject({
        name: 'RunStateUnreadableError',
        message: "run 'd1-run' of workflow 'd1-workflow' state is not readable",
        cause,
      });
      vi.spyOn(f.storage, 'getStore').mockResolvedValue(undefined);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
      const ownMastra = f.workflow.mastra;
      assert(ownMastra);
      vi.spyOn(ownMastra, 'getStorage').mockReturnValue(undefined);
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'undefined',
    'storage failure',
  ] as const)('refuses custom %s instead of reporting absence', async (mode) => {
    const f = await d1Fixture('unfenced');
    try {
      const read = vi.spyOn(f.workflows, 'getWorkflowRunById');
      if (mode === 'undefined') read.mockResolvedValue(undefined as never);
      else read.mockRejectedValue(new Error('secret-storage-token'));
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toMatchObject({
        name: 'RunStateUnreadableError',
        message: "run 'd1-run' of workflow 'd1-workflow' state is not readable",
      });
    } finally {
      f.close();
    }
  });

  it.each([
    'fence',
    'start',
  ] as const)('checks matching %s binding without reading or seeding its state', async (bound) => {
    const f = await d1Fixture('default', bound);
    try {
      await f.seed();
      const store =
        bound === 'fence'
          ? f.runtime.executionFence
          : f.runtime.startIdempotency;
      assert(store);
      const admission = vi.spyOn(store, 'readForAdmission');
      const prepare = vi.spyOn(f.sql, 'prepare');
      expect(
        await f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).toMatchObject({ execution: { startToken: 'S1' } });
      expect(admission).not.toHaveBeenCalled();
      expect(
        prepare.mock.calls.every(
          ([sql]) =>
            /^\s*SELECT\b/i.test(sql) &&
            sql.includes('mastra_workflow_snapshot'),
        ),
      ).toBe(true);
    } finally {
      f.close();
    }
  });

  it.each([
    'fence',
    'start',
  ] as const)('rejects mismatched %s binding before reading a snapshot', async (bound) => {
    const f = await d1Fixture('default', bound);
    try {
      const store =
        bound === 'fence'
          ? f.runtime.executionFence
          : f.runtime.startIdempotency;
      assert(store);
      vi.spyOn(store, 'usesDatabase').mockReturnValue(false);
      const read = vi.spyOn(f.capability, 'readSnapshot');
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
      expect(read).not.toHaveBeenCalled();
    } finally {
      f.close();
    }
  });

  it('refuses a fenced custom domain instead of granting a fallback identity', async () => {
    const f = await d1Fixture('unfenced', 'fence');
    try {
      const read = vi.spyOn(f.workflows, 'getWorkflowRunById');
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).rejects.toThrow(RunStateUnreadableError);
      expect(read).not.toHaveBeenCalled();
    } finally {
      f.close();
    }
  });

  it('uses the registered workflow storage instead of nominal Runtime storage', async () => {
    const f = await d1Fixture();
    const actual = await d1Fixture('prefixed');
    try {
      await actual.seed();
      const ownMastra = f.workflow.mastra;
      assert(ownMastra);
      vi.spyOn(ownMastra, 'getStorage').mockReturnValue(actual.storage);
      const nominal = vi.spyOn(f.capability, 'readSnapshot');
      expect(
        await f.runtime.authoritativeStartState('d1-workflow', 'd1-run'),
      ).toMatchObject({
        storage: 'd1',
        execution: { tablePrefix: 'd1_', startToken: 'S1' },
      });
      expect(nominal).not.toHaveBeenCalled();
    } finally {
      f.close();
      actual.close();
    }
  });

  it.each([
    'capability',
    'workflow storage',
  ] as const)('captures the original D1 source across held-read %s replacement', async (replacement) => {
    const f = await d1Fixture();
    const other = await d1Fixture('prefixed');
    const held = deferredSignal();
    const release = deferredSignal();
    try {
      await f.seed();
      await other.seed();
      const capability = f.capability;
      const read = capability.readSnapshot;
      const original = await read.call(capability, {
        workflowId: 'd1-workflow',
        runId: 'd1-run',
      });
      const receivers: unknown[] = [];
      const selectedRead = vi.fn(async function (
        this: FencedWorkflowAdmissionCapability,
        address: { workflowId: string; runId: string },
      ) {
        receivers.push(this);
        const row = await read.call(this, address);
        held.resolve();
        await release.promise;
        return row;
      });
      capability.readSnapshot = selectedRead;
      const pending = f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      await held.promise;
      const otherRead = vi.spyOn(other.capability, 'readSnapshot');
      if (replacement === 'capability') {
        Object.assign(capability, other.capability);
        Object.defineProperty(f.workflows, FENCED_WORKFLOW_STORAGE, {
          value: other.capability,
        });
      } else
        vi.spyOn(f.workflow, 'mastra', 'get').mockReturnValue(
          new Mastra({ storage: other.storage, logger: false }),
        );
      release.resolve();
      const selected = await pending;
      expect(selected).toMatchObject({
        storage: 'd1',
        execution: {
          tablePrefix: '',
          workflowId: 'd1-workflow',
          runId: 'd1-run',
          startToken: 'S1',
        },
        raw: original,
      });
      expect(receivers).toEqual([capability]);
      expect(selectedRead).toHaveBeenCalledTimes(1);
      expect(otherRead).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      f.close();
      other.close();
    }
  });

  it('captures the custom domain method and receiver before a held read', async () => {
    const f = await d1Fixture('unfenced');
    const held = deferredSignal();
    const release = deferredSignal();
    try {
      await f.seed();
      const read = f.workflows.getWorkflowRunById;
      const receivers: unknown[] = [];
      const selectedRead = vi
        .spyOn(f.workflows, 'getWorkflowRunById')
        .mockImplementation(async function (this: typeof f.workflows, input) {
          receivers.push(this);
          const row = await read.call(this, input);
          held.resolve();
          await release.promise;
          return row;
        });
      const pending = f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      await held.promise;
      const replacement = vi.fn(read.bind(f.workflows));
      f.workflows.getWorkflowRunById = replacement;
      release.resolve();
      expect(await pending).toMatchObject({
        execution: { tablePrefix: null, startToken: 'S1' },
      });
      expect(receivers).toEqual([f.workflows]);
      expect(selectedRead).toHaveBeenCalledTimes(1);
      expect(replacement).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      f.close();
    }
  });

  it.each([
    'success',
    'failed',
    'suspended',
    'foreach',
    'lifecycle',
  ] as const)('matches a frozen v1 summary twin for %s', async (mode) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot(
        mode === 'foreach' || mode === 'lifecycle' ? 'suspended' : mode,
      );
      if (mode === 'foreach')
        snapshot.context.gate = [
          {
            status: 'suspended',
            suspendPayload: { source: 'old' },
            suspendedAt: 50,
          },
          snapshot.context.gate,
        ] as unknown as WorkflowRunState['context'][string];
      if (mode === 'lifecycle')
        snapshot.requestContext['flowsafe.runLifecycle'] = {
          version: 1,
          revision: 1,
          deadlineAt: 50,
          terminal: {
            status: 'timed_out',
            error: { code: 'TIMED_OUT', message: 'run timed out' },
            transitionedAt: 100,
            replayPrincipals: [{ kind: 'human', id: 'owner' }],
          },
        };
      snapshot.requestContext['flowsafe.runProvenance'].version = 1;
      await f.seed(snapshot);
      const twin = structuredClone(
        await f.runtime.status('d1-workflow', 'd1-run'),
      );
      const before = await f.capability.readSnapshot({
        workflowId: 'd1-workflow',
        runId: 'd1-run',
      });
      assert(before);
      snapshot.requestContext['flowsafe.runProvenance'].version = 2;
      f.sql
        .prepare(
          'UPDATE mastra_workflow_snapshot SET snapshot = ? WHERE workflow_name = ? AND run_id = ?',
        )
        .run(JSON.stringify(snapshot), 'd1-workflow', 'd1-run');
      const selected = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      expect(selected?.summary).toEqual(twin);
      expect(selected?.snapshot).toEqual(snapshot);
      expect(selected?.summary).toMatchObject({
        createdAt: new Date(before.createdAt).toISOString(),
        updatedAt: new Date(before.updatedAt).toISOString(),
        requestedBy: 'owner',
        requestedByKind: 'human',
      });
      if (mode === 'success')
        expect(selected?.summary?.result).toEqual({ source: 'S1' });
      if (mode === 'failed')
        expect(selected?.summary?.error).toBe('S1 failure');
      if (mode === 'suspended' || mode === 'foreach')
        expect(selected?.summary).toMatchObject({
          suspendPayload: { gate: { source: 'S1' } },
          suspendedAt: { gate: 100 },
          resumedAt: { gate: 90 },
          resumeCount: { gate: 2 },
        });
      for (const key of [
        'provenance',
        'requestContext',
        'startToken',
        'attemptToken',
        'raw',
        'snapshot',
      ])
        expect(selected?.summary).not.toHaveProperty(key);
    } finally {
      f.close();
    }
  });

  it('keeps nested collisions root-local against a frozen v1 twin and raw payload', async () => {
    const f = rootSummaryFixture();
    rootSummaryCollisionWorkflow(f);
    try {
      await f.runtime.start('summary-root-workflow', rootSummaryStartOptions);
      const twin = structuredClone(
        await f.runtime.status('summary-root-workflow', 'summary-run'),
      );
      const row = f.row();
      const snapshot = JSON.parse(row.snapshot) as WorkflowRunState;
      assert(snapshot.requestContext);
      snapshot.requestContext['flowsafe.runProvenance'].version = 2;
      f.replaceSnapshot(snapshot);
      const selectedRow = f.row();
      f.changeChild('a', 'summary-run', 'b', 123456);
      f.reads.length = 0;
      const selected = await f.runtime.authoritativeStartState(
        'summary-root-workflow',
        'summary-run',
      );
      expect(selected?.summary).toEqual(twin);
      assertRootSummary(selected?.summary ?? null, selectedRow);
      expect(selected?.summary?.suspendPayload).toHaveProperty('a.b', {
        reason: 'root a.b',
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000,
      });
      expect(f.reads).toHaveLength(1);
      assert(selected?.summary);
      expect(suspensionDeadlinesOf(selected.summary)).toEqual(
        rootSummaryDeadlineRefusal,
      );
    } finally {
      f.close();
    }
  });

  it('preserves magic own step keys and excludes Core control entries', async () => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot('suspended');
      const keys = ['__proto__', 'constructor', 'toString'];
      snapshot.context = Object.fromEntries(
        [...keys, 'input', '__state'].map((key, index) => [
          key,
          {
            status: 'suspended',
            payload: {},
            startedAt: 50,
            suspendPayload: { key },
            suspendedAt: 100 + index,
            resumedAt: 90 + index,
          },
        ]),
      );
      snapshot.suspendedPaths = Object.fromEntries(
        [...keys, 'input', '__state'].map((key) => [key, [0]]),
      );
      snapshot.requestContext['flowsafe.runProvenance'].resumeCounts = keys.map(
        (key, index) => [key, index + 1],
      );
      await f.seed(snapshot);
      const selected = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
      );
      assert(selected?.summary);
      const summary = selected.summary;
      for (const field of [
        'suspendPayload',
        'suspendedAt',
        'resumedAt',
        'resumeCount',
      ] as const) {
        const map = summary[field] as Record<string, unknown>;
        expect(Object.getPrototypeOf(map)).toBeNull();
        expect(Object.keys(map)).toEqual(keys);
        keys.forEach((key, index) => {
          expect(Object.hasOwn(map, key)).toBe(true);
          expect(map[key]).toEqual(
            field === 'suspendPayload'
              ? { key }
              : field === 'suspendedAt'
                ? 100 + index
                : field === 'resumedAt'
                  ? 90 + index
                  : index + 1,
          );
        });
      }
    } finally {
      f.close();
    }
  });

  it('writes v2 starts through its captured private reader', async () => {
    const f = await d1Fixture();
    try {
      const read = vi.spyOn(f.runtime, 'authoritativeStartState');
      await f.runtime.start('d1-workflow', {
        runId: 'd1-run',
        inputData: {},
        attemptToken: 'ordinary',
      });
      expect(read).not.toHaveBeenCalled();
      const row = await f.capability.readSnapshot({
        workflowId: 'd1-workflow',
        runId: 'd1-run',
      });
      assert(row);
      expect(
        JSON.parse(row.snapshot).requestContext['flowsafe.runProvenance'],
      ).toMatchObject({
        version: 2,
        startToken: expect.any(String),
        attemptToken: 'ordinary',
      });
    } finally {
      f.close();
    }
  });
});

interface RootSummarySnapshot {
  status: RunSummary['status'];
  result?: unknown;
  error?: unknown;
  context: Record<string, RootSummaryStep | RootSummaryStep[]>;
  suspendedPaths?: Record<string, number[]>;
  requestContext?: Record<string, unknown> & {
    'flowsafe.runProvenance'?: {
      version: number;
      startToken: string;
      requestedBy?: string;
      requestedByKind?: 'human' | 'service' | 'system';
      resumeCounts: Array<[string, number]>;
    };
  };
}

interface RootSummaryStep {
  suspendPayload?: unknown;
  suspendedAt?: number;
  resumedAt?: number;
}

function rootSummaryFixture(requestContextForRun?: RequestContextProvider) {
  const sqlite = openSqlite() as ReturnType<typeof openSqlite> & {
    close(): void;
  };
  const prepare = sqlite.prepare.bind(sqlite);
  const reads: unknown[][] = [];
  const tracked = vi.spyOn(sqlite, 'prepare').mockImplementation((sql) => {
    const statement = prepare(sql);
    if (/^\s*SELECT\b/i.test(sql) && sql.includes('mastra_workflow_snapshot')) {
      const get = statement.get.bind(statement);
      const all = statement.all.bind(statement);
      statement.get = (...args) => {
        reads.push(args);
        return get(...args);
      };
      statement.all = (...args) => {
        reads.push(args);
        return all(...args);
      };
    }
    return statement;
  });
  const host = init(
    { DB: sqliteUnitDatabase(sqlite) as D1DatabaseBinding },
    { executionFence: 'none', startIdempotency: 'none', requestContextForRun },
  );
  const effects = vi.fn();
  const schema = z.looseObject({});
  const gate = (id: string, reason: string) =>
    host.createStep({
      id,
      inputSchema: schema,
      outputSchema: schema,
      execute: async ({ inputData, resumeData, suspend, requestContext }) => {
        effects(id, requestContext);
        return resumeData
          ? inputData
          : suspend({ reason, [SUSPENSION_DEADLINE_PAYLOAD_KEY]: 900_000 });
      },
    });
  const row = (workflowId = 'summary-root-workflow', runId = 'summary-run') =>
    prepare(
      'SELECT * FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
    ).get(workflowId, runId) as {
      snapshot: string;
      createdAt: string;
      updatedAt: string;
      [key: string]: unknown;
    };
  return {
    ...host,
    effects,
    schema,
    gate,
    row,
    reads,
    snapshot: (workflowId = 'summary-root-workflow', runId = 'summary-run') =>
      JSON.parse(row(workflowId, runId).snapshot) as RootSummarySnapshot,
    replaceSnapshot(snapshot: WorkflowRunState) {
      prepare(
        'UPDATE mastra_workflow_snapshot SET snapshot = ? WHERE workflow_name = ? AND run_id = ?',
      ).run(JSON.stringify(snapshot), 'summary-root-workflow', 'summary-run');
    },
    changeChild(workflowId: string, runId: string, step: string, time: number) {
      const snapshot = JSON.parse(
        row(workflowId, runId).snapshot,
      ) as RootSummarySnapshot;
      const entry = snapshot.context[step] as RootSummaryStep;
      entry.suspendedAt = time;
      entry.suspendPayload = { reason: 'CHILD ONLY CHANGE' };
      prepare(
        'UPDATE mastra_workflow_snapshot SET snapshot = ? WHERE workflow_name = ? AND run_id = ?',
      ).run(JSON.stringify(snapshot), workflowId, runId);
    },
    close() {
      tracked.mockRestore();
      sqlite.close();
    },
  };
}

function rootSummaryCollisionWorkflow(
  f: ReturnType<typeof rootSummaryFixture>,
) {
  const child = f
    .createWorkflow({
      id: 'a',
      inputSchema: f.schema,
      outputSchema: f.schema,
    })
    .then(f.gate('b', 'nested b'))
    .commit();
  return f
    .createWorkflow({
      id: 'summary-root-workflow',
      inputSchema: f.schema,
      outputSchema: f.schema,
    })
    .parallel([f.gate('a.b', 'root a.b'), child])
    .commit();
}

function assertRootSummary(
  summary: RunSummary | null,
  row: ReturnType<ReturnType<typeof rootSummaryFixture>['row']>,
) {
  const snapshot = JSON.parse(row.snapshot) as RootSummarySnapshot;
  const keys = Object.keys(snapshot.suspendedPaths ?? {});
  const entry = (key: string) => {
    const value = snapshot.context[key];
    return Array.isArray(value) ? value[value.length - 1] : value;
  };
  const project = <T>(read: (key: string) => T | undefined) => {
    const pairs = keys
      .map((key) => [key, read(key)] as const)
      .filter(([, value]) => value !== undefined);
    return pairs.length ? Object.fromEntries(pairs) : undefined;
  };
  const provenance = snapshot.requestContext?.['flowsafe.runProvenance'];
  expect(summary).toMatchObject({
    status: snapshot.status,
    createdAt: new Date(row.createdAt).toISOString(),
    updatedAt: new Date(row.updatedAt).toISOString(),
  });
  expect(summary?.requestedBy).toBe(provenance?.requestedBy);
  expect(summary?.requestedByKind).toBe(provenance?.requestedByKind);
  if (snapshot.status === 'suspended') {
    expect(summary?.suspended).toEqual(keys.map((key) => key.split('.')));
    expect(summary?.suspendPayload).toEqual(
      project((key) => entry(key)?.suspendPayload),
    );
    expect(summary?.suspendedAt).toEqual(
      project((key) => entry(key)?.suspendedAt),
    );
    expect(summary?.resumedAt).toEqual(project((key) => entry(key)?.resumedAt));
    expect(summary?.resumeCount).toEqual(
      project((key) => new Map(provenance?.resumeCounts).get(key)),
    );
  }
  if (snapshot.status === 'success')
    expect(summary?.result).toEqual(snapshot.result);
  expect(summary).not.toHaveProperty('requestContext');
  expect(summary).not.toHaveProperty('startToken');
  expect(summary).not.toHaveProperty('attemptToken');
}

const rootSummaryStartOptions = {
  runId: 'summary-run',
  inputData: {},
  attemptToken: 'summary-attempt',
  requestedBy: 'owner',
  requestedByKind: 'human',
} as const;

const rootSummaryDeadlineRefusal = {
  entries: [],
  rejected: [
    { step: 'a.b', reason: 'ambiguous suspended step path' },
    { step: 'a', reason: 'nested suspension paths are not supported' },
  ],
};

describe('root-local stored summaries', () => {
  it.each([
    'status',
    'authoritativeStatus',
    'recoverStartAttempt',
  ] as const)('projects the selected root for summary reads: %s', async (method) => {
    const f = rootSummaryFixture();
    rootSummaryCollisionWorkflow(f);
    try {
      await f.runtime.start('summary-root-workflow', rootSummaryStartOptions);
      const parent = f.row();
      const read = () =>
        method === 'recoverStartAttempt'
          ? f.runtime
              .recoverStartAttempt(
                {
                  tablePrefix: '',
                  workflowId: 'summary-root-workflow',
                  runId: 'summary-run',
                  startToken: f.snapshot().requestContext?.[
                    'flowsafe.runProvenance'
                  ]?.startToken as string,
                },
                {
                  attemptToken: 'summary-attempt',
                  isOwnerQuiescent: () => true,
                  startLeg: 'unwound',
                },
              )
              .then((value) =>
                value?.kind === 'ordinary'
                  ? value.summary
                  : (value?.transition.summary ?? null),
              )
          : f.runtime[method]('summary-root-workflow', 'summary-run');
      f.reads.length = 0;
      const first = await read();
      assertRootSummary(first, parent);
      expect(f.reads).toEqual([
        method === 'recoverStartAttempt'
          ? ['summary-root-workflow', 'summary-run']
          : ['summary-run', 'summary-root-workflow'],
      ]);
      expect(suspensionDeadlinesOf(first as RunSummary)).toEqual(
        rootSummaryDeadlineRefusal,
      );
      const rootTime = (f.snapshot().context['a.b'] as RootSummaryStep)
        .suspendedAt as number;
      f.changeChild('a', 'summary-run', 'b', rootTime + 1234);
      expect(f.row()).toEqual(parent);
      f.reads.length = 0;
      const second = await read();
      expect(second).toEqual(first);
      expect(f.reads).toEqual([
        method === 'recoverStartAttempt'
          ? ['summary-root-workflow', 'summary-run']
          : ['summary-run', 'summary-root-workflow'],
      ]);
      expect(f.effects).toHaveBeenCalledTimes(2);
    } finally {
      f.close();
    }
  });

  it('recovers a stored start result without child projection', async () => {
    const f = rootSummaryFixture();
    const flow = rootSummaryCollisionWorkflow(f);
    const create = flow.createRun.bind(flow);
    const spy = vi
      .spyOn(flow, 'createRun')
      .mockImplementation(async (options) => {
        const run = await create(options);
        const start = run.start.bind(run);
        run.start = async (input) => {
          try {
            await start(input);
            f.reads.length = 0;
            throw new Error('native start receipt lost');
          } finally {
            run.start = start;
          }
        };
        return run;
      });
    try {
      const result = await f.runtime.start(
        'summary-root-workflow',
        rootSummaryStartOptions,
      );
      assertRootSummary(result, f.row());
      expect(f.reads).toEqual([['summary-root-workflow', 'summary-run']]);
      expect(f.effects).toHaveBeenCalledTimes(2);
      expect(
        f.snapshot().requestContext?.['flowsafe.runProvenance'],
      ).toMatchObject({
        version: 2,
        startToken: expect.any(String),
        resumeCounts: [],
      });
    } finally {
      spy.mockRestore();
      f.close();
    }
  });

  it('projects lifecycle completion from one root read', async () => {
    const f = rootSummaryFixture();
    const flow = rootSummaryCollisionWorkflow(f);
    const windows: unknown[][][] = [];
    let spy: { mockRestore(): void } | undefined;
    try {
      await f.runtime.start('summary-root-workflow', rootSummaryStartOptions);
      const domain = (await flow.mastra
        ?.getStorage()
        ?.getStore('workflows')) as FencedWorkflowsStorageD1;
      const native = domain[FENCED_WORKFLOW_STORAGE];
      assert(native);
      const capability = { ...native };
      Object.defineProperty(domain, FENCED_WORKFLOW_STORAGE, {
        value: capability,
        configurable: true,
      });
      const nativeRead = capability.readSnapshot;
      spy = vi
        .spyOn(capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          const start = f.reads.length;
          const selected = await nativeRead(address);
          windows.push(f.reads.slice(start));
          return selected;
        });
      const parent = f.row();
      const principal = { kind: 'human', id: 'owner' } as const;
      const missed = await f.runtime.timeOut(
        'summary-root-workflow',
        'summary-run',
        { expectedRevision: 1 },
        100,
      );
      expect(missed).toMatchObject({ transitioned: false, casMatched: false });
      assertRootSummary(missed.summary, parent);
      const terminal = await f.runtime.terminateAsPrincipal(
        'summary-root-workflow',
        'summary-run',
        principal,
        principal,
        101,
      );
      expect(terminal).toMatchObject({
        transitioned: true,
        casMatched: true,
        summary: { status: 'cancelled' },
        cleanup: { cleanupCompleted: false, revision: 1 },
      });
      const retry = await f.runtime.terminateAsPrincipal(
        'summary-root-workflow',
        'summary-run',
        principal,
        principal,
        102,
      );
      expect(retry).toMatchObject({
        transitioned: false,
        summary: { status: 'cancelled' },
        cleanup: terminal.cleanup,
      });
      const completed = await f.runtime.completeTerminalCleanup(
        'summary-root-workflow',
        'summary-run',
        terminal.cleanup.revision,
        103,
      );
      expect(completed.status).toBe('cancelled');
      await expect(
        f.runtime.completeTerminalCleanup(
          'summary-root-workflow',
          'summary-run',
          terminal.cleanup.revision,
          104,
        ),
      ).resolves.toEqual(completed);
      expect(windows).toEqual(
        Array.from({ length: 8 }, () => [
          ['summary-root-workflow', 'summary-run'],
        ]),
      );
      expect(f.effects).toHaveBeenCalledTimes(2);
    } finally {
      spy?.mockRestore();
      f.close();
    }
  });

  it('preserves detailed nested resume preparation', async () => {
    const legs: RunLeg[] = [];
    const f = rootSummaryFixture((_workflowId, _runId, leg) => {
      legs.push(leg);
      return { rootSummaryProvider: true };
    });
    const child = f
      .createWorkflow({
        id: 'nested',
        inputSchema: f.schema,
        outputSchema: f.schema,
      })
      .then(f.gate('approval', 'nested approval'))
      .commit();
    const flow = f
      .createWorkflow({
        id: 'summary-root-workflow',
        inputSchema: f.schema,
        outputSchema: f.schema,
      })
      .then(child)
      .commit();
    const spy = vi.spyOn(flow, 'getWorkflowRunById');
    try {
      await f.runtime.start('summary-root-workflow', rootSummaryStartOptions);
      f.changeChild('nested', 'summary-run', 'approval', 12345);
      legs.length = 0;
      spy.mockClear();
      const result = await f.runtime.resume(
        'summary-root-workflow',
        'summary-run',
        {
          step: ['nested', 'approval'],
          resumeData: { approve: true },
          requestedBy: 'reviewer',
          requestedByKind: 'human',
        },
      );
      expect(result.status).toBe('success');
      expect(legs).toEqual([
        {
          kind: 'resume',
          step: ['nested', 'approval'],
          suspendedAt: 12345,
          resumeCount: undefined,
        },
      ]);
      const preparation = spy.mock.calls.filter(([, options]) =>
        options?.fields?.includes('requestContext'),
      );
      expect(preparation).toHaveLength(1);
      expect(preparation[0]?.[1]?.withNestedWorkflows ?? true).toBe(true);
      expect(f.effects).toHaveBeenCalledTimes(2);
      const context = f.effects.mock.calls[1]?.[1] as RequestContext;
      expect(context.get('breakwater.connectorExecution')).toMatchObject({
        suspension: { stepPath: ['nested', 'approval'], suspendedAt: 12345 },
      });
      expect(context.get('rootSummaryProvider')).toBe(true);
      expect(
        f.snapshot().requestContext?.['flowsafe.runProvenance'],
      ).toMatchObject({
        version: 2,
        startToken: expect.any(String),
        requestedBy: 'reviewer',
        requestedByKind: 'human',
        resumeCounts: [['nested.approval', 1]],
      });
    } finally {
      spy.mockRestore();
      f.close();
    }
  });
});

describe('summary compatibility', () => {
  it.each([
    'success',
    'failure',
    'suspension',
    'resuspension',
    'parallel',
    'foreach-1',
    'foreach-3',
    'nested-1',
    'nested-2',
    'plain-dots',
    'unattributed',
  ] as const)('preserves root summary fields: %s', async (mode) => {
    const f = rootSummaryFixture();
    const root = () =>
      f.createWorkflow({
        id: 'summary-root-workflow',
        inputSchema: f.schema,
        outputSchema: f.schema,
      });
    let inputData: unknown = {};
    if (mode === 'success' || mode === 'failure') {
      root()
        .then(
          f.createStep({
            id: 'result',
            inputSchema: f.schema,
            outputSchema: f.schema,
            execute: async ({ inputData: input }) => {
              if (mode === 'failure')
                throw new Error('expected workflow failure');
              return input;
            },
          }),
        )
        .commit();
      inputData = { value: 'root result' };
    } else if (mode === 'foreach-1' || mode === 'foreach-3') {
      f.createWorkflow({
        id: 'summary-root-workflow',
        inputSchema: z.array(f.schema),
        outputSchema: z.array(f.schema),
      })
        .foreach(f.gate('gate', 'iteration'), {
          concurrency: mode === 'foreach-1' ? 1 : 3,
        })
        .commit();
      inputData = [{ n: 1 }, { n: 2 }, { n: 3 }];
    } else if (mode === 'nested-1' || mode === 'nested-2') {
      const inner = f
        .createWorkflow({
          id: 'inner',
          inputSchema: f.schema,
          outputSchema: f.schema,
        })
        .then(f.gate('gate', 'nested'))
        .commit();
      const nested =
        mode === 'nested-1'
          ? inner
          : f
              .createWorkflow({
                id: 'middle',
                inputSchema: f.schema,
                outputSchema: f.schema,
              })
              .then(inner)
              .commit();
      root().then(nested).commit();
    } else if (mode === 'parallel' || mode === 'plain-dots') {
      root()
        .parallel(
          mode === 'parallel'
            ? [f.gate('left', 'left'), f.gate('right', 'right')]
            : [f.gate('a', 'plain a'), f.gate('a.b', 'plain a.b')],
        )
        .commit();
    } else if (mode === 'resuspension') {
      root()
        .then(
          f.createStep({
            id: 'gate',
            inputSchema: f.schema,
            outputSchema: f.schema,
            execute: async ({ resumeData, suspend }) =>
              suspend({ reason: resumeData ? 'again' : 'first' }),
          }),
        )
        .commit();
    } else root().then(f.gate('gate', 'first')).commit();
    try {
      await f.runtime.start(
        'summary-root-workflow',
        mode === 'unattributed'
          ? { runId: 'summary-run', inputData }
          : { ...rootSummaryStartOptions, inputData },
      );
      if (mode === 'resuspension')
        await f.runtime.resume('summary-root-workflow', 'summary-run', {
          step: 'gate',
          resumeData: { again: true },
          requestedBy: 'reviewer',
          requestedByKind: 'human',
        });
      const before = f.row();
      for (const method of ['status', 'authoritativeStatus'] as const) {
        f.reads.length = 0;
        const summary = await f.runtime[method](
          'summary-root-workflow',
          'summary-run',
        );
        assertRootSummary(summary, before);
        expect(f.reads).toEqual([['summary-run', 'summary-root-workflow']]);
        if (mode === 'failure')
          expect(summary?.error).toContain('expected workflow failure');
        if (mode === 'resuspension')
          expect(summary?.resumeCount).toEqual({ gate: 1 });
        if (mode === 'plain-dots')
          expect(suspensionDeadlinesOf(summary as RunSummary)).toMatchObject({
            entries: [{ step: 'a' }, { step: 'a.b' }],
            rejected: [],
          });
        if (mode === 'nested-1' || mode === 'nested-2') {
          const deadlines = suspensionDeadlinesOf(summary as RunSummary);
          expect(deadlines.entries).toEqual([]);
          expect(deadlines.rejected).toEqual([
            {
              step: mode === 'nested-1' ? 'inner' : 'middle',
              reason: 'nested suspension paths are not supported',
            },
          ]);
        }
      }
      expect(f.row()).toEqual(before);
    } finally {
      f.close();
    }
  });

  it('preserves genuine missing-run and recovery mismatch behavior', async () => {
    const f = rootSummaryFixture();
    rootSummaryCollisionWorkflow(f);
    try {
      await expect(
        f.runtime.status('summary-root-workflow', 'absent'),
      ).resolves.toBeNull();
      await expect(
        f.runtime.authoritativeStatus('summary-root-workflow', 'absent'),
      ).resolves.toBeNull();
      await expect(
        f.runtime.recoverStartAttempt(
          {
            tablePrefix: '',
            workflowId: 'summary-root-workflow',
            runId: 'absent',
            startToken: 'absent',
          },
          {
            attemptToken: 'summary-attempt',
            isOwnerQuiescent: () => true,
            startLeg: 'unwound',
          },
        ),
      ).resolves.toBeNull();
      await f.runtime.start('summary-root-workflow', rootSummaryStartOptions);
      const before = f.row();
      await expect(
        f.runtime.recoverStartAttempt(
          {
            tablePrefix: '',
            workflowId: 'summary-root-workflow',
            runId: 'summary-run',
            startToken: 'wrong',
          },
          {
            attemptToken: 'summary-attempt',
            isOwnerQuiescent: () => true,
            startLeg: 'unwound',
          },
        ),
      ).rejects.toThrow('run start recovery is unresolved');
      expect(f.row()).toEqual(before);
      expect(f.effects).toHaveBeenCalledTimes(2);
    } finally {
      f.close();
    }
  });

  it('preserves fallback refusal before recovery deletion', async () => {
    const storage = new InMemoryStore();
    const { runtime, createStep, createWorkflow } = init(
      { storage },
      {
        executionFence: 'none',
        startIdempotency: 'none',
      },
    );
    const workflow = createWorkflow({
      id: 'summary-fallback-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(
        createStep({
          id: 'gate',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async ({ suspend }) => suspend({ reason: 'waiting' }),
        }),
      )
      .commit();
    const started = await runtime.start('summary-fallback-workflow', {
      runId: 'summary-run',
      inputData: {},
    });
    const remove = vi.spyOn(workflow, 'deleteWorkflowRunById');
    const domain = await storage.getStore('workflows');
    if (!domain) throw new Error('workflows domain missing');
    const blind = vi
      .spyOn(domain, 'getWorkflowRunById')
      .mockResolvedValue(null);
    const restore = () => blind.mockRestore();
    try {
      await expect(
        runtime.status('summary-fallback-workflow', started.runId),
      ).resolves.toMatchObject({ status: 'suspended' });
      await expect(
        runtime.authoritativeStatus('summary-fallback-workflow', started.runId),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
      await expect(
        runtime.recoverStartAttempt(
          {
            tablePrefix: '',
            workflowId: 'summary-fallback-workflow',
            runId: started.runId,
            startToken: 'valid',
          },
          {
            attemptToken: 'valid',
            isOwnerQuiescent: () => true,
            startLeg: 'unwound',
          },
        ),
      ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
      expect(remove).not.toHaveBeenCalled();
    } finally {
      restore();
      remove.mockRestore();
    }
    await expect(
      runtime.authoritativeStatus('summary-fallback-workflow', started.runId),
    ).resolves.toHaveProperty('status', 'suspended');
  });
});

interface Counters {
  /** Times the approval step's post-approval body ran (the gated action). */
  approvalResumes: number;
  /** Times the echo step executed. */
  echoRuns: number;
}

function runtimeAgent(id: string): Agent {
  return new Agent({
    id,
    name: id,
    instructions: 'Test agent.',
    model: 'openai/gpt-4o-mini',
  });
}

// demo-approval: research -> approval (suspends; counts resumed executions).
// echo: single step, completes immediately (counts executions).
function buildRuntime(storage: InMemoryStore): {
  runtime: RunnerRuntime;
  counters: Counters;
} {
  const counters: Counters = { approvalResumes: 0, echoRuns: 0 };
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { startIdempotency: 'none', executionFence: 'none' },
  );

  const research = createStep({
    id: 'research',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), notes: z.string() }),
    execute: async ({ inputData }) => ({
      topic: inputData.topic,
      notes: `notes:${inputData.topic}`,
    }),
  });
  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({ topic: z.string(), notes: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
      approvedBy: z.string(),
    }),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ approvedBy: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'human approval required' });
      counters.approvalResumes += 1;
      return { ...inputData, approvedBy: resumeData.approvedBy };
    },
  });
  createWorkflow({
    id: 'demo-approval',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({
      topic: z.string(),
      notes: z.string(),
      approvedBy: z.string(),
    }),
  })
    .then(research)
    .then(approval)
    .commit();

  const echo = createStep({
    id: 'echo',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
    execute: async ({ inputData }) => {
      counters.echoRuns += 1;
      return inputData;
    },
  });
  createWorkflow({
    id: 'echo',
    inputSchema: z.object({ value: z.string() }),
    outputSchema: z.object({ value: z.string() }),
  })
    .then(echo)
    .commit();

  return { runtime, counters };
}

function deferredSignal() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function ownedRuntimeFixture(
  domain: 'default' | 'background' = 'default',
  provider?: RequestContextProvider,
) {
  const sql = openSqlite() as ReturnType<typeof openSqlite> & { close(): void };
  const binding = sqliteUnitDatabase(sql) as ExecutionFenceDatabase;
  const db = binding as D1DatabaseBinding;
  const storage = createD1Storage({
    binding: db,
    ...(domain === 'background'
      ? { domains: createBackgroundTaskD1Domains({ binding: db }) }
      : {}),
  });
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
  const workflows = (await storage.getStore(
    'workflows',
  )) as FencedWorkflowsStorageD1;
  const native = workflows[FENCED_WORKFLOW_STORAGE];
  if (!native) throw new Error('missing owned capability');
  const counts = { admission: 0, terminalization: 0, callback: 0 };
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
  const app = init(
    { storage },
    {
      startIdempotency: 'none',
      executionFence: fence,
      requestContextForRun: provider,
    },
  );
  const execute = vi.fn(
    async ({ inputData }: { inputData: { value: string } }) => inputData,
  );
  const workflow = app
    .createWorkflow({
      id: 'c-workflow',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      stateSchema: z.object({ flag: z.string().optional() }),
    })
    .then(
      app.createStep({
        id: 'c-step',
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ value: z.string() }),
        execute,
      }),
    )
    .commit();
  const callback = () => {
    counts.callback++;
  };
  return {
    ...app,
    sql,
    storage,
    workflows,
    fence,
    counts,
    workflow,
    execute,
    callback,
  };
}

function capturedStartOptions(runId = 'c-run'): StartRunOptions {
  return {
    runId,
    inputData: { value: 'original' },
    initialState: { flag: 'original' },
    storedRequestContext: { 'test.c': 'original' },
    attemptToken: 'attempt-original',
    deadlineMs: 50,
    economicOperations: [
      { id: 'operation-original', settlementState: 'settled' },
    ],
    scheduleDispatch: {
      scheduleId: 'schedule-original',
      dispatchId: 'dispatch-original',
    },
    idempotencyKey: 'key-original',
    requestedBy: 'operator-1',
    requestedByKind: 'human',
    mutationEpoch: 2,
    startIdentity: {
      owner: { kind: 'human', id: 'operator-1' },
      target: { kind: 'workflow', id: 'c-workflow' },
    },
    onPreparedStartIdentity: vi.fn(),
  };
}

function observedPrimitive(
  value: string,
  mode: 'stable' | 'alternating' | 'second-throw',
) {
  return vi
    .fn<() => string>()
    .mockReturnValueOnce(value)
    .mockImplementation(() => {
      if (mode === 'second-throw') throw new Error('second primitive read');
      return mode === 'alternating' ? '' : value;
    });
}

function observedStartOptions(values: StartRunOptions) {
  const getters = Object.fromEntries(
    Object.entries(values).map(([key, value]) => [key, vi.fn(() => value)]),
  );
  const source = {} as StartRunOptions;
  for (const [key, get] of Object.entries(getters))
    Object.defineProperty(source, key, {
      get,
      configurable: true,
      enumerable: false,
    });
  return { source, getters };
}

type EconomicOperationsShape =
  | 'leading'
  | 'interior'
  | 'trailing'
  | 'all-hole'
  | 'dense'
  | 'inherited'
  | 'empty';

function economicOperationsArray(shape: EconomicOperationsShape) {
  const operations: Array<{ id: string; settlementState: string }> =
    shape === 'all-hole'
      ? new Array(3)
      : shape === 'empty'
        ? []
        : [
            { id: 'first', settlementState: 'settled' },
            { id: 'second', settlementState: 'held' },
            { id: 'third', settlementState: 'disputed' },
          ];
  if (shape === 'inherited') {
    const prototype = Object.create(Array.prototype);
    Object.defineProperty(prototype, '1', { value: operations[1] });
    Object.setPrototypeOf(operations, prototype);
    delete operations[1];
  } else if (
    shape === 'leading' ||
    shape === 'interior' ||
    shape === 'trailing'
  ) {
    delete operations[{ leading: 0, interior: 1, trailing: 2 }[shape]];
  }
  return operations;
}

describe('economic format safety', () => {
  it.each(
    (['default', 'background'] as const).flatMap((domain) =>
      (['leading', 'interior', 'trailing', 'all-hole'] as const).map(
        (shape) => ({ domain, shape }),
      ),
    ),
  )('refuses sparse economic operations before start effects ($domain, $shape)', async ({
    domain,
    shape,
  }) => {
    const provider = vi.fn(() => ({}));
    const f = await ownedRuntimeFixture(domain, provider);
    const read = vi.spyOn(f.fence, 'read');
    const create = vi.spyOn(f.workflow, 'createRun');
    const persist = vi.spyOn(f.workflows, 'persistWorkflowSnapshot');
    try {
      const error = await f.runtime
        .start('c-workflow', {
          ...capturedStartOptions(),
          economicOperations: economicOperationsArray(shape),
        })
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(Error);
      expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(read).not.toHaveBeenCalled();
      expect(provider).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(persist).not.toHaveBeenCalled();
      expect(f.execute).not.toHaveBeenCalled();
      expect(
        await f.workflows.loadWorkflowSnapshot({
          workflowName: 'c-workflow',
          runId: 'c-run',
        }),
      ).toBeNull();
    } finally {
      f.sql.close();
    }
  });

  it('refuses sparse capture before reading later economic entries', async () => {
    const f = await ownedRuntimeFixture();
    const later = vi.fn(() => {
      throw new Error('later entry must not be read');
    });
    const operations = new Array(2);
    operations[1] = {
      get id() {
        return later();
      },
      settlementState: 'held',
    };
    try {
      const error = await f.runtime
        .start('c-workflow', {
          ...capturedStartOptions(),
          economicOperations: operations,
        })
        .catch((cause: unknown) => cause);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(later).not.toHaveBeenCalled();
      expect(f.execute).not.toHaveBeenCalled();
    } finally {
      f.sql.close();
    }
  });

  it.each(
    (['default', 'background'] as const).flatMap((domain) =>
      (['dense', 'inherited', 'empty'] as const).map((shape) => ({
        domain,
        shape,
      })),
    ),
  )('round-trips dense economic arrays on owned storage ($domain, $shape)', async ({
    domain,
    shape,
  }) => {
    const f = await ownedRuntimeFixture(domain);
    const operations = economicOperationsArray(shape);
    const expected = JSON.parse(JSON.stringify(operations));
    try {
      await expect(
        f.runtime.start('c-workflow', {
          ...capturedStartOptions(),
          economicOperations: operations,
        }),
      ).resolves.toMatchObject({ status: 'success' });
      const snapshot = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'c-workflow',
        runId: 'c-run',
      });
      expect(snapshot?.requestContext?.['flowsafe.runLifecycle']).toMatchObject(
        { economicOperations: expected },
      );
      expect((await f.runtime.status('c-workflow', 'c-run'))?.status).toBe(
        'success',
      );
      expect(f.execute).toHaveBeenCalledOnce();
    } finally {
      f.sql.close();
    }
  });

  it.each(
    (['default', 'background'] as const).flatMap((domain) =>
      (['leading', 'interior', 'trailing', 'all-hole'] as const).map(
        (shape) => ({ domain, shape }),
      ),
    ),
  )('sparse resume preserves the readable suspended snapshot ($domain, $shape)', async ({
    domain,
    shape,
  }) => {
    const provider = vi.fn(() => ({}));
    const f = await ownedRuntimeFixture(domain, provider);
    const resumed = vi.fn();
    const schema = z.object({ value: z.string() });
    const workflow = f
      .createWorkflow({
        id: 'c-sparse-resume',
        inputSchema: schema,
        outputSchema: schema,
      })
      .then(
        f.createStep({
          id: 'gate',
          inputSchema: schema,
          outputSchema: schema,
          suspendSchema: z.object({}),
          resumeSchema: z.object({ ok: z.boolean() }),
          execute: async ({ inputData, resumeData, suspend }) => {
            if (!resumeData) return suspend({});
            resumed();
            return inputData;
          },
        }),
      )
      .commit();
    try {
      expect(
        (
          await f.runtime.start(workflow.id, {
            runId: 'resume-run',
            mutationEpoch: 2,
            inputData: { value: 'original' },
          })
        ).status,
      ).toBe('suspended');
      const before = await f.workflows.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'resume-run',
      });
      const create = vi.spyOn(workflow, 'createRun');
      const persist = vi.spyOn(f.workflows, 'persistWorkflowSnapshot');
      provider.mockClear();
      const error = await f.runtime
        .resume(workflow.id, 'resume-run', {
          step: 'gate',
          resumeData: { ok: true },
          economicOperations: economicOperationsArray(shape),
        })
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(Error);
      expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(provider).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(persist).not.toHaveBeenCalled();
      expect(resumed).not.toHaveBeenCalled();
      expect(
        await f.workflows.loadWorkflowSnapshot({
          workflowName: workflow.id,
          runId: 'resume-run',
        }),
      ).toEqual(before);
      expect((await f.runtime.status(workflow.id, 'resume-run'))?.status).toBe(
        'suspended',
      );
      expect(
        (
          await f.runtime.resume(workflow.id, 'resume-run', {
            step: 'gate',
            resumeData: { ok: true },
            economicOperations: economicOperationsArray('dense'),
          })
        ).status,
      ).toBe('success');
    } finally {
      f.sql.close();
    }
  });
});

describe('RunnerRuntime run state SQLite cannot store', () => {
  const schema = z.any();

  /** A workflow over memory storage whose one step suspends until it is resumed. */
  function nestedInputApp() {
    const app = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none', startIdempotency: 'none' },
    );
    app
      .createWorkflow({
        id: 'nested-input',
        inputSchema: schema,
        outputSchema: schema,
        stateSchema: schema,
      })
      .then(
        app.createStep({
          id: 'gate',
          inputSchema: schema,
          outputSchema: schema,
          stateSchema: schema,
          suspendSchema: schema,
          resumeSchema: schema,
          execute: async ({ resumeData, suspend }) => {
            if (!resumeData) return suspend({});
            return {};
          },
        }),
      )
      .commit();
    return app;
  }

  function storedRow(sql: ReturnType<typeof openSqlite>, workflowId: string) {
    const row = sql
      .prepare(
        'SELECT json_valid(snapshot) AS valid, snapshot FROM mastra_workflow_snapshot WHERE workflow_name = ?',
      )
      .get(workflowId) as { valid: number; snapshot: string };
    return { valid: row.valid, snapshot: JSON.parse(row.snapshot) };
  }

  /** D1 storage and the workflows these tests run; `shared` is the storage another instance already uses. */
  async function deepOutputApp(
    requestContextForRun?: RequestContextProvider,
    shared?: {
      sql: ReturnType<typeof openSqlite>;
      storage: MastraCompositeStore;
    },
  ) {
    const sql = shared?.sql ?? openSqlite();
    const storage =
      shared?.storage ??
      createD1Storage({
        binding: sqliteUnitDatabase(sql) as D1DatabaseBinding,
      });
    if (!shared) await storage.init();
    const app = init(
      { storage },
      {
        executionFence: 'none',
        startIdempotency: 'none',
        requestContextForRun,
      },
    );
    const entered = deferredSignal();
    const release = deferredSignal();
    const deep = { value: nestedArray(PAST_SQLITE_JSON_DEPTH) };
    const oneStep = (
      workflowId: string,
      stepId: string,
      execute: Parameters<typeof app.createStep>[0]['execute'],
    ) =>
      app
        .createWorkflow({
          id: workflowId,
          inputSchema: schema,
          outputSchema: schema,
        })
        .then(
          app.createStep({
            id: stepId,
            inputSchema: schema,
            outputSchema: schema,
            suspendSchema: schema,
            resumeSchema: schema,
            execute,
          }),
        )
        .commit();
    oneStep('deep-context', 'cache', async ({ requestContext }) => {
      requestContext.set('cache', nestedArray(PAST_SQLITE_JSON_DEPTH));
      return {};
    });
    oneStep('shallow-on-resume', 'gate', async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({});
      return {};
    });
    oneStep('held-deep', 'hold', async () => {
      entered.resolve();
      await release.promise;
      return deep;
    });
    oneStep('deep-output', 'produce', async () => deep);
    oneStep('deep-on-resume', 'gate', async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({});
      return deep;
    });
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    return { sql, storage, app, workflows, entered, release };
  }

  it.each([
    ['inputData', 'inputData', { inputData: nestedObject(257) }],
    ['initialState', 'initialState', { initialState: nestedObject(257) }],
    [
      'requestContext value',
      'requestContext',
      { storedRequestContext: { app: nestedObject(257) } },
    ],
  ] as const)('refuses a start whose %s nests 257 levels deep', async (_, field, deep) => {
    // #given a workflow and a start request whose one tenant value nests 257 levels
    const { runtime } = nestedInputApp();

    // #when the run is started
    const started = runtime.start('nested-input', {
      runId: 'deep-start',
      inputData: {},
      ...deep,
    });

    // #then it is refused as an invalid request naming the field, and no run exists
    await expect(started).rejects.toMatchObject({
      name: 'InvalidRunRequestError',
      message: expect.stringContaining(field),
    });
    await expect(runtime.status('nested-input', 'deep-start')).resolves.toBe(
      null,
    );
  });

  it('starts a run whose input nests exactly 256 levels deep', async () => {
    // #given a workflow
    const { runtime } = nestedInputApp();

    // #when the run is started with input nested 256 levels
    const started = runtime.start('nested-input', {
      runId: 'deep-start',
      inputData: nestedObject(256),
    });

    // #then it runs to its suspension
    await expect(started).resolves.toMatchObject({ status: 'suspended' });
  });

  it('refuses a resume whose resumeData nests 257 levels deep and leaves the run suspended', async () => {
    // #given a suspended run
    const { runtime } = nestedInputApp();
    await runtime.start('nested-input', {
      runId: 'deep-resume',
      inputData: {},
    });

    // #when it is resumed with data nested 257 levels
    const resumed = runtime.resume('nested-input', 'deep-resume', {
      step: 'gate',
      resumeData: nestedObject(257),
    });

    // #then the resume is refused as an invalid request and the run stays suspended
    await expect(resumed).rejects.toMatchObject({
      name: 'InvalidRunRequestError',
      message: expect.stringContaining('resumeData'),
    });
    await expect(
      runtime.status('nested-input', 'deep-resume'),
    ).resolves.toMatchObject({ status: 'suspended' });
  });

  it('does not bound the loop input of an agent start', async () => {
    // #given a workflow and an agent start, whose loop input carries thread
    // history the caller did not send
    const { runtime } = nestedInputApp();

    // #when the run is started with input nested 300 levels
    const started = runtime.start('nested-input', {
      runId: 'agent-loop',
      inputData: nestedObject(300),
      requestedBy: 'operator-1',
      requestedByKind: 'human',
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'agent-1', threadId: 'thread-1' },
      },
      agentStart: { threaded: false },
    });

    // #then it is not refused for its depth
    await expect(started).resolves.toMatchObject({ status: 'suspended' });
  });

  it('refuses an agent start whose initial state nests 257 levels deep', async () => {
    // #given a workflow and an agent start
    const { runtime } = nestedInputApp();

    // #when the run is started with an initial state nested 257 levels
    const started = runtime.start('nested-input', {
      runId: 'agent-state',
      inputData: {},
      initialState: nestedObject(257),
      requestedBy: 'operator-1',
      requestedByKind: 'human',
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'agent-1', threadId: 'thread-1' },
      },
      agentStart: { threaded: false },
    });

    // #then it is refused as an invalid request
    await expect(started).rejects.toMatchObject({
      name: 'InvalidRunRequestError',
      message: expect.stringContaining('initialState'),
    });
  });

  it.each([
    'start',
    'resume',
  ] as const)('fails a run at once when its leg returns state too deep to store (%s)', async (leg) => {
    // #given D1 storage and a run whose step returns state nested past the
    // depth SQLite parses
    const { sql, app, workflows } = await deepOutputApp();
    const workflowId = leg === 'start' ? 'deep-output' : 'deep-on-resume';

    // #when the leg runs
    const summary =
      leg === 'start'
        ? await app.runtime.start(workflowId, {
            runId: 'deep-run',
            inputData: {},
          })
        : await app.runtime
            .start(workflowId, { runId: 'deep-run', inputData: {} })
            .then(() =>
              app.runtime.resume(workflowId, 'deep-run', {
                step: 'gate',
                resumeData: { ok: true },
              }),
            );

    // #then the run fails at once with the storage refusal as its envelope
    expect(summary).toMatchObject({
      status: 'failed',
      errorEnvelope: { code: 'RUN_STATE_NOT_STORABLE' },
    });

    // #then the stored row is one SQLite parses, and records the settlement
    const { valid, snapshot } = storedRow(sql, workflowId);
    expect(valid).toBe(1);
    expect(snapshot.requestContext['flowsafe.runLifecycle']).toMatchObject({
      stateNotStorableAt: expect.any(Number),
    });

    // #when a stale write persists over it
    const stale = workflows.persistWorkflowSnapshot({
      workflowName: workflowId,
      runId: 'deep-run',
      snapshot: { ...snapshot, status: 'running' },
    });

    // #then the settlement refuses it
    await expect(stale).rejects.toMatchObject({
      name: 'RunSettledConflictError',
    });
  });

  it('stores a settlement SQLite parses when a step put the deep value in its request context', async () => {
    // #given a step that caches a value nested past the depth SQLite parses in
    // its request context, which the engine writes with every snapshot
    const { sql, app } = await deepOutputApp();

    // #when the leg runs
    const summary = await app.runtime.start('deep-context', {
      runId: 'deep-context-run',
      inputData: {},
    });

    // #then the run fails at once with the storage refusal as its envelope
    expect(summary).toMatchObject({
      status: 'failed',
      errorEnvelope: { code: 'RUN_STATE_NOT_STORABLE' },
    });

    // #then the settlement is a row SQLite parses
    expect(storedRow(sql, 'deep-context').valid).toBe(1);
  });

  it('stores a settlement SQLite parses when the host supplies the deep value to a resume leg', async () => {
    // #given a suspended run, and a host whose request context for its resume
    // legs holds a value nested past the depth SQLite parses
    const { sql, app } = await deepOutputApp((_workflowId, _runId, leg) =>
      leg.kind === 'resume'
        ? { cache: nestedArray(PAST_SQLITE_JSON_DEPTH) }
        : undefined,
    );
    await app.runtime.start('shallow-on-resume', {
      runId: 'host-deep-run',
      inputData: {},
    });

    // #when the run is resumed
    const summary = await app.runtime.resume(
      'shallow-on-resume',
      'host-deep-run',
      {
        step: 'gate',
        resumeData: { ok: true },
      },
    );

    // #then the run fails at once with the storage refusal as its envelope
    expect(summary).toMatchObject({
      status: 'failed',
      errorEnvelope: { code: 'RUN_STATE_NOT_STORABLE' },
    });

    // #then the settlement is a row SQLite parses
    expect(storedRow(sql, 'shallow-on-resume').valid).toBe(1);
  });

  it('aborts the leg when it fails the run at once', async () => {
    // #given a leg with an abort controller, whose step returns state nested
    // past the depth SQLite parses
    const { app } = await deepOutputApp();
    const legAbort = new AbortController();

    // #when the leg runs
    const summary = await app.runtime.start('deep-output', {
      runId: 'aborted-run',
      inputData: {},
      legAbort,
    });

    // #then the run fails at once, and the leg's model and tool calls in flight
    // are told to stop
    expect(summary).toMatchObject({
      status: 'failed',
      errorEnvelope: { code: 'RUN_STATE_NOT_STORABLE' },
    });
    expect(legAbort.signal.aborted).toBe(true);
    expect(legAbort.signal.reason).toMatchObject({
      name: 'AbortError',
      cause: { name: 'RunStateNotStorableError' },
    });
  });

  it('settles a run over its pending initial row when the first engine write is refused', async () => {
    // #given a fenced runtime, and an agent start whose loop input nests past
    // the depth SQLite parses, as recalled thread history can
    const f = await ownedRuntimeFixture();
    f.createWorkflow({
      id: 'deep-input',
      inputSchema: schema,
      outputSchema: schema,
    })
      .then(
        f.createStep({
          id: 'quiet',
          inputSchema: schema,
          outputSchema: schema,
          execute: async () => ({}),
        }),
      )
      .commit();
    const landed: string[] = [];
    const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
    vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
      async (args) => {
        await persist(args);
        landed.push(args.snapshot.status);
      },
    );

    // #when the leg runs
    const summary = await f.runtime.start('deep-input', {
      runId: 'pending-run',
      mutationEpoch: 2,
      inputData: nestedArray(PAST_SQLITE_JSON_DEPTH),
      requestedBy: 'operator-1',
      requestedByKind: 'human',
      startIdentity: {
        owner: { kind: 'human', id: 'operator-1' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread-1' },
      },
      agentStart: { threaded: false },
    });

    // #then nothing but the admission row landed before the settlement, which
    // reads as this leg's failed outcome
    expect(landed).toEqual(['pending']);
    expect(summary).toMatchObject({
      status: 'failed',
      errorEnvelope: { code: 'RUN_STATE_NOT_STORABLE' },
    });

    // #then the stored row is one SQLite parses, without the admission stamp
    const stored = storedRow(f.sql, 'deep-input');
    expect(stored.valid).toBe(1);
    expect(stored.snapshot.status).toBe('failed');
    expect(
      stored.snapshot.requestContext['flowsafe.runProvenance'],
    ).not.toHaveProperty('initialAdmission');
  });

  describe('a leg whose write is refused while another instance changed its run', () => {
    const OPERATOR = { kind: 'human' as const, id: 'operator-1' };

    /** A `held-deep` leg held in its step, and a second instance on its storage. */
    async function heldDeepLeg() {
      const leg = await deepOutputApp();
      const other = await deepOutputApp(undefined, leg);
      const outcome = leg.app.runtime
        .start('held-deep', { runId: 'held-run', inputData: {} })
        .then(
          (summary) => summary,
          (error: unknown) => error,
        );
      onTestFinished(async () => {
        leg.release.resolve();
        await outcome;
      });
      await leg.entered.promise;
      return {
        ...leg,
        other,
        outcome,
        stored: () => storedRow(leg.sql, 'held-deep').snapshot,
      };
    }

    it('keeps a recorded cancellation, which the terminate then completes', async () => {
      // #given a held leg whose run another instance recorded a cancellation for
      const h = await heldDeepLeg();
      await h.other.app.runtime.cancelActiveExecution(
        'held-deep',
        'held-run',
        'cancelled',
        [OPERATOR],
      );

      // #when the step returns state nested past the depth SQLite parses
      h.release.resolve();
      await h.outcome;

      // #then the run was not recorded as failed, and the terminate cancels it
      const lifecycle = h.stored().requestContext['flowsafe.runLifecycle'];
      expect(lifecycle).toMatchObject({
        transitionIntent: { status: 'cancelled' },
      });
      expect(lifecycle).not.toHaveProperty('stateNotStorableAt');
      await expect(
        h.other.app.runtime.terminate('held-deep', 'held-run'),
      ).resolves.toMatchObject({
        transitioned: true,
        summary: { status: 'cancelled' },
      });
    });

    it('keeps a run another instance settled', async () => {
      // #given a held leg whose run another instance terminated
      const h = await heldDeepLeg();
      await h.other.app.runtime.terminate('held-deep', 'held-run');

      // #when the step returns state nested past the depth SQLite parses
      h.release.resolve();
      const ended = await h.outcome;

      // #then the run stays cancelled, and the leg answers that outcome
      expect(h.stored()).toMatchObject({ status: 'cancelled' });
      expect(
        h.stored().requestContext['flowsafe.runLifecycle'],
      ).not.toHaveProperty('stateNotStorableAt');
      expect(ended).toMatchObject({ status: 'cancelled' });
    });

    it('keeps a run whose lifecycle records a settlement while its status still reads running', async () => {
      // #given a held leg whose run row records an interruption
      const h = await heldDeepLeg();
      const lifecycle = { version: 1, revision: 3, interruptedAt: 1 };
      h.sql
        .prepare(
          `UPDATE mastra_workflow_snapshot SET snapshot = json_set(snapshot, '$.requestContext."flowsafe.runLifecycle"', json(?)) WHERE workflow_name = ?`,
        )
        .run(JSON.stringify(lifecycle), 'held-deep');

      // #when the step returns state nested past the depth SQLite parses
      h.release.resolve();
      await h.outcome;

      // #then the row keeps the lifecycle it had and records no further failure
      expect(h.stored()).toMatchObject({
        status: 'running',
        requestContext: { 'flowsafe.runLifecycle': lifecycle },
      });
    });

    it('keeps a run whose status is not one a run can still leave', async () => {
      // #given a held leg whose run row reads success
      const h = await heldDeepLeg();
      h.sql
        .prepare(
          `UPDATE mastra_workflow_snapshot SET snapshot = json_set(snapshot, '$.status', 'success') WHERE workflow_name = ?`,
        )
        .run('held-deep');

      // #when the step returns state nested past the depth SQLite parses
      h.release.resolve();
      await h.outcome;

      // #then the row keeps its status and records no failure
      expect(h.stored()).toMatchObject({ status: 'success' });
      expect(
        h.stored().requestContext?.['flowsafe.runLifecycle']
          ?.stateNotStorableAt,
      ).toBeUndefined();
    });
  });
});

describe('Runtime capture', () => {
  it.each(
    (['default', 'background'] as const).flatMap((domain) =>
      [undefined, 1, 2, 3].map((epoch) => ({ domain, epoch })),
    ),
  )('enforces active mutation epoch at Runtime start ($domain, $epoch)', async ({
    domain,
    epoch,
  }) => {
    const f = await ownedRuntimeFixture(domain);
    try {
      expect(await f.fence.read()).toMatchObject({
        state: 'open',
        mutationEpoch: 2,
        requireMutationEpoch: true,
      });
      const runId = `epoch-${epoch ?? 'missing'}`;
      const options = {
        ...capturedStartOptions(runId),
        onPreparedStartIdentity: f.callback,
      };
      if (epoch === undefined)
        delete (options as { mutationEpoch?: number }).mutationEpoch;
      else Object.assign(options, { mutationEpoch: epoch });
      if (epoch !== 2) {
        await expect(f.runtime.start('c-workflow', options)).rejects.toThrow(
          'mutation epoch does not match',
        );
        expect(f.execute).not.toHaveBeenCalled();
        expect(f.counts).toEqual({
          callback: 0,
          admission: 0,
          terminalization: 0,
        });
        return;
      }
      await expect(
        f.runtime.start('c-workflow', options),
      ).resolves.toMatchObject({ status: 'success' });
      const snapshot = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'c-workflow',
        runId,
      });
      expect(snapshot?.requestContext?.['flowsafe.runProvenance']).toEqual({
        version: 2,
        requestedBy: 'operator-1',
        requestedByKind: 'human',
        startToken: expect.any(String),
        mutationEpoch: 2,
        startIdentity: capturedStartOptions().startIdentity,
        attemptToken: 'attempt-original',
        resumeCounts: [],
      });
      for (const key of [
        'mutationEpoch',
        'startIdentity',
        'agentStart',
        'execution',
        'onPreparedStartIdentity',
        'runOwnerGuard',
        'flowsafe.initialAdmission',
      ])
        expect(snapshot?.requestContext).not.toHaveProperty(key);
      expect(f.execute).toHaveBeenCalledOnce();
      expect(f.counts).toEqual({
        callback: 1,
        admission: 1,
        terminalization: 0,
      });
    } finally {
      f.sql.close();
    }
  });

  for (const [title, counter] of [
    ['invokes preparation only after provider success', 'callback'],
    ['admits each prepared start through the owned capability', 'admission'],
    ['keeps terminalization exclusively in owning recovery', 'terminalization'],
  ] as const) {
    it.each([
      'default',
      'background',
    ] as const)(`${title} (%s)`, async (domain) => {
      let failProvider = false;
      const failure = new Error('provider failure');
      const f = await ownedRuntimeFixture(domain, () => {
        if (failProvider) throw failure;
        return {};
      });
      try {
        let admitted = 0;
        for (const phase of [
          'normal',
          'failure',
          'recovery',
          'failed-step',
        ] as const) {
          const create = f.workflow.createRun.bind(f.workflow);
          let restore = () => {};
          if (phase === 'recovery') {
            const spy = vi
              .spyOn(f.workflow, 'createRun')
              .mockImplementation(async (...args) => {
                const run = await create(...args);
                const start = run.start.bind(run);
                vi.spyOn(run, 'start').mockImplementation(
                  async (...startArgs) => {
                    await start(...startArgs);
                    throw new Error('lost start result');
                  },
                );
                return run;
              });
            restore = () => spy.mockRestore();
          }
          if (phase === 'failed-step')
            f.execute.mockRejectedValueOnce(new Error('step failed'));
          failProvider = phase === 'failure';
          const runId = `c-${phase}`;
          let outcome: RunSummary | undefined;
          try {
            const pending = f.runtime.start('c-workflow', {
              ...capturedStartOptions(runId),
              onPreparedStartIdentity: f.callback,
            });
            if (phase === 'failure')
              await expect(pending).rejects.toBe(failure);
            else outcome = await pending;
          } finally {
            restore();
            if (phase !== 'failure') admitted++;
            expect(f.counts[counter]).toBe(
              counter === 'terminalization' ? 0 : admitted,
            );
            expect(f.counts).toEqual({
              callback: admitted,
              admission: admitted,
              terminalization: 0,
            });
          }
          const snapshot = await f.workflows.loadWorkflowSnapshot({
            workflowName: 'c-workflow',
            runId,
          });
          if (phase === 'failure') expect(snapshot).toBeNull();
          else {
            expect(outcome?.status).toBe(
              phase === 'failed-step' ? 'failed' : 'success',
            );
            expect(
              snapshot?.requestContext?.['flowsafe.runProvenance'],
            ).toEqual({
              version: 2,
              requestedBy: 'operator-1',
              requestedByKind: 'human',
              startToken: expect.any(String),
              mutationEpoch: 2,
              startIdentity: capturedStartOptions().startIdentity,
              attemptToken: 'attempt-original',
              resumeCounts: [],
            });
            expect(snapshot?.requestContext).not.toHaveProperty(
              'flowsafe.initialAdmission',
            );
            expect((await f.runtime.status('c-workflow', runId))?.status).toBe(
              outcome?.status,
            );
          }
        }
      } finally {
        f.sql.close();
      }
    });
  }

  it('captures Runtime options before the fence wait', async () => {
    const providerEntered = deferredSignal();
    const providerRelease = deferredSignal();
    const f = await ownedRuntimeFixture('default', async () => {
      providerEntered.resolve();
      await providerRelease.promise;
      return {};
    });
    const entered = deferredSignal();
    const release = deferredSignal();
    const read = f.fence.read.bind(f.fence);
    const fenceRead = vi
      .spyOn(f.fence, 'read')
      .mockImplementationOnce(async () => {
        entered.resolve();
        await release.promise;
        return read();
      });
    const clock = vi.spyOn(Date, 'now').mockReturnValue(1000);
    const { source, getters } = observedStartOptions(capturedStartOptions());
    const pending = f.runtime.start('c-workflow', source);
    void pending.catch(() => undefined);
    try {
      await entered.promise;
      for (const getter of Object.values(getters))
        expect(getter).toHaveBeenCalledTimes(1);
      expect(f.execute).not.toHaveBeenCalled();
      for (const getter of Object.values(getters))
        getter.mockImplementation(() => {
          throw new Error('late Runtime option read');
        });
      clock.mockReturnValue(2000);
      release.resolve();
      await providerEntered.promise;
      clock.mockReturnValue(3000);
      providerRelease.resolve();
      expect(await pending).toMatchObject({
        runId: 'c-run',
        status: 'success',
        requestedBy: 'operator-1',
        deadlineAt: 2050,
      });
      const snapshot = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'c-workflow',
        runId: 'c-run',
      });
      expect(snapshot?.requestContext).toMatchObject({
        'test.c': 'original',
        'flowsafe.runLifecycle': {
          deadlineAt: 2050,
          scheduleDispatch: {
            scheduleId: 'schedule-original',
            dispatchId: 'dispatch-original',
          },
          economicOperations: [
            { id: 'operation-original', settlementState: 'settled' },
          ],
        },
      });
      expect(f.execute.mock.calls[0]?.[0].inputData).toEqual({
        value: 'original',
      });
      for (const getter of Object.values(getters))
        expect(getter).toHaveBeenCalledTimes(1);
      expect(Object.isFrozen(source)).toBe(false);
    } finally {
      release.resolve();
      providerRelease.resolve();
      await pending.catch(() => undefined);
      clock.mockRestore();
      fenceRead.mockRestore();
      f.sql.close();
    }
  });

  it('captures Runtime options before waiting for the run lock', async () => {
    const entered = deferredSignal();
    const release = deferredSignal();
    const f = await ownedRuntimeFixture('default', async () => {
      entered.resolve();
      await release.promise;
      return {};
    });
    const first = f.runtime.start('c-workflow', capturedStartOptions());
    void first.catch(() => undefined);
    await entered.promise;
    const queued = deferredSignal();
    const nativeSet = Map.prototype.set;
    const set = vi.spyOn(Map.prototype, 'set').mockImplementation(function (
      this: Map<unknown, unknown>,
      key,
      value,
    ) {
      if (key === 'c-workflow:c-run' && value instanceof Promise)
        queued.resolve();
      return nativeSet.call(this, key, value);
    });
    const { source, getters } = observedStartOptions(capturedStartOptions());
    const second = f.runtime.start('c-workflow', source);
    void second.catch(() => undefined);
    try {
      await queued.promise;
      for (const getter of Object.values(getters))
        expect(getter).toHaveBeenCalledTimes(1);
      for (const getter of Object.values(getters))
        getter.mockImplementation(() => {
          throw new Error('late lock option read');
        });
      expect(f.execute).not.toHaveBeenCalled();
      release.resolve();
      expect((await first).status).toBe('success');
      await expect(second).rejects.toBeInstanceOf(RunAlreadyExistsError);
      for (const getter of Object.values(getters))
        expect(getter).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await Promise.allSettled([first, second]);
      set.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    ['epoch', { mutationEpoch: -1 }, InvalidMutationEpochError],
    ['identity', { startIdentity: null }, InvalidExecutionIdentityError],
    [
      'target',
      {
        startIdentity: {
          owner: { kind: 'human', id: 'operator-1' },
          target: { kind: 'workflow', id: 'other' },
        },
      },
      InvalidRunRequestError,
    ],
    [
      'owner',
      {
        startIdentity: {
          owner: { kind: 'service', id: 'other' },
          target: { kind: 'workflow', id: 'c-workflow' },
        },
      },
      InvalidRunRequestError,
    ],
    ['mode', { agentStart: { threaded: 'yes' } }, InvalidRunRequestError],
    [
      'agent-mode',
      {
        startIdentity: {
          owner: { kind: 'human', id: 'operator-1' },
          target: { kind: 'agent', id: 'writer', threadId: 'thread' },
        },
        agentStart: undefined,
      },
      InvalidRunRequestError,
    ],
    ['callback', { onPreparedStartIdentity: null }, InvalidRunRequestError],
    [
      'guard',
      {
        runOwnerGuard: {
          owner: { kind: 'other', id: 'owner' },
          reservationToken: 'token',
        },
      },
      InvalidRunRequestError,
    ],
    ['requester', { requestedByKind: undefined }, InvalidRunRequestError],
    ['deadline', { deadlineMs: -1 }, InvalidRunRequestError],
    ['leg abort', { legAbort: {} }, InvalidRunRequestError],
    ['dispatch', { scheduleDispatch: [] }, Error],
    ['operations', { economicOperations: [null] }, Error],
  ] as const)('validates supplied fields before fence and storage (%s)', async (_label, changes, errorType) => {
    const f = await ownedRuntimeFixture();
    const read = vi.spyOn(f.fence, 'read');
    const create = vi.spyOn(f.workflow, 'createRun');
    try {
      const error = await f.runtime
        .start('c-workflow', {
          ...capturedStartOptions(),
          ...changes,
        } as StartRunOptions)
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(errorType);
      expect(Object.getPrototypeOf(error)).toBe(errorType.prototype);
      if (errorType === Error)
        expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(read).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(f.execute).not.toHaveBeenCalled();
    } finally {
      read.mockRestore();
      create.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    ['dispatch-null', { scheduleDispatch: null }],
    ['dispatch-array', { scheduleDispatch: [] }],
    [
      'schedule-empty',
      { scheduleDispatch: { scheduleId: '', dispatchId: 'dispatch' } },
    ],
    [
      'schedule-number',
      { scheduleDispatch: { scheduleId: 2, dispatchId: 'dispatch' } },
    ],
    [
      'dispatch-null-id',
      { scheduleDispatch: { scheduleId: 'schedule', dispatchId: null } },
    ],
    [
      'dispatch-path',
      { scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'bad/path' } },
    ],
    ['operations-object', { economicOperations: {} }],
    ['operation-null', { economicOperations: [null] }],
    ['operation-array', { economicOperations: [[]] }],
    [
      'operation-empty-id',
      { economicOperations: [{ id: '', settlementState: 'held' }] },
    ],
    [
      'operation-number-id',
      { economicOperations: [{ id: 2, settlementState: 'held' }] },
    ],
    [
      'operation-path-id',
      { economicOperations: [{ id: 'bad/path', settlementState: 'held' }] },
    ],
    [
      'state-null',
      { economicOperations: [{ id: 'operation', settlementState: null }] },
    ],
    [
      'state-boolean',
      { economicOperations: [{ id: 'operation', settlementState: true }] },
    ],
    [
      'state-empty',
      { economicOperations: [{ id: 'operation', settlementState: '' }] },
    ],
    [
      'state-long',
      {
        economicOperations: [
          { id: 'operation', settlementState: 'x'.repeat(101) },
        ],
      },
    ],
  ] as const)('rejects malformed lifecycle input with the exact legacy error before effects: %s', async (_label, changes) => {
    const provider = vi.fn(() => ({}));
    const f = await ownedRuntimeFixture('default', provider);
    const read = vi.spyOn(f.fence, 'read');
    const create = vi.spyOn(f.workflow, 'createRun');
    const persist = vi.spyOn(f.workflows, 'persistWorkflowSnapshot');
    try {
      const error = await f.runtime
        .start('c-workflow', {
          ...capturedStartOptions(),
          ...changes,
        } as StartRunOptions)
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(Error);
      expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(read).not.toHaveBeenCalled();
      expect(provider).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(persist).not.toHaveBeenCalled();
      expect(f.execute).not.toHaveBeenCalled();
    } finally {
      f.sql.close();
    }
  });

  it.each([
    null,
    '1',
    true,
    -1,
    1.5,
    NaN,
    Infinity,
    Number.MAX_SAFE_INTEGER,
  ])('rejects malformed economic array length before effects: %s', async (length) => {
    const provider = vi.fn(() => ({}));
    const f = await ownedRuntimeFixture('default', provider);
    const read = vi.spyOn(f.fence, 'read');
    const create = vi.spyOn(f.workflow, 'createRun');
    const operations = new Proxy(economicOperationsArray('dense'), {
      get(target, key, receiver) {
        return key === 'length' ? length : Reflect.get(target, key, receiver);
      },
    });
    try {
      const error = await f.runtime
        .start('c-workflow', {
          ...capturedStartOptions(),
          economicOperations: operations,
        })
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(Error);
      expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
      expect(error).toEqual(new Error('stored run lifecycle is malformed'));
      expect(read).not.toHaveBeenCalled();
      expect(provider).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(f.execute).not.toHaveBeenCalled();
    } finally {
      f.sql.close();
    }
  });

  it('keeps unattributed unkeyed starts and first getter faults without effects', async () => {
    const f = await ownedRuntimeFixture();
    const read = vi.spyOn(f.fence, 'read');
    const fault = new Error('first Runtime read');
    try {
      await expect(
        f.runtime.start('c-workflow', {
          get runId(): string {
            throw fault;
          },
        }),
      ).rejects.toBe(fault);
      for (const changes of [
        {
          scheduleDispatch: {
            get scheduleId(): string {
              throw fault;
            },
            dispatchId: 'dispatch',
          },
        },
        {
          economicOperations: [
            {
              get id(): string {
                throw fault;
              },
              settlementState: 'settled',
            },
          ],
        },
      ])
        await expect(
          f.runtime.start('c-workflow', {
            ...capturedStartOptions(),
            ...changes,
          }),
        ).rejects.toBe(fault);
      expect(read).not.toHaveBeenCalled();
      expect(
        (
          await f.runtime.start('c-workflow', {
            runId: 'unattributed',
            mutationEpoch: 2,
            inputData: { value: 'plain' },
          })
        ).status,
      ).toBe('success');
    } finally {
      read.mockRestore();
      f.sql.close();
    }
  });

  it.each([
    'stable',
    'alternating',
    'second-throw',
  ] as const)('captures each schedule dispatch primitive once before canonicalization: %s', async (mode) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const scheduleId = observedPrimitive('schedule-original', mode);
    const dispatchId = observedPrimitive('dispatch-original', mode);
    const pending = runtime.start('echo', {
      runId: 'capture-dispatch',
      inputData: { value: 'original' },
      scheduleDispatch: {
        get scheduleId() {
          return scheduleId();
        },
        get dispatchId() {
          return dispatchId();
        },
      },
    });
    await expect(pending).resolves.toMatchObject({ status: 'success' });
    expect(scheduleId).toHaveBeenCalledTimes(1);
    expect(dispatchId).toHaveBeenCalledTimes(1);
  });

  it('captures economic entries without invoking caller array methods', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const id = vi.fn(() => 'operation-original');
    const settlementState = vi.fn(() => 'settled');
    const operations = [
      {
        get id() {
          return id();
        },
        get settlementState() {
          return settlementState();
        },
      },
    ];
    const map = vi.fn(() => operations);
    Object.defineProperty(operations, 'map', { value: map });
    try {
      await expect(
        runtime.start('echo', {
          runId: 'caller-array',
          inputData: { value: 'original' },
          economicOperations: operations,
        }),
      ).resolves.toMatchObject({ status: 'success' });
      expect(id).toHaveBeenCalledTimes(1);
      expect(settlementState).toHaveBeenCalledTimes(1);
    } finally {
      expect(map).not.toHaveBeenCalled();
    }
  });

  it.each([
    'stable',
    'alternating',
    'second-throw',
  ] as const)('captures each economic operation primitive once before canonicalization: %s', async (mode) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const id = observedPrimitive('operation-original', mode);
    const settlementState = observedPrimitive('settled', mode);
    const pending = runtime.start('echo', {
      runId: 'capture-operations',
      inputData: { value: 'original' },
      economicOperations: [
        {
          get id() {
            return id();
          },
          get settlementState() {
            return settlementState();
          },
        },
      ],
    });
    await expect(pending).resolves.toMatchObject({ status: 'success' });
    expect(id).toHaveBeenCalledTimes(1);
    expect(settlementState).toHaveBeenCalledTimes(1);
  });
});

describe('RunnerRuntime host pubsub identity', () => {
  it('preserves the moved lifecycle error constructor identity', () => {
    expect(RunLifecycleBlockedError).toBe(LeafRunLifecycleBlockedError);
    expect(
      new LeafRunLifecycleBlockedError({
        code: 'DISPUTED_SETTLEMENT',
        message: 'blocked',
      }),
    ).toBeInstanceOf(RunLifecycleBlockedError);
  });
  it('threads the pubsub instance from init() through to runtime.pubsub', () => {
    // #given — a host builds ONE pubsub identity for its DO
    const pubsub = createHostPubSub();

    // #when — init() threads it (InitOptions.pubsub -> RunnerRuntimeOptions.pubsub)
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', pubsub, executionFence: 'none' },
    );

    // #then — the SAME instance is reachable. Delete the thread in init.ts and
    // this fails.
    expect(runtime.pubsub).toBe(pubsub);
  });

  it('leaves runtime.pubsub undefined when the host configures none (byte-identical)', () => {
    // #when — no pubsub passed
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );

    // #then — undefined, the polling-fallback posture
    expect(runtime.pubsub).toBeUndefined();
  });
});

describe('Runtime checked durable counters', () => {
  const MAX = Number.MAX_SAFE_INTEGER;
  const owner = { kind: 'human' as const, id: 'owner' };
  it('rejects an exhausted live cancellation before context publication or cancel invocation', async () => {
    const sql = openSqlite();
    const db = sqliteUnitDatabase(sql) as D1DatabaseBinding;
    const app = init({ DB: db });
    let entered = () => {};
    let release = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const enteredPromise = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let context: RequestContext | undefined;
    const workflow = app
      .createWorkflow({
        id: 'live-counter',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
      .then(
        app.createStep({
          id: 'held',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
          execute: async ({ requestContext }) => {
            context = requestContext;
            entered();
            await held;
            return {};
          },
        }),
      )
      .commit();
    const create = workflow.createRun.bind(workflow);
    const cancel = vi.fn();
    vi.spyOn(workflow, 'createRun').mockImplementation(async (...args) => {
      const run = await create(...args);
      const originalCancel = run.cancel.bind(run);
      vi.spyOn(run, 'cancel').mockImplementation(async () => {
        cancel();
        await originalCancel();
      });
      return run;
    });
    const running = app.runtime.start('live-counter', {
      runId: 'live-run',
      inputData: {},
      requestedBy: owner.id,
      requestedByKind: owner.kind,
    });
    try {
      await enteredPromise;
      const read = () =>
        sql.prepare('SELECT * FROM mastra_workflow_snapshot').get() as {
          snapshot: string;
        };
      const snapshot = JSON.parse(read().snapshot);
      snapshot.requestContext = {
        ...snapshot.requestContext,
        'flowsafe.runLifecycle': { version: 1, revision: MAX },
      };
      sql
        .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
        .run(JSON.stringify(snapshot));
      if (!context) throw new Error('live request context missing');
      const beforeContext = [...context.entries()];
      const before = read();
      const outcome = await app.runtime
        .cancelActiveExecution(
          'live-counter',
          'live-run',
          'cancelled',
          [owner],
          undefined,
          200,
        )
        .catch((error: unknown) => error);
      expect(read()).toEqual(before);
      expect([...context.entries()]).toEqual(beforeContext);
      expect(cancel).not.toHaveBeenCalled();
      expect(outcome).toMatchObject({
        message: 'run lifecycle revision cannot advance',
      });
    } finally {
      release();
      await running;
    }
  });
  async function counterFixture() {
    const sql = openSqlite();
    const db = sqliteUnitDatabase(sql) as D1DatabaseBinding;
    const provider = vi.fn(() => ({}));
    const prepareExecution = vi.fn(async () => undefined);
    const effects = vi.fn();
    function makeRuntime() {
      const app = init({ DB: db }, { requestContextForRun: provider });
      app
        .createWorkflow({
          id: 'counter',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
        })
        .then(
          app.createStep({
            id: 'gate',
            inputSchema: z.object({}),
            outputSchema: z.object({}),
            execute: async ({ resumeData, suspend }) => {
              if (!resumeData) return suspend({ reason: 'counter test' });
              effects();
              return {};
            },
          }),
        )
        .commit();
      return app.runtime;
    }
    await makeRuntime().start('counter', {
      runId: 'counter-run',
      inputData: {},
      requestedBy: owner.id,
      requestedByKind: owner.kind,
    });
    const read = () =>
      sql.prepare('SELECT * FROM mastra_workflow_snapshot').get() as {
        snapshot: string;
      };
    const seed = (
      edit: (state: {
        status: string;
        requestContext: Record<string, unknown> & {
          'flowsafe.runProvenance': { resumeCounts: Array<[string, number]> };
        };
      }) => void,
    ) => {
      const state = JSON.parse(read().snapshot);
      edit(state);
      sql
        .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
        .run(JSON.stringify(state));
    };
    const resume = {
      step: 'gate',
      resumeData: { go: true },
      requestedBy: owner.id,
      requestedByKind: owner.kind,
      prepareExecution,
    };
    return {
      sql,
      db,
      makeRuntime,
      read,
      seed,
      provider,
      prepareExecution,
      effects,
      resume,
    };
  }

  it.each([
    'cancel-intent',
    'terminate',
    'timeout',
    'cleanup',
    'resume-count',
    'resume-deadline',
    'resume-economic',
  ])('rejects exhausted %s before durable or execution effects', async (variant) => {
    const h = await counterFixture();
    h.seed((state) => {
      state.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: MAX,
        ...(variant === 'timeout' ? { deadlineAt: 100 } : {}),
      };
      if (variant === 'resume-count')
        state.requestContext['flowsafe.runProvenance'].resumeCounts = [
          ['gate', MAX],
        ];
      if (variant === 'cleanup') {
        state.status = 'cancelled';
        state.requestContext['flowsafe.runLifecycle'] = {
          version: 1,
          revision: MAX,
          terminal: {
            status: 'cancelled',
            error: { code: 'CANCELLED', message: 'run was cancelled' },
            transitionedAt: 100,
            replayPrincipals: [owner],
          },
        };
      }
    });
    const runtime = h.makeRuntime();
    await expect(
      runtime.authoritativeStatus('counter', 'counter-run'),
    ).resolves.toHaveProperty('runId', 'counter-run');
    h.provider.mockClear();
    const before = h.read();
    const prepare = vi.spyOn(h.db, 'prepare');
    const operation =
      variant === 'cancel-intent'
        ? runtime.cancelActiveExecution(
            'counter',
            'counter-run',
            'cancelled',
            [owner],
            undefined,
            200,
          )
        : variant === 'terminate'
          ? runtime.terminateAsPrincipal(
              'counter',
              'counter-run',
              owner,
              owner,
              200,
            )
          : variant === 'timeout'
            ? runtime.timeOutAsPrincipal(
                'counter',
                'counter-run',
                { expectedRevision: MAX, expectedDeadlineAt: 100 },
                owner,
                owner,
                200,
              )
            : variant === 'cleanup'
              ? runtime.completeTerminalCleanup(
                  'counter',
                  'counter-run',
                  MAX,
                  200,
                )
              : runtime.resume('counter', 'counter-run', {
                  ...h.resume,
                  ...(variant === 'resume-deadline'
                    ? { deadlineMs: 60_000 }
                    : {}),
                  ...(variant === 'resume-economic'
                    ? { economicOperations: [] }
                    : {}),
                });
    const outcome = await operation.catch((error: unknown) => error);
    expect(h.read()).toEqual(before);
    expect(
      prepare.mock.calls.filter(([sql]) =>
        /^(?:UPDATE|INSERT|DELETE|REPLACE)\b/i.test(sql.trim()),
      ),
    ).toEqual([]);
    expect(h.effects).not.toHaveBeenCalled();
    expect(h.provider).not.toHaveBeenCalled();
    expect(h.prepareExecution).not.toHaveBeenCalled();
    expect(outcome).toBeInstanceOf(Error);
    expect(outcome).toMatchObject({
      message:
        variant === 'resume-count'
          ? 'run resume count cannot advance'
          : 'run lifecycle revision cannot advance',
    });
    await expect(
      h.makeRuntime().authoritativeStatus('counter', 'counter-run'),
    ).resolves.toHaveProperty('runId', 'counter-run');
  });

  it.each([
    'resume-no-replacement',
    'matching-intent',
    'already-terminal',
    'completed-cleanup',
  ])('preserves maximum counters on %s nonincrementing paths', async (variant) => {
    const h = await counterFixture();
    h.seed((runState) => {
      const terminal = {
        status: 'cancelled',
        error: { code: 'CANCELLED', message: 'run was cancelled' },
        transitionedAt: 100,
        replayPrincipals: [owner],
        ...(variant === 'completed-cleanup' ? { cleanupCompletedAt: 0 } : {}),
      };
      runState.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: MAX,
        ...(variant === 'matching-intent'
          ? {
              transitionIntent: {
                status: 'cancelled',
                requestedAt: 100,
                replayPrincipals: [owner],
              },
            }
          : {}),
        ...(variant === 'already-terminal' || variant === 'completed-cleanup'
          ? { terminal }
          : {}),
      };
      runState.requestContext['flowsafe.runProvenance'].resumeCounts = [
        ['unselected', MAX],
      ];
      if (variant === 'already-terminal' || variant === 'completed-cleanup')
        runState.status = 'cancelled';
    });
    const runtime = h.makeRuntime();
    const before = h.read();
    if (variant === 'resume-no-replacement')
      await expect(
        runtime.resume('counter', 'counter-run', h.resume),
      ).resolves.toMatchObject({ status: 'success' });
    else if (variant === 'matching-intent')
      await expect(
        runtime.cancelActiveExecution(
          'counter',
          'counter-run',
          'cancelled',
          [owner],
          undefined,
          200,
        ),
      ).resolves.toBe(false);
    else if (variant === 'already-terminal')
      await runtime.terminateAsPrincipal(
        'counter',
        'counter-run',
        owner,
        owner,
        200,
      );
    else
      await runtime.completeTerminalCleanup('counter', 'counter-run', MAX, 200);
    const state = JSON.parse(h.read().snapshot);
    expect(
      parseRunLifecycle(state.requestContext['flowsafe.runLifecycle'])
        ?.revision,
    ).toBe(MAX);
    if (variant !== 'resume-no-replacement') expect(h.read()).toEqual(before);
    await expect(
      h.makeRuntime().authoritativeStatus('counter', 'counter-run'),
    ).resolves.toHaveProperty('runId', 'counter-run');
  });

  it.each([
    'cancel-intent',
    'terminate',
    'cleanup',
    'resume-count',
    'resume-revision',
  ])('advances MAX minus one %s exactly once without losing readability', async (variant) => {
    const h = await counterFixture();
    h.seed((runState) => {
      runState.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: MAX - 1,
      };
      if (variant === 'resume-count')
        runState.requestContext['flowsafe.runProvenance'].resumeCounts = [
          ['gate', MAX - 1],
        ];
      if (variant === 'cleanup') {
        runState.status = 'cancelled';
        runState.requestContext['flowsafe.runLifecycle'] = {
          version: 1,
          revision: MAX - 1,
          terminal: {
            status: 'cancelled',
            error: { code: 'CANCELLED', message: 'run was cancelled' },
            transitionedAt: 100,
            replayPrincipals: [owner],
          },
        };
      }
    });
    const runtime = h.makeRuntime();
    if (variant === 'cancel-intent')
      await runtime.cancelActiveExecution(
        'counter',
        'counter-run',
        'cancelled',
        [owner],
        undefined,
        200,
      );
    else if (variant === 'terminate')
      await runtime.terminateAsPrincipal(
        'counter',
        'counter-run',
        owner,
        owner,
        200,
      );
    else if (variant === 'cleanup')
      await runtime.completeTerminalCleanup(
        'counter',
        'counter-run',
        MAX - 1,
        200,
      );
    else
      await runtime.resume('counter', 'counter-run', {
        ...h.resume,
        ...(variant === 'resume-revision' ? { economicOperations: [] } : {}),
      });
    const state = JSON.parse(h.read().snapshot);
    if (variant === 'resume-count')
      expect(
        state.requestContext['flowsafe.runProvenance'].resumeCounts,
      ).toContainEqual(['gate', MAX]);
    else
      expect(
        parseRunLifecycle(state.requestContext['flowsafe.runLifecycle'])
          ?.revision,
      ).toBe(MAX);
    expect(state.requestContext['flowsafe.runProvenance'].version).toBe(2);
    await expect(
      h.makeRuntime().authoritativeStatus('counter', 'counter-run'),
    ).resolves.toHaveProperty('runId', 'counter-run');
  });
});

describe('RunnerRuntime', () => {
  it('passes initial workflow state through to core execution', async () => {
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const stateSchema = z.object({ seed: z.string() });
    const inspect = createStep({
      id: 'inspect-state',
      stateSchema,
      inputSchema: z.object({}),
      outputSchema: z.object({ seed: z.string() }),
      execute: async ({ state }) => ({ seed: state.seed }),
    });
    createWorkflow({
      id: 'stateful',
      stateSchema,
      inputSchema: z.object({}),
      outputSchema: z.object({ seed: z.string() }),
    })
      .then(inspect)
      .commit();

    await expect(
      runtime.start('stateful', {
        runId: 'scheduled-state',
        inputData: {},
        initialState: { seed: 'from-schedule' },
      }),
    ).resolves.toMatchObject({
      status: 'success',
      result: { seed: 'from-schedule' },
    });
  });

  it('rejects numeric run and start-attempt ids without RegExp coercion', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 123 as unknown as string,
        inputData: { value: 'x' },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    await expect(
      runtime.start('echo', {
        runId: 'run-1',
        inputData: { value: 'x' },
        attemptToken: 123 as unknown as string,
      }),
    ).rejects.toThrow('attemptToken is malformed');
  });

  it('accepts a requester id exactly at the principal-id bound', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const requestedBy = 'r'.repeat(200);

    await expect(
      runtime.start('echo', {
        runId: 'bounded-requester',
        inputData: { value: 'x' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({ requestedBy, requestedByKind: 'human' });
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u000aforged'],
  ])('rejects %s on start', async (_label, requestedBy) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'invalid-requester',
        inputData: { value: 'x' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).rejects.toThrow('requestedBy is malformed');
  });

  it.each([
    'human',
    'service',
    'agent',
    'system',
  ] as const)("accepts the '%s' requester kind", async (requestedByKind) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: `requester-kind-${requestedByKind}`,
        inputData: { value: 'x' },
        requestedBy: 'requester',
        requestedByKind,
      }),
    ).resolves.toMatchObject({ requestedByKind });
  });

  it.each([
    'operator',
    '',
    null,
    1,
  ])('rejects an invalid requester kind (%s)', async (requestedByKind) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'invalid-requester-kind',
        inputData: { value: 'x' },
        requestedBy: 'requester',
        requestedByKind: requestedByKind as never,
      }),
    ).rejects.toThrow('requestedByKind is malformed');
  });

  it.each([
    ['requestedBy without requestedByKind', { requestedBy: 'requester' }],
    ['requestedByKind without requestedBy', { requestedByKind: 'human' }],
  ])('rejects %s on start', async (_label, requester) => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.start('echo', {
        runId: 'half-attributed-start',
        inputData: { value: 'x' },
        ...requester,
      } as never),
    ).rejects.toThrow(
      'requestedBy and requestedByKind must be provided together',
    );
  });

  it.each([
    ['an overlong requester', 'r'.repeat(201)],
    ['an all-whitespace requester', ' '.repeat(200)],
    ['a control-bearing requester', 'reviewer\u007fforged'],
  ])('rejects %s on resume', async (_label, requestedBy) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'invalid-resume-requester',
      inputData: { topic: 'launch' },
      requestedBy: 'initiator',
      requestedByKind: 'human',
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
        requestedBy,
        requestedByKind: 'human',
      }),
    ).rejects.toThrow('requestedBy is malformed');
  });

  it.each([
    ['requestedBy without requestedByKind', { requestedBy: 'requester' }],
    ['requestedByKind without requestedBy', { requestedByKind: 'human' }],
  ])('rejects %s on resume', async (_label, requester) => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'half-attributed-resume',
      inputData: { topic: 'launch' },
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
        ...requester,
      } as never),
    ).rejects.toThrow(
      'requestedBy and requestedByKind must be provided together',
    );
  });

  it('inherits a complete stored requester pair when a resume supplies neither', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'inherited-requester-pair',
      inputData: { topic: 'launch' },
      requestedBy: 'initiator',
      requestedByKind: 'service',
    });

    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
      }),
    ).resolves.toMatchObject({
      requestedBy: 'initiator',
      requestedByKind: 'service',
    });
  });

  it('rejects a numeric recovery token without RegExp coercion', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());

    await expect(
      runtime.recoverStartAttempt(
        {
          tablePrefix: '',
          workflowId: 'echo',
          runId: 'run-1',
          startToken: 'S',
        },
        {
          attemptToken: 123 as unknown as string,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        },
      ),
    ).rejects.toThrow('start recovery authority is malformed');
  });

  it('runs a workflow to suspension and resumes it to success', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });

    // #then
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['approval']]);
    // suspendPayload is keyed by suspended step id
    expect(started.suspendPayload).toEqual({
      approval: { reason: 'human approval required' },
    });

    // #when
    const resumed = await runtime.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'alice' },
    });

    // #then
    expect(resumed.status).toBe('success');
    expect(resumed.result).toEqual({
      topic: 'launch',
      notes: 'notes:launch',
      approvedBy: 'alice',
    });
  });

  it('resumes in a fresh runtime sharing only storage — the restart simulation', async () => {
    // #given — a run suspended by one runtime
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    const started = await before.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });
    expect(started.status).toBe('suspended');

    // #when — a fresh runtime (fresh Mastra instance and run registry, as
    // after a Worker restart; only the storage handle is shared) resumes it
    const after = buildRuntime(storage).runtime;
    const observed = await after.status('demo-approval', started.runId);
    const resumed = await after.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'alice' },
    });

    // #then
    expect(observed).toMatchObject({ status: 'suspended' });
    expect(resumed.status).toBe('success');
    expect(resumed.result).toMatchObject({
      approvedBy: 'alice',
      topic: 'launch',
    });
  });

  it('executes the gated step exactly once under concurrent resume', async () => {
    // #given — a suspended approval run
    const { runtime, counters } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'race' },
    });
    expect(started.status).toBe('suspended');

    // #when — two racing approvals for the same run
    const outcomes = await Promise.allSettled([
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'alice' },
      }),
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'bob' },
      }),
    ]);

    // #then — exactly one wins; the loser gets RunNotSuspendedError; the
    // gated action ran once. This is the product's core guarantee.
    const fulfilled = outcomes.filter((o) => o.status === 'fulfilled');
    const rejected = outcomes.filter((o) => o.status === 'rejected');
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(
      RunNotSuspendedError,
    );
    expect(counters.approvalResumes).toBe(1);
  });

  it('executes a caller-keyed run exactly once under concurrent start', async () => {
    // #given
    const { runtime, counters } = buildRuntime(new InMemoryStore());

    // #when — two racing starts sharing a caller-supplied runId
    const outcomes = await Promise.allSettled([
      runtime.start('echo', { runId: 'shared', inputData: { value: 'a' } }),
      runtime.start('echo', { runId: 'shared', inputData: { value: 'b' } }),
    ]);

    // #then — one executes, the other is rejected as already existing
    expect(outcomes.filter((o) => o.status === 'fulfilled')).toHaveLength(1);
    const rejection = outcomes.find((o) => o.status === 'rejected');
    expect((rejection as PromiseRejectedResult).reason).toBeInstanceOf(
      RunAlreadyExistsError,
    );
    expect(counters.echoRuns).toBe(1);
  });

  it('rejects a sequential start that reuses an existing runId', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    await runtime.start('echo', { runId: 'r1', inputData: { value: 'x' } });

    // #when / #then
    await expect(
      runtime.start('echo', { runId: 'r1', inputData: { value: 'y' } }),
    ).rejects.toBeInstanceOf(RunAlreadyExistsError);
  });

  it('completes non-suspending workflows and rejects resuming them', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const done = await runtime.start('echo', {
      runId: crypto.randomUUID(),
      inputData: { value: 'hi' },
    });

    // #then
    expect(done.status).toBe('success');
    expect(done.result).toEqual({ value: 'hi' });
    await expect(
      runtime.resume('echo', done.runId, { resumeData: {} }),
    ).rejects.toBeInstanceOf(RunNotSuspendedError);
  });

  it('classifies resume-schema violations as InvalidRunRequestError and keeps the run resumable', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'validate' },
    });

    // #when / #then — bad resumeData is a client error, not a server fault
    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 123 },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);

    // #then — the run is still suspended and a valid resume completes it
    expect(await runtime.status('demo-approval', started.runId)).toMatchObject({
      status: 'suspended',
    });
    const resumed = await runtime.resume('demo-approval', started.runId, {
      step: 'approval',
      resumeData: { approvedBy: 'carol' },
    });
    expect(resumed.status).toBe('success');
  });

  it('refuses a resume whose legAbort is not an AbortController and keeps the run resumable', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'leg-abort' },
    });

    // #when / #then
    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'approval',
        resumeData: { approvedBy: 'carol' },
        legAbort: {} as AbortController,
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
    expect(await runtime.status('demo-approval', started.runId)).toMatchObject({
      status: 'suspended',
    });
  });

  it('classifies a resume targeting a non-suspended step as InvalidRunRequestError', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'wrong-step' },
    });

    // #when / #then
    await expect(
      runtime.resume('demo-approval', started.runId, {
        step: 'research',
        resumeData: { approvedBy: 'dave' },
      }),
    ).rejects.toBeInstanceOf(InvalidRunRequestError);
  });

  it('honors caller-provided runIds', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when
    const started = await runtime.start('echo', {
      runId: 'fixed-run-id',
      inputData: { value: 'x' },
    });

    // #then
    expect(started.runId).toBe('fixed-run-id');
  });

  it('throws UnknownWorkflowError for unregistered workflows', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    await expect(
      runtime.start('nope', { runId: 'r-unknown' }),
    ).rejects.toBeInstanceOf(UnknownWorkflowError);
  });

  it('returns null status for unknown runs', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    expect(await runtime.status('echo', 'missing-run')).toBeNull();
  });

  it('lists registered workflow ids', () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());

    // #when / #then
    expect(runtime.workflowIds().sort()).toEqual(['demo-approval', 'echo']);
  });

  it('rejects duplicate workflow ids at registration', () => {
    // #given
    const { createWorkflow } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    createWorkflow({
      id: 'wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });

    // #when / #then
    expect(() =>
      createWorkflow({
        id: 'wf',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/duplicate workflow id/);
  });

  it('rejects duplicate agent ids at registration', () => {
    // #given
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    runtime.registerAgent(runtimeAgent('writer'));

    // #when / #then
    expect(() => runtime.registerAgent(runtimeAgent('writer'))).toThrowError(
      /duplicate agent id/,
    );
  });

  it('exposes every registered agent to workflow execution', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const agents = [
      runtimeAgent('a'),
      runtimeAgent('b'),
      runtimeAgent('__proto__'),
      runtimeAgent('runtime-agent:__proto__'),
    ];
    for (const agent of agents) runtime.registerAgent(agent);
    const resolveAgents = createStep({
      id: 'resolve-agents',
      inputSchema: z.object({}),
      outputSchema: z.object({
        firstByKey: z.string(),
        secondById: z.string(),
        collisionById: z.string(),
        prefixedByKey: z.string(),
        workflowByKey: z.string(),
      }),
      execute: async ({ mastra }) => ({
        firstByKey: mastra.getAgent('a').id,
        secondById: mastra.getAgentById('b').id,
        collisionById: mastra.getAgentById('__proto__').id,
        prefixedByKey: mastra.getAgent('runtime-agent:__proto__').id,
        workflowByKey: mastra.getWorkflow('agent-resolution').id,
      }),
    });
    createWorkflow({
      id: 'agent-resolution',
      inputSchema: z.object({}),
      outputSchema: z.object({
        firstByKey: z.string(),
        secondById: z.string(),
        collisionById: z.string(),
        prefixedByKey: z.string(),
        workflowByKey: z.string(),
      }),
    })
      .then(resolveAgents)
      .commit();

    // #when
    const summary = await runtime.start('agent-resolution', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then
    expect(summary).toMatchObject({
      status: 'success',
      result: {
        firstByKey: 'a',
        secondById: 'b',
        collisionById: '__proto__',
        prefixedByKey: 'runtime-agent:__proto__',
        workflowByKey: 'agent-resolution',
      },
    });
  });

  it('passes the runtime pubsub to registered agents', async () => {
    // #given
    const pubsub = createHostPubSub();
    const publish = vi.spyOn(pubsub, 'publish');
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', pubsub, executionFence: 'none' },
    );
    const agent = runtimeAgent('writer');
    runtime.registerAgent(agent);
    const noop = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    createWorkflow({
      id: 'pubsub-agent',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(noop)
      .commit();
    await runtime.start('pubsub-agent', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    publish.mockClear();

    // #when
    const agentPubsub = agent.getPubSub();
    expect(agentPubsub).toBeDefined();
    await agentPubsub?.publish('runtime-agent-test', {
      type: 'runtime-agent-test',
      data: agent.id,
      runId: 'runtime-agent-test',
    });

    // #then
    expect(publish).toHaveBeenCalledOnce();
  });

  it.each([
    '__proto__',
    'constructor',
  ])("executes prototype-collision workflow id '%s'", async (workflowId) => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const noop = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({ resolved: z.string() }),
      execute: async ({ mastra }) => ({
        resolved: mastra.getWorkflowById(workflowId).id,
      }),
    });
    createWorkflow({
      id: workflowId,
      inputSchema: z.object({}),
      outputSchema: z.object({ resolved: z.string() }),
    })
      .then(noop)
      .commit();

    // #when
    const summary = await runtime.start(workflowId, {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then
    expect(summary).toMatchObject({
      status: 'success',
      result: { resolved: workflowId },
    });
  });

  it.each([
    'team:wf',
    'a/b',
    '.',
    '..',
    '',
  ])("rejects non-path-safe workflow id '%s' at registration", (id) => {
    // #given
    const { createWorkflow } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );

    // #when / #then — a ':' or '/' in the id would make the DO name join
    // (`${workflowId}:${runId}`) and the /runs/:workflowId/:runId path
    // ambiguous; fail at register(), before any run can be minted under it
    expect(() =>
      createWorkflow({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/must be URL-path-safe/);
  });

  it('accepts path-safe workflow ids at registration', () => {
    // #given
    const { createWorkflow, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );

    // #when — unreserved-character id, including '.'
    createWorkflow({
      id: 'demo-approval.v2_~ok',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    });

    // #then
    expect(runtime.workflowIds()).toContain('demo-approval.v2_~ok');
  });

  it('rejects registration after the first run', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const step = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    createWorkflow({
      id: 'wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(step)
      .commit();
    await runtime.start('wf', { runId: crypto.randomUUID(), inputData: {} });

    // #when / #then
    expect(() =>
      createWorkflow({
        id: 'late',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      }),
    ).toThrowError(/before the first run/);
    expect(() => runtime.registerAgent(runtimeAgent('late'))).toThrowError(
      /before the first run/,
    );
  });

  it('refuses a workflow another Mastra registered after the first run', async () => {
    // #given a workflow whose runtime built its Mastra on the first run
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const step = createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    const workflow = createWorkflow({
      id: 'wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(step)
      .commit();
    const runId = crypto.randomUUID();
    await expect(
      runtime.start('wf', { runId, inputData: {} }),
    ).resolves.toMatchObject({ status: 'success' });

    // #when another Mastra registers the same workflow object
    new Mastra({
      storage: new InMemoryStore(),
      logger: false,
      workflows: { wf: workflow },
    });

    // #then the runtime refuses it rather than read that Mastra's storage
    const refusal =
      "RunnerRuntime: workflow 'wf' is not registered on this runtime's Mastra";
    await expect(runtime.status('wf', runId)).rejects.toThrow(refusal);
    await expect(
      runtime.start('wf', { runId: crypto.randomUUID(), inputData: {} }),
    ).rejects.toThrow(refusal);
  });

  it('refuses a workflow object on the runtime whose Mastra no longer holds it', async () => {
    // #given one workflow object registered on two runtimes, run on the first
    const first = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const step = first.createStep({
      id: 'noop',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => ({}),
    });
    const workflow = first
      .createWorkflow({
        id: 'shared',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
      .then(step)
      .commit();
    const second = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    second.runtime.register(workflow);
    const runId = crypto.randomUUID();
    await expect(
      first.runtime.start('shared', { runId, inputData: {} }),
    ).resolves.toMatchObject({ status: 'success' });

    // #when the second runtime's first operation builds its Mastra, which
    // registers the object there
    await expect(second.runtime.status('shared', runId)).resolves.toBeNull();

    // #then the first runtime refuses the object rather than read the second
    // runtime's storage
    await expect(first.runtime.status('shared', runId)).rejects.toThrow(
      "RunnerRuntime: workflow 'shared' is not registered on this runtime's Mastra",
    );
  });
});

describe('RunnerRuntime ownership changes during operations', () => {
  const refusal = /is not registered on this runtime's Mastra/;

  function repoint(workflow: Parameters<RunnerRuntime['register']>[0]) {
    const storage = new InMemoryStore();
    const foreign = new Mastra({
      storage,
      logger: false,
      workflows: { [workflow.id]: workflow },
    });
    const getStorage = vi.spyOn(foreign, 'getStorage');
    const row = async (runId: string) =>
      (await storage.getStore('workflows'))?.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId,
      });
    return { getStorage, row };
  }

  async function expectRefusedWithoutForeignStorage(
    result: unknown,
    foreign: ReturnType<typeof repoint> | undefined,
    runId: string,
  ) {
    assert(foreign);
    expect(result).toBeInstanceOf(Error);
    expect((result as Error).message).toMatch(refusal);
    expect(foreign.getStorage).not.toHaveBeenCalled();
    expect(await foreign.row(runId)).toBeNull();
  }

  it('refuses start repointed after entry before storage capture', async () => {
    const f = await runtimeActivationFixture('custom');
    const runId = 'start-after-entry';
    try {
      const pending = f.runtime.start(f.workflow.id, f.options(runId));
      const foreign = repoint(f.workflow);
      const result = await pending.catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(result, foreign, runId);
    } finally {
      f.close();
    }
  });

  it('refuses resume repointed after entry before storage capture', async () => {
    const f = await runtimeActivationFixture('custom');
    const runId = 'resume-after-entry';
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(runId),
        inputData: { suspend: true },
      });
      const pending = f.runtime.resume(f.workflow.id, runId, {
        resumeData: { go: true },
      });
      const foreign = repoint(f.workflow);
      const result = await pending.catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(result, foreign, runId);
    } finally {
      f.close();
    }
  });

  it('refuses start recovery repointed after entry before storage capture', async () => {
    const f = await runtimeActivationFixture('fenced');
    try {
      await f.runtime.start(f.workflow.id, f.options());
      const state = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      assert(state?.storage === 'd1');
      const pending = f.runtime.recoverStartAttempt(state.execution, {
        attemptToken: 'H',
        isOwnerQuiescent: () => true,
        startLeg: 'unwound',
      });
      const foreign = repoint(f.workflow);
      const result = await pending.catch((error: unknown) => error);
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
      await expectRefusedWithoutForeignStorage(
        (result as Error & { cause?: Error }).cause,
        foreign,
        'activation-run',
      );
    } finally {
      f.close();
    }
  });

  it('refuses unfenced start repointed after storage capture before createRun', async () => {
    const f = await runtimeActivationFixture('custom');
    const runId = 'start-after-capture';
    let foreign: ReturnType<typeof repoint> | undefined;
    try {
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(runId),
          onPreparedStartIdentity: async () => {
            foreign = repoint(f.workflow);
          },
        })
        .catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(result, foreign, runId);
    } finally {
      f.close();
    }
  });

  it('refuses fenced start repointed after storage capture before createRun', async () => {
    const f = await runtimeActivationFixture('fenced');
    let foreign: ReturnType<typeof repoint> | undefined;
    try {
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          onPreparedStartIdentity: async () => {
            foreign = repoint(f.workflow);
          },
        })
        .catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(
        result,
        foreign,
        'activation-run',
      );
    } finally {
      f.close();
    }
  });

  it('refuses resume repointed after storage capture before createRun', async () => {
    const f = await runtimeActivationFixture('custom');
    const runId = 'resume-after-capture';
    let foreign: ReturnType<typeof repoint> | undefined;
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(runId),
        inputData: { suspend: true },
      });
      const result = await f.runtime
        .resume(f.workflow.id, runId, {
          resumeData: { go: true },
          prepareExecution: async () => {
            foreign = repoint(f.workflow);
          },
        })
        .catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(result, foreign, runId);
    } finally {
      f.close();
    }
  });

  it('refuses resume repointed during capture before its state read', async () => {
    const f = await runtimeActivationFixture('custom');
    const runId = 'resume-during-capture';
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(runId),
        inputData: { suspend: true },
      });
      const originalGetStore = f.storage.getStore.bind(f.storage);
      let foreign: ReturnType<typeof repoint> | undefined;
      vi.spyOn(f.storage, 'getStore').mockImplementationOnce((...args) => {
        foreign = repoint(f.workflow);
        return originalGetStore(...args);
      });
      const result = await f.runtime
        .resume(f.workflow.id, runId, {
          resumeData: { go: true },
        })
        .catch((error: unknown) => error);
      await expectRefusedWithoutForeignStorage(result, foreign, runId);
    } finally {
      f.close();
    }
  });
});

const ISO_8601 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

describe('RunnerRuntime.status projection', () => {
  it('persists a terminal status when a workflow retains only resume snapshots', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) =>
        resumeData?.approved
          ? inputData
          : suspend({ reason: 'approval required' }),
    });
    createWorkflow({
      id: 'resume-artifact-only',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending' || workflowStatus === 'suspended',
      },
    })
      .then(gate)
      .commit();
    const started = await runtime.start('resume-artifact-only', {
      runId: 'acme_terminal-status',
      inputData: { value: 'durable' },
    });

    const resumed = await runtime.resume(
      'resume-artifact-only',
      started.runId,
      {
        resumeData: { approved: true },
      },
    );
    const status = await runtime.status('resume-artifact-only', started.runId);

    expect(resumed).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
  });

  it('persists a terminal start when the engine omits terminal snapshots', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const echo = createStep({
      id: 'echo-once',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ inputData }) => inputData,
    });
    createWorkflow({
      id: 'start-artifact-only',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending',
      },
    })
      .then(echo)
      .commit();

    const done = await runtime.start('start-artifact-only', {
      runId: 'acme_terminal-start',
      inputData: { value: 'durable' },
    });
    const status = await runtime.status('start-artifact-only', done.runId);

    expect(done).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'durable' },
    });
  });

  it('projects suspended detail — paths, payload, timestamps — from the snapshot', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: crypto.randomUUID(),
      inputData: { topic: 'launch' },
    });
    expect(started.status).toBe('suspended');

    // #when
    const status = await runtime.status('demo-approval', started.runId);

    // #then
    expect(status).toMatchObject({
      status: 'suspended',
      suspended: [['approval']],
      suspendPayload: { approval: { reason: 'human approval required' } },
    });
    expect(status?.createdAt).toMatch(ISO_8601);
    expect(status?.updatedAt).toMatch(ISO_8601);
  });

  it('projects the result for a completed run', async () => {
    // #given
    const { runtime } = buildRuntime(new InMemoryStore());
    const done = await runtime.start('echo', {
      runId: crypto.randomUUID(),
      inputData: { value: 'hi' },
    });

    // #when
    const status = await runtime.status('echo', done.runId);

    // #then
    expect(status).toMatchObject({
      status: 'success',
      result: { value: 'hi' },
    });
  });

  it('projects the failure message for a failed run', async () => {
    // #given
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const boom = createStep({
      id: 'boom',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        throw new Error('boom');
      },
    });
    createWorkflow({
      id: 'failing',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(boom)
      .commit();
    const started = await runtime.start('failing', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('failed');

    // #when
    const status = await runtime.status('failing', started.runId);

    // #then
    expect(status?.status).toBe('failed');
    expect(status?.error).toContain('boom');
  });

  it('extracts the message from a thrown non-Error object', async () => {
    // #given — a step that throws a serialized-style { name, message, stack }
    // object, not an Error instance. This is the shape errorText() defends
    // against at an engine/persistence boundary; String() on it reads
    // '[object Object]', so a naive projection would lose the message.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const boom = createStep({
      id: 'boom-object',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async () => {
        throw {
          name: 'WeirdError',
          message: 'object-shaped failure',
          stack: 'x',
        };
      },
    });
    createWorkflow({
      id: 'obj-failing',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(boom)
      .commit();

    // #when
    const started = await runtime.start('obj-failing', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — both the run summary and the status projection surface the
    // message field, never '[object Object]'
    expect(started.status).toBe('failed');
    expect(started.error).toBe('object-shaped failure');
    expect((await runtime.status('obj-failing', started.runId))?.error).toBe(
      'object-shaped failure',
    );
  });

  it('projects every branch of a multi-step (parallel) suspension', async () => {
    // #given — two parallel steps that both suspend in the same run
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const makeGate = (id: string, reason: string) =>
      createStep({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        suspendSchema: z.object({ reason: z.string() }),
        resumeSchema: z.object({ go: z.boolean() }),
        execute: async ({ resumeData, suspend }) =>
          resumeData ? { ok: true } : suspend({ reason }),
      });
    createWorkflow({
      id: 'parallel-suspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .parallel([makeGate('gateA', 'A waits'), makeGate('gateB', 'B waits')])
      .commit();
    const started = await runtime.start('parallel-suspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');

    // #when
    const status = await runtime.status('parallel-suspend', started.runId);

    // #then — both suspended paths and both keyed suspend payloads project
    expect(status?.suspended).toHaveLength(2);
    expect(status?.suspended).toContainEqual(['gateA']);
    expect(status?.suspended).toContainEqual(['gateB']);
    expect(status?.suspendPayload).toEqual({
      gateA: { reason: 'A waits' },
      gateB: { reason: 'B waits' },
    });
  });
});

describe('RunnerRuntime run lifecycle', () => {
  const principal = { kind: 'human' as const, id: 'operator-1' };

  function heldRuntime(
    options: {
      suspendFirst?: boolean;
      storage?: InMemoryStore;
      cancelFailures?: number;
      cancelNoop?: boolean;
    } = {},
  ): {
    runtime: RunnerRuntime;
    entered: Promise<void>;
    release: () => void;
    completed: () => boolean;
  } {
    let enter!: () => void;
    let release!: () => void;
    let completed = false;
    const entered = new Promise<void>((resolve) => {
      enter = resolve;
    });
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const { createWorkflow, createStep, runtime } = init(
      { storage: options.storage ?? new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const step = createStep({
      id: 'held',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend, abortSignal }) => {
        if (options.suspendFirst && !resumeData) {
          return suspend({ reason: 'resume to enter held work' });
        }
        enter();
        await Promise.race([
          held,
          new Promise<never>((_resolve, reject) => {
            abortSignal.addEventListener(
              'abort',
              () => reject(new Error('held work aborted')),
              { once: true },
            );
          }),
        ]);
        completed = true;
        return { ok: true };
      },
    });
    const workflow = createWorkflow({
      id: 'held-workflow',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
    })
      .then(step)
      .commit();
    if ((options.cancelFailures ?? 0) > 0 || options.cancelNoop) {
      let remaining = options.cancelFailures ?? 0;
      const originalCreateRun = workflow.createRun.bind(workflow);
      Object.defineProperty(workflow, 'createRun', {
        configurable: true,
        value: async (...args: Parameters<typeof workflow.createRun>) => {
          const run = await originalCreateRun(...args);
          const originalCancel = run.cancel.bind(run);
          Object.defineProperty(run, 'cancel', {
            configurable: true,
            value: async () => {
              if (options.cancelNoop) return;
              if (remaining > 0) {
                remaining -= 1;
                throw new Error('injected cancel failure');
              }
              await originalCancel();
            },
          });
          return run;
        },
      });
    }
    return { runtime, entered, release, completed: () => completed };
  }

  it('keeps ordinary start and resume snapshots lifecycle-free', async () => {
    const storage = new InMemoryStore();
    const { runtime } = buildRuntime(storage);
    const started = await runtime.start('demo-approval', {
      runId: 'lazy-lifecycle',
      inputData: { topic: 'plain' },
    });
    expect(started.status).toBe('suspended');

    const workflows = await storage.getStore('workflows');
    const before = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: started.runId,
    });
    expect(before?.requestContext).not.toHaveProperty('flowsafe.runLifecycle');

    await runtime.resume('demo-approval', started.runId, {
      resumeData: { approvedBy: 'reviewer-1' },
    });
    const after = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: started.runId,
    });
    expect(after?.requestContext).not.toHaveProperty('flowsafe.runLifecycle');
  });

  it('rejects a provider-forged lifecycle projection on start and resume', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      now.mockReturnValue(40_000);
      const storage = new InMemoryStore();
      const { createWorkflow, createStep, runtime } = init(
        { storage },
        {
          startIdempotency: 'none',
          executionFence: 'none',
          requestContextForRun: () => ({
            'flowsafe.runLifecycle': {
              version: 1,
              revision: 999,
              deadlineAt: 1,
              economicOperations: [
                { id: 'forged', settlementState: 'disputed' },
              ],
              terminal: {
                status: 'cancelled',
                transitionedAt: 1,
                replayPrincipals: [{ kind: 'human', id: 'forged' }],
                error: { code: 'CANCELLED', message: 'forged' },
              },
            },
          }),
        },
      );
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ resumeData, suspend }) =>
          resumeData ? { ok: true } : suspend({ reason: 'wait' }),
      });
      createWorkflow({
        id: 'provider-lifecycle-fence',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
      })
        .then(gate)
        .commit();

      const started = await runtime.start('provider-lifecycle-fence', {
        runId: 'provider-lifecycle-fence-run',
        inputData: {},
        deadlineMs: 100,
      });
      expect(started).toMatchObject({
        status: 'suspended',
        deadlineAt: 40_100,
      });
      now.mockReturnValue(40_050);
      const resumed = await runtime.resume(
        'provider-lifecycle-fence',
        started.runId,
        { resumeData: { approved: true }, deadlineMs: 1_000 },
      );
      expect(resumed).toMatchObject({
        status: 'success',
        deadlineAt: 41_050,
      });
      const workflows = await storage.getStore('workflows');
      const snapshot = await workflows?.loadWorkflowSnapshot({
        workflowName: 'provider-lifecycle-fence',
        runId: started.runId,
      });
      expect(snapshot?.requestContext?.['flowsafe.runLifecycle']).toMatchObject(
        {
          revision: 2,
          deadlineAt: 41_050,
        },
      );
      expect(
        snapshot?.requestContext?.['flowsafe.runLifecycle'],
      ).not.toHaveProperty('terminal');
      expect(
        snapshot?.requestContext?.['flowsafe.runLifecycle'],
      ).not.toHaveProperty('economicOperations');
    } finally {
      now.mockRestore();
    }
  });

  it('terminates a suspension exactly once with the stable structured shape', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'terminate-suspended',
      inputData: { topic: 'cancel' },
      requestedBy: principal.id,
      requestedByKind: principal.kind,
      deadlineMs: 60_000,
    });

    const first = await runtime.terminateAsPrincipal(
      'demo-approval',
      started.runId,
      principal,
      principal,
    );
    const second = await runtime.terminateAsPrincipal(
      'demo-approval',
      started.runId,
      principal,
      principal,
    );

    expect(first.transitioned).toBe(true);
    expect(second.transitioned).toBe(false);
    expect(second.summary).toEqual(first.summary);
    expect(first.summary).toMatchObject({
      runId: 'terminate-suspended',
      status: 'cancelled',
      requestedBy: principal.id,
      requestedByKind: principal.kind,
      deadlineAt: expect.any(Number),
      errorEnvelope: { code: 'CANCELLED', message: 'run was cancelled' },
    });
    expect(first.summary).not.toHaveProperty('error');
  });

  it('refuses a persisted disputed settlement before cancellation', async () => {
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'disputed-run',
      inputData: { topic: 'held' },
      economicOperations: [{ id: 'charge-1', settlementState: 'disputed' }],
    });

    await expect(
      runtime.cancelActiveExecution(
        'demo-approval',
        started.runId,
        'cancelled',
        [principal],
      ),
    ).rejects.toMatchObject({
      reason: {
        code: 'DISPUTED_SETTLEMENT',
        message:
          'run termination is blocked while an economic operation is disputed',
      },
    });
    await expect(
      runtime.terminateAsPrincipal(
        'demo-approval',
        started.runId,
        principal,
        principal,
      ),
    ).rejects.toMatchObject({
      reason: { code: 'DISPUTED_SETTLEMENT' },
    });
    await expect(
      runtime.status('demo-approval', started.runId),
    ).resolves.toMatchObject({ status: 'suspended' });
  });

  it('aborts a live running step before persisting the cancelled terminal', async () => {
    const { runtime, entered, completed } = heldRuntime();
    const starting = runtime.start('held-workflow', {
      runId: 'held-cancel',
      inputData: {},
      requestedBy: principal.id,
      requestedByKind: principal.kind,
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    await expect(starting).resolves.toMatchObject({ status: 'canceled' });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'held-cancel',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled', errorEnvelope: { code: 'CANCELLED' } },
    });
    expect(completed()).toBe(false);
    await expect(
      runtime.status('held-workflow', 'held-cancel'),
    ).resolves.toMatchObject({ status: 'cancelled' });
  });

  it('retries a live abort when the durable intent survived a transient cancel failure', async () => {
    const { runtime, entered, completed } = heldRuntime({
      cancelFailures: 1,
    });
    const starting = runtime.start('held-workflow', {
      runId: 'held-cancel-retry',
      inputData: {},
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel-retry',
        'cancelled',
        [principal],
      ),
    ).rejects.toThrow('injected cancel failure');
    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-cancel-retry',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    await expect(starting).resolves.toMatchObject({ status: 'canceled' });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'held-cancel-retry',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      summary: { status: 'cancelled' },
      transitioned: true,
    });
    expect(completed()).toBe(false);
  });

  it('does not abort a live running step initialized with a disputed settlement', async () => {
    const { runtime, entered, release, completed } = heldRuntime();
    const starting = runtime.start('held-workflow', {
      runId: 'held-disputed',
      inputData: {},
      economicOperations: [{ id: 'charge-live', settlementState: 'disputed' }],
    });
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'held-disputed',
        'cancelled',
        [principal],
      ),
    ).rejects.toMatchObject({
      reason: { code: 'DISPUTED_SETTLEMENT' },
    });
    release();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    expect(completed()).toBe(true);
  });

  it('extends a persisted deadline on resume and times out only once', async () => {
    const now = vi.spyOn(Date, 'now');
    now.mockReturnValue(10_000);
    const { runtime } = buildRuntime(new InMemoryStore());
    const started = await runtime.start('demo-approval', {
      runId: 'deadline-resume',
      inputData: { topic: 'deadline' },
      deadlineMs: 100,
    });
    expect(started.deadlineAt).toBe(10_100);

    now.mockReturnValue(10_050);
    const resumed = await runtime.resume('demo-approval', started.runId, {
      resumeData: { approvedBy: 'reviewer-1' },
      deadlineMs: 1_000,
    });
    expect(resumed.deadlineAt).toBe(11_050);

    const timeoutRun = await runtime.start('demo-approval', {
      runId: 'deadline-once',
      inputData: { topic: 'timeout' },
      deadlineMs: 0,
    });
    const first = await runtime.timeOut(
      'demo-approval',
      timeoutRun.runId,
      { expectedRevision: 1, expectedDeadlineAt: 10_050 },
      10_050,
    );
    const second = await runtime.timeOut(
      'demo-approval',
      timeoutRun.runId,
      { expectedRevision: 1, expectedDeadlineAt: 10_050 },
      10_051,
    );
    expect(first.transitioned).toBe(true);
    expect(second.transitioned).toBe(false);
    expect(second.summary).toEqual(first.summary);
    expect(first.summary).toMatchObject({
      status: 'timed_out',
      errorEnvelope: { code: 'TIMED_OUT', message: 'run deadline expired' },
    });
    now.mockRestore();
  });

  it('rejects a stale deadline CAS while an extended resume leg is active', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      now.mockReturnValue(20_000);
      const { runtime, entered, release, completed } = heldRuntime({
        suspendFirst: true,
      });
      const started = await runtime.start('held-workflow', {
        runId: 'held-resume-extension',
        inputData: {},
        deadlineMs: 100,
      });
      expect(started).toMatchObject({
        status: 'suspended',
        deadlineAt: 20_100,
      });

      now.mockReturnValue(20_050);
      const resuming = runtime.resume(
        'held-workflow',
        'held-resume-extension',
        { resumeData: { approved: true }, deadlineMs: 1_000 },
      );
      await entered;
      await expect(
        runtime.cancelActiveExecution(
          'held-workflow',
          'held-resume-extension',
          'timed_out',
          [principal],
          { expectedRevision: 1, expectedDeadlineAt: 20_100 },
          20_100,
        ),
      ).resolves.toBe(false);

      release();
      await expect(resuming).resolves.toMatchObject({
        status: 'success',
        deadlineAt: 21_050,
      });
      expect(completed()).toBe(true);
    } finally {
      now.mockRestore();
    }
  });

  it('preserves an active resume extension and cancellation intent through fresh-runtime recovery', async () => {
    const now = vi.spyOn(Date, 'now');
    try {
      const storage = new InMemoryStore();
      now.mockReturnValue(30_000);
      const { runtime, entered } = heldRuntime({
        suspendFirst: true,
        storage,
      });
      await runtime.start('held-workflow', {
        runId: 'held-resume-cancel-recovery',
        inputData: {},
        deadlineMs: 100,
      });

      now.mockReturnValue(30_050);
      const resuming = runtime.resume(
        'held-workflow',
        'held-resume-cancel-recovery',
        {
          resumeData: { approved: true },
          deadlineMs: 1_000,
          economicOperations: [
            { id: 'charge-resume', settlementState: 'settled' },
          ],
        },
      );
      await entered;
      await expect(
        runtime.cancelActiveExecution(
          'held-workflow',
          'held-resume-cancel-recovery',
          'cancelled',
          [principal],
        ),
      ).resolves.toBe(true);
      await expect(resuming).resolves.toMatchObject({ status: 'canceled' });

      const workflows = await storage.getStore('workflows');
      const precursor = await workflows?.loadWorkflowSnapshot({
        workflowName: 'held-workflow',
        runId: 'held-resume-cancel-recovery',
      });
      expect(
        precursor?.requestContext?.['flowsafe.runLifecycle'],
      ).toMatchObject({
        revision: 3,
        deadlineAt: 31_050,
        economicOperations: [
          { id: 'charge-resume', settlementState: 'settled' },
        ],
        transitionIntent: { status: 'cancelled' },
      });

      const fresh = heldRuntime({ suspendFirst: true, storage }).runtime;
      await expect(
        fresh.terminateAsPrincipal(
          'held-workflow',
          'held-resume-cancel-recovery',
          principal,
          principal,
        ),
      ).resolves.toMatchObject({
        transitioned: true,
        summary: {
          status: 'cancelled',
          deadlineAt: 31_050,
          errorEnvelope: { code: 'CANCELLED' },
        },
      });
      const terminal = await workflows?.loadWorkflowSnapshot({
        workflowName: 'held-workflow',
        runId: 'held-resume-cancel-recovery',
      });
      expect(terminal?.requestContext?.['flowsafe.runLifecycle']).toMatchObject(
        {
          economicOperations: [
            { id: 'charge-resume', settlementState: 'settled' },
          ],
          terminal: { status: 'cancelled' },
        },
      );
    } finally {
      now.mockRestore();
    }
  });

  it('fences a resume that is still preparing when termination begins', async () => {
    const storage = new InMemoryStore();
    const { runtime, completed } = heldRuntime({
      suspendFirst: true,
      storage,
    });
    await runtime.start('held-workflow', {
      runId: 'resume-preparation-terminate',
      inputData: {},
      deadlineMs: 100,
    });
    let preparationEntered!: () => void;
    let releasePreparation!: () => void;
    const entered = new Promise<void>((resolve) => {
      preparationEntered = resolve;
    });
    const preparation = new Promise<void>((resolve) => {
      releasePreparation = resolve;
    });

    const resuming = runtime.resume(
      'held-workflow',
      'resume-preparation-terminate',
      {
        resumeData: { approved: true },
        deadlineMs: 1_000,
        prepareExecution: async () => {
          preparationEntered();
          await preparation;
        },
      },
    );
    await entered;

    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'resume-preparation-terminate',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(false);
    releasePreparation();
    await expect(resuming).rejects.toMatchObject({
      name: 'RunTerminalConflictError',
    });
    await expect(
      runtime.terminateAsPrincipal(
        'held-workflow',
        'resume-preparation-terminate',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        deadlineAt: expect.any(Number),
      },
    });
    expect(completed()).toBe(false);
  });

  it('re-drives a persisted core-canceled precursor after runtime eviction', async () => {
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    await before.start('demo-approval', {
      runId: 'canceled-wedge',
      inputData: { topic: 'wedge' },
      deadlineMs: 1_000,
    });
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'canceled-wedge',
    });
    if (!workflows || !snapshot) throw new Error('snapshot missing');
    await workflows.persistWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'canceled-wedge',
      snapshot: {
        ...snapshot,
        status: 'canceled',
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runLifecycle': {
            version: 1,
            revision: 2,
            deadlineAt: snapshot.requestContext?.['flowsafe.runLifecycle']
              ? (
                  snapshot.requestContext['flowsafe.runLifecycle'] as {
                    deadlineAt: number;
                  }
                ).deadlineAt
              : 0,
            transitionIntent: {
              status: 'cancelled',
              requestedAt: Date.now(),
              replayPrincipals: [principal],
            },
          },
        },
      },
    });

    const after = buildRuntime(storage).runtime;
    await expect(
      after.terminateAsPrincipal(
        'demo-approval',
        'canceled-wedge',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        errorEnvelope: { code: 'CANCELLED' },
      },
    });
  });

  it('re-drives a persisted cancellation intent after core writes late success', async () => {
    const storage = new InMemoryStore();
    const before = buildRuntime(storage).runtime;
    await before.start('demo-approval', {
      runId: 'success-after-cancel-intent',
      inputData: { topic: 'late-success' },
      deadlineMs: 1_000,
    });
    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'success-after-cancel-intent',
    });
    if (!workflows || !snapshot) throw new Error('snapshot missing');
    const lifecycle = snapshot.requestContext?.[
      'flowsafe.runLifecycle'
    ] as Record<string, unknown>;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'demo-approval',
      runId: 'success-after-cancel-intent',
      snapshot: {
        ...snapshot,
        status: 'success',
        result: { late: true },
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runLifecycle': {
            ...lifecycle,
            revision: 2,
            transitionIntent: {
              status: 'cancelled',
              requestedAt: Date.now(),
              replayPrincipals: [principal],
            },
          },
        },
      },
    });

    const after = buildRuntime(storage).runtime;
    await expect(
      after.terminateAsPrincipal(
        'demo-approval',
        'success-after-cancel-intent',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: {
        status: 'cancelled',
        errorEnvelope: { code: 'CANCELLED' },
      },
    });
    await expect(
      after.status('demo-approval', 'success-after-cancel-intent'),
    ).resolves.toMatchObject({
      status: 'cancelled',
      errorEnvelope: { code: 'CANCELLED' },
    });
  });

  it('repairs an intent dropped by one stale late-success snapshot write', async () => {
    const storage = new InMemoryStore();
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows storage missing');
    const originalPersist = workflows.persistWorkflowSnapshot.bind(workflows);
    let droppedIntent = false;
    Object.defineProperty(workflows, 'persistWorkflowSnapshot', {
      configurable: true,
      value: async (
        input: Parameters<typeof workflows.persistWorkflowSnapshot>[0],
      ) => {
        const lifecycle = input.snapshot.requestContext?.[
          'flowsafe.runLifecycle'
        ] as Record<string, unknown> | undefined;
        if (
          !droppedIntent &&
          input.snapshot.status === 'success' &&
          lifecycle?.transitionIntent
        ) {
          droppedIntent = true;
          const { transitionIntent: _intent, ...staleLifecycle } = lifecycle;
          return originalPersist({
            ...input,
            snapshot: {
              ...input.snapshot,
              requestContext: {
                ...input.snapshot.requestContext,
                'flowsafe.runLifecycle': staleLifecycle,
              },
            },
          });
        }
        return originalPersist(input);
      },
    });
    const { runtime, entered, release } = heldRuntime({
      storage,
      cancelNoop: true,
    });
    const starting = runtime.start('held-workflow', {
      runId: 'stale-success-intent-repair',
      inputData: {},
    });
    await entered;
    await expect(
      runtime.cancelActiveExecution(
        'held-workflow',
        'stale-success-intent-repair',
        'cancelled',
        [principal],
      ),
    ).resolves.toBe(true);
    release();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    expect(droppedIntent).toBe(true);

    const repaired = await workflows.loadWorkflowSnapshot({
      workflowName: 'held-workflow',
      runId: 'stale-success-intent-repair',
    });
    expect(repaired?.requestContext?.['flowsafe.runLifecycle']).toMatchObject({
      transitionIntent: { status: 'cancelled' },
    });

    const fresh = heldRuntime({ storage }).runtime;
    await expect(
      fresh.terminateAsPrincipal(
        'held-workflow',
        'stale-success-intent-repair',
        principal,
        principal,
      ),
    ).resolves.toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled' },
    });
  });

  it('serializes terminal reconciliation with a concurrent cancellation preflight', async () => {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const step = createStep({
      id: 'finish',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      execute: async () => ({ ok: true }),
    });
    const workflow = createWorkflow({
      id: 'reconcile-lock',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending',
      },
    })
      .then(step)
      .commit();
    let coreReturned = false;
    const originalCreateRun = workflow.createRun.bind(workflow);
    Object.defineProperty(workflow, 'createRun', {
      configurable: true,
      value: async (...args: Parameters<typeof workflow.createRun>) => {
        const run = await originalCreateRun(...args);
        const originalStart = run.start.bind(run);
        Object.defineProperty(run, 'start', {
          configurable: true,
          value: async (...startArgs: Parameters<typeof run.start>) => {
            const result = await originalStart(...startArgs);
            coreReturned = true;
            return result;
          },
        });
        return run;
      },
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows storage missing');
    const originalLoad = workflows.loadWorkflowSnapshot.bind(workflows);
    let reconciliationLoaded!: () => void;
    let releaseReconciliation!: () => void;
    const loaded = new Promise<void>((resolve) => {
      reconciliationLoaded = resolve;
    });
    const held = new Promise<void>((resolve) => {
      releaseReconciliation = resolve;
    });
    let blocked = false;
    vi.spyOn(workflows, 'loadWorkflowSnapshot').mockImplementation(
      async (input) => {
        const snapshot = await originalLoad(input);
        if (
          coreReturned &&
          !blocked &&
          input.workflowName === 'reconcile-lock' &&
          input.runId === 'reconcile-race'
        ) {
          blocked = true;
          reconciliationLoaded();
          await held;
        }
        return snapshot;
      },
    );

    const starting = runtime.start('reconcile-lock', {
      runId: 'reconcile-race',
      inputData: {},
    });
    await loaded;
    let cancellationSettled = false;
    const cancellation = runtime
      .cancelActiveExecution('reconcile-lock', 'reconcile-race', 'cancelled', [
        principal,
      ])
      .finally(() => {
        cancellationSettled = true;
      });
    await Promise.resolve();
    expect(cancellationSettled).toBe(false);

    releaseReconciliation();
    await expect(starting).resolves.toMatchObject({ status: 'success' });
    await expect(cancellation).rejects.toMatchObject({
      name: 'RunTerminalConflictError',
    });
    await expect(
      runtime.status('reconcile-lock', 'reconcile-race'),
    ).resolves.toMatchObject({ status: 'success' });
  });
});

describe('RunnerRuntime requestContextForRun', () => {
  interface Observation {
    leg: 'start' | 'resume';
    a: unknown;
    b: unknown;
  }

  function buildContextProbe(
    provider: RequestContextProvider,
    storage: MastraCompositeStore = new InMemoryStore(),
  ): {
    runtime: RunnerRuntime;
    seen: Observation[];
  } {
    const seen: Observation[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        startIdempotency: 'none',
        requestContextForRun: provider,
        executionFence: 'none',
      },
    );
    const inspect = createConnector({
      id: 'inspect-context',
      description: 'Inspect application context at the connector boundary',
      inputSchema: z.object({ leg: z.enum(['start', 'resume']) }),
      outputSchema: z.object({}),
      permissions: { sideEffect: 'read' },
      execute: async ({ leg }, { requestContext }) => {
        assert(requestContext);
        seen.push({
          leg,
          a: requestContext.get('test.a'),
          b: requestContext.get('test.b'),
        });
        return {};
      },
    });
    const first = createStep({
      id: 'first',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) =>
        invokeConnector(inspect, { leg: 'start' as const }, { requestContext }),
    });
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend, requestContext }) => {
        if (!resumeData) return suspend({ reason: 'wait' });
        return invokeConnector(
          inspect,
          { leg: 'resume' as const },
          { requestContext },
        );
      },
    });
    createWorkflow({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(first)
      .then(gate)
      .commit();
    return { runtime, seen };
  }

  it('merges stored application context below provider and Runtime authority in a connector', async () => {
    const seen: Record<string, unknown> = {};
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        startIdempotency: 'none',
        requestContextForRun: () => ({
          'test.a': 'provider',
          'breakwater.actor': { id: 'trusted-operator', role: 'operator' },
          threadId: 'trusted-thread',
          'breakwater.workflowScope': 'provider-forged-scope',
          runId: 'provider-forged-run',
        }),
        executionFence: 'none',
      },
    );
    const connector = createConnector({
      id: 'inspect-application-context',
      description: 'Inspect stored context and execution authority',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      permissions: { sideEffect: 'read' },
      execute: async (_input, { requestContext }) => {
        assert(requestContext);
        seen.a = requestContext.get('test.a');
        seen.b = requestContext.get('test.b');
        seen.workflowScope = requestContext.get('breakwater.workflowScope');
        seen.actor = requestContext.get('breakwater.actor');
        seen.threadId = requestContext.get('threadId');
        seen.runId = requestContext.get('runId');
        seen.execution = requestContext.get('breakwater.connectorExecution');
        seen.isolation = requestContext.get('breakwater.isolationScope');
        seen.grants = requestContext.get('breakwater.connectorGrants');
        seen.customCapability = requestContext.get('breakwater.customGrant');
        return {};
      },
    });
    const inspect = createStep({
      id: 'inspect-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) =>
        invokeConnector(connector, {}, { requestContext }),
    });
    createWorkflow({
      id: 'application-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(inspect)
      .commit();

    const result = await runtime.start('application-context', {
      runId: 'application-context-run',
      inputData: {},
      storedRequestContext: {
        'test.a': 'stored',
        'test.b': 'stored-only',
        'breakwater.workflowScope': 'forged',
        'breakwater.actor': { id: 'forged-admin', role: 'admin' },
        'breakwater.isolationScope': 'forged-isolation',
        'breakwater.connectorGrants': ['forged-grant'],
        'breakwater.customGrant': 'forged-capability',
        'breakwater.connectorExecution': { kind: 'resume' },
        threadId: 'forged-thread',
        runId: 'forged-run',
      },
    });

    expect(result.status).toBe('success');
    expect(seen).toEqual({
      a: 'provider',
      b: 'stored-only',
      workflowScope: 'application-context',
      actor: { id: 'trusted-operator', role: 'operator' },
      threadId: 'trusted-thread',
      runId: 'application-context-run',
      execution: {
        kind: 'start',
        workflowId: 'application-context',
        runId: 'application-context-run',
      },
      isolation: undefined,
      grants: undefined,
      customCapability: undefined,
    });
  });

  it('restores stored application context through a fresh Runtime and D1 storage adapter', async () => {
    const binding = sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding;
    const provider = vi.fn<RequestContextProvider>(
      (_workflowId, _runId, leg) => ({
        'test.a': `provider-${leg.kind}`,
      }),
    );
    const stored = { workspaceId: 'workspace-1', tags: ['persisted', 'api'] };
    const before = buildContextProbe(provider, createD1Storage({ binding }));
    const started = await before.runtime.start('probe', {
      runId: 'stored-context-resume',
      inputData: {},
      storedRequestContext: { 'test.a': 'stored', 'test.b': stored },
    });
    expect(started.status).toBe('suspended');
    expect(before.seen).toEqual([
      { leg: 'start', a: 'provider-start', b: stored },
    ]);

    const after = buildContextProbe(provider, createD1Storage({ binding }));
    const resumed = await after.runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    expect(resumed.status).toBe('success');
    expect(after.seen).toEqual([
      { leg: 'resume', a: 'provider-resume', b: stored },
    ]);
    expect(
      provider.mock.calls.map(([_workflowId, _runId, leg]) => leg.kind),
    ).toEqual(['start', 'resume']);
  });

  it.each([
    'stored-grants',
    'provider-refresh',
    'provider-revocation',
  ] as const)('enforces connector capability provenance across fresh Runtime legs: %s', async (mode) => {
    const storage = new InMemoryStore();
    const seen: Record<string, unknown>[] = [];
    const storedGrant = {
      scope: 'run',
      connectorId: 'context-writer',
      workflowId: 'context-grants',
      runId: 'context-grants-run',
    };
    const provider = vi.fn<RequestContextProvider>(
      (workflowId, runId, leg) => ({
        'test.source': `provider-${leg.kind}`,
        ...(mode === 'stored-grants'
          ? {}
          : {
              'breakwater.connectorGrants':
                leg.kind === 'resume' && mode === 'provider-revocation'
                  ? []
                  : [
                      leg.kind === 'start'
                        ? storedGrant
                        : {
                            scope: 'suspension',
                            connectorId: 'context-writer',
                            workflowId,
                            runId,
                            suspension: {
                              stepPath: leg.step,
                              suspendedAt: leg.suspendedAt,
                            },
                          },
                    ],
            }),
      }),
    );
    const build = () => {
      const app = init(
        { storage },
        {
          startIdempotency: 'none',
          executionFence: 'none',
          requestContextForRun: provider,
        },
      );
      const writer = createConnector({
        id: 'context-writer',
        description: 'Exercise application context and provider-derived grants',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        permissions: { sideEffect: 'write', requiresApproval: true },
        execute: async (_input, { requestContext }) => {
          assert(requestContext);
          seen.push({
            source: requestContext.get('test.source'),
            attribution: requestContext.get('app.attribution'),
            mutationEpoch: requestContext.get('mutationEpoch'),
            startToken: requestContext.get('startToken'),
            grants: requestContext.get('breakwater.connectorGrants'),
          });
          return {};
        },
      });
      app
        .createWorkflow({
          id: 'context-grants',
          inputSchema: z.object({}),
          outputSchema: z.object({}),
        })
        .then(
          app.createStep({
            id: 'first',
            inputSchema: z.object({}),
            outputSchema: z.object({}),
            execute: async ({ requestContext }) =>
              invokeConnector(writer, {}, { requestContext }),
          }),
        )
        .then(
          app.createStep({
            id: 'gate',
            inputSchema: z.object({}),
            outputSchema: z.object({}),
            suspendSchema: z.object({ reason: z.string() }),
            resumeSchema: z.object({ go: z.boolean() }),
            execute: async ({ resumeData, suspend, requestContext }) => {
              if (!resumeData) return suspend({ reason: 'wait' });
              return invokeConnector(writer, {}, { requestContext });
            },
          }),
        )
        .commit();
      return app.runtime;
    };
    const before = build();
    const started = await before.start('context-grants', {
      runId: 'context-grants-run',
      inputData: {},
      requestedBy: 'trusted-requester',
      requestedByKind: 'human',
      storedRequestContext: {
        'test.source': 'stored',
        'app.attribution': { workspaceId: 'workspace-1' },
        mutationEpoch: 'application-value',
        startToken: 'application-token',
        connectorGrants: [storedGrant],
        approved: true,
        'breakwater.connectorGrants': [storedGrant],
        'flowsafe.runProvenance': { requestedBy: 'forged-requester' },
      },
    });
    expect(started.requestedBy).toBe('trusted-requester');
    if (mode === 'stored-grants') {
      expect(started.status).toBe('failed');
      expect(started.error).toContain(
        'approval required and no matching structured grant was found',
      );
      expect(seen).toEqual([]);
      expect(provider).toHaveBeenCalledOnce();
      return;
    }
    expect(started.status).toBe('suspended');
    expect(seen).toEqual([
      {
        source: 'provider-start',
        attribution: { workspaceId: 'workspace-1' },
        mutationEpoch: 'application-value',
        startToken: 'application-token',
        grants: [storedGrant],
      },
    ]);
    const resumed = await build().resume('context-grants', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(
      provider.mock.calls.map(([_workflowId, _runId, leg]) => leg.kind),
    ).toEqual(['start', 'resume']);
    if (mode === 'provider-revocation') {
      expect(resumed.status).toBe('failed');
      expect(resumed.error).toContain(
        'approval required and no matching structured grant was found',
      );
      expect(seen).toHaveLength(1);
    } else {
      expect(resumed.status).toBe('success');
      expect(seen).toHaveLength(2);
      expect(seen[1]).toEqual({
        ...seen[0],
        source: 'provider-resume',
        grants: [
          {
            scope: 'suspension',
            connectorId: 'context-writer',
            workflowId: 'context-grants',
            runId: 'context-grants-run',
            suspension: {
              stepPath: ['gate'],
              suspendedAt: expect.any(Number),
            },
          },
        ],
      });
    }
  });

  it('consults the provider on every start and resume leg', async () => {
    // #given — a provider that mints a distinct context per consult
    let calls = 0;
    const { runtime, seen } = buildContextProbe(() => {
      calls += 1;
      return { 'test.a': `mint-${calls}` };
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');
    const resumed = await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — start leg saw the first mint, resume leg the second: the
    // provider is re-consulted per leg and the resume-time context is what
    // the resumed step observes.
    expect(resumed.status).toBe('success');
    expect(calls).toBe(2);
    expect(seen).toEqual([
      { leg: 'start', a: 'mint-1', b: undefined },
      { leg: 'resume', a: 'mint-2', b: undefined },
    ]);
  });

  it('pins resume-context semantics: the provided context merges over the persisted one', async () => {
    // #given — start mints {a, b}; resume mints only {a}
    let leg = 0;
    const { runtime, seen } = buildContextProbe(() => {
      leg += 1;
      return leg === 1
        ? { 'test.a': 'start-a', 'test.b': 'start-b' }
        : { 'test.a': 'resume-a' };
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — empirical pin (core 1.49.0): resume-provided keys override,
    // but persisted start-time keys SURVIVE ('test.b' is still visible).
    // Consequence: omitting a key at resume does not revoke it — a provider
    // that needs to withdraw a capability must overwrite the key (e.g. an
    // empty grant list), not omit it.
    expect(seen[1]).toEqual({ leg: 'resume', a: 'resume-a', b: 'start-b' });
  });

  it('propagates a provider failure instead of running without context', async () => {
    // #given
    const { runtime } = buildContextProbe(() => {
      throw new Error('grant store down');
    });

    // #when / #then
    await expect(
      runtime.start('probe', { runId: crypto.randomUUID(), inputData: {} }),
    ).rejects.toThrow('grant store down');
  });

  it('passes the execution leg: start, then resume with the explicit step', async () => {
    // #given
    const legs: RunLeg[] = [];
    const { runtime } = buildContextProbe((_workflowId, _runId, leg) => {
      legs.push(leg);
      return undefined;
    });

    // #when
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('probe', started.runId, {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — string step selections normalize to a path; the leg carries
    // the step's current suspension timestamp from the snapshot
    expect(legs).toEqual([
      { kind: 'start' },
      { kind: 'resume', step: ['gate'], suspendedAt: expect.any(Number) },
    ]);
  });

  it('resolves the resume-leg step from the snapshot when none is selected', async () => {
    // #given
    const legs: RunLeg[] = [];
    const { runtime } = buildContextProbe((_workflowId, _runId, leg) => {
      legs.push(leg);
      return undefined;
    });
    const started = await runtime.start('probe', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when — no explicit step; 'gate' is the only suspended step
    await runtime.resume('probe', started.runId, { resumeData: { go: true } });

    // #then
    expect(legs[1]).toEqual({
      kind: 'resume',
      step: ['gate'],
      suspendedAt: expect.any(Number),
    });
  });

  it('mints the workflow-scope key on every leg, even without a provider', async () => {
    // #given — NO requestContextForRun provider; a step that records the
    // runtime-minted scope
    const seen: unknown[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push(requestContext.get('breakwater.workflowScope'));
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when
    await runtime.start('scoped-wf', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — the executing workflow's own id, minted by the runtime
    expect(seen).toEqual(['scoped-wf']);
  });

  it('does not synthesize breakwater isolation scope from runId prefixes', async () => {
    // #given — a probe recording both server-minted keys
    const seen: Array<{ scope: unknown; isolation: unknown }> = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push({
          scope: requestContext.get('breakwater.workflowScope'),
          isolation: requestContext.get('breakwater.isolationScope'),
        });
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf2',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when — differently shaped opaque run ids
    await runtime.start('scoped-wf2', { runId: 'acme_r1', inputData: {} });
    await runtime.start('scoped-wf2', { runId: 'plain-run', inputData: {} });
    await runtime.start('scoped-wf2', { runId: 'AB_r1', inputData: {} });

    // #then — none becomes an implicit isolation key
    expect(seen).toEqual([
      { scope: 'scoped-wf2', isolation: undefined },
      { scope: 'scoped-wf2', isolation: undefined },
      { scope: 'scoped-wf2', isolation: undefined },
    ]);
  });

  it('does not let a provider override the runtime-minted workflow scope', async () => {
    // #given — a provider that attempts to override the scope key
    const seen: unknown[] = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        startIdempotency: 'none',
        executionFence: 'none',
        requestContextForRun: () => ({
          'breakwater.workflowScope': 'overridden',
        }),
      },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push(requestContext.get('breakwater.workflowScope'));
        return {};
      },
    });
    createWorkflow({
      id: 'scoped-wf',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    // #when
    await runtime.start('scoped-wf', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — runtime identity merges over provider values
    expect(seen).toEqual(['scoped-wf']);
  });

  it('orders stored context before runtime scope, grants, and trusted identity', async () => {
    const seen: Array<{ keys: string[]; values: Record<string, unknown> }> = [];
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        startIdempotency: 'none',
        executionFence: 'none',
        requestContextForRun: () => ({
          stored: 'kept',
          'breakwater.workflowScope': 'forged-workflow',
          'breakwater.isolationScope': 'forged-tenant',
          'breakwater.connectorExecution': { kind: 'start' },
          'breakwater.connectorGrants': [],
          'breakwater.principalPermissions': {
            permissions: ['reports.read'],
            policyVersion: 'permissions-v1',
          },
          runId: 'acme_forged',
          threadId: 'acme_thread',
          resourceId: 'acme_resource',
          'breakwater.actor': { id: 'operator-1', role: 'operator' },
          'breakwater.auditContext': {
            agentId: 'writer',
            entryPath: 'http.start',
          },
        }),
      },
    );
    const probe = createStep({
      id: 'probe',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      execute: async ({ requestContext }) => {
        seen.push({
          keys: [...requestContext.keys()],
          values: requestContext.toJSON(),
        });
        return {};
      },
    });
    createWorkflow({
      id: 'ordered-context',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(probe)
      .commit();

    await runtime.start('ordered-context', {
      runId: 'acme_ordered',
      inputData: {},
    });

    expect(seen[0]?.keys).toEqual([
      'stored',
      'breakwater.workflowScope',
      'runId',
      'flowsafe.runProvenance',
      'breakwater.connectorExecution',
      'breakwater.connectorGrants',
      'breakwater.principalPermissions',
      'threadId',
      'resourceId',
      'breakwater.actor',
      'breakwater.auditContext',
    ]);
    expect(seen[0]?.values).toMatchObject({
      'breakwater.workflowScope': 'ordered-context',
      // A trusted provider's projection passes through like the grant key —
      // this is the seam a workflow host uses to authorize its connectors.
      'breakwater.principalPermissions': {
        permissions: ['reports.read'],
        policyVersion: 'permissions-v1',
      },
    });
  });

  it('leaves nothing persisted when the provider fails on start — the runId stays retryable', async () => {
    // #given — a provider that fails once (e.g. the grant store's D1 is
    // briefly unreachable), then recovers
    let failures = 1;
    const { runtime } = buildContextProbe(() => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('grant store down');
      }
      return undefined;
    });

    // #when — the first start fails BEFORE createRun persists anything
    await expect(
      runtime.start('probe', { runId: 'retry-me', inputData: {} }),
    ).rejects.toThrow('grant store down');

    // #then — no orphaned pending run, and the same runId starts cleanly
    expect(await runtime.status('probe', 'retry-me')).toBeNull();
    const retried = await runtime.start('probe', {
      runId: 'retry-me',
      inputData: {},
    });
    expect(retried.status).toBe('suspended');
  });
});

describe('RunnerRuntime resumeCount projection (re-suspension)', () => {
  // The do-runner OWNS the RunSummary/RunLeg projection, so the layer that
  // publishes the resumeCount contract must pin it against the real engine.
  // resumeCount is the categorical signal flowsafe's grant binding depends on
  // ("undefined on first suspension, defined on re-suspension"); unlike the
  // informational resumedAt (which Mastra stamps only on a payload-bearing
  // resume) the runtime increments resumeCount on EVERY resume, so it holds
  // even for a no-payload re-suspension. gate2x suspends, and re-suspends on a
  // no-payload resume (round 1) or after a payload resume (round 2).
  function buildReSuspender(
    onLeg?: (leg: RunLeg) => void,
    storage = new InMemoryStore(),
  ): RunnerRuntime {
    let rounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        startIdempotency: 'none',
        executionFence: 'none',
        ...(onLeg
          ? {
              requestContextForRun: (
                _workflowId: string,
                _runId: string,
                leg: RunLeg,
              ) => {
                onLeg(leg);
                return undefined;
              },
            }
          : {}),
      },
    );
    const gate2x = createStep({
      id: 'gate2x',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      // No resumeSchema on purpose: with a required schema, core rejects a
      // no-payload resume before execute, so the falsy-resume re-suspension is
      // unreachable. Without one, a falsy resume passes validation and
      // re-suspends via the guard below.
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'round 1' });
        rounds += 1;
        if (rounds < 2) return suspend({ reason: 'round 2' });
        return {};
      },
    });
    createWorkflow({
      id: 'resuspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gate2x)
      .commit();
    return runtime;
  }

  it('omits resumeCount on the first suspension and carries it on a re-suspension', async () => {
    // #given
    const runtime = buildReSuspender();

    // #when — first suspension
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #then — a first suspension carries suspendedAt but NO resumeCount; this
    // undefined is the categorical tie-breaker the grant binding relies on
    expect(started.status).toBe('suspended');
    expect(started.suspendedAt?.gate2x).toBeTypeOf('number');
    expect(started.resumeCount?.gate2x).toBeUndefined();

    // #when — resume (payload); gate2x runs and re-suspends (round 2)
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — the re-suspension summary carries resumeCount 1 (one resume so
    // far); resumedAt is also present here because the resume carried a payload
    expect(reSuspended.suspended).toEqual([['gate2x']]);
    expect(reSuspended.resumeCount?.gate2x).toBe(1);
    expect(reSuspended.suspendedAt?.gate2x).toBeTypeOf('number');
    expect(reSuspended.resumedAt?.gate2x).toBeTypeOf('number');
  });

  it('refuses a resume naming a suspension its step has left, before the step runs', async () => {
    // #given a step resumed once and suspended again
    const runtime = buildReSuspender();
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #when a resume names the first suspension
    const stale = runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      expectedSuspension: { suspendedAt: started.suspendedAt?.gate2x ?? -1 },
    });

    // #then it is refused, and the step stays at its second suspension
    await expect(stale).rejects.toMatchObject({
      name: 'SuspensionChangedError',
      status: 409,
      reason: { code: 'SUSPENSION_CHANGED' },
    });
    await expect(
      runtime.status('resuspend', started.runId),
    ).resolves.toMatchObject({
      status: 'suspended',
      resumeCount: { gate2x: 1 },
      suspendedAt: { gate2x: reSuspended.suspendedAt?.gate2x },
    });
  });

  it('resumes when the named suspension is the one the step is at', async () => {
    // #given a step at its first suspension
    const runtime = buildReSuspender();
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when each resume names the suspension the step is at
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      expectedSuspension: { suspendedAt: started.suspendedAt?.gate2x ?? -1 },
    });
    const finished = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      expectedSuspension: {
        suspendedAt: reSuspended.suspendedAt?.gate2x ?? -1,
        resumeCount: 1,
      },
    });

    // #then both resume
    expect(reSuspended.resumeCount?.gate2x).toBe(1);
    expect(finished.status).toBe('success');
  });

  it.each([
    { label: 'a resume count of 0', value: { suspendedAt: 1, resumeCount: 0 } },
    { label: 'a value that is not an object', value: 'first' },
  ])('refuses a named suspension with $label as a malformed request', async ({
    value,
  }) => {
    // #given a step at its first suspension
    const runtime = buildReSuspender();
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when a resume names a malformed suspension
    const resumed = runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      expectedSuspension: value as never,
    });

    // #then it is refused before the step runs
    await expect(resumed).rejects.toMatchObject({
      name: 'InvalidRunRequestError',
    });
    await expect(
      runtime.status('resuspend', started.runId),
    ).resolves.not.toHaveProperty('resumeCount.gate2x');
  });

  it('refuses a named suspension when the resumed step cannot be resolved', async () => {
    // #given two steps suspended in parallel
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const gate = (id: string) =>
      createStep({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ resumeData, suspend }) =>
          resumeData ? {} : suspend({ reason: `${id} waits` }),
      });
    createWorkflow({
      id: 'parallel-named',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .parallel([gate('gateA'), gate('gateB')])
      .commit();
    const started = await runtime.start('parallel-named', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when a resume names a suspension but no step
    const resumed = runtime.resume('parallel-named', started.runId, {
      resumeData: { go: true },
      expectedSuspension: { suspendedAt: 1 },
    });

    // #then it is refused as a malformed request
    await expect(resumed).rejects.toMatchObject({
      name: 'InvalidRunRequestError',
    });
  });

  it('prepares a host from one isolated copy of the trusted resume context', async () => {
    const legs: RunLeg[] = [];
    const storage = new InMemoryStore();
    const runtime = buildReSuspender(
      (leg) => legs.push(structuredClone(leg)),
      storage,
    );
    const started = await runtime.start('resuspend', {
      runId: 'acme_resume-context',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    legs.length = 0;

    let preparedWorkflowScope: unknown;
    let preparedIsolationScope: unknown;
    await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
      prepareExecution: async (context) => {
        preparedWorkflowScope = context.get('breakwater.workflowScope');
        preparedIsolationScope = context.get('breakwater.isolationScope');
        const provenance = context.get('flowsafe.runProvenance') as {
          requestedBy?: string;
        };
        provenance.requestedBy = 'forged';
        context.clear();
      },
    });

    expect(preparedWorkflowScope).toBe('resuspend');
    expect(preparedIsolationScope).toBeUndefined();
    expect(legs).toHaveLength(1);
    expect(legs[0]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
    });
    await expect(
      runtime.status('resuspend', started.runId),
    ).resolves.toMatchObject({
      requestedBy: 'reviewer-1',
      resumeCount: { gate2x: 1 },
    });
  });

  it('carries resumeCount on a NO-PAYLOAD re-suspension even though Mastra omits resumedAt', async () => {
    // #given — a falsy resume re-suspends via
    // `if (!resumeData) return suspend(...)`, so Mastra never stamps resumedAt.
    const runtime = buildReSuspender();
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    // #when — resume with NO resumeData; gate2x re-suspends (round 1 again)
    const reSuspended = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
    });

    // #then — resumedAt stays undefined (Mastra's payload-conditional stamp),
    // but the runtime-owned resumeCount is present, so the grant binding can
    // still tell this re-suspension apart from the first suspension.
    expect(reSuspended.suspended).toEqual([['gate2x']]);
    expect(reSuspended.resumedAt?.gate2x).toBeUndefined();
    expect(reSuspended.resumeCount?.gate2x).toBe(1);
  });

  it('a required resumeSchema rejects a no-payload resume (why the falsy path needs a schema-less step)', async () => {
    // Tripwire pinning the Mastra-version-dependent boundary the falsy-resume
    // fixtures rely on: with a REQUIRED resumeSchema, core validates resume
    // data and rejects a no-payload resume BEFORE execute, so the falsy-resume
    // re-suspension is only reachable for schema-less / optional-schema /
    // validateInputs-off steps. If a Mastra bump changes
    // this, the "schema-less fixture required" assumption goes silently stale.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const schemaGate = createStep({
      id: 'schemaGate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'awaiting' });
        return {};
      },
    });
    createWorkflow({
      id: 'schema-gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(schemaGate)
      .commit();
    const started = await runtime.start('schema-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.status).toBe('suspended');

    // #when / #then — a no-payload resume is rejected as invalid resume data,
    // never reaching execute (so it cannot re-suspend without a resumedAt)
    await expect(
      runtime.resume('schema-gate', started.runId, { step: 'schemaGate' }),
    ).rejects.toThrow(/resume data/i);
  });

  it('passes resumeCount on the resume leg, incrementing per resume', async () => {
    // #given — a provider recording every leg
    const legs: RunLeg[] = [];
    const runtime = buildReSuspender((leg) => legs.push(leg));

    // #when — start (round-1 suspension) then resume, which re-suspends
    const started = await runtime.start('resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — the leg that reattached to the first suspension read snapshot
    // provenance before any resume, so its resumeCount is undefined
    expect(legs[1]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
      suspendedAt: expect.any(Number),
    });
    expect((legs[1] as { resumeCount?: number }).resumeCount).toBeUndefined();

    // #when — resume again, reattaching to the RE-suspension
    const done = await runtime.resume('resuspend', started.runId, {
      step: 'gate2x',
      resumeData: { go: true },
    });

    // #then — one prior resume happened, so this leg carries resumeCount 1
    expect(done.status).toBe('success');
    expect(legs[2]).toMatchObject({
      kind: 'resume',
      step: ['gate2x'],
      suspendedAt: expect.any(Number),
      resumeCount: 1,
    });
  });

  it('marks only the resumed branch, leaving a co-suspended branch a first suspension', async () => {
    // #given — two parallel gates both suspend; gateA re-suspends on a payload
    // resume (round 2), gateB stays at its first suspension.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    let aRounds = 0;
    const gateA = createStep({
      id: 'gateA',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'A round 1' });
        aRounds += 1;
        if (aRounds < 2) return suspend({ reason: 'A round 2' });
        return { ok: true };
      },
    });
    const gateB = createStep({
      id: 'gateB',
      inputSchema: z.object({}),
      outputSchema: z.object({ ok: z.boolean() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({ go: z.boolean() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? { ok: true } : suspend({ reason: 'B waits' }),
    });
    createWorkflow({
      id: 'parallel-resuspend',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .parallel([gateA, gateB])
      .commit();
    const started = await runtime.start('parallel-resuspend', {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    expect(started.suspended).toHaveLength(2);
    expect(started.resumeCount?.gateA).toBeUndefined();
    expect(started.resumeCount?.gateB).toBeUndefined();

    // #when — resume ONLY gateA; it re-suspends (round 2), gateB is untouched
    const reSuspended = await runtime.resume(
      'parallel-resuspend',
      started.runId,
      { step: 'gateA', resumeData: { go: true } },
    );

    // #then — gateA carries resumeCount 1 (it was resumed); gateB, never
    // resumed, stays undefined — its own first suspension, not collapsed into
    // gateA's ordinal in snapshot provenance.
    expect(reSuspended.status).toBe('suspended');
    expect(reSuspended.resumeCount?.gateA).toBe(1);
    expect(reSuspended.resumeCount?.gateB).toBeUndefined();
  });
});

describe('RunnerRuntime resumeCount snapshot provenance (shared runId across workflows)', () => {
  // Two suspending workflows under DIFFERENT ids on ONE runtime, both driven with
  // the SAME caller runId. Mastra persists them as distinct runs (snapshots key on
  // `${workflowName}-${runId}`). wfA completes on its first payload resume;
  // wfB re-suspends once, so its snapshot retains count 1 while wfA becomes
  // terminal. Both gates share step id 'gate' to prove each snapshot owns its
  // own provenance even when caller run ids and inner step keys match.
  function buildSharedRunIdPair(
    onLeg?: (workflowId: string, leg: RunLeg) => void,
  ): RunnerRuntime {
    let bRounds = 0;
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      {
        startIdempotency: 'none',
        executionFence: 'none',
        ...(onLeg
          ? {
              requestContextForRun: (
                workflowId: string,
                _runId: string,
                leg: RunLeg,
              ) => {
                onLeg(workflowId, leg);
                return undefined;
              },
            }
          : {}),
      },
    );
    const gateA = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'A waits' }),
    });
    createWorkflow({
      id: 'wfA',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gateA)
      .commit();
    const gateB = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: 'B round 1' });
        bRounds += 1;
        if (bRounds < 2) return suspend({ reason: 'B round 2' });
        return {};
      },
    });
    createWorkflow({
      id: 'wfB',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gateB)
      .commit();
    return runtime;
  }

  it("preserves a sibling run's snapshot provenance when a run sharing its runId reaches terminal status", async () => {
    // #given — wfA and wfB both suspended under the SAME runId 'shared'
    const runtime = buildSharedRunIdPair();
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, so its snapshot provenance holds gate -> 1
    const reSuspendedB = await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(reSuspendedB.suspended).toEqual([['gate']]);
    expect(reSuspendedB.resumeCount?.gate).toBe(1);

    // #when — wfA with the same caller runId resumes to success
    const doneA = await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    expect(doneA.status).toBe('success');

    // #then — wfB's still-suspended round-2 snapshot keeps resumeCount 1
    const statusB = await runtime.status('wfB', 'shared');
    expect(statusB).toMatchObject({
      status: 'suspended',
      suspended: [['gate']],
    });
    expect(statusB?.resumeCount?.gate).toBe(1);
  });

  it("reads a run's own snapshot provenance on the resume leg", async () => {
    // #given — a provider recording (workflowId, leg) for every consult
    const legs: Array<{ workflowId: string; leg: RunLeg }> = [];
    const runtime = buildSharedRunIdPair((workflowId, leg) =>
      legs.push({ workflowId, leg }),
    );
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #given — wfB resumed once, bumping the gate ordinal in wfB's snapshot
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #when — wfA's first resume reads its snapshot provenance before incrementing it
    await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — wfA never resumed before, so its own snapshot has no gate entry
    // and the leg's resumeCount is undefined
    const wfAResumeLeg = legs.find(
      (e) => e.workflowId === 'wfA' && e.leg.kind === 'resume',
    )?.leg as { resumeCount?: number } | undefined;
    expect(wfAResumeLeg).toBeDefined();
    expect(wfAResumeLeg?.resumeCount).toBeUndefined();
  });

  // A step that re-suspends on EVERY resume (never completes), so a run stays
  // suspended at any depth and status() keeps projecting its accumulating
  // ordinal — the deep-chain (3+ suspension) case the pair-binding relies on.
  function buildSharedDeepChain(): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    for (const id of ['wfA', 'wfB']) {
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        suspendSchema: z.object({ reason: z.string() }),
        execute: async ({ resumeData, suspend }) =>
          suspend({ reason: resumeData ? 'again' : 'first' }),
      });
      createWorkflow({
        id,
        inputSchema: z.object({}),
        outputSchema: z.object({}),
      })
        .then(gate)
        .commit();
    }
    return runtime;
  }

  it('accumulates per-workflow resume ordinals independently past depth 1 under a shared runId', async () => {
    // #given — wfA and wfB both suspended under the SAME runId 'shared'
    const runtime = buildSharedDeepChain();
    await runtime.start('wfA', { runId: 'shared', inputData: {} });
    await runtime.start('wfB', { runId: 'shared', inputData: {} });

    // #when — wfA resumed once (ordinal 1), wfB resumed twice (ordinal 2); both
    // re-suspend each time, so snapshot provenance must accumulate, not reset to 1
    await runtime.resume('wfA', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });
    await runtime.resume('wfB', 'shared', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — each workflow's ordinal is its OWN accumulated count: wfA=1, wfB=2.
    // A fully shared bucket takes all 3 increments, so both reads see 3; a
    // get-or-create keyed wrong freezes both at 1 (the deep-chain leak: a round-2
    // approval minting into round 3).
    const statusA = await runtime.status('wfA', 'shared');
    const statusB = await runtime.status('wfB', 'shared');
    expect(statusA?.resumeCount?.gate).toBe(1);
    expect(statusB?.resumeCount?.gate).toBe(2);
  });
});

describe('RunnerRuntime snapshot provenance durability', () => {
  function buildDurable(
    storage: InMemoryStore,
    onLeg?: (leg: RunLeg) => void,
    providedContext?: Record<string, unknown>,
  ): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        startIdempotency: 'none',
        executionFence: 'none',
        requestContextForRun: (_workflowId, _runId, leg) => {
          onLeg?.(leg);
          return providedContext;
        },
      },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'wait' }),
    });
    createWorkflow({
      id: 'durable-gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
    })
      .then(gate)
      .commit();
    return runtime;
  }

  function buildTerminalRepair(
    storage: InMemoryStore,
    requestContextForRun?: RequestContextProvider,
  ): RunnerRuntime {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      {
        startIdempotency: 'none',
        requestContextForRun,
        executionFence: 'none',
      },
    );
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      suspendSchema: z.object({ reason: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData ? {} : suspend({ reason: 'wait' }),
    });
    createWorkflow({
      id: 'terminal-repair',
      inputSchema: z.object({}),
      outputSchema: z.object({}),
      options: {
        shouldPersistSnapshot: ({ workflowStatus }) =>
          workflowStatus === 'pending' || workflowStatus === 'suspended',
      },
    })
      .then(gate)
      .commit();
    return runtime;
  }

  it('projects requester and resume ordinal after runtime eviction', async () => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    const reSuspended = await before.resume('durable-gate', started.runId, {
      step: 'gate',
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });
    expect(reSuspended).toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      resumeCount: { gate: 1 },
    });

    const legs: RunLeg[] = [];
    const after = buildDurable(storage, (leg) => legs.push(leg));
    const recovered = await after.status('durable-gate', started.runId);
    expect(recovered).toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      resumeCount: { gate: 1 },
    });
    const done = await after.resume('durable-gate', started.runId, {
      step: 'gate',
      resumeData: { go: true },
      requestedBy: 'reviewer-2',
      requestedByKind: 'human',
    });
    expect(done).toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-2',
    });
    expect(legs.find((leg) => leg.kind === 'resume')).toMatchObject({
      resumeCount: 1,
    });
  });

  it.each([
    ['an overlong requester', { requestedBy: 'r'.repeat(201) }],
    ['an all-whitespace requester', { requestedBy: ' '.repeat(200) }],
    ['a control-bearing requester', { requestedBy: 'requester\u0000forged' }],
    ['an invalid requester kind', { requestedByKind: 'operator' }],
  ])('fails closed on stored run provenance with %s', async (_label, corruption) => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: 'corrupt-requester-provenance',
      inputData: {},
      requestedBy: 'initiator',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const snapshot = await workflows.loadWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
    });
    if (!snapshot) throw new Error('workflow snapshot missing');
    const provenance = snapshot.requestContext?.[
      'flowsafe.runProvenance'
    ] as Record<string, unknown>;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
      snapshot: {
        ...snapshot,
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runProvenance': {
            ...provenance,
            ...corruption,
          },
        },
      },
    });

    await expect(
      buildDurable(storage).status('durable-gate', started.runId),
    ).rejects.toThrow('run provenance is not readable');
  });

  it('reads legacy id-only provenance but requires a new complete pair before resume writes', async () => {
    const storage = new InMemoryStore();
    const before = buildDurable(storage);
    const started = await before.start('durable-gate', {
      runId: 'legacy-requester-provenance',
      inputData: {},
      requestedBy: 'legacy-initiator',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const snapshot = await workflows.loadWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
    });
    if (!snapshot) throw new Error('workflow snapshot missing');
    const legacyProvenance = {
      ...(snapshot.requestContext?.['flowsafe.runProvenance'] as Record<
        string,
        unknown
      >),
    };
    legacyProvenance.version = 1;
    delete legacyProvenance.requestedByKind;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-gate',
      runId: started.runId,
      snapshot: {
        ...snapshot,
        requestContext: {
          ...snapshot.requestContext,
          'flowsafe.runProvenance': legacyProvenance,
        },
      },
    });

    const after = buildDurable(storage);
    const legacyStatus = await after.status('durable-gate', started.runId);
    expect(legacyStatus).toMatchObject({ requestedBy: 'legacy-initiator' });
    expect(legacyStatus?.requestedByKind).toBeUndefined();
    await expect(
      after.resume('durable-gate', started.runId, { step: 'gate' }),
    ).rejects.toThrow(
      'legacy requestedBy provenance requires an explicit requestedBy and requestedByKind to resume',
    );
    expect(
      (await after.status('durable-gate', started.runId))?.requestedByKind,
    ).toBeUndefined();
    await expect(
      after.resume('durable-gate', started.runId, {
        step: 'gate',
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'suspended',
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });
  });

  it('reserves snapshot provenance against provider override', async () => {
    const storage = new InMemoryStore();
    const runtime = buildDurable(storage, undefined, {
      'flowsafe.runProvenance': {
        version: 1,
        requestedBy: 'forged',
        attemptToken: 'forged',
        resumeCounts: [['gate', 99]],
      },
    });
    const started = await runtime.start('durable-gate', {
      runId: crypto.randomUUID(),
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });

    expect(started.requestedBy).toBe('operator-1');
    await expect(
      runtime.status('durable-gate', started.runId),
    ).resolves.toMatchObject({ requestedBy: 'operator-1' });
  });

  it('repairs a terminal-only snapshot with the current resume provenance', async () => {
    const storage = new InMemoryStore();
    const requestContextForRun: RequestContextProvider = (
      _workflowId,
      _runId,
      leg,
    ) =>
      leg.kind === 'start'
        ? { 'test.a': 'start-a', 'test.b': 'start-b' }
        : { 'test.a': 'resume-a' };
    const before = buildTerminalRepair(storage, requestContextForRun);
    const started = await before.start('terminal-repair', {
      runId: 'terminal-provenance',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });

    await expect(
      before.resume('terminal-repair', started.runId, {
        step: 'gate',
        resumeData: { approved: true },
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });

    const workflows = await storage.getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: 'terminal-repair',
      runId: started.runId,
    });
    expect(snapshot?.requestContext).toMatchObject({
      'test.a': 'resume-a',
      'test.b': 'start-b',
      'flowsafe.runProvenance': expect.objectContaining({
        requestedBy: 'reviewer-1',
      }),
    });

    const after = buildTerminalRepair(storage, requestContextForRun);
    await expect(
      after.status('terminal-repair', started.runId),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });
  });

  it('returns the committed terminal summary when repair acknowledgement is lost', async () => {
    const storage = new InMemoryStore();
    const runtime = buildTerminalRepair(storage);
    const started = await runtime.start('terminal-repair', {
      runId: 'terminal-lost-ack',
      inputData: {},
      requestedBy: 'operator-1',
      requestedByKind: 'human',
    });
    const workflows = await storage.getStore('workflows');
    if (!workflows) throw new Error('workflows store missing');
    const persist = workflows.persistWorkflowSnapshot.bind(workflows);
    let loseAcknowledgement = true;
    vi.spyOn(workflows, 'persistWorkflowSnapshot').mockImplementation(
      async (input) => {
        await persist(input);
        if (loseAcknowledgement && input.snapshot.status === 'success') {
          loseAcknowledgement = false;
          throw new Error('terminal persist acknowledgement lost');
        }
      },
    );

    await expect(
      runtime.resume('terminal-repair', started.runId, {
        step: 'gate',
        resumeData: { approved: true },
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    ).resolves.toMatchObject({
      status: 'success',
      requestedBy: 'reviewer-1',
    });
    expect(loseAcknowledgement).toBe(false);
  });
});

// A step arms a per-suspension deadline through Mastra's own suspend payload.
describe('per-suspension deadline contract', () => {
  const SUSPENSION_DEADLINE_MS = 900_000;

  function timedGateRuntime(
    id: string,
    suspendSchema?: z.ZodType,
    resumeSchema?: z.ZodType,
  ): {
    runtime: RunnerRuntime;
    storage: InMemoryStore;
    workflow: { getWorkflowRunById: (runId: string) => Promise<unknown> };
    start: () => Promise<RunSummary>;
  } {
    const storage = new InMemoryStore();
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    // Built as a value so the reserved key survives a suspendSchema that does
    // not declare it.
    const suspendPayload: Record<string, unknown> = {
      reason: 'awaiting signal',
      [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
    };
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      ...(suspendSchema ? { suspendSchema } : {}),
      ...(resumeSchema ? { resumeSchema } : {}),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? {
              settledBy: isSuspensionTimeoutResumeData(resumeData)
                ? 'timeout'
                : 'signal',
            }
          : suspend(suspendPayload),
    });
    const workflow = createWorkflow({
      id,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(gate)
      .commit();
    return {
      runtime,
      storage,
      workflow,
      start: () =>
        runtime.start(id, { runId: crypto.randomUUID(), inputData: {} }),
    };
  }

  /**
   * Blind the workflows store's row read — the exact seam Mastra falls back
   * from — and hand back the restore. NOT the Workflow method: stubbing that
   * would FABRICATE the fallback, and what is under test is that Mastra
   * produces it and stamps it. File-local rather than shared: keeping each copy
   * beside the fixtures it serves is cheaper than a new shared module.
   */
  async function blindWorkflowRow(storage: InMemoryStore): Promise<() => void> {
    const store = (await storage.getStore('workflows')) as unknown as {
      getWorkflowRunById: (args: unknown) => Promise<unknown>;
    };
    const original = store.getWorkflowRunById;
    store.getWorkflowRunById = async () => null;
    return () => {
      store.getWorkflowRunById = original;
    };
  }

  it('lets the resumed step tell a timeout from a real signal', async () => {
    const { runtime, start } = timedGateRuntime('timed-gate');
    const started = await start();

    // #then — the summary carries everything the deadline is derived from
    const { entries, rejected } = suspensionDeadlinesOf(started);
    expect(rejected).toEqual([]);
    expect(entries[0]).toEqual({
      step: 'gate',
      deadlineAt:
        (started.suspendedAt?.gate as number) + SUSPENSION_DEADLINE_MS,
      suspendedAt: started.suspendedAt?.gate,
      resumeCount: 0,
    });

    // #when — the deadline elapses and flowsafe resumes the run itself
    const timedOut = await runtime.resume('timed-gate', started.runId, {
      step: ['gate'],
      resumeData: suspensionTimeoutResumeData(
        entries[0] as SuspensionDeadlineEntry,
        Date.now(),
      ),
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });

    expect(timedOut).toMatchObject({
      status: 'success',
      result: { settledBy: 'timeout' },
      requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
      requestedByKind: 'system',
    });

    // #when — the same step reached by a genuine signal instead
    const signalled = await start();
    const resumed = await runtime.resume('timed-gate', signalled.runId, {
      step: ['gate'],
      resumeData: { approvedBy: 'bob' },
      requestedBy: 'reviewer-1',
      requestedByKind: 'human',
    });

    expect(resumed).toMatchObject({
      status: 'success',
      result: { settledBy: 'signal' },
      requestedBy: 'reviewer-1',
    });
  });

  it('pins Mastra stripping the reserved key from an undeclared suspendSchema', async () => {
    // Tripwire on the documented authoring caveat: Mastra validates the suspend
    // payload and SUBSTITUTES the parsed output, so a z.object() that does not
    // declare the reserved field drops it and nothing arms. If a Mastra upgrade
    // changes that, this fails instead of the caveat going silently stale.
    const { start } = timedGateRuntime(
      'stripped-gate',
      z.object({ reason: z.string() }),
    );

    const started = await start();

    const payload = (started.suspendPayload as Record<string, unknown>).gate;
    expect(payload).toEqual({ reason: 'awaiting signal' });
    expect(suspensionDeadlinesOf(started).entries).toEqual([]);
  });

  it('pins the reserved key surviving a suspendSchema that declares it', async () => {
    const { start } = timedGateRuntime(
      'declared-gate',
      z.object({
        reason: z.string(),
        [SUSPENSION_DEADLINE_PAYLOAD_KEY]: z.number(),
      }),
    );

    const started = await start();

    expect(
      (started.suspendPayload as Record<string, Record<string, unknown>>)
        .gate?.[SUSPENSION_DEADLINE_PAYLOAD_KEY],
    ).toBe(SUSPENSION_DEADLINE_MS);
    expect(suspensionDeadlinesOf(started).entries).toHaveLength(1);
  });

  it.each([
    ['loose-object-gate', z.looseObject({ reason: z.string() })],
    [
      'passthrough-gate',
      z.object({ reason: z.string() }).passthrough() as unknown as z.ZodType,
    ],
  ])('arms through the documented loose-schema escape hatch (%s)', async (id, suspendSchema) => {
    // The escape hatch the README and the design doc offer authors who do
    // not want to name a flowsafe key in their own schema. If a Mastra or
    // zod upgrade stops honouring it, that advice is wrong and this fails.
    const { start } = timedGateRuntime(id, suspendSchema);

    const started = await start();

    expect(suspensionDeadlinesOf(started).entries).toHaveLength(1);
  });

  it('fails the timeout resume of a step whose resumeSchema rejects the envelope', async () => {
    // Every realistic approval step declares a resumeSchema, and Mastra
    // validates resume data before the engine is touched, so a schema that
    // does not accept the envelope makes the timeout resume throw.
    const { runtime, start } = timedGateRuntime(
      'resume-schema-gate',
      undefined,
      z.object({ approvedBy: z.string() }),
    );
    const started = await start();
    const entry = suspensionDeadlinesOf(started)
      .entries[0] as SuspensionDeadlineEntry;

    // The message matters: a bare rejection would also pass if the resume threw
    // for an unrelated reason, and then this would stop pinning the footgun.
    await expect(
      runtime.resume('resume-schema-gate', started.runId, {
        step: ['gate'],
        resumeData: suspensionTimeoutResumeData(entry, Date.now()),
        requestedBy: SUSPENSION_DEADLINE_PRINCIPAL_ID,
        requestedByKind: 'system',
      }),
    ).rejects.toThrow('Invalid resume data');

    // #then — the run is untouched: the step never took its timeout branch
    const after = await runtime.status('resume-schema-gate', started.runId);
    expect(after?.status).toBe('suspended');
    expect(after?.resumeCount?.gate).toBeUndefined();
  });

  it('refuses to arm a nested suspension instead of arming nothing silently', async () => {
    // Mastra reports a nested suspension as the nested path but keys the
    // payload and the fence by the TOP-LEVEL step, so there is no fence for
    // the step that actually suspended. v1 refuses it, loudly, rather than
    // leaving an author to believe a deadline was accepted.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const approval = createStep({
      id: 'approval',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? { settledBy: 'signal' }
          : suspend({
              reason: 'awaiting signal',
              [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
            }),
    });
    const inner = createWorkflow({
      id: 'nested',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(approval)
      .commit();
    createWorkflow({
      id: 'nested-outer',
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(inner)
      .commit();

    const started = await runtime.start('nested-outer', {
      runId: crypto.randomUUID(),
      inputData: {},
    });

    expect(started.suspended).toEqual([['nested']]);
    expect(suspensionDeadlinesOf(started)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });

    // #then — and refused again on the status() projection, which reports the
    // SAME suspension as the enclosing step alone. A refusal on only one of the
    // two projections is worse than none: the entry arms from the projection
    // that misses it and then resumes a step whose fence describes a different
    // suspension.
    const rehydrated = await runtime.status('nested-outer', started.runId);
    expect(rehydrated?.suspended).toEqual([['nested']]);
    expect(suspensionDeadlinesOf(rehydrated as RunSummary)).toEqual({
      entries: [],
      rejected: [
        {
          step: 'nested',
          reason: 'nested suspension paths are not supported',
        },
      ],
    });

    // Tripwire on the marker that refusal depends on: Mastra stamps the inner
    // path into the persisted payload, and that is the only thing telling this
    // suspension apart from an ordinary top-level one. A rename would make
    // nested deadlines arm again, so it fails here rather than there.
    expect(
      (rehydrated?.suspendPayload as Record<string, Record<string, unknown>>)
        .nested?.[MASTRA_WORKFLOW_META_KEY],
    ).toMatchObject({ path: ['approval'] });
  });

  it.each([
    ['a plain step id', 'plain-gate'],
    ['a step id containing a dot', 'dotted.gate'],
  ])('derives the same deadline from the live summary and from status() for %s', async (_label, stepId) => {
    // The contract the whole design rests on: a lifecycle boundary arms from
    // the live summary while the alarm fences against the rehydrated one, so
    // the two must derive identical entries. They key `suspended`
    // differently for a dotted id — ['a.b'] live, ['a','b'] rehydrated — and
    // an entry derived from one that the other cannot recognize is a deadline
    // that arms and then silently disappears.
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: 'none' },
    );
    const gate = createStep({
      id: stepId,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
      execute: async ({ resumeData, suspend }) =>
        resumeData
          ? { settledBy: 'signal' }
          : suspend({
              reason: 'awaiting signal',
              [SUSPENSION_DEADLINE_PAYLOAD_KEY]: SUSPENSION_DEADLINE_MS,
            }),
    });
    createWorkflow({
      id: `projection-${stepId}`,
      inputSchema: z.object({}),
      outputSchema: z.object({ settledBy: z.string() }),
    })
      .then(gate)
      .commit();

    const live = await runtime.start(`projection-${stepId}`, {
      runId: crypto.randomUUID(),
      inputData: {},
    });
    const rehydrated = await runtime.status(`projection-${stepId}`, live.runId);

    expect(suspensionDeadlinesOf(live).entries).toEqual([
      {
        step: stepId,
        deadlineAt:
          (live.suspendedAt?.[stepId] as number) + SUSPENSION_DEADLINE_MS,
        suspendedAt: live.suspendedAt?.[stepId],
        resumeCount: 0,
      },
    ]);
    expect(suspensionDeadlinesOf(rehydrated as RunSummary)).toEqual(
      suspensionDeadlinesOf(live),
    );
  });

  it('refuses to answer authoritatively from Mastra in-memory fallback state', async () => {
    // The fallback reports the in-memory run's lifecycle status without its
    // suspended paths. Authoritative reads refuse it through isFromInMemory.
    const { runtime, storage, workflow, start } =
      timedGateRuntime('marker-gate');
    const started = await start();
    expect(started.status).toBe('suspended');

    const restore = await blindWorkflowRow(storage);
    try {
      const state = (await workflow.getWorkflowRunById(
        started.runId,
      )) as Record<string, unknown>;
      expect(state.isFromInMemory).toBe(true);
      expect(state.status).toBe('suspended');
      expect(state.requestContext).toBeUndefined();
      const fallback = (await runtime.status(
        'marker-gate',
        started.runId,
      )) as RunSummary;
      expect(
        (fallback as unknown as { isFromInMemory?: boolean }).isFromInMemory,
      ).toBeUndefined();

      await expect(
        runtime.authoritativeStatus('marker-gate', started.runId),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
      await expect(
        runtime.authoritativeStatus('marker-gate', started.runId),
      ).rejects.toThrow(
        new RegExp(
          `^run '${started.runId}' of workflow 'marker-gate' state is not readable$`,
        ),
      );

      expect(fallback.status).toBe('suspended');
      expect(fallback.suspended).toEqual([]);
      expect(isReadableRunSummary(fallback)).toBe(false);
    } finally {
      restore();
    }

    const healed = await runtime.status('marker-gate', started.runId);
    expect(healed?.status).toBe('suspended');
    expect(healed?.suspended).toEqual([['gate']]);
  });

  it('throws the class a consumer catches through the package barrel', async () => {
    // #given — a host catching this failure imports it from the barrel, never
    // from the module that throws it. Anything that turned the re-export into
    // a second declaration would leave every consumer `instanceof` false while
    // both files still compiled.
    const { runtime, storage, start } = timedGateRuntime('barrel-gate');
    const started = await start();
    const restore = await blindWorkflowRow(storage);

    // #when / #then
    try {
      await expect(
        runtime.authoritativeStatus('barrel-gate', started.runId),
      ).rejects.toBeInstanceOf(BarrelRunStateUnreadableError);
    } finally {
      restore();
    }
  });
});

async function runtimeActivationFixture(
  mode: 'fenced' | 'prefixed' | 'custom' = 'fenced',
  provider?: RequestContextProvider,
) {
  const sql = openSqlite() as ReturnType<typeof openSqlite> & { close(): void };
  const binding = sqliteUnitDatabase(sql) as D1DatabaseBinding;
  const storage =
    mode === 'custom'
      ? new InMemoryStore()
      : createD1Storage({ binding, tablePrefix: 'activation_' });
  await storage.init();
  const fence = new ExecutionFenceStore(binding as ExecutionFenceDatabase);
  await fence.seed('open');
  const reservations = new StartIdempotencyStore(
    binding as ExecutionFenceDatabase,
  );
  const app = init(
    { storage },
    {
      executionFence: mode === 'fenced' ? fence : 'none',
      startIdempotency: reservations,
      requestContextForRun: provider,
    },
  );
  const effects = vi.fn();
  const schema = z.looseObject({});
  const workflow = app
    .createWorkflow({
      id: 'activation-workflow',
      inputSchema: schema,
      outputSchema: schema,
    })
    .then(
      app.createStep({
        id: 'gate',
        inputSchema: schema,
        outputSchema: schema,
        resumeSchema: schema,
        execute: async ({ inputData, resumeData, suspend }) => {
          effects();
          return inputData.suspend && !resumeData
            ? suspend({ waiting: true })
            : { done: true };
        },
      }),
    )
    .commit();
  await app.runtime.status(workflow.id, 'initialize');
  const workflows = (await storage.getStore(
    'workflows',
  )) as FencedWorkflowsStorageD1;
  const native = workflows[FENCED_WORKFLOW_STORAGE];
  const capability: FencedWorkflowAdmissionCapability | undefined =
    mode === 'custom' || !native ? undefined : { ...native };
  if (capability)
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: capability,
      writable: true,
      configurable: true,
    });
  const options = (runId = 'activation-run'): StartRunOptions => ({
    runId,
    inputData: {},
    attemptToken: 'H',
    requestedBy: 'owner',
    requestedByKind: 'human',
  });
  const claim = async (runId = 'activation-run', key = 'activation-key') => {
    const reserved = await reservations.reserve({
      key,
      owner: { kind: 'human', id: 'owner' },
      targetKind: 'workflow',
      targetId: workflow.id,
      mintRunId: () => runId,
    });
    const claimed = await reservations.claimReservation(
      reserved.reservation as import('./start-idempotency.js').StartReservationReading,
    );
    assert(claimed);
    return claimed;
  };
  const row = (runId = 'activation-run') =>
    workflows.loadWorkflowSnapshot({ workflowName: workflow.id, runId });
  return {
    ...app,
    sql,
    storage,
    fence,
    reservations,
    workflow,
    workflows,
    capability,
    effects,
    options,
    claim,
    row,
    close: () => sql.close(),
  };
}

describe('Runtime final fence structural admission', () => {
  async function structuralFixture(proof: boolean) {
    const f = await runtimeActivationFixture();
    assert(f.capability);
    const owner = { kind: 'human' as const, id: 'owner' };
    const resources = new D1ResourceOwnershipStore(
      f.capability.database as unknown as ResourceOwnershipDatabase,
    );
    expect(
      await resources.reserveAll(
        [{ kind: 'run', resourceId: 'activation-run' }],
        owner,
        'H',
      ),
    ).toBe(true);
    const claim = proof ? await f.claim() : undefined;
    const reading = await f.fence.transition({
      expected: 'open',
      next: proof ? 'proof-only' : 'open',
      ...(claim ? { proofKey: claim.key } : {}),
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    const startOptions: StartRunOptions = {
      ...f.options(),
      mutationEpoch: reading.mutationEpoch,
      runOwnerGuard: { owner, reservationToken: 'H' },
      ...(claim ? { idempotencyKey: claim.key, startReservation: claim } : {}),
    };
    return {
      ...f,
      claim,
      startOptions,
      database: f.capability.database,
      snapshots: () =>
        f.sql
          .prepare('SELECT * FROM activation_mastra_workflow_snapshot')
          .all(),
      owners: () =>
        f.sql.prepare('SELECT * FROM flowsafe_resource_owners').all(),
      proofRows: () =>
        f.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
    };
  }

  it.each([
    [
      'unkeyed unsupported column',
      false,
      'ALTER TABLE flowsafe_execution_fence ADD COLUMN admission_extension TEXT',
    ],
    [
      'keyed proof nullable-id second row',
      true,
      "INSERT INTO flowsafe_execution_fence (id, state, updated_at) VALUES (NULL, 'open', 0)",
    ],
  ] as const)('refuses the original Runtime initial batch for %s', async (_condition, proof, change) => {
    const f = await structuralFixture(proof);
    try {
      const ownersBefore = f.owners();
      const reservationBefore = f.claim
        ? await f.reservations.readForAdmission(f.claim.key)
        : undefined;
      const batch = f.database.batch.bind(f.database);
      let changedFence: unknown;
      const write = vi
        .spyOn(f.database, 'batch')
        .mockImplementationOnce(async (statements) => {
          expect(f.snapshots()).toEqual([]);
          expect(
            f.sql.prepare('PRAGMA ignore_check_constraints').get(),
          ).toMatchObject({
            ignore_check_constraints: 0,
          });
          f.sql.exec(change);
          changedFence = f.proofRows();
          const result = await batch(statements);
          expect(
            (result as Array<{ results: unknown[] }>).map(
              ({ results }) => results,
            ),
          ).toEqual(statements.map(() => []));
          expect(f.snapshots()).toEqual([]);
          expect(f.owners()).toEqual(ownersBefore);
          expect(f.proofRows()).toEqual(changedFence);
          if (f.claim)
            expect(await f.reservations.readForAdmission(f.claim.key)).toEqual(
              reservationBefore,
            );
          return result;
        });
      await expect(
        f.runtime.start(f.workflow.id, f.startOptions),
      ).rejects.toMatchObject({
        status: 503,
        reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
      });
      expect(write).toHaveBeenCalledOnce();
      expect(f.snapshots()).toEqual([]);
      expect(f.effects).not.toHaveBeenCalled();
      expect(f.owners()).toEqual(ownersBefore);
      expect(f.proofRows()).toEqual(changedFence);
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
      expect(f.workflow.runs.has('activation-run')).toBe(false);
      if (f.claim)
        expect(
          await f.reservations.readForAdmission(f.claim.key),
        ).toMatchObject({
          state: 'reserved',
          binding: { kind: 'unbound' },
        });
      await expect(f.fence.readForAdmission()).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
    } finally {
      f.close();
    }
  });

  it.each([
    false,
    true,
  ])('executes the original Runtime with exact active authority (proof=%s)', async (proof) => {
    const f = await structuralFixture(proof);
    try {
      const ownersBefore = f.owners();
      expect(f.startOptions.mutationEpoch).toBe(1);
      await expect(
        f.runtime.start(f.workflow.id, f.startOptions),
      ).resolves.toMatchObject({ status: 'success' });
      expect(f.effects).toHaveBeenCalledOnce();
      expect(f.snapshots()).toHaveLength(1);
      expect(f.owners()).toEqual(ownersBefore);
      const reading = await f.fence.readForAdmission();
      expect(reading.reading).toMatchObject({
        mutationEpoch: 1,
        requireMutationEpoch: true,
      });
      if (f.claim) {
        const reservation = await f.reservations.readForAdmission(f.claim.key);
        expect(reservation).toMatchObject({
          state: 'terminal',
          binding: { kind: 'bound' },
        });
        if (reservation?.binding.kind !== 'bound')
          throw new Error('missing admitted execution');
        expect(reading.reading.proofExecution).toEqual(
          reservation.binding.execution,
        );
      } else expect(reading.reading.proofExecution).toBeUndefined();
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
    } finally {
      f.close();
    }
  });

  it('rejects a lost response after structural refusal without entering the engine', async () => {
    const f = await structuralFixture(true);
    assert(f.claim);
    try {
      const before = await f.reservations.readForAdmission(f.claim.key);
      const ownersBefore = f.owners();
      const lost = new Error('refused initial batch response lost');
      const batch = f.database.batch.bind(f.database);
      const write = vi
        .spyOn(f.database, 'batch')
        .mockImplementationOnce(async (statements) => {
          f.sql.exec(
            'ALTER TABLE flowsafe_execution_fence ADD COLUMN admission_extension TEXT',
          );
          await batch(statements);
          expect(f.snapshots()).toEqual([]);
          throw lost;
        });
      await expect(
        f.runtime.start(f.workflow.id, f.startOptions),
      ).rejects.toMatchObject({
        status: 503,
        reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
        cause: lost,
      });
      expect(write).toHaveBeenCalledTimes(2);
      expect(f.snapshots()).toEqual([]);
      expect(f.effects).not.toHaveBeenCalled();
      expect(f.owners()).toEqual(ownersBefore);
      expect(await f.reservations.readForAdmission(f.claim.key)).toEqual(
        before,
      );
      expect(f.proofRows()).toEqual([
        expect.objectContaining({
          proof_run_id: null,
          proof_start_token: null,
        }),
      ]);
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it('independently mints S when H repeats and retains immutable owner and epoch on resume', async () => {
    const f = await runtimeActivationFixture('fenced', () => ({
      'flowsafe.runProvenance': { version: 2, startToken: 'forged' },
    }));
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
        mutationEpoch: 0,
      });
      const first = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      assert(first);
      await f.runtime.start(f.workflow.id, f.options('second'));
      const second = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'second',
      );
      expect(second?.execution.startToken).not.toBe(first.execution.startToken);
      expect(first.execution.startToken).not.toBe('H');
      await expect(
        f.runtime.resume(f.workflow.id, 'activation-run', {
          resumeData: { go: true },
          requestedBy: 'reviewer',
          requestedByKind: 'service',
        }),
      ).resolves.toMatchObject({ status: 'success', requestedBy: 'reviewer' });
      const resumed = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      expect(resumed?.provenance).toMatchObject({
        version: 2,
        startToken: first.execution.startToken,
        mutationEpoch: 0,
        startIdentity: { owner: { kind: 'human', id: 'owner' } },
        resumeCounts: [['gate', 1]],
      });
      expect(resumed?.provenance.attemptToken).not.toBe('H');
      expect(f.effects).toHaveBeenCalledTimes(3);
    } finally {
      f.close();
    }
  });

  it.each([
    'prefixed',
    'custom',
  ] as const)('binds the original claim before ordinary create with the actual %s namespace', async (mode) => {
    const f = await runtimeActivationFixture(mode);
    try {
      const claim = await f.claim();
      const create = f.workflow.createRun.bind(f.workflow);
      let beforeCreate: unknown;
      vi.spyOn(f.workflow, 'createRun').mockImplementation(async (...args) => {
        beforeCreate = await f.reservations.readForAdmission(claim.key);
        return create(...args);
      });
      await expect(
        f.runtime.start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
        }),
      ).resolves.toMatchObject({ status: 'success' });
      expect(beforeCreate).toMatchObject({
        binding: {
          kind: 'bound',
          execution: { tablePrefix: mode === 'custom' ? null : 'activation_' },
        },
      });
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'terminal',
      );
      expect(f.effects).toHaveBeenCalledOnce();
    } finally {
      f.close();
    }
  });

  it.each([
    'missing',
    'foreign',
  ] as const)('requires its positive witness before engine entry: %s', async (mode) => {
    const f = await runtimeActivationFixture();
    assert(f.capability);
    const original = f.capability.withInitialAdmission;
    let prepared: D1RunExecutionIdentity | undefined;
    try {
      f.capability.withInitialAdmission = async (input, create) => {
        const result = await original(input, create);
        return mode === 'missing'
          ? ({ ...result, witness: undefined } as never)
          : {
              ...result,
              witness: {
                ...result.witness,
                execution: {
                  ...result.witness.execution,
                  startToken: 'foreign',
                },
              },
            };
      };
      const claim = await f.claim();
      const outcome = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
          onPreparedStartIdentity: (identity) => {
            prepared = normalizeD1RunExecutionIdentity(identity);
          },
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect(
        (await f.reservations.readForAdmission(claim.key))?.binding,
      ).toMatchObject({ kind: 'bound', execution: prepared });
      expect(f.effects).not.toHaveBeenCalled();
      expect(outcome).toBeInstanceOf(RunStateUnreadableError);
      expect(f.workflow.runs.has('activation-run')).toBe(true);
    } finally {
      f.close();
    }
  });

  it('clears only its captured new cache entry after a definitive same-S refusal', async () => {
    const f = await runtimeActivationFixture();
    try {
      const claim = await f.claim();
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
          onPreparedStartIdentity: async () => {
            await f.fence.transition({ expected: 'open', next: 'draining' });
          },
        })
        .catch((error) => error);
      expect(await f.row()).toBeNull();
      expect(f.workflow.runs.has('activation-run')).toBe(false);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'reserved',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(Error);
    } finally {
      f.close();
    }
  });

  it.each([
    'provider',
    'prepared',
    'engine',
    'terminal',
    'terminal-repair',
  ] as const)('retains its active frame during the %s wait', async (phase) => {
    const entered = deferredSignal(),
      release = deferredSignal();
    const f = await runtimeActivationFixture(
      'fenced',
      phase === 'provider'
        ? async () => {
            entered.resolve();
            await release.promise;
            return {};
          }
        : undefined,
    );
    let waiting: Promise<RunSummary> | undefined;
    try {
      if (phase === 'engine') f.effects.mockImplementationOnce(async () => {});
      const create = f.workflow.createRun.bind(f.workflow);
      if (phase === 'engine')
        vi.spyOn(f.workflow, 'createRun').mockImplementation(
          async (...args) => {
            const run = await create(...args);
            const start = run.start.bind(run);
            vi.spyOn(run, 'start').mockImplementation(async (...input) => {
              entered.resolve();
              await release.promise;
              return start(...input);
            });
            return run;
          },
        );
      if (phase === 'terminal-repair')
        f.workflow.options.shouldPersistSnapshot = ({ workflowStatus }) =>
          workflowStatus === 'pending';
      if (phase === 'terminal' || phase === 'terminal-repair') {
        const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
        vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
          async (input) => {
            if (input.snapshot.status === 'success') {
              entered.resolve();
              await release.promise;
            }
            await persist(input);
          },
        );
      }
      waiting = f.runtime.start(f.workflow.id, {
        ...f.options(),
        onPreparedStartIdentity:
          phase === 'prepared'
            ? async () => {
                entered.resolve();
                await release.promise;
              }
            : undefined,
      });
      await entered.promise;
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(true);
      release.resolve();
      await expect(waiting).resolves.toMatchObject({ status: 'success' });
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
    } finally {
      release.resolve();
      await waiting?.catch(() => undefined);
      f.close();
    }
  });

  it.each([
    'preflight',
    'callback',
    'unknown-create',
  ] as const)('releases only local preflight with the original claim: %s', async (phase) => {
    const f = await runtimeActivationFixture(
      phase === 'unknown-create' ? 'prefixed' : 'fenced',
    );
    try {
      const claim = await f.claim();
      if (phase === 'preflight')
        await f.fence.transition({ expected: 'open', next: 'draining' });
      if (phase === 'unknown-create')
        vi.spyOn(f.workflow, 'createRun').mockRejectedValue(
          new Error('unknown persistence'),
        );
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
          onPreparedStartIdentity:
            phase === 'callback'
              ? () => {
                  throw { status: 503, reason: { code: 'RUN_START_PENDING' } };
                }
              : undefined,
        })
        .catch((error) => error);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        phase === 'preflight' ? 'reserved' : 'started',
      );
      expect(
        (await f.reservations.readForAdmission(claim.key))?.binding.kind,
      ).toBe(phase === 'unknown-create' ? 'bound' : 'unbound');
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeDefined();
    } finally {
      f.close();
    }
  });

  it.each([
    'S2',
    'v1',
    'tokenless',
    'pending',
  ] as const)('does not recover a replacement %s after a lost engine result with repeated H', async (replacement) => {
    const f = await runtimeActivationFixture();
    const fault = new Error('lost original result');
    try {
      const create = f.workflow.createRun.bind(f.workflow);
      vi.spyOn(f.workflow, 'createRun').mockImplementation(async (...args) => {
        const run = await create(...args),
          start = run.start.bind(run);
        vi.spyOn(run, 'start').mockImplementation(async (...input) => {
          await start(...input);
          const snapshot = await f.row();
          assert(snapshot);
          const provenance =
            snapshot.requestContext?.['flowsafe.runProvenance'];
          assert(provenance);
          if (replacement === 'S2') provenance.startToken = 'S2';
          if (replacement === 'v1') provenance.version = 1;
          if (replacement === 'tokenless')
            delete snapshot.requestContext?.['flowsafe.runProvenance'];
          if (replacement === 'pending') snapshot.status = 'pending';
          await f.workflows.persistWorkflowSnapshot({
            workflowName: f.workflow.id,
            runId: 'activation-run',
            snapshot,
          });
          throw fault;
        });
        return run;
      });
      const result = await f.runtime
        .start(f.workflow.id, f.options())
        .catch((error) => error);
      const row = await f.row();
      expect(row?.status).toBe(
        replacement === 'pending' ? 'pending' : 'success',
      );
      if (replacement === 'S2')
        expect(row?.requestContext?.['flowsafe.runProvenance'].startToken).toBe(
          'S2',
        );
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result).toBe(fault);
    } finally {
      f.close();
    }
  });

  it.each([
    false,
    undefined,
    1,
    'yes',
  ])('requires awaited literal true owning quiescence: %s', async (quiescent) => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, f.options());
      const state = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      assert(state?.storage === 'd1');
      const before = await f.row();
      const result = await f.runtime
        .recoverStartAttempt(state.execution, {
          attemptToken: 'H',
          isOwnerQuiescent: async () => quiescent as boolean,
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect(await f.row()).toEqual(before);
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it('recovers a progressed same-S resume with its new leg H', async () => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
      });
      const initial = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      assert(initial?.storage === 'd1');
      await f.runtime.resume(f.workflow.id, 'activation-run', {
        resumeData: { go: true },
      });
      await expect(
        f.runtime.recoverStartAttempt(initial.execution, {
          attemptToken: 'H',
          isOwnerQuiescent: async () => true,
          startLeg: 'unwound',
        }),
      ).resolves.toMatchObject({
        kind: 'ordinary',
        summary: { status: 'success' },
      });
      expect(f.effects).toHaveBeenCalledTimes(2);
    } finally {
      f.close();
    }
  });

  it.each([
    'fenced',
    'prefixed',
    'custom',
  ] as const)('rejects normal start pending from captured %s storage before settlement', async (mode) => {
    const f = await runtimeActivationFixture(mode);
    let prepared: D1RunExecutionIdentity | undefined;
    try {
      const claim = await f.claim();
      const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
      vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
        async (input) => {
          if (input.snapshot.status === 'pending') return persist(input);
          const pending = { ...input.snapshot, status: 'pending' as const };
          return persist({ ...input, snapshot: pending });
        },
      );
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          inputData: { suspend: true },
          startReservation: claim,
          idempotencyKey: claim.key,
          onPreparedStartIdentity: (identity) => {
            if (identity.tablePrefix !== null)
              prepared = normalizeD1RunExecutionIdentity(identity);
          },
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStartPendingError);
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
      if (prepared)
        expect(
          (await f.reservations.readForAdmission(claim.key))?.binding,
        ).toMatchObject({ execution: prepared });
    } finally {
      f.close();
    }
  });

  it('rejects normal resume pending even when terminal repair acknowledges the write', async () => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
      });
      const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
      vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
        (input) =>
          persist({
            ...input,
            snapshot: { ...input.snapshot, status: 'pending' as const },
          }),
      );
      const result = await f.runtime
        .resume(f.workflow.id, 'activation-run', { resumeData: { go: true } })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect(f.effects).toHaveBeenCalledTimes(2);
      expect(result).toBeInstanceOf(RunStartPendingError);
    } finally {
      f.close();
    }
  });
});

async function preparedPendingFixture(
  f: Awaited<ReturnType<typeof runtimeActivationFixture>>,
) {
  assert(f.capability);
  const admit = f.capability.withInitialAdmission;
  let execution: D1RunExecutionIdentity | undefined;
  const claim = await f.claim();
  f.capability.withInitialAdmission = async (input, create) => {
    const result = await admit(input, create);
    return { ...result, witness: undefined } as never;
  };
  try {
    await expect(
      f.runtime.start(f.workflow.id, {
        ...f.options(),
        startReservation: claim,
        idempotencyKey: claim.key,
        onPreparedStartIdentity: (value) => {
          execution = normalizeD1RunExecutionIdentity(value);
        },
      }),
    ).rejects.toBeInstanceOf(RunStateUnreadableError);
    assert(execution);
    return { execution, claim };
  } finally {
    f.capability.withInitialAdmission = admit;
  }
}

describe('Runtime activation', () => {
  it('captures the actual alternate prefixed domain and method receivers through preparation', async () => {
    const nominal = await runtimeActivationFixture('prefixed'),
      actual = await runtimeActivationFixture('prefixed');
    const entered = deferredSignal(),
      release = deferredSignal();
    let pending: Promise<RunSummary> | undefined;
    try {
      const nominalMastra = nominal.workflow.mastra;
      assert(nominalMastra);
      const nominalSource = vi
        .spyOn(nominalMastra, 'getStorage')
        .mockReturnValue(actual.storage);
      // A participating reservation store must match the selected D1 binding.
      const refused = await nominal.runtime
        .start(nominal.workflow.id, nominal.options())
        .catch((error) => error);
      expect(await actual.row()).toBeNull();
      expect(await nominal.row()).toBeNull();
      expect(nominal.effects).not.toHaveBeenCalled();
      expect(refused).toBeInstanceOf(Error);
      expect((refused as Error).message).toBe(
        'workflow storage binding disagrees with runtime stores',
      );
      nominalSource.mockRestore();
      const standalone = init(
        { storage: nominal.storage },
        { executionFence: 'none', startIdempotency: 'none' },
      ).runtime;
      standalone.register(nominal.workflow);
      await standalone.status(nominal.workflow.id, 'initialize-actual');
      const standaloneMastra = nominal.workflow.mastra;
      assert(standaloneMastra);
      vi.spyOn(standaloneMastra, 'getStorage').mockReturnValue(actual.storage);
      const source = actual.capability;
      assert(source);
      const receivers: unknown[] = [];
      const read = source.readSnapshot;
      source.readSnapshot = async function (address) {
        receivers.push(this);
        return read.call(this, address);
      };
      pending = standalone.start(nominal.workflow.id, {
        ...nominal.options(),
        onPreparedStartIdentity: async (identity) => {
          expect(identity.tablePrefix).toBe('activation_');
          entered.resolve();
          await release.promise;
        },
      });
      await entered.promise;
      const late = vi
        .fn()
        .mockRejectedValue(new Error('late replacement read'));
      source.readSnapshot = late;
      release.resolve();
      await expect(pending).resolves.toMatchObject({ status: 'success' });
      expect(await nominal.row()).toBeNull();
      expect((await actual.row())?.status).toBe('success');
      expect(late).not.toHaveBeenCalled();
      expect(receivers).toEqual([actual.capability]);
    } finally {
      release.resolve();
      await pending?.catch(() => undefined);
      nominal.close();
      actual.close();
    }
  });

  it('terminalizes the original selected pending bytes and returns recovered S1 after a later S2 write', async () => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      Reflect.deleteProperty(f.capability, 'touchRun');
      const original = f.capability.terminalizeInitialAdmission;
      const read = vi.spyOn(f.capability, 'readSnapshot');
      f.capability.terminalizeInitialAdmission = async (input) => {
        const outcome = await original(input);
        if (outcome.kind !== 'conflict') {
          const later = JSON.parse(outcome.row.snapshot);
          later.requestContext['flowsafe.runProvenance'].startToken = 'S2';
          later.result = { foreign: true };
          await f.workflows.persistWorkflowSnapshot({
            workflowName: f.workflow.id,
            runId: 'activation-run',
            snapshot: later,
          });
        }
        return outcome;
      };
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: async () => true,
          startReservation: claim,
          startLeg: 'touched',
        })
        .catch((error) => error);
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].startToken,
      ).toBe('S2');
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'terminal',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(read).toHaveBeenCalledOnce();
      expect(result).toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });
    } finally {
      f.close();
    }
  });

  it.each([
    { evidence: 'unwound', startLeg: 'unwound', stamped: true },
    { evidence: 'touched', startLeg: 'touched', stamped: true },
    {
      evidence: 'touched without a touch',
      startLeg: 'touched',
      stamped: false,
    },
  ] as const)('repairs a stopped start with leg evidence $evidence, refusing the leg a later write when stamped', async ({
    evidence,
    startLeg,
    stamped,
  }) => {
    // #given an admitted start whose leg stopped before its first write, its
    // row silent for six minutes
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      if (evidence === 'touched without a touch')
        Reflect.deleteProperty(f.capability, 'touchRun');
      const admitted = await f.row();
      assert(admitted);
      const now = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 360_001);
      const recovered = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: async () => true,
          startReservation: claim,
          startLeg,
        })
        .finally(() => now.mockRestore());
      expect(recovered).toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });

      // #when the stopped leg's engine writes on
      const late = await f.workflows
        .persistWorkflowSnapshot({
          workflowName: f.workflow.id,
          runId: 'activation-run',
          snapshot: { ...admitted, status: 'running' },
        })
        .catch((error: unknown) => error);

      // #then a stamped repair stands
      expect((await f.row())?.status).toBe(stamped ? 'failed' : 'running');
      if (stamped) expect(late).toBeInstanceOf(RunSettledConflictError);
    } finally {
      f.close();
    }
  });

  it('stamps a repaired start without changing its summary', async () => {
    // #given the same stopped start repaired on storage without the touch,
    // which does not stamp, and with the stamp
    const summaries: Record<string, unknown>[] = [];
    for (const stamped of [false, true]) {
      const f = await runtimeActivationFixture();
      try {
        const { execution, claim } = await preparedPendingFixture(f);
        assert(f.capability);
        if (!stamped) Reflect.deleteProperty(f.capability, 'touchRun');

        // #when
        const recovered = await f.runtime.recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: async () => true,
          startReservation: claim,
          startLeg: stamped ? 'unwound' : 'touched',
        });

        assert(recovered?.kind === 'ordinary');
        const {
          createdAt: _created,
          updatedAt: _updated,
          ...summary
        } = recovered.summary;
        summaries.push(summary);
      } finally {
        f.close();
      }
    }

    // #then
    expect(summaries[1]).toEqual(summaries[0]);
  });

  it('leaves a start whose leg marks its row live to that leg until the row is silent', async () => {
    // #given an admitted start whose row was written moments ago
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      const address = { workflowId: f.workflow.id, runId: 'activation-run' };
      const before = await f.capability.readSnapshot(address);

      // #when recovery runs for a leg that touches its row
      const recovering = f.runtime.recoverStartAttempt(execution, {
        attemptToken: 'H',
        isOwnerQuiescent: async () => true,
        startReservation: claim,
        startLeg: 'touched',
      });

      // #then the start stays pending and its row untouched
      await expect(recovering).rejects.toBeInstanceOf(RunStartPendingError);
      expect(await f.capability.readSnapshot(address)).toEqual(before);
    } finally {
      f.close();
    }
  });

  it('preserves the captured source across owning quiescence and read replacement', async () => {
    const f = await runtimeActivationFixture(),
      other = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      assert(other.capability);
      const foreign = vi.spyOn(other.capability, 'terminalizeInitialAdmission');
      const own = vi.spyOn(f.capability, 'terminalizeInitialAdmission');
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: claim,
          startLeg: 'unwound',
          isOwnerQuiescent: async () => {
            Object.defineProperty(f.workflows, FENCED_WORKFLOW_STORAGE, {
              value: other.capability,
            });
            return true;
          },
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('failed');
      expect(await other.row()).toBeNull();
      expect(f.effects).not.toHaveBeenCalled();
      expect(own).toHaveBeenCalledOnce();
      expect(foreign).not.toHaveBeenCalled();
      expect(result).toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });
    } finally {
      f.close();
      other.close();
    }
  });

  it.each([
    'unmarked',
    'resumed',
    'different-H',
    'conflict',
  ] as const)('keeps uncertain initial %s nonreplayable', async (mode) => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      const snapshot = await f.row();
      assert(snapshot);
      const provenance = snapshot.requestContext?.['flowsafe.runProvenance'];
      assert(provenance);
      if (mode === 'unmarked') delete provenance.initialAdmission;
      if (mode === 'resumed') provenance.resumeCounts = [['gate', 1]];
      if (mode === 'different-H') provenance.attemptToken = 'other';
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      if (mode === 'conflict')
        f.capability.terminalizeInitialAdmission = async () => ({
          kind: 'conflict',
        });
      const before = await f.row();
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: claim,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect(await f.row()).toEqual(before);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(
        mode === 'conflict'
          ? ExecutionFenceUnreadableError
          : RunStartPendingError,
      );
    } finally {
      f.close();
    }
  });

  it('blocks keyed terminal recovery when exact settlement fails and retries that settlement', async () => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      const settle = vi
        .spyOn(f.reservations, 'settleExecution')
        .mockRejectedValueOnce(new Error('settle unavailable'));
      const refused = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: claim,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('failed');
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(refused).toBeInstanceOf(ExecutionFenceUnreadableError);
      await expect(
        f.runtime.recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: claim,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        }),
      ).resolves.toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });
      expect(settle).toHaveBeenCalledTimes(2);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'terminal',
      );
    } finally {
      f.close();
    }
  });

  it.each([
    'wrong-owner',
    'wrong-target',
    'missing-store',
  ] as const)('refuses keyed recovery with %s authority', async (mode) => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      const runtime =
        mode === 'missing-store'
          ? init(
              { storage: f.storage },
              { executionFence: f.fence, startIdempotency: 'none' },
            ).runtime
          : f.runtime;
      if (runtime !== f.runtime) runtime.register(f.workflow);
      const original =
        mode === 'wrong-owner'
          ? { ...claim, owner: { kind: 'human' as const, id: 'other' } }
          : mode === 'wrong-target'
            ? { ...claim, targetId: 'other' }
            : claim;
      const before = await f.row();
      const result = await runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: original,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect(await f.row()).toEqual(before);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'provider',
    'preparation',
    'create',
    'fence-wait',
  ] as const)('retains the originally admitted proof S through the %s wait', async (phase) => {
    const entered = deferredSignal(),
      release = deferredSignal();
    let resumePhase = false;
    const f = await runtimeActivationFixture(
      'fenced',
      phase === 'provider'
        ? async () => {
            if (resumePhase) {
              entered.resolve();
              await release.promise;
            }
            return {};
          }
        : undefined,
    );
    let pending: Promise<RunSummary> | undefined;
    try {
      const claim = await f.claim();
      await f.fence.transition({
        expected: 'open',
        next: 'proof-only',
        proofKey: claim.key,
      });
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
        idempotencyKey: claim.key,
        startReservation: claim,
      });
      const before = await f.row();
      assert(before);
      resumePhase = true;
      if (phase === 'create') {
        const create = f.workflow.createRun.bind(f.workflow);
        vi.spyOn(f.workflow, 'createRun').mockImplementation(
          async (...args) => {
            const run = await create(...args);
            entered.resolve();
            await release.promise;
            return run;
          },
        );
      }
      if (phase === 'fence-wait') {
        const read = f.fence.read.bind(f.fence);
        let count = 0;
        vi.spyOn(f.fence, 'read').mockImplementation(async () => {
          const result = await read();
          if (++count === 2) {
            entered.resolve();
            await release.promise;
          }
          return result;
        });
      }
      pending = f.runtime.resume(f.workflow.id, 'activation-run', {
        resumeData: { go: true },
        prepareExecution:
          phase === 'preparation'
            ? async () => {
                entered.resolve();
                await release.promise;
              }
            : undefined,
      });
      void pending.catch(() => undefined);
      await entered.promise;
      assert(before.requestContext);
      before.requestContext['flowsafe.runProvenance'].startToken = 'S2';
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot: before,
      });
      release.resolve();
      const result = await pending.catch((error) => error);
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].startToken,
      ).toBe('S2');
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      release.resolve();
      await pending?.catch(() => undefined);
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it.each([
    'cancelled',
    'timed_out',
  ] as const)('recovers saved %s intent as a lifecycle descriptor and preserves completed cleanup', async (status) => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      const snapshot = await f.row();
      assert(snapshot?.requestContext);
      snapshot.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: 1,
        transitionIntent: {
          status,
          requestedAt: 1,
          replayPrincipals: [{ kind: 'service', id: 'source-owner' }],
        },
      };
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      const result = await f.runtime.recoverStartAttempt(execution, {
        attemptToken: 'H',
        startReservation: claim,
        isOwnerQuiescent: () => true,
        startLeg: 'unwound',
      });
      expect((await f.row())?.status).toBe(status);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'terminal',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toMatchObject({
        kind: 'lifecycle',
        transition: {
          transitioned: true,
          casMatched: true,
          summary: { status },
          cleanup: { cleanupCompleted: false, status },
        },
      });
      assert(result?.kind === 'lifecycle');
      await f.runtime.completeTerminalCleanup(
        f.workflow.id,
        'activation-run',
        result.transition.cleanup.revision,
      );
      await expect(
        f.runtime.recoverStartAttempt(execution, {
          attemptToken: 'H',
          startReservation: claim,
          isOwnerQuiescent: () => true,
          startLeg: 'unwound',
        }),
      ).resolves.toMatchObject({
        kind: 'lifecycle',
        transition: {
          transitioned: false,
          cleanup: { cleanupCompleted: true },
        },
      });
    } finally {
      f.close();
    }
  });

  it('preserves a raw pending row with terminal-looking metadata on public status reads', async () => {
    const f = await runtimeActivationFixture();
    try {
      await preparedPendingFixture(f);
      const snapshot = await f.row();
      assert(snapshot?.requestContext);
      snapshot.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: 1,
        terminal: {
          status: 'cancelled',
          transitionedAt: 1,
          error: { code: 'CANCELLED', message: 'run was cancelled' },
          replayPrincipals: [{ kind: 'human', id: 'owner' }],
        },
      };
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      const before = await f.row();
      for (const method of ['status', 'authoritativeStatus'] as const)
        await expect(
          f.runtime[method](f.workflow.id, 'activation-run'),
        ).rejects.toBeInstanceOf(RunStartPendingError);
      const observation = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      expect(await f.row()).toEqual(before);
      expect(f.effects).not.toHaveBeenCalled();
      expect(observation).toMatchObject({ kind: 'initial' });
      expect(observation).not.toHaveProperty('summary');
    } finally {
      f.close();
    }
  });

  it('permits capable no-fence initial pending omission followed by a real nonpending result', async () => {
    const f = await runtimeActivationFixture('prefixed');
    try {
      f.workflow.options.shouldPersistSnapshot = ({ workflowStatus }) =>
        workflowStatus !== 'pending';
      const persist = vi.spyOn(f.workflows, 'persistWorkflowSnapshot');
      await expect(
        f.runtime.start(f.workflow.id, f.options()),
      ).resolves.toMatchObject({ status: 'success' });
      expect((await f.row())?.status).toBe('success');
      expect(
        persist.mock.calls.some(
          ([input]) => input.snapshot.status === 'pending',
        ),
      ).toBe(false);
      expect(f.effects).toHaveBeenCalledOnce();
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it.each([
    'start',
    'resume',
  ] as const)('does not replace a selected pending normal %s observation with a later readable result', async (leg) => {
    const f = await runtimeActivationFixture();
    try {
      if (leg === 'resume')
        await f.runtime.start(f.workflow.id, {
          ...f.options(),
          inputData: { suspend: true },
        });
      assert(f.capability);
      const read = f.capability.readSnapshot;
      const selected = vi
        .spyOn(f.capability, 'readSnapshot')
        .mockImplementationOnce(async (address) => {
          const row = await read(address);
          assert(row);
          const snapshot = JSON.parse(row.snapshot);
          snapshot.status = 'pending';
          return { ...row, snapshot: JSON.stringify(snapshot) };
        });
      const result = await (leg === 'start'
        ? f.runtime.start(f.workflow.id, f.options())
        : f.runtime.resume(f.workflow.id, 'activation-run', {
            resumeData: { go: true },
          })
      ).catch((error) => error);
      expect((await f.row())?.status).toBe('success');
      expect(f.effects).toHaveBeenCalledTimes(leg === 'start' ? 1 : 2);
      expect(selected).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStartPendingError);
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it.each([
    'skip',
    'swallow',
    'reject',
    'response-loss',
  ] as const)('prevents unwitnessed engine entry and preserves response-loss evidence: %s', async (mode) => {
    const f = await runtimeActivationFixture();
    try {
      const claim = await f.claim();
      assert(f.capability);
      if (mode === 'skip')
        f.workflow.options.shouldPersistSnapshot = () => false;
      if (mode === 'swallow')
        vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockResolvedValueOnce(
          undefined,
        );
      if (mode === 'reject')
        vi.spyOn(f.capability.database, 'batch').mockRejectedValueOnce(
          new Error('initial SQL unavailable'),
        );
      if (mode === 'response-loss') {
        const batch = f.capability.database.batch.bind(f.capability.database);
        vi.spyOn(f.capability.database, 'batch').mockImplementationOnce(
          async (statements) => {
            await batch(statements);
            throw new Error('lost SQL response');
          },
        );
      }
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
        })
        .catch((error) => error);
      expect((await f.row())?.status ?? null).toBe(
        mode === 'response-loss' ? 'success' : null,
      );
      expect(f.effects).toHaveBeenCalledTimes(mode === 'response-loss' ? 1 : 0);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        mode === 'response-loss' ? 'terminal' : 'started',
      );
      if (mode === 'response-loss')
        expect(result).toMatchObject({ status: 'success' });
      else expect(result).toBeInstanceOf(Error);
    } finally {
      f.close();
    }
  });

  it('preserves a replacement cache and tokenless row after its own no-insert refusal', async () => {
    const f = await runtimeActivationFixture(),
      other = await runtimeActivationFixture('prefixed');
    try {
      const replacement = await other.workflow.createRun({
        runId: 'activation-run',
      });
      assert(f.capability);
      const native = f.capability.withInitialAdmission;
      f.capability.withInitialAdmission = async (input, create) => {
        try {
          return await native(input, create);
        } catch (error) {
          const tokenless = await other.row();
          assert(tokenless);
          await f.workflows.persistWorkflowSnapshot({
            workflowName: f.workflow.id,
            runId: 'activation-run',
            snapshot: tokenless,
          });
          f.workflow.runs.set('activation-run', replacement);
          throw error;
        }
      };
      const claim = await f.claim();
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
          onPreparedStartIdentity: async () => {
            await f.fence.transition({ expected: 'open', next: 'draining' });
          },
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect((await f.row())?.requestContext).toBeUndefined();
      expect(f.workflow.runs.get('activation-run')).toBe(replacement);
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(Error);
    } finally {
      f.close();
      other.close();
    }
  });

  it('installs the frame before the existence read and keeps it during result recovery', async () => {
    const entered = deferredSignal(),
      release = deferredSignal(),
      reading = deferredSignal(),
      readRelease = deferredSignal();
    const f = await runtimeActivationFixture();
    let pending: Promise<RunSummary> | undefined;
    try {
      const existing = f.workflow.getWorkflowRunById.bind(f.workflow);
      vi.spyOn(f.workflow, 'getWorkflowRunById').mockImplementationOnce(
        async (...args) => {
          entered.resolve();
          await release.promise;
          return existing(...args);
        },
      );
      const create = f.workflow.createRun.bind(f.workflow);
      vi.spyOn(f.workflow, 'createRun').mockImplementation(async (...args) => {
        const run = await create(...args),
          start = run.start.bind(run);
        vi.spyOn(run, 'start').mockImplementation(async (...input) => {
          await start(...input);
          throw new Error('lost engine result');
        });
        return run;
      });
      assert(f.capability);
      const read = f.capability.readSnapshot;
      f.capability.readSnapshot = async (address) => {
        reading.resolve();
        await readRelease.promise;
        return read(address);
      };
      pending = f.runtime.start(f.workflow.id, f.options());
      await entered.promise;
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(true);
      release.resolve();
      await reading.promise;
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(true);
      readRelease.resolve();
      await expect(pending).resolves.toMatchObject({ status: 'success' });
      expect(f.runtime.isRunActive(f.workflow.id, 'activation-run')).toBe(
        false,
      );
    } finally {
      release.resolve();
      readRelease.resolve();
      await pending?.catch(() => undefined);
      f.close();
    }
  });

  it('refuses a replacement after early proof selection before invoking the resume provider', async () => {
    const provider = vi.fn(() => ({}));
    const f = await runtimeActivationFixture('fenced', provider);
    try {
      const claim = await f.claim();
      await f.fence.transition({
        expected: 'open',
        next: 'proof-only',
        proofKey: claim.key,
      });
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
        startReservation: claim,
        idempotencyKey: claim.key,
      });
      provider.mockClear();
      assert(f.capability);
      const read = f.capability.readSnapshot;
      vi.spyOn(f.capability, 'readSnapshot').mockImplementationOnce(
        async (address) => {
          const original = await read(address);
          const snapshot = await f.row();
          assert(snapshot?.requestContext);
          snapshot.requestContext['flowsafe.runProvenance'].startToken = 'S2';
          await f.workflows.persistWorkflowSnapshot({
            workflowName: f.workflow.id,
            runId: 'activation-run',
            snapshot,
          });
          return original;
        },
      );
      const result = await f.runtime
        .resume(f.workflow.id, 'activation-run', { resumeData: { go: true } })
        .catch((error) => error);
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].startToken,
      ).toBe('S2');
      expect(provider).not.toHaveBeenCalled();
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it('keeps lifecycle terminal persistence and settlement on the selected source across load replacement', async () => {
    const f = await runtimeActivationFixture(),
      other = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
      });
      await other.runtime.start(other.workflow.id, {
        ...other.options(),
        inputData: { suspend: true },
      });
      const foreignBefore = await other.row();
      const load = f.workflows.loadWorkflowSnapshot.bind(f.workflows);
      vi.spyOn(f.workflows, 'loadWorkflowSnapshot').mockImplementationOnce(
        async (input) => {
          const selected = await load(input);
          vi.spyOn(f.workflow, 'mastra', 'get').mockReturnValue(
            new Mastra({ storage: other.storage, logger: false }),
          );
          return selected;
        },
      );
      const result = await f.runtime.terminate(f.workflow.id, 'activation-run');
      expect((await f.row())?.status).toBe('cancelled');
      expect(await other.row()).toEqual(foreignBefore);
      expect(f.effects).toHaveBeenCalledOnce();
      expect(result.summary.status).toBe('cancelled');
    } finally {
      f.close();
      other.close();
    }
  });
});

describe('Runtime activation', () => {
  it('refuses a Core pending result after a real positive initial admission witness', async () => {
    const f = await runtimeActivationFixture();
    try {
      const claim = await f.claim();
      const create = f.workflow.createRun.bind(f.workflow);
      const starts = vi.fn();
      vi.spyOn(f.workflow, 'createRun').mockImplementation(async (...args) => {
        const run = await create(...args);
        vi.spyOn(run, 'start').mockImplementation(async () => {
          starts();
          return { status: 'pending' } as never;
        });
        return run;
      });
      const result = await f.runtime
        .start(f.workflow.id, {
          ...f.options(),
          startReservation: claim,
          idempotencyKey: claim.key,
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .initialAdmission,
      ).toBe(true);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(starts).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStartPendingError);
    } finally {
      f.close();
    }
  });

  it.each([
    'fenced',
    'prefixed',
    'custom',
  ] as const)('permits missing-provenance terminal repair only for no-fence %s execution', async (mode) => {
    const f = await runtimeActivationFixture(mode);
    try {
      const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
      let stripped = false;
      vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
        async (input) => {
          if (input.snapshot.status === 'success' && !stripped) {
            stripped = true;
            const snapshot = structuredClone(input.snapshot);
            delete snapshot.requestContext;
            return persist({ ...input, snapshot });
          }
          return persist(input);
        },
      );
      const result = await f.runtime
        .start(f.workflow.id, f.options())
        .catch((error) => error);
      expect((await f.row())?.status).toBe('success');
      expect(f.effects).toHaveBeenCalledOnce();
      if (mode === 'fenced') {
        expect((await f.row())?.requestContext).toBeUndefined();
        expect(result).toBeInstanceOf(RunStateUnreadableError);
      } else {
        expect(
          (await f.row())?.requestContext?.['flowsafe.runProvenance'].version,
        ).toBe(2);
        expect(result).toMatchObject({ status: 'success' });
      }
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it('invokes the engine synchronously before a cancellation queued during initial persistence enters the lifecycle lock', async () => {
    const f = await runtimeActivationFixture();
    let cancellation: Promise<unknown> | undefined;
    let engineEntered = false;
    let readBeforeEngine = false;
    try {
      const load = f.workflows.loadWorkflowSnapshot.bind(f.workflows);
      vi.spyOn(f.workflows, 'loadWorkflowSnapshot').mockImplementation(
        async (input) => {
          if (cancellation && !engineEntered) readBeforeEngine = true;
          return load(input);
        },
      );
      const create = f.workflow.createRun.bind(f.workflow);
      vi.spyOn(f.workflow, 'createRun').mockImplementation(async (...args) => {
        const run = await create(...args),
          start = run.start.bind(run);
        vi.spyOn(run, 'start').mockImplementation((...input) => {
          engineEntered = true;
          return start(...input);
        });
        return run;
      });
      assert(f.capability);
      const admit = f.capability.withInitialAdmission;
      f.capability.withInitialAdmission = (input, createRun) =>
        admit(
          {
            ...input,
            onInitialWriteAttempt: () => {
              input.onInitialWriteAttempt();
              cancellation = f.runtime
                .cancelActiveExecution(
                  f.workflow.id,
                  'activation-run',
                  'cancelled',
                  [{ kind: 'human', id: 'owner' }],
                )
                .catch((error) => error);
            },
          },
          createRun,
        );
      await f.runtime.start(f.workflow.id, f.options()).catch((error) => error);
      await cancellation;
      expect(readBeforeEngine).toBe(false);
      expect(engineEntered).toBe(true);
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].version,
      ).toBe(2);
    } finally {
      await cancellation;
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it.each([
    'cancel',
    'terminate',
    'cleanup',
    'resume',
    'repair',
  ] as const)('preserves the selected row when its internal run id contradicts the %s address', async (operation) => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
      });
      let cleanupRevision = 1;
      if (operation === 'cleanup')
        cleanupRevision = (
          await f.runtime.terminate(f.workflow.id, 'activation-run')
        ).cleanup.revision;
      if (operation === 'repair') {
        const persist = f.workflows.persistWorkflowSnapshot.bind(f.workflows);
        vi.spyOn(f.workflows, 'persistWorkflowSnapshot').mockImplementation(
          (input) =>
            persist({
              ...input,
              snapshot: {
                ...input.snapshot,
                runId: 'foreign-run',
                ...(input.snapshot.status === 'success'
                  ? { status: 'pending' as const }
                  : {}),
              },
            }),
        );
      } else {
        const snapshot = await f.row();
        assert(snapshot);
        snapshot.runId = 'foreign-run';
        // Raw: the settled-row guard refuses this same-revision rewrite of a
        // terminated row through persistWorkflowSnapshot.
        f.sql
          .prepare(
            'UPDATE activation_mastra_workflow_snapshot SET snapshot = ? WHERE workflow_name = ? AND run_id = ?',
          )
          .run(JSON.stringify(snapshot), f.workflow.id, 'activation-run');
      }
      const before = await f.row();
      const persist = vi.spyOn(f.workflows, 'persistWorkflowSnapshot');
      persist.mockClear();
      const pending =
        operation === 'cancel'
          ? f.runtime.cancelActiveExecution(
              f.workflow.id,
              'activation-run',
              'cancelled',
              [{ kind: 'human', id: 'owner' }],
            )
          : operation === 'terminate'
            ? f.runtime.terminate(f.workflow.id, 'activation-run')
            : operation === 'cleanup'
              ? f.runtime.completeTerminalCleanup(
                  f.workflow.id,
                  'activation-run',
                  cleanupRevision,
                )
              : f.runtime.resume(f.workflow.id, 'activation-run', {
                  resumeData: { go: true },
                });
      const result = await pending.catch((error) => error);
      if (operation !== 'repair') {
        expect(await f.row()).toEqual(before);
        expect(persist).not.toHaveBeenCalled();
        expect(f.effects).toHaveBeenCalledOnce();
      } else {
        expect((await f.row())?.runId).toBe('foreign-run');
        expect(
          persist.mock.calls.filter(
            ([input]) => input.snapshot.status === 'success',
          ),
        ).toHaveLength(1);
      }
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });
});

async function unkeyedTargetPendingFixture(
  f: Awaited<ReturnType<typeof runtimeActivationFixture>>,
  kind: 'agent' | 'workflow' = 'agent',
) {
  assert(f.capability);
  const admit = f.capability.withInitialAdmission;
  let execution: D1RunExecutionIdentity | undefined;
  f.capability.withInitialAdmission = async (input, create) => {
    const result = await admit(input, create);
    return { ...result, witness: undefined } as never;
  };
  try {
    await expect(
      f.runtime.start(f.workflow.id, {
        ...f.options(),
        ...(kind === 'agent'
          ? {
              startIdentity: {
                owner: { kind: 'human', id: 'owner' },
                target: { kind: 'agent', id: 'writer', threadId: 'thread' },
              },
              agentStart: { threaded: false },
            }
          : {}),
        onPreparedStartIdentity: (identity) => {
          execution = normalizeD1RunExecutionIdentity(identity);
        },
      }),
    ).rejects.toBeInstanceOf(RunStateUnreadableError);
    assert(execution);
    return execution;
  } finally {
    f.capability.withInitialAdmission = admit;
  }
}

function expectedAgentTarget() {
  return {
    kind: 'agent' as const,
    id: 'writer',
    threadId: 'thread',
    owner: { kind: 'human' as const, id: 'owner' },
    threaded: false,
  };
}

function replaceRecoveryTarget(
  provenance: NonNullable<WorkflowRunState['requestContext']>[string],
  mismatch: string,
) {
  if (mismatch === 'owner-id') {
    provenance.startIdentity.owner.id = 'other-owner';
    provenance.requestedBy = 'other-owner';
  } else if (mismatch === 'owner-kind') {
    provenance.startIdentity.owner.kind = 'service';
    provenance.requestedByKind = 'service';
  } else if (mismatch === 'agent')
    provenance.startIdentity.target.id = 'other-agent';
  else if (mismatch === 'thread')
    provenance.startIdentity.target.threadId = 'other-thread';
  else if (mismatch === 'mode') provenance.agentStart.threaded = true;
  else if (mismatch === 'workflow-role') {
    provenance.startIdentity.target = {
      kind: 'workflow',
      id: 'activation-workflow',
    };
    delete provenance.agentStart;
  } else if (mismatch === 'missing-identity') {
    delete provenance.startIdentity;
    delete provenance.agentStart;
    delete provenance.requestedBy;
    delete provenance.requestedByKind;
  }
}

describe('Runtime activation', () => {
  it.each(
    (['initial', 'result'] as const).flatMap((phase) =>
      [
        'owner-id',
        'owner-kind',
        'agent',
        'thread',
        'mode',
        'workflow-role',
        'missing-identity',
      ].map((mismatch) => ({ phase, mismatch })),
    ),
  )('managed agent recovery validates its single $phase observation before mutation: $mismatch', async ({
    phase,
    mismatch,
  }) => {
    const f = await runtimeActivationFixture();
    try {
      const execution = await unkeyedTargetPendingFixture(f);
      const snapshot = await f.row();
      assert(snapshot?.requestContext);
      replaceRecoveryTarget(
        snapshot.requestContext['flowsafe.runProvenance'],
        mismatch,
      );
      if (phase === 'result')
        Object.assign(snapshot, {
          status: 'success',
          result: { selected: true },
        });
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      assert(f.capability);
      const rawRead = f.capability.readSnapshot;
      const before = await rawRead({
        workflowId: f.workflow.id,
        runId: 'activation-run',
      });
      const read = vi.spyOn(f.capability, 'readSnapshot');
      const terminalize = vi.spyOn(f.capability, 'terminalizeInitialAdmission');
      const settle = vi.spyOn(f.reservations, 'settleExecution');
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          expectedTarget: expectedAgentTarget(),
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect(
        await rawRead({ workflowId: f.workflow.id, runId: 'activation-run' }),
      ).toEqual(before);
      expect(read).toHaveBeenCalledOnce();
      expect(terminalize).not.toHaveBeenCalled();
      expect(settle).not.toHaveBeenCalled();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'agent-role',
    'foreign-workflow',
    'missing-identity',
  ] as const)('managed workflow recovery refuses a same-S %s before terminalization', async (mismatch) => {
    const f = await runtimeActivationFixture();
    try {
      const execution = await unkeyedTargetPendingFixture(
        f,
        mismatch === 'agent-role' ? 'agent' : 'workflow',
      );
      const snapshot = await f.row();
      assert(snapshot?.requestContext);
      if (mismatch === 'foreign-workflow')
        snapshot.requestContext[
          'flowsafe.runProvenance'
        ].startIdentity.target.id = 'other-workflow';
      if (mismatch === 'missing-identity')
        replaceRecoveryTarget(
          snapshot.requestContext['flowsafe.runProvenance'],
          mismatch,
        );
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      const before = await f.row();
      assert(f.capability);
      const read = vi.spyOn(f.capability, 'readSnapshot'),
        terminalize = vi.spyOn(f.capability, 'terminalizeInitialAdmission'),
        settle = vi.spyOn(f.reservations, 'settleExecution');
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          expectedTarget: { kind: 'workflow' },
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect(await f.row()).toEqual(before);
      expect(read).toHaveBeenCalledOnce();
      expect(terminalize).not.toHaveBeenCalled();
      expect(settle).not.toHaveBeenCalled();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'quiescence',
    'read',
  ] as const)('freezes recovery expectation values and owner before the %s wait', async (phase) => {
    const entered = deferredSignal(),
      release = deferredSignal();
    const f = await runtimeActivationFixture();
    let pending: Promise<unknown> | undefined;
    try {
      const execution = await unkeyedTargetPendingFixture(f);
      assert(f.capability);
      const expectedTarget = expectedAgentTarget();
      const originalRead = f.capability.readSnapshot;
      const read = vi
        .spyOn(f.capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          const row = await originalRead(address);
          if (phase === 'read') {
            entered.resolve();
            await release.promise;
          }
          return row;
        });
      pending = f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          expectedTarget,
          startLeg: 'unwound',
          isOwnerQuiescent: async () => {
            if (phase === 'quiescence') {
              entered.resolve();
              await release.promise;
            }
            return true;
          },
        })
        .catch((error) => error);
      await entered.promise;
      expect(Object.isFrozen(expectedTarget)).toBe(false);
      expect(Object.isFrozen(expectedTarget.owner)).toBe(false);
      Object.assign(expectedTarget, {
        kind: 'workflow',
        id: 'other-agent',
        threadId: 'other-thread',
        threaded: true,
      });
      Object.assign(expectedTarget.owner, {
        kind: 'service',
        id: 'other-owner',
      });
      release.resolve();
      const result = await pending;
      expect((await f.row())?.status).toBe('failed');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .startIdentity,
      ).toEqual({
        owner: { kind: 'human', id: 'owner' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread' },
      });
      expect(read).toHaveBeenCalledOnce();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });
    } finally {
      release.resolve();
      await pending;
      f.close();
    }
  });

  it.each(
    (['terminalized', 'progressed'] as const).flatMap((kind) =>
      [
        'owner-id',
        'owner-kind',
        'agent',
        'thread',
        'mode',
        'workflow-role',
        'missing-identity',
      ].map((mismatch) => ({ kind, mismatch })),
    ),
  )('rejects a contradictory $kind target returned by terminalization before settlement: $mismatch', async ({
    kind,
    mismatch,
  }) => {
    const f = await runtimeActivationFixture();
    try {
      const execution = await unkeyedTargetPendingFixture(f);
      assert(f.capability);
      const native = f.capability.terminalizeInitialAdmission;
      const terminalize = vi
        .spyOn(f.capability, 'terminalizeInitialAdmission')
        .mockImplementation(async (input) => {
          const result = await native(input);
          assert(result.kind !== 'conflict');
          const snapshot = JSON.parse(result.row.snapshot);
          replaceRecoveryTarget(
            snapshot.requestContext['flowsafe.runProvenance'],
            mismatch,
          );
          return {
            ...result,
            kind,
            row: { ...result.row, snapshot: JSON.stringify(snapshot) },
          };
        });
      const read = vi.spyOn(f.capability, 'readSnapshot'),
        settle = vi.spyOn(f.reservations, 'settleExecution');
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          expectedTarget: expectedAgentTarget(),
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('failed');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .startIdentity,
      ).toEqual({
        owner: { kind: 'human', id: 'owner' },
        target: { kind: 'agent', id: 'writer', threadId: 'thread' },
      });
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].agentStart,
      ).toEqual({ threaded: false });
      expect(read).toHaveBeenCalledOnce();
      expect(terminalize).toHaveBeenCalledOnce();
      expect(settle).not.toHaveBeenCalled();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it('accepts a managed agent progressed result with a changed current-leg H and requester', async () => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
        startIdentity: {
          owner: { kind: 'human', id: 'owner' },
          target: { kind: 'agent', id: 'writer', threadId: 'thread' },
        },
        agentStart: { threaded: false },
      });
      const original = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
      );
      assert(original?.storage === 'd1');
      await f.runtime.resume(f.workflow.id, 'activation-run', {
        resumeData: { go: true },
        requestedBy: 'reviewer',
        requestedByKind: 'service',
      });
      assert(f.capability);
      const read = vi.spyOn(f.capability, 'readSnapshot');
      const result = await f.runtime.recoverStartAttempt(original.execution, {
        attemptToken: 'H',
        isOwnerQuiescent: () => true,
        expectedTarget: expectedAgentTarget(),
        startLeg: 'unwound',
      });
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .attemptToken,
      ).not.toBe('H');
      expect(read).toHaveBeenCalledOnce();
      expect(f.effects).toHaveBeenCalledTimes(2);
      expect(result).toMatchObject({
        kind: 'ordinary',
        summary: {
          status: 'success',
          requestedBy: 'reviewer',
          requestedByKind: 'service',
        },
      });
    } finally {
      f.close();
    }
  });

  it('workflow expectation preserves the initiating owner without a source-owner assertion', async () => {
    const f = await runtimeActivationFixture();
    try {
      const execution = await unkeyedTargetPendingFixture(f, 'workflow');
      await expect(
        f.runtime.recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          expectedTarget: { kind: 'workflow' },
          startLeg: 'unwound',
        }),
      ).resolves.toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed', requestedBy: 'owner' },
      });
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .startIdentity.owner,
      ).toEqual({ kind: 'human', id: 'owner' });
      expect(f.effects).not.toHaveBeenCalled();
    } finally {
      f.close();
    }
  });

  it.each([
    'inherited',
    'unattributed',
  ] as const)('direct generic recovery stays role-neutral for %s results', async (kind) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot();
      if (kind === 'inherited')
        snapshot.requestContext[
          'flowsafe.runProvenance'
        ].startIdentity.target.id = 'another-root';
      else {
        delete snapshot.requestContext['flowsafe.runProvenance'].startIdentity;
        delete snapshot.requestContext['flowsafe.runProvenance'].requestedBy;
        delete snapshot.requestContext['flowsafe.runProvenance']
          .requestedByKind;
      }
      await f.seed(snapshot);
      await expect(
        f.runtime.recoverStartAttempt(
          {
            tablePrefix: '',
            workflowId: 'd1-workflow',
            runId: 'd1-run',
            startToken: 'S1',
          },
          {
            attemptToken: 'initial-H',
            isOwnerQuiescent: () => true,
            startLeg: 'unwound',
          },
        ),
      ).resolves.toMatchObject({
        kind: 'ordinary',
        summary: { status: 'success', result: { source: 'S1' } },
      });
    } finally {
      f.close();
    }
  });

  it.each([
    null,
    [],
    1,
    'workflow',
    { kind: 'wrong' },
    {
      kind: 'agent',
      id: 'writer',
      threadId: 'thread',
      owner: { kind: 'human', id: 'owner' },
      threaded: 'false',
    },
  ])('rejects malformed recovery expectation %j before I/O', async (expectedTarget) => {
    const f = await runtimeActivationFixture();
    try {
      assert(f.capability);
      const read = vi.spyOn(f.capability, 'readSnapshot'),
        quiescent = vi.fn(() => true);
      const result = await f.runtime
        .recoverStartAttempt(
          {
            tablePrefix: 'activation_',
            workflowId: f.workflow.id,
            runId: 'activation-run',
            startToken: 'S',
          },
          {
            attemptToken: 'H',
            isOwnerQuiescent: quiescent,
            expectedTarget: expectedTarget as never,
            startLeg: 'unwound',
          },
        )
        .catch((error) => error);
      expect(read).not.toHaveBeenCalled();
      expect(quiescent).not.toHaveBeenCalled();
      expect(await f.row()).toBeNull();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(InvalidRunRequestError);
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it.each([
    'agent-role',
    'foreign-workflow',
    'missing-identity',
  ] as const)('rejects a workflow expectation mismatch returned by terminalization before settlement: %s', async (mismatch) => {
    const f = await runtimeActivationFixture();
    try {
      const execution = await unkeyedTargetPendingFixture(f, 'workflow');
      assert(f.capability);
      const native = f.capability.terminalizeInitialAdmission;
      vi.spyOn(f.capability, 'terminalizeInitialAdmission').mockImplementation(
        async (input) => {
          const result = await native(input);
          assert(result.kind !== 'conflict');
          const snapshot = JSON.parse(result.row.snapshot);
          const provenance = snapshot.requestContext['flowsafe.runProvenance'];
          if (mismatch === 'agent-role') {
            provenance.startIdentity.target = {
              kind: 'agent',
              id: 'writer',
              threadId: 'thread',
            };
            provenance.agentStart = { threaded: false };
          } else if (mismatch === 'foreign-workflow')
            provenance.startIdentity.target.id = 'other-workflow';
          else replaceRecoveryTarget(provenance, 'missing-identity');
          return {
            ...result,
            kind: 'progressed',
            row: { ...result.row, snapshot: JSON.stringify(snapshot) },
          };
        },
      );
      const read = vi.spyOn(f.capability, 'readSnapshot'),
        settle = vi.spyOn(f.reservations, 'settleExecution');
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          expectedTarget: { kind: 'workflow' },
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('failed');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .startIdentity.target,
      ).toEqual({ kind: 'workflow', id: f.workflow.id });
      expect(read).toHaveBeenCalledOnce();
      expect(settle).not.toHaveBeenCalled();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    { owner: { kind: 'operator', id: 'owner' } },
    { owner: { kind: 'human', id: '' } },
    { owner: null },
    { id: 'bad/agent' },
    { threadId: 'bad/thread' },
  ])('uses the existing identity normalizer for invalid managed recovery target %j', async (invalid) => {
    const f = await runtimeActivationFixture();
    try {
      assert(f.capability);
      const read = vi.spyOn(f.capability, 'readSnapshot'),
        quiescent = vi.fn(() => true);
      const result = await f.runtime
        .recoverStartAttempt(
          {
            tablePrefix: 'activation_',
            workflowId: f.workflow.id,
            runId: 'activation-run',
            startToken: 'S',
          },
          {
            attemptToken: 'H',
            isOwnerQuiescent: quiescent,
            expectedTarget: { ...expectedAgentTarget(), ...invalid } as never,
            startLeg: 'unwound',
          },
        )
        .catch((error) => error);
      expect(read).not.toHaveBeenCalled();
      expect(quiescent).not.toHaveBeenCalled();
      expect(await f.row()).toBeNull();
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(InvalidExecutionIdentityError);
    } finally {
      f.close();
    }
  });
});

describe('Runtime activation', () => {
  it('freezes workflow recovery kind before owning quiescence', async () => {
    const f = await runtimeActivationFixture(),
      entered = deferredSignal(),
      release = deferredSignal();
    let pending: Promise<unknown> | undefined;
    try {
      const execution = await unkeyedTargetPendingFixture(f, 'workflow');
      const expectedTarget = { kind: 'workflow' as const };
      pending = f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          expectedTarget,
          startLeg: 'unwound',
          isOwnerQuiescent: async () => {
            entered.resolve();
            await release.promise;
            return true;
          },
        })
        .catch((error) => error);
      await entered.promise;
      expect(Object.isFrozen(expectedTarget)).toBe(false);
      Object.assign(expectedTarget, expectedAgentTarget());
      release.resolve();
      const result = await pending;
      expect((await f.row())?.status).toBe('failed');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance']
          .startIdentity.target,
      ).toEqual({ kind: 'workflow', id: f.workflow.id });
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toMatchObject({
        kind: 'ordinary',
        summary: { status: 'failed' },
      });
    } finally {
      release.resolve();
      await pending;
      f.close();
    }
  });
});

function legacySnapshotFixture(
  version: 'v1' | 'absent',
  status: RunSummary['status'] = 'success',
) {
  const snapshot = d1Snapshot(status);
  if (version === 'v1')
    snapshot.requestContext['flowsafe.runProvenance'] = {
      version: 1,
      attemptToken: 'legacy-H',
      requestedBy: 'legacy-requester',
      resumeCounts: [['gate', 2]],
    };
  else delete snapshot.requestContext['flowsafe.runProvenance'];
  return snapshot;
}

describe('legacy Runtime observations', () => {
  it.each(
    (['default', 'prefixed', 'unfenced'] as const).flatMap((storage) =>
      (['v1', 'absent'] as const).flatMap((version) =>
        (['pending', 'suspended', 'success'] as const).map((status) => ({
          storage,
          version,
          status,
        })),
      ),
    ),
  )('selects $storage $version $status once only with explicit legacy opt-in', async ({
    storage,
    version,
    status,
  }) => {
    const f = await d1Fixture(storage);
    try {
      const snapshot = legacySnapshotFixture(version, status);
      await f.seed(snapshot);
      const read =
        storage === 'unfenced'
          ? vi.spyOn(f.workflows, 'getWorkflowRunById')
          : vi.spyOn(f.capability, 'readSnapshot');
      const mint = vi.spyOn(crypto, 'randomUUID');
      const result = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
        { includeLegacy: true },
      );
      expect(result).toMatchObject({
        kind: 'legacy',
        provenanceVersion: version === 'v1' ? 1 : undefined,
        address: {
          tablePrefix:
            storage === 'unfenced' ? null : storage === 'prefixed' ? 'd1_' : '',
          workflowId: 'd1-workflow',
          runId: 'd1-run',
        },
        snapshot,
        summary: { runId: 'd1-run', status },
      });
      expect(result).not.toHaveProperty('execution');
      expect(result?.kind === 'legacy' && result.address).not.toHaveProperty(
        'startToken',
      );
      expect(read).toHaveBeenCalledOnce();
      expect(mint).not.toHaveBeenCalled();
      if (version === 'v1')
        expect(result?.summary?.requestedBy).toBe('legacy-requester');
      else expect(result?.summary).not.toHaveProperty('requestedBy');
      if (status === 'suspended')
        expect(result?.summary?.resumeCount).toEqual(
          version === 'v1' ? { gate: 2 } : undefined,
        );
      mint.mockRestore();
      read.mockRestore();
      const before = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'd1-workflow',
        runId: 'd1-run',
      });
      const refused = await f.runtime
        .authoritativeStartState('d1-workflow', 'd1-run')
        .catch((error) => error);
      expect(
        await f.workflows.loadWorkflowSnapshot({
          workflowName: 'd1-workflow',
          runId: 'd1-run',
        }),
      ).toEqual(before);
      expect(refused).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      vi.restoreAllMocks();
      f.close();
    }
  });

  it.each([
    null,
    'legacy',
    3,
    [[]],
    { version: 3 },
    { version: 1 },
    { version: 1, attemptToken: 'bad/token', resumeCounts: [] },
    { version: 1, attemptToken: 'H', requestedBy: ' ', resumeCounts: [] },
    {
      version: 1,
      attemptToken: 'H',
      requestedByKind: 'human',
      resumeCounts: [],
    },
    { version: 1, attemptToken: 'H', resumeCounts: [['gate', 0]] },
    { version: 2, startToken: 'S1', attemptToken: 'H', resumeCounts: 'wrong' },
  ])('does not reclassify malformed provenance as legacy (%j)', async (provenance) => {
    const f = await d1Fixture();
    try {
      const snapshot = d1Snapshot();
      snapshot.requestContext['flowsafe.runProvenance'] = provenance;
      await f.seed(snapshot);
      const before = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'd1-workflow',
        runId: 'd1-run',
      });
      const read = vi.spyOn(f.capability, 'readSnapshot');
      const result = await f.runtime
        .authoritativeStartState('d1-workflow', 'd1-run', {
          includeLegacy: true,
        })
        .catch((error) => error);
      expect(
        await f.workflows.loadWorkflowSnapshot({
          workflowName: 'd1-workflow',
          runId: 'd1-run',
        }),
      ).toEqual(before);
      expect(read).toHaveBeenCalledOnce();
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'read failure',
    'JSON',
    'selector',
    'timestamp',
    'container',
  ] as const)('retains authoritative unreadability instead of a legacy fallback after %s', async (fault) => {
    const f = await d1Fixture();
    try {
      await f.seed(legacySnapshotFixture('v1'));
      const native = f.capability.readSnapshot;
      const read = vi
        .spyOn(f.capability, 'readSnapshot')
        .mockImplementation(async (address) => {
          if (fault === 'read failure') throw new Error('selected read failed');
          const row = await native(address);
          assert(row);
          if (fault === 'JSON') return { ...row, snapshot: '{' };
          if (fault === 'selector') return { ...row, workflowId: 'other' };
          if (fault === 'timestamp') return { ...row, updatedAt: 'invalid' };
          const snapshot = JSON.parse(row.snapshot);
          snapshot.context = [];
          return { ...row, snapshot: JSON.stringify(snapshot) };
        });
      const ordinary = vi.spyOn(f.workflows, 'getWorkflowRunById');
      const result = await f.runtime
        .authoritativeStartState('d1-workflow', 'd1-run', {
          includeLegacy: true,
        })
        .catch((error) => error);
      expect(read).toHaveBeenCalledOnce();
      expect(ordinary).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it.each([
    'prefixed',
    'unfenced',
  ] as const)('captures actual %s source, method and legacy option before the selected read waits', async (storage) => {
    const nominal = await d1Fixture(),
      actual = await d1Fixture(storage),
      entered = deferredSignal(),
      release = deferredSignal();
    let pending: Promise<unknown> | undefined;
    try {
      await nominal.seed(d1Snapshot());
      await actual.seed(legacySnapshotFixture('v1'));
      const nominalMastra = nominal.workflow.mastra;
      assert(nominalMastra);
      vi.spyOn(nominalMastra, 'getStorage').mockReturnValue(actual.storage);
      const options = { includeLegacy: true as const };
      const receivers: unknown[] = [];
      if (storage === 'prefixed') {
        const native = actual.capability.readSnapshot;
        actual.capability.readSnapshot = async function (address) {
          receivers.push(this);
          const row = await native.call(this, address);
          entered.resolve();
          await release.promise;
          return row;
        };
      } else {
        const native = actual.workflows.getWorkflowRunById;
        actual.workflows.getWorkflowRunById = async function (address) {
          receivers.push(this);
          const row = await native.call(this, address);
          entered.resolve();
          await release.promise;
          return row;
        };
      }
      const nominalRead = vi.spyOn(nominal.capability, 'readSnapshot');
      pending = nominal.runtime
        .authoritativeStartState('d1-workflow', 'd1-run', options)
        .catch((error) => error);
      const actualReadEntered = await Promise.race([
        entered.promise.then(() => true),
        pending.then(() => false),
      ]);
      expect(actualReadEntered).toBe(true);
      Object.assign(options, { includeLegacy: false });
      const replacement = vi
        .fn()
        .mockRejectedValue(new Error('replacement read must not run'));
      if (storage === 'prefixed') actual.capability.readSnapshot = replacement;
      else actual.workflows.getWorkflowRunById = replacement;
      release.resolve();
      const result = await pending;
      expect(result).toMatchObject({
        kind: 'legacy',
        address: { tablePrefix: storage === 'prefixed' ? 'd1_' : null },
        summary: { requestedBy: 'legacy-requester' },
      });
      expect(receivers).toEqual([
        storage === 'prefixed' ? actual.capability : actual.workflows,
      ]);
      expect(nominalRead).not.toHaveBeenCalled();
      expect(replacement).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await pending;
      nominal.close();
      actual.close();
    }
  });

  it('does not use a cached Core run when the selected legacy-capable row is absent', async () => {
    const f = await d1Fixture();
    try {
      await f.workflow.createRun({ runId: 'd1-run' });
      await f.seed(legacySnapshotFixture('v1'));
      f.sql
        .prepare(
          'DELETE FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
        )
        .run('d1-workflow', 'd1-run');
      const before = f.workflow.runs.get('d1-run');
      expect(before).toBeDefined();
      const read = vi.spyOn(f.capability, 'readSnapshot');
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run', {
          includeLegacy: true,
        }),
      ).resolves.toBeNull();
      expect(f.workflow.runs.get('d1-run')).toBe(before);
      expect(read).toHaveBeenCalledOnce();
    } finally {
      f.close();
    }
  });

  it('keeps raw legacy pending visible even when lifecycle projection looks terminal', async () => {
    const f = await d1Fixture();
    try {
      const snapshot = legacySnapshotFixture('v1', 'pending');
      snapshot.requestContext['flowsafe.runLifecycle'] = {
        version: 1,
        revision: 1,
        terminal: {
          status: 'cancelled',
          transitionedAt: 1,
          error: { code: 'CANCELLED', message: 'run was cancelled' },
          replayPrincipals: [{ kind: 'human', id: 'owner' }],
        },
      };
      await f.seed(snapshot);
      const result = await f.runtime.authoritativeStartState(
        'd1-workflow',
        'd1-run',
        { includeLegacy: true },
      );
      expect(result).toMatchObject({
        kind: 'legacy',
        snapshot: { status: 'pending' },
        summary: { status: 'cancelled' },
      });
      expect(result).not.toHaveProperty('execution');
    } finally {
      f.close();
    }
  });

  it('preserves changed v1 current requester during native authorized resume without creating v2 identity', async () => {
    const f = await runtimeActivationFixture();
    try {
      await f.runtime.start(f.workflow.id, {
        ...f.options(),
        inputData: { suspend: true },
      });
      const snapshot = await f.row();
      assert(snapshot?.requestContext);
      snapshot.requestContext['flowsafe.runProvenance'] = {
        version: 1,
        attemptToken: 'legacy-H',
        requestedBy: 'initial-owner',
        requestedByKind: 'human',
        resumeCounts: [],
      };
      await f.workflows.persistWorkflowSnapshot({
        workflowName: f.workflow.id,
        runId: 'activation-run',
        snapshot,
      });
      await expect(
        f.runtime.resume(f.workflow.id, 'activation-run', {
          resumeData: { go: true },
          requestedBy: 'reviewer',
          requestedByKind: 'service',
        }),
      ).resolves.toMatchObject({ status: 'success', requestedBy: 'reviewer' });
      const result = await f.runtime.authoritativeStartState(
        f.workflow.id,
        'activation-run',
        { includeLegacy: true },
      );
      expect(result).toMatchObject({
        kind: 'legacy',
        provenanceVersion: 1,
        summary: {
          status: 'success',
          requestedBy: 'reviewer',
          requestedByKind: 'service',
        },
      });
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'],
      ).toMatchObject({
        version: 1,
        startToken: 'legacy-H',
        resumeCounts: [['gate', 1]],
      });
      expect(result).not.toHaveProperty('execution');
      expect(f.effects).toHaveBeenCalledTimes(2);
      await expect(
        f.runtime.authoritativeStartState(f.workflow.id, 'activation-run'),
      ).rejects.toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it('preserves strict ReturnType and Parameters while typing explicit legacy reads', () => {
    expectTypeOf<
      ReturnType<RunnerRuntime['authoritativeStartState']>
    >().toEqualTypeOf<Promise<AuthoritativeStartState | null>>();
    expectTypeOf<
      Parameters<RunnerRuntime['authoritativeStartState']>
    >().toEqualTypeOf<[string, string]>();
    const readLegacy = (runtime: RunnerRuntime) =>
      runtime.authoritativeStartState('workflow', 'run', {
        includeLegacy: true,
      });
    expectTypeOf(readLegacy).returns.toEqualTypeOf<
      Promise<AuthoritativeStartState | LegacyRunState | null>
    >();
    expectTypeOf<
      Extract<keyof LegacyRunState, 'execution' | 'startToken' | 'attemptToken'>
    >().toEqualTypeOf<never>();
  });
});

describe('legacy Runtime observations', () => {
  it.each([
    'v1',
    'absent',
  ] as const)('does not admit %s to private recovery or proof through the opt-in capability', async (version) => {
    const f = await d1Fixture('default', 'fence');
    try {
      await f.seed(legacySnapshotFixture(version));
      const fence = f.runtime.executionFence;
      assert(fence);
      await fence.seed('open');
      await fence.transition({
        expected: 'open',
        next: 'proof-only',
        proofKey: 'proof-key',
      });
      const before = await f.workflows.loadWorkflowSnapshot({
        workflowName: 'd1-workflow',
        runId: 'd1-run',
      });
      const terminalize = vi.spyOn(f.capability, 'terminalizeInitialAdmission');
      const settle = vi.spyOn(f.runtime, 'settleStartExecution');
      await expect(
        f.runtime.authoritativeStartState('d1-workflow', 'd1-run', {
          includeLegacy: true,
        }),
      ).resolves.toMatchObject({ kind: 'legacy' });
      const recovery = await f.runtime
        .recoverStartAttempt(
          {
            tablePrefix: '',
            workflowId: 'd1-workflow',
            runId: 'd1-run',
            startToken: 'legacy-H',
          },
          {
            attemptToken: 'legacy-H',
            isOwnerQuiescent: () => true,
            startLeg: 'unwound',
          },
        )
        .catch((error) => error);
      const proof = await f.runtime
        .assertExistingRunAllowed('d1-workflow', 'd1-run')
        .catch((error) => error);
      expect(
        await f.workflows.loadWorkflowSnapshot({
          workflowName: 'd1-workflow',
          runId: 'd1-run',
        }),
      ).toEqual(before);
      expect(terminalize).not.toHaveBeenCalled();
      expect(settle).not.toHaveBeenCalled();
      expect(recovery).toBeInstanceOf(RunStateUnreadableError);
      expect(proof).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it('refuses a legacy-shaped initial witness without losing the actual modern pending row', async () => {
    const f = await runtimeActivationFixture();
    try {
      assert(f.capability);
      const native = f.capability.withInitialAdmission;
      f.capability.withInitialAdmission = async (input, create) => {
        const result = await native(input, create);
        const snapshot = JSON.parse(result.witness.row.snapshot);
        snapshot.requestContext['flowsafe.runProvenance'].version = 1;
        return {
          ...result,
          witness: {
            ...result.witness,
            row: { ...result.witness.row, snapshot: JSON.stringify(snapshot) },
          },
        };
      };
      const result = await f.runtime
        .start(f.workflow.id, f.options())
        .catch((error) => error);
      expect((await f.row())?.status).toBe('pending');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].version,
      ).toBe(2);
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(RunStateUnreadableError);
    } finally {
      f.close();
    }
  });

  it('refuses a legacy-shaped row returned by terminalization before modern reservation settlement', async () => {
    const f = await runtimeActivationFixture();
    try {
      const { execution, claim } = await preparedPendingFixture(f);
      assert(f.capability);
      const native = f.capability.terminalizeInitialAdmission;
      f.capability.terminalizeInitialAdmission = async (input) => {
        const result = await native(input);
        assert(result.kind !== 'conflict');
        const snapshot = JSON.parse(result.row.snapshot);
        snapshot.requestContext['flowsafe.runProvenance'].version = 1;
        return {
          ...result,
          row: { ...result.row, snapshot: JSON.stringify(snapshot) },
        };
      };
      const result = await f.runtime
        .recoverStartAttempt(execution, {
          attemptToken: 'H',
          isOwnerQuiescent: () => true,
          startReservation: claim,
          startLeg: 'unwound',
        })
        .catch((error) => error);
      expect((await f.row())?.status).toBe('failed');
      expect(
        (await f.row())?.requestContext?.['flowsafe.runProvenance'].version,
      ).toBe(2);
      expect((await f.reservations.readForAdmission(claim.key))?.state).toBe(
        'started',
      );
      expect(f.effects).not.toHaveBeenCalled();
      expect(result).toBeInstanceOf(ExecutionFenceUnreadableError);
    } finally {
      f.close();
    }
  });
});

function holdingApp(
  hold: Promise<void>,
  entered: () => void,
  storage: MastraCompositeStore = new InMemoryStore(),
) {
  const app = init(
    { storage },
    { executionFence: 'none', startIdempotency: 'none' },
  );
  const schema = z.object({});
  const workflow = app
    .createWorkflow({
      id: 'interrupt-workflow',
      inputSchema: schema,
      outputSchema: schema,
    })
    .then(
      app.createStep({
        id: 'hold',
        inputSchema: schema,
        outputSchema: schema,
        execute: async () => {
          entered();
          await hold;
          throw Object.assign(new Error('step gave up'), {
            name: 'RunInterruptedError',
          });
        },
      }),
    )
    .commit();
  return { app, workflow };
}

/**
 * A run another runtime left mid-step, rewritten to `status`, and a fresh
 * runtime over the same storage that drives nothing. `startedAt` is the
 * clock reading before the row's last write.
 */
async function strandedRun(
  status: 'running' | 'waiting' | 'paused' = 'running',
  storage: MastraCompositeStore = createD1Storage({
    binding: sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding,
  }),
  start: { deadlineMs?: number } = {},
) {
  await storage.init();
  const startedAt = Date.now();
  const entered = deferredSignal();
  const stranded = holdingApp(
    new Promise<void>(() => undefined),
    entered.resolve,
    storage,
  );
  void stranded.app.runtime.start(stranded.workflow.id, {
    runId: 'stranded-run',
    inputData: {},
    ...start,
  });
  await entered.promise;
  const workflows = await storage.getStore('workflows');
  const snapshot = await workflows?.loadWorkflowSnapshot({
    workflowName: stranded.workflow.id,
    runId: 'stranded-run',
  });
  assert(workflows && snapshot);
  await workflows.persistWorkflowSnapshot({
    workflowName: stranded.workflow.id,
    runId: 'stranded-run',
    snapshot: { ...snapshot, status },
  });
  return {
    startedAt,
    ...holdingApp(Promise.resolve(), () => undefined, storage),
  };
}

describe('RunnerRuntime.settleInterruptedRun', () => {
  it.each([
    'running',
    'waiting',
    'paused',
  ] as const)('fails a %s row no leg has touched for six minutes as INTERRUPTED', async (stored) => {
    // #given a row left mid-step (`waiting` is an in-memory `.sleep()`,
    // `paused` a per-step pause), read from a runtime that drives nothing
    const { app, workflow } = await strandedRun(stored);

    // #when
    const settled = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      Date.now() + 360_001,
    );

    // #then
    expect(settled).toMatchObject({
      kind: 'interrupted',
      summary: { status: 'failed', errorEnvelope: { code: 'INTERRUPTED' } },
    });
  });

  it('reads a row a leg touched within six minutes as live, and settles it once that passes', async () => {
    // #given a stranded row a leg on another instance touched five minutes on
    const { app, workflow, startedAt } = await strandedRun();
    const touchedAt = startedAt + 300_000;
    await app.runtime.touchRun(workflow.id, 'stranded-run', touchedAt);

    // #when
    const early = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      touchedAt + 359_000,
    );
    const late = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      touchedAt + 360_000,
    );

    // #then
    expect(early).toEqual({ kind: 'live' });
    expect(late).toMatchObject({ kind: 'interrupted' });
  });

  it.each([
    ['an INTERRUPTED', 'touch'],
    ['an INTERRUPTED', 'write'],
    ['a recorded cancellation', 'touch'],
    ['a recorded cancellation', 'write'],
  ] as const)('reads %s settlement as live when a %s lands after its silence read', async (settlement, landed) => {
    // #given a silent stranded row, and a writer that lands on it just after
    // settlement reads it silent
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding,
    });
    const { app, workflow } = await strandedRun('running', storage);
    if (settlement === 'a recorded cancellation')
      await app.runtime.cancelActiveExecution(
        workflow.id,
        'stranded-run',
        'cancelled',
        [{ kind: 'human', id: 'owner' }],
      );
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native);
    const address = { workflowId: workflow.id, runId: 'stranded-run' };
    const land = async () => {
      // A second on, so the touch moves `updatedAt` within one millisecond too.
      if (landed === 'touch')
        return native.touchRun?.(address, Date.now() + 1_000);
      const current = await workflows.loadWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'stranded-run',
      });
      assert(current);
      await workflows.persistWorkflowSnapshot({
        workflowName: workflow.id,
        runId: 'stranded-run',
        snapshot: { ...current, timestamp: current.timestamp + 1 },
      });
    };
    let landing: Promise<unknown> | undefined;
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        readSnapshot: async (read: typeof address) => {
          const row = await native.readSnapshot(read);
          landing ??= land();
          await landing;
          return row;
        },
      },
      configurable: true,
    });

    // #when
    const settled = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      Date.now() + 360_001,
    );

    // #then the landed write stands
    expect(settled).toEqual({ kind: 'live' });
    const row = await native.readSnapshot(address);
    assert(row);
    expect(JSON.parse(row.snapshot)).toMatchObject({ status: 'running' });
  });

  it('reads an INTERRUPTED settlement as live when its silent row differs from the state it decided on', async () => {
    // #given a stranded row that a writer with a lagging clock rewrites, without
    // a fresh `updatedAt`, between settlement's load and its silence read
    const sql = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sql) as D1DatabaseBinding,
    });
    const { app, workflow } = await strandedRun('running', storage);
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native);
    const address = { workflowId: workflow.id, runId: 'stranded-run' };
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        readSnapshot: async (read: typeof address) => {
          const row = await native.readSnapshot(read);
          assert(row);
          sql
            .prepare(
              'UPDATE mastra_workflow_snapshot SET snapshot = ? WHERE run_id = ?',
            )
            .run(
              JSON.stringify({
                ...JSON.parse(row.snapshot),
                status: 'waiting',
              }),
              'stranded-run',
            );
          return native.readSnapshot(read);
        },
      },
      configurable: true,
    });

    // #when
    const settled = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      Date.now() + 360_001,
    );

    // #then
    expect(settled).toEqual({ kind: 'live' });
    const row = await native.readSnapshot(address);
    assert(row);
    expect(JSON.parse(row.snapshot)).toMatchObject({ status: 'waiting' });
  });

  it('never settles a run whose storage cannot mark legs live', async () => {
    // #given a stranded row on storage without the D1 liveness touch
    const { app, workflow } = await strandedRun('running', new InMemoryStore());

    // #when
    const settled = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      Date.now() + 86_400_000,
    );

    // #then
    expect(settled).toEqual({ kind: 'live' });
  });

  it('replays a terminal transition until its cleanup completes', async () => {
    // #given a stranded run whose cancellation was persisted, cleanup not yet
    const { app, workflow } = await strandedRun();
    const cancelled = await app.runtime.terminate(workflow.id, 'stranded-run');
    const later = Date.now() + 360_001;

    // #when
    const unfinished = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      later,
    );
    await app.runtime.completeTerminalCleanup(
      workflow.id,
      'stranded-run',
      cancelled.cleanup.revision,
    );
    const finished = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      later,
    );

    // #then
    expect(unfinished).toMatchObject({
      kind: 'transition',
      transition: { cleanup: { status: 'cancelled', cleanupCompleted: false } },
    });
    expect(finished).toEqual({ kind: 'durable' });
  });

  it('reads a run this runtime is still executing as live without waiting for its leg', async () => {
    // #given an in-process leg holding the run lock inside its step
    const entered = deferredSignal();
    const { app, workflow } = holdingApp(
      new Promise<void>(() => undefined),
      entered.resolve,
    );
    void app.runtime.start(workflow.id, { runId: 'live-run', inputData: {} });
    await entered.promise;

    // #when / #then
    await expect(
      app.runtime.settleInterruptedRun(workflow.id, 'live-run'),
    ).resolves.toEqual({ kind: 'live' });
  });

  it('refuses the pending initial row that start recovery owns, leaving it unchanged', async () => {
    // #given an admitted initial row whose start never reached the engine
    const f = await runtimeActivationFixture();
    try {
      await preparedPendingFixture(f);
      const before = await f.row();

      // #when / #then
      await expect(
        f.runtime.settleInterruptedRun(f.workflow.id, 'activation-run'),
      ).rejects.toBeInstanceOf(RunStartPendingError);
      expect(await f.row()).toStrictEqual(before);
    } finally {
      f.close();
    }
  });

  it('gives a step failure that borrows the interruption error name no INTERRUPTED envelope', async () => {
    // #given a step that throws an error named like the interruption
    const { app, workflow } = holdingApp(Promise.resolve(), () => undefined);

    // #when
    await app.runtime.start(workflow.id, {
      runId: 'borrowed-name',
      inputData: {},
    });

    // #then
    const summary = await app.runtime.status(workflow.id, 'borrowed-name');
    expect(summary?.status).toBe('failed');
    expect(summary?.errorEnvelope).toBeUndefined();
  });
});

describe('RunnerRuntime lifecycle transitions on D1 storage', () => {
  const owner = { kind: 'human', id: 'owner' } as const;
  const lifecycleKey = 'flowsafe.runLifecycle';

  /**
   * After each of the next `times` reads of the run row, by either read path
   * a lifecycle transition may take, rewrites the row from `edit` before the
   * reader continues. `edit` is given the current snapshot, read through the
   * unwrapped original, and a distinct `attempt` to make each rewrite's bytes
   * differ.
   */
  function landAfterReads(
    workflows: FencedWorkflowsStorageD1,
    edit: (current: WorkflowRunState, attempt: number) => WorkflowRunState,
    times = 1,
  ): void {
    const load = workflows.loadWorkflowSnapshot.bind(workflows);
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native);
    let landed = 0;
    const land = async (workflowName: string, runId: string) => {
      if (landed >= times) return;
      landed += 1;
      const current = await load({ workflowName, runId });
      assert(current);
      await workflows.persistWorkflowSnapshot({
        workflowName,
        runId,
        snapshot: edit(current, landed),
      });
    };
    workflows.loadWorkflowSnapshot = async (args) => {
      const state = await load(args);
      await land(args.workflowName, args.runId);
      return state;
    };
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        readSnapshot: async (read: { workflowId: string; runId: string }) => {
          const row = await native.readSnapshot(read);
          await land(read.workflowId, read.runId);
          return row;
        },
      },
      configurable: true,
    });
  }

  async function racingRun(start: { deadlineMs?: number } = {}) {
    const sql = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sql) as D1DatabaseBinding,
    });
    const run = await strandedRun('running', storage, start);
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const load = workflows.loadWorkflowSnapshot.bind(workflows);
    const stored = async () => {
      const state = await load({
        workflowName: run.workflow.id,
        runId: 'stranded-run',
      });
      assert(state);
      return state;
    };
    const storedCas = async () => {
      const lifecycle = (await stored()).requestContext?.[lifecycleKey];
      return {
        expectedRevision: lifecycle.revision,
        expectedDeadlineAt: lifecycle.deadlineAt,
      };
    };
    return { ...run, sql, workflows, stored, storedCas };
  }

  const finishes = (current: WorkflowRunState): WorkflowRunState => ({
    ...current,
    status: 'success',
    result: { done: true },
  });

  const keepsRunning = (
    current: WorkflowRunState,
    attempt: number,
  ): WorkflowRunState => ({
    ...current,
    timestamp: current.timestamp + attempt,
  });

  const waits = (current: WorkflowRunState): WorkflowRunState => ({
    ...current,
    status: 'waiting',
  });

  it.each([
    'terminate',
    'run-deadline timeout',
  ] as const)('keeps a result a leg wrote between the %s read and its write', async (route) => {
    // #given a stranded run whose leg writes its result just after the
    // transition reads the row
    const { app, workflow, workflows, stored, storedCas } = await racingRun(
      route === 'terminate' ? {} : { deadlineMs: 1 },
    );
    const cas = route === 'terminate' ? undefined : await storedCas();
    landAfterReads(workflows, finishes);

    // #when the transition is requested
    const transition = cas
      ? app.runtime.timeOut(
          workflow.id,
          'stranded-run',
          cas,
          Date.now() + 1_000,
        )
      : app.runtime.terminate(workflow.id, 'stranded-run');

    // #then it answers the conflict and the result stands
    await expect(transition).rejects.toBeInstanceOf(RunTerminalConflictError);
    expect(await stored()).toMatchObject({
      status: 'success',
      result: { done: true },
    });
  });

  it.each([
    { label: 'terminate', route: 'terminate', patch: true },
    { label: 'run-deadline timeout', route: 'timeout', patch: true },
    {
      label: 'terminate on storage that only replaces whole rows',
      route: 'terminate',
      patch: false,
    },
  ] as const)('keeps a result a leg wrote between the cancellation-intent read and its write ($label)', async ({
    route,
    patch,
  }) => {
    // #given a stranded run whose leg writes its result just after the intent
    // pass reads the row
    const { app, workflow, workflows, stored, storedCas } = await racingRun(
      route === 'terminate' ? {} : { deadlineMs: 1 },
    );
    const cas = route === 'terminate' ? undefined : await storedCas();
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native);
    if (!patch)
      Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
        value: { ...native, patchRunLifecycle: undefined },
        configurable: true,
      });
    landAfterReads(workflows, finishes);

    // #when the intent is requested
    const cancelling = cas
      ? app.runtime.cancelActiveExecution(
          workflow.id,
          'stranded-run',
          'timed_out',
          [owner],
          cas,
          Date.now() + 1_000,
        )
      : app.runtime.cancelActiveExecution(
          workflow.id,
          'stranded-run',
          'cancelled',
          [owner],
        );

    // #then it answers the conflict, and the result stands without an intent
    await expect(cancelling).rejects.toBeInstanceOf(RunTerminalConflictError);
    const row = await stored();
    expect(row).toMatchObject({ status: 'success', result: { done: true } });
    expect(
      row.requestContext?.[lifecycleKey]?.transitionIntent,
    ).toBeUndefined();
  });

  it('records the cancellation when a leg write serialized before the intent lands after it', async () => {
    // #given a stranded run whose leg write, made from the row before the
    // intent, lands right after the intent patch
    const { app, workflow, workflows, stored } = await racingRun();
    const before = await stored();
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native?.patchRunLifecycle);
    const patch = native.patchRunLifecycle.bind(native);
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        patchRunLifecycle: async (
          ...args: Parameters<typeof patch>
        ): Promise<boolean> => {
          const patched = await patch(...args);
          if (patched)
            await workflows.persistWorkflowSnapshot({
              workflowName: workflow.id,
              runId: 'stranded-run',
              snapshot: before,
            });
          return patched;
        },
      },
      configurable: true,
    });

    // #when the intent is recorded and the run later settles as stranded
    await app.runtime.cancelActiveExecution(
      workflow.id,
      'stranded-run',
      'cancelled',
      [owner],
    );
    const settled = await app.runtime.settleInterruptedRun(
      workflow.id,
      'stranded-run',
      Date.now() + 360_001,
    );

    // #then the recorded cancellation decides it
    expect(settled).toMatchObject({
      kind: 'transition',
      transition: { summary: { status: 'cancelled' } },
    });
  });

  it.each([
    { label: 'terminate, no lifecycle', route: 'terminate', start: {} },
    {
      label: 'terminate, a lower revision',
      route: 'terminate',
      start: { deadlineMs: 3_600_000 },
    },
    {
      label: 'run-deadline timeout, a lower revision',
      route: 'timeout',
      start: { deadlineMs: 1 },
    },
  ] as const)('records the transition of a run whose leg on another instance completes after the intent ($label)', async ({
    route,
    start,
  }) => {
    // #given a stranded run with a recorded intent
    const { app, workflow, workflows, stored, storedCas } =
      await racingRun(start);
    const cas = route === 'terminate' ? undefined : await storedCas();
    const now = Date.now() + 1_000;
    const { requestContext } = await stored();
    await (cas
      ? app.runtime.cancelActiveExecution(
          workflow.id,
          'stranded-run',
          'timed_out',
          [owner],
          cas,
          now,
        )
      : app.runtime.cancelActiveExecution(
          workflow.id,
          'stranded-run',
          'cancelled',
          [owner],
        ));

    // #when its leg, holding the context from before the intent, writes its
    // result
    await workflows.persistWorkflowSnapshot({
      workflowName: workflow.id,
      runId: 'stranded-run',
      snapshot: { ...finishes(await stored()), requestContext },
    });
    const transition = cas
      ? app.runtime.timeOut(workflow.id, 'stranded-run', cas, now)
      : app.runtime.terminate(workflow.id, 'stranded-run');

    // #then the transition records the run with the intent's status
    await expect(transition).resolves.toMatchObject({
      transitioned: true,
      summary: { status: cas ? 'timed_out' : 'cancelled' },
    });
  });

  it('lands the cancellation intent while the leg keeps rewriting the row', async () => {
    // #given a stranded run whose leg rewrites the row, still running, after
    // each of up to five reads: enough to make a whole-row compare-and-set
    // miss on every attempt, which the intent patch does not
    const { app, workflow, workflows, stored } = await racingRun();
    landAfterReads(workflows, keepsRunning, 5);

    // #when the intent is requested
    const cancelled = await app.runtime.cancelActiveExecution(
      workflow.id,
      'stranded-run',
      'cancelled',
      [owner],
    );

    // #then no leg here is cancelled, and the intent is stored on the running row
    expect(cancelled).toBe(false);
    const row = await stored();
    expect(row.status).toBe('running');
    expect(row.requestContext?.[lifecycleKey]).toMatchObject({
      transitionIntent: { status: 'cancelled' },
    });
  });

  it('records the run-deadline timeout after its intent decides again from a changed row', async () => {
    // #given a stranded run whose status changes, still terminable, just after
    // the deadline route's intent pass reads it
    const { app, workflow, workflows, stored, storedCas } = await racingRun({
      deadlineMs: 1,
    });
    const cas = await storedCas();
    landAfterReads(workflows, waits);
    const now = Date.now() + 1_000;

    // #when the intent is requested and then the timeout
    await app.runtime.cancelActiveExecution(
      workflow.id,
      'stranded-run',
      'timed_out',
      [owner],
      cas,
      now,
    );
    const intent = (await stored()).requestContext?.[lifecycleKey]
      ?.transitionIntent;
    const timedOut = await app.runtime.timeOut(
      workflow.id,
      'stranded-run',
      cas,
      now,
    );

    // #then the intent landed on the changed row and the timeout is recorded
    expect(intent).toMatchObject({ status: 'timed_out', ...cas });
    expect(timedOut).toMatchObject({
      transitioned: true,
      casMatched: true,
      summary: { status: 'timed_out' },
    });
  });

  it.each([
    1, 4,
  ])('lands the terminal write after %i leg writes that keep the run running', async (writes) => {
    // #given a stranded run whose leg rewrites the row, still running, after
    // each of the first reads
    const { app, workflow, workflows, stored } = await racingRun();
    landAfterReads(workflows, keepsRunning, writes);

    // #when
    const terminated = await app.runtime.terminate(workflow.id, 'stranded-run');

    // #then the transition decides again from the rewritten row and lands
    expect(terminated).toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled' },
    });
    expect((await stored()).status).toBe('cancelled');
  });

  it('answers that the run state is not readable while its row changes on every attempt', async () => {
    // #given a stranded run whose row is rewritten after each of up to five
    // reads, one per attempt
    const { app, workflow, workflows, stored } = await racingRun();
    landAfterReads(workflows, keepsRunning, 5);

    // #when
    const terminating = app.runtime.terminate(workflow.id, 'stranded-run');

    // #then
    await expect(terminating).rejects.toBeInstanceOf(RunStateUnreadableError);
    expect((await stored()).status).toBe('running');
  });

  it('records the cancellation of a run whose snapshot nests past the depth SQLite reads', async () => {
    // #given a stranded run whose stored snapshot SQLite cannot parse as JSON
    const { app, workflow, sql } = await racingRun();
    const { snapshot } = sql
      .prepare('SELECT snapshot FROM mastra_workflow_snapshot')
      .get() as { snapshot: string };
    sql.prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?').run(
      JSON.stringify({
        ...JSON.parse(snapshot),
        result: nestedArray(PAST_SQLITE_JSON_DEPTH),
      }),
    );
    expectSnapshotUnreadable(sql);

    // #when the terminate route's intent pass runs, then its terminal pass
    const cancelled = await app.runtime.cancelActiveExecution(
      workflow.id,
      'stranded-run',
      'cancelled',
      [owner],
    );
    const intent = JSON.parse(
      (
        sql.prepare('SELECT snapshot FROM mastra_workflow_snapshot').get() as {
          snapshot: string;
        }
      ).snapshot,
    ).requestContext?.[lifecycleKey]?.transitionIntent;
    const terminated = await app.runtime.terminate(workflow.id, 'stranded-run');

    // #then the intent was stored and the run is recorded cancelled
    expect(cancelled).toBe(false);
    expect(intent).toMatchObject({ status: 'cancelled' });
    expect(terminated).toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled' },
    });
  });

  it('answers that the run is unknown when its row is absent', async () => {
    // #given D1 storage that holds no run
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding,
    });
    await storage.init();
    const { app, workflow } = holdingApp(
      Promise.resolve(),
      () => undefined,
      storage,
    );

    // #when / #then
    await expect(
      app.runtime.terminate(workflow.id, 'no-such-run'),
    ).rejects.toBeInstanceOf(UnknownRunError);
  });

  it.each([
    { label: 'is not a JSON object', rewrite: () => '[]' },
    {
      label: 'belongs to another run',
      rewrite: (snapshot: string) =>
        JSON.stringify({ ...JSON.parse(snapshot), runId: 'other-run' }),
    },
  ])('refuses to decide from a row whose snapshot $label', async ({
    rewrite,
  }) => {
    // #given a stranded run whose stored snapshot is rewritten
    const { app, workflow, sql } = await racingRun();
    const { snapshot } = sql
      .prepare('SELECT snapshot FROM mastra_workflow_snapshot')
      .get() as { snapshot: string };
    const rewritten = rewrite(snapshot);
    sql
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run(rewritten);

    // #when
    const terminating = app.runtime.terminate(workflow.id, 'stranded-run');

    // #then it answers that the state is not readable and writes nothing
    await expect(terminating).rejects.toBeInstanceOf(RunStateUnreadableError);
    expect(
      sql.prepare('SELECT snapshot FROM mastra_workflow_snapshot').get(),
    ).toEqual({ snapshot: rewritten });
  });

  it('lands the cancellation intent of a leg in this runtime that keeps writing, then stops the leg', async () => {
    // #given a leg looping through fast steps that each write the row. The unit
    // loop never lands a write inside the intent pass's read-to-write window on
    // its own, so the wrapper stands in for the leg's interleaving write: it
    // rewrites the row after each read while the leg is active.
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(openSqlite()) as D1DatabaseBinding,
    });
    await storage.init();
    const app = init(
      { storage },
      { executionFence: 'none', startIdempotency: 'none' },
    );
    const schema = z.object({});
    let ticks = 0;
    const looping = deferredSignal();
    const workflow = app
      .createWorkflow({
        id: 'looping-workflow',
        inputSchema: schema,
        outputSchema: schema,
      })
      .dowhile(
        app.createStep({
          id: 'tick',
          inputSchema: schema,
          outputSchema: schema,
          execute: async () => {
            ticks += 1;
            if (ticks === 10) looping.resolve();
            return {};
          },
        }),
        async () => ticks < 5_000,
      )
      .commit();
    const workflows = (await storage.getStore(
      'workflows',
    )) as FencedWorkflowsStorageD1;
    const native = workflows[FENCED_WORKFLOW_STORAGE];
    assert(native);
    const load = workflows.loadWorkflowSnapshot.bind(workflows);
    Object.defineProperty(workflows, FENCED_WORKFLOW_STORAGE, {
      value: {
        ...native,
        readSnapshot: async (read: { workflowId: string; runId: string }) => {
          const row = await native.readSnapshot(read);
          if (
            ticks >= 10 &&
            app.runtime.isRunActive(read.workflowId, read.runId)
          ) {
            const current = await load({
              workflowName: read.workflowId,
              runId: read.runId,
            });
            assert(current);
            await workflows.persistWorkflowSnapshot({
              workflowName: read.workflowId,
              runId: read.runId,
              snapshot: keepsRunning(current, 1),
            });
          }
          return row;
        },
      },
      configurable: true,
    });
    const leg = app.runtime.start(workflow.id, {
      runId: 'looping-run',
      inputData: {},
    });
    await looping.promise;

    // #when it is terminated while it loops, as the terminate route does
    const cancelled = await app.runtime.cancelActiveExecution(
      workflow.id,
      'looping-run',
      'cancelled',
      [owner],
    );
    const left = await leg;
    const intent = (
      await load({ workflowName: workflow.id, runId: 'looping-run' })
    )?.requestContext?.[lifecycleKey]?.transitionIntent;
    const terminated = await app.runtime.terminate(workflow.id, 'looping-run');

    // #then the intent was stored, the leg stopped short of its loop, and the
    // run reads cancelled
    expect(cancelled).toBe(true);
    expect(intent).toMatchObject({ status: 'cancelled' });
    expect(left.status).toBe('canceled');
    expect(ticks).toBeLessThan(5_000);
    expect(terminated).toMatchObject({
      transitioned: true,
      summary: { status: 'cancelled' },
    });
  });
});

describe('RunnerRuntime leg liveness touch', () => {
  const WORKFLOW_ID = 'abortable-workflow';
  const RUN_ID = 'touched-run';

  async function d1Storage() {
    const sql = openSqlite();
    const storage = createD1Storage({
      binding: sqliteUnitDatabase(sql) as D1DatabaseBinding,
    });
    await storage.init();
    return { sql, storage };
  }

  /**
   * Two steps: `hold` records the `abortSignal` it receives, signals `entered`
   * and waits for `release`; `after` counts its executions. `quietAfterHold`
   * drops the engine's `running` writes once `hold` has begun, so nothing but
   * an abort stands between the steps; `suspends` makes `hold` suspend until it
   * is resumed.
   */
  function abortableApp(
    storage: MastraCompositeStore,
    options: { quietAfterHold?: boolean; suspends?: boolean } = {},
  ) {
    const app = init(
      { storage },
      { executionFence: 'none', startIdempotency: 'none' },
    );
    const schema = z.object({});
    const entered = deferredSignal();
    const release = deferredSignal();
    const observed: { signal?: AbortSignal; afterRuns: number } = {
      afterRuns: 0,
    };
    const workflow = app
      .createWorkflow({
        id: WORKFLOW_ID,
        inputSchema: schema,
        outputSchema: schema,
        ...(options.quietAfterHold
          ? {
              options: {
                shouldPersistSnapshot: ({ workflowStatus }) =>
                  workflowStatus !== 'running' || observed.signal === undefined,
              },
            }
          : {}),
      })
      .then(
        app.createStep({
          id: 'hold',
          inputSchema: schema,
          outputSchema: schema,
          suspendSchema: schema,
          resumeSchema: schema,
          execute: async ({ abortSignal, resumeData, suspend }) => {
            if (options.suspends && !resumeData) return suspend({});
            observed.signal = abortSignal;
            entered.resolve();
            await release.promise;
            return {};
          },
        }),
      )
      .then(
        app.createStep({
          id: 'after',
          inputSchema: schema,
          outputSchema: schema,
          execute: async () => {
            observed.afterRuns += 1;
            return {};
          },
        }),
      )
      .commit();
    return { app, workflow, entered, release, observed };
  }

  /** Starts a leg that stays in `hold` until the test ends or releases it. */
  function startHeldLeg(
    storage: MastraCompositeStore,
    options: Parameters<typeof abortableApp>[1] = {},
    legAbort?: AbortController,
  ) {
    const leg = abortableApp(storage, options);
    const started = leg.app.runtime.start(WORKFLOW_ID, {
      runId: RUN_ID,
      inputData: {},
      ...(legAbort ? { legAbort } : {}),
    });
    onTestFinished(async () => {
      leg.release.resolve();
      await started.catch(() => undefined);
    });
    return { ...leg, started };
  }

  const SETTLEMENTS = [
    { settled: 'terminated', summary: { status: 'cancelled' } },
    {
      settled: 'interrupted',
      summary: { status: 'failed', errorEnvelope: { code: 'INTERRUPTED' } },
    },
  ] as const;

  /** A held leg, and its run row after another instance settled the run. */
  async function holdLegSettledElsewhere(
    settled: (typeof SETTLEMENTS)[number]['settled'],
    options: Parameters<typeof abortableApp>[1] = {},
  ) {
    const { sql, storage } = await d1Storage();
    const leg = startHeldLeg(storage, options);
    await leg.entered.promise;
    const other = abortableApp(storage);
    if (settled === 'terminated')
      await other.app.runtime.terminate(WORKFLOW_ID, RUN_ID);
    else
      await other.app.runtime.settleInterruptedRun(
        WORKFLOW_ID,
        RUN_ID,
        Date.now() + 360_001,
      );
    const rows = () =>
      sql.prepare('SELECT * FROM mastra_workflow_snapshot').all();
    return { leg, sql, rows, settledRows: rows() };
  }

  it.each(
    SETTLEMENTS,
  )('aborts a leg whose run another instance $settled at its next touch', async ({
    settled,
    summary,
  }) => {
    // #given a leg held in its first step, on a run another instance settled
    const { leg, rows, settledRows } = await holdLegSettledElsewhere(settled);

    // #when the leg's touch runs
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);

    // #then the step in flight sees its signal aborted, with the settlement as
    // the cause
    expect(leg.observed.signal?.aborted).toBe(true);
    expect(leg.observed.signal?.reason).toMatchObject({
      name: 'AbortError',
      cause: expect.any(RunSettledConflictError),
    });

    // #when the step returns
    leg.release.resolve();
    const ended = await leg.started;

    // #then the start answers the settled summary and the settlement stands
    expect(ended).toMatchObject(summary);
    expect(rows()).toEqual(settledRows);
  });

  it.each(
    SETTLEMENTS,
  )('starts no further step on a leg whose run another instance $settled when the engine writes nothing between steps', async ({
    settled,
  }) => {
    // #given a leg held in its first step, on a workflow whose engine writes
    // nothing once that step has begun, and a run another instance settled
    const { leg } = await holdLegSettledElsewhere(settled, {
      quietAfterHold: true,
    });

    // #when the leg's touch runs and the step returns
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);
    leg.release.resolve();
    await leg.started;

    // #then the second step never ran
    expect(leg.observed.afterRuns).toBe(0);
  });

  it('aborts a leg whose terminated run is stored too deep for SQLite to parse, and keeps the termination', async () => {
    // #given a held leg whose run another instance terminated, with the stored
    // snapshot nested past the depth SQLite parses
    const { leg, sql, rows } = await holdLegSettledElsewhere('terminated');
    const [terminated] = rows() as { snapshot: string }[];
    sql
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run(
        JSON.stringify(
          withDeepValue(JSON.parse(terminated?.snapshot ?? 'null')),
        ),
      );
    expectSnapshotUnreadable(sql);

    // #when the leg's touch runs
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);

    // #then the step in flight sees its signal aborted
    expect(leg.observed.signal?.aborted).toBe(true);

    // #when the step returns
    leg.release.resolve();
    const ended = await leg.started;

    // #then the start answers cancelled, no further step ran, and the run
    // stays cancelled
    expect(ended).toMatchObject({ status: 'cancelled' });
    expect(leg.observed.afterRuns).toBe(0);
    const [stored] = rows() as { snapshot: string }[];
    expect(JSON.parse(stored?.snapshot ?? 'null')).toMatchObject({
      status: 'cancelled',
    });
  });

  it('logs the abort of a settled leg once however many touches follow', async () => {
    // #given a held leg whose run another instance terminated
    const logged = vi
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    onTestFinished(() => {
      logged.mockRestore();
    });
    const { leg } = await holdLegSettledElsewhere('terminated');

    // #when two touches run
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);

    // #then one structured line says so
    expect(logged).toHaveBeenCalledExactlyOnceWith(
      JSON.stringify({
        type: 'run-leg-settled-abort',
        workflowId: WORKFLOW_ID,
        runId: RUN_ID,
      }),
    );
  });

  it('aborts a leg abort controller after the engine run when it cancels the leg', async () => {
    // #given a leg started with an abort controller, held in its first step
    const { storage } = await d1Storage();
    const legAbort = new AbortController();
    const leg = startHeldLeg(storage, {}, legAbort);
    await leg.entered.promise;
    const aborted: string[] = [];
    leg.observed.signal?.addEventListener('abort', () => {
      aborted.push('engine');
    });
    legAbort.signal.addEventListener('abort', () => {
      aborted.push('legAbort');
    });

    // #when the runtime cancels the leg for a terminate
    const cancelled = await leg.app.runtime.cancelActiveExecution(
      WORKFLOW_ID,
      RUN_ID,
      'cancelled',
      [{ kind: 'human', id: 'owner' }],
    );

    // #then both are aborted, the engine run first
    expect(cancelled).toBe(true);
    expect(aborted).toEqual(['engine', 'legAbort']);
  });

  it('leaves a leg whose row is live running', async () => {
    // #given a leg held in its first step, on a row nobody settled
    const { storage } = await d1Storage();
    const leg = startHeldLeg(storage);
    await leg.entered.promise;

    // #when its touch runs
    await leg.app.runtime.touchRun(WORKFLOW_ID, RUN_ID);

    // #then the step's signal is not aborted
    expect(leg.observed.signal?.aborted).toBe(false);

    // #when the step returns
    leg.release.resolve();
    const ended = await leg.started;

    // #then the run completes both steps
    expect(ended.status).toBe('success');
    expect(leg.observed.afterRuns).toBe(1);
  });

  it.each([
    'start',
    'resume',
    'engine failure',
  ] as const)('touches the row every interval while the leg runs and stops when it ends (%s)', async (kind) => {
    // #given a leg held in a step, with interval timers faked
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    const { storage } = await d1Storage();
    const leg = abortableApp(storage, { suspends: kind === 'resume' });
    const touch = vi.spyOn(leg.app.runtime, 'touchRun').mockResolvedValue();
    if (kind === 'engine failure') {
      const create = leg.workflow.createRun.bind(leg.workflow);
      vi.spyOn(leg.workflow, 'createRun').mockImplementation(
        async (...args) => {
          const run = await create(...args);
          const engine = run.start.bind(run);
          vi.spyOn(run, 'start').mockImplementation(async (...input) => {
            await engine(...input);
            throw new Error('engine lost its result');
          });
          return run;
        },
      );
    }
    let ended: Promise<unknown> = Promise.resolve();
    try {
      const first = leg.app.runtime.start(WORKFLOW_ID, {
        runId: RUN_ID,
        inputData: {},
      });
      if (kind === 'resume') {
        await first;
        touch.mockClear();
        ended = leg.app.runtime.resume(WORKFLOW_ID, RUN_ID, { resumeData: {} });
      } else ended = first;
      await leg.entered.promise;

      // #when the interval elapses twice while the leg runs
      vi.advanceTimersByTime(60_000);

      // #then it touched the row twice
      expect(touch.mock.calls).toEqual([
        [WORKFLOW_ID, RUN_ID],
        [WORKFLOW_ID, RUN_ID],
      ]);

      // #when the leg ends and the interval elapses twice more
      leg.release.resolve();
      await Promise.allSettled([ended]);
      vi.advanceTimersByTime(60_000);

      // #then it touched no more
      expect(touch).toHaveBeenCalledTimes(2);
    } finally {
      leg.release.resolve();
      await Promise.allSettled([ended]);
      vi.useRealTimers();
    }
  });

  it('logs a touch that fails inside the interval and keeps the leg running', async () => {
    // #given a leg held in a step whose touch rejects, with interval timers
    // faked
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    onTestFinished(() => {
      vi.useRealTimers();
    });
    const logged = vi
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    onTestFinished(() => {
      logged.mockRestore();
    });
    const { storage } = await d1Storage();
    const leg = startHeldLeg(storage);
    const failure = new Error('D1 unavailable');
    vi.spyOn(leg.app.runtime, 'touchRun').mockRejectedValue(failure);
    await leg.entered.promise;

    // #when the interval elapses once and the step returns
    vi.advanceTimersByTime(30_000);
    leg.release.resolve();
    const ended = await leg.started;

    // #then the failure is logged and the run completes
    expect(logged).toHaveBeenCalledExactlyOnceWith(
      'run leg liveness touch failed',
      failure,
    );
    expect(ended.status).toBe('success');
  });
});
