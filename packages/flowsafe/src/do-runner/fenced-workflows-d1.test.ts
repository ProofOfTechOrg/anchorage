// SPDX-License-Identifier: Apache-2.0

import { WorkflowsStorageD1 } from '@mastra/cloudflare-d1';
import { Mastra } from '@mastra/core/mastra';
import { RequestContext } from '@mastra/core/request-context';
import {
  createStep,
  createWorkflow,
  type WorkflowRunState,
} from '@mastra/core/workflows';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  D1ResourceOwnershipStore,
  type ResourceOwnershipDatabase,
} from '../approval-api/resource-ownership.js';
import type { D1DatabaseBinding } from './cf-types.js';
import { createD1Storage } from './d1-storage.js';
import {
  ExecutionFenceUnreadableError,
  InvalidExecutionIdentityError,
} from './execution-admission.js';
import {
  type ExecutionFenceState,
  ExecutionFenceStore,
} from './execution-fence.js';
import {
  FENCED_WORKFLOW_STORAGE,
  type InitialAdmissionDatabase,
  type InitialRunAdmission,
  type InitialTerminalizationRequest,
} from './fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { isDefinitiveInitialAdmissionRefusal } from './initial-admission-refusal.js';
import {
  RUN_LIFECYCLE_CONTEXT_KEY,
  RunLifecycleBlockedError,
} from './run-lifecycle.js';
import {
  StartIdempotencyStore,
  type StartReservationReading,
} from './start-idempotency.js';
import type { RawWorkflowSnapshot } from './workflow-snapshot-row.js';

const PROVENANCE = 'flowsafe.runProvenance';
const OWNER = { kind: 'human' as const, id: 'Alice' };

async function fixture(
  options: {
    keyed?: boolean;
    state?: ExecutionFenceState;
    prefix?: string;
    persist?: boolean;
    shouldPersist?: () => boolean;
    prune?: (args: { snapshot: WorkflowRunState }) => WorkflowRunState;
  } = {},
) {
  const sql = openSqlite();
  const db = sqliteUnitDatabase(sql) as InitialAdmissionDatabase &
    D1DatabaseBinding;
  const prefix = options.prefix ?? '';
  const storage = createD1Storage({ binding: db, tablePrefix: prefix });
  let effects = 0;
  const workflow = createWorkflow({
    id: 'workflow',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    options: {
      shouldPersistSnapshot:
        options.shouldPersist ?? (() => options.persist !== false),
      ...(options.prune ? { pruneSnapshot: options.prune } : {}),
    },
  })
    .then(
      createStep({
        id: 'effect',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        execute: async () => {
          effects += 1;
          return {};
        },
      }),
    )
    .commit();
  new Mastra({ storage, workflows: { workflow } });
  await storage.init();
  const domain = await storage.getStore('workflows');
  if (!(domain instanceof FencedWorkflowsStorageD1))
    throw new Error('owned default missing');
  const capability = domain[FENCED_WORKFLOW_STORAGE];
  if (!capability) throw new Error('capability missing');
  const fence = new ExecutionFenceStore(db);
  await fence.seed(
    options.state === 'proof-only' ? 'open' : (options.state ?? 'open'),
  );
  if (options.state === 'proof-only')
    await fence.transition({
      expected: 'open',
      next: 'proof-only',
      proofKey: 'key',
    });
  const execution = {
    tablePrefix: prefix.toLowerCase(),
    workflowId: 'workflow',
    runId: 'run',
    startToken: 'generation',
  };
  const startIdentity = {
    owner: OWNER,
    target: { kind: 'workflow' as const, id: 'workflow' },
  };
  const reservationStore = options.keyed
    ? new StartIdempotencyStore(db)
    : undefined;
  let reservation: StartReservationReading | undefined;
  if (reservationStore) {
    const reserved = await reservationStore.reserve({
      key: 'key',
      owner: OWNER,
      targetKind: 'workflow',
      targetId: 'workflow',
      mintRunId: () => execution.runId,
    });
    reservation = await reservationStore.claimReservation(reserved.reservation);
    if (!reservation) throw new Error('initial reservation claim was lost');
  }
  const reading = await fence.read();
  const onInitialWriteAttempt = vi.fn();
  const input: InitialRunAdmission = {
    execution,
    attemptToken: 'correlation',
    startIdentity,
    fence,
    requestContext: {
      runId: 'run',
      'breakwater.workflowScope': 'workflow',
      app: { text: 'λ', nullable: null },
      [PROVENANCE]: {
        version: 2,
        startToken: execution.startToken,
        attemptToken: 'correlation',
        startIdentity,
        requestedBy: OWNER.id,
        requestedByKind: OWNER.kind,
        resumeCounts: [],
      },
    },
    ...(reservation ? { reservation, reservationStore } : {}),
    ...(options.state === 'proof-only'
      ? {
          proof: {
            key: 'key',
            mutationEpoch: reading.mutationEpoch,
            transitionRevision: reading.transitionRevision,
          },
        }
      : {}),
    onInitialWriteAttempt,
  };
  const admit = (supplied = input) =>
    capability.withInitialAdmission(supplied, () =>
      workflow.createRun({ runId: execution.runId }),
    );
  return {
    sql,
    db,
    storage,
    workflow,
    domain,
    capability,
    fence,
    input,
    admit,
    onInitialWriteAttempt,
    effects: () => effects,
    rows: () =>
      sql.prepare(`SELECT * FROM "${prefix}mastra_workflow_snapshot"`).all(),
  };
}

function pending(): WorkflowRunState {
  return {
    runId: 'run',
    status: 'pending',
    value: {},
    context: {},
    serializedStepGraph: [],
    activePaths: [],
    activeStepsPath: {},
    suspendedPaths: {},
    resumeLabels: {},
    waitingPaths: {},
    timestamp: 123,
  };
}

async function direct(
  h: Awaited<ReturnType<typeof fixture>>,
  patch: Partial<
    Parameters<FencedWorkflowsStorageD1['persistWorkflowSnapshot']>[0]
  > = {},
  input = h.input,
) {
  return h.capability.withInitialAdmission(input, () =>
    h.domain.persistWorkflowSnapshot({
      workflowName: 'workflow',
      runId: 'run',
      snapshot: pending(),
      ...patch,
    }),
  );
}

afterEach(() => vi.restoreAllMocks());

async function terminalFixture() {
  const h = await fixture({ keyed: true, state: 'proof-only' });
  await direct(h, { resourceId: 'resource' });
  const expected = await h.capability.readSnapshot(h.input.execution);
  if (!expected) throw new Error('initial row missing');
  const request: InitialTerminalizationRequest = {
    expected,
    execution: h.input.execution,
    attemptToken: h.input.attemptToken,
    nowMs: 1_700_000_000_123,
  };
  const replace = (
    edit: (
      snapshot: Record<string, unknown> & {
        requestContext: Record<string, unknown> & {
          [PROVENANCE]: Record<string, unknown> & {
            startIdentity: {
              owner: { id: string; kind: string };
              target: { id: string; kind: string; threadId?: string };
            };
          };
        };
      },
    ) => void,
  ) => {
    const value = JSON.parse(request.expected.snapshot);
    edit(value);
    const row = { ...request.expected, snapshot: JSON.stringify(value) };
    h.sql
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run(row.snapshot);
    return { ...request, expected: row };
  };
  return { ...h, request, replace };
}

function terminalResponse(
  h: Awaited<ReturnType<typeof terminalFixture>>,
  change: (result: unknown) => unknown | Promise<unknown>,
) {
  const prepare = h.db.prepare.bind(h.db);
  return vi.spyOn(h.db, 'prepare').mockImplementation((sql) => {
    const statement = prepare(sql);
    if (sql.startsWith('UPDATE "')) {
      const bind = statement.bind.bind(statement);
      vi.spyOn(statement, 'bind').mockImplementation((...values) => {
        const bound = bind(...values);
        const all = bound.all.bind(bound);
        vi.spyOn(bound, 'all').mockImplementation(
          async () => change(await all()) as never,
        );
        return bound;
      });
    }
    return statement;
  });
}

describe('owned initial terminalization', () => {
  describe('terminalization readback and caller capture', () => {
    it.each([
      'matching',
      'changed thread',
      'changed mode',
    ])('classifies %s agent readback without rewriting its raw row', async (variant) => {
      const h = await terminalFixture();
      const request = h.replace((snapshot) => {
        snapshot.requestContext[PROVENANCE].startIdentity = {
          owner: OWNER,
          target: { kind: 'agent', id: 'agent', threadId: 'thread' },
        };
        snapshot.requestContext[PROVENANCE].agentStart = { threaded: false };
        snapshot.requestContext[PROVENANCE].mutationEpoch = 7;
      });
      const observed = h.replace((snapshot) => {
        snapshot.status = 'running';
        snapshot.requestContext[PROVENANCE].startIdentity = {
          owner: OWNER,
          target: {
            kind: 'agent',
            id: 'agent',
            threadId: variant === 'changed thread' ? 'other-thread' : 'thread',
          },
        };
        snapshot.requestContext[PROVENANCE].agentStart = {
          threaded: variant === 'changed mode',
        };
        snapshot.requestContext[PROVENANCE].mutationEpoch = 7;
        snapshot.requestContext[PROVENANCE].attemptToken = 'resume';
        snapshot.requestContext[PROVENANCE].requestedBy = 'Bob';
        snapshot.requestContext[PROVENANCE].resumeCounts = [['gate', 1]];
        delete snapshot.requestContext[PROVENANCE].initialAdmission;
      }).expected;
      const before = h.rows();
      const calls = vi.spyOn(h.db, 'prepare');
      const result = await h.capability.terminalizeInitialAdmission(request);
      expect(result.kind).toBe(
        variant === 'matching' ? 'progressed' : 'conflict',
      );
      expect(result).toEqual({
        kind: variant === 'matching' ? 'progressed' : 'conflict',
        row: observed,
      });
      expect(h.rows()).toEqual(before);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
      expect(h.effects()).toBe(0);
    });

    it.each([
      ['zero', 'cancelled', false],
      ['zero', 'cancelled', true],
      ['zero', 'timed_out', false],
      ['zero', 'timed_out', true],
      ['throw', 'cancelled', false],
      ['throw', 'cancelled', true],
      ['throw', 'timed_out', false],
      ['throw', 'timed_out', true],
    ] as const)('returns actual %s %s completed=%s readback cleanup without retry', async (response, status, complete) => {
      const h = await terminalFixture();
      const request = h.replace((snapshot) => {
        snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
          version: 1,
          revision: 2,
          scheduleDispatch: {
            scheduleId: 'old-schedule',
            dispatchId: 'old-dispatch',
          },
          transitionIntent: {
            status: 'cancelled',
            requestedAt: 1,
            replayPrincipals: [OWNER],
          },
        };
      });
      const observed = h.replace((snapshot) => {
        snapshot.status = status;
        snapshot.timestamp = 456;
        delete snapshot.requestContext[PROVENANCE].initialAdmission;
        snapshot.requestContext[PROVENANCE].attemptToken = 'resume';
        snapshot.requestContext[PROVENANCE].requestedBy = 'Bob';
        snapshot.requestContext[PROVENANCE].resumeCounts = [['gate', 2]];
        snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
          version: 1,
          revision: 9,
          scheduleDispatch: {
            scheduleId: 'current-schedule',
            dispatchId: 'current-dispatch',
          },
          terminal: {
            status,
            error: {
              code: status === 'cancelled' ? 'CANCELLED' : 'TIMED_OUT',
              message:
                status === 'cancelled'
                  ? 'run was cancelled'
                  : 'run deadline expired',
            },
            transitionedAt: 456,
            replayPrincipals: [{ kind: 'service', id: 'current-replay' }],
            ...(complete ? { cleanupCompletedAt: 0 } : {}),
          },
        };
      }).expected;
      const before = h.rows();
      const participants = () => [
        h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
        h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
      ];
      const participantsBefore = participants();
      const fault = new Error('UPDATE response lost before progress readback');
      const calls = terminalResponse(h, (result) => {
        if (response === 'throw') throw fault;
        return result;
      });
      const outcome = await h.capability
        .terminalizeInitialAdmission(request)
        .catch((error: unknown) => error);
      expect(outcome).toEqual({
        kind: 'progressed',
        row: observed,
        cleanup: {
          revision: 9,
          status,
          cleanupCompleted: complete,
          scheduleDispatch: {
            scheduleId: 'current-schedule',
            dispatchId: 'current-dispatch',
          },
        },
      });
      expect(h.rows()).toEqual(before);
      expect(participants()).toEqual(participantsBefore);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
      expect(h.effects()).toBe(0);
    });

    it.each([
      'pending',
      'other generation',
    ])('retains the original fault for %s thrown readback', async (variant) => {
      const h = await terminalFixture();
      h.replace((snapshot) => {
        snapshot.changed = true;
        delete snapshot.requestContext[PROVENANCE].initialAdmission;
        if (variant === 'other generation') {
          snapshot.status = 'running';
          snapshot.requestContext[PROVENANCE].startToken = 'other-generation';
        }
      });
      const before = h.rows();
      const fault = new Error('original nonconvergent UPDATE fault');
      const calls = terminalResponse(h, () => {
        throw fault;
      });
      const outcome = await h.capability
        .terminalizeInitialAdmission(h.request)
        .catch((error: unknown) => error);
      expect(outcome).toBeInstanceOf(ExecutionFenceUnreadableError);
      if (!(outcome instanceof ExecutionFenceUnreadableError))
        throw new Error('expected original operation uncertainty');
      expect(outcome).toMatchObject({
        status: 503,
        message: 'initial admission cannot be terminalized',
      });
      expect(outcome.cause).toBe(fault);
      expect(h.rows()).toEqual(before);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    });

    it.each([
      'changed intent',
      'terminal lifecycle',
    ])('keeps %s pending lifecycle observations as conflict', async (variant) => {
      const h = await terminalFixture();
      const request = h.replace((snapshot) => {
        snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
          version: 1,
          revision: 2,
          transitionIntent: {
            status: 'cancelled',
            requestedAt: 1,
            replayPrincipals: [OWNER],
          },
        };
      });
      const observed = h.replace((snapshot) => {
        snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
          version: 1,
          revision: 4,
          ...(variant === 'changed intent'
            ? {
                transitionIntent: {
                  status: 'timed_out',
                  requestedAt: 2,
                  replayPrincipals: [{ kind: 'service', id: 'later' }],
                },
              }
            : {
                terminal: {
                  status: 'cancelled',
                  error: { code: 'CANCELLED', message: 'run was cancelled' },
                  transitionedAt: 2,
                  replayPrincipals: [OWNER],
                },
              }),
        };
      }).expected;
      const before = h.rows();
      const calls = vi.spyOn(h.db, 'prepare');
      const result = await h.capability.terminalizeInitialAdmission(request);
      expect(result).toEqual({ kind: 'conflict', row: observed });
      expect(h.rows()).toEqual(before);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    });

    it.each([
      false,
      null,
      'true',
      0,
    ])('refuses malformed admission marker %j in nonpending readback', async (marker) => {
      const h = await terminalFixture();
      h.replace((snapshot) => {
        snapshot.status = 'success';
        snapshot.requestContext[PROVENANCE].initialAdmission = marker;
      });
      const before = h.rows();
      const calls = vi.spyOn(h.db, 'prepare');
      const outcome = await h.capability
        .terminalizeInitialAdmission(h.request)
        .catch((error: unknown) => error);
      expect(outcome).toBeInstanceOf(ExecutionFenceUnreadableError);
      expect(outcome).toMatchObject({
        status: 503,
        message: 'initial admission cannot be terminalized',
        cause: expect.any(Error),
      });
      expect(h.rows()).toEqual(before);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    });

    it('ignores SELECT changes metadata when returning an unmarked raw progress row', async () => {
      const h = await terminalFixture();
      const observed = h.replace((snapshot) => {
        snapshot.status = 'running';
        delete snapshot.requestContext[PROVENANCE].initialAdmission;
      }).expected;
      const before = h.rows();
      let selectResponses = 0;
      const prepare = h.db.prepare.bind(h.db);
      const calls = vi.spyOn(h.db, 'prepare').mockImplementation((sql) => {
        const statement = prepare(sql);
        if (sql.startsWith('SELECT ')) {
          const bind = statement.bind.bind(statement);
          vi.spyOn(statement, 'bind').mockImplementation((...values) => {
            const bound = bind(...values);
            const all = bound.all.bind(bound);
            vi.spyOn(bound, 'all').mockImplementation(async () => {
              const result = await all();
              selectResponses += 1;
              return { ...result, meta: { changes: 77 } } as never;
            });
            return bound;
          });
        }
        return statement;
      });
      const result = await h.capability.terminalizeInitialAdmission(h.request);
      expect(result).toEqual({ kind: 'progressed', row: observed });
      expect(selectResponses).toBe(1);
      expect(h.rows()).toEqual(before);
      expect(calls).toHaveBeenCalledTimes(2);
      expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    });

    it.each([
      'request.expected',
      'request.execution',
      'request.attemptToken',
      'request.nowMs',
      'expected.tablePrefix',
      'expected.workflowId',
      'expected.runId',
      'expected.resourceId',
      'expected.snapshot',
      'expected.createdAt',
      'expected.updatedAt',
      'execution.tablePrefix',
      'execution.workflowId',
      'execution.runId',
      'execution.startToken',
    ])('preserves first throwing %s getter on the default domain without SQL', async (field) => {
      const h = await terminalFixture();
      const request = {
        ...h.request,
        expected: { ...h.request.expected },
        execution: { ...h.request.execution },
      };
      const fault = new Error(`caller fault at ${field}`);
      const getter = vi.fn(() => {
        throw fault;
      });
      const [part, key] = field.split('.');
      if (!key) throw new Error('caller field missing');
      const target =
        part === 'request'
          ? request
          : part === 'expected'
            ? request.expected
            : request.execution;
      Object.defineProperty(target, key, { enumerable: true, get: getter });
      const before = h.rows();
      const prepare = vi.spyOn(h.db, 'prepare');
      const batch = vi.spyOn(h.db, 'batch');
      const outcome = await h.capability
        .terminalizeInitialAdmission(request)
        .catch((error: unknown) => error);
      expect(outcome).toBe(fault);
      expect(getter).toHaveBeenCalledTimes(1);
      expect(prepare).not.toHaveBeenCalled();
      expect(batch).not.toHaveBeenCalled();
      expect(h.rows()).toEqual(before);
      expect(h.effects()).toBe(0);
    });

    it('captures every default-domain caller getter once before a held UPDATE and ignores later faults', async () => {
      const h = await terminalFixture();
      const reads = new Map<string, number>();
      const fault = new Error('caller reread after capture');
      let late = false;
      function observed<T extends object>(source: T, label: string): T {
        const copy = { ...source };
        for (const key of Object.keys(source))
          Object.defineProperty(copy, key, {
            enumerable: true,
            get() {
              const name = `${label}.${key}`;
              reads.set(name, (reads.get(name) ?? 0) + 1);
              if (late) throw fault;
              return source[key as keyof T];
            },
          });
        return copy;
      }
      const request = observed(
        {
          ...h.request,
          expected: observed({ ...h.request.expected }, 'expected'),
          execution: observed({ ...h.request.execution }, 'execution'),
        },
        'request',
      );
      let enter = () => {};
      let release = () => {};
      const entered = new Promise<void>((resolve) => {
        enter = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const calls = terminalResponse(h, async (result) => {
        enter();
        await held;
        return result;
      });
      const operation = h.capability.terminalizeInitialAdmission(request);
      try {
        await Promise.race([
          entered,
          operation.then(() => {
            throw new Error('UPDATE did not remain held');
          }),
        ]);
        expect(reads.size).toBe(15);
        expect([...reads.values()]).toEqual(new Array(15).fill(1));
        late = true;
        release();
        const result = await operation;
        expect(result.kind).toBe('terminalized');
        if (result.kind === 'conflict') throw new Error('unexpected conflict');
        const snapshot = JSON.parse(result.row.snapshot);
        const original = JSON.parse(h.request.expected.snapshot);
        const provenance = { ...original.requestContext[PROVENANCE] };
        delete provenance.initialAdmission;
        expect(snapshot).toEqual({
          ...original,
          status: 'failed',
          error: {
            name: 'StartOutcomeUnknown',
            message:
              'Start interrupted before a durable execution outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.',
          },
          requestContext: {
            ...original.requestContext,
            [PROVENANCE]: provenance,
          },
          timestamp: h.request.nowMs,
        });
        expect(result.row).toEqual({
          ...h.request.expected,
          snapshot: result.row.snapshot,
          updatedAt: new Date(h.request.nowMs).toISOString(),
        });
        expect(result).not.toHaveProperty('cleanup');
        expect(calls).toHaveBeenCalledTimes(1);
        expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
        expect(calls.mock.results[0]?.value.bind).toHaveBeenCalledWith(
          result.row.snapshot,
          result.row.updatedAt,
          h.request.expected.workflowId,
          h.request.expected.runId,
          h.request.expected.snapshot,
          h.request.expected.createdAt,
          h.request.expected.updatedAt,
          h.request.expected.resourceId,
        );
        expect(h.rows()).toEqual([
          expect.objectContaining({
            workflow_name: h.request.expected.workflowId,
            run_id: h.request.expected.runId,
            resourceId: h.request.expected.resourceId,
            createdAt: h.request.expected.createdAt,
            updatedAt: result.row.updatedAt,
            snapshot: result.row.snapshot,
          }),
        ]);
        expect([...reads.values()]).toEqual(new Array(15).fill(1));
        expect(h.effects()).toBe(0);
      } finally {
        release();
        await Promise.allSettled([operation]);
      }
    });
  });

  it.each([
    ['identity', null, 'execution identity must be an object'],
    ['identity', [], 'execution identity must be an object'],
    [
      'tablePrefix',
      null,
      'tablePrefix is not valid for this execution identity',
    ],
    [
      'tablePrefix',
      'bad-prefix',
      'tablePrefix is not valid for this execution identity',
    ],
    ['tablePrefix', 42, 'tablePrefix is not valid for this execution identity'],
    ['workflowId', '', 'workflowId must be a URL-path-safe identifier'],
    ['runId', 'bad/run', 'runId must be a URL-path-safe identifier'],
    ['startToken', '', 'startToken must be a URL-path-safe identifier'],
  ] as const)('preserves the field-specific %s identity400 for %j before stored-data validation', async (field, value, message) => {
    const h = await terminalFixture();
    const before = h.rows();
    const prepare = vi.spyOn(h.db, 'prepare');
    const batch = vi.spyOn(h.db, 'batch');
    const error = await h.capability
      .terminalizeInitialAdmission({
        ...h.request,
        expected: { ...h.request.expected, snapshot: '{' },
        execution:
          field === 'identity'
            ? value
            : { ...h.request.execution, [field]: value },
      } as InitialTerminalizationRequest)
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
    if (!(error instanceof InvalidExecutionIdentityError))
      throw new Error('expected input error');
    expect(error.constructor).toBe(InvalidExecutionIdentityError);
    expect(error.reason).toEqual({ code: 'INVALID_EXECUTION_IDENTITY' });
    expect(error).toMatchObject({
      name: 'InvalidExecutionIdentityError',
      status: 400,
      reason: { code: 'INVALID_EXECUTION_IDENTITY' },
      message,
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(prepare).not.toHaveBeenCalled();
    expect(batch).not.toHaveBeenCalled();
    expect(h.rows()).toEqual(before);
    expect(h.effects()).toBe(0);
  });

  it.each([
    ['pending', false],
    ['pending', null],
    ['pending', 'true'],
    ['pending', 0],
    ['success', false],
    ['success', null],
    ['success', 'true'],
    ['success', 0],
  ] as const)('refuses malformed admission marker in expected %s observation: %j', async (status, marker) => {
    const h = await terminalFixture();
    const request = h.replace((snapshot) => {
      snapshot.status = status;
      snapshot.requestContext[PROVENANCE].initialAdmission = marker;
    });
    const before = h.rows();
    const prepare = vi.spyOn(h.db, 'prepare');
    const batch = vi.spyOn(h.db, 'batch');
    const error = await h.capability
      .terminalizeInitialAdmission(request)
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(error).toMatchObject({
      name: 'ExecutionFenceUnreadableError',
      status: 503,
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
      message: 'initial admission cannot be terminalized',
      cause: expect.any(Error),
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(prepare).not.toHaveBeenCalled();
    expect(batch).not.toHaveBeenCalled();
    expect(h.rows()).toEqual(before);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'before',
    'after',
  ] as const)('recovers later progress after throwing %s actual UPDATE commit', async (phase) => {
    const h = await terminalFixture();
    const initial = h.rows();
    const fault = new Error(`${phase} actual commit response loss`);
    let observed: RawWorkflowSnapshot | undefined;
    let laterRows: unknown[] = [];
    let committed = 0;
    const prepare = h.db.prepare.bind(h.db);
    const calls = vi.spyOn(h.db, 'prepare').mockImplementation((sql) => {
      const statement = prepare(sql);
      if (sql.startsWith('UPDATE "')) {
        const bind = statement.bind.bind(statement);
        vi.spyOn(statement, 'bind').mockImplementation((...values) => {
          const bound = bind(...values);
          const all = bound.all.bind(bound);
          vi.spyOn(bound, 'all').mockImplementation(async () => {
            if (phase === 'after') {
              const response = await all();
              expect(response.results).toHaveLength(1);
              expect(h.rows()).toEqual([
                expect.objectContaining({
                  snapshot: values[0],
                  updatedAt: values[1],
                }),
              ]);
              committed += 1;
            } else {
              expect(h.rows()).toEqual(initial);
            }
            const row = h.replace((snapshot) => {
              snapshot.status = 'running';
              snapshot.timestamp = 456;
              snapshot.requestContext[PROVENANCE].attemptToken = 'resume';
              snapshot.requestContext[PROVENANCE].requestedBy = 'Bob';
              snapshot.requestContext[PROVENANCE].resumeCounts = [['gate', 1]];
              delete snapshot.requestContext[PROVENANCE].initialAdmission;
            }).expected;
            observed = { ...row, updatedAt: new Date(456).toISOString() };
            h.sql
              .prepare('UPDATE mastra_workflow_snapshot SET updatedAt = ?')
              .run(observed.updatedAt);
            laterRows = h.rows();
            throw fault;
          });
          return bound;
        });
      }
      return statement;
    });
    const result = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(result).toEqual({ kind: 'progressed', row: observed });
    expect(committed).toBe(phase === 'after' ? 1 : 0);
    expect(observed).toBeDefined();
    expect(h.rows()).toEqual(laterRows);
    expect(calls).toHaveBeenCalledTimes(2);
    expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
    expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    expect(h.effects()).toBe(0);
  });

  it.each([
    ['cancelled', 'throw'],
    ['cancelled', 'zero'],
    ['cancelled', 'returned'],
    ['timed_out', 'throw'],
    ['timed_out', 'zero'],
    ['timed_out', 'returned'],
  ] as const)('converges exact stored %s intent after %s response and explicit retry with incomplete cleanup', async (status, response) => {
    const h = await terminalFixture();
    const principals = [{ kind: 'service', id: 'original-replay' }];
    const scheduleDispatch = {
      scheduleId: 'intent-schedule',
      dispatchId: 'intent-dispatch',
    };
    const economicOperations = [{ id: 'economic', settlementState: 'settled' }];
    const request = h.replace((snapshot) => {
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: 4,
        deadlineAt: 123,
        scheduleDispatch,
        economicOperations,
        transitionIntent: {
          status,
          requestedAt: 1,
          replayPrincipals: principals,
        },
      };
    });
    const participants = () => [
      h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
      h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
    ];
    const before = participants();
    const cardinalities: number[] = [];
    const fault = new Error('stored intent response lost after commit');
    const calls = terminalResponse(h, (raw) => {
      const result = raw as { results: unknown[] };
      cardinalities.push(result.results.length);
      if (cardinalities.length === 1) {
        expect(result.results).toHaveLength(1);
        if (response === 'throw') throw fault;
        if (response === 'zero') return { results: [], meta: { changes: 0 } };
      }
      return raw;
    });
    const result = await h.capability.terminalizeInitialAdmission(request);
    expect(result.kind).toBe(
      response === 'returned' ? 'terminalized' : 'already-terminalized',
    );
    if (result.kind === 'conflict') throw new Error('unexpected conflict');
    const original = JSON.parse(request.expected.snapshot);
    const provenance = { ...original.requestContext[PROVENANCE] };
    delete provenance.initialAdmission;
    expect(JSON.parse(result.row.snapshot)).toEqual({
      ...original,
      status,
      error: {
        name: status === 'cancelled' ? 'RunCancelledError' : 'RunTimedOutError',
        message:
          status === 'cancelled' ? 'run was cancelled' : 'run deadline expired',
      },
      requestContext: {
        ...original.requestContext,
        [PROVENANCE]: provenance,
        [RUN_LIFECYCLE_CONTEXT_KEY]: {
          version: 1,
          revision: 5,
          deadlineAt: 123,
          scheduleDispatch,
          economicOperations,
          terminal: {
            status,
            error: {
              code: status === 'cancelled' ? 'CANCELLED' : 'TIMED_OUT',
              message:
                status === 'cancelled'
                  ? 'run was cancelled'
                  : 'run deadline expired',
            },
            transitionedAt: request.nowMs,
            replayPrincipals: principals,
          },
        },
      },
      timestamp: request.nowMs,
    });
    expect(result.row).toEqual({
      ...request.expected,
      snapshot: result.row.snapshot,
      updatedAt: new Date(request.nowMs).toISOString(),
    });
    expect(result.cleanup).toEqual({
      revision: 5,
      status,
      cleanupCompleted: false,
      scheduleDispatch,
    });
    expect(calls).toHaveBeenCalledTimes(response === 'returned' ? 1 : 2);
    expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
    if (response !== 'returned')
      expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    const committedRows = h.rows();
    expect(committedRows).toEqual([
      {
        workflow_name: result.row.workflowId,
        run_id: result.row.runId,
        snapshot: result.row.snapshot,
        resourceId: result.row.resourceId,
        createdAt: result.row.createdAt,
        updatedAt: result.row.updatedAt,
      },
    ]);
    calls.mockClear();
    const retry = await h.capability.terminalizeInitialAdmission(request);
    expect(retry).toEqual({ ...result, kind: 'already-terminalized' });
    expect(cardinalities).toEqual([1, 0]);
    expect(calls).toHaveBeenCalledTimes(2);
    expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
    expect(calls.mock.calls[1]?.[0]).toMatch(/^SELECT /);
    expect(h.rows()).toEqual(committedRows);
    expect(participants()).toEqual(before);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'unknown',
    'cancelled',
    'timed_out',
    'progressed',
    'conflict',
    'invalid',
    'unreadable',
  ] as const)('never enters engine, adapter upsert/delete, side tables or snapshot callbacks for %s terminalization', async (variant) => {
    const shouldPersist = vi.fn(() => true);
    const prune = vi.fn(
      ({ snapshot }: { snapshot: WorkflowRunState }) => snapshot,
    );
    const h = await fixture({
      keyed: true,
      state: 'proof-only',
      shouldPersist,
      prune,
    });
    const { value: run, witness } = await h.admit();
    expect(shouldPersist).toHaveBeenCalled();
    expect(prune).toHaveBeenCalled();
    const snapshot = JSON.parse(witness.row.snapshot);
    if (variant === 'cancelled' || variant === 'timed_out') {
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: 1,
        transitionIntent: {
          status: variant,
          requestedAt: 1,
          replayPrincipals: [OWNER],
        },
      };
    }
    const expected = { ...witness.row, snapshot: JSON.stringify(snapshot) };
    if (variant === 'progressed') snapshot.status = 'running';
    if (variant === 'conflict') snapshot.changed = true;
    h.sql
      .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
      .run(JSON.stringify(snapshot));
    const request: InitialTerminalizationRequest = {
      expected:
        variant === 'unreadable' ? { ...expected, snapshot: '{' } : expected,
      execution: h.input.execution,
      attemptToken: variant === 'invalid' ? '' : h.input.attemptToken,
      nowMs: 1_700_000_000_123,
    };
    const before = h.rows();
    const participants = () => [
      h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
      h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
    ];
    const participantsBefore = participants();
    const forbidden = vi.fn(() => {
      throw new Error('forbidden terminalization entry');
    });
    shouldPersist.mockClear().mockImplementation(forbidden);
    prune.mockClear().mockImplementation(forbidden);
    const sentinels = [
      forbidden,
      vi.spyOn(h.workflow, 'createRun').mockImplementation(forbidden),
      vi.spyOn(run, 'start').mockImplementation(forbidden),
      vi.spyOn(run, 'resume').mockImplementation(forbidden),
      vi
        .spyOn(h.domain, 'persistWorkflowSnapshot')
        .mockImplementation(forbidden),
      vi
        .spyOn(WorkflowsStorageD1.prototype, 'persistWorkflowSnapshot')
        .mockImplementation(forbidden),
      vi.spyOn(h.domain, 'deleteWorkflowRunById').mockImplementation(forbidden),
      vi
        .spyOn(WorkflowsStorageD1.prototype, 'deleteWorkflowRunById')
        .mockImplementation(forbidden),
      vi.spyOn(h.db, 'batch').mockImplementation(forbidden),
      shouldPersist,
      prune,
    ];
    const prepare = h.db.prepare.bind(h.db);
    const calls = vi.spyOn(h.db, 'prepare').mockImplementation((sql) => {
      if (
        !/^(UPDATE|SELECT) /.test(sql) ||
        !sql.includes('"mastra_workflow_snapshot"') ||
        /flowsafe_|ON CONFLICT|PRAGMA/.test(sql)
      )
        forbidden();
      return prepare(sql);
    });
    const result = await h.capability
      .terminalizeInitialAdmission(request)
      .catch((error: unknown) => error);
    for (const sentinel of sentinels) expect(sentinel).not.toHaveBeenCalled();
    expect(participants()).toEqual(participantsBefore);
    expect(h.effects()).toBe(0);
    expect(isDefinitiveInitialAdmissionRefusal(result, h.input.execution)).toBe(
      false,
    );
    if (variant === 'invalid' || variant === 'unreadable') {
      expect(result).toBeInstanceOf(
        variant === 'invalid'
          ? InvalidExecutionIdentityError
          : ExecutionFenceUnreadableError,
      );
      expect(calls).not.toHaveBeenCalled();
      expect(h.rows()).toEqual(before);
    } else if (variant === 'progressed' || variant === 'conflict') {
      expect(result).toMatchObject({ kind: variant });
      expect(calls).toHaveBeenCalledTimes(2);
      expect(h.rows()).toEqual(before);
    } else {
      expect(result).toMatchObject({ kind: 'terminalized' });
      expect(calls).toHaveBeenCalledTimes(1);
      expect(h.rows()).toEqual([
        expect.objectContaining({
          snapshot: expect.stringContaining(
            `"status":"${variant === 'unknown' ? 'failed' : variant}"`,
          ),
        }),
      ]);
    }
  });

  it('preserves the initiating agent mode, identity and original epoch', async () => {
    const h = await terminalFixture();
    const request = h.replace((snapshot) => {
      snapshot.requestContext[PROVENANCE].startIdentity = {
        owner: OWNER,
        target: { kind: 'agent', id: 'agent', threadId: 'thread' },
      };
      snapshot.requestContext[PROVENANCE].agentStart = { threaded: false };
      snapshot.requestContext[PROVENANCE].mutationEpoch = 7;
    });
    const result = await h.capability.terminalizeInitialAdmission(request);
    if (result.kind === 'conflict') throw new Error('unexpected conflict');
    expect(
      JSON.parse(result.row.snapshot).requestContext[PROVENANCE],
    ).toMatchObject({
      startIdentity: {
        owner: OWNER,
        target: { kind: 'agent', id: 'agent', threadId: 'thread' },
      },
      agentStart: { threaded: false },
      mutationEpoch: 7,
    });
  });
  it('preserves current lifecycle-terminal precedence and cleanup on progress readback', async () => {
    const h = await terminalFixture();
    h.replace((snapshot) => {
      snapshot.status = 'success';
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: 9,
        scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
        terminal: {
          status: 'timed_out',
          error: { code: 'TIMED_OUT', message: 'run deadline expired' },
          transitionedAt: 1,
          replayPrincipals: [OWNER],
          cleanupCompletedAt: 0,
        },
      };
    });
    expect(
      await h.capability.terminalizeInitialAdmission(h.request),
    ).toMatchObject({
      kind: 'progressed',
      cleanup: {
        revision: 9,
        status: 'timed_out',
        cleanupCompleted: true,
        scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
      },
    });
  });
  it.each([
    'zero',
    'throw',
  ] as const)('keeps malformed %s readback unreadable with the original operation cause', async (mode) => {
    const h = await terminalFixture();
    const fault = new Error('original write fault');
    terminalResponse(h, () => {
      h.sql
        .prepare('UPDATE mastra_workflow_snapshot SET snapshot = ?')
        .run('{');
      if (mode === 'throw') throw fault;
      return { results: [], meta: { changes: 0 } };
    });
    const error = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(error).toMatchObject({
      status: 503,
      message: 'initial admission cannot be terminalized',
      cause: mode === 'throw' ? fault : expect.any(Error),
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
  });
  it('derives unknown-effects failure solely from the expected initial row', async () => {
    const h = await terminalFixture();
    const request = h.replace((snapshot) => {
      snapshot.result = { stale: true };
      snapshot.error = { message: 'stale' };
      snapshot.steps = { retained: { output: 'λ' } };
      snapshot.requestContext[PROVENANCE].unknown = { keep: true };
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: Number.MAX_SAFE_INTEGER,
        economicOperations: [{ id: 'economic', settlementState: 'disputed' }],
      };
    });
    const participants = () => [
      h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
      h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
    ];
    const before = participants();
    const prepare = vi.spyOn(h.db, 'prepare');
    const batch = vi.spyOn(h.db, 'batch');
    const result = await h.capability.terminalizeInitialAdmission({
      ...request,
      requestedStatus: 'cancelled',
      failedSnapshot: { status: 'success' },
    } as InitialTerminalizationRequest);
    expect(result.kind).toBe('terminalized');
    if (result.kind === 'conflict') throw new Error('unexpected conflict');
    const snapshot = JSON.parse(result.row.snapshot);
    expect(snapshot).toMatchObject({
      status: 'failed',
      runId: 'run',
      error: {
        name: 'StartOutcomeUnknown',
        message:
          'Start interrupted before a durable execution outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.',
      },
      activePaths: [],
      activeStepsPath: {},
      suspendedPaths: {},
      waitingPaths: {},
      resumeLabels: {},
      steps: { retained: { output: 'λ' } },
      timestamp: request.nowMs,
    });
    expect(snapshot).not.toHaveProperty('result');
    expect(snapshot.error).not.toHaveProperty('stack');
    const original = JSON.parse(request.expected.snapshot).requestContext;
    const expectedContext = {
      ...original,
      [PROVENANCE]: { ...original[PROVENANCE] },
    };
    delete expectedContext[PROVENANCE].initialAdmission;
    expect(snapshot.requestContext).toEqual(expectedContext);
    expect(result).not.toHaveProperty('cleanup');
    expect(result.row).toMatchObject({
      resourceId: request.expected.resourceId,
      createdAt: request.expected.createdAt,
      updatedAt: new Date(request.nowMs).toISOString(),
    });
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(prepare.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
    expect(batch).not.toHaveBeenCalled();
    expect(participants()).toEqual(before);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'cancelled',
    'timed_out',
  ] as const)('honors stored %s intent without new caller authority', async (status) => {
    const h = await terminalFixture();
    const principals = [{ kind: 'service', id: 'original' }];
    const request = h.replace((snapshot) => {
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: Number.MAX_SAFE_INTEGER - 1,
        deadlineAt: 123,
        scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
        economicOperations: [{ id: 'economic', settlementState: 'settled' }],
        transitionIntent: {
          status,
          requestedAt: 1,
          replayPrincipals: principals,
        },
      };
    });
    const result = await h.capability.terminalizeInitialAdmission(request);
    expect(result.kind).toBe('terminalized');
    if (result.kind === 'conflict') throw new Error('unexpected conflict');
    const snapshot = JSON.parse(result.row.snapshot);
    expect(snapshot.status).toBe(status);
    expect(snapshot.error).toEqual({
      name: status === 'cancelled' ? 'RunCancelledError' : 'RunTimedOutError',
      message:
        status === 'cancelled' ? 'run was cancelled' : 'run deadline expired',
    });
    expect(snapshot.requestContext[PROVENANCE]).not.toHaveProperty(
      'initialAdmission',
    );
    const lifecycle = snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY];
    expect(lifecycle).not.toHaveProperty('transitionIntent');
    expect(lifecycle).toMatchObject({
      revision: Number.MAX_SAFE_INTEGER,
      deadlineAt: 123,
      economicOperations: [{ id: 'economic', settlementState: 'settled' }],
      terminal: {
        status,
        transitionedAt: request.nowMs,
        replayPrincipals: principals,
      },
    });
    expect(lifecycle.terminal).not.toHaveProperty('cleanupCompletedAt');
    expect(result.cleanup).toEqual({
      revision: Number.MAX_SAFE_INTEGER,
      status,
      cleanupCompleted: false,
      scheduleDispatch: { scheduleId: 'schedule', dispatchId: 'dispatch' },
    });
  });

  it.each([
    'disputed',
    'exhausted',
  ])('retains %s lifecycle state without a write', async (variant) => {
    const h = await terminalFixture();
    const request = h.replace((snapshot) => {
      snapshot.requestContext[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: variant === 'exhausted' ? Number.MAX_SAFE_INTEGER : 1,
        economicOperations: [{ id: 'operation', settlementState: variant }],
        transitionIntent: {
          status: 'cancelled',
          requestedAt: 1,
          replayPrincipals: [OWNER],
        },
      };
    });
    const before = h.rows();
    const prepare = vi.spyOn(h.db, 'prepare');
    const error = await h.capability
      .terminalizeInitialAdmission(request)
      .catch((error: unknown) => error);
    expect(h.rows()).toEqual(before);
    expect(prepare).not.toHaveBeenCalled();
    if (variant === 'disputed')
      expect(error).toBeInstanceOf(RunLifecycleBlockedError);
    else
      expect(error).toMatchObject({
        message: 'initial admission cannot be terminalized',
        cause: { message: 'run lifecycle revision cannot advance' },
      });
    expect(isDefinitiveInitialAdmissionRefusal(error, request.execution)).toBe(
      false,
    );
  });

  it.each([
    ['invalid JSON', 503],
    ['array root', 503],
    ['unknown status', 503],
    ['missing run', 503],
    ['wrong body run', 503],
    ['array context', 503],
    ['null provenance', 503],
    ['unknown version', 503],
    ['malformed counts nonpending', 503],
    ['malformed lifecycle nonpending', 503],
    ['malformed epoch', 503],
    ['legacy', 400],
    ['absent provenance', 400],
    ['nonpending', 400],
    ['unmarked', 400],
    ['other S', 400],
    ['other H', 400],
    ['progressed requester', 400],
    ['progressed count', 400],
    ['terminal lifecycle', 400],
    ['active path', 400],
    ['missing control', 400],
    ['wrong aux run', 400],
    ['wrong aux workflow', 400],
    ['inherited workflow target', 400],
  ] as const)('distinguishes %s observations with status %i before SQL', async (variant, status) => {
    const h = await terminalFixture();
    const value = JSON.parse(h.request.expected.snapshot);
    const context = value.requestContext;
    const provenance = context[PROVENANCE];
    if (variant === 'unknown status') value.status = 'invented';
    if (variant === 'missing run') delete value.runId;
    if (variant === 'wrong body run') value.runId = 'other';
    if (variant === 'array context') value.requestContext = [];
    if (variant === 'null provenance') context[PROVENANCE] = null;
    if (variant === 'unknown version') provenance.version = 3;
    if (variant === 'legacy') provenance.version = 1;
    if (variant === 'absent provenance') delete context[PROVENANCE];
    if (variant.includes('nonpending')) value.status = 'success';
    if (variant === 'malformed counts nonpending')
      provenance.resumeCounts = [['step', -1]];
    if (variant === 'malformed lifecycle nonpending')
      context[RUN_LIFECYCLE_CONTEXT_KEY] = { version: 1, revision: 0 };
    if (variant === 'malformed epoch') provenance.mutationEpoch = '0';
    if (variant === 'unmarked') delete provenance.initialAdmission;
    if (variant === 'other S') provenance.startToken = 'other';
    if (variant === 'other H') provenance.attemptToken = 'other';
    if (variant === 'progressed requester') provenance.requestedBy = 'Bob';
    if (variant === 'progressed count') provenance.resumeCounts = [['step', 1]];
    if (variant === 'terminal lifecycle')
      context[RUN_LIFECYCLE_CONTEXT_KEY] = {
        version: 1,
        revision: 1,
        terminal: {
          status: 'cancelled',
          error: { code: 'CANCELLED', message: 'run was cancelled' },
          transitionedAt: 1,
          replayPrincipals: [OWNER],
        },
      };
    if (variant === 'active path') value.activePaths = ['step'];
    if (variant === 'missing control') delete value.waitingPaths;
    if (variant === 'wrong aux run') context.runId = 'other';
    if (variant === 'wrong aux workflow')
      context['breakwater.workflowScope'] = 'other';
    if (variant === 'inherited workflow target')
      provenance.startIdentity.target.id = 'parent';
    const snapshot =
      variant === 'invalid JSON'
        ? '{'
        : variant === 'array root'
          ? '[]'
          : JSON.stringify(value);
    const before = h.rows();
    const prepare = vi.spyOn(h.db, 'prepare');
    const error = await h.capability
      .terminalizeInitialAdmission({
        ...h.request,
        expected: { ...h.request.expected, snapshot },
      })
      .catch((error: unknown) => error);
    expect(h.rows()).toEqual(before);
    expect(prepare).not.toHaveBeenCalled();
    expect(error).toMatchObject({ status });
    if (status === 503) {
      expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
      expect(error).toMatchObject({
        name: 'ExecutionFenceUnreadableError',
        reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
        message: 'initial admission cannot be terminalized',
        cause: expect.any(Error),
      });
    } else {
      expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
      if (!(error instanceof InvalidExecutionIdentityError))
        throw new Error('expected input error');
      expect(error.constructor).toBe(InvalidExecutionIdentityError);
      expect(error.reason).toEqual({ code: 'INVALID_EXECUTION_IDENTITY' });
      expect(error).toMatchObject({
        name: 'InvalidExecutionIdentityError',
        reason: { code: 'INVALID_EXECUTION_IDENTITY' },
        message: 'initial admission identity is inconsistent',
      });
    }
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
  });

  it('rejects incoherent caller frames and preserves original getter faults before SQL', async () => {
    const h = await terminalFixture();
    const admissionMessage = 'initial admission identity is inconsistent';
    const invalid: Array<[unknown, string]> = [
      [null, admissionMessage],
      [[], admissionMessage],
      [{}, 'execution identity must be an object'],
      ...[NaN, Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER].map(
        (nowMs): [unknown, string] => [
          { ...h.request, nowMs },
          admissionMessage,
        ],
      ),
      [{ ...h.request, attemptToken: '' }, admissionMessage],
      [
        {
          ...h.request,
          execution: { ...h.request.execution, tablePrefix: null },
        },
        'tablePrefix is not valid for this execution identity',
      ],
    ];
    for (const [field, value] of Object.entries({
      tablePrefix: 'other_',
      workflowId: 'other',
      runId: 'other',
      snapshot: null,
      resourceId: 1,
      createdAt: null,
      updatedAt: null,
    }))
      invalid.push([
        {
          ...h.request,
          expected: { ...h.request.expected, [field]: value },
        },
        admissionMessage,
      ]);
    const prepare = vi.spyOn(h.db, 'prepare');
    const batch = vi.spyOn(h.db, 'batch');
    const before = h.rows();
    for (const [request, message] of invalid) {
      const error = await h.capability
        .terminalizeInitialAdmission(request as InitialTerminalizationRequest)
        .catch((error: unknown) => error);
      expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
      if (!(error instanceof InvalidExecutionIdentityError))
        throw new Error('expected input error');
      expect(error.constructor).toBe(InvalidExecutionIdentityError);
      expect(error.reason).toEqual({ code: 'INVALID_EXECUTION_IDENTITY' });
      expect(error).toMatchObject({
        status: 400,
        name: 'InvalidExecutionIdentityError',
        reason: { code: 'INVALID_EXECUTION_IDENTITY' },
        message,
      });
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(false);
      expect(prepare).not.toHaveBeenCalled();
      expect(batch).not.toHaveBeenCalled();
      expect(h.rows()).toEqual(before);
    }
    const fault = new Error('caller accessor');
    await expect(
      h.capability.terminalizeInitialAdmission({
        ...h.request,
        get expected(): RawWorkflowSnapshot {
          throw fault;
        },
      }),
    ).rejects.toBe(fault);
    expect(prepare).not.toHaveBeenCalled();
  });

  it.each([
    'workflow_name',
    'run_id',
    'snapshot',
    'createdAt',
    'updatedAt',
    'resourceId',
  ])('compares the original raw %s field without retry', async (field) => {
    const h = await terminalFixture();
    const value =
      field === 'snapshot'
        ? JSON.stringify({
            ...JSON.parse(h.request.expected.snapshot),
            changed: true,
          })
        : 'other';
    h.sql
      .prepare(`UPDATE mastra_workflow_snapshot SET ${field} = ?`)
      .run(value);
    const before = h.rows();
    const prepare = vi.spyOn(h.db, 'prepare');
    const result = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(h.rows()).toEqual(before);
    expect(result).toMatchObject({ kind: 'conflict' });
    expect(prepare).toHaveBeenCalledTimes(2);
  });

  it.each([
    'committed',
    'zero',
    'throw-before',
    'missing-table',
  ])('classifies %s response loss without automatic replay', async (variant) => {
    const h = await terminalFixture();
    const fault = new Error('write response lost');
    if (variant === 'missing-table')
      h.sql.exec('DROP TABLE mastra_workflow_snapshot');
    const prepare = h.db.prepare.bind(h.db);
    const calls = vi.spyOn(h.db, 'prepare').mockImplementation((sql) => {
      const statement = prepare(sql);
      if (sql.startsWith('UPDATE "')) {
        const bind = statement.bind.bind(statement);
        vi.spyOn(statement, 'bind').mockImplementation((...values) => {
          const bound = bind(...values);
          const all = bound.all.bind(bound);
          vi.spyOn(bound, 'all').mockImplementation(async () => {
            if (variant !== 'throw-before' && variant !== 'missing-table')
              await all();
            if (variant === 'zero')
              return { results: [], meta: { changes: 0 } };
            throw fault;
          });
          return bound;
        });
      }
      return statement;
    });
    const outcome = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(calls).toHaveBeenCalledTimes(2);
    if (variant === 'committed' || variant === 'zero')
      expect(outcome).toMatchObject({ kind: 'already-terminalized' });
    else
      expect(outcome).toMatchObject({
        message: 'initial admission cannot be terminalized',
        cause: fault,
      });
    expect(
      isDefinitiveInitialAdmissionRefusal(outcome, h.input.execution),
    ).toBe(false);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'running',
    'success',
    'failed',
    'suspended',
    'waiting',
    'paused',
    'canceled',
    'bailed',
    'skipped',
    'tripwire',
    'waiting_callback',
    'waiting_signal',
    'retry_wait',
    'cancelled',
    'timed_out',
  ])('recognizes same-generation %s progress with a retained admission stamp', async (status) => {
    const h = await terminalFixture();
    h.replace((snapshot) => {
      snapshot.status = status;
      snapshot.requestContext[PROVENANCE].requestedBy = 'Bob';
      snapshot.requestContext[PROVENANCE].attemptToken = 'resume';
      snapshot.requestContext[PROVENANCE].resumeCounts = [
        ['gate', Number.MAX_SAFE_INTEGER],
      ];
    });
    const before = h.rows();
    const result = await h.capability.terminalizeInitialAdmission(h.request);
    expect(h.rows()).toEqual(before);
    expect(result.kind).toBe('progressed');
    expect(result).not.toHaveProperty('cleanup');
  });

  it.each([
    'pending marked',
    'pending unmarked',
    'other generation',
    'other owner',
    'other target',
    'other epoch',
    'legacy',
  ])('does not call %s convergence progress', async (variant) => {
    const h = await terminalFixture();
    h.replace((snapshot) => {
      snapshot.changed = true;
      snapshot.status = variant.startsWith('pending') ? 'pending' : 'success';
      const provenance = snapshot.requestContext[PROVENANCE];
      if (variant === 'pending unmarked') delete provenance.initialAdmission;
      if (variant === 'other generation') provenance.startToken = 'other';
      if (variant === 'other owner') provenance.startIdentity.owner.id = 'Bob';
      if (variant === 'other target')
        provenance.startIdentity.target.id = 'other';
      if (variant === 'other epoch') provenance.mutationEpoch = 1;
      if (variant === 'legacy') provenance.version = 1;
    });
    const before = h.rows();
    expect(
      await h.capability.terminalizeInitialAdmission(h.request),
    ).toMatchObject({ kind: 'conflict' });
    expect(h.rows()).toEqual(before);
  });

  it.each([
    'workflow_name',
    'run_id',
    'resourceId',
    'snapshot',
    'createdAt',
    'updatedAt',
  ] as const)('rejects changed returned %s after an actual commit without readback', async (field) => {
    const h = await terminalFixture();
    const committed: Record<string, unknown>[] = [];
    const calls = terminalResponse(h, (raw) => {
      const response = raw as { results: Record<string, unknown>[] };
      expect(response.results).toHaveLength(1);
      const row = response.results[0];
      if (!row) throw new Error('committed returned row required');
      committed.push({ ...row });
      const value =
        field === 'snapshot'
          ? JSON.stringify({
              ...JSON.parse(row.snapshot as string),
              changedReturn: true,
            })
          : field === 'createdAt' || field === 'updatedAt'
            ? '2000-01-01T00:00:00.000Z'
            : 'different-returned-value';
      expect(value).not.toEqual(row[field]);
      return { ...response, results: [{ ...row, [field]: value }] };
    });
    const outcome = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(outcome).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(outcome).toMatchObject({
      status: 503,
      message: 'initial admission cannot be terminalized',
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
      cause: expect.any(Error),
    });
    expect(calls).toHaveBeenCalledTimes(1);
    expect(calls.mock.calls[0]?.[0]).toMatch(/^UPDATE /);
    expect(committed).toHaveLength(1);
    expect(h.rows()).toEqual(committed);
    expect(
      isDefinitiveInitialAdmissionRefusal(outcome, h.input.execution),
    ).toBe(false);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'false success',
    'missing results',
    'multiple rows',
    'wrong row',
    'inconsistent changes',
    'negative changes',
    'sparse results',
    'inherited result',
    'shrinking results',
  ])('refuses %s RETURNING without a recovery read or definitive-zero evidence', async (variant) => {
    const h = await terminalFixture();
    const calls = terminalResponse(h, (raw) => {
      const response = raw as {
        results: Record<string, unknown>[];
        meta: { changes: number };
      };
      const row = response.results[0];
      if (!row) throw new Error('fixture requires committed returned row');
      if (variant === 'false success') return { ...response, success: false };
      if (variant === 'missing results') return {};
      if (variant === 'multiple rows')
        return { ...response, results: [row, row] };
      if (variant === 'wrong row')
        return { ...response, results: [{ ...row, snapshot: '{}' }] };
      if (variant === 'inconsistent changes')
        return { ...response, meta: { changes: 0 } };
      if (variant === 'negative changes')
        return { ...response, meta: { changes: -1 } };
      if (variant === 'sparse results')
        return { ...response, results: new Array(1) };
      if (variant === 'inherited result') {
        const rows = new Array(1);
        Object.setPrototypeOf(
          rows,
          Object.assign(Object.create(Array.prototype), { 0: row }),
        );
        return { ...response, results: rows };
      }
      const rows = [row, row];
      Object.defineProperty(rows, 0, {
        get() {
          rows.length = 1;
          return row;
        },
      });
      return { ...response, results: rows };
    });
    const error = await h.capability
      .terminalizeInitialAdmission(h.request)
      .catch((error: unknown) => error);
    expect(calls).toHaveBeenCalledTimes(1);
    expect(error).toMatchObject({
      message: 'initial admission cannot be terminalized',
      status: 503,
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(h.rows()).toEqual([
      expect.objectContaining({
        snapshot: expect.stringContaining('"status":"failed"'),
      }),
    ]);
  });

  it('captures returned envelope, row fields and changes once without consulting custom iterators', async () => {
    const h = await terminalFixture();
    const reads: string[] = [];
    terminalResponse(h, (raw) => {
      const response = raw as {
        results: Record<string, unknown>[];
        meta: { changes: number };
      };
      const original = response.results[0];
      if (!original) throw new Error('committed row required');
      const row = Object.fromEntries(
        Object.keys(original).map((key) => [key, original[key]]),
      );
      for (const key of Object.keys(row))
        Object.defineProperty(row, key, {
          get() {
            reads.push(key);
            if (reads.filter((value) => value === key).length > 1)
              throw new Error('row reread');
            return original[key];
          },
        });
      const rows = [row];
      rows[Symbol.iterator] = () => {
        throw new Error('custom iterator');
      };
      return {
        get results() {
          reads.push('results');
          return rows;
        },
        get meta() {
          reads.push('meta');
          return {
            get changes() {
              reads.push('changes');
              return 1;
            },
          };
        },
      };
    });
    expect(
      await h.capability.terminalizeInitialAdmission(h.request),
    ).toMatchObject({ kind: 'terminalized' });
    expect(reads.length).toBe(9);
    expect(new Set(reads).size).toBe(9);
  });
});

function claim(input: InitialRunAdmission) {
  if (!input.reservation) throw new Error('test requires a keyed fixture');
  return input.reservation;
}

describe('FS8 D2 dormant reservation primitives', () => {
  async function modernClaim() {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const store = h.input.reservationStore;
    if (!store) throw new Error('reservation store is missing');
    h.sql.exec("UPDATE flowsafe_start_idempotency SET state = 'reserved'");
    const reserved = await store.readForAdmission('key');
    if (!reserved) throw new Error('reserved row is missing');
    const claimed = await store.claimReservation(reserved);
    if (!claimed) throw new Error('claimed row is missing');
    expect(claimed.state).toBe('started');
    expect(claimed.updatedAt).toBeGreaterThan(reserved.updatedAt);
    return { ...h, store, reserved, claimed };
  }

  it('binds a real exact claim through atomic initial admission and returns its witness', async () => {
    const h = await modernClaim();
    const admitted = await h.admit({ ...h.input, reservation: h.claimed });
    expect(admitted.witness.execution).toEqual(h.input.execution);
    expect(h.rows()).toHaveLength(1);
    expect((await h.store.readForAdmission('key'))?.binding).toEqual({
      kind: 'bound',
      execution: h.input.execution,
    });
    expect((await h.fence.read()).proofExecution).toEqual(h.input.execution);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'reserved',
    'stale',
    'owner',
    'target',
    'thread',
  ] as const)('refuses a %s observation without an initial row or binding', async (mode) => {
    const h = await modernClaim();
    let reservation = h.claimed;
    if (mode === 'reserved') reservation = h.reserved;
    if (mode === 'stale') {
      expect(await h.store.releaseReservation(h.claimed)).toBe(true);
      const released = await h.store.readForAdmission('key');
      if (!released) throw new Error('released row is missing');
      expect(await h.store.claimReservation(released)).toBeDefined();
    }
    if (mode === 'owner')
      reservation = { ...reservation, owner: { ...OWNER, id: 'other' } };
    if (mode === 'target') reservation = { ...reservation, targetId: 'other' };
    if (mode === 'thread') reservation = { ...reservation, threadId: 'other' };
    const before = h.sql
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    const outcome = await h
      .admit({ ...h.input, reservation })
      .catch((error: unknown) => error);
    expect(
      h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
    ).toEqual(before);
    expect(h.rows()).toEqual([]);
    expect(outcome).toBeInstanceOf(Error);
    expect((await h.fence.read()).proofExecution).toBeUndefined();
    expect(h.effects()).toBe(0);
  });
});

describe('owned initial workflow admission', () => {
  it.each([
    [
      'unsupported column',
      'ALTER TABLE flowsafe_execution_fence ADD COLUMN admission_extension TEXT',
    ],
    [
      'nullable-id second row',
      "INSERT INTO flowsafe_execution_fence (id, state, updated_at) VALUES (NULL, 'open', 0)",
    ],
  ])('refuses a final %s without changing keyed proof participants', async (_condition, change) => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const resources = new D1ResourceOwnershipStore(
      h.db as unknown as ResourceOwnershipDatabase,
    );
    expect(
      await resources.reserveAll(
        [{ kind: 'run', resourceId: 'run' }],
        OWNER,
        'correlation',
      ),
    ).toBe(true);
    const input = {
      ...h.input,
      runOwnerGuard: { owner: OWNER, reservationToken: 'correlation' },
    };
    const participants = () => ({
      reservations: h.sql
        .prepare('SELECT * FROM flowsafe_start_idempotency')
        .all(),
      owners: h.sql.prepare('SELECT * FROM flowsafe_resource_owners').all(),
    });
    const before = participants();
    const batch = h.db.batch.bind(h.db);
    let changedFence: unknown;
    const write = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        expect(h.rows()).toEqual([]);
        expect(
          h.sql.prepare('PRAGMA ignore_check_constraints').get(),
        ).toMatchObject({
          ignore_check_constraints: 0,
        });
        h.sql.exec(change);
        changedFence = h.sql
          .prepare('SELECT * FROM flowsafe_execution_fence')
          .all();
        const result = await batch(statements);
        expect(h.rows()).toEqual([]);
        expect(participants()).toEqual(before);
        expect(
          h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
        ).toEqual(changedFence);
        return result;
      });
    const outcome = await h.admit(input).catch((error: unknown) => error);
    expect(outcome).toMatchObject({
      status: 503,
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
    expect(isDefinitiveInitialAdmissionRefusal(outcome, input.execution)).toBe(
      true,
    );
    expect(write).toHaveBeenCalledOnce();
    expect(h.onInitialWriteAttempt).toHaveBeenCalledOnce();
    expect(h.rows()).toEqual([]);
    expect(participants()).toEqual(before);
    expect(
      h.sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
    ).toEqual(changedFence);
    expect(h.effects()).toBe(0);
    await expect(h.fence.readForAdmission()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it('admits exact active-epoch proof and reservation participants under the supported schema', async () => {
    const h = await fixture({ keyed: true });
    const reading = await h.fence.transition({
      expected: 'open',
      next: 'proof-only',
      proofKey: 'key',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    const input = {
      ...h.input,
      mutationEpoch: reading.mutationEpoch,
      proof: {
        key: 'key',
        mutationEpoch: reading.mutationEpoch,
        transitionRevision: reading.transitionRevision,
      },
      requestContext: {
        ...h.input.requestContext,
        [PROVENANCE]: {
          ...(h.input.requestContext[PROVENANCE] as object),
          mutationEpoch: reading.mutationEpoch,
        },
      },
    };
    expect((await h.admit(input)).witness.execution).toEqual(input.execution);
    expect(h.rows()).toHaveLength(1);
    expect(
      (await h.input.reservationStore?.readForAdmission('key'))?.binding,
    ).toEqual({
      kind: 'bound',
      execution: input.execution,
    });
    expect((await h.fence.readForAdmission()).reading.proofExecution).toEqual(
      input.execution,
    );
    expect(h.effects()).toBe(0);
  });

  it.each([
    'before insert',
    'after insert',
  ] as const)('classifies response loss when the fence schema changes %s', async (phase) => {
    const h = await fixture({ keyed: true });
    const before = h.sql
      .prepare('SELECT * FROM flowsafe_start_idempotency')
      .all();
    const batch = h.db.batch.bind(h.db);
    const lost = new Error('initial admission response lost');
    const change = () =>
      h.sql.exec(
        'ALTER TABLE flowsafe_execution_fence ADD COLUMN admission_extension TEXT',
      );
    const write = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        if (phase === 'before insert') change();
        await batch(statements);
        if (phase === 'after insert') change();
        throw lost;
      });
    const outcome = await h.admit().catch((error: unknown) => error);
    expect(write).toHaveBeenCalledTimes(2);
    expect(h.onInitialWriteAttempt).toHaveBeenCalledOnce();
    expect(h.effects()).toBe(0);
    if (phase === 'before insert') {
      expect(outcome).toBeInstanceOf(ExecutionFenceUnreadableError);
      expect(outcome).toMatchObject({ cause: lost });
      expect(
        isDefinitiveInitialAdmissionRefusal(outcome, h.input.execution),
      ).toBe(false);
      expect(h.rows()).toEqual([]);
      expect(
        h.sql.prepare('SELECT * FROM flowsafe_start_idempotency').all(),
      ).toEqual(before);
    } else {
      expect(outcome).toMatchObject({
        witness: { execution: h.input.execution },
      });
      expect(h.rows()).toHaveLength(1);
      expect(
        (await h.input.reservationStore?.readForAdmission('key'))?.binding,
      ).toEqual({
        kind: 'bound',
        execution: h.input.execution,
      });
    }
    await expect(h.fence.readForAdmission()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it.each([
    'foreign fence binding',
    'foreign reservation binding',
    'wrapped fence binding',
  ])('rejects a valid %s before any initial-admission I/O', async (variant) => {
    vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000);
    const local = await fixture({ keyed: true, state: 'proof-only' });
    const foreign = await fixture({ keyed: true, state: 'proof-only' });
    const foreignReservations = foreign.input.reservationStore;
    if (!foreignReservations)
      throw new Error('test requires foreign reservations');
    const wrapped: InitialAdmissionDatabase = {
      prepare: (sql) => local.db.prepare(sql),
      batch: (statements) => local.db.batch(statements),
    };
    const input: InitialRunAdmission = {
      ...local.input,
      fence:
        variant === 'foreign fence binding'
          ? foreign.fence
          : variant === 'wrapped fence binding'
            ? new ExecutionFenceStore(wrapped)
            : local.fence,
      reservationStore:
        variant === 'foreign reservation binding'
          ? foreignReservations
          : local.input.reservationStore,
    };
    const participants = () =>
      [local, foreign].map(({ sql, rows }) => ({
        snapshots: rows(),
        fence: sql.prepare('SELECT * FROM flowsafe_execution_fence').all(),
        reservations: sql
          .prepare('SELECT * FROM flowsafe_start_idempotency ORDER BY key')
          .all(),
      }));
    const before = participants();
    const io = [local.db, foreign.db, wrapped].flatMap((db) => [
      vi.spyOn(db, 'prepare'),
      vi.spyOn(db, 'batch'),
    ]);
    const createRun = vi.fn(() => local.workflow.createRun({ runId: 'run' }));
    const outcome = await local.capability
      .withInitialAdmission(input, createRun)
      .catch((error: unknown) => error);

    expect(participants()).toEqual(before);
    for (const method of io) expect(method).not.toHaveBeenCalled();
    expect(createRun).not.toHaveBeenCalled();
    expect(local.onInitialWriteAttempt).not.toHaveBeenCalled();
    expect(foreign.onInitialWriteAttempt).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({
      status: 400,
      reason: { code: 'INVALID_EXECUTION_IDENTITY' },
    });
    expect(outcome).not.toHaveProperty('witness');
    expect(isDefinitiveInitialAdmissionRefusal(outcome, input.execution)).toBe(
      false,
    );
    expect([local.effects(), foreign.effects()]).toEqual([0, 0]);
  });
  it('refuses inherited batch slots after snapshot key and proof have all committed', async () => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const batch = h.db.batch.bind(h.db);
    const calls = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        const results = await batch(statements);
        const sparse = new Array(results.length);
        const prototype = Object.create(Array.prototype);
        for (let index = 0; index < results.length; index += 1)
          prototype[index] = { results: [], meta: { changes: 0 } };
        Object.setPrototypeOf(sparse, prototype);
        expect(Object.hasOwn(sparse, 0)).toBe(false);
        return sparse;
      });
    const reads = vi.spyOn(h.fence, 'readForAdmission');
    const error = await h.admit().catch((error: unknown) => error);
    expect(h.rows()).toHaveLength(1);
    expect(
      (await h.input.reservationStore?.readForAdmission('key'))?.binding,
    ).toEqual({ kind: 'bound', execution: h.input.execution });
    expect((await h.fence.read()).proofExecution).toEqual(h.input.execution);
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(error).toMatchObject({
      status: 503,
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
    expect(calls).toHaveBeenCalledTimes(1);
    expect(reads).toHaveBeenCalledTimes(1);
    expect(h.effects()).toBe(0);
  });
  it('ignores a custom batch iterator that fabricates zero evidence after a real INSERT', async () => {
    const h = await fixture();
    const batch = h.db.batch.bind(h.db);
    vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
      const results = await batch(statements);
      Object.defineProperty(results, Symbol.iterator, {
        value: function* () {
          yield { results: [] };
          yield { results: [] };
        },
      });
      return results;
    });
    const outcome = await h.admit().catch((error: unknown) => error);
    expect(
      isDefinitiveInitialAdmissionRefusal(outcome, h.input.execution),
    ).toBe(false);
    expect(outcome).toMatchObject({
      witness: { execution: h.input.execution },
    });
    expect(h.rows()).toHaveLength(1);
  });
  it('never grants zero evidence when a result getter shrinks the batch during capture', async () => {
    const h = await fixture({ state: 'draining' });
    const batch = h.db.batch.bind(h.db);
    vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
      const results = await batch(statements);
      const first = results[0];
      Object.defineProperty(results, 0, {
        get() {
          results.pop();
          return first;
        },
      });
      return results;
    });
    const reads = vi.spyOn(h.fence, 'readForAdmission');
    const error = await h.admit().catch((error: unknown) => error);
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(reads).toHaveBeenCalledTimes(1);
    expect(h.rows()).toEqual([]);
  });
  it.each([
    'method',
    'prototype',
    'constructor',
  ])('keeps genuine all-zero evidence private against %s forgery', async (attack) => {
    const h = await fixture({ state: 'draining' });
    const error = await h.admit().catch((error: unknown) => error);
    if (!(error instanceof Error))
      throw new Error('expected genuine all-zero error');
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      true,
    );
    expect(
      isDefinitiveInitialAdmissionRefusal(
        new Error('wrapper', { cause: error }),
        h.input.execution,
      ),
    ).toBe(true);
    const other = { ...h.input.execution, startToken: 'another-generation' };
    if (attack === 'method') {
      Object.assign(error, { matches: () => true });
      expect(isDefinitiveInitialAdmissionRefusal(error, other)).toBe(false);
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(true);
    } else if (attack === 'prototype') {
      const forged = Object.assign(
        Object.create(Object.getPrototypeOf(error)),
        { matches: () => true },
      );
      expect(isDefinitiveInitialAdmissionRefusal(forged, other)).toBe(false);
      expect(
        isDefinitiveInitialAdmissionRefusal(forged, h.input.execution),
      ).toBe(false);
    } else {
      const Constructor = Object.getPrototypeOf(error).constructor;
      const forged = Reflect.construct(
        Constructor,
        Constructor.length === 2
          ? [h.input.execution, error.cause]
          : [error.cause],
      );
      expect(forged.message).toBe(error.message);
      expect(
        isDefinitiveInitialAdmissionRefusal(forged, h.input.execution),
      ).toBe(false);
    }
  });
  it('rejects malformed caller objects and missing participant methods before I/O', async () => {
    const h = await fixture({ keyed: true });
    const values: unknown[] = [null, [], false, 1, 'invalid'];
    const invalid: unknown[] = [...values];
    for (const field of [
      'requestContext',
      'proof',
      'runOwnerGuard',
      'reservation',
      'reservationStore',
      'fence',
    ]) {
      for (const value of values) invalid.push({ ...h.input, [field]: value });
    }
    for (const field of ['fence', 'reservationStore'] as const) {
      const original = h.input[field];
      if (!original) throw new Error('test requires a participating store');
      const methods =
        field === 'fence'
          ? ['usesDatabase', 'seed', 'readForAdmission']
          : ['usesDatabase', 'readForAdmission'];
      for (const method of methods)
        for (const value of [undefined, null, false, 1]) {
          invalid.push({
            ...h.input,
            [field]: Object.defineProperty(Object.create(original), method, {
              value,
            }),
          });
        }
    }
    const prepare = vi.spyOn(h.db, 'prepare');
    const batch = vi.spyOn(h.db, 'batch');
    const create = vi.fn(() => h.workflow.createRun({ runId: 'run' }));
    for (const input of invalid) {
      const error = await h.capability
        .withInitialAdmission(input as InitialRunAdmission, create)
        .catch((error: unknown) => error);
      expect(error).toMatchObject({
        status: 400,
        reason: { code: 'INVALID_EXECUTION_IDENTITY' },
      });
      expect(prepare).not.toHaveBeenCalled();
      expect(batch).not.toHaveBeenCalled();
      expect(create).not.toHaveBeenCalled();
      expect(h.onInitialWriteAttempt).not.toHaveBeenCalled();
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(false);
    }
  });

  it('preserves original caller getter and callback failures', async () => {
    const h = await fixture();
    const fault = new Error('caller getter fault');
    for (const requestContext of [
      {
        get [PROVENANCE]() {
          throw fault;
        },
      },
      {
        [PROVENANCE]: Object.defineProperty(
          { ...(h.input.requestContext[PROVENANCE] as object) },
          'startToken',
          {
            get() {
              throw fault;
            },
          },
        ),
      },
      {
        [PROVENANCE]: {
          ...(h.input.requestContext[PROVENANCE] as object),
          startIdentity: {
            get owner() {
              throw fault;
            },
          },
        },
      },
    ]) {
      await expect(h.admit({ ...h.input, requestContext })).rejects.toBe(fault);
    }
    await expect(
      h.capability.withInitialAdmission(h.input, async () => {
        throw fault;
      }),
    ).rejects.toBe(fault);
    await expect(
      h.admit({
        ...h.input,
        onInitialWriteAttempt() {
          throw fault;
        },
      }),
    ).rejects.toBe(fault);
  });
  it('admits an agent logical target without guessing its physical workflow and rejects mismatched threads', async () => {
    for (const mismatch of [false, true]) {
      const h = await fixture({ keyed: true });
      h.sql.exec(
        "UPDATE flowsafe_start_idempotency SET target_kind = 'agent', target_id = 'agent', thread_id = 'thread'",
      );
      const reservation =
        await h.input.reservationStore?.readForAdmission('key');
      const startIdentity = {
        owner: OWNER,
        target: {
          kind: 'agent' as const,
          id: 'agent',
          threadId: mismatch ? 'other-thread' : 'thread',
        },
      };
      const input = {
        ...h.input,
        startIdentity,
        reservation,
        requestContext: {
          ...h.input.requestContext,
          [PROVENANCE]: {
            ...(h.input.requestContext[PROVENANCE] as object),
            startIdentity,
            agentStart: { threaded: false },
          },
        },
      };
      const prepare = vi.spyOn(h.db, 'prepare');
      const result = await h.admit(input).catch((error: unknown) => error);
      if (mismatch) {
        expect(result).toBeInstanceOf(InvalidExecutionIdentityError);
        expect(prepare).not.toHaveBeenCalled();
      } else expect(h.rows()).toHaveLength(1);
    }
  });

  it('keeps optional caller epochs separate from original proof-round counters', async () => {
    const h = await fixture({ state: 'proof-only' });
    const input = {
      ...h.input,
      mutationEpoch: 99,
      requestContext: {
        ...h.input.requestContext,
        [PROVENANCE]: {
          ...(h.input.requestContext[PROVENANCE] as object),
          mutationEpoch: 99,
        },
      },
    };
    expect((await h.admit(input)).witness.execution).toEqual(h.input.execution);
    expect((await h.fence.read()).mutationEpoch).toBe(0);
  });

  it('isolates concurrent scopes on one actual Core workflow domain', async () => {
    const h = await fixture();
    const inputs = ['first', 'second'].map((runId) => ({
      ...h.input,
      execution: { ...h.input.execution, runId, startToken: runId },
      requestContext: {
        ...h.input.requestContext,
        runId,
        [PROVENANCE]: {
          ...(h.input.requestContext[PROVENANCE] as object),
          startToken: runId,
        },
      },
    }));
    const outcomes = await Promise.all(
      inputs.map((input) =>
        h.capability.withInitialAdmission(input, () =>
          h.workflow.createRun({ runId: input.execution.runId }),
        ),
      ),
    );
    expect(outcomes.map(({ witness }) => witness.execution.startToken)).toEqual(
      ['first', 'second'],
    );
    expect(h.rows()).toHaveLength(2);
  });

  it('rejects malformed Date and JSON serialization before batch without no-write evidence', async () => {
    for (const patch of [
      { createdAt: new Date(Number.NaN) },
      { snapshot: { ...pending(), unsupported: BigInt(1) } },
    ]) {
      const h = await fixture();
      const batch = vi.spyOn(h.db, 'batch');
      const error = await direct(h, patch).catch((error: unknown) => error);
      expect(error).toBeInstanceOf(Error);
      expect(batch).not.toHaveBeenCalled();
      expect(h.rows()).toEqual([]);
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(false);
    }
  });
  it('does not let prepared context serialization replace or manufacture auxiliary identity', async () => {
    const h = await fixture();
    const prepare = vi.spyOn(h.db, 'prepare');
    for (const runId of ['foreign', undefined, 'run']) {
      const requestContext = {
        ...h.input.requestContext,
        runId,
        toJSON() {
          return {
            runId: runId === 'run' ? 'foreign' : 'run',
            'breakwater.workflowScope': 'workflow',
          };
        },
      };
      const error = await h
        .admit({ ...h.input, requestContext })
        .catch((error: unknown) => error);
      expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
      expect(prepare).not.toHaveBeenCalled();
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(false);
    }
  });
  it('guards an actual reused committed owner and preserves distinct initiating ownership', async () => {
    for (const disposition of [
      'keep',
      'delete',
      'replace-owner',
      'replace-token',
    ]) {
      const h = await fixture({ keyed: true, state: 'proof-only' });
      const actualOwner = { kind: 'human' as const, id: 'Resource owner' };
      const ownership = new D1ResourceOwnershipStore(
        h.db as unknown as ResourceOwnershipDatabase,
      );
      expect(await ownership.claim('run', 'run', actualOwner)).toBe(true);
      expect(
        await ownership.reserveAll(
          [{ kind: 'run', resourceId: 'run' }],
          actualOwner,
          'correlation',
        ),
      ).toBe(true);
      const input = {
        ...h.input,
        runOwnerGuard: { owner: actualOwner, reservationToken: 'correlation' },
      };
      const batch = h.db.batch.bind(h.db);
      const prepare = h.db.prepare.bind(h.db);
      let parameters = 0;
      vi.spyOn(h.db, 'prepare').mockImplementation((query) => {
        const statement = prepare(query);
        const bind = statement.bind.bind(statement);
        statement.bind = (...values) => {
          if (query.startsWith('INSERT INTO "mastra_workflow_snapshot"'))
            parameters = values.length;
          return bind(...values);
        };
        return statement;
      });
      vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
        if (disposition === 'delete')
          h.sql.exec('DELETE FROM flowsafe_resource_owners');
        if (disposition === 'replace-owner')
          h.sql.exec(
            "UPDATE flowsafe_resource_owners SET owner_id = 'replacement'",
          );
        if (disposition === 'replace-token')
          h.sql.exec(
            "UPDATE flowsafe_resource_owners SET reservation_token = 'replacement'",
          );
        return batch(statements);
      });
      const result = await h.admit(input).catch((error: unknown) => error);
      expect(h.rows()).toHaveLength(disposition === 'keep' ? 1 : 0);
      expect(parameters).toBe(32);
      if (disposition !== 'keep') {
        expect(result).toMatchObject({
          reason: {
            code: 'RUN_ADMISSION_CONFLICT',
            classification: 'run-owner-changed',
          },
        });
        expect(
          isDefinitiveInitialAdmissionRefusal(result, input.execution),
        ).toBe(true);
        expect(
          (await h.input.reservationStore?.readForAdmission('key'))?.binding
            .kind,
        ).toBe('unbound');
        expect((await h.fence.read()).proofRunId).toBeUndefined();
      }
    }
  });

  it('checks the exact winning claim on the initial INSERT before any binding effects', async () => {
    for (const change of [
      "owner_id = 'Bob'",
      "target_id = 'other'",
      "run_id = 'other'",
      'updated_at = updated_at + 1',
    ]) {
      const h = await fixture({ keyed: true, state: 'proof-only' });
      const batch = h.db.batch.bind(h.db);
      vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
        h.sql.exec(`UPDATE flowsafe_start_idempotency SET ${change}`);
        return batch(statements);
      });
      const error = await h.admit().catch((error: unknown) => error);
      expect(h.rows()).toEqual([]);
      expect(
        (await h.input.reservationStore?.readForAdmission('key'))?.binding.kind,
      ).toBe('unbound');
      expect((await h.fence.read()).proofRunId).toBeUndefined();
      expect(error).toMatchObject({
        reason: {
          code: change.startsWith('owner')
            ? 'IDEMPOTENT_START_OWNER_MISMATCH'
            : change.startsWith('target')
              ? 'IDEMPOTENT_START_TARGET_MISMATCH'
              : 'RUN_ADMISSION_CONFLICT',
        },
      });
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(true);
    }
  });

  it.each([
    'mastra_workflow_snapshot',
    'flowsafe_start_idempotency',
    'flowsafe_execution_fence',
  ])('rolls back all participants on an actual %s statement exception', async (table) => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const verb = table === 'mastra_workflow_snapshot' ? 'INSERT' : 'UPDATE';
    h.sql.exec(
      `CREATE TRIGGER reject_write BEFORE ${verb} ON ${table} BEGIN SELECT RAISE(ABORT, 'injected statement failure'); END`,
    );
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(h.rows()).toEqual([]);
    expect(
      (await h.input.reservationStore?.readForAdmission('key'))?.binding.kind,
    ).toBe('unbound');
    expect((await h.fence.read()).proofRunId).toBeUndefined();
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
  });

  it('gives no no-write evidence when the callback uses another actual Core domain', async () => {
    const h = await fixture();
    const other = await fixture();
    const error = await h.capability
      .withInitialAdmission(h.input, () =>
        other.workflow.createRun({ runId: 'run' }),
      )
      .catch((error: unknown) => error);
    expect(other.rows()).toHaveLength(1);
    expect(h.rows()).toEqual([]);
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
  });

  it('captures a coherent frame before awaited preparation and refuses closed detached continuations', async () => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    let release!: () => void;
    const wait = new Promise<void>((resolve) => {
      release = resolve;
    });
    const seed = h.fence.seed.bind(h.fence);
    vi.spyOn(h.fence, 'seed').mockImplementationOnce(async (state) => {
      await wait;
      return seed(state);
    });
    const original = {
      ...h.input,
      execution: { ...h.input.execution },
      reservation: { ...claim(h.input), owner: { ...OWNER } },
      proof: { key: 'key', mutationEpoch: 0, transitionRevision: 1 },
      requestContext: structuredClone(h.input.requestContext),
    };
    const admitted = direct(h, {}, original);
    original.execution.startToken = 'changed';
    original.reservation.owner.id = 'changed';
    original.proof.transitionRevision = 99;
    release();
    expect((await admitted).witness.execution.startToken).toBe('generation');
    const detached = await fixture();
    let resume!: () => void;
    const pause = new Promise<void>((resolve) => {
      resume = resolve;
    });
    let later: Promise<unknown> | undefined;
    await detached.capability.withInitialAdmission(detached.input, async () => {
      await detached.domain.persistWorkflowSnapshot({
        workflowName: 'workflow',
        runId: 'run',
        snapshot: pending(),
      });
      later = pause
        .then(() =>
          detached.domain.persistWorkflowSnapshot({
            workflowName: 'workflow',
            runId: 'run',
            snapshot: pending(),
          }),
        )
        .catch((error: unknown) => error);
    });
    resume();
    expect(await later).toBeInstanceOf(InvalidExecutionIdentityError);
    expect(detached.rows()).toHaveLength(1);
  });
  it('rejects a prune toJSON that mutates the nested prepared context', async () => {
    const h = await fixture({
      prune: ({ snapshot }) => ({
        ...snapshot,
        toJSON(this: WorkflowRunState) {
          if (this.requestContext)
            this.requestContext.app.text = 'changed by serialization';
          return this;
        },
      }),
    });
    const batch = vi.spyOn(h.db, 'batch');
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
    expect(batch).not.toHaveBeenCalled();
    expect(h.rows()).toEqual([]);
    expect(h.effects()).toBe(0);
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
  });
  it.each([
    'before entry',
    'persist hook',
  ])('keeps the constructor binding after public capability replacement %s', async (when) => {
    const first = await fixture();
    const second = await fixture();
    const saved = first.capability;
    const replace = () =>
      Object.defineProperty(first.domain, FENCED_WORKFLOW_STORAGE, {
        value: second.capability,
      });
    if (when === 'before entry') replace();
    const result = await saved.withInitialAdmission(
      {
        ...first.input,
        onInitialWriteAttempt:
          when === 'persist hook' ? replace : () => undefined,
      },
      () => first.workflow.createRun({ runId: 'run' }),
    );
    expect(result.witness.execution).toEqual(first.input.execution);
    expect(first.rows()).toHaveLength(1);
    expect(second.rows()).toEqual([]);
    expect(first.domain[FENCED_WORKFLOW_STORAGE]?.database).toBe(second.db);
    expect(await saved.readSnapshot(first.input.execution)).toEqual(
      result.witness.row,
    );
  });
  it('rejects incoherent initial admission participants before I/O', async () => {
    const cases: Array<{
      name: string;
      change: (input: InitialRunAdmission) => InitialRunAdmission;
      code: string;
    }> = [
      {
        name: 'owner',
        change: (i) => ({
          ...i,
          reservation: {
            ...claim(i),
            owner: { kind: 'human', id: 'Bob' },
            targetId: 'wf-b',
          },
        }),
        code: 'IDEMPOTENT_START_OWNER_MISMATCH',
      },
      {
        name: 'target',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), targetId: 'wf-b' },
        }),
        code: 'IDEMPOTENT_START_TARGET_MISMATCH',
      },
      {
        name: 'run',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), runId: 'other-run' },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'no identity',
        change: (i) => ({ ...i, startIdentity: undefined }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'no store',
        change: (i) => ({ ...i, reservationStore: undefined }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'no claim',
        change: (i) => ({ ...i, reservation: undefined }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'reserved',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), state: 'reserved' },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'legacy',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), binding: { kind: 'legacy' } },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'bound',
        change: (i) => ({
          ...i,
          reservation: {
            ...claim(i),
            binding: { kind: 'bound', execution: i.execution },
          },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'proof key',
        change: (i) => ({
          ...i,
          proof: { key: 'other', mutationEpoch: 0, transitionRevision: 0 },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'owner correlation',
        change: (i) => ({
          ...i,
          runOwnerGuard: { owner: OWNER, reservationToken: 'other' },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'workflow thread',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), threadId: 'unexpected' },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
      {
        name: 'invalid timestamp',
        change: (i) => ({
          ...i,
          reservation: { ...claim(i), createdAt: Number.NaN },
        }),
        code: 'INVALID_EXECUTION_IDENTITY',
      },
    ];
    for (const { name, change, code } of cases) {
      const h = await fixture({ keyed: true });
      if (name === 'owner')
        h.sql.exec(
          "UPDATE flowsafe_start_idempotency SET owner_id = 'Bob', target_id = 'wf-b'",
        );
      if (name === 'target')
        h.sql.exec("UPDATE flowsafe_start_idempotency SET target_id = 'wf-b'");
      const prepare = vi.spyOn(h.db, 'prepare');
      const batch = vi.spyOn(h.db, 'batch');
      const create = vi.fn(() => h.workflow.createRun({ runId: 'run' }));
      const error = await h.capability
        .withInitialAdmission(change(h.input), create)
        .catch((error: unknown) => error);
      expect(error, name).toMatchObject({ reason: { code } });
      expect(prepare, name).not.toHaveBeenCalled();
      expect(batch, name).not.toHaveBeenCalled();
      expect(create, name).not.toHaveBeenCalled();
      expect(h.onInitialWriteAttempt, name).not.toHaveBeenCalled();
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
        name,
      ).toBe(false);
    }
  });

  it.each([
    undefined,
    0,
    2,
  ])('refuses original active epoch %s at the atomic boundary', async (mutationEpoch) => {
    const h = await fixture({ keyed: true });
    await h.fence.transition({
      expected: 'open',
      next: 'open',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
      advanceMutationEpoch: true,
    });
    const input = {
      ...h.input,
      mutationEpoch,
      requestContext: {
        ...h.input.requestContext,
        [PROVENANCE]: {
          ...(h.input.requestContext[PROVENANCE] as object),
          mutationEpoch,
        },
      },
    };
    const error = await h.admit(input).catch((error: unknown) => error);
    expect(error).toMatchObject({
      status: 409,
      reason: {
        code: 'MUTATION_EPOCH_MISMATCH',
        classification:
          mutationEpoch === undefined
            ? 'missing'
            : mutationEpoch === 0
              ? 'stale'
              : 'future',
      },
    });
    expect(h.rows()).toEqual([]);
    expect(isDefinitiveInitialAdmissionRefusal(error, input.execution)).toBe(
      true,
    );
  });

  it('distinguishes late same-state open revisions and transient races without fencing open', async () => {
    for (const classification of ['fence-changed', 'admission-raced']) {
      const h = await fixture();
      const batch = h.db.batch.bind(h.db);
      vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
        if (classification === 'fence-changed')
          await h.fence.transition({ expected: 'open', next: 'open' });
        else
          h.sql.exec("UPDATE flowsafe_execution_fence SET state = 'draining'");
        const result = await batch(statements);
        if (classification === 'admission-raced')
          h.sql.exec("UPDATE flowsafe_execution_fence SET state = 'open'");
        return result;
      });
      const error = await h.admit().catch((error: unknown) => error);
      expect(error).toMatchObject({
        status: 409,
        reason: { code: 'RUN_ADMISSION_CONFLICT', classification },
      });
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(true);
      expect(h.rows()).toEqual([]);
    }
  });

  it('makes zero initial insertion leave proof and winning reservation unchanged for a same-byte occupied row', async () => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const prepare = h.db.prepare.bind(h.db);
    let initial: unknown[] = [];
    vi.spyOn(h.db, 'prepare').mockImplementation((query) => {
      const statement = prepare(query);
      const bind = statement.bind.bind(statement);
      statement.bind = (...values) => {
        if (query.startsWith('INSERT INTO "mastra_workflow_snapshot"'))
          initial = values;
        return bind(...values);
      };
      return statement;
    });
    const batch = h.db.batch.bind(h.db);
    vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
      h.sql
        .prepare(
          'INSERT INTO mastra_workflow_snapshot VALUES (?, ?, ?, ?, ?, ?)',
        )
        .run(...initial.slice(0, 6));
      return batch(statements);
    });
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toMatchObject({
      reason: { code: 'RUN_ADMISSION_CONFLICT', classification: 'run-exists' },
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      true,
    );
    expect(
      (await h.input.reservationStore?.readForAdmission('key'))?.binding.kind,
    ).toBe('unbound');
    expect((await h.fence.read()).proofRunId).toBeUndefined();
    expect(h.rows()).toHaveLength(1);
    expect(h.effects()).toBe(0);
  });

  it('preserves all-zero evidence when required readback becomes corrupt or disappears', async () => {
    for (const change of [
      "UPDATE flowsafe_start_idempotency SET thread_id = 'unexpected'",
      'DROP TABLE mastra_workflow_snapshot',
      'DROP TABLE flowsafe_start_idempotency',
    ]) {
      const h = await fixture({ keyed: true, state: 'draining' });
      const batch = h.db.batch.bind(h.db);
      vi.spyOn(h.db, 'batch').mockImplementationOnce(async (statements) => {
        const result = await batch(statements);
        h.sql.exec(change);
        return result;
      });
      const error = await h.admit().catch((error: unknown) => error);
      expect(error).toMatchObject({
        status: 503,
        reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
      });
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(true);
    }
  });

  it.each([
    false,
    true,
  ])('converges only exact initial response-loss evidence (keyed=%s)', async (keyed) => {
    const h = await fixture({ keyed, state: 'proof-only' });
    const batch = h.db.batch.bind(h.db);
    const calls = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        await batch(statements);
        throw new Error('response lost');
      });
    expect((await h.admit()).witness.execution).toEqual(h.input.execution);
    expect(calls).toHaveBeenCalledTimes(2);
    expect(h.rows()).toHaveLength(1);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'workflow_name',
    'run_id',
    'resourceId',
    'snapshot',
    'createdAt',
    'updatedAt',
    'reservation',
    'proof',
  ])('does not converge response loss after %s evidence changes', async (field) => {
    const h = await fixture({ keyed: true, state: 'proof-only' });
    const lost = new Error('original lost response');
    const batch = h.db.batch.bind(h.db);
    const calls = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        await batch(statements);
        if (field === 'reservation')
          h.sql.exec('DELETE FROM flowsafe_start_idempotency');
        else if (field === 'proof')
          h.sql.exec(
            'UPDATE flowsafe_execution_fence SET proof_run_id = NULL, proof_table_prefix = NULL, proof_workflow_id = NULL, proof_start_token = NULL',
          );
        else
          h.sql
            .prepare(`UPDATE mastra_workflow_snapshot SET "${field}" = ?`)
            .run(
              field === 'snapshot'
                ? `${(h.rows()[0] as { snapshot: string }).snapshot} `
                : 'changed',
            );
        throw lost;
      });
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toMatchObject({ status: 503, cause: lost });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(calls).toHaveBeenCalledTimes(2);
    expect(h.effects()).toBe(0);
  });

  it.each([
    'sparse',
    'failed',
    'count',
    'missing-row',
    'wrong-row',
  ])('never repairs malformed returned %s envelopes through readback', async (mode) => {
    const h = await fixture();
    const batch = h.db.batch.bind(h.db);
    const calls = vi
      .spyOn(h.db, 'batch')
      .mockImplementationOnce(async (statements) => {
        const result = (await batch(statements)) as Array<{
          success: boolean;
          results: Array<Record<string, unknown>>;
          meta: { changes: number };
        }>;
        if (mode === 'sparse') return new Array(2);
        const initial = result[0];
        if (!initial) throw new Error('missing actual initial result');
        if (mode === 'failed') initial.success = false;
        if (mode === 'count') initial.meta.changes = 0;
        if (mode === 'missing-row') initial.results = [];
        if (mode === 'wrong-row')
          initial.results[0] = { ...initial.results[0], snapshot: '{}' };
        return result;
      });
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(calls).toHaveBeenCalledTimes(1);
    expect(h.rows()).toHaveLength(1);
  });
  it.each([
    false,
    true,
  ])('chains snapshot reservation and proof RETURNING outcomes atomically (keyed=%s)', async (keyed) => {
    for (const state of ['open', 'proof-only'] as const) {
      const h = await fixture({ keyed, state, prefix: 'Tenant_' });
      const { witness, value } = await h.admit();
      expect(witness.execution).toEqual(h.input.execution);
      expect(h.effects()).toBe(0);
      expect(h.onInitialWriteAttempt).toHaveBeenCalledTimes(1);
      expect(
        JSON.parse(witness.row.snapshot).requestContext[PROVENANCE]
          .initialAdmission,
      ).toBe(true);
      expect((await h.fence.read()).proofExecution).toEqual(
        state === 'proof-only' ? h.input.execution : undefined,
      );
      if (keyed)
        expect(
          (await h.input.reservationStore?.readForAdmission('key'))?.binding,
        ).toEqual({ kind: 'bound', execution: h.input.execution });
      else
        expect(
          h.sql
            .prepare(
              "SELECT name FROM sqlite_master WHERE name = 'flowsafe_start_idempotency'",
            )
            .all(),
        ).toEqual([]);
      await value.start({
        inputData: {},
        requestContext: new RequestContext(
          Object.entries(h.input.requestContext),
        ),
      });
      expect(h.effects()).toBe(1);
      const stored = await h.capability.readSnapshot(h.input.execution);
      expect(
        JSON.parse(stored?.snapshot ?? '').requestContext[PROVENANCE]
          .initialAdmission,
      ).toBeUndefined();
    }
  });

  it('serializes the exact pinned initial six-field record', async () => {
    const h = await fixture();
    vi.spyOn(Date, 'now').mockReturnValue(1234567890123);
    const { witness } = await direct(h, {
      createdAt: new Date('2020-01-02T03:04:05.000Z'),
      resourceId: undefined,
    });
    expect(witness.row.createdAt).toBe('2020-01-02T03:04:05.000Z');
    expect(witness.row.updatedAt).toBe('2009-02-13T23:31:30.123Z');
    expect(witness.row.resourceId).toBeNull();
    expect(
      h.sql
        .prepare(
          'SELECT typeof(createdAt) AS created, typeof(updatedAt) AS updated FROM mastra_workflow_snapshot',
        )
        .get(),
    ).toEqual({ created: 'text', updated: 'text' });
    expect(Object.keys(h.rows()[0] as object)).toEqual([
      'workflow_name',
      'run_id',
      'resourceId',
      'snapshot',
      'createdAt',
      'updatedAt',
    ]);
    expect(JSON.parse(witness.row.snapshot).requestContext.app).toEqual({
      text: 'λ',
      nullable: null,
    });
  });

  it('stamps trusted context after pruning and refuses serialization authority changes', async () => {
    const h = await fixture({
      prune: ({ snapshot }) => ({
        ...snapshot,
        requestContext: { [PROVENANCE]: { version: 99 } },
      }),
    });
    expect(
      JSON.parse((await h.admit()).witness.row.snapshot).requestContext[
        PROVENANCE
      ].version,
    ).toBe(2);
    for (const mutate of ['generation', 'runId', 'workflowScope', 'active']) {
      const candidate = await fixture();
      const snapshot = {
        ...pending(),
        toJSON() {
          const requestContext = {
            ...structuredClone(candidate.input.requestContext),
          };
          if (mutate === 'generation')
            (requestContext[PROVENANCE] as { startToken: string }).startToken =
              'foreign';
          if (mutate === 'runId') requestContext.runId = 'foreign';
          if (mutate === 'workflowScope')
            requestContext['breakwater.workflowScope'] = 'foreign';
          return {
            ...pending(),
            requestContext,
            ...(mutate === 'active' ? { activePaths: [0] } : {}),
          };
        },
      };
      const batch = vi.spyOn(candidate.db, 'batch');
      const error = await direct(candidate, { snapshot }).catch(
        (error: unknown) => error,
      );
      expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
      expect(batch).not.toHaveBeenCalled();
      expect(
        isDefinitiveInitialAdmissionRefusal(error, candidate.input.execution),
      ).toBe(false);
    }
  });

  it.each([
    'draining',
    'migration-locked',
    'proof-only',
  ] as const)('refuses %s without any snapshot or participant changes', async (state) => {
    const h = await fixture({ keyed: true, state });
    const before = h.sql
      .prepare('SELECT * FROM flowsafe_execution_fence')
      .get();
    const error = await h
      .admit({ ...h.input, proof: undefined })
      .catch((error: unknown) => error);
    expect(error).toMatchObject({
      status: 503,
      reason: { code: 'EXECUTION_FENCED', state },
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      true,
    );
    expect(h.rows()).toEqual([]);
    expect(
      (await h.input.reservationStore?.readForAdmission('key'))?.binding.kind,
    ).toBe('unbound');
    expect(
      h.sql.prepare('SELECT * FROM flowsafe_execution_fence').get(),
    ).toEqual(before);
    expect(h.effects()).toBe(0);
  });

  it('requires a positive witness from the matching Core initial write', async () => {
    const h = await fixture({ persist: false });
    const error = await h.admit().catch((error: unknown) => error);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(h.onInitialWriteAttempt).not.toHaveBeenCalled();
    expect(isDefinitiveInitialAdmissionRefusal(error, h.input.execution)).toBe(
      false,
    );
    expect(h.effects()).toBe(0);
    const cached = await fixture();
    await cached.admit();
    await expect(cached.admit()).rejects.toThrow('no persistence witness');
  });

  it('latches repeated mismatched and nested writes even when swallowed', async () => {
    for (const kind of ['repeat', 'mismatch', 'nested']) {
      const h = await fixture();
      const error = await h.capability
        .withInitialAdmission(h.input, async () => {
          await h.domain.persistWorkflowSnapshot({
            workflowName: 'workflow',
            runId: 'run',
            snapshot: pending(),
          });
          try {
            if (kind === 'nested')
              await h.capability.withInitialAdmission(
                h.input,
                async () => undefined,
              );
            else
              await h.domain.persistWorkflowSnapshot({
                workflowName: kind === 'repeat' ? 'workflow' : 'foreign',
                runId: 'run',
                snapshot: pending(),
              });
          } catch {
            /* Deliberately swallowed to exercise the scope latch. */
          }
        })
        .catch((error: unknown) => error);
      expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
      expect(
        isDefinitiveInitialAdmissionRefusal(error, h.input.execution),
      ).toBe(false);
      expect(h.rows()).toHaveLength(1);
    }
  });

  it('captures callbacks as plain functions without consulting their call properties', async () => {
    const h = await fixture();
    let receiver: unknown = 'uninvoked';
    const hook = Object.assign(
      function (this: unknown) {
        receiver = this;
      },
      {
        call: () => {
          throw new Error('shadowed call');
        },
      },
    );
    const create = Object.assign(() => h.workflow.createRun({ runId: 'run' }), {
      call: () => {
        throw new Error('shadowed call');
      },
    });
    await h.capability.withInitialAdmission(
      { ...h.input, onInitialWriteAttempt: hook },
      create,
    );
    expect(receiver).toBeUndefined();
  });
});
