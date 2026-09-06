// SPDX-License-Identifier: Apache-2.0

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
} from './fenced-workflow-capability.js';
import { FencedWorkflowsStorageD1 } from './fenced-workflows-d1.js';
import { isDefinitiveInitialAdmissionRefusal } from './initial-admission-refusal.js';
import { StartIdempotencyStore } from './start-idempotency.js';

const PROVENANCE = 'flowsafe.runProvenance';
const OWNER = { kind: 'human' as const, id: 'Alice' };

async function fixture(
  options: {
    keyed?: boolean;
    state?: ExecutionFenceState;
    prefix?: string;
    persist?: boolean;
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
      shouldPersistSnapshot: () => options.persist !== false,
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
  if (reservationStore) {
    await reservationStore.reserve({
      key: 'key',
      owner: OWNER,
      targetKind: 'workflow',
      targetId: 'workflow',
      mintRunId: () => execution.runId,
    });
    expect(await reservationStore.claim('key', execution.runId)).toBe(true);
    sql.exec("UPDATE flowsafe_start_idempotency SET start_token = ''");
  }
  const reservation = await reservationStore?.readForAdmission('key');
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

function claim(input: InitialRunAdmission) {
  if (!input.reservation) throw new Error('test requires a keyed fixture');
  return input.reservation;
}

describe('owned initial workflow admission', () => {
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
      expect(parameters).toBe(31);
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
