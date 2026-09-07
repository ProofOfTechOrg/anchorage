// SPDX-License-Identifier: Apache-2.0
// The reservation state machine, exercised as real SQL over node:sqlite.
//
// Every assertion here is ultimately about a PAID external call. A workflow's
// first step can wire funds; a second run of it wires them twice. So the cases
// below are written adversarially — two callers racing the same insert, a
// caller probing somebody else's key, a claim taken twice, a purge reaching a
// row whose run is still readable — and each one asserts the EXPENSIVE
// direction: that exactly one caller was told to start.

import { describe, expect, it, vi } from 'vitest';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import { doErrorResponse } from './do-error-response.js';
import {
  InvalidExecutionIdentityError,
  RunAdmissionConflictError,
  type StartExecutionIdentity,
} from './execution-admission.js';
import type { ExecutionFenceDatabase } from './execution-fence.js';
import {
  ExecutionFencedError,
  ExecutionFenceStore,
} from './execution-fence.js';
import {
  beginIdempotentStart,
  decodeStartReservationAdmissionResult,
  IdempotentStartAlreadySettledError,
  IdempotentStartPendingError,
  type IdempotentStartSurface,
  IdempotentStartUnresolvableError,
  InvalidStartIdempotencyRequestError,
  isStartReservationRefusal,
  requireStartIdempotency,
  rollbackFencedStart,
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_TABLE,
  type StartIdempotencyDatabase,
  type StartIdempotencyStatement,
  StartIdempotencyStore,
  StartIdempotencyUnsupportedError,
  type StartReservation,
  StartReservationOwnerMismatchError,
  type StartReservationReading,
  StartReservationTargetMismatchError,
  StartReservationUnreadableError,
  validateStartReservationAdmissionSchema,
} from './start-idempotency.js';

const OWNER = { kind: 'human', id: 'operator-1' } as const;
const OTHER_OWNER = { kind: 'human', id: 'operator-2' } as const;

describe('FS8 D2 dormant reservation primitives', () => {
  const execution: StartExecutionIdentity = {
    tablePrefix: '',
    workflowId: 'payout',
    runId: 'run',
    startToken: 'generation',
    owner: OWNER,
    target: { kind: 'workflow', id: 'payout' },
  };
  const methods = [
    'claimReservation',
    'releaseReservation',
    'associateReservation',
    'bindPreparedStart',
  ] as const;
  type Method = (typeof methods)[number];

  async function modern(state: 'reserved' | 'started' = 'reserved') {
    const h = harness();
    const reserved = await h.store.reserve(workflowRequest('key', 'run'));
    expect(reserved.reservation.binding).toEqual({ kind: 'legacy' });
    expect(rows(h.sqlite)[0]).toMatchObject(legacyBinding);
    h.sqlite
      .prepare(
        "UPDATE flowsafe_start_idempotency SET state = ?, start_token = ''",
      )
      .run(state);
    const observed = await h.store.readForAdmission('key');
    if (!observed) throw new Error('modern fixture is missing');
    return { ...h, observed };
  }

  function invoke(
    store: StartIdempotencyStore,
    method: Method,
    observed: StartReservationReading,
    identity = execution,
  ) {
    return method === 'claimReservation' || method === 'releaseReservation'
      ? store[method](observed)
      : store[method](observed, identity);
  }

  const stateFor = (method: Method) =>
    method === 'claimReservation' ? 'reserved' : 'started';
  const bindingFor = (identity = execution) => ({
    kind: 'bound' as const,
    execution: {
      tablePrefix: identity.tablePrefix,
      workflowId: identity.workflowId,
      runId: identity.runId,
      startToken: identity.startToken,
    },
  });

  it('returns exactly one own claim receipt for simultaneous equal-stamp contenders', async () => {
    const h = await modern();
    const claims = await Promise.all([
      h.store.claimReservation(h.observed),
      new StartIdempotencyStore(h.binding, {
        now: () => 1_000,
      }).claimReservation(h.observed),
    ]);
    expect(claims.filter(Boolean)).toHaveLength(1);
    const winner = claims.find(Boolean);
    if (!winner) throw new Error('winning claim is missing');
    expect(winner).toEqual({
      ...h.observed,
      state: 'started',
      updatedAt: 1_001,
    });
    expect(Object.isFrozen(winner)).toBe(true);
    expect(Object.isFrozen(winner?.owner)).toBe(true);
    expect(Object.isFrozen(winner?.binding)).toBe(true);
    expect(await h.store.releaseReservation(winner)).toBe(true);
    expect(rows(h.sqlite)[0]).toMatchObject({
      state: 'reserved',
      updated_at: 1_002,
    });
  });

  it('returns one successful release for simultaneous callers holding the same claim', async () => {
    const h = await modern('started');
    await expect(
      Promise.all([
        h.store.releaseReservation(h.observed),
        h.store.releaseReservation(h.observed),
      ]),
    ).resolves.toEqual([true, false]);
    expect(rows(h.sqlite)[0]).toMatchObject({
      state: 'reserved',
      updated_at: 1_001,
    });
  });

  it.each([
    1_000, 100,
  ])('rejects stale release and claim observations through an ABA cycle at clock %s', async (clock) => {
    const h = await modern();
    const store = new StartIdempotencyStore(h.binding, { now: () => clock });
    const first = await store.claimReservation(h.observed);
    if (!first) throw new Error('first claim is missing');
    expect(await store.releaseReservation(first)).toBe(true);
    const released = await store.readForAdmission('key');
    if (!released) throw new Error('released row is missing');
    const second = await store.claimReservation(released);
    if (!second) throw new Error('second claim is missing');
    const before = rows(h.sqlite);
    const staleRelease = await store
      .releaseReservation(first)
      .catch((error: unknown) => error);
    expect(rows(h.sqlite)).toEqual(before);
    expect(staleRelease).toBe(false);
    expect(first.updatedAt).toBe(1_001);
    expect(released.updatedAt).toBe(1_002);
    expect(second.updatedAt).toBe(1_003);
    expect(await store.claimReservation(h.observed)).toBeUndefined();
    expect(await store.releaseReservation(second)).toBe(true);
    const staleClaim = await store
      .claimReservation(released)
      .catch((error: unknown) => error);
    expect(rows(h.sqlite)[0]).toMatchObject({
      state: 'reserved',
      updated_at: 1_004,
    });
    expect(staleClaim).toBeUndefined();
  });

  it.each([
    'claimReservation',
    'releaseReservation',
  ] as const)('%s refuses invalid or exhausted clocks before preparing SQL', async (method) => {
    const h = await modern(stateFor(method));
    for (const [clock, updatedAt] of [
      [Number.NaN, 1_000],
      [Number.POSITIVE_INFINITY, 1_000],
      [Number.NEGATIVE_INFINITY, 1_000],
      [1_000, Number.MAX_VALUE],
      [1_000, 2 ** 54],
    ] as const) {
      const prepare = vi.fn(h.binding.prepare.bind(h.binding));
      const now = vi.fn(() => clock);
      const store = new StartIdempotencyStore({ prepare }, { now });
      await expect(
        store[method]({ ...h.observed, updatedAt }),
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
      expect(now).toHaveBeenCalledTimes(1);
      expect(prepare).not.toHaveBeenCalled();
    }
    const store = new StartIdempotencyStore(h.binding, { now: () => 1e100 });
    h.sqlite
      .prepare('UPDATE flowsafe_start_idempotency SET updated_at = ?')
      .run(2 ** 54);
    const outcome = await store[method]({ ...h.observed, updatedAt: 2 ** 54 });
    expect(outcome).toBeTruthy();
    expect(rows(h.sqlite)[0]?.updated_at).toBe(1e100);
  });

  it.each(
    methods,
  )('%s preserves every rewritten observation field before reporting a miss', async (method) => {
    for (const change of [
      "key = 'replacement'",
      "run_id = 'replacement'",
      "owner_kind = 'service'",
      "owner_id = 'replacement'",
      "target_kind = 'agent', thread_id = 'thread'",
      "target_id = 'replacement'",
      "thread_id = 'replacement'",
      'created_at = created_at + 1',
      'updated_at = updated_at + 1',
      "state = 'terminal'",
    ]) {
      const h = await modern(stateFor(method));
      h.sqlite.exec(`UPDATE flowsafe_start_idempotency SET ${change}`);
      const before = rows(h.sqlite);
      const outcome = await invoke(h.store, method, h.observed).catch(
        (error: unknown) => error,
      );
      expect(rows(h.sqlite), change).toEqual(before);
      if (method === 'claimReservation')
        expect(outcome, change).toBeUndefined();
      else if (method === 'releaseReservation')
        expect(outcome, change).toBe(false);
      else if (
        method === 'associateReservation' &&
        change.startsWith('thread_id')
      )
        expect(outcome).toBeInstanceOf(StartReservationUnreadableError);
      else expect(outcome, change).toBeInstanceOf(RunAdmissionConflictError);
    }
  });

  it.each(
    methods,
  )('%s preserves each independently changed unbound column before reporting failure', async (method) => {
    for (const change of [
      "start_token = 'other'",
      "start_table_prefix = ''",
      "start_workflow_id = 'payout'",
      'start_token = NULL',
    ]) {
      const h = await modern(stateFor(method));
      h.sqlite.exec(`UPDATE flowsafe_start_idempotency SET ${change}`);
      const before = rows(h.sqlite);
      const outcome = await invoke(h.store, method, h.observed).catch(
        (error: unknown) => error,
      );
      expect(rows(h.sqlite), change).toEqual(before);
      if (method === 'claimReservation') expect(outcome).toBeUndefined();
      else if (method === 'releaseReservation') expect(outcome).toBe(false);
      else
        expect(outcome).toBeInstanceOf(
          method === 'associateReservation' && change !== 'start_token = NULL'
            ? StartReservationUnreadableError
            : RunAdmissionConflictError,
        );
    }
  });

  it.each(
    methods,
  )('%s refuses legacy, bound, terminal and wrong-state observations before I/O', async (method) => {
    const h = await modern(stateFor(method));
    const prepare = vi.fn(h.binding.prepare.bind(h.binding));
    const store = new StartIdempotencyStore({ prepare });
    const invalid: StartReservationReading[] = [
      { ...h.observed, binding: { kind: 'legacy' } },
      {
        ...h.observed,
        binding: bindingFor(),
      },
      { ...h.observed, state: 'terminal' },
      { ...h.observed, key: 'bad/key' },
      { ...h.observed, targetId: 'bad/target' },
      { ...h.observed, createdAt: Number.NaN },
      { ...h.observed, updatedAt: Number.POSITIVE_INFINITY },
    ];
    if (method !== 'associateReservation')
      invalid.push({
        ...h.observed,
        state: method === 'claimReservation' ? 'started' : 'reserved',
      });
    for (const observed of invalid)
      await expect(invoke(store, method, observed)).rejects.toBeInstanceOf(
        InvalidExecutionIdentityError,
      );
    expect(prepare).not.toHaveBeenCalled();
  });

  it.each(
    methods,
  )('%s captures caller getters, nested identities and clock before held schema I/O', async (method) => {
    const h = await modern(stateFor(method));
    let resume!: () => void;
    const held = new Promise<void>((resolve) => {
      resume = resolve;
    });
    const original = structuredClone(h.observed);
    const identity = structuredClone(execution);
    const counts = new Map<string, number>();
    function getters<T extends object>(source: T, prefix: string): T {
      return Object.defineProperties(
        {},
        Object.fromEntries(
          Object.keys(source).map((key) => [
            key,
            {
              enumerable: true,
              get() {
                const name = `${prefix}.${key}`;
                counts.set(name, (counts.get(name) ?? 0) + 1);
                return source[key as keyof T];
              },
            },
          ]),
        ),
      ) as T;
    }
    const observationSource = {
      ...original,
      owner: getters(original.owner, 'owner'),
      binding: getters(original.binding, 'binding'),
    };
    const observation = getters(observationSource, 'reservation');
    const executionSource = {
      ...identity,
      owner: getters(identity.owner, 'execution.owner'),
      target: getters(identity.target, 'execution.target'),
    };
    const supplied = getters(executionSource, 'execution');
    let clock = 2_000;
    const now = vi.fn(() => clock);
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        if (sql.startsWith('PRAGMA')) await held;
        return execute();
      }),
      { now },
    );
    const operation = invoke(store, method, observation, supplied);
    Object.assign(original.owner, { kind: 'service', id: 'changed' });
    Object.assign(original.binding, { kind: 'legacy' });
    Object.assign(identity.owner, { kind: 'service', id: 'changed' });
    Object.assign(identity.target, {
      kind: 'agent',
      id: 'changed',
      threadId: 'changed',
    });
    Object.assign(observationSource, {
      key: 'changed',
      runId: 'changed',
      targetKind: 'agent',
      targetId: 'changed',
      threadId: 'changed',
      state: 'terminal',
      createdAt: 77,
      updatedAt: 88,
    });
    Object.assign(executionSource, {
      workflowId: 'changed',
      runId: 'changed',
      startToken: 'changed',
      tablePrefix: 'changed_',
    });
    clock = 9_000;
    resume();
    await expect(operation).resolves.toBeTruthy();
    expect(rows(h.sqlite)[0]).toMatchObject({
      owner_id: OWNER.id,
      target_id: 'payout',
      updated_at:
        method === 'claimReservation' || method === 'releaseReservation'
          ? 2_000
          : 1_000,
      start_token:
        method === 'claimReservation' || method === 'releaseReservation'
          ? ''
          : 'generation',
    });
    expect([...counts.values()].every((count) => count === 1)).toBe(true);
    expect(now).toHaveBeenCalledTimes(
      method === 'claimReservation' || method === 'releaseReservation' ? 1 : 0,
    );
  });

  it.each([
    'claimReservation',
    'releaseReservation',
  ] as const)('%s never recovers or retries a thrown UPDATE', async (method) => {
    for (const executeFirst of [false, true]) {
      const h = await modern(stateFor(method));
      const sqlSeen: string[] = [];
      const cause = new Error('lost response');
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          sqlSeen.push(sql);
          if (sql.startsWith('UPDATE')) {
            if (executeFirst) await execute();
            else if (method === 'claimReservation')
              await h.store.claimReservation(h.observed);
            throw cause;
          }
          return execute();
        }),
        { now: () => 1_000 },
      );
      await expect(store[method](h.observed)).rejects.toMatchObject({ cause });
      expect(sqlSeen.filter((sql) => sql.startsWith('UPDATE'))).toHaveLength(1);
      expect(sqlSeen.some((sql) => sql.startsWith('SELECT'))).toBe(false);
      expect(rows(h.sqlite)[0]).toMatchObject({
        state:
          executeFirst && method === 'releaseReservation'
            ? 'reserved'
            : 'started',
        updated_at:
          executeFirst || method === 'claimReservation' ? 1_001 : 1_000,
      });
    }
  });

  it('preserves a bound row when a delayed old release arrives', async () => {
    const h = await modern('started');
    await h.store.bindPreparedStart(h.observed, execution);
    const before = rows(h.sqlite);
    const outcome = await h.store
      .releaseReservation(h.observed)
      .catch((error: unknown) => error);
    expect(rows(h.sqlite)).toEqual(before);
    expect(outcome).toBe(false);
  });

  it.each([
    'associateReservation',
    'bindPreparedStart',
  ] as const)('%s binds canonical D1 and explicit custom namespaces without clock or snapshot access', async (method) => {
    for (const tablePrefix of ['', 'APP_', null]) {
      const h = await modern('started');
      const now = vi.fn(() => {
        throw new Error('binding must not read a clock');
      });
      const queries: string[] = [];
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          queries.push(sql);
          return execute();
        }),
        { now },
      );
      await expect(
        store[method](h.observed, { ...execution, tablePrefix }),
      ).resolves.toEqual({
        ...h.observed,
        binding: bindingFor({
          ...execution,
          tablePrefix: tablePrefix?.toLowerCase() ?? null,
        }),
      });
      expect(now).not.toHaveBeenCalled();
      expect(queries).toHaveLength(2);
      expect(queries[0]).toContain(
        'PRAGMA table_xinfo(flowsafe_start_idempotency)',
      );
      expect(queries[1]).toContain('UPDATE flowsafe_start_idempotency');
    }
  });

  it.each([
    'associateReservation',
    'bindPreparedStart',
  ] as const)('%s validates root introduction in owner-first order while permitting agent wrappers', async (method) => {
    const h = await modern('started');
    const prepare = vi.fn(h.binding.prepare.bind(h.binding));
    const store = new StartIdempotencyStore({ prepare });
    const cases: Array<
      [
        StartExecutionIdentity,
        (
          | typeof StartReservationOwnerMismatchError
          | typeof StartReservationTargetMismatchError
          | typeof InvalidExecutionIdentityError
        ),
      ]
    > = [
      [
        {
          ...execution,
          owner: OTHER_OWNER,
          target: { kind: 'workflow', id: 'other' },
        },
        StartReservationOwnerMismatchError,
      ],
      [
        { ...execution, target: { kind: 'workflow', id: 'other' } },
        StartReservationTargetMismatchError,
      ],
      [
        {
          ...execution,
          target: { kind: 'agent', id: 'payout', threadId: 'thread' },
        },
        StartReservationTargetMismatchError,
      ],
      [{ ...execution, runId: 'other' }, InvalidExecutionIdentityError],
      [{ ...execution, workflowId: 'child' }, InvalidExecutionIdentityError],
    ];
    for (const [identity, error] of cases)
      await expect(store[method](h.observed, identity)).rejects.toBeInstanceOf(
        error,
      );
    expect(prepare).not.toHaveBeenCalled();
    h.sqlite.exec(
      "UPDATE flowsafe_start_idempotency SET target_kind = 'agent', thread_id = 'thread'",
    );
    const observed = await h.store.readForAdmission('key');
    if (!observed) throw new Error('agent reservation is missing');
    const agent: StartExecutionIdentity = {
      ...execution,
      workflowId: 'trusted-wrapper',
      target: { kind: 'agent', id: 'payout', threadId: 'thread' },
    };
    await expect(
      store[method](observed, {
        ...agent,
        target: { ...agent.target, threadId: 'other' },
      } as StartExecutionIdentity),
    ).rejects.toBeInstanceOf(InvalidExecutionIdentityError);
    expect(prepare).not.toHaveBeenCalled();
    await expect(store[method](observed, agent)).resolves.toMatchObject({
      binding: bindingFor(agent),
    });
  });

  it.each([
    'reserved',
    'started',
    'terminal',
  ] as const)('alias lost-receipt convergence accepts S1 %s advancement without restamping', async (state) => {
    const h = await modern();
    const queries: string[] = [];
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        queries.push(sql);
        const result = await execute();
        if (sql.startsWith('UPDATE')) {
          h.sqlite
            .prepare(
              'UPDATE flowsafe_start_idempotency SET state = ?, updated_at = ?',
            )
            .run(state, 4_000);
          throw new Error('lost alias receipt');
        }
        return result;
      }),
      { now: () => 9_000 },
    );
    await expect(
      store.associateReservation(h.observed, execution),
    ).resolves.toEqual({
      ...h.observed,
      state,
      updatedAt: 4_000,
      binding: bindingFor(),
    });
    expect(rows(h.sqlite)[0]?.updated_at).toBe(4_000);
    expect(queries.filter((sql) => sql.startsWith('UPDATE'))).toHaveLength(1);
    expect(queries.filter((sql) => sql.startsWith('SELECT'))).toHaveLength(1);
  });

  it('associates the same S1 result after its snapshot disappears without reading a replacement result', async () => {
    const h = await modern();
    h.sqlite.exec('CREATE TABLE result_fixture (token TEXT, result TEXT)');
    h.sqlite
      .prepare('INSERT INTO result_fixture VALUES (?, ?)')
      .run('generation', 'original-result');
    const readResult = vi.fn(() =>
      h.sqlite.prepare('SELECT * FROM result_fixture').get(),
    );
    const found = readResult();
    h.sqlite.exec(
      "DELETE FROM result_fixture; INSERT INTO result_fixture VALUES ('replacement', 'replacement-result')",
    );
    const queries: string[] = [];
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        queries.push(sql);
        return execute();
      }),
    );
    await expect(
      store.associateReservation(h.observed, execution),
    ).resolves.toMatchObject({ binding: bindingFor() });
    expect(found).toEqual({ token: 'generation', result: 'original-result' });
    expect(readResult).toHaveBeenCalledTimes(1);
    expect(queries).toHaveLength(2);
    expect(rows(h.sqlite)[0]?.updated_at).toBe(h.observed.updatedAt);
  });

  it.each([
    'known-zero',
    'lost-receipt',
  ] as const)('alias %s converges only the same immutable identity and complete binding', async (mode) => {
    for (const change of [
      '',
      "start_token = 'replacement'",
      "start_table_prefix = 'other_'",
      "start_workflow_id = 'other'",
      "owner_id = 'other'",
      'created_at = created_at + 1',
      "start_token = '', start_table_prefix = NULL, start_workflow_id = NULL",
      'start_token = NULL, start_table_prefix = NULL, start_workflow_id = NULL',
      'DELETE',
      'DROP',
      "thread_id = 'bad/thread'",
    ]) {
      const h = await modern('started');
      const cause = new Error('lost alias write');
      let reads = 0;
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          if (sql.startsWith('SELECT')) reads += 1;
          if (!sql.startsWith('UPDATE')) return execute();
          await execute();
          if (change === 'DELETE')
            h.sqlite.exec('DELETE FROM flowsafe_start_idempotency');
          else if (change === 'DROP')
            h.sqlite.exec('DROP TABLE flowsafe_start_idempotency');
          else if (change)
            h.sqlite.exec(`UPDATE flowsafe_start_idempotency SET ${change}`);
          if (mode === 'lost-receipt') throw cause;
          return { results: [] };
        }),
      );
      if (!change)
        await expect(
          store.associateReservation(h.observed, execution),
        ).resolves.toMatchObject({ binding: bindingFor() });
      else if (mode === 'lost-receipt')
        await expect(
          store.associateReservation(h.observed, execution),
        ).rejects.toMatchObject({ cause });
      else
        await expect(
          store.associateReservation(h.observed, execution),
        ).rejects.toBeInstanceOf(
          change === 'DROP' || change.startsWith('thread_id')
            ? StartReservationUnreadableError
            : RunAdmissionConflictError,
        );
      expect(reads).toBe(1);
    }
  });

  it('prepared known-zero never reads back another caller claim', async () => {
    const h = await modern('started');
    await h.store.bindPreparedStart(h.observed, execution);
    const queries: string[] = [];
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        queries.push(sql);
        return execute();
      }),
    );
    await expect(
      store.bindPreparedStart(h.observed, execution),
    ).rejects.toBeInstanceOf(RunAdmissionConflictError);
    expect(queries.some((sql) => sql.startsWith('SELECT'))).toBe(false);
    expect(queries.filter((sql) => sql.startsWith('UPDATE'))).toHaveLength(1);
  });

  it.each([
    '',
    "state = 'terminal'",
    "state = 'reserved'",
    'updated_at = updated_at + 1',
    'created_at = created_at + 1',
    "start_token = 'other'",
    "start_table_prefix = 'other_'",
    "start_workflow_id = 'other'",
    "owner_id = 'other'",
    "target_id = 'other'",
    "run_id = 'other'",
  ])('prepared lost-receipt readback requires the exact original claim: %s', async (change) => {
    const h = await modern('started');
    const cause = new Error('lost prepared receipt');
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        const result = await execute();
        if (sql.startsWith('UPDATE')) {
          if (change)
            h.sqlite.exec(`UPDATE flowsafe_start_idempotency SET ${change}`);
          throw cause;
        }
        return result;
      }),
    );
    if (change)
      await expect(
        store.bindPreparedStart(h.observed, execution),
      ).rejects.toMatchObject({ cause });
    else
      await expect(
        store.bindPreparedStart(h.observed, execution),
      ).resolves.toEqual({ ...h.observed, binding: bindingFor() });
  });

  it.each(
    methods,
  )('%s rejects malformed RETURNING envelopes without readback', async (method) => {
    const corruptions: Array<
      [string, (row: Record<string, unknown>) => unknown]
    > = [
      ['failed', (row) => ({ success: false, results: [row] })],
      ['missing results', () => ({ meta: { changes: 1 } })],
      ['nonarray', (row) => ({ results: { 0: row, length: 1 } })],
      ['sparse', () => ({ results: new Array(1) })],
      [
        'inherited',
        (row) => ({
          results: Object.setPrototypeOf(
            new Array(1),
            Object.assign(Object.create(Array.prototype), { 0: row }),
          ),
        }),
      ],
      [
        'iterator',
        (row) => ({
          results: Object.assign([null], {
            *[Symbol.iterator]() {
              yield row;
            },
          }),
        }),
      ],
      ['multiple', (row) => ({ results: [row, row] })],
      [
        'missing column',
        (row) => ({
          results: [
            Object.fromEntries(
              Object.entries(row).filter(([key]) => key !== 'created_at'),
            ),
          ],
        }),
      ],
      ...Object.entries({
        key: 'other',
        run_id: 'other',
        owner_id: 'other',
        target_id: 'other',
        created_at: 2,
        updated_at: 2,
        state: 'terminal',
        start_token: 'other',
        target_kind: 'unknown',
        thread_id: 'bad/thread',
      }).map(
        ([key, value]) =>
          [
            key,
            (row: Record<string, unknown>) => ({
              results: [{ ...row, [key]: value }],
            }),
          ] as [string, (row: Record<string, unknown>) => unknown],
      ),
    ];
    for (const [name, corrupt] of corruptions) {
      const h = await modern(stateFor(method));
      let reads = 0;
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          if (sql.startsWith('SELECT')) reads += 1;
          const result = (await execute()) as {
            results: Array<Record<string, unknown>>;
          };
          if (!sql.startsWith('UPDATE')) return result;
          const row = result.results[0];
          if (!row) throw new Error('successful mutation row is missing');
          return corrupt(row);
        }),
        { now: () => 1_000 },
      );
      await expect(
        invoke(store, method, h.observed),
        name,
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
      expect(reads, name).toBe(0);
    }
  });

  it.each(
    methods,
  )('%s captures each successful RETURNING field once', async (method) => {
    const h = await modern(stateFor(method));
    const counts = new Map<string, number>();
    const once = (key: string, value: unknown) => ({
      get() {
        const count = (counts.get(key) ?? 0) + 1;
        counts.set(key, count);
        if (count > 1) throw new Error(`reread ${key}`);
        return value;
      },
      enumerable: true,
    });
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        const result = (await execute()) as {
          results: Array<Record<string, unknown>>;
        };
        if (!sql.startsWith('UPDATE')) return result;
        const returnedRow = result.results[0];
        if (!returnedRow) throw new Error('successful mutation row is missing');
        const row = Object.defineProperties(
          {},
          Object.fromEntries(
            Object.entries(returnedRow).map(([key, value]) => [
              key,
              once(key, value),
            ]),
          ),
        );
        const returned: unknown[] = [];
        Object.defineProperty(returned, 0, once('slot', row));
        return Object.defineProperties(
          {},
          {
            success: once('success', true),
            results: once('results', returned),
          },
        );
      }),
      { now: () => 1_000 },
    );
    await expect(invoke(store, method, h.observed)).resolves.toBeTruthy();
    expect(counts.size).toBe(16);
    expect([...counts.values()].every((count) => count === 1)).toBe(true);
  });

  it.each(
    methods,
  )('%s requires the current schema without running readiness', async (method) => {
    for (const stage of [0, 1, 2, 3, -1]) {
      const h = await modern(stateFor(method));
      if (stage === -1) h.sqlite.exec('DROP TABLE flowsafe_start_idempotency');
      else if (stage < 3)
        for (const column of bindingColumns.slice(stage).reverse())
          h.sqlite.exec(
            `ALTER TABLE flowsafe_start_idempotency DROP COLUMN ${column}`,
          );
      else
        h.sqlite.exec(
          'ALTER TABLE flowsafe_start_idempotency ADD COLUMN unexpected TEXT',
        );
      const ready = vi.fn(async () => {});
      const queries: string[] = [];
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          queries.push(sql);
          return execute();
        }),
        { ready, now: () => 1_000 },
      );
      await expect(invoke(store, method, h.observed)).rejects.toBeInstanceOf(
        StartReservationUnreadableError,
      );
      expect(ready).not.toHaveBeenCalled();
      expect(queries).toHaveLength(1);
      expect(queries[0]).toMatch(/^PRAGMA/);
    }
  });

  it('settles both aliases of one full execution and preserves every neighboring execution before checking outcome', async () => {
    const h = await modern('started');
    await h.store.bindPreparedStart(h.observed, execution);
    const insert = h.sqlite.prepare(
      `INSERT INTO flowsafe_start_idempotency SELECT ?, owner_kind, owner_id, target_kind, target_id, run_id, thread_id, state, created_at, updated_at, start_token, start_table_prefix, start_workflow_id FROM flowsafe_start_idempotency WHERE key = 'key'`,
    );
    insert.run('alias');
    const neighbors = [
      "run_id = 'other'",
      "start_token = 'other'",
      "start_table_prefix = 'other_'",
      'start_table_prefix = NULL',
      "start_workflow_id = 'other'",
      "owner_kind = 'service'",
      "owner_id = 'other'",
      "target_kind = 'agent', thread_id = 'thread'",
      "target_id = 'other'",
      "thread_id = 'other'",
      "start_token = '', start_table_prefix = NULL, start_workflow_id = NULL",
      'start_token = NULL, start_table_prefix = NULL, start_workflow_id = NULL',
    ];
    for (const [index, change] of neighbors.entries()) {
      insert.run(`neighbor-${index}`);
      h.sqlite.exec(
        `UPDATE flowsafe_start_idempotency SET ${change} WHERE key = 'neighbor-${index}'`,
      );
    }
    const before = rows(h.sqlite).filter((row) =>
      String(row.key).startsWith('neighbor-'),
    );
    const store = new StartIdempotencyStore(h.binding, { now: () => 500 });
    const outcome = await store
      .settleExecution(execution)
      .catch((error: unknown) => error);
    expect(
      rows(h.sqlite).filter((row) => String(row.key).startsWith('neighbor-')),
    ).toEqual(before);
    expect(outcome).toBe(2);
    expect(
      rows(h.sqlite).filter((row) => row.key === 'key' || row.key === 'alias'),
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          key: 'key',
          state: 'terminal',
          updated_at: 500,
        }),
        expect.objectContaining({
          key: 'alias',
          state: 'terminal',
          updated_at: 500,
        }),
      ]),
    );
    expect(
      await new StartIdempotencyStore(h.binding, {
        now: () => 9_000,
      }).settleExecution(execution),
    ).toBe(0);
    expect(rows(h.sqlite)[0]?.updated_at).toBe(500);
  });

  it('settles explicit null and inherited child identities without a root classifier', async () => {
    const h = await modern('started');
    h.sqlite.exec(
      "UPDATE flowsafe_start_idempotency SET start_token = 'generation', start_workflow_id = 'child'",
    );
    const inherited = { ...execution, workflowId: 'child', tablePrefix: null };
    expect(
      await h.store.settleExecution({ ...inherited, tablePrefix: '' }),
    ).toBe(0);
    expect(await h.store.settleExecution(inherited)).toBe(1);
    expect(rows(h.sqlite)[0]).toMatchObject({
      state: 'terminal',
      start_table_prefix: null,
      start_workflow_id: 'child',
    });
  });

  it('settlement captures its execution and finite clock before I/O and permits only genuine table absence', async () => {
    const h = await modern('started');
    await h.store.bindPreparedStart(h.observed, execution);
    let resume!: () => void;
    const held = new Promise<void>((resolve) => {
      resume = resolve;
    });
    const identity = structuredClone(execution);
    let clock = 3_000;
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        if (sql.startsWith('PRAGMA')) await held;
        return execute();
      }),
      { now: () => clock },
    );
    const settling = store.settleExecution(identity);
    Object.assign(identity, { workflowId: 'other', startToken: 'other' });
    Object.assign(identity.owner, { id: 'other' });
    Object.assign(identity.target, { id: 'other' });
    clock = 9_000;
    resume();
    await expect(settling).resolves.toBe(1);
    expect(rows(h.sqlite)[0]?.updated_at).toBe(3_000);
    const prepare = vi.fn(h.binding.prepare.bind(h.binding));
    await expect(
      new StartIdempotencyStore(
        { prepare },
        { now: () => Number.NaN },
      ).settleExecution(execution),
    ).rejects.toBeInstanceOf(StartReservationUnreadableError);
    expect(prepare).not.toHaveBeenCalled();
    for (const stage of [0, 1, 2])
      await expect(
        schemaHarness(stage).store.settleExecution(execution),
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
    expect(await harness().store.settleExecution(execution)).toBe(0);
    for (const root of [true, false]) {
      const missing = new Error('no such table: flowsafe_start_idempotency');
      const cause = root
        ? new Error('wrapper', { cause: missing })
        : new Error(missing.message, { cause: new Error('transport') });
      const broken = new StartIdempotencyStore(
        interceptReservations(h.binding, async () => {
          throw cause;
        }),
      );
      if (root)
        await expect(broken.settleExecution(execution)).resolves.toBe(0);
      else
        await expect(broken.settleExecution(execution)).rejects.toMatchObject({
          cause,
        });
    }
  });

  it.each([
    'associateReservation',
    'bindPreparedStart',
  ] as const)('%s retains the original thrown write cause when its row is absent or readback fails', async (method) => {
    for (const mode of ['unbound', 'absent', 'dropped', 'read-error']) {
      const h = await modern('started');
      const cause = new Error('write response unavailable');
      let reads = 0;
      let updates = 0;
      const store = new StartIdempotencyStore(
        interceptReservations(h.binding, async (sql, execute) => {
          if (sql.startsWith('UPDATE')) {
            updates += 1;
            if (mode === 'absent')
              h.sqlite.exec('DELETE FROM flowsafe_start_idempotency');
            if (mode === 'dropped')
              h.sqlite.exec('DROP TABLE flowsafe_start_idempotency');
            throw cause;
          }
          if (sql.startsWith('SELECT')) {
            reads += 1;
            if (mode === 'read-error') throw new Error('readback failed');
          }
          return execute();
        }),
      );
      await expect(store[method](h.observed, execution)).rejects.toMatchObject({
        cause,
      });
      expect(updates).toBe(1);
      expect(reads).toBe(1);
    }
  });

  it.each([
    ...methods,
    'settleExecution' as const,
  ])('%s handles a table disappearing after schema validation without recreating it', async (method) => {
    const h = await modern(
      method === 'claimReservation' ? 'reserved' : 'started',
    );
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        if (sql.startsWith('UPDATE'))
          h.sqlite.exec('DROP TABLE flowsafe_start_idempotency');
        return execute();
      }),
    );
    const operation =
      method === 'settleExecution'
        ? store.settleExecution(execution)
        : invoke(store, method, h.observed);
    if (method === 'settleExecution') await expect(operation).resolves.toBe(0);
    else
      await expect(operation).rejects.toBeInstanceOf(
        StartReservationUnreadableError,
      );
    expect(
      h.sqlite
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all(),
    ).toEqual([]);
  });

  it.each([
    'duplicate',
    'foreign',
    'timestamp',
    'sparse',
    'failed',
    'throw',
  ] as const)('settlement rejects %s responses without recovery or restamping on retry', async (mode) => {
    const h = await modern('started');
    await h.store.bindPreparedStart(h.observed, execution);
    const queries: string[] = [];
    const store = new StartIdempotencyStore(
      interceptReservations(h.binding, async (sql, execute) => {
        queries.push(sql);
        const result = (await execute()) as {
          results: Array<Record<string, unknown>>;
        };
        if (!sql.startsWith('UPDATE')) return result;
        const row = result.results[0];
        if (!row) throw new Error('settlement row is missing');
        if (mode === 'throw') throw new Error('lost settlement response');
        if (mode === 'failed') return { success: false, results: [row] };
        if (mode === 'sparse') return { results: new Array(1) };
        return {
          results:
            mode === 'duplicate'
              ? [row, row]
              : [
                  {
                    ...row,
                    [mode === 'foreign' ? 'start_token' : 'updated_at']:
                      mode === 'foreign' ? 'other' : 99,
                  },
                ],
        };
      }),
      { now: () => 2_000 },
    );
    await expect(store.settleExecution(execution)).rejects.toBeInstanceOf(
      StartReservationUnreadableError,
    );
    expect(queries.some((sql) => sql.startsWith('SELECT'))).toBe(false);
    expect(await h.store.settleExecution(execution)).toBe(0);
    expect(rows(h.sqlite)[0]?.updated_at).toBe(2_000);
  });

  it('keeps modern primitives dormant during ordinary start replay rollback and terminal flows', async () => {
    const h = harness();
    const spies = [...methods, 'settleExecution' as const].map((method) =>
      vi.spyOn(h.store, method),
    );
    const request = workflowRequest('key', 'run');
    await expect(
      beginIdempotentStart(h.store, request, EMPTY_SURFACE),
    ).resolves.toMatchObject({ kind: 'start' });
    expect(await h.store.release('key', 'run')).toBe(true);
    await expect(
      beginIdempotentStart(h.store, request, EMPTY_SURFACE),
    ).resolves.toMatchObject({ kind: 'start' });
    await expect(
      beginIdempotentStart(h.store, request, {
        ...EMPTY_SURFACE,
        persisted: async () => 'done',
      }),
    ).resolves.toMatchObject({ kind: 'replay', persisted: 'done' });
    expect(await h.store.settleRun('run')).toBe(1);
    h.sqlite.exec(
      'UPDATE flowsafe_start_idempotency SET created_at = 100, updated_at = -0.5',
    );
    expect((await h.store.read('key'))?.updatedAt).toBe(-0.5);
    expect(rows(h.sqlite)[0]).toMatchObject(legacyBinding);
    for (const spy of spies) {
      expect(spy).not.toHaveBeenCalled();
      spy.mockRestore();
    }
  });
});

describe('strict initial-admission reservation observations', () => {
  it.each([
    'row',
    'schema',
  ])('rejects inherited slots in the reservation %s observation', async (mode) => {
    const { sqlite, binding } = schemaHarness(3);
    const sparse = (rows: unknown[]) =>
      Object.setPrototypeOf(
        new Array(rows.length),
        Object.assign(Object.create(Array.prototype), rows),
      );
    if (mode === 'schema') {
      const columns = sqlite
        .prepare('PRAGMA table_xinfo(flowsafe_start_idempotency)')
        .all();
      expect(() =>
        validateStartReservationAdmissionSchema({ results: sparse(columns) }),
      ).toThrow('invalid row');
    } else {
      const wrapped = interceptReservations(binding, async (sql, execute) => {
        const result = (await execute()) as { results: unknown[] };
        return sql.startsWith('SELECT * FROM flowsafe_start_idempotency')
          ? { results: sparse(result.results) }
          : result;
      });
      await expect(
        new StartIdempotencyStore(wrapped).readForAdmission('key'),
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
    }
  });
  it('does not let a custom iterator hide a malformed admission row', async () => {
    const { binding } = schemaHarness(3);
    const wrapped = interceptReservations(binding, async (sql, execute) => {
      const result = (await execute()) as { results: unknown[] };
      if (!sql.startsWith('SELECT * FROM flowsafe_start_idempotency'))
        return result;
      const rows = [null];
      Object.defineProperty(rows, Symbol.iterator, {
        value: function* () {
          yield* result.results;
        },
      });
      return { results: rows };
    });
    await expect(
      new StartIdempotencyStore(wrapped).readForAdmission('key'),
    ).rejects.toBeInstanceOf(StartReservationUnreadableError);
  });
  it('rejects the first malformed schema envelope even if its getter later returns real columns', () => {
    const { sqlite } = schemaHarness(3);
    const columns = sqlite
      .prepare('PRAGMA table_xinfo(flowsafe_start_idempotency)')
      .all();
    let reads = 0;
    expect(() =>
      validateStartReservationAdmissionSchema({
        get results() {
          return ++reads === 1 ? [null] : columns;
        },
      }),
    ).toThrow('invalid row');
    expect(reads).toBe(1);
  });
  it.each([
    'envelope',
    'element',
  ])('captures one %s observation before admission decoding', async (mode) => {
    const { binding, sqlite } = schemaHarness(3);
    sqlite.exec("UPDATE flowsafe_start_idempotency SET start_token = ''");
    let reads = 0;
    const wrapped = interceptReservations(binding, async (sql, execute) => {
      const result = (await execute()) as { results: unknown[] };
      if (!sql.startsWith('SELECT * FROM flowsafe_start_idempotency'))
        return result;
      if (mode === 'envelope')
        return {
          get results() {
            return ++reads === 1 ? result.results : new Array(1);
          },
        };
      const rows: unknown[] = [];
      Object.defineProperty(rows, 0, {
        get() {
          return ++reads === 1 ? result.results[0] : undefined;
        },
      });
      return { results: rows };
    });
    expect(
      (await new StartIdempotencyStore(wrapped).readForAdmission('key'))?.runId,
    ).toBe('run');
    expect(reads).toBe(1);
  });
  const raw = {
    key: 'key',
    owner_kind: 'human',
    owner_id: 'owner',
    target_kind: 'workflow',
    target_id: 'workflow',
    run_id: 'run',
    thread_id: null,
    state: 'started',
    created_at: 100,
    updated_at: 200,
    start_token: '',
    start_table_prefix: null,
    start_workflow_id: null,
  };

  it.each([
    ['workflow', 'thread', 'admission workflow thread must be null'],
    ['workflow', 'bad/thread', 'admission workflow thread must be null'],
    ['agent', null, 'admission agent thread is invalid'],
    ['agent', undefined, 'admission agent thread is invalid'],
    ['agent', 'bad/thread', 'admission agent thread is invalid'],
  ])('validates raw %s thread %s before normalization', (target_kind, thread_id, cause) => {
    expect(() =>
      decodeStartReservationAdmissionResult({
        results: [{ ...raw, target_kind, thread_id }],
      }),
    ).toThrow(cause);
  });

  it('validates every own current field and logical identifier with populated rows', () => {
    for (const key of Object.keys(raw)) {
      const row = Object.fromEntries(
        Object.entries(raw).filter(([name]) => name !== key),
      );
      expect(() =>
        decodeStartReservationAdmissionResult({ results: [row] }),
      ).toThrow(`row is missing ${key}`);
    }
    for (const target_id of ['', 'bad/id'])
      expect(() =>
        decodeStartReservationAdmissionResult({
          results: [{ ...raw, target_id }],
        }),
      ).toThrow('admission target id is invalid');
    expect(() =>
      decodeStartReservationAdmissionResult({
        results: [{ ...raw, key: 'bad/key' }],
      }),
    ).toThrow('admission key is invalid');
    expect(() =>
      decodeStartReservationAdmissionResult({ results: new Array(1) }),
    ).toThrow('invalid row');
    expect(() =>
      decodeStartReservationAdmissionResult({ results: [raw, raw] }),
    ).toThrow('multiple rows');
    expect(
      decodeStartReservationAdmissionResult({ results: [raw] })?.threadId,
    ).toBeUndefined();
    expect(
      decodeStartReservationAdmissionResult({
        results: [{ ...raw, target_kind: 'agent', thread_id: 'thread' }],
      })?.threadId,
    ).toBe('thread');
  });

  it('requires a current schema without readiness writes and preserves ordinary compatibility', async () => {
    const { sqlite, binding, store } = harness();
    expect(store.usesDatabase(binding)).toBe(true);
    expect(store.usesDatabase({ ...binding })).toBe(false);
    await expect(store.readForAdmission('absent')).rejects.toBeInstanceOf(
      StartReservationUnreadableError,
    );
    expect(
      sqlite
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all(),
    ).toEqual([]);
    sqlite.exec(START_IDEMPOTENCY_DDL);
    const schema = sqlite
      .prepare('PRAGMA table_xinfo(flowsafe_start_idempotency)')
      .all();
    for (let stage = 0; stage < 3; stage += 1)
      expect(() =>
        validateStartReservationAdmissionSchema({
          results: schema.slice(0, 10 + stage),
        }),
      ).toThrow('admission requires current schema');
    expect(() =>
      validateStartReservationAdmissionSchema({ results: schema }),
    ).not.toThrow();
    await expect(store.readForAdmission('absent')).resolves.toBeUndefined();
    sqlite
      .prepare(
        'INSERT INTO flowsafe_start_idempotency VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      )
      .run(...Object.values({ ...raw, thread_id: 'bad/thread' }));
    expect((await store.read('key'))?.threadId).toBeUndefined();
    const error = await store
      .readForAdmission('key')
      .catch((error: unknown) => error);
    expect(error).toMatchObject({
      status: 503,
      cause: {
        message: expect.stringContaining(
          'admission workflow thread must be null',
        ),
      },
    });
    expect(
      sqlite.prepare('SELECT thread_id FROM flowsafe_start_idempotency').get(),
    ).toEqual({ thread_id: 'bad/thread' });
  });
});

function harness(now: () => number = () => 1_000) {
  const sqlite = openSqlite();
  const binding = sqliteUnitDatabase(sqlite) as StartIdempotencyDatabase;
  return {
    sqlite,
    binding,
    store: new StartIdempotencyStore(binding, { now }),
  };
}

function rows(sqlite: SqliteDatabase): Array<Record<string, unknown>> {
  return sqlite
    .prepare(`SELECT * FROM ${START_IDEMPOTENCY_TABLE}`)
    .all() as Array<Record<string, unknown>>;
}

const bindingColumns = [
  'start_token',
  'start_table_prefix',
  'start_workflow_id',
];
const legacyBinding = {
  start_token: null,
  start_table_prefix: null,
  start_workflow_id: null,
};

function schemaHarness(stage: number) {
  const fixture = harness();
  fixture.sqlite.exec(START_IDEMPOTENCY_DDL);
  for (const column of bindingColumns.slice(stage).reverse())
    fixture.sqlite.exec(
      `ALTER TABLE ${START_IDEMPOTENCY_TABLE} DROP COLUMN ${column}`,
    );
  fixture.sqlite
    .prepare(
      `INSERT INTO ${START_IDEMPOTENCY_TABLE} (key, owner_kind, owner_id, target_kind, target_id, run_id, thread_id, state, created_at, updated_at) VALUES ('key', 'human', 'operator-1', 'workflow', 'payout', 'run', NULL, 'started', 10, 20)`,
    )
    .run();
  return fixture;
}

function interceptReservations(
  db: StartIdempotencyDatabase,
  intercept: (sql: string, execute: () => Promise<unknown>) => Promise<unknown>,
): StartIdempotencyDatabase {
  const statement = (
    sql: string,
    values: unknown[],
  ): StartIdempotencyStatement => ({
    bind: (...bound) => statement(sql, bound),
    run: () =>
      intercept(sql, () =>
        db
          .prepare(sql)
          .bind(...values)
          .run(),
      ),
    all: async <T>() =>
      (await intercept(sql, () =>
        db
          .prepare(sql)
          .bind(...values)
          .all(),
      )) as { results: T[] },
  });
  return { prepare: (sql) => statement(sql, []) };
}

function workflowRequest(key: string, runId: string, workflowId = 'payout') {
  return {
    key,
    owner: OWNER,
    targetKind: 'workflow' as const,
    targetId: workflowId,
    mintRunId: () => runId,
  };
}

/** A surface that has nothing persisted and nothing live — the fresh case. */
const EMPTY_SURFACE: IdempotentStartSurface<string> = {
  persisted: async () => undefined,
  live: async () => false,
};

describe('start idempotency error taxonomy', () => {
  it('publishes the invalid-request status and reason code', async () => {
    const error = new InvalidStartIdempotencyRequestError('key is malformed');
    const response = doErrorResponse(error);

    expect(error.status).toBe(400);
    expect(error.reason.code).toBe('INVALID_START_IDEMPOTENCY_REQUEST');
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'key is malformed',
      reason: { code: 'INVALID_START_IDEMPOTENCY_REQUEST' },
    });
  });
});

describe('isStartReservationRefusal', () => {
  it('recognizes every union member and excludes unreadable storage', () => {
    const reservation: StartReservation = {
      key: 'key-1',
      owner: OWNER,
      targetKind: 'workflow',
      targetId: 'payout',
      runId: 'run-1',
      state: 'started',
      createdAt: 1_000,
      updatedAt: 2_000,
    };
    const refusals = [
      new StartReservationOwnerMismatchError('key-1'),
      new StartReservationTargetMismatchError('key-1', reservation),
      new IdempotentStartPendingError(reservation),
      new IdempotentStartUnresolvableError(reservation),
      new IdempotentStartAlreadySettledError(reservation),
      new StartIdempotencyUnsupportedError(),
      new InvalidStartIdempotencyRequestError('key is malformed'),
    ];

    expect(refusals.map(isStartReservationRefusal)).toEqual([
      true,
      true,
      true,
      true,
      true,
      true,
      true,
    ]);
    expect(
      isStartReservationRefusal(new StartReservationUnreadableError('key-1')),
    ).toBe(false);
  });
});

describe('reservation binding representation', () => {
  it('reads legacy and supported partial reservation schemas without DDL', async () => {
    for (const stage of [0, 1, 2, 3]) {
      const { sqlite, binding } = schemaHarness(stage);
      const statements: string[] = [];
      const store = new StartIdempotencyStore(
        interceptReservations(binding, async (sql, execute) => {
          statements.push(sql);
          return execute();
        }),
      );
      expect(await store.read('key')).toMatchObject({
        key: 'key',
        binding: { kind: 'legacy' },
      });
      expect(await store.reservationsForRuns(['run'])).toHaveLength(1);
      expect(await store.read('missing')).toBeUndefined();
      sqlite.exec(`DELETE FROM ${START_IDEMPOTENCY_TABLE}`);
      expect(await store.read('key')).toBeUndefined();
      expect(await store.reservationsForRuns(['run'])).toEqual([]);
      expect(statements.every((sql) => /^(SELECT|PRAGMA)/.test(sql))).toBe(
        true,
      );
      expect(
        sqlite.prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`).all(),
      ).toHaveLength(10 + stage);
    }
    const { sqlite, binding, store } = schemaHarness(0);
    expect((await store.read('key'))?.binding).toEqual({ kind: 'legacy' });
    let advanced = false;
    const reader = new StartIdempotencyStore(
      interceptReservations(binding, async (sql, execute) => {
        const result = await execute();
        if (!advanced && sql.startsWith('SELECT *')) {
          advanced = true;
          for (const column of bindingColumns)
            sqlite.exec(
              `ALTER TABLE ${START_IDEMPOTENCY_TABLE} ADD COLUMN ${column} TEXT`,
            );
          sqlite.exec(
            `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token = 'generation', start_table_prefix = '', start_workflow_id = 'payout'`,
          );
        }
        return result;
      }),
    );
    expect((await reader.read('key'))?.binding).toEqual({ kind: 'legacy' });
    expect((await store.read('key'))?.binding).toEqual({
      kind: 'bound',
      execution: {
        tablePrefix: '',
        workflowId: 'payout',
        runId: 'run',
        startToken: 'generation',
      },
    });
  });

  it('resumes concurrent reservation schema upgrades without changing rows', async () => {
    for (const stopAfter of [1, 2, 3]) {
      const { sqlite, binding } = schemaHarness(0);
      const before = rows(sqlite)[0];
      let additions = 0;
      let stopped = false;
      const crashed = new StartIdempotencyStore(
        interceptReservations(binding, async (sql, execute) => {
          if (stopped) throw new Error('process interrupted');
          const result = await execute();
          if (sql.startsWith('ALTER TABLE') && ++additions === stopAfter)
            stopped = true;
          return result;
        }),
      );
      await expect(
        crashed.reserve(workflowRequest('new', 'new-run')),
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
      expect(
        sqlite.prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`).all(),
      ).toHaveLength(10 + stopAfter);
      const recovered = new StartIdempotencyStore(binding);
      await recovered.reserve(workflowRequest('new', 'new-run'));
      expect(
        sqlite
          .prepare(`SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE key = 'key'`)
          .get(),
      ).toEqual({ ...before, ...legacyBinding });
    }
    for (const outcome of ['before', 'after', 'incompatible'] as const) {
      const { sqlite, binding } = schemaHarness(0);
      const failure = new Error('ALTER response lost');
      let injected = false;
      const store = new StartIdempotencyStore(
        interceptReservations(binding, async (sql, execute) => {
          if (injected || !sql.startsWith('ALTER TABLE')) return execute();
          injected = true;
          if (outcome === 'after') await execute();
          if (outcome === 'incompatible')
            sqlite.exec(
              `ALTER TABLE ${START_IDEMPOTENCY_TABLE} ADD COLUMN start_token INTEGER`,
            );
          throw failure;
        }),
      );
      if (outcome === 'after')
        await expect(
          store.reserve(workflowRequest('new', 'new-run')),
        ).resolves.toMatchObject({
          reservation: { binding: { kind: 'legacy' } },
        });
      else {
        const error = await store
          .reserve(workflowRequest('new', 'new-run'))
          .catch((error: unknown) => error);
        expect(error).toBeInstanceOf(StartReservationUnreadableError);
        if (outcome === 'before') {
          expect((error as Error).cause).toBe(failure);
          await expect(
            store.reserve(workflowRequest('retry', 'retry-run')),
          ).resolves.toMatchObject({ created: true });
        } else
          expect(String((error as Error).cause)).toContain(
            'column start_token differs',
          );
      }
    }
    const { sqlite, binding, store: competing } = schemaHarness(0);
    let raced = false;
    const first = new StartIdempotencyStore(
      interceptReservations(binding, async (sql, execute) => {
        if (!raced && sql.startsWith('ALTER TABLE')) {
          raced = true;
          await competing.reserve(workflowRequest('other', 'other-run'));
        }
        return execute();
      }),
    );
    await first.reserve(workflowRequest('first', 'first-run'));
    expect(rows(sqlite)).toHaveLength(3);
    expect(
      sqlite.prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`).all(),
    ).toHaveLength(13);
    const empty = harness();
    const hostReady = new StartIdempotencyStore(empty.binding, {
      ready: async () => {
        empty.sqlite.exec(START_IDEMPOTENCY_DDL);
      },
    });
    await expect(
      hostReady.reserve(workflowRequest('new', 'run')),
    ).resolves.toMatchObject({ created: true });
    const legacy = schemaHarness(0);
    const noMigration = new StartIdempotencyStore(legacy.binding, {
      ready: async () => {},
    });
    await expect(
      noMigration.reserve(workflowRequest('new', 'run')),
    ).rejects.toMatchObject({
      cause: {
        message: `${START_IDEMPOTENCY_TABLE} has an invalid reservation schema (host readiness did not reach the current schema)`,
      },
    });
    expect(
      legacy.sqlite
        .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
        .all(),
    ).toHaveLength(10);
  });

  it('distinguishes legacy unbound D1-bound and unfenced-bound reservations', async () => {
    const { sqlite, store, binding } = schemaHarness(3);
    for (const [token, prefix, workflow, expected] of [
      [null, null, null, { kind: 'legacy' }],
      ['', null, null, { kind: 'unbound' }],
      [
        'generation',
        '',
        'payout',
        {
          kind: 'bound',
          execution: {
            tablePrefix: '',
            workflowId: 'payout',
            runId: 'run',
            startToken: 'generation',
          },
        },
      ],
      [
        'generation',
        null,
        'payout',
        {
          kind: 'bound',
          execution: {
            tablePrefix: null,
            workflowId: 'payout',
            runId: 'run',
            startToken: 'generation',
          },
        },
      ],
    ] as const) {
      sqlite
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token = ?, start_table_prefix = ?, start_workflow_id = ?`,
        )
        .run(token, prefix, workflow);
      expect((await store.read('key'))?.binding).toEqual(expected);
    }
    for (const values of [
      [null, '', null],
      ['', '', null],
      ['generation', null, null],
      ['generation', 'Mixed_', 'payout'],
      ['bad token', '', 'payout'],
      ['generation', '', 'bad/workflow'],
    ]) {
      sqlite
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token = ?, start_table_prefix = ?, start_workflow_id = ?`,
        )
        .run(...values);
      await expect(store.read('key')).rejects.toBeInstanceOf(
        StartReservationUnreadableError,
      );
    }
    sqlite.exec(
      `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token = 'generation', start_table_prefix = '', start_workflow_id = 'payout', target_kind = 'agent'`,
    );
    await expect(store.read('key')).rejects.toBeInstanceOf(
      StartReservationUnreadableError,
    );
    sqlite.exec(
      `UPDATE ${START_IDEMPOTENCY_TABLE} SET target_kind = 'workflow', start_token = NULL, start_table_prefix = NULL, start_workflow_id = NULL`,
    );
    for (const result of [
      null,
      {},
      { results: null },
      { results: [undefined] },
      { results: new Array(1) },
      { results: [], success: false },
    ]) {
      const corrupt = new StartIdempotencyStore(
        interceptReservations(binding, async (sql, execute) =>
          sql.startsWith('SELECT *') ? result : execute(),
        ),
      );
      await expect(corrupt.read('key')).rejects.toBeInstanceOf(
        StartReservationUnreadableError,
      );
    }
    for (const stage of [1, 2]) {
      const partial = schemaHarness(stage);
      partial.sqlite.exec(
        `UPDATE ${START_IDEMPOTENCY_TABLE} SET start_token = ''`,
      );
      await expect(partial.store.read('key')).rejects.toMatchObject({
        cause: {
          message: `${START_IDEMPOTENCY_TABLE} has an invalid reservation schema (partial binding is not legacy defaults)`,
        },
      });
      await expect(
        partial.store.reserve(workflowRequest('new', 'new-run')),
      ).rejects.toBeInstanceOf(StartReservationUnreadableError);
      expect(
        partial.sqlite
          .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
          .all(),
      ).toHaveLength(10 + stage);
    }
    for (const [schema, field] of [
      [
        START_IDEMPOTENCY_DDL.replace(
          'start_token TEXT',
          'start_token INTEGER',
        ),
        'start_token',
      ],
      [
        START_IDEMPOTENCY_DDL.replace(
          'start_table_prefix TEXT',
          'start_table_prefix TEXT DEFAULT NULL',
        ),
        'start_table_prefix',
      ],
      [
        START_IDEMPOTENCY_DDL.replace(
          'owner_id TEXT NOT NULL,\n    target_kind',
          'target_id_alias TEXT NOT NULL,\n    target_kind',
        ),
        'owner_id',
      ],
    ] as const) {
      const malformed = harness();
      malformed.sqlite.exec(schema);
      const ownerColumn = schema.includes('target_id_alias')
        ? 'target_id_alias'
        : 'owner_id';
      malformed.sqlite.exec(
        `INSERT INTO ${START_IDEMPOTENCY_TABLE} (key,owner_kind,${ownerColumn},target_kind,target_id,run_id,state,created_at,updated_at) VALUES ('key','human','operator-1','workflow','payout','run','started',10,20)`,
      );
      await expect(malformed.store.read('key')).rejects.toMatchObject({
        cause: {
          message: `${START_IDEMPOTENCY_TABLE} has an invalid reservation schema (column ${field} differs)`,
        },
      });
    }
  });

  it('keeps reserve claim release and settle on legacy-null rows in A', async () => {
    const { store, sqlite } = harness();
    const created = await store.reserve(workflowRequest('key', 'run'));
    expect(created.reservation.binding).toEqual({ kind: 'legacy' });
    expect(await store.claim('key', 'run')).toBe(true);
    expect(await store.release('key', 'run')).toBe(true);
    expect(await store.claim('key', 'run')).toBe(true);
    expect(await store.settleRun('run')).toBe(1);
    expect(rows(sqlite)).toEqual([
      expect.objectContaining({ ...legacyBinding, state: 'terminal' }),
    ]);
  });

  it('captures the first ready getter result with the store receiver', async () => {
    const { sqlite, binding } = harness();
    let getterReads = 0;
    const calls: number[] = [];
    const receivers: StartIdempotencyStore[] = [];
    const store = new StartIdempotencyStore(binding, {
      get ready() {
        const selected = ++getterReads;
        return async function (this: StartIdempotencyStore) {
          calls.push(selected);
          receivers.push(this);
          sqlite.exec(START_IDEMPOTENCY_DDL);
        };
      },
    });
    await store.reserve(workflowRequest('first', 'first-run'));
    await store.reserve(workflowRequest('second', 'second-run'));
    expect(getterReads).toBe(1);
    expect(calls).toEqual([1, 1]);
    expect(receivers).toEqual([store, store]);
  });

  it('captures reserve authority and mint callback before readiness waits', async () => {
    const { sqlite, binding } = harness();
    let calls = 0;
    const request = {
      key: 'key',
      owner: { ...OWNER, id: 'operator-1' },
      targetKind: 'agent' as 'agent' | 'workflow',
      targetId: 'agent',
      threadId: 'thread',
      marker: 'receiver',
      mintRunId() {
        expect(this.marker).toBe('receiver');
        calls += 1;
        this.targetId = 'mint-mutated';
        return 'run';
      },
    };
    const store = new StartIdempotencyStore(binding, {
      ready: async () => {
        sqlite.exec(START_IDEMPOTENCY_DDL);
        request.owner.id = 'other';
        request.targetKind = 'workflow';
        request.targetId = 'changed';
        request.threadId = 'changed';
        request.mintRunId = () => {
          throw new Error('replacement mint');
        };
      },
    });
    const created = await store.reserve(request);
    expect(calls).toBe(1);
    expect(created.reservation).toMatchObject({
      owner: OWNER,
      targetKind: 'agent',
      targetId: 'agent',
      threadId: 'thread',
      runId: 'run',
      binding: { kind: 'legacy' },
    });
    const counts = new Map<string, number>();
    const data = workflowRequest('getter-key', 'getter-run');
    const getters = Object.defineProperties(
      {},
      Object.fromEntries(
        Object.entries(data).map(([field, value]) => [
          field,
          {
            get() {
              const count = (counts.get(field) ?? 0) + 1;
              counts.set(field, count);
              return count === 1 ? value : 'changed';
            },
          },
        ]),
      ),
    );
    await expect(
      new StartIdempotencyStore(binding).reserve(getters as typeof data),
    ).resolves.toMatchObject({
      created: true,
      reservation: { targetId: 'payout' },
    });
    expect([...counts.values()].every((count) => count === 1)).toBe(true);
    const invalid = harness();
    await expect(
      invalid.store.reserve({
        ...workflowRequest('key', 'run'),
        mintRunId: false as never,
      }),
    ).rejects.toBeInstanceOf(InvalidStartIdempotencyRequestError);
    expect(
      invalid.sqlite
        .prepare('SELECT name FROM sqlite_schema WHERE type = ?')
        .all('table'),
    ).toEqual([]);
    const shadowed = harness();
    let readyCalls = 0;
    let mintCalls = 0;
    const mint = () => {
      mintCalls += 1;
      return 'captured-run';
    };
    const ready = async () => {
      readyCalls += 1;
      shadowed.sqlite.exec(START_IDEMPOTENCY_DDL);
      Object.defineProperty(mint, 'call', { value: () => 'replacement-run' });
    };
    Object.defineProperty(ready, 'call', {
      value: () => {
        throw new Error('shadowed call invoked');
      },
    });
    await expect(
      new StartIdempotencyStore(shadowed.binding, { ready }).reserve({
        ...workflowRequest('shadowed', 'unused'),
        mintRunId: mint,
      }),
    ).resolves.toMatchObject({ reservation: { runId: 'captured-run' } });
    expect([readyCalls, mintCalls]).toEqual([1, 1]);
  });

  it.each([
    {
      name: 'a binding tail hole',
      missingToken: true,
      schemaStage: 3,
      cause: 'binding prefix has a hole',
    },
    {
      name: 'a row binding ahead of the schema',
      missingToken: false,
      schemaStage: 2,
      cause: 'schema observation precedes row binding',
    },
  ])('rejects $name without writes', async (scenario) => {
    const { sqlite, binding } = schemaHarness(3);
    const before = rows(sqlite);
    const observed = before.map((row) => ({ ...row }));
    if (scenario.missingToken) {
      for (const row of observed) delete row.start_token;
    }
    const schema = sqlite
      .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
      .all();
    const statements: string[] = [];
    const store = new StartIdempotencyStore(
      interceptReservations(binding, async (sql, execute) => {
        statements.push(sql);
        if (sql.startsWith('SELECT *')) return { results: observed };
        if (sql.startsWith('PRAGMA'))
          return { results: schema.slice(0, 10 + scenario.schemaStage) };
        return execute();
      }),
    );
    for (const read of [
      () => store.read('key'),
      () => store.reservationsForRuns(['run']),
    ]) {
      const error = await read().catch((error: unknown) => error);
      expect(error).toBeInstanceOf(StartReservationUnreadableError);
      expect((error as StartReservationUnreadableError).status).toBe(503);
      expect(String((error as Error).cause)).toContain(scenario.cause);
    }
    expect(statements.every((sql) => /^(SELECT|PRAGMA)/.test(sql))).toBe(true);
    expect(rows(sqlite)).toEqual(before);
    expect(
      sqlite.prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`).all(),
    ).toEqual(schema);
  });

  it('rejects invalid reservation observations without absence fallback', async () => {
    const { sqlite, binding } = schemaHarness(3);
    const valid = rows(sqlite)[0];
    if (valid === undefined) throw new Error('fixture reservation is missing');
    const cases = [
      {
        name: 'wrong key',
        rows: [{ ...valid, key: 'other-key' }],
        lookup: false,
        schemaMissing: false,
        cause: 'requested singleton key',
      },
      {
        name: 'multiple rows',
        rows: [valid, valid],
        lookup: false,
        schemaMissing: false,
        cause: 'requested singleton key',
      },
      {
        name: 'foreign run',
        rows: [{ ...valid, run_id: 'other-run' }],
        lookup: true,
        schemaMissing: false,
        cause: 'unrequested run',
      },
      ...Object.keys(valid)
        .slice(0, 10)
        .map((field) => {
          const copy = { ...valid };
          delete copy[field];
          return {
            name: `missing ${field}`,
            rows: [copy],
            lookup: false,
            schemaMissing: false,
            cause: `row is missing ${field}`,
          };
        }),
      ...[[], [valid]].map((result) => ({
        name: 'schema disappeared',
        rows: result,
        lookup: false,
        schemaMissing: true,
        cause: 'row observation has no schema',
      })),
    ];
    for (const scenario of cases) {
      const statements: string[] = [];
      const store = new StartIdempotencyStore(
        interceptReservations(binding, async (sql, execute) => {
          statements.push(sql);
          if (sql.startsWith('SELECT *')) return { results: scenario.rows };
          if (scenario.schemaMissing && sql.startsWith('PRAGMA'))
            return { results: [] };
          return execute();
        }),
      );
      const error = await (scenario.lookup
        ? store.reservationsForRuns(['run'])
        : store.read('key')
      ).catch((error: unknown) => error);
      expect(error, scenario.name).toBeInstanceOf(
        StartReservationUnreadableError,
      );
      expect((error as StartReservationUnreadableError).status).toBe(503);
      expect(String((error as Error).cause), scenario.name).toContain(
        scenario.cause,
      );
      expect(statements.every((sql) => /^(SELECT|PRAGMA)/.test(sql))).toBe(
        true,
      );
      expect(rows(sqlite)).toEqual([valid]);
    }
  });
});

describe('StartIdempotencyStore.reserve', () => {
  it('creates the reservation and reports the caller as its creator', async () => {
    // #given a key nobody has used
    const { store, sqlite } = harness();

    // #when
    const outcome = await store.reserve(workflowRequest('key-1', 'run-1'));

    // #then the caller owns the start, and the row records exactly what it
    // minted — the store never generates a run id of its own.
    expect(outcome.created).toBe(true);
    expect(outcome.reservation).toMatchObject({
      key: 'key-1',
      runId: 'run-1',
      state: 'reserved',
      targetKind: 'workflow',
      targetId: 'payout',
      owner: { kind: 'human', id: 'operator-1' },
    });
    expect(rows(sqlite)).toHaveLength(1);
  });

  it('gives every later caller the WINNER’s run id, and tells none of them they created it', async () => {
    // #given two callers minting DIFFERENT run ids under one key — the shape a
    // lost response takes when the client retries into a second isolate
    const { store, sqlite } = harness();
    await store.reserve(workflowRequest('key-1', 'run-first'));

    // #when
    const second = await store.reserve(workflowRequest('key-1', 'run-second'));

    // #then the loser converges onto the first run rather than starting one:
    // two winners is the failure this whole module exists to prevent.
    expect(second.created).toBe(false);
    expect(second.reservation.runId).toBe('run-first');
    expect(rows(sqlite)).toHaveLength(1);
  });

  it('refuses a key owned by another principal without naming what it holds', async () => {
    // #given a key already reserved by someone else
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));

    // #when a different principal probes it
    const refusal = await store
      .reserve({
        key: 'key-1',
        owner: OTHER_OWNER,
        targetKind: 'workflow',
        targetId: 'payout',
        mintRunId: () => 'run-2',
      })
      .catch((error: unknown) => error);

    // #then 403, and a reason carrying nothing about the reservation: a key is
    // guessable by construction, so this response is reachable by probing.
    expect(refusal).toBeInstanceOf(StartReservationOwnerMismatchError);
    expect((refusal as StartReservationOwnerMismatchError).status).toBe(403);
    expect((refusal as StartReservationOwnerMismatchError).reason).toEqual({
      code: 'IDEMPOTENT_START_OWNER_MISMATCH',
    });
  });

  it('checks the owner BEFORE the target, so a foreign caller learns nothing about either', async () => {
    // #given a reservation whose owner AND target both differ from the probe
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1', 'payout'));

    // #when
    const refusal = await store
      .reserve({
        key: 'key-1',
        owner: OTHER_OWNER,
        targetKind: 'workflow',
        targetId: 'refund',
        mintRunId: () => 'run-2',
      })
      .catch((error: unknown) => error);

    // #then owner wins: a target mismatch here would leak 'payout' to a
    // principal that has no claim on the key.
    expect(refusal).toBeInstanceOf(StartReservationOwnerMismatchError);
  });

  it('refuses the owner’s own key pointed at a different workflow, and names the target it holds', async () => {
    // #given
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1', 'payout'));

    // #when the same principal reuses the key for another workflow
    const refusal = await store
      .reserve(workflowRequest('key-1', 'run-2', 'refund'))
      .catch((error: unknown) => error);

    // #then 409 naming 'payout' — the caller owns this key, so telling it what
    // the key means is telling it about its own state.
    expect(refusal).toBeInstanceOf(StartReservationTargetMismatchError);
    expect((refusal as StartReservationTargetMismatchError).status).toBe(409);
    expect((refusal as StartReservationTargetMismatchError).reason).toEqual({
      code: 'IDEMPOTENT_START_TARGET_MISMATCH',
      targetKind: 'workflow',
      targetId: 'payout',
    });
  });

  it('refuses a key that switches target KIND, not just target id', async () => {
    // #given a workflow reservation
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1', 'payout'));

    // #when the same key names an AGENT called 'payout'
    const refusal = await store
      .reserve({
        key: 'key-1',
        owner: OWNER,
        targetKind: 'agent',
        targetId: 'payout',
        threadId: 'thread-1',
        mintRunId: () => 'run-2',
      })
      .catch((error: unknown) => error);

    // #then refused: a workflow and an agent that share a name are two
    // different execution families and two different charges.
    expect(refusal).toBeInstanceOf(StartReservationTargetMismatchError);
  });

  it('requires a thread for an agent reservation and rejects one for a workflow', async () => {
    // #given — the thread is the agent run's ADDRESS: without it a retry that
    // minted a fresh thread could never reach the original run.
    const { store } = harness();

    // #when / #then
    await expect(
      store.reserve({
        key: 'key-agent',
        owner: OWNER,
        targetKind: 'agent',
        targetId: 'writer',
        mintRunId: () => 'run-1',
      }),
    ).rejects.toBeInstanceOf(InvalidStartIdempotencyRequestError);
    await expect(
      store.reserve({
        key: 'key-workflow',
        owner: OWNER,
        targetKind: 'workflow',
        targetId: 'payout',
        threadId: 'thread-1',
        mintRunId: () => 'run-1',
      }),
    ).rejects.toBeInstanceOf(InvalidStartIdempotencyRequestError);
  });

  it('rejects a key that is not path-safe', async () => {
    // #given — the key is a primary key AND is compared against the execution
    // fence's proof key, so an unvalidated one reaches both.
    const { store } = harness();

    // #when / #then
    await expect(
      store.reserve(workflowRequest('key/../escape', 'run-1')),
    ).rejects.toBeInstanceOf(InvalidStartIdempotencyRequestError);
  });

  it('rejects a host mint that is not path-safe rather than storing it', async () => {
    // #given a host whose mint returns something the run addressing cannot use
    const { store, sqlite } = harness();

    // #when
    await expect(
      store.reserve({
        ...workflowRequest('key-1', 'unused'),
        mintRunId: () => 'run id with spaces',
      }),
    ).rejects.toBeInstanceOf(InvalidStartIdempotencyRequestError);

    // #then nothing was written: a stored id the DO name join cannot address
    // would be a reservation pointing at an unreachable run.
    expect(rows(sqlite)).toHaveLength(0);
  });
});

describe('StartIdempotencyStore.claim', () => {
  it('lets exactly one of many concurrent callers through', async () => {
    // #given one reservation and five callers racing its claim — the
    // cross-isolate race the agent surface cannot serialize any other way
    const { store } = harness();
    const { reservation } = await store.reserve(
      workflowRequest('key-1', 'run-1'),
    );

    // #when
    const outcomes = await Promise.all(
      Array.from({ length: 5 }, () =>
        store.claim(reservation.key, reservation.runId),
      ),
    );

    // #then exactly one winner. Not "at most one", not "usually one".
    expect(outcomes.filter(Boolean)).toHaveLength(1);
  });

  it('refuses a claim naming a different run than the reservation holds', async () => {
    // #given
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));

    // #when / #then a claim can never land on a row rewritten underneath it
    expect(await store.claim('key-1', 'run-other')).toBe(false);
  });

  it('cannot re-claim a reservation that is already started', async () => {
    // #given a claimed reservation
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    expect(await store.claim('key-1', 'run-1')).toBe(true);

    // #when / #then
    expect(await store.claim('key-1', 'run-1')).toBe(false);
  });
});

describe('StartIdempotencyStore.release', () => {
  it('returns a claim to reserved so a retry after the fence reopens converges', async () => {
    // #given a claim taken and then refused by the fence
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');

    // #when
    expect(await store.release('key-1', 'run-1')).toBe(true);

    // #then the SAME run id is claimable again — a fence transition mid-start
    // must not manufacture an unresolvable reservation out of an operator
    // action, nor hand the retry a second run.
    expect((await store.read('key-1'))?.state).toBe('reserved');
    expect(await store.claim('key-1', 'run-1')).toBe(true);
    expect((await store.read('key-1'))?.runId).toBe('run-1');
  });

  it('cannot release a reservation that already settled', async () => {
    // #given a terminal reservation
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    await store.settleRun('run-1');

    // #when / #then a spent key never becomes startable again
    expect(await store.release('key-1', 'run-1')).toBe(false);
    expect((await store.read('key-1'))?.state).toBe('terminal');
  });
});

describe('StartIdempotencyStore.settleRun', () => {
  it('marks the run’s reservation terminal and stamps the horizon from that moment', async () => {
    // #given a claimed reservation, and a clock that moves
    let now = 1_000;
    const { store } = harness(() => now);
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    now = 5_000;

    // #when
    expect(await store.settleRun('run-1')).toBe(1);

    // #then
    const stored = await store.read('key-1');
    expect(stored?.state).toBe('terminal');
    expect(stored?.updatedAt).toBe(5_000);
  });

  it('is a no-op the second time, so every terminal path may call it', async () => {
    // #given — a run can reach terminal by completing, failing, being cancelled
    // or timing out, and those paths do not coordinate.
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.settleRun('run-1');

    // #when / #then
    expect(await store.settleRun('run-1')).toBe(0);
  });

  it('settles nothing for a run nobody reserved', async () => {
    // #given the overwhelmingly common case: a run started without a key
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));

    // #when / #then
    expect(await store.settleRun('run-unrelated')).toBe(0);
  });
});

describe('StartIdempotencyStore against a missing table', () => {
  it('reads as absent, and neither claims nor settles', async () => {
    // #given a database on which no key has ever been used, so the lazy DDL
    // has never run
    const { store } = harness();

    // #when / #then absence is not a fault — it is an empty table by another
    // name — but it must also never look like a successful transition.
    expect(await store.read('key-1')).toBeUndefined();
    expect(await store.claim('key-1', 'run-1')).toBe(false);
    expect(await store.release('key-1', 'run-1')).toBe(false);
    expect(await store.settleRun('run-1')).toBe(0);
    expect(await store.reservationsForRuns(['run-1'])).toEqual([]);
  });

  it('creates NOTHING on a read — the inventory sweep must not be a write', async () => {
    // #given
    const { store, sqlite } = harness();

    // #when
    await store.read('key-1');
    await store.reservationsForRuns(['run-1']);

    // #then no lazy DDL: a read path that emits CREATE TABLE is a write path
    // wearing a read's name, and the drain inventory reads this table on every
    // sweep of a deployment that is deliberately not executing.
    const tables = sqlite
      .prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`)
      .all(START_IDEMPOTENCY_TABLE);
    expect(tables).toEqual([]);
  });

  it('degrades CLOSED when the store cannot be read for any other reason', async () => {
    // #given a binding whose reads fail with something that is NOT a missing
    // table — the case where answering "no reservation" would start a run
    const failing: StartIdempotencyDatabase = {
      prepare: () => ({
        bind: () => failing.prepare('') as never,
        run: async () => {
          throw new Error('D1_ERROR: network');
        },
        all: async () => {
          throw new Error('D1_ERROR: network');
        },
      }),
    };
    const store = new StartIdempotencyStore(failing);

    // #when / #then a 503, never `undefined`
    await expect(store.read('key-1')).rejects.toMatchObject({
      status: 503,
      reason: { code: 'IDEMPOTENT_START_UNREADABLE' },
    });
  });
});

describe('beginIdempotentStart', () => {
  it('tells the first caller to start and every retry to replay the same run', async () => {
    // #given a run that persisted after the first start
    const { store } = harness();
    const persisted = new Map<string, string>();
    const surface: IdempotentStartSurface<string> = {
      persisted: async (reservation) => persisted.get(reservation.runId),
      live: async () => false,
    };

    // #when the first call starts, records a summary, and a retry arrives
    const first = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-1'),
      surface,
    );
    expect(first.kind).toBe('start');
    persisted.set('run-1', 'summary-of-run-1');
    const retry = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      surface,
    );

    // #then the retry answers with the FIRST run's state and starts nothing
    expect(retry).toEqual({
      kind: 'replay',
      reservation: expect.objectContaining({ runId: 'run-1' }),
      persisted: 'summary-of-run-1',
    });
  });

  it('answers with the persisted run even when the reservation still reads started', async () => {
    // #given a reservation whose terminal reconcile was lost to a crash, but
    // whose run persisted
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');

    // #when
    const decision = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      { persisted: async () => 'summary', live: async () => false },
    );

    // #then the persisted state wins over the row's state: a stale `started`
    // must not refuse a retry whose run is sitting right there, finished.
    expect(decision.kind).toBe('replay');
  });

  it('lets a retry re-claim a reservation whose first caller died BEFORE the claim', async () => {
    // #given a bare reservation — the crash window between insert and claim,
    // in which nothing has executed
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));

    // #when
    const decision = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-ignored'),
      EMPTY_SURFACE,
    );

    // #then it proceeds with the RESERVED run id, not a fresh one: converging
    // here is what makes a crashed reservation self-healing instead of a key
    // that can never be used again.
    expect(decision).toMatchObject({
      kind: 'start',
      reservation: { runId: 'run-1', state: 'reserved' },
    });
  });

  it('refuses a claimed-but-unpersisted run as PENDING while its host is executing it', async () => {
    // #given a claim held by a run that is genuinely still working — the
    // normal in-flight window, which is legitimately unbounded because the
    // first persisted summary lands only at the first suspend or terminal
    let now = 1_000;
    const { store } = harness(() => now);
    await store.reserve(workflowRequest('key-1', 'run-1'));
    now = 2_500;
    await store.claim('key-1', 'run-1');

    // #when
    const refusal = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      { persisted: async () => undefined, live: async () => true },
    ).catch((error: unknown) => error);

    // #then 503 with the claim's own timestamp — retryable, and no timer
    // anywhere: a bound on legitimate work would misclassify a long live run
    // and invite a fresh key and a second charge.
    expect(refusal).toBeInstanceOf(IdempotentStartPendingError);
    expect((refusal as IdempotentStartPendingError).status).toBe(503);
    expect((refusal as IdempotentStartPendingError).reason).toEqual({
      code: 'IDEMPOTENT_START_PENDING',
      runId: 'run-1',
      pendingSince: 2_500,
    });
  });

  it('refuses a claimed-but-unpersisted run as UNRESOLVABLE when nothing is running it', async () => {
    // #given the one genuinely ambiguous state: the claim was taken, nothing
    // persisted, and the host that took it is gone
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');

    // #when
    const refusal = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      EMPTY_SURFACE,
    ).catch((error: unknown) => error);

    // #then 409 and NEVER a re-execution: whether the first step already took
    // effect is unknowable here, and the message says so.
    expect(refusal).toBeInstanceOf(IdempotentStartUnresolvableError);
    expect((refusal as IdempotentStartUnresolvableError).status).toBe(409);
    expect((refusal as IdempotentStartUnresolvableError).reason).toEqual({
      code: 'IDEMPOTENT_START_UNRESOLVABLE',
      runId: 'run-1',
    });
    expect((refusal as Error).message).toMatch(/fresh key/);
  });

  it('refuses a settled key whose summary has aged out as ALREADY_SETTLED', async () => {
    // #given a completed run whose snapshot the retention purge removed
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    await store.settleRun('run-1');

    // #when
    const refusal = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      EMPTY_SURFACE,
    ).catch((error: unknown) => error);

    // #then the work is done even though nobody can still read the outcome.
    // The reservation outliving the snapshot is the ONLY reason this answer
    // exists rather than a fresh key and a second run.
    expect(refusal).toBeInstanceOf(IdempotentStartAlreadySettledError);
    expect((refusal as IdempotentStartAlreadySettledError).reason).toEqual({
      code: 'IDEMPOTENT_START_ALREADY_SETTLED',
      runId: 'run-1',
    });
  });

  it('never returns `start` twice for one key, however many callers race it', async () => {
    // #given ten concurrent first-calls on one key against one database — the
    // shape of a client retrying into parallel isolates
    const { store } = harness();
    let mints = 0;
    const surface: IdempotentStartSurface<string> = {
      persisted: async () => undefined,
      live: async () => false,
    };

    // #when
    const decisions = await Promise.allSettled(
      Array.from({ length: 10 }, () =>
        beginIdempotentStart(
          store,
          {
            key: 'key-1',
            owner: OWNER,
            targetKind: 'workflow',
            targetId: 'payout',
            mintRunId: () => {
              mints += 1;
              return `run-${mints}`;
            },
          },
          surface,
        ),
      ),
    );

    // #then exactly one caller was told to start, and every other was refused
    // or told to replay. Two `start` decisions is two executions.
    const starts = decisions.filter(
      (outcome) =>
        outcome.status === 'fulfilled' && outcome.value.kind === 'start',
    );
    expect(starts).toHaveLength(1);
  });

  it('lets the CREATOR lose the claim to a later caller, and refuses the creator rather than starting it', async () => {
    // #given the interleaving the two-signal `created` test cannot reach by
    // racing: caller A wins the INSERT and caller B wins the CAS. Nothing about
    // creating the row entitles A to start it, and if both ever believed they
    // could, the key would have bought nothing.
    const { store } = harness();
    const live: IdempotentStartSurface<string> = {
      persisted: async () => undefined,
      // The winner IS executing — the realistic state of the world at the
      // moment the loser asks.
      live: async () => true,
    };
    const decisions: string[] = [];
    let loserDecision: unknown;
    const realClaim = store.claim.bind(store);
    let interleaved = false;
    store.claim = async (key: string, runId: string) => {
      if (!interleaved) {
        // B arrives in the window between A's insert and A's claim.
        interleaved = true;
        loserDecision = await beginIdempotentStart(
          store,
          workflowRequest('key-1', 'run-B'),
          live,
        );
      }
      return realClaim(key, runId);
    };

    // #when A (the creator) races its own claim against B's
    const refusal = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-A'),
      live,
    ).catch((error: unknown) => error);
    for (const decision of [loserDecision, refusal]) {
      if (
        decision !== null &&
        typeof decision === 'object' &&
        'kind' in decision
      ) {
        decisions.push(String((decision as { kind: string }).kind));
      }
    }

    // #then exactly one caller was told to start, it was B, and the run it
    // starts is the one A RESERVED — the reservation decides the run id, the
    // claim only decides who executes it.
    expect(decisions).toEqual(['start']);
    expect(loserDecision).toMatchObject({
      kind: 'start',
      reservation: { runId: 'run-A' },
    });
    // And the creator is REFUSED, pointed at the run its own key already names.
    expect(refusal).toBeInstanceOf(IdempotentStartPendingError);
    expect((refusal as IdempotentStartPendingError).reason.runId).toBe('run-A');
  });

  it('re-claims the reserved run after a crash between reserve and claim, and stores no second id', async () => {
    // #given a reservation whose creator died before claiming — nothing has
    // executed, and the row is the only thing that knows which run this key
    // means
    const { store, sqlite } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    let minted = 0;

    // #when the retry arrives, mints its own candidate, and is resolved
    const decision = await beginIdempotentStart(
      store,
      {
        key: 'key-1',
        owner: OWNER,
        targetKind: 'workflow',
        targetId: 'payout',
        mintRunId: () => {
          minted += 1;
          return 'run-retry-candidate';
        },
      },
      EMPTY_SURFACE,
    );

    // #then it starts the FIRST run, and the candidate it minted is discarded
    // rather than stored: a second id in this table is a second run waiting to
    // happen, whatever the decision said.
    expect(decision).toMatchObject({
      kind: 'start',
      reservation: { runId: 'run-1' },
    });
    expect(minted).toBe(1);
    expect(rows(sqlite)).toEqual([
      expect.objectContaining({ key: 'key-1', run_id: 'run-1' }),
    ]);
  });

  it('refuses a key belonging to another SURFACE’s principal, in both directions, and says nothing else', async () => {
    // #given the collision a shared key namespace makes reachable: a public
    // human actor on the run router and a stamped agent principal on the agent
    // topology, deriving the same key from the same order id
    const { store } = harness();
    const humanOwner = { kind: 'human', id: 'opal' } as const;
    const agentOwner = { kind: 'agent', id: 'writer-agent' } as const;
    const agentRequest = (key: string) => ({
      key,
      owner: agentOwner,
      targetKind: 'agent' as const,
      targetId: 'writer',
      threadId: 'thread-1',
      mintRunId: () => 'agent-run',
    });
    const humanRequest = (key: string) => ({
      key,
      owner: humanOwner,
      targetKind: 'workflow' as const,
      targetId: 'payout',
      mintRunId: () => 'workflow-run',
    });

    // #when each surface reserves first and the other follows
    await store.reserve(humanRequest('key-human-first'));
    const agentRefusal = await store
      .reserve(agentRequest('key-human-first'))
      .catch((error: unknown) => error);
    await store.reserve(agentRequest('key-agent-first'));
    const humanRefusal = await store
      .reserve(humanRequest('key-agent-first'))
      .catch((error: unknown) => error);

    // #then both directions refuse with 403 and an EMPTY body — no target, no
    // kind, no run. A key is guessable by construction, so this refusal is
    // reachable by probing, and anything it named would be something the prober
    // learned about a principal it has no claim on.
    for (const refusal of [agentRefusal, humanRefusal]) {
      expect(refusal).toBeInstanceOf(StartReservationOwnerMismatchError);
      expect((refusal as StartReservationOwnerMismatchError).status).toBe(403);
      expect((refusal as StartReservationOwnerMismatchError).reason).toEqual({
        code: 'IDEMPOTENT_START_OWNER_MISMATCH',
      });
    }
  });

  it('refuses rather than starting when the reservation row cannot be read', async () => {
    // #given a row this build cannot parse — here a corrupt `updated_at`, the
    // column the purge horizon and `pendingSince` are both measured from
    const { store, sqlite } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    sqlite
      .prepare(
        `UPDATE ${START_IDEMPOTENCY_TABLE} SET updated_at = 'corrupt' WHERE key = ?`,
      )
      .run('key-1');

    // #when the key is consulted again
    const refusal = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      EMPTY_SURFACE,
    ).catch((error: unknown) => error);

    // #then 503, and never the absent answer. "There is no reservation" is the
    // answer that STARTS A RUN, so it must be unreachable from a row that
    // cannot be understood — a corrupt terminal `updated_at` would otherwise
    // read as epoch 0 and make the row immediately reapable as well.
    expect(refusal).toBeInstanceOf(StartReservationUnreadableError);
    expect((refusal as StartReservationUnreadableError).status).toBe(503);
    expect((refusal as StartReservationUnreadableError).reason).toEqual({
      code: 'IDEMPOTENT_START_UNREADABLE',
    });
  });
});

describe('rollbackFencedStart', () => {
  it('gives the claim back for a fence refusal and re-throws it unchanged', async () => {
    // #given a claim consumed by a start the fence refused — provably
    // pre-execution, because the fence is read before the run lock
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    const fenced = new ExecutionFencedError('migration-locked', 'run start');

    // #when
    const thrown = await rollbackFencedStart(
      store,
      'key-1',
      'run-1',
      fenced,
    ).catch((error: unknown) => error);

    // #then the caller still sees the fence's own refusal, and the key is
    // usable again once the operator reopens.
    expect(thrown).toBe(fenced);
    expect((await store.read('key-1'))?.state).toBe('reserved');
  });

  it('recognizes a fence refusal that crossed a Durable Object boundary', async () => {
    // #given the shape a fenced run-DO start takes on the Worker side: the
    // class is gone, the status and structured reason survive
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    const wire = Object.assign(new Error('deployment execution is fenced'), {
      status: 503,
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });

    // #when
    await rollbackFencedStart(store, 'key-1', 'run-1', wire).catch(
      () => undefined,
    );

    // #then rolled back all the same — an instanceof-only test would answer
    // "not a fence refusal" for every caller on the far side of the boundary,
    // which is where the run router actually sits.
    expect((await store.read('key-1'))?.state).toBe('reserved');
  });

  it('KEEPS the claim for any other start failure', async () => {
    // #given a start that failed for a reason that may well have executed
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');

    // #when
    await rollbackFencedStart(
      store,
      'key-1',
      'run-1',
      new Error('step exploded'),
    ).catch(() => undefined);

    // #then still claimed: releasing here would hand the next retry a second
    // run after a start that may already have charged somebody.
    expect((await store.read('key-1'))?.state).toBe('started');
  });

  it('completes the whole round trip: claim, fence refusal, release, reopen, and ONE execution of the same run', async () => {
    // #given a real fence and a real reservation over one database, and a host
    // whose start executes paid work — the shape the round trip has to be
    // proved in, because each half of it is only correct given the other.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const store = new StartIdempotencyStore(
      binding as StartIdempotencyDatabase,
    );
    const fence = new ExecutionFenceStore(binding as ExecutionFenceDatabase);
    await fence.seed('open');
    await fence.transition({ expected: 'open', next: 'migration-locked' });
    let executions = 0;
    const startRun = async (): Promise<void> => {
      const reading = await fence.read();
      if (reading.state !== 'open') {
        throw new ExecutionFencedError(reading.state, 'run start');
      }
      executions += 1;
    };
    const attempt = async (candidate: string): Promise<StartReservation> => {
      const decision = await beginIdempotentStart(
        store,
        workflowRequest('key-1', candidate),
        EMPTY_SURFACE,
      );
      if (decision.kind !== 'start') throw new Error('expected a start');
      const { key, runId } = decision.reservation;
      try {
        await startRun();
      } catch (error) {
        return rollbackFencedStart(store, key, runId, error);
      }
      return decision.reservation;
    };

    // #when the fenced attempt is refused, the operator reopens, and the client
    // retries with the same key
    const refused = await attempt('run-1').catch((error: unknown) => error);
    expect(refused).toBeInstanceOf(ExecutionFencedError);
    expect((await store.read('key-1'))?.state).toBe('reserved');
    await fence.transition({ expected: 'migration-locked', next: 'open' });
    const started = await attempt('run-ignored');

    // #then the retry ran the SAME run the fenced attempt reserved, exactly
    // once. A rollback that did not land would have left the key UNRESOLVABLE
    // forever; a rollback that handed back a fresh run id would have made an
    // operator's drain the cause of a second charge.
    expect(started.runId).toBe('run-1');
    expect(executions).toBe(1);
    expect((await store.read('key-1'))?.state).toBe('started');
  });

  it('still re-throws the fence refusal when the release itself fails', async () => {
    // #given a claimed reservation and a store whose release cannot be written
    // — a storage incident arriving during a deployment that is already
    // refusing to execute
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    store.release = async () => {
      throw new Error('D1_ERROR: network');
    };
    const fenced = new ExecutionFencedError('migration-locked', 'run start');

    // #when
    const thrown = await rollbackFencedStart(
      store,
      'key-1',
      'run-1',
      fenced,
    ).catch((error: unknown) => error);

    // #then the caller sees the FENCE's refusal, not the storage error:
    // swallowing it would leave the caller believing the deployment is broken
    // rather than fenced, and the rollback's own failure is not something the
    // caller can act on.
    expect(thrown).toBe(fenced);
    // The claim stayed taken, so the key is now UNRESOLVABLE rather than
    // startable — recoverable by investigation, which is the direction this
    // best-effort rollback deliberately fails in.
    expect((await store.read('key-1'))?.state).toBe('started');
    await expect(
      beginIdempotentStart(
        store,
        workflowRequest('key-1', 'run-2'),
        EMPTY_SURFACE,
      ),
    ).rejects.toBeInstanceOf(IdempotentStartUnresolvableError);
  });
});

describe('requireStartIdempotency', () => {
  it('refuses a key on a host that wired no store', () => {
    // #given / #when / #then honouring the key silently would answer an
    // exactly-once REQUEST with at-least-once BEHAVIOUR, and the caller would
    // have no way to find out.
    expect(() => requireStartIdempotency('none')).toThrow(
      StartIdempotencyUnsupportedError,
    );
    expect(() => requireStartIdempotency(undefined)).toThrow(
      StartIdempotencyUnsupportedError,
    );
  });

  it('returns the store when one is wired', () => {
    // #given
    const { store } = harness();

    // #then
    expect(requireStartIdempotency(store)).toBe(store);
  });
});

describe('reservationsForRuns', () => {
  it('returns every reservation naming the given runs', async () => {
    // #given — the drain inventory and the purge both ask this question
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.reserve(workflowRequest('key-2', 'run-2'));

    // #when
    const found = await store.reservationsForRuns(['run-2', 'run-missing']);

    // #then
    expect(
      found.map((reservation: StartReservation) => reservation.key),
    ).toEqual(['key-2']);
  });
});

describe('proof-only composition', () => {
  it('re-asserts the proof binding on a REPLAY, so a run whose binding was lost stays resumable', async () => {
    // #given a proof-only deployment whose proof run already exists and
    // persisted, but whose fence has lost its proof_run_id — the shape left by
    // a fence moved away and back onto the same key while the run survived.
    // Without the binding, proof-only admits no resume for it at all.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const store = new StartIdempotencyStore(
      binding as StartIdempotencyDatabase,
    );
    const fence = new ExecutionFenceStore(binding as ExecutionFenceDatabase);
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-key-1',
    });
    await store.reserve({
      ...workflowRequest('proof-key-1', 'proof-run'),
      key: 'proof-key-1',
    });
    await store.claim('proof-key-1', 'proof-run');
    expect((await fence.read()).proofRunId).toBeUndefined();

    // #when a retry carrying the same key finds the run persisted
    const decision = await beginIdempotentStart(
      store,
      workflowRequest('proof-key-1', 'ignored'),
      { persisted: async () => 'summary', live: async () => false },
      fence,
    );

    // #then the replay answered with the run's state AND put the binding back
    expect(decision.kind).toBe('replay');
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-key-1',
      proofRunId: 'proof-run',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 1,
    });
  });

  it('changes nothing on a replay whose key is not the nominated proof key', async () => {
    // #given a proof-only fence nominating a DIFFERENT key
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const store = new StartIdempotencyStore(
      binding as StartIdempotencyDatabase,
    );
    const fence = new ExecutionFenceStore(binding as ExecutionFenceDatabase);
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-key-1',
    });
    await store.reserve(workflowRequest('other-key', 'other-run'));
    await store.claim('other-key', 'other-run');

    // #when
    await beginIdempotentStart(
      store,
      workflowRequest('other-key', 'ignored'),
      { persisted: async () => 'summary', live: async () => false },
      fence,
    );

    // #then the proof slot is untouched: every guard lives in recordProofRun's
    // own CAS, so an unrelated key is zero rows and no harm.
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-key-1',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 1,
    });
  });

  it('does not fail a replay when the proof re-bind cannot be written', async () => {
    // #given a fence whose write-back throws — a storage incident during a
    // replay of a run that already happened
    const { store } = harness();
    await store.reserve(workflowRequest('key-1', 'run-1'));
    await store.claim('key-1', 'run-1');
    const failing = {
      recordProofRun: async () => {
        throw new Error('D1_ERROR: network');
      },
    } as unknown as ExecutionFenceStore;

    // #when / #then the caller still gets the run's persisted state: refusing
    // the read would answer a successful retry with an error while changing
    // nothing about the run.
    const decision = await beginIdempotentStart(
      store,
      workflowRequest('key-1', 'run-2'),
      { persisted: async () => 'summary', live: async () => false },
      failing,
    );
    expect(decision.kind).toBe('replay');
  });
});
