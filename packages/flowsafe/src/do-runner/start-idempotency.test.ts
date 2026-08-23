// SPDX-License-Identifier: Apache-2.0
// The reservation state machine, exercised as real SQL over node:sqlite.
//
// Every assertion here is ultimately about a PAID external call. A workflow's
// first step can wire funds; a second run of it wires them twice. So the cases
// below are written adversarially — two callers racing the same insert, a
// caller probing somebody else's key, a claim taken twice, a purge reaching a
// row whose run is still readable — and each one asserts the EXPENSIVE
// direction: that exactly one caller was told to start.

import { describe, expect, it } from 'vitest';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import type { ExecutionFenceDatabase } from './execution-fence.js';
import {
  ExecutionFencedError,
  ExecutionFenceStore,
} from './execution-fence.js';
import {
  beginIdempotentStart,
  IdempotentStartAlreadySettledError,
  IdempotentStartPendingError,
  type IdempotentStartSurface,
  IdempotentStartUnresolvableError,
  InvalidStartIdempotencyRequestError,
  requireStartIdempotency,
  rollbackFencedStart,
  START_IDEMPOTENCY_TABLE,
  type StartIdempotencyDatabase,
  StartIdempotencyStore,
  StartIdempotencyUnsupportedError,
  type StartReservation,
  StartReservationOwnerMismatchError,
  StartReservationTargetMismatchError,
} from './start-idempotency.js';

const OWNER = { kind: 'human', id: 'operator-1' } as const;
const OTHER_OWNER = { kind: 'human', id: 'operator-2' } as const;

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

describe('StartIdempotencyStore.reserve', () => {
  it('creates the reservation and reports the caller as its creator', async () => {
    // #given a key nobody has used
    const { store, sqlite } = harness();

    // #when
    const outcome = await store.reserve(workflowRequest('key-1', 'run-1'));

    // #then the caller owns the start, and the row records exactly what it
    // minted — the store never generates a run id of its own (INV-1).
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
    // wearing a read's name, and F2's drain inventory reads this table on every
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
    // #given — the F2 inventory and the purge both ask this question
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
