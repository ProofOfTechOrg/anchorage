// SPDX-License-Identifier: Apache-2.0
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
// The raw table name and the state list come from the PROVISIONING PROTOCOL,
// which is their single home — `./execution-fence.js` deliberately does not
// re-export them (see its header), so a test that pinned them off the runtime
// module would be pinning a second copy.
import {
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
} from '../deployment-identity-protocol.js';
import { doErrorResponse } from './do-error-response.js';
import {
  admitsDrainableExecution,
  admitsExistingRun,
  admitsRunStart,
  admitsWorkAuthoring,
  type ExecutionFenceDatabase,
  ExecutionFencedError,
  type ExecutionFenceReading,
  type ExecutionFenceState,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
  FenceTransitionConflictError,
  InvalidExecutionFenceRequestError,
} from './execution-fence.js';
import { init } from './init.js';
import type { RunnerRuntime } from './runtime.js';

function fenceFixture(): {
  sqlite: SqliteDatabase;
  db: ExecutionFenceDatabase;
  fence: ExecutionFenceStore;
} {
  const sqlite = openSqlite();
  const db = sqliteUnitDatabase(sqlite) as ExecutionFenceDatabase;
  return { sqlite, db, fence: new ExecutionFenceStore(db) };
}

/** The schema as SQLite records it — the evidence a read wrote no DDL. */
function schemaSnapshot(sqlite: SqliteDatabase): unknown[] {
  return sqlite
    .prepare('SELECT type, name, sql FROM sqlite_master ORDER BY name')
    .all();
}

function reading(
  state: ExecutionFenceState,
  extra: Omit<ExecutionFenceReading, 'state'> = {},
): ExecutionFenceReading {
  return { state, ...extra };
}

describe('ExecutionFenceStore', () => {
  it('reads a database with no fence table as open, and writes no DDL doing it', async () => {
    // #given — a 0.19-era database: the fence table does not exist.
    const { sqlite, fence } = fenceFixture();
    const before = schemaSnapshot(sqlite);

    // #when
    const observed = await fence.read();

    // #then — open, and NOTHING was created. A read path that emits
    // `CREATE TABLE IF NOT EXISTS` is a write path wearing a read's name; it
    // would make a fenced deployment mutate its own database to answer a
    // question, and would turn a revoked-write incident into an outage.
    expect(observed).toEqual({ state: 'open' });
    expect(schemaSnapshot(sqlite)).toEqual(before);
    expect(before).toEqual([]);
  });

  it('reads a seeded-but-rowless table as open', async () => {
    // #given — the table exists (a crash between DDL and the row).
    const { db, fence } = fenceFixture();
    await fence.seed('open');
    await db.prepare(`DELETE FROM ${EXECUTION_FENCE_TABLE}`).run();

    // #then
    await expect(fence.read()).resolves.toEqual({ state: 'open' });
  });

  it('seed() requires an explicit state and never overwrites an existing row', async () => {
    // #given — a deployment seeded locked at birth.
    const { fence } = fenceFixture();
    await fence.seed('migration-locked');

    // #when — provisioning runs again (the already-owned early-return path).
    await fence.seed('open');

    // #then — the operator's state survives. An upsert here would silently
    // reopen a fence a migration closed.
    await expect(fence.read()).resolves.toEqual({ state: 'migration-locked' });

    // #and — the state is a required argument with no default, so a migration
    // host cannot forget it and silently get 'open'.
    await expect(
      (fence as unknown as { seed(state?: unknown): Promise<void> }).seed(),
    ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
  });

  it('transitions on a matching expected state', async () => {
    // #given
    const { fence } = fenceFixture();
    await fence.seed('open');

    // #when
    const next = await fence.transition({ expected: 'open', next: 'draining' });

    // #then
    expect(next).toEqual({ state: 'draining' });
    await expect(fence.read()).resolves.toEqual({ state: 'draining' });
  });

  it('materializes the implicit-open row of a database that has no fence table', async () => {
    // #given — a 0.19 database whose fence reads as open with no row at all.
    const { fence } = fenceFixture();

    // #when — the first transition is also the first write.
    await fence.transition({ expected: 'open', next: 'draining' });

    // #then
    await expect(fence.read()).resolves.toEqual({ state: 'draining' });
  });

  it('refuses a CAS whose expected state is stale, and reports the CURRENT one', async () => {
    // #given — another control-plane actor already locked it.
    const { fence } = fenceFixture();
    await fence.seed('open');
    await fence.transition({ expected: 'open', next: 'draining' });

    // #when / #then — the conflict carries the state the loser must re-plan
    // against, so it needs no second round trip to find out.
    const refusal = await fence
      .transition({ expected: 'open', next: 'migration-locked' })
      .catch((error: unknown) => error);
    expect(refusal).toBeInstanceOf(FenceTransitionConflictError);
    expect((refusal as FenceTransitionConflictError).status).toBe(409);
    expect((refusal as FenceTransitionConflictError).reason).toEqual({
      code: 'FENCE_CAS_CONFLICT',
      state: 'draining',
    });
    await expect(fence.read()).resolves.toEqual({ state: 'draining' });
  });

  it("requires a proofKey to enter 'proof-only', and rejects one anywhere else", async () => {
    // #given
    const { fence } = fenceFixture();
    await fence.seed('migration-locked');

    // #then — no key, no proof state.
    await expect(
      fence.transition({ expected: 'migration-locked', next: 'proof-only' }),
    ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
    // #and — a key for a state that has no proof is a caller belief that is
    // false, so it is refused rather than ignored.
    await expect(
      fence.transition({
        expected: 'migration-locked',
        next: 'open',
        proofKey: 'proof-1',
      }),
    ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
    await expect(fence.read()).resolves.toEqual({ state: 'migration-locked' });
  });

  it('clears the proof run on entry to and exit from proof-only', async () => {
    // #given — a proof state already bound to a run.
    const { fence } = fenceFixture();
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-1',
    });
    expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(true);
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-1',
      proofRunId: 'run-1',
    });

    // #when — a SECOND proof attempt under a new key.
    await fence.transition({
      expected: 'proof-only',
      next: 'proof-only',
      proofKey: 'proof-2',
    });

    // #then — the prior proof's run is gone; the new key admits a fresh one.
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-2',
    });

    // #and — leaving proof-only clears both fields.
    await fence.transition({ expected: 'proof-only', next: 'open' });
    await expect(fence.read()).resolves.toEqual({ state: 'open' });
  });

  describe('recordProofRun', () => {
    it('binds the first run, and admits the SAME run again on replay', async () => {
      // #given
      const { fence } = fenceFixture();
      await fence.seed('migration-locked');
      await fence.transition({
        expected: 'migration-locked',
        next: 'proof-only',
        proofKey: 'proof-1',
      });

      // #then — first write binds; a retry of the interrupted start converges
      // on the same runId instead of deadlocking on its own earlier write.
      expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(true);
      expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(true);
    });

    it('refuses a DIFFERENT run once the proof is bound', async () => {
      const { fence } = fenceFixture();
      await fence.seed('migration-locked');
      await fence.transition({
        expected: 'migration-locked',
        next: 'proof-only',
        proofKey: 'proof-1',
      });
      expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(true);

      expect(await fence.recordProofRun('proof-1', 'run-2')).toBe(false);
    });

    it('refuses when the fence moved between the admit-read and the write-back', async () => {
      // #given — admitted under proof-only...
      const { fence } = fenceFixture();
      await fence.seed('migration-locked');
      await fence.transition({
        expected: 'migration-locked',
        next: 'proof-only',
        proofKey: 'proof-1',
      });

      // #when — ...and the operator locked it again before the write-back.
      await fence.transition({
        expected: 'proof-only',
        next: 'migration-locked',
      });

      // #then — zero rows changed, so the caller refuses the start.
      expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(false);
    });

    it('refuses a key that is not the nominated one', async () => {
      const { fence } = fenceFixture();
      await fence.seed('migration-locked');
      await fence.transition({
        expected: 'migration-locked',
        next: 'proof-only',
        proofKey: 'proof-1',
      });

      expect(await fence.recordProofRun('proof-other', 'run-1')).toBe(false);
    });

    it('answers "not admitted" on a database with no fence table', async () => {
      const { fence } = fenceFixture();
      expect(await fence.recordProofRun('proof-1', 'run-1')).toBe(false);
    });
  });

  it('fails closed on a state name this build does not understand', async () => {
    // #given — a table this build did not create: a hand-edited row, or one
    // written by a NEWER flowsafe that added a state. The CHECK constraint is
    // deliberately absent, which is exactly what such a database would look
    // like from here.
    const { sqlite, db } = fenceFixture();
    const fence = new ExecutionFenceStore(db);
    sqlite.exec(
      `CREATE TABLE ${EXECUTION_FENCE_TABLE} (
         id TEXT PRIMARY KEY,
         state TEXT NOT NULL,
         proof_key TEXT,
         proof_run_id TEXT,
         updated_at INTEGER NOT NULL
       )`,
    );
    sqlite.exec(
      `INSERT INTO ${EXECUTION_FENCE_TABLE} (id, state, updated_at)
       VALUES ('deployment', 'quiesced-v2-from-the-future', 0)`,
    );

    // #then — never 'open'. Answering "I do not understand this fence" with
    // "there is no fence" is the one answer that must never be wrong.
    await expect(fence.read()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it('reads a pre-0.20 database as open when the adapter wraps the SQLite error', async () => {
    // #given — an adapter that reports its own message and carries the driver's
    // text on `cause`. This is the shape that makes the difference load-bearing:
    // matching only the TOP message would classify a correctly upgraded 0.19
    // database as unreadable, and every gated path on it would answer 503
    // permanently — the exact opposite of the upgrade rule.
    const wrapped = new Error('D1_ERROR: query failed', {
      cause: new Error(
        `SqliteError: no such table: ${EXECUTION_FENCE_TABLE}`,
        // Two links deep, because an adapter over a driver over SQLite is the
        // normal number of wrappers, not the pathological one.
        { cause: new Error(`no such table: ${EXECUTION_FENCE_TABLE}`) },
      ),
    });
    const fence = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          run: () => Promise.reject(wrapped),
          all: () => Promise.reject(wrapped),
        }),
        run: () => Promise.reject(wrapped),
        all: () => Promise.reject(wrapped),
      }),
    } as unknown as ExecutionFenceDatabase);

    // #then — open, and `recordProofRun` reaches the same conclusion: a
    // database with no fence table cannot be in proof-only.
    await expect(fence.read()).resolves.toEqual({ state: 'open' });
    await expect(fence.recordProofRun('proof-1', 'acme_r1')).resolves.toBe(
      false,
    );
  });

  it('still degrades closed when a wrapped cause is a genuine fault', async () => {
    // #given — the same wrapping shape, but the buried error is a real storage
    // fault. Walking the chain must not turn every wrapped error into an open
    // fence: only the missing TABLE reads as open.
    const wrapped = new Error('D1_ERROR: query failed', {
      cause: new Error('no such table: mastra_workflow_snapshot'),
    });
    const fence = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          run: () => Promise.reject(wrapped),
          all: () => Promise.reject(wrapped),
        }),
        run: () => Promise.reject(wrapped),
        all: () => Promise.reject(wrapped),
      }),
    } as unknown as ExecutionFenceDatabase);

    // #then
    await expect(fence.read()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it('terminates on a cyclic cause chain rather than degrading into a hang', async () => {
    // #given — an error whose cause is itself. The walk runs on the fence read
    // that fronts every gated request, so it is bounded and cycle-aware.
    const cyclic = new Error('D1_ERROR: query failed');
    (cyclic as { cause?: unknown }).cause = cyclic;
    const fence = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          run: () => Promise.reject(cyclic),
          all: () => Promise.reject(cyclic),
        }),
        run: () => Promise.reject(cyclic),
        all: () => Promise.reject(cyclic),
      }),
    } as unknown as ExecutionFenceDatabase);

    // #then — a decided answer, not a hang.
    await expect(fence.read()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it('degrades closed when the read itself fails', async () => {
    // #given — storage that answers every query with a fault (NOT the
    // "no such table" that legitimately reads as open).
    const fence = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          bind: () => {
            throw new Error('unreachable');
          },
          run: () => Promise.reject(new Error('D1_ERROR: network')),
          all: () => Promise.reject(new Error('D1_ERROR: network')),
        }),
        run: () => Promise.reject(new Error('D1_ERROR: network')),
        all: () => Promise.reject(new Error('D1_ERROR: network')),
      }),
    } as unknown as ExecutionFenceDatabase);

    // #then — a 503 that names the condition, never a silent open.
    const error = await fence.read().catch((thrown: unknown) => thrown);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect((error as ExecutionFenceUnreadableError).status).toBe(503);
    expect((error as ExecutionFenceUnreadableError).reason).toEqual({
      code: 'EXECUTION_FENCE_UNREADABLE',
    });
  });
});

describe('execution fence admission predicates', () => {
  it('admits a run START only in open, or in proof-only with the exact key', () => {
    expect(admitsRunStart(reading('open'))).toBe(true);
    expect(admitsRunStart(reading('draining'))).toBe(false);
    expect(admitsRunStart(reading('migration-locked'))).toBe(false);

    const proof = reading('proof-only', { proofKey: 'proof-1' });
    expect(admitsRunStart(proof, 'proof-1')).toBe(true);
    expect(admitsRunStart(proof, 'proof-2')).toBe(false);
    expect(admitsRunStart(proof)).toBe(false);
    // A proof state with no key admits nothing — never "any key matches".
    expect(admitsRunStart(reading('proof-only'), 'proof-1')).toBe(false);
  });

  it('admits work on an EXISTING run through a drain, and in proof-only only for the proof run', () => {
    expect(admitsExistingRun(reading('open'), 'run-1')).toBe(true);
    expect(admitsExistingRun(reading('draining'), 'run-1')).toBe(true);
    expect(admitsExistingRun(reading('migration-locked'), 'run-1')).toBe(false);

    const proof = reading('proof-only', {
      proofKey: 'proof-1',
      proofRunId: 'run-1',
    });
    expect(admitsExistingRun(proof, 'run-1')).toBe(true);
    expect(admitsExistingRun(proof, 'run-2')).toBe(false);
    expect(admitsExistingRun(proof)).toBe(false);
    expect(admitsExistingRun(reading('proof-only'), 'run-1')).toBe(false);
  });

  it('admits AUTHORING future work only while open', () => {
    expect(admitsWorkAuthoring(reading('open'))).toBe(true);
    for (const state of [
      'draining',
      'migration-locked',
      'proof-only',
    ] as const) {
      expect(admitsWorkAuthoring(reading(state))).toBe(false);
    }
  });

  it('admits draining execution of already-queued work, but not past the lock', () => {
    expect(admitsDrainableExecution(reading('open'))).toBe(true);
    expect(admitsDrainableExecution(reading('draining'))).toBe(true);
    expect(admitsDrainableExecution(reading('migration-locked'))).toBe(false);
    expect(admitsDrainableExecution(reading('proof-only'))).toBe(false);
  });

  it('covers every declared state', () => {
    // A new state must be adjudicated by every predicate above rather than
    // falling through one of them by default.
    expect([...EXECUTION_FENCE_STATES]).toEqual([
      'open',
      'draining',
      'migration-locked',
      'proof-only',
    ]);
  });
});

describe('doErrorResponse', () => {
  it('maps an ExecutionFencedError to 503 with its reason code', async () => {
    const response = doErrorResponse(
      new ExecutionFencedError('migration-locked', 'run start'),
    );

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error:
        "deployment execution is fenced ('migration-locked'): run start is refused",
      reason: { code: 'EXECUTION_FENCED', state: 'migration-locked' },
    });
  });

  it('maps an unreadable fence to 503 with its own code', async () => {
    const response = doErrorResponse(
      new ExecutionFenceUnreadableError(
        'execution fence state is not readable',
      ),
    );

    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error: 'execution fence state is not readable',
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
  });
});

describe('init() fence wiring', () => {
  it('auto-builds a fence from a { DB } source', async () => {
    // #given — the shape every production host passes.
    const { sqlite, db } = fenceFixture();
    const { runtime, executionFence } = init({
      DB: db as never,
    });

    // #then — fenced by construction: the host asked for nothing.
    expect(executionFence).toBeInstanceOf(ExecutionFenceStore);
    expect(runtime.executionFence).toBe(executionFence);

    // #and — it is THE SAME database, so nothing can fence one and read another.
    await executionFence?.seed('draining');
    await expect(runtime.executionFence?.read()).resolves.toEqual({
      state: 'draining',
    });
    expect(
      schemaSnapshot(sqlite).some(
        (row) => (row as { name?: string }).name === EXECUTION_FENCE_TABLE,
      ),
    ).toBe(true);
  });

  it("accepts an explicit 'none' for a { storage } source", () => {
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: 'none' },
    );

    expect(runtime.executionFence).toBeUndefined();
  });

  it('takes a shared store for a { storage } source', async () => {
    const { fence } = fenceFixture();
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: fence },
    );

    expect(runtime.executionFence).toBe(fence);
  });

  it('will not compile a { storage } source without explicit fence wiring', () => {
    // A TYPE-level pin. An UNUSED @ts-expect-error is itself an error in this
    // package's tsconfig, so `tsc` exiting 0 is what proves the negative: the
    // options argument is required, and omitting `executionFence` from it
    // fails. This is the compile-time obligation that keeps a host from
    // silently building an unfenced runtime.
    const build = (): unknown =>
      // @ts-expect-error a { storage } source must state its fence wiring
      init({ storage: new InMemoryStore() });
    expect(build).toBeTypeOf('function');
  });
});

// The workflow the enforcement matrix drives: one step that suspends until it
// is resumed, so a single fixture covers both start and resume.
function fencedRuntime(fence: ExecutionFenceStore): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init(
    { storage: new InMemoryStore() },
    { executionFence: fence },
  );
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({ done: z.boolean() }),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ ok: z.boolean() }),
    execute: async ({ resumeData, suspend }) =>
      resumeData ? { done: true } : suspend({ reason: 'awaiting' }),
  });
  createWorkflow({
    id: 'gated',
    inputSchema: z.object({}),
    outputSchema: z.object({ done: z.boolean() }),
  })
    .then(gate)
    .commit();
  return runtime;
}

describe('RunnerRuntime enforcement', () => {
  it('starts and resumes freely while open', async () => {
    const { fence } = fenceFixture();
    await fence.seed('open');
    const runtime = fencedRuntime(fence);

    const started = await runtime.start('gated', {
      runId: 'run-open',
      inputData: {},
    });
    expect(started.status).toBe('suspended');
    const resumed = await runtime.resume('gated', 'run-open', {
      resumeData: { ok: true },
    });
    expect(resumed.status).toBe('success');
  });

  it('blocks a start but still resumes while draining', async () => {
    // #given — a run already suspended before the drain began.
    const { fence } = fenceFixture();
    await fence.seed('open');
    const runtime = fencedRuntime(fence);
    await runtime.start('gated', { runId: 'run-drain', inputData: {} });
    await fence.transition({ expected: 'open', next: 'draining' });

    // #then — no new work...
    const refusal = await runtime
      .start('gated', { runId: 'run-drain-2' })
      .catch((error: unknown) => error);
    expect(refusal).toBeInstanceOf(ExecutionFencedError);
    expect((refusal as ExecutionFencedError).reason).toEqual({
      code: 'EXECUTION_FENCED',
      state: 'draining',
    });

    // #and — ...but the outstanding run still finishes, which is the entire
    // point of a drain.
    const resumed = await runtime.resume('gated', 'run-drain', {
      resumeData: { ok: true },
    });
    expect(resumed.status).toBe('success');
  });

  it('blocks both a start and a resume under migration-locked', async () => {
    const { fence } = fenceFixture();
    await fence.seed('open');
    const runtime = fencedRuntime(fence);
    await runtime.start('gated', { runId: 'run-locked', inputData: {} });
    await fence.transition({ expected: 'open', next: 'migration-locked' });

    await expect(
      runtime.start('gated', { runId: 'run-locked-2', inputData: {} }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
    await expect(
      runtime.resume('gated', 'run-locked', { resumeData: { ok: true } }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
  });

  it('admits exactly the proof start and the proof resume under proof-only', async () => {
    // #given
    const { fence } = fenceFixture();
    await fence.seed('migration-locked');
    await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-key-1',
    });
    const runtime = fencedRuntime(fence);

    // #then — a start with no key, or the wrong key, is refused.
    await expect(
      runtime.start('gated', { runId: 'proof-run', inputData: {} }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
    await expect(
      runtime.start('gated', {
        runId: 'proof-run',
        idempotencyKey: 'guessed-key',
        inputData: {},
      }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);

    // #and — the nominated start is admitted, and BINDS the proof run.
    const started = await runtime.start('gated', {
      runId: 'proof-run',
      idempotencyKey: 'proof-key-1',
      inputData: {},
    });
    expect(started.status).toBe('suspended');
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-key-1',
      proofRunId: 'proof-run',
    });

    // #and — a SECOND start under the same key is refused: the proof is one
    // run, and recordProofRun's CAS is what says so.
    await expect(
      runtime.start('gated', {
        runId: 'other-run',
        idempotencyKey: 'proof-key-1',
        inputData: {},
      }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);

    // #and — only the proof run may be resumed.
    await expect(
      runtime.resume('gated', 'unrelated-run', { resumeData: { ok: true } }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
    const resumed = await runtime.resume('gated', 'proof-run', {
      resumeData: { ok: true },
    });
    expect(resumed.status).toBe('success');
  });

  it('lets a start already past its fence read complete when the fence moves', async () => {
    // #given — a workflow whose FIRST step closes the fence mid-run, which is
    // the in-flight race a transition must not preempt.
    const { fence } = fenceFixture();
    await fence.seed('open');
    const { createWorkflow, createStep, runtime } = init(
      { storage: new InMemoryStore() },
      { executionFence: fence },
    );
    const drainMidRun = createStep({
      id: 'drain-mid-run',
      inputSchema: z.object({}),
      outputSchema: z.object({ done: z.boolean() }),
      execute: async () => {
        await fence.transition({ expected: 'open', next: 'draining' });
        return { done: true };
      },
    });
    createWorkflow({
      id: 'racing',
      inputSchema: z.object({}),
      outputSchema: z.object({ done: z.boolean() }),
    })
      .then(drainMidRun)
      .commit();

    // #when
    const summary = await runtime.start('racing', {
      runId: 'run-racing',
      inputData: {},
    });

    // #then — in-flight compute is never preempted; only the NEXT start is
    // refused. The drain sequence is drain, then prove empty, then lock.
    expect(summary.status).toBe('success');
    await expect(fence.read()).resolves.toEqual({ state: 'draining' });
    await expect(
      runtime.start('racing', { runId: 'run-racing-2', inputData: {} }),
    ).rejects.toBeInstanceOf(ExecutionFencedError);
  });

  it('degrades a start closed when the fence cannot be read', async () => {
    // #given — a fence whose storage is down.
    const failing = new ExecutionFenceStore({
      prepare: () => ({
        bind: () => ({
          bind: () => {
            throw new Error('unreachable');
          },
          run: () => Promise.reject(new Error('D1_ERROR: network')),
          all: () => Promise.reject(new Error('D1_ERROR: network')),
        }),
        run: () => Promise.reject(new Error('D1_ERROR: network')),
        all: () => Promise.reject(new Error('D1_ERROR: network')),
      }),
    } as unknown as ExecutionFenceDatabase);
    const runtime = fencedRuntime(failing);

    // #then — 503, not a start on a deployment whose state is unknown.
    const error = await runtime
      .start('gated', { runId: 'run-unreadable' })
      .catch((thrown: unknown) => thrown);
    expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
    expect(doErrorResponse(error).status).toBe(503);
  });
});
