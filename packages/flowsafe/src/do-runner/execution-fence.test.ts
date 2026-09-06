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
  EXECUTION_FENCE_DDL,
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
} from '../deployment-identity-protocol.js';
import { seedDeploymentIdentity } from './deployment-identity.js';
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
  type ExecutionFenceStatement,
  ExecutionFenceStore,
  ExecutionFenceUnreadableError,
  executionFenceReadingPayload,
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
  const backing = sqliteUnitDatabase(sqlite) as ExecutionFenceDatabase;
  const db = { prepare: (sql: string) => backing.prepare(sql) };
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

const optionalMetadata = {
  mutationEpoch: 0,
  requireMutationEpoch: false,
  transitionRevision: 0,
};
const proofColumns = [
  'proof_table_prefix',
  'proof_workflow_id',
  'proof_start_token',
];
const emptyProofIdentity = {
  proof_table_prefix: null,
  proof_workflow_id: null,
  proof_start_token: null,
};
const legacyFenceDdl = `CREATE TABLE flowsafe_execution_fence (
  id TEXT PRIMARY KEY CHECK (id = 'deployment'),
  state TEXT NOT NULL CHECK (state IN ('open', 'draining', 'migration-locked', 'proof-only')),
  proof_key TEXT, proof_run_id TEXT, updated_at INTEGER NOT NULL
)`;

function rawFence(sqlite: SqliteDatabase): Record<string, unknown> {
  return sqlite
    .prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`)
    .get() as Record<string, unknown>;
}

function interceptedDatabase(
  db: ExecutionFenceDatabase,
  intercept: (sql: string, execute: () => Promise<unknown>) => Promise<unknown>,
): ExecutionFenceDatabase {
  function statement(sql: string, values: unknown[]): ExecutionFenceStatement {
    return {
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
    };
  }
  return { prepare: (sql) => statement(sql, []) };
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
    expect(observed).toEqual({ state: 'open', ...optionalMetadata });
    expect(schemaSnapshot(sqlite)).toEqual(before);
    expect(before).toEqual([]);
  });

  it('reads a seeded-but-rowless table as open', async () => {
    // #given — the table exists (a crash between DDL and the row).
    const { sqlite, fence } = fenceFixture();
    sqlite.exec(legacyFenceDdl);

    // #then
    await expect(fence.read()).resolves.toEqual({
      state: 'open',
      ...optionalMetadata,
    });
  });

  it('seed() requires an explicit state and never overwrites an existing row', async () => {
    // #given — a deployment seeded locked at birth.
    const { fence } = fenceFixture();
    await fence.seed('migration-locked');

    // #when — provisioning runs again (the already-owned early-return path).
    await fence.seed('open');

    // #then — the operator's state survives. An upsert here would silently
    // reopen a fence a migration closed.
    await expect(fence.read()).resolves.toEqual({
      state: 'migration-locked',
      ...optionalMetadata,
    });

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
    expect(next).toEqual({
      state: 'draining',
      ...optionalMetadata,
      transitionRevision: 1,
    });
    await expect(fence.read()).resolves.toEqual(next);
  });

  it('materializes the implicit-open row of a database that has no fence table', async () => {
    // #given — a 0.19 database whose fence reads as open with no row at all.
    const { fence } = fenceFixture();

    // #when — the first transition is also the first write.
    await fence.transition({ expected: 'open', next: 'draining' });

    // #then
    await expect(fence.read()).resolves.toEqual({
      state: 'draining',
      ...optionalMetadata,
      transitionRevision: 1,
    });
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
      ...optionalMetadata,
      transitionRevision: 1,
      conflict: 'expectation-mismatch',
    });
    await expect(fence.read()).resolves.toEqual({
      state: 'draining',
      ...optionalMetadata,
      transitionRevision: 1,
    });
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
    await expect(fence.read()).resolves.toEqual({
      state: 'migration-locked',
      ...optionalMetadata,
    });
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
      ...optionalMetadata,
      transitionRevision: 1,
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
      ...optionalMetadata,
      transitionRevision: 2,
    });

    // #and — leaving proof-only clears both fields.
    await fence.transition({ expected: 'proof-only', next: 'open' });
    await expect(fence.read()).resolves.toEqual({
      state: 'open',
      ...optionalMetadata,
      transitionRevision: 3,
    });
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
    // text on `cause`, with the missing table at the ROOT of the chain. This is
    // the shape that makes the difference load-bearing: matching only the TOP
    // message would classify a correctly upgraded 0.19 database as unreadable,
    // and every gated path on it would answer 503 permanently — the exact
    // opposite of the upgrade rule.
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
    await expect(fence.read()).resolves.toEqual({
      state: 'open',
      ...optionalMetadata,
    });
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

  it('degrades closed when the missing table is mid-chain and the ROOT is a real fault', async () => {
    // #given — a chain that MENTIONS the fence table on its way past, but whose
    // innermost fault is something else: a failed migration that dropped the
    // table and then hit a real storage error underneath it. Matching any link
    // would read this as "there is no fence table, so the deployment is open" —
    // which is the one conclusion that must never be reached from a fault. Only
    // the root says what actually happened.
    const wrapped = new Error('D1_ERROR: query failed', {
      cause: new Error(`no such table: ${EXECUTION_FENCE_TABLE}`, {
        cause: new Error('D1_ERROR: database is locked'),
      }),
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

    // #then — unreadable, not open. `recordProofRun` agrees, for the same
    // reason: nothing about a locked database says the fence is absent.
    await expect(fence.read()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
    await expect(
      fence.recordProofRun('proof-1', 'acme_r1'),
    ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
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

describe('versioned execution fence persistence', () => {
  const activation = {
    expected: 'open',
    next: 'draining',
    expectedMutationEpoch: 0,
    expectedRevision: 0,
    advanceMutationEpoch: true,
  } as const;
  const activeReading = {
    state: 'draining',
    mutationEpoch: 1,
    requireMutationEpoch: true,
    transitionRevision: 1,
  } as const;

  it('keeps legacy readings optional and state-only predicate inputs compatible', async () => {
    for (const state of EXECUTION_FENCE_STATES) {
      const { sqlite, fence } = fenceFixture();
      sqlite.exec(legacyFenceDdl);
      await expect(fence.read()).resolves.toEqual({
        state: 'open',
        ...optionalMetadata,
      });
      sqlite
        .prepare(`INSERT INTO ${EXECUTION_FENCE_TABLE} VALUES (?, ?, ?, ?, ?)`)
        .run('deployment', state, 'old key', 'old run', 19);
      const expected = {
        state,
        ...optionalMetadata,
        proofKey: 'old key',
        proofRunId: 'old run',
      };
      await expect(fence.read()).resolves.toEqual(expected);
      expect(executionFenceReadingPayload(expected)).toEqual(expected);
      expect(executionFenceReadingPayload({ state })).toEqual({ state });
      expect(admitsRunStart({ state })).toBe(state === 'open');
      expect(admitsWorkAuthoring({ state })).toBe(state === 'open');
      expect(admitsExistingRun({ state })).toBe(
        state === 'open' || state === 'draining',
      );
      expect(admitsDrainableExecution({ state })).toBe(
        state === 'open' || state === 'draining',
      );
      expect(
        sqlite.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`).all(),
      ).toHaveLength(5);
    }
    const { fence } = fenceFixture();
    await expect(fence.read()).resolves.toEqual({
      state: 'open',
      ...optionalMetadata,
    });
    const conflict = new FenceTransitionConflictError('open', 'draining');
    expect(conflict.message).toBe(
      "execution fence transition expected state 'open' but found 'draining'",
    );
    expect(conflict.reason).toEqual({
      code: 'FENCE_CAS_CONFLICT',
      state: 'draining',
    });
  });

  it('refuses a missing row once any FS8 column exists', async () => {
    for (let stage = 1; stage <= 7; stage += 1) {
      const { sqlite, db } = fenceFixture();
      sqlite.exec(EXECUTION_FENCE_DDL);
      const additions = [
        'last_transition_request',
        'transition_revision',
        'mutation_epoch',
        'require_mutation_epoch',
        ...proofColumns,
      ];
      for (const column of additions.slice(stage).reverse())
        sqlite.exec(
          `ALTER TABLE ${EXECUTION_FENCE_TABLE} DROP COLUMN ${column}`,
        );
      const statements: string[] = [];
      const fence = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          statements.push(sql);
          return execute();
        }),
      );
      for (const action of [
        () => fence.read(),
        () => fence.seed('open'),
        () => fence.transition(activation),
      ]) {
        await expect(action()).rejects.toBeInstanceOf(
          ExecutionFenceUnreadableError,
        );
      }
      expect(
        sqlite.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`).all(),
      ).toEqual([]);
      expect(
        statements.filter((sql) => /^(INSERT|UPDATE|ALTER|CREATE)/.test(sql)),
      ).toEqual([]);
    }
    for (const operation of ['read', 'seed'] as const) {
      const { sqlite, db } = fenceFixture();
      sqlite.exec(EXECUTION_FENCE_DDL);
      let reads = 0;
      const writes: string[] = [];
      const regressed = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (/^(INSERT|UPDATE|ALTER|CREATE)/.test(sql)) writes.push(sql);
          if (sql.startsWith('SELECT *') && ++reads === 2) {
            sqlite.exec(`DROP TABLE ${EXECUTION_FENCE_TABLE}`);
            sqlite.exec(legacyFenceDdl);
            sqlite.exec(
              `INSERT INTO ${EXECUTION_FENCE_TABLE} VALUES ('deployment', 'open', NULL, NULL, 0)`,
            );
          }
          return execute();
        }),
      );
      await expect(
        operation === 'read' ? regressed.read() : regressed.seed('open'),
      ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
      expect(reads).toBe(2);
      expect(writes).toEqual([]);
    }
  });

  it('resumes every supported fence schema prefix without reopening the row', async () => {
    for (const state of EXECUTION_FENCE_STATES) {
      for (let stopAfter = 0; stopAfter <= 7; stopAfter += 1) {
        const { sqlite, db } = fenceFixture();
        let additions = 0;
        let stopped = false;
        const crashing = new ExecutionFenceStore(
          interceptedDatabase(db, async (sql, execute) => {
            if (stopped) throw new Error('process interrupted');
            const result = await execute();
            if (sql.startsWith('ALTER TABLE')) additions += 1;
            if (
              (sql.startsWith('INSERT OR IGNORE') ||
                sql.startsWith('ALTER TABLE')) &&
              additions === stopAfter
            )
              stopped = true;
            return result;
          }),
          { now: () => 12 },
        );
        await expect(crashing.seed(state)).rejects.toBeInstanceOf(
          ExecutionFenceUnreadableError,
        );
        expect(
          sqlite.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`).all(),
        ).toHaveLength(5 + stopAfter);
        const before = rawFence(sqlite);
        const fence = new ExecutionFenceStore(db, { now: () => 99 });
        await fence.seed('open');
        await expect(fence.read()).resolves.toEqual({
          state,
          ...optionalMetadata,
        });
        expect(rawFence(sqlite)).toEqual({
          ...before,
          last_transition_request: null,
          transition_revision: 0,
          mutation_epoch: 0,
          require_mutation_epoch: 0,
          ...emptyProofIdentity,
        });
        expect(rawFence(sqlite).updated_at).toBe(12);
      }
    }
  });

  it('converges concurrent fence schema initialization without batch', async () => {
    const { sqlite, db } = fenceFixture();
    const second = new ExecutionFenceStore(db, { now: () => 21 });
    let interleaved = false;
    const first = new ExecutionFenceStore(
      interceptedDatabase(db, async (sql, execute) => {
        const result = await execute();
        if (!interleaved && sql.startsWith('SELECT *')) {
          interleaved = true;
          await second.seed('migration-locked');
        }
        return result;
      }),
      { now: () => 13 },
    );
    await first.seed('open');
    await expect(first.read()).resolves.toEqual({
      state: 'migration-locked',
      ...optionalMetadata,
    });
    expect(rawFence(sqlite).updated_at).toBe(21);
    expect(
      sqlite.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`).all(),
    ).toHaveLength(1);

    const parallel = fenceFixture();
    const outcomes = await Promise.allSettled([
      parallel.fence.seed('draining'),
      new ExecutionFenceStore(parallel.db).seed('proof-only'),
    ]);
    expect(outcomes.map((outcome) => outcome.status)).toEqual([
      'fulfilled',
      'fulfilled',
    ]);
    expect((await parallel.fence.read()).transitionRevision).toBe(0);
    expect(
      parallel.sqlite.prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}`).all(),
    ).toHaveLength(1);

    const alterRace = fenceFixture();
    let paused = false;
    let duplicateAlter = false;
    const racingInitializer = new ExecutionFenceStore(
      interceptedDatabase(alterRace.db, async (sql, execute) => {
        if (!paused && sql.startsWith('ALTER TABLE')) {
          paused = true;
          await alterRace.fence.seed('open');
          try {
            return await execute();
          } catch (error) {
            duplicateAlter = true;
            throw error;
          }
        }
        return execute();
      }),
    );
    await racingInitializer.seed('migration-locked');
    expect(duplicateAlter).toBe(true);
    await expect(racingInitializer.read()).resolves.toEqual({
      state: 'migration-locked',
      ...optionalMetadata,
    });
  });

  it('preserves active P1 metadata through every new proof-column prefix', async () => {
    for (const stage of [4, 5, 6]) {
      const { fence, sqlite, db } = fenceFixture();
      const admitted = await fence.transition({
        ...activation,
        next: 'proof-only',
        proofKey: 'key',
      });
      await fence.recordProofRun('key', 'run', admitted);
      for (const column of proofColumns.slice(stage - 4).reverse())
        sqlite.exec(
          `ALTER TABLE ${EXECUTION_FENCE_TABLE} DROP COLUMN ${column}`,
        );
      const before = rawFence(sqlite);
      await expect(fence.read()).resolves.toEqual({
        ...admitted,
        proofRunId: 'run',
      });
      await fence.seed('open');
      expect(rawFence(sqlite)).toEqual({ ...before, ...emptyProofIdentity });
      const writes: string[] = [];
      await new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (/^(CREATE|INSERT|UPDATE|ALTER)/.test(sql)) writes.push(sql);
          return execute();
        }),
      ).seed('open');
      expect(writes).toEqual([]);
    }
    const { fence, sqlite, db } = fenceFixture();
    await fence.transition(activation);
    for (const column of [...proofColumns].reverse())
      sqlite.exec(`ALTER TABLE ${EXECUTION_FENCE_TABLE} DROP COLUMN ${column}`);
    let advanced = false;
    const reader = new ExecutionFenceStore(
      interceptedDatabase(db, async (sql, execute) => {
        const result = await execute();
        if (!advanced && sql.startsWith('SELECT *')) {
          advanced = true;
          await fence.seed('open');
        }
        return result;
      }),
    );
    await expect(reader.read()).resolves.toEqual(activeReading);
  });

  it('distinguishes partial mutation defaults from partial proof defaults', async () => {
    for (const stage of [1, 2, 3, 5, 6]) {
      const { fence, sqlite, db } = fenceFixture();
      if (stage >= 4) await fence.transition(activation);
      else await fence.seed('open');
      const names = [
        'last_transition_request',
        'transition_revision',
        'mutation_epoch',
        'require_mutation_epoch',
        ...proofColumns,
      ];
      for (const column of names.slice(stage).reverse())
        sqlite.exec(
          `ALTER TABLE ${EXECUTION_FENCE_TABLE} DROP COLUMN ${column}`,
        );
      const column = stage < 4 ? names[stage - 1] : proofColumns[stage - 5];
      sqlite
        .prepare(`UPDATE ${EXECUTION_FENCE_TABLE} SET ${column} = ?`)
        .run(stage === 1 ? 'receipt' : stage < 4 ? 1 : 'not-null');
      const before = rawFence(sqlite);
      const writes: string[] = [];
      const subject = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (/^(CREATE|INSERT|UPDATE|ALTER)/.test(sql)) writes.push(sql);
          return execute();
        }),
      );
      for (const action of [() => subject.read(), () => subject.seed('open')]) {
        await expect(action()).rejects.toMatchObject({
          cause: {
            message: `${EXECUTION_FENCE_TABLE} has an invalid execution-fence schema (${stage < 4 ? 'partial metadata is not optional defaults' : 'partial proof identity is not null'})`,
          },
        });
      }
      expect(writes).toEqual([]);
      expect(rawFence(sqlite)).toEqual(before);
    }
  });

  it('decodes complete D1 proof identity but omits it from admin payloads', async () => {
    const { fence, sqlite, db } = fenceFixture();
    const admitted = await fence.transition({
      ...activation,
      next: 'proof-only',
      proofKey: 'key',
    });
    await fence.recordProofRun('key', 'run', admitted);
    sqlite.exec(
      `UPDATE ${EXECUTION_FENCE_TABLE} SET proof_table_prefix = '', proof_workflow_id = 'workflow', proof_start_token = 'generation'`,
    );
    const proofExecution = {
      tablePrefix: '',
      workflowId: 'workflow',
      runId: 'run',
      startToken: 'generation',
    };
    await expect(fence.read()).resolves.toEqual({
      ...admitted,
      proofRunId: 'run',
      proofExecution,
    });
    expect(executionFenceReadingPayload(await fence.read())).toEqual({
      ...admitted,
      proofRunId: 'run',
    });
    const valid = rawFence(sqlite);
    for (const corruption of [
      { proof_table_prefix: null },
      { proof_workflow_id: null },
      { proof_start_token: null },
      { proof_table_prefix: 'Mixed_' },
      { proof_table_prefix: 'bad-prefix' },
      { proof_workflow_id: 'bad/workflow' },
      { proof_start_token: 'bad token' },
      { proof_run_id: null },
      { proof_run_id: 'bad/run' },
      { state: 'open' },
      { proof_key: '' },
    ]) {
      const entries = Object.entries(corruption);
      sqlite
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE} SET ${entries.map(([key]) => `${key} = ?`).join(', ')}`,
        )
        .run(...entries.map(([, value]) => value));
      const before = rawFence(sqlite);
      await expect(fence.read()).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
      await expect(fence.seed('open')).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
      expect(rawFence(sqlite)).toEqual(before);
      sqlite
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE} SET ${Object.keys(valid)
            .map((key) => `${key} = ?`)
            .join(', ')}`,
        )
        .run(...Object.values(valid));
    }
    sqlite.exec(
      `UPDATE ${EXECUTION_FENCE_TABLE} SET proof_table_prefix = 'Mixed_'`,
    );
    const before = rawFence(sqlite);
    await seedDeploymentIdentity(db, 'acme', 'open');
    expect(rawFence(sqlite)).toEqual(before);
    await expect(fence.read()).rejects.toBeInstanceOf(
      ExecutionFenceUnreadableError,
    );
  });

  it('clears proof identity on a new admin command and preserves it on exact retry', async () => {
    for (const upgraded of [false, true]) {
      const { fence, sqlite } = fenceFixture();
      const command = {
        expected: 'open',
        next: 'proof-only',
        proofKey: 'key',
        expectedMutationEpoch: 0,
        expectedRevision: 0,
      } as const;
      const admitted = await fence.transition(command);
      await fence.recordProofRun('key', 'run', admitted);
      sqlite.exec(
        `UPDATE ${EXECUTION_FENCE_TABLE} SET proof_table_prefix = 'tenant_', proof_workflow_id = 'workflow', proof_start_token = 'generation'`,
      );
      const before = rawFence(sqlite);
      await expect(fence.transition(command)).resolves.toEqual(
        await fence.read(),
      );
      expect(rawFence(sqlite)).toEqual(before);
      await fence.transition({
        expected: 'proof-only',
        next: 'proof-only',
        proofKey: 'key',
        ...(upgraded ? { expectedMutationEpoch: 0, expectedRevision: 1 } : {}),
      });
      expect(rawFence(sqlite)).toMatchObject({
        proof_run_id: null,
        ...emptyProofIdentity,
        transition_revision: 2,
      });
      expect((await fence.read()).proofExecution).toBeUndefined();
    }
  });

  it('does not swallow an ALTER failure unless compatible metadata proves completion', async () => {
    for (const outcome of ['before', 'after', 'incompatible'] as const) {
      const { db, sqlite } = fenceFixture();
      const failure = new Error('lost ALTER response');
      let injected = false;
      const fence = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (injected || !sql.startsWith('ALTER TABLE')) return execute();
          injected = true;
          if (outcome === 'after') await execute();
          if (outcome === 'incompatible')
            sqlite.exec(
              `ALTER TABLE ${EXECUTION_FENCE_TABLE} ADD COLUMN last_transition_request INTEGER`,
            );
          throw failure;
        }),
      );
      if (outcome === 'after') {
        await fence.seed('draining');
        await expect(fence.read()).resolves.toEqual({
          state: 'draining',
          ...optionalMetadata,
        });
      } else {
        const error = await fence
          .seed('draining')
          .catch((error: unknown) => error);
        expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
        if (outcome === 'before') expect((error as Error).cause).toBe(failure);
        else
          expect(String((error as Error).cause)).toContain(
            'column last_transition_request differs',
          );
      }
    }
  });

  it('activates the mutation epoch and requirement in one compare-and-set', async () => {
    const { fence, db, sqlite } = fenceFixture();
    await fence.seed('open');
    const outcomes = await Promise.allSettled([
      fence.transition(activation),
      new ExecutionFenceStore(db).transition({
        ...activation,
        next: 'migration-locked',
      }),
    ]);
    expect(
      outcomes.filter((outcome) => outcome.status === 'fulfilled'),
    ).toHaveLength(1);
    expect(
      outcomes.filter((outcome) => outcome.status === 'rejected'),
    ).toHaveLength(1);
    const stored = rawFence(sqlite);
    expect(stored.mutation_epoch).toBe(1);
    expect(stored.require_mutation_epoch).toBe(1);
    expect(stored.transition_revision).toBe(1);
    expect((await fence.read()).mutationEpoch).toBe(1);
  });

  it('converges an identical activation after a lost UPDATE response', async () => {
    const { db, sqlite } = fenceFixture();
    let now = 30;
    const fence = new ExecutionFenceStore(
      interceptedDatabase(db, async (sql, execute) => {
        const result = await execute();
        if (sql.startsWith('UPDATE')) throw new Error('response lost');
        return result;
      }),
      { now: () => now },
    );
    await expect(fence.transition(activation)).resolves.toEqual(activeReading);
    const before = rawFence(sqlite);
    now = 40;
    await expect(fence.transition(activation)).resolves.toEqual(activeReading);
    expect(rawFence(sqlite)).toEqual(before);
    expect(before.last_transition_request).toBe(
      '[1,"open","draining",null,0,0,true]',
    );
  });

  it('distinguishes a conflicting request from an identical resulting state', async () => {
    const { fence, sqlite } = fenceFixture();
    await fence.transition(activation);
    const before = rawFence(sqlite);
    for (const difference of [
      { next: 'open' as const },
      { advanceMutationEpoch: false },
      { expectedMutationEpoch: 1 },
      { expectedRevision: 1 },
      { expected: 'draining' as const },
      { next: 'proof-only' as const, proofKey: 'another-key' },
    ]) {
      await expect(
        fence.transition({ ...activation, ...difference }),
      ).rejects.toBeInstanceOf(FenceTransitionConflictError);
      expect(rawFence(sqlite)).toEqual(before);
    }
    const proof = {
      ...activation,
      next: 'proof-only' as const,
      proofKey: 'key-a',
    };
    const other = fenceFixture();
    await other.fence.transition(proof);
    await expect(
      other.fence.transition({ ...proof, proofKey: 'key-b' }),
    ).rejects.toBeInstanceOf(FenceTransitionConflictError);
  });

  it('rejects an upgraded stale revision after a same-epoch state cycle', async () => {
    const { fence } = fenceFixture();
    const initial = {
      expected: 'open',
      next: 'draining',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
    } as const;
    await fence.transition(initial);
    await fence.transition({
      expected: 'draining',
      next: 'open',
      expectedMutationEpoch: 0,
      expectedRevision: 1,
    });
    await expect(fence.transition(initial)).rejects.toBeInstanceOf(
      FenceTransitionConflictError,
    );
    await expect(fence.read()).resolves.toEqual({
      state: 'open',
      ...optionalMetadata,
      transitionRevision: 2,
    });
    await fence.transition({ expected: 'open', next: 'open' });
    await expect(
      fence.transition({ ...initial, expectedRevision: 2 }),
    ).rejects.toBeInstanceOf(FenceTransitionConflictError);
  });

  it('keeps the artifact epoch and requirement through lock proof and reopen', async () => {
    const { fence } = fenceFixture();
    await fence.transition(activation);
    let expected: ExecutionFenceState = 'draining';
    let revision = 1;
    for (const next of [
      'migration-locked',
      'proof-only',
      'open',
      'open',
    ] as const) {
      await expect(
        fence.transition({
          expected,
          next,
          expectedMutationEpoch: 1,
          expectedRevision: revision,
          ...(next === 'proof-only' ? { proofKey: 'proof-a' } : {}),
        }),
      ).resolves.toEqual({
        state: next,
        mutationEpoch: 1,
        requireMutationEpoch: true,
        transitionRevision: ++revision,
        ...(next === 'proof-only' ? { proofKey: 'proof-a' } : {}),
      });
      expected = next;
    }
  });

  it('preserves a bound proof run when the entry transition is retried', async () => {
    const { fence, sqlite } = fenceFixture();
    const command = {
      ...activation,
      next: 'proof-only' as const,
      proofKey: 'proof-a',
    };
    const admitted = await fence.transition(command);
    expect(await fence.recordProofRun('proof-a', 'run-a', admitted)).toBe(true);
    const before = rawFence(sqlite);
    await expect(fence.transition(command)).resolves.toEqual({
      ...admitted,
      proofRunId: 'run-a',
    });
    expect(rawFence(sqlite)).toEqual(before);
    await expect(
      fence.transition({
        expected: 'proof-only',
        next: 'proof-only',
        proofKey: 'proof-a',
        expectedMutationEpoch: 1,
        expectedRevision: 1,
      }),
    ).resolves.toEqual({ ...admitted, transitionRevision: 2 });
    expect(rawFence(sqlite).proof_run_id).toBeNull();
  });

  it('rejects an old proof admission after same-key proof reentry', async () => {
    const { db, sqlite } = fenceFixture();
    let now = 20;
    const fence = new ExecutionFenceStore(db, { now: () => now });
    const admitted = await fence.transition({
      ...activation,
      next: 'proof-only',
      proofKey: 'proof-a',
    });
    await fence.transition({
      expected: 'proof-only',
      next: 'migration-locked',
      expectedMutationEpoch: 1,
      expectedRevision: 1,
    });
    const current = await fence.transition({
      expected: 'migration-locked',
      next: 'proof-only',
      proofKey: 'proof-a',
      expectedMutationEpoch: 1,
      expectedRevision: 2,
    });
    expect(await fence.recordProofRun('proof-a', 'run-a', admitted)).toBe(
      false,
    );
    expect(await fence.recordProofRun('proof-a', 'run-a', current)).toBe(true);
    const before = rawFence(sqlite);
    now = 99;
    expect(await fence.recordProofRun('proof-a', 'run-a', current)).toBe(true);
    expect(await fence.recordProofRun('proof-a', 'run-b', current)).toBe(false);
    expect(rawFence(sqlite)).toEqual(before);
  });

  it('fences a queued legacy admin write at activation', async () => {
    for (const legacyFirst of [true, false]) {
      const { fence, db } = fenceFixture();
      await fence.seed('open');
      let triggered = false;
      const queued = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (!triggered && sql.startsWith('UPDATE')) {
            triggered = true;
            if (legacyFirst)
              await fence.transition({ expected: 'open', next: 'open' });
            else await fence.transition({ ...activation, next: 'open' });
          }
          return execute();
        }),
      );
      await expect(
        queued.transition(
          legacyFirst ? activation : { expected: 'open', next: 'draining' },
        ),
      ).rejects.toBeInstanceOf(FenceTransitionConflictError);
      await expect(fence.read()).resolves.toEqual({
        state: 'open',
        mutationEpoch: legacyFirst ? 0 : 1,
        requireMutationEpoch: !legacyFirst,
        transitionRevision: 1,
      });
    }
  });

  it('captures proof admission counters before storage waits', async () => {
    for (const advance of [false, true]) {
      for (const responseLost of [false, true]) {
        const { fence, db, sqlite } = fenceFixture();
        const admitted = {
          ...(await fence.transition({
            ...activation,
            next: 'proof-only',
            proofKey: 'key',
          })),
        };
        const failure = new Error('proof response lost');
        let replaced = false;
        const queued = new ExecutionFenceStore(
          interceptedDatabase(db, async (sql, execute) => {
            const result = await execute();
            if (!replaced && sql.startsWith('SELECT *')) {
              replaced = true;
              const current = await fence.transition({
                expected: 'proof-only',
                next: 'proof-only',
                proofKey: 'key',
                expectedMutationEpoch: 1,
                expectedRevision: 1,
                advanceMutationEpoch: advance,
              });
              admitted.mutationEpoch = current.mutationEpoch;
              admitted.transitionRevision = current.transitionRevision;
            }
            if (responseLost && sql.startsWith('UPDATE')) {
              expect(await fence.recordProofRun('key', 'run', admitted)).toBe(
                true,
              );
              throw failure;
            }
            return result;
          }),
        );
        if (responseLost) {
          const error = await queued
            .recordProofRun('key', 'run', admitted)
            .catch((error: unknown) => error);
          expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
          expect((error as Error).cause).toBe(failure);
        } else {
          expect(await queued.recordProofRun('key', 'run', admitted)).toBe(
            false,
          );
          expect(rawFence(sqlite).proof_run_id).toBeNull();
        }
        expect(rawFence(sqlite).transition_revision).toBe(2);
        expect(rawFence(sqlite).mutation_epoch).toBe(advance ? 2 : 1);
      }
    }
  });

  it('refuses legacy proof binding after activation without upgrading its authority', async () => {
    const { fence, db, sqlite } = fenceFixture();
    await fence.transition({
      expected: 'open',
      next: 'proof-only',
      proofKey: 'proof-a',
    });
    const queued = new ExecutionFenceStore(
      interceptedDatabase(db, async (sql, execute) => {
        if (sql.startsWith('UPDATE'))
          await fence.transition({
            expected: 'proof-only',
            next: 'proof-only',
            proofKey: 'proof-a',
            expectedMutationEpoch: 0,
            expectedRevision: 1,
            advanceMutationEpoch: true,
          });
        return execute();
      }),
    );
    expect(await queued.recordProofRun('proof-a', 'run-a')).toBe(false);
    expect(rawFence(sqlite).proof_run_id).toBeNull();
    await expect(fence.read()).resolves.toEqual({
      state: 'proof-only',
      proofKey: 'proof-a',
      mutationEpoch: 1,
      requireMutationEpoch: true,
      transitionRevision: 2,
    });
  });

  it('validates the captured advance flag before initialization', async () => {
    const { fence, sqlite } = fenceFixture();
    let reads = 0;
    await expect(
      fence.transition({
        ...activation,
        get advanceMutationEpoch() {
          reads += 1;
          return reads === 1 ? 'false' : false;
        },
      }),
    ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
    expect(reads).toBe(1);
    expect(schemaSnapshot(sqlite)).toEqual([]);
  });

  it('validates exact counters and never wraps the epoch or revision', async () => {
    const { fence, sqlite } = fenceFixture();
    const invalid = [
      -1,
      0.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      '0',
      null,
      true,
      Number.MAX_SAFE_INTEGER + 1,
    ];
    for (const value of invalid) {
      for (const key of [
        'expectedMutationEpoch',
        'expectedRevision',
      ] as const) {
        await expect(
          fence.transition({ ...activation, [key]: value }),
        ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
      }
      await expect(
        fence.recordProofRun('key', 'run', {
          mutationEpoch: value as number,
          transitionRevision: 0,
        }),
      ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
      await expect(
        fence.recordProofRun('key', 'run', {
          mutationEpoch: 0,
          transitionRevision: value as number,
        }),
      ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
    }
    for (const admitted of [
      null,
      {},
      { mutationEpoch: 0 },
      { transitionRevision: 0 },
    ]) {
      await expect(
        fence.recordProofRun(
          'key',
          'run',
          // @ts-expect-error runtime validation also rejects untyped partial authority
          admitted,
        ),
      ).rejects.toBeInstanceOf(InvalidExecutionFenceRequestError);
    }
    for (const input of [
      {
        expected: 'open' as const,
        next: 'open' as const,
        expectedMutationEpoch: 0,
      },
      { expected: 'open' as const, next: 'open' as const, expectedRevision: 0 },
      {
        expected: 'open' as const,
        next: 'open' as const,
        advanceMutationEpoch: true,
      },
      { ...activation, expectedRevision: Number.MAX_SAFE_INTEGER },
      { ...activation, expectedMutationEpoch: Number.MAX_SAFE_INTEGER },
      ...[null, 0, 'false'].map((advanceMutationEpoch) => ({
        ...activation,
        advanceMutationEpoch,
      })),
    ])
      await expect(fence.transition(input)).rejects.toBeInstanceOf(
        InvalidExecutionFenceRequestError,
      );
    expect(schemaSnapshot(sqlite)).toEqual([]);
    await fence.seed('open');
    sqlite.exec(
      `UPDATE ${EXECUTION_FENCE_TABLE} SET transition_revision = 9007199254740990`,
    );
    const maximum = {
      ...activation,
      advanceMutationEpoch: false,
      expectedRevision: Number.MAX_SAFE_INTEGER - 1,
    };
    await fence.transition(maximum);
    await expect(fence.transition(maximum)).resolves.toEqual({
      state: 'draining',
      ...optionalMetadata,
      transitionRevision: Number.MAX_SAFE_INTEGER,
    });
    await expect(
      fence.transition({ expected: 'draining', next: 'open' }),
    ).rejects.toBeInstanceOf(FenceTransitionConflictError);
    expect(rawFence(sqlite).transition_revision).toBe(Number.MAX_SAFE_INTEGER);

    const nearMaximumEpoch = fenceFixture();
    await nearMaximumEpoch.fence.transition(activation);
    nearMaximumEpoch.sqlite
      .prepare(
        `UPDATE ${EXECUTION_FENCE_TABLE} SET mutation_epoch = ?, last_transition_request = ?`,
      )
      .run(
        Number.MAX_SAFE_INTEGER - 1,
        JSON.stringify([
          1,
          'open',
          'draining',
          null,
          Number.MAX_SAFE_INTEGER - 2,
          0,
          true,
        ]),
      );
    const lastEpoch = {
      ...activation,
      expected: 'draining' as const,
      next: 'proof-only' as const,
      proofKey: 'key',
      expectedMutationEpoch: Number.MAX_SAFE_INTEGER - 1,
      expectedRevision: 1,
    };
    const full = {
      state: 'proof-only',
      proofKey: 'key',
      mutationEpoch: Number.MAX_SAFE_INTEGER,
      requireMutationEpoch: true,
      transitionRevision: 2,
    };
    await expect(nearMaximumEpoch.fence.transition(lastEpoch)).resolves.toEqual(
      full,
    );
    await expect(nearMaximumEpoch.fence.transition(lastEpoch)).resolves.toEqual(
      full,
    );
    expect(
      await nearMaximumEpoch.fence.recordProofRun('key', 'run', {
        mutationEpoch: Number.MAX_SAFE_INTEGER,
        transitionRevision: 2,
      }),
    ).toBe(true);
    await expect(
      nearMaximumEpoch.fence.transition({
        expected: 'proof-only',
        next: 'open',
        expectedMutationEpoch: Number.MAX_SAFE_INTEGER,
        expectedRevision: 2,
      }),
    ).resolves.toEqual({
      state: 'open',
      mutationEpoch: Number.MAX_SAFE_INTEGER,
      requireMutationEpoch: true,
      transitionRevision: 3,
    });
  });

  it('rejects malformed receipt bytes and incoherent upgraded rows', async () => {
    const { fence, sqlite } = fenceFixture();
    await fence.transition(activation);
    const corruptions: Record<string, unknown>[] = [
      { last_transition_request: '{' },
      { last_transition_request: ' '.repeat(513) },
      ...[
        null,
        {},
        [],
        [2, 'open', 'draining', null, 0, 0, true],
        [1, 'open', 'draining', null, '0', 0, true],
        [1, 'open', 'draining', null, 0, 0, 1],
        [1, 'open', 'proof-only', 'bad key', 0, 0, true],
        [1, 'open', 'draining', 'bad-key', 0, 0, true],
        [1, 'open', 'draining', null, 0, 0, true, 0],
      ].map((receipt) => ({
        last_transition_request: JSON.stringify(receipt),
      })),
      { last_transition_request: '[1, "open","draining",null,0,0,true]' },
      { state: 'open' },
      { proof_key: 'unexpected' },
      { proof_run_id: 'unexpected' },
      { mutation_epoch: 2 },
      { transition_revision: 2 },
      { require_mutation_epoch: 0 },
      { mutation_epoch: 0 },
      { transition_revision: 0 },
      { last_transition_request: null },
    ];
    const before = rawFence(sqlite);
    for (const corruption of corruptions) {
      const columns = Object.keys(corruption);
      sqlite
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE} SET ${columns.map((key) => `${key} = ?`).join(', ')}`,
        )
        .run(...Object.values(corruption));
      for (const action of [
        () => fence.read(),
        () => fence.seed('open'),
        () => fence.transition(activation),
      ]) {
        await expect(action()).rejects.toBeInstanceOf(
          ExecutionFenceUnreadableError,
        );
      }
      sqlite
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE} SET ${Object.keys(before)
            .map((key) => `${key} = ?`)
            .join(', ')}`,
        )
        .run(...Object.values(before));
    }
    for (const result of [
      null,
      {},
      { results: null },
      { results: [undefined] },
      { results: [before, before] },
      { results: [{ ...before, transition_revision: 2 }] },
    ]) {
      let reads = 0;
      const wrapped = new ExecutionFenceStore(
        interceptedDatabase(fenceFixture().db, async (sql, execute) => {
          if (sql.startsWith('UPDATE')) return result;
          if (sql.startsWith('SELECT *')) reads += 1;
          return execute();
        }),
      );
      await wrapped.seed('open');
      reads = 0;
      await expect(wrapped.transition(activation)).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
      expect(reads).toBe(3);
    }

    for (const success of [false, undefined, null, 0, 'true']) {
      for (const target of ['row', 'schema', 'transition', 'proof'] as const) {
        const { db, fence: authority } = fenceFixture();
        const admitted = await authority.transition({
          ...activation,
          next: 'proof-only',
          proofKey: 'key',
        });
        let updateReturned = false;
        let postUpdateReads = 0;
        const malformed = new ExecutionFenceStore(
          interceptedDatabase(db, async (sql, execute) => {
            const result = await execute();
            if (updateReturned && /^(SELECT|PRAGMA)/.test(sql))
              postUpdateReads += 1;
            const selected =
              target === 'row'
                ? sql.startsWith('SELECT *')
                : target === 'schema'
                  ? sql.startsWith('PRAGMA')
                  : sql.startsWith('UPDATE');
            if (!selected) return result;
            if (sql.startsWith('UPDATE')) updateReturned = true;
            return { ...(result as { results: unknown[] }), success };
          }),
        );
        const operation =
          target === 'transition'
            ? malformed.transition({
                expected: 'proof-only',
                next: 'open',
                expectedMutationEpoch: 1,
                expectedRevision: 1,
              })
            : target === 'proof'
              ? malformed.recordProofRun('key', 'run', admitted)
              : malformed.read();
        await expect(operation).rejects.toBeInstanceOf(
          ExecutionFenceUnreadableError,
        );
        expect(postUpdateReads).toBe(0);
      }
    }

    for (const [column, value] of [
      ['last_transition_request', null],
      ['transition_revision', '0'],
      ['mutation_epoch', true],
      ['require_mutation_epoch', false],
      ['require_mutation_epoch', '1'],
    ] as const) {
      const { db, fence: valid } = fenceFixture();
      await valid.transition(activation);
      const malformed = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          const result = await execute();
          if (!sql.startsWith('SELECT *')) return result;
          const rows = result as { results: Record<string, unknown>[] };
          return {
            results: rows.results.map((row) => ({ ...row, [column]: value })),
          };
        }),
      );
      await expect(malformed.read()).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
    }
  });

  it('rejects sparse result rows without uncertain-write recovery', async () => {
    for (const target of ['row', 'schema', 'transition', 'proof'] as const) {
      const { fence, db, sqlite } = fenceFixture();
      const admitted = await fence.transition({
        ...activation,
        next: 'proof-only',
        proofKey: 'key',
      });
      let malformedReturned = false;
      let subsequentReads = 0;
      const malformed = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (malformedReturned && /^(SELECT|PRAGMA)/.test(sql))
            subsequentReads += 1;
          const result = await execute();
          const selected =
            target === 'row'
              ? sql.startsWith('SELECT *')
              : target === 'schema'
                ? sql.startsWith('PRAGMA')
                : sql.startsWith('UPDATE');
          if (selected) {
            const rows = result as { results: unknown[] };
            expect(rows.results.length).toBeGreaterThan(0);
            delete rows.results[0];
            malformedReturned = true;
          }
          return result;
        }),
      );
      const operation =
        target === 'transition'
          ? malformed.transition({
              expected: 'proof-only',
              next: 'open',
              expectedMutationEpoch: 1,
              expectedRevision: 1,
            })
          : target === 'proof'
            ? malformed.recordProofRun('key', 'run', admitted)
            : malformed.read();
      await expect(operation).rejects.toBeInstanceOf(
        ExecutionFenceUnreadableError,
      );
      expect(subsequentReads).toBe(0);
      if (target === 'transition') expect(rawFence(sqlite).state).toBe('open');
      if (target === 'proof') expect(rawFence(sqlite).proof_run_id).toBe('run');
    }
  });

  it('rejects generated or hidden extra fence columns', async () => {
    const { fence, sqlite } = fenceFixture();
    await fence.seed('open');
    sqlite.exec(
      `ALTER TABLE ${EXECUTION_FENCE_TABLE} ADD COLUMN invisible TEXT GENERATED ALWAYS AS (state) VIRTUAL`,
    );
    expect(
      sqlite.prepare(`PRAGMA table_info(${EXECUTION_FENCE_TABLE})`).all(),
    ).toHaveLength(12);
    expect(
      sqlite.prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`).all(),
    ).toHaveLength(13);
    const schemaMessage = (reason: string) =>
      `${EXECUTION_FENCE_TABLE} has an invalid execution-fence schema (${reason})`;
    for (const action of [() => fence.read(), () => fence.seed('open')]) {
      await expect(action()).rejects.toMatchObject({
        cause: {
          name: 'DeploymentIdentityError',
          message: schemaMessage('unexpected columns'),
        },
      });
    }
    for (const [schema, diagnostic] of [
      [
        EXECUTION_FENCE_DDL.replace(
          'last_transition_request TEXT',
          'last_transition_request INTEGER',
        ),
        'column last_transition_request differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'last_transition_request TEXT',
          'last_transition_request TEXT DEFAULT NULL',
        ),
        'column last_transition_request differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'transition_revision INTEGER NOT NULL DEFAULT 0',
          "transition_revision INTEGER NOT NULL DEFAULT '0'",
        ),
        'column transition_revision differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'proof_key TEXT,\n    proof_run_id TEXT',
          'proof_run_id TEXT,\n    proof_key TEXT',
        ),
        'column proof_key differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'last_transition_request TEXT',
          'other_receipt TEXT',
        ),
        'column last_transition_request differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'proof_key TEXT,',
          'proof_key TEXT GENERATED ALWAYS AS (state) VIRTUAL,',
        ),
        'column proof_key differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'proof_table_prefix TEXT',
          'proof_table_prefix INTEGER',
        ),
        'column proof_table_prefix differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          'proof_start_token TEXT',
          'proof_start_token TEXT DEFAULT NULL',
        ),
        'column proof_start_token differs',
      ],
      [
        EXECUTION_FENCE_DDL.replace(
          /\n {2}\)$/,
          ',\n    future_column TEXT\n  )',
        ),
        'unexpected columns',
      ],
    ] as const) {
      const malformed = fenceFixture();
      malformed.sqlite.exec(schema);
      malformed.sqlite.exec(
        `INSERT INTO ${EXECUTION_FENCE_TABLE} (id, state, updated_at) VALUES ('deployment', 'open', 19)`,
      );
      const before = rawFence(malformed.sqlite);
      const writes: string[] = [];
      const observed = new ExecutionFenceStore(
        interceptedDatabase(malformed.db, async (sql, execute) => {
          if (/^(CREATE|INSERT|UPDATE|ALTER)/.test(sql)) writes.push(sql);
          return execute();
        }),
      );
      for (const action of [
        () => observed.read(),
        () => observed.seed('open'),
      ]) {
        await expect(action()).rejects.toMatchObject({
          cause: {
            name: 'DeploymentIdentityError',
            message: schemaMessage(diagnostic),
          },
        });
      }
      expect(writes).toEqual([]);
      expect(rawFence(malformed.sqlite)).toEqual(before);
    }
  });

  it('distinguishes proof misses from missing or malformed modern state', async () => {
    for (const outcome of [
      'missing',
      'malformed',
      'changed',
      'competing-bind',
    ] as const) {
      const { fence, sqlite, db } = fenceFixture();
      const admitted = await fence.transition({
        ...activation,
        next: 'proof-only',
        proofKey: 'key',
      });
      const raced = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (!sql.startsWith('UPDATE')) return execute();
          if (outcome === 'missing')
            sqlite.exec(`DELETE FROM ${EXECUTION_FENCE_TABLE}`);
          else if (outcome === 'malformed')
            sqlite.exec(
              `UPDATE ${EXECUTION_FENCE_TABLE} SET last_transition_request = 'broken', proof_key = 'different-key'`,
            );
          else if (outcome === 'changed')
            await fence.transition({
              expected: 'proof-only',
              next: 'open',
              expectedMutationEpoch: 1,
              expectedRevision: 1,
            });
          else await fence.recordProofRun('key', 'other-run', admitted);
          const result = await execute();
          if (outcome === 'competing-bind') {
            sqlite.exec(
              `UPDATE ${EXECUTION_FENCE_TABLE} SET proof_run_id = NULL`,
            );
            await fence.recordProofRun('key', 'run', admitted);
          }
          return result;
        }),
      );
      if (outcome === 'missing' || outcome === 'malformed')
        await expect(
          raced.recordProofRun('key', 'run', admitted),
        ).rejects.toBeInstanceOf(ExecutionFenceUnreadableError);
      else
        expect(await raced.recordProofRun('key', 'run', admitted)).toBe(false);
    }
  });

  it('preserves write uncertainty when readback cannot prove the exact command', async () => {
    for (const outcome of [
      'before',
      'readback-fails',
      'intervening',
    ] as const) {
      const { db, fence } = fenceFixture();
      await fence.seed('open');
      const failure = new Error('lost write response');
      let wrote = false;
      const uncertain = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          if (sql.startsWith('UPDATE')) {
            wrote = true;
            if (outcome !== 'before') await execute();
            if (outcome === 'intervening')
              await fence.transition({
                expected: 'draining',
                next: 'open',
                expectedMutationEpoch: 1,
                expectedRevision: 1,
              });
            throw failure;
          }
          if (wrote && outcome === 'readback-fails')
            throw new Error('readback failed');
          return execute();
        }),
      );
      const error = await uncertain
        .transition(activation)
        .catch((error: unknown) => error);
      expect(error).toBeInstanceOf(ExecutionFenceUnreadableError);
      expect((error as Error).cause).toBe(failure);
    }
    for (const versioned of [false, true]) {
      const { fence, db, sqlite } = fenceFixture();
      const admitted = await fence.transition({
        ...activation,
        next: 'proof-only',
        proofKey: 'key',
        advanceMutationEpoch: versioned,
      });
      const lost = new ExecutionFenceStore(
        interceptedDatabase(db, async (sql, execute) => {
          const result = await execute();
          if (sql.startsWith('UPDATE')) throw new Error('lost proof response');
          return result;
        }),
      );
      expect(
        await lost.recordProofRun(
          'key',
          'run',
          versioned ? admitted : undefined,
        ),
      ).toBe(true);
      expect(rawFence(sqlite).proof_run_id).toBe('run');
      expect(rawFence(sqlite).transition_revision).toBe(1);
    }
  });

  it('runs all persistence operations on a database without batch', async () => {
    const { db } = fenceFixture();
    const fence = new ExecutionFenceStore(
      interceptedDatabase(db, async (_sql, execute) => {
        const result = await execute();
        if (result && typeof result === 'object' && 'results' in result)
          return { results: result.results };
        return result;
      }),
    );
    expect('batch' in db).toBe(false);
    await fence.seed('proof-only');
    const admitted = await fence.transition({
      expected: 'proof-only',
      next: 'proof-only',
      proofKey: 'key',
      expectedMutationEpoch: 0,
      expectedRevision: 0,
    });
    expect(await fence.recordProofRun('key', 'run', admitted)).toBe(true);
    await expect(fence.read()).resolves.toEqual({
      ...admitted,
      proofRunId: 'run',
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
  it('maps an invalid fence request to 400 with its reason code', async () => {
    const error = new InvalidExecutionFenceRequestError('state is invalid');
    const response = doErrorResponse(error);

    expect(error.status).toBe(400);
    expect(error.reason.code).toBe('INVALID_EXECUTION_FENCE_REQUEST');
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'state is invalid',
      reason: { code: 'INVALID_EXECUTION_FENCE_REQUEST' },
    });
  });

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
      ...optionalMetadata,
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
      { startIdempotency: 'none', executionFence: 'none' },
    );

    expect(runtime.executionFence).toBeUndefined();
  });

  it('takes a shared store for a { storage } source', async () => {
    const { fence } = fenceFixture();
    const { runtime } = init(
      { storage: new InMemoryStore() },
      { startIdempotency: 'none', executionFence: fence },
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
    { startIdempotency: 'none', executionFence: fence },
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
  it('keeps current Runtime provenance and string-only fence behavior in this prerequisite', async () => {
    const { sqlite, db } = fenceFixture();
    const {
      createStep,
      createWorkflow,
      runtime,
      executionFence,
      startIdempotency,
    } = init({ DB: db });
    await executionFence?.seed('open');
    const step = createStep({
      id: 'finish',
      inputSchema: z.object({}),
      outputSchema: z.object({ done: z.boolean() }),
      execute: async () => ({ done: true }),
    });
    createWorkflow({
      id: 'unchanged',
      inputSchema: z.object({}),
      outputSchema: z.object({ done: z.boolean() }),
    })
      .then(step)
      .commit();
    await startIdempotency?.reserve({
      key: 'key',
      owner: { kind: 'human', id: 'owner' },
      targetKind: 'workflow',
      targetId: 'unchanged',
      mintRunId: () => 'run',
    });
    await startIdempotency?.claim('key', 'run');
    const result = await runtime.start('unchanged', {
      runId: 'run',
      inputData: {},
      requestedBy: 'owner',
      requestedByKind: 'human',
      idempotencyKey: 'key',
    });
    expect(result.status).toBe('success');
    const row = sqlite
      .prepare(
        'SELECT snapshot FROM mastra_workflow_snapshot WHERE workflow_name = ? AND run_id = ?',
      )
      .get('unchanged', 'run') as { snapshot: string };
    expect(
      JSON.parse(row.snapshot).requestContext['flowsafe.runProvenance'].version,
    ).toBe(1);
    expect(await startIdempotency?.read('key')).toMatchObject({
      state: 'terminal',
      binding: { kind: 'legacy' },
    });
    expect(
      admitsExistingRun(
        {
          state: 'proof-only',
          proofRunId: 'run',
          proofExecution: {
            tablePrefix: 'other_',
            workflowId: 'other',
            runId: 'run',
            startToken: 'generation',
          },
        },
        'run',
      ),
    ).toBe(true);
  });

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
      ...optionalMetadata,
      transitionRevision: 1,
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
      { startIdempotency: 'none', executionFence: fence },
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
    await expect(fence.read()).resolves.toEqual({
      state: 'draining',
      ...optionalMetadata,
      transitionRevision: 1,
    });
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
