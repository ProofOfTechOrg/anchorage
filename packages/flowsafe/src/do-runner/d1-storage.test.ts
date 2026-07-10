// purgeExpiredWorkflowRuns against REAL SQLite via node:sqlite (D1 is
// SQLite), so the json_extract status filter and the ISO-cutoff comparison
// run for real. The openSqlite() fixture matches the approval-api store
// tests; the d1Like adapter here maps run() results to D1's { meta } shape.

import { describe, expect, it } from 'vitest';

import { openSqlite, type SqliteDatabase } from '../../test-support/sqlite.js';
import {
  purgeExpiredWorkflowRuns,
  purgeTenant,
  type SnapshotDatabase,
  type SnapshotStatement,
} from './d1-storage.js';

// Faithful to D1: run() resolves to a result whose meta.changes carries the
// affected-row count (node:sqlite reports it as `changes`).
function d1Like(db: SqliteDatabase): SnapshotDatabase {
  function statement(sql: string, params: unknown[]): SnapshotStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      all: async <T>() => ({
        results: db.prepare(sql).all(...params) as T[],
      }),
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return { meta: { changes: Number(outcome.changes ?? 0) } };
      },
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

const NOW = Date.parse('2026-07-07T12:00:00.000Z');
const DAY_MS = 86_400_000;

// Column set per @mastra/core storage constants for mastra_workflow_snapshot
// (camelCase timestamps, snapshot serialized as JSON TEXT).
function createSnapshotTable(db: SqliteDatabase, prefix = ''): void {
  db.prepare(
    `CREATE TABLE ${prefix}mastra_workflow_snapshot (
      workflow_name TEXT NOT NULL,
      run_id TEXT NOT NULL,
      resourceId TEXT,
      snapshot TEXT NOT NULL,
      createdAt TEXT NOT NULL,
      updatedAt TEXT NOT NULL
    )`,
  ).run();
}

function seedRun(
  db: SqliteDatabase,
  options: {
    runId: string;
    status: string;
    updatedAt: number;
    prefix?: string;
  },
): void {
  const iso = new Date(options.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO ${options.prefix ?? ''}mastra_workflow_snapshot
     (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
     VALUES (?, ?, NULL, ?, ?, ?)`,
  ).run(
    'wf',
    options.runId,
    JSON.stringify({ status: options.status, runId: options.runId }),
    iso,
    iso,
  );
}

function remainingRunIds(db: SqliteDatabase, prefix = ''): string[] {
  const rows = (
    db.prepare(
      `SELECT run_id FROM ${prefix}mastra_workflow_snapshot ORDER BY run_id`,
    ) as unknown as { all(): Array<{ run_id: string }> }
  ).all();
  return rows.map((row) => row.run_id);
}

const TERMINAL = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
];
const LIVE = ['running', 'suspended', 'waiting', 'pending', 'paused'];

describe('purgeExpiredWorkflowRuns', () => {
  it('deletes only stale TERMINAL runs and returns the count', async () => {
    // #given — every terminal status seeded fresh AND stale, every live
    // status seeded stale
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const status of TERMINAL) {
      seedRun(sqlite, {
        runId: `stale-${status}`,
        status,
        updatedAt: NOW - 8 * DAY_MS,
      });
      seedRun(sqlite, {
        runId: `fresh-${status}`,
        status,
        updatedAt: NOW - 1 * DAY_MS,
      });
    }
    for (const status of LIVE) {
      seedRun(sqlite, {
        runId: `stale-${status}`,
        status,
        updatedAt: NOW - 30 * DAY_MS,
      });
    }

    // #when — 7-day TTL
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — exactly the six stale terminal rows are gone; fresh terminal
    // rows and ALL live rows (however old — a stale suspended run is a
    // pending approval, not garbage) survive
    expect(deleted).toBe(TERMINAL.length);
    expect(remainingRunIds(sqlite)).toEqual(
      [
        ...TERMINAL.map((status) => `fresh-${status}`),
        ...LIVE.map((status) => `stale-${status}`),
      ].sort(),
    );
  });

  it('returns 0 when nothing qualifies', async () => {
    // #given
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'r1',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });

    // #when / #then
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
  });

  it('respects the table prefix', async () => {
    // #given — two tables in one database, only the prefixed one targeted
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createSnapshotTable(sqlite, 'flowsafe_');
    seedRun(sqlite, {
      runId: 'unprefixed',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'prefixed',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
      prefix: 'flowsafe_',
    });

    // #when
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      tablePrefix: 'flowsafe_',
      now: () => NOW,
    });

    // #then
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['unprefixed']);
    expect(remainingRunIds(sqlite, 'flowsafe_')).toEqual([]);
  });

  it('skips malformed snapshot rows instead of aborting the purge', async () => {
    // #given — a corrupt (non-JSON) snapshot beside a valid stale terminal
    // row and a valid stale live row
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-ok',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'stale-live',
      status: 'suspended',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const corruptIso = new Date(NOW - 9 * DAY_MS).toISOString();
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
         (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES ('wf', 'corrupt', NULL, 'not-json{oops', ?, ?)`,
      )
      .run(corruptIso, corruptIso);

    // #when — one corrupt row must not abort reclaiming the valid ones
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — the valid stale terminal row is gone; the corrupt row (not
    // provably terminal — fail safe) and the live row survive
    expect(deleted).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual(['corrupt', 'stale-live']);
  });

  it('treats the TTL boundary exclusively: exactly-at-cutoff rows survive', async () => {
    // #given — a run whose updatedAt equals the cutoff instant
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'boundary',
      status: 'success',
      updatedAt: NOW - 7 * DAY_MS,
    });

    // #when / #then — strict < : the boundary row is not yet expired
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
  });

  it('treats a MISSING snapshot table as zero purgeable runs (Mastra creates it lazily)', async () => {
    // #given — a database where no run ever persisted, so Mastra's lazy
    // CREATE TABLE never happened
    const sqlite = openSqlite();

    // #when / #then — the purge cron must not fail until some unrelated run
    // initializes the schema
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
      }),
    ).toBe(0);
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore: {
          deleteRun: async () => {
            throw new Error('must not be called without a snapshot table');
          },
        },
      }),
    ).toBe(0);
  });

  it("pairs each purged run's artifact deletion with its snapshot row when artifactStore is wired", async () => {
    // #given — a stale terminal run beside a fresh terminal and a stale live
    // one; only the first is eligible
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-done',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'fresh-done',
      status: 'success',
      updatedAt: NOW - 1 * DAY_MS,
    });
    seedRun(sqlite, {
      runId: 'stale-open',
      status: 'suspended',
      updatedAt: NOW - 30 * DAY_MS,
    });
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (workflowId: string, runId: string) => {
        deletedArtifacts.push(`${workflowId}/${runId}`);
        return 2;
      },
    };

    // #when
    const deleted = await purgeExpiredWorkflowRuns(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      artifactStore,
    });

    // #then — exactly the purged run's artifacts went with its row; the
    // survivors keep theirs (purgeTenant can still enumerate them later)
    expect(deleted).toBe(1);
    expect(deletedArtifacts).toEqual(['wf/stale-done']);
    expect(remainingRunIds(sqlite)).toEqual(['fresh-done', 'stale-open']);
  });

  it('LIMIT-batches the artifact-paired path; the shrinking eligible set is the cursor', async () => {
    // #given — three stale terminal runs, batch size 2 (the subrequest-
    // budget guard: an unbounded first backlog would blow the Workers
    // per-invocation cap)
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const runId of ['stale-a', 'stale-b', 'stale-c']) {
      seedRun(sqlite, {
        runId,
        status: 'success',
        updatedAt: NOW - 8 * DAY_MS,
      });
    }
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (_workflowId: string, runId: string) => {
        deletedArtifacts.push(runId);
        return 1;
      },
    };
    const options = {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
      artifactStore,
      limit: 2,
    };

    // #when — two passes
    const first = await purgeExpiredWorkflowRuns(d1Like(sqlite), options);
    const second = await purgeExpiredWorkflowRuns(d1Like(sqlite), options);

    // #then — the batches advance without a cursor row and stay paired
    expect(first).toBe(2);
    expect(second).toBe(1);
    expect(deletedArtifacts.sort()).toEqual(['stale-a', 'stale-b', 'stale-c']);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });

  it("a failing artifact delete leaves that run's snapshot row for the next sweep (artifacts-first ordering)", async () => {
    // #given — artifacts go BEFORE the row: if this order ever flips, a
    // crash between the two strands the artifacts forever (the row is their
    // only enumerable record)
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'stale-done',
      status: 'success',
      updatedAt: NOW - 8 * DAY_MS,
    });
    const artifactStore = {
      deleteRun: async () => {
        throw new Error('R2 unavailable');
      },
    };

    // #when / #then — the failure propagates (the cron logs it)...
    await expect(
      purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ttlMs: 7 * DAY_MS,
        now: () => NOW,
        artifactStore,
      }),
    ).rejects.toThrow('R2 unavailable');
    // ...and the row survives as the retry cursor
    expect(remainingRunIds(sqlite)).toEqual(['stale-done']);
  });

  it("one run's wedged artifact delete does not stall the eligible rows behind it", async () => {
    // #given — five stale terminal runs; only the middle one's deleteRun is
    // permanently broken. Without per-run isolation the loop aborts at the
    // same scan position EVERY firing and the runs behind it never purge.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (const runId of ['r1-ok', 'r2-ok', 'r3-bad', 'r4-ok', 'r5-ok']) {
      seedRun(sqlite, {
        runId,
        status: 'success',
        updatedAt: NOW - 8 * DAY_MS,
      });
    }
    const artifactStore = {
      deleteRun: async (_workflowId: string, runId: string) => {
        if (runId === 'r3-bad') throw new Error('permanently broken');
        return 1;
      },
    };
    const options = { ttlMs: 7 * DAY_MS, now: () => NOW, artifactStore };

    // #when / #then — the pass purges the other four, then reports the
    // failure (naming the run) so the cron's error surface still fires
    await expect(
      purgeExpiredWorkflowRuns(d1Like(sqlite), options),
    ).rejects.toThrow('wf/r3-bad: permanently broken');
    expect(remainingRunIds(sqlite)).toEqual(['r3-bad']);

    // #then — a later pass with the store healed reaps the survivor
    expect(
      await purgeExpiredWorkflowRuns(d1Like(sqlite), {
        ...options,
        artifactStore: { deleteRun: async () => 1 },
      }),
    ).toBe(1);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });
});

describe('purgeTenant (complete offboarding)', () => {
  function seedApprovalsTable(db: SqliteDatabase): void {
    db.prepare(
      `CREATE TABLE flowsafe_approvals (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        title TEXT NOT NULL
      )`,
    ).run();
    db.prepare(
      `INSERT INTO flowsafe_approvals (id, tenant_id, title)
       VALUES ('a1', 'abc', 'abc approval'),
              ('b1', 'abcdefg', 'neighbor approval')`,
    ).run();
  }

  it("reaps all three stores for the tenant and leaves the prefix-NEIGHBOR untouched ('abc' vs 'abcdefg')", async () => {
    // #given — snapshots of EVERY status for two tenants whose slugs share a
    // prefix (the range-exactness worst case), plus approvals and artifacts
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedApprovalsTable(sqlite);
    seedRun(sqlite, {
      runId: 'abc_r-terminal',
      status: 'success',
      updatedAt: NOW,
    });
    seedRun(sqlite, {
      runId: 'abc_r-suspended',
      status: 'suspended',
      updatedAt: NOW,
    });
    seedRun(sqlite, {
      runId: 'abcdefg_r1',
      status: 'suspended',
      updatedAt: NOW,
    });
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (workflowId: string, runId: string) => {
        deletedArtifacts.push(`${workflowId}/${runId}`);
        return 1;
      },
    };

    // #when
    const result = await purgeTenant(d1Like(sqlite), {
      tenantId: 'abc',
      artifactStore,
    });

    // #then — abc's rows (INCLUDING the suspended one the retention purge can
    // never reap) are gone from all three stores; abcdefg survives intact
    expect(result).toEqual({ snapshots: 2, approvals: 1, artifacts: 2 });
    expect(remainingRunIds(sqlite)).toEqual(['abcdefg_r1']);
    expect(deletedArtifacts.sort()).toEqual([
      'wf/abc_r-suspended',
      'wf/abc_r-terminal',
    ]);
    const approvals = (
      sqlite.prepare('SELECT tenant_id FROM flowsafe_approvals') as unknown as {
        all(): Array<{ tenant_id: string }>;
      }
    ).all();
    expect(approvals).toEqual([{ tenant_id: 'abcdefg' }]);
  });

  it('a run landing INSIDE the artifact-SELECT → snapshot-DELETE window is still deleted, and its artifacts are NOT enumerated', async () => {
    // The three statements are not one transaction, and the artifact deletes
    // are R2 calls no SQL transaction could span. Pin the resulting behavior
    // rather than pretend it away: the DELETE re-evaluates the range at
    // execution time, so a run started mid-purge IS reaped — but the SELECT
    // feeding artifact cleanup already ran, so that run's artifacts are never
    // enumerated. Only self-service reset (showcase /demo/reset) can reach
    // this, by racing its OWN tenant's start; the reaper purges expired
    // tenants, which have no live caller by construction.
    // #given — one pre-existing run; a second lands between the two statements
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedApprovalsTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'suspended', updatedAt: NOW });
    const real = d1Like(sqlite);
    let selected = false;
    const racing: SnapshotDatabase = {
      prepare: (sql: string) => {
        const statement = real.prepare(sql);
        if (!sql.includes('SELECT workflow_name')) return statement;
        return {
          ...statement,
          bind: (...values: unknown[]) => {
            const bound = statement.bind(...values);
            return {
              ...bound,
              all: async <T>() => {
                const out = await bound.all<T>();
                if (!selected) {
                  selected = true;
                  // a POST /runs for the same tenant commits right here
                  seedRun(sqlite, {
                    runId: 'abc_r2',
                    status: 'running',
                    updatedAt: NOW,
                  });
                }
                return out;
              },
            };
          },
        };
      },
    };
    const deletedArtifacts: string[] = [];
    const artifactStore = {
      deleteRun: async (workflowId: string, runId: string) => {
        deletedArtifacts.push(`${workflowId}/${runId}`);
        return 1;
      },
    };

    // #when
    const result = await purgeTenant(racing, {
      tenantId: 'abc',
      artifactStore,
    });

    // #then — both rows are gone (the DELETE re-reads the range), but only the
    // run visible to the SELECT had its artifacts cleaned
    expect(result.snapshots).toBe(2);
    expect(remainingRunIds(sqlite)).toEqual([]);
    expect(deletedArtifacts).toEqual(['wf/abc_r1']);
  });

  it('a failing approvals DELETE leaves the purge PARTIALLY applied — the snapshots are already gone', async () => {
    // The three statements are not one transaction, so a throw at the last one
    // cannot roll the first ones back. A caller that surfaces the rejection
    // (the /demo/reset route 500s) must therefore treat its local view as
    // stale, not as "nothing happened". Pinned so nobody reads the sequence as
    // atomic.
    // #given — a tenant with a snapshot and an approvals table that throws
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedApprovalsTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'suspended', updatedAt: NOW });
    const real = d1Like(sqlite);
    const failing: SnapshotDatabase = {
      prepare: (sql: string) => {
        const statement = real.prepare(sql);
        if (!sql.includes('DELETE FROM flowsafe_approvals')) return statement;
        return {
          ...statement,
          bind: () => ({
            ...statement,
            run: async () => {
              throw new Error('D1_ERROR: network');
            },
          }),
        };
      },
    };

    // #when / #then — the failure propagates (never silently swallowed)
    await expect(purgeTenant(failing, { tenantId: 'abc' })).rejects.toThrow(
      /D1_ERROR/,
    );

    // #then — the snapshot DELETE already committed: the purge is half-applied
    expect(remainingRunIds(sqlite)).toEqual([]);
    const approvals = (
      sqlite.prepare('SELECT tenant_id FROM flowsafe_approvals') as unknown as {
        all(): Array<{ tenant_id: string }>;
      }
    ).all();
    expect(approvals).toEqual([{ tenant_id: 'abc' }, { tenant_id: 'abcdefg' }]);
  });

  it('tolerates a host without the approvals table (snapshot-only deployments)', async () => {
    // #given — no flowsafe_approvals table at all
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'running', updatedAt: NOW });

    // #when / #then
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });
    expect(result).toEqual({ snapshots: 1, approvals: 0, artifacts: 0 });
  });

  it('offboards a tenant that never started a run: a MISSING snapshot table reads as empty and approvals are still reaped', async () => {
    // #given — Mastra creates the snapshot table lazily with the first
    // persisted run, and an expired demo sandbox routinely dies without one;
    // the approvals table exists (the store factory creates it eagerly)
    const sqlite = openSqlite();
    seedApprovalsTable(sqlite);
    const artifactStore = {
      deleteRun: async () => {
        throw new Error('must not be called without a snapshot table');
      },
    };

    // #when — must not throw before the approval delete (a throw here wedged
    // BOTH the purge cron and re-auth for the sandbox's identity)
    const result = await purgeTenant(d1Like(sqlite), {
      tenantId: 'abc',
      artifactStore,
    });

    // #then — the tenant's approval rows are gone; the neighbor survives
    expect(result).toEqual({ snapshots: 0, approvals: 1, artifacts: 0 });
    const approvals = (
      sqlite.prepare('SELECT tenant_id FROM flowsafe_approvals') as unknown as {
        all(): Array<{ tenant_id: string }>;
      }
    ).all();
    expect(approvals).toEqual([{ tenant_id: 'abcdefg' }]);
  });

  it('offboards cleanly when NEITHER table exists yet (fresh deployment)', async () => {
    // #given — no snapshot table, no approvals table
    const sqlite = openSqlite();

    // #when / #then
    expect(await purgeTenant(d1Like(sqlite), { tenantId: 'abc' })).toEqual({
      snapshots: 0,
      approvals: 0,
      artifacts: 0,
    });
  });

  it.each(['Abc', 'a_b', 'ab', "abc'; DROP TABLE x; --", ''])(
    "rejects the non-INV-3 tenantId '%s' BEFORE any interpolation (range-exactness, not injection)",
    async (tenantId) => {
      // #given
      const sqlite = openSqlite();
      createSnapshotTable(sqlite);

      // #when / #then
      await expect(purgeTenant(d1Like(sqlite), { tenantId })).rejects.toThrow(
        /INV-3/,
      );
    },
  );

  it('respects the table prefix', async () => {
    // #given
    const sqlite = openSqlite();
    createSnapshotTable(sqlite, 'p_');
    seedRun(sqlite, {
      runId: 'abc_r1',
      status: 'suspended',
      updatedAt: NOW,
      prefix: 'p_',
    });

    // #when
    const result = await purgeTenant(d1Like(sqlite), {
      tenantId: 'abc',
      tablePrefix: 'p_',
    });

    // #then
    expect(result.snapshots).toBe(1);
    expect(remainingRunIds(sqlite, 'p_')).toEqual([]);
  });
});

describe('purgeExpiredWorkflowRuns row-only batching', () => {
  it('LIMIT-batches the bulk path: one firing reclaims at most `limit` rows; the next resumes at the survivors', async () => {
    // #given — more expired terminal rows than one batch
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    for (let index = 0; index < 5; index += 1) {
      seedRun(sqlite, {
        runId: `stale-${index}`,
        status: 'success',
        updatedAt: NOW - 40 * DAY_MS,
      });
    }
    const db = d1Like(sqlite);

    // #when — two firings at limit 3
    const first = await purgeExpiredWorkflowRuns(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
    const survivors = remainingRunIds(sqlite).length;
    const second = await purgeExpiredWorkflowRuns(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });

    // #then — the shrinking eligible set is the cursor across firings
    expect(first).toBe(3);
    expect(survivors).toBe(2);
    expect(second).toBe(2);
    expect(remainingRunIds(sqlite)).toEqual([]);
  });
});

describe('ensureSnapshotRunIdIndex memoization (via purgeTenant)', () => {
  function countingDb(inner: SnapshotDatabase): {
    db: SnapshotDatabase;
    indexStatements: () => number;
  } {
    let count = 0;
    return {
      db: {
        prepare(sql: string) {
          if (sql.includes('CREATE INDEX')) count += 1;
          return inner.prepare(sql);
        },
      },
      indexStatements: () => count,
    };
  }

  it('runs CREATE INDEX once per database binding — the reaper loop pays no per-tenant DDL', async () => {
    // #given — a snapshot table and a DDL-counting wrapper over ONE binding
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, { runId: 'aaa111_r1', status: 'success', updatedAt: NOW });
    const { db, indexStatements } = countingDb(d1Like(sqlite));

    // #when — three offboardings, the cron reaper's loop shape
    await purgeTenant(db, { tenantId: 'aaa111' });
    await purgeTenant(db, { tenantId: 'bbb222' });
    await purgeTenant(db, { tenantId: 'ccc333' });

    // #then — one DDL round-trip, not one per tenant
    expect(indexStatements()).toBe(1);
  });

  it('memoizes success only: a missing snapshot table re-probes per call and recovers once the table exists', async () => {
    // #given — no snapshot table yet (a run-less deployment)
    const sqlite = openSqlite();
    const { db, indexStatements } = countingDb(d1Like(sqlite));

    // #when — offboarding tolerates the missing table as empty
    const first = await purgeTenant(db, { tenantId: 'aaa111' });
    expect(first.snapshots).toBe(0);
    expect(indexStatements()).toBe(1);

    // the table appears (a first run persisted); the next offboarding must
    // probe again rather than trust a memoized failure
    createSnapshotTable(sqlite);
    seedRun(sqlite, {
      runId: 'aaa111_r1',
      status: 'suspended',
      updatedAt: NOW,
    });
    const second = await purgeTenant(db, { tenantId: 'aaa111' });

    // #then — retried (2 DDL statements total) and the rows were reaped
    expect(indexStatements()).toBe(2);
    expect(second.snapshots).toBe(1);
  });
});
