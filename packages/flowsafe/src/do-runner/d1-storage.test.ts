// SPDX-License-Identifier: Apache-2.0
// purgeExpiredWorkflowRuns against REAL SQLite via node:sqlite (D1 is
// SQLite), so the json_extract status filter and the ISO-cutoff comparison
// run for real. The openSqlite() fixture matches the approval-api store
// tests; the d1Like adapter here maps run() results to D1's { meta } shape.

import { describe, expect, it } from 'vitest';

import {
  d1DatabaseLike,
  openSqlite,
  type SqliteDatabase,
} from '../../test-support/sqlite.js';
import {
  D1SchedulesStorage,
  type ScheduleDatabase,
} from '../schedules/schedules-d1.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import { D1NotificationsStorage } from '../signals/notifications-d1.js';
import { D1ThreadStateStorage } from '../signals/thread-state-d1.js';
import {
  purgeExpiredBackgroundTasks,
  purgeExpiredNotifications,
  purgeExpiredScheduleTriggers,
  purgeExpiredThreadState,
  purgeExpiredThreads,
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

  // Track E: the flowsafe-owned signal-subscription table purgeTenant reaps by
  // its `tenant_id` column (the flowsafe_approvals leg, not the salted range).
  function seedSubscriptionsTable(db: SqliteDatabase): void {
    db.prepare(
      `CREATE TABLE flowsafe_signal_subscriptions (
        id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        provider_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        resource_id TEXT NOT NULL,
        external_resource_id TEXT NOT NULL,
        subscribed_at TEXT NOT NULL,
        metadata TEXT
      )`,
    ).run();
    db.prepare(
      `INSERT INTO flowsafe_signal_subscriptions
         (id, tenant_id, provider_id, thread_id, resource_id, external_resource_id, subscribed_at, metadata)
       VALUES
         ('s1', 'abc', 'github', 'abc_t1', 'abc_o', 'github:x', '2026-01-01T00:00:00Z', NULL),
         ('s2', 'abcdefg', 'github', 'abcdefg_t1', 'abcdefg_o', 'github:y', '2026-01-01T00:00:00Z', NULL)`,
    ).run();
  }

  // The three agent-memory tables purgeTenant range-DELETEs in PARALLEL
  // (Promise.all). Column names mirror the real @mastra/cloudflare-d1 schema
  // mastra-schema-guard.test.ts pins: messages carry a NOT-NULL salted
  // `thread_id` (their own `id` is unsalted by design), threads/resources key
  // on a salted `id`.
  function createMemoryTables(db: SqliteDatabase): void {
    db.prepare(
      `CREATE TABLE mastra_messages (
        id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        resourceId TEXT
      )`,
    ).run();
    db.prepare(
      `CREATE TABLE mastra_threads (
        id TEXT PRIMARY KEY,
        resourceId TEXT
      )`,
    ).run();
    db.prepare('CREATE TABLE mastra_resources (id TEXT PRIMARY KEY)').run();
  }

  function seedMessage(db: SqliteDatabase, id: string, threadId: string): void {
    db.prepare(
      'INSERT INTO mastra_messages (id, thread_id, resourceId) VALUES (?, ?, NULL)',
    ).run(id, threadId);
  }

  function seedThread(db: SqliteDatabase, id: string): void {
    db.prepare(
      'INSERT INTO mastra_threads (id, resourceId) VALUES (?, NULL)',
    ).run(id);
  }

  function seedResource(db: SqliteDatabase, id: string): void {
    db.prepare('INSERT INTO mastra_resources (id) VALUES (?)').run(id);
  }

  function columnValues(
    db: SqliteDatabase,
    table: string,
    column: string,
  ): string[] {
    const rows = (
      db.prepare(
        `SELECT ${column} AS value FROM ${table} ORDER BY ${column}`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    return rows.map((row) => row.value);
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
    // Digit-suffixed neighbor: '5' (0x35) sorts BELOW '_' (0x5F), so this
    // row is inside the broken range if the lower bound ever loses its
    // trailing underscore ('abc' instead of 'abc_') — the letter neighbor
    // above cannot catch that mutant (excluded by the upper bound alone).
    seedRun(sqlite, {
      runId: 'abc5_r1',
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
    // never reap) are gone from all three stores; both neighbors survive
    expect(result).toEqual({
      snapshots: 2,
      threads: 0,
      messages: 0,
      resources: 0,
      backgroundTasks: 0,
      notifications: 0,
      threadState: 0,
      schedules: 0,
      scheduleTriggers: 0,
      approvals: 1,
      subscriptions: 0,
      artifacts: 2,
    });
    expect(remainingRunIds(sqlite)).toEqual(['abc5_r1', 'abcdefg_r1']);
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

  it('reaps Track E signal subscriptions by tenant_id and leaves the neighbor tenant intact', async () => {
    // #given — two tenants' subscriptions in the flowsafe-owned table
    const sqlite = openSqlite();
    seedSubscriptionsTable(sqlite);

    // #when
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — only abc's row reaped (exact tenant_id match); abcdefg survives
    expect(result.subscriptions).toBe(1);
    const rows = (
      sqlite.prepare(
        'SELECT tenant_id FROM flowsafe_signal_subscriptions',
      ) as unknown as { all(): Array<{ tenant_id: string }> }
    ).all();
    expect(rows).toEqual([{ tenant_id: 'abcdefg' }]);
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
    expect(result).toEqual({
      snapshots: 1,
      threads: 0,
      messages: 0,
      resources: 0,
      backgroundTasks: 0,
      notifications: 0,
      threadState: 0,
      schedules: 0,
      scheduleTriggers: 0,
      approvals: 0,
      subscriptions: 0,
      artifacts: 0,
    });
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
    expect(result).toEqual({
      snapshots: 0,
      threads: 0,
      messages: 0,
      resources: 0,
      backgroundTasks: 0,
      notifications: 0,
      threadState: 0,
      schedules: 0,
      scheduleTriggers: 0,
      approvals: 1,
      subscriptions: 0,
      artifacts: 0,
    });
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
      threads: 0,
      messages: 0,
      resources: 0,
      backgroundTasks: 0,
      notifications: 0,
      threadState: 0,
      schedules: 0,
      scheduleTriggers: 0,
      approvals: 0,
      subscriptions: 0,
      artifacts: 0,
    });
  });

  it('reads missing agent-memory tables as empty (crafted DBs, non-D1Store hosts)', async () => {
    // #given — a snapshot table but none of the three memory tables
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'success', updatedAt: NOW });

    // #when
    const purged = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — snapshots reaped; the memory sweep absorbed the missing
    // tables as zero rows instead of wedging the offboarding
    expect(purged.snapshots).toBe(1);
    expect(purged.threads).toBe(0);
    expect(purged.messages).toBe(0);
    expect(purged.resources).toBe(0);
  });

  it('reaps REAL rows from all three memory tables under Promise.all, each range-exact against a prefix neighbor', async () => {
    // #given — the three memory tables carrying target-tenant rows (salted
    // ids: threads/resources by a salted `id`, messages by a salted
    // `thread_id`) beside prefix-neighbor rows that straddle BOTH range bounds
    // — 'abc5_…' ('5' 0x35 < '_' 0x5F, below the lower bound) and 'abcdefg_…'
    // ('d' 0x64 > backtick 0x60, above the upper bound). Distinct per-table
    // counts (messages 3 / threads 2 / resources 1) prove each CONCURRENT
    // DELETE reports its OWN range's changes, not a shared or clobbered count.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    createMemoryTables(sqlite);
    for (const id of ['abc-m1', 'abc-m2', 'abc-m3']) {
      seedMessage(sqlite, id, 'abc_thread-1');
    }
    seedMessage(sqlite, 'abc5-m1', 'abc5_thread-9');
    seedMessage(sqlite, 'abcdefg-m1', 'abcdefg_thread-9');
    for (const id of ['abc_thread-1', 'abc_thread-2']) {
      seedThread(sqlite, id);
    }
    seedThread(sqlite, 'abc5_thread-9');
    seedThread(sqlite, 'abcdefg_thread-9');
    seedResource(sqlite, 'abc_user-1');
    seedResource(sqlite, 'abc5_user-9');
    seedResource(sqlite, 'abcdefg_user-9');

    // #given (sanity) — the target rows are really present, so the concurrent
    // deletes below are non-vacuous (unlike the missing-table pins this
    // supplements, which take the isMissingTable branch and delete nothing)
    expect(columnValues(sqlite, 'mastra_messages', 'thread_id').length).toBe(5);
    expect(columnValues(sqlite, 'mastra_threads', 'id').length).toBe(4);
    expect(columnValues(sqlite, 'mastra_resources', 'id').length).toBe(3);

    // #when
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — each concurrent DELETE reaped exactly its tenant's rows
    expect(result.messages).toBe(3);
    expect(result.threads).toBe(2);
    expect(result.resources).toBe(1);

    // #then — both prefix neighbors survive in EVERY table: the parallelized
    // deletes stayed range-exact and never clobbered a neighbor
    expect(columnValues(sqlite, 'mastra_messages', 'thread_id')).toEqual([
      'abc5_thread-9',
      'abcdefg_thread-9',
    ]);
    expect(columnValues(sqlite, 'mastra_threads', 'id')).toEqual([
      'abc5_thread-9',
      'abcdefg_thread-9',
    ]);
    expect(columnValues(sqlite, 'mastra_resources', 'id')).toEqual([
      'abc5_user-9',
      'abcdefg_user-9',
    ]);
  });

  it('a genuine (non-missing-table) error in ONE memory DELETE rejects the whole purge — the missing-table tolerance never masks it', async () => {
    // #given — all three memory tables with real rows (every leg has work to
    // do), then a genuine D1_ERROR injected into exactly ONE leg's DELETE via
    // the SAME seam the approvals-failure pin above uses. A D1_ERROR is NOT
    // 'no such table', so the per-statement isMissingTable catch must RE-THROW
    // it rather than swallow it — even though it fires inside the Promise.all.
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'suspended', updatedAt: NOW });
    createMemoryTables(sqlite);
    seedMessage(sqlite, 'abc-m1', 'abc_thread-1');
    seedThread(sqlite, 'abc_thread-1');
    seedResource(sqlite, 'abc_user-1');
    const real = d1Like(sqlite);
    const failing: SnapshotDatabase = {
      prepare: (sql: string) => {
        const statement = real.prepare(sql);
        if (!sql.includes('DELETE FROM mastra_threads')) return statement;
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

    // #when / #then — the first real error propagates out of Promise.all; the
    // sibling legs' missing-table tolerance does not absorb it
    await expect(purgeTenant(failing, { tenantId: 'abc' })).rejects.toThrow(
      /D1_ERROR/,
    );
  });

  it.each([
    'Abc',
    'a_b',
    'ab',
    "abc'; DROP TABLE x; --",
    '',
  ])("rejects the non-INV-3 tenantId '%s' BEFORE any interpolation (range-exactness, not injection)", async (tenantId) => {
    // #given
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);

    // #when / #then
    await expect(purgeTenant(d1Like(sqlite), { tenantId })).rejects.toThrow(
      /INV-3/,
    );
  });

  it.each<[string, unknown]>([
    ['undefined', undefined],
    ['null', null],
    // String(['acme']) === 'acme', so a bare TENANT_ID_PATTERN.test(['acme'])
    // coerces to a valid slug and would build the range bounds from 'acme',
    // purging that tenant; the typeof guard refuses it.
    ['an array coercing to a valid-looking slug', ['acme']],
  ])('rejects a non-string tenantId (%s) BEFORE building the range bounds (red-first: today the bare .test coerces it)', async (_label, tenantId) => {
    // #given — purgeTenant is EXPORTED and reached from demo-reset/purge-cron
    // with a post-resolver tenant.tenantId. RegExp.test would coerce
    // undefined -> 'undefined' and build the range bounds from that literal,
    // purging an unintended range; the typeof guard refuses it (DL-002).
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);

    // #when / #then
    await expect(
      purgeTenant(d1Like(sqlite), {
        tenantId: tenantId as unknown as string,
      }),
    ).rejects.toThrow(/INV-3/);
  });

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

describe('purgeExpiredThreads (agent-memory thread TTL)', () => {
  // The mastra_threads/mastra_messages columns mastra-schema-guard.test.ts pins
  // against the real @mastra/cloudflare-d1 schema: TIMESTAMP columns hold
  // ISO-8601 TEXT, messages carry a NOT-NULL thread_id and NO updatedAt of
  // their own (why they can only be reached through their thread).
  function createThreadTables(db: SqliteDatabase, prefix = ''): void {
    db.prepare(
      `CREATE TABLE ${prefix}mastra_threads (
        id TEXT PRIMARY KEY,
        resourceId TEXT,
        updatedAt TEXT NOT NULL
      )`,
    ).run();
    db.prepare(
      `CREATE TABLE ${prefix}mastra_messages (
        id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        createdAt TEXT NOT NULL
      )`,
    ).run();
  }

  function seedThreadAt(
    db: SqliteDatabase,
    id: string,
    updatedAt: number,
    messageIds: string[] = [],
  ): void {
    db.prepare(
      `INSERT INTO mastra_threads (id, resourceId, updatedAt) VALUES (?, NULL, ?)`,
    ).run(id, new Date(updatedAt).toISOString());
    for (const messageId of messageIds) {
      // A message is as old as its thread's last write unless a test says
      // otherwise — the real invariant, since saveMessages bumps updatedAt.
      db.prepare(
        'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
      ).run(messageId, id, new Date(updatedAt).toISOString());
    }
  }

  function idsIn(db: SqliteDatabase, table: string, column: string): string[] {
    const rows = (
      db.prepare(
        `SELECT ${column} AS value FROM ${table} ORDER BY ${column}`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    return rows.map((row) => row.value);
  }

  it('deletes idle threads WITH their messages and leaves active ones intact', async () => {
    // #given — one thread untouched past the TTL, one written to yesterday
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1', 'm2']);
    seedThreadAt(sqlite, 'abc_active', NOW - 1 * DAY_MS, ['m3']);

    // #when — a 30-day TTL
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the idle thread and BOTH its messages are gone; the active
    // conversation is untouched
    expect(purged).toEqual({ threads: 1, messages: 2 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_active']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m3']);
  });

  it('keys on updatedAt, not createdAt: an OLD thread still being written to never expires', async () => {
    // #given — the distinction that makes a TTL safe for conversations: age is
    // not idleness. A year-old thread answered this morning must survive.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_ancient-but-live', NOW - 60_000, ['m1']);

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 0, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([
      'abc_ancient-but-live',
    ]);
  });

  it('is exact at the TTL boundary (strictly older expires)', async () => {
    // #given — one thread exactly at the cutoff, one a millisecond past it
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_at-cutoff', NOW - 30 * DAY_MS);
    seedThreadAt(sqlite, 'abc_past-cutoff', NOW - 30 * DAY_MS - 1);

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — `<` cutoff: the boundary row survives
    expect(purged.threads).toBe(1);
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_at-cutoff']);
  });

  it('never touches mastra_resources — working memory is the OWNER’s, not the thread’s', async () => {
    // #given — a resource whose only thread ages out. The owner still exists;
    // their working memory outlives any one conversation and is reaped only at
    // offboarding (purgeTenant).
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    sqlite.prepare('CREATE TABLE mastra_resources (id TEXT PRIMARY KEY)').run();
    sqlite
      .prepare('INSERT INTO mastra_resources (id) VALUES (?)')
      .run('abc_user-1');
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS);

    // #when
    await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(idsIn(sqlite, 'mastra_resources', 'id')).toEqual(['abc_user-1']);
  });

  it('LIMIT-batches: one firing takes at most `limit`, the next resumes at the survivors', async () => {
    // #given — more idle threads than one batch
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    for (let index = 0; index < 5; index += 1) {
      seedThreadAt(sqlite, `abc_idle-${index}`, NOW - (40 + index) * DAY_MS, [
        `m${index}`,
      ]);
    }
    const db = d1Like(sqlite);

    // #when — two firings at limit 3
    const first = await purgeExpiredThreads(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });
    const second = await purgeExpiredThreads(db, {
      ttlMs: 30 * DAY_MS,
      limit: 3,
      now: () => NOW,
    });

    // #then — the shrinking eligible set is the cursor across firings
    expect(first).toEqual({ threads: 3, messages: 3 });
    expect(second).toEqual({ threads: 2, messages: 2 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([]);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([]);
  });

  it('deletes messages BEFORE the thread, so a crash between them leaves a retry cursor — never an orphan', async () => {
    // #given — the ordering is load-bearing: mastra_messages has no updatedAt,
    // so its rows are reachable only via their thread. Thread-first would put
    // them beyond every later firing of this purge. Simulate the crash by
    // failing the thread DELETE after the message DELETE has committed.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1']);
    const inner = d1Like(sqlite);
    const crashingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_threads')) {
          throw new Error('connection lost');
        }
        return inner.prepare(sql);
      },
    };

    // #when
    await expect(
      purgeExpiredThreads(crashingDb, { ttlMs: 30 * DAY_MS, now: () => NOW }),
    ).rejects.toThrow('connection lost');

    // #then — the thread survives as its own cursor (its messages are gone,
    // which is exactly recoverable)...
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([]);

    // ...and the next firing completes the job rather than wedging
    const retried = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });
    expect(retried).toEqual({ threads: 1, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual([]);
  });

  // The purge races ordinary traffic, not just crashes: @mastra/cloudflare-d1's
  // saveMessages issues its message insert and its `UPDATE mastra_threads SET
  // updatedAt` CONCURRENTLY (Promise.all), so a message arriving mid-purge is
  // exactly the interleaving below. Both tests inject that write through the
  // .prepare wrapper the purgeTenant race tests already use.
  function resurrectOn(
    sqlite: SqliteDatabase,
    trigger: (sql: string) => boolean,
  ): SnapshotDatabase {
    const inner = d1Like(sqlite);
    let fired = false;
    const send = (): void => {
      if (fired) return;
      fired = true;
      // A saveMessages-shaped write: the new message AND the thread's bump.
      sqlite
        .prepare(
          'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
        )
        .run('m-during-race', 'abc_idle', new Date(NOW).toISOString());
      sqlite
        .prepare('UPDATE mastra_threads SET updatedAt = ? WHERE id = ?')
        .run(new Date(NOW).toISOString(), 'abc_idle');
    };
    return {
      prepare: (sql: string) => {
        if (trigger(sql)) send();
        return inner.prepare(sql);
      },
    };
  }

  it('spares a thread resurrected between the SELECT and the message DELETE — history intact', async () => {
    // #given — an idle thread whose user sends a message just as the purge
    // picks it up. Keying the deletes on the SELECT's stale id list would reap
    // the conversation AND the message that just arrived.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);

    // #when — the write lands before either DELETE runs
    const purged = await purgeExpiredThreads(
      resurrectOn(sqlite, (sql) => sql.includes('DELETE FROM mastra_messages')),
      { ttlMs: 30 * DAY_MS, now: () => NOW },
    );

    // #then — the re-check excludes it from BOTH statements: nothing is lost
    expect(purged).toEqual({ threads: 0, messages: 0 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual([
      'm-during-race',
      'm-old',
    ]);
  });

  it('never orphans a message when the resurrection lands between the two DELETEs', async () => {
    // #given — the narrower window: the messages DELETE has already committed
    // when the write arrives. Without the thread DELETE's own re-check, the
    // thread row would go on stale membership and leave 'm-during-race'
    // pointing at a thread that no longer exists — reachable then only by
    // purgeTenant at offboarding, since nothing lists a vanished thread.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);

    // #when — the write lands as the thread DELETE is about to run
    const purged = await purgeExpiredThreads(
      resurrectOn(sqlite, (sql) => sql.includes('DELETE FROM mastra_threads')),
      { ttlMs: 30 * DAY_MS, now: () => NOW },
    );

    // #then — the thread survives with its new message; the expiring history
    // is gone (the accepted residual), and NO row is orphaned
    expect(purged).toEqual({ threads: 0, messages: 1 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-during-race']);
    const orphans = (
      sqlite.prepare(
        `SELECT m.id AS value FROM mastra_messages m
         LEFT JOIN mastra_threads t ON t.id = m.thread_id
         WHERE t.id IS NULL`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    expect(orphans).toEqual([]);
  });

  it('never destroys a just-sent message when the writer TEARS BEFORE the message DELETE', async () => {
    // #given — the same torn write as below, one statement earlier: the insert
    // half commits while the updatedAt bump is still in flight, and the purge's
    // message DELETE runs next. That DELETE's subquery keys on the THREAD's
    // staleness, which still reads idle — so an updatedAt-only guard sweeps the
    // message the user just sent into the same statement as the genuinely old
    // ones, and the now-empty thread follows it. No orphan, no trace: both
    // simply vanish. `createdAt` is the message's OWN evidence of recency and
    // the only guard that survives a torn write.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);
    const inner = d1Like(sqlite);
    let inserted = false;
    const tearingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_messages') && !inserted) {
          inserted = true;
          sqlite
            .prepare(
              'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
            )
            .run('m-just-sent', 'abc_idle', new Date(NOW).toISOString());
        }
        return inner.prepare(sql);
      },
    };

    // #when
    const purged = await purgeExpiredThreads(tearingDb, {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the expiring history goes, the just-sent message stays, and the
    // thread survives because a message still points at it
    expect(purged).toEqual({ threads: 0, messages: 1 });
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-just-sent']);
  });

  it('never orphans when the writer TEARS: the message lands but its updatedAt bump is still in flight', async () => {
    // #given — the writer is not atomic either. saveMessages issues its message
    // insert and its `UPDATE mastra_threads SET updatedAt` as two INDEPENDENT
    // calls under one Promise.all, so the insert can commit while the bump is
    // still in flight. Model exactly that: the message appears after the message
    // DELETE, but the thread STILL looks idle when the thread DELETE re-checks
    // updatedAt — so an updatedAt-only guard deletes the thread out from under
    // the message that just arrived. Only the NOT EXISTS catches this.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m-old']);
    const inner = d1Like(sqlite);
    let inserted = false;
    const tearingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_threads') && !inserted) {
          inserted = true;
          // The insert half commits; the updatedAt half has NOT landed.
          sqlite
            .prepare(
              'INSERT INTO mastra_messages (id, thread_id, createdAt) VALUES (?, ?, ?)',
            )
            .run('m-torn-write', 'abc_idle', new Date(NOW).toISOString());
        }
        return inner.prepare(sql);
      },
    };

    // #when
    const purged = await purgeExpiredThreads(tearingDb, {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then — the thread survives because a message points at it, even though
    // its updatedAt still reads idle. No orphan.
    expect(purged.threads).toBe(0);
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m-torn-write']);
    const orphans = (
      sqlite.prepare(
        `SELECT m.id AS value FROM mastra_messages m
         LEFT JOIN mastra_threads t ON t.id = m.thread_id
         WHERE t.id IS NULL`,
      ) as unknown as { all(): Array<{ value: string }> }
    ).all();
    expect(orphans).toEqual([]);
  });

  it('chunks the id lists under D1’s 100-bound-parameter ceiling', async () => {
    // #given — a batch larger than one bind chunk. An unchunked IN list would
    // be a D1 error at exactly the scale a first backlog produces, so the purge
    // would work in every test and fail in production.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    for (let index = 0; index < 120; index += 1) {
      seedThreadAt(sqlite, `abc_idle-${index}`, NOW - 40 * DAY_MS, [
        `m${index}`,
      ]);
    }
    const bindCounts: number[] = [];
    const inner = d1Like(sqlite);
    const countingDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        const statement = inner.prepare(sql);
        if (!sql.includes('DELETE')) return statement;
        return {
          ...statement,
          bind: (...values: unknown[]) => {
            bindCounts.push(values.length);
            return statement.bind(...values);
          },
        };
      },
    };

    // #when — one firing over all 120
    const purged = await purgeExpiredThreads(countingDb, {
      ttlMs: 30 * DAY_MS,
      limit: 120,
      now: () => NOW,
    });

    // #then — everything reaped, and no single statement bound over 100 params
    expect(purged).toEqual({ threads: 120, messages: 120 });
    expect(bindCounts.length).toBeGreaterThan(2);
    expect(Math.max(...bindCounts)).toBeLessThanOrEqual(100);
  });

  it('reads missing tables as empty, so a memory-less deployment purges unchanged', async () => {
    // #given — a fresh DB: no host has enabled agent memory, so Mastra never
    // created the tables. The duty must no-op, not wedge the purge cron.
    const sqlite = openSqlite();

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 0, messages: 0 });
  });

  it('reads a missing MESSAGES table as empty but still expires the threads', async () => {
    // #given — threads without the messages table (a host whose memory domain
    // never fully initialized)
    const sqlite = openSqlite();
    sqlite
      .prepare(
        `CREATE TABLE mastra_threads (id TEXT PRIMARY KEY, updatedAt TEXT NOT NULL)`,
      )
      .run();
    sqlite
      .prepare('INSERT INTO mastra_threads (id, updatedAt) VALUES (?, ?)')
      .run('abc_idle', new Date(NOW - 40 * DAY_MS).toISOString());

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(purged).toEqual({ threads: 1, messages: 0 });
  });

  it('honors the tablePrefix', async () => {
    // #given
    const sqlite = openSqlite();
    createThreadTables(sqlite, 'p_');
    sqlite
      .prepare(
        'INSERT INTO p_mastra_threads (id, resourceId, updatedAt) VALUES (?, NULL, ?)',
      )
      .run('abc_idle', new Date(NOW - 40 * DAY_MS).toISOString());

    // #when
    const purged = await purgeExpiredThreads(d1Like(sqlite), {
      ttlMs: 30 * DAY_MS,
      tablePrefix: 'p_',
      now: () => NOW,
    });

    // #then
    expect(purged.threads).toBe(1);
  });

  it('surfaces a NON-missing-table message failure rather than orphaning the history', async () => {
    // #given — a wedged messages table. Swallowing this would delete the
    // threads whose messages survived, stranding them forever.
    const sqlite = openSqlite();
    createThreadTables(sqlite);
    seedThreadAt(sqlite, 'abc_idle', NOW - 40 * DAY_MS, ['m1']);
    const inner = d1Like(sqlite);
    const wedgedDb: SnapshotDatabase = {
      prepare: (sql: string) => {
        if (sql.includes('DELETE FROM mastra_messages')) {
          throw new Error('database is locked');
        }
        return inner.prepare(sql);
      },
    };

    // #when / #then — the cron's error surface fires and the thread survives
    await expect(
      purgeExpiredThreads(wedgedDb, { ttlMs: 30 * DAY_MS, now: () => NOW }),
    ).rejects.toThrow('database is locked');
    expect(idsIn(sqlite, 'mastra_threads', 'id')).toEqual(['abc_idle']);
    expect(idsIn(sqlite, 'mastra_messages', 'id')).toEqual(['m1']);
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

// mastra_background_tasks per @mastra/cloudflare-d1 DDL: run_id snake_case
// (the INV-1 salted originating run), status machine, camelCase completedAt as
// ISO TEXT — exactly the columns Track B's two purges ride on.
function createBackgroundTasksTable(db: SqliteDatabase, prefix = ''): void {
  db.prepare(
    `CREATE TABLE ${prefix}mastra_background_tasks (
      id TEXT PRIMARY KEY,
      run_id TEXT NOT NULL,
      status TEXT NOT NULL,
      completedAt TEXT,
      createdAt TEXT NOT NULL
    )`,
  ).run();
}

function seedTask(
  db: SqliteDatabase,
  options: {
    id: string;
    runId: string;
    status: string;
    completedAt?: number | null;
    prefix?: string;
  },
): void {
  const completed =
    options.completedAt === undefined || options.completedAt === null
      ? null
      : new Date(options.completedAt).toISOString();
  db.prepare(
    `INSERT INTO ${options.prefix ?? ''}mastra_background_tasks
     (id, run_id, status, completedAt, createdAt)
     VALUES (?, ?, ?, ?, ?)`,
  ).run(
    options.id,
    options.runId,
    options.status,
    completed,
    new Date(NOW).toISOString(),
  );
}

function taskIds(db: SqliteDatabase, prefix = ''): string[] {
  return (
    db
      .prepare(`SELECT id FROM ${prefix}mastra_background_tasks ORDER BY id`)
      .all() as Array<{ id: string }>
  ).map((row) => row.id);
}

describe('purgeExpiredBackgroundTasks', () => {
  it('reaps completed rows past completedTtlMs and keeps recent ones', async () => {
    // #given — one completed 2h ago, one 30m ago; default completedTtlMs 1h
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'old',
      runId: 'abc_r1',
      status: 'completed',
      completedAt: NOW - 2 * 3_600_000,
    });
    seedTask(sqlite, {
      id: 'fresh',
      runId: 'abc_r2',
      status: 'completed',
      completedAt: NOW - 30 * 60_000,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — only the old one goes
    expect(result).toEqual({ completed: 1, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['fresh']);
  });

  it('reaps failed / cancelled / timed_out on the SLOWER failed window, not the completed one', async () => {
    // #given — a failed row 2h old: past the 1h completed window but INSIDE the
    // 24h failed window, so it must survive (a failure stays inspectable)
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'failed-2h',
      runId: 'abc_r1',
      status: 'failed',
      completedAt: NOW - 2 * 3_600_000,
    });
    seedTask(sqlite, {
      id: 'cancelled-2d',
      runId: 'abc_r2',
      status: 'cancelled',
      completedAt: NOW - 2 * DAY_MS,
    });
    seedTask(sqlite, {
      id: 'timedout-2d',
      runId: 'abc_r3',
      status: 'timed_out',
      completedAt: NOW - 2 * DAY_MS,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — the two 2-day rows go; the 2-hour failure survives its slow window
    expect(result).toEqual({ completed: 0, failed: 2 });
    expect(taskIds(sqlite)).toEqual(['failed-2h']);
  });

  it('never reaps live rows (pending / running / suspended) whatever their age', async () => {
    // #given — an ancient suspended task; deleting it would strand its resume
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    for (const status of ['pending', 'running', 'suspended']) {
      seedTask(sqlite, {
        id: status,
        runId: `abc_${status}`,
        status,
        completedAt: NOW - 10 * DAY_MS,
      });
    }

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then — nothing terminal, nothing deleted
    expect(result).toEqual({ completed: 0, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['pending', 'running', 'suspended']);
  });

  it('keeps a terminal row with a NULL completedAt (cannot be proven old — fail safe)', async () => {
    // #given
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, {
      id: 'no-stamp',
      runId: 'abc_r1',
      status: 'completed',
      completedAt: null,
    });

    // #when
    const result = await purgeExpiredBackgroundTasks(d1Like(sqlite), {
      now: () => NOW,
    });

    // #then
    expect(result).toEqual({ completed: 0, failed: 0 });
    expect(taskIds(sqlite)).toEqual(['no-stamp']);
  });

  it('reads a missing table as zero (background tasks may never have run)', async () => {
    // #given — no table created
    const sqlite = openSqlite();

    // #when / #then
    expect(
      await purgeExpiredBackgroundTasks(d1Like(sqlite), { now: () => NOW }),
    ).toEqual({ completed: 0, failed: 0 });
  });
});

describe('purgeTenant background-task coverage (DL-003)', () => {
  it('reaps a tenant’s background-task rows by the run_id range, exactly one tenant', async () => {
    // #given — abc and its digit-suffixed prefix neighbor abc5 (the range
    // exactness case), plus another tenant xyz, all keyed by run_id
    const sqlite = openSqlite();
    createBackgroundTasksTable(sqlite);
    seedTask(sqlite, { id: 't-abc', runId: 'abc_r1', status: 'completed' });
    seedTask(sqlite, { id: 't-abc5', runId: 'abc5_r1', status: 'running' });
    seedTask(sqlite, { id: 't-xyz', runId: 'xyz_r1', status: 'suspended' });

    // #when — offboard exactly abc
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — only abc's task goes (running included: offboarding ignores
    // status); abc5 and xyz survive — the range is exact over run_id
    expect(result.backgroundTasks).toBe(1);
    expect(taskIds(sqlite)).toEqual(['t-abc5', 't-xyz']);
  });

  it('missing background-tasks table offboards cleanly (run-less tenant)', async () => {
    // #given — a tenant with no background tasks table at all
    const sqlite = openSqlite();

    // #when / #then — no throw, counter is zero
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });
    expect(result.backgroundTasks).toBe(0);
  });
});

// --- Track C (M-004) signal-domain retention + offboarding -----------------
// The two flowsafe-owned tables are created by their D1 domains (the adapter
// ships neither), then seeded with RAW inserts so the timestamps are precise.
async function signalDb(sqlite: SqliteDatabase): Promise<SignalDatabase> {
  const db = d1DatabaseLike(sqlite) as unknown as SignalDatabase;
  await new D1NotificationsStorage(db, '').init();
  await new D1ThreadStateStorage(db, '').init();
  return db;
}

function seedNotification(
  db: SqliteDatabase,
  row: { id: string; threadId: string; status: string; updatedAt: number },
): void {
  const iso = new Date(row.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO mastra_notifications
       (id, thread_id, source, kind, priority, status, summary, coalescedCount,
        createdAt, updatedAt, deliveryAttempts)
     VALUES (?, ?, 'x', 'y', 'medium', ?, 'z', 1, ?, ?, 0)`,
  ).run(row.id, row.threadId, row.status, iso, iso);
}

function seedThreadState(
  db: SqliteDatabase,
  row: { threadId: string; type: string; updatedAt: number },
): void {
  const iso = new Date(row.updatedAt).toISOString();
  db.prepare(
    `INSERT INTO mastra_thread_state (thread_id, type, value, updatedAt)
     VALUES (?, ?, '{}', ?)`,
  ).run(row.threadId, row.type, iso);
}

describe('purgeExpiredNotifications', () => {
  it('reaps TERMINAL rows past the TTL and keeps pending ones', async () => {
    // #given — a delivered row long past the TTL, a fresh delivered row, and a
    // pending row (even an ancient one) that must survive.
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedNotification(sqlite, {
      id: 'old-delivered',
      threadId: 'abc_t1',
      status: 'delivered',
      updatedAt: NOW - 2 * DAY_MS,
    });
    seedNotification(sqlite, {
      id: 'fresh-delivered',
      threadId: 'abc_t1',
      status: 'delivered',
      updatedAt: NOW,
    });
    seedNotification(sqlite, {
      id: 'ancient-pending',
      threadId: 'abc_t1',
      status: 'pending',
      updatedAt: NOW - 30 * DAY_MS,
    });

    // #when — one-day TTL
    const deleted = await purgeExpiredNotifications(d1Like(sqlite), {
      ttlMs: DAY_MS,
      now: () => NOW,
    });

    // #then — only the old TERMINAL row went; pending is never reaped by age.
    expect(deleted).toBe(1);
    const ids = (
      sqlite
        .prepare('SELECT id FROM mastra_notifications ORDER BY id')
        .all() as { id: string }[]
    ).map((r) => r.id);
    expect(ids).toEqual(['ancient-pending', 'fresh-delivered']);
  });

  it('reads a missing table as zero', async () => {
    const sqlite = openSqlite();
    expect(
      await purgeExpiredNotifications(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});

describe('purgeExpiredThreadState', () => {
  it('reaps rows past the updatedAt TTL and keeps fresh ones', async () => {
    // #given — an abandoned goal (old) and an active one (fresh)
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedThreadState(sqlite, {
      threadId: 'abc_t1',
      type: 'goal',
      updatedAt: NOW - 10 * DAY_MS,
    });
    seedThreadState(sqlite, {
      threadId: 'abc_t2',
      type: 'goal',
      updatedAt: NOW,
    });

    // #when
    const deleted = await purgeExpiredThreadState(d1Like(sqlite), {
      ttlMs: DAY_MS,
      now: () => NOW,
    });

    // #then
    expect(deleted).toBe(1);
    const rows = sqlite
      .prepare('SELECT thread_id FROM mastra_thread_state')
      .all() as { thread_id: string }[];
    expect(rows.map((r) => r.thread_id)).toEqual(['abc_t2']);
  });

  it('reads a missing table as zero', async () => {
    const sqlite = openSqlite();
    expect(
      await purgeExpiredThreadState(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});

describe('purgeTenant — Track C signal tables', () => {
  it('reaps notifications + thread-state by the salted thread_id range, exactly one tenant', async () => {
    // #given — two tenants' rows keyed by salted threadIds (same suffix)
    const sqlite = openSqlite();
    await signalDb(sqlite);
    seedNotification(sqlite, {
      id: 'abc-n1',
      threadId: 'abc_t1',
      status: 'pending',
      updatedAt: NOW,
    });
    seedNotification(sqlite, {
      id: 'xyz-n1',
      threadId: 'xyz_t1',
      status: 'pending',
      updatedAt: NOW,
    });
    seedThreadState(sqlite, {
      threadId: 'abc_t1',
      type: 'goal',
      updatedAt: NOW,
    });
    seedThreadState(sqlite, {
      threadId: 'xyz_t1',
      type: 'goal',
      updatedAt: NOW,
    });
    // A prefix-neighbor ('abc5') that must NOT fall in the 'abc' range.
    seedNotification(sqlite, {
      id: 'abc5-n1',
      threadId: 'abc5_t1',
      status: 'pending',
      updatedAt: NOW,
    });

    // #when — offboard exactly 'abc'
    const purged = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — abc's rows only; the counters name them; xyz + abc5 survive.
    expect(purged.notifications).toBe(1);
    expect(purged.threadState).toBe(1);
    const notifIds = (
      sqlite
        .prepare('SELECT id FROM mastra_notifications ORDER BY id')
        .all() as { id: string }[]
    ).map((r) => r.id);
    expect(notifIds).toEqual(['abc5-n1', 'xyz-n1']);
    const stateThreads = (
      sqlite.prepare('SELECT thread_id FROM mastra_thread_state').all() as {
        thread_id: string;
      }[]
    ).map((r) => r.thread_id);
    expect(stateThreads).toEqual(['xyz_t1']);
  });

  it('purgeTenant reaps a tenant’s schedules + trigger history by metadata.tenantId, sparing others', async () => {
    // #given — two tenants' schedules + trigger rows in the flowsafe-owned Track
    // D tables. Their ids are slugified (schedule_<x>), NOT tenant-salted, so the
    // range predicate cannot reach them — the metadata-filter is what does.
    const sqlite = openSqlite();
    const store = new D1SchedulesStorage(
      d1DatabaseLike(sqlite) as unknown as ScheduleDatabase,
    );
    const seed = async (tenantId: string, id: string) => {
      await store.createSchedule({
        id,
        target: { type: 'workflow', workflowId: 'wf' },
        cron: '* * * * *',
        status: 'active',
        nextFireAt: NOW,
        createdAt: NOW,
        updatedAt: NOW,
        metadata: { tenantId },
      });
      await store.recordTrigger({
        scheduleId: id,
        runId: `${tenantId}_r1`,
        scheduledFireAt: NOW,
        actualFireAt: NOW,
        outcome: 'published',
        metadata: { tenantId },
      });
    };
    await seed('abc', 'schedule_a');
    await seed('xyz', 'schedule_x');

    // #when — offboard exactly 'abc'
    const purged = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });

    // #then — abc's schedule + its one trigger left; the counters name them; xyz
    // survives, proving the metadata filter is exact (not a substring match).
    expect(purged.schedules).toBe(1);
    expect(purged.scheduleTriggers).toBe(1);
    const scheduleIds = (
      sqlite.prepare('SELECT id FROM mastra_schedules').all() as {
        id: string;
      }[]
    ).map((r) => r.id);
    expect(scheduleIds).toEqual(['schedule_x']);
    const triggerTenants = (
      sqlite
        .prepare(
          "SELECT json_extract(metadata,'$.tenantId') AS t FROM mastra_schedule_triggers",
        )
        .all() as { t: string }[]
    ).map((r) => r.t);
    expect(triggerTenants).toEqual(['xyz']);
  });

  it('purgeExpiredScheduleTriggers reaps trigger rows past the actualFireAt TTL, keeping recent ones (numeric compare)', async () => {
    // #given — one old + one recent trigger. actualFireAt is INTEGER ms-epoch, so
    // the TTL is a NUMERIC comparison (not the ISO-text bet the other purges take).
    const sqlite = openSqlite();
    const store = new D1SchedulesStorage(
      d1DatabaseLike(sqlite) as unknown as ScheduleDatabase,
    );
    await store.createSchedule({
      id: 'schedule_a',
      target: { type: 'workflow', workflowId: 'wf' },
      cron: '* * * * *',
      status: 'active',
      nextFireAt: NOW,
      createdAt: NOW,
      updatedAt: NOW,
      metadata: { tenantId: 'abc' },
    });
    await store.recordTrigger({
      id: 'old',
      scheduleId: 'schedule_a',
      runId: 'abc_r1',
      scheduledFireAt: NOW - 10 * DAY_MS,
      actualFireAt: NOW - 10 * DAY_MS,
      outcome: 'published',
      metadata: { tenantId: 'abc' },
    });
    await store.recordTrigger({
      id: 'recent',
      scheduleId: 'schedule_a',
      runId: 'abc_r2',
      scheduledFireAt: NOW - 1000,
      actualFireAt: NOW - 1000,
      outcome: 'published',
      metadata: { tenantId: 'abc' },
    });

    // #when — a 7-day window at NOW
    const deleted = await purgeExpiredScheduleTriggers(d1Like(sqlite), {
      ttlMs: 7 * DAY_MS,
      now: () => NOW,
    });

    // #then — the 10-day-old row is reaped; the recent one stays.
    expect(deleted).toBe(1);
    const ids = (
      sqlite.prepare('SELECT id FROM mastra_schedule_triggers').all() as {
        id: string;
      }[]
    ).map((r) => r.id);
    expect(ids).toEqual(['recent']);
  });

  it('purgeExpiredScheduleTriggers reads a missing trigger table as zero', async () => {
    // #given — a fresh db, no schedule tables (schedules may never have fired)
    const sqlite = openSqlite();

    // #then — no throw, zero deleted
    expect(
      await purgeExpiredScheduleTriggers(d1Like(sqlite), { ttlMs: DAY_MS }),
    ).toBe(0);
  });
});
