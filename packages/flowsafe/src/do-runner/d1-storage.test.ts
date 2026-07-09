// purgeExpiredWorkflowRuns against REAL SQLite via node:sqlite (D1 is
// SQLite), so the json_extract status filter and the ISO-cutoff comparison
// run for real. The openSqlite() fixture matches the approval-api store
// tests; the d1Like adapter here maps run() results to D1's { meta } shape.

import { describe, expect, it } from 'vitest';

import {
  purgeExpiredWorkflowRuns,
  purgeTenant,
  type SnapshotDatabase,
  type SnapshotStatement,
} from './d1-storage.js';

interface SqliteStatement {
  get(...params: unknown[]): unknown;
  run(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

interface SqliteDatabase {
  prepare(sql: string): SqliteStatement;
}

// process.getBuiltinModule loads the builtin without import machinery, so
// neither vite's resolver (which cannot resolve node:sqlite) nor the
// workers-types tsconfig (no @types/node) ever sees the specifier.
function openSqlite(): SqliteDatabase {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error('node:sqlite unavailable — tests require node >= 22.13');
  }
  const mod = getBuiltin('node:sqlite') as {
    DatabaseSync: new (path: string) => SqliteDatabase;
  };
  return new mod.DatabaseSync(':memory:');
}

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

  it('tolerates a host without the approvals table (snapshot-only deployments)', async () => {
    // #given — no flowsafe_approvals table at all
    const sqlite = openSqlite();
    createSnapshotTable(sqlite);
    seedRun(sqlite, { runId: 'abc_r1', status: 'running', updatedAt: NOW });

    // #when / #then
    const result = await purgeTenant(d1Like(sqlite), { tenantId: 'abc' });
    expect(result).toEqual({ snapshots: 1, approvals: 0, artifacts: 0 });
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
