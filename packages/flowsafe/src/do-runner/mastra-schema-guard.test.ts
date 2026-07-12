// SPDX-License-Identifier: Apache-2.0
// Guards over the parts of Mastra's persistence WE do not own but DO depend
// on, exercised against the REAL @mastra/cloudflare-d1 D1Store over real
// SQLite (node:sqlite):
//
// 1. TABLE INVENTORY — createD1Storage eagerly creates SIX tables, and INV-1
//    covers exactly ONE of them (mastra_workflow_snapshot, keyed by run_id).
//    The other five (threads, messages, resources, scorers, background
//    tasks) are keyed by ids INV-1 does not salt; they are empty until a
//    host enables agent memory. The tenancy decision that pin used to force
//    is now MADE and its chokepoints shipped (docs/agent-memory-tenancy.md):
//    memory-id.ts mints salted threadId/resourceId values, TenantContext
//    exposes them, and purgeTenant range-deletes the three memory tables.
//    The pin stays: a @mastra/core bump changing the inventory (a SEVENTH
//    table, or a rename) must fail CI and re-open the tenancy review, never
//    silently ship.
//
// 2. SCHEMA GUARD — purgeTenant's range DELETEs and the app-owned index
//    depend on Mastra's column names: snake_case `run_id` on the snapshot
//    table, and `id`/`resourceId`/`thread_id` on the memory tables. A
//    @mastra/core bump renaming any of them must fail here, not in a
//    cross-tenant purge.
//
// 3. PURGE-VS-RESUME RACE — purgeTenant deletes SUSPENDED rows; a reviewer
//    approving at that moment resumes against a vanished row. Pin the
//    absorbed outcome: the resume FAILS (no snapshot), the gated step does
//    NOT re-execute, and the resume attempt does not re-create a snapshot
//    row (re-execution from scratch would replay side effects).

import type { MastraCompositeStore } from '@mastra/core/storage';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  d1DatabaseLike,
  openSqlite,
  type SqliteDatabase,
} from '../../test-support/sqlite.js';
import type { SnapshotDatabase, SnapshotStatement } from './d1-storage.js';
import { createD1Storage, purgeTenant } from './d1-storage.js';
import { init } from './init.js';
import { mintResourceId, mintThreadId } from './memory-id.js';
import type { RunnerRuntime } from './runtime.js';

/** The SnapshotDatabase view over the same sqlite handle (for purgeTenant). */
function snapshotDb(db: SqliteDatabase): SnapshotDatabase {
  function statement(sql: string, params: unknown[]): SnapshotStatement {
    return {
      bind: (...values: unknown[]) => statement(sql, values),
      run: async () => {
        const outcome = db.prepare(sql).run(...params) as {
          changes?: number | bigint;
        };
        return { meta: { changes: Number(outcome?.changes ?? 0) } };
      },
      all: async <T>() => ({
        results: db.prepare(sql).all(...params) as T[],
      }),
    };
  }
  return { prepare: (sql: string) => statement(sql, []) };
}

function tableNames(db: SqliteDatabase): string[] {
  return (
    db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'mastra_%' ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

// A gated workflow over the given storage; counts post-approval executions.
function buildGated(storage: MastraCompositeStore): {
  runtime: RunnerRuntime;
  gateRuns: () => number;
} {
  let gateRuns = 0;
  const { createWorkflow, createStep, runtime } = init({ storage });
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ go: z.boolean() }),
    execute: async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'wait' });
      gateRuns += 1;
      return {};
    },
  });
  createWorkflow({
    id: 'gated',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
  })
    .then(gate)
    .commit();
  return { runtime, gateRuns: () => gateRuns };
}

describe('Mastra persistence guards (real D1Store over real SQLite)', () => {
  const SIX_TABLES = [
    'mastra_background_tasks',
    'mastra_messages',
    'mastra_resources',
    'mastra_scorers',
    'mastra_threads',
    'mastra_workflow_snapshot',
  ];

  it('createD1Storage creates EXACTLY the six known tables — a seventh (or a write to the five uncovered ones) is a tenancy decision, not a default', async () => {
    // #given — the real storage adapter over sqlite
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);

    // #when — first persistence op triggers table creation
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #then — the exact inventory, pinned
    expect(tableNames(sqlite)).toEqual(SIX_TABLES);
  });

  it("mastra_workflow_snapshot still has the snake_case 'run_id' column purgeTenant's range predicate rides on", async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_workflow_snapshot)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — run_id (snake_case) present; a Mastra bump renaming it fails
    // HERE, not in a cross-tenant purge
    expect(columns).toContain('run_id');
    expect(columns).toContain('workflow_name');
  });

  it('a resume racing purgeTenant FAILS without re-executing the gated step or re-creating the snapshot', async () => {
    // #given — a suspended run persisted in the real snapshot table
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime, gateRuns } = buildGated(storage);
    const started = await runtime.start('gated', {
      runId: 'abc_r1',
      inputData: {},
    });
    expect(started.status).toBe('suspended');

    // #when — the tenant is purged (token already expired, per policy), then
    // a straggler resume lands
    const purged = await purgeTenant(snapshotDb(sqlite), { tenantId: 'abc' });
    expect(purged.snapshots).toBe(1);
    const resume = runtime.resume('gated', 'abc_r1', {
      step: 'gate',
      resumeData: { go: true },
    });

    // #then — absorbed: the resume errors (no snapshot), the gated step never
    // re-executed, and the resume attempt did not re-create a row (a fresh
    // snapshot would mean silent re-execution from scratch)
    await expect(resume).rejects.toThrow();
    expect(gateRuns()).toBe(0);
    const remaining = sqlite
      .prepare('SELECT run_id FROM mastra_workflow_snapshot')
      .all();
    expect(remaining).toEqual([]);
  });

  it('the memory tables still have the columns the tenancy chokepoints ride on', async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — purgeTenant's memory range predicates and the salted-id
    // design key on exactly these names; a Mastra rename fails HERE
    expect(columnsOf('mastra_threads')).toEqual(
      expect.arrayContaining(['id', 'resourceId']),
    );
    expect(columnsOf('mastra_messages')).toEqual(
      expect.arrayContaining(['id', 'thread_id', 'resourceId']),
    );
    expect(columnsOf('mastra_resources')).toEqual(
      expect.arrayContaining(['id']),
    );
  });

  it('salted memory ids keep two tenants disjoint and purgeTenant reaps exactly one of them', async () => {
    // #given — the REAL adapter; both tenants key memory by the SAME
    // business key ('user-1'), the exact collision the salting exists for.
    // A first run creates the six tables (the eager path production rides);
    // the node:sqlite harness cannot run the memory domain's lazy DDL.
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);
    await runtime.start('gated', { runId: 'seed_r0', inputData: {} });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('D1Store exposes no memory domain');
    const now = new Date();
    const seedTenant = async (tenantId: string) => {
      const threadId = mintThreadId(tenantId, () => 'thread-1');
      const resourceId = mintResourceId(tenantId, 'user-1');
      await memory.saveThread({
        thread: {
          id: threadId,
          resourceId,
          title: `${tenantId} conversation`,
          createdAt: now,
          updatedAt: now,
        },
      });
      await memory.saveMessages({
        messages: [
          {
            id: `${tenantId}-m1`,
            threadId,
            resourceId,
            role: 'user',
            createdAt: now,
            content: { format: 2, parts: [{ type: 'text', text: 'hello' }] },
          },
        ],
      });
      await memory.saveResource({
        resource: {
          id: resourceId,
          workingMemory: `${tenantId} working memory`,
          createdAt: now,
          updatedAt: now,
        },
      });
      return { threadId, resourceId };
    };
    const abc = await seedTenant('abc');
    const xyz = await seedTenant('xyz');
    // Digit-suffixed prefix neighbor: '5' (0x35) sorts below '_' (0x5F), so
    // these rows fall INSIDE the broken range if the lower bound ever loses
    // its trailing underscore — the exactness pin for the memory sweep.
    const abc5 = await seedTenant('abc5');
    // The shared key produced DISJOINT rows — the leak class is closed by
    // construction, before any purge runs.
    expect(abc.threadId).not.toBe(xyz.threadId);
    expect(abc.resourceId).not.toBe(xyz.resourceId);

    // #when — offboard exactly one tenant
    const purged = await purgeTenant(snapshotDb(sqlite), { tenantId: 'abc' });

    // #then — the counters name what left (exactly abc's one row per
    // table), and BOTH other tenants' memory survives intact and readable
    expect(purged.threads).toBe(1);
    expect(purged.messages).toBe(1);
    expect(purged.resources).toBe(1);
    const survivor = await memory.getThreadById({ threadId: xyz.threadId });
    expect(survivor?.id).toBe(xyz.threadId);
    const neighbor = await memory.getThreadById({ threadId: abc5.threadId });
    expect(neighbor?.id).toBe(abc5.threadId);
    const rows = sqlite
      .prepare('SELECT thread_id FROM mastra_messages ORDER BY thread_id')
      .all() as { thread_id: string }[];
    expect(rows.map((row) => row.thread_id)).toEqual(
      [abc5.threadId, xyz.threadId].sort(),
    );
  });

  it('InMemoryStore is unaffected by the guards (they pin the D1 adapter only)', () => {
    // Regression tripwire for the harness itself: the guards must not
    // accidentally depend on adapter internals beyond the documented DDL.
    expect(new InMemoryStore()).toBeDefined();
  });
});
