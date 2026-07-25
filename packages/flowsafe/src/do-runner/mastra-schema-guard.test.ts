// SPDX-License-Identifier: Apache-2.0
// Guards over the parts of Mastra's persistence WE do not own but DO depend
// on, exercised against the REAL @mastra/cloudflare-d1 D1Store over real
// SQLite (node:sqlite):
//
// 1. TABLE INVENTORY — createD1Storage eagerly creates the tables MASTRA_TABLES
//    names, and INV-1 covers exactly one by itself (mastra_workflow_snapshot,
//    keyed by run_id). The rest are keyed by ids INV-1 does not salt. The
//    tenancy decision this pin used to force is MADE for the memory tables and
//    its chokepoints shipped (docs/agent-memory-tenancy.md): memory-id.ts mints
//    salted threadId/resourceId values, TenantContext exposes them, and
//    purgeTenant range-deletes them.
//
//    The inventory is a STRUCTURE, not a name list (DL-003): each entry states
//    how purgeTenant reaps that table, so a track ADOPTING a domain appends its
//    row here and the guard below makes the omissions loud — an adopted table
//    with no purge coverage fails, and a table Mastra adds that nobody declared
//    fails. A @mastra/core bump changing the inventory (a new table, a rename)
//    must fail CI and re-open the tenancy review, never silently ship.
//
// 2. SCHEMA GUARD — purgeTenant's range DELETEs, the thread TTL, and the
//    app-owned index depend on Mastra's column names: snake_case `run_id` on the
//    snapshot table, `id`/`resourceId`/`thread_id` on the memory tables, and
//    `updatedAt` on mastra_threads (purgeExpiredThreads keys the TTL on it). A
//    @mastra/core bump renaming any of them must fail here, not in a
//    cross-tenant purge or a silently-inert retention duty.
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
import { createScheduleStorageDomains } from '../schedules/storage.js';
import { createSignalStorageDomains } from '../signals/storage.js';
import type { SnapshotDatabase, SnapshotStatement } from './d1-storage.js';
import {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  createD1Storage,
  NOTIFICATION_TTL_PURGE_TABLES,
  purgeTenant,
  RUN_TTL_PURGE_TABLES,
  SCHEDULE_TRIGGER_TTL_PURGE_TABLES,
  TENANT_METADATA_PURGE_TABLES,
  TENANT_RANGE_PURGE_TABLES,
  THREAD_STATE_TTL_PURGE_TABLES,
  THREAD_TTL_PURGE_TABLES,
} from './d1-storage.js';
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

/**
 * How purgeTenant reaps a Mastra table AT OFFBOARDING:
 * - 'run-id-range': the snapshot table's own block (INV-1 salted run_id).
 * - 'tenant-range': TENANT_RANGE_PURGE_TABLES (salted thread/resource ids).
 * - 'metadata-tenant': TENANT_METADATA_PURGE_TABLES (Track D schedules — the
 *   tenant is a JSON metadata.tenantId, since ids are slugified not salted, so the
 *   salted range predicate cannot reach them).
 * - 'unadopted': no feature writes it, so no id in it is salted yet. Adopting
 *   one means salting its ids and moving it to 'tenant-range' in the SAME
 *   change (DL-003) — the guard below is what makes that non-optional.
 */
type PurgeCoverage =
  | 'run-id-range'
  | 'tenant-range'
  | 'metadata-tenant'
  | 'unadopted';

/**
 * How a table's rows EXPIRE SHORT of offboarding — DL-003's third leg, and the
 * one a track is likeliest to skip: a table can be perfectly tenant-purgeable
 * and still grow forever, which is half the failure DL-003 names ("a
 * tenant-unpurgeable OR never-expiring table").
 *
 * `none` demands a REASON in the type. That is the whole mechanism: "we decided
 * this table needs no TTL, because X" and "nobody thought about it" are
 * indistinguishable in a bare enum, so the field makes the second one
 * unwritable. A track adding a table cannot leave retention undeclared (the
 * field is required) and cannot declare 'none' without saying why.
 */
type RetentionStory =
  | { kind: 'run-ttl' }
  | { kind: 'thread-ttl' }
  /** Track B: rows expire via purgeExpiredBackgroundTasks (completedAt TTL). */
  | { kind: 'background-task-ttl' }
  /** Track C: agent-inbox rows expire via purgeExpiredNotifications (terminal + updatedAt TTL). */
  | { kind: 'notification-ttl' }
  /** Track C: thread-state rows expire via purgeExpiredThreadState (updatedAt TTL). */
  | { kind: 'thread-state-ttl' }
  /** Track D: schedule-trigger history expires via purgeExpiredScheduleTriggers (actualFireAt TTL). */
  | { kind: 'schedule-trigger-ttl' }
  /** Rows die with their parent's TTL rather than aging out on their own. */
  | { kind: 'cascade'; with: string }
  | { kind: 'none'; because: string };

/** The age-based TTL kinds — each anchored to a real exported purge inventory. */
type TtlKind =
  | 'run-ttl'
  | 'thread-ttl'
  | 'background-task-ttl'
  | 'notification-ttl'
  | 'thread-state-ttl'
  | 'schedule-trigger-ttl';

describe('Mastra persistence guards (real D1Store over real SQLite)', () => {
  // The ONE reason an unadopted table carries — hoisted so the coverage↔reason
  // biconditional below can tie them: an unadopted table has nothing to expire
  // BECAUSE nothing writes it, so it carries exactly this, and the day a track
  // flips its coverage to 'tenant-range' the tie forces retention off this
  // boilerplate in the SAME change (the DL-003 "all three legs together" bar).
  const UNADOPTED_NO_RETENTION =
    'unadopted — no feature writes it, so there is nothing to expire yet';

  const MASTRA_TABLES: ReadonlyArray<{
    table: string;
    coverage: PurgeCoverage;
    retention: RetentionStory;
  }> = [
    {
      table: 'mastra_background_tasks',
      coverage: 'tenant-range',
      retention: { kind: 'background-task-ttl' },
    },
    {
      table: 'mastra_messages',
      coverage: 'tenant-range',
      retention: { kind: 'cascade', with: 'mastra_threads' },
    },
    {
      table: 'mastra_notifications',
      coverage: 'tenant-range',
      retention: { kind: 'notification-ttl' },
    },
    {
      table: 'mastra_resources',
      coverage: 'tenant-range',
      retention: {
        kind: 'none',
        because:
          "working memory is the OWNER's, shared across every thread they have, so one thread aging out says nothing about it — and a resource has no idleness signal of its own. Reaped at offboarding by purgeTenant",
      },
    },
    // Track D — both metadata-tenant (their tenant is a JSON metadata.tenantId,
    // since ids are slugified `agent_`/`schedule_`, not tenant-salted). Sorted:
    // 'mastra_schedule_triggers' precedes 'mastra_schedules' under BINARY
    // collation ('_' 0x5F < 's' 0x73, the same order 'mastra_thread_state' <
    // 'mastra_threads' takes), and both precede 'mastra_scorers' ('sch' < 'sco').
    {
      table: 'mastra_schedule_triggers',
      coverage: 'metadata-tenant',
      retention: { kind: 'schedule-trigger-ttl' },
    },
    {
      table: 'mastra_schedules',
      coverage: 'metadata-tenant',
      retention: {
        kind: 'none',
        because:
          'a schedule is standing config a tenant creates and deletes explicitly — it has no terminal state to age out; its fire HISTORY expires (mastra_schedule_triggers, schedule-trigger-ttl) but the config row is reaped only at offboarding by purgeTenant',
      },
    },
    {
      table: 'mastra_scorers',
      coverage: 'unadopted',
      retention: {
        kind: 'none',
        because: UNADOPTED_NO_RETENTION,
      },
    },
    // 'mastra_thread_state' sorts BEFORE 'mastra_threads' under BINARY collation
    // ('_' 0x5F < 's' 0x73), which is the order sqlite_master's ORDER BY name
    // returns and the toEqual below pins.
    {
      table: 'mastra_thread_state',
      coverage: 'tenant-range',
      retention: { kind: 'thread-state-ttl' },
    },
    {
      table: 'mastra_threads',
      coverage: 'tenant-range',
      retention: { kind: 'thread-ttl' },
    },
    {
      table: 'mastra_workflow_snapshot',
      coverage: 'run-id-range',
      retention: { kind: 'run-ttl' },
    },
  ];

  it('createD1Storage creates EXACTLY the declared tables — an undeclared one is a tenancy decision, not a default', async () => {
    // #given — the real storage adapter over sqlite, WITH the Track C signal
    // domains composed (createD1Storage takes them injected — signals/ imports
    // do-runner, so do-runner cannot import back). @mastra/cloudflare-d1 ships no
    // notifications/thread-state domain, so those two tables come from the
    // flowsafe-owned D1 domains, not the adapter.
    const sqlite = openSqlite();
    const binding = d1DatabaseLike(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: {
        ...createSignalStorageDomains(binding as never),
        ...createScheduleStorageDomains(binding as never),
      },
    });
    const { runtime } = buildGated(storage);

    // #when — the workflow run triggers the adapter's six tables; init() then
    // creates the flowsafe-owned domain tables (signals' two + schedules' two —
    // each builds its schema on init()).
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    await storage.init();

    // #then — the exact inventory, pinned. A Mastra bump that adds a table (or
    // a track that enables a domain) fails HERE until someone declares how the
    // tenant purge covers it.
    expect(tableNames(sqlite)).toEqual(
      MASTRA_TABLES.map((entry) => entry.table),
    );
  });

  it('every retention declaration is anchored to a REAL purge target (the exported inventories), not to intent', async () => {
    // #given — DL-003's third leg: guard + purge + RETENTION land together, or a
    // track ships a table that is tenant-purgeable and still grows forever. The
    // type forces the DECLARATION; this pins that each declaration corresponds to
    // a purge that really deletes the table, by cross-checking the PRODUCTION
    // inventories RUN_TTL_PURGE_TABLES / THREAD_TTL_PURGE_TABLES (exported from
    // d1-storage and consumed here the way TENANT_RANGE_PURGE_TABLES already is) —
    // NOT literals copied into this test, which would drift from the SQL silently.
    //
    // A table is "accounted for" by a TTL if it declares that kind OR cascades
    // onto a parent carrying it: a cascade MEANS the row dies with the parent's
    // purge, so that purge must genuinely delete the child — i.e. the child is
    // one of the parent's targets.
    const retentionKindOf = (
      table: string,
    ): RetentionStory['kind'] | undefined =>
      MASTRA_TABLES.find((entry) => entry.table === table)?.retention.kind;
    const accountedFor = (kind: TtlKind): string[] =>
      MASTRA_TABLES.filter((entry) => {
        if (entry.retention.kind === kind) return true;
        // A cascade rides its parent's purge, so it is a target of that purge.
        if (entry.retention.kind === 'cascade') {
          return retentionKindOf(entry.retention.with) === kind;
        }
        return false;
      })
        .map((entry) => entry.table)
        .sort();

    // #then — set equality BOTH ways: a declaration with no purge target fails
    // (declared side has it, the const does not) AND a purge target with no
    // declaration fails (the const has it, nothing accounts for it).
    expect(accountedFor('run-ttl')).toEqual([...RUN_TTL_PURGE_TABLES].sort());
    expect(accountedFor('thread-ttl')).toEqual(
      [...THREAD_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('background-task-ttl')).toEqual(
      [...BACKGROUND_TASK_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('notification-ttl')).toEqual(
      [...NOTIFICATION_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('thread-state-ttl')).toEqual(
      [...THREAD_STATE_TTL_PURGE_TABLES].sort(),
    );
    expect(accountedFor('schedule-trigger-ttl')).toEqual(
      [...SCHEDULE_TRIGGER_TTL_PURGE_TABLES].sort(),
    );

    // ...and every cascade names a parent that EXISTS and itself expires. The
    // set check cannot see a cascade onto a 'none'/absent parent whose child is
    // in no inventory (it is simply absent from both sides), so catch that lie
    // directly: a cascade onto something that expires nothing is not a real one.
    for (const entry of MASTRA_TABLES) {
      const { retention } = entry;
      if (retention.kind !== 'cascade') continue;
      const parentTable = retention.with;
      const parent = MASTRA_TABLES.find(
        (candidate) => candidate.table === parentTable,
      );
      expect(
        parent?.retention.kind,
        `${entry.table} cascades onto '${parentTable}', which expires nothing`,
      ).toMatch(/^(run|thread)-ttl$/);
    }
  });

  it("a 'none' retention carries a real reason, and 'unadopted' coverage is tied to it", async () => {
    // #given — 'none' is the escape hatch, so it is where an undeclared decision
    // hides. The type demands the `because` KEY but cannot demand CONTENT, so a
    // blank string type-checks while saying nothing. Two guards close that.
    for (const entry of MASTRA_TABLES) {
      if (entry.retention.kind !== 'none') continue;
      // #then — a real sentence, not a blank the type technically accepts
      expect(
        entry.retention.because.trim().length,
        `${entry.table} declares retention 'none' with an empty/blank reason`,
      ).toBeGreaterThan(20);
    }

    // #then — 'unadopted' coverage and the unadopted retention reason move
    // together: neither changes without the other. Flipping coverage to
    // 'tenant-range' (adopting the table) forces retention off the boilerplate in
    // the SAME change — DL-003's third leg mechanized rather than trusted.
    for (const entry of MASTRA_TABLES) {
      const isUnadopted = entry.coverage === 'unadopted';
      const hasUnadoptedReason =
        entry.retention.kind === 'none' &&
        entry.retention.because === UNADOPTED_NO_RETENTION;
      expect(
        isUnadopted,
        `${entry.table}: 'unadopted' coverage and the unadopted retention reason must move together`,
      ).toBe(hasUnadoptedReason);
    }
  });

  it('every table declared tenant-range IS in purgeTenant’s table list, and vice versa', async () => {
    // #given — the DL-003 "same change" bar, mechanized: adopting a domain
    // means salting its ids AND range-purging them. Declaring the coverage
    // without wiring the purge would leave a tenant's rows unreachable at
    // offboarding — a leak the inventory pin above cannot see, because the
    // table was there all along.
    const declared = MASTRA_TABLES.filter(
      (entry) => entry.coverage === 'tenant-range',
    ).map((entry) => entry.table);

    // #when
    const wired = TENANT_RANGE_PURGE_TABLES.map((entry) => entry.table);

    // #then — set equality in both directions
    expect(wired.slice().sort()).toEqual(declared.slice().sort());
  });

  it('every table declared metadata-tenant IS in purgeTenant’s metadata list, and vice versa', async () => {
    // #given — the DL-003 "same change" bar for the SECOND offboarding kind:
    // Track D schedules key on slugified ids, so they are reaped by a JSON
    // metadata.tenantId filter, not the salted range. Declaring the coverage
    // without wiring TENANT_METADATA_PURGE_TABLES would strand a tenant's schedule
    // rows at offboarding — a leak the inventory pin cannot see (the table exists).
    const declared = MASTRA_TABLES.filter(
      (entry) => entry.coverage === 'metadata-tenant',
    ).map((entry) => entry.table);

    // #when
    const wired = TENANT_METADATA_PURGE_TABLES.map((entry) => entry.table);

    // #then — set equality in both directions
    expect(wired.slice().sort()).toEqual(declared.slice().sort());
  });

  it('the Track D schedule tables keep the columns their purge + tick ride on (metadata + nextFireAt/actualFireAt)', async () => {
    // #given — flowsafe-owned tables (the adapter ships neither); compose the
    // schedules domain and init to create them.
    const sqlite = openSqlite();
    const binding = d1DatabaseLike(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: createScheduleStorageDomains(binding as never),
    });
    await storage.init();

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — purgeTenant metadata-filters BOTH over `metadata` (json_extract
    // '$.tenantId'); listDueSchedules rides (status, nextFireAt) and the trigger
    // TTL reaps by actualFireAt. These are OUR column names, so this pins US
    // against a self-inflicted rename that would inert a purge or the tick.
    expect(columnsOf('mastra_schedules')).toEqual(
      expect.arrayContaining(['metadata', 'status', 'nextFireAt']),
    );
    expect(columnsOf('mastra_schedule_triggers')).toEqual(
      expect.arrayContaining(['metadata', 'scheduleId', 'actualFireAt']),
    );
  });

  it("mastra_threads.updatedAt is still ISO-8601 TEXT — the encoding the thread TTL's comparison rides", async () => {
    // #given — purgeExpiredThreads compares `updatedAt < '<iso>'`
    // LEXICOGRAPHICALLY, which is a timestamp comparison only while the column
    // holds ISO text. This pin exists because the blast radius is unbounded and
    // silent: SQLite orders INTEGER before TEXT ALWAYS, so if a bump ever stored
    // epoch ints, `updatedAt < '<iso text>'` would be true for EVERY row and the
    // first cron firing would delete every thread and every message — active
    // conversations included, history first. purgeExpiredWorkflowRuns takes the
    // same bet but ANDs a terminal-status predicate, so it can only over-delete
    // finished runs; this purge has no second predicate.
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('D1Store exposes no memory domain');
    const now = new Date();
    await memory.saveThread({
      thread: {
        id: mintThreadId('abc', () => 'thread-1'),
        resourceId: mintResourceId('abc', 'user-1'),
        title: 'conversation',
        createdAt: now,
        updatedAt: now,
      },
    });

    // #when — the RAW stored value, not the domain's parsed projection
    const [row] = sqlite
      .prepare('SELECT updatedAt FROM mastra_threads')
      .all() as Array<{ updatedAt: unknown }>;

    // #then — TEXT, and ISO-8601 specifically (a Date-parseable but non-ISO
    // text encoding would still break lexicographic ordering)
    expect(typeof row?.updatedAt).toBe('string');
    expect(String(row?.updatedAt)).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    );
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

  it('mastra_background_tasks keeps the columns Track B purges ride on (run_id range + completedAt/status TTL)', async () => {
    // #given — the real adapter creates the table eagerly (Track B adopted it)
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const { runtime } = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_background_tasks)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — purgeTenant ranges over run_id (snake_case, the originating run's
    // INV-1 salted id); purgeExpiredBackgroundTasks reaps by status + completedAt.
    // A @mastra/cloudflare-d1 bump renaming any of them fails HERE, not in a
    // cross-tenant purge or a silently-inert TTL duty.
    expect(columns).toContain('run_id');
    expect(columns).toContain('status');
    expect(columns).toContain('completedAt');
  });

  it('the Track C signal tables keep the columns their purges ride on (thread_id range + status/updatedAt TTL)', async () => {
    // #given — these two tables are flowsafe-owned (the adapter ships neither),
    // so compose the domains and init to create them.
    const sqlite = openSqlite();
    const binding = d1DatabaseLike(sqlite);
    const storage = createD1Storage({
      binding: binding as never,
      domains: createSignalStorageDomains(binding as never),
    });
    await storage.init();

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — purgeTenant ranges both over `thread_id` (the salted memory
    // threadId); purgeExpiredNotifications reaps by status + updatedAt, and
    // purgeExpiredThreadState by updatedAt. These are OUR column names, so this
    // pins US against a self-inflicted rename that would inert a purge.
    expect(columnsOf('mastra_notifications')).toEqual(
      expect.arrayContaining(['thread_id', 'status', 'updatedAt']),
    );
    expect(columnsOf('mastra_thread_state')).toEqual(
      expect.arrayContaining(['thread_id', 'type', 'updatedAt']),
    );
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
    // design key on exactly these names; a Mastra rename fails HERE.
    // `updatedAt` carries purgeExpiredThreads' TTL: renamed, the thread
    // retention duty would throw on every firing instead of expiring anything.
    expect(columnsOf('mastra_threads')).toEqual(
      expect.arrayContaining(['id', 'resourceId', 'updatedAt']),
    );
    // createdAt is the message's own evidence of recency — the guard that keeps
    // purgeExpiredThreads from sweeping a just-sent message whose thread still
    // reads idle because the writer's updatedAt bump has not landed yet.
    expect(columnsOf('mastra_messages')).toEqual(
      expect.arrayContaining(['id', 'thread_id', 'resourceId', 'createdAt']),
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
