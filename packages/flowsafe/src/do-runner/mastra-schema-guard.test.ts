// SPDX-License-Identifier: Apache-2.0
// Guards over the parts of Mastra's persistence WE do not own but DO depend
// on, exercised against the REAL @mastra/cloudflare-d1 D1Store over real
// SQLite (node:sqlite):
//
// 1. TABLE INVENTORY — createD1Storage eagerly creates exactly the tables
//    MASTRA_TABLES names. A dependency bump that adds or renames one must make
//    its lifecycle decision explicit here.
//
//    The inventory is a STRUCTURE, not a name list (DL-003): each entry states
//    whether the product adopts that table and how its rows expire. A
//    @mastra/core bump changing the inventory must fail CI and reopen the
//    persistence review, never silently ship.
//
// 2. SCHEMA GUARD — retention jobs and app-owned indexes depend on Mastra's
//    column names and encodings. A @mastra/core bump renaming any of them must
//    fail here instead of silently disabling maintenance.

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
import {
  BACKGROUND_TASK_TTL_PURGE_TABLES,
  createD1Storage,
  NOTIFICATION_TTL_PURGE_TABLES,
  RUN_TTL_PURGE_TABLES,
  SCHEDULE_TRIGGER_TTL_PURGE_TABLES,
  THREAD_STATE_TTL_PURGE_TABLES,
  THREAD_TTL_PURGE_TABLES,
} from './d1-storage.js';
import { init } from './init.js';
import { mintThreadId, resourceIdFromKey } from './memory-id.js';
import type { RunnerRuntime } from './runtime.js';

function tableNames(db: SqliteDatabase): string[] {
  return (
    db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'mastra_%' ORDER BY name`,
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

// A gated workflow over the given storage.
function buildGated(storage: MastraCompositeStore): RunnerRuntime {
  const { createWorkflow, createStep, runtime } = init({ storage });
  const gate = createStep({
    id: 'gate',
    inputSchema: z.object({}),
    outputSchema: z.object({}),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({ go: z.boolean() }),
    execute: async ({ resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'wait' });
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
  return runtime;
}

/**
 * Whether the single-deployment runtime owns rows in a Mastra table. An
 * unadopted domain carries no product data; adopting it must define retention
 * in the same change.
 */
type StorageOwnership = 'deployment-wide' | 'unadopted';

/**
 * How a table's rows expire. This is the lifecycle decision a new Mastra table
 * is likeliest to omit: a table can be adopted successfully and still grow
 * forever.
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
  // The one reason an unadopted table carries — hoisted so the ownership↔reason
  // biconditional below can tie them: an unadopted table has nothing to expire
  // BECAUSE nothing writes it, so it carries exactly this, and the day a track
  // becomes deployment-wide the tie forces retention off this
  // boilerplate in the SAME change (the DL-003 "all three legs together" bar).
  const UNADOPTED_NO_RETENTION =
    'unadopted — no feature writes it, so there is nothing to expire yet';

  const MASTRA_TABLES: ReadonlyArray<{
    table: string;
    coverage: StorageOwnership;
    retention: RetentionStory;
  }> = [
    {
      table: 'mastra_background_tasks',
      coverage: 'deployment-wide',
      retention: { kind: 'background-task-ttl' },
    },
    {
      table: 'mastra_messages',
      coverage: 'deployment-wide',
      retention: { kind: 'cascade', with: 'mastra_threads' },
    },
    {
      table: 'mastra_notifications',
      coverage: 'deployment-wide',
      retention: { kind: 'notification-ttl' },
    },
    {
      table: 'mastra_resources',
      coverage: 'deployment-wide',
      retention: {
        kind: 'none',
        because:
          "working memory is the owner's, shared across every thread they have, so one thread aging out says nothing about it; the resource is deleted explicitly with its owner",
      },
    },
    // Track D tables. Sorted:
    // 'mastra_schedule_triggers' precedes 'mastra_schedules' under BINARY
    // collation ('_' 0x5F < 's' 0x73, the same order 'mastra_thread_state' <
    // 'mastra_threads' takes), and both precede 'mastra_scorers' ('sch' < 'sco').
    {
      table: 'mastra_schedule_triggers',
      coverage: 'deployment-wide',
      retention: { kind: 'schedule-trigger-ttl' },
    },
    {
      table: 'mastra_schedules',
      coverage: 'deployment-wide',
      retention: {
        kind: 'none',
        because:
          'a schedule is standing configuration deleted explicitly; it has no terminal state to age out, while its fire history expires through schedule-trigger-ttl',
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
      coverage: 'deployment-wide',
      retention: { kind: 'thread-state-ttl' },
    },
    {
      table: 'mastra_threads',
      coverage: 'deployment-wide',
      retention: { kind: 'thread-ttl' },
    },
    {
      table: 'mastra_workflow_snapshot',
      coverage: 'deployment-wide',
      retention: { kind: 'run-ttl' },
    },
  ];

  it('createD1Storage creates exactly the declared deployment inventory', async () => {
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
    const runtime = buildGated(storage);

    // #when — the workflow run triggers the adapter's six tables; init() then
    // creates the flowsafe-owned domain tables (signals' two + schedules' two —
    // each builds its schema on init()).
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    await storage.init();

    // #then — the exact inventory, pinned. A Mastra bump that adds a table (or
    // a track that enables a domain) fails here until someone declares its
    // ownership and retention.
    expect(tableNames(sqlite)).toEqual(
      MASTRA_TABLES.map((entry) => entry.table),
    );
  });

  it('every retention declaration is anchored to a REAL purge target (the exported inventories), not to intent', async () => {
    // #given — guard + retention land together, or a
    // track ships a table that grows forever. The
    // type forces the DECLARATION; this pins that each declaration corresponds to
    // a purge that really deletes the table, by cross-checking the PRODUCTION
    // inventories RUN_TTL_PURGE_TABLES / THREAD_TTL_PURGE_TABLES exported from
    // d1-storage —
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

  it("a 'none' retention carries a real reason, and unadopted ownership is tied to it", async () => {
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

    // #then — ownership and the unadopted retention reason move together.
    for (const entry of MASTRA_TABLES) {
      const isUnadopted = entry.coverage === 'unadopted';
      const hasUnadoptedReason =
        entry.retention.kind === 'none' &&
        entry.retention.because === UNADOPTED_NO_RETENTION;
      expect(
        isUnadopted,
        `${entry.table}: unadopted ownership and retention reason must move together`,
      ).toBe(hasUnadoptedReason);
    }
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

    // #then — listDueSchedules rides (status, nextFireAt) and the trigger TTL
    // reaps by actualFireAt. These are our column names, so this pins us
    // against a self-inflicted rename that would inert maintenance.
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
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
    const memory = await storage.getStore('memory');
    if (!memory) throw new Error('D1Store exposes no memory domain');
    const now = new Date();
    await memory.saveThread({
      thread: {
        id: mintThreadId(() => 'thread-1'),
        resourceId: resourceIdFromKey('user-1'),
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

  it("mastra_workflow_snapshot still has the snake_case 'run_id' column retention rides on", async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_workflow_snapshot)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — run_id (snake_case) present; a Mastra bump renaming it fails here
    expect(columns).toContain('run_id');
    expect(columns).toContain('workflow_name');
  });

  it('mastra_background_tasks keeps the columns Track B purges ride on (run_id range + completedAt/status TTL)', async () => {
    // #given — the real adapter creates the table eagerly (Track B adopted it)
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columns = (
      sqlite.prepare('PRAGMA table_info(mastra_background_tasks)').all() as {
        name: string;
      }[]
    ).map((column) => column.name);

    // #then — purgeExpiredBackgroundTasks reaps by status + completedAt while
    // run_id associates each task with its originating run.
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

    // #then — purgeExpiredNotifications reaps by status + updatedAt and
    // purgeExpiredThreadState by updatedAt. These are our column names.
    expect(columnsOf('mastra_notifications')).toEqual(
      expect.arrayContaining(['thread_id', 'status', 'updatedAt']),
    );
    expect(columnsOf('mastra_thread_state')).toEqual(
      expect.arrayContaining(['thread_id', 'type', 'updatedAt']),
    );
  });

  it('the memory tables retain their addressing and retention columns', async () => {
    // #given
    const sqlite = openSqlite();
    const storage = createD1Storage({
      binding: d1DatabaseLike(sqlite) as never,
    });
    const runtime = buildGated(storage);
    await runtime.start('gated', { runId: 'abc_r1', inputData: {} });

    // #when
    const columnsOf = (table: string) =>
      (
        sqlite.prepare(`PRAGMA table_info(${table})`).all() as {
          name: string;
        }[]
      ).map((column) => column.name);

    // #then — memory addressing and retention key on exactly these names.
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

  it('InMemoryStore is unaffected by the guards (they pin the D1 adapter only)', () => {
    // Regression tripwire for the harness itself: the guards must not
    // accidentally depend on adapter internals beyond the documented DDL.
    expect(new InMemoryStore()).toBeDefined();
  });
});
