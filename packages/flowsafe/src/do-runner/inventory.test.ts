// SPDX-License-Identifier: Apache-2.0
// The drain inventory, over the REAL schemas.
//
// Every table below is created by the production code that owns it —
// createD1Storage plus the signal and schedule domains, the approval store
// factory, the resource-ownership schema, the reservation store, the
// subscription factory. A fixture that hand-wrote the DDL would pass forever
// while a column rename quietly emptied a category, which is the one failure
// this surface must not have: an empty category is what an operator reads as
// permission to migrate.

import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  openSqlite,
  type SqliteDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import type { ApprovalRecord } from '../approval-api/index.js';
import {
  createResourceOwnershipSchema,
  D1ApprovalStoreFactory,
  D1ResourceOwnershipStore,
} from '../approval-api/index.js';
// From the leaf that owns it, not the barrel: the table name is deliberately
// NOT part of the package's public surface — it is the one home two layers
// share, and widening it to consumers would invite queries the store cannot
// keep correct.
import { APPROVALS_TABLE } from '../approval-api/types.js';
// The REAL marker the background-task host stamps. The inventory restates it
// (it may not import a module that imports do-runner), so this fixture is what
// pins the copy: rename the constant without renaming the SQL and the
// fenceSuspended assertion below fails.
import { EXECUTION_FENCE_SUSPEND_KEY } from '../background-tasks/index.js';
import { createScheduleStorageDomains } from '../schedules/storage.js';
import { D1SubscriptionStoreFactory } from '../signal-providers/index.js';
import { createSignalStorageDomains } from '../signals/storage.js';
import { createD1Storage, RESOURCE_OWNER_TABLE } from './d1-storage.js';
import {
  DEPLOYMENT_IDENTITY_HEADER,
  DurableObjectRunner,
  type DurableObjectRunOwnershipStore,
  EXECUTION_PRINCIPAL_HEADER,
  ExecutionFenceStore,
  type RunnerRuntime,
} from './index.js';
import { init } from './init.js';
import {
  DeploymentInventory,
  INVENTORY_CATEGORY_DESCRIPTORS,
  InvalidInventoryRequestError,
  type InventoryCategory,
  type InventoryDatabase,
} from './inventory.js';
import {
  START_IDEMPOTENCY_TABLE,
  StartIdempotencyStore,
} from './start-idempotency.js';

const NOW = Date.parse('2026-08-24T12:00:00.000Z');

/** Every category name, so a sweep in a test can never miss one. */
const ALL_CATEGORIES: readonly InventoryCategory[] =
  INVENTORY_CATEGORY_DESCRIPTORS.map((entry) => entry.category);

const WORK_CATEGORIES: readonly InventoryCategory[] =
  INVENTORY_CATEGORY_DESCRIPTORS.filter((entry) => entry.class === 'work').map(
    (entry) => entry.category,
  );

/**
 * A database that RECORDS every statement prepared through it.
 *
 * This is the read-only pin's primary instrument, and it is stronger than
 * counting rows afterwards: a write that happened to change nothing (a
 * `CREATE TABLE IF NOT EXISTS` on an existing table, an UPDATE matching zero
 * rows) leaves the data identical and is still exactly the class of side effect
 * this surface must not have. What the deployment is about to be COPIED from
 * must not be mutated by the act of measuring it.
 */
function recordingDatabase(binding: unknown): {
  db: InventoryDatabase;
  statements: string[];
  /** One entry per `bind()`: the SQL and how many parameters it carried. */
  bindings: Array<{ sql: string; count: number }>;
} {
  const statements: string[] = [];
  const bindings: Array<{ sql: string; count: number }> = [];
  const inner = binding as InventoryDatabase;
  return {
    statements,
    bindings,
    db: {
      prepare(query: string) {
        statements.push(query);
        const prepared = inner.prepare(query);
        return {
          ...prepared,
          bind(...values: unknown[]) {
            bindings.push({ sql: query, count: values.length });
            return prepared.bind(...values);
          },
        };
      },
    },
  };
}

/** The schema exactly as SQLite records it — the evidence no DDL ran. */
function schemaSnapshot(sqlite: SqliteDatabase): unknown[] {
  return sqlite
    .prepare(
      'SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name',
    )
    .all();
}

/** Every row of every table, so a change of any kind shows up. */
function dataSnapshot(sqlite: SqliteDatabase): Record<string, unknown[]> {
  const tables = (
    sqlite
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
      )
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
  const snapshot: Record<string, unknown[]> = {};
  for (const table of tables) {
    snapshot[table] = sqlite.prepare(`SELECT * FROM ${table}`).all();
  }
  return snapshot;
}

function approval(overrides: Partial<ApprovalRecord>): ApprovalRecord {
  const at = new Date(NOW).toISOString();
  return {
    id: 'apr-1',
    workflowId: 'wf',
    runId: 'run-1',
    title: 'approval',
    connectors: [],
    priority: 'normal',
    status: 'pending',
    createdAt: at,
    updatedAt: at,
    ...overrides,
  };
}

interface Fixture {
  sqlite: SqliteDatabase;
  binding: unknown;
  inventory: DeploymentInventory;
}

/**
 * A deployment with every table created by its real owner, and one outstanding
 * item in each work category plus one row in each standing category.
 */
async function seeded(): Promise<Fixture> {
  const sqlite = openSqlite();
  const binding = sqliteUnitDatabase(sqlite);
  const storage = createD1Storage({
    binding: binding as never,
    domains: {
      ...createSignalStorageDomains(binding as never),
      ...createScheduleStorageDomains(binding as never),
    },
  });

  // --- runs: one suspended run, minted by the real runtime ------------------
  const { createWorkflow, createStep, runtime } = init(
    { storage },
    { startIdempotency: 'none', executionFence: 'none' },
  );
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
  await runtime.start('gated', { runId: 'abc_r1', inputData: {} });
  await storage.init();

  // --- approvals-waiting ----------------------------------------------------
  // Distinct runIds: the store's open-step uniqueness index refuses a second
  // OPEN record for the same (workflow, run, step), which is the invariant that
  // stops a re-suspension filing a duplicate.
  const approvals = new D1ApprovalStoreFactory(binding as never).store();
  await approvals.create(approval({ id: 'apr-open', status: 'pending' }));
  await approvals.create(
    approval({ id: 'apr-claimed', status: 'claimed', runId: 'run-1b' }),
  );
  await approvals.create(
    approval({ id: 'apr-done', status: 'approved', runId: 'run-2' }),
  );

  // --- resource-owners: one settled claim, one live reservation -------------
  await createResourceOwnershipSchema(binding as never);
  const owners = new D1ResourceOwnershipStore(binding as never);
  await owners.claim('run', 'abc_r1', { kind: 'human', id: 'ada' });
  await owners.reserveAll(
    [{ kind: 'thread', resourceId: 'thr-1' }],
    { kind: 'service', id: 'svc' },
    'tok-1',
  );

  // --- start-reservations ---------------------------------------------------
  const reservations = new StartIdempotencyStore(binding as never, {
    now: () => NOW,
  });
  await reservations.reserve({
    key: 'key-live',
    owner: { kind: 'human', id: 'ada' },
    targetKind: 'workflow',
    targetId: 'gated',
    mintRunId: () => 'abc_r2',
  });
  await reservations.reserve({
    key: 'key-settled',
    owner: { kind: 'human', id: 'ada' },
    targetKind: 'workflow',
    targetId: 'gated',
    mintRunId: () => 'abc_r3',
  });
  await reservations.settleRun('abc_r3');

  // --- signal-subscriptions -------------------------------------------------
  await new D1SubscriptionStoreFactory(binding as never, {
    uuid: () => 'sub-1',
  })
    .store()
    .subscribe({
      providerId: 'github',
      externalResourceId: 'octo/repo#1',
      threadId: 'thr-1',
      resourceId: 'res-1',
    });

  // --- the rows whose writers are Mastra's own domains ----------------------
  // Inserted directly, into tables the production DDL above created: what this
  // suite pins is the READER's predicate against the real column names, and
  // driving a whole agent loop to park one background task would test the loop.
  const iso = new Date(NOW).toISOString();
  sqlite
    .prepare(
      `INSERT INTO mastra_background_tasks
         (id, tool_call_id, tool_name, agent_id, run_id, thread_id, resource_id,
          status, args, result, error, suspend_payload, retry_count,
          max_retries, timeout_ms, createdAt, startedAt, suspendedAt, completedAt)
       VALUES (?, 'tc', 'tool', 'agent', 'abc_r1', 'thr-1', 'res-1', ?, '{}',
               NULL, NULL, ?, 0, 0, 1000, ?, NULL, NULL, ?)`,
    )
    .run('bg-queued', 'pending', null, iso, null);
  sqlite
    .prepare(
      `INSERT INTO mastra_background_tasks
         (id, tool_call_id, tool_name, agent_id, run_id, thread_id, resource_id,
          status, args, result, error, suspend_payload, retry_count,
          max_retries, timeout_ms, createdAt, startedAt, suspendedAt, completedAt)
       VALUES (?, 'tc', 'tool', 'agent', 'abc_r1', 'thr-1', 'res-1', ?, '{}',
               NULL, NULL, ?, 0, 0, 1000, ?, NULL, ?, NULL)`,
    )
    .run(
      'bg-parked',
      'suspended',
      JSON.stringify({ [EXECUTION_FENCE_SUSPEND_KEY]: { state: 'draining' } }),
      iso,
      iso,
    );
  sqlite
    .prepare(
      `INSERT INTO mastra_background_tasks
         (id, tool_call_id, tool_name, agent_id, run_id, thread_id, resource_id,
          status, args, result, error, suspend_payload, retry_count,
          max_retries, timeout_ms, createdAt, startedAt, suspendedAt, completedAt)
       VALUES (?, 'tc', 'tool', 'agent', 'abc_r1', 'thr-1', 'res-1', ?, '{}',
               NULL, NULL, NULL, 0, 0, 1000, ?, NULL, NULL, ?)`,
    )
    .run('bg-done', 'completed', iso, iso);

  const past = new Date(NOW - 60_000).toISOString();
  const future = new Date(NOW + 3_600_000).toISOString();
  const insertNotification = (
    id: string,
    status: string,
    deliverAt: string | null,
  ): void => {
    sqlite
      .prepare(
        `INSERT INTO mastra_notifications
           (id, thread_id, source, kind, priority, status, summary, payload,
            resourceId, agentId, sourceId, dedupeKey, coalesceKey,
            coalescedCount, attributes, createdAt, updatedAt, deliverAt,
            summaryAt, deliveryReason, deliveryAttempts, lastDeliveryAttemptAt,
            lastDeliveryError, deliveredSignalId, summarySignalId, deliveredAt,
            seenAt, dismissedAt, archivedAt, discardedAt, metadata,
            insertionOrdinal)
         VALUES (?, 'thr-1', 'src', 'kind', 'medium', ?, 'summary', NULL,
                 'res-1', 'agent', NULL, NULL, NULL, 1, NULL, ?, ?, ?, NULL,
                 NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL)`,
      )
      .run(id, status, past, past, deliverAt);
  };
  insertNotification('ntf-due', 'pending', past);
  insertNotification('ntf-later', 'pending', future);
  insertNotification('ntf-never', 'pending', null);
  insertNotification('ntf-delivered', 'delivered', past);

  const insertTrigger = (id: string, outcome: string): void => {
    sqlite
      .prepare(
        `INSERT INTO mastra_schedule_triggers
           (id, scheduleId, runId, scheduledFireAt, actualFireAt, outcome,
            error, triggerKind, parentTriggerId, metadata)
         VALUES (?, 'sch-1', NULL, ?, ?, ?, NULL, 'schedule-fire', NULL, NULL)`,
      )
      .run(id, NOW, NOW, outcome);
  };
  insertTrigger('trg-deferred', 'deferred');
  insertTrigger('trg-started', 'started');
  insertTrigger('trg-skipped', 'skipped');

  sqlite
    .prepare(
      `INSERT INTO mastra_schedules
         (id, target, cron, timezone, status, nextFireAt, lastFireAt, lastRunId,
          createdAt, updatedAt, metadata, ownerType, ownerId, creatorRole,
          deletionRequestedAt)
       VALUES ('sch-1', ?, '* * * * *', NULL, 'active', ?, NULL, NULL, ?, ?,
               NULL, NULL, NULL, NULL, NULL)`,
    )
    .run(
      JSON.stringify({ type: 'workflow', workflowId: 'gated' }),
      NOW + 60_000,
      NOW,
      NOW,
    );

  return {
    sqlite,
    binding,
    inventory: new DeploymentInventory(binding as InventoryDatabase, {
      now: () => NOW,
    }),
  };
}

/** Page a category to exhaustion, following its own cursors. */
async function drain(
  inventory: DeploymentInventory,
  category: InventoryCategory,
  limit?: number,
): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  for (let pass = 0; pass < 100; pass += 1) {
    const page = await inventory.read(category, {
      ...(cursor === undefined ? {} : { cursor }),
      ...(limit === undefined ? {} : { limit }),
    });
    keys.push(...page.entries.map((entry) => JSON.stringify(entry.key)));
    if (page.cursor === undefined) return keys;
    cursor = page.cursor;
  }
  throw new Error(`inventory paging did not terminate for ${category}`);
}

describe('deployment drain inventory', () => {
  it('reports the index as a contract: every category, its class, what it cannot see, and how to read empty', async () => {
    // #given — an inventory over any database (the index is a constant: what
    // can be asked is not a function of what happens to be stored).
    const { inventory } = await seeded();

    // #when
    const index = inventory.index();

    // #then — work first, then standing, and each named exactly once.
    expect(index.categories.map((entry) => entry.category)).toEqual([
      ...WORK_CATEGORIES,
      'schedules',
      'signal-subscriptions',
    ]);
    // #then — the two states no query can see are DECLARED, not omitted.
    expect(index.unenumerable.map((entry) => entry.name)).toEqual([
      'run-owner-recovery-journal',
      'persisted-idle-signals',
    ]);
    // #then — and the rule an empty answer means something under.
    expect(index.drainProof.reachableFrom).toEqual(['draining']);
    expect(index.drainProof.proof).toMatch(/TWO consecutive full sweeps/);
  });

  it('reads each work category with the predicate its production writer settles on', async () => {
    // #given — one outstanding item per category beside settled siblings that
    // must NOT be reported: a decided approval, a spent reservation, a
    // completed background task, a delivered notification, settled fire
    // history, and a released ownership row.
    const { inventory } = await seeded();

    // #when / #then — runs: the suspended run, annotated with its owner.
    const runs = await inventory.read('runs');
    expect(runs.entries).toEqual([
      {
        key: ['gated', 'abc_r1'],
        detail: expect.objectContaining({
          status: 'suspended',
          owner: 'human:ada',
        }),
      },
    ]);
    expect(runs.count).toBe(1);

    // #then — approvals: pending AND claimed (both still undecided); never the
    // approved one.
    expect((await inventory.read('approvals-waiting')).entries).toEqual([
      {
        key: ['apr-claimed'],
        detail: expect.objectContaining({ status: 'claimed' }),
      },
      {
        key: ['apr-open'],
        detail: expect.objectContaining({ status: 'pending' }),
      },
    ]);

    // #then — deferred dispatches only: the started and skipped fires are
    // settled history in the same table, and counting them would keep this
    // category permanently non-empty.
    expect(
      (await inventory.read('schedule-deferred-dispatches')).entries.map(
        (entry) => entry.key,
      ),
    ).toEqual([['trg-deferred']]);

    // #then — notifications: only the DUE pending row is drainable work; the
    // future-dated and the never-due rows are reported as a total instead.
    const notifications = await inventory.read('pending-notifications');
    expect(notifications.entries.map((entry) => entry.key)).toEqual([
      ['thr-1', 'ntf-due'],
    ]);
    // `count` is this category's own rows (the DUE one); `notDue` describes
    // the pending rows the page deliberately excludes.
    expect(notifications.count).toBe(1);
    expect(notifications.totals).toEqual({ notDue: 2 });

    // #then — background tasks: nonterminal only, and the fence-parked one is
    // flagged so an operator can tell it from one awaiting a webhook.
    const tasks = await inventory.read('background-tasks');
    expect(
      tasks.entries.map((entry) => [entry.key[0], entry.detail.fenceSuspended]),
    ).toEqual([
      ['bg-parked', true],
      ['bg-queued', false],
    ]);
    expect(tasks.totals).toEqual({ fenceSuspended: 1 });

    // #then — ownership: the unsettled reservation, never the settled claim,
    // and never the reservation token itself.
    const ownersPage = await inventory.read('resource-owners');
    expect(ownersPage.entries).toEqual([
      {
        key: ['thread', 'thr-1'],
        detail: { owner_kind: 'service', owner_id: 'svc' },
      },
    ]);

    // #then — reservations: reserved/started, never terminal.
    expect(
      (await inventory.read('start-reservations')).entries.map(
        (entry) => entry.key,
      ),
    ).toEqual([['key-live']]);
  });

  it('reports standing configuration without asking a drain to empty it', async () => {
    // #given
    const { inventory } = await seeded();

    // #when
    const schedules = await inventory.read('schedules');
    const subscriptions = await inventory.read('signal-subscriptions');

    // #then — the schedule's trigger identity comes through, so a reconciling
    // operator sees WHAT it will fire, not just that it exists.
    expect(schedules.class).toBe('standing');
    expect(schedules.entries).toEqual([
      {
        key: ['sch-1'],
        detail: expect.objectContaining({
          status: 'active',
          targetType: 'workflow',
          targetId: 'gated',
        }),
      },
    ]);
    expect(subscriptions.class).toBe('standing');
    expect(subscriptions.entries).toEqual([
      {
        key: ['sub-1'],
        detail: expect.objectContaining({
          provider_id: 'github',
          thread_id: 'thr-1',
        }),
      },
    ]);
  });

  it('counts a snapshot it cannot classify as WORK rather than as finished', async () => {
    // #given — a snapshot row whose JSON is corrupt, beside one whose JSON is
    // valid but carries no status. Neither can be proven terminal.
    const { sqlite, inventory } = await seeded();
    const iso = new Date(NOW).toISOString();
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
           (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES (?, ?, NULL, ?, ?, ?)`,
      )
      .run('gated', 'abc_corrupt', '{not json', iso, iso);
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
           (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES (?, ?, NULL, ?, ?, ?)`,
      )
      .run('gated', 'abc_nostatus', '{"steps":{}}', iso, iso);

    // #when
    const runs = await inventory.read('runs');

    // #then — both are reported. A row nobody can prove is finished is exactly
    // the row a migration must not walk away from, and the alternative reading
    // would let a corrupt snapshot certify a deployment empty.
    expect(runs.entries.map((entry) => entry.key[1])).toEqual([
      'abc_corrupt',
      'abc_nostatus',
      'abc_r1',
    ]);
  });

  it('excludes a run whose terminal status still owes lifecycle cleanup', async () => {
    // #given — a 'timed_out' run with no cleanupCompletedAt (still compensating)
    // beside one that has completed cleanup. The retention purge draws the line
    // here; so must the drain proof, or the two disagree about what "finished"
    // means on the same deployment.
    const { sqlite, inventory } = await seeded();
    const iso = new Date(NOW).toISOString();
    const snapshot = (cleanup: string | null): string =>
      JSON.stringify({
        status: 'timed_out',
        requestContext: {
          'flowsafe.runLifecycle':
            cleanup === null
              ? {}
              : { terminal: { cleanupCompletedAt: cleanup } },
        },
      });
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
           (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES (?, ?, NULL, ?, ?, ?)`,
      )
      .run('gated', 'abc_cleaning', snapshot(null), iso, iso);
    sqlite
      .prepare(
        `INSERT INTO mastra_workflow_snapshot
           (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
         VALUES (?, ?, NULL, ?, ?, ?)`,
      )
      .run('gated', 'abc_cleaned', snapshot(iso), iso, iso);

    // #when
    const runs = await inventory.read('runs');

    // #then — mid-cleanup is work; cleanup-complete is not.
    expect(runs.entries.map((entry) => entry.key[1])).toEqual([
      'abc_cleaning',
      'abc_r1',
    ]);
  });

  it('reads a table that was never created as an EMPTY category, not a fault', async () => {
    // #given — a database with nothing in it at all: the state of a deployment
    // provisioned and never used. Every table here is created lazily by the
    // first feature that writes it.
    const sqlite = openSqlite();
    const inventory = new DeploymentInventory(
      sqliteUnitDatabase(sqlite) as InventoryDatabase,
      { now: () => NOW },
    );

    // #when — the whole sweep an operator would run.
    const pages = await inventory.sweep();

    // #then — every category answers, none throws, and no count is invented
    // for a table that does not exist.
    expect(pages.map((page) => page.category)).toEqual(ALL_CATEGORIES);
    expect(pages.every((page) => page.entries.length === 0)).toBe(true);
    expect(pages.every((page) => page.cursor === undefined)).toBe(true);
    expect(pages.every((page) => page.count === undefined)).toBe(true);
  });

  it('still reports runs when the ownership registry does not exist', async () => {
    // #given — a deployment whose runs exist but that never wired resource
    // ownership. The owner is an ANNOTATION, and losing an annotation must
    // never cost the category a drain proof depends on.
    const { sqlite, inventory } = await seeded();
    sqlite.exec(`DROP TABLE ${RESOURCE_OWNER_TABLE}`);

    // #when
    const runs = await inventory.read('runs');

    // #then — the run is still there, just unannotated.
    expect(runs.entries.map((entry) => entry.key[1])).toEqual(['abc_r1']);
    expect(runs.entries[0]?.detail.owner).toBeUndefined();
  });

  it('pages a keyset that neither skips a row nor returns one twice', async () => {
    // #given — more approvals than one page holds.
    const { binding, inventory } = await seeded();
    const approvals = new D1ApprovalStoreFactory(binding as never).store();
    for (let index = 0; index < 7; index += 1) {
      await approvals.create(
        approval({ id: `apr-p${index}`, runId: `run-p${index}` }),
      );
    }

    // #when — paged two at a time, following the inventory's own cursors.
    const paged = await drain(inventory, 'approvals-waiting', 2);

    // #then — identical to the single-page read, in the same order, with no
    // duplicates. A keyset that drifted would either strand rows (a drain that
    // never proves empty) or repeat them (a proof that never converges).
    const whole = await drain(inventory, 'approvals-waiting');
    expect(paged).toEqual(whole);
    expect(new Set(paged).size).toBe(paged.length);
    expect(paged.length).toBe(9);
  });

  it('offers a cursor only while a page is full, so an empty page IS the end', async () => {
    // #given — exactly the drain proof's observation: a work category whose
    // page did not fill.
    const { inventory } = await seeded();

    // #when
    const page = await inventory.read('runs', { limit: 50 });

    // #then — no cursor. A cursor here would make an operator page again and
    // read the same empty answer forever.
    expect(page.cursor).toBeUndefined();
    expect(page.count).toBe(1);
  });

  it('counts only on the first page of a sweep', async () => {
    // #given
    const { inventory } = await seeded();

    // #when — a continuation page.
    const first = await inventory.read('approvals-waiting', { limit: 1 });
    const second = await inventory.read('approvals-waiting', {
      limit: 1,
      ...(first.cursor === undefined ? {} : { cursor: first.cursor }),
    });

    // #then — the total is taken once, where a sweep starts; a caller already
    // paging has committed to walking the category.
    expect(first.count).toBe(2);
    expect(second.count).toBeUndefined();
  });

  it('refuses a malformed cursor instead of silently restarting the scan', async () => {
    // #given
    const { inventory } = await seeded();

    // #then — a cursor that is not JSON, and one shaped for a DIFFERENT
    // category, are both refused. Silently restarting would make a sweep
    // re-read rows it had counted and never reach the end, and an operator
    // waiting for two empty sweeps would wait forever without being told why.
    await expect(
      inventory.read('runs', { cursor: 'not-json' }),
    ).rejects.toBeInstanceOf(InvalidInventoryRequestError);
    await expect(
      inventory.read('runs', { cursor: '["only-one"]' }),
    ).rejects.toBeInstanceOf(InvalidInventoryRequestError);
    await expect(
      inventory.read('approvals-waiting', { cursor: '[1]' }),
    ).rejects.toBeInstanceOf(InvalidInventoryRequestError);
    await expect(inventory.read('runs', { limit: 0 })).rejects.toBeInstanceOf(
      InvalidInventoryRequestError,
    );
  });

  it('applies the storage prefix to Mastra tables and never to the flowsafe registries', async () => {
    // #given — a prefixed deployment. createD1Storage prefixes its own tables;
    // the approval, ownership, reservation, and subscription registries are
    // never prefixed, exactly as their production writers spell them.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const { statements, db } = recordingDatabase(binding);
    const inventory = new DeploymentInventory(db, {
      tablePrefix: 'p_',
      now: () => NOW,
    });

    // #when
    await inventory.sweep();

    // #then — the prefix lands on exactly the Mastra-owned tables.
    expect(
      statements.some((sql) => sql.includes('FROM p_mastra_workflow_snapshot')),
    ).toBe(true);
    expect(
      statements.some((sql) => sql.includes('FROM p_mastra_notifications')),
    ).toBe(true);
    expect(
      statements.some((sql) => sql.includes(`FROM p_${APPROVALS_TABLE}`)),
    ).toBe(false);
    expect(
      statements.some((sql) => sql.includes(`FROM ${APPROVALS_TABLE}`)),
    ).toBe(true);
    expect(
      statements.some((sql) => sql.includes(`FROM ${START_IDEMPOTENCY_TABLE}`)),
    ).toBe(true);
  });

  it('READ-ONLY PIN: a full sweep, paged to exhaustion, prepares nothing but SELECTs and changes nothing', async () => {
    // #given — a populated deployment, and a complete record of it. This is the
    // property the whole surface rests on: an operator runs this against the
    // database they are about to copy, so measuring it must not change it.
    //
    // No Durable Object appears anywhere in this test, and that is the pin for
    // "no DO storage key changes": the inventory's only seam is a database
    // (`InventoryDatabase`, which has no `run()` at all), so there is no object
    // for it to wake, no alarm for it to re-arm, and no key for it to write.
    const { sqlite, binding } = await seeded();
    const schemaBefore = schemaSnapshot(sqlite);
    const dataBefore = dataSnapshot(sqlite);
    const { statements, db } = recordingDatabase(binding);
    const inventory = new DeploymentInventory(db, { now: () => NOW });

    // #when — the index plus every category paged to the end, one row at a
    // time so every keyset continuation is exercised too.
    inventory.index();
    for (const category of ALL_CATEGORIES) {
      await drain(inventory, category, 1);
    }

    // #then — every statement it prepared was a SELECT. Not "no rows changed":
    // a CREATE TABLE IF NOT EXISTS on an existing table changes no rows either,
    // and is exactly the lazy-schema write this surface must not perform.
    expect(statements.length).toBeGreaterThan(ALL_CATEGORIES.length);
    expect(statements.filter((sql) => !/^\s*SELECT\b/i.test(sql))).toEqual([]);

    // #then — and the database is byte-identical: same schema objects, same
    // rows, same timestamps.
    expect(schemaSnapshot(sqlite)).toEqual(schemaBefore);
    expect(dataSnapshot(sqlite)).toEqual(dataBefore);
  });

  it('READ-ONLY PIN: an empty deployment is not given schema by being measured', async () => {
    // #given — the case a lazy `#ensureSchema` would quietly break: a database
    // with no tables at all. Every domain store in this package creates its
    // schema on first use, so a reader built on one of them would leave a
    // freshly provisioned deployment with tables it never had.
    const sqlite = openSqlite();
    const { statements, db } = recordingDatabase(sqliteUnitDatabase(sqlite));

    // #when
    await new DeploymentInventory(db, { now: () => NOW }).sweep();

    // #then — nothing was created.
    expect(
      sqlite.prepare("SELECT name FROM sqlite_master WHERE type='table'").all(),
    ).toEqual([]);
    expect(statements.filter((sql) => !/^\s*SELECT\b/i.test(sql))).toEqual([]);
  });

  it('keeps every statement inside D1 100-parameter budget on a maximum page', async () => {
    // #given — a page at INVENTORY_MAX_LIMIT, which is a real knob an operator
    // may turn. The owner annotation binds one parameter per row, so a 200-row
    // page would build a 200-parameter IN list — which real D1 REFUSES and
    // node:sqlite (32766 variables) accepts without complaint. That pairing is
    // the dangerous one: the suite stays green while the route answers a
    // generic 500 on the deployment an operator is trying to drain.
    const { sqlite, binding } = await seeded();
    const iso = new Date(NOW).toISOString();
    const snapshot = JSON.stringify({ status: 'suspended' });
    for (let index = 0; index < 199; index += 1) {
      const runId = `abc_p${String(index).padStart(3, '0')}`;
      sqlite
        .prepare(
          `INSERT INTO mastra_workflow_snapshot
             (workflow_name, run_id, resourceId, snapshot, createdAt, updatedAt)
           VALUES ('gated', ?, NULL, ?, ?, ?)`,
        )
        .run(runId, snapshot, iso, iso);
      sqlite
        .prepare(
          `INSERT INTO ${RESOURCE_OWNER_TABLE}
             (resource_kind, resource_id, owner_kind, owner_id, reservation_token)
           VALUES ('run', ?, 'human', 'ada', NULL)`,
        )
        .run(runId);
    }
    const { statements, bindings, db } = recordingDatabase(binding);

    // #when — the largest page the route will serve.
    const page = await new DeploymentInventory(db, { now: () => NOW }).read(
      'runs',
      { limit: 200 },
    );

    // #then — a full page, every row annotated: the chunking must not cost the
    // annotation, or "fixed" would mean "stopped looking".
    expect(page.entries).toHaveLength(200);
    expect(
      page.entries.filter((entry) => typeof entry.detail.owner !== 'string'),
    ).toEqual([]);

    // #then — and no statement exceeded the budget. Asserted over EVERY
    // statement rather than just the IN list, so a future reader that starts
    // binding per-row fails here too.
    expect(bindings.filter((entry) => entry.count > 100)).toEqual([]);

    // #then — the lookup really was split rather than merely shortened: 200
    // ids at 100 per statement is exactly two.
    expect(
      statements.filter(
        (sql) => sql.includes(RESOURCE_OWNER_TABLE) && sql.includes('IN ('),
      ),
    ).toHaveLength(2);
  });

  it('OWNERSHIP-ORDERING PIN: an in-flight run with no snapshot is visible under resource-owners', async () => {
    // #given — THE cross-module invariant the whole drain proof rests on, and
    // the one nothing else enforces: the run object RESERVES ownership before
    // it calls runtime.start (durable-object.ts #reserveRunOwner precedes
    // runtime.start) and SETTLES that reservation only AFTER a summary has
    // persisted (#settleRunOwnerBestEffort follows it).
    //
    // The engine writes a `running` snapshot inside runtime.start, so an
    // executing run is normally under `runs` too — but that write lands after
    // the reservation, and the unsettled reservation is what marks a start as
    // not-yet-durably-settled whatever the snapshot currently says. If
    // settlement ever moves ahead of the persisted summary, an operator
    // sweeping a draining deployment reads a clean `resource-owners` while a
    // start is still in flight, locks, and the deployment taking over resumes
    // work that never stopped. This test fails the moment that ordering
    // changes.
    const sqlite = openSqlite();
    const binding = sqliteUnitDatabase(sqlite);
    const storage = createD1Storage({ binding: binding as never });
    await createResourceOwnershipSchema(binding as never);
    sqlite.exec(
      `CREATE TABLE flowsafe_deployment (
         id INTEGER PRIMARY KEY CHECK (id = 1),
         tenant_tag TEXT NOT NULL,
         provisioned_at TEXT NOT NULL
       )`,
    );
    sqlite
      .prepare(
        'INSERT INTO flowsafe_deployment (id, tenant_tag, provisioned_at) VALUES (1, ?, ?)',
      )
      .run('acme', new Date(NOW).toISOString());

    // A step that PARKS mid-execution, which is what makes the in-flight window
    // observable at all: without it the start returns before any assertion can
    // run, and the window this pins would never be open.
    let announceStarted: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      announceStarted = resolve;
    });
    let releaseStep: () => void = () => undefined;
    const held = new Promise<void>((resolve) => {
      releaseStep = resolve;
    });

    const fence = new ExecutionFenceStore(binding as never);
    await fence.seed('open');
    const reservations = new StartIdempotencyStore(binding as never);
    const buildRuntime = (): RunnerRuntime => {
      const { createWorkflow, createStep, runtime } = init(
        { storage },
        { executionFence: fence, startIdempotency: reservations },
      );
      const gate = createStep({
        id: 'gate',
        inputSchema: z.object({}),
        outputSchema: z.object({}),
        suspendSchema: z.object({ reason: z.string() }),
        resumeSchema: z.object({ go: z.boolean() }),
        execute: async ({ resumeData, suspend }) => {
          if (resumeData) return {};
          announceStarted();
          await held;
          return suspend({ reason: 'wait' });
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
    };

    interface OwnerEnv {
      owners: D1ResourceOwnershipStore;
      DEPLOYMENT_TENANT: string;
      DEPLOYMENT_IDENTITY_SECRET: string;
      DB: unknown;
    }
    class OwnerRunner extends DurableObjectRunner<OwnerEnv> {
      protected runOwnership(env: OwnerEnv): DurableObjectRunOwnershipStore {
        return env.owners;
      }
      protected runLifecycle(): { abandonApprovals: () => Promise<void> } {
        return { abandonApprovals: async () => undefined };
      }
      protected build(): RunnerRuntime {
        return buildRuntime();
      }
    }
    const secret = 'inventory-ownership-pin-secret-00001';
    const runner = new OwnerRunner(undefined, {
      owners: new D1ResourceOwnershipStore(binding as never),
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET: secret,
      DB: binding,
    });
    const runId = 'abc_inflight';
    const post = (path: string, body: unknown): Request =>
      new Request(`http://do${path}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          [DEPLOYMENT_IDENTITY_HEADER]: secret,
          [EXECUTION_PRINCIPAL_HEADER]: JSON.stringify({
            kind: 'human',
            id: 'ada',
            role: 'operator',
          }),
        },
        body: JSON.stringify(body),
      });
    const inventory = new DeploymentInventory(binding as InventoryDatabase, {
      now: () => NOW,
    });

    // #when — the start is IN FLIGHT: ownership reserved, step executing, and
    // nothing persisted yet.
    const start = runner.fetch(
      post('/runs', { workflowId: 'gated', runId, inputData: {} }),
    );
    await started;

    // #then — the reservation is UNSETTLED while the start is in flight. This
    // is the assertion the invariant lives in: it fails if settlement moves
    // ahead of the persisted summary.
    const inFlight = await inventory.read('resource-owners');
    expect(inFlight.entries).toEqual([
      {
        key: ['run', runId],
        detail: { owner_kind: 'human', owner_id: 'ada' },
      },
    ]);
    expect(inFlight.count).toBe(1);

    // #then — and the executing run is not hidden from `runs` either: the
    // engine's own `running` snapshot is already there. Recorded because the
    // two categories overlap DURING execution and diverge only at settlement,
    // which is what the next step asserts.
    expect(
      (await inventory.read('runs')).entries.map((entry) => [
        entry.key[1],
        entry.detail.status,
      ]),
    ).toEqual([[runId, 'running']]);

    // #when — the step reaches its first suspend, so a summary persists and the
    // reservation settles.
    releaseStep();
    const summary = (await (await start).json()) as { status: string };
    expect(summary.status).toBe('suspended');

    // #then — the categories DIVERGE at settlement: `runs` still carries the
    // suspended run, and the settled reservation has left `resource-owners`.
    expect(
      (await inventory.read('runs')).entries.map((entry) => entry.key),
    ).toEqual([['gated', runId]]);
    expect((await inventory.read('resource-owners')).entries).toEqual([]);

    // #when — the run reaches a terminal state.
    const resumed = await runner.fetch(
      post(`/runs/gated/${runId}/resume`, {
        step: 'gate',
        resumeData: { go: true },
        requestedBy: 'reviewer-1',
        requestedByKind: 'human',
      }),
    );
    expect(resumed.status).toBe(200);

    // #then — both categories are empty, which is what a drain proof reads as
    // permission to lock.
    expect((await inventory.read('runs')).entries).toEqual([]);
    expect((await inventory.read('resource-owners')).entries).toEqual([]);
  });
});
