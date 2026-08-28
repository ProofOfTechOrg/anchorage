// SPDX-License-Identifier: Apache-2.0
/// <reference types="@cloudflare/workers-types" />

import { D1FleetStateDatabase } from '../../src/d1-fleet-state-database.js';
import { applyMigrationsWithLedger } from '../../src/migration-ledger.js';

interface Env {
  DB: D1Database;
}

const LEDGER = 'anchorage_fleet_migrations';

function errorShape(error: unknown): { name: string; message: string } {
  return error instanceof Error
    ? { name: error.name, message: error.message }
    : { name: typeof error, message: String(error) };
}

async function ensureLedger(db: D1Database): Promise<void> {
  await db
    .prepare(`CREATE TABLE IF NOT EXISTS ${LEDGER} (
      version INTEGER PRIMARY KEY,
      sql_sha256 TEXT NOT NULL,
      applied_at TEXT NOT NULL
    )`)
    .run();
}

async function atomicRollback(db: D1Database): Promise<unknown> {
  const version = 1;
  const trigger = 'fail_atomic_ledger_insert';
  await ensureLedger(db);
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS migration_atomic_values (value TEXT NOT NULL)`,
    )
    .run();
  await db.prepare(`DELETE FROM migration_atomic_values`).run();
  await db.prepare(`DELETE FROM ${LEDGER}`).run();
  await db.prepare(`DROP TRIGGER IF EXISTS ${trigger}`).run();
  await db
    .prepare(
      `CREATE TRIGGER ${trigger}
       BEFORE INSERT ON ${LEDGER}
       WHEN NEW.version = ${version}
       BEGIN
         SELECT RAISE(ABORT, 'forced ledger insert failure');
       END`,
    )
    .run();
  let failure: unknown;
  try {
    await applyMigrationsWithLedger(new D1FleetStateDatabase(db), [
      {
        version,
        sql: `INSERT INTO migration_atomic_values (value) VALUES ('must-rollback')`,
      },
    ]);
  } catch (error) {
    failure = errorShape(error);
  } finally {
    await db.prepare(`DROP TRIGGER IF EXISTS ${trigger}`).run();
  }
  const values = await db
    .prepare(`SELECT COUNT(*) AS count FROM migration_atomic_values`)
    .first<{ count: number }>();
  const ledger = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${LEDGER} WHERE version = ?`)
    .bind(version)
    .first<{ count: number }>();
  return { failure, values: values?.count, ledger: ledger?.count };
}

async function concurrentApplication(db: D1Database): Promise<unknown> {
  const version = 1;
  await ensureLedger(db);
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS migration_concurrent_values (value TEXT NOT NULL)`,
    )
    .run();
  await db.prepare(`DELETE FROM migration_concurrent_values`).run();
  await db.prepare(`DELETE FROM ${LEDGER}`).run();
  const migration = {
    version,
    sql: `INSERT INTO migration_concurrent_values (value) VALUES ('once')`,
  };
  const outcomes = await Promise.allSettled(
    Array.from({ length: 12 }, () =>
      applyMigrationsWithLedger(new D1FleetStateDatabase(db), [migration]),
    ),
  );
  const values = await db
    .prepare(`SELECT COUNT(*) AS count FROM migration_concurrent_values`)
    .first<{ count: number }>();
  const ledger = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${LEDGER} WHERE version = ?`)
    .bind(version)
    .first<{ count: number }>();
  return {
    fulfilled: outcomes.filter(({ status }) => status === 'fulfilled').length,
    rejected: outcomes
      .filter(
        (outcome): outcome is PromiseRejectedResult =>
          outcome.status === 'rejected',
      )
      .map(({ reason }) => errorShape(reason)),
    values: values?.count,
    ledger: ledger?.count,
  };
}

async function changedHistoricalSql(db: D1Database): Promise<unknown> {
  const version = 1;
  await ensureLedger(db);
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS migration_history_values (value TEXT NOT NULL)`,
    )
    .run();
  await db.prepare(`DELETE FROM migration_history_values`).run();
  await db.prepare(`DELETE FROM ${LEDGER}`).run();
  await applyMigrationsWithLedger(new D1FleetStateDatabase(db), [
    {
      version,
      sql: `INSERT INTO migration_history_values (value) VALUES ('original')`,
    },
  ]);
  let failure: unknown;
  try {
    await applyMigrationsWithLedger(new D1FleetStateDatabase(db), [
      {
        version,
        sql: `INSERT INTO migration_history_values (value) VALUES ('changed')`,
      },
    ]);
  } catch (error) {
    failure = errorShape(error);
  }
  const values = await db
    .prepare(`SELECT value FROM migration_history_values ORDER BY rowid`)
    .all<{ value: string }>();
  return { failure, values: values.results.map(({ value }) => value) };
}

async function commitAcrossBoundary(db: D1Database): Promise<unknown> {
  const version = 1;
  await ensureLedger(db);
  await db
    .prepare(
      `CREATE TABLE IF NOT EXISTS migration_boundary_values (value TEXT NOT NULL)`,
    )
    .run();
  await db.prepare(`DELETE FROM migration_boundary_values`).run();
  await db.prepare(`DELETE FROM ${LEDGER}`).run();
  await applyMigrationsWithLedger(new D1FleetStateDatabase(db), [
    {
      version,
      sql: `INSERT INTO migration_boundary_values (value) VALUES ('durable')`,
    },
  ]);
  return { committed: true };
}

async function readAcrossBoundary(db: D1Database): Promise<unknown> {
  const table = await db
    .prepare(
      `SELECT COUNT(*) AS count FROM sqlite_master
       WHERE type = 'table' AND name = 'migration_boundary_values'`,
    )
    .first<{ count: number }>();
  const values = await db
    .prepare(`SELECT value FROM migration_boundary_values ORDER BY rowid`)
    .all<{ value: string }>();
  const ledger = await db
    .prepare(`SELECT COUNT(*) AS count FROM ${LEDGER} WHERE version = 1`)
    .first<{ count: number }>();
  return {
    table: table?.count,
    values: values.results.map(({ value }) => value),
    ledger: ledger?.count,
  };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== 'POST' || url.pathname !== '/migration-ledger') {
      return new Response('not found', { status: 404 });
    }
    const body = (await request.json()) as { action?: unknown };
    try {
      switch (body.action) {
        case 'atomic-rollback':
          return Response.json(await atomicRollback(env.DB));
        case 'concurrent-application':
          return Response.json(await concurrentApplication(env.DB));
        case 'changed-history':
          return Response.json(await changedHistoricalSql(env.DB));
        case 'commit-boundary':
          return Response.json(await commitAcrossBoundary(env.DB));
        case 'read-boundary':
          return Response.json(await readAcrossBoundary(env.DB));
        default:
          return Response.json({ error: 'unknown action' }, { status: 400 });
      }
    } catch (error) {
      return Response.json({ error: errorShape(error) }, { status: 500 });
    }
  },
} satisfies ExportedHandler<Env>;
