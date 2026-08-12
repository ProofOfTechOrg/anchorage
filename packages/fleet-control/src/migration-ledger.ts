// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import type { D1Migration } from './types.js';

const LEDGER = 'anchorage_fleet_migrations';

export interface MigrationDatabase {
  query(
    sql: string,
    bindings?: readonly string[],
  ): Promise<readonly Readonly<Record<string, unknown>>[]>;
  batch(
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly string[];
    }[],
  ): Promise<void>;
}

function migrationDigest(migration: D1Migration): string {
  return createHash('sha256').update(migration.sql).digest('hex');
}

export function d1MigrationHistoryDigest(
  migrations: readonly D1Migration[],
): string {
  assertDeclaredHistory(migrations);
  return createHash('sha256')
    .update(
      JSON.stringify(
        migrations.map((migration) => ({
          version: migration.version,
          sqlSha256: migrationDigest(migration),
          rollbackCompatible: migration.rollbackCompatible === true,
        })),
      ),
    )
    .digest('hex');
}

function assertDeclaredHistory(migrations: readonly D1Migration[]): void {
  for (const [index, migration] of migrations.entries()) {
    const expectedVersion = index + 1;
    if (migration.version !== expectedVersion) {
      throw new Error(
        `D1 migration history must be contiguous from version 1; expected version ${expectedVersion}, found ${migration.version}`,
      );
    }
  }
}

async function verifyPersistedHistory(
  database: MigrationDatabase,
  migrations: readonly D1Migration[],
): Promise<number> {
  const rows = await database.query(
    `SELECT version, sql_sha256 FROM ${LEDGER} ORDER BY version`,
  );
  if (rows.length > migrations.length) {
    throw new Error(
      `D1 migration ledger contains ${rows.length} versions but the declared history contains ${migrations.length}`,
    );
  }
  for (const [index, row] of rows.entries()) {
    const migration = migrations[index];
    const version = row?.version;
    if (!migration || version !== migration.version) {
      throw new Error(
        `D1 migration ledger is not the declared contiguous history at version ${index + 1}`,
      );
    }
    if (row.sql_sha256 !== migrationDigest(migration)) {
      throw new Error(
        `D1 migration ${migration.version} was already applied with different SQL`,
      );
    }
  }
  return rows.length;
}

export async function applyMigrationsWithLedger(
  database: MigrationDatabase,
  migrations: readonly D1Migration[],
): Promise<void> {
  assertDeclaredHistory(migrations);
  await database.query(`CREATE TABLE IF NOT EXISTS ${LEDGER} (
    version INTEGER PRIMARY KEY,
    sql_sha256 TEXT NOT NULL,
    applied_at TEXT NOT NULL
  )`);
  while (true) {
    const appliedCount = await verifyPersistedHistory(database, migrations);
    const migration = migrations[appliedCount];
    if (!migration) return;
    try {
      await database.batch([
        { sql: migration.sql },
        {
          sql: `INSERT INTO ${LEDGER} (version, sql_sha256, applied_at) VALUES (?, ?, ?)`,
          bindings: [
            String(migration.version),
            migrationDigest(migration),
            new Date().toISOString(),
          ],
        },
      ]);
    } catch (cause) {
      const racedCount = await verifyPersistedHistory(database, migrations);
      if (racedCount > appliedCount) continue;
      throw new Error(`failed to apply D1 migration ${migration.version}`, {
        cause,
      });
    }
  }
}
