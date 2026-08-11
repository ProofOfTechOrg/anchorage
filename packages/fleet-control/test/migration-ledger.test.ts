// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  applyMigrationsWithLedger,
  d1MigrationHistoryDigest,
  type MigrationDatabase,
} from '../src/migration-ledger.js';

class TransactionalDatabase implements MigrationDatabase {
  readonly ledger = new Map<number, string>();
  sideEffects = 0;

  async query(
    sql: string,
    _bindings: readonly unknown[] = [],
  ): Promise<readonly Readonly<Record<string, unknown>>[]> {
    if (sql.includes('CREATE TABLE')) return [];
    return [...this.ledger.entries()]
      .sort(([left], [right]) => left - right)
      .map(([version, sql_sha256]) => ({ version, sql_sha256 }));
  }

  async batch(
    statements: readonly {
      readonly sql: string;
      readonly bindings?: readonly unknown[];
    }[],
  ): Promise<void> {
    const ledgerInsert = statements[1];
    if (!ledgerInsert?.bindings) throw new Error('missing ledger insert');
    const version = Number(ledgerInsert.bindings[0]);
    const digest = String(ledgerInsert.bindings[1]);
    if (this.ledger.has(version)) throw new Error('duplicate version');
    this.sideEffects += 1;
    this.ledger.set(version, digest);
  }
}

describe('D1 migration ledger', () => {
  it('binds the complete ordered SQL and rollback attestation into the history digest', () => {
    const history = [
      { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
      {
        version: 2,
        sql: 'ALTER TABLE example ADD COLUMN value TEXT',
        rollbackCompatible: true as const,
      },
    ];
    const digest = d1MigrationHistoryDigest(history);
    expect(digest).toMatch(/^[a-f0-9]{64}$/u);
    expect(
      d1MigrationHistoryDigest([
        { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
        {
          version: 2,
          sql: 'ALTER TABLE example ADD COLUMN value TEXT',
        },
      ]),
    ).not.toBe(digest);
    expect(() =>
      d1MigrationHistoryDigest([
        { version: 2, sql: 'CREATE TABLE example (id TEXT)' },
      ]),
    ).toThrow(/expected version 1, found 2/);
  });

  it('does not repeat committed SQL after a control-plane crash and retry', async () => {
    const database = new TransactionalDatabase();
    const migrations = [
      {
        version: 1,
        sql: 'ALTER TABLE example ADD COLUMN value TEXT',
      },
    ];

    await applyMigrationsWithLedger(database, migrations);
    await applyMigrationsWithLedger(database, migrations);

    expect(database.sideEffects).toBe(1);
    expect(database.ledger.get(1)).toMatch(/^[a-f0-9]{64}$/);
  });

  it('rejects changed SQL for an already committed version', async () => {
    const database = new TransactionalDatabase();
    await applyMigrationsWithLedger(database, [
      {
        version: 1,
        sql: 'CREATE TABLE example (id TEXT)',
      },
    ]);

    await expect(
      applyMigrationsWithLedger(database, [
        {
          version: 1,
          sql: 'CREATE TABLE different (id TEXT)',
        },
      ]),
    ).rejects.toThrow(/different SQL/);
    expect(database.sideEffects).toBe(1);
  });

  it('rejects missing and extra persisted history before applying new SQL', async () => {
    const history = [
      { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
      { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      { version: 3, sql: 'ALTER TABLE example ADD COLUMN note TEXT' },
    ];

    const missing = new TransactionalDatabase();
    await applyMigrationsWithLedger(missing, history.slice(0, 2));
    missing.ledger.delete(1);
    await expect(applyMigrationsWithLedger(missing, history)).rejects.toThrow(
      /not the declared contiguous history at version 1/,
    );
    expect(missing.sideEffects).toBe(2);

    const extra = new TransactionalDatabase();
    await applyMigrationsWithLedger(extra, history);
    await expect(
      applyMigrationsWithLedger(extra, history.slice(0, 2)),
    ).rejects.toThrow(/ledger contains 3 versions/);
    expect(extra.sideEffects).toBe(3);
  });

  it('checks every historical digest before applying the next version', async () => {
    const database = new TransactionalDatabase();
    const history = [
      { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
      { version: 2, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      { version: 3, sql: 'ALTER TABLE example ADD COLUMN note TEXT' },
    ];
    await applyMigrationsWithLedger(database, history.slice(0, 2));
    database.ledger.set(1, '0'.repeat(64));

    await expect(applyMigrationsWithLedger(database, history)).rejects.toThrow(
      /migration 1 was already applied with different SQL/,
    );
    expect(database.sideEffects).toBe(2);
    expect(database.ledger.has(3)).toBe(false);
  });

  it('rejects a noncontiguous declared history before touching the database', async () => {
    const database = new TransactionalDatabase();
    await expect(
      applyMigrationsWithLedger(database, [
        { version: 1, sql: 'CREATE TABLE example (id TEXT)' },
        { version: 3, sql: 'ALTER TABLE example ADD COLUMN value TEXT' },
      ]),
    ).rejects.toThrow(/expected version 2, found 3/);
    expect(database.sideEffects).toBe(0);
  });
});
