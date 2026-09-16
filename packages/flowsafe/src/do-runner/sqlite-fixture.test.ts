// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  openSqlite,
  type SqliteUnitDatabase,
  sqliteUnitDatabase,
} from '../../test-support/sqlite.js';
import type { ResourceOwnershipDatabase } from '../approval-api/resource-ownership.js';
import type { SignalDatabase } from '../signals/d1-shared.js';
import type { ExecutionFenceDatabase } from './execution-fence.js';
import type { InitialAdmissionDatabase } from './fenced-workflow-capability.js';
import type { SnapshotDatabase } from './workflow-snapshot-row.js';

// The declared return of sqliteUnitDatabase is what the `as` assertion in each
// suite that drives it is checked against. These erased assertions hold the
// facade against the D1 subsets those suites name, so a narrowing of the facade
// fails here rather than at the assertion sites. `ScheduleDatabase` is an alias
// of `SignalDatabase` (schedules-d1.ts:87). Same technique as
// test-support/d1-type-compatibility.ts, which holds the D1 side of this seam.
type AssertTrue<T extends true> = T;
type _FacadeSatisfiesSignalDatabase = AssertTrue<
  SqliteUnitDatabase extends SignalDatabase ? true : false
>;
type _FacadeSatisfiesSnapshotDatabase = AssertTrue<
  SqliteUnitDatabase extends SnapshotDatabase ? true : false
>;
type _FacadeSatisfiesInitialAdmissionDatabase = AssertTrue<
  SqliteUnitDatabase extends InitialAdmissionDatabase ? true : false
>;
type _FacadeSatisfiesExecutionFenceDatabase = AssertTrue<
  SqliteUnitDatabase extends ExecutionFenceDatabase ? true : false
>;
type _FacadeSatisfiesResourceOwnershipDatabase = AssertTrue<
  SqliteUnitDatabase extends ResourceOwnershipDatabase ? true : false
>;

describe('native SQLite unit batch transport', () => {
  it('returns actual rows, executes DML once and preserves changes through SELECT', async () => {
    const sqlite = openSqlite();
    sqlite.exec('CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT)');
    const db: InitialAdmissionDatabase = sqliteUnitDatabase(sqlite);
    const results = await db.batch([
      db.prepare("INSERT INTO fixture(value) VALUES ('initial') RETURNING *"),
      db.prepare('SELECT * FROM fixture WHERE id = 0'),
      db.prepare(
        "UPDATE fixture SET value = 'bound' WHERE changes() = 1 RETURNING *",
      ),
    ]);
    expect(results).toEqual([
      {
        success: true,
        results: [{ id: 1, value: 'initial' }],
        meta: { changes: 1 },
      },
      { success: true, results: [], meta: { changes: 1 } },
      {
        success: true,
        results: [{ id: 1, value: 'bound' }],
        meta: { changes: 1 },
      },
    ]);
    expect(sqlite.prepare('SELECT * FROM fixture').all()).toEqual([
      { id: 1, value: 'bound' },
    ]);
    expect(
      await db.prepare("INSERT INTO fixture(value) VALUES ('ordinary')").run(),
    ).toEqual({ success: true, results: [], meta: { changes: 1 } });
    expect(
      await db.prepare('SELECT value FROM fixture WHERE id = 2').all(),
    ).toEqual({ success: true, results: [{ value: 'ordinary' }], meta: {} });
  });

  it('rolls back preceding native writes when a later statement throws', async () => {
    const sqlite = openSqlite();
    sqlite.exec('CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT)');
    const db: InitialAdmissionDatabase = sqliteUnitDatabase(sqlite);
    await expect(
      db.batch([
        db.prepare("INSERT INTO fixture VALUES (1, 'first') RETURNING *"),
        db.prepare('INSERT INTO missing_fixture VALUES (2)'),
      ]),
    ).rejects.toThrow('no such table');
    expect(sqlite.prepare('SELECT * FROM fixture').all()).toEqual([]);
  });

  it('makes zero DML stop a following changes chain', async () => {
    const sqlite = openSqlite();
    sqlite.exec(
      "CREATE TABLE fixture (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO fixture VALUES (1, 'occupied')",
    );
    const db: InitialAdmissionDatabase = sqliteUnitDatabase(sqlite);
    expect(
      await db.batch([
        db.prepare(
          "INSERT INTO fixture VALUES (1, 'occupied') ON CONFLICT DO NOTHING RETURNING *",
        ),
        db.prepare(
          "UPDATE fixture SET value = 'wrong' WHERE changes() = 1 RETURNING *",
        ),
      ]),
    ).toEqual([
      { success: true, results: [], meta: { changes: 0 } },
      { success: true, results: [], meta: { changes: 0 } },
    ]);
    expect(sqlite.prepare('SELECT value FROM fixture').get()).toEqual({
      value: 'occupied',
    });
  });
});
