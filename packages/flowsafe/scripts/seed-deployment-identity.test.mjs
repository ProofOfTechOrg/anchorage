// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import {
  DeploymentIdentityError,
  readDeploymentIdentity,
  seedDeploymentIdentity,
} from '../src/do-runner/deployment-identity.js';
import { openSqlite, sqliteUnitDatabase } from '../test-support/sqlite.js';
import {
  parseProvisioningArguments,
  provisionDeploymentIdentity,
  wranglerTargetArguments,
} from './seed-deployment-identity.mjs';

const OPTIONS = {
  database: 'flowsafe-acme',
  tag: 'acme',
  target: '--remote',
};
const SQL = `CREATE TABLE flowsafe_deployment (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tenant_tag TEXT NOT NULL,
  provisioned_at TEXT NOT NULL
)`;
const COLUMNS = [
  { name: 'id', type: 'INTEGER', notnull: 0, pk: 1 },
  { name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 },
  { name: 'provisioned_at', type: 'TEXT', notnull: 1, pk: 0 },
];

function databaseQuery(initialTables = [], ownerTag = 'acme') {
  const tables = [...initialTables];
  let seeded = initialTables.some((row) => row.name === 'flowsafe_deployment');
  let storedOwner = seeded ? (ownerTag ?? undefined) : undefined;
  const mutations = [];
  return {
    mutations,
    addTable: (row) => tables.push(row),
    query: async (statement) => {
      if (statement.startsWith('SELECT name, sql')) {
        const applicationTables = tables.filter(
          (row) => row.name !== 'flowsafe_deployment',
        );
        return seeded
          ? [{ name: 'flowsafe_deployment', sql: SQL }, ...applicationTables]
          : applicationTables;
      }
      if (statement.startsWith('CREATE TABLE')) {
        mutations.push(statement);
        seeded = true;
        return [];
      }
      if (statement.startsWith('INSERT OR IGNORE')) {
        mutations.push(statement);
        const blocking = tables.filter(
          (row) =>
            row.name !== 'flowsafe_deployment' &&
            row.name !== '_cf_KV' &&
            row.name !== '_cf_METADATA' &&
            !row.name.startsWith('sqlite_'),
        );
        if (blocking.length === 0) {
          storedOwner ??= statement.match(/SELECT 1, '([^']+)'/)?.[1];
        }
        return [];
      }
      if (statement.startsWith('SELECT sql')) return [{ sql: SQL }];
      if (statement.startsWith('PRAGMA')) return COLUMNS;
      if (statement.startsWith('SELECT id')) {
        return storedOwner ? [{ id: 1, tenant_tag: storedOwner }] : [];
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };
}

function sqliteQuery(sqlite, beforeExecute = () => undefined) {
  return async (statement) => {
    await beforeExecute(statement);
    if (
      statement.startsWith('CREATE TABLE') ||
      statement.startsWith('INSERT OR IGNORE')
    ) {
      sqlite.exec(statement);
      return [];
    }
    return sqlite.prepare(statement).all();
  };
}

function sentinelSnapshot(sqlite) {
  const schema = sqlite
    .prepare(
      `SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'flowsafe_deployment'`,
    )
    .get();
  return {
    sql: schema.sql.replace(/\s+/g, ' ').trim(),
    columns: sqlite.prepare('PRAGMA table_info(flowsafe_deployment)').all(),
    owner: sqlite
      .prepare(
        'SELECT id, tenant_tag, provisioned_at FROM flowsafe_deployment ORDER BY id',
      )
      .all()
      .map(({ id, tenant_tag }) => ({ id, tenant_tag })),
  };
}

async function rejectedError(action) {
  try {
    await action();
  } catch (error) {
    return error;
  }
  throw new Error('expected operation to reject');
}

describe('deployment identity provisioning CLI', () => {
  it('requires an explicit database, valid tag, and execution target', () => {
    expect(
      parseProvisioningArguments([
        '--',
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--remote',
      ]),
    ).toEqual(OPTIONS);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'ACME',
        '--remote',
      ]),
    ).toThrow(/must match/);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
      ]),
    ).toThrow(/Usage/);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--remote',
        '--preview',
      ]),
    ).toThrow(/choose exactly one execution target/);
  });

  it.each([
    ['--local', ['--local']],
    ['--remote', ['--remote']],
    ['--preview', ['--remote', '--preview']],
  ])('maps logical target %s to Wrangler arguments', (target, expected) => {
    expect(wranglerTargetArguments(target)).toEqual(expected);
  });

  it('rejects an unsafe tag before rendering or executing protocol SQL', async () => {
    const statements = [];
    await expect(
      provisionDeploymentIdentity(
        { ...OPTIONS, tag: "acme' OR 1 = 1 --" },
        async (statement) => {
          statements.push(statement);
          return [];
        },
      ),
    ).rejects.toThrow(/must match/);
    expect(statements).toEqual([]);
  });

  it('seeds a fresh database and verifies the strict sentinel', async () => {
    const fake = databaseQuery();
    await provisionDeploymentIdentity(OPTIONS, fake.query);
    expect(fake.mutations).toHaveLength(2);
    expect(fake.mutations[0]).toMatch(/^CREATE TABLE/);
    expect(fake.mutations[1]).toMatch(/^INSERT OR IGNORE/);
  });

  it('seeds the exact schema accepted by the runtime guard', async () => {
    const sqlite = openSqlite();
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(sqlite));

    await expect(
      readDeploymentIdentity(sqliteUnitDatabase(sqlite)),
    ).resolves.toBe('acme');
  });

  it('keeps the runtime and packed-CLI adapters on the same protocol schema and idempotency path', async () => {
    const runtimeSqlite = openSqlite();
    const cliSqlite = openSqlite();
    await seedDeploymentIdentity(sqliteUnitDatabase(runtimeSqlite), 'acme');
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(cliSqlite));

    expect(sentinelSnapshot(runtimeSqlite)).toEqual(
      sentinelSnapshot(cliSqlite),
    );
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(runtimeSqlite));
    await seedDeploymentIdentity(sqliteUnitDatabase(cliSqlite), 'acme');
    expect(
      await readDeploymentIdentity(sqliteUnitDatabase(runtimeSqlite)),
    ).toBe('acme');
    expect(await readDeploymentIdentity(sqliteUnitDatabase(cliSqlite))).toBe(
      'acme',
    );
  });

  it.each([
    `CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY,
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    )`,
    `CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL,
      note TEXT
    )`,
  ])('rejects malformed ownership schema identically through both adapters', async (ddl) => {
    const runtimeSqlite = openSqlite();
    const cliSqlite = openSqlite();
    runtimeSqlite.exec(ddl);
    cliSqlite.exec(ddl);

    const runtimeError = await rejectedError(() =>
      seedDeploymentIdentity(sqliteUnitDatabase(runtimeSqlite), 'acme'),
    );
    const cliError = await rejectedError(() =>
      provisionDeploymentIdentity(OPTIONS, sqliteQuery(cliSqlite)),
    );
    expect(runtimeError).toBeInstanceOf(DeploymentIdentityError);
    expect(cliError).toBeInstanceOf(DeploymentIdentityError);
    expect(cliError.message).toBe(runtimeError.message);
  });

  it('completes an exact empty sentinel left by an interrupted seed', async () => {
    const fake = databaseQuery(
      [{ name: 'flowsafe_deployment', sql: SQL }],
      null,
    );

    await provisionDeploymentIdentity(OPTIONS, fake.query);

    expect(fake.mutations).toHaveLength(1);
    expect(fake.mutations[0]).toMatch(/^INSERT OR IGNORE/);
  });

  it('refuses an unowned database with any application table', async () => {
    const fake = databaseQuery([{ name: 'starter_actions', sql: 'CREATE' }]);
    await expect(
      provisionDeploymentIdentity(OPTIONS, fake.query),
    ).rejects.toThrow(/provision a fresh database/);
    expect(fake.mutations).toEqual([]);
  });

  it.each([
    '_cf_customer_data',
    '_cf_METADATA_backup',
    '_cf_metadata',
    'sqliteX_application',
  ])('refuses near-system application table %s', async (name) => {
    const fake = databaseQuery([{ name, sql: 'CREATE' }]);
    await expect(
      provisionDeploymentIdentity(OPTIONS, fake.query),
    ).rejects.toThrow(/provision a fresh database/);
  });

  it.each([
    '_cf_KV',
    '_cf_METADATA',
  ])('allows the exact D1-owned %s table', async (name) => {
    const fake = databaseQuery([{ name, sql: 'CREATE' }]);
    await provisionDeploymentIdentity(OPTIONS, fake.query);
    expect(fake.mutations).toHaveLength(2);
  });

  it('refuses a table created between scan and the conditional insert', async () => {
    const fake = databaseQuery();
    const query = async (statement) => {
      if (statement.startsWith('INSERT OR IGNORE')) {
        fake.addTable({ name: 'raced_application', sql: 'CREATE' });
      }
      return fake.query(statement);
    };
    await expect(provisionDeploymentIdentity(OPTIONS, query)).rejects.toThrow(
      /raced_application/,
    );
  });

  it('preserves the scan-to-insert race refusal through both execution adapters', async () => {
    const runtimeSqlite = openSqlite();
    const cliSqlite = openSqlite();
    const runtimeDb = sqliteUnitDatabase(runtimeSqlite);
    let runtimeRaced = false;
    const racingRuntime = {
      prepare(statement) {
        const prepared = runtimeDb.prepare(statement);
        if (!statement.startsWith('INSERT OR IGNORE')) return prepared;
        let bound = prepared;
        const wrapped = {
          bind(...values) {
            bound = prepared.bind(...values);
            return wrapped;
          },
          async run() {
            if (!runtimeRaced) {
              runtimeRaced = true;
              runtimeSqlite.exec(
                'CREATE TABLE raced_application (id TEXT PRIMARY KEY)',
              );
            }
            return bound.run();
          },
          all: () => bound.all(),
        };
        return wrapped;
      },
    };
    let cliRaced = false;
    const racingCliQuery = sqliteQuery(cliSqlite, (statement) => {
      if (statement.startsWith('INSERT OR IGNORE') && !cliRaced) {
        cliRaced = true;
        cliSqlite.exec('CREATE TABLE raced_application (id TEXT PRIMARY KEY)');
      }
    });

    const runtimeError = await rejectedError(() =>
      seedDeploymentIdentity(racingRuntime, 'acme'),
    );
    const cliError = await rejectedError(() =>
      provisionDeploymentIdentity(OPTIONS, racingCliQuery),
    );
    expect(runtimeError).toBeInstanceOf(DeploymentIdentityError);
    expect(cliError).toBeInstanceOf(DeploymentIdentityError);
    expect(runtimeError.message).toMatch(/raced_application/);
    expect(cliError.message).toMatch(/raced_application/);
    await expect(readDeploymentIdentity(runtimeDb)).resolves.toBeUndefined();
    await expect(
      readDeploymentIdentity(sqliteUnitDatabase(cliSqlite)),
    ).resolves.toBeUndefined();
  });

  it('refuses to complete an empty sentinel beside application tables', async () => {
    const fake = databaseQuery(
      [
        { name: 'flowsafe_deployment', sql: SQL },
        { name: 'starter_actions', sql: 'CREATE' },
      ],
      null,
    );
    await expect(
      provisionDeploymentIdentity(OPTIONS, fake.query),
    ).rejects.toThrow(/provision a fresh database/);
    expect(fake.mutations).toEqual([]);
  });

  it('refuses to re-home an already owned database', async () => {
    const fake = databaseQuery([{ name: 'flowsafe_deployment' }], 'other');
    await expect(
      provisionDeploymentIdentity(OPTIONS, fake.query),
    ).rejects.toThrow(/already belongs to deployment 'other'/);
    expect(fake.mutations).toEqual([]);
  });
});
