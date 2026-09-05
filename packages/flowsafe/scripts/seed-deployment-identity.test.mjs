// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { EXECUTION_FENCE_DDL } from '../deployment-identity-protocol.mjs';
import {
  DeploymentIdentityError,
  readDeploymentIdentity,
  seedDeploymentIdentity,
} from '../src/do-runner/deployment-identity.js';
import { ExecutionFenceStore } from '../src/do-runner/execution-fence.js';
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
  initialFenceState: 'open',
};
const FENCE_TABLE = 'flowsafe_execution_fence';
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
const LEGACY_FENCE_DDL = `CREATE TABLE ${FENCE_TABLE} (
  id TEXT PRIMARY KEY CHECK (id = 'deployment'),
  state TEXT NOT NULL CHECK (state IN ('open', 'draining', 'migration-locked', 'proof-only')),
  proof_key TEXT, proof_run_id TEXT, updated_at INTEGER NOT NULL
)`;

function databaseQuery(initialTables = [], ownerTag = 'acme') {
  const tables = [...initialTables];
  let seeded = initialTables.some((row) => row.name === 'flowsafe_deployment');
  let storedOwner = seeded ? (ownerTag ?? undefined) : undefined;
  // The fence table is reported by the schema scan once created, so the
  // ownership guard below faces the same residue a crashed provisioning pass
  // would leave behind.
  const fenceSqlite = openSqlite();
  if (initialTables.some((row) => row.name === FENCE_TABLE))
    fenceSqlite.exec(LEGACY_FENCE_DDL);
  const fenceSchema = () =>
    fenceSqlite
      .prepare(`SELECT name, sql FROM sqlite_schema WHERE type = 'table'`)
      .all();
  const mutations = [];
  return {
    mutations,
    fence: () => ({
      table: fenceSchema().length === 1,
      state:
        fenceSchema().length === 0
          ? undefined
          : fenceSqlite.prepare(`SELECT state FROM ${FENCE_TABLE}`).get()
              ?.state,
    }),
    addTable: (row) => tables.push(row),
    query: async (statement) => {
      if (
        /^(?:CREATE TABLE IF NOT EXISTS|INSERT OR IGNORE INTO|ALTER TABLE) flowsafe_execution_fence\b/.test(
          statement,
        ) ||
        statement.startsWith(`SELECT * FROM ${FENCE_TABLE}`) ||
        statement === `PRAGMA table_xinfo(${FENCE_TABLE})`
      ) {
        if (/^(CREATE|INSERT|ALTER)/.test(statement)) mutations.push(statement);
        return fenceSqlite.prepare(statement).all();
      }
      if (statement.startsWith('SELECT name, sql')) {
        const applicationTables = tables.filter(
          (row) =>
            row.name !== 'flowsafe_deployment' && row.name !== FENCE_TABLE,
        );
        return [
          ...(seeded ? [{ name: 'flowsafe_deployment', sql: SQL }] : []),
          ...fenceSchema(),
          ...applicationTables,
        ];
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
            row.name !== FENCE_TABLE &&
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

// The fence table AS SQLITE STORED IT, plus its row without the timestamp.
// This is what proves the runtime store and the provisioning protocol issue one
// schema: a drifted copy would still be accepted by `CREATE TABLE IF NOT
// EXISTS`, so only comparing the materialized tables catches it.
function fenceSnapshot(sqlite) {
  const schema = sqlite
    .prepare(
      `SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = '${FENCE_TABLE}'`,
    )
    .get();
  return {
    sql: schema.sql.replace(/\s+/g, ' ').trim(),
    columns: sqlite.prepare(`PRAGMA table_info(${FENCE_TABLE})`).all(),
    rows: sqlite
      .prepare(`SELECT id, state, proof_key, proof_run_id FROM ${FENCE_TABLE}`)
      .all(),
    timestamps: sqlite
      .prepare(`SELECT updated_at FROM ${FENCE_TABLE}`)
      .all()
      .map((row) => typeof row.updated_at),
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
  it('migrates a legacy fence identically through runtime and CLI executors', async () => {
    const runtimeSqlite = openSqlite();
    const cliSqlite = openSqlite();
    for (const sqlite of [runtimeSqlite, cliSqlite]) {
      sqlite.exec(LEGACY_FENCE_DDL);
      sqlite.exec(
        `INSERT INTO ${FENCE_TABLE} VALUES ('deployment', 'proof-only', 'old-key', 'old-run', 17)`,
      );
    }
    await new ExecutionFenceStore(sqliteUnitDatabase(runtimeSqlite)).seed(
      'open',
    );
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(cliSqlite));
    expect(fenceSnapshot(runtimeSqlite)).toEqual(fenceSnapshot(cliSqlite));
    expect(cliSqlite.prepare(`SELECT * FROM ${FENCE_TABLE}`).get()).toEqual({
      id: 'deployment',
      state: 'proof-only',
      proof_key: 'old-key',
      proof_run_id: 'old-run',
      updated_at: 17,
      last_transition_request: null,
      transition_revision: 0,
      mutation_epoch: 0,
      require_mutation_epoch: 0,
    });
  });

  it('does not seed a new-format row from a stale legacy-empty observation', async () => {
    const sqlite = openSqlite();
    sqlite.exec(LEGACY_FENCE_DDL);
    const statements = [];
    const query = sqliteQuery(sqlite, (sql) => {
      statements.push(sql);
      if (sql.startsWith(`INSERT OR IGNORE INTO ${FENCE_TABLE}`)) {
        sqlite.exec(`DROP TABLE ${FENCE_TABLE}`);
        sqlite.exec(EXECUTION_FENCE_DDL);
      }
    });
    await expect(
      provisionDeploymentIdentity(OPTIONS, query),
    ).rejects.toBeInstanceOf(DeploymentIdentityError);
    expect(sqlite.prepare(`SELECT * FROM ${FENCE_TABLE}`).all()).toEqual([]);
    expect(
      statements.filter((sql) =>
        sql.startsWith(`INSERT OR IGNORE INTO ${FENCE_TABLE}`),
      ),
    ).toHaveLength(1);
  });

  it('preserves the runtime seed vocabulary and the provisioning birth-state restriction', async () => {
    for (const state of ['draining', 'proof-only']) {
      const sqlite = openSqlite();
      const fence = new ExecutionFenceStore(sqliteUnitDatabase(sqlite));
      await fence.seed(state);
      expect(await fence.read()).toEqual({
        state,
        mutationEpoch: 0,
        requireMutationEpoch: false,
        transitionRevision: 0,
      });
      const statements = [];
      await expect(
        provisionDeploymentIdentity(
          { ...OPTIONS, initialFenceState: state },
          async (sql) => {
            statements.push(sql);
            return [];
          },
        ),
      ).rejects.toBeInstanceOf(DeploymentIdentityError);
      expect(statements).toEqual([]);
    }
  });
  it('requires an explicit database, valid tag, fence state, and execution target', () => {
    expect(
      parseProvisioningArguments([
        '--',
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--initial-fence-state',
        'open',
        '--remote',
      ]),
    ).toEqual(OPTIONS);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'ACME',
        '--initial-fence-state',
        'open',
        '--remote',
      ]),
    ).toThrow(/must match/);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--initial-fence-state',
        'open',
      ]),
    ).toThrow(/Usage/);
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--initial-fence-state',
        'open',
        '--remote',
        '--preview',
      ]),
    ).toThrow(/choose exactly one execution target/);
  });

  it('refuses a missing or unknown initial fence state', () => {
    // No default: omitting the flag is a usage error, not an open deployment.
    expect(() =>
      parseProvisioningArguments([
        '--database',
        'flowsafe-acme',
        '--tag',
        'acme',
        '--remote',
      ]),
    ).toThrow(/Usage/);
    for (const state of ['draining', 'proof-only', 'OPEN', 'locked']) {
      expect(() =>
        parseProvisioningArguments([
          '--database',
          'flowsafe-acme',
          '--tag',
          'acme',
          '--initial-fence-state',
          state,
          '--remote',
        ]),
      ).toThrow(/must be one of open, migration-locked/);
    }
  });

  it('accepts migration-locked and seeds it verbatim', async () => {
    const fake = databaseQuery();
    await provisionDeploymentIdentity(
      { ...OPTIONS, initialFenceState: 'migration-locked' },
      fake.query,
    );
    expect(fake.fence()).toEqual({ table: true, state: 'migration-locked' });
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
    // Sentinel DDL, ownership insert, then the fence: the fence DDL runs LAST
    // so it can never add a table to a database whose ownership is still being
    // decided.
    expect(fake.mutations).toHaveLength(8);
    expect(fake.mutations[0]).toMatch(
      /^CREATE TABLE IF NOT EXISTS flowsafe_deployment/,
    );
    expect(fake.mutations[1]).toMatch(
      /^INSERT OR IGNORE INTO flowsafe_deployment/,
    );
    expect(fake.mutations[2]).toMatch(
      /^CREATE TABLE IF NOT EXISTS flowsafe_execution_fence/,
    );
    expect(fake.mutations[3]).toMatch(
      /^INSERT OR IGNORE INTO flowsafe_execution_fence/,
    );
    expect(
      fake.mutations
        .slice(4)
        .every((sql) =>
          sql.startsWith(`ALTER TABLE ${FENCE_TABLE} ADD COLUMN`),
        ),
    ).toBe(true);
    expect(fake.fence()).toEqual({ table: true, state: 'open' });
  });

  it('re-seeds the fence on the already-owned early return without reopening it', async () => {
    const fake = databaseQuery();
    await provisionDeploymentIdentity(OPTIONS, fake.query);
    const afterFirst = fake.mutations.length;

    // A current singleton is validated without a fence mutation.
    await provisionDeploymentIdentity(
      { ...OPTIONS, initialFenceState: 'migration-locked' },
      fake.query,
    );

    expect(fake.mutations.slice(afterFirst)).toEqual([]);
    // INSERT-if-absent: the existing row survives a re-provision that asked for
    // a different state.
    expect(fake.fence()).toEqual({ table: true, state: 'open' });
  });

  it('heals a fence row that a previous pass never wrote', async () => {
    // The residue of a crash between the ownership insert and the fence seed:
    // an owned database whose fence table exists but holds no row.
    const fake = databaseQuery([
      { name: 'flowsafe_deployment', sql: SQL },
      { name: FENCE_TABLE, sql: 'CREATE' },
    ]);

    await provisionDeploymentIdentity(
      { ...OPTIONS, initialFenceState: 'migration-locked' },
      fake.query,
    );

    expect(fake.fence()).toEqual({ table: true, state: 'migration-locked' });
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
    await seedDeploymentIdentity(
      sqliteUnitDatabase(runtimeSqlite),
      'acme',
      'open',
    );
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(cliSqlite));

    expect(sentinelSnapshot(runtimeSqlite)).toEqual(
      sentinelSnapshot(cliSqlite),
    );
    // The fence table too: the runtime binds its parameters and the CLI renders
    // them as literals, so agreeing here is what proves one DDL and one row
    // shape reach D1 down both paths.
    expect(fenceSnapshot(runtimeSqlite)).toEqual(fenceSnapshot(cliSqlite));
    expect(fenceSnapshot(runtimeSqlite)).toMatchObject({
      rows: [
        {
          id: 'deployment',
          state: 'open',
          proof_key: null,
          proof_run_id: null,
        },
      ],
      // Bound as a string against an INTEGER column: SQLite's affinity has to
      // have converted it, or every later `updated_at` comparison is text.
      timestamps: ['number'],
    });
    await provisionDeploymentIdentity(OPTIONS, sqliteQuery(runtimeSqlite));
    await seedDeploymentIdentity(sqliteUnitDatabase(cliSqlite), 'acme', 'open');
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
      seedDeploymentIdentity(sqliteUnitDatabase(runtimeSqlite), 'acme', 'open'),
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

    expect(fake.mutations).toHaveLength(7);
    expect(fake.mutations[0]).toMatch(
      /^INSERT OR IGNORE INTO flowsafe_deployment/,
    );
    expect(fake.mutations[1]).toMatch(
      /^CREATE TABLE IF NOT EXISTS flowsafe_execution_fence/,
    );
    expect(fake.mutations[2]).toMatch(
      /^INSERT OR IGNORE INTO flowsafe_execution_fence/,
    );
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
    expect(fake.mutations).toHaveLength(8);
  });

  it('allows a pre-existing execution fence table left by an interrupted pass', async () => {
    // The fence table is protocol-owned, so its presence must never be read as
    // "somebody else's application data" — otherwise a pass that died after the
    // fence DDL would refuse the database it had just started provisioning.
    const fake = databaseQuery([{ name: FENCE_TABLE, sql: 'CREATE' }]);
    await provisionDeploymentIdentity(OPTIONS, fake.query);
    expect(await fake.query('SELECT id FROM flowsafe_deployment')).toEqual([
      { id: 1, tenant_tag: 'acme' },
    ]);
    expect(fake.fence()).toEqual({ table: true, state: 'open' });
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
      seedDeploymentIdentity(racingRuntime, 'acme', 'open'),
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
