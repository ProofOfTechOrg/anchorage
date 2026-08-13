// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { openSqlite, sqliteUnitDatabase } from '../../test-support/sqlite.js';
import {
  assertDeploymentIdentity,
  assertDeploymentIdentitySecret,
  DEPLOYMENT_IDENTITY_HEADER,
  DEPLOYMENT_TAG_PATTERN,
  type DeploymentIdentityDatabase,
  DeploymentIdentityError,
  type DeploymentIdentityStatement,
  ensureDeploymentIdentity,
  ensureDeploymentIdentityBindings,
  readDeploymentIdentity,
  seedDeploymentIdentity,
  verifyDurableObjectDeploymentIdentity,
  verifyDurableObjectDeploymentRequest,
} from './deployment-identity.js';

const DEPLOYMENT_IDENTITY_SECRET = 'test-deployment-identity-secret-0001';

function sqliteDatabase(): DeploymentIdentityDatabase {
  return sqliteUnitDatabase(openSqlite()) as DeploymentIdentityDatabase;
}

function interceptSentinelRead(
  db: DeploymentIdentityDatabase,
  beforeRead: () => void | Promise<void>,
): DeploymentIdentityDatabase {
  return {
    prepare(query) {
      const prepared = db.prepare(query);
      if (!query.includes('SELECT sql FROM sqlite_schema')) return prepared;
      let bound = prepared;
      const statement: DeploymentIdentityStatement = {
        bind(...values) {
          bound = prepared.bind(...values);
          return statement;
        },
        run: () => bound.run(),
        async all<T>() {
          await beforeRead();
          return bound.all<T>();
        },
      };
      return statement;
    },
  };
}

describe('deployment identity provisioning', () => {
  it.each([
    'abc',
    'a0z',
    'a'.repeat(32),
  ])('accepts deployment tag %s', (tag) => {
    expect(DEPLOYMENT_TAG_PATTERN.test(tag)).toBe(true);
  });

  it.each([
    'ab',
    'a'.repeat(33),
    'ACME',
    'ac-me',
    'ac_me',
    ' acme',
  ])('rejects deployment tag %s', (tag) => {
    expect(DEPLOYMENT_TAG_PATTERN.test(tag)).toBe(false);
  });

  it.each([
    'x'.repeat(32),
    'x'.repeat(256),
  ])('accepts a header-stable deployment credential at a supported boundary', (secret) => {
    expect(() => assertDeploymentIdentitySecret(secret)).not.toThrow();
  });

  it.each([
    'x'.repeat(31),
    'x'.repeat(257),
    ' '.repeat(32),
    `${'x'.repeat(31)} `,
    ` ${'x'.repeat(31)}`,
    `${'x'.repeat(31)}\n`,
    '😀'.repeat(16),
  ])('rejects a non-header-stable deployment credential', (secret) => {
    expect(() => assertDeploymentIdentitySecret(secret)).toThrow(
      /visible ASCII/,
    );
  });

  it('seeds once, tolerates concurrent retries, and refuses re-homing', async () => {
    const db = sqliteDatabase();

    expect(await readDeploymentIdentity(db)).toBeUndefined();
    await Promise.all([
      seedDeploymentIdentity(db, 'acme'),
      seedDeploymentIdentity(db, 'acme'),
    ]);
    expect(await readDeploymentIdentity(db)).toBe('acme');
    await expect(seedDeploymentIdentity(db, 'globex')).rejects.toThrow(
      /already belongs to deployment 'acme'/,
    );
    expect(await readDeploymentIdentity(db)).toBe('acme');
  });

  it('retries after DDL succeeds but the ownership insert is interrupted', async () => {
    const db = sqliteDatabase();
    await db
      .prepare(`CREATE TABLE flowsafe_deployment (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      tenant_tag TEXT NOT NULL,
      provisioned_at TEXT NOT NULL
    )`)
      .run();

    expect(await readDeploymentIdentity(db)).toBeUndefined();
    await seedDeploymentIdentity(db, 'acme');
    expect(await readDeploymentIdentity(db)).toBe('acme');
  });

  it('keeps deployment values bound at the D1 adapter boundary', async () => {
    const base = sqliteDatabase();
    const preparedQueries: string[] = [];
    const insertBindings: unknown[][] = [];
    const db: DeploymentIdentityDatabase = {
      prepare(query) {
        preparedQueries.push(query);
        const prepared = base.prepare(query);
        let bound = prepared;
        const statement: DeploymentIdentityStatement = {
          bind(...values) {
            if (query.startsWith('INSERT OR IGNORE')) {
              insertBindings.push(values);
            }
            bound = prepared.bind(...values);
            return statement;
          },
          run: () => bound.run(),
          all: <T>() => bound.all<T>(),
        };
        return statement;
      },
    };

    await seedDeploymentIdentity(db, 'acme');

    const insert = preparedQueries.find((query) =>
      query.startsWith('INSERT OR IGNORE'),
    );
    expect(insert).toContain('SELECT 1, ?, ?');
    expect(insert).not.toContain("'acme'");
    expect(insertBindings).toHaveLength(1);
    expect(insertBindings[0]?.[0]).toBe('acme');
    expect(insertBindings[0]?.[1]).toMatch(
      /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    );
  });

  it('refuses malformed provisioning input and malformed sentinel content', async () => {
    const db = sqliteDatabase();
    await expect(seedDeploymentIdentity(db, 'ACME')).rejects.toBeInstanceOf(
      DeploymentIdentityError,
    );
    await seedDeploymentIdentity(db, 'acme');
    await db
      .prepare('UPDATE flowsafe_deployment SET tenant_tag = ? WHERE id = 1')
      .bind('ACME')
      .run();
    await expect(readDeploymentIdentity(db)).rejects.toThrow(
      /tenant_tag is malformed/,
    );
    await expect(assertDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /tenant_tag is malformed/,
    );
    await expect(seedDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /tenant_tag is malformed/,
    );
  });

  it.each([
    {
      label: 'missing singleton check',
      ddl: `CREATE TABLE flowsafe_deployment (
        id INTEGER PRIMARY KEY,
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      )`,
      insert: `INSERT INTO flowsafe_deployment VALUES
        (1, 'acme', '2026-08-10T00:00:00.000Z')`,
    },
    {
      label: 'weakened singleton check',
      ddl: `CREATE TABLE flowsafe_deployment (
        id INTEGER PRIMARY KEY CHECK (id = 1 OR tenant_tag = 'acme'),
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      )`,
      insert: `INSERT INTO flowsafe_deployment VALUES
        (1, 'acme', '2026-08-10T00:00:00.000Z')`,
    },
    {
      label: 'duplicate ownership rows',
      ddl: `CREATE TABLE flowsafe_deployment (
        id INTEGER CHECK (id = 1),
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL
      )`,
      insert: `INSERT INTO flowsafe_deployment VALUES
        (1, 'acme', '2026-08-10T00:00:00.000Z'),
        (1, 'acme', '2026-08-10T00:00:00.000Z')`,
    },
    {
      label: 'extra column',
      ddl: `CREATE TABLE flowsafe_deployment (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        tenant_tag TEXT NOT NULL,
        provisioned_at TEXT NOT NULL,
        note TEXT
      )`,
      insert: `INSERT INTO flowsafe_deployment
        (id, tenant_tag, provisioned_at) VALUES
        (1, 'acme', '2026-08-10T00:00:00.000Z')`,
    },
  ])('rejects a malformed sentinel schema: $label', async ({ ddl, insert }) => {
    const db = sqliteDatabase();
    await db.prepare(ddl).run();
    await db.prepare(insert).run();
    await expect(readDeploymentIdentity(db)).rejects.toThrow(
      /invalid ownership schema/,
    );
  });

  it.each([
    'flowsafe_approvals',
    'mastra_schedules',
    'mastra_workflow_snapshot',
    'mastra_threads',
    'mastra_resources',
    'mastra_background_tasks',
    'starter_actions',
  ])('refuses to adopt an unowned database containing %s', async (table) => {
    const db = sqliteDatabase();
    await db.prepare(`CREATE TABLE ${table} (id TEXT PRIMARY KEY)`).run();
    await expect(seedDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /unowned database already contains application tables/,
    );
  });

  it.each([
    '_cf_customer_data',
    '_cf_METADATA_backup',
    '_cf_metadata',
    'sqliteX_application',
  ])('does not treat near-system table %s as D1-owned', async (table) => {
    const db = sqliteDatabase();
    await db.prepare(`CREATE TABLE ${table} (id TEXT PRIMARY KEY)`).run();
    await expect(seedDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /unowned database already contains application tables/,
    );
  });

  it.each([
    '_cf_KV',
    '_cf_METADATA',
  ])('allows the exact D1-owned %s table', async (table) => {
    const db = sqliteDatabase();
    await db.prepare(`CREATE TABLE ${table} (key TEXT PRIMARY KEY)`).run();
    await seedDeploymentIdentity(db, 'acme');
    await expect(readDeploymentIdentity(db)).resolves.toBe('acme');
  });

  it('refuses an application table created between the initial scan and ownership insert', async () => {
    const sqlite = openSqlite();
    const base = sqliteUnitDatabase(sqlite) as DeploymentIdentityDatabase;
    let injected = false;
    const racing: DeploymentIdentityDatabase = {
      prepare(query) {
        const prepared = base.prepare(query);
        if (!query.startsWith('INSERT OR IGNORE INTO flowsafe_deployment')) {
          return prepared;
        }
        let bound = prepared;
        const statement: DeploymentIdentityStatement = {
          bind(...values) {
            bound = prepared.bind(...values);
            return statement;
          },
          async run() {
            if (!injected) {
              injected = true;
              sqlite.exec(
                'CREATE TABLE raced_application (id TEXT PRIMARY KEY)',
              );
            }
            return bound.run();
          },
          all: <T>() => bound.all<T>(),
        };
        return statement;
      },
    };

    await expect(seedDeploymentIdentity(racing, 'acme')).rejects.toThrow(
      /raced_application/,
    );
    await expect(readDeploymentIdentity(base)).resolves.toBeUndefined();
  });
});

describe('deployment identity runtime guard', () => {
  it('accepts a matching sentinel and fails closed on missing or mismatched identity', async () => {
    const matching = sqliteDatabase();
    await seedDeploymentIdentity(matching, 'acme');
    await expect(assertDeploymentIdentity(matching, 'acme')).resolves.toBe(
      undefined,
    );
    await expect(assertDeploymentIdentity(matching, 'globex')).rejects.toThrow(
      /configured as 'globex'.*belongs to 'acme'/,
    );

    const unseeded = sqliteDatabase();
    await expect(assertDeploymentIdentity(unseeded, 'acme')).rejects.toThrow(
      /no deployment sentinel/,
    );
  });

  it('memoizes only success and retries after a failure', async () => {
    let reads = 0;
    let failures = 2;
    const seeded = sqliteDatabase();
    await seedDeploymentIdentity(seeded, 'acme');
    const db = interceptSentinelRead(seeded, () => {
      reads += 1;
      if (failures > 0) {
        failures -= 1;
        throw new Error('temporary D1 outage');
      }
    });

    await expect(ensureDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /temporary D1 outage/,
    );
    await expect(ensureDeploymentIdentity(db, 'acme')).rejects.toThrow(
      /temporary D1 outage/,
    );
    expect(reads).toBe(2);

    await Promise.all([
      ensureDeploymentIdentity(db, 'acme'),
      ensureDeploymentIdentity(db, 'acme'),
    ]);
    await ensureDeploymentIdentity(db, 'acme');
    expect(reads).toBe(3);
  });

  it('propagates database outages instead of misreporting them as unseeded', async () => {
    const outage = new Error('D1 unavailable');
    const db = interceptSentinelRead(sqliteDatabase(), () => {
      throw outage;
    });
    await expect(readDeploymentIdentity(db)).rejects.toBe(outage);
    await expect(ensureDeploymentIdentity(db, 'acme')).rejects.toBe(outage);
  });

  it('requires matching bindings in production DOs and skips node-only instances', async () => {
    const db = sqliteDatabase();
    await seedDeploymentIdentity(db, 'acme');
    const state = { id: { name: 'instance' } };

    await expect(
      verifyDurableObjectDeploymentIdentity(state, {
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET,
        DB: db,
      }),
    ).resolves.toBe('acme');
    await expect(
      verifyDurableObjectDeploymentIdentity(state, {}),
    ).rejects.toBeInstanceOf(DeploymentIdentityError);
    await expect(
      verifyDurableObjectDeploymentIdentity(state, {
        DEPLOYMENT_TENANT: 'globex',
        DEPLOYMENT_IDENTITY_SECRET,
        DB: db,
      }),
    ).rejects.toBeInstanceOf(DeploymentIdentityError);
    await expect(
      verifyDurableObjectDeploymentIdentity(undefined, {}),
    ).resolves.toBeUndefined();
  });

  it('validates all Worker bindings before touching routes', async () => {
    const db = sqliteDatabase();
    await seedDeploymentIdentity(db, 'acme');
    await expect(
      ensureDeploymentIdentityBindings({
        DB: db,
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET,
      }),
    ).resolves.toBeUndefined();
    await expect(
      ensureDeploymentIdentityBindings({
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET,
      }),
    ).rejects.toThrow(/no valid DB binding/);
    await expect(
      ensureDeploymentIdentityBindings({
        DB: db,
        DEPLOYMENT_TENANT: 'acme',
      }),
    ).rejects.toThrow(/DEPLOYMENT_IDENTITY_SECRET/);
  });

  it('verifies every D1-shaped binding at Worker and Durable Object entry', async () => {
    const primary = sqliteDatabase();
    const secondary = sqliteDatabase();
    await seedDeploymentIdentity(primary, 'acme');
    await seedDeploymentIdentity(secondary, 'other');
    const env = {
      DB: primary,
      SCHEDULES_DB: secondary,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    };

    await expect(ensureDeploymentIdentityBindings(env)).rejects.toThrow(
      /belongs to 'other'/,
    );
    await expect(
      verifyDurableObjectDeploymentIdentity({ id: { name: 'instance' } }, env),
    ).rejects.toThrow(/belongs to 'other'/);

    const matchingSecondary = sqliteDatabase();
    await seedDeploymentIdentity(matchingSecondary, 'acme');
    const matchingEnv = {
      ...env,
      SCHEDULES_DB: matchingSecondary,
    };
    await expect(
      ensureDeploymentIdentityBindings(matchingEnv),
    ).resolves.toBeUndefined();
  });

  it('never adopts an RPC binding as a deployment database', async () => {
    const db = sqliteDatabase();
    await seedDeploymentIdentity(db, 'acme');
    // A service binding with a named entrypoint, and a Durable Object stub, are
    // proxies that answer EVERY property with a callable. A `prepare`-only test
    // adopts them, and the sentinel scan then dies with "The RPC receiver does
    // not implement the method". Fleet trusted state carries exactly this pair.
    const rpcBinding = new Proxy(
      {},
      {
        get: (_target, property) =>
          typeof property === 'symbol'
            ? undefined
            : () => {
                throw new Error(
                  `The RPC receiver does not implement the method "${String(property)}".`,
                );
              },
      },
    );
    const env = {
      DB: db,
      OUTBOUND_PROXY: rpcBinding,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    };

    await expect(
      ensureDeploymentIdentityBindings(env),
    ).resolves.toBeUndefined();
    await expect(
      verifyDurableObjectDeploymentIdentity({ id: { name: 'instance' } }, env),
    ).resolves.toBe('acme');

    // The positive control: a real second D1 binding is still scanned, so the
    // exclusion above cannot be over-broad.
    const secondary = sqliteDatabase();
    await seedDeploymentIdentity(secondary, 'other');
    const withSecondDatabase = { ...env, SCHEDULES_DB: secondary };
    await expect(
      ensureDeploymentIdentityBindings(withSecondDatabase),
    ).rejects.toThrow(/belongs to 'other'/);
  });

  it('rejects a Worker request from a differently credentialed deployment', async () => {
    const db = sqliteDatabase();
    await seedDeploymentIdentity(db, 'acme');
    const state = { id: { name: 'instance' } };
    const env = {
      DB: db,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    };
    const accepted = new Request('http://do/internal', {
      headers: { [DEPLOYMENT_IDENTITY_HEADER]: DEPLOYMENT_IDENTITY_SECRET },
    });
    await expect(
      verifyDurableObjectDeploymentRequest(accepted, state, env),
    ).resolves.toBe('acme');
    await expect(
      verifyDurableObjectDeploymentRequest(
        new Request('http://do/internal'),
        state,
        env,
      ),
    ).rejects.toThrow(/internal identity credential/);
    await expect(
      verifyDurableObjectDeploymentRequest(
        new Request('http://do/internal', {
          headers: {
            [DEPLOYMENT_IDENTITY_HEADER]:
              'different-deployment-identity-secret',
          },
        }),
        state,
        env,
      ),
    ).rejects.toThrow(/internal identity credential/);
  });

  it('rejects the caller credential before reading any D1 sentinel', async () => {
    const seeded = sqliteDatabase();
    await seedDeploymentIdentity(seeded, 'acme');
    let sentinelReads = 0;
    const db = interceptSentinelRead(seeded, () => {
      sentinelReads += 1;
    });
    const env = {
      DB: db,
      DEPLOYMENT_TENANT: 'acme',
      DEPLOYMENT_IDENTITY_SECRET,
    };

    await expect(
      verifyDurableObjectDeploymentRequest(
        new Request('http://do/internal', {
          headers: {
            [DEPLOYMENT_IDENTITY_HEADER]:
              'different-deployment-identity-secret',
          },
        }),
        { id: { name: 'instance' } },
        env,
      ),
    ).rejects.toThrow(/internal identity credential/);
    expect(sentinelReads).toBe(0);
  });

  it('rejects a malformed configured credential before reading D1', async () => {
    const seeded = sqliteDatabase();
    await seedDeploymentIdentity(seeded, 'acme');
    let sentinelReads = 0;
    const db = interceptSentinelRead(seeded, () => {
      sentinelReads += 1;
    });

    await expect(
      ensureDeploymentIdentityBindings({
        DB: db,
        DEPLOYMENT_TENANT: 'acme',
        DEPLOYMENT_IDENTITY_SECRET: ' '.repeat(32),
      }),
    ).rejects.toThrow(/visible ASCII/);
    expect(sentinelReads).toBe(0);
  });
});
