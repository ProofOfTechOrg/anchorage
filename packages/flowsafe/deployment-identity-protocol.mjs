// SPDX-License-Identifier: Apache-2.0

export const DEPLOYMENT_TAG_PATTERN = /^[a-z0-9]{3,32}$/;
export const DEPLOYMENT_ENVIRONMENT_PATTERN =
  /^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$/;
export const DEPLOYMENT_SENTINEL_TABLE = 'flowsafe_deployment';
export const DEPLOYMENT_SENTINEL_DDL = `CREATE TABLE IF NOT EXISTS ${DEPLOYMENT_SENTINEL_TABLE} (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  tenant_tag TEXT NOT NULL,
  provisioned_at TEXT NOT NULL
)`;
export const DEPLOYMENT_SENTINEL_COLUMNS = Object.freeze([
  Object.freeze({ name: 'id', type: 'INTEGER', notnull: 0, pk: 1 }),
  Object.freeze({ name: 'tenant_tag', type: 'TEXT', notnull: 1, pk: 0 }),
  Object.freeze({
    name: 'provisioned_at',
    type: 'TEXT',
    notnull: 1,
    pk: 0,
  }),
]);

const SENTINEL_SQL_PATTERN =
  /^create table (?:if not exists )?flowsafe_deployment\s*\(\s*id integer primary key check\s*\(\s*id\s*=\s*1\s*\)\s*,\s*tenant_tag text not null\s*,\s*provisioned_at text not null\s*\)$/i;
const D1_OWNED_INTERNAL_TABLES = Object.freeze(['_cf_KV', '_cf_METADATA']);
const D1_OWNED_TABLE_EXCLUSIONS = D1_OWNED_INTERNAL_TABLES.map(
  (name) => `           AND name <> '${name}'`,
).join('\n');

const SCAN_TABLES = Object.freeze({
  mode: 'read',
  sql: `SELECT name, sql FROM sqlite_schema WHERE type = 'table' ORDER BY name`,
  bindings: Object.freeze([]),
});
const READ_SENTINEL_SCHEMA = Object.freeze({
  mode: 'read',
  sql: `SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`,
  bindings: Object.freeze([DEPLOYMENT_SENTINEL_TABLE]),
});
const READ_SENTINEL_COLUMNS = Object.freeze({
  mode: 'read',
  sql: `PRAGMA table_info(${DEPLOYMENT_SENTINEL_TABLE})`,
  bindings: Object.freeze([]),
});
const READ_SENTINEL_OWNER = Object.freeze({
  mode: 'read',
  sql: `SELECT id, tenant_tag FROM ${DEPLOYMENT_SENTINEL_TABLE} ORDER BY id`,
  bindings: Object.freeze([]),
});
const CREATE_SENTINEL = Object.freeze({
  mode: 'write',
  sql: DEPLOYMENT_SENTINEL_DDL,
  bindings: Object.freeze([]),
});

export class DeploymentIdentityError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DeploymentIdentityError';
  }
}

export function isDeploymentEnvironment(value) {
  return (
    typeof value === 'string' && DEPLOYMENT_ENVIRONMENT_PATTERN.test(value)
  );
}

export function assertValidDeploymentTag(tag, caller) {
  if (typeof tag !== 'string' || !DEPLOYMENT_TAG_PATTERN.test(tag)) {
    throw new DeploymentIdentityError(
      `${caller}: deployment tag '${String(tag)}' must match ${DEPLOYMENT_TAG_PATTERN} — fix the DEPLOYMENT_TENANT binding or the provisioning input`,
    );
  }
}

export function normalizeDeploymentSentinelSql(sql) {
  return sql.replace(/\s+/g, ' ').trim();
}

export function deploymentIdentityApplicationTables(rows) {
  return rows
    .map((row) => rowField(row, 'name'))
    .filter(
      (name) =>
        typeof name === 'string' &&
        name !== DEPLOYMENT_SENTINEL_TABLE &&
        !name.startsWith('sqlite_') &&
        !D1_OWNED_INTERNAL_TABLES.includes(name),
    )
    .sort();
}

function rowField(row, name) {
  return row !== null && typeof row === 'object' ? row[name] : undefined;
}

function malformedSentinel(reason) {
  return new DeploymentIdentityError(
    `${DEPLOYMENT_SENTINEL_TABLE} has an invalid ownership schema (${reason}) — recreate the database before serving`,
  );
}

function assertSentinelSchemaDefinition(rows) {
  const sql = rowField(rows[0], 'sql');
  if (rows.length !== 1 || typeof sql !== 'string') {
    throw malformedSentinel('table definition is missing or ambiguous');
  }
  if (!SENTINEL_SQL_PATTERN.test(normalizeDeploymentSentinelSql(sql))) {
    throw malformedSentinel('table definition differs from the owned schema');
  }
}

function assertSentinelColumns(rows) {
  if (rows.length !== DEPLOYMENT_SENTINEL_COLUMNS.length) {
    throw malformedSentinel('unexpected columns');
  }
  for (let index = 0; index < DEPLOYMENT_SENTINEL_COLUMNS.length; index += 1) {
    const actual = rows[index];
    const expected = DEPLOYMENT_SENTINEL_COLUMNS[index];
    if (
      rowField(actual, 'name') !== expected.name ||
      String(rowField(actual, 'type')).toUpperCase() !== expected.type ||
      Number(rowField(actual, 'notnull')) !== expected.notnull ||
      Number(rowField(actual, 'pk')) !== expected.pk
    ) {
      throw malformedSentinel(`column ${expected.name} differs`);
    }
  }
}

function conditionalOwnershipInsert(tag, provisionedAt) {
  return {
    mode: 'write',
    sql: `INSERT OR IGNORE INTO ${DEPLOYMENT_SENTINEL_TABLE} (id, tenant_tag, provisioned_at)
       SELECT 1, ?, ?
       WHERE NOT EXISTS (
         SELECT 1 FROM sqlite_schema
         WHERE type = 'table'
           AND name <> '${DEPLOYMENT_SENTINEL_TABLE}'
${D1_OWNED_TABLE_EXCLUSIONS}
           AND name NOT GLOB 'sqlite_*'
       )`,
    bindings: [tag, provisionedAt],
  };
}

async function scanTables(execute) {
  return execute(SCAN_TABLES);
}

export async function readDeploymentIdentityProtocol(execute) {
  const schema = await execute(READ_SENTINEL_SCHEMA);
  if (schema.length === 0) return undefined;
  assertSentinelSchemaDefinition(schema);
  assertSentinelColumns(await execute(READ_SENTINEL_COLUMNS));

  const owners = await execute(READ_SENTINEL_OWNER);
  if (owners.length === 0) return undefined;
  if (owners.length !== 1 || Number(rowField(owners[0], 'id')) !== 1) {
    throw malformedSentinel('the ownership row is not an exact singleton');
  }
  const tag = rowField(owners[0], 'tenant_tag');
  if (typeof tag !== 'string' || !DEPLOYMENT_TAG_PATTERN.test(tag)) {
    throw malformedSentinel('tenant_tag is malformed');
  }
  return tag;
}

function unownedDatabaseError(caller, applicationTables) {
  return new DeploymentIdentityError(
    `${caller}: unowned database already contains application tables (${applicationTables.join(', ')}) — provision a fresh database instead of adopting pooled or unknown state`,
  );
}

function differentOwnerError(caller, stored, tag) {
  return new DeploymentIdentityError(
    `${caller}: database already belongs to deployment '${stored}' — refusing to re-stamp it as '${tag}' (decommission and recreate instead)`,
  );
}

export async function provisionDeploymentIdentityProtocol(
  execute,
  tag,
  { caller = 'seedDeploymentIdentity', provisionedAt } = {},
) {
  assertValidDeploymentTag(tag, caller);
  const tables = await scanTables(execute);
  const applicationTables = deploymentIdentityApplicationTables(tables);
  const sentinelExists = tables.some(
    (row) => rowField(row, 'name') === DEPLOYMENT_SENTINEL_TABLE,
  );
  const storedBeforeCreate = await readDeploymentIdentityProtocol(execute);
  if (storedBeforeCreate !== undefined) {
    if (storedBeforeCreate !== tag) {
      throw differentOwnerError(caller, storedBeforeCreate, tag);
    }
    return;
  }
  if (applicationTables.length > 0) {
    throw unownedDatabaseError(caller, applicationTables);
  }

  if (!sentinelExists) await execute(CREATE_SENTINEL);
  const storedAfterCreate = await readDeploymentIdentityProtocol(execute);
  if (storedAfterCreate !== undefined) {
    if (storedAfterCreate !== tag) {
      throw differentOwnerError(caller, storedAfterCreate, tag);
    }
    return;
  }

  await execute(
    conditionalOwnershipInsert(tag, provisionedAt ?? new Date().toISOString()),
  );
  const seeded = await readDeploymentIdentityProtocol(execute);
  if (seeded === undefined) {
    const racedApplicationTables = deploymentIdentityApplicationTables(
      await scanTables(execute),
    );
    if (racedApplicationTables.length > 0) {
      throw unownedDatabaseError(caller, racedApplicationTables);
    }
    throw new DeploymentIdentityError(
      `${caller}: database sentinel is missing or malformed after seeding — recreate the database before serving`,
    );
  }
  if (seeded !== tag) {
    throw differentOwnerError(caller, seeded, tag);
  }
}
