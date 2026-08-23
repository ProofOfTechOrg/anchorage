// SPDX-License-Identifier: Apache-2.0

export const DEPLOYMENT_TAG_PATTERN = /^[a-z0-9]{3,32}$/;
export const DEPLOYMENT_ENVIRONMENT_PATTERN =
  /^[a-z0-9](?:[a-z0-9-]{0,30}[a-z0-9])?$/;
export const DEPLOYMENT_SENTINEL_TABLE = 'flowsafe_deployment';
/**
 * Internal Worker-to-Durable-Object credential header. Topology helpers always
 * overwrite it, and public request resolvers reject it. The value is a
 * deployment secret rather than the public deployment tag.
 */
export const DEPLOYMENT_IDENTITY_HEADER = 'x-flowsafe-deployment-identity';
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

// --- Deployment execution fence (F1) ---------------------------------------
//
// The fence table and its single row are created BY THIS PROTOCOL, so every
// database provisioned from 0.20 on is born carrying an EXPLICIT fence state
// instead of leaning on the absent-row-reads-open upgrade rule.
//
// The vocabulary and the DDL live HERE rather than beside the store that reads
// them (src/do-runner/execution-fence.ts, which imports them from this module)
// for one reason: this file is the only place both sides can share. It ships at
// the package root and is loaded by the provisioning CLI, by fleet-control's
// backends, and by the runtime — none of which can import the package's
// TypeScript sources. And the two MUST be one string: the store's own
// `CREATE TABLE IF NOT EXISTS` silently accepts a differently-shaped table this
// protocol created, so a drifted copy would not fail, it would quietly drop the
// CHECK constraints that make the store's compare-and-sets total.

/** The table the single fence row lives in — flowsafe-owned, outside `mastra_%`. */
export const EXECUTION_FENCE_TABLE = 'flowsafe_execution_fence';

/**
 * The fence row's fixed primary key. The fence is a property of the DEPLOYMENT
 * and a deployment is one database, so there is exactly one row and its key is
 * a constant.
 */
export const EXECUTION_FENCE_ROW_ID = 'deployment';

/** Every fence state, ordered from most to least permissive. */
export const EXECUTION_FENCE_STATES = Object.freeze([
  'open',
  'draining',
  'migration-locked',
  'proof-only',
]);

/**
 * The states a deployment may be BORN in. `draining` and `proof-only` are
 * transitions out of a state that already exists — draining finishes work a
 * fresh database has none of, and proof-only nominates a run nothing has yet
 * started — so neither is a coherent initial condition.
 */
export const INITIAL_EXECUTION_FENCE_STATES = Object.freeze([
  'open',
  'migration-locked',
]);

export const EXECUTION_FENCE_DDL = `CREATE TABLE IF NOT EXISTS ${EXECUTION_FENCE_TABLE} (
    id TEXT PRIMARY KEY CHECK (id = '${EXECUTION_FENCE_ROW_ID}'),
    state TEXT NOT NULL CHECK (state IN (${EXECUTION_FENCE_STATES.map((state) => `'${state}'`).join(', ')})),
    proof_key TEXT,
    proof_run_id TEXT,
    updated_at INTEGER NOT NULL
  )`;

const SENTINEL_SQL_PATTERN =
  /^create table (?:if not exists )?flowsafe_deployment\s*\(\s*id integer primary key check\s*\(\s*id\s*=\s*1\s*\)\s*,\s*tenant_tag text not null\s*,\s*provisioned_at text not null\s*\)$/i;
const D1_OWNED_INTERNAL_TABLES = Object.freeze(['_cf_KV', '_cf_METADATA']);
/**
 * Tables whose presence does NOT make a database "unowned application state":
 * D1's own internal tables plus every table this protocol creates itself.
 *
 * The fence table belongs here because it can legitimately exist BEFORE the
 * ownership row does — a previous provisioning attempt that died between the
 * fence DDL and the sentinel insert leaves exactly that residue, and the
 * runtime store also materializes the table on its first control-plane
 * transition. Without the exclusion the next provisioning pass would read its
 * own leftovers as somebody else's application data and refuse the database
 * forever (`unownedDatabaseError`), and the conditional ownership insert below
 * would never fire.
 */
const NON_APPLICATION_TABLES = Object.freeze([
  ...D1_OWNED_INTERNAL_TABLES,
  EXECUTION_FENCE_TABLE,
]);
const MIN_DEPLOYMENT_CREDENTIAL_LENGTH = 32;
const MAX_DEPLOYMENT_CREDENTIAL_LENGTH = 256;
const NON_APPLICATION_TABLE_EXCLUSIONS = NON_APPLICATION_TABLES.map(
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
const CREATE_EXECUTION_FENCE = Object.freeze({
  mode: 'write',
  sql: EXECUTION_FENCE_DDL,
  bindings: Object.freeze([]),
});

export class DeploymentIdentityError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DeploymentIdentityError';
  }
}

export function assertDeploymentIdentitySecret(
  secret,
  caller = 'deployment identity',
) {
  if (
    typeof secret !== 'string' ||
    secret.length < MIN_DEPLOYMENT_CREDENTIAL_LENGTH ||
    secret.length > MAX_DEPLOYMENT_CREDENTIAL_LENGTH ||
    !/^[\x21-\x7e]+$/.test(secret)
  ) {
    throw new DeploymentIdentityError(
      `${caller}: DEPLOYMENT_IDENTITY_SECRET must contain ${MIN_DEPLOYMENT_CREDENTIAL_LENGTH}-${MAX_DEPLOYMENT_CREDENTIAL_LENGTH} visible ASCII characters`,
    );
  }
}

/** Stamp the internal credential onto an ordinary topology request. */
export function deploymentIdentityHeaders(secret, initial) {
  assertDeploymentIdentitySecret(secret, 'deploymentIdentityHeaders');
  const merged = new Headers(initial);
  merged.set(DEPLOYMENT_IDENTITY_HEADER, secret);
  return Object.fromEntries(merged.entries());
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

/**
 * Validate the fence state a deployment is to be born in.
 *
 * Loud on anything else, and with NO default anywhere above it: the failure
 * this closes is a migration host forgetting to ask for 'migration-locked' and
 * silently getting an executing deployment, which is exactly the condition a
 * migration exists to prevent. Making the argument required turns that into an
 * obligation the caller cannot skip, while still letting a host that wants an
 * open deployment say so.
 */
export function assertInitialExecutionFenceState(state, caller) {
  if (
    typeof state !== 'string' ||
    !INITIAL_EXECUTION_FENCE_STATES.includes(state)
  ) {
    throw new DeploymentIdentityError(
      `${caller}: initialExecutionFenceState must be one of ${INITIAL_EXECUTION_FENCE_STATES.join(', ')} (got '${String(state)}') — it has no default on purpose`,
    );
  }
  return state;
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
        !NON_APPLICATION_TABLES.includes(name),
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
${NON_APPLICATION_TABLE_EXCLUSIONS}
           AND name NOT GLOB 'sqlite_*'
       )`,
    bindings: [tag, provisionedAt],
  };
}

function seedExecutionFenceRow(state, seededAt) {
  return {
    mode: 'write',
    sql: `INSERT OR IGNORE INTO ${EXECUTION_FENCE_TABLE}
       (id, state, proof_key, proof_run_id, updated_at)
     VALUES (?, ?, NULL, NULL, ?)`,
    // INSERT OR IGNORE, never an upsert: seeding runs on every provisioning
    // pass, and a re-provision of a LIVE deployment must not silently reopen a
    // fence an operator closed.
    //
    // `updated_at` is an INTEGER column bound as TEXT because D1's REST query
    // API takes every parameter as a string (fleet-control's
    // d1RestParameters rejects anything else). SQLite's INTEGER affinity
    // converts a well-formed integer literal on write, so the column still
    // holds a number.
    bindings: [EXECUTION_FENCE_ROW_ID, state, String(seededAt)],
  };
}

/**
 * Write the deployment's initial fence row, if it has none.
 *
 * Two statements rather than one request: every executor this protocol is
 * driven through — the runtime's `db.prepare()`, the CLI's
 * `wrangler d1 execute --command`, and both fleet-control backends' REST
 * `/query` with bound parameters — carries exactly ONE statement per call, so
 * there is no seam here through which a batch could be sent. The DDL therefore
 * runs first and the row second; a crash between them leaves an empty fence
 * table, which reads as `open` and is healed by the next invocation.
 */
async function seedExecutionFence(execute, state, seededAt) {
  await execute(CREATE_EXECUTION_FENCE);
  await execute(seedExecutionFenceRow(state, seededAt));
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
  {
    caller = 'seedDeploymentIdentity',
    provisionedAt,
    now = Date.now,
    initialExecutionFenceState,
  } = {},
) {
  assertValidDeploymentTag(tag, caller);
  // Validated BEFORE the first statement: a caller that omitted the fence state
  // must learn so without having stamped ownership onto a database first.
  const fenceState = assertInitialExecutionFenceState(
    initialExecutionFenceState,
    caller,
  );
  // One instant for both rows. The sentinel stores it as ISO TEXT and the fence
  // as epoch-milliseconds INTEGER because that is what each column already is;
  // an explicit `provisionedAt` still wins for the sentinel, as before.
  const seededAt = now();
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
    // The already-owned early return still seeds the fence. A previous pass
    // that died between the ownership insert and the fence row left a
    // deployment with an owner and NO explicit fence — permanently implicit-open
    // residue, on the one deployment a migration most needs to be able to lock.
    // Seeding here is what heals it, and INSERT-if-absent is what makes
    // repeating it safe on a deployment whose fence has since been moved.
    await seedExecutionFence(execute, fenceState, seededAt);
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
    await seedExecutionFence(execute, fenceState, seededAt);
    return;
  }

  await execute(
    conditionalOwnershipInsert(
      tag,
      provisionedAt ?? new Date(seededAt).toISOString(),
    ),
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
  // Last, never first: the fence DDL is the one statement that could add a
  // table to an as-yet-unowned database, and running it only after ownership is
  // PROVEN keeps it out of the window where `unownedDatabaseError` and the
  // conditional ownership insert are still deciding whether this database is
  // ours to write to at all.
  await seedExecutionFence(execute, fenceState, seededAt);
}
