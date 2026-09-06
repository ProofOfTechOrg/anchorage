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
export const EXECUTION_FENCE_CURRENT_SCHEMA_STAGE = 7;

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

const EXECUTION_FENCE_BASE_COLUMNS = `
    id TEXT PRIMARY KEY CHECK (id = '${EXECUTION_FENCE_ROW_ID}'),
    state TEXT NOT NULL CHECK (state IN (${EXECUTION_FENCE_STATES.map((state) => `'${state}'`).join(', ')})),
    proof_key TEXT,
    proof_run_id TEXT,
    updated_at INTEGER NOT NULL`;
const EXECUTION_FENCE_ADDITIONS = Object.freeze([
  'last_transition_request TEXT',
  `transition_revision INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(transition_revision) = 'integer'
      AND transition_revision BETWEEN 0 AND 9007199254740991)`,
  `mutation_epoch INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(mutation_epoch) = 'integer'
      AND mutation_epoch BETWEEN 0 AND 9007199254740991)`,
  `require_mutation_epoch INTEGER NOT NULL DEFAULT 0
    CHECK (typeof(require_mutation_epoch) = 'integer'
      AND require_mutation_epoch IN (0, 1))`,
  'proof_table_prefix TEXT',
  'proof_workflow_id TEXT',
  'proof_start_token TEXT',
]);
const EXECUTION_FENCE_COLUMNS = Object.freeze([
  ['id', 'TEXT', 0, 1, null],
  ['state', 'TEXT', 1, 0, null],
  ['proof_key', 'TEXT', 0, 0, null],
  ['proof_run_id', 'TEXT', 0, 0, null],
  ['updated_at', 'INTEGER', 1, 0, null],
  ['last_transition_request', 'TEXT', 0, 0, null],
  ['transition_revision', 'INTEGER', 1, 0, '0'],
  ['mutation_epoch', 'INTEGER', 1, 0, '0'],
  ['require_mutation_epoch', 'INTEGER', 1, 0, '0'],
  ['proof_table_prefix', 'TEXT', 0, 0, null],
  ['proof_workflow_id', 'TEXT', 0, 0, null],
  ['proof_start_token', 'TEXT', 0, 0, null],
]);
const EXECUTION_FENCE_BOOTSTRAP_DDL = `CREATE TABLE IF NOT EXISTS ${EXECUTION_FENCE_TABLE} (${EXECUTION_FENCE_BASE_COLUMNS}
  )`;
export const EXECUTION_FENCE_DDL = `CREATE TABLE IF NOT EXISTS ${EXECUTION_FENCE_TABLE} (${EXECUTION_FENCE_BASE_COLUMNS},
    ${EXECUTION_FENCE_ADDITIONS.join(',\n    ')}
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
  sql: EXECUTION_FENCE_BOOTSTRAP_DDL,
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
     SELECT ?, ?, NULL, NULL, ?
     WHERE NOT EXISTS (
       SELECT 1 FROM pragma_table_xinfo('${EXECUTION_FENCE_TABLE}')
       WHERE name IN ('last_transition_request', 'transition_revision',
         'mutation_epoch', 'require_mutation_epoch')
     )`,
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

function malformedExecutionFence(reason) {
  return new DeploymentIdentityError(
    `${EXECUTION_FENCE_TABLE} has an invalid execution-fence schema (${reason})`,
  );
}

export async function readExecutionFenceSchemaProtocol(execute) {
  const columns = await execute({
    mode: 'read',
    sql: `PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`,
    bindings: [],
  });
  if (!Array.isArray(columns)) {
    throw malformedExecutionFence('column metadata is not an array');
  }
  if (columns.length === 0) return undefined;
  if (columns.length < 5 || columns.length > EXECUTION_FENCE_COLUMNS.length) {
    throw malformedExecutionFence('unexpected columns');
  }
  for (let index = 0; index < columns.length; index += 1) {
    const actual = columns[index];
    const [name, type, notnull, pk, defaultValue] =
      EXECUTION_FENCE_COLUMNS[index];
    if (
      rowField(actual, 'name') !== name ||
      rowField(actual, 'type') !== type ||
      rowField(actual, 'notnull') !== notnull ||
      rowField(actual, 'pk') !== pk ||
      rowField(actual, 'dflt_value') !== defaultValue ||
      rowField(actual, 'hidden') !== 0
    ) {
      throw malformedExecutionFence(`column ${name} differs`);
    }
  }
  return columns.length - 5;
}

export function decodeExecutionFenceMutationMetadata(row) {
  if (row === null || typeof row !== 'object' || Array.isArray(row)) {
    throw malformedExecutionFence('row is not an object');
  }
  let stage = 0;
  let missing = false;
  for (const [name] of EXECUTION_FENCE_COLUMNS.slice(5)) {
    if (Object.hasOwn(row, name)) {
      if (missing) throw malformedExecutionFence('metadata prefix has a hole');
      stage += 1;
    } else {
      missing = true;
    }
  }
  const receipt = row.last_transition_request;
  const revision = row.transition_revision;
  const epoch = row.mutation_epoch;
  const required = row.require_mutation_epoch;
  const proofNames = [
    'proof_table_prefix',
    'proof_workflow_id',
    'proof_start_token',
  ];
  const proofValues = proofNames.map((name) =>
    Object.hasOwn(row, name) ? row[name] : null,
  );
  if (
    stage < EXECUTION_FENCE_CURRENT_SCHEMA_STAGE &&
    proofValues.some((value) => value !== null)
  ) {
    throw malformedExecutionFence('partial proof identity is not null');
  }
  if (
    proofValues.some((value) => value !== null) &&
    (proofValues.some((value) => typeof value !== 'string') ||
      row.state !== 'proof-only' ||
      typeof row.proof_key !== 'string' ||
      row.proof_key.length === 0 ||
      typeof row.proof_run_id !== 'string' ||
      row.proof_run_id.length === 0)
  ) {
    throw malformedExecutionFence('proof identity is inconsistent');
  }
  const proofMetadata = {
    proofTablePrefix: proofValues[0],
    proofWorkflowId: proofValues[1],
    proofStartToken: proofValues[2],
  };
  if (stage < 4) {
    if (
      (stage >= 1 && receipt !== null) ||
      (stage >= 2 && revision !== 0) ||
      (stage >= 3 && epoch !== 0)
    ) {
      throw malformedExecutionFence(
        'partial metadata is not optional defaults',
      );
    }
    return {
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 0,
      lastTransitionRequest: null,
      schemaStage: stage,
      ...proofMetadata,
    };
  }
  if (
    !Number.isSafeInteger(epoch) ||
    epoch < 0 ||
    !Number.isSafeInteger(revision) ||
    revision < 0 ||
    (required !== 0 && required !== 1) ||
    (required === 1) !== epoch > 0 ||
    (receipt !== null &&
      (typeof receipt !== 'string' || receipt.length > 512)) ||
    (revision === 0 && receipt !== null) ||
    (required === 1 && (receipt === null || revision === 0))
  ) {
    throw malformedExecutionFence('mutation metadata is inconsistent');
  }
  return {
    mutationEpoch: epoch,
    requireMutationEpoch: required === 1,
    transitionRevision: revision,
    lastTransitionRequest: receipt,
    schemaStage: stage,
    ...proofMetadata,
  };
}

async function observeExecutionFence(execute, allowLegacyEmpty) {
  let minimumStage = 0;
  for (let attempt = 0; attempt < 2; attempt += 1) {
    const rows = await execute({
      mode: 'read',
      sql: `SELECT * FROM ${EXECUTION_FENCE_TABLE} LIMIT 2`,
      bindings: [],
    });
    const stage = await readExecutionFenceSchemaProtocol(execute);
    if (!Array.isArray(rows) || stage === undefined || stage < minimumStage) {
      throw malformedExecutionFence('row observation has no compatible schema');
    }
    if (rows.length === 0 && attempt === 0) {
      if (stage === 0 && allowLegacyEmpty) return { stage, empty: true };
      if (stage > 0) {
        minimumStage = stage;
        continue;
      }
    }
    if (
      rows.length !== 1 ||
      rowField(rows[0], 'id') !== EXECUTION_FENCE_ROW_ID ||
      !EXECUTION_FENCE_STATES.includes(rowField(rows[0], 'state'))
    ) {
      throw malformedExecutionFence('fence row is not a recognized singleton');
    }
    const metadata = decodeExecutionFenceMutationMetadata(rows[0]);
    if (metadata.schemaStage > stage) {
      throw malformedExecutionFence('schema observation precedes row metadata');
    }
    return { stage, empty: false, rowStage: metadata.schemaStage };
  }
  throw malformedExecutionFence('fence row is missing');
}

export async function initializeExecutionFenceProtocol(
  execute,
  { state, seededAt },
) {
  if (!EXECUTION_FENCE_STATES.includes(state)) {
    throw malformedExecutionFence('initial state is unrecognized');
  }
  if (!Number.isSafeInteger(seededAt) || seededAt < 0) {
    throw malformedExecutionFence('seed timestamp is invalid');
  }
  if ((await readExecutionFenceSchemaProtocol(execute)) === undefined) {
    await execute(CREATE_EXECUTION_FENCE);
    if ((await readExecutionFenceSchemaProtocol(execute)) === undefined) {
      throw malformedExecutionFence('bootstrap did not create the table');
    }
  }
  let observation = await observeExecutionFence(execute, true);
  if (observation.empty) {
    await execute(seedExecutionFenceRow(state, seededAt));
    observation = await observeExecutionFence(execute, false);
  }
  for (
    let index = observation.stage;
    index < EXECUTION_FENCE_CURRENT_SCHEMA_STAGE;
    index += 1
  ) {
    const stage = await readExecutionFenceSchemaProtocol(execute);
    if (stage === undefined || stage < index) {
      throw malformedExecutionFence('schema regressed during initialization');
    }
    if (stage > index) continue;
    try {
      await execute({
        mode: 'write',
        sql: `ALTER TABLE ${EXECUTION_FENCE_TABLE} ADD COLUMN ${EXECUTION_FENCE_ADDITIONS[index]}`,
        bindings: [],
      });
    } catch (error) {
      let observedStage;
      try {
        observedStage = await readExecutionFenceSchemaProtocol(execute);
      } catch (readError) {
        if (readError instanceof DeploymentIdentityError) throw readError;
        throw error;
      }
      if (observedStage === undefined || observedStage <= index) throw error;
    }
  }
  const final = await observeExecutionFence(execute, false);
  if (
    final.stage !== EXECUTION_FENCE_CURRENT_SCHEMA_STAGE ||
    final.rowStage !== EXECUTION_FENCE_CURRENT_SCHEMA_STAGE
  ) {
    throw malformedExecutionFence(
      'initialization did not reach the current schema',
    );
  }
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
    await initializeExecutionFenceProtocol(execute, {
      state: fenceState,
      seededAt,
    });
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
    await initializeExecutionFenceProtocol(execute, {
      state: fenceState,
      seededAt,
    });
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
  await initializeExecutionFenceProtocol(execute, {
    state: fenceState,
    seededAt,
  });
}
