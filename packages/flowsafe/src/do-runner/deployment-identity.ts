// SPDX-License-Identifier: Apache-2.0
// biome-ignore-all assist/source/organizeImports: distinct re-exports keep both public API docs attached
// Deployment identity — the one isolation control a single-tenant deployment
// still enforces in code. Physical separation (one Worker + one D1 + dedicated
// Durable Object namespaces per customer organization) replaced the pooled
// tenant predicates, so the residual failure mode is MIS-WIRING: a
// provisioning bug that binds organization B's Worker to organization A's
// database would be a full cross-organization breach with no in-code predicate
// left to catch it.
//
// Provisioning stamps the deployment tag twice — in DEPLOYMENT_TENANT and in
// D1 — and every entry surface verifies the pair before touching data. Worker
// topologies also authenticate requests to Durable Objects with a
// per-deployment secret because a cross-script binding supplies the target
// object's otherwise-valid env and D1. Missing, malformed, or mismatched state
// fails closed. The tag remains audit attribution from infrastructure, never a
// request claim.

import {
  assertDeploymentIdentitySecret,
  assertValidDeploymentTag,
  DEPLOYMENT_IDENTITY_HEADER,
  DEPLOYMENT_TAG_PATTERN,
  DeploymentIdentityError,
  deploymentIdentityHeaders,
  type DeploymentIdentityProtocolExecutor,
  type InitialExecutionFenceState,
  provisionDeploymentIdentityProtocol,
  readDeploymentIdentityProtocol,
} from '#deployment-identity-protocol';

/**
 * The deployment tag charset. Deliberately strict — lowercase alphanumeric,
 * 3-32 chars — because the tag doubles as provisioning material: a subdomain
 * label, a D1 database-name component, a Worker-name suffix. Provisioning
 * validates it before creating resources; the runtime re-validates so a
 * hand-edited binding cannot smuggle whitespace or a look-alike into audit
 * attribution.
 */
export { DEPLOYMENT_TAG_PATTERN };

/**
 * A failed deployment-identity assertion. Fail-closed by design: callers map
 * it to a 503 (the deployment is mis-provisioned; an operator must intervene)
 * rather than a generic 500, so a wiring fault is distinguishable from a code
 * fault in logs and monitors.
 */
export { DeploymentIdentityError };

export {
  assertDeploymentIdentitySecret,
  DEPLOYMENT_IDENTITY_HEADER,
  deploymentIdentityHeaders,
};

/**
 * The execution-fence state a deployment is provisioned into — 'open' or
 * 'migration-locked'. Named here so a host wiring `seedDeploymentIdentity`
 * types the choice rather than passing a bare string.
 */
export type { InitialExecutionFenceState };

/**
 * Minimal structural D1 surface the sentinel check uses — same posture as
 * SnapshotDatabase: tests back it with node:sqlite, Workers pass env.DB.
 */
export interface DeploymentIdentityDatabase {
  prepare(query: string): DeploymentIdentityStatement;
}

export interface DeploymentIdentityStatement {
  bind(...values: unknown[]): DeploymentIdentityStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/**
 * The env bindings the guard reads, structurally. A deployment's own Env
 * satisfies this once it declares the DEPLOYMENT_TENANT var and the D1
 * binding.
 */
export interface DeploymentIdentityEnv {
  /** The deployment tag, stamped by provisioning. */
  DEPLOYMENT_TENANT?: string;
  /** The deployment's D1 database, carrying the sentinel row. */
  DB?: DeploymentIdentityDatabase;
  /** Per-deployment Worker-to-DO credential. */
  DEPLOYMENT_IDENTITY_SECRET?: string;
}

function assertValidTag(tag: unknown, caller: string): asserts tag is string {
  assertValidDeploymentTag(tag, caller);
}

/**
 * Whether a binding is a D1 database rather than something else bound under the
 * same name.
 *
 * Exported for this package's own modules and deliberately NOT re-exported from
 * `./index.js` — the same posture `safeDecodeSegment` has. It is internal
 * plumbing two files must agree on (durable-object.ts asks the same question of
 * `env.DB` before it insists on a fence), not a contract a consumer should be
 * able to pin; barrel-exporting it would inflate the semver surface of a
 * predicate that exists to describe Cloudflare's binding shapes.
 */
export function isDatabaseBinding(
  db: unknown,
): db is DeploymentIdentityDatabase {
  if ((typeof db !== 'object' && typeof db !== 'function') || db === null) {
    return false;
  }
  const candidate = db as { prepare?: unknown; fetch?: unknown };
  // An RPC binding — a service binding with a named `entrypoint`, or a Durable
  // Object stub — is a proxy that answers EVERY property with a callable, so a
  // bare `typeof binding.prepare === 'function'` test says yes to all of them.
  // The sentinel pass then calls prepare() on the proxy and the request dies
  // with "The RPC receiver does not implement the method \"prepare\"".
  //
  // Discriminate on `fetch`: every Fetcher-shaped binding has one and
  // D1Database has none, so this excludes the whole RPC family without naming
  // any of them. Fleet trusted state is exactly this combination — a `DB`
  // binding beside `OUTBOUND_PROXY` bound to the shared outbound Worker's
  // `StateEgress` entrypoint — so without this the deployment sentinel fails
  // on the first request every such Worker serves.
  if (typeof candidate.fetch === 'function') return false;
  return typeof candidate.prepare === 'function';
}

function databaseBinding(
  db: unknown,
  caller: string,
): asserts db is DeploymentIdentityDatabase {
  if (!isDatabaseBinding(db)) {
    throw new DeploymentIdentityError(
      `${caller}: the deployment carries no valid DB binding`,
    );
  }
}

function deploymentDatabases(
  env: DeploymentIdentityEnv,
  caller: string,
): readonly DeploymentIdentityDatabase[] {
  databaseBinding(env?.DB, caller);
  const databases = new Set<DeploymentIdentityDatabase>([env.DB]);
  for (const key of Reflect.ownKeys(env)) {
    let binding: unknown;
    try {
      binding = Reflect.get(env, key);
    } catch (cause) {
      throw new DeploymentIdentityError(
        `${caller}: failed to inspect deployment binding '${String(key)}' (${String(cause)})`,
      );
    }
    if (isDatabaseBinding(binding)) databases.add(binding);
  }
  return [...databases];
}

function deploymentIdentityProtocolExecutor(
  db: DeploymentIdentityDatabase,
): DeploymentIdentityProtocolExecutor {
  return async (statement) => {
    let prepared = db.prepare(statement.sql);
    if (statement.bindings.length > 0) {
      prepared = prepared.bind(...statement.bindings);
    }
    if (statement.mode === 'write') {
      await prepared.run();
      return [];
    }
    return (await prepared.all<Record<string, unknown>>()).results;
  };
}

/**
 * Provisioning-time seeding: stamp the database with its owning deployment's
 * tag, and write its initial execution-fence row. Idempotent for the SAME tag
 * (re-running provisioning is safe); throws for a DIFFERENT tag — a database is
 * never silently re-homed; deployment decommissioning deletes it instead.
 *
 * `initialExecutionFenceState` is REQUIRED and has no default: a host that
 * means to bring a deployment up already locked for a migration must say so,
 * and one that forgets must not silently get an executing deployment. The fence
 * row is INSERT-if-absent, so re-seeding never reopens a fence an operator
 * closed.
 */
export async function seedDeploymentIdentity(
  db: DeploymentIdentityDatabase,
  tag: string,
  initialExecutionFenceState: InitialExecutionFenceState,
): Promise<void> {
  databaseBinding(db, 'seedDeploymentIdentity');
  await provisionDeploymentIdentityProtocol(
    deploymentIdentityProtocolExecutor(db),
    tag,
    { initialExecutionFenceState },
  );
}

/**
 * Read the database's sentinel tag, or undefined when seeding has not completed
 * (no table, or the exact table exists with no row after an interrupted write).
 * A malformed schema or duplicate row throws DeploymentIdentityError. Any OTHER
 * database error (a transient D1 fault) propagates unchanged: an outage is an
 * availability problem and must not be misreported as "unprovisioned".
 */
export async function readDeploymentIdentity(
  db: DeploymentIdentityDatabase,
): Promise<string | undefined> {
  databaseBinding(db, 'readDeploymentIdentity');
  return readDeploymentIdentityProtocol(deploymentIdentityProtocolExecutor(db));
}

/**
 * The core assertion: the environment-configured tag and the database's
 * sentinel row must both exist and agree. Throws DeploymentIdentityError on
 * any other outcome — missing binding, malformed tag, unseeded database,
 * mismatch. One cheap invariant instead of predicates on every query.
 */
export async function assertDeploymentIdentity(
  db: DeploymentIdentityDatabase,
  expectedTag: string,
): Promise<void> {
  assertValidTag(expectedTag, 'assertDeploymentIdentity');
  const stored = await readDeploymentIdentity(db);
  if (stored === undefined) {
    throw new DeploymentIdentityError(
      `assertDeploymentIdentity: the database carries no deployment sentinel — provisioning must seed it (seedDeploymentIdentity) before this deployment serves`,
    );
  }
  if (stored !== expectedTag) {
    throw new DeploymentIdentityError(
      `assertDeploymentIdentity: this deployment is configured as '${expectedTag}' but its database belongs to '${stored}' — refusing to serve (mis-provisioned bindings)`,
    );
  }
}

// Success memo per (db object, tag): a deployment's identity cannot change
// while the isolate lives, so one verified read amortizes over every request.
// FAILURES ARE NEVER MEMOIZED — a transient D1 error must not brick the
// isolate, and a real mismatch stays a per-request refusal (cheap: it answers
// before any work happens). WeakMap so a test harness cycling env objects
// never leaks.
const verifiedIdentity = new WeakMap<object, Map<string, Promise<void>>>();

/**
 * Memoized assertion for hot paths (the Worker fetch pipeline, DO routes).
 * First call per isolate performs the sentinel read; later calls ride the
 * memoized success. A failed check clears the memo so the next request
 * retries instead of pinning the isolate to a dead promise — the same
 * containment pattern as the approval store's schema memo.
 */
export function ensureDeploymentIdentity(
  db: DeploymentIdentityDatabase,
  expectedTag: string,
): Promise<void> {
  try {
    databaseBinding(db, 'ensureDeploymentIdentity');
    assertValidTag(expectedTag, 'ensureDeploymentIdentity');
  } catch (error) {
    return Promise.reject(error);
  }
  let byTag = verifiedIdentity.get(db);
  if (!byTag) {
    byTag = new Map();
    verifiedIdentity.set(db, byTag);
  }
  let pending = byTag.get(expectedTag);
  if (!pending) {
    pending = assertDeploymentIdentity(db, expectedTag).catch(
      (error: unknown) => {
        byTag.delete(expectedTag);
        throw error;
      },
    );
    byTag.set(expectedTag, pending);
  }
  return pending;
}

/** Validate every Worker-side deployment binding before route dispatch. */
export function ensureDeploymentIdentityBindings(
  env: DeploymentIdentityEnv,
): Promise<void> {
  let databases: readonly DeploymentIdentityDatabase[];
  const expectedTag = env?.DEPLOYMENT_TENANT;
  try {
    databases = deploymentDatabases(env, 'ensureDeploymentIdentityBindings');
    assertValidTag(expectedTag, 'ensureDeploymentIdentityBindings');
    assertDeploymentIdentitySecret(
      env?.DEPLOYMENT_IDENTITY_SECRET,
      'ensureDeploymentIdentityBindings',
    );
  } catch (error) {
    return Promise.reject(error);
  }
  return Promise.all(
    databases.map((db) => ensureDeploymentIdentity(db, expectedTag)),
  ).then(() => undefined);
}

/** Clone a raw upgrade/forward request and overwrite the internal credential. */
export function stampDeploymentIdentityRequest(
  request: Request,
  secret: string,
): Request {
  assertDeploymentIdentitySecret(secret, 'stampDeploymentIdentityRequest');
  const forwarded = new Request(request);
  forwarded.headers.set(DEPLOYMENT_IDENTITY_HEADER, secret);
  return forwarded;
}

interface TimingSafeSubtleCrypto extends SubtleCrypto {
  timingSafeEqual(
    actual: ArrayBuffer | ArrayBufferView,
    expected: ArrayBuffer | ArrayBufferView,
  ): boolean;
}

function supportsTimingSafeEqual(
  subtle: SubtleCrypto,
): subtle is TimingSafeSubtleCrypto {
  return (
    typeof (subtle as Partial<TimingSafeSubtleCrypto>).timingSafeEqual ===
    'function'
  );
}

/** Compare two credentials without an early-exit string comparison. */
export async function credentialsMatch(
  actual: string,
  expected: string,
): Promise<boolean> {
  const encoded = new TextEncoder();
  const [actualHash, expectedHash] = await Promise.all([
    crypto.subtle.digest('SHA-256', encoded.encode(actual)),
    crypto.subtle.digest('SHA-256', encoded.encode(expected)),
  ]);
  if (supportsTimingSafeEqual(crypto.subtle)) {
    return crypto.subtle.timingSafeEqual(actualHash, expectedHash);
  }

  // Node's Web Crypto does not expose Workers' timingSafeEqual. Tests and
  // non-Workers adapters compare the fixed-size digests without early exits.
  const actualBytes = new Uint8Array(actualHash);
  const expectedBytes = new Uint8Array(expectedHash);
  let difference = 0;
  for (let index = 0; index < actualBytes.length; index += 1) {
    difference |= (actualBytes[index] ?? 0) ^ (expectedBytes[index] ?? 0);
  }
  return difference === 0;
}

/**
 * The Durable Object initialization check. Every flowsafe DO base class calls
 * this before serving its first request:
 *
 * - Under workerd (state present), the env MUST carry both DEPLOYMENT_TENANT
 *   and DB, and the sentinel must match — a DO namespace bound to the wrong
 *   deployment (or a Worker bound to the wrong database) refuses instead of
 *   serving another organization's data. Missing bindings fail closed too:
 *   an unguarded production DO is exactly the silent gap this check exists
 *   to close.
 * - Off workerd (node/vitest, state undefined), the check is skipped — the
 *   same posture as every other workerd-only affordance on these classes
 *   (hibernatable WebSockets and Durable Object alarm/recovery metadata).
 */
export function verifyDurableObjectDeploymentIdentity(
  state: { id?: { name?: string } } | undefined,
  env: unknown,
): Promise<string | undefined> {
  if (state === undefined) return Promise.resolve(undefined);
  const bindings = (env ?? {}) as DeploymentIdentityEnv;
  const expectedTag = bindings.DEPLOYMENT_TENANT;
  let databases: readonly DeploymentIdentityDatabase[];
  try {
    databases = deploymentDatabases(
      bindings,
      'verifyDurableObjectDeploymentIdentity',
    );
    assertValidTag(expectedTag, 'verifyDurableObjectDeploymentIdentity');
    assertDeploymentIdentitySecret(
      bindings.DEPLOYMENT_IDENTITY_SECRET,
      'verifyDurableObjectDeploymentIdentity',
    );
  } catch (error) {
    return Promise.reject(error);
  }
  return Promise.all(
    databases.map((db) => ensureDeploymentIdentity(db, expectedTag)),
  ).then(() => expectedTag);
}

/**
 * Fetch-path verification: target-side tag/sentinel validation plus caller
 * attestation. A cross-script namespace binding supplies the target object's
 * env, so the request credential is what proves the source Worker belongs to
 * the same deployment.
 */
export async function verifyDurableObjectDeploymentRequest(
  request: Request,
  state: { id?: { name?: string } } | undefined,
  env: unknown,
): Promise<string | undefined> {
  if (state === undefined) {
    return verifyDurableObjectDeploymentIdentity(state, env);
  }
  const expected = (env as DeploymentIdentityEnv).DEPLOYMENT_IDENTITY_SECRET;
  assertDeploymentIdentitySecret(
    expected,
    'verifyDurableObjectDeploymentRequest',
  );
  const actual = request.headers.get(DEPLOYMENT_IDENTITY_HEADER);
  if (actual === null || !(await credentialsMatch(actual, expected))) {
    throw new DeploymentIdentityError(
      "verifyDurableObjectDeploymentRequest: caller does not carry this deployment's internal identity credential",
    );
  }
  return verifyDurableObjectDeploymentIdentity(state, env);
}
