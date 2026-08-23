// SPDX-License-Identifier: Apache-2.0
// The deployment execution fence — the control that lets an operator stop a
// deployment from MINTING work without stopping it from finishing the work it
// already has.
//
// WHY it exists: a flowsafe deployment is one tenant's whole execution surface
// (one Worker, one D1, its own Durable Object namespaces). Moving that surface
// to another deployment is only safe if the old one can be brought to a state
// where nothing new starts, everything outstanding drains, and then nothing
// runs at all — otherwise the two deployments execute the same run's steps
// against the same rows and every exactly-once property this package defends
// dies at the migration boundary.
//
// The states are ordered by how much they forbid, and the ORDER is the whole
// contract:
//
//   open             everything (the steady state)
//   draining         finish what exists; mint nothing new
//   migration-locked execute nothing; reads and admin still answer
//   proof-only       migration-locked, except ONE nominated run — the proof
//                    that the deployment still works before it is reopened
//
// Two invariants hold everywhere the fence is consulted:
//
//   NEVER MEMOIZED. The fence is an operational control an operator moves
//   between requests; a memo would serve a stale answer for the length of an
//   isolate, which is exactly the window a migration is trying to close. One
//   read per request, or one per tick/dispatch PASS — never per row.
//
//   DEGRADE CLOSED. A read that did not reach storage is not evidence that the
//   deployment is open. On a request path it is a 503 (the operator's problem,
//   retryable — the same answer DeploymentIdentityError gets); on an alarm path
//   it is logged, swallowed, and left for the next wake, because a thrown alarm
//   is retried by workerd and would answer a storage incident with a storm.
//
// ABSENT ROW (and absent TABLE) READ AS `open`. That is the 0.19-to-0.20
// upgrade rule and nothing more: a database seeded before this table existed
// must keep serving. Provisioning writes an explicit row from 0.20 on, so a
// deployment that means to start locked says so rather than relying on a
// default — `seed()` therefore takes the state as a REQUIRED argument.

import {
  EXECUTION_FENCE_DDL,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
} from '#deployment-identity-protocol';
import { DoStatusError } from './do-status-error.js';
import { isPathSafeId } from './path-safe-id.js';

/**
 * Rows affected by a write, read from D1's `{ meta: { changes } }` envelope —
 * the same accessor d1-storage exports as `d1Changes`, restated here so this
 * module imports only leaves. Every surface that consults the fence imports
 * it, including ones that must not drag the D1 storage adapter (and
 * @mastra/cloudflare-d1 with it) into their bundle.
 */
function changesOf(result: unknown): number {
  const changes = (result as { meta?: { changes?: number } } | undefined)?.meta
    ?.changes;
  return typeof changes === 'number' ? changes : 0;
}

/**
 * The fence states, ordered from most to least permissive; the table the single
 * row lives in (flowsafe-owned, outside the `mastra_%` guard); and that row's
 * fixed primary key — the fence is a property of the DEPLOYMENT, and a
 * deployment is one database, so there is exactly one row and its key is a
 * constant. A CHECK constraint keeps a second one from ever being inserted,
 * which is what makes every CAS below total rather than "the CAS, on whichever
 * row you meant".
 *
 * All three, and the DDL built from them, come from the provisioning protocol
 * module rather than being declared here: PROVISIONING creates this table
 * (deployment-identity-protocol.mjs), and `CREATE TABLE IF NOT EXISTS` would
 * silently accept a differently-shaped table it had already made — so a second
 * copy of the schema would not fail loudly, it would quietly drop the CHECK
 * constraints. That module is also the only file both sides can share: it ships
 * at the package root for the provisioning CLI and fleet-control, neither of
 * which can import this package's TypeScript.
 */
export { EXECUTION_FENCE_STATES, EXECUTION_FENCE_TABLE };

export type ExecutionFenceState = (typeof EXECUTION_FENCE_STATES)[number];

/** What one fence read observed. `proofKey`/`proofRunId` exist only in proof-only. */
export interface ExecutionFenceReading {
  readonly state: ExecutionFenceState;
  /** The key a proof-only start must carry to be admitted. */
  readonly proofKey?: string;
  /** The run the proof-only state has already admitted, once one started. */
  readonly proofRunId?: string;
}

/** The reading every unfenced surface uses — see `ExecutionFenceStore` absence. */
export const OPEN_EXECUTION_FENCE: ExecutionFenceReading = Object.freeze({
  state: 'open',
});

/**
 * Minimal structural D1 surface, the same posture as SnapshotDatabase and
 * ApprovalDatabase: tests back it with node:sqlite, Workers pass env.DB.
 *
 * `all()` rather than `first()` for the one read, deliberately: this exact
 * shape is what DeploymentIdentityDatabase, SnapshotDatabase, and
 * ApprovalDatabase all already satisfy, so every surface that must consult the
 * fence can hand over the binding it already holds with no cast and no second
 * seam. The fence's row is unique by primary key, so the two are equivalent.
 */
export interface ExecutionFenceDatabase {
  prepare(query: string): ExecutionFenceStatement;
}

export interface ExecutionFenceStatement {
  bind(...values: unknown[]): ExecutionFenceStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/**
 * The refusal a fenced surface answers with: 503, because the deployment is
 * deliberately not executing right now and will be again — an operator's
 * condition, not the caller's mistake and not a code fault. Providers,
 * schedulers, and clients that honour Retry-After semantics therefore
 * redeliver rather than discard.
 */
export class ExecutionFencedError extends DoStatusError {
  readonly status = 503;
  readonly reason: {
    readonly code: 'EXECUTION_FENCED';
    readonly state: ExecutionFenceState;
  };

  constructor(state: ExecutionFenceState, entry?: string) {
    super(
      entry === undefined
        ? `deployment execution is fenced ('${state}')`
        : `deployment execution is fenced ('${state}'): ${entry} is refused`,
    );
    this.name = 'ExecutionFencedError';
    this.reason = { code: 'EXECUTION_FENCED', state };
  }
}

/**
 * A fence transition whose compare-and-set found a different state. 409 rather
 * than 503: the deployment is fine, the CALLER's expectation is stale — two
 * control-plane actors raced, or an operator retried a transition that already
 * landed. The current state rides on the reason so the caller can re-plan
 * without a second round trip.
 */
export class FenceTransitionConflictError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'FENCE_CAS_CONFLICT';
    readonly state: ExecutionFenceState;
  };

  constructor(expected: ExecutionFenceState, current: ExecutionFenceState) {
    super(
      `execution fence transition expected state '${expected}' but found '${current}'`,
    );
    this.name = 'FenceTransitionConflictError';
    this.reason = { code: 'FENCE_CAS_CONFLICT', state: current };
  }
}

/**
 * The fence could not be READ. Deliberately distinct from
 * ExecutionFencedError: no state was observed, so nothing may conclude the
 * deployment is open — which is why this carries the same 503 a refusal does
 * and every request-path caller lets it propagate.
 */
export class ExecutionFenceUnreadableError extends DoStatusError {
  readonly status = 503;
  readonly reason: { readonly code: 'EXECUTION_FENCE_UNREADABLE' };

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'ExecutionFenceUnreadableError';
    this.reason = { code: 'EXECUTION_FENCE_UNREADABLE' };
  }
}

/**
 * Render a fence refusal as the JSON response the taxonomy specifies.
 *
 * For the surfaces that answer with a Response instead of throwing — a Worker
 * router, or a Durable Object route whose own catch would re-map a thrown
 * status. Built from the ERROR so the body a router writes and the body
 * doErrorResponse writes for the same refusal cannot drift apart.
 */
export function executionFencedResponse(
  state: ExecutionFenceState,
  entry?: string,
): Response {
  const refusal = new ExecutionFencedError(state, entry);
  return new Response(
    JSON.stringify({ error: refusal.message, reason: refusal.reason }),
    {
      status: refusal.status,
      headers: {
        'content-type': 'application/json',
        'cache-control': 'no-store',
      },
    },
  );
}

/** Every fence-authored refusal — the family a fenced surface catches as one. */
export type ExecutionFenceRefusal =
  | ExecutionFencedError
  | ExecutionFenceUnreadableError;

/**
 * Whether an error is the fence refusing (or failing to answer). The two are
 * one class for every CALLER that must degrade closed — an alarm swallowing
 * both, a tick skipping its pass — because a state it could not read and a
 * state that forbids the work lead to the same action.
 */
export function isExecutionFenceRefusal(
  error: unknown,
): error is ExecutionFenceRefusal {
  return (
    error instanceof ExecutionFencedError ||
    error instanceof ExecutionFenceUnreadableError
  );
}

function isExecutionFenceState(value: unknown): value is ExecutionFenceState {
  return (
    typeof value === 'string' &&
    (EXECUTION_FENCE_STATES as readonly string[]).includes(value)
  );
}

/**
 * Validate a state name from a control-plane request. Exported because the
 * admin route validates the WIRE shape and the package validates nothing else
 * about a transition: which transitions are legal is host policy, and this
 * package only enforces the state vocabulary, the CAS, and the proof-only
 * key requirement.
 */
export function assertExecutionFenceState(
  value: unknown,
  field: string,
): ExecutionFenceState {
  if (!isExecutionFenceState(value)) {
    throw new InvalidExecutionFenceRequestError(
      `${field} must be one of ${EXECUTION_FENCE_STATES.join(', ')}`,
    );
  }
  return value;
}

/** A malformed control-plane fence request — the caller's to fix. */
export class InvalidExecutionFenceRequestError extends DoStatusError {
  readonly status = 400;

  constructor(message: string) {
    super(message);
    this.name = 'InvalidExecutionFenceRequestError';
  }
}

// ---------------------------------------------------------------------------
// Admission predicates — the semantics matrix, as four total functions.
//
// One predicate per COLUMN of behaviour rather than one per call site, so two
// surfaces that must answer the same way cannot drift apart. Resume, approval
// decide, and signal delivery share `admitsExistingRun` for exactly that
// reason: all three act on a run that already exists, so all three must stay
// open through a drain and all three must admit the proof run and nothing else.
// ---------------------------------------------------------------------------

/**
 * May a NEW run be minted? Only `open`, plus the proof-only exception: a start
 * whose idempotency key is the nominated proof key. The key is internal
 * material (never a request-body field, never an open request-context key), so
 * nothing a tenant can send reaches this branch.
 */
export function admitsRunStart(
  reading: ExecutionFenceReading,
  idempotencyKey?: string,
): boolean {
  if (reading.state === 'open') return true;
  if (reading.state !== 'proof-only') return false;
  return (
    reading.proofKey !== undefined &&
    idempotencyKey !== undefined &&
    idempotencyKey === reading.proofKey
  );
}

/**
 * May work proceed on a run that ALREADY exists — resume, approval decide,
 * signal delivery? Through a drain, yes: a drain that refused these could
 * never finish, because finishing is what the suspended runs are waiting for.
 * In proof-only, only the nominated run.
 */
export function admitsExistingRun(
  reading: ExecutionFenceReading,
  runId?: string,
): boolean {
  if (reading.state === 'open' || reading.state === 'draining') return true;
  if (reading.state !== 'proof-only') return false;
  return (
    reading.proofRunId !== undefined &&
    runId !== undefined &&
    runId === reading.proofRunId
  );
}

/**
 * May standing configuration that ARMS future work be authored — a schedule
 * created/updated/resumed, an objective set, a due schedule fire claimed?
 * `open` only. Pausing and deleting stay allowed in every state: they remove
 * future work, which is the direction a drain is going.
 */
export function admitsWorkAuthoring(reading: ExecutionFenceReading): boolean {
  return reading.state === 'open';
}

/**
 * May queued, already-owned work be executed — a background task body, a
 * notification dispatch pass? Through a drain, yes; that queue IS the work a
 * drain exists to finish. Not in proof-only: the proof is one run, and a task
 * queue re-driving itself alongside it is not a proof of anything.
 */
export function admitsDrainableExecution(
  reading: ExecutionFenceReading,
): boolean {
  return reading.state === 'open' || reading.state === 'draining';
}

interface ExecutionFenceRow {
  state?: unknown;
  proof_key?: unknown;
  proof_run_id?: unknown;
}

function readingFromRow(row: ExecutionFenceRow): ExecutionFenceReading {
  const { state } = row;
  if (!isExecutionFenceState(state)) {
    // Fail CLOSED on a state name this build does not know: a hand-edited row,
    // or a row written by a NEWER flowsafe that added a state. Returning
    // `open` for either would answer "I do not understand this fence" with
    // "there is no fence", which is the one answer that must never be wrong.
    throw new ExecutionFenceUnreadableError(
      `execution fence row carries an unrecognized state '${String(state)}'`,
    );
  }
  const proofKey = row.proof_key;
  const proofRunId = row.proof_run_id;
  return {
    state,
    ...(typeof proofKey === 'string' && proofKey.length > 0
      ? { proofKey }
      : {}),
    ...(typeof proofRunId === 'string' && proofRunId.length > 0
      ? { proofRunId }
      : {}),
  };
}

/**
 * SQLite/D1's "no such table". Same message match, and the same reasoning, as
 * d1-storage's purge helpers: the structural database seam carries no error
 * codes, and a table that was never created is not a fault — for this store it
 * is a pre-0.20 database, which reads as `open`.
 */
function isMissingFenceTable(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return (
    /no such table/i.test(message) && message.includes(EXECUTION_FENCE_TABLE)
  );
}

export interface ExecutionFenceStoreOptions {
  /** Injectable clock for `updated_at` (tests, deterministic fixtures). */
  now?: () => number;
}

export interface ExecutionFenceTransition {
  /** The state the caller believes the fence is in. */
  expected: ExecutionFenceState;
  /** The state to move to. */
  next: ExecutionFenceState;
  /** Required when `next` is 'proof-only'; rejected otherwise. */
  proofKey?: string;
}

/**
 * The deployment's fence, over one D1 database — the SAME database the runner's
 * snapshots and the deployment sentinel live in, so the fence cannot be
 * separated from the state it fences by any binding mistake.
 */
export class ExecutionFenceStore {
  readonly #db: ExecutionFenceDatabase;
  readonly #now: () => number;

  constructor(
    db: ExecutionFenceDatabase,
    options: ExecutionFenceStoreOptions = {},
  ) {
    this.#db = db;
    this.#now = options.now ?? Date.now;
  }

  /**
   * The current fence state. NEVER memoized (see the module header) and never
   * a write: this is on every gated request path, and a read path that emits
   * `CREATE TABLE IF NOT EXISTS` is a write path wearing a read's name — it
   * would make a fenced deployment mutate its own database to answer a
   * question, and would turn a read-only replica or a revoked-write incident
   * into an outage instead of a degrade.
   *
   * A missing table and a missing row both read as `open` — the 0.19 upgrade
   * rule. Anything else that fails becomes ExecutionFenceUnreadableError, so
   * no caller can mistake a storage fault for an open deployment.
   */
  async read(): Promise<ExecutionFenceReading> {
    let rows: ExecutionFenceRow[];
    try {
      rows = (
        await this.#db
          .prepare(
            `SELECT state, proof_key, proof_run_id FROM ${EXECUTION_FENCE_TABLE}
             WHERE id = ?`,
          )
          .bind(EXECUTION_FENCE_ROW_ID)
          .all<ExecutionFenceRow>()
      ).results;
    } catch (error) {
      if (isMissingFenceTable(error)) return OPEN_EXECUTION_FENCE;
      throw new ExecutionFenceUnreadableError(
        'execution fence state is not readable',
        { cause: error },
      );
    }
    const row = rows[0];
    if (row === undefined) return OPEN_EXECUTION_FENCE;
    return readingFromRow(row);
  }

  /**
   * Provisioning-time seeding: write the deployment's INITIAL fence state.
   *
   * INSERT-if-absent, never an overwrite. Seeding runs on every provisioning
   * pass (including the already-owned early return), so a crash between the
   * deployment sentinel and this row heals on the next attempt — but a
   * re-provision of a LIVE deployment must never silently reopen a fence an
   * operator closed, which an upsert would do.
   *
   * `state` has no default on purpose. The failure this closes is a migration
   * host forgetting to ask for `migration-locked` and silently getting `open`;
   * making the argument required turns that into a compile-time obligation,
   * while still letting a host that wants an open deployment say so.
   */
  async seed(state: ExecutionFenceState): Promise<void> {
    const safeState = assertExecutionFenceState(state, 'seed state');
    await this.#createTable();
    await this.#db
      .prepare(
        `INSERT OR IGNORE INTO ${EXECUTION_FENCE_TABLE}
           (id, state, proof_key, proof_run_id, updated_at)
         VALUES (?, ?, NULL, NULL, ?)`,
      )
      .bind(EXECUTION_FENCE_ROW_ID, safeState, this.#now())
      .run();
  }

  /**
   * Move the fence, compare-and-set on the CURRENT state. One conditional
   * UPDATE, so two control-plane actors racing the same transition cannot both
   * win: the loser changes zero rows and gets the state the winner left behind.
   *
   * Unlike `read()` this MAY create the table — a transition is a control-plane
   * write, and a legacy database whose fence is implicitly open has no row to
   * compare against. The row is materialized as `open` first, which is the
   * state the implicit reading already reported, so the CAS that follows means
   * exactly what it would have meant on a seeded database.
   */
  async transition(
    input: ExecutionFenceTransition,
  ): Promise<ExecutionFenceReading> {
    const expected = assertExecutionFenceState(
      input.expected,
      'expected state',
    );
    const next = assertExecutionFenceState(input.next, 'next state');
    const proofKey = this.#proofKeyFor(next, input.proofKey);
    await this.#createTable();
    // Materialize the implicit-open row of a pre-0.20 database. INSERT OR
    // IGNORE, so a seeded database is untouched and the CAS below is still the
    // only thing that decides the outcome.
    await this.#db
      .prepare(
        `INSERT OR IGNORE INTO ${EXECUTION_FENCE_TABLE}
           (id, state, proof_key, proof_run_id, updated_at)
         VALUES (?, 'open', NULL, NULL, ?)`,
      )
      .bind(EXECUTION_FENCE_ROW_ID, this.#now())
      .run();
    // proof_run_id is cleared unconditionally: ENTERING proof-only must not
    // inherit a previous proof's run, and LEAVING it must not leave a stale
    // admission behind for the next one to trip over.
    const changed = changesOf(
      await this.#db
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE}
             SET state = ?, proof_key = ?, proof_run_id = NULL, updated_at = ?
           WHERE id = ? AND state = ?`,
        )
        .bind(
          next,
          proofKey ?? null,
          this.#now(),
          EXECUTION_FENCE_ROW_ID,
          expected,
        )
        .run(),
    );
    if (changed === 0) {
      throw new FenceTransitionConflictError(
        expected,
        (await this.read()).state,
      );
    }
    return { state: next, ...(proofKey === undefined ? {} : { proofKey }) };
  }

  /**
   * Bind the proof-only state to the run it admitted, conditionally.
   *
   * The condition is the whole point: between the read that ADMITTED a start
   * and this write-back the fence may have moved, or a different run may have
   * claimed the proof. Zero rows changed means the caller must refuse the
   * start it was about to make — the fence is no longer the one it read.
   * Re-writing the SAME runId is admitted so a retry of an interrupted start
   * converges instead of deadlocking on its own earlier write.
   */
  async recordProofRun(proofKey: string, runId: string): Promise<boolean> {
    if (!isPathSafeId(proofKey)) {
      throw new InvalidExecutionFenceRequestError(
        'proofKey must be a URL-path-safe identifier',
      );
    }
    if (!isPathSafeId(runId)) {
      throw new InvalidExecutionFenceRequestError(
        'proof runId must be a URL-path-safe identifier',
      );
    }
    try {
      return (
        changesOf(
          await this.#db
            .prepare(
              `UPDATE ${EXECUTION_FENCE_TABLE}
                 SET proof_run_id = ?, updated_at = ?
               WHERE id = ? AND state = 'proof-only' AND proof_key = ?
                 AND (proof_run_id IS NULL OR proof_run_id = ?)`,
            )
            .bind(runId, this.#now(), EXECUTION_FENCE_ROW_ID, proofKey, runId)
            .run(),
        ) > 0
      );
    } catch (error) {
      // A database with no fence table cannot be in proof-only, so there is
      // nothing to record and nothing to conclude beyond "not admitted".
      if (isMissingFenceTable(error)) return false;
      throw new ExecutionFenceUnreadableError(
        'execution fence proof run could not be recorded',
        { cause: error },
      );
    }
  }

  #proofKeyFor(
    next: ExecutionFenceState,
    proofKey: unknown,
  ): string | undefined {
    if (next === 'proof-only') {
      if (!isPathSafeId(proofKey)) {
        throw new InvalidExecutionFenceRequestError(
          "a URL-path-safe proofKey is required to enter 'proof-only'",
        );
      }
      return proofKey;
    }
    if (proofKey !== undefined) {
      // Rejected rather than ignored: a caller passing a key for a state that
      // has no proof believes something about this transition that is false.
      throw new InvalidExecutionFenceRequestError(
        `proofKey applies only to 'proof-only' transitions, not '${next}'`,
      );
    }
    return undefined;
  }

  async #createTable(): Promise<void> {
    await this.#db.prepare(EXECUTION_FENCE_DDL).run();
  }
}
