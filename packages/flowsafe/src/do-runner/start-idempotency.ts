// SPDX-License-Identifier: Apache-2.0
// Owner-bound idempotent start — the reservation that makes "start this once"
// mean once, across retries, isolates, and deployments.
//
// WHY it exists: a run start is the moment a deployment commits to spending
// money. The first step of a workflow can wire funds, file an order, or call a
// paid API, and every layer between a caller and that step is allowed to lose a
// RESPONSE without losing the WORK — a Worker eviction, a client timeout, a
// load balancer retry, an operator re-running a script. Without a reservation
// the only honest answer to "did my start land?" is "retry and find out", and
// that answer charges the card twice.
//
// The reservation is what a key BUYS: a durable row, written before anything
// executes, that says which run this key already means. A retry carrying the
// same key does not start a second run — it finds the first one and is told
// what happened to it.
//
// THE TAXONOMY IS THE CONTRACT. Every refusal below is a distinct sentence
// about what flowsafe KNOWS, because the caller's next action differs for each
// and a collapsed code would make them all "retry":
//
//   IDEMPOTENT_START_OWNER_MISMATCH (403) this key is somebody else's. Keys are
//                                   owner-scoped, so one tenant principal
//                                   cannot probe, hijack, or collide with
//                                   another's — and never learns more than
//                                   "not yours".
//   IDEMPOTENT_START_TARGET_MISMATCH (409) the key is yours but names a
//                                   different workflow/agent than last time.
//                                   Reusing a key across targets is a caller
//                                   bug, and silently honouring it would make
//                                   one key mean two different charges.
//   IDEMPOTENT_START_PENDING (503)   the run this key names is RUNNING right
//                                   now. Retryable, and legitimately unbounded:
//                                   the first persisted summary lands at the
//                                   first suspend or terminal state, so a long
//                                   live run has no summary yet and is not lost.
//                                   `pendingSince` lets a caller reason about
//                                   how long, without this package pretending a
//                                   timeout would be safe.
//   IDEMPOTENT_START_UNRESOLVABLE (409) the claim was taken, nothing persisted,
//                                   and nothing is running. Whether a side
//                                   effect fired before the crash is UNKNOWABLE
//                                   to flowsafe. So it refuses, and says so, and
//                                   never re-executes on its own. A host that
//                                   investigates and decides to re-run does it
//                                   with a FRESH key — a deliberate second
//                                   charge, not one this package invented.
//   IDEMPOTENT_START_ALREADY_SETTLED (409) the run finished and its summary has
//                                   aged out. The reservation deliberately
//                                   OUTLIVES the snapshot so this answer exists
//                                   at all; the alternative is a purged run
//                                   looking exactly like a fresh key.
//
// NO TIMER ANYWHERE. Two of those branches are separated by a LIVENESS PROBE,
// never by elapsed time. A timer would have to guess a bound on legitimate
// in-flight work, and every guess is wrong in the expensive direction: too
// short and a long live run is declared dead, inviting a fresh key and a second
// charge; too long and a genuinely crashed start wedges its key. The probe asks
// the run's own host whether it is executing, which is the question a timer was
// only ever approximating.
//
// THE CLAIM IS THE SERIALIZER. `reserve()` decides which runId a key means;
// `claim()` decides who gets to START it. The claim is one conditional UPDATE,
// so exactly one caller changes a row and every other caller reads the outcome
// instead of racing it. That matters most where Durable Object serialization
// cannot help: agent runs live in thread objects keyed by threadId, so two
// same-key starts naming different threads are two different objects with no
// shared lock at all. This CAS is the only thing between them.
//
// RUN IDS ARE NEVER MINTED HERE. Run ids are server-minted, at the host's own
// existing mint sites; the reservation STORES one and hands the same id back to
// every later caller. A store that generated ids would be a second minting
// authority, and the whole rule is that there is exactly one.

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import { DoStatusError } from './do-status-error.js';
import type { ExecutionFenceWiring } from './execution-fence.js';
import { isExecutionFenceRefusal } from './execution-fence.js';
import { isPathSafeId } from './path-safe-id.js';

/**
 * The reservation table — flowsafe-owned, so outside the `mastra_%` schema
 * guard, and created lazily by the first `reserve()` rather than by the
 * provisioning protocol. Unlike the execution fence (whose ABSENCE has to read
 * as a state, so provisioning writes an explicit row), an absent reservation
 * table simply means no key has ever been used on this deployment, which is
 * indistinguishable from an empty one.
 */
export const START_IDEMPOTENCY_TABLE = 'flowsafe_start_idempotency';

export const START_RESERVATION_STATES = [
  'reserved',
  'started',
  'terminal',
] as const;

/**
 * Where a reservation is in its life:
 *
 *   reserved  the key means this runId, and nobody has started it yet
 *   started   one caller won the claim and is (or was) executing
 *   terminal  the run reached a terminal state; the key is spent
 *
 * The states only ever move forward, with ONE exception: a start refused by the
 * execution fence rolls `started` back to `reserved` (see `release`), because a
 * fence refusal is the one failure that provably executed nothing.
 */
export type StartReservationState = (typeof START_RESERVATION_STATES)[number];

export const START_TARGET_KINDS = ['workflow', 'agent'] as const;

/** Which execution family a key names — a workflow run, or an agent run. */
export type StartTargetKind = (typeof START_TARGET_KINDS)[number];

/**
 * WHO a key belongs to. An execution principal, projected to the same two
 * fields `ResourceOwner` carries, and for the same reason: a key is a
 * capability to converge on somebody's run, so it must be scoped to whoever
 * created it and unforgeable from tenant traffic.
 */
export interface StartReservationOwner {
  readonly kind: ExecutionPrincipalKind;
  readonly id: string;
}

/** One reservation row, as every surface reads it. */
export interface StartReservation {
  readonly key: string;
  readonly owner: StartReservationOwner;
  readonly targetKind: StartTargetKind;
  readonly targetId: string;
  readonly runId: string;
  /**
   * The agent run's thread, when the target is an agent. It is the run's
   * ADDRESS: a workflow run is reachable from (workflowId, runId) alone, but an
   * agent run lives in a thread object and a retry that minted a fresh thread
   * would otherwise have no way back to the original. Absent for workflows,
   * where storing a derivable address would be a second source of truth.
   */
  readonly threadId?: string;
  readonly state: StartReservationState;
  /** Epoch ms; the key's own age, which the purge horizon is measured from. */
  readonly createdAt: number;
  /** Epoch ms of the last state change — `pendingSince` on a live claim. */
  readonly updatedAt: number;
}

export interface StartReservationRequest {
  key: string;
  owner: StartReservationOwner;
  targetKind: StartTargetKind;
  targetId: string;
  /**
   * The host's own run-id mint — `context.newRunId()` on the run router, the
   * thread topology's minted id on the agent surface. Run ids are server-minted
   * and this store never generates one; it only ever stores what it is handed.
   *
   * A THUNK rather than a value so the store, not the caller, decides when an
   * id is needed: the caller that loses the insert never uses its candidate,
   * and a host whose mint is expensive or audited should not pay for one it
   * throws away. (The mints in this package are pure `crypto.randomUUID()`
   * derivations, so a discarded candidate costs nothing and leaks nothing.)
   */
  mintRunId: () => string;
  /** Required for `targetKind: 'agent'`, rejected for 'workflow'. */
  threadId?: string;
}

export interface StartReservationOutcome {
  /** The authoritative reservation — this caller's, or the winner's. */
  reservation: StartReservation;
  /**
   * Whether THIS call created the row. Only a creator may go straight to the
   * claim; everyone else takes the replay path, which is where the "what
   * happened to the first one?" answers live.
   */
  created: boolean;
}

/**
 * The key names a reservation owned by a different principal.
 *
 * 403 rather than 404: the caller sent a syntactically valid key it is simply
 * not entitled to, and this is the one refusal whose body deliberately carries
 * nothing else — not the owner, not the target, not the run. A key is guessable
 * by construction (hosts derive them from order ids and request ids), so this
 * response is reachable by probing, and everything it does not say is something
 * a prober does not learn.
 */
export class StartReservationOwnerMismatchError extends DoStatusError {
  readonly status = 403;
  readonly reason: { readonly code: 'IDEMPOTENT_START_OWNER_MISMATCH' };

  constructor(key: string) {
    super(`idempotency key '${key}' belongs to another principal`);
    this.name = 'StartReservationOwnerMismatchError';
    this.reason = { code: 'IDEMPOTENT_START_OWNER_MISMATCH' };
  }
}

/**
 * The caller's own key, pointed at a different workflow or agent than the one
 * it reserved. 409, and it DOES name the reservation's target: the caller owns
 * this key, so telling it what the key already means is telling it about its
 * own state, and a caller that reused a key by accident needs exactly that to
 * find the bug.
 */
export class StartReservationTargetMismatchError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'IDEMPOTENT_START_TARGET_MISMATCH';
    readonly targetKind: StartTargetKind;
    readonly targetId: string;
  };

  constructor(key: string, reservation: StartReservation) {
    super(
      `idempotency key '${key}' already names ${reservation.targetKind} '${reservation.targetId}'`,
    );
    this.name = 'StartReservationTargetMismatchError';
    this.reason = {
      code: 'IDEMPOTENT_START_TARGET_MISMATCH',
      targetKind: reservation.targetKind,
      targetId: reservation.targetId,
    };
  }
}

/**
 * The key's run is executing right now. 503 for the same reason every
 * operator-transient refusal in this package is: the condition is real, it is
 * nobody's mistake, and it clears on its own — so a client that honours
 * retry semantics converges instead of giving up.
 */
export class IdempotentStartPendingError extends DoStatusError {
  readonly status = 503;
  readonly reason: {
    readonly code: 'IDEMPOTENT_START_PENDING';
    readonly runId: string;
    /** Epoch ms of the claim, so a caller can reason about how long. */
    readonly pendingSince: number;
  };

  constructor(reservation: StartReservation) {
    super(
      `run '${reservation.runId}' for this idempotency key is still starting`,
    );
    this.name = 'IdempotentStartPendingError';
    this.reason = {
      code: 'IDEMPOTENT_START_PENDING',
      runId: reservation.runId,
      pendingSince: reservation.updatedAt,
    };
  }
}

/**
 * The claim was taken, nothing persisted, and nothing is running.
 *
 * This is the refusal that refuses to guess. The message is written for the
 * human who will read it in a log at 3am, because the decision it asks for is
 * a judgement call this package cannot make: a side effect may or may not have
 * fired before the crash, and only the host knows whether its first step is the
 * kind that charges.
 */
export class IdempotentStartUnresolvableError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'IDEMPOTENT_START_UNRESOLVABLE';
    readonly runId: string;
  };

  constructor(reservation: StartReservation) {
    super(
      `run '${reservation.runId}' for this idempotency key was claimed but never persisted and is not running — investigate whether its first step already took effect before re-running with a fresh key`,
    );
    this.name = 'IdempotentStartUnresolvableError';
    this.reason = {
      code: 'IDEMPOTENT_START_UNRESOLVABLE',
      runId: reservation.runId,
    };
  }
}

/**
 * The run finished and its summary has aged out of retention. 409 and never a
 * re-execution: a key whose run completed is spent, and the fact that nobody
 * can still read the OUTCOME does not make the WORK un-done.
 */
export class IdempotentStartAlreadySettledError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'IDEMPOTENT_START_ALREADY_SETTLED';
    readonly runId: string;
  };

  constructor(reservation: StartReservation) {
    super(
      `run '${reservation.runId}' for this idempotency key already completed and its summary has expired`,
    );
    this.name = 'IdempotentStartAlreadySettledError';
    this.reason = {
      code: 'IDEMPOTENT_START_ALREADY_SETTLED',
      runId: reservation.runId,
    };
  }
}

/** A malformed idempotency key or reservation request — the caller's to fix. */
export class InvalidStartIdempotencyRequestError extends DoStatusError {
  readonly status = 400;

  constructor(message: string) {
    super(message);
    this.name = 'InvalidStartIdempotencyRequestError';
  }
}

/**
 * A start carried a key onto a deployment whose host never wired the
 * reservation store.
 *
 * 503, and never a silent pass-through. Honouring the request without a
 * reservation would answer an exactly-once REQUEST with at-least-once
 * BEHAVIOUR, which is worse than refusing: the caller would have every reason
 * to believe a retry is safe, and no way to find out it is not. The condition
 * is a wiring fault an operator fixes, so it reads as transient rather than as
 * the caller's mistake.
 */
export class StartIdempotencyUnsupportedError extends DoStatusError {
  readonly status = 503;
  readonly reason: { readonly code: 'IDEMPOTENT_START_UNSUPPORTED' };

  constructor() {
    super(
      'idempotent starts are not configured on this deployment — wire a StartIdempotencyStore before accepting idempotencyKey',
    );
    this.name = 'StartIdempotencyUnsupportedError';
    this.reason = { code: 'IDEMPOTENT_START_UNSUPPORTED' };
  }
}

/** Every reservation-authored refusal — the family a surface catches as one. */
export type StartReservationRefusal =
  | StartReservationOwnerMismatchError
  | StartReservationTargetMismatchError
  | IdempotentStartPendingError
  | IdempotentStartUnresolvableError
  | IdempotentStartAlreadySettledError
  | StartIdempotencyUnsupportedError
  | InvalidStartIdempotencyRequestError;

export function isStartReservationRefusal(
  error: unknown,
): error is StartReservationRefusal {
  return (
    error instanceof StartReservationOwnerMismatchError ||
    error instanceof StartReservationTargetMismatchError ||
    error instanceof IdempotentStartPendingError ||
    error instanceof IdempotentStartUnresolvableError ||
    error instanceof IdempotentStartAlreadySettledError ||
    error instanceof StartIdempotencyUnsupportedError ||
    error instanceof InvalidStartIdempotencyRequestError
  );
}

/**
 * Minimal structural D1 surface — the same posture as SnapshotDatabase,
 * ApprovalDatabase, and ExecutionFenceDatabase, so every surface that must
 * reserve can hand over the binding it already holds with no cast and no second
 * seam. Tests back it with node:sqlite; Workers pass `env.DB`.
 */
export interface StartIdempotencyDatabase {
  prepare(query: string): StartIdempotencyStatement;
}

export interface StartIdempotencyStatement {
  bind(...values: unknown[]): StartIdempotencyStatement;
  run(): Promise<unknown>;
  all<T = unknown>(): Promise<{ results: T[] }>;
}

/** How a surface is wired to the reservation store, or the typed opt-out. */
export type StartIdempotencyWiring = StartIdempotencyStore | 'none';

/**
 * Rows affected by a write, read from D1's `{ meta: { changes } }` envelope.
 * Restated here for the same reason execution-fence.ts restates it: this module
 * imports only leaves, and every surface that reserves must be able to import
 * it without dragging the D1 storage adapter into its bundle.
 */
function changesOf(result: unknown): number {
  const changes = (result as { meta?: { changes?: number } } | undefined)?.meta
    ?.changes;
  return typeof changes === 'number' ? changes : 0;
}

/**
 * One reservation store per DATABASE, keyed on the BINDING — the same memo, for
 * the same reason, as `executionFenceFor`: the router that reserves, the
 * topology that claims, the runtime that settles, and the purge that reaps must
 * all be looking at one table, and keying on an env object would hand two
 * databases the same store the first time a host mutated env across requests.
 */
const startIdempotencyStores = new WeakMap<object, StartIdempotencyStore>();

export function startIdempotencyFor(
  db: StartIdempotencyDatabase,
): StartIdempotencyStore {
  const existing = startIdempotencyStores.get(db);
  if (existing) return existing;
  const store = new StartIdempotencyStore(db);
  startIdempotencyStores.set(db, store);
  return store;
}

const STATE_CHECK = START_RESERVATION_STATES.map((state) => `'${state}'`).join(
  ', ',
);
const TARGET_CHECK = START_TARGET_KINDS.map((kind) => `'${kind}'`).join(', ');

/**
 * The reservation schema.
 *
 * The CHECK constraints are load-bearing, not decoration: every compare-and-set
 * below is stated as `WHERE ... AND state = '<literal>'`, which is only a TOTAL
 * decision while the column cannot hold a fourth value. A row hand-edited into
 * an unknown state would otherwise be a reservation no CAS can advance and no
 * purge can reap — a permanently wedged key.
 */
export const START_IDEMPOTENCY_DDL = `CREATE TABLE IF NOT EXISTS ${START_IDEMPOTENCY_TABLE} (
    key TEXT PRIMARY KEY,
    owner_kind TEXT NOT NULL CHECK (owner_kind IN ('human', 'service', 'agent', 'system')),
    owner_id TEXT NOT NULL,
    target_kind TEXT NOT NULL CHECK (target_kind IN (${TARGET_CHECK})),
    target_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    thread_id TEXT,
    state TEXT NOT NULL CHECK (state IN (${STATE_CHECK})),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
  )`;

/**
 * `run_id` is how the RUNTIME finds a reservation (terminal reconcile knows the
 * run, never the key) and how the purge pairs a reservation with the snapshot
 * it outlived. Without the index both degrade to a table scan on every terminal
 * run.
 */
export const START_IDEMPOTENCY_RUN_INDEX_DDL = `CREATE INDEX IF NOT EXISTS ${START_IDEMPOTENCY_TABLE}_run
    ON ${START_IDEMPOTENCY_TABLE} (run_id)`;

/** The purge's own access path: terminal rows past the key-validity horizon. */
export const START_IDEMPOTENCY_STATE_INDEX_DDL = `CREATE INDEX IF NOT EXISTS ${START_IDEMPOTENCY_TABLE}_state
    ON ${START_IDEMPOTENCY_TABLE} (state, updated_at)`;

interface StartReservationRow {
  key?: unknown;
  owner_kind?: unknown;
  owner_id?: unknown;
  target_kind?: unknown;
  target_id?: unknown;
  run_id?: unknown;
  thread_id?: unknown;
  state?: unknown;
  created_at?: unknown;
  updated_at?: unknown;
}

function isStartReservationState(
  value: unknown,
): value is StartReservationState {
  return (
    typeof value === 'string' &&
    (START_RESERVATION_STATES as readonly string[]).includes(value)
  );
}

function isStartTargetKind(value: unknown): value is StartTargetKind {
  return (
    typeof value === 'string' &&
    (START_TARGET_KINDS as readonly string[]).includes(value)
  );
}

function epochMs(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

/**
 * Project a stored row, or refuse it.
 *
 * A malformed row throws rather than reading as absent, and that direction is
 * deliberate: "there is no reservation" is the answer that STARTS A RUN, so it
 * must never be reachable from a row this build cannot parse. The CHECK
 * constraints make this unreachable on a database this package created; it
 * exists for the one that was hand-edited.
 */
function reservationFromRow(row: StartReservationRow): StartReservation {
  const {
    key,
    owner_kind: ownerKind,
    owner_id: ownerId,
    target_kind: targetKind,
    target_id: targetId,
    run_id: runId,
    thread_id: threadId,
    state,
  } = row;
  if (
    typeof key !== 'string' ||
    !isExecutionPrincipalKind(ownerKind) ||
    !isExecutionPrincipalId(ownerId) ||
    !isStartTargetKind(targetKind) ||
    typeof targetId !== 'string' ||
    !isPathSafeId(runId) ||
    !isStartReservationState(state)
  ) {
    throw new StartReservationUnreadableError(
      typeof key === 'string' ? key : '(unknown)',
    );
  }
  return {
    key,
    owner: { kind: ownerKind, id: ownerId },
    targetKind,
    targetId,
    runId,
    ...(isPathSafeId(threadId) ? { threadId } : {}),
    state,
    createdAt: epochMs(row.created_at),
    updatedAt: epochMs(row.updated_at),
  };
}

/**
 * A reservation exists but cannot be understood, or the table could not be
 * read. 503 and never "no reservation": the absent answer is the one that
 * starts a run, so an unreadable store degrades CLOSED exactly as an unreadable
 * fence does.
 */
export class StartReservationUnreadableError extends DoStatusError {
  readonly status = 503;
  readonly reason: { readonly code: 'IDEMPOTENT_START_UNREADABLE' };

  constructor(key: string, options?: ErrorOptions) {
    super(`start reservation '${key}' is not readable`, options);
    this.name = 'StartReservationUnreadableError';
    this.reason = { code: 'IDEMPOTENT_START_UNREADABLE' };
  }
}

/**
 * How far down an error's `cause` chain the missing-table test looks. Bounded
 * because a chain can be cyclic or adversarially deep — same rule, same bound,
 * as the fence's.
 */
const MAX_RESERVATION_ERROR_CAUSE_DEPTH = 8;

/**
 * SQLite/D1's "no such table", matched at the ROOT of the cause chain only.
 *
 * The same discrimination the fence makes, for the same reason: a missing table
 * mentioned part-way down a chain describes a fault that merely PASSED this
 * table on its way out (a failed migration, an adapter reporting the last thing
 * it saw), and concluding "no reservations exist" from that would start a run.
 * If the innermost fault IS the missing table, the table genuinely does not
 * exist yet and there is nothing to find.
 */
function isMissingReservationTable(error: unknown): boolean {
  const seen = new Set<unknown>();
  let current: unknown = error;
  for (let depth = 0; depth < MAX_RESERVATION_ERROR_CAUSE_DEPTH; depth += 1) {
    seen.add(current);
    const cause = current instanceof Error ? current.cause : undefined;
    if (cause !== undefined && cause !== null && !seen.has(cause)) {
      current = cause;
      continue;
    }
    const message =
      current instanceof Error ? current.message : String(current);
    return (
      /no such table/i.test(message) &&
      message.includes(START_IDEMPOTENCY_TABLE)
    );
  }
  return false;
}

function assertKey(key: unknown): string {
  if (!isPathSafeId(key)) {
    throw new InvalidStartIdempotencyRequestError(
      "idempotencyKey must be a URL-path-safe identifier (letters, digits, '.', '_', '~', '-'; 1-200 chars)",
    );
  }
  return key;
}

function assertOwner(owner: unknown): StartReservationOwner {
  if (owner === null || typeof owner !== 'object') {
    throw new InvalidStartIdempotencyRequestError(
      'reservation owner must be an execution principal',
    );
  }
  const { kind, id } = owner as { kind?: unknown; id?: unknown };
  if (!isExecutionPrincipalKind(kind) || !isExecutionPrincipalId(id)) {
    throw new InvalidStartIdempotencyRequestError(
      'reservation owner must be an execution principal',
    );
  }
  return { kind, id };
}

export interface StartIdempotencyStoreOptions {
  /** Injectable clock for `created_at`/`updated_at` (tests, fixtures). */
  now?: () => number;
  /** Schema readiness override, for a host that owns its own migrations. */
  ready?: () => Promise<void>;
}

/**
 * The deployment's start reservations, over the SAME D1 database its snapshots,
 * its resource owners, and its execution fence live in — so a reservation
 * cannot be separated from the run it reserves by any binding mistake.
 */
export class StartIdempotencyStore {
  readonly #db: StartIdempotencyDatabase;
  readonly #now: () => number;
  readonly #ready: () => Promise<void>;

  constructor(
    db: StartIdempotencyDatabase,
    options: StartIdempotencyStoreOptions = {},
  ) {
    this.#db = db;
    this.#now = options.now ?? Date.now;
    if (options.ready) {
      this.#ready = options.ready;
    } else {
      let ready: Promise<void> | undefined;
      this.#ready = () => {
        ready ??= this.#createSchema().catch((error: unknown) => {
          ready = undefined;
          throw error;
        });
        return ready;
      };
    }
  }

  /**
   * Decide which run this key means, creating the reservation if it is new.
   *
   * INSERT OR IGNORE then READ BACK — the resource-ownership idiom, and the
   * only shape that is correct without a transaction: the insert is the race,
   * the read-back is the result, and every caller that lost the insert reads
   * the winner's row rather than its own intention. Two callers can therefore
   * never both believe they created the reservation, whatever order their
   * statements interleave in, and neither needs to know it raced.
   *
   * This is the ONE method that may create the table. A reservation is the
   * first write anything here makes, so the schema belongs on its path and
   * nowhere else — in particular not on `read`, where lazy DDL would turn the
   * deployment's read-only drain inventory into a write.
   */
  async reserve(
    request: StartReservationRequest,
  ): Promise<StartReservationOutcome> {
    const key = assertKey(request.key);
    const owner = assertOwner(request.owner);
    if (!isStartTargetKind(request.targetKind)) {
      throw new InvalidStartIdempotencyRequestError(
        `target kind must be one of ${START_TARGET_KINDS.join(', ')}`,
      );
    }
    if (!isPathSafeId(request.targetId)) {
      throw new InvalidStartIdempotencyRequestError(
        'target id must be a URL-path-safe identifier',
      );
    }
    // The thread is the agent run's ADDRESS, so requiring it for agents and
    // rejecting it for workflows is not tidiness: an agent reservation without
    // one is a run a retry can never reach, and a workflow reservation WITH one
    // is a second, silently divergent copy of an address that is already
    // derivable from (workflowId, runId).
    if (request.targetKind === 'agent') {
      if (!isPathSafeId(request.threadId)) {
        throw new InvalidStartIdempotencyRequestError(
          'an agent start reservation requires a URL-path-safe threadId',
        );
      }
    } else if (request.threadId !== undefined) {
      throw new InvalidStartIdempotencyRequestError(
        'threadId applies only to agent start reservations',
      );
    }
    await this.#ready();
    const candidateRunId = request.mintRunId();
    if (!isPathSafeId(candidateRunId)) {
      throw new InvalidStartIdempotencyRequestError(
        'the host minted a runId that is not URL-path-safe',
      );
    }
    const now = this.#now();
    const inserted = changesOf(
      await this.#db
        .prepare(
          `INSERT OR IGNORE INTO ${START_IDEMPOTENCY_TABLE}
             (key, owner_kind, owner_id, target_kind, target_id, run_id,
              thread_id, state, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?)`,
        )
        .bind(
          key,
          owner.kind,
          owner.id,
          request.targetKind,
          request.targetId,
          candidateRunId,
          request.threadId ?? null,
          now,
          now,
        )
        .run(),
    );
    const stored = await this.read(key);
    if (stored === undefined) {
      // The row was written and is already gone: the purge cannot reach a row
      // this young, so this is a storage fault, not a reservation state.
      throw new StartReservationUnreadableError(key);
    }
    // Owner FIRST, always. A caller that is not the owner must learn nothing
    // about the target, and checking the target first would leak it through the
    // choice of refusal.
    if (stored.owner.kind !== owner.kind || stored.owner.id !== owner.id) {
      throw new StartReservationOwnerMismatchError(key);
    }
    if (
      stored.targetKind !== request.targetKind ||
      stored.targetId !== request.targetId
    ) {
      throw new StartReservationTargetMismatchError(key, stored);
    }
    // Both signals must agree before this caller believes it created the row.
    // `changes` alone would trust the adapter's bookkeeping; the id comparison
    // alone would trust that two mints never collide. Requiring both makes the
    // false answer the SAFE one — a creator misread as a replayer takes the
    // replay path and converges, while the reverse would start a second run.
    return {
      reservation: stored,
      created: inserted > 0 && stored.runId === candidateRunId,
    };
  }

  /**
   * Take the claim: `reserved` -> `started`, for this exact run.
   *
   * ONE conditional UPDATE, and the whole cross-isolate serialization of the
   * feature. Exactly one caller changes a row; every other caller sees zero
   * changes and must go and find out what the winner did rather than starting
   * anything. The `run_id` predicate rides along so a claim can never land on a
   * row that was rewritten underneath it.
   *
   * Never creates the table: a claim can only ever follow a reserve, which did.
   */
  async claim(key: string, runId: string): Promise<boolean> {
    return this.#casState(key, runId, 'reserved', 'started');
  }

  /**
   * Give the claim back: `started` -> `reserved`, for this exact run.
   *
   * The ONLY backwards transition, and it exists for exactly one caller: a
   * start the EXECUTION FENCE refused. That refusal is special because it is
   * provably pre-execution — the fence is read before the run lock and before
   * any storage write — so the claim it consumed bought nothing and holding on
   * to it would manufacture an UNRESOLVABLE reservation out of an operator
   * action. Leaving the claim taken would mean a deployment that drained,
   * migrated, and reopened had permanently poisoned every key that happened to
   * be in flight.
   *
   * It is deliberately NOT used for other start failures. Anything that reached
   * the runtime's execution path may have taken effect, and a rollback there
   * would hand the next retry a fresh run — the exact double-charge this whole
   * module exists to prevent.
   */
  async release(key: string, runId: string): Promise<boolean> {
    return this.#casState(key, runId, 'started', 'reserved');
  }

  /**
   * Terminal reconcile, keyed by RUN rather than by key: the runtime observes a
   * run reaching a terminal state and has no idea which key (if any) named it.
   *
   * Idempotent by construction (`state <> 'terminal'`), so the several places a
   * run can reach terminal — completing, failing, being cancelled, timing out —
   * can all call it without coordinating, and a re-entry after a crash is a
   * no-op rather than a conflict.
   *
   * Returns the number of reservations settled, which is 0 for the overwhelming
   * majority of runs (nobody used a key) and 1 for the rest.
   */
  async settleRun(runId: string): Promise<number> {
    if (!isPathSafeId(runId)) return 0;
    try {
      return changesOf(
        await this.#db
          .prepare(
            `UPDATE ${START_IDEMPOTENCY_TABLE}
               SET state = 'terminal', updated_at = ?
             WHERE run_id = ? AND state <> 'terminal'`,
          )
          .bind(this.#now(), runId)
          .run(),
      );
    } catch (error) {
      // A database with no reservation table has no reservation to settle. Any
      // other fault is real and must not be mistaken for "nothing to do".
      if (isMissingReservationTable(error)) return 0;
      throw new StartReservationUnreadableError(runId, { cause: error });
    }
  }

  /**
   * Read one reservation. A PURE read: no lazy DDL, no upsert, nothing that
   * would make consulting a key mutate the database — which is what lets a
   * read-only drain inventory and every replay path use it freely, and what
   * keeps a read-only replica or a revoked-write incident a degrade rather than
   * an outage.
   *
   * An absent TABLE reads as an absent reservation, because on a deployment
   * where no key has ever been used those are the same fact.
   */
  async read(key: string): Promise<StartReservation | undefined> {
    const safeKey = assertKey(key);
    let rows: StartReservationRow[];
    try {
      rows = (
        await this.#db
          .prepare(
            `SELECT key, owner_kind, owner_id, target_kind, target_id, run_id,
                    thread_id, state, created_at, updated_at
             FROM ${START_IDEMPOTENCY_TABLE} WHERE key = ?`,
          )
          .bind(safeKey)
          .all<StartReservationRow>()
      ).results;
    } catch (error) {
      if (isMissingReservationTable(error)) return undefined;
      throw new StartReservationUnreadableError(safeKey, { cause: error });
    }
    const row = rows[0];
    return row === undefined ? undefined : reservationFromRow(row);
  }

  /** Create the table and its two access paths. Only `reserve()` reaches this. */
  async #createSchema(): Promise<void> {
    await this.#db.prepare(START_IDEMPOTENCY_DDL).run();
    await this.#db.prepare(START_IDEMPOTENCY_RUN_INDEX_DDL).run();
    await this.#db.prepare(START_IDEMPOTENCY_STATE_INDEX_DDL).run();
  }

  /**
   * Every reservation naming one of these runs, for the purge and the
   * inventory. Chunked by the caller; this method binds exactly what it is
   * given, so a caller must respect D1's 100-parameter statement limit.
   */
  async reservationsForRuns(
    runIds: readonly string[],
  ): Promise<StartReservation[]> {
    const safeRunIds = runIds.filter((runId) => isPathSafeId(runId));
    if (safeRunIds.length === 0) return [];
    const placeholders = safeRunIds.map(() => '?').join(', ');
    let rows: StartReservationRow[];
    try {
      rows = (
        await this.#db
          .prepare(
            `SELECT key, owner_kind, owner_id, target_kind, target_id, run_id,
                    thread_id, state, created_at, updated_at
             FROM ${START_IDEMPOTENCY_TABLE} WHERE run_id IN (${placeholders})`,
          )
          .bind(...safeRunIds)
          .all<StartReservationRow>()
      ).results;
    } catch (error) {
      if (isMissingReservationTable(error)) return [];
      throw new StartReservationUnreadableError('(run lookup)', {
        cause: error,
      });
    }
    return rows.map((row) => reservationFromRow(row));
  }

  async #casState(
    key: string,
    runId: string,
    from: StartReservationState,
    to: StartReservationState,
  ): Promise<boolean> {
    const safeKey = assertKey(key);
    if (!isPathSafeId(runId)) {
      throw new InvalidStartIdempotencyRequestError(
        'reservation runId must be a URL-path-safe identifier',
      );
    }
    try {
      return (
        changesOf(
          await this.#db
            .prepare(
              `UPDATE ${START_IDEMPOTENCY_TABLE}
                 SET state = ?, updated_at = ?
               WHERE key = ? AND run_id = ? AND state = ?`,
            )
            .bind(to, this.#now(), safeKey, runId, from)
            .run(),
        ) > 0
      );
    } catch (error) {
      // No table means no reservation, so no transition happened — which is
      // exactly what `false` says, and the caller's replay path handles it.
      if (isMissingReservationTable(error)) return false;
      throw new StartReservationUnreadableError(safeKey, { cause: error });
    }
  }
}

// ---------------------------------------------------------------------------
// The replay resolver — the state machine's OBSERVABLE half.
//
// One function rather than one per surface, because the workflow router and the
// agent topology must answer identically: a caller that retried a workflow
// start and a caller that retried an agent start are asking the same question
// about the same table, and two implementations of "what happened to the first
// one?" is two chances for one of them to answer "nothing did" when something
// had. What differs between surfaces is only WHERE a run's persisted state and
// its liveness are read from, which is exactly what the injected surface says.
// ---------------------------------------------------------------------------

/** The two surface-specific reads the replay decision needs. */
export interface IdempotentStartSurface<TPersisted> {
  /**
   * The reserved run's persisted state, or undefined when nothing has been
   * persisted yet. This is the FIRST question asked on every replay, because a
   * persisted run makes every other branch moot: the work happened, its outcome
   * is readable, and the honest answer to the retry is that outcome.
   */
  persisted(reservation: StartReservation): Promise<TPersisted | undefined>;
  /**
   * Whether the reserved run is executing RIGHT NOW — asked only when nothing
   * is persisted, and only to separate "still working" from "died holding the
   * claim". Never a timer (see the module header).
   *
   * KNOWN WINDOW — the claim-to-dispatch gap. The winning CAS lands on the
   * Worker, and the object that would report the run live only learns about it
   * one dispatch later. A concurrent retry probing inside that gap is told
   * UNRESOLVABLE for a run that is about to execute perfectly well.
   *
   * That is a FALSE ALARM, never a lost or duplicated run: the claim still
   * stands, the winner still executes, and the caller's next retry replays the
   * persisted summary. Closing it would take moving the claim into the target
   * object itself, so that `started` becomes observable only from inside the
   * body that is already executing. It is deliberately NOT closed with a grace
   * period: a bound short enough to cover a dispatch is indistinguishable from
   * the timer this design rejected, and once a timer exists somebody will grow
   * it to cover a slow run and re-open the double-charge it was rejected for.
   */
  live(reservation: StartReservation): Promise<boolean>;
}

/** What a surface must do next, once the reservation has been resolved. */
export type IdempotentStartDecision<TPersisted> =
  | {
      /** Nobody has started this key's run: proceed, using THIS runId. */
      kind: 'start';
      reservation: StartReservation;
    }
  | {
      /** The run already exists: answer with its persisted state, unchanged. */
      kind: 'replay';
      reservation: StartReservation;
      persisted: TPersisted;
    };

/**
 * Resolve an idempotency key into "start this run" or "replay that one".
 *
 * The order of the checks below IS the semantics:
 *
 *  1. Reserve. A brand-new key that also wins the claim is the only path that
 *     starts anything.
 *  2. Persisted state, BEFORE the reservation's own state. A run that persisted
 *     is answerable whatever the reservation says, and reading the row's state
 *     first would let a stale `started` refuse a retry whose run is sitting
 *     right there, finished.
 *  3. `reserved` with nothing persisted means the first caller died between the
 *     insert and the claim, having executed nothing — so this caller may take
 *     the claim and proceed with the SAME runId. That convergence is what makes
 *     a crashed reservation self-healing instead of a wedged key.
 *  4. `started` with nothing persisted is the only ambiguous state in the
 *     system, and the liveness probe is what resolves it.
 *  5. `terminal` with nothing persisted is a completed run whose summary aged
 *     out. Spent, never re-run.
 *
 * Throws the taxonomy's refusals; a surface renders them through
 * `doErrorResponse` (or its router's equivalent) with no re-mapping, because
 * the code is the contract and a collapsed status hides which of five very
 * different things happened.
 */
export async function beginIdempotentStart<TPersisted>(
  store: StartIdempotencyStore,
  request: StartReservationRequest,
  surface: IdempotentStartSurface<TPersisted>,
  fence?: ExecutionFenceWiring,
): Promise<IdempotentStartDecision<TPersisted>> {
  const { reservation, created } = await store.reserve(request);
  if (created && (await store.claim(reservation.key, reservation.runId))) {
    return { kind: 'start', reservation };
  }
  const decision = await resolveExistingReservation(
    store,
    reservation,
    surface,
  );
  if (decision.kind === 'replay') {
    await rebindProofRun(fence, decision.reservation);
  }
  return decision;
}

/**
 * Re-assert a proof-only fence's binding to the run this key already made.
 *
 * The binding is written by `RunnerRuntime.start`, which a REPLAY never
 * reaches — so without this, a proof run whose fence lost its `proof_run_id`
 * (the fence was moved away and back onto the same key while its run survived)
 * would be a run the deployment can read but can no longer RESUME: proof-only
 * admits existing work only for `proof_run_id`, and there would be none.
 *
 * Every guard lives in `recordProofRun`'s own CAS, which is why this can be
 * unconditional and best-effort: it changes nothing unless the fence is in
 * proof-only under EXACTLY this key with the slot empty or already holding this
 * run. A different key, a different state, a different proof run — all are zero
 * rows and no harm. A failure is swallowed rather than raised, because the
 * caller is being handed the persisted state of a run that already happened,
 * and refusing that read would answer a successful retry with an error while
 * changing nothing about the run.
 */
async function rebindProofRun(
  fence: ExecutionFenceWiring | undefined,
  reservation: StartReservation,
): Promise<void> {
  if (fence === undefined || fence === 'none') return;
  try {
    await fence.recordProofRun(reservation.key, reservation.runId);
  } catch (error) {
    console.error(
      JSON.stringify({
        type: 'start-reservation-proof-rebind-failed',
        key: reservation.key,
        runId: reservation.runId,
        error: error instanceof Error ? error.message : String(error),
      }),
    );
  }
}

async function resolveExistingReservation<TPersisted>(
  store: StartIdempotencyStore,
  observed: StartReservation,
  surface: IdempotentStartSurface<TPersisted>,
): Promise<IdempotentStartDecision<TPersisted>> {
  const persisted = await surface.persisted(observed);
  if (persisted !== undefined) {
    return { kind: 'replay', reservation: observed, persisted };
  }
  if (observed.state === 'reserved') {
    if (await store.claim(observed.key, observed.runId)) {
      return { kind: 'start', reservation: observed };
    }
    // The claim was taken between the read and here. Re-read rather than
    // assuming: the winner may already have persisted, in which case the right
    // answer is its outcome and not a refusal.
    const current = await store.read(observed.key);
    if (current === undefined || current.runId !== observed.runId) {
      // The reservation vanished or was replaced under us. Refusing is the only
      // safe answer — a caller that retries gets a clean reserve, while
      // silently starting here would start a run under an id nothing reserved.
      throw new StartReservationUnreadableError(observed.key);
    }
    return resolveClaimedReservation(current, surface);
  }
  return resolveClaimedReservation(observed, surface);
}

async function resolveClaimedReservation<TPersisted>(
  reservation: StartReservation,
  surface: IdempotentStartSurface<TPersisted>,
): Promise<IdempotentStartDecision<TPersisted>> {
  if (reservation.state === 'terminal') {
    throw new IdempotentStartAlreadySettledError(reservation);
  }
  if (await surface.live(reservation)) {
    throw new IdempotentStartPendingError(reservation);
  }
  throw new IdempotentStartUnresolvableError(reservation);
}

/**
 * Give back a claim that the EXECUTION FENCE refused, then re-throw.
 *
 * Homed here, beside the state machine, rather than written out at each start
 * site: the rollback is only correct for this one error family, and a copy that
 * widened its catch — to "any start failure", say — would hand the next retry a
 * fresh run after a start that may well have executed. Keeping the predicate
 * and the CAS in one function is what stops that widening from being a one-line
 * edit somebody makes in a hurry.
 *
 * The rollback itself is best-effort: it runs while the deployment is already
 * refusing to execute, so its own failure must not replace the fence's refusal
 * with a storage error the caller cannot act on. A rollback that does not land
 * leaves an UNRESOLVABLE reservation — recoverable by investigation — while a
 * swallowed fence refusal would leave the caller believing the deployment is
 * broken rather than fenced.
 */
export async function rollbackFencedStart(
  store: StartIdempotencyStore,
  key: string,
  runId: string,
  error: unknown,
): Promise<never> {
  if (isExecutionFenceRefusal(error)) {
    try {
      await store.release(key, runId);
    } catch (rollbackError) {
      console.error(
        JSON.stringify({
          type: 'start-reservation-rollback-failed',
          key,
          runId,
          error:
            rollbackError instanceof Error
              ? rollbackError.message
              : String(rollbackError),
        }),
      );
    }
  }
  throw error;
}

/**
 * Resolve a surface's reservation wiring, refusing a key the host cannot
 * honour.
 *
 * `undefined` is admitted alongside `'none'` for the same reason
 * `readExecutionFence` admits it — a wiring that arrives through an object the
 * host may not have populated — and means the same thing: no store. What it
 * does NOT mean is "ignore the key", which is why this throws rather than
 * returning undefined. A start carrying an idempotency key onto an unwired
 * deployment is a request this host cannot answer truthfully.
 */
export function requireStartIdempotency(
  wiring: StartIdempotencyWiring | undefined,
): StartIdempotencyStore {
  if (wiring === undefined || wiring === 'none') {
    throw new StartIdempotencyUnsupportedError();
  }
  return wiring;
}
