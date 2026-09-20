// SPDX-License-Identifier: Apache-2.0
// A missing result cannot establish whether a claimed start had effects.

import {
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import { missingTableReadsEmpty } from './cause-chain.js';
import { DoStatusError } from './do-status-error.js';
import {
  InvalidExecutionIdentityError,
  normalizeMutationEpoch,
  normalizeStartExecutionIdentity,
  type ProofEntryExpectation,
  RunAdmissionConflictError,
  type StartExecutionIdentity,
} from './execution-admission.js';
import type { ExecutionFenceWiring } from './execution-fence.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  admissionReservationFromRow,
  captureReservation,
  decodeStartReservationAdmissionResult,
  isStartTargetKind,
  ReservationSchemaError,
  reservationFromRow,
  reservationResultRows,
  reservationSchemaStage,
  START_IDEMPOTENCY_ADDITIONS,
  START_IDEMPOTENCY_COLUMNS,
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_RUN_INDEX_DDL,
  START_IDEMPOTENCY_STATE_INDEX_DDL,
  START_IDEMPOTENCY_TABLE,
  START_TARGET_KINDS,
  type StartReservation,
  type StartReservationOwner,
  type StartReservationReading,
  type StartReservationSchemaStage,
  type StartReservationState,
  type StartTargetKind,
  sameReservationIdentity,
  validateStartReservationAdmissionSchema,
} from './start-reservation-contract.js';

export {
  decodeStartReservationAdmissionResult,
  START_IDEMPOTENCY_DDL,
  START_IDEMPOTENCY_RUN_INDEX_DDL,
  START_IDEMPOTENCY_STATE_INDEX_DDL,
  START_IDEMPOTENCY_TABLE,
  START_RESERVATION_STATES,
  START_TARGET_KINDS,
  type StartReservation,
  type StartReservationBinding,
  type StartReservationOwner,
  type StartReservationReading,
  type StartReservationState,
  type StartTargetKind,
  validateStartReservationAdmissionSchema,
} from './start-reservation-contract.js';

export interface StartReservationRequest {
  key: unknown;
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

function captureStartRequest(
  request: StartReservationRequest,
): StartReservationRequest & { key: string } {
  const key = assertKey(request.key);
  const owner = assertOwner(request.owner);
  const { targetKind, targetId, threadId, mintRunId } = request;
  if (!isStartTargetKind(targetKind)) {
    throw new InvalidStartIdempotencyRequestError(
      `target kind must be one of ${START_TARGET_KINDS.join(', ')}`,
    );
  }
  if (!isPathSafeId(targetId)) {
    throw new InvalidStartIdempotencyRequestError(
      'target id must be a URL-path-safe identifier',
    );
  }
  // The thread is the agent run's ADDRESS, so requiring it for agents and
  // rejecting it for workflows is not tidiness: an agent reservation without
  // one is a run a retry can never reach, and a workflow reservation WITH one
  // is a second, silently divergent copy of an address that is already
  // derivable from (workflowId, runId).
  if (targetKind === 'agent') {
    if (!isPathSafeId(threadId)) {
      throw new InvalidStartIdempotencyRequestError(
        'an agent start reservation requires a URL-path-safe threadId',
      );
    }
  } else if (threadId !== undefined) {
    throw new InvalidStartIdempotencyRequestError(
      'threadId applies only to agent start reservations',
    );
  }
  if (typeof mintRunId !== 'function') {
    throw new InvalidStartIdempotencyRequestError(
      'mintRunId must be a function',
    );
  }
  return Object.freeze({
    key,
    owner,
    targetKind,
    targetId,
    threadId,
    mintRunId: () => Reflect.apply(mintRunId, request, []),
  });
}

export interface StartReservationOutcome {
  /** The authoritative reservation — this caller's, or the winner's. */
  reservation: StartReservationReading;
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

export class IdempotentStartPendingError extends DoStatusError {
  readonly status = 503;
  readonly reason: {
    readonly code: 'IDEMPOTENT_START_PENDING';
    readonly runId: string;
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
  readonly reason: { readonly code: 'INVALID_START_IDEMPOTENCY_REQUEST' };

  constructor(message: string) {
    super(message);
    this.name = 'InvalidStartIdempotencyRequestError';
    this.reason = { code: 'INVALID_START_IDEMPOTENCY_REQUEST' };
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

/** Failures a start surface may reject while admitting or resolving a key. */
export type StartReservationRefusal =
  | StartReservationOwnerMismatchError
  | StartReservationTargetMismatchError
  | IdempotentStartPendingError
  | IdempotentStartUnresolvableError
  | IdempotentStartAlreadySettledError
  | StartIdempotencyUnsupportedError
  | InvalidStartIdempotencyRequestError;

/**
 * Recognize the five reservation-decision refusals:
 *
 * - `IDEMPOTENT_START_OWNER_MISMATCH` (403)
 * - `IDEMPOTENT_START_TARGET_MISMATCH` (409)
 * - `IDEMPOTENT_START_PENDING` (503)
 * - `IDEMPOTENT_START_UNRESOLVABLE` (409)
 * - `IDEMPOTENT_START_ALREADY_SETTLED` (409)
 *
 * The guard also recognizes `IDEMPOTENT_START_UNSUPPORTED` keyed admission
 * (503) and malformed reservation input (400,
 * `INVALID_START_IDEMPOTENCY_REQUEST`).
 *
 * Unsupported wiring remains in this guard because the keyed request is
 * intentionally refused at admission instead of being executed without its
 * exactly-once guarantee. `StartReservationUnreadableError` is deliberately
 * outside it: unreadable storage is an integrity or availability failure, not
 * a reservation decision, and propagates through the shared `DoStatusError`
 * renderer with `IDEMPOTENT_START_UNREADABLE` and its 503 status. A false
 * answer never permits an unkeyed start: a `StartReservationUnreadableError`
 * must still propagate as 503.
 */
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
 * Restated here for the same reason execution-fence.ts restates it: every
 * surface that reserves must be able to import this module without dragging
 * the D1 storage adapter (and @mastra/cloudflare-d1 with it) into its bundle.
 *
 * That bundle rule is why the imports at the top of this file are what they
 * are: lightweight identity/error leaves and the execution-fence store, whose
 * dependencies never reach the adapter. Nothing on that graph can cycle
 * back here, which matters more than usual: the eight DoStatusError subclasses
 * in this module are evaluated at module load, so an import edge that came
 * back around would meet a class expression still in its temporal dead zone.
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
 * SQLite/D1's "no such table", for THIS store's table: the reservation table is
 * created lazily by the first `reserve()`, so its absence means no key has ever
 * been used here and there is nothing to find.
 *
 * The rule itself — bounded, cycle-safe, and matched at the ROOT of the cause
 * chain only — lives in cause-chain.ts, shared with the fence store. Root-only
 * is the load-bearing half: a missing-table link mentioned part-way down a
 * chain describes a fault that merely PASSED this table on its way out (a
 * failed migration, an adapter reporting the last thing it saw), and concluding
 * "no reservations exist" from that would start a run.
 *
 * Kept local because every consumer is in this store; the drain inventory uses
 * `missingTableReadsEmpty` directly with `START_IDEMPOTENCY_TABLE`.
 */
function isMissingReservationTable(error: unknown): boolean {
  return missingTableReadsEmpty(error, START_IDEMPOTENCY_TABLE);
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

function assertReservationStartIdentity(
  reservation: StartReservationReading,
  execution: StartExecutionIdentity,
): void {
  if (
    reservation.owner.kind !== execution.owner.kind ||
    reservation.owner.id !== execution.owner.id
  )
    throw new StartReservationOwnerMismatchError(reservation.key);
  if (
    reservation.targetKind !== execution.target.kind ||
    reservation.targetId !== execution.target.id
  )
    throw new StartReservationTargetMismatchError(reservation.key, reservation);
  if (
    reservation.runId !== execution.runId ||
    reservation.threadId !==
      (execution.target.kind === 'agent'
        ? execution.target.threadId
        : undefined) ||
    (execution.target.kind === 'workflow' &&
      execution.workflowId !== execution.target.id)
  )
    throw new InvalidExecutionIdentityError('admission');
}

function sameReservationExecution(
  reservation: StartReservationReading,
  execution: StartExecutionIdentity,
): boolean {
  return (
    reservation.runId === execution.runId &&
    reservation.owner.kind === execution.owner.kind &&
    reservation.owner.id === execution.owner.id &&
    reservation.targetKind === execution.target.kind &&
    reservation.targetId === execution.target.id &&
    reservation.threadId ===
      (execution.target.kind === 'agent'
        ? execution.target.threadId
        : undefined) &&
    reservation.binding.kind === 'bound' &&
    reservation.binding.execution.runId === execution.runId &&
    reservation.binding.execution.startToken === execution.startToken &&
    reservation.binding.execution.tablePrefix === execution.tablePrefix &&
    reservation.binding.execution.workflowId === execution.workflowId
  );
}

function reservationMutationResult(
  result: unknown,
  observed: StartReservationReading,
  state: StartReservationState,
  updatedAt: number,
  execution?: StartExecutionIdentity,
): StartReservationReading | undefined {
  const row = decodeStartReservationAdmissionResult(result);
  if (
    row !== undefined &&
    (!sameReservationIdentity(row, observed) ||
      row.state !== state ||
      row.updatedAt !== updatedAt ||
      (execution === undefined
        ? row.binding.kind !== 'unbound'
        : !sameReservationExecution(row, execution)))
  )
    throw new Error('reservation mutation returned an unexpected row');
  return row;
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
    const ready = options.ready;
    if (ready) {
      this.#ready = async () => {
        await Reflect.apply(ready, this, []);
        if ((await this.#schemaStage()) !== 3)
          throw new ReservationSchemaError(
            'host readiness did not reach the current schema',
          );
      };
    } else {
      let storageReady: Promise<void> | undefined;
      this.#ready = () => {
        storageReady ??= this.#createSchema().catch((error: unknown) => {
          storageReady = undefined;
          throw error;
        });
        return storageReady;
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
    const { key, owner, targetKind, targetId, threadId, mintRunId } =
      captureStartRequest(request);
    try {
      await this.#ready();
    } catch (error) {
      throw new StartReservationUnreadableError(key, { cause: error });
    }
    const candidateRunId = Reflect.apply(mintRunId, request, []);
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
              thread_id, state, created_at, updated_at, start_token, start_table_prefix, start_workflow_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?, ?, '', NULL, NULL)`,
        )
        .bind(
          key,
          owner.kind,
          owner.id,
          targetKind,
          targetId,
          candidateRunId,
          threadId ?? null,
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
    if (stored.targetKind !== targetKind || stored.targetId !== targetId) {
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

  /** Claim only this observed modern-unbound reservation. */
  async claimReservation(
    observed: StartReservationReading,
  ): Promise<StartReservationReading | undefined> {
    const reservation = captureReservation(observed, 'reserved');
    const next = this.#nextReservationStamp(reservation);
    await this.#requireReservationSchema(reservation.key);
    let result: unknown;
    try {
      result = await this.#db
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET state = 'started', updated_at = ?
         WHERE key = ? AND run_id = ?
           AND owner_kind = ? AND owner_id = ?
           AND target_kind = ? AND target_id = ? AND thread_id IS ?
           AND created_at = ? AND updated_at = ? AND state = ?
           AND state = 'reserved'
           AND start_token = '' AND start_table_prefix IS NULL AND start_workflow_id IS NULL
         RETURNING *`,
        )
        .bind(
          next,
          reservation.key,
          reservation.runId,
          reservation.owner.kind,
          reservation.owner.id,
          reservation.targetKind,
          reservation.targetId,
          reservation.threadId ?? null,
          reservation.createdAt,
          reservation.updatedAt,
          reservation.state,
        )
        .all();
    } catch (error) {
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
    try {
      return reservationMutationResult(result, reservation, 'started', next);
    } catch (error) {
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
  }

  /** Release only the exact unbound claim, without readback recovery. */
  async releaseReservation(
    observed: StartReservationReading,
  ): Promise<boolean> {
    const reservation = captureReservation(observed, 'started');
    const next = this.#nextReservationStamp(reservation);
    await this.#requireReservationSchema(reservation.key);
    let result: unknown;
    try {
      result = await this.#db
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET state = 'reserved', updated_at = ?
         WHERE key = ? AND run_id = ?
           AND owner_kind = ? AND owner_id = ?
           AND target_kind = ? AND target_id = ? AND thread_id IS ?
           AND created_at = ? AND updated_at = ? AND state = ?
           AND state = 'started'
           AND start_token = '' AND start_table_prefix IS NULL AND start_workflow_id IS NULL
         RETURNING *`,
        )
        .bind(
          next,
          reservation.key,
          reservation.runId,
          reservation.owner.kind,
          reservation.owner.id,
          reservation.targetKind,
          reservation.targetId,
          reservation.threadId ?? null,
          reservation.createdAt,
          reservation.updatedAt,
          reservation.state,
        )
        .all();
    } catch (error) {
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
    try {
      return (
        reservationMutationResult(result, reservation, 'reserved', next) !==
        undefined
      );
    } catch (error) {
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
  }

  /** Associate an alias with an already-observed nonpending execution. */
  async associateReservation(
    observed: StartReservationReading,
    execution: StartExecutionIdentity,
  ): Promise<StartReservationReading> {
    const reservation = captureReservation(observed, 'nonterminal');
    const identity = normalizeStartExecutionIdentity(execution);
    assertReservationStartIdentity(reservation, identity);
    await this.#requireReservationSchema(reservation.key);
    let result: unknown;
    let failed = false;
    let writeCause: unknown;
    try {
      result = await this.#db
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE}
         SET start_token = ?, start_table_prefix = ?, start_workflow_id = ?
         WHERE key = ? AND run_id = ?
           AND owner_kind = ? AND owner_id = ?
           AND target_kind = ? AND target_id = ? AND thread_id IS ?
           AND created_at = ? AND updated_at = ? AND state = ?
           AND state <> 'terminal'
           AND start_token = '' AND start_table_prefix IS NULL AND start_workflow_id IS NULL
         RETURNING *`,
        )
        .bind(
          identity.startToken,
          identity.tablePrefix,
          identity.workflowId,
          reservation.key,
          reservation.runId,
          reservation.owner.kind,
          reservation.owner.id,
          reservation.targetKind,
          reservation.targetId,
          reservation.threadId ?? null,
          reservation.createdAt,
          reservation.updatedAt,
          reservation.state,
        )
        .all();
    } catch (error) {
      failed = true;
      writeCause = error;
    }
    if (!failed) {
      try {
        const row = reservationMutationResult(
          result,
          reservation,
          reservation.state,
          reservation.updatedAt,
          identity,
        );
        if (row) return row;
      } catch (error) {
        throw new StartReservationUnreadableError(reservation.key, {
          cause: error,
        });
      }
    }
    let current: StartReservationReading | undefined;
    try {
      current = await this.readForAdmission(reservation.key);
    } catch (error) {
      if (!failed) throw error;
    }
    if (
      current &&
      sameReservationIdentity(current, reservation) &&
      sameReservationExecution(current, identity)
    )
      return current;
    if (failed)
      throw new StartReservationUnreadableError(reservation.key, {
        cause: writeCause,
      });
    throw new RunAdmissionConflictError('reservation-changed');
  }

  /** Bind a prepared start; only exact lost-write readback can recover. */
  async bindPreparedStart(
    observed: StartReservationReading,
    execution: StartExecutionIdentity,
  ): Promise<StartReservationReading> {
    const reservation = captureReservation(observed, 'started');
    const identity = normalizeStartExecutionIdentity(execution);
    assertReservationStartIdentity(reservation, identity);
    await this.#requireReservationSchema(reservation.key);
    let result: unknown;
    try {
      result = await this.#db
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE}
         SET start_token = ?, start_table_prefix = ?, start_workflow_id = ?
         WHERE key = ? AND run_id = ?
           AND owner_kind = ? AND owner_id = ?
           AND target_kind = ? AND target_id = ? AND thread_id IS ?
           AND created_at = ? AND updated_at = ? AND state = ?
           AND state = 'started'
           AND start_token = '' AND start_table_prefix IS NULL AND start_workflow_id IS NULL
         RETURNING *`,
        )
        .bind(
          identity.startToken,
          identity.tablePrefix,
          identity.workflowId,
          reservation.key,
          reservation.runId,
          reservation.owner.kind,
          reservation.owner.id,
          reservation.targetKind,
          reservation.targetId,
          reservation.threadId ?? null,
          reservation.createdAt,
          reservation.updatedAt,
          reservation.state,
        )
        .all();
    } catch (error) {
      let current: StartReservationReading | undefined;
      try {
        current = await this.readForAdmission(reservation.key);
      } catch {
        // Preserve the write uncertainty even when its one readback also fails.
      }
      if (
        current &&
        sameReservationIdentity(current, reservation) &&
        current.state === reservation.state &&
        current.updatedAt === reservation.updatedAt &&
        sameReservationExecution(current, identity)
      )
        return current;
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
    let row: StartReservationReading | undefined;
    try {
      row = reservationMutationResult(
        result,
        reservation,
        reservation.state,
        reservation.updatedAt,
        identity,
      );
    } catch (error) {
      throw new StartReservationUnreadableError(reservation.key, {
        cause: error,
      });
    }
    if (row) return row;
    throw new RunAdmissionConflictError('reservation-changed');
  }

  /** Settle every alias of this complete physical/logical execution. */
  async settleExecution(execution: StartExecutionIdentity): Promise<number> {
    const identity = normalizeStartExecutionIdentity(execution);
    const clock = this.#reservationClock(identity.runId);
    let result: unknown;
    try {
      const stage = await this.#schemaStage();
      if (stage === undefined) return 0;
      if (stage !== 3)
        throw new ReservationSchemaError('settlement requires current schema');
      result = await this.#db
        .prepare(
          `UPDATE ${START_IDEMPOTENCY_TABLE} SET state = 'terminal', updated_at = ?
         WHERE run_id = ? AND start_token = ? AND start_table_prefix IS ?
           AND start_workflow_id = ? AND owner_kind = ? AND owner_id = ?
           AND target_kind = ? AND target_id = ? AND thread_id IS ?
           AND state <> 'terminal' RETURNING *`,
        )
        .bind(
          clock,
          identity.runId,
          identity.startToken,
          identity.tablePrefix,
          identity.workflowId,
          identity.owner.kind,
          identity.owner.id,
          identity.target.kind,
          identity.target.id,
          identity.target.kind === 'agent' ? identity.target.threadId : null,
        )
        .all();
    } catch (error) {
      if (isMissingReservationTable(error)) return 0;
      throw new StartReservationUnreadableError(identity.runId, {
        cause: error,
      });
    }
    try {
      const keys = new Set<string>();
      for (const raw of reservationResultRows(result)) {
        const row = admissionReservationFromRow(raw, 3);
        if (
          keys.has(row.key) ||
          row.state !== 'terminal' ||
          row.updatedAt !== clock ||
          !sameReservationExecution(row, identity)
        )
          throw new Error('settlement returned an unexpected reservation');
        keys.add(row.key);
      }
      return keys.size;
    } catch (error) {
      throw new StartReservationUnreadableError(identity.runId, {
        cause: error,
      });
    }
  }

  #reservationClock(key: string): number {
    try {
      const clock = this.#now();
      if (!Number.isFinite(clock))
        throw new Error('reservation clock is not finite');
      return clock;
    } catch (error) {
      throw new StartReservationUnreadableError(key, { cause: error });
    }
  }

  #nextReservationStamp(reservation: StartReservationReading): number {
    const clock = this.#reservationClock(reservation.key);
    const next = Math.max(clock, reservation.updatedAt + 1);
    if (!Number.isFinite(next) || next <= reservation.updatedAt)
      throw new StartReservationUnreadableError(reservation.key, {
        cause: new Error('reservation update stamp is exhausted'),
      });
    return next;
  }

  async #requireReservationSchema(key: string): Promise<void> {
    try {
      if ((await this.#schemaStage()) !== 3)
        throw new ReservationSchemaError('mutation requires current schema');
    } catch (error) {
      throw new StartReservationUnreadableError(key, { cause: error });
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
  async read(key: string): Promise<StartReservationReading | undefined> {
    const safeKey = assertKey(key);
    const rows = await this.#readReservations(
      `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE key = ? LIMIT 2`,
      [safeKey],
      safeKey,
    );
    if (rows.length > 1 || (rows[0] !== undefined && rows[0].key !== safeKey)) {
      throw new StartReservationUnreadableError(safeKey, {
        cause: new Error(
          'reservation query did not return the requested singleton key',
        ),
      });
    }
    const row = rows[0];
    return row;
  }

  usesDatabase(binding: object): boolean {
    return this.#db === binding;
  }

  /** @internal Pure current-stage read, including for an absent key. */
  async readForAdmission(
    key: string,
  ): Promise<StartReservationReading | undefined> {
    const safeKey = assertKey(key);
    try {
      const result = await this.#db
        .prepare(
          `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE key = ? LIMIT 2`,
        )
        .bind(safeKey)
        .all();
      const row = decodeStartReservationAdmissionResult(result);
      validateStartReservationAdmissionSchema(
        await this.#db
          .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
          .all(),
      );
      if (row !== undefined && row.key !== safeKey)
        throw new ReservationSchemaError('admission returned another key');
      return row;
    } catch (error) {
      throw new StartReservationUnreadableError(safeKey, { cause: error });
    }
  }

  async #schemaStage(): Promise<StartReservationSchemaStage | undefined> {
    return reservationSchemaStage(
      await this.#db
        .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
        .all(),
    );
  }

  async #readReservations(
    sql: string,
    bindings: readonly unknown[],
    key: string,
  ): Promise<StartReservationReading[]> {
    try {
      let result: unknown;
      try {
        result = await this.#db
          .prepare(sql)
          .bind(...bindings)
          .all();
      } catch (error) {
        if (isMissingReservationTable(error)) return [];
        throw error;
      }
      const rows = reservationResultRows(result);
      const stage = await this.#schemaStage();
      if (stage === undefined)
        throw new ReservationSchemaError('row observation has no schema');
      return rows.map((row) => reservationFromRow(row, stage));
    } catch (error) {
      throw new StartReservationUnreadableError(key, { cause: error });
    }
  }

  async #migrationStage(): Promise<StartReservationSchemaStage> {
    const stage = await this.#schemaStage();
    if (stage === undefined)
      throw new ReservationSchemaError(
        'table is missing during initialization',
      );
    if (stage === 0 || stage === 3) return stage;
    const predicate = START_IDEMPOTENCY_COLUMNS.slice(10, 10 + stage)
      .map(([name]) => `${name} IS NOT NULL`)
      .join(' OR ');
    const rows = reservationResultRows(
      await this.#db
        .prepare(
          `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE ${predicate} LIMIT 1`,
        )
        .all(),
    );
    const observed = await this.#schemaStage();
    if (observed === undefined || observed < stage)
      throw new ReservationSchemaError(
        'schema regressed during initialization',
      );
    if (rows.length > 1)
      throw new ReservationSchemaError(
        'partial binding query returned multiple rows',
      );
    for (const row of rows) reservationFromRow(row, observed);
    return observed;
  }

  /** Create the table and its two access paths. Only `reserve()` reaches this. */
  async #createSchema(): Promise<void> {
    if ((await this.#schemaStage()) === undefined) {
      await this.#db.prepare(START_IDEMPOTENCY_DDL).run();
    }
    for (
      let index: number = await this.#migrationStage();
      index < START_IDEMPOTENCY_ADDITIONS.length;
      index += 1
    ) {
      const stage = await this.#migrationStage();
      if (stage < index)
        throw new ReservationSchemaError(
          'schema regressed during initialization',
        );
      if (stage > index) continue;
      try {
        await this.#db
          .prepare(
            `ALTER TABLE ${START_IDEMPOTENCY_TABLE} ADD COLUMN ${START_IDEMPOTENCY_ADDITIONS[index]}`,
          )
          .run();
      } catch (error) {
        let observed: StartReservationSchemaStage | undefined;
        try {
          observed = await this.#schemaStage();
        } catch (readError) {
          if (readError instanceof ReservationSchemaError) throw readError;
          throw error;
        }
        if (observed === undefined || observed <= index) throw error;
      }
    }
    if ((await this.#schemaStage()) !== 3)
      throw new ReservationSchemaError(
        'initialization did not reach the current schema',
      );
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
  ): Promise<StartReservationReading[]> {
    const safeRunIds = runIds.filter((runId) => isPathSafeId(runId));
    if (safeRunIds.length === 0) return [];
    const placeholders = safeRunIds.map(() => '?').join(', ');
    const rows = await this.#readReservations(
      `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE run_id IN (${placeholders})`,
      safeRunIds,
      '(run lookup)',
    );
    const requested = new Set(safeRunIds);
    if (rows.some((row) => !requested.has(row.runId))) {
      throw new StartReservationUnreadableError('(run lookup)', {
        cause: new Error('reservation lookup returned an unrequested run'),
      });
    }
    return rows;
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

export type PersistedStartResult<T> =
  | { readonly kind: 'initial'; readonly execution: StartExecutionIdentity }
  | {
      readonly kind: 'result';
      readonly value: T;
      readonly execution: StartExecutionIdentity;
    };

export interface IdempotentStartSurface<T> {
  persisted(
    reservation: StartReservationReading,
  ): Promise<PersistedStartResult<T> | undefined>;
  live(reservation: StartReservationReading): Promise<boolean>;
}

export type IdempotentStartDecision<T> =
  | { kind: 'start'; reservation: StartReservationReading }
  | { kind: 'replay'; reservation: StartReservationReading; persisted: T };

export async function beginIdempotentStart<T>(
  store: StartIdempotencyStore,
  request: StartReservationRequest,
  surface: IdempotentStartSurface<T>,
  fence?: ExecutionFenceWiring,
  mutationEpoch?: number,
): Promise<IdempotentStartDecision<T>> {
  const capturedRequest = captureStartRequest(request);
  const epoch = normalizeMutationEpoch(mutationEpoch);
  const { persisted, live } = surface;
  const capturedSurface: IdempotentStartSurface<T> = {
    persisted: (row) => Reflect.apply(persisted, surface, [row]),
    live: (row) => Reflect.apply(live, surface, [row]),
  };
  let proof: ProofEntryExpectation | undefined;
  if (fence !== undefined && fence !== 'none') {
    try {
      const reading = await fence.read();
      if (
        reading.state === 'proof-only' &&
        reading.proofKey === capturedRequest.key
      )
        proof = Object.freeze({
          key: capturedRequest.key,
          mutationEpoch: reading.mutationEpoch,
          transitionRevision: reading.transitionRevision,
        });
    } catch {
      /* A failed observation cannot authorize nomination. */
    }
  }
  const { reservation, created } = await store.reserve(capturedRequest);
  let decision: IdempotentStartDecision<T>;
  // A creator can read back another caller's released reservation.
  if (
    created &&
    reservation.binding.kind === 'unbound' &&
    reservation.state === 'reserved' &&
    reservation.updatedAt === reservation.createdAt
  ) {
    const claimed = await store.claimReservation(reservation);
    if (claimed !== undefined) return { kind: 'start', reservation: claimed };
    decision = await resolveLostClaim(store, reservation, capturedSurface);
  } else
    decision = await resolveExistingReservation(
      store,
      reservation,
      capturedSurface,
      true,
    );
  if (
    decision.kind === 'replay' &&
    proof !== undefined &&
    fence !== undefined &&
    fence !== 'none'
  ) {
    const row = decision.reservation;
    if (
      row.binding.kind === 'bound' &&
      row.binding.execution.tablePrefix !== null
    ) {
      try {
        if (row.targetKind === 'agent' && row.threadId === undefined)
          throw new InvalidExecutionIdentityError('admission');
        await fence.rebindProofRun({
          reservation: row,
          execution: {
            ...row.binding.execution,
            tablePrefix: row.binding.execution.tablePrefix,
            owner: row.owner,
            target:
              row.targetKind === 'agent'
                ? {
                    kind: 'agent',
                    id: row.targetId,
                    threadId: row.threadId as string,
                  }
                : { kind: 'workflow', id: row.targetId },
          },
          proof,
          mutationEpoch: epoch,
          reservationStore: store,
        });
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'start-reservation-proof-rebind-failed',
            key: row.key,
            runId: row.runId,
            error: error instanceof Error ? error.message : String(error),
          }),
        );
      }
    }
  }
  return decision;
}

function decodePersistedStart<T>(
  value: PersistedStartResult<T>,
  row: StartReservationReading,
): PersistedStartResult<T> {
  try {
    if (
      value === null ||
      typeof value !== 'object' ||
      Array.isArray(value) ||
      !Object.hasOwn(value, 'kind') ||
      !Object.hasOwn(value, 'execution')
    )
      throw new Error('persisted start result is malformed');
    const { kind, execution: raw } = value;
    if (
      (kind !== 'initial' && kind !== 'result') ||
      (kind === 'initial'
        ? Object.hasOwn(value, 'value')
        : !Object.hasOwn(value, 'value'))
    )
      throw new Error('persisted start result is malformed');
    const { tablePrefix, workflowId, runId, startToken, owner, target } = raw;
    const execution = normalizeStartExecutionIdentity({
      tablePrefix,
      workflowId,
      runId,
      startToken,
      owner,
      target,
    });
    if (execution.tablePrefix !== tablePrefix)
      throw new Error('persisted start prefix is not canonical');
    assertReservationStartIdentity(row, execution);
    return kind === 'initial'
      ? { kind, execution }
      : { kind, execution, value: value.value };
  } catch (cause) {
    throw new StartReservationUnreadableError(row.key, { cause });
  }
}

async function resolveExistingReservation<T>(
  store: StartIdempotencyStore,
  observed: StartReservationReading,
  surface: IdempotentStartSurface<T>,
  mayClaim: boolean,
): Promise<IdempotentStartDecision<T>> {
  if (observed.binding.kind === 'legacy') {
    if (observed.state === 'terminal')
      throw new IdempotentStartAlreadySettledError(observed);
    throw new IdempotentStartUnresolvableError(observed);
  }
  const value = await surface.persisted(observed);
  if (value !== undefined) {
    const persisted = decodePersistedStart(value, observed);
    if (
      observed.binding.kind === 'bound' &&
      !sameReservationExecution(observed, persisted.execution)
    ) {
      if (observed.state === 'terminal')
        throw new IdempotentStartAlreadySettledError(observed);
      throw new IdempotentStartUnresolvableError(observed);
    }
    if (persisted.kind === 'result') {
      if (observed.binding.kind === 'bound')
        return {
          kind: 'replay',
          reservation: observed,
          persisted: persisted.value,
        };
      if (observed.state === 'terminal')
        throw new IdempotentStartAlreadySettledError(observed);
      const reservation = await store.associateReservation(
        observed,
        persisted.execution,
      );
      return { kind: 'replay', reservation, persisted: persisted.value };
    }
  } else if (
    mayClaim &&
    observed.state === 'reserved' &&
    observed.binding.kind === 'unbound'
  ) {
    if (await surface.live(observed))
      throw new IdempotentStartPendingError(observed);
    const claimed = await store.claimReservation(observed);
    if (claimed !== undefined) return { kind: 'start', reservation: claimed };
    return resolveLostClaim(store, observed, surface);
  }
  if (observed.state === 'terminal')
    throw new IdempotentStartAlreadySettledError(observed);
  if (await surface.live(observed))
    throw new IdempotentStartPendingError(observed);
  throw new IdempotentStartUnresolvableError(observed);
}

async function resolveLostClaim<T>(
  store: StartIdempotencyStore,
  observed: StartReservationReading,
  surface: IdempotentStartSurface<T>,
): Promise<IdempotentStartDecision<T>> {
  const current = await store.readForAdmission(observed.key);
  if (current === undefined || !sameReservationIdentity(current, observed))
    throw new StartReservationUnreadableError(observed.key);
  return resolveExistingReservation(store, current, surface, false);
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
