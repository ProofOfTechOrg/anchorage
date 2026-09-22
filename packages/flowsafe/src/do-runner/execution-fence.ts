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
// An absent legacy row (or absent TABLE) reads as `open`. That is the 0.19-to-0.20
// upgrade rule and nothing more: a database seeded before this table existed
// must keep serving. Provisioning writes an explicit row from 0.20 on, so a
// deployment that means to start locked says so rather than relying on a
// default — `seed()` therefore takes the state as a REQUIRED argument.

import {
  type DeploymentIdentityProtocolExecutor,
  type DeploymentIdentityProtocolRow,
  decodeExecutionFenceMutationMetadata,
  EXECUTION_FENCE_CURRENT_SCHEMA_STAGE,
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_STATES,
  EXECUTION_FENCE_TABLE,
  type ExecutionFenceSchemaStage,
  initializeExecutionFenceProtocol,
  readExecutionFenceSchemaProtocol,
} from '#deployment-identity-protocol';
import { missingTableReadsEmpty } from './cause-chain.js';
import { DoStatusError } from './do-status-error.js';
import {
  type D1RunExecutionIdentity,
  type D1StartExecutionIdentity,
  ExecutionFenceUnreadableError,
  InvalidExecutionIdentityError,
  normalizeD1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeStartExecutionIdentity,
  type ProofEntryExpectation,
  type RunExecutionIdentity,
} from './execution-admission.js';
import { RUN_PROVENANCE_CONTEXT_KEY } from './execution-context.js';
import { isPathSafeId } from './path-safe-id.js';
import {
  decodeProgressRunProvenance,
  decodeRunStartIdentity,
} from './run-provenance.js';
import { isRunStatus } from './run-terminal-state.js';
import {
  captureBoundReservation,
  decodeStartReservationAdmissionResult,
  START_IDEMPOTENCY_TABLE,
  type StartReservationReading,
  sameReservationIdentity,
  validateStartReservationAdmissionSchema,
} from './start-reservation-contract.js';
import {
  type D1RunAddress,
  decodeRawWorkflowSnapshotResult,
  prepareRawWorkflowSnapshotRead,
  type RawWorkflowSnapshot,
} from './workflow-snapshot-row.js';

export { ExecutionFenceUnreadableError } from './execution-admission.js';

/**
 * The state vocabulary, the table, that table's fixed row key, and the DDL
 * built from all three are IMPORTED, never declared here — and they are not
 * re-exported from this module either.
 *
 * Imported because PROVISIONING creates this table
 * (deployment-identity-protocol.mjs), and `CREATE TABLE IF NOT EXISTS` would
 * silently accept a differently-shaped table it had already made — so a second
 * copy of the schema would not fail loudly, it would quietly drop the CHECK
 * constraints that make every CAS below total rather than "the CAS, on
 * whichever row you meant". That module is also the only file both sides can
 * share: it ships at the package root for the provisioning CLI and
 * fleet-control, neither of which can import this package's TypeScript.
 *
 * Not re-exported because a raw constant with two homes is a constant two
 * consumers can disagree about. `@proofoftech/flowsafe/deployment-identity-protocol`
 * is the one place to import the table name or the state list from; what this
 * module publishes is the TYPED surface built on them — the state type, the
 * store, the refusals, and the admission predicates.
 */
export type ExecutionFenceState = (typeof EXECUTION_FENCE_STATES)[number];

/**
 * The suspend-payload key the executor backstop stamps on a task it parked
 * because the deployment was fenced mid-dispatch. Namespaced so it cannot
 * collide with a tool's own suspend payload, and read back by
 * `BackgroundTaskHost.#resumeFenceSuspendedTasks` — which is what makes the
 * parking reversible rather than a quieter kind of loss.
 *
 * PUBLISHED via `./background-tasks` because it is the only way a host can tell
 * a fence-parked row from a tool-suspended one: `listTasks({ status:
 * 'suspended' })` returns both, and the marker lives in the suspend payload
 * where no filter can express it. The drain inventory imports this same
 * definition directly, so a rename cannot silently leave a hard-coded census
 * predicate behind.
 */
export const EXECUTION_FENCE_SUSPEND_KEY = 'flowsafe.executionFenced';

/**
 * How a surface is wired to the fence: a store, or the typed opt-out.
 *
 * Written as a union with no `undefined` so every option type carrying it can
 * be REQUIRED. That is the whole forcing function: a fence option a host may
 * omit is one a host will omit, and a partially wired deployment is worse than
 * an unwired one — an unfenced schedule tick claims a due fire through the CAS
 * (which advances `nextFireAt`) and the fenced runtime then refuses the start,
 * so the fire is consumed and never runs. Making the caller WRITE `'none'`
 * turns that split brain into a decision someone made rather than one they
 * missed, and `'none'` stays honest for the callers that genuinely have no
 * database to fence against (in-memory tests, adapters).
 */
export type ExecutionFenceWiring = ExecutionFenceStore | 'none';

/** What one fence read observed. `proofKey`/`proofRunId` exist only in proof-only. */
export interface ExecutionFenceReading {
  readonly state: ExecutionFenceState;
  /** The key a proof-only start must carry to be admitted. */
  readonly proofKey?: string;
  /** The run the proof-only state has already admitted, once one started. */
  readonly proofRunId?: string;
  readonly mutationEpoch?: number;
  readonly requireMutationEpoch?: boolean;
  readonly transitionRevision?: number;
  /** Server-side proof identity; omitted from the admin JSON projection. */
  readonly proofExecution?: D1RunExecutionIdentity;
}

/** An authoritative store reading, including durable administrative versioning. */
export interface ExecutionFenceVersionedReading extends ExecutionFenceReading {
  readonly mutationEpoch: number;
  readonly requireMutationEpoch: boolean;
  readonly transitionRevision: number;
}

/** The reading every unfenced surface uses — see `ExecutionFenceStore` absence. */
export const OPEN_EXECUTION_FENCE: ExecutionFenceVersionedReading =
  Object.freeze({
    state: 'open',
    mutationEpoch: 0,
    requireMutationEpoch: false,
    transitionRevision: 0,
  });

/**
 * Read the fence a surface was wired with, resolving the typed opt-out.
 *
 * ONE function rather than a `fence ? await fence.read() : OPEN` at every gate,
 * because those are the places a mistake is invisible: an unfenced surface and
 * an open one behave identically until the day an operator closes the fence, so
 * a call site that got the ternary subtly wrong would pass every test written
 * against an open deployment. Every gate resolving absence through here means
 * there is exactly one definition of what "no fence" does.
 *
 * `undefined` is admitted alongside `'none'` for the surfaces whose fence
 * arrives through an object the caller may not have populated (an
 * `InitResult.executionFence` on an unfenced host); it reads as open for the
 * same reason `'none'` does, and never as a silent default a host can reach by
 * forgetting — the option types that feed this are required.
 */
export async function readExecutionFence(
  fence: ExecutionFenceWiring | undefined,
): Promise<ExecutionFenceVersionedReading> {
  if (fence === undefined || fence === 'none') return OPEN_EXECUTION_FENCE;
  return fence.read();
}

/**
 * One fence store per DATABASE — not per request, and not per call site.
 *
 * The store holds no state of its own (a read is never memoized — see the
 * module header), so this is about IDENTITY rather than caching: the admin
 * route that MOVES the fence, the approval service that OBEYS it, the schedule
 * tick that must not claim a fire behind it, and the runner's runtime all have
 * to be looking at the same database. Keyed on the BINDING rather than on an
 * env object, because a host mutates one env across requests and because the
 * fence belongs to the database, not to the request that happens to reach it.
 * WeakMap, so a test harness cycling bindings never leaks.
 *
 * Lives here rather than once per host because every composer needed the same
 * three lines, and four copies of a memo are four chances for one of them to
 * key on the wrong thing — an env-keyed copy hands two databases the same
 * fence. A host whose call sites pass `env` keeps a one-line wrapper of its
 * own; the memo itself is this one.
 */
const executionFenceStores = new WeakMap<object, ExecutionFenceStore>();

export function executionFenceFor(
  db: ExecutionFenceDatabase,
): ExecutionFenceStore {
  const existing = executionFenceStores.get(db);
  if (existing) return existing;
  const store = new ExecutionFenceStore(db);
  executionFenceStores.set(db, store);
  return store;
}

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
    readonly mutationEpoch?: number;
    readonly requireMutationEpoch?: boolean;
    readonly transitionRevision?: number;
    readonly proofKey?: string;
    readonly proofRunId?: string;
    readonly conflict?:
      | 'expectation-mismatch'
      | 'versioned-expectation-required';
  };

  constructor(
    expected: ExecutionFenceState,
    current: ExecutionFenceState,
    details?: {
      reading: ExecutionFenceVersionedReading;
      conflict: 'expectation-mismatch' | 'versioned-expectation-required';
    },
  ) {
    super(
      details === undefined
        ? `execution fence transition expected state '${expected}' but found '${current}'`
        : 'execution fence transition conflicts with the current reading',
    );
    this.name = 'FenceTransitionConflictError';
    this.reason =
      details === undefined
        ? { code: 'FENCE_CAS_CONFLICT', state: current }
        : {
            code: 'FENCE_CAS_CONFLICT',
            ...executionFenceReadingPayload(details.reading),
            conflict: details.conflict,
          };
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

/**
 * A fence reading as the JSON body a control-plane read answers with.
 *
 * Beside `executionFencedResponse` and for the same reason: the published
 * `GET /admin/execution-fence` route and the spike's local control probe both
 * project a reading into this exact shape, and a projection written twice is
 * one an operator's tooling can watch drift. Absent fields are OMITTED rather
 * than sent as null — `proofKey`/`proofRunId` exist only in proof-only, and a
 * null would invite a caller to read "no proof run yet" out of a state that has
 * no proof at all.
 */
export function executionFenceReadingPayload(
  reading: ExecutionFenceVersionedReading,
): ExecutionFenceVersionedReading;
export function executionFenceReadingPayload(
  reading: ExecutionFenceReading,
): ExecutionFenceReading;
export function executionFenceReadingPayload(
  reading: ExecutionFenceReading,
): ExecutionFenceReading {
  return {
    state: reading.state,
    ...(reading.proofKey === undefined ? {} : { proofKey: reading.proofKey }),
    ...(reading.proofRunId === undefined
      ? {}
      : { proofRunId: reading.proofRunId }),
    ...(reading.mutationEpoch === undefined
      ? {}
      : { mutationEpoch: reading.mutationEpoch }),
    ...(reading.requireMutationEpoch === undefined
      ? {}
      : { requireMutationEpoch: reading.requireMutationEpoch }),
    ...(reading.transitionRevision === undefined
      ? {}
      : { transitionRevision: reading.transitionRevision }),
  };
}

/**
 * A fence refusal that CROSSED a Durable Object boundary and was rebuilt on the
 * far side.
 *
 * The run object throws ExecutionFencedError, `doErrorResponse` renders it, and
 * `doSummary` reconstructs it as a `RunRouteError` carrying the same status,
 * message and structured reason — but not the same class. This structural arm
 * is what lets a Worker-side caller treat that reconstruction as the refusal it
 * is, with the same three fields every render site already reads.
 */
export interface WireExecutionFenceRefusal {
  readonly message: string;
  readonly status: number;
  readonly reason: { readonly code: string };
}

/** Every fence-authored refusal — the family a fenced surface catches as one. */
export type ExecutionFenceRefusal =
  | ExecutionFencedError
  | ExecutionFenceUnreadableError
  | WireExecutionFenceRefusal;

/** The two reason codes a fence refusal publishes, whatever carried it. */
const FENCE_REFUSAL_CODES: ReadonlySet<string> = new Set([
  'EXECUTION_FENCED',
  'EXECUTION_FENCE_UNREADABLE',
]);

/**
 * Whether an error is the fence refusing (or failing to answer). The two are
 * one class for every CALLER that must degrade closed — an alarm swallowing
 * both, a tick skipping its pass — because a state it could not read and a
 * state that forbids the work lead to the same action.
 *
 * The CODE counts as well as the class, because a refusal that crossed a
 * Durable Object boundary is no longer an instance of anything: the run object
 * threw ExecutionFencedError, `doErrorResponse` rendered it, and `doSummary`
 * rebuilt it on the Worker side as a `RunRouteError` carrying the same status
 * and the same structured reason. That rebuilt error IS the fence refusing —
 * the callers most in need of recognizing one (a run router deciding whether to
 * give a reservation's claim back, a tick deciding whether to skip a pass) are
 * exactly the ones sitting on the far side of that boundary, and an
 * instanceof-only test would answer "no" for every one of them.
 *
 * The two codes are matched by name rather than by any structural sniff: only
 * refusals this package authors publish them, and both are declared as literals
 * on the corresponding error classes, so a code arriving over the wire came from one of them.
 */
export function isExecutionFenceRefusal(
  error: unknown,
): error is ExecutionFenceRefusal {
  if (
    error instanceof ExecutionFencedError ||
    error instanceof ExecutionFenceUnreadableError
  ) {
    return true;
  }
  if (error === null || typeof error !== 'object') return false;
  const { message, status, reason } = error as {
    message?: unknown;
    status?: unknown;
    reason?: unknown;
  };
  // All three fields are checked, not just the code: this predicate NARROWS,
  // and every render site immediately reads message/status/reason off what it
  // narrowed. Asserting a shape on the strength of one field would hand them a
  // `status` of undefined, and `new Response(body, { status: undefined })`
  // fails inside the very catch block whose job is to never throw.
  if (typeof message !== 'string' || !Number.isInteger(status)) return false;
  if (reason === null || typeof reason !== 'object') return false;
  const { code } = reason as { code?: unknown };
  return typeof code === 'string' && FENCE_REFUSAL_CODES.has(code);
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
  readonly reason: { readonly code: 'INVALID_EXECUTION_FENCE_REQUEST' };

  constructor(message: string) {
    super(message);
    this.name = 'InvalidExecutionFenceRequestError';
    this.reason = { code: 'INVALID_EXECUTION_FENCE_REQUEST' };
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
  candidate?: string | RunExecutionIdentity,
): boolean {
  if (reading.state === 'open' || reading.state === 'draining') return true;
  if (
    reading.state !== 'proof-only' ||
    typeof candidate !== 'object' ||
    candidate === null ||
    candidate.tablePrefix === null
  )
    return false;
  try {
    const execution = normalizeD1RunExecutionIdentity(candidate);
    const proof = reading.proofExecution;
    return (
      execution.tablePrefix === candidate.tablePrefix &&
      proof !== undefined &&
      proof.tablePrefix === execution.tablePrefix &&
      proof.workflowId === execution.workflowId &&
      proof.runId === execution.runId &&
      proof.startToken === execution.startToken
    );
  } catch {
    return false;
  }
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

type FenceTransitionReceipt = readonly [
  1,
  ExecutionFenceState,
  ExecutionFenceState,
  string | null,
  number,
  number,
  boolean,
];

interface StoredExecutionFence {
  reading: ExecutionFenceVersionedReading;
  receipt: string | null;
  schemaStage: ExecutionFenceSchemaStage;
  raw: DeploymentIdentityProtocolRow;
}

/** @internal Exact current-stage observation for initial admission. */
export interface ExecutionFenceAdmissionObservation {
  readonly reading: ExecutionFenceVersionedReading;
  readonly schemaStage: 7;
  readonly raw: DeploymentIdentityProtocolRow;
}

const FENCE_ADMISSION_FIELDS = [
  'state',
  'mutation_epoch',
  'require_mutation_epoch',
  'transition_revision',
  'last_transition_request',
  'proof_key',
  'proof_run_id',
  'proof_table_prefix',
  'proof_workflow_id',
  'proof_start_token',
] as const;

/** @internal */
export function executionFenceAdmissionValues(
  observation: ExecutionFenceAdmissionObservation,
): readonly unknown[] {
  const values = FENCE_ADMISSION_FIELDS.map((key) => observation.raw[key]);
  for (const value of values.slice(4)) {
    if (value !== null && typeof value !== 'string') {
      throw new ExecutionFenceUnreadableError(
        'execution fence semantic fields are not readable',
      );
    }
  }
  return Object.freeze(values);
}

/** @internal */
export function executionFenceAdmissionSql(input: {
  readonly callerEpoch: string;
  readonly semantic: readonly string[];
  readonly schema: string;
  readonly statePredicate: string;
}): string {
  const { callerEpoch, semantic, schema, statePredicate } = input;
  if (semantic.length !== FENCE_ADMISSION_FIELDS.length) {
    throw new Error('execution fence admission parameter frame is invalid');
  }
  const nullable = FENCE_ADMISSION_FIELDS.slice(4)
    .map((key, index) => {
      const parameter = semantic[index + 4];
      return `typeof(f.${key}) IN ('null', 'text')
      AND typeof(f.${key}) = typeof(${parameter})
      AND f.${key} COLLATE BINARY IS ${parameter}`;
    })
    .join(' AND ');
  return `(SELECT json_group_array(json_array(name, type, "notnull", dflt_value, pk, hidden))
    FROM (SELECT name, type, "notnull", dflt_value, pk, hidden
      FROM pragma_table_xinfo('${EXECUTION_FENCE_TABLE}') ORDER BY cid)) COLLATE BINARY = ${schema}
    AND (SELECT COUNT(*) FROM ${EXECUTION_FENCE_TABLE}) = 1
    AND EXISTS (SELECT 1 FROM ${EXECUTION_FENCE_TABLE} AS f
      WHERE typeof(f.id) = 'text' AND f.id COLLATE BINARY = 'deployment'
        AND typeof(f.state) = 'text' AND f.state COLLATE BINARY IS ${semantic[0]}
        AND typeof(f.mutation_epoch) = 'integer' AND f.mutation_epoch IS ${semantic[1]}
        AND typeof(f.require_mutation_epoch) = 'integer' AND f.require_mutation_epoch IS ${semantic[2]}
        AND typeof(f.transition_revision) = 'integer' AND f.transition_revision IS ${semantic[3]}
        AND ${nullable}
        AND (f.require_mutation_epoch = 0 OR f.mutation_epoch = ${callerEpoch})
        AND (${statePredicate}))`;
}

function isFenceCounter(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function decodeTransitionReceipt(text: string): FenceTransitionReceipt {
  const value: unknown = JSON.parse(text);
  if (
    text.length > 512 ||
    !Array.isArray(value) ||
    value.length !== 7 ||
    value[0] !== 1 ||
    !isExecutionFenceState(value[1]) ||
    !isExecutionFenceState(value[2]) ||
    (value[2] === 'proof-only' ? !isPathSafeId(value[3]) : value[3] !== null) ||
    !isFenceCounter(value[4]) ||
    !isFenceCounter(value[5]) ||
    value[5] === Number.MAX_SAFE_INTEGER ||
    typeof value[6] !== 'boolean' ||
    (value[6] && value[4] === Number.MAX_SAFE_INTEGER) ||
    JSON.stringify(value) !== text
  ) {
    throw new Error('execution fence transition receipt is malformed');
  }
  return [1, value[1], value[2], value[3], value[4], value[5], value[6]];
}

function readingFromRow(
  row: DeploymentIdentityProtocolRow,
): StoredExecutionFence {
  row = Object.freeze(
    Object.fromEntries(
      Object.getOwnPropertyNames(row).map((key) => [key, row[key]]),
    ),
  );
  const metadata = decodeExecutionFenceMutationMetadata(row);
  const { state } = row;
  if (row.id !== EXECUTION_FENCE_ROW_ID || !isExecutionFenceState(state)) {
    throw new Error('execution fence row is not a recognized singleton');
  }
  const proofKey = row.proof_key;
  const proofRunId = row.proof_run_id;
  let proofExecution: D1RunExecutionIdentity | undefined;
  if (metadata.proofStartToken !== null) {
    proofExecution = normalizeD1RunExecutionIdentity({
      tablePrefix: metadata.proofTablePrefix,
      workflowId: metadata.proofWorkflowId,
      runId: proofRunId,
      startToken: metadata.proofStartToken,
    });
    if (
      proofExecution.tablePrefix !== metadata.proofTablePrefix ||
      !isPathSafeId(proofKey)
    ) {
      throw new Error('execution fence proof identity is not canonical');
    }
  }
  if (metadata.lastTransitionRequest !== null) {
    const [, , next, key, epoch, revision, advance] = decodeTransitionReceipt(
      metadata.lastTransitionRequest,
    );
    if (
      state !== next ||
      metadata.mutationEpoch !== epoch + Number(advance) ||
      metadata.transitionRevision !== revision + 1 ||
      (state === 'proof-only'
        ? proofKey !== key || (proofRunId !== null && !isPathSafeId(proofRunId))
        : proofKey !== null || proofRunId !== null)
    ) {
      throw new Error(
        'execution fence row disagrees with its transition receipt',
      );
    }
  }
  return {
    reading: {
      state,
      mutationEpoch: metadata.mutationEpoch,
      requireMutationEpoch: metadata.requireMutationEpoch,
      transitionRevision: metadata.transitionRevision,
      ...(proofExecution === undefined ? {} : { proofExecution }),
      ...(typeof proofKey === 'string' && proofKey.length > 0
        ? { proofKey }
        : {}),
      ...(typeof proofRunId === 'string' && proofRunId.length > 0
        ? { proofRunId }
        : {}),
    },
    receipt: metadata.lastTransitionRequest,
    schemaStage: metadata.schemaStage,
    raw: row,
  };
}

function fenceResultRows(result: unknown): DeploymentIdentityProtocolRow[] {
  if (
    result === null ||
    typeof result !== 'object' ||
    ('success' in result && result.success !== true) ||
    !('results' in result)
  ) {
    throw new Error('execution fence statement returned an invalid result');
  }
  const rows: unknown = result.results;
  if (!Array.isArray(rows))
    throw new Error('execution fence statement returned an invalid result');
  const length = rows.length;
  if (!Number.isSafeInteger(length) || length < 0)
    throw new Error('execution fence statement returned an invalid result');
  return Array.from({ length }, (_, index) => {
    if (!Object.hasOwn(rows, index))
      throw new Error('execution fence statement returned an invalid row');
    const row = rows[index];
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      throw new Error('execution fence statement returned an invalid row');
    }
    return row;
  });
}

function returningFence(result: unknown): StoredExecutionFence | undefined {
  const rows = fenceResultRows(result);
  if (rows.length > 1)
    throw new Error('execution fence UPDATE returned multiple rows');
  const row = rows[0];
  if (row === undefined) return undefined;
  const stored = readingFromRow(row);
  if (stored.schemaStage !== EXECUTION_FENCE_CURRENT_SCHEMA_STAGE)
    throw new Error('execution fence UPDATE returned a legacy row');
  return stored;
}

/** @internal Validate an already-observed current-schema RETURNING row. */
export function decodeExecutionFenceAdmissionRow(
  row: DeploymentIdentityProtocolRow,
): ExecutionFenceAdmissionObservation {
  const stored = readingFromRow(row);
  if (stored.schemaStage !== 7)
    throw new Error('initial admission requires current fence metadata');
  return Object.freeze({
    reading: Object.freeze(stored.reading),
    schemaStage: 7,
    raw: stored.raw,
  });
}

/** @internal Validate actual PRAGMA rows from a consistent readback batch. */
export async function validateExecutionFenceAdmissionSchema(
  result: unknown,
): Promise<void> {
  const columns = fenceResultRows(result);
  if ((await readExecutionFenceSchemaProtocol(async () => columns)) !== 7)
    throw new Error('initial admission requires the current fence schema');
}

/** @internal */
export async function captureExecutionFenceAdmissionSchema(
  result: unknown,
): Promise<string> {
  const rows = fenceResultRows(result).map((row) =>
    Object.freeze(
      Object.fromEntries(
        Object.getOwnPropertyNames(row).map((key) => [key, row[key]]),
      ),
    ),
  );
  await validateExecutionFenceAdmissionSchema({ results: rows });
  return JSON.stringify(
    rows.map(({ name, type, notnull, dflt_value, pk, hidden }) => [
      name,
      type,
      notnull,
      dflt_value,
      pk,
      hidden,
    ]),
  );
}

/**
 * SQLite/D1's "no such table", for THIS store's table: a table that was never
 * created is not a fault here — it is a pre-0.20 database, which reads as
 * `open`.
 *
 * The rule itself (walk the cause chain, bounded and cycle-safe, and test the
 * ROOT only) lives in cause-chain.ts, shared with the start-reservation store
 * because both answer the same question and both are dangerous in the same
 * direction. A chain with no reachable root degrades CLOSED there for the same
 * reason it does here: no root was observed, and an unobserved root is not
 * evidence of an absent table.
 */
function isMissingFenceTable(error: unknown): boolean {
  return missingTableReadsEmpty(error, EXECUTION_FENCE_TABLE);
}

async function selectedProofObservation(
  db: ExecutionFenceDatabase,
  address: D1RunAddress,
) {
  try {
    const prepared = prepareRawWorkflowSnapshotRead(db, address);
    const raw = decodeRawWorkflowSnapshotResult(
      await prepared.statement.all(),
      prepared.address,
    );
    if (raw === undefined) return undefined;
    const snapshot: unknown = JSON.parse(raw.snapshot);
    if (
      snapshot === null ||
      typeof snapshot !== 'object' ||
      Array.isArray(snapshot)
    )
      throw new Error('proof snapshot is malformed');
    const state = snapshot as Record<string, unknown>;
    if (state.runId !== raw.runId || !isRunStatus(state.status))
      throw new Error('proof snapshot identity or status is malformed');
    for (const key of ['requestContext', 'context', 'steps']) {
      const value = state[key];
      if (
        value !== undefined &&
        (value === null || typeof value !== 'object' || Array.isArray(value))
      )
        throw new Error('proof snapshot container is malformed');
    }
    const source = (
      state.requestContext as Record<string, unknown> | undefined
    )?.[RUN_PROVENANCE_CONTEXT_KEY];
    const start = decodeRunStartIdentity(source);
    if (start === undefined) return { raw, status: state.status };
    const provenance = decodeProgressRunProvenance(source);
    const execution = normalizeD1RunExecutionIdentity({
      ...prepared.address,
      startToken: provenance.startToken,
    });
    return { raw, status: state.status, execution, provenance };
  } catch (cause) {
    throw new ExecutionFenceUnreadableError('proof snapshot is not readable', {
      cause,
    });
  }
}

function reservationValues(row: StartReservationReading): unknown[] {
  if (row.binding.kind !== 'bound')
    throw new InvalidExecutionIdentityError('admission');
  return [
    row.key,
    row.owner.kind,
    row.owner.id,
    row.targetKind,
    row.targetId,
    row.runId,
    row.threadId ?? null,
    row.state,
    row.createdAt,
    row.updatedAt,
    row.binding.execution.startToken,
    row.binding.execution.tablePrefix,
    row.binding.execution.workflowId,
  ];
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
  /**
   * Required when `next` is 'proof-only'; rejected otherwise.
   *
   * `unknown` rather than `string` because every caller is a control-plane
   * route holding a parsed JSON body, and `#proofKeyFor` already validates it
   * against PATH_SAFE_ID_PATTERN and throws InvalidExecutionFenceRequestError
   * on anything else. Typing it `string` bought nothing and cost something: it
   * made every route write `body.proofKey as string`, an assertion that is
   * false exactly when the caller sent the wrong thing, so the one input this
   * field exists to police arrived pre-blessed at the type level.
   */
  proofKey?: unknown;
  expectedMutationEpoch?: unknown;
  expectedRevision?: unknown;
  advanceMutationEpoch?: unknown;
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
   * A missing table or missing legacy row reads as `open`. A missing modern row is
   * unreadable and is never silently recreated.
   */
  async read(): Promise<ExecutionFenceVersionedReading> {
    return (await this.#readStored())?.reading ?? OPEN_EXECUTION_FENCE;
  }

  usesDatabase(binding: object): boolean {
    return this.#db === binding;
  }

  /** @internal Pure strict observation; seed only on the admission preparation path. */
  async readForAdmission(): Promise<ExecutionFenceAdmissionObservation> {
    try {
      const rows = fenceResultRows(
        await this.#db
          .prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE} LIMIT 2`)
          .all(),
      );
      const row = rows[0];
      if (rows.length !== 1 || row === undefined)
        throw new Error('initial admission requires an exact fence singleton');
      const observation = decodeExecutionFenceAdmissionRow(row);
      await validateExecutionFenceAdmissionSchema(
        await this.#db
          .prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`)
          .all(),
      );
      return observation;
    } catch (error) {
      throw new ExecutionFenceUnreadableError(
        'initial admission requires current fence metadata',
        { cause: error },
      );
    }
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
    await this.#initialize(safeState);
    await this.#readStored();
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
  ): Promise<ExecutionFenceVersionedReading> {
    const expected = assertExecutionFenceState(
      input.expected,
      'expected state',
    );
    const next = assertExecutionFenceState(input.next, 'next state');
    const proofKey = this.#proofKeyFor(next, input.proofKey);
    const { expectedMutationEpoch: epoch, expectedRevision: revision } = input;
    const rawAdvance = input.advanceMutationEpoch;
    const advance = rawAdvance ?? false;
    const upgraded = epoch !== undefined || revision !== undefined;
    if (
      (rawAdvance !== undefined && typeof rawAdvance !== 'boolean') ||
      (upgraded && (!isFenceCounter(epoch) || !isFenceCounter(revision))) ||
      (advance && !upgraded) ||
      revision === Number.MAX_SAFE_INTEGER ||
      (advance && epoch === Number.MAX_SAFE_INTEGER)
    ) {
      throw new InvalidExecutionFenceRequestError(
        'fence expectations must be paired safe counters and advanceMutationEpoch must be boolean',
      );
    }
    const receipt = upgraded
      ? JSON.stringify([
          1,
          expected,
          next,
          proofKey ?? null,
          epoch,
          revision,
          advance,
        ])
      : null;
    await this.#initialize('open');
    await this.#readStored();
    let result: unknown;
    try {
      const statement = upgraded
        ? this.#db
            .prepare(
              `UPDATE ${EXECUTION_FENCE_TABLE}
           SET state = ?1, proof_key = ?2, proof_run_id = NULL,
               proof_table_prefix = NULL, proof_workflow_id = NULL, proof_start_token = NULL,
               mutation_epoch = mutation_epoch + ?3,
               require_mutation_epoch = CASE WHEN ?3 = 1 THEN 1 ELSE require_mutation_epoch END,
               transition_revision = transition_revision + 1,
               last_transition_request = ?4, updated_at = ?5
           WHERE id = ?6 AND state = ?7
             AND mutation_epoch = ?8 AND transition_revision = ?9
             AND require_mutation_epoch IN (0, 1)
             AND transition_revision < 9007199254740991
             AND (?3 = 0 OR mutation_epoch < 9007199254740991)
           RETURNING *`,
            )
            .bind(
              next,
              proofKey ?? null,
              Number(advance),
              receipt,
              this.#now(),
              EXECUTION_FENCE_ROW_ID,
              expected,
              epoch,
              revision,
            )
        : this.#db
            .prepare(
              `UPDATE ${EXECUTION_FENCE_TABLE}
           SET state = ?, proof_key = ?, proof_run_id = NULL,
               proof_table_prefix = NULL, proof_workflow_id = NULL, proof_start_token = NULL,
               transition_revision = transition_revision + 1,
               last_transition_request = NULL, updated_at = ?
           WHERE id = ? AND state = ? AND require_mutation_epoch = 0
             AND mutation_epoch = 0 AND transition_revision < 9007199254740991
           RETURNING *`,
            )
            .bind(
              next,
              proofKey ?? null,
              this.#now(),
              EXECUTION_FENCE_ROW_ID,
              expected,
            );
      result = await statement.all();
    } catch (error) {
      if (receipt !== null) {
        const stored = await this.#readStored().catch(() => undefined);
        if (stored?.receipt === receipt) return stored.reading;
      }
      throw new ExecutionFenceUnreadableError(
        'execution fence transition could not be recorded',
        { cause: error },
      );
    }
    const returned = this.#decodeReturned(result);
    if (returned !== undefined) return returned.reading;
    const stored = await this.#readStored();
    if (receipt !== null && stored?.receipt === receipt) return stored.reading;
    const reading = stored?.reading ?? OPEN_EXECUTION_FENCE;
    throw new FenceTransitionConflictError(expected, reading.state, {
      reading,
      conflict:
        !upgraded && reading.requireMutationEpoch
          ? 'versioned-expectation-required'
          : 'expectation-mismatch',
    });
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
  async recordProofRun(
    proofKey: string,
    runId: string,
    admitted?: Pick<
      ExecutionFenceVersionedReading,
      'mutationEpoch' | 'transitionRevision'
    >,
  ): Promise<boolean> {
    const epoch = admitted?.mutationEpoch;
    const revision = admitted?.transitionRevision;
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
    if (
      admitted !== undefined &&
      (admitted === null ||
        typeof admitted !== 'object' ||
        !isFenceCounter(epoch) ||
        !isFenceCounter(revision))
    ) {
      throw new InvalidExecutionFenceRequestError(
        'proof admission must contain safe epoch and revision counters',
      );
    }
    const observed = await this.#readStored();
    if (observed === undefined) return false;
    if (observed.schemaStage < EXECUTION_FENCE_CURRENT_SCHEMA_STAGE) {
      await this.#initialize(observed.reading.state);
      await this.#readStored();
    }
    let result: unknown;
    try {
      const statement = this.#db
        .prepare(
          `UPDATE ${EXECUTION_FENCE_TABLE}
       SET updated_at = CASE WHEN proof_run_id IS NULL THEN ? ELSE updated_at END,
           proof_run_id = ?
       WHERE id = ? AND state = 'proof-only' AND proof_key = ?
         AND (proof_run_id IS NULL OR proof_run_id = ?)
         AND proof_table_prefix IS NULL AND proof_workflow_id IS NULL AND proof_start_token IS NULL
         AND ${
           admitted === undefined
             ? 'require_mutation_epoch = 0 AND mutation_epoch = 0'
             : 'mutation_epoch = ? AND transition_revision = ?'
}
       RETURNING *`,
        )
        .bind(
          this.#now(),
          runId,
          EXECUTION_FENCE_ROW_ID,
          proofKey,
          runId,
          ...(admitted === undefined ? [] : [epoch, revision]),
        );
      result = await statement.all();
    } catch (error) {
      const stored = await this.#readStored().catch(() => undefined);
      const reading = stored?.reading;
      if (
        reading?.state === 'proof-only' &&
        reading.proofKey === proofKey &&
        reading.proofRunId === runId &&
        reading.proofExecution === undefined &&
        stored?.raw.proof_table_prefix === null &&
        stored.raw.proof_workflow_id === null &&
        stored.raw.proof_start_token === null &&
        (admitted === undefined
          ? !reading.requireMutationEpoch && reading.mutationEpoch === 0
          : reading.mutationEpoch === epoch &&
            reading.transitionRevision === revision)
      ) {
        return true;
      }
      throw new ExecutionFenceUnreadableError(
        'execution fence proof run could not be recorded',
        { cause: error },
      );
    }
    const returned = this.#decodeReturned(result);
    if (returned !== undefined) {
      if (
        returned.reading.proofExecution !== undefined ||
        returned.raw.proof_table_prefix !== null ||
        returned.raw.proof_workflow_id !== null ||
        returned.raw.proof_start_token !== null
      )
        throw new ExecutionFenceUnreadableError(
          'legacy proof write returned a modern binding',
        );
      return true;
    }
    await this.#readStored();
    return false;
  }

  async readCurrentRunExecution(
    address: D1RunAddress,
  ): Promise<D1RunExecutionIdentity | undefined> {
    return (await selectedProofObservation(this.#db, address))?.execution;
  }

  async rebindProofRun(options: {
    reservation: StartReservationReading;
    execution: D1StartExecutionIdentity;
    proof: ProofEntryExpectation;
    mutationEpoch?: number;
    reservationStore: { usesDatabase(binding: object): boolean };
  }): Promise<boolean> {
    const {
      reservation: input,
      execution: rawExecution,
      proof: rawProof,
      mutationEpoch,
      reservationStore,
    } = options;
    const reservation = captureBoundReservation(input);
    const { tablePrefix, workflowId, runId, startToken, owner, target } =
      rawExecution;
    const normalized = normalizeStartExecutionIdentity({
      tablePrefix,
      workflowId,
      runId,
      startToken,
      owner,
      target,
    });
    const execution = normalizeD1RunExecutionIdentity(normalized);
    const {
      key,
      mutationEpoch: epoch,
      transitionRevision: revision,
    } = rawProof;
    const callerEpoch = normalizeMutationEpoch(mutationEpoch);
    const usesDatabase = reservationStore.usesDatabase;
    const now = this.#now();
    if (
      tablePrefix !== execution.tablePrefix ||
      key !== reservation.key ||
      !isFenceCounter(epoch) ||
      !isFenceCounter(revision) ||
      typeof now !== 'number' ||
      !Number.isFinite(now) ||
      typeof usesDatabase !== 'function' ||
      !Reflect.apply(usesDatabase, reservationStore, [this.#db]) ||
      reservation.binding.kind !== 'bound' ||
      normalized.owner.kind !== reservation.owner.kind ||
      normalized.owner.id !== reservation.owner.id ||
      normalized.target.kind !== reservation.targetKind ||
      normalized.target.id !== reservation.targetId ||
      (normalized.target.kind === 'agent'
        ? normalized.target.threadId
        : undefined) !== reservation.threadId ||
      !admitsExistingRun(
        { state: 'proof-only', proofExecution: execution },
        reservation.binding.execution,
      )
    )
      throw new InvalidExecutionIdentityError('admission');
    try {
      const observed = await this.readForAdmission();
      const requireSchemas = async () => {
        await validateExecutionFenceAdmissionSchema(
          await this.#db
            .prepare(`PRAGMA table_xinfo(${EXECUTION_FENCE_TABLE})`)
            .all(),
        );
        validateStartReservationAdmissionSchema(
          await this.#db
            .prepare(`PRAGMA table_xinfo(${START_IDEMPOTENCY_TABLE})`)
            .all(),
        );
      };
      await requireSchemas();
      const current = decodeStartReservationAdmissionResult(
        await this.#db
          .prepare(
            `SELECT * FROM ${START_IDEMPOTENCY_TABLE} WHERE key = ? LIMIT 2`,
          )
          .bind(key)
          .all(),
      );
      const selected = await selectedProofObservation(this.#db, execution);
      const reading = observed.reading;
      if (
        current === undefined ||
        current.binding.kind !== 'bound' ||
        !sameReservationIdentity(current, reservation) ||
        JSON.stringify(reservationValues(current)) !==
          JSON.stringify(reservationValues(reservation)) ||
        selected?.execution === undefined ||
        selected.status === 'pending' ||
        !admitsExistingRun(
          { state: 'proof-only', proofExecution: execution },
          selected.execution,
        ) ||
        selected.provenance?.startIdentity?.owner.kind !==
          reservation.owner.kind ||
        selected.provenance.startIdentity.owner.id !== reservation.owner.id ||
        selected.provenance.startIdentity.target.kind !==
          reservation.targetKind ||
        selected.provenance.startIdentity.target.id !== reservation.targetId ||
        (selected.provenance.startIdentity.target.kind === 'agent'
          ? selected.provenance.startIdentity.target.threadId
          : undefined) !== reservation.threadId ||
        reading.state !== 'proof-only' ||
        reading.proofKey !== key ||
        reading.mutationEpoch !== epoch ||
        reading.transitionRevision !== revision ||
        (reading.requireMutationEpoch && callerEpoch !== epoch) ||
        (reading.proofRunId !== undefined &&
          !admitsExistingRun(reading, execution))
      )
        return false;
      const raw: RawWorkflowSnapshot = selected.raw;
      const snapshotPredicate = `EXISTS (SELECT 1 FROM "${execution.tablePrefix}mastra_workflow_snapshot"
        WHERE workflow_name = ? AND run_id = ? AND resourceId IS ? AND snapshot = ? AND createdAt = ? AND updatedAt = ?)`;
      const reservationPredicate = `EXISTS (SELECT 1 FROM ${START_IDEMPOTENCY_TABLE}
        WHERE key = ? AND owner_kind = ? AND owner_id = ? AND target_kind = ? AND target_id = ? AND run_id = ?
        AND thread_id IS ? AND state = ? AND created_at = ? AND updated_at = ?
        AND start_token = ? AND start_table_prefix IS ? AND start_workflow_id = ?)`;
      const framePredicate = `id = 'deployment' AND state = 'proof-only' AND proof_key = ?
        AND mutation_epoch = ? AND transition_revision = ? AND require_mutation_epoch = ? AND last_transition_request IS ?
        AND (require_mutation_epoch = 0 OR mutation_epoch = ?)`;
      const exactTuple =
        'proof_run_id = ? AND proof_table_prefix = ? AND proof_workflow_id = ? AND proof_start_token = ?';
      const emptyTuple =
        'proof_run_id IS NULL AND proof_table_prefix IS NULL AND proof_workflow_id IS NULL AND proof_start_token IS NULL';
      const tuple = [
        execution.runId,
        execution.tablePrefix,
        execution.workflowId,
        execution.startToken,
      ];
      const frame = [
        key,
        epoch,
        revision,
        Number(reading.requireMutationEpoch),
        observed.raw.last_transition_request,
        callerEpoch ?? null,
      ];
      const rowValues = [
        raw.workflowId,
        raw.runId,
        raw.resourceId,
        raw.snapshot,
        raw.createdAt,
        raw.updatedAt,
        ...reservationValues(reservation),
      ];
      const expectedTime =
        reading.proofRunId === undefined ? now : observed.raw.updated_at;
      const validateReturned = (returnedResult: unknown): boolean => {
        const returned = this.#decodeReturned(returnedResult);
        if (returned === undefined) return false;
        const next = returned.reading;
        if (
          next.state !== 'proof-only' ||
          next.proofKey !== key ||
          next.mutationEpoch !== epoch ||
          next.transitionRevision !== revision ||
          next.requireMutationEpoch !== reading.requireMutationEpoch ||
          returned.receipt !== observed.raw.last_transition_request ||
          !admitsExistingRun(next, execution) ||
          returned.raw.updated_at !== expectedTime
        )
          throw new ExecutionFenceUnreadableError(
            'proof nomination returned an unexpected fence',
          );
        return true;
      };
      let result: unknown;
      try {
        result = await this.#db
          .prepare(`UPDATE ${EXECUTION_FENCE_TABLE}
          SET updated_at = CASE WHEN proof_run_id IS NULL THEN ? ELSE updated_at END,
            proof_run_id = ?, proof_table_prefix = ?, proof_workflow_id = ?, proof_start_token = ?
          WHERE ${framePredicate} AND ((${emptyTuple}) OR (${exactTuple}))
            AND ${snapshotPredicate} AND ${reservationPredicate} RETURNING *`)
          .bind(now, ...tuple, ...frame, ...tuple, ...rowValues)
          .all();
      } catch (cause) {
        try {
          const converged = await this.#db
            .prepare(`SELECT * FROM ${EXECUTION_FENCE_TABLE}
            WHERE ${framePredicate} AND ${exactTuple} AND ${snapshotPredicate} AND ${reservationPredicate} LIMIT 2`)
            .bind(...frame, ...tuple, ...rowValues)
            .all();
          await requireSchemas();
          if (validateReturned(converged)) return true;
        } catch {
          /* Preserve the failed write's cause. */
        }
        throw new ExecutionFenceUnreadableError(
          'proof nomination could not be recorded',
          { cause },
        );
      }
      const nominated = validateReturned(result);
      if (!nominated) {
        await requireSchemas();
        await this.readForAdmission();
      }
      return nominated;
    } catch (cause) {
      if (cause instanceof ExecutionFenceUnreadableError) throw cause;
      throw new ExecutionFenceUnreadableError(
        'proof nomination is not readable',
        { cause },
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

  readonly #execute: DeploymentIdentityProtocolExecutor = async (statement) => {
    const prepared = this.#db
      .prepare(statement.sql)
      .bind(...statement.bindings);
    if (statement.mode === 'write') {
      await prepared.run();
      return [];
    }
    return fenceResultRows(await prepared.all<DeploymentIdentityProtocolRow>());
  };

  async #initialize(state: ExecutionFenceState): Promise<void> {
    try {
      await initializeExecutionFenceProtocol(this.#execute, {
        state,
        seededAt: this.#now(),
      });
    } catch (error) {
      throw new ExecutionFenceUnreadableError(
        'execution fence could not be initialized',
        { cause: error },
      );
    }
  }

  #decodeReturned(result: unknown): StoredExecutionFence | undefined {
    try {
      return returningFence(result);
    } catch (error) {
      throw new ExecutionFenceUnreadableError(
        'execution fence UPDATE result is not readable',
        { cause: error },
      );
    }
  }

  async #readStored(): Promise<StoredExecutionFence | undefined> {
    try {
      let minimumStage = 0;
      for (let attempt = 0; attempt < 2; attempt += 1) {
        let rows: readonly DeploymentIdentityProtocolRow[];
        try {
          rows = await this.#execute({
            mode: 'read',
            sql: `SELECT * FROM ${EXECUTION_FENCE_TABLE} LIMIT 2`,
            bindings: [],
          });
        } catch (error) {
          if (attempt === 0 && isMissingFenceTable(error)) return undefined;
          throw error;
        }
        const stage = await readExecutionFenceSchemaProtocol(this.#execute);
        if (
          !Array.isArray(rows) ||
          stage === undefined ||
          stage < minimumStage
        ) {
          throw new Error(
            'execution fence row observation has no compatible schema',
          );
        }
        if (rows.length === 0 && attempt === 0) {
          if (stage === 0) return undefined;
          minimumStage = stage;
          continue;
        }
        if (rows.length !== 1)
          throw new Error('execution fence row is not an exact singleton');
        const stored = readingFromRow(rows[0]);
        if (stored.schemaStage > stage)
          throw new Error(
            'execution fence schema observation precedes row metadata',
          );
        return stored;
      }
      throw new Error('execution fence row is missing');
    } catch (error) {
      throw new ExecutionFenceUnreadableError(
        'execution fence state is not readable',
        { cause: error },
      );
    }
  }
}
