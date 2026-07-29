// SPDX-License-Identifier: Apache-2.0
// Connector SDK — createConnector() wraps Mastra's createTool() with an
// enforced permission manifest. Mastra createTool() has no manifest field
// (its MCP annotations are descriptive only), so the manifest is stripped
// from the config, compiled, and enforced by wrapping execute:
//
// 1. Network egress — declared domains checked against the org allowlist
// 2. Write gate     — write-class calls needing approval are denied unless
//                     the request carries a grant, on every path; Mastra's
//                     native requireApproval is also compiled so agent runs
//                     pause for the decision, but it never substitutes for
//                     the grant
// 3. Idempotency    — keyed replay returns the stored result, so retries
//                     and DO lifecycle boundaries cannot duplicate a side
//                     effect
// 4. Dry-run        — a caller-requested simulation (DRY_RUN_CONTEXT_KEY)
//                     runs the connector's side-effect-free dryRunExecute;
//                     connectors that do not declare dry-run support fail
//                     the request closed instead of executing for real
// 5. Rate limit     — a '<count>/<unit>' manifest budget enforced against a
//                     fixed-window counter store; only actual executions
//                     consume it
//
// Denials throw ConnectorPolicyError; every decision lands in the audit log.
// The manifest carries only fields the wrapper enforces (see
// docs/connector-interface.md).

import type { RequestContext } from '@mastra/core/request-context';
import type { PublicSchema } from '@mastra/core/schema';
import type { Tool, ToolExecutionContext } from '@mastra/core/tools';
import { createTool } from '@mastra/core/tools';
import type { AuditLogger } from '../audit/index.js';
import { safeAuditErrorSummary } from '../audit/safe-error.js';
import type {
  NetworkEgressOptions,
  PolicyDecision,
  SideEffect,
  ToolCallContext,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from '../policy-engine/tool-policy.js';
import {
  approvalRequired,
  assertEgressHostList,
  ISOLATION_SCOPE_CONTEXT_KEY,
  LLM_BACKGROUND_OVERRIDE_KEY,
  networkEgress,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from '../policy-engine/tool-policy.js';
import { actorFromRequestContext } from '../rbac/index.js';
import type { EgressFetchBase, EgressGuardedFetch } from './egress-fetch.js';
import { EgressDeniedError, egressFetch } from './egress-fetch.js';
import { newToken } from './new-token.js';

/** Permission manifest — what the connector declares about itself. */
export interface PermissionManifest {
  /** Worst side effect the connector can cause. */
  sideEffect: SideEffect;
  /** Hostnames this connector calls; gated by the networkEgress policy. */
  egress?: readonly string[];
  /**
   * Caller must supply a per-call idempotency key
   * (IDEMPOTENCY_KEY_CONTEXT_KEY in requestContext). Replays of a stored
   * key return the stored result without re-executing.
   */
  idempotencyKey?: boolean;
  /** Always require human approval, regardless of org policy. */
  requiresApproval?: boolean;
  /**
   * Connector supports side-effect-free simulation: requires
   * `ConnectorConfig.dryRunExecute`. Callers request a simulation per call
   * by setting requestContext DRY_RUN_CONTEXT_KEY to true.
   */
  dryRun?: boolean;
  /**
   * Execution budget as '<count>/<unit>' — e.g. '100/min'; units are the
   * singular s|sec|second|m|min|minute|h|hour|d|day. Enforced with fixed
   * windows against `policies.rateLimitStore`; only actual executions
   * consume budget (denied calls, replays, and shared in-flight joins do
   * not).
   */
  rateLimit?: string;
  /**
   * Allow Mastra background intent for this connector. The default is
   * foreground-only. Only a read-only connector may enable this field;
   * write-class connectors fail at construction.
   */
  background?: boolean;
}

/** Completed result stored for idempotent replay. */
export interface IdempotencyRecord {
  /** Connector result returned by future calls with the same scoped key. */
  result: unknown;
}

/**
 * Result storage keyed by `${connectorId}:${key}`. The record wrapper
 * distinguishes a stored undefined result from a miss.
 *
 * get/put plus the wrapper's in-flight dedup close same-isolate races only.
 * Durable implementations (D1/KV) must implement AtomicIdempotencyStore —
 * its reserve() claim is what stops two isolates racing one key from both
 * missing get() and both executing. D1IdempotencyStore ships that shape.
 */
export interface IdempotencyStore {
  /** Return the completed record for a scoped key, or `undefined` on a miss. */
  get(
    key: string,
  ): IdempotencyRecord | undefined | Promise<IdempotencyRecord | undefined>;
  /**
   * Finalize a key's record. `token` is the lease returned by an atomic
   * reserve(): when supplied, the store finalizes ONLY if the key still
   * belongs to that lease. A stale holder whose lease was taken over cannot
   * overwrite the new result. Omit the token on the legacy get/put path,
   * which upserts
   * unconditionally (same-isolate protection only).
   */
  put(
    key: string,
    record: IdempotencyRecord,
    token?: string,
  ): void | Promise<void>;
}

/**
 * Outcome of an atomic reservation: execute a newly reserved key, replay a
 * completed record, or report that another isolate still owns the key.
 */
export type IdempotencyReservation =
  | {
      /** This caller owns the reservation and may execute. */
      state: 'reserved';
      /** Opaque lease required to finalize or release the reservation. */
      token: string;
      /** Whether this reservation replaced a stale pending holder. */
      tookOver?: boolean;
    }
  | {
      /** A completed result exists and must be replayed without execution. */
      state: 'replay';
      /** Completed result associated with the key. */
      record: IdempotencyRecord;
    }
  | {
      /** Another isolate owns a non-stale reservation. */
      state: 'pending';
    };

/**
 * Idempotency store with an atomic claim — the shape durable, cross-isolate
 * implementations must take: reserve() is a compare-and-set, so two isolates
 * racing one key resolve to exactly one 'reserved' winner. The connector
 * wrapper prefers this path whenever a store implements it.
 */
export interface AtomicIdempotencyStore extends IdempotencyStore {
  /** Atomically reserve a scoped key or return its current state. */
  reserve(
    key: string,
  ): IdempotencyReservation | Promise<IdempotencyReservation>;
  /**
   * Drop a pending reservation after a failed execute — failures stay
   * retryable. `token` is the lease from reserve(): when supplied, only the
   * matching lease's pending row is dropped, so a stale holder cannot delete
   * a newer claim.
   */
  release(key: string, token?: string): void | Promise<void>;
}

function isAtomicStore(
  store: IdempotencyStore,
): store is AtomicIdempotencyStore {
  return (
    typeof (store as Partial<AtomicIdempotencyStore>).reserve === 'function' &&
    typeof (store as Partial<AtomicIdempotencyStore>).release === 'function'
  );
}

// Outcome of a keyed attempt, shareable with same-isolate twins before the
// store round-trip completes. Pre-marked handled: when no twin joins, a
// rejection must not surface as unhandled — the leader rethrows through its
// own path.
function sharedOutcome<T>(): {
  promise: Promise<T>;
  resolve: (value: T | Promise<T>) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: T | Promise<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  promise.catch(() => {});
  return { promise, resolve, reject };
}

// What a keyed probe resolved to: a replayed stored result, or a fresh
// attempt for the caller (and any joined twins) to adopt.
type KeyedOutcome<T> =
  | { kind: 'replay'; result: T }
  | { kind: 'attempt'; attempt: Promise<T> };

/**
 * Dev/test store. Per-isolate and evictable — production replay protection
 * needs a durable store (D1IdempotencyStore).
 */
export class InMemoryIdempotencyStore implements AtomicIdempotencyStore {
  readonly #maxEntries: number;
  #entries = new Map<string, IdempotencyRecord>();
  // Reservations live outside the LRU: a pending key is never evictable. Value
  // is the reservation lease token so release()/put() can CAS on ownership.
  #pending = new Map<string, string>();

  constructor(options: { maxEntries?: number } = {}) {
    this.#maxEntries = options.maxEntries ?? 1000;
  }

  /** Return a completed in-memory record, or `undefined` on a miss. */
  get(key: string): IdempotencyRecord | undefined {
    return this.#entries.get(key);
  }

  /** Atomically reserve a key within this JavaScript isolate. */
  reserve(key: string): IdempotencyReservation {
    const record = this.#entries.get(key);
    if (record) return { state: 'replay', record };
    if (this.#pending.has(key)) return { state: 'pending' };
    const token = newToken();
    this.#pending.set(key, token);
    return { state: 'reserved', token };
  }

  release(key: string, token?: string): void {
    // CAS on the lease: a token-less caller (legacy) drops any pending row; a
    // token-bearing caller drops only its own, so a taken-over stale holder
    // never deletes the current holder's claim (audit D2).
    if (token === undefined || this.#pending.get(key) === token) {
      this.#pending.delete(key);
    }
  }

  put(key: string, record: IdempotencyRecord, token?: string): void {
    // Lost the lease (taken over as stale) — do NOT overwrite the new holder's
    // in-flight claim or its record (audit D2). Token-less puts (legacy path)
    // finalize unconditionally.
    if (token !== undefined && this.#pending.get(key) !== token) return;
    this.#pending.delete(key);
    // Delete-before-set refreshes insertion order, so eviction drops the
    // least recently written key (Map iterates in insertion order).
    this.#entries.delete(key);
    this.#entries.set(key, record);
    if (this.#entries.size > this.#maxEntries) {
      const oldest = this.#entries.keys().next().value;
      if (oldest !== undefined) this.#entries.delete(oldest);
    }
  }

  /** Remove all completed records and pending reservations. */
  clear(): void {
    this.#entries.clear();
    this.#pending.clear();
  }
}

/**
 * Fixed-window rate-limit counters keyed by connector id. Implementations
 * back the manifest's `rateLimit` budget. The store's reach IS the budget's
 * reach: InMemoryRateLimitStore caps per isolate (per RUN under DO-per-run
 * routing); a declared cap that must hold across isolates needs
 * D1RateLimitStore (or an equivalent shared store).
 */
export interface RateLimitStore {
  /**
   * Atomically count one call against the connector's current fixed window
   * and return the post-increment count. `now` is caller-supplied epoch ms
   * so stores stay clock-free.
   */
  increment(
    key: string,
    windowMs: number,
    now: number,
  ): number | Promise<number>;
}

/** Dev/test store — per-isolate fixed windows, replaced on rollover. */
export class InMemoryRateLimitStore implements RateLimitStore {
  #windows = new Map<string, { windowStart: number; count: number }>();

  increment(key: string, windowMs: number, now: number): number {
    // Deterministic bucketing: the window containing `now`, aligned to the
    // epoch, so every isolate computes the same window boundaries.
    const windowStart = now - (now % windowMs);
    const entry = this.#windows.get(key);
    if (!entry || entry.windowStart !== windowStart) {
      this.#windows.set(key, { windowStart, count: 1 });
      return 1;
    }
    entry.count += 1;
    return entry.count;
  }
}

/** Org-level policy bindings enforced by the connector's execute wrapper. */
export interface ConnectorPolicies {
  /** Organization allowlist applied to the manifest's declared hosts. */
  networkEgress?: NetworkEgressOptions;
  /** Organization approval rules for write-class connector IDs. */
  writePermissions?: WritePermissionsPolicy;
  /**
   * Custom tool-boundary evaluators, run pre-execute after the built-in
   * network-egress gate, in registration order.
   */
  evaluators?: readonly ToolPolicyEvaluator[];
  /** Store used when the manifest requires an idempotency key. */
  idempotencyStore?: IdempotencyStore;
  /** Required when the manifest declares `rateLimit`. */
  rateLimitStore?: RateLimitStore;
  /** Optional audit logger for connector decisions and failures. */
  audit?: AuditLogger;
  /**
   * Base fetch the per-call egress guard wraps before handing it to
   * `execute` as `ConnectorRuntime.fetch` (tests inject the vendor mock
   * here). Defaults to the runtime's global fetch.
   */
  fetch?: EgressFetchBase;
}

/**
 * Per-execution runtime handed to `execute`/`dryRunExecute` as the third
 * argument. `fetch` is bound to the manifest's declared `egress`: every
 * actual request — redirect hops included — must resolve to a declared host
 * or it is denied (`ConnectorPolicyError`, policy 'egress-fetch') and
 * audited. This is the runtime half of the egress posture (the networkEgress
 * policy gates the declared list; this guard pins actual requests to it), so
 * actual ⊆ declared ⊆ org-allowed. A manifest with no `egress` gets a fetch
 * that denies everything. A vendor SDK carrying its own HTTP stack bypasses
 * the guard — route its traffic through this fetch (most SDKs accept a
 * fetch/transport option) or that connector's egress posture degrades to
 * declaration-only.
 */
export interface ConnectorRuntime {
  /** Fetch guarded by the connector manifest's declared egress hosts. */
  fetch: EgressGuardedFetch;
}

/** Definition compiled by `createConnector()` into an enforced Mastra tool. */
export interface ConnectorConfig<TInput = unknown, TOutput = unknown> {
  /** Stable, colon-free connector identifier. */
  id: string;
  /** Description presented to the model and tool consumers. */
  description: string;
  /** Optional schema that Mastra validates before connector policies run. */
  inputSchema?: PublicSchema<TInput>;
  /** Optional schema that Mastra validates after execution. */
  outputSchema?: PublicSchema<TOutput>;
  /** Execute the connector after every configured gate has allowed the call. */
  execute: (
    inputData: TInput,
    context: ToolExecutionContext,
    runtime: ConnectorRuntime,
  ) => Promise<TOutput>;
  /**
   * Side-effect-free simulation of `execute`, returning the same output
   * shape. Required when `permissions.dryRun` is declared, forbidden
   * otherwise — the manifest must state what the connector supports. Gets
   * the same egress-guarded runtime as `execute`: a simulation's read-only
   * vendor calls stay inside the declared egress too.
   */
  dryRunExecute?: (
    inputData: TInput,
    context: ToolExecutionContext,
    runtime: ConnectorRuntime,
  ) => Promise<TOutput>;
  /** Enforced declaration of side effects and supported controls. */
  permissions: PermissionManifest;
  /** Omit for an ungated connector (classification + audit only). */
  policies?: ConnectorPolicies;
}

/** Exact suspension identity shared by a resume leg and its grants. */
export interface ConnectorApprovalSuspension {
  /** Suspended workflow step path. */
  stepPath: readonly string[];
  /** Epoch-ms timestamp of the suspension in Mastra's persisted snapshot. */
  suspendedAt: number;
  /** Runtime-owned ordinal; absent only for the step's first suspension. */
  resumeCount?: number;
}

/** Identity fields shared by every structured connector grant scope. */
export interface ConnectorApprovalGrantBase {
  /** Connector this grant can authorize. */
  connectorId: string;
  /** Workflow whose runtime minted the grant. */
  workflowId: string;
  /** Server-minted run whose runtime minted the grant. */
  runId: string;
  /** Opaque tenant/isolation scope, when the runtime has one. */
  isolationScope?: string;
}

/**
 * Structured connector capability. Tool-call grants are the default for
 * durable-agent approvals because Mastra persists and reproduces that identity.
 * Workflow gates without a reproducible tool call use exact suspension scope.
 * Run scope is a deliberate standing grant and is never inferred.
 */
export type ConnectorApprovalGrant =
  | (ConnectorApprovalGrantBase & {
      scope: 'tool-call';
      suspension: ConnectorApprovalSuspension;
      toolCallId: string;
    })
  | (ConnectorApprovalGrantBase & {
      scope: 'suspension';
      suspension: ConnectorApprovalSuspension;
    })
  | (ConnectorApprovalGrantBase & {
      scope: 'run';
    });

/** Runtime-owned identity of the leg currently executing the connector. */
export type ConnectorExecutionIdentity =
  | {
      kind: 'start';
      workflowId: string;
      runId: string;
      isolationScope?: string;
    }
  | {
      kind: 'resume';
      workflowId: string;
      runId: string;
      isolationScope?: string;
      suspension: ConnectorApprovalSuspension;
    };

/**
 * requestContext key containing readonly structured connector grants. Only
 * trusted runtime code may populate it. Legacy connector-ID arrays are invalid
 * and fail closed.
 */
export const CONNECTOR_GRANTS_CONTEXT_KEY = 'breakwater.connectorGrants';

/**
 * requestContext key containing the runtime-owned current execution identity.
 * The connector compares grants against it so a stale exact-leg grant cannot
 * authorize a later suspension even if context propagation regresses.
 */
export const CONNECTOR_EXECUTION_CONTEXT_KEY = 'breakwater.connectorExecution';

const LEGACY_CONNECTOR_GRANTS_CONTEXT_KEY = 'breakwater.approvedConnectors';

/** requestContext key: per-call idempotency key (non-empty string). */
export const IDEMPOTENCY_KEY_CONTEXT_KEY = 'breakwater.idempotencyKey';

/**
 * requestContext key: set to `true` to request a dry-run simulation of the
 * call. Supported connectors run `dryRunExecute` (pre-execute gates still
 * apply; the approval grant, Mastra's native approval pause, the rate-limit
 * budget, and the idempotency machinery are skipped — a simulation has no
 * side effect to protect and must not poison the replay store). The native
 * pause is skipped via the compiled `requireApproval` predicate on the
 * standard agent path; runtime paths that evaluate the predicate without a
 * context still pause (fail closed). Connectors without dry-run support
 * deny.
 */
export const DRY_RUN_CONTEXT_KEY = 'breakwater.dryRun';

/** Policy denial raised before a connector side effect is allowed to run. */
export class ConnectorPolicyError extends Error {
  /** Connector ID associated with the denial. */
  readonly connector: string;
  /** Name of the policy that denied the call. */
  readonly policy: string;
  /** Policy-supplied denial reason. */
  readonly reason: string;

  constructor(connector: string, policy: string, reason: string) {
    super(`connector ${connector} denied by ${policy}: ${reason}`);
    this.name = 'ConnectorPolicyError';
    this.connector = connector;
    this.policy = policy;
    this.reason = reason;
  }
}

const manifests = new WeakMap<object, PermissionManifest>();

/** Manifest a connector was created with (undefined for plain tools). */
export function connectorManifest(
  tool: object,
): PermissionManifest | undefined {
  return manifests.get(tool);
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function validSuspension(value: unknown): value is ConnectorApprovalSuspension {
  if (value === null || typeof value !== 'object') return false;
  const suspension = value as Record<string, unknown>;
  return (
    Array.isArray(suspension.stepPath) &&
    suspension.stepPath.length > 0 &&
    suspension.stepPath.every(nonEmptyString) &&
    typeof suspension.suspendedAt === 'number' &&
    Number.isFinite(suspension.suspendedAt) &&
    suspension.suspendedAt > 0 &&
    (suspension.resumeCount === undefined ||
      (typeof suspension.resumeCount === 'number' &&
        Number.isInteger(suspension.resumeCount) &&
        suspension.resumeCount > 0))
  );
}

function validGrant(value: unknown): value is ConnectorApprovalGrant {
  if (value === null || typeof value !== 'object') return false;
  const grant = value as Record<string, unknown>;
  if (
    !nonEmptyString(grant.connectorId) ||
    !nonEmptyString(grant.workflowId) ||
    !nonEmptyString(grant.runId) ||
    (grant.isolationScope !== undefined &&
      !nonEmptyString(grant.isolationScope))
  ) {
    return false;
  }
  if (grant.scope === 'run') {
    return grant.suspension === undefined && grant.toolCallId === undefined;
  }
  if (
    (grant.scope !== 'suspension' && grant.scope !== 'tool-call') ||
    !validSuspension(grant.suspension)
  ) {
    return false;
  }
  return grant.scope === 'tool-call'
    ? nonEmptyString(grant.toolCallId)
    : grant.toolCallId === undefined;
}

function validExecutionIdentity(
  value: unknown,
): value is ConnectorExecutionIdentity {
  if (value === null || typeof value !== 'object') return false;
  const identity = value as Record<string, unknown>;
  if (
    !nonEmptyString(identity.workflowId) ||
    !nonEmptyString(identity.runId) ||
    (identity.isolationScope !== undefined &&
      !nonEmptyString(identity.isolationScope))
  ) {
    return false;
  }
  if (identity.kind === 'start') return identity.suspension === undefined;
  return identity.kind === 'resume' && validSuspension(identity.suspension);
}

function sameSuspension(
  left: ConnectorApprovalSuspension,
  right: ConnectorApprovalSuspension,
): boolean {
  return (
    left.suspendedAt === right.suspendedAt &&
    left.resumeCount === right.resumeCount &&
    left.stepPath.length === right.stepPath.length &&
    left.stepPath.every((segment, index) => segment === right.stepPath[index])
  );
}

function grantForExecution(
  context: ToolExecutionContext,
  connectorId: string,
): ConnectorApprovalGrant | undefined {
  const requestContext = context.requestContext;
  if (requestContext?.get(LEGACY_CONNECTOR_GRANTS_CONTEXT_KEY) !== undefined) {
    return undefined;
  }
  const grants = requestContext?.get(CONNECTOR_GRANTS_CONTEXT_KEY);
  const identity = requestContext?.get(CONNECTOR_EXECUTION_CONTEXT_KEY);
  if (
    !Array.isArray(grants) ||
    !grants.every(validGrant) ||
    !validExecutionIdentity(identity)
  ) {
    return undefined;
  }

  const workflowScope = requestContext?.get(WORKFLOW_SCOPE_CONTEXT_KEY);
  const isolationScope = isolationScopeOf(requestContext);
  const requestRunId = requestContext?.get('runId');
  if (
    workflowScope !== identity.workflowId ||
    requestRunId !== identity.runId ||
    isolationScope !== identity.isolationScope ||
    (context.workflow !== undefined &&
      (context.workflow.workflowId !== identity.workflowId ||
        context.workflow.runId !== identity.runId))
  ) {
    return undefined;
  }

  return grants.find((grant) => {
    if (
      grant.connectorId !== connectorId ||
      grant.workflowId !== identity.workflowId ||
      grant.runId !== identity.runId ||
      grant.isolationScope !== identity.isolationScope
    ) {
      return false;
    }
    if (grant.scope === 'run') return true;
    if (
      identity.kind !== 'resume' ||
      !sameSuspension(grant.suspension, identity.suspension)
    ) {
      return false;
    }
    return (
      grant.scope === 'suspension' ||
      grant.toolCallId === context.agent?.toolCallId
    );
  });
}

// The `_background` model-override field (core LLMBackgroundOverride) smuggled
// into tool-call args. Presence alone is the smuggling signal — a foreground-
// only connector must never receive it, whatever its `enabled` value — so this
// is the field-presence check, stricter than the backgroundExecution
// evaluator's resolved-eligibility read (which permits enabled:false).
function hasBackgroundOverride(input: unknown): boolean {
  return (
    typeof input === 'object' &&
    input !== null &&
    LLM_BACKGROUND_OVERRIDE_KEY in input
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

// The caller's opaque isolation scope (multi-tenant hosts mint their tenant
// id; see ISOLATION_SCOPE_CONTEXT_KEY). Used as a KEY SEGMENT only — never
// parsed. Absent (or non-string) scope preserves the single-tenant keys
// exactly; deployments that must not run scope-less include the
// tenantIsolation evaluator in their policy set.
function isolationScopeOf(
  requestContext: RequestContext | undefined,
): string | undefined {
  const scope = requestContext?.get(ISOLATION_SCOPE_CONTEXT_KEY);
  return typeof scope === 'string' && scope.length > 0 ? scope : undefined;
}

// D5: an in-memory store loses its cross-isolate reach the moment a host
// mints an isolation scope — the budget/replay cache narrows to per-isolate
// (per RUN under DO-per-run routing), not per-tenant, with no error. Warn
// once per STORE INSTANCE (not per call, not per connector — the same store
// can back several connectors) the first time this combination is seen.
const warnedInMemoryStores = new WeakSet<object>();

function warnOnceForInMemoryStore(store: object, message: string): void {
  if (warnedInMemoryStores.has(store)) return;
  warnedInMemoryStores.add(store);
  console.warn(message);
}

const RATE_LIMIT_PATTERN = /^(\d+)\/(s|sec|second|m|min|minute|h|hour|d|day)$/;

const RATE_LIMIT_WINDOW_MS: Record<string, number> = {
  s: 1_000,
  sec: 1_000,
  second: 1_000,
  m: 60_000,
  min: 60_000,
  minute: 60_000,
  h: 3_600_000,
  hour: 3_600_000,
  d: 86_400_000,
  day: 86_400_000,
};

function parseRateLimit(
  connectorId: string,
  expr: string,
): { limit: number; windowMs: number } {
  const match = RATE_LIMIT_PATTERN.exec(expr);
  const limit = match ? Number(match[1]) : 0;
  const windowMs = match?.[2] ? RATE_LIMIT_WINDOW_MS[match[2]] : undefined;
  if (limit < 1 || windowMs === undefined) {
    throw new TypeError(
      `connector ${connectorId}: rate limit '${expr}' must be '<count>/<unit>' with count >= 1 and unit s|sec|second|m|min|minute|h|hour|d|day`,
    );
  }
  return { limit, windowMs };
}

/**
 * Compile a connector: a real Mastra createTool() call whose execute is
 * wrapped with the manifest's enforcement. `requiresApproval` (and the org
 * write policy) compile to Mastra's native `requireApproval` so agent runs
 * pause for approval before execute — but enforcement is the wrapper's
 * grant check, on every caller: an agent-shaped context is forwardable into
 * nested and direct calls, and the native approval outcome is not
 * observable at execute time, so approving a call means minting a structured
 * grant into the requestContext the resumed call executes under.
 */
export function createConnector<TInput = unknown, TOutput = unknown>(
  config: ConnectorConfig<TInput, TOutput>,
): Tool<TInput, TOutput> {
  const { id, policies = {} } = config;
  // One construction guard closes BOTH id-derived colon-joined store keys —
  // the idempotency scoped key and the rate-limit budget key — since both are
  // built from this single id (details in the thrown message).
  if (typeof id === 'string' && id.includes(':')) {
    throw new TypeError(
      `connector id '${id}' must not contain a colon: id is joined UNESCAPED with ':' into BOTH id-derived store keys (the idempotency scoped key '<id>:<key>' / '<isolationScope>:<id>:<key>', and the rate-limit budget key '<scope>:<id>'), so a colon in id can collide two distinct tuples onto one key on a shared store. Use a colon-free id (camelCase or dot-delimited).`,
    );
  }
  const manifest: PermissionManifest = Object.freeze({
    ...config.permissions,
    egress: Object.freeze([...(config.permissions.egress ?? [])]),
  });

  assertEgressHostList(
    manifest.egress ?? [],
    (entry) =>
      `connector ${id}: egress entry '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com')`,
  );
  // The runtime egress guard is call-invariant — declared egress and the base
  // fetch are frozen at construction — so build it ONCE. Each call wraps it
  // only to bind that call's requestContext for the denial audit.
  const baseEgressGuard = egressFetch(manifest.egress ?? [], {
    fetch: policies.fetch,
  });
  if (manifest.idempotencyKey && !policies.idempotencyStore) {
    throw new TypeError(
      `connector ${id}: permissions.idempotencyKey requires policies.idempotencyStore (InMemoryIdempotencyStore works for dev/tests)`,
    );
  }
  if (manifest.dryRun && !config.dryRunExecute) {
    throw new TypeError(
      `connector ${id}: permissions.dryRun requires config.dryRunExecute (the side-effect-free simulation)`,
    );
  }
  if (config.dryRunExecute && !manifest.dryRun) {
    throw new TypeError(
      `connector ${id}: config.dryRunExecute requires permissions.dryRun (the manifest must declare what the connector supports)`,
    );
  }
  // v1: only a read-only connector may opt into background execution (DL-005).
  // A write / destructive / idempotent connector carries a side effect whose
  // approval topology the background flip would move off the foreground path,
  // so opting one in is a construction-time error, not a runtime denial.
  if (manifest.background && manifest.sideEffect !== 'read') {
    throw new TypeError(
      `connector ${id}: permissions.background is only allowed on a read-only connector (sideEffect 'read'); a ${manifest.sideEffect}-class connector is foreground-only in v1`,
    );
  }
  const rateLimitSpec =
    manifest.rateLimit !== undefined
      ? parseRateLimit(id, manifest.rateLimit)
      : undefined;
  if (rateLimitSpec && !policies.rateLimitStore) {
    throw new TypeError(
      `connector ${id}: permissions.rateLimit requires policies.rateLimitStore (InMemoryRateLimitStore works for dev/tests)`,
    );
  }
  const rateLimit =
    rateLimitSpec && policies.rateLimitStore
      ? { ...rateLimitSpec, store: policies.rateLimitStore }
      : undefined;

  const audit = policies.audit;
  const gates: readonly ToolPolicyEvaluator[] = [
    ...(policies.networkEgress ? [networkEgress(policies.networkEgress)] : []),
    ...(policies.evaluators ?? []),
  ];
  const needsApproval = approvalRequired(
    id,
    manifest,
    policies.writePermissions,
  );
  const store = policies.idempotencyStore;
  // Concurrent calls sharing a key await the same attempt instead of both
  // executing on a store miss.
  const inflight = new Map<string, Promise<TOutput>>();

  function record(
    requestContext: RequestContext | undefined,
    decision: 'allowed' | 'denied' | 'error',
    extra: { reason?: string; detail?: Record<string, unknown> } = {},
    action = 'connector.execute',
  ): void {
    audit?.record({
      actor: actorFromRequestContext(requestContext) ?? null,
      action,
      resource: id,
      decision,
      reason: extra.reason,
      detail: { sideEffect: manifest.sideEffect, ...extra.detail },
    });
  }

  function deny(
    requestContext: RequestContext | undefined,
    policy: string,
    reason: string,
  ): never {
    record(requestContext, 'denied', {
      reason: `${policy}: ${reason}`,
      detail: { policy },
    });
    throw new ConnectorPolicyError(id, policy, reason);
  }

  // Single audit seam: every path that produces a successful result — a
  // fresh execute, a replayed/joined idempotent result, or a dry-run
  // simulation — routes through here to record the one 'allowed' audit
  // event, instead of duplicating that call at each site. There is no
  // post-execute policy stage today: retention and isolation
  // (crossWorkflowIsolation, tenantIsolation) are PRE-execute evaluators run
  // from the `gates` loop above, before execute — nothing currently gates
  // the result of a call.
  function finishAllowed(
    requestContext: RequestContext | undefined,
    result: TOutput,
    detail?: Record<string, unknown>,
  ): TOutput {
    record(requestContext, 'allowed', detail ? { detail } : {});
    return result;
  }

  // Errors an earlier audit stage already recorded (e.g. a rate-limit store
  // crash inside a keyed attempt): recordExecuteError must not re-record
  // them as 'execute threw' when they propagate out through the attempt.
  const auditedErrors = new WeakSet<object>();

  // The WeakSet can only hold objects, so a primitive throw from a custom
  // store is wrapped once (message preserved, original on `cause`) and the
  // WRAPPER is what propagates — otherwise audit-once breaks and the same
  // store crash records a second, misattributed 'execute threw' event.
  function markAudited(error: unknown): unknown {
    if (typeof error === 'object' && error !== null) {
      auditedErrors.add(error);
      return error;
    }
    const wrapped = new Error(errorMessage(error), { cause: error });
    auditedErrors.add(wrapped);
    return wrapped;
  }

  function recordExecuteError(
    requestContext: RequestContext | undefined,
    error: unknown,
    detail: Record<string, unknown> = {},
  ): void {
    // This connector's own policy denials (e.g. the rate-limit gate inside
    // a keyed attempt) were already audited by deny(); a second 'execute
    // threw' record would misattribute them to the connector's code. A
    // NESTED connector's denial still records here — that composite call
    // did fail in execute.
    if (error instanceof ConnectorPolicyError && error.connector === id) {
      return;
    }
    if (
      typeof error === 'object' &&
      error !== null &&
      auditedErrors.has(error)
    ) {
      return;
    }
    const safe = safeAuditErrorSummary(error);
    record(requestContext, 'error', {
      reason: safe?.reason ?? 'connector execution failed',
      detail: { stage: 'execute', ...safe?.detail, ...detail },
    });
  }

  // Budget counts ACTUAL executions: denied calls, cached replays, and
  // shared in-flight joins never consume it — hence an internal gate invoked
  // immediately before each config.execute call site (reserve-keyed,
  // legacy-keyed, plain), not a pre-execute evaluator. Audits exactly once:
  // a denial goes through deny(), and a store crash is recorded here as
  // 'rate-limit-store' and marked so recordExecuteError never re-records it
  // as 'execute threw' when it propagates out of a keyed attempt.
  // Fixed-window semantics: counts bucket into epoch-aligned windows, so a
  // burst may span two adjacent windows; simplest correct budget for a
  // per-connector cap.
  async function consumeRateLimit(
    requestContext: RequestContext | undefined,
  ): Promise<void> {
    if (!rateLimit) return;
    // Budget key segments by isolation scope: tenant A exhausting connector
    // `c` must not throttle tenant B. No scope => the connector id alone,
    // today's single-tenant key. The key only NAMES the budget — how far it
    // is shared is the store's reach: an in-memory store bounds the window
    // to this isolate (under DO-per-run routing, to this RUN), so a cap that
    // must hold across isolates needs a durable store (D1RateLimitStore).
    const scope = isolationScopeOf(requestContext);
    if (
      scope !== undefined &&
      rateLimit.store instanceof InMemoryRateLimitStore
    ) {
      warnOnceForInMemoryStore(
        rateLimit.store,
        `breakwater: connector '${id}' uses InMemoryRateLimitStore under isolation scope '${scope}' — the rate-limit budget becomes per-isolate, not per-tenant; use D1RateLimitStore on DO-per-run hosts.`,
      );
    }
    const budgetKey = scope === undefined ? id : `${scope}:${id}`;
    let count: number;
    try {
      count = await rateLimit.store.increment(
        budgetKey,
        rateLimit.windowMs,
        Date.now(),
      );
    } catch (error) {
      // Fail closed: an unbudgeted execution would break the declared cap.
      record(requestContext, 'error', {
        reason: 'rate-limit store increment failed',
        detail: { stage: 'rate-limit-store' },
      });
      throw markAudited(error);
    }
    if (count > rateLimit.limit) {
      deny(requestContext, 'rate-limit', `exceeded ${manifest.rateLimit}`);
    }
  }

  function recordStoreError(
    requestContext: RequestContext | undefined,
    op: 'get' | 'put' | 'reserve' | 'release',
    _error: unknown,
    key: string,
  ): void {
    record(requestContext, 'error', {
      reason: `idempotency store ${op} failed`,
      detail: { stage: 'idempotency-store', idempotencyKey: key },
    });
  }

  // Join a same-isolate in-flight attempt for the same key. Await before
  // recording — the shared attempt may still reject, and this call's audit
  // record must reflect its actual outcome.
  async function joinInflight(
    requestContext: RequestContext | undefined,
    pending: Promise<TOutput>,
    key: string,
  ): Promise<TOutput> {
    try {
      return finishAllowed(requestContext, await pending, {
        replayed: true,
        idempotencyKey: key,
      });
    } catch (error) {
      recordExecuteError(requestContext, error, {
        replayed: true,
        idempotencyKey: key,
      });
      throw error;
    }
  }

  // Shared lifecycle of a keyed call: same-isolate twins join the in-flight
  // attempt via a placeholder registered synchronously BEFORE the store
  // round-trip — with an async store the claimed row can become visible to
  // a twin before the claimer's own store promise resumes, so a
  // set-after-await would leave a window where the twin misreads the store
  // state and races (or denies) instead of joining. `probe` performs the
  // store round-trip and returns the outcome; this lifecycle owns every
  // settle path, so no exit can leave a joined twin hanging. Audit-once
  // applies to store errors and this connector's own denials (auditedErrors
  // / deny); an execute failure records once per caller — each call's audit
  // trail reflects its own outcome.
  async function keyedFlow(
    requestContext: RequestContext | undefined,
    scoped: string,
    key: string,
    probe: () => Promise<KeyedOutcome<TOutput>>,
  ): Promise<TOutput> {
    const pending = inflight.get(scoped);
    if (pending) return joinInflight(requestContext, pending, key);
    const settle = sharedOutcome<TOutput>();
    inflight.set(scoped, settle.promise);
    try {
      const outcome = await probe();
      if (outcome.kind === 'replay') {
        settle.resolve(outcome.result);
        return finishAllowed(requestContext, outcome.result, {
          replayed: true,
          idempotencyKey: key,
        });
      }
      settle.resolve(outcome.attempt);
      try {
        return finishAllowed(requestContext, await outcome.attempt, {
          idempotencyKey: key,
        });
      } catch (error) {
        recordExecuteError(requestContext, error, { idempotencyKey: key });
        throw error;
      }
    } catch (error) {
      // Probe throw (store crash, cross-isolate deny): joined twins must
      // settle too. A no-op after settle.resolve — first settle wins.
      settle.reject(error);
      throw error;
    } finally {
      inflight.delete(scoped);
    }
  }

  // Takes unknown because createTool's inference degrades over an abstract
  // PublicSchema<TInput>; Mastra's Tool wrapper validates inputData against
  // inputSchema before this runs, so the one cast at the user-execute
  // boundary is checked at runtime whenever a schema is declared.
  async function gatedExecute(
    inputData: unknown,
    context: ToolExecutionContext,
  ): Promise<TOutput> {
    const requestContext = context?.requestContext;
    const typedInput = inputData as TInput;

    // _background model-override defense (DL-005), FIRST — before the gates,
    // the dry-run branch, and any execute. A `_background` field in the args
    // asks the runtime to flip this call to background execution, a topology
    // change a foreground-only connector must never take. Reject its presence
    // outright unless the manifest opts in (read-only tools only, enforced at
    // construction). Same argv-flag-smuggling posture as agent-cli buildFlags;
    // fires for BOTH execute and dryRunExecute (both route through here).
    //
    // REACH (accuracy): this is DEFENSE-IN-DEPTH for direct / nested programmatic
    // calls, NOT the agent-path guard. On the AGENT path core deletes
    // `_background` from the tool-call args before dispatch (`delete args
    // ._background`), UNCONDITIONALLY — schema or not — so this presence check
    // sees clean args and fires on nothing there. What actually stops the model
    // backgrounding a call on the agent path is core's OWN `resolveBackgroundConfig`
    // baseEnabled gate: a breakwater connector sets no background config, so the
    // tool is ineligible and the override cannot enable it. This check's
    // independent teeth are direct / nested calls that hand args straight to
    // execute, bypassing core's agent dispatch (and its stripping) entirely. And
    // on EVERY path — including inside the background executor — the real write
    // boundary is the requestContext GRANT gate below, not this check. The
    // `backgroundExecution` tool-policy evaluator is the same defense-in-depth at
    // the gate loop.
    if (!manifest.background && hasBackgroundOverride(inputData)) {
      deny(
        requestContext,
        'background',
        `tool-call args carry a '${LLM_BACKGROUND_OVERRIDE_KEY}' override but this connector is foreground-only (the manifest does not opt into background execution)`,
      );
    }

    // The base egress guard is built once at construction; this per-call
    // wrapper binds only this call's requestContext so an egress denial
    // audits under it. The org allowlist already gated the DECLARED egress
    // list (the networkEgress evaluator below); the guard pins the
    // connector's ACTUAL requests to that list — redirect hops included. No
    // declared egress means the guard denies all network. The denial audit
    // fires here at the guard boundary, guaranteed even if the connector
    // swallows the ConnectorPolicyError (recordExecuteError early-returns on
    // this connector's own ConnectorPolicyError, so it is never re-recorded).
    const runtime: ConnectorRuntime = {
      fetch: async (input, init) => {
        try {
          return await baseEgressGuard(input, init);
        } catch (error) {
          if (error instanceof EgressDeniedError) {
            record(requestContext, 'denied', {
              reason: `egress-fetch: ${error.reason}`,
              detail: {
                policy: 'egress-fetch',
                host: error.host,
                hop: error.hop,
              },
            });
            throw new ConnectorPolicyError(id, 'egress-fetch', error.reason);
          }
          throw error;
        }
      },
    };

    if (gates.length > 0) {
      const toolCall: ToolCallContext = {
        connectorId: id,
        sideEffect: manifest.sideEffect,
        egress: manifest.egress ?? [],
        input: inputData,
        requestContext,
      };
      for (const gate of gates) {
        let decision: PolicyDecision;
        try {
          decision = await gate.evaluate(toolCall);
        } catch (error) {
          // Mirror PolicyEngine: an evaluator crash must not leave less
          // audit evidence than a denial. Record, then fail closed.
          record(requestContext, 'error', {
            reason: `${gate.name} evaluator failed`,
            detail: { policy: gate.name },
          });
          throw error;
        }
        if (!decision.allowed) {
          deny(requestContext, gate.name, decision.reason);
        }
      }
    }

    if (requestContext?.get(DRY_RUN_CONTEXT_KEY) === true) {
      // The caller asked for a simulation; an unsupported manifest fails
      // closed — executing for real would violate the caller's intent. This
      // branches BEFORE the approval gate on the documented no-side-effect
      // contract of dryRunExecute: there is no side effect to approve, no
      // budget to spend, and no replay store to poison. The pre-execute
      // gates above still applied.
      const simulate = config.dryRunExecute;
      if (!manifest.dryRun || !simulate) {
        deny(requestContext, 'dry-run', 'connector does not support dry-run');
      }
      try {
        return finishAllowed(
          requestContext,
          await simulate(typedInput, context, runtime),
          { dryRun: true },
        );
      } catch (error) {
        recordExecuteError(requestContext, error, { dryRun: true });
        throw error;
      }
    }

    // The grant is the only approval token, on every path. Mastra's native
    // requireApproval (compiled below) pauses agent runs for a decision and
    // an unapproved resume never reaches execute — but the approved outcome
    // is not observable here (the runtime strips the pure {approved} resume
    // before invoking the tool), and an agent-shaped context can be
    // forwarded into nested or direct calls. So context.agent is no proof
    // of approval and does not bypass this gate: whatever approves the call
    // must mint the grant into the requestContext the resumed call runs
    // under (the Phase 3 approval API's job).
    if (needsApproval) {
      const grant = grantForExecution(context, id);
      if (!grant) {
        deny(
          requestContext,
          'write-permissions',
          'approval required and no matching structured grant was found',
        );
      }
      record(
        requestContext,
        'allowed',
        {
          reason: 'structured approval grant matched',
          detail: {
            policy: 'write-permissions',
            grantScope: grant.scope,
            workflowId: grant.workflowId,
            runId: grant.runId,
            ...(grant.scope === 'run'
              ? {}
              : {
                  stepPath: [...grant.suspension.stepPath],
                  suspendedAt: grant.suspension.suspendedAt,
                  resumeCount: grant.suspension.resumeCount,
                }),
            ...(grant.scope === 'tool-call'
              ? { toolCallId: grant.toolCallId }
              : {}),
          },
        },
        'connector.approval',
      );
    }

    if (manifest.idempotencyKey && store) {
      const key = requestContext?.get(IDEMPOTENCY_KEY_CONTEXT_KEY);
      if (typeof key !== 'string' || key === '') {
        deny(
          requestContext,
          'idempotency',
          `manifest requires an idempotency key; set requestContext '${IDEMPOTENCY_KEY_CONTEXT_KEY}'`,
        );
      }
      // Replay-cache key segments by isolation scope: metamind's canonical
      // key is CROSS-RUN business identity ("never email this lead twice"),
      // which two tenants can legitimately share — without the scope segment
      // tenant B's send would replay tenant A's cached result object
      // (confidentiality AND availability). No scope => today's key.
      const isolationScope = isolationScopeOf(requestContext);
      if (
        isolationScope !== undefined &&
        store instanceof InMemoryIdempotencyStore
      ) {
        warnOnceForInMemoryStore(
          store,
          `breakwater: connector '${id}' uses InMemoryIdempotencyStore under isolation scope '${isolationScope}' — replay protection becomes per-isolate, not per-tenant; use D1IdempotencyStore on DO-per-run hosts.`,
        );
      }
      const scoped =
        isolationScope === undefined
          ? `${id}:${key}`
          : `${isolationScope}:${id}:${key}`;
      if (isAtomicStore(store)) {
        return keyedFlow(requestContext, scoped, key, async () => {
          // Atomic claim — exactly one isolate wins a key.
          let reservation: IdempotencyReservation;
          try {
            reservation = await store.reserve(scoped);
          } catch (error) {
            // Fail closed: nothing has executed yet, so failing the call is
            // safe and preserves replay protection for the retry. Marked
            // audited: joined twins rethrow it via joinInflight, and
            // recordExecuteError must not re-record it as 'execute threw'.
            recordStoreError(requestContext, 'reserve', error, key);
            throw markAudited(error);
          }
          if (reservation.state === 'replay') {
            return {
              kind: 'replay',
              result: reservation.record.result as TOutput,
            };
          }
          if (reservation.state === 'pending') {
            // Always cross-isolate (a same-isolate twin joins keyedFlow's
            // placeholder and never probes reserve): a promise cannot be
            // shared across isolates, so deny honestly — the retry replays
            // the winner's stored result.
            deny(
              requestContext,
              'idempotency',
              'another execution for this key is in progress; retry to replay its result',
            );
          }
          if (reservation.tookOver) {
            // D2: a dedicated signal, separate from this call's own outcome
            // record below — the previous holder may only have been slow,
            // not dead, if pendingTtlMs was set too low relative to the real
            // execute duration (agent-cli's definition-time guard checks
            // this; other connectors must size the store's TTL themselves).
            record(requestContext, 'allowed', {
              reason:
                'stale-pending idempotency reservation taken over; the previous holder may still be executing',
              detail: { idempotencyKey: key, tookOver: true },
            });
          }
          // The reservation lease — put()/release() CAS on it so this holder
          // can only finalize or drop the row it still owns (audit D2).
          // Captured as a const OUTSIDE the attempt closure: narrowing on the
          // `let reservation` does not persist into the closure body.
          const { token } = reservation;
          // Reserved: consume the rate budget, execute, then put()
          // finalizes the record. Any throw before put releases the
          // reservation — failures are never cached, the key stays
          // retryable. The consume stays INSIDE the attempt so denied
          // calls, replays, and joins never spend budget; single-audit of
          // rate failures is handled by deny()/auditedErrors.
          // Accepted ordering quirk (audit D5, deliberate): reserve() runs
          // BEFORE the rate-limit check, so a concurrent cross-isolate call
          // for this same key that arrives while THIS attempt is later
          // denied by the rate limit (and its reservation released) sees
          // 'pending' and is denied 'idempotency' rather than 'rate-limit' —
          // a transient misattribution in the audit reason, self-correcting
          // on retry, with no duplicated side effect. Not fixed.
          const attempt = (async () => {
            try {
              await consumeRateLimit(requestContext);
              const result = await config.execute(typedInput, context, runtime);
              try {
                await store.put(scoped, { result }, token);
              } catch (error) {
                // The side effect already succeeded; failing the call now
                // would invite a retry that re-executes it — the exact
                // duplication this store exists to prevent. Deliver the
                // result and surface the degraded replay protection in the
                // audit log. The reservation is deliberately NOT released: a
                // pending row blocks duplicates until the stale-pending TTL,
                // safer than inviting an immediate re-execute.
                recordStoreError(requestContext, 'put', error, key);
              }
              return result;
            } catch (error) {
              try {
                await store.release(scoped, token);
              } catch (releaseError) {
                // Best effort: an unreleased reservation is recovered by the
                // store's stale-pending takeover.
                recordStoreError(requestContext, 'release', releaseError, key);
              }
              throw error;
            }
          })();
          return { kind: 'attempt', attempt };
        });
      }
      // Legacy get/put store: same-isolate protection only (join → get →
      // attempt). Durable cross-isolate stores implement
      // AtomicIdempotencyStore and take the reserve path above.
      return keyedFlow(requestContext, scoped, key, async () => {
        let cached: IdempotencyRecord | undefined;
        try {
          cached = await store.get(scoped);
        } catch (error) {
          // Fail closed: nothing has executed yet, so failing the call is
          // safe and preserves replay protection for the retry. Marked
          // audited so joined twins do not re-record it (see reserve probe).
          recordStoreError(requestContext, 'get', error, key);
          throw markAudited(error);
        }
        if (cached) {
          return { kind: 'replay', result: cached.result as TOutput };
        }
        const attempt = (async () => {
          // Consume inside the attempt — see the reserve-probe note above.
          await consumeRateLimit(requestContext);
          const result = await config.execute(typedInput, context, runtime);
          // Only successful results are replayable — a thrown execute must
          // stay retryable under the same key.
          try {
            await store.put(scoped, { result });
          } catch (error) {
            // The side effect already succeeded; failing the call now would
            // invite a retry that re-executes it — the exact duplication this
            // store exists to prevent. Deliver the result and surface the
            // degraded replay protection in the audit log.
            recordStoreError(requestContext, 'put', error, key);
          }
          return result;
        })();
        return { kind: 'attempt', attempt };
      });
    }

    await consumeRateLimit(requestContext);
    try {
      return finishAllowed(
        requestContext,
        await config.execute(typedInput, context, runtime),
      );
    } catch (error) {
      recordExecuteError(requestContext, error);
      throw error;
    }
  }

  // createTool's generics key off concrete schema types; abstracting over
  // PublicSchema<TInput> degrades its inference, so pin the honest public
  // shape explicitly.
  const tool = createTool({
    id,
    description: config.description,
    inputSchema: config.inputSchema,
    outputSchema: config.outputSchema,
    // Compiled as a per-call predicate: Mastra resolves approval BEFORE
    // execute, so a static `true` would pause an agent's dry-run request
    // that the wrapper's dry-run branch never lets reach a side effect
    // (dryRunExecute or fail-closed denial). ctx is the plain-object view
    // of requestContext on the standard agent path; runtime paths that
    // omit ctx fall back to requiring approval — fail closed.
    requireApproval: needsApproval
      ? (
          _input: unknown,
          ctx?: { requestContext?: Record<string, unknown> },
        ): boolean => ctx?.requestContext?.[DRY_RUN_CONTEXT_KEY] !== true
      : undefined,
    // Truthful MCP annotations compiled from the manifest — descriptive
    // metadata for MCP clients; enforcement stays in the wrapper above.
    mcp: {
      annotations: {
        readOnlyHint: manifest.sideEffect === 'read',
        destructiveHint: manifest.sideEffect === 'destructive',
        idempotentHint:
          manifest.sideEffect === 'idempotent' ||
          manifest.idempotencyKey === true,
        openWorldHint: (manifest.egress?.length ?? 0) > 0,
      },
    },
    execute: gatedExecute,
  }) as unknown as Tool<TInput, TOutput>;

  manifests.set(tool, manifest);
  return tool;
}

export type {
  D1IdempotencyStoreOptions,
  IdempotencyDatabase,
  IdempotencyStatement,
} from './d1-idempotency-store.js';
// Durable D1-backed stores (kept in their own modules; only type imports
// flow back into this one, so there is no runtime cycle).
export { D1IdempotencyStore } from './d1-idempotency-store.js';
export type {
  D1RateLimitStoreOptions,
  RateLimitDatabase,
  RateLimitStatement,
} from './d1-rate-limit-store.js';
export { D1RateLimitStore } from './d1-rate-limit-store.js';
export type {
  EgressDenial,
  EgressFetchBase,
  EgressFetchOptions,
  EgressGuardedFetch,
  EgressRequestInit,
  EgressResponse,
  EgressResponseHeaders,
} from './egress-fetch.js';
// Fetch-level egress enforcement (own module; createConnector wires it per
// call as ConnectorRuntime.fetch, but it also works standalone).
export { EgressDeniedError, egressFetch } from './egress-fetch.js';
