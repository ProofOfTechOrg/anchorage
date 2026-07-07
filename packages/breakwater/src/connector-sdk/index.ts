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
import { createTool } from '@mastra/core/tools';
import type { Tool, ToolExecutionContext } from '@mastra/core/tools';

import {
  approvalRequired,
  EGRESS_HOSTNAME_PATTERN,
  networkEgress,
} from '../policy-engine/tool-policy.js';
import type {
  NetworkEgressOptions,
  PolicyDecision,
  SideEffect,
  ToolCallContext,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from '../policy-engine/tool-policy.js';
import type { AuditLogger } from '../audit/index.js';
import { actorFromRequestContext } from '../rbac/index.js';

/** Permission manifest — what the connector declares about itself. */
export interface PermissionManifest {
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
}

export interface IdempotencyRecord {
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
  get(
    key: string,
  ): IdempotencyRecord | undefined | Promise<IdempotencyRecord | undefined>;
  put(key: string, record: IdempotencyRecord): void | Promise<void>;
}

/**
 * Outcome of an atomic reserve():
 * - 'reserved' — this caller claimed the key: execute, then put();
 *   release() after a failed execute so the key stays retryable.
 * - 'replay'   — a completed execution's record: return it, do not execute.
 * - 'pending'  — another isolate is executing this key right now.
 */
export type IdempotencyReservation =
  | { state: 'reserved' }
  | { state: 'replay'; record: IdempotencyRecord }
  | { state: 'pending' };

/**
 * Idempotency store with an atomic claim — the shape durable, cross-isolate
 * implementations must take: reserve() is a compare-and-set, so two isolates
 * racing one key resolve to exactly one 'reserved' winner. The connector
 * wrapper prefers this path whenever a store implements it.
 */
export interface AtomicIdempotencyStore extends IdempotencyStore {
  reserve(
    key: string,
  ): IdempotencyReservation | Promise<IdempotencyReservation>;
  /** Drop a pending reservation after a failed execute — failures stay retryable. */
  release(key: string): void | Promise<void>;
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
  // Reservations live outside the LRU: a pending key is never evictable.
  #pending = new Set<string>();

  constructor(options: { maxEntries?: number } = {}) {
    this.#maxEntries = options.maxEntries ?? 1000;
  }

  get(key: string): IdempotencyRecord | undefined {
    return this.#entries.get(key);
  }

  reserve(key: string): IdempotencyReservation {
    const record = this.#entries.get(key);
    if (record) return { state: 'replay', record };
    if (this.#pending.has(key)) return { state: 'pending' };
    this.#pending.add(key);
    return { state: 'reserved' };
  }

  release(key: string): void {
    this.#pending.delete(key);
  }

  put(key: string, record: IdempotencyRecord): void {
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

  clear(): void {
    this.#entries.clear();
    this.#pending.clear();
  }
}

/**
 * Fixed-window rate-limit counters keyed by connector id. Implementations
 * back the manifest's `rateLimit` budget.
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
  networkEgress?: NetworkEgressOptions;
  writePermissions?: WritePermissionsPolicy;
  /**
   * Custom tool-boundary evaluators, run pre-execute after the built-in
   * network-egress gate, in registration order. The slot later policy
   * domains (e.g. retention/isolation pre-checks) plug into.
   */
  evaluators?: readonly ToolPolicyEvaluator[];
  idempotencyStore?: IdempotencyStore;
  /** Required when the manifest declares `rateLimit`. */
  rateLimitStore?: RateLimitStore;
  audit?: AuditLogger;
}

export interface ConnectorConfig<TInput = unknown, TOutput = unknown> {
  id: string;
  description: string;
  inputSchema?: PublicSchema<TInput>;
  outputSchema?: PublicSchema<TOutput>;
  execute: (
    inputData: TInput,
    context: ToolExecutionContext,
  ) => Promise<TOutput>;
  /**
   * Side-effect-free simulation of `execute`, returning the same output
   * shape. Required when `permissions.dryRun` is declared, forbidden
   * otherwise — the manifest must state what the connector supports.
   */
  dryRunExecute?: (
    inputData: TInput,
    context: ToolExecutionContext,
  ) => Promise<TOutput>;
  permissions: PermissionManifest;
  /** Omit for an ungated connector (classification + audit only). */
  policies?: ConnectorPolicies;
}

/**
 * requestContext key: readonly string[] of connector ids approved for this
 * request. A capability token — whoever can write this key can authorize
 * any write-class connector — so it must only ever be set by trusted
 * server-side code after an out-of-band approval, never derived from client
 * input, model output, or tool results. The flowsafe approval API (Phase 3)
 * mints these on resume and is part of the trusted computing base. See
 * docs/security-threat-model.md, trust boundary 6.
 */
export const APPROVED_CONNECTORS_CONTEXT_KEY = 'breakwater.approvedConnectors';

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

export class ConnectorPolicyError extends Error {
  readonly connector: string;
  readonly policy: string;
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

function approvalGranted(
  requestContext: RequestContext | undefined,
  connectorId: string,
): boolean {
  const value = requestContext?.get(APPROVED_CONNECTORS_CONTEXT_KEY);
  return Array.isArray(value) && value.includes(connectorId);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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
 * observable at execute time, so approving a call means minting the grant
 * (APPROVED_CONNECTORS_CONTEXT_KEY) into the requestContext the resumed
 * call executes under.
 */
export function createConnector<TInput = unknown, TOutput = unknown>(
  config: ConnectorConfig<TInput, TOutput>,
): Tool<TInput, TOutput> {
  const { id, policies = {} } = config;
  const manifest: PermissionManifest = Object.freeze({
    ...config.permissions,
    egress: Object.freeze([...(config.permissions.egress ?? [])]),
  });

  for (const entry of manifest.egress ?? []) {
    if (!EGRESS_HOSTNAME_PATTERN.test(entry)) {
      throw new TypeError(
        `connector ${id}: egress entry '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com')`,
      );
    }
  }
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
  ): void {
    audit?.record({
      actor: actorFromRequestContext(requestContext) ?? null,
      action: 'connector.execute',
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

  // Single success seam: every result a call returns passes through here,
  // so the post-execute policy stage (retention/isolation, later phases)
  // slots in at one point instead of four.
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

  // WeakSet holds objects only; a primitive throw simply stays re-recordable.
  function markAudited(error: unknown): void {
    if (typeof error === 'object' && error !== null) auditedErrors.add(error);
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
    record(requestContext, 'error', {
      reason: `execute threw: ${errorMessage(error)}`,
      detail: { stage: 'execute', ...detail },
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
    let count: number;
    try {
      count = await rateLimit.store.increment(
        id,
        rateLimit.windowMs,
        Date.now(),
      );
    } catch (error) {
      // Fail closed: an unbudgeted execution would break the declared cap.
      record(requestContext, 'error', {
        reason: `rate-limit store increment failed: ${errorMessage(error)}`,
        detail: { stage: 'rate-limit-store' },
      });
      markAudited(error);
      throw error;
    }
    if (count > rateLimit.limit) {
      deny(requestContext, 'rate-limit', `exceeded ${manifest.rateLimit}`);
    }
  }

  function recordStoreError(
    requestContext: RequestContext | undefined,
    op: 'get' | 'put' | 'reserve' | 'release',
    error: unknown,
    key: string,
  ): void {
    record(requestContext, 'error', {
      reason: `idempotency store ${op} failed: ${errorMessage(error)}`,
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
            reason: `${gate.name} threw: ${errorMessage(error)}`,
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
          await simulate(typedInput, context),
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
    if (needsApproval && !approvalGranted(requestContext, id)) {
      deny(
        requestContext,
        'write-permissions',
        'approval required and not granted for this request',
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
      const scoped = `${id}:${key}`;
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
            markAudited(error);
            throw error;
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
          // Reserved: consume the rate budget, execute, then put()
          // finalizes the record. Any throw before put releases the
          // reservation — failures are never cached, the key stays
          // retryable. The consume stays INSIDE the attempt so denied
          // calls, replays, and joins never spend budget; single-audit of
          // rate failures is handled by deny()/auditedErrors.
          const attempt = (async () => {
            try {
              await consumeRateLimit(requestContext);
              const result = await config.execute(typedInput, context);
              try {
                await store.put(scoped, { result });
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
                await store.release(scoped);
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
          markAudited(error);
          throw error;
        }
        if (cached) {
          return { kind: 'replay', result: cached.result as TOutput };
        }
        const attempt = (async () => {
          // Consume inside the attempt — see the reserve-probe note above.
          await consumeRateLimit(requestContext);
          const result = await config.execute(typedInput, context);
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
        await config.execute(typedInput, context),
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

// Durable D1-backed idempotency store (kept in its own module; only type
// imports flow back into this one, so there is no runtime cycle).
export { D1IdempotencyStore } from './d1-idempotency-store.js';
export type {
  D1IdempotencyStoreOptions,
  IdempotencyDatabase,
  IdempotencyStatement,
} from './d1-idempotency-store.js';
