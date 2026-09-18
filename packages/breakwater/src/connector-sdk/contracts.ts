// SPDX-License-Identifier: Apache-2.0
// Connector contracts — the declarations a connector-sdk sibling needs without
// reaching the barrel that imports them back: manifest, store, policy and
// connector shapes that cross that edge.
//
// A type-only leaf, so the preset, the D1 stores, the key migration and the
// conformance harness take these declarations without importing the barrel.
// That need is the criterion for what moves here; a declaration the barrel
// alone consumes stays in the barrel.
//
// `AuditLogger` arrives from the `../audit/index.js` barrel rather than from a
// leaf because audit publishes it as a class. The edge is admissible while
// audit reaches nothing under `connector-sdk/`; the day it does, the cycle
// fails the build instead of passing silently.

import type { RequestContext } from '@mastra/core/request-context';
import type { PublicSchema } from '@mastra/core/schema';
import type { Tool, ToolExecutionContext } from '@mastra/core/tools';

import type { AuditLogger } from '../audit/index.js';
import type {
  NetworkEgressOptions,
  SideEffect,
  ToolPolicyEvaluator,
  WritePermissionsPolicy,
} from '../policy-engine/tool-policy.js';
import type { Permission } from '../rbac/permission.js';
import type { EgressFetchBase, EgressGuardedFetch } from './egress-fetch.js';

/**
 * Whether a connector's declared egress binds its actual traffic.
 *
 * The manifest field an author writes and the audit key an operator filters
 * on are both `egressEnforcement`; `connectorEgressPosture()` is the readback
 * of the value that field resolves to. The audit key keeps the field's name so
 * that a log query and a manifest use the same word for the same value, and
 * the readback keeps "posture" because it answers for a tool rather than for
 * a declaration.
 */
export type ConnectorEgressPosture = 'enforced' | 'declaration-only';

/** Permission manifest — what the connector declares about itself. */
export interface PermissionManifest {
  /** Worst side effect the connector can cause. */
  sideEffect: SideEffect;
  /** Hostnames this connector calls; gated by the networkEgress policy. */
  egress?: readonly string[];
  /**
   * Whether the declared `egress` binds the connector's actual traffic.
   * 'enforced' asserts every HTTP request leaves through
   * `ConnectorRuntime.fetch`. It covers a connector that issues no HTTP
   * request at all; it is a claim about HTTP traffic, not about platform
   * bindings (D1, KV, R2, service bindings), which the guard never sees.
   * 'declaration-only' states that a vendor SDK or child process carries its
   * own transport, so the list is checked against organization policy but not
   * against sockets. An omitted field resolves to 'declaration-only':
   * nothing has proven enforcement. `connectorEgressPosture()` reads the
   * resolved value.
   */
  egressEnforcement?: ConnectorEgressPosture;
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
  /**
   * Server-derived permissions required to invoke this connector, with
   * explicit ALL-OF semantics: the executing principal must hold every
   * listed identifier. Enforced against the trusted
   * `breakwater.principalPermissions` projection BEFORE the dry-run branch
   * and the approval-grant gate — authorization applies to simulations too,
   * and a valid approval must not elevate an unauthorized principal. A call
   * with no valid projection fails closed. Omission preserves the existing
   * approval/policy-only behavior; a present list must be non-empty.
   */
  requiredPermissions?: readonly Permission[];
}

/** Completed result stored for idempotent replay. */
export interface IdempotencyRecord {
  /** Connector result returned by future calls with the same scoped key. */
  result: unknown;
}

/**
 * Result storage keyed by a private, versioned composite key. Callers must
 * treat keys as opaque. The record wrapper distinguishes a stored undefined
 * result from a miss.
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

/** Non-mutating state returned by an inspectable idempotency store. */
export type IdempotencyInspection =
  | { state: 'absent' }
  | { state: 'pending' }
  | { state: 'replay'; record: IdempotencyRecord };

/**
 * Idempotency store that can distinguish an absent key from a pending claim
 * without reserving it. Atomic stores need this capability during the v1-to-v2
 * composite-key transition so legacy pending work cannot be mistaken for a
 * miss and executed again.
 */
export interface InspectableIdempotencyStore extends IdempotencyStore {
  /** Inspect a key without reserving, finalizing, or releasing it. */
  inspect(key: string): IdempotencyInspection | Promise<IdempotencyInspection>;
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
  /**
   * Explicit v2 composite-key rollout acknowledgement. Set only after every
   * legacy writer sharing the store has been stopped and drained and legacy
   * rows have been inventoried. Without it, an absent legacy key fails closed
   * instead of racing an old writer that could still create a v1 record.
   */
  idempotencyKeyMigration?: 'legacy-writers-drained';
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
  /**
   * Refuse, at construction, any connector whose resolved egress posture is not
   * 'enforced'. Pass it on a `createConnector()` call whose connector must not
   * rely on a boundary outside ConnectorRuntime.fetch.
   *
   * The construction gate reads this flag for truthiness and validates no
   * value: a falsy one leaves the posture unrequired, which is what a caller
   * passing `false` asks for. `singleTenantConnectorPolicies()` is stricter
   * because it parses its options object through a schema, where the same
   * field accepts the literal `true` alone.
   */
  requireEgressEnforcement?: true;
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
 * fetch/transport option), or declare that connector
 * `permissions.egressEnforcement: 'declaration-only'`, the posture
 * `connectorEgressPosture()` then reads back.
 */
export interface ConnectorRuntime {
  /** Fetch guarded by the connector manifest's declared egress hosts. */
  fetch: EgressGuardedFetch;
}

/** Definition compiled by `createConnector()` into an enforced Mastra tool. */
export interface ConnectorConfig<TInput = unknown, TOutput = unknown> {
  /**
   * Stable, colon-free connector identifier. The colon restriction keeps the
   * unchanged `[scope:]connector` rate-budget key injective.
   */
  id: string;
  /** Description presented to the model and tool consumers. */
  description: string;
  /** Optional schema that Mastra validates before connector policies run. */
  inputSchema?: PublicSchema<TInput>;
  /**
   * Optional schema Breakwater validates/transforms before replay commit;
   * Mastra consumes the captured Standard Schema result without rerunning it.
   */
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

/** Breakwater connector with the execution function guaranteed at construction. */
export type Connector<TInput = unknown, TOutput = unknown> = Tool<
  TInput,
  TOutput
> & {
  execute: NonNullable<Tool<TInput, TOutput>['execute']>;
};

/** Trusted host context accepted by {@link invokeConnector}. */
export interface ConnectorInvocationOptions {
  /** Request context carrying trusted policy, identity, and grant values. */
  requestContext?: RequestContext;
  /** Abort signal forwarded to the connector execution context. */
  abortSignal?: AbortSignal;
  /** Mastra observability helper, or the public no-op helper when omitted. */
  observe?: ToolExecutionContext['observe'];
  /** Exact Mastra tool-call identity used to match a tool-call approval grant. */
  toolCallId?: string;
}
