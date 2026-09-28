// SPDX-License-Identifier: Apache-2.0
// Tool-boundary policies — evaluated inside the connector SDK's execute
// wrapper, not the agent processor chain. Mastra's processor seam only wraps
// the agent loop; tools invoked from workflow steps (createStep(tool)) or
// called directly never pass through it. Caller-independent gates therefore
// run here, against what a connector *declares* in its permission manifest,
// immediately before its execute runs.
// See docs/policy-engine-design.md.

import type { RequestContext } from '@mastra/core/request-context';
import type { ConnectorDenialMetadata } from '../connector-decision.js';
import {
  assertKnownFields,
  describeEntry,
  readFrozenList,
} from '../host-input.js';

/**
 * Decision shape a policy evaluator returns. Defined here — the leaf module —
 * so a seam that uses it imports no other seam for it.
 */
export type PolicyDecision =
  | {
      /** Allow the operation. */
      allowed: true;
    }
  | ({
      /** Deny the operation. */
      allowed: false;
      /** Human-readable denial reason suitable for audit records. */
      reason: string;
    } & (ConnectorDenialMetadata | { code?: undefined; details?: undefined }));

/** Side-effect classification a connector declares in its manifest. */
export type SideEffect = 'read' | 'write' | 'destructive' | 'idempotent';

/** @internal The members of {@link SideEffect}, as a refusal message names them. */
export const SIDE_EFFECT_MEMBERS =
  "'read', 'write', 'destructive' or 'idempotent'";

/** @internal Whether a value is one of the {@link SideEffect} members. */
export function isSideEffect(value: unknown): value is SideEffect {
  return (
    value === 'read' ||
    value === 'write' ||
    value === 'destructive' ||
    value === 'idempotent'
  );
}

/** One connector call, as seen by tool-boundary policies. */
export interface ToolCallContext {
  /** Connector id declared by the tool. */
  connectorId: string;
  /** Side-effect classification declared by the connector. */
  sideEffect: SideEffect;
  /** Hostnames the connector's manifest declares it calls. */
  egress: readonly string[];
  /** Validated connector input. */
  input: unknown;
  /** Trusted per-call context supplied by the host, when available. */
  requestContext?: RequestContext;
}

/** Evaluates one policy at the connector execution boundary. */
export interface ToolPolicyEvaluator {
  /** Diagnostic policy name used in denials and audit records. */
  name: string;
  /** Return whether this connector call may proceed. */
  evaluate(context: ToolCallContext): PolicyDecision | Promise<PolicyDecision>;
}

/** Configuration for {@link networkEgress}. */
export interface NetworkEgressOptions {
  /**
   * Hostnames connectors may call: exact entries ('api.openai.com') or
   * leading wildcards ('*.googleapis.com' — subdomains only, not the apex).
   * An empty list denies all declared egress; there is no allow-all entry —
   * omit the policy instead. Malformed entries throw at construction.
   */
  allowedDomains: readonly string[];
  /** Policy name used in denials and audit records. */
  name?: string;
}

// Bare hostname or leading '*.' wildcard. A malformed entry could never match
// and would read as a silent permanent deny or as a dead allowlist line an
// admin believes is live, so it fails fast instead.
export const EGRESS_HOSTNAME_PATTERN = /^(\*\.)?[a-z0-9][a-z0-9.-]*$/i;

/**
 * 'API.x.com.' and 'api.x.com' are the same DNS name: lowercase, then strip a
 * single trailing dot.
 */
export function normalizeDomain(domain: string): string {
  return domain.toLowerCase().replace(/\.$/, '');
}

/**
 * Lower-level host match: exact hostname or a leading-'*.' wildcard on a label
 * boundary (apex excluded). PRECONDITION: `domain` and every entry in
 * `allowed` are ALREADY normalized (see normalizeDomain), so a caller can
 * normalize a fixed allowlist once instead of on every match.
 */
export function domainAllowed(
  domain: string,
  allowed: readonly string[],
): boolean {
  for (const entry of allowed) {
    if (entry.startsWith('*.')) {
      // Keeping the leading dot in the suffix holds the label boundary:
      // '*.example.com' must not match 'evil-example.com', and the apex
      // 'example.com' stays excluded (declare it separately).
      const suffix = entry.slice(1);
      if (domain.length > suffix.length && domain.endsWith(suffix)) {
        return true;
      }
    } else if (domain === entry) {
      return true;
    }
  }
  return false;
}

/**
 * One-shot public wrapper: normalizes `domain` and each string entry in
 * `allowedDomains` (case/trailing-dot) then delegates to `domainAllowed` for
 * the match, for a single normalized comparison without keeping a normalized
 * allowlist. A `domain` or entry that is not a string matching
 * EGRESS_HOSTNAME_PATTERN, or a list that is not an array, matches nothing:
 * this wrapper refuses no list, an object could answer the matcher's string
 * methods itself, and an empty host would otherwise match an empty entry. The
 * list is read by index into a fresh array, so a method of the caller's
 * container cannot answer for it.
 */
export function egressDomainAllowed(
  domain: string,
  allowedDomains: readonly string[],
): boolean {
  if (
    typeof domain !== 'string' ||
    !EGRESS_HOSTNAME_PATTERN.test(domain) ||
    !Array.isArray(allowedDomains)
  ) {
    return false;
  }
  const length = allowedDomains.length;
  const normalizedAllowed: string[] = [];
  for (let index = 0; index < length; index += 1) {
    const entry: unknown = allowedDomains[index];
    if (typeof entry === 'string' && EGRESS_HOSTNAME_PATTERN.test(entry)) {
      normalizedAllowed.push(normalizeDomain(entry));
    }
  }
  return domainAllowed(normalizeDomain(domain), normalizedAllowed);
}

/**
 * Read an egress host list once into a frozen snapshot, validate it against
 * EGRESS_HOSTNAME_PATTERN, and return it. A caller normalizes the snapshot,
 * never its input, so a list whose reads change cannot pass with one entry
 * and register another. A string container would be read by index as
 * one-character hosts, and a non-string entry could pass the pattern through
 * its `toString` and then answer the matcher's string methods itself, so both
 * are refused without coercion. `subject` names the caller's field; `describe`
 * builds each call site's exact message for a string the pattern refuses.
 */
export function assertEgressHostList(
  subject: string,
  hosts: unknown,
  describe: (entry: string) => string,
): readonly string[] {
  if (!Array.isArray(hosts)) {
    throw new TypeError(`${subject} must be an array`);
  }
  const length = hosts.length;
  const snapshot: string[] = [];
  for (let index = 0; index < length; index += 1) {
    const entry: unknown = hosts[index];
    if (typeof entry !== 'string') {
      throw new TypeError(
        `${subject} entry ${index} must be a string (got ${entry === null ? 'null' : typeof entry})`,
      );
    }
    if (!EGRESS_HOSTNAME_PATTERN.test(entry)) {
      throw new TypeError(describe(entry));
    }
    snapshot.push(entry);
  }
  return Object.freeze(snapshot);
}

/**
 * Deny when the connector declares egress to a domain outside the allowlist.
 *
 * Enforcement is declaration-based: it gates the egress surface the manifest
 * claims, guarding against misconfiguration and org-policy drift — not
 * against a connector that lies about what it calls. The runtime half is
 * `egressFetch` (connector SDK).
 */
export function networkEgress(
  options: NetworkEgressOptions,
): ToolPolicyEvaluator {
  const allowedDomains = assertEgressHostList(
    'networkEgress: allowedDomains',
    options.allowedDomains,
    (entry) =>
      `networkEgress: allowed domain '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com'); there is no allow-all entry — omit the policy instead`,
  );
  // Normalize the allowlist ONCE at construction, so a call normalizes only
  // its declared hosts.
  const normalizedAllow = allowedDomains.map(normalizeDomain);
  return {
    name: options.name ?? 'network-egress',
    evaluate({ egress }): PolicyDecision {
      for (const declared of egress) {
        const normalizedDeclared = normalizeDomain(declared);
        if (!domainAllowed(normalizedDeclared, normalizedAllow)) {
          return {
            allowed: false,
            reason: `egress to ${normalizedDeclared} is not in the allowed domains`,
            code: 'EGRESS_HOST_NOT_ALLOWED_BY_ORG',
            details: { declaredHost: normalizedDeclared },
          };
        }
      }
      return { allowed: true };
    },
  };
}

/**
 * requestContext key: the calling workflow's scope (its workflowId). Minted
 * by the trusted runtime (flowsafe's RunnerRuntime) — trust boundary 6
 * applies: never populate it from client input, model output, or tool
 * results (security-threat-model.md).
 */
export const WORKFLOW_SCOPE_CONTEXT_KEY = 'breakwater.workflowScope';

/** Configuration for {@link crossWorkflowIsolation}. */
export interface CrossWorkflowIsolationOptions {
  /**
   * Extract the workflow scope this call TARGETS (connector-specific — e.g.
   * an input field naming a workflowId). undefined = the call does not
   * address workflow state and passes untouched.
   */
  targetScopeOf: (call: ToolCallContext) => string | undefined;
  /** Policy name used in denials and audit records. */
  name?: string;
}

/**
 * Deny a connector call that addresses another workflow's state. The
 * caller's scope comes from WORKFLOW_SCOPE_CONTEXT_KEY (runtime-minted); the
 * target scope comes from the connector-specific extractor. Fail closed: a
 * call that targets workflow state without a minted caller scope is denied.
 * Register through ConnectorPolicies.evaluators.
 */
export function crossWorkflowIsolation(
  options: CrossWorkflowIsolationOptions,
): ToolPolicyEvaluator {
  return {
    name: options.name ?? 'cross-workflow-isolation',
    evaluate(call): PolicyDecision {
      const target = options.targetScopeOf(call);
      if (target === undefined) return { allowed: true };
      const scope = call.requestContext?.get(WORKFLOW_SCOPE_CONTEXT_KEY);
      if (typeof scope !== 'string') {
        return {
          allowed: false,
          reason: 'caller has no workflow scope; cross-workflow access denied',
          code: 'WORKFLOW_SCOPE_MISSING',
        };
      }
      if (target !== scope) {
        return {
          allowed: false,
          reason: `workflow '${scope}' may not access state of '${target}'`,
          code: 'CROSS_WORKFLOW_ACCESS_DENIED',
        };
      }
      return { allowed: true };
    },
  };
}

/**
 * requestContext key: the caller's isolation scope, an opaque non-empty string
 * (a multi-tenant host mints its tenant id here); breakwater never parses the
 * value. A connector denies a present value of any other kind with
 * `ISOLATION_SCOPE_INVALID`, since reading it as no scope would share one
 * tenant's replay cache, budget and grants with another. Minted by the trusted
 * runtime — trust boundary 6 applies: never populate it from client input,
 * model output, or tool results.
 */
export const ISOLATION_SCOPE_CONTEXT_KEY = 'breakwater.isolationScope';

const tenantIsolationEvaluators = new WeakSet<object>();

/**
 * Deny any call whose requestContext carries NO isolation scope. Deployments
 * that segment budgets/replay caches by tenant include this in their policy
 * set, turning "the scope is absent" from silently-shared-keys into a denial.
 * It runs in the PRE-EXECUTE gates loop — which matters because the dry-run
 * branch returns before the idempotency and rate-limit machinery, and a
 * constraint that must bind simulations cannot live on those paths. Without
 * this evaluator, a call with no scope uses the unscoped keys.
 */
export function tenantIsolation(
  options: { name?: string } = {},
): ToolPolicyEvaluator {
  const evaluator: ToolPolicyEvaluator = {
    name: options.name ?? 'tenant-isolation',
    evaluate(call): PolicyDecision {
      const scope = call.requestContext?.get(ISOLATION_SCOPE_CONTEXT_KEY);
      if (typeof scope !== 'string' || scope.length === 0) {
        return {
          allowed: false,
          reason:
            'caller carries no isolation scope; this deployment requires tenant-scoped connector calls',
          code: 'ISOLATION_SCOPE_MISSING',
        };
      }
      return { allowed: true };
    },
  };
  tenantIsolationEvaluators.add(evaluator);
  return evaluator;
}

/** @internal Identify the built-in evaluator even when it has a custom name. */
export function isTenantIsolationEvaluator(
  evaluator: ToolPolicyEvaluator,
): boolean {
  return tenantIsolationEvaluators.has(evaluator);
}

/**
 * The field the LLM can include in tool-call args to override background
 * behavior per call (core `LLMBackgroundOverride`,
 * background-tasks/types.d.ts). Defined here so the name the model would
 * smuggle lives in one place.
 */
export const LLM_BACKGROUND_OVERRIDE_KEY = '_background';

/**
 * @internal Whether tool-call arguments carry the `_background` key, whatever
 * its value, `undefined` included. The connector wrapper and
 * {@link backgroundExecution} share it, so both decide on one presence test.
 */
export function hasBackgroundOverride(input: unknown): boolean {
  return (
    typeof input === 'object' &&
    input !== null &&
    LLM_BACKGROUND_OVERRIDE_KEY in input
  );
}

/** Configuration for {@link backgroundExecution}. */
export interface BackgroundExecutionOptions {
  /**
   * Side effects treated as write-class — background execution denied for
   * these. A write-class connector carries a side effect whose approval
   * topology and timing the background flip would move off the foreground
   * path, so v1 keeps them foreground-only. Default: everything but 'read'.
   * A present list must be a non-empty array of {@link SideEffect} members;
   * construction copies it.
   */
  writeClass?: readonly SideEffect[];
  /** Policy name used in denials and audit records. */
  name?: string;
}

const DEFAULT_WRITE_CLASS: readonly SideEffect[] = Object.freeze([
  'write',
  'destructive',
  'idempotent',
]);

const backgroundExecutionEvaluators = new WeakSet<object>();

/**
 * Deny a write-class connector call whose arguments carry the LLM
 * `_background` key, whatever its value. This complements `createConnector`'s
 * hard `_background` argument rejection, which uses the same presence test.
 *
 * Mastra's standard agent loop removes a truthy `_background` from the
 * arguments before dispatch, so a call Mastra runs as a background task
 * carries no key and this evaluator cannot see it; the connector wrapper
 * refuses that call instead. Read-only calls pass. A call whose `sideEffect`
 * is not a {@link SideEffect} member is denied, since it cannot be shown to be
 * read-only. Register the evaluator through `ConnectorPolicies.evaluators`.
 */
export function backgroundExecution(
  options: BackgroundExecutionOptions = {},
): ToolPolicyEvaluator {
  const configuredWriteClass = options.writeClass;
  const writeClass =
    configuredWriteClass === undefined
      ? DEFAULT_WRITE_CLASS
      : readFrozenList(
          'backgroundExecution: writeClass',
          configuredWriteClass,
          (entry, index) => {
            if (!isSideEffect(entry)) {
              throw new TypeError(
                `backgroundExecution: writeClass entry ${index} must be ${SIDE_EFFECT_MEMBERS} (got ${describeEntry(entry)})`,
              );
            }
            return entry;
          },
          true,
        );
  const evaluator: ToolPolicyEvaluator = {
    name: options.name ?? 'background-execution',
    evaluate({ sideEffect, input, connectorId }): PolicyDecision {
      if (!isSideEffect(sideEffect)) {
        return {
          allowed: false,
          reason: `the call's side effect must be ${SIDE_EFFECT_MEMBERS} (got ${describeEntry(sideEffect)}), so background execution cannot be ruled out`,
          code: 'BACKGROUND_EXECUTION_DENIED',
        };
      }
      if (!writeClass.includes(sideEffect)) return { allowed: true };
      if (hasBackgroundOverride(input)) {
        return {
          allowed: false,
          reason: `write-class connector '${connectorId}' may not run in background: its arguments carry an LLM _background override (v1 connectors are foreground-only)`,
          code: 'BACKGROUND_EXECUTION_DENIED',
        };
      }
      return { allowed: true };
    },
  };
  backgroundExecutionEvaluators.add(evaluator);
  return evaluator;
}

/** @internal Identify the built-in evaluator even when it has a custom name. */
export function isBackgroundExecutionEvaluator(
  evaluator: ToolPolicyEvaluator,
): boolean {
  return backgroundExecutionEvaluators.has(evaluator);
}

/** Org-level approval policy for write-class connector calls. */
export interface WritePermissionsPolicy {
  /**
   * Connector-id globs ('salesforce.*') whose write-class calls
   * (write | destructive | idempotent) require approval. Each pattern follows
   * the connector-id character rule, and '*' matches any run of characters.
   */
  requireApproval?: readonly string[];
  /** Destructive connectors always require approval. Default true. */
  destructiveRequiresApproval?: boolean;
}

// Ids and approval patterns share one character rule. A pattern carrying a
// character no accepted id can contain, such as a trailing space or a
// zero-width character, would never match, and an approval pattern that never
// matches stops requiring approval without any error.
const CONNECTOR_ID_EXCLUDED_CHARACTER = /[:\s\p{Cc}\p{Cf}]/u;

/**
 * @internal Whether a value is a valid connector id or approval pattern: a
 * non-empty string with no ':', whitespace, or control or format character.
 */
export function isConnectorIdText(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    !CONNECTOR_ID_EXCLUDED_CHARACTER.test(value)
  );
}

const EMPTY_WRITE_PERMISSIONS: Readonly<WritePermissionsPolicy> = Object.freeze(
  {},
);

const WRITE_PERMISSIONS_POLICY_KEYS = {
  requireApproval: true,
  destructiveRequiresApproval: true,
} satisfies Record<keyof WritePermissionsPolicy, true>;

/**
 * @internal The `PermissionManifest` members. That interface is declared in
 * `connector-sdk/contracts.ts`, which imports this module, so the table cannot
 * name it here without an import cycle; `connector-sdk/egress-conformance.ts`
 * checks the table against the interface at compile time.
 */
export const PERMISSION_MANIFEST_KEYS = {
  sideEffect: true,
  egress: true,
  egressEnforcement: true,
  idempotencyKey: true,
  requiresApproval: true,
  dryRun: true,
  rateLimit: true,
  background: true,
  requiredPermissions: true,
} satisfies Record<string, true>;

/**
 * @internal The boolean `PermissionManifest` members. `connector-sdk/index.ts`
 * checks the table against the interface at compile time, for the reason
 * {@link PERMISSION_MANIFEST_KEYS} gives.
 */
export const MANIFEST_BOOLEAN_FIELD_SET = {
  idempotencyKey: true,
  requiresApproval: true,
  dryRun: true,
  background: true,
} satisfies Partial<Record<keyof typeof PERMISSION_MANIFEST_KEYS, true>>;

/**
 * @internal Refuse a boolean manifest member whose present value is not a
 * boolean: each is read for truthiness, so a value such as '' or 0 would
 * silently read as off. `subject` names the manifest in the refusal.
 */
export function assertManifestBooleans(
  subject: string,
  manifest: object,
): void {
  for (const field of Object.keys(MANIFEST_BOOLEAN_FIELD_SET)) {
    const value: unknown = (manifest as Record<string, unknown>)[field];
    if (value !== undefined && typeof value !== 'boolean') {
      throw new TypeError(
        `${subject}.${field} must be a boolean when provided (got ${describeEntry(value)})`,
      );
    }
  }
}

/**
 * @internal Validate a write-permissions policy and return a frozen copy built
 * from one read of each field. `subject` names the caller's field in refusals.
 */
export function snapshotWritePermissions(
  subject: string,
  policy: unknown,
): Readonly<WritePermissionsPolicy> {
  if (policy === undefined) return EMPTY_WRITE_PERMISSIONS;
  assertKnownFields(subject, policy, WRITE_PERMISSIONS_POLICY_KEYS);
  const fields = policy as Record<string, unknown>;
  const configuredPatterns = fields.requireApproval;
  const destructiveRequiresApproval = fields.destructiveRequiresApproval;
  const requireApproval =
    configuredPatterns === undefined
      ? undefined
      : readFrozenList(
          `${subject}.requireApproval`,
          configuredPatterns,
          (entry, index) => {
            if (!isConnectorIdText(entry)) {
              throw new TypeError(
                `${subject}.requireApproval entry ${index} must be a non-empty string without ':', whitespace, or a control or format character (got ${describeEntry(entry)})`,
              );
            }
            return entry;
          },
        );
  if (
    destructiveRequiresApproval !== undefined &&
    typeof destructiveRequiresApproval !== 'boolean'
  ) {
    throw new TypeError(
      `${subject}.destructiveRequiresApproval must be a boolean (got ${describeEntry(destructiveRequiresApproval)})`,
    );
  }
  return Object.freeze({
    ...(requireApproval === undefined ? {} : { requireApproval }),
    ...(destructiveRequiresApproval === undefined
      ? {}
      : { destructiveRequiresApproval }),
  });
}

// '*' is the only glob token; every other character matches literally. The
// `s` flag lets '*' span line terminators, which ids that approvalRequired()
// receives from callers other than createConnector() may contain.
function matchesConnectorId(pattern: string, connectorId: string): boolean {
  const source = pattern
    .split('*')
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('.*');
  return new RegExp(`^${source}$`, 's').test(connectorId);
}

/**
 * Whether a call to this connector needs human approval — the single source
 * of truth the connector SDK compiles into its approval enforcement. Throws a
 * `TypeError` for a non-string `connectorId`, a `manifest` field that is not a
 * `PermissionManifest` member, a boolean manifest member such as
 * `requiresApproval` whose present value is not a boolean, an unknown
 * `sideEffect` or a malformed `policy`, whatever the connector.
 */
export function approvalRequired(
  connectorId: string,
  manifest: { sideEffect: SideEffect; requiresApproval?: boolean },
  policy: WritePermissionsPolicy = {},
): boolean {
  if (typeof connectorId !== 'string') {
    throw new TypeError(
      `approvalRequired: connectorId must be a string (got ${describeEntry(connectorId)})`,
    );
  }
  // A full manifest is a valid argument, so the check reads every manifest
  // member as known, not only the members its parameter type declares.
  assertKnownFields(
    'approvalRequired: manifest',
    manifest,
    PERMISSION_MANIFEST_KEYS,
  );
  assertManifestBooleans('approvalRequired: manifest', manifest);
  const requiresApproval = manifest.requiresApproval;
  const sideEffect: unknown = manifest.sideEffect;
  if (!isSideEffect(sideEffect)) {
    throw new TypeError(
      `approvalRequired: manifest.sideEffect must be ${SIDE_EFFECT_MEMBERS} (got ${describeEntry(sideEffect)})`,
    );
  }
  const { requireApproval = [], destructiveRequiresApproval } =
    snapshotWritePermissions('approvalRequired: policy', policy);
  if (requiresApproval) return true;
  if (sideEffect === 'destructive' && destructiveRequiresApproval !== false) {
    return true;
  }
  return (
    sideEffect !== 'read' &&
    requireApproval.some((pattern) => matchesConnectorId(pattern, connectorId))
  );
}
