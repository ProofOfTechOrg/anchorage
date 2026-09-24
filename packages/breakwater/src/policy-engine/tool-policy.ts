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
 * allowlist. A non-string `domain` or entry, or a list that is not an array,
 * matches nothing: this wrapper validates no list, and an object could answer
 * the matcher's string methods itself. The list is read by index into a fresh
 * array, so a method of the caller's container cannot answer for it.
 */
export function egressDomainAllowed(
  domain: string,
  allowedDomains: readonly string[],
): boolean {
  if (typeof domain !== 'string' || !Array.isArray(allowedDomains)) {
    return false;
  }
  const length = allowedDomains.length;
  const normalizedAllowed: string[] = [];
  for (let index = 0; index < length; index += 1) {
    const entry: unknown = allowedDomains[index];
    if (typeof entry === 'string') {
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
 * requestContext key: the caller's OPAQUE isolation scope (a multi-tenant
 * host mints its tenant id here); breakwater never parses the value. Minted
 * by the trusted runtime — trust boundary 6 applies: never populate it from
 * client input, model output, or tool results.
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

/** The `_background` override shape, as seen at the tool boundary. */
interface LlmBackgroundOverride {
  enabled?: boolean;
  timeoutMs?: number;
  maxRetries?: number;
}

function backgroundOverrideOf(
  input: unknown,
): LlmBackgroundOverride | undefined {
  if (typeof input !== 'object' || input === null) return undefined;
  const value = (input as Record<string, unknown>)[LLM_BACKGROUND_OVERRIDE_KEY];
  if (typeof value !== 'object' || value === null) return undefined;
  return value as LlmBackgroundOverride;
}

/** Configuration for {@link backgroundExecution}. */
export interface BackgroundExecutionOptions {
  /**
   * Side effects treated as write-class — background execution denied for
   * these. A write-class connector carries a side effect whose approval
   * topology and timing the background flip would move off the foreground
   * path, so v1 keeps them foreground-only. Default: everything but 'read'.
   */
  writeClass?: readonly SideEffect[];
  /** Policy name used in denials and audit records. */
  name?: string;
}

const backgroundExecutionEvaluators = new WeakSet<object>();

/**
 * Deny a write-class connector call carrying an LLM `_background` override
 * that asks for background execution. This complements `createConnector`'s
 * hard `_background` argument rejection.
 *
 * This evaluator sees only the override present in the connector arguments.
 * Mastra removes `_background` before dispatching agent tool calls, so the
 * check primarily protects direct and nested programmatic calls. Breakwater
 * connectors do not enable Mastra background execution by default, which
 * prevents an agent override from opting them in upstream. Read-only calls and
 * explicit `{ enabled: false }` overrides pass. Register the evaluator through
 * `ConnectorPolicies.evaluators`.
 */
export function backgroundExecution(
  options: BackgroundExecutionOptions = {},
): ToolPolicyEvaluator {
  const writeClass = options.writeClass ?? [
    'write',
    'destructive',
    'idempotent',
  ];
  const evaluator: ToolPolicyEvaluator = {
    name: options.name ?? 'background-execution',
    evaluate({ sideEffect, input, connectorId }): PolicyDecision {
      if (!writeClass.includes(sideEffect)) return { allowed: true };
      const override = backgroundOverrideOf(input);
      if (override !== undefined && override.enabled !== false) {
        return {
          allowed: false,
          reason: `write-class connector '${connectorId}' may not run in background: an LLM _background override would move it off the foreground path (v1 connectors are foreground-only)`,
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
   * (write | destructive | idempotent) require approval.
   */
  requireApproval?: readonly string[];
  /** Destructive connectors always require approval. Default true. */
  destructiveRequiresApproval?: boolean;
}

// '*' is the only glob token; every other character matches literally.
function matchesConnectorId(pattern: string, connectorId: string): boolean {
  const source = pattern
    .split('*')
    .map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('.*');
  return new RegExp(`^${source}$`).test(connectorId);
}

/**
 * Whether a call to this connector needs human approval — the single source
 * of truth the connector SDK compiles into its approval enforcement.
 */
export function approvalRequired(
  connectorId: string,
  manifest: { sideEffect: SideEffect; requiresApproval?: boolean },
  policy: WritePermissionsPolicy = {},
): boolean {
  if (manifest.requiresApproval) return true;
  if (
    manifest.sideEffect === 'destructive' &&
    policy.destructiveRequiresApproval !== false
  ) {
    return true;
  }
  return (
    manifest.sideEffect !== 'read' &&
    (policy.requireApproval ?? []).some((pattern) =>
      matchesConnectorId(pattern, connectorId),
    )
  );
}
