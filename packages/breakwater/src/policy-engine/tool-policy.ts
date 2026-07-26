// SPDX-License-Identifier: Apache-2.0
// Tool-boundary policies — evaluated inside the connector SDK's execute
// wrapper, not the agent processor chain. Mastra's processor seam only wraps
// the agent loop; tools invoked from workflow steps (createStep(tool)) or
// called directly never pass through it. Caller-independent gates therefore
// run here, against what a connector *declares* in its permission manifest,
// immediately before its execute runs.
// See docs/policy-engine-design.md.

import type { RequestContext } from '@mastra/core/request-context';

/**
 * Decision shape shared by both policy seams (agent-boundary evaluators in
 * index.ts and the tool-boundary evaluators below). Defined here — the leaf
 * module — so neither seam imports the other for it.
 */
export type PolicyDecision =
  | {
      /** Allow the operation. */
      allowed: true;
    }
  | {
      /** Deny the operation. */
      allowed: false;
      /** Human-readable denial reason suitable for audit records. */
      reason: string;
    };

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
  /** Stable policy name used in denials and audit records. */
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

// Bare hostname or leading '*.' wildcard — no scheme, path, port, or space.
// Shared by the manifest side (connector egress declarations) and the org
// side (allowlist entries): a malformed entry on either side could never
// match and would read as a silent permanent deny (manifest) or a dead
// allowlist line an admin believes is live (org), so both fail fast instead.
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
 * `allowed` are ALREADY normalized (see normalizeDomain) — networkEgress and
 * egressFetch normalize their allowlist ONCE at construction and call this per
 * hop; egressDomainAllowed is the one-shot matcher that normalizes both sides
 * for external callers.
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
 * One-shot public wrapper: normalizes `domain` and every entry in
 * `allowedDomains` (case/trailing-dot) then delegates to `domainAllowed` for
 * the match. `networkEgress` and `egressFetch` call `domainAllowed` directly
 * instead, each normalizing its own allowlist ONCE at construction; this
 * wrapper is for barrel/external callers that just want a single normalized
 * comparison without owning that memoization.
 */
export function egressDomainAllowed(
  domain: string,
  allowedDomains: readonly string[],
): boolean {
  return domainAllowed(
    normalizeDomain(domain),
    allowedDomains.map(normalizeDomain),
  );
}

/**
 * Validate an egress host list against EGRESS_HOSTNAME_PATTERN, throwing a
 * TypeError for the first entry that is not a bare hostname or '*.' wildcard.
 * `describe` builds each call site's exact message — networkEgress,
 * egressFetch, and createConnector share one pattern but keep their own
 * wording.
 */
export function assertEgressHostList(
  hosts: readonly string[],
  describe: (entry: string) => string,
): void {
  for (const entry of hosts) {
    if (!EGRESS_HOSTNAME_PATTERN.test(entry)) {
      throw new TypeError(describe(entry));
    }
  }
}

/**
 * Deny when the connector declares egress to a domain outside the allowlist.
 *
 * Enforcement is declaration-based: it gates the egress surface the manifest
 * claims, guarding against misconfiguration and org-policy drift — not
 * against a connector that lies about what it calls. The runtime half is
 * `egressFetch` (connector SDK): every actual request a connector makes
 * through its `ConnectorRuntime.fetch` is checked against the manifest's
 * declared hosts, so actual ⊆ declared ⊆ this allowlist.
 */
export function networkEgress(
  options: NetworkEgressOptions,
): ToolPolicyEvaluator {
  assertEgressHostList(
    options.allowedDomains,
    (entry) =>
      `networkEgress: allowed domain '${entry}' must be a bare hostname ('api.example.com') or wildcard ('*.example.com'); there is no allow-all entry — omit the policy instead`,
  );
  // Normalize the allowlist ONCE at construction and match the incoming value
  // per call through the SAME lower-level matcher the runtime guard
  // (egressFetch) uses — declared and enforced semantics cannot drift, and no
  // allowlist re-normalization happens per call.
  const normalizedAllow = options.allowedDomains.map(normalizeDomain);
  return {
    name: options.name ?? 'network-egress',
    evaluate({ egress }): PolicyDecision {
      for (const declared of egress) {
        const normalizedDeclared = normalizeDomain(declared);
        if (!domainAllowed(normalizedDeclared, normalizedAllow)) {
          return {
            allowed: false,
            reason: `egress to ${normalizedDeclared} is not in the allowed domains`,
          };
        }
      }
      return { allowed: true };
    },
  };
}

/**
 * requestContext key: the calling workflow's scope (its workflowId). Minted
 * by the trusted runtime (flowsafe's RunnerRuntime) on every leg — trust
 * boundary 6 applies: never populate it from client input, model output, or
 * tool results (security-threat-model.md).
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
        };
      }
      if (target !== scope) {
        return {
          allowed: false,
          reason: `workflow '${scope}' may not access state of '${target}'`,
        };
      }
      return { allowed: true };
    },
  };
}

/**
 * requestContext key: the caller's OPAQUE isolation scope (a multi-tenant
 * host mints its tenant id here). breakwater never parses the value — it
 * segments the connector SDK's idempotency and rate-limit keys and feeds the
 * tenantIsolation evaluator. Minted by the trusted runtime on every leg,
 * mirroring WORKFLOW_SCOPE_CONTEXT_KEY — trust boundary 6 applies: never
 * populate it from client input, model output, or tool results.
 */
export const ISOLATION_SCOPE_CONTEXT_KEY = 'breakwater.isolationScope';

/**
 * Deny any call whose requestContext carries NO isolation scope. Deployments
 * that segment budgets/replay caches by tenant include this in their policy
 * set, turning "the scope is absent" from silently-shared-keys into a denial.
 * It runs in the PRE-EXECUTE gates loop — which matters because the dry-run
 * branch returns before the idempotency and rate-limit machinery, and a
 * constraint that must bind simulations cannot live on those paths. The
 * single-tenant OSS default simply omits this evaluator: absent scope then
 * preserves unsegmented keys exactly.
 */
export function tenantIsolation(
  options: { name?: string } = {},
): ToolPolicyEvaluator {
  return {
    name: options.name ?? 'tenant-isolation',
    evaluate(call): PolicyDecision {
      const scope = call.requestContext?.get(ISOLATION_SCOPE_CONTEXT_KEY);
      if (typeof scope !== 'string' || scope.length === 0) {
        return {
          allowed: false,
          reason:
            'caller carries no isolation scope; this deployment requires tenant-scoped connector calls',
        };
      }
      return { allowed: true };
    },
  };
}

/**
 * The field the LLM can include in tool-call args to override background
 * behavior per call (core `LLMBackgroundOverride` — `{ enabled?, timeoutMs?,
 * maxRetries? }`, background-tasks/types.d.ts). Shared by the connector SDK's
 * hard rejection and the `backgroundExecution` evaluator below so the one name
 * the model would smuggle lives in one place.
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
   * these. A write / destructive / idempotent connector carries a side effect
   * whose approval topology and timing the background flip would move off the
   * foreground path, so v1 keeps them foreground-only. Default: everything but
   * 'read'.
   */
  writeClass?: readonly SideEffect[];
  /** Policy name used in denials and audit records. */
  name?: string;
}

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
 * explicit `{ enabled: false }` overrides pass. Approval grants in the trusted
 * request context remain the final write boundary on every execution path.
 * Register the evaluator through `ConnectorPolicies.evaluators`.
 */
export function backgroundExecution(
  options: BackgroundExecutionOptions = {},
): ToolPolicyEvaluator {
  const writeClass = options.writeClass ?? [
    'write',
    'destructive',
    'idempotent',
  ];
  return {
    name: options.name ?? 'background-execution',
    evaluate({ sideEffect, input, connectorId }): PolicyDecision {
      if (!writeClass.includes(sideEffect)) return { allowed: true };
      const override = backgroundOverrideOf(input);
      if (override !== undefined && override.enabled !== false) {
        return {
          allowed: false,
          reason: `write-class connector '${connectorId}' may not run in background: an LLM _background override would move it off the foreground path (v1 connectors are foreground-only)`,
        };
      }
      return { allowed: true };
    },
  };
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
 * of truth the connector SDK compiles into both enforcement paths: Mastra's
 * native `requireApproval` for agent runs, and the execute wrapper's hard
 * gate for workflow steps and direct calls.
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
