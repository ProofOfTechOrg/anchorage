// SPDX-License-Identifier: Apache-2.0

import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import type { Permission } from '@proofoftech/breakwater/rbac';
import type { AgentEntryPath } from '../agent-runner/index.js';
import type {
  ApprovalRecord,
  ApprovalRole,
  AutomatedPrincipalKind,
  ExecutionPrincipal,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';

// The permission identifier vocabulary moved to breakwater so the agent-entry
// gate here and the connector invocation gate there share one grammar;
// re-exported to keep this subpath's public surface stable.
export type { Permission } from '@proofoftech/breakwater/rbac';
export { isPermissionIdentifier } from '@proofoftech/breakwater/rbac';
export type { AgentEntryPath } from '../agent-runner/index.js';

/** The server-derived permissions for one trusted execution principal. */
export interface PrincipalPermissionResolution {
  /** Exact permission identifiers granted by this policy snapshot. */
  permissions: readonly Permission[];
  /**
   * Stable version or hash identifying the policy snapshot used. Must be
   * non-blank, at most 200 characters, and free of ASCII control characters;
   * anything else is malformed output and fails closed.
   */
  policyVersion: string;
}

/**
 * Resolve a trusted principal to permissions owned by the host.
 *
 * The resolver receives no request body or caller-provided context. Human
 * roles and automated identity/provenance are already carried by the validated
 * principal, so both kinds use the same server-side boundary without treating
 * an automated principal's compatibility role projection as authority.
 *
 * A throw, rejection, or malformed result fails closed: it denies an agent
 * that declares `requiredPermissions`, and it costs any other run its
 * permission projection — so a connector that declares `requiredPermissions`
 * denies inside that run. Both outcomes audit the generic reason `permission
 * resolution failed`; the underlying error is never re-exported, so log
 * failures inside the resolver itself.
 */
export type PrincipalPermissionResolver = (
  principal: ExecutionPrincipal,
) => PrincipalPermissionResolution | Promise<PrincipalPermissionResolution>;

/**
 * One automated entry an agent accepts: a principal kind paired with the exact
 * entry paths it may arrive on. Kind alone is too coarse — a schedule-driven
 * agent should not thereby accept webhook-delivered signals.
 */
export interface AgentAutomationRule {
  kind: AutomatedPrincipalKind;
  entryPaths: readonly AgentEntryPath[];
}

export interface AgentMeta {
  id: string;
  title: string;
  description: string;
  /** Human roles permitted to start the agent over authenticated HTTP. */
  allowedRoles?: readonly ApprovalRole[];
  /**
   * Server-derived permissions required to enter this agent. Semantics are
   * explicitly ALL-OF: the principal must hold every listed identifier.
   * Omission preserves the role/automation-only authorization path; a present
   * list must be non-empty.
   */
  requiredPermissions?: readonly Permission[];
  /**
   * Automated entries the agent accepts. ABSENT OR EMPTY DENIES EVERY
   * automated start and resume — a schedule, signal, provider, or delegating
   * agent must be named here to reach this agent at all.
   *
   * Declared on the metadata rather than injected at wiring time so the edge
   * router and the thread host enforce it from the same catalog, and so an
   * agent cannot become reachable by automation through a host that simply
   * forgot to pass a policy.
   */
  allowedAutomation?: readonly AgentAutomationRule[];
}

export interface AgentModule {
  meta: AgentMeta;
  agent: GuardedAgentHandle;
}

/**
 * Whether an automated principal may enter this agent on this path.
 *
 * Returns false for an unknown agent and for every human principal — humans go
 * through the role gate instead, and answering "true" here for a human would
 * make two gates look interchangeable when they are not.
 */
export type AutomationCheck = (
  agentId: string,
  principal: ExecutionPrincipal,
  entryPath: AgentEntryPath,
) => boolean;

export interface AgentCatalog {
  readonly agents: readonly AgentMeta[];
  get(agentId: string): AgentMeta | undefined;
  allowedRoles(agentId: string): readonly ApprovalRole[] | undefined;
  automationAllowed: AutomationCheck;
}

export interface AgentModuleCatalog {
  readonly agents: readonly AgentMeta[];
  readonly modules: readonly AgentModule[];
  get(agentId: string): AgentModule | undefined;
  allowedRoles(agentId: string): readonly ApprovalRole[] | undefined;
  automationAllowed: AutomationCheck;
}

export interface AgentRunEnvelope {
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  summary: RunSummary;
  approval?: ApprovalRecord;
  approvals?: ApprovalRecord[];
}

export interface TrustedAgentExecution {
  agentId: string;
  /** Infrastructure-verified deployment tag for audit attribution. */
  deploymentTag?: string;
  /**
   * WHO is executing. On an approval resume this is the restored original
   * principal, never the reviewer who decided — so a human approval does not
   * transfer that human's authority into the resumed run.
   */
  principal: ExecutionPrincipal;
  threadId: string;
  resourceId: string;
  runId: string;
  entryPath: AgentEntryPath;
  /**
   * The server-resolved permissions this execution runs under, projected into
   * derived request context as `breakwater.principalPermissions` on every
   * leg. `null` when no resolution exists — no resolver configured, or the
   * resolution failed on an agent that requires no permissions. The null is
   * projected explicitly rather than omitted so a resume leg retires any
   * stale persisted projection instead of inheriting it.
   */
  principalPermissions: PrincipalPermissionResolution | null;
  /**
   * Non-reserved context accepted only from trusted internal entry paths.
   * Public HTTP agent starts never populate this field.
   */
  safeContext?: Readonly<Record<string, unknown>>;
}
