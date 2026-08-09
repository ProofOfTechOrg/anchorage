// SPDX-License-Identifier: Apache-2.0

import type { GuardedAgentHandle } from '@proofoftech/breakwater/agent';
import type { AgentEntryPath } from '../agent-runner/index.js';
import type {
  ApprovalRecord,
  ApprovalRole,
  AutomatedPrincipalKind,
  ExecutionPrincipal,
} from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';

export type { AgentEntryPath } from '../agent-runner/index.js';

/**
 * A canonical server-owned permission identifier.
 *
 * The runtime form is two or more lowercase ASCII segments separated by dots,
 * with each segment starting with a letter and continuing with letters or
 * digits. Identifiers are bounded to 200 characters.
 */
export type Permission = string;

const PERMISSION_IDENTIFIER_PATTERN =
  /^(?=.{3,200}$)[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$/;

/** Whether a value is a canonical permission identifier. */
export function isPermissionIdentifier(value: unknown): value is Permission {
  return typeof value === 'string' && PERMISSION_IDENTIFIER_PATTERN.test(value);
}

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
   * Non-reserved context accepted only from trusted internal entry paths.
   * Public HTTP agent starts never populate this field.
   */
  safeContext?: Readonly<Record<string, unknown>>;
}
