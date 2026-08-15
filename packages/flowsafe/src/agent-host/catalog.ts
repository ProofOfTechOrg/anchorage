// SPDX-License-Identifier: Apache-2.0

import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';
import { breakwaterGuardedAgentHostProtocol } from '../agent-runner/durable-agent-runner.js';
import { AGENT_ENTRY_PATHS } from '../agent-runner/index.js';
import {
  type ApprovalRole,
  AUTOMATED_PRINCIPAL_KINDS,
  type AutomatedPrincipalKind,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';
import {
  type AgentAutomationRule,
  type AgentCatalog,
  type AgentEntryPath,
  type AgentMeta,
  type AgentModule,
  type AgentModuleCatalog,
  type AutomationCheck,
  isPermissionIdentifier,
  type Permission,
} from './types.js';

function fail(message: string): never {
  throw new Error(`agent catalog: ${message}`);
}

function normalizedRoles(
  roles: readonly ApprovalRole[] | undefined,
  agentId: string,
): readonly ApprovalRole[] {
  const effective = roles ?? RUN_START_ROLES;
  if (effective.length === 0) {
    fail(`agent '${agentId}' allowedRoles must not be empty`);
  }
  const unique = new Set<ApprovalRole>();
  for (const role of effective) {
    if (!RUN_START_ROLES.includes(role)) {
      fail(`agent '${agentId}' role '${String(role)}' is not a run-start role`);
    }
    if (unique.has(role)) {
      fail(`agent '${agentId}' allowedRoles contains duplicate '${role}'`);
    }
    unique.add(role);
  }
  return Object.freeze([...effective]);
}

function normalizedPermissions(
  permissions: readonly Permission[] | undefined,
  agentId: string,
): readonly Permission[] | undefined {
  if (permissions === undefined) return undefined;
  if (!Array.isArray(permissions)) {
    fail(`agent '${agentId}' requiredPermissions must be an array`);
  }
  if (permissions.length === 0) {
    fail(`agent '${agentId}' requiredPermissions must not be empty`);
  }
  const unique = new Set<Permission>();
  for (const permission of permissions) {
    if (!isPermissionIdentifier(permission)) {
      fail(
        `agent '${agentId}' requiredPermissions contains a malformed permission identifier`,
      );
    }
    if (unique.has(permission)) {
      fail(
        `agent '${agentId}' requiredPermissions contains duplicate '${permission}'`,
      );
    }
    unique.add(permission);
  }
  return Object.freeze([...permissions]);
}

/**
 * Normalize the automation declaration. An absent field stays absent rather
 * than becoming a default set: there is no safe default for "which robots may
 * drive this agent", and materializing one would make the deny-by-default read
 * as an oversight instead of the contract.
 */
function normalizedAutomation(
  rules: readonly AgentAutomationRule[] | undefined,
  agentId: string,
): readonly AgentAutomationRule[] | undefined {
  if (rules === undefined) return undefined;
  if (!Array.isArray(rules)) {
    fail(`agent '${agentId}' allowedAutomation must be an array`);
  }
  const seenKinds = new Set<AutomatedPrincipalKind>();
  const normalized = rules.map((rule) => {
    if (rule === null || typeof rule !== 'object') {
      fail(`agent '${agentId}' allowedAutomation entries must be objects`);
    }
    if (!AUTOMATED_PRINCIPAL_KINDS.includes(rule.kind)) {
      fail(
        `agent '${agentId}' allowedAutomation kind '${String(rule.kind)}' is not an automated principal kind`,
      );
    }
    if (seenKinds.has(rule.kind)) {
      fail(
        `agent '${agentId}' allowedAutomation repeats kind '${rule.kind}'; list its entry paths once`,
      );
    }
    seenKinds.add(rule.kind);
    if (!Array.isArray(rule.entryPaths) || rule.entryPaths.length === 0) {
      fail(
        `agent '${agentId}' allowedAutomation kind '${rule.kind}' must name at least one entry path`,
      );
    }
    const seenPaths = new Set<AgentEntryPath>();
    for (const entryPath of rule.entryPaths) {
      if (entryPath === 'approval.resume') {
        fail(
          `agent '${agentId}' must not declare 'approval.resume'; resuming is implied by the kind that started the run`,
        );
      }
      if (!(AGENT_ENTRY_PATHS as readonly string[]).includes(entryPath)) {
        fail(
          `agent '${agentId}' allowedAutomation names unknown entry path '${String(entryPath)}'`,
        );
      }
      if (seenPaths.has(entryPath)) {
        fail(
          `agent '${agentId}' allowedAutomation repeats entry path '${entryPath}'`,
        );
      }
      seenPaths.add(entryPath);
    }
    return Object.freeze({
      kind: rule.kind,
      entryPaths: Object.freeze([...rule.entryPaths]),
    });
  });
  return Object.freeze(normalized);
}

function automationCheckFor(
  automationById: ReadonlyMap<string, readonly AgentAutomationRule[]>,
  known: (agentId: string) => boolean,
): AutomationCheck {
  return (agentId, principal, entryPath) => {
    if (!known(agentId)) return false;
    // Humans are authorized by role, elsewhere. Saying "not allowed" here is
    // the honest answer for a gate that only speaks about automation.
    if (principal.kind === 'human') return false;
    const rules = automationById.get(agentId);
    if (!rules) return false;
    // Resuming is CONTINUING a run this kind was already admitted to start, so
    // it asks a different question: may this kind still drive this agent at
    // all? Demanding that hosts also list 'approval.resume' would mean any
    // automated agent that suspends for approval loses the run the moment a
    // human approves it — a decided approval and a stranded run. The narrowing
    // that matters is still enforced: a kind removed from the declaration
    // entirely can no longer resume.
    if (entryPath === 'approval.resume') {
      return rules.some((rule) => rule.kind === principal.kind);
    }
    return rules.some(
      (rule) =>
        rule.kind === principal.kind && rule.entryPaths.includes(entryPath),
    );
  };
}

export function validateAgentMeta(meta: AgentMeta): AgentMeta {
  if (!isPathSafeId(meta.id)) {
    fail('id must be URL-path-safe');
  }
  if (typeof meta.title !== 'string' || meta.title.trim() === '') {
    fail(`agent '${meta.id}' title must not be empty`);
  }
  if (typeof meta.description !== 'string' || meta.description.trim() === '') {
    fail(`agent '${meta.id}' description must not be empty`);
  }
  const roles = normalizedRoles(meta.allowedRoles, meta.id);
  const permissions = normalizedPermissions(meta.requiredPermissions, meta.id);
  const automation = normalizedAutomation(meta.allowedAutomation, meta.id);
  return Object.freeze({
    id: meta.id,
    title: meta.title,
    description: meta.description,
    ...(meta.allowedRoles !== undefined ? { allowedRoles: roles } : {}),
    ...(permissions !== undefined ? { requiredPermissions: permissions } : {}),
    ...(automation !== undefined ? { allowedAutomation: automation } : {}),
  });
}

export function createAgentCatalog(
  metadata: readonly AgentMeta[],
): AgentCatalog {
  const agents: AgentMeta[] = [];
  const byId = new Map<string, AgentMeta>();
  const rolesById = new Map<string, readonly ApprovalRole[]>();
  const automationById = new Map<string, readonly AgentAutomationRule[]>();
  for (const candidate of metadata) {
    const meta = validateAgentMeta(candidate);
    if (byId.has(meta.id)) fail(`duplicate agent id '${meta.id}'`);
    agents.push(meta);
    byId.set(meta.id, meta);
    rolesById.set(meta.id, normalizedRoles(meta.allowedRoles, meta.id));
    if (meta.allowedAutomation !== undefined) {
      automationById.set(meta.id, meta.allowedAutomation);
    }
  }
  const frozen = Object.freeze(agents);
  return Object.freeze({
    agents: frozen,
    get: (agentId: string) => byId.get(agentId),
    allowedRoles: (agentId: string) => rolesById.get(agentId),
    automationAllowed: automationCheckFor(automationById, (agentId) =>
      byId.has(agentId),
    ),
  });
}

export function validateAgentModule(module: AgentModule): AgentModule {
  const meta = validateAgentMeta(module.meta);
  if (!isGuardedAgentHandle(module.agent)) {
    fail(`agent '${meta.id}' must be created by createGuardedAgent`);
  }
  if (breakwaterGuardedAgentHostProtocol(module.agent) === undefined) {
    fail(
      `agent '${meta.id}' was built by a @proofoftech/breakwater without the durable-host protocol; >=0.12.0 is required`,
    );
  }
  if (module.agent.id !== meta.id) {
    fail(
      `metadata id '${meta.id}' does not match guarded agent id '${module.agent.id}'`,
    );
  }
  const metaRoles = normalizedRoles(meta.allowedRoles, meta.id);
  const handleRoles = normalizedRoles(
    module.agent.allowedRoles as readonly ApprovalRole[],
    meta.id,
  );
  if (
    metaRoles.length !== handleRoles.length ||
    metaRoles.some((role) => !handleRoles.includes(role))
  ) {
    fail(
      `agent '${meta.id}' metadata roles must exactly match guarded agent roles`,
    );
  }
  // Two halves of one decision: flowsafe's catalog decides WHICH automated
  // entry paths reach the agent, breakwater's handle decides WHICH kinds may
  // execute at all. If they disagree, the host either advertises automation
  // breakwater will refuse, or declares an agent automation-capable that its
  // catalog will never route to. Both are wiring bugs, so fail at construction.
  // A handle from a breakwater older than the principal-kinds release has no
  // such field. Say so, rather than throwing on `.filter` of undefined.
  if (!Array.isArray(module.agent.allowedPrincipalKinds)) {
    fail(
      `agent '${meta.id}' was built by a @proofoftech/breakwater without principal kinds; >=0.7.0 is required`,
    );
  }
  const metaKinds = new Set(
    (meta.allowedAutomation ?? []).map((rule) => rule.kind),
  );
  const handleKinds = new Set(
    module.agent.allowedPrincipalKinds.filter((kind) => kind !== 'human'),
  );
  if (
    metaKinds.size !== handleKinds.size ||
    [...metaKinds].some((kind) => !handleKinds.has(kind))
  ) {
    fail(
      `agent '${meta.id}' allowedAutomation kinds [${[...metaKinds].sort().join(', ')}] must exactly match guarded agent allowedPrincipalKinds [${[...handleKinds].sort().join(', ')}]`,
    );
  }
  return Object.freeze({ meta, agent: module.agent });
}

export function createAgentModuleCatalog(
  modules: readonly AgentModule[],
): AgentModuleCatalog {
  const validated: AgentModule[] = [];
  const byId = new Map<string, AgentModule>();
  for (const candidate of modules) {
    const module = validateAgentModule(candidate);
    if (byId.has(module.meta.id)) {
      fail(`duplicate agent id '${module.meta.id}'`);
    }
    validated.push(module);
    byId.set(module.meta.id, module);
  }
  const frozen = Object.freeze(validated);
  const catalog = createAgentCatalog(frozen.map((module) => module.meta));
  return Object.freeze({
    agents: catalog.agents,
    modules: frozen,
    get: (agentId: string) => byId.get(agentId),
    allowedRoles: catalog.allowedRoles,
    automationAllowed: catalog.automationAllowed,
  });
}
