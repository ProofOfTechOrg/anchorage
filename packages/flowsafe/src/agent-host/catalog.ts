// SPDX-License-Identifier: Apache-2.0

import { isGuardedAgentHandle } from '@proofoftech/breakwater/agent';

import { type ApprovalRole, RUN_START_ROLES } from '../approval-api/index.js';
import { PATH_SAFE_ID_PATTERN } from '../do-runner/index.js';
import type {
  AgentCatalog,
  AgentMeta,
  AgentModule,
  AgentModuleCatalog,
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

export function validateAgentMeta(meta: AgentMeta): AgentMeta {
  if (typeof meta.id !== 'string' || !PATH_SAFE_ID_PATTERN.test(meta.id)) {
    fail('id must be URL-path-safe');
  }
  if (typeof meta.title !== 'string' || meta.title.trim() === '') {
    fail(`agent '${meta.id}' title must not be empty`);
  }
  if (typeof meta.description !== 'string' || meta.description.trim() === '') {
    fail(`agent '${meta.id}' description must not be empty`);
  }
  const roles = normalizedRoles(meta.allowedRoles, meta.id);
  return Object.freeze({
    id: meta.id,
    title: meta.title,
    description: meta.description,
    ...(meta.allowedRoles !== undefined ? { allowedRoles: roles } : {}),
  });
}

export function createAgentCatalog(
  metadata: readonly AgentMeta[],
): AgentCatalog {
  const agents: AgentMeta[] = [];
  const byId = new Map<string, AgentMeta>();
  const rolesById = new Map<string, readonly ApprovalRole[]>();
  for (const candidate of metadata) {
    const meta = validateAgentMeta(candidate);
    if (byId.has(meta.id)) fail(`duplicate agent id '${meta.id}'`);
    agents.push(meta);
    byId.set(meta.id, meta);
    rolesById.set(meta.id, normalizedRoles(meta.allowedRoles, meta.id));
  }
  const frozen = Object.freeze(agents);
  return Object.freeze({
    agents: frozen,
    get: (agentId: string) => byId.get(agentId),
    allowedRoles: (agentId: string) => rolesById.get(agentId),
  });
}

export function validateAgentModule(module: AgentModule): AgentModule {
  const meta = validateAgentMeta(module.meta);
  if (!isGuardedAgentHandle(module.agent)) {
    fail(`agent '${meta.id}' must be created by createGuardedAgent`);
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
  });
}
