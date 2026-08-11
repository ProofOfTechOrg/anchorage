// SPDX-License-Identifier: Apache-2.0

import type { Schedule } from '@mastra/core/storage';
import type { AgentAutomationRule } from '../agent-host/types.js';
import {
  APPROVAL_ROLES,
  type ApprovalRole,
  RUN_START_ROLES,
} from '../approval-api/index.js';
import { isPathSafeId } from '../do-runner/index.js';

export type AuthorizedSchedule = Schedule & { creatorRole: ApprovalRole };

export function scheduleWithCreatorRole(
  schedule: Schedule,
  creatorRole: ApprovalRole,
): AuthorizedSchedule {
  return { ...schedule, creatorRole };
}

export function scheduleCreatorRole(schedule: Schedule): ApprovalRole {
  const role = (schedule as Partial<AuthorizedSchedule>).creatorRole;
  if (!(APPROVAL_ROLES as readonly unknown[]).includes(role)) {
    throw new Error(`schedule '${schedule.id}' has no valid creator role`);
  }
  return role as ApprovalRole;
}

export interface ScheduleTargetCatalogEntry {
  id: string;
  allowedRoles?: readonly ApprovalRole[];
  allowedAutomation?: readonly AgentAutomationRule[];
}

export interface ScheduleTargetPolicyOptions {
  workflows: readonly ScheduleTargetCatalogEntry[];
  agents: readonly ScheduleTargetCatalogEntry[];
}

export type ScheduleTargetDecision =
  | { allowed: true }
  | {
      allowed: false;
      status: 403 | 404;
      reason:
        | 'unknown-target'
        | 'target-role-forbidden'
        | 'automation-forbidden';
    };

export interface ScheduleTargetPolicy {
  authorize(
    target: Schedule['target'],
    creatorRole: ApprovalRole,
  ): ScheduleTargetDecision;
}

function roleAllowed(
  entry: ScheduleTargetCatalogEntry,
  role: ApprovalRole,
): boolean {
  return (entry.allowedRoles ?? RUN_START_ROLES).includes(role);
}

function targetCatalog(
  label: 'workflow' | 'agent',
  entries: readonly ScheduleTargetCatalogEntry[],
): ReadonlyMap<string, ScheduleTargetCatalogEntry> {
  if (!Array.isArray(entries)) {
    throw new TypeError(`${label} target catalog must be an array`);
  }
  const catalog = new Map<string, ScheduleTargetCatalogEntry>();
  for (const value of entries as readonly unknown[]) {
    if (value === null || typeof value !== 'object') {
      throw new TypeError(`${label} target catalog entry is invalid`);
    }
    const entry = value as ScheduleTargetCatalogEntry;
    if (!isPathSafeId(entry.id)) {
      throw new TypeError(`${label} target id must be path-safe`);
    }
    if (catalog.has(entry.id)) {
      throw new TypeError(`duplicate ${label} target id '${entry.id}'`);
    }
    catalog.set(entry.id, entry);
  }
  return catalog;
}

export function createScheduleTargetPolicy(
  options: ScheduleTargetPolicyOptions,
): ScheduleTargetPolicy {
  if (options === null || typeof options !== 'object') {
    throw new TypeError('schedule target policy options are required');
  }
  const workflows = targetCatalog('workflow', options.workflows);
  const agents = targetCatalog('agent', options.agents);
  return {
    authorize(target, creatorRole) {
      const entry =
        target.type === 'workflow'
          ? workflows.get(target.workflowId)
          : agents.get(target.agentId);
      if (!entry) {
        return { allowed: false, status: 404, reason: 'unknown-target' };
      }
      if (!roleAllowed(entry, creatorRole)) {
        return {
          allowed: false,
          status: 403,
          reason: 'target-role-forbidden',
        };
      }
      if (
        target.type === 'agent' &&
        !entry.allowedAutomation?.some(
          (rule) =>
            rule.kind === 'system' && rule.entryPaths.includes('schedule.fire'),
        )
      ) {
        return {
          allowed: false,
          status: 403,
          reason: 'automation-forbidden',
        };
      }
      return { allowed: true };
    },
  };
}
