// SPDX-License-Identifier: Apache-2.0

import {
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from './breakwater-keys.js';

const BREAKWATER_KEY_PREFIX = 'breakwater.';
const GOAL_REQUEST_CONTEXT_KEY = 'mastra:goal';

/**
 * Runtime-owned request-context keys. The predicate below also reserves the
 * complete `breakwater.*` namespace so future capability keys are protected
 * before this inventory is updated.
 */
export const RESERVED_EXECUTION_CONTEXT_KEYS: readonly string[] = [
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
  GOAL_REQUEST_CONTEXT_KEY,
  'runId',
  'threadId',
  'resourceId',
  '__proto__',
  'constructor',
  'prototype',
];

export function isReservedExecutionContextKey(key: string): boolean {
  return (
    key.startsWith(BREAKWATER_KEY_PREFIX) ||
    key === GOAL_REQUEST_CONTEXT_KEY ||
    key === 'runId' ||
    key === 'threadId' ||
    key === 'resourceId' ||
    key === '__proto__' ||
    key === 'constructor' ||
    key === 'prototype'
  );
}

export function findReservedExecutionContextKey(
  context: Record<string, unknown>,
): string | undefined {
  return Object.keys(context).find(isReservedExecutionContextKey);
}

/**
 * Sanitize a persisted compatibility context. External request boundaries
 * must reject reserved keys with assertNoReservedExecutionContext instead.
 */
export function stripReservedExecutionContext(
  context: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const safe: Record<string, unknown> = {};
  if (!context) return safe;
  for (const [key, value] of Object.entries(context)) {
    if (!isReservedExecutionContextKey(key)) {
      Object.defineProperty(safe, key, {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
  }
  return safe;
}

export class ReservedExecutionContextError extends Error {
  readonly key: string;

  constructor(key: string, label = 'requestContext') {
    super(`${label} may not carry the reserved key '${key}'`);
    this.name = 'ReservedExecutionContextError';
    this.key = key;
  }
}

export function assertNoReservedExecutionContext(
  context: Record<string, unknown>,
  label?: string,
): void {
  const key = findReservedExecutionContextKey(context);
  if (key !== undefined) {
    throw new ReservedExecutionContextError(key, label);
  }
}
