// SPDX-License-Identifier: Apache-2.0

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import { DoStatusError } from './do-status-error.js';
import { isPathSafeId } from './path-safe-id.js';
import { validateTablePrefix } from './table-prefix.js';

export interface MutationEpochContext {
  readonly mutationEpoch?: number;
}

/** Identity data; null explicitly makes no D1 namespace assertion. */
export interface RunExecutionIdentity {
  readonly tablePrefix: string | null;
  readonly workflowId: string;
  readonly runId: string;
  readonly startToken: string;
}

export interface D1RunExecutionIdentity extends RunExecutionIdentity {
  readonly tablePrefix: string;
}

export interface StartIdentity {
  readonly owner: {
    readonly kind: ExecutionPrincipalKind;
    readonly id: string;
  };
  readonly target:
    | { readonly kind: 'workflow'; readonly id: string }
    | {
        readonly kind: 'agent';
        readonly id: string;
        readonly threadId: string;
      };
}

export interface StartExecutionIdentity
  extends RunExecutionIdentity,
    StartIdentity {}
export interface D1StartExecutionIdentity
  extends D1RunExecutionIdentity,
    StartIdentity {}

const IDENTITY_ERRORS = {
  identity: 'execution identity must be an object',
  tablePrefix: 'tablePrefix is not valid for this execution identity',
  workflowId: 'workflowId must be a URL-path-safe identifier',
  runId: 'runId must be a URL-path-safe identifier',
  startToken: 'startToken must be a URL-path-safe identifier',
  owner: 'owner must be an execution principal object',
  'owner.kind': 'owner.kind must be an execution principal kind',
  'owner.id': 'owner.id must be a valid execution principal identifier',
  target: 'target must be an object',
  'target.kind': 'target.kind must be workflow or agent',
  'target.id': 'target.id must be a URL-path-safe identifier',
  'target.threadId':
    'target.threadId is required only for an agent target and must be URL-path-safe',
} as const;

export class InvalidExecutionIdentityError extends DoStatusError {
  readonly status = 400;
  readonly reason = { code: 'INVALID_EXECUTION_IDENTITY' } as const;

  constructor(field: keyof typeof IDENTITY_ERRORS) {
    super(
      Object.hasOwn(IDENTITY_ERRORS, field)
        ? IDENTITY_ERRORS[field]
        : 'execution identity is malformed',
    );
    this.name = 'InvalidExecutionIdentityError';
  }
}

export class InvalidMutationEpochError extends DoStatusError {
  readonly status = 400;
  readonly reason = { code: 'INVALID_MUTATION_EPOCH' } as const;

  constructor() {
    super('mutationEpoch must be a nonnegative safe integer or undefined');
    this.name = 'InvalidMutationEpochError';
  }
}

export class MutationEpochMismatchError extends DoStatusError {
  readonly status = 409;
  readonly reason: {
    readonly code: 'MUTATION_EPOCH_MISMATCH';
    readonly classification: 'missing' | 'stale' | 'future';
    readonly mutationEpoch: number;
  };

  constructor(
    classification: 'missing' | 'stale' | 'future',
    mutationEpoch: number,
  ) {
    super('mutation epoch does not match the active deployment');
    this.name = 'MutationEpochMismatchError';
    this.reason = {
      code: 'MUTATION_EPOCH_MISMATCH',
      classification,
      mutationEpoch,
    };
  }
}

export class ExecutionFenceUnreadableError extends DoStatusError {
  readonly status = 503;
  readonly reason: { readonly code: 'EXECUTION_FENCE_UNREADABLE' };

  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'ExecutionFenceUnreadableError';
    this.reason = { code: 'EXECUTION_FENCE_UNREADABLE' };
  }
}

function identityObject(
  value: unknown,
  field: 'identity' | 'owner' | 'target',
): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new InvalidExecutionIdentityError(field);
  }
  return value as Record<string, unknown>;
}

export function normalizeRunExecutionIdentity(
  value: unknown,
): RunExecutionIdentity {
  const { tablePrefix, workflowId, runId, startToken } = identityObject(
    value,
    'identity',
  );
  if (tablePrefix !== null && typeof tablePrefix !== 'string') {
    throw new InvalidExecutionIdentityError('tablePrefix');
  }
  if (tablePrefix !== null) {
    try {
      validateTablePrefix(tablePrefix);
    } catch {
      throw new InvalidExecutionIdentityError('tablePrefix');
    }
  }
  if (!isPathSafeId(workflowId))
    throw new InvalidExecutionIdentityError('workflowId');
  if (!isPathSafeId(runId)) throw new InvalidExecutionIdentityError('runId');
  if (!isPathSafeId(startToken))
    throw new InvalidExecutionIdentityError('startToken');
  return Object.freeze({
    tablePrefix: tablePrefix?.toLowerCase() ?? null,
    workflowId,
    runId,
    startToken,
  });
}

export function normalizeD1RunExecutionIdentity(
  value: unknown,
): D1RunExecutionIdentity {
  const identity = normalizeRunExecutionIdentity(value);
  if (identity.tablePrefix === null)
    throw new InvalidExecutionIdentityError('tablePrefix');
  return Object.freeze({ ...identity, tablePrefix: identity.tablePrefix });
}

export function normalizeStartIdentity(value: unknown): StartIdentity {
  const { owner: rawOwner, target: rawTarget } = identityObject(
    value,
    'identity',
  );
  const { kind: ownerKind, id: ownerId } = identityObject(rawOwner, 'owner');
  const { kind, id, threadId } = identityObject(rawTarget, 'target');
  if (!isExecutionPrincipalKind(ownerKind))
    throw new InvalidExecutionIdentityError('owner.kind');
  if (!isExecutionPrincipalId(ownerId))
    throw new InvalidExecutionIdentityError('owner.id');
  if (kind !== 'workflow' && kind !== 'agent')
    throw new InvalidExecutionIdentityError('target.kind');
  if (!isPathSafeId(id)) throw new InvalidExecutionIdentityError('target.id');
  const owner = Object.freeze({ kind: ownerKind, id: ownerId });
  if (kind === 'agent') {
    if (!isPathSafeId(threadId))
      throw new InvalidExecutionIdentityError('target.threadId');
    return Object.freeze({
      owner,
      target: Object.freeze({ kind, id, threadId }),
    });
  }
  if (threadId !== undefined)
    throw new InvalidExecutionIdentityError('target.threadId');
  return Object.freeze({ owner, target: Object.freeze({ kind, id }) });
}

export function normalizeStartExecutionIdentity(
  value: unknown,
): StartExecutionIdentity {
  return Object.freeze({
    ...normalizeRunExecutionIdentity(value),
    ...normalizeStartIdentity(value),
  });
}

export function normalizeMutationEpoch(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new InvalidMutationEpochError();
  }
  return value === 0 ? 0 : value;
}

export function assertMutationEpoch(
  reading: {
    readonly mutationEpoch: number;
    readonly requireMutationEpoch: boolean;
  },
  mutationEpoch: number | undefined,
): void {
  const supplied = normalizeMutationEpoch(mutationEpoch);
  const candidate =
    reading !== null && typeof reading === 'object' && !Array.isArray(reading)
      ? reading
      : undefined;
  const current = candidate?.mutationEpoch;
  const required = candidate?.requireMutationEpoch;
  if (
    typeof current !== 'number' ||
    !Number.isSafeInteger(current) ||
    current < 0 ||
    typeof required !== 'boolean' ||
    required !== current > 0
  ) {
    throw new ExecutionFenceUnreadableError(
      'execution fence mutation metadata is not readable',
    );
  }
  if (!required || supplied === current) return;
  throw new MutationEpochMismatchError(
    supplied === undefined
      ? 'missing'
      : supplied < current
        ? 'stale'
        : 'future',
    current,
  );
}

export const MUTATION_EPOCH_HEADER = 'x-flowsafe-mutation-epoch';

export function mutationEpochFromHeader(
  value: string | null,
): number | undefined {
  if (value === null) return undefined;
  if (
    typeof value !== 'string' ||
    value.length > 16 ||
    !/^(?:0|[1-9][0-9]*)$/.test(value)
  ) {
    throw new InvalidMutationEpochError();
  }
  return normalizeMutationEpoch(Number(value));
}

export function stampMutationEpoch(
  headers: Headers,
  epoch: number | undefined,
): void {
  const value = normalizeMutationEpoch(epoch);
  if (value === undefined) headers.delete(MUTATION_EPOCH_HEADER);
  else headers.set(MUTATION_EPOCH_HEADER, String(value));
}
