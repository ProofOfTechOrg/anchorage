// SPDX-License-Identifier: Apache-2.0

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal-identity.js';
import {
  ExecutionFenceUnreadableError,
  normalizeMutationEpoch,
  normalizeRunExecutionIdentity,
  normalizeStartIdentity,
  type RunExecutionIdentity,
  type StartIdentity,
} from './execution-admission.js';
import { isPathSafeId } from './path-safe-id.js';

export interface DecodedRunStartIdentity {
  readonly version: 2;
  readonly startToken: string;
  readonly startIdentity?: StartIdentity;
  readonly agentStart?: { readonly threaded: boolean };
}

export interface InitialRunProvenance extends DecodedRunStartIdentity {
  readonly attemptToken: string;
  readonly requestedBy?: string;
  readonly requestedByKind?: ExecutionPrincipalKind;
  readonly resumeCounts: readonly [];
  readonly mutationEpoch?: number;
  readonly initialAdmission?: true;
}

function provenanceObject(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== 'object' || Array.isArray(value))
    throw new Error('run provenance must be an object');
  return value as Record<string, unknown>;
}

function startIdentityFromObject(
  row: Record<string, unknown>,
): DecodedRunStartIdentity {
  const { startToken, startIdentity: rawIdentity, agentStart: rawAgent } = row;
  if (!isPathSafeId(startToken)) throw new Error('run start token is invalid');
  const startIdentity =
    rawIdentity === undefined ? undefined : normalizeStartIdentity(rawIdentity);
  let agentStart: { readonly threaded: boolean } | undefined;
  if (startIdentity?.target.kind === 'agent') {
    const { threaded } = provenanceObject(rawAgent);
    if (typeof threaded !== 'boolean')
      throw new Error('agent start mode is invalid');
    agentStart = Object.freeze({ threaded });
  } else if (rawAgent !== undefined)
    throw new Error('agent start mode has no agent target');
  return Object.freeze({
    version: 2,
    startToken,
    ...(startIdentity === undefined ? {} : { startIdentity }),
    ...(agentStart === undefined ? {} : { agentStart }),
  });
}

/** Decode role-neutral data; inherited logical targets need not name this row. */
export function decodeRunStartIdentity(
  value: unknown,
): DecodedRunStartIdentity | undefined {
  try {
    if (value === undefined) return undefined;
    const row = provenanceObject(value);
    const version = row.version;
    if (version === 1) return undefined;
    if (version !== 2) throw new Error('run provenance version is invalid');
    return startIdentityFromObject(row);
  } catch (error) {
    throw new ExecutionFenceUnreadableError(
      'run start identity is not readable',
      { cause: error },
    );
  }
}

export function runExecutionIdentityFor(
  address: { tablePrefix: string | null; workflowId: string; runId: string },
  decoded: DecodedRunStartIdentity,
): RunExecutionIdentity {
  return normalizeRunExecutionIdentity({
    ...address,
    startToken: decoded.startToken,
  });
}

export function decodeInitialRunProvenance(
  value: unknown,
  marker: 'absent' | 'present',
): InitialRunProvenance {
  try {
    const row = provenanceObject(value);
    const {
      version,
      attemptToken,
      requestedBy,
      requestedByKind,
      resumeCounts,
      mutationEpoch: rawEpoch,
      initialAdmission,
    } = row;
    if (
      version !== 2 ||
      !isPathSafeId(attemptToken) ||
      !Array.isArray(resumeCounts) ||
      resumeCounts.length !== 0 ||
      (marker === 'absent'
        ? initialAdmission !== undefined
        : marker !== 'present' || initialAdmission !== true)
    )
      throw new Error('initial run provenance is malformed');
    const start = startIdentityFromObject(row);
    const mutationEpoch = normalizeMutationEpoch(rawEpoch);
    if (requestedBy !== undefined || requestedByKind !== undefined) {
      if (
        !isExecutionPrincipalId(requestedBy) ||
        !isExecutionPrincipalKind(requestedByKind)
      )
        throw new Error('initial requester is malformed');
    }
    if (
      (requestedBy !== undefined && start.startIdentity === undefined) ||
      (start.startIdentity !== undefined &&
        (start.startIdentity.owner.id !== requestedBy ||
          start.startIdentity.owner.kind !== requestedByKind))
    )
      throw new Error('initial requester disagrees with start identity');
    return Object.freeze({
      ...start,
      attemptToken,
      ...(requestedBy === undefined
        ? {}
        : {
            requestedBy: requestedBy as string,
            requestedByKind: requestedByKind as ExecutionPrincipalKind,
          }),
      resumeCounts: Object.freeze([]) as readonly [],
      ...(mutationEpoch === undefined ? {} : { mutationEpoch }),
      ...(marker === 'present' ? { initialAdmission: true as const } : {}),
    });
  } catch (error) {
    throw new ExecutionFenceUnreadableError(
      'initial run provenance is not readable',
      { cause: error },
    );
  }
}
