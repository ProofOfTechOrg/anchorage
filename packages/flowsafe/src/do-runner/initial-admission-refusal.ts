// SPDX-License-Identifier: Apache-2.0

import { findInCauseChain } from './cause-chain.js';
import { DoStatusError } from './do-status-error.js';
import {
  type D1RunExecutionIdentity,
  normalizeD1RunExecutionIdentity,
} from './execution-admission.js';

const refusalEvidence = new WeakMap<object, D1RunExecutionIdentity>();

class DefinitiveInitialAdmissionRefusal extends DoStatusError {
  readonly status: number;
  override readonly reason;
  constructor(cause: DoStatusError) {
    super(cause.message, { cause });
    this.status = cause.status;
    this.reason = cause.reason;
  }
}

/** @internal Called only after the owned batch validates an all-zero result. */
export function definitiveInitialAdmissionRefusal(
  execution: D1RunExecutionIdentity,
  cause: DoStatusError,
): DoStatusError {
  const captured = normalizeD1RunExecutionIdentity(execution);
  const refusal = new DefinitiveInitialAdmissionRefusal(cause);
  refusalEvidence.set(refusal, captured);
  return refusal;
}

/** In-process no-insert evidence for this exact scope, never deletion authority. */
export function isDefinitiveInitialAdmissionRefusal(
  error: unknown,
  execution: D1RunExecutionIdentity,
): boolean {
  try {
    const expected = normalizeD1RunExecutionIdentity(execution);
    return findInCauseChain(
      error,
      (link) => {
        const observed =
          link !== null && typeof link === 'object'
            ? refusalEvidence.get(link)
            : undefined;
        return (
          observed !== undefined &&
          observed.tablePrefix === expected.tablePrefix &&
          observed.workflowId === expected.workflowId &&
          observed.runId === expected.runId &&
          observed.startToken === expected.startToken
        );
      },
      { rootOnly: false },
    );
  } catch {
    return false;
  }
}
