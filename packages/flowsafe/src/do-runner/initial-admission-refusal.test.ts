// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  RunAdmissionConflictError,
} from './execution-admission.js';
import {
  definitiveInitialAdmissionRefusal,
  isDefinitiveInitialAdmissionRefusal,
} from './initial-admission-refusal.js';

const EXECUTION = {
  tablePrefix: '',
  workflowId: 'workflow',
  runId: 'run',
  startToken: 'generation',
};

describe('definitive initial admission refusal', () => {
  it('retains private immutable exact-scope evidence across public diagnostic failures', () => {
    const source = { ...EXECUTION };
    const cause = new ExecutionFenceUnreadableError(
      'initial admission readback is not readable',
    );
    const error = definitiveInitialAdmissionRefusal(source, cause);
    source.startToken = 'changed';
    expect(error).toMatchObject({
      status: 503,
      reason: cause.reason,
      message: cause.message,
      cause,
    });
    expect(isDefinitiveInitialAdmissionRefusal(error, EXECUTION)).toBe(true);
    expect(
      isDefinitiveInitialAdmissionRefusal(
        new Error('adapter wrapper', { cause: error }),
        EXECUTION,
      ),
    ).toBe(true);
    expect(JSON.stringify(error)).not.toContain('generation');
    for (const key of Object.keys(EXECUTION)) {
      expect(
        isDefinitiveInitialAdmissionRefusal(error, {
          ...EXECUTION,
          [key]: 'different',
        }),
      ).toBe(false);
    }
  });

  it('rejects forgery, another namespace and generic uncertainty', () => {
    const conflict = new RunAdmissionConflictError('admission-raced');
    for (const error of [
      conflict,
      new ExecutionFenceUnreadableError('unknown'),
      { status: 409, reason: conflict.reason, execution: EXECUTION },
      undefined,
    ]) {
      expect(isDefinitiveInitialAdmissionRefusal(error, EXECUTION)).toBe(false);
    }
    const error = definitiveInitialAdmissionRefusal(EXECUTION, conflict);
    expect(
      isDefinitiveInitialAdmissionRefusal(
        JSON.parse(JSON.stringify(error)),
        EXECUTION,
      ),
    ).toBe(false);
    expect(
      isDefinitiveInitialAdmissionRefusal(error, {
        ...EXECUTION,
        tablePrefix: null,
      } as unknown as D1RunExecutionIdentity),
    ).toBe(false);
    expect(
      isDefinitiveInitialAdmissionRefusal(
        Object.create(Object.getPrototypeOf(error)),
        EXECUTION,
      ),
    ).toBe(false);
  });

  it('bounds cause traversal and terminates cycles without evidence', () => {
    const valid = definitiveInitialAdmissionRefusal(
      EXECUTION,
      new RunAdmissionConflictError('run-exists'),
    );
    let wrapped: Error = valid;
    for (let index = 0; index < 8; index += 1)
      wrapped = new Error('wrapper', { cause: wrapped });
    expect(isDefinitiveInitialAdmissionRefusal(wrapped, EXECUTION)).toBe(false);
    const cyclic = new Error('cycle');
    cyclic.cause = cyclic;
    expect(isDefinitiveInitialAdmissionRefusal(cyclic, EXECUTION)).toBe(false);
  });
});
