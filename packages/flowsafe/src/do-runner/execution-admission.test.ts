// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { ExecutionFenceUnreadableError as HostUnreadableError } from '../host-kit/index.js';
import { doErrorResponse } from './do-error-response.js';
import {
  assertMutationEpoch,
  ExecutionFenceUnreadableError,
  InvalidExecutionIdentityError,
  InvalidMutationEpochError,
  MUTATION_EPOCH_HEADER,
  MutationEpochMismatchError,
  mutationEpochFromHeader,
  normalizeD1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeRunExecutionIdentity,
  normalizeStartExecutionIdentity,
  normalizeStartIdentity,
  RunAdmissionConflictError,
  stampMutationEpoch,
} from './execution-admission.js';
import { ExecutionFenceUnreadableError as LegacyUnreadableError } from './execution-fence.js';
import { ExecutionFenceUnreadableError as RunnerUnreadableError } from './index.js';

const RUN = {
  tablePrefix: 'Tenant_',
  workflowId: 'child',
  runId: 'run',
  startToken: 'generation',
};
const START = {
  owner: { kind: 'human', id: 'Principal with spaces' },
  target: { kind: 'agent', id: 'agent', threadId: 'thread' },
};

describe('execution identity and epoch helpers', () => {
  it('uses fixed admission conflict and inconsistent-input errors without identity values', () => {
    for (const classification of [
      'run-owner-changed',
      'reservation-changed',
      'run-exists',
      'fence-changed',
      'admission-raced',
    ] as const) {
      const error = new RunAdmissionConflictError(classification);
      expect(error).toMatchObject({
        status: 409,
        message: 'initial run admission conflicts with current durable state',
        reason: { code: 'RUN_ADMISSION_CONFLICT', classification },
      });
    }
    expect(new InvalidExecutionIdentityError('admission')).toMatchObject({
      status: 400,
      message: 'initial admission identity is inconsistent',
      reason: { code: 'INVALID_EXECUTION_IDENTITY' },
    });
  });
  it('normalizes identity data without inventing a namespace or owner', () => {
    expect(normalizeRunExecutionIdentity(RUN)).toEqual({
      ...RUN,
      tablePrefix: 'tenant_',
    });
    expect(
      normalizeRunExecutionIdentity({ ...RUN, tablePrefix: null }).tablePrefix,
    ).toBeNull();
    expect(
      normalizeD1RunExecutionIdentity({ ...RUN, tablePrefix: '' }).tablePrefix,
    ).toBe('');
    expect(
      normalizeD1RunExecutionIdentity({ ...RUN, tablePrefix: 'A'.repeat(39) })
        .tablePrefix,
    ).toBe('a'.repeat(39));
    expect(normalizeStartIdentity(START)).toEqual(START);
    const workflow = {
      owner: START.owner,
      target: { kind: 'workflow', id: 'logical-parent' },
    };
    expect(normalizeStartExecutionIdentity({ ...RUN, ...workflow })).toEqual({
      ...RUN,
      tablePrefix: 'tenant_',
      ...workflow,
    });
    for (const value of [undefined, null, [], 'identity', 1]) {
      expect(() => normalizeRunExecutionIdentity(value)).toThrow(
        InvalidExecutionIdentityError,
      );
      expect(() => normalizeStartIdentity(value)).toThrow(
        InvalidExecutionIdentityError,
      );
    }
    for (const tablePrefix of [
      undefined,
      0,
      {},
      'a'.repeat(40),
      'invalid-prefix',
    ]) {
      expect(() =>
        normalizeRunExecutionIdentity({ ...RUN, tablePrefix }),
      ).toThrow(InvalidExecutionIdentityError);
    }
    expect(() =>
      normalizeD1RunExecutionIdentity({ ...RUN, tablePrefix: null }),
    ).toThrow(InvalidExecutionIdentityError);
    for (const field of ['workflowId', 'runId', 'startToken']) {
      for (const value of [
        '',
        '.',
        '..',
        'bad/path',
        'with space',
        4,
        null,
        'a'.repeat(201),
      ]) {
        expect(() =>
          normalizeRunExecutionIdentity({ ...RUN, [field]: value }),
        ).toThrow(InvalidExecutionIdentityError);
      }
    }
    for (const owner of [
      null,
      [],
      {},
      { kind: 'unknown', id: 'owner' },
      { kind: 'human', id: '  ' },
      { kind: 'human', id: 'control\ntext' },
    ]) {
      expect(() => normalizeStartIdentity({ ...START, owner })).toThrow(
        InvalidExecutionIdentityError,
      );
    }
    for (const target of [
      null,
      [],
      {},
      { kind: 'other', id: 'target' },
      { kind: 'workflow', id: 'bad/path' },
      { kind: 'agent', id: 'agent' },
      { kind: 'agent', id: 'agent', threadId: null },
      { kind: 'workflow', id: 'workflow', threadId: 'thread' },
    ]) {
      expect(() => normalizeStartIdentity({ ...START, target })).toThrow(
        InvalidExecutionIdentityError,
      );
    }
  });

  it('captures identity and epoch fields once without freezing caller objects', () => {
    const counts = new Map<string, number>();
    const getters = (values: Record<string, unknown>, prefix = '') =>
      Object.defineProperties(
        {},
        Object.fromEntries(
          Object.entries(values).map(([key, value]) => [
            key,
            {
              enumerable: true,
              get() {
                const name = `${prefix}${key}`;
                const count = (counts.get(name) ?? 0) + 1;
                counts.set(name, count);
                return count === 1 ? value : 'changed';
              },
            },
          ]),
        ),
      );
    const owner = getters(START.owner, 'owner.');
    const target = getters(START.target, 'target.');
    const input = getters({ ...RUN, owner, target });
    const normalized = normalizeStartExecutionIdentity(input);
    expect(normalized).toEqual({ ...RUN, tablePrefix: 'tenant_', ...START });
    expect([...counts.values()].every((count) => count === 1)).toBe(true);
    expect(Object.isFrozen(normalized)).toBe(true);
    expect(Object.isFrozen(normalized.owner)).toBe(true);
    expect(Object.isFrozen(normalized.target)).toBe(true);
    expect(Object.isFrozen(input)).toBe(false);
    expect(Object.isFrozen(owner)).toBe(false);
    const mutable = {
      ...RUN,
      owner: { ...START.owner },
      target: { ...START.target },
    };
    const copy = normalizeStartExecutionIdentity(mutable);
    mutable.owner.id = 'other';
    mutable.target.id = 'other';
    mutable.startToken = 'other';
    expect(copy).toEqual(normalized);
    let epochReads = 0;
    let requirementReads = 0;
    assertMutationEpoch(
      {
        get mutationEpoch() {
          epochReads += 1;
          return epochReads === 1 ? 1 : 2;
        },
        get requireMutationEpoch() {
          requirementReads += 1;
          return true;
        },
      },
      1,
    );
    expect([epochReads, requirementReads]).toEqual([1, 1]);
    for (const invalid of ['private/path/token', 'sensitive owner\nvalue']) {
      try {
        normalizeStartExecutionIdentity({
          ...RUN,
          ...START,
          startToken: invalid,
        });
        expect.fail('invalid token was accepted');
      } catch (error) {
        expect(error).toBeInstanceOf(InvalidExecutionIdentityError);
        expect((error as Error).message).not.toContain(invalid);
        expect((error as InvalidExecutionIdentityError).reason).toEqual({
          code: 'INVALID_EXECUTION_IDENTITY',
        });
      }
    }
  });

  it('validates caller epochs and fails malformed readings closed', () => {
    expect(normalizeMutationEpoch(undefined)).toBeUndefined();
    expect(Object.is(normalizeMutationEpoch(-0), 0)).toBe(true);
    for (const value of [0, 1, Number.MAX_SAFE_INTEGER]) {
      expect(normalizeMutationEpoch(value)).toBe(value);
      expect(() =>
        assertMutationEpoch(
          { mutationEpoch: 0, requireMutationEpoch: false },
          value,
        ),
      ).not.toThrow();
    }
    for (const value of [
      null,
      false,
      '0',
      -1,
      0.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1,
    ]) {
      expect(() => normalizeMutationEpoch(value)).toThrow(
        InvalidMutationEpochError,
      );
    }
    expect(() =>
      assertMutationEpoch({ mutationEpoch: 1, requireMutationEpoch: true }, 1),
    ).not.toThrow();
    for (const [value, classification] of [
      [undefined, 'missing'],
      [0, 'stale'],
      [2, 'future'],
    ] as const) {
      try {
        assertMutationEpoch(
          { mutationEpoch: 1, requireMutationEpoch: true },
          value,
        );
        expect.fail('mismatch was accepted');
      } catch (error) {
        expect(error).toBeInstanceOf(MutationEpochMismatchError);
        expect((error as MutationEpochMismatchError).status).toBe(409);
        expect((error as MutationEpochMismatchError).reason).toEqual({
          code: 'MUTATION_EPOCH_MISMATCH',
          classification,
          mutationEpoch: 1,
        });
      }
    }
    for (const reading of [
      null,
      [],
      {},
      { mutationEpoch: '0', requireMutationEpoch: false },
      { mutationEpoch: -1, requireMutationEpoch: false },
      { mutationEpoch: 0.1, requireMutationEpoch: true },
      { mutationEpoch: 0, requireMutationEpoch: 0 },
      { mutationEpoch: 0, requireMutationEpoch: true },
      { mutationEpoch: 1, requireMutationEpoch: false },
    ]) {
      expect(() => assertMutationEpoch(reading as never, 0)).toThrow(
        ExecutionFenceUnreadableError,
      );
    }
  });

  it('round-trips only canonical mutation epoch headers', () => {
    expect(mutationEpochFromHeader(null)).toBeUndefined();
    for (const value of [0, 1, Number.MAX_SAFE_INTEGER]) {
      const headers = new Headers({ 'X-Flowsafe-Mutation-Epoch': 'forged' });
      stampMutationEpoch(headers, value);
      expect(headers.get(MUTATION_EPOCH_HEADER)).toBe(String(value));
      expect(mutationEpochFromHeader(headers.get(MUTATION_EPOCH_HEADER))).toBe(
        value,
      );
      stampMutationEpoch(headers, undefined);
      expect(headers.has(MUTATION_EPOCH_HEADER)).toBe(false);
    }
    for (const value of [
      '',
      ' 0',
      '0 ',
      '1\n',
      '1\r\n',
      '+1',
      '-0',
      '01',
      '1.0',
      '1e0',
      '1, 2',
      '9007199254740992',
      '1'.repeat(17),
    ]) {
      expect(() => mutationEpochFromHeader(value)).toThrow(
        InvalidMutationEpochError,
      );
    }
    const headers = new Headers({ [MUTATION_EPOCH_HEADER]: '1' });
    headers.append('X-Flowsafe-Mutation-Epoch', '2');
    expect(() =>
      mutationEpochFromHeader(headers.get(MUTATION_EPOCH_HEADER)),
    ).toThrow(InvalidMutationEpochError);
    const before = [...headers];
    expect(() => stampMutationEpoch(headers, 'private' as never)).toThrow(
      InvalidMutationEpochError,
    );
    expect([...headers]).toEqual(before);
  });

  it('preserves the unreadable error constructor across old and new imports', async () => {
    expect(ExecutionFenceUnreadableError).toBe(LegacyUnreadableError);
    expect(ExecutionFenceUnreadableError).toBe(RunnerUnreadableError);
    expect(ExecutionFenceUnreadableError).toBe(HostUnreadableError);
    const cause = new Error('underlying failure');
    const error = new ExecutionFenceUnreadableError('unreadable', { cause });
    expect(error).toBeInstanceOf(LegacyUnreadableError);
    expect(error.name).toBe('ExecutionFenceUnreadableError');
    expect(error.message).toBe('unreadable');
    expect(error.cause).toBe(cause);
    const response = doErrorResponse(error);
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({
      error: 'unreadable',
      reason: { code: 'EXECUTION_FENCE_UNREADABLE' },
    });
  });
});
