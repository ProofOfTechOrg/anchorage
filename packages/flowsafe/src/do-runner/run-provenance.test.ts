// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import { ExecutionFenceUnreadableError } from './execution-admission.js';
import {
  decodeInitialRunProvenance,
  decodeRunStartIdentity,
  runExecutionIdentityFor,
} from './run-provenance.js';

const START = {
  owner: { kind: 'human', id: 'Alice' },
  target: { kind: 'workflow', id: 'parent' },
};
const INITIAL = {
  version: 2,
  startToken: 'server-generation',
  attemptToken: 'host-correlation',
  requestedBy: 'Alice',
  requestedByKind: 'human',
  startIdentity: START,
  resumeCounts: [],
};

describe('run provenance', () => {
  it('reads inherited and pruned provenance without inventing root authority', () => {
    const decoded = decodeRunStartIdentity(INITIAL);
    expect(decoded).toEqual({
      version: 2,
      startToken: INITIAL.startToken,
      startIdentity: START,
    });
    if (!decoded) throw new Error('expected modern data');
    expect(
      runExecutionIdentityFor(
        { tablePrefix: 'Sibling_', workflowId: 'child', runId: 'run' },
        decoded,
      ),
    ).toEqual({
      tablePrefix: 'sibling_',
      workflowId: 'child',
      runId: 'run',
      startToken: INITIAL.startToken,
    });
    expect(
      runExecutionIdentityFor(
        { tablePrefix: null, workflowId: 'parent', runId: 'run' },
        decoded,
      ).tablePrefix,
    ).toBeNull();
    expect(Object.isFrozen(decoded.startIdentity?.owner)).toBe(true);
    expect(decodeRunStartIdentity(undefined)).toBeUndefined();
    expect(decodeRunStartIdentity({ version: 1 })).toBeUndefined();
  });

  it.each([
    null,
    false,
    2,
    [],
    {},
    { version: 3 },
    { version: 2 },
    { ...INITIAL, startToken: '' },
    {
      ...INITIAL,
      startIdentity: { ...START, target: { kind: 'workflow', id: '' } },
    },
    { ...INITIAL, agentStart: { threaded: false } },
  ])('rejects malformed or unknown modern data %#', (value) => {
    expect(() => decodeRunStartIdentity(value)).toThrow(
      ExecutionFenceUnreadableError,
    );
  });

  it('separates generation and correlation and requires the selected marker mode', () => {
    expect(decodeInitialRunProvenance(INITIAL, 'absent')).toEqual(INITIAL);
    const marked = { ...INITIAL, initialAdmission: true };
    expect(decodeInitialRunProvenance(marked, 'present')).toEqual(marked);
    expect(() => decodeInitialRunProvenance(marked, 'absent')).toThrow(
      ExecutionFenceUnreadableError,
    );
    expect(() => decodeInitialRunProvenance(INITIAL, 'present')).toThrow(
      ExecutionFenceUnreadableError,
    );
    expect(
      decodeInitialRunProvenance(
        { version: 2, startToken: 'S', attemptToken: 'H', resumeCounts: [] },
        'absent',
      ),
    ).toEqual({
      version: 2,
      startToken: 'S',
      attemptToken: 'H',
      resumeCounts: [],
    });
  });

  it.each([
    { version: 1 },
    { attemptToken: '' },
    { resumeCounts: new Array(1) },
    { resumeCounts: [['step', 0]] },
    { resumeCounts: {} },
    { requestedBy: undefined },
    { startIdentity: undefined },
    { requestedByKind: undefined },
    { requestedBy: 'Bob' },
    { requestedByKind: 'other' },
    { mutationEpoch: '0' },
    { mutationEpoch: -1 },
    { mutationEpoch: Number.MAX_SAFE_INTEGER + 1 },
    { initialAdmission: false },
  ])('refuses inconsistent initial fields %#', (patch) => {
    expect(() =>
      decodeInitialRunProvenance({ ...INITIAL, ...patch }, 'absent'),
    ).toThrow(ExecutionFenceUnreadableError);
  });

  it('requires agent mode only for an agent identity', () => {
    const agent = {
      ...INITIAL,
      startIdentity: {
        owner: START.owner,
        target: { kind: 'agent', id: 'agent', threadId: 'thread' },
      },
      agentStart: { threaded: false },
    };
    expect(decodeInitialRunProvenance(agent, 'absent').agentStart).toEqual({
      threaded: false,
    });
    for (const agentStart of [undefined, {}, null, { threaded: 0 }]) {
      expect(() =>
        decodeInitialRunProvenance({ ...agent, agentStart }, 'absent'),
      ).toThrow(ExecutionFenceUnreadableError);
    }
  });

  it('captures each owned getter once and returns fresh frozen data', () => {
    const calls = new Map<string, number>();
    const source = Object.fromEntries(Object.entries(INITIAL));
    for (const [key, value] of Object.entries(INITIAL))
      Object.defineProperty(source, key, {
        get() {
          calls.set(key, (calls.get(key) ?? 0) + 1);
          return value;
        },
      });
    const result = decodeInitialRunProvenance(source, 'absent');
    expect([...calls.values()]).toEqual(Object.keys(INITIAL).map(() => 1));
    expect(result).not.toBe(source);
    expect(Object.isFrozen(result)).toBe(true);
    expect(Object.isFrozen(result.resumeCounts)).toBe(true);
    expect(result.startIdentity).not.toBe(START);
  });
});
