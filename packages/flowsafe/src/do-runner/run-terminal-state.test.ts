// SPDX-License-Identifier: Apache-2.0

import type { WorkflowRunStatus } from '@mastra/core/workflows';
import { describe, expect, it } from 'vitest';
import {
  type CoreRunResult,
  errorText,
  isRunStatus,
  type RunStatus,
  terminalStateFields,
  terminalStateUpdate,
} from './run-terminal-state.js';

const CLEARED_FIELDS = {
  result: undefined,
  error: undefined,
  suspendedPaths: {},
  waitingPaths: {},
  resumeLabels: {},
  activePaths: [],
  activeStepsPath: {},
};

const CORE_RESULTS = {
  running: { status: 'running' },
  success: { status: 'success', result: { completed: true } },
  failed: { status: 'failed', error: 'step failed' },
  tripwire: { status: 'tripwire', tripwire: { reason: 'stop here' } },
  suspended: {
    status: 'suspended',
    suspended: [['approval']],
    suspendPayload: { reason: 'review' },
  },
  waiting: { status: 'waiting' },
  pending: { status: 'pending' },
  canceled: { status: 'canceled' },
  bailed: { status: 'bailed' },
  paused: { status: 'paused' },
  skipped: { status: 'skipped' },
} satisfies Record<WorkflowRunStatus, CoreRunResult>;

const RUN_STATUSES = {
  running: true,
  success: true,
  failed: true,
  tripwire: true,
  suspended: true,
  waiting: true,
  pending: true,
  canceled: true,
  bailed: true,
  paused: true,
  skipped: true,
  waiting_callback: true,
  waiting_signal: true,
  retry_wait: true,
  cancelled: true,
  timed_out: true,
} satisfies Record<RunStatus, true>;

describe('isRunStatus', () => {
  it.each(
    Object.keys(RUN_STATUSES),
  )('recognizes existing status %s', (status) => {
    expect(isRunStatus(status)).toBe(true);
  });

  it.each([
    undefined,
    null,
    false,
    0,
    {},
    [],
    { status: 'running' },
    '',
    'completed',
    'error',
    'RUNNING',
    ' running',
  ])('rejects a value outside the existing status union: %j', (value) => {
    expect(isRunStatus(value)).toBe(false);
  });
});

describe('terminalStateFields', () => {
  it.each(
    Object.keys(RUN_STATUSES) as RunStatus[],
  )('keeps %s and explicitly clears every common terminal field', (status) => {
    expect(terminalStateFields(status)).toStrictEqual({
      status,
      ...CLEARED_FIELDS,
    });
  });

  it('does not share mutable control-path containers between projections', () => {
    const first = terminalStateFields('cancelled');
    const second = terminalStateFields('timed_out');
    for (const field of [
      'suspendedPaths',
      'waitingPaths',
      'resumeLabels',
      'activePaths',
      'activeStepsPath',
    ] as const) {
      expect(first[field]).not.toBe(second[field]);
    }
  });
});

describe('terminalStateUpdate', () => {
  it.each([
    'running',
    'suspended',
    'waiting',
    'pending',
    'paused',
  ] as const)('does not overwrite the existing nonterminal %s state', (status) => {
    const result = CORE_RESULTS[status];
    const before = structuredClone(result);
    expect(terminalStateUpdate(result)).toBeUndefined();
    expect(result).toStrictEqual(before);
  });

  it.each([
    'tripwire',
    'canceled',
    'bailed',
    'skipped',
  ] as const)('preserves the existing common-only %s projection', (status) => {
    expect(terminalStateUpdate(CORE_RESULTS[status])).toStrictEqual({
      status,
      ...CLEARED_FIELDS,
    });
  });

  it.each([
    [undefined],
    [null],
    [false],
    [0],
    ['done'],
    [[1, 2]],
    [{ completed: true }],
  ])('preserves the successful result without coercion: %j', (result) => {
    const update = terminalStateUpdate({ status: 'success', result });
    expect(update).toStrictEqual({
      status: 'success',
      ...CLEARED_FIELDS,
      result,
    });
    expect(update?.result).toBe(result);
  });

  it('preserves an Error name, message and stack without its other fields', () => {
    const error = new Error('connector failed', { cause: new Error('cause') });
    error.name = 'ConnectorError';
    error.stack = 'ConnectorError: connector failed\n  at step';
    expect(terminalStateUpdate({ status: 'failed', error })).toStrictEqual({
      status: 'failed',
      ...CLEARED_FIELDS,
      error: {
        name: 'ConnectorError',
        message: 'connector failed',
        stack: 'ConnectorError: connector failed\n  at step',
      },
    });
    expect(error.cause).toBeInstanceOf(Error);
  });

  it.each([
    ['plain failure', { name: 'Error', message: 'plain failure' }],
    [null, { name: 'Error', message: 'null' }],
    [undefined, { name: 'Error', message: 'undefined' }],
    [17, { name: 'Error', message: '17' }],
    [
      {
        name: 'SerializedError',
        message: 'stored failure',
        stack: 'stored stack',
        detail: 'not projected',
      },
      {
        name: 'SerializedError',
        message: 'stored failure',
        stack: 'stored stack',
      },
    ],
    [
      { name: '', message: '', stack: '' },
      { name: '', message: '', stack: '' },
    ],
    [
      { name: 3, message: 'only message', stack: false },
      { name: 'Error', message: 'only message' },
    ],
    [
      { name: 'NamedError', message: 3, stack: 8 },
      { name: 'NamedError', message: '[object Object]' },
    ],
  ])('keeps the existing serialized failure projection: %j', (error, expected) => {
    expect(terminalStateUpdate({ status: 'failed', error })).toStrictEqual({
      status: 'failed',
      ...CLEARED_FIELDS,
      error: expected,
    });
  });

  it('retains supported inherited serialized error fields', () => {
    const error: unknown = Object.create({
      name: 'InheritedError',
      message: 'inherited failure',
      stack: 'inherited stack',
    });
    expect(terminalStateUpdate({ status: 'failed', error })).toStrictEqual({
      status: 'failed',
      ...CLEARED_FIELDS,
      error: {
        name: 'InheritedError',
        message: 'inherited failure',
        stack: 'inherited stack',
      },
    });
  });

  it('projects the fixed unknown-effects failure without inventing a stack', () => {
    const error = {
      name: 'StartOutcomeUnknown',
      message:
        'Start interrupted before a durable execution outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.',
    };
    expect(terminalStateUpdate({ status: 'failed', error })).toStrictEqual({
      status: 'failed',
      ...CLEARED_FIELDS,
      error: {
        name: 'StartOutcomeUnknown',
        message:
          'Start interrupted before a durable execution outcome was recorded; external effects may have occurred. This run will not be automatically re-executed.',
      },
    });
    expect(error).not.toHaveProperty('stack');
  });
});

describe('errorText', () => {
  it.each([
    [new Error('native failure'), 'native failure'],
    [new Error(''), ''],
    ['string failure', 'string failure'],
    ['', ''],
    [{ message: 'serialized failure' }, 'serialized failure'],
    [{ message: '' }, ''],
    [null, 'null'],
    [undefined, 'undefined'],
    [false, 'false'],
    [42, '42'],
    [Symbol('failure'), 'Symbol(failure)'],
    [{ message: 42 }, '[object Object]'],
    [{ message: 42, toString: () => 'custom fallback' }, 'custom fallback'],
  ])('preserves the existing error text for %j', (error, expected) => {
    expect(errorText(error)).toBe(expected);
  });

  it('preserves a fault from the existing fallback conversion', () => {
    const fault = new Error('conversion failed');
    expect(() =>
      errorText({
        toString: () => {
          throw fault;
        },
      }),
    ).toThrow(fault);
  });
});
