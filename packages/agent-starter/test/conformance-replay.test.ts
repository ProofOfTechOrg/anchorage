// SPDX-License-Identifier: Apache-2.0

import {
  ApprovalConflictError,
  type ApprovalStatus,
} from '@proofoftech/flowsafe/approval-api';
import { RunRouteError } from '@proofoftech/flowsafe/host-kit';
import { describe, expect, it } from 'vitest';

import { isExpectedReplayRefusal } from '../src/conformance/state-durable-objects.js';

/**
 * The gate reads `rejected: true` plus HTTP 409 as proof that FlowSafe refuses
 * a replayed decision and a raw resume. With a blanket catch it would read the
 * same for ANY failure, so those two actions would stay green with the
 * defenses removed — a green run proving nothing. These cases pin the
 * distinction the gate cannot make for itself.
 */
describe('replay refusal classification', () => {
  it('accepts the decision compare-and-swap refusing an already-approved record', () => {
    expect(
      isExpectedReplayRefusal(
        'flowsafe-replay-decision',
        new ApprovalConflictError('approval-1', 'decide', 'approved'),
      ),
    ).toBe(true);
  });

  it.each<ApprovalStatus>([
    'pending',
    'claimed',
    'rejected',
    'escalated',
  ])('rejects a conflict whose current status is %s', (status) => {
    expect(
      isExpectedReplayRefusal(
        'flowsafe-replay-decision',
        new ApprovalConflictError('approval-1', 'decide', status),
      ),
    ).toBe(false);
  });

  it('accepts the runner refusing a raw resume because it is not suspended', () => {
    expect(
      isExpectedReplayRefusal(
        'flowsafe-replay-resume',
        new RunRouteError(409, "run 'r' is 'success', not 'suspended'"),
      ),
    ).toBe(true);
  });

  it.each([
    ['a transport failure', new TypeError('fetch failed')],
    ['an unrelated bug', new Error('cannot read properties of undefined')],
    ['a server fault from the runner', new RunRouteError(500, 'internal')],
    ['a nested contract failure', new RunRouteError(503, 'unavailable')],
    // These three are 4xx and would pass a status-class test, but each means
    // the request never reached the anti-replay bar: an unknown run, a
    // malformed body, or a body over the limit.
    ['an unknown run', new RunRouteError(404, "no run 'r' found")],
    ['a malformed resume body', new RunRouteError(400, 'invalid resume data')],
    ['an oversized body', new RunRouteError(413, 'payload too large')],
  ])('rejects %s on the resume replay', (_label, error) => {
    expect(isExpectedReplayRefusal('flowsafe-replay-resume', error)).toBe(
      false,
    );
  });

  it('does not accept one action’s refusal for the other', () => {
    expect(
      isExpectedReplayRefusal(
        'flowsafe-replay-resume',
        new ApprovalConflictError('approval-1', 'decide', 'approved'),
      ),
    ).toBe(false);
    expect(
      isExpectedReplayRefusal(
        'flowsafe-replay-decision',
        new RunRouteError(409, 'not suspended'),
      ),
    ).toBe(false);
  });
});
