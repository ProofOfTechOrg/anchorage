// SPDX-License-Identifier: Apache-2.0
// The delivery-outcome rule both provider lanes share. These pin the two
// branches that exist to STOP an authentic provider event from being dropped:
// a sub-500 status that is nonetheless transient, and a thrown value that only
// looks like it carries a deliberate status. A regression in either silently
// discards notifications, which is exactly the failure the classifier replaced.

import { describe, expect, it } from 'vitest';

import { RunRouteError } from '../host-kit/run-route-error.js';
import { classifyDeliveryError, classifyDeliveryResponse } from './delivery.js';

describe('classifyDeliveryResponse', () => {
  it.each([
    { status: 200, outcome: 'delivered' },
    { status: 204, outcome: 'delivered' },
    // The content policy's terminal refusal: identical bytes would be denied
    // again, so redelivering is pointless rather than merely wasteful.
    { status: 422, outcome: 'denied' },
    // Deterministic for identical bytes — a malformed notification, a
    // malformed stored thread id, a missing resource, a refused identity.
    { status: 400, outcome: 'failed' },
    { status: 403, outcome: 'failed' },
    { status: 404, outcome: 'failed' },
    { status: 409, outcome: 'failed' },
    { status: 413, outcome: 'failed' },
    // Transient despite being 4xx: the same bytes can succeed later.
    { status: 408, outcome: 'deferred' },
    { status: 429, outcome: 'deferred' },
    // The deployment could not decide — policy evaluator down, storage down,
    // the Durable Object failing.
    { status: 500, outcome: 'deferred' },
    { status: 502, outcome: 'deferred' },
    { status: 503, outcome: 'deferred' },
  ])('classifies $status as $outcome', ({ status, outcome }) => {
    expect(classifyDeliveryResponse(status)).toBe(outcome);
  });
});

describe('classifyDeliveryError', () => {
  it.each([
    { case: 'a routed 404', error: new RunRouteError(404, 'no such thread') },
    { case: 'a routed 409', error: new RunRouteError(409, 'conflict') },
  ])('treats $case as a terminal failure', ({ error }) => {
    expect(classifyDeliveryError(error)).toBe('failed');
  });

  it('treats a routed content refusal as a denial, not a retry', () => {
    expect(classifyDeliveryError(new RunRouteError(422, 'denied'))).toBe(
      'denied',
    );
  });

  it.each([
    {
      case: 'a routed transient status',
      error: new RunRouteError(429, 'slow down'),
    },
    { case: 'a routed server error', error: new RunRouteError(503, 'down') },
    { case: 'an ordinary error', error: new Error('socket hang up') },
    { case: 'a non-error throw', error: 'boom' },
    { case: 'nothing at all', error: undefined },
  ])('defers $case so the event comes back', ({ error }) => {
    expect(classifyDeliveryError(error)).toBe('deferred');
  });

  // The regression this classifier was extracted to fix: a structural read of
  // `.status` would have called this terminal and dropped the notification.
  // Only a status the topology assigned deliberately may be trusted.
  it('defers a foreign object that merely carries a numeric status', () => {
    expect(classifyDeliveryError({ status: 400, message: 'not ours' })).toBe(
      'deferred',
    );
    expect(classifyDeliveryError({ status: 404 })).toBe('deferred');
  });

  // RunRouteError.status is an open number, so a 2xx is constructible even
  // though nothing builds one today. A throw is never a delivery.
  it('never reports a throw as delivered', () => {
    expect(classifyDeliveryError(new RunRouteError(200, 'thrown anyway'))).toBe(
      'deferred',
    );
  });
});
