// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import { createWebhookSignalProvider } from './provider.js';

const buildNotification = () => ({
  source: 'test',
  kind: 'changed',
  summary: 'changed',
});

describe('createWebhookSignalProvider', () => {
  it.each([
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ])('rejects an invalid poll interval synchronously: %s', (pollInterval) => {
    expect(() =>
      createWebhookSignalProvider({
        id: 'poller',
        pollInterval,
        buildNotification,
      }),
    ).toThrow(RangeError);
  });

  it('accepts zero as an explicit no-automatic-polling interval', () => {
    expect(
      createWebhookSignalProvider({
        id: 'webhook',
        pollInterval: 0,
        buildNotification,
      }).pollInterval,
    ).toBe(0);
  });

  it('describes the provider id as a stable slug, not a composite DO name', () => {
    expect(() =>
      createWebhookSignalProvider({
        id: 'not_valid',
        buildNotification,
      }),
    ).toThrow('stable lowercase URL/config slug');
  });
});
