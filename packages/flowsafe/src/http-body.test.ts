// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';

import { readBoundedBytes } from './http-body.js';

describe('readBoundedBytes', () => {
  it.each([
    -1,
    1.5,
    Number.MAX_SAFE_INTEGER + 1,
  ])('rejects invalid maxBytes %s', async (maxBytes) => {
    await expect(
      readBoundedBytes(new Request('https://example.test'), maxBytes),
    ).rejects.toThrow('maxBytes must be a nonnegative safe integer');
  });

  it('keeps the payload-too-large verdict when stream cancellation rejects', async () => {
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1, 2]));
      },
      cancel() {
        throw new Error('cancel failed');
      },
    });
    const request = { headers: new Headers(), body } as Request;

    await expect(readBoundedBytes(request, 1)).resolves.toEqual({
      ok: false,
      reason: 'payload-too-large',
      bytesRead: 2,
    });
  });
});
