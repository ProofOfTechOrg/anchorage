// SPDX-License-Identifier: Apache-2.0
// safeDecodeSegment — the pre-auth path-decode guard shared by every P6 router.

import { describe, expect, it } from 'vitest';

import { safeDecodeSegment } from './route-path.js';

describe('safeDecodeSegment', () => {
  it('decodes a valid percent-encoded segment', () => {
    expect(safeDecodeSegment('a%2Fb')).toBe('a/b');
    expect(safeDecodeSegment('acme_t1')).toBe('acme_t1');
  });

  it('returns undefined on malformed percent-encoding instead of throwing', () => {
    // bare decodeURIComponent throws a URIError on each of these — the exact
    // pre-auth fault this guard exists to contain.
    expect(safeDecodeSegment('%')).toBeUndefined();
    expect(safeDecodeSegment('%zz')).toBeUndefined();
    expect(safeDecodeSegment('a%')).toBeUndefined();
  });

  it('passes a missing segment (undefined) straight through', () => {
    expect(safeDecodeSegment(undefined)).toBeUndefined();
  });
});
