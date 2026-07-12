// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  mintResourceId,
  mintThreadId,
  tenantOfMemoryId,
  tenantOwnsMemoryId,
} from './memory-id.js';
import { tenantOfRunId } from './path-safe-id.js';

describe('mintThreadId', () => {
  it('mints the INV-1 carrier over the injected uuid seam', () => {
    // #given / #when
    const threadId = mintThreadId('acme', () => 'uuid-1');
    // #then
    expect(threadId).toBe('acme_uuid-1');
    expect(tenantOfMemoryId(threadId)).toBe('acme');
  });

  it('defaults to crypto.randomUUID', () => {
    // #when
    const threadId = mintThreadId('acme');
    // #then — `${tenantId}_${uuid}`; the uuid half is a v4 UUID
    expect(threadId).toMatch(
      /^acme_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
  });

  it('refuses non-INV-3 and reserved tenant ids', () => {
    // #given — uppercase, too short, delimiter-carrying, reserved
    for (const tenantId of ['ACME', 'ab', 'abc_d', 'system']) {
      // #when / #then
      expect(() => mintThreadId(tenantId)).toThrow(/INV-3|reserved/);
    }
  });
});

describe('mintResourceId', () => {
  it('salts the business key so two tenants sharing it stay disjoint', () => {
    // #given — the leak class: both tenants key memory by the same user
    const a = mintResourceId('acme', 'user-1');
    const b = mintResourceId('globex', 'user-1');
    // #then
    expect(a).toBe('acme_user-1');
    expect(b).toBe('globex_user-1');
    expect(a).not.toBe(b);
  });

  it("keeps the tenant decode exact when the key itself contains '_'", () => {
    // #given — INV-3 excludes '_' from tenant ids, so the FIRST underscore
    // is the boundary no matter what the key half contains
    const resourceId = mintResourceId('acme', 'user_1_east');
    // #when / #then
    expect(resourceId).toBe('acme_user_1_east');
    expect(tenantOfMemoryId(resourceId)).toBe('acme');
  });

  it('refuses keys outside PATH_SAFE_ID_PATTERN', () => {
    // #given — empty, dot-segments, separator chars, over-length
    for (const key of ['', '.', '..', 'a/b', 'a b', 'a:b', 'x'.repeat(201)]) {
      // #when / #then
      expect(() => mintResourceId('acme', key)).toThrow(/PATH_SAFE_ID_PATTERN/);
    }
  });

  it('refuses non-INV-3 and reserved tenant ids', () => {
    // #when / #then
    expect(() => mintResourceId('ACME', 'user-1')).toThrow(/INV-3/);
    expect(() => mintResourceId('system', 'user-1')).toThrow(/reserved/);
  });
});

describe('tenantOfMemoryId', () => {
  it('delegates to the ONE salted-id decode (tenantOfRunId)', () => {
    // #given — memory ids share the runId carrier by design
    for (const id of ['acme_t1', 'no-separator', '_leading', 'ACME_t1']) {
      // #then — byte-identical behavior, so the two parses can never drift
      expect(tenantOfMemoryId(id)).toBe(tenantOfRunId(id));
    }
  });

  it('returns undefined for unsalted or invalid prefixes', () => {
    // #when / #then
    expect(tenantOfMemoryId('bare')).toBeUndefined();
    expect(tenantOfMemoryId('_t1')).toBeUndefined();
    expect(tenantOfMemoryId('AB_t1')).toBeUndefined();
  });
});

describe('tenantOwnsMemoryId', () => {
  it('is exact at the tenant boundary (the acme vs acmecorp pin)', () => {
    // #given — the delimiter cannot occur inside a tenantId (INV-3)
    const foreign = mintThreadId('acmecorp', () => 't1');
    const own = mintThreadId('acme', () => 't1');
    // #when / #then
    expect(tenantOwnsMemoryId('acme', own)).toBe(true);
    expect(tenantOwnsMemoryId('acme', foreign)).toBe(false);
    expect(tenantOwnsMemoryId('acmecorp', own)).toBe(false);
  });

  it('rejects the bare tenant id (no delimiter, no suffix)', () => {
    // #when / #then
    expect(tenantOwnsMemoryId('acme', 'acme')).toBe(false);
  });
});
