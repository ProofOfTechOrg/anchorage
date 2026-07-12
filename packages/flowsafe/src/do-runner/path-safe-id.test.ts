// SPDX-License-Identifier: Apache-2.0
// Character-level pins for the two id charsets. TENANT_ID_PATTERN (INV-3) is
// load-bearing beyond validation: the tenant is recovered from runIds by
// prefix (`${tenantId}_`) and purged by the range [`${tid}_`, `${tid}\x60`),
// and BOTH are exact only because no valid tenantId character falls in
// [0x5F, 0x60]. A future loosening of the charset must fail here, not in a
// cross-tenant purge.

import { describe, expect, it } from 'vitest';

import {
  assertMintableTenantId,
  mintSaltedId,
  PATH_SAFE_ID_PATTERN,
  TENANT_ID_PATTERN,
  tenantOfRunId,
  tenantOwnsSaltedId,
} from './path-safe-id.js';

describe('TENANT_ID_PATTERN (INV-3)', () => {
  it('accepts lowercase alphanumeric slugs from 3 to 32 chars', () => {
    for (const id of ['abc', 'acme', 'metamind', 'a1b2c3', 'x'.repeat(32)]) {
      expect(TENANT_ID_PATTERN.test(id)).toBe(true);
    }
  });

  it.each([
    ['too short', 'ab'],
    ['too long', 'a'.repeat(33)],
    ['uppercase', 'Acme'],
    ['underscore (the runId delimiter)', 'a_b'],
    ['backtick (the range terminator)', 'a`b'],
    ['hyphen', 'a-b'],
    ['dot', 'a.b'],
    ['empty', ''],
    ['literal undefined coercion target', 'UNDEFINED'],
  ])('rejects %s', (_label, id) => {
    expect(TENANT_ID_PATTERN.test(id)).toBe(false);
  });

  it("accepts the string 'undefined' — which is WHY callers must validate before concatenating", () => {
    // 'undefined' is nine lowercase letters: INV-3-valid by construction. An
    // unvalidated `String(undefined)` would therefore silently authorize a
    // tenant literally named 'undefined'. The ownership check validates the
    // CLAIM against the token, so this is a documented sharp edge, not a
    // pattern bug.
    expect(TENANT_ID_PATTERN.test('undefined')).toBe(true);
  });

  it('admits NO character in [0x5F, 0x60] — exhaustive over the byte range', () => {
    // #given — every single-character extension of a valid stem
    for (let code = 0; code <= 0xff; code += 1) {
      const char = String.fromCharCode(code);
      const accepted = TENANT_ID_PATTERN.test(`aa${char}`);

      // #then — if the pattern accepts the character anywhere, it must be
      // strictly below '_' (0x5F) and strictly above '`' (0x60): digits
      // 0x30-0x39 or lowercase letters 0x61-0x7A. This is the property the
      // prefix-exactness and range-purge proofs rest on.
      const isDigit = code >= 0x30 && code <= 0x39;
      const isLowerAlpha = code >= 0x61 && code <= 0x7a;
      expect(accepted).toBe(isDigit || isLowerAlpha);
      if (accepted) {
        expect(code === 0x5f || code === 0x60).toBe(false);
      }
    }
  });

  it('every valid tenantId is also PATH_SAFE (runIds embed it)', () => {
    // The salted runId `${tenantId}_${uuid}` must pass PATH_SAFE_ID_PATTERN
    // end-to-end; the tenant segment can only use a subset of its charset.
    for (const id of ['abc', 'acme', 'x'.repeat(32)]) {
      expect(
        PATH_SAFE_ID_PATTERN.test(`${id}_9f2c7d4e-0000-4000-8000-000000000000`),
      ).toBe(true);
    }
  });
});

describe('tenantOfRunId — the ONE INV-1 decode', () => {
  it('extracts the validated tenant prefix', () => {
    expect(tenantOfRunId('acme_9f2c-uuid')).toBe('acme');
    expect(tenantOfRunId('dm0011223344556677_x')).toBe('dm0011223344556677');
  });

  it("splits on the FIRST underscore — a uuid half containing '_' cannot confuse it", () => {
    // INV-3 excludes '_' from tenantId, so the first one is always the boundary
    expect(tenantOfRunId('acme_bravo_x')).toBe('acme');
  });

  it.each([
    ['no separator', 'bare-run'],
    ['leading separator', '_uuid'],
    ['too-short tenant', 'ab_uuid'],
    ['uppercase tenant', 'ACME_uuid'],
    ['hyphenated tenant', 'ac-me_uuid'],
    ['empty', ''],
  ])('returns undefined for %s (callers fail closed on their own terms)', (_label, runId) => {
    expect(tenantOfRunId(runId)).toBeUndefined();
  });

  it('rejects a non-INV-3 prefix that a bare indexOf would have accepted — the drift this centralization closes', () => {
    // A hand-rolled `runId.slice(0, indexOf('_'))` returns 'AB' here; the
    // canonical decoder validates and refuses.
    expect('AB_x'.slice(0, 'AB_x'.indexOf('_'))).toBe('AB');
    expect(tenantOfRunId('AB_x')).toBeUndefined();
  });
});

describe('tenantOwnsSaltedId — the ONE tenant-salted ownership predicate', () => {
  it('is exact at the tenant boundary (the acme vs acmecorp pin)', () => {
    // #given — INV-3 excludes '_'/backtick, so the first delimiter is the
    // boundary and 'acme' can never own 'acmecorp_...'
    // #when / #then
    expect(tenantOwnsSaltedId('acme', 'acme_x')).toBe(true);
    expect(tenantOwnsSaltedId('acme', 'acmecorp_x')).toBe(false);
    expect(tenantOwnsSaltedId('acmecorp', 'acme_x')).toBe(false);
  });

  it('rejects the bare tenant id (no delimiter, no suffix)', () => {
    // #when / #then — the trailing '_' delimiter is load-bearing
    expect(tenantOwnsSaltedId('acme', 'acme')).toBe(false);
  });
});

describe('assertMintableTenantId — the mint-path precondition (generic Error)', () => {
  it.each([
    ['uppercase', 'ACME'],
    ['too short', 'ab'],
    ['underscore (the runId delimiter)', 'a_b'],
  ])('throws INV-3 for %s', (_label, tenantId) => {
    // #when / #then
    expect(() => assertMintableTenantId(tenantId, 'mint')).toThrow(/INV-3/);
  });

  it("throws for the reserved 'system' identity", () => {
    // #when / #then
    expect(() => assertMintableTenantId('system', 'mint')).toThrow(/reserved/);
  });

  it('does not throw for a valid INV-3 tenantId', () => {
    // #when / #then
    expect(() => assertMintableTenantId('acme', 'mint')).not.toThrow();
  });
});

describe('mintSaltedId — the ONE salted-id constructor', () => {
  it('salts the suffix onto the validated tenant', () => {
    // #given / #when / #then
    expect(mintSaltedId('acme', 'sfx', 't')).toBe('acme_sfx');
  });

  it('propagates the mint-path assert for a bad tenant', () => {
    // #when / #then
    expect(() => mintSaltedId('ACME', 'sfx', 't')).toThrow(/INV-3/);
    expect(() => mintSaltedId('system', 'sfx', 't')).toThrow(/reserved/);
  });

  it('accepts a thunk suffix, evaluated onto the validated tenant', () => {
    // #given / #when / #then — a suffix producer (a uuid generator) passed lazily
    expect(mintSaltedId('acme', () => 'sfx', 't')).toBe('acme_sfx');
  });

  it('validates the tenant BEFORE evaluating a thunk suffix (no side effect, no masking throw)', () => {
    // #given — a suffix producer that records whether it ran
    let calls = 0;
    const suffix = () => {
      calls += 1;
      return 'sfx';
    };
    // #when / #then — an invalid then reserved tenant is rejected...
    expect(() => mintSaltedId('ACME', suffix, 't')).toThrow(/INV-3/);
    expect(() => mintSaltedId('system', suffix, 't')).toThrow(/reserved/);
    // ...and the thunk never ran, so its side effects can't precede the rejection
    expect(calls).toBe(0);
  });
});
