// SPDX-License-Identifier: Apache-2.0
// Env-var parsing pins. The stakes are operational: numberVar guards the
// demo's spend caps (where a rejected `0` silently reverted an incident
// freeze to the 500-run fallback) and boolVar guards the kill switch (where
// any spelling but the one literal used to read as "demo stays up").

import { afterEach, describe, expect, it, vi } from 'vitest';

import { boolVar, numberVar, selfDecisionPolicyVar } from './env-vars.js';

const ROLES = ['admin', 'builder', 'operator', 'reviewer', 'viewer'] as const;

afterEach(() => {
  vi.restoreAllMocks();
});

describe('numberVar', () => {
  it('returns the fallback for unset or empty vars, silently', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(numberVar(undefined, 30, 'X')).toBe(30);
    expect(numberVar('', 30, 'X')).toBe(30);
    expect(errors).not.toHaveBeenCalled();
  });

  it('parses a positive number', () => {
    expect(numberVar('12', 30, 'X')).toBe(12);
  });

  it('logs a config-error and falls back on garbage', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(numberVar('soon', 30, 'X')).toBe(30);
    expect(errors).toHaveBeenCalledTimes(1);
  });

  it('rejects 0 by default (durations/TTLs are never meant to be zero)', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(numberVar('0', 3600, 'DEMO_JWT_TTL_SECONDS')).toBe(3600);
    expect(errors).toHaveBeenCalledTimes(1);
  });

  it('accepts 0 under allowZero — the incident freeze must not revert to the fallback', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when — DEMO_DAILY_RUN_CAP=0 mid-incident
    const value = numberVar('0', 500, 'DEMO_DAILY_RUN_CAP', {
      allowZero: true,
    });

    // #then — the freeze holds; no log claims a typo was caught
    expect(value).toBe(0);
    expect(errors).not.toHaveBeenCalled();
  });

  it('still rejects negatives under allowZero', () => {
    // #given
    vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(numberVar('-1', 500, 'X', { allowZero: true })).toBe(500);
  });
});

describe('boolVar', () => {
  it('reads unset/empty as false without logging', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(boolVar(undefined, 'X', { onInvalid: true })).toBe(false);
    expect(boolVar('', 'X', { onInvalid: true })).toBe(false);
    expect(errors).not.toHaveBeenCalled();
  });

  it.each([
    'true',
    '1',
    'yes',
    'on',
    'TRUE',
    ' On ',
  ])("parses '%s' as true", (raw) => {
    expect(boolVar(raw, 'X', { onInvalid: false })).toBe(true);
  });

  it.each([
    'false',
    '0',
    'no',
    'off',
    'FALSE',
  ])("parses '%s' as false", (raw) => {
    expect(boolVar(raw, 'X', { onInvalid: true })).toBe(false);
  });

  it('returns the caller-named fail-closed value on garbage, and logs', () => {
    // #given — a kill switch fed a typo must kill, not carry on
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(boolVar('disable-now', 'DEMO_DISABLED', { onInvalid: true })).toBe(
      true,
    );
    expect(errors).toHaveBeenCalledTimes(1);
  });
});

describe('selfDecisionPolicyVar', () => {
  it('reads unset/empty as OFF (SoD on) without logging', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(selfDecisionPolicyVar(undefined, 'X', ROLES)).toBe(false);
    expect(selfDecisionPolicyVar('', 'X', ROLES)).toBe(false);
    expect(errors).not.toHaveBeenCalled();
  });

  it.each([
    'true',
    '1',
    'yes',
    'on',
    'TRUE',
    ' On ',
  ])("parses '%s' as every-decider (true)", (raw) => {
    expect(selfDecisionPolicyVar(raw, 'X', ROLES)).toBe(true);
  });

  it.each([
    'false',
    '0',
    'no',
    'off',
    'FALSE',
  ])("parses '%s' as OFF (false)", (raw) => {
    expect(selfDecisionPolicyVar(raw, 'X', ROLES)).toBe(false);
  });

  it('parses a single role', () => {
    expect(selfDecisionPolicyVar('admin', 'X', ROLES)).toEqual({
      roles: ['admin'],
    });
  });

  it('parses a CSV of roles, case- and space-normalized', () => {
    expect(selfDecisionPolicyVar('Admin, Reviewer ', 'X', ROLES)).toEqual({
      roles: ['admin', 'reviewer'],
    });
  });

  it('falls back to OFF and logs when ANY token is not a role', () => {
    // #given — one bad token must not silently widen who can self-approve
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then
    expect(selfDecisionPolicyVar('admin,bogus', 'X', ROLES)).toBe(false);
    expect(selfDecisionPolicyVar('bogus', 'X', ROLES)).toBe(false);
    expect(errors).toHaveBeenCalledTimes(2);
  });

  it('falls back to OFF and logs when the list is empty after splitting', () => {
    // #given
    const errors = vi.spyOn(console, 'error').mockImplementation(() => {});

    // #when / #then — ',' / ', ,' / ' ' have no tokens
    expect(selfDecisionPolicyVar(',', 'X', ROLES)).toBe(false);
    expect(selfDecisionPolicyVar(' ', 'X', ROLES)).toBe(false);
    expect(errors).toHaveBeenCalledTimes(2);
  });

  it('tolerates a trailing comma (empty segment dropped)', () => {
    expect(selfDecisionPolicyVar('admin,', 'X', ROLES)).toEqual({
      roles: ['admin'],
    });
  });

  it('does not dedupe repeated roles (harmless, membership is what matters)', () => {
    expect(selfDecisionPolicyVar('admin,admin', 'X', ROLES)).toEqual({
      roles: ['admin', 'admin'],
    });
  });
});
