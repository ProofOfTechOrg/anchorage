// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import {
  approvalCursor,
  clampApprovalLimit,
  MAX_APPROVAL_LIST_LIMIT,
  OPEN_STATUSES,
  parseApprovalCursor,
  TERMINAL_APPROVAL_STATUSES,
} from './types.js';

describe('approvalCursor / parseApprovalCursor', () => {
  it('round-trips createdAt + id through the opaque cursor', () => {
    // #given / #when
    const cursor = approvalCursor({
      createdAt: '2026-07-06T12:00:00.000Z',
      id: 'apr-42',
    });

    // #then
    expect(parseApprovalCursor(cursor)).toEqual({
      createdAt: '2026-07-06T12:00:00.000Z',
      id: 'apr-42',
    });
  });

  it('is base64 of the pipe-joined createdAt and id — the documented wire shape', () => {
    // #when
    const cursor = approvalCursor({
      createdAt: '2026-07-06T12:00:00.000Z',
      id: 'apr-42',
    });

    // #then
    expect(atob(cursor)).toBe('2026-07-06T12:00:00.000Z|apr-42');
  });

  it('rejects a cursor that is not valid base64', () => {
    // #when / #then
    expect(() => parseApprovalCursor('not valid base64!!')).toThrow(
      /invalid approval cursor/,
    );
  });

  it('rejects a validly-encoded string with no delimiter', () => {
    // #when / #then
    expect(() => parseApprovalCursor(btoa('no-delimiter-here'))).toThrow(
      /invalid approval cursor/,
    );
  });

  it('rejects a cursor with an empty id half', () => {
    // #when / #then
    expect(() =>
      parseApprovalCursor(btoa('2026-07-06T12:00:00.000Z|')),
    ).toThrow(/invalid approval cursor/);
  });

  it('rejects a cursor with an empty createdAt half', () => {
    // #when / #then
    expect(() => parseApprovalCursor(btoa('|apr-42'))).toThrow(
      /invalid approval cursor/,
    );
  });
});

describe('clampApprovalLimit', () => {
  it('passes undefined through unchanged (no limit applied)', () => {
    expect(clampApprovalLimit(undefined)).toBeUndefined();
  });

  it('clamps a value above the max down to MAX_APPROVAL_LIST_LIMIT', () => {
    expect(clampApprovalLimit(10_000)).toBe(MAX_APPROVAL_LIST_LIMIT);
  });

  it('clamps a non-positive value up to 1', () => {
    expect(clampApprovalLimit(0)).toBe(1);
    expect(clampApprovalLimit(-5)).toBe(1);
  });

  it('truncates a fractional value', () => {
    expect(clampApprovalLimit(10.9)).toBe(10);
  });

  it('passes a value already inside the range through unchanged', () => {
    expect(clampApprovalLimit(42)).toBe(42);
  });

  it('clamps non-finite input to the max — never "no limit" — so a caller cannot reopen an unbounded query', () => {
    expect(clampApprovalLimit(Number.NaN)).toBe(MAX_APPROVAL_LIST_LIMIT);
    expect(clampApprovalLimit(Number.POSITIVE_INFINITY)).toBe(
      MAX_APPROVAL_LIST_LIMIT,
    );
    expect(clampApprovalLimit(Number.NEGATIVE_INFINITY)).toBe(
      MAX_APPROVAL_LIST_LIMIT,
    );
  });
});

describe('TERMINAL_APPROVAL_STATUSES', () => {
  it('is exactly the complement of OPEN_STATUSES', () => {
    const all = ['pending', 'claimed', 'approved', 'rejected', 'escalated'];
    const open = new Set(OPEN_STATUSES);
    const expectedTerminal = all.filter((status) => !open.has(status as never));
    expect([...TERMINAL_APPROVAL_STATUSES].sort()).toEqual(
      expectedTerminal.sort(),
    );
  });
});
