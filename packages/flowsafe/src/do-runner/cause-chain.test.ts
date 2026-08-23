// SPDX-License-Identifier: Apache-2.0
// The shared cause-chain walk, tested directly rather than only through the two
// stores that consult it.
//
// It is worth its own file because both of its callers use its answer in the
// dangerous direction: "this table does not exist" is what makes a pre-0.20
// database read as an OPEN fence, and what makes an unused key read as NO
// reservation — the answer that starts a run. The cases below are the ones a
// walker written twice gets wrong once: a cycle, a chain deeper than the bound,
// and a missing-table message that is a WRAPPER rather than the fault.

import { describe, expect, it } from 'vitest';

import { findInCauseChain, missingTableReadsEmpty } from './cause-chain.js';

/** An error chain, innermost last. */
function chain(...messages: string[]): Error {
  let current: Error | undefined;
  for (const message of [...messages].reverse()) {
    current =
      current === undefined
        ? new Error(message)
        : new Error(message, { cause: current });
  }
  return current as Error;
}

describe('findInCauseChain', () => {
  it('tests only the innermost error for a rootOnly search', () => {
    // #given a chain whose TOP link matches and whose root does not
    const error = chain('matches', 'does not');

    // #when / #then rootOnly answers about the fault itself, not about what
    // some wrapper mentioned on the way out.
    expect(
      findInCauseChain(error, (link) => String(link).includes('matches'), {
        rootOnly: true,
      }),
    ).toBe(false);
    expect(
      findInCauseChain(error, (link) => String(link).includes('does not'), {
        rootOnly: true,
      }),
    ).toBe(true);
  });

  it('answers on the first matching link when rootOnly is off', () => {
    // #given the same chain
    const error = chain('matches', 'does not');

    // #when / #then the any-level policy is a different claim, and the flag is
    // what makes which one a caller asked for visible at its call site.
    expect(
      findInCauseChain(error, (link) => String(link).includes('matches'), {
        rootOnly: false,
      }),
    ).toBe(true);
  });

  it('terminates on a cyclic chain instead of spinning', () => {
    // #given two errors that cause each other — reachable from any adapter
    // that re-wraps an error it is already carrying
    const inner = new Error('inner');
    const outer = new Error('outer', { cause: inner });
    (inner as { cause?: unknown }).cause = outer;

    // #then it returns, and it returns the CLOSED answer: the walk reached no
    // root, and an unobserved root is not evidence of anything.
    expect(findInCauseChain(inner, () => true, { rootOnly: true })).toBe(false);
  });

  it('degrades closed on a chain deeper than the bound', () => {
    // #given a chain longer than the depth bound, whose root WOULD match
    const error = chain(
      'w1',
      'w2',
      'w3',
      'w4',
      'w5',
      'w6',
      'w7',
      'w8',
      'w9',
      'root',
    );

    // #then the root was never observed, so the answer is `false` rather than
    // an unbounded walk or an optimistic guess.
    expect(
      findInCauseChain(error, (link) => String(link).includes('root'), {
        rootOnly: true,
      }),
    ).toBe(false);
  });
});

describe('missingTableReadsEmpty', () => {
  it('reads a wrapped SQLite miss as an empty table', () => {
    // #given the shape an adapter produces: its own message on top, the driver
    // text on `cause`. Reading only the top message here would turn a correctly
    // upgraded database into a permanent 503.
    const error = chain(
      'D1_ERROR: query failed',
      'no such table: flowsafe_execution_fence',
    );

    // #then
    expect(missingTableReadsEmpty(error, 'flowsafe_execution_fence')).toBe(
      true,
    );
  });

  it('refuses a missing-table message whose own cause is something else', () => {
    // #given a fault that merely PASSED this table on its way out — a failed
    // migration, an adapter reporting the last thing it saw
    const error = chain(
      'no such table: flowsafe_execution_fence',
      'D1_ERROR: connection reset',
    );

    // #then not an empty table. Concluding "there is no fence" from this is the
    // one answer that must never be wrong.
    expect(missingTableReadsEmpty(error, 'flowsafe_execution_fence')).toBe(
      false,
    );
  });

  it('refuses a miss naming a DIFFERENT table', () => {
    // #given — each store asks about its own table, and a deployment can have
    // one without the other
    const error = new Error('no such table: flowsafe_start_idempotency');

    // #then
    expect(missingTableReadsEmpty(error, 'flowsafe_execution_fence')).toBe(
      false,
    );
  });

  it('refuses an ordinary failure', () => {
    // #given / #then anything that is not a missing table is a real fault, and
    // both stores turn it into their own 503.
    expect(
      missingTableReadsEmpty(new Error('D1_ERROR: network'), 'any_table'),
    ).toBe(false);
  });
});
