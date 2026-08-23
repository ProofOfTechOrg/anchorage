// SPDX-License-Identifier: Apache-2.0
// Reading an error's `cause` chain, safely — and the one classification every
// lazily-created flowsafe table needs from it.
//
// WHY A SHARED MODULE. Two stores (the execution fence, the start reservation)
// have to answer the same question about the same kind of failure: "did this
// read fail because the table does not exist yet, or because something is
// wrong?" Both answers are load-bearing in the dangerous direction — "the table
// does not exist" is what makes a pre-0.20 database read as an OPEN fence and
// what makes an unused key read as NO reservation, and the second of those is
// the answer that STARTS A RUN. Two hand-written walkers is two chances for one
// of them to lose the depth bound, the cycle guard, or the root-only rule.
//
// This module imports NOTHING, so the modules that consult it can keep their
// own "imports only leaves" property — the fence and the reservation store are
// both imported by surfaces that must not drag the D1 storage adapter (and
// @mastra/cloudflare-d1 with it) into their bundle.
//
// F2 (the drain inventory) reads the reservation table on every sweep and needs
// the same classification: import `missingTableReadsEmpty` from HERE, not from
// start-idempotency.ts, and pass the table it is reading.

/**
 * How far down a `cause` chain any walk here looks.
 *
 * Bounded because a chain can be cyclic or adversarially deep, and these walks
 * run on the read that fronts every gated request. A chain that does not
 * terminate within the bound has NO REACHABLE ROOT, which is not the same as
 * having a root that fails the predicate — see `findInCauseChain`.
 */
const MAX_CAUSE_CHAIN_DEPTH = 8;

export interface CauseChainSearch {
  /**
   * `true`  test ONLY the innermost error, after descending the whole chain.
   * `false` test every link, and answer `true` on the first that matches.
   *
   * Required rather than defaulted, because the two are different CLAIMS and a
   * caller that has not thought about which it wants has not thought about the
   * question. Root-only says "the fault IS this thing"; any-level says "the
   * fault MENTIONS this thing somewhere on its way out", which a failed
   * migration or an adapter reporting the last thing it saw also satisfies.
   */
  rootOnly: boolean;
}

/**
 * Walk `error.cause` and test the links this search asks for.
 *
 * A ROOT is a link that nothing caused. Only that link answers a `rootOnly`
 * search, and the two ways a walk can end WITHOUT reaching one both answer
 * `false`:
 *
 *   A CYCLE (`a.cause = b; b.cause = a`). The walk stops on the link whose own
 *   cause it has already visited, and that link is NOT a root — nothing about
 *   it is innermost, the chain simply has no innermost. Testing it anyway
 *   would answer a rootOnly search on a guess, in the direction where the
 *   guess opens a fence or starts a second run.
 *
 *   A CHAIN DEEPER THAN THE BOUND. No root was observed, and an unobserved
 *   root is not evidence of anything.
 *
 * Every caller here treats `false` as the closed direction, so both degrade
 * the way the surrounding store degrades.
 */
export function findInCauseChain(
  error: unknown,
  predicate: (link: unknown) => boolean,
  options: CauseChainSearch,
): boolean {
  const seen = new Set<unknown>();
  let current: unknown = error;
  for (let depth = 0; depth < MAX_CAUSE_CHAIN_DEPTH; depth += 1) {
    if (!options.rootOnly && predicate(current)) return true;
    seen.add(current);
    const cause = current instanceof Error ? current.cause : undefined;
    if (cause === undefined || cause === null) {
      // A genuine root: nothing caused this one.
      return options.rootOnly ? predicate(current) : false;
    }
    if (seen.has(cause)) return false;
    current = cause;
  }
  return false;
}

/** An error's own message, however it was thrown. */
function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Does this failure mean "that table has never been created", so a read of it
 * should answer EMPTY rather than fault?
 *
 * Matched on the MESSAGE because the structural database seams these stores use
 * (`ExecutionFenceDatabase`, `StartIdempotencyDatabase`) carry no error codes —
 * D1 wraps SQLite's text but preserves it.
 *
 * Matched at the ROOT of the cause chain and nowhere else. The chain is walked
 * at all because the seam is structural: an adapter is free to wrap the
 * driver's error in one of its own ("D1 query failed") and carry the SQLite
 * text on `cause`, and reading only the top message there would classify a
 * correctly-upgraded database as unreadable — a permanent 503 on every gated
 * path. But a missing-table link whose own cause is something ELSE describes a
 * fault that merely PASSED this table on its way out, and concluding "there is
 * nothing here" from that is the answer that opens a fence or starts a second
 * run. So the walk descends first and tests once.
 */
export function missingTableReadsEmpty(error: unknown, table: string): boolean {
  return findInCauseChain(
    error,
    (link) => {
      const message = messageOf(link);
      return /no such table/i.test(message) && message.includes(table);
    },
    { rootOnly: true },
  );
}
