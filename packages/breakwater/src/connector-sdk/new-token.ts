// Opaque reservation lease tokens for the idempotency stores (audit D2). A
// token is minted per successful reserve() (and rotated on a stale-pending
// takeover) so put()/release() can compare-and-set on ownership — a
// taken-over holder can no longer delete or finalize the new holder's claim.
//
// crypto.randomUUID is a runtime global in Workers and Node >= 19; accessed
// via a globalThis cast because breakwater's BUILD tsconfig is intentionally
// types-free (the library is runtime-agnostic — no @types/node, no
// workers-types), so a bare `crypto` reference would not type-check. Fail
// fast if the global is absent rather than mint a non-unique token.
export function newToken(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if (!c?.randomUUID) {
    throw new Error(
      'crypto.randomUUID unavailable — breakwater idempotency tokens require Workers or Node >= 19',
    );
  }
  return c.randomUUID();
}
