---
"@proofoftech/breakwater": patch
"@proofoftech/flowsafe": patch
---

Harden the 0.1.0 cut against the three audit residuals:

- **breakwater (D2):** bind idempotency `put`/`release` to an opaque reservation
  lease token minted by `reserve()` (rotated on a stale-pending takeover), so a
  slow holder that was taken over as stale can no longer delete or finalize the
  new holder's claim.
- **flowsafe (D3):** a bare tenant `ApprovalStore.list()` / `ApprovalService.list()`
  / `GET /api/approvals` now defaults to `MAX_APPROVAL_LIST_LIMIT` instead of an
  unbounded scan (page complete history with an explicit `after` cursor); the
  cron SLA sweep pages the system view explicitly so no unbounded query remains.
- **breakwater (D1):** `PolicyEngine` now rejects an object-only policy
  (`channels: ['object']` without `'answer'`) constructed without an audit sink,
  rather than silently no-op'ing under @mastra/core 1.50.0.

Also: the approval dashboard hook re-sorts into reviewer order only when the
filter requests it, so a FIFO/`after`-paged caller is no longer client-resorted
against the server's paging.
