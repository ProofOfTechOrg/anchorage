---
'@proofoftech/fleet-control': minor
---

Add token-driven bounded normal decommissioning for at-least-once control-plane Worker workflows. Fleet D1 owns scan progress. Each call performs at most one bounded scan chunk; only an exact matching verify may immediately consume that result through its single same-lease resource action. Other calls perform at most one lifecycle or resource action group.

Persist an immutable database-export receipt authority before the first D1 scan or export. Retries after artifact commit or Fleet state-write loss converge on the same filesystem or R2 receipt; authority changes and byte collisions preserve the committed winner and fail closed. Custom bounded backends must expose the paired receipt authority and export capability. Queue-driven bounded decommissioning requires Workers Paid because its bounded multi-R2 read groups can exceed the Free plan external-subrequest limit.

Add a root-only bounded backend-switch advance API that uses the same durable token and receipt guarantees. It binds teardown to one immutable switch snapshot and captured entry subphase, advances at most one release, R2 resource, scan chunk, or D1 action group per call, and preserves legacy recovery after a shell-less deployment reaches export authorization. A pending ordinary Worker requires lossless exact-version inspection, authoritative secret-name inventory, and its persisted Durable Object namespace identities. Custom switch providers must expose the bounded scan, receipt, database, residual, delete, and conditional pending-artifact inspection capabilities required by the durable state they resume.
