---
'@proofoftech/fleet-control': minor
---

Add token-driven bounded normal decommissioning for at-least-once control-plane Worker workflows. Fleet D1 owns scan progress. Each call performs at most one bounded scan chunk; only an exact matching verify may immediately consume that result through its single same-lease resource action. Other calls perform at most one lifecycle or resource action group.

Persist an immutable database-export receipt authority before the first D1 scan or export. Retries after artifact commit or Fleet state-write loss converge on the same filesystem or R2 receipt; authority changes and byte collisions preserve the committed winner and fail closed. Custom bounded backends must expose the paired receipt authority and export capability. Queue-driven bounded decommissioning requires Workers Paid because its bounded multi-R2 read groups can exceed the Free plan external-subrequest limit.
