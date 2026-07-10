# connector-sdk/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | `createConnector()` wrapping `createTool()` — network-egress allowlist gate, write-approval gate (a `requestContext` grant enforced on every path), keyed idempotent replay (atomic reserve path when the store implements `AtomicIdempotencyStore`), dry-run simulation (`dryRun` manifest + `dryRunExecute`, requested via `DRY_RUN_CONTEXT_KEY`), fixed-window rate limiting (`rateLimit` manifest + `RateLimitStore`/`InMemoryRateLimitStore`); plus `connectorManifest`, `InMemoryIdempotencyStore` (atomic), the approved-connectors / idempotency-key / dry-run context-key constants, and `ConnectorPolicyError` | Implementing a connector, changing egress/approval/idempotency/dry-run/rate-limit enforcement, adding a permission-manifest field |
| `d1-idempotency-store.ts` | `D1IdempotencyStore` — durable `AtomicIdempotencyStore`: `INSERT ... ON CONFLICT DO NOTHING RETURNING` claim, stale-pending TTL takeover, JSON record round-trip; structural `IdempotencyDatabase`/`IdempotencyStatement` (no workers-types dependency) | Changing the durable replay store's schema, claim SQL, or TTL semantics |
| `d1-rate-limit-store.ts` | `D1RateLimitStore` — durable `RateLimitStore`: `UPSERT ... RETURNING count` atomic fixed-window increment shared across isolates (an in-memory window under DO-per-run routing is a per-RUN budget), rollover reap of expired windows; structural `RateLimitDatabase`/`RateLimitStatement` | Changing the durable budget store's schema or window semantics |
| `connector-sdk.test.ts` | Tests for every enforcement path — egress allow/deny, grant gating on nested and direct calls, idempotent replay (legacy get/put and atomic reserve paths), dry-run, rate limit | Adding enforcement tests, debugging a gate that fails open or closed |
| `d1-idempotency-store.test.ts` | `D1IdempotencyStore` against real SQLite (node:sqlite): claim races over one database, release/retry, stale takeover, undefined-result round-trip | Adding durable-store tests, debugging the reserve CAS |
| `d1-rate-limit-store.test.ts` | `D1RateLimitStore` against real SQLite: window counting, cross-instance sharing over one database, rollover + reap, per-key isolation, schema-retry | Adding durable budget-store tests |
