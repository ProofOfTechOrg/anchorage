# Agent-memory tenancy

Mastra memory uses caller-selected thread and resource ids. Two tenants can choose the same business key, so a shared D1 database needs server-minted tenant identities before any agent reads or writes memory.

Flowsafe ships the complete boundary:

- memory-id mint and ownership helpers;
- `TenantContext` constructors;
- public-body rejection and foreign-id 404;
- thread Durable Object topology and identity assertion;
- real Mastra D1 recall-path isolation proof;
- offboarding for threads, messages, resources, notifications, state, and tasks;
- opt-in idle-thread retention.

Using unsalted client-selected ids in a multi-tenant host is a cross-tenant disclosure, not a degraded mode.

## Identity shape

### Thread

```text
tenantId_uuid
```

Create it with:

```typescript
const threadId = tenant.newThreadId();
```

or the lower-level `mintThreadId(tenantId)`.

### Resource

```text
tenantId_resourceKey
```

Create it with:

```typescript
const resourceId = tenant.newResourceId(resourceKey);
```

or `mintResourceId(tenantId, resourceKey)`.

The resource key must satisfy the runner's path-safe pattern. It is a host-selected business identity, not a client-provided full memory id.

### Ownership

Use `tenant.ownsMemoryId(id)` or `tenantOwnsMemoryId(tenantId, id)`. Decode through `tenantOfMemoryId()`; do not split the string in application code.

Tenant ids exclude `_`, making the first delimiter unambiguous.

## Public host boundary

Memory-touching routes apply:

```typescript
assertNoClientMemoryIds(body);
requireOwnedMemoryId(tenant, threadId, 'threadId');
```

`assertNoClientMemoryIds()` rejects nested body fields named `threadId` or `resourceId`. A client that can choose the full id can choose another tenant's memory.

`requireOwnedMemoryId()` returns the owned id or throws a route error that maps to 404 for a foreign id. The response is not an existence oracle.

The path can reference an already owned thread, like a run-status path references an owned run. Creation mints a new id server-side.

## Thread Durable Object boundary

`createThreadTopology()` is the only supported path to a thread object:

1. Check ownership before addressing the namespace.
2. Use `idFromName(threadId)`.
3. Clone forwarded requests.
4. Overwrite the internal tenant and actor headers from resolved context.
5. Let `ThreadDurableObject` compare the tenant header with the tenant decoded from its own name.

Do not use `namespace.get(...).fetch(request)` directly. That pattern forwards an attacker's version of the header the object is supposed to verify.

## Recall-path proof

The integration test creates one real `@mastra/cloudflare-d1` store and one Mastra memory implementation. Two tenants use the same business resource key.

It verifies isolation through the exact surfaces an agent turn uses:

- `recall()`;
- `listThreads({ filter: { resourceId } })`;
- resource-scoped working memory.

Schema separation alone is insufficient if a recall query uses a different key. This test pins the read path as well as row identity.

## D1 table coverage

With the opt-in signal and schedule domains composed, the guarded Mastra inventory is:

| Table | Tenant key | Retention |
| --- | --- | --- |
| `mastra_workflow_snapshot` | Salted `run_id` | Terminal-run TTL |
| `mastra_threads` | Salted `id` | Optional idle-thread TTL |
| `mastra_messages` | Salted `thread_id` | Deletes with thread |
| `mastra_resources` | Salted `id` | Offboarding only |
| `mastra_notifications` | Salted `thread_id` | Optional terminal TTL |
| `mastra_thread_state` | Salted `thread_id` | Optional updated-time TTL |
| `mastra_background_tasks` | Salted `run_id` | Terminal-task TTL |
| `mastra_schedules` | Exact `metadata.tenantId` | Offboarding only |
| `mastra_schedule_triggers` | Exact `metadata.tenantId` | Optional trigger-history TTL |
| `mastra_scorers` | Unadopted | No feature writes it |

Flowsafe's schema guard fails when the inventory changes. Adopting a table requires declaring its tenant purge and retention treatment in the same change.

Provider subscriptions live in the flowsafe-owned `flowsafe_signal_subscriptions` table rather than `mastra_*`; its tenant-bound store and offboarding path are separately tested.

## Offboarding

`purgeTenant()` range-deletes:

- messages by `thread_id`;
- threads by `id`;
- resources by `id`;
- notifications and thread state by `thread_id`;
- background tasks by `run_id`.

It deletes schedules and trigger history through exact `metadata.tenantId`, plus workflows, approvals, subscriptions, and artifacts through their domain-specific paths.

The range is:

```text
[`${tenantId}_`, `${tenantId}\x60`)
```

under the expected binary ordering. The restricted tenant charset makes it exact.

Missing lazily created tables report zero rows so a memory-less tenant still offboards cleanly.

## Thread retention

`purgeExpiredThreads(db, { ttlMs, limit, tablePrefix })` selects `mastra_threads.updatedAt`.

A thread has no terminal status, so idleness is the only available lifecycle signal. The helper:

- is disabled unless the operator sets a thread TTL;
- deletes messages with their thread;
- prevents an orphan through a `NOT EXISTS` guard;
- keeps `mastra_resources`, because working memory belongs to the resource across threads;
- uses bounded batches.

A conversation is intended to persist, so no default TTL is selected.

## Notifications and state

Signal features add:

- `mastra_notifications`, keyed by salted thread and purging terminal records only;
- `mastra_thread_state`, keyed by salted thread and holding state lanes plus goals.

Both TTLs are opt-in and use separate failure boundaries in the retention invocation.

An active goal updates its timestamp. An operator must choose a state TTL that does not silently remove standing instructions still needed by a tenant.

## Durable-agent resume

An approval for a durable agent stores a server-authored `resumeTarget` with the thread and optional resource id.

Before restart resume, the host:

1. validates that the target belongs to the record's tenant;
2. validates the persisted snapshot's memory binding;
3. prepares Mastra's in-process durable-agent registry from stored message state;
4. restores observation on the thread runtime;
5. resumes through `RunnerRuntime`.

A reviewer payload cannot choose this target.

## Rules for new memory features

Any new route or background path that touches memory must:

1. resolve verified tenant context;
2. refuse client-selected full memory ids;
3. mint or ownership-check through the exported helpers;
4. route a thread through `createThreadTopology()`;
5. use a tenant-aware storage predicate;
6. include an adversarial same-business-key test;
7. add offboarding counters and retention policy;
8. update the schema inventory if a new Mastra table appears.

Do not use post-read filtering as the primary isolation control. A forgotten filter fails open; an unaddressable salted id fails closed.

## Verification

```bash
pnpm --filter @proofoftech/flowsafe test
pnpm --filter @proofoftech/flowsafe typecheck
pnpm --filter @proofoftech/flowsafe spike:verify
```

The focused coverage includes memory ids, host boundary, thread topology, real D1 recall, schema inventory, thread retention, tenant purge, signals, and durable-agent restart resume.
