# Agent memory isolation

Flowsafe keeps Mastra memory inside one physically isolated organization deployment. The host mints opaque thread ids, validates resource ids, rejects client-selected memory identities, and routes each thread through its owning Durable Object.

## Deployment boundary

One organization owns the Worker, D1 database, and Durable Object namespaces. Flowsafe does not partition one database by a request tenant claim.

Provisioning writes the same stable tag to `DEPLOYMENT_TENANT` and the singleton `flowsafe_deployment.tenant_tag` row before application tables exist. The Worker validates that pair. Every thread topology also stamps `DEPLOYMENT_IDENTITY_SECRET`; the target compares it before validating its own tag and sentinel or reading memory. The tag is audit attribution only.

## Memory identity shape

Thread and resource ids match `PATH_SAFE_ID_PATTERN`. They contain only RFC 3986 unreserved characters, are 1 to 200 characters long, and cannot equal `.` or `..`.

### Thread ids

Mint a thread id after authentication:

```typescript
const threadId = context.newThreadId();
```

The default minter returns an opaque UUID. The id becomes a D1 key, URL segment, and Durable Object name, so application code must not encode customer identity into it.

### Resource ids

Validate a server-owned business key as the resource id:

```typescript
const resourceId = context.resourceIdFromKey(accountId);
```

The resource key identifies the memory owner inside this deployment. Do not accept a full resource id from an untrusted body.

## Public host boundary

Reject client-selected memory identities before route-specific processing:

```typescript
assertNoClientMemoryIds(body);
```

`assertNoClientMemoryIds()` recursively rejects `threadId` and `resourceId` fields. `flowsafe_resource_owners` records the principal that owns each server-minted thread and validated host-owned resource key. A route that addresses memory checks that registry before role authorization and returns `404` when the principal cannot see the resource.

Validate path parameters with the shared memory boundary before you address a namespace. Never coerce or partially decode an invalid id.

## Thread Durable Object topology

Reach a thread object through `createThreadTopology()`:

1. Validate the path-safe thread id before calling `idFromName()`.
2. Stamp the verified `ExecutionPrincipal` into `x-flowsafe-principal`.
3. Stamp the internal deployment credential and replace any caller-supplied value.
4. Strip retired `x-flowsafe-actor`, `x-flowsafe-role`, and `x-flowsafe-tenant` headers.
5. Let `ThreadDurableObject` use its own `id.name` as the authoritative thread id.
6. Verify the caller credential and deployment sentinel before building storage or serving the route.

Do not call a raw thread namespace stub from a public route. A direct `stub.fetch(request)` can forward attacker-controlled internal headers and bypasses the supported topology.

## D1 storage inventory

The optional signal, schedule, and background-task domains extend Mastra's D1 storage:

| Table or domain | Local identity | Retention |
| --- | --- | --- |
| `mastra_workflow_snapshot` | `workflow_name` and opaque `run_id` | Terminal-run TTL |
| `mastra_threads` | Opaque `id` | Optional idle-thread TTL |
| `mastra_messages` | Opaque `thread_id` | Deletes with its thread |
| `mastra_resources` | Validated host-owned `id` | Explicit host teardown or deployment decommissioning |
| `mastra_notifications` | Opaque `thread_id` | Optional terminal TTL |
| `mastra_thread_state` | Opaque `thread_id` | Optional updated-time TTL |
| Background-task domains | Opaque run and task ids | Terminal-task TTL |
| `mastra_schedules` | Server-minted schedule id | Authorized schedule deletion or deployment decommissioning |
| `mastra_schedule_triggers` | Server-minted schedule id | Optional trigger-history TTL |
| `flowsafe_signal_subscriptions` | Server-minted subscription id | Authorized subscription deletion or deployment decommissioning |
| `flowsafe_resource_owners` | Run, thread, resource, and schedule id | Run retention and schedule deletion release their claims. Thread and resource claims require explicit host teardown or deployment decommissioning |

The deployment sentinel refuses first-time ownership of any database that already contains application tables. Provision a fresh per-organization database instead of attempting to preserve pooled rows.

## Thread retention

`purgeExpiredThreads()` deletes idle thread rows and their messages only when you configure a thread TTL. It never deletes `mastra_resources`; working memory can outlive one conversation.

The purge rechecks activity at deletion time. A message that arrives during the retention pass keeps the thread alive and cannot become orphaned.

Related retention helpers cover terminal notifications, thread state, schedule trigger history, background tasks, workflow snapshots, approvals, and paired R2 artifacts. Live or suspended authorization state is never age-purged. Authorized domain deletion removes standing records such as schedules and subscriptions; decommissioning deletes the remaining physical storage. TTL retention, authorized deletion, and deployment decommissioning are distinct lifecycle mechanisms.

Idle-thread retention deletes `mastra_threads` and `mastra_messages`, but it keeps the thread and resource ownership claims. Agent bindings, schedules, subscriptions, goals, or other standing wake sources can still address the same thread and recreate its memory. When a host supports permanent thread or resource deletion, it must remove every authoritative standing record first and then call `ActorContext.releaseResource()` for the corresponding claims.

## Deployment decommissioning

There is no in-database organization purge. Decommission the physical deployment instead:

1. Stop new traffic and unattended starts.
2. Revoke actor, provider, webhook, and stream credentials.
3. Export records required by policy.
4. Delete the Worker and its routes.
5. Delete the bound D1 database, Durable Object namespaces, R2 bucket, queues, and secrets.

Do not reuse a database or namespace set for another organization. The sentinel detects a mismatched D1 binding, but it does not sanitize old data.

## Durable-agent resume

An approval record for a durable agent stores the agent, thread, resource, and original `ExecutionPrincipal`. After eviction, the host:

1. validates the persisted path-safe memory binding;
2. reconstructs the guarded agent from the server catalog;
3. reauthorizes the stored original principal against the current catalog's role, automation, and permission policy;
4. derives fresh trusted request context;
5. rehydrates Mastra's in-process registries without replaying initial application input processors;
6. resumes through `RunnerRuntime`.

The reviewer remains the decision actor and cannot replace the principal that resumes execution.

## Extension checklist

Any new route or background path that touches memory must:

1. verify deployment identity before protected work;
2. resolve a trusted actor or automated principal;
3. refuse client-selected full memory ids;
4. mint or validate ids through exported helpers;
5. route threads through `createThreadTopology()`;
6. define retention or standing-state treatment;
7. update the schema inventory when it adopts another table;
8. add adversarial malformed-id, restart, and retention-race tests.

## Verification

Run the focused package suite and the real workerd spike:

```bash
pnpm --filter @proofoftech/flowsafe test
pnpm --filter @proofoftech/flowsafe spike:verify
```

The spike covers deployment-sentinel mismatch refusal, thread routing, signal delivery, durable-agent recovery, and restart-safe approval resume.
