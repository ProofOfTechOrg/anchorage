---
"@proofoftech/flowsafe": minor
---

Require `@mastra/core` `1.73.0` exactly and `@proofoftech/breakwater` `>=0.17.0 <1.0.0` when used. Pin the D1 storage dependency to `@mastra/cloudflare-d1` `1.4.0`.

Breaking: Guarded durable calls refuse call-level `errorProcessors`, accepted in 0.23, alongside the existing structured-output restriction. See the [durable call-option restrictions](https://github.com/ProofOfTechOrg/anchorage/blob/main/docs/durable-agents.md#durable-call-options).

Breaking: When a state read falls back to Mastra's in-memory run, `status()` and the run status, dispatch-status and stream routes report that run's lifecycle status, `suspended` with no suspended paths or `running`, instead of `pending`. The fallback state remains marked `isFromInMemory`; authoritative reads used by resume, deadline and terminate paths still refuse it.

Core 1.73.0's `@mastra/core/events` entry calls `crypto.randomUUID()` while it loads, which Cloudflare Workers refuse during startup. Flowsafe's do-runner supplies that value before the entry loads, so Workers that build their pub/sub from `createHostPubSub()` start. A Worker whose bundle evaluates `@mastra/core/events` before Flowsafe's do-runner fails at startup with `Disallowed operation called within global scope`, for example a Worker importing `CachingPubSub` or `withCaching` from that entry, since import sorters order `@mastra/*` before `@proofoftech/*`. On core 1.73.0, such a Worker should not import `@mastra/core/events` itself.

Storage initialization (`init()`) automatically adds `ownerId` and `leaseExpiresAt` to `mastra_background_tasks` with additive `ALTER TABLE … ADD COLUMN` statements. No manual migration is needed. Mastra's background-task manager claims and renews task leases, skips live leases during recovery, reclaims running tasks with expired or absent leases, and clears leases on suspension. Flowsafe continues applying the `resourceId` filter omitted by the D1 adapter's `listTasks`.

Guarded agents inherit Breakwater's [input policy coverage](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#input-policy-coverage), [input policies and memory](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#input-policies-and-memory), [application processor rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#application-processors), and [streaming rules](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/README.md#understand-streaming-hold-back).

Migration: Upgrade `@mastra/core` to `1.73.0`, `@mastra/cloudflare-d1` to `1.4.0`, and `@proofoftech/breakwater` to `>=0.17.0 <1.0.0` when used. Remove call-level `errorProcessors` from guarded durable calls. Build the bus with `createHostPubSub()` and do not import `@mastra/core/events` in Worker code on core 1.73.0.
