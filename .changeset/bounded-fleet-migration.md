---
"@proofoftech/fleet-control": minor
---

Add durable fleet upgrades through `advanceFleetMigration()`, with a frozen target digest and per-item plan, one admission or plan step per call, first-error stopping, item paging and explicit abandonment. The bounded API requires a `FleetOperationStore` alongside the existing deployment store; custom deployment stores can continue using the one-call `migrateFleet()` drain.

Fleet and provider state remain mutation authority. Continuations re-read that state under per-call deployment leases and recover cursor-loss windows through the shared migration engine. Public documentation describes accepted inter-call races, strict fresh-admission limits after Durable Object tag movement, at-least-once settlement, operation-store convergence residuals, and the repeated resolver/provider and whole-item-read costs. One step is not a fixed provider-request, CPU or row-read budget.

The existing drain retains its recorded ordering and behavior apart from the separately documented correction to target application-binding validation during plain Worker upgrades.
