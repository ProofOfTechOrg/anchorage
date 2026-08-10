# Approval API navigation

- `types.ts`: records, filters, cursors, and response contracts
- `service.ts`: authorization, CAS lifecycle, batch decision, SLA, notification, and resume
- `d1-store.ts`, `store.ts`, `store-factory.ts`: durable, in-memory, and deployment-store assembly
- `actor-context.ts`: authenticated actor context, server-owned id minters, and trusted resource-key validation
- `router.ts`: authenticated REST surface
- `grants.ts`: exact suspension-bound grant derivation
- `retention.ts`: terminal-record purge

See [`../../../../docs/approval-system.md`](../../../../docs/approval-system.md).
