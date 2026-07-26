# Background task navigation

- `d1-storage.ts`: serialized workflow and tenant task domains plus purge
- `host.ts`: one task manager per tenant Durable Object
- `routes.ts`: host route adapter
- matching `*.test.ts`: lifecycle, recovery, isolation, and shutdown coverage

See [`../../../../docs/durable-agents.md`](../../../../docs/durable-agents.md#add-background-tasks).
