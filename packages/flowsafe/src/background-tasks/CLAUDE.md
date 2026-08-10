# Background task navigation

- `d1-storage.ts`: serialized workflow and deployment task domains plus purge
- `host.ts`: task manager lifecycle for a deployment singleton Durable Object
- `routes.ts`: host route adapter
- matching `*.test.ts`: lifecycle, recovery, singleton identity, and shutdown coverage

See [`../../../../docs/durable-agents.md`](../../../../docs/durable-agents.md#add-background-tasks).
