# flowsafe source navigation

- `index.ts`: compatibility barrel for approval API, runner, artifacts, and audit export
- `approval-api/`: approval lifecycle and tenant-bound stores
- `do-runner/`: runtime, Durable Objects, identities, storage, retention
- `host-kit/`: authenticated host composition and topologies
- `approval-ui/`: optional React dashboard
- `agent-runner/`: runtime-driven durable agents
- `signals/`, `goals/`, `schedules/`, `background-tasks/`, `signal-providers/`: opt-in long-running-agent surfaces
- `artifacts/`, `audit-export/`: R2 and Queue integrations

See [`../../../docs/api-reference.md`](../../../docs/api-reference.md) for public subpaths.
