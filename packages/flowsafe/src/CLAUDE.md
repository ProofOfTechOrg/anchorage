# flowsafe source navigation

- `index.ts`: compatibility barrel for approval API, runner, artifacts, and audit export
- `deployment-identity-protocol.ts`: deployment tag, identity header, and execution fence protocol shared with hosts
- `approval-api/`: approval lifecycle and deployment store
- `do-runner/`: runtime, Durable Objects, identities, storage, retention
- `host-kit/`: authenticated host composition and topologies
- `approval-ui/`: optional React dashboard
- `agent-host/`: server-only guarded-agent catalog and Durable Object host
- `agent-runner/`: runtime-driven durable agents
- `signals/`, `goals/`, `schedules/`, `background-tasks/`, `signal-providers/`: opt-in long-running-agent surfaces
- `artifacts/`, `audit-export/`: R2 and Queue integrations

See [`../../../docs/api-reference.md`](../../../docs/api-reference.md) for public subpaths.
