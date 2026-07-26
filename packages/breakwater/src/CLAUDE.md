# breakwater source navigation

- `index.ts`: root public barrel
- `policy-engine/`: agent processors, content policy, and tool evaluators
- `rbac/`: actor roles and input authorization
- `audit/`: audit logger, metrics, and sink composition
- `connector-sdk/`: guarded Mastra tool wrapper and stores
- `agent-cli/`: Claude Code and Codex connector adapters
- `chain.test.ts`: processor integration

Every new public symbol must be exported from its module and documented in the generated API reference.
