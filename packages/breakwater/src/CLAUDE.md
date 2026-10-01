# breakwater source navigation

- `index.ts`: root public barrel
- `agent/`: guarded agent construction and narrow execution handle
- `policy-engine/`: agent processors, content policy, and tool evaluators
- `rbac/`: actor roles and input authorization
- `audit/`: audit logger, metrics, and sink composition
- `connector-sdk/`: guarded Mastra tool wrapper and stores
- `agent-cli/`: Claude Code and Codex connector adapters
- `connector-decision.ts`: connector decision codes, their classification, and the connector error classes
- `host-input.ts`: internal readers for host-supplied configuration
- `input-refusal.ts`: internal removal of what a refused call leaves on Mastra's message list before an input gate stops the call
- `processor-additions.ts`: internal per-call record of the prompt messages a guarded agent's application input processors add or change, which the policy engine evaluates with the input; also holds the per-list client tool outcome record and the `callerMessages` reader
- `chain.test.ts`: processor integration

Every new public symbol must be exported from its module and documented in the generated API reference.
