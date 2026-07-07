# agent-cli/

## Files

| File | What | When to read |
| ---- | ---- | ------------ |
| `index.ts` | Claude Code / Codex CLIs as breakwater connectors — `createClaudeCodeConnector` / `createCodexConnector` / generic `createAgentCliConnector(definition, options)`; write-class approval-gated manifest (grant via `APPROVED_CONNECTORS_CONTEXT_KEY`), dry-run = command preview (never spawns), injectable `AgentCliExec` seam, default runner spawns via `process.getBuiltinModule('node:child_process')` (arg-array, no shell), timeout SIGKILL. `AgentCliDefinition.buildFlags` returns option flags only; the wrapper appends `'--', input.prompt` so the caller-controlled prompt is always a trailing positional behind end-of-options (flag-smuggling defense), and definitions bind option values with `--flag=value` | Adding an agent CLI adapter, changing flags/parse/spawn/timeout behavior |
| `agent-cli.test.ts` | Adapter tests over a mocked `exec`: flag building + `--`-isolated prompt + JSON-result parse (claude), raw stdout (codex), grant denial, manifest shape, dry-run preview, `requiresApproval: false`, non-zero exit, binary/id overrides, and two flag-smuggling regressions (flag-shaped prompt behind `--`, flag-shaped model fused with `=`) | Adding adapter tests, debugging enforcement or arg drift |

## Runtime constraint

Constructing a connector works anywhere; executing one uses Node's
`child_process` (inherited env carries the CLI's auth). Non-Node runtimes
must inject `options.exec` (e.g. a containerized runner) or calls throw
`AgentCliError`.
