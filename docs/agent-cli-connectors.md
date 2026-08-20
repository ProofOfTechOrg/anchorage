# Agent CLI connectors

Breakwater can expose Claude Code and Codex as Mastra tools. Each adapter is a write-class `createConnector()` tool, so a human grant, optional durable idempotency, rate limit, audit, and dry-run preview apply before the child process starts.

Use these connectors when an agent workflow must delegate a bounded coding task into a dedicated workspace. They are not a process sandbox. The child CLI can edit files and run commands according to its own permissions, credentials, and host environment.

## Install the vendor CLI

Install and authenticate the CLI through its official instructions:

- [Claude Code setup](https://docs.anthropic.com/en/docs/claude-code/setup)
- [Codex CLI](https://developers.openai.com/codex/cli)

The default connector runner requires Node.js. It resolves `node:child_process` at execution time so importing the module does not force a static Node built-in into non-Node bundles.

## Create a connector

```typescript
import {
  createClaudeCodeConnector,
  createCodexConnector,
} from '@proofoftech/breakwater/agent-cli';
import {
  D1IdempotencyStore,
  D1RateLimitStore,
  type IdempotencyDatabase,
  type RateLimitDatabase,
} from '@proofoftech/breakwater/connector-sdk';

type ConnectorDatabase = IdempotencyDatabase & RateLimitDatabase;

export function codingConnectors(db: ConnectorDatabase) {
  const policies = {
    idempotencyStore: new D1IdempotencyStore(db, {
      pendingTtlMs: 20 * 60 * 1_000,
    }),
    idempotencyKeyMigration: 'legacy-writers-drained' as const,
    rateLimitStore: new D1RateLimitStore(db),
  };

  return {
    claude: createClaudeCodeConnector({
      timeoutMs: 15 * 60 * 1_000,
      maxOutputBytes: 1024 * 1024,
      idempotencyKey: true,
      rateLimit: '10/hour',
      policies,
    }),
    codex: createCodexConnector({
      timeoutMs: 15 * 60 * 1_000,
      maxOutputBytes: 1024 * 1024,
      idempotencyKey: true,
      rateLimit: '10/hour',
      policies,
    }),
  };
}
```

Both adapters require approval by default. Keep that default for any writable workspace.

Set `idempotencyKeyMigration` only after every older connector writer sharing
the D1 store has stopped and drained. A new empty deployment can acknowledge
that no legacy writer exists. See the
[connector idempotency migration contract](connector-interface.md#idempotency)
before upgrading a store with existing rows.

## Call shape

The tool input is:

```typescript
interface AgentCliInput {
  prompt: string;
  cwd?: string;
  model?: string;
}
```

The output is:

```typescript
interface AgentCliOutput {
  text: string;
  exitCode: number;
  command: string;
  simulated?: boolean;
}
```

`text` is the functional result. `command` is a redacted display value for diagnostics, not an executable command line. It never contains the prompt.

Claude Code uses headless print mode with JSON output and returns the envelope's `result` field when it is present. Codex uses non-interactive `exec` mode and returns stdout text.

The shipped definitions explicitly select workspace-edit permissions:

- Claude Code uses `--permission-mode=acceptEdits`.
- Codex uses `--sandbox=workspace-write`.

These flags allow workspace changes without granting arbitrary host access beyond what the CLI and surrounding process isolation allow. They do not make the process safe to run in a sensitive checkout.

## Approval and dry-run

The adapters declare `sideEffect: "write"` and `requiresApproval: true`. Execution requires a matching structured grant in `breakwater.connectorGrants`:

```text
agent-cli.claude-code
agent-cli.codex
```

Flowsafe can place one of these IDs in a server-authored suspension, store the reviewer decision, and derive the matching exact-suspension grant for the resumed leg. A legacy connector ID array does not authorize execution.

A caller can set breakwater's dry-run request-context key to preview the redacted command. Dry-run never spawns a process and does not need an approval grant.

Do not use the dry-run preview as proof that the eventual workspace is unchanged. It validates connector construction and argv presentation only.

## Prompt and option separation

The generic wrapper owns prompt placement:

```text
binary [definition flags] -- [raw prompt]
```

`AgentCliDefinition.buildFlags()` receives a frozen input whose prompt is already redacted. The wrapper appends the original prompt exactly once after `--`.

This prevents a prompt beginning with `-` from becoming a CLI flag. Shipped model flags use `--model=value`, so a model string beginning with `-` remains the value rather than a new option.

Custom definitions must:

- return flags and subcommands only;
- never include the prompt;
- bind option values with `=`;
- avoid shell fragments;
- keep a stable connector id;
- declare every host the CLI contacts;
- parse stdout without copying it into errors.

## Safe diagnostics

Raw prompts and option values may contain secrets, source code, customer data,
or instructions. Breakwater keeps them only in the final child-process argv.

The public diagnostics contract:

- returned `command` redacts the prompt and option values;
- `AgentCliError` uses a stable code and static message;
- metadata contains only connector id, redacted command, numeric exit/timeout data, a sanitized system code, a fixed process-tree termination method when applicable, and booleans indicating whether stdout or stderr existed;
- stdout is returned only as the successful functional `text`;
- stderr and failed stdout are never copied into errors or audit;
- validation failures, connector-policy failures, idempotent replays, custom flag builders, and output parsers cross the same sanitizing boundary;
- arbitrary thrown values become static audit reasons.

Treat successful `text` as untrusted tool output. Do not put it into logs or another model prompt without the policy appropriate for that destination.

## Error handling

Catch `AgentCliError` and branch on its exported `code` rather than parsing `message`:

```typescript
import { AgentCliError } from '@proofoftech/breakwater/agent-cli';
import { invokeConnector } from '@proofoftech/breakwater/connector-sdk';

try {
  const result = await invokeConnector(connector, input, {
    requestContext,
  });
  return result;
} catch (error) {
  if (error instanceof AgentCliError) {
    metrics.increment('agent_cli.failure', {
      connector: error.connectorId ?? 'unknown',
      code: error.code,
    });
  }
  throw error;
}
```

Codes distinguish unavailable runtime or codec, definition/flag failure, spawn failure, timeout, process-tree termination failure, malformed execution result, non-zero exit, parser failure, and a contained outer execution failure. Consult the generated API reference for the exact union in the installed version.

The one-argument `new AgentCliError(message)` constructor remains compatible for consumers that created their own error, but errors emitted by shipped connectors use the structured metadata.

## Timeouts and output limits

`timeoutMs` defaults to 10 minutes and must be a safe integer from 1 through the JavaScript timer ceiling. On POSIX, the built-in runner starts the CLI as a new process-group and session leader, sends `SIGKILL` to the negative group id, and waits up to five seconds for that group to disappear. On Windows, it resolves `taskkill.exe` under a drive-absolute local `SystemRoot` or `WINDIR` before starting the CLI, then invokes that absolute path with `['/pid', pid, '/T', '/F']`, `shell: false`, and a hidden window. It rejects relative, root-relative, Universal Naming Convention (UNC), and device paths. This prevents a writable current directory, network share, device path, or `PATH` entry from replacing the timeout helper. It waits for taskkill to complete before returning the timeout.

An already-absent POSIX group (`ESRCH`) is a successful termination. A Windows runtime with no absolute system root fails with `runtime-unavailable` before the CLI starts. Permission failure, unavailable process id, a group that remains present after the bounded wait, taskkill spawn failure, or taskkill non-zero exit reports `termination-failed` with only the sanitized system code, numeric exit code, timeout, and termination method. No path invokes a shell. A process that deliberately creates a different POSIX session escapes the inherited group, so retain the host/container process boundary and teardown policy.

`maxOutputBytes` defaults to 1 MiB per stream. The runner retains the UTF-8 tail, cuts only at a code-point boundary, and marks truncation. A zero cap retains no process output.

When durable idempotency is enabled, `D1IdempotencyStore.pendingTtlMs` must exceed `timeoutMs`. Otherwise a second isolate could take over a still-running process and start it again. The store constructor also requires a positive safe-integer TTL within the JavaScript timestamp range.

The connector rejects malformed numeric values at construction, including `NaN`, infinities, negatives, unsafe integers, and fractions.

## Inject execution

`AgentCliExec` is the process boundary:

```typescript
type AgentCliExec = (
  command: string,
  args: readonly string[],
  options: {
    cwd?: string;
    timeoutMs: number;
  },
) => Promise<{
  stdout: string;
  stderr: string;
  exitCode: number;
}>;
```

Inject it to:

- run the CLI inside a container or remote build worker;
- apply a process allowlist;
- enforce an independent timeout, process-tree teardown, and resource quota;
- supply a test double;
- construct the connector outside Node.

The connector still applies its approval, idempotency, rate, input/output, error, and audit boundaries around an injected executor.

## Production isolation

Run coding connectors in a dedicated environment:

1. Create an ephemeral or resettable checkout for the task.
2. Derive `cwd` from trusted host configuration, not model or client input.
3. Give the process the minimum repository and credential access.
4. Keep the vendor CLI updated and pin the version in your image.
5. Apply operating-system or container CPU, memory, process, filesystem, and network limits.
6. Route outbound traffic through infrastructure policy where possible.
7. Keep approval enabled and bind it to the exact durable suspension.
8. Use a per-task idempotency key and a shared store.
9. Review the resulting diff before it reaches a protected branch.
10. Destroy or clean the workspace after the task.

The egress manifest describes the vendor API hosts. A child process does not use `ConnectorRuntime.fetch`, so breakwater cannot inspect its sockets. Enforce actual CLI egress outside the connector.

## Custom CLI definitions

Use `createAgentCliConnector(definition, options)` for another non-interactive agent CLI:

```typescript
import {
  createAgentCliConnector,
  type AgentCliDefinition,
} from '@proofoftech/breakwater/agent-cli';

const definition: AgentCliDefinition = {
  id: 'agent-cli.example',
  description: 'Run Example CLI in a dedicated workspace',
  binary: 'example-agent',
  egress: ['api.example.com'],
  buildFlags: (input) => [
    'run',
    '--workspace-write',
    ...(input.model ? [`--model=${input.model}`] : []),
  ],
  parseOutput: (stdout) => stdout.trim(),
};

export const exampleAgent = createAgentCliConnector(definition);
```

Test:

- the actual argv and final `--`;
- a `-`-prefixed prompt and model;
- approval denial and grant success;
- dry-run with no executor call;
- timeout and non-zero exit;
- output truncation;
- idempotent replay;
- every public and audit error surface for prompt, stdout, and stderr leakage;
- the packed npm consumer, not only source imports.

See the [connector authoring guide](https://github.com/ProofOfTechOrg/anchorage/blob/main/packages/breakwater/CONNECTORS.md) for the general manifest contract.
