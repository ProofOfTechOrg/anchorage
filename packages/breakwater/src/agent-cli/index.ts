// Agent CLI adapters — Claude Code and Codex as Mastra tools (Phase 4
// "Agent CLI adapters"). Each adapter is a breakwater connector, so the full
// permission manifest governs it: an agent CLI edits files and runs commands
// on the host, which makes it write-class and approval-gated by default (the
// APPROVED_CONNECTORS_CONTEXT_KEY grant, mintable through flowsafe's
// approval queue), with dry-run simulation always available (the command
// preview, never a spawn) and optional rate limiting.
//
// Node-only at execution time: the default runner spawns the CLI via
// node:child_process (resolved through process.getBuiltinModule so
// non-Node bundles never see a static node: import). Constructing a
// connector works anywhere; calling one without an `exec` override outside
// Node throws AgentCliError. The `exec` seam is also the test surface.

import type { Tool } from '@mastra/core/tools';
import { z } from 'zod';

import type { ConnectorPolicies } from '../connector-sdk/index.js';
import { createConnector } from '../connector-sdk/index.js';
import type {
  TextCodecLookups,
  TextDecoderLike,
  TextEncoderLike,
} from './tail-accumulator.js';
import { tailAccumulator } from './tail-accumulator.js';

export interface AgentCliInput {
  /** The task prompt handed to the agent CLI. */
  prompt: string;
  /** Working directory the CLI runs in (the workspace it may modify). */
  cwd?: string;
  /** Model override forwarded to the CLI. */
  model?: string;
}

export interface AgentCliOutput {
  /** The agent's final text (parsed when the CLI supports it, else raw stdout). */
  text: string;
  exitCode: number;
  /** The exact command line, for audit trails and debugging. */
  command: string;
  /** Present (true) when this was a dry-run simulation — nothing was spawned. */
  simulated?: boolean;
}

export interface AgentCliExecResult {
  stdout: string;
  stderr: string;
  exitCode: number;
}

/** Spawn seam — inject in tests or to sandbox/containerize execution. */
export type AgentCliExec = (
  command: string,
  args: readonly string[],
  options: { cwd?: string; timeoutMs: number },
) => Promise<AgentCliExecResult>;

export interface AgentCliDefinition {
  id: string;
  description: string;
  binary: string;
  /** Hosts the CLI itself calls; declared on the manifest for egress policy. */
  egress: readonly string[];
  /**
   * The option flags and any subcommand — everything EXCEPT the prompt. The
   * wrapper appends `'--', input.prompt`, so the caller-controlled prompt is
   * always the trailing positional behind an end-of-options separator and
   * can never be parsed as a CLI flag (argv flag-smuggling defense — a
   * prompt like '--allow-dangerously-skip-permissions' stays inert data).
   * Put option VALUES in `--flag=value` form for the same reason: a value
   * starting with '-' binds to its flag instead of smuggling a new one.
   */
  buildFlags: (input: AgentCliInput) => string[];
  /**
   * Extract the agent's final text from stdout; return undefined to fall
   * back to raw stdout (e.g. on output-format drift across CLI versions).
   */
  parseOutput?: (stdout: string) => string | undefined;
}

export interface AgentCliConnectorOptions {
  /** Spawn implementation; defaults to node:child_process. */
  exec?: AgentCliExec;
  /** Override the binary (absolute path or PATH name). */
  binaryPath?: string;
  /**
   * Kill the CLI after this long. Default 600000 (10 min — agent tasks run
   * long). Must be an integer in [1, 2^31-1]: setTimeout treats NaN and
   * negatives as 0 and clamps beyond-2^31-1 delays to 1ms — either way an
   * instant SIGKILL — and a NaN additionally passes the pendingTtlMs guard
   * below vacuously. Construction throws on a malformed value.
   */
  timeoutMs?: number;
  /**
   * Agent CLIs are approval-gated by default (they execute arbitrary
   * actions on the host). Opt out only for sandboxed/read-only setups.
   */
  requiresApproval?: boolean;
  /** Manifest rate limit, e.g. '10/hour'. Requires policies.rateLimitStore. */
  rateLimit?: string;
  /**
   * Require callers to supply a per-call idempotency key; replays return the
   * stored result instead of re-spawning the CLI. Requires
   * policies.idempotencyStore. When the store exposes a numeric
   * `pendingTtlMs` (D1IdempotencyStore), construction throws unless it
   * exceeds `timeoutMs` — a shorter TTL lets a still-running invocation be
   * taken over as stale and spawned a second time (audit D2).
   */
  idempotencyKey?: boolean;
  /** Connector id override (running two differently-configured instances). */
  id?: string;
  /** Passed through to createConnector (audit, stores, evaluators). */
  policies?: ConnectorPolicies;
  /**
   * Cap on retained stdout/stderr per stream in the default exec (node's
   * child_process) — a runaway CLI must not balloon memory unbounded.
   * Counted in actual UTF-8 encoded BYTES (not JS string length / UTF-16
   * code units — a naive char-length cap over-counts non-Latin1 output and
   * can cut mid-codepoint). The TAIL is kept, cut on a codepoint boundary,
   * and truncation is marked (the agent's final answer text arrives last).
   * Ignored when `exec` is injected. Default 1 MiB (1 048 576). Must be a
   * non-negative safe integer (0 retains nothing): NaN/Infinity satisfy no
   * eviction comparison, so a malformed cap silently DISARMS the bound —
   * construction throws instead, even alongside an injected `exec`.
   */
  maxOutputBytes?: number;
}

export class AgentCliError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AgentCliError';
  }
}

const DEFAULT_TIMEOUT_MS = 600_000;
const DEFAULT_MAX_OUTPUT_BYTES = 1024 * 1024;
// setTimeout's delay ceiling (signed 32-bit ms, ~24.8 days): Node clamps
// anything above it DOWN TO 1ms, so a "generous" timeout would SIGKILL every
// CLI instantly.
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

/**
 * Construction-time guard for the numeric safety knobs. Each bounds a
 * runaway CLI (timeoutMs → SIGKILL budget, maxOutputBytes → retained-tail
 * cap), and each silently DISARMS on a malformed value instead of failing:
 * NaN satisfies no comparison (the accumulator evicts nothing; the D2
 * pendingTtlMs guard passes vacuously), `totalBytes <= Infinity` is always
 * true, setTimeout reinterprets NaN/negative delays as 0 and beyond-2^31-1
 * delays as 1ms, and fractional/negative byte caps corrupt the truncation
 * arithmetic. Same posture as the pendingTtlMs check below: a PRESENT-but-
 * malformed value is a misconfiguration to reject loudly, never to
 * reinterpret.
 */
function assertIntegerOption(
  connectorId: string,
  name: 'timeoutMs' | 'maxOutputBytes',
  value: number,
  min: number,
  max: number,
): void {
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  ) {
    throw new TypeError(
      `agent CLI connector ${connectorId}: ${name} must be an integer in [${min}, ${max}] (got ${typeof value}: ${String(value)}) — a malformed value silently disarms the bound it configures instead of failing it`,
    );
  }
}

const inputSchema = z.object({
  prompt: z.string().min(1),
  cwd: z.string().optional(),
  model: z.string().optional(),
});

const outputSchema = z.object({
  text: z.string(),
  exitCode: z.number(),
  command: z.string(),
  simulated: z.boolean().optional(),
});

// Structural node:child_process subset — breakwater compiles without node
// types, and a static `import 'node:child_process'` would break non-Node
// bundles even though only Node ever calls this path.
interface SpawnedProcess {
  stdout: {
    on(event: 'data', listener: (chunk: unknown) => void): unknown;
  } | null;
  stderr: {
    on(event: 'data', listener: (chunk: unknown) => void): unknown;
  } | null;
  on(event: 'error', listener: (error: unknown) => void): unknown;
  on(event: 'close', listener: (code: number | null) => void): unknown;
  kill(signal?: string): unknown;
}

interface ChildProcessModule {
  spawn(
    command: string,
    args: readonly string[],
    options: { cwd?: string; stdio: readonly [string, string, string] },
  ): SpawnedProcess;
}

interface TimerGlobals {
  setTimeout(handler: () => void, timeoutMs: number): unknown;
  clearTimeout(handle: unknown): void;
}

function nodeChildProcess(): ChildProcessModule {
  const proc = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process;
  const childProcess = proc?.getBuiltinModule?.('node:child_process') as
    | ChildProcessModule
    | undefined;
  if (!childProcess) {
    throw new AgentCliError(
      'agent CLI connectors execute via node:child_process, which this runtime does not provide — run on Node.js or inject an `exec` implementation',
    );
  }
  return childProcess;
}

// TextDecoder/TextEncoder looked up off globalThis — like nodeChildProcess()
// above, structurally typed rather than assumed ambient: both are
// unconditional Node/Workers/browser globals (no import, no @types/node, no
// DOM lib needed to typecheck), which is what the default exec's real chunks
// (always Uint8Array — node:child_process's 'pipe' stdio, no encoding ever
// set) actually run under.
function globalTextDecoder(): TextDecoderLike {
  const Ctor = (globalThis as { TextDecoder?: new () => TextDecoderLike })
    .TextDecoder;
  if (!Ctor) {
    throw new AgentCliError(
      'agent CLI connectors decode captured process output via the global TextDecoder, which this runtime does not provide',
    );
  }
  return new Ctor();
}

function globalTextEncoder(): TextEncoderLike {
  const Ctor = (globalThis as { TextEncoder?: new () => TextEncoderLike })
    .TextEncoder;
  if (!Ctor) {
    throw new AgentCliError(
      'agent CLI connectors encode captured process output via the global TextEncoder, which this runtime does not provide',
    );
  }
  return new Ctor();
}

// The bounded tail buffer lives in tail-accumulator.ts (package-internal —
// the './agent-cli' subpath maps to this module only) so tests can drive
// chunk-boundary geometry deterministically; the AgentCliError-throwing
// global lookups above are injected into it here.
const TEXT_CODEC_LOOKUPS: TextCodecLookups = {
  encoder: globalTextEncoder,
  decoder: globalTextDecoder,
};

function createDefaultExec(maxOutputBytes: number): AgentCliExec {
  return (command, args, options) => {
    const { spawn } = nodeChildProcess();
    const timers = globalThis as unknown as TimerGlobals;
    return new Promise<AgentCliExecResult>((resolve, reject) => {
      // No shell: args go straight to execve, so prompt content can't inject.
      const child = spawn(command, args, {
        cwd: options.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const stdout = tailAccumulator(maxOutputBytes, TEXT_CODEC_LOOKUPS);
      const stderr = tailAccumulator(maxOutputBytes, TEXT_CODEC_LOOKUPS);
      let settled = false;
      const timer = timers.setTimeout(() => {
        settled = true;
        child.kill('SIGKILL');
        reject(
          new AgentCliError(
            `'${command}' timed out after ${options.timeoutMs}ms`,
          ),
        );
      }, options.timeoutMs);
      child.stdout?.on('data', (chunk) => {
        stdout.push(chunk);
      });
      child.stderr?.on('data', (chunk) => {
        stderr.push(chunk);
      });
      child.on('error', (error) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        reject(
          new AgentCliError(`failed to spawn '${command}': ${String(error)}`),
        );
      });
      child.on('close', (code) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        resolve({
          stdout: stdout.value(),
          stderr: stderr.value(),
          exitCode: code ?? -1,
        });
      });
    });
  };
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max)}…` : text;
}

/**
 * Wrap any agent CLI as an approval-gated breakwater connector. The two
 * shipped definitions are createClaudeCodeConnector / createCodexConnector;
 * this is the seam for third-party CLIs.
 */
export function createAgentCliConnector(
  definition: AgentCliDefinition,
  options: AgentCliConnectorOptions = {},
): Tool<AgentCliInput, AgentCliOutput> {
  const connectorId = options.id ?? definition.id;
  if (options.timeoutMs !== undefined) {
    assertIntegerOption(
      connectorId,
      'timeoutMs',
      options.timeoutMs,
      1,
      MAX_TIMEOUT_MS,
    );
  }
  if (options.maxOutputBytes !== undefined) {
    // 0 is legal (retain nothing — exit-code-only usage); the accumulator
    // handles it. Validated even when `exec` is injected and the value goes
    // unused: present-but-malformed is a misconfiguration, not an exemption.
    assertIntegerOption(
      connectorId,
      'maxOutputBytes',
      options.maxOutputBytes,
      0,
      Number.MAX_SAFE_INTEGER,
    );
  }
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  if (options.idempotencyKey) {
    // D2: the store's stale-pending takeover TTL must outlive the longest
    // expected execute, or a still-running invocation can be taken over as
    // abandoned and spawned a second time. Only checked when the configured
    // store exposes a pendingTtlMs (D1IdempotencyStore) — other stores (e.g.
    // InMemoryIdempotencyStore) have no such window to violate, and a truly
    // ABSENT property stays exempt. A PRESENT-but-malformed value (e.g. a
    // string like '600000' from a hand-built or misconfigured store) is a
    // misconfiguration, not an exemption — silently skipping the guard here
    // would defeat the whole check by duck-typing coincidence.
    const pendingTtlMs = (
      options.policies?.idempotencyStore as
        | { pendingTtlMs?: unknown }
        | undefined
    )?.pendingTtlMs;
    if (pendingTtlMs !== undefined) {
      if (typeof pendingTtlMs !== 'number' || !Number.isFinite(pendingTtlMs)) {
        throw new TypeError(
          `agent CLI connector ${connectorId}: permissions.idempotencyKey is enabled with an idempotency store whose pendingTtlMs is present but not a finite number (got ${typeof pendingTtlMs}: ${String(pendingTtlMs)}) — a present pendingTtlMs must be a finite number of milliseconds, or the property must be absent entirely to stay exempt`,
        );
      }
      if (pendingTtlMs <= timeoutMs) {
        throw new TypeError(
          `agent CLI connector ${connectorId}: permissions.idempotencyKey is enabled with an idempotency store whose pendingTtlMs (${pendingTtlMs}ms) is <= this connector's timeoutMs (${timeoutMs}ms) — a still-running execution could be taken over as stale and spawned a second time; raise the store's pendingTtlMs above timeoutMs`,
        );
      }
    }
  }
  const exec =
    options.exec ??
    createDefaultExec(options.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES);
  const binary = options.binaryPath ?? definition.binary;

  const commandLine = (
    input: AgentCliInput,
  ): { args: string[]; command: string } => {
    // '--' before the prompt is load-bearing: it ends option parsing so the
    // caller-controlled prompt is a positional, never a smuggled flag. The
    // prompt is always last (both shipped CLIs take a trailing positional).
    const args = [...definition.buildFlags(input), '--', input.prompt];
    return { args, command: [binary, ...args].join(' ') };
  };

  return createConnector<AgentCliInput, AgentCliOutput>({
    id: connectorId,
    description: definition.description,
    inputSchema,
    outputSchema,
    permissions: {
      // Write-class: the CLI mutates the workspace it runs in.
      sideEffect: 'write',
      egress: definition.egress,
      requiresApproval: options.requiresApproval ?? true,
      dryRun: true,
      rateLimit: options.rateLimit,
      idempotencyKey: options.idempotencyKey,
    },
    policies: options.policies,
    execute: async (input) => {
      const { args, command } = commandLine(input);
      const result = await exec(binary, args, { cwd: input.cwd, timeoutMs });
      if (result.exitCode !== 0) {
        throw new AgentCliError(
          `'${command}' exited ${result.exitCode}: ${truncate(
            result.stderr || result.stdout,
            2000,
          )}`,
        );
      }
      return {
        text: definition.parseOutput?.(result.stdout) ?? result.stdout,
        exitCode: result.exitCode,
        command,
      };
    },
    // The simulation is the command preview — nothing spawns, so it is
    // side-effect-free by construction on every CLI.
    dryRunExecute: async (input) => ({
      text: '',
      exitCode: 0,
      command: commandLine(input).command,
      simulated: true,
    }),
  });
}

/**
 * Claude Code headless mode: `claude -p <prompt> --output-format json`.
 * stdout carries a JSON envelope whose `result` field is the agent's final
 * text; on parse drift the raw stdout is returned instead. Authentication
 * comes from the inherited environment/keychain, exactly like an
 * interactive `claude` session.
 */
export const CLAUDE_CODE_CLI: AgentCliDefinition = {
  id: 'agent-cli.claude-code',
  description:
    'Dispatch a coding task to the Claude Code CLI in headless mode and return its final result text',
  binary: 'claude',
  egress: ['api.anthropic.com'],
  // Prompt is a positional (`claude [options] [prompt]`); the wrapper appends
  // it behind '--'. `--flag=value` form keeps a '-'-leading model inert.
  buildFlags: (input) => [
    '-p',
    '--output-format=json',
    ...(input.model ? [`--model=${input.model}`] : []),
  ],
  parseOutput: (stdout) => {
    try {
      const parsed = JSON.parse(stdout) as { result?: unknown };
      return typeof parsed.result === 'string' ? parsed.result : undefined;
    } catch {
      return undefined;
    }
  },
};

/**
 * Codex non-interactive mode: `codex exec <prompt>`. Output is left as raw
 * stdout deliberately — Codex's machine-readable format is still moving
 * across versions, and raw text degrades gracefully everywhere.
 */
export const CODEX_CLI: AgentCliDefinition = {
  id: 'agent-cli.codex',
  description:
    'Dispatch a coding task to the Codex CLI non-interactively and return its output',
  binary: 'codex',
  egress: ['api.openai.com', 'chatgpt.com'],
  // `codex exec [OPTIONS] [PROMPT]`; the wrapper appends the prompt behind
  // '--' (clap honors it as end-of-options). `--model=value` keeps a
  // '-'-leading model from smuggling a flag.
  buildFlags: (input) => [
    'exec',
    ...(input.model ? [`--model=${input.model}`] : []),
  ],
};

export function createClaudeCodeConnector(
  options?: AgentCliConnectorOptions,
): Tool<AgentCliInput, AgentCliOutput> {
  return createAgentCliConnector(CLAUDE_CODE_CLI, options);
}

export function createCodexConnector(
  options?: AgentCliConnectorOptions,
): Tool<AgentCliInput, AgentCliOutput> {
  return createAgentCliConnector(CODEX_CLI, options);
}
