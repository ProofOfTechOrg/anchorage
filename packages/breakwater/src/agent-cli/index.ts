// SPDX-License-Identifier: Apache-2.0
// Agent CLI adapters — Claude Code and Codex as Mastra tools (Phase 4
// "Agent CLI adapters"). Each adapter is a breakwater connector, so the full
// permission manifest governs it: an agent CLI edits files and runs commands
// on the host, which makes it write-class and approval-gated by default (the
// CONNECTOR_GRANTS_CONTEXT_KEY grant, mintable through flowsafe's
// approval queue), with dry-run simulation always available (the command
// preview, never a spawn) and optional rate limiting.
//
// Node-only at execution time: the default runner spawns the CLI via
// node:child_process (resolved through process.getBuiltinModule so
// non-Node bundles never see a static node: import). Constructing a
// connector works anywhere; calling one without an `exec` override outside
// Node throws AgentCliError. The `exec` seam is also the test surface.

import {
  isValidationError,
  type Tool,
  type ValidationError,
} from '@mastra/core/tools';
import { z } from 'zod';

import {
  registerSafeAuditError,
  safeAuditErrorSummary,
} from '../audit/safe-error.js';
import type { ConnectorPolicies } from '../connector-sdk/index.js';
import {
  ConnectorPolicyError,
  createConnector,
} from '../connector-sdk/index.js';
import type {
  TextCodecLookups,
  TextDecoderLike,
  TextEncoderLike,
} from './tail-accumulator.js';
import { tailAccumulator } from './tail-accumulator.js';

/** Input accepted by an agent CLI connector. */
export interface AgentCliInput {
  /** The task prompt handed to the agent CLI. */
  prompt: string;
  /** Working directory the CLI runs in (the workspace it may modify). */
  cwd?: string;
  /** Model override forwarded to the CLI. */
  model?: string;
}

/** Successful result returned by an agent CLI connector. */
export interface AgentCliOutput {
  /** The agent's final text (parsed when the CLI supports it, else raw stdout). */
  text: string;
  /** Process exit code. Successful connector calls return zero. */
  exitCode: number;
  /**
   * Redacted display command for diagnostics. It is not executable and never
   * contains the prompt.
   */
  command: string;
  /** Present (true) when this was a dry-run simulation; nothing was spawned. */
  simulated?: boolean;
}

/** Raw process result returned by an injected {@link AgentCliExec}. */
export interface AgentCliExecResult {
  /** Standard output captured from the process. */
  stdout: string;
  /** Standard error captured from the process. */
  stderr: string;
  /** Integer process exit code. */
  exitCode: number;
}

/** Spawn seam — inject in tests or to sandbox/containerize execution. */
export type AgentCliExec = (
  command: string,
  args: readonly string[],
  options: { cwd?: string; timeoutMs: number },
) => Promise<AgentCliExecResult>;

/** Describes how to invoke and parse one agent CLI. */
export interface AgentCliDefinition {
  /** Default connector id. */
  id: string;
  /** Tool description exposed to the model. */
  description: string;
  /** Default executable name or path. */
  binary: string;
  /** Hosts the CLI itself calls; declared on the manifest for egress policy. */
  egress: readonly string[];
  /**
   * The option flags and any subcommand, excluding the prompt. The wrapper
   * supplies a redacted prompt value to this callback and appends the real
   * prompt after `--`. Put option values in `--flag=value` form so a value
   * starting with `-` cannot become a separate flag.
   */
  buildFlags: (input: AgentCliInput) => string[];
  /**
   * Extract the agent's final text from stdout; return undefined to fall
   * back to raw stdout (e.g. on output-format drift across CLI versions).
   */
  parseOutput?: (stdout: string) => string | undefined;
}

/** Runtime and policy options for an agent CLI connector. */
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
   * taken over as stale and spawned a second time.
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

/** Stable failure categories reported by {@link AgentCliError}. */
export type AgentCliErrorCode =
  | 'unknown'
  | 'runtime-unavailable'
  | 'codec-unavailable'
  | 'flags-failed'
  | 'invalid-flags'
  | 'spawn-failed'
  | 'timeout'
  | 'exec-failed'
  | 'invalid-exec-result'
  | 'nonzero-exit'
  | 'parse-output-failed'
  | 'connector-failed';

/** Structured, redacted diagnostic fields for {@link AgentCliError}. */
export interface AgentCliErrorMetadata {
  /** Stable error category. */
  code: AgentCliErrorCode;
  /** Connector that failed. */
  connectorId?: string;
  /** Redacted display command. */
  command?: string;
  /** Nonzero process exit code, when a process completed unsuccessfully. */
  exitCode?: number;
  /** Configured process timeout, when the failure was timeout-related. */
  timeoutMs?: number;
  /** Platform process error code, such as `ENOENT`, when safely available. */
  systemCode?: string;
  /** Whether any standard output was captured. Contents are never included. */
  stdoutCaptured?: boolean;
  /** Whether any standard error was captured. Contents are never included. */
  stderrCaptured?: boolean;
}

/** Redacted error raised while preparing, executing, or parsing an agent CLI call. */
export class AgentCliError extends Error {
  /** Stable error category. */
  readonly code: AgentCliErrorCode;
  /** Connector that failed. */
  readonly connectorId?: string;
  /** Redacted display command, when command construction succeeded. */
  readonly command?: string;
  /** Nonzero process exit code, when available. */
  readonly exitCode?: number;
  /** Configured process timeout, when the failure was timeout-related. */
  readonly timeoutMs?: number;
  /** Platform process error code, such as `ENOENT`, when safely available. */
  readonly systemCode?: string;
  /** Whether any standard output was captured. Contents are never included. */
  readonly stdoutCaptured?: boolean;
  /** Whether any standard error was captured. Contents are never included. */
  readonly stderrCaptured?: boolean;

  constructor(message: string, metadata: Partial<AgentCliErrorMetadata> = {}) {
    super(message);
    this.name = 'AgentCliError';
    this.code = metadata.code ?? 'unknown';
    this.connectorId = metadata.connectorId;
    this.command = metadata.command;
    this.exitCode = metadata.exitCode;
    this.timeoutMs = metadata.timeoutMs;
    this.systemCode = metadata.systemCode;
    this.stdoutCaptured = metadata.stdoutCaptured;
    this.stderrCaptured = metadata.stderrCaptured;
  }
}

const DEFAULT_TIMEOUT_MS = 600_000;
const DEFAULT_MAX_OUTPUT_BYTES = 1024 * 1024;
const REDACTED_PROMPT = '<prompt:redacted>';
const REDACTED_OPTION_VALUE = '<value:redacted>';
// setTimeout's delay ceiling (signed 32-bit ms, ~24.8 days): Node clamps
// anything above it DOWN TO 1ms, so a "generous" timeout would SIGKILL every
// CLI instantly.
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

interface DefaultExecFailureOptions {
  code:
    | 'runtime-unavailable'
    | 'codec-unavailable'
    | 'spawn-failed'
    | 'timeout';
  timeoutMs?: number;
  systemCode?: string;
}

class DefaultExecFailure {
  readonly code: DefaultExecFailureOptions['code'];
  readonly timeoutMs?: number;
  readonly systemCode?: string;

  constructor(options: DefaultExecFailureOptions) {
    this.code = options.code;
    this.timeoutMs = options.timeoutMs;
    this.systemCode = options.systemCode;
  }
}

const AGENT_CLI_ERROR_MESSAGES: Readonly<Record<AgentCliErrorCode, string>> = {
  unknown: 'Agent CLI execution failed.',
  'runtime-unavailable':
    'Agent CLI execution requires Node.js or an injected exec implementation.',
  'codec-unavailable':
    'Agent CLI process output could not be encoded or decoded.',
  'flags-failed': 'Agent CLI flag construction failed.',
  'invalid-flags': 'Agent CLI flag construction returned invalid flags.',
  'spawn-failed': 'Agent CLI process could not be started.',
  timeout: 'Agent CLI process exceeded its execution timeout.',
  'exec-failed': 'Agent CLI executor failed.',
  'invalid-exec-result': 'Agent CLI executor returned an invalid result.',
  'nonzero-exit': 'Agent CLI process exited unsuccessfully.',
  'parse-output-failed': 'Agent CLI output parsing failed.',
  'connector-failed': 'Agent CLI connector execution failed.',
};

function safeSystemCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === 'string' && /^[A-Z][A-Z0-9_]{0,31}$/.test(code)
    ? code
    : undefined;
}

function createAgentCliError(
  code: AgentCliErrorCode,
  connectorId: string,
  metadata: Omit<Partial<AgentCliErrorMetadata>, 'code' | 'connectorId'> = {},
): AgentCliError {
  const error = new AgentCliError(AGENT_CLI_ERROR_MESSAGES[code], {
    code,
    connectorId,
    ...metadata,
  });
  const detail: Record<string, string | number | boolean> = {
    errorCode: code,
  };
  if (metadata.exitCode !== undefined) detail.exitCode = metadata.exitCode;
  if (metadata.timeoutMs !== undefined) detail.timeoutMs = metadata.timeoutMs;
  if (metadata.systemCode !== undefined)
    detail.systemCode = metadata.systemCode;
  if (metadata.stdoutCaptured !== undefined) {
    detail.stdoutCaptured = metadata.stdoutCaptured;
  }
  if (metadata.stderrCaptured !== undefined) {
    detail.stderrCaptured = metadata.stderrCaptured;
  }
  return registerSafeAuditError(error, {
    reason: AGENT_CLI_ERROR_MESSAGES[code],
    detail,
  });
}

/**
 * Construction-time guard for the numeric safety knobs. Each bounds a
 * runaway CLI (timeoutMs → SIGKILL budget, maxOutputBytes → retained-tail
 * cap), and each silently DISARMS on a malformed value instead of failing:
 * NaN satisfies no comparison (the accumulator evicts nothing and the
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
    throw new DefaultExecFailure({ code: 'runtime-unavailable' });
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
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
  try {
    return new Ctor();
  } catch {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
}

function globalTextEncoder(): TextEncoderLike {
  const Ctor = (globalThis as { TextEncoder?: new () => TextEncoderLike })
    .TextEncoder;
  if (!Ctor) {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
  try {
    return new Ctor();
  } catch {
    throw new DefaultExecFailure({ code: 'codec-unavailable' });
  }
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
    let childProcess: ChildProcessModule;
    try {
      childProcess = nodeChildProcess();
    } catch (error) {
      return Promise.reject(error);
    }
    const timers = globalThis as unknown as TimerGlobals;
    return new Promise<AgentCliExecResult>((resolve, reject) => {
      // No shell: args go straight to execve, so prompt content can't inject.
      let child: SpawnedProcess;
      try {
        child = childProcess.spawn(command, args, {
          cwd: options.cwd,
          stdio: ['ignore', 'pipe', 'pipe'],
        });
      } catch (error) {
        reject(
          new DefaultExecFailure({
            code: 'spawn-failed',
            systemCode: safeSystemCode(error),
          }),
        );
        return;
      }
      const stdout = tailAccumulator(maxOutputBytes, TEXT_CODEC_LOOKUPS);
      const stderr = tailAccumulator(maxOutputBytes, TEXT_CODEC_LOOKUPS);
      let settled = false;
      const killChild = (): void => {
        try {
          child.kill('SIGKILL');
        } catch {
          // The process may already be gone. The safe failure below remains
          // the public result.
        }
      };
      const rejectCodecFailure = (): void => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        killChild();
        reject(new DefaultExecFailure({ code: 'codec-unavailable' }));
      };
      const timer = timers.setTimeout(() => {
        if (settled) return;
        settled = true;
        killChild();
        reject(
          new DefaultExecFailure({
            code: 'timeout',
            timeoutMs: options.timeoutMs,
          }),
        );
      }, options.timeoutMs);
      child.stdout?.on('data', (chunk) => {
        try {
          stdout.push(chunk);
        } catch {
          rejectCodecFailure();
        }
      });
      child.stderr?.on('data', (chunk) => {
        try {
          stderr.push(chunk);
        } catch {
          rejectCodecFailure();
        }
      });
      child.on('error', (error) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        reject(
          new DefaultExecFailure({
            code: 'spawn-failed',
            systemCode: safeSystemCode(error),
          }),
        );
      });
      child.on('close', (code) => {
        if (settled) return;
        settled = true;
        timers.clearTimeout(timer);
        try {
          resolve({
            stdout: stdout.value(),
            stderr: stderr.value(),
            exitCode: code ?? -1,
          });
        } catch {
          reject(new DefaultExecFailure({ code: 'codec-unavailable' }));
        }
      });
    });
  };
}

function sanitizeValidationError<T>(
  error: ValidationError<T>,
): ValidationError<T> {
  return {
    error: true,
    message: 'Agent CLI input or output validation failed.',
    validationErrors: error.validationErrors,
  };
}

function isAgentCliOutput(value: unknown): value is AgentCliOutput {
  if (typeof value !== 'object' || value === null) return false;
  const output = value as Partial<AgentCliOutput>;
  return (
    typeof output.text === 'string' &&
    typeof output.exitCode === 'number' &&
    typeof output.command === 'string' &&
    (output.simulated === undefined || typeof output.simulated === 'boolean')
  );
}

function redactCommandPrompt(command: string, prompt: unknown): string {
  if (command.endsWith(` -- ${REDACTED_PROMPT}`)) return command;
  if (typeof prompt !== 'string' || prompt.length === 0) {
    return '<command:redacted>';
  }
  const rawPromptSuffix = ` -- ${prompt}`;
  if (!command.endsWith(rawPromptSuffix)) return '<command:redacted>';
  // A replay from an older store can contain unredacted option values in its
  // display command. Rebuilding that command safely is impossible because the
  // joined string has lost argv boundaries, so redact the whole legacy value.
  return '<command:redacted>';
}

function sanitizeAgentCliOutput(
  output: AgentCliOutput,
  prompt: unknown,
): AgentCliOutput {
  return {
    text: output.text,
    exitCode: output.exitCode,
    command: redactCommandPrompt(output.command, prompt),
    ...(output.simulated === undefined ? {} : { simulated: output.simulated }),
  };
}

function redactDisplayFlag(flag: string): string {
  const separator = flag.indexOf('=');
  return flag.startsWith('--') && separator > 2
    ? `${flag.slice(0, separator + 1)}${REDACTED_OPTION_VALUE}`
    : flag;
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
    let flags: unknown;
    try {
      flags = definition.buildFlags(
        Object.freeze({ ...input, prompt: REDACTED_PROMPT }),
      );
    } catch {
      throw createAgentCliError('flags-failed', connectorId);
    }
    if (
      !Array.isArray(flags) ||
      flags.some((flag) => typeof flag !== 'string')
    ) {
      throw createAgentCliError('invalid-flags', connectorId);
    }
    const args = [...flags, '--', input.prompt];
    return {
      args,
      command: [
        binary,
        ...flags.map(redactDisplayFlag),
        '--',
        REDACTED_PROMPT,
      ].join(' '),
    };
  };

  const connector = createConnector<AgentCliInput, AgentCliOutput>({
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
      let result: AgentCliExecResult;
      try {
        result = await exec(binary, args, { cwd: input.cwd, timeoutMs });
      } catch (error) {
        if (error instanceof DefaultExecFailure) {
          throw createAgentCliError(error.code, connectorId, {
            command,
            timeoutMs: error.timeoutMs,
            systemCode: error.systemCode,
          });
        }
        throw createAgentCliError('exec-failed', connectorId, { command });
      }
      if (
        typeof result !== 'object' ||
        result === null ||
        typeof result.stdout !== 'string' ||
        typeof result.stderr !== 'string' ||
        !Number.isSafeInteger(result.exitCode)
      ) {
        throw createAgentCliError('invalid-exec-result', connectorId, {
          command,
        });
      }
      if (result.exitCode !== 0) {
        throw createAgentCliError('nonzero-exit', connectorId, {
          command,
          exitCode: result.exitCode,
          stdoutCaptured: result.stdout.length > 0,
          stderrCaptured: result.stderr.length > 0,
        });
      }
      let text: string;
      try {
        const parsed = definition.parseOutput?.(result.stdout);
        if (parsed !== undefined && typeof parsed !== 'string') {
          throw createAgentCliError('parse-output-failed', connectorId, {
            command,
          });
        }
        text = parsed ?? result.stdout;
      } catch (error) {
        if (
          error instanceof AgentCliError &&
          safeAuditErrorSummary(error) !== undefined
        ) {
          throw error;
        }
        throw createAgentCliError('parse-output-failed', connectorId, {
          command,
        });
      }
      return {
        text,
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

  const execute = connector.execute;
  if (!execute) {
    throw createAgentCliError('connector-failed', connectorId);
  }
  connector.execute = async (input, context) => {
    try {
      const result = await execute.call(connector, input, context);
      if (isValidationError(result)) {
        return sanitizeValidationError(result);
      }
      if (isAgentCliOutput(result)) {
        return sanitizeAgentCliOutput(
          result,
          (input as Partial<AgentCliInput>).prompt,
        );
      }
      return result;
    } catch (error) {
      if (
        error instanceof AgentCliError &&
        safeAuditErrorSummary(error) !== undefined
      ) {
        throw error;
      }
      if (error instanceof ConnectorPolicyError) {
        throw new ConnectorPolicyError(
          error.connector,
          error.policy,
          'agent CLI connector policy denied execution',
        );
      }
      throw createAgentCliError('connector-failed', connectorId);
    }
  };
  return connector;
}

/**
 * Claude Code headless mode:
 * `claude -p --output-format=json --permission-mode=acceptEdits
 * [--model=value] -- <prompt>`.
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
    '--permission-mode=acceptEdits',
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
 * Codex non-interactive mode:
 * `codex exec --sandbox=workspace-write [--model=value] -- <prompt>`.
 * Output is left as raw stdout deliberately — Codex's machine-readable format
 * is still moving across versions, and raw text degrades gracefully everywhere.
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
    '--sandbox=workspace-write',
    ...(input.model ? [`--model=${input.model}`] : []),
  ],
};

/**
 * Create an approval-gated Claude Code connector using
 * {@link CLAUDE_CODE_CLI}.
 */
export function createClaudeCodeConnector(
  options?: AgentCliConnectorOptions,
): Tool<AgentCliInput, AgentCliOutput> {
  return createAgentCliConnector(CLAUDE_CODE_CLI, options);
}

/**
 * Create an approval-gated Codex connector using {@link CODEX_CLI}.
 */
export function createCodexConnector(
  options?: AgentCliConnectorOptions,
): Tool<AgentCliInput, AgentCliOutput> {
  return createAgentCliConnector(CODEX_CLI, options);
}
