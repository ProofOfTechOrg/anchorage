// SPDX-License-Identifier: Apache-2.0
import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, type Mock, vi } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import {
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  ConnectorPolicyError,
  connectorManifest,
  DRY_RUN_CONTEXT_KEY,
  IDEMPOTENCY_KEY_CONTEXT_KEY,
  InMemoryIdempotencyStore,
} from '../connector-sdk/index.js';
import { WORKFLOW_SCOPE_CONTEXT_KEY } from '../policy-engine/index.js';
import {
  type AgentCliDefinition,
  AgentCliError,
  type AgentCliExec,
  type AgentCliInput,
  createAgentCliConnector,
  createClaudeCodeConnector,
  createCodexConnector,
} from './index.js';
import { tailAccumulator } from './tail-accumulator.js';

function makeContext(
  options: {
    approved?: readonly string[];
    dryRun?: boolean;
    idempotencyKey?: string;
  } = {},
): ToolExecutionContext {
  const requestContext = new RequestContext();
  if (options.approved) {
    requestContext.set(WORKFLOW_SCOPE_CONTEXT_KEY, 'workflow-1');
    requestContext.set('runId', 'run-1');
    requestContext.set(CONNECTOR_EXECUTION_CONTEXT_KEY, {
      kind: 'resume',
      workflowId: 'workflow-1',
      runId: 'run-1',
      suspension: {
        stepPath: ['gate'],
        suspendedAt: 1_000,
      },
    });
    requestContext.set(
      CONNECTOR_GRANTS_CONTEXT_KEY,
      options.approved.map((connectorId) => ({
        scope: 'suspension',
        connectorId,
        workflowId: 'workflow-1',
        runId: 'run-1',
        suspension: {
          stepPath: ['gate'],
          suspendedAt: 1_000,
        },
      })),
    );
  }
  if (options.dryRun) {
    requestContext.set(DRY_RUN_CONTEXT_KEY, true);
  }
  if (options.idempotencyKey) {
    requestContext.set(IDEMPOTENCY_KEY_CONTEXT_KEY, options.idempotencyKey);
  }
  return { requestContext } as unknown as ToolExecutionContext;
}

// Structural on purpose, matching connector-sdk.test.ts: Tool<...>
// instantiations don't cross-assign cleanly. Input is typed (not unknown)
// because every adapter here pins TInput = AgentCliInput.
async function run(
  tool: {
    execute?: (
      inputData: AgentCliInput,
      context: ToolExecutionContext,
    ) => Promise<unknown>;
  },
  input: AgentCliInput,
  context: ToolExecutionContext,
): Promise<unknown> {
  if (!tool.execute) throw new Error('tool has no execute');
  return tool.execute(input, context);
}

function mockExec(
  result: Partial<Awaited<ReturnType<AgentCliExec>>> = {},
  // vitest 4 types a bare vi.fn() as Mock<Procedure | Constructable>, which no
  // longer structurally satisfies AgentCliExec — type the mock at creation.
): Mock<AgentCliExec> {
  return vi.fn<AgentCliExec>().mockResolvedValue({
    stdout: '',
    stderr: '',
    exitCode: 0,
    ...result,
  });
}

const GRANTED = (id: string) => makeContext({ approved: [id] });
const PRIVATE_PROMPT = 'private-prompt-7b3b631a';
const PRIVATE_PROCESS_OUTPUT = 'private-process-output-10fd1f62';

function errorSurface(error: unknown): string {
  if (!(error instanceof Error)) return JSON.stringify(error);
  const structured = error as Error & Partial<AgentCliError>;
  return JSON.stringify({
    name: structured.name,
    message: structured.message,
    code: structured.code,
    connectorId: structured.connectorId,
    command: structured.command,
    exitCode: structured.exitCode,
    timeoutMs: structured.timeoutMs,
    systemCode: structured.systemCode,
    stdoutCaptured: structured.stdoutCaptured,
    stderrCaptured: structured.stderrCaptured,
  });
}

function privateDefinition(
  overrides: Partial<AgentCliDefinition> = {},
): AgentCliDefinition {
  return {
    id: 'agent-cli.private-probe',
    description: 'private surface probe',
    binary: 'private-probe',
    egress: [],
    buildFlags: () => [],
    ...overrides,
  };
}

describe('createClaudeCodeConnector', () => {
  it('builds headless args, forwards cwd/timeout, parses the JSON result', async () => {
    // #given
    const exec = mockExec({
      stdout: JSON.stringify({ result: 'done: created 3 files' }),
    });
    const tool = createClaudeCodeConnector({ exec, timeoutMs: 1234 });

    // #when
    const output = await run(
      tool,
      { prompt: 'add tests', cwd: '/repo', model: 'claude-sonnet-5' },
      GRANTED('agent-cli.claude-code'),
    );

    // #then — prompt is the trailing positional behind '--'; option values
    // use '=' form.
    expect(exec).toHaveBeenCalledTimes(1);
    expect(exec).toHaveBeenCalledWith(
      'claude',
      [
        '-p',
        '--output-format=json',
        '--permission-mode=acceptEdits',
        '--model=claude-sonnet-5',
        '--',
        'add tests',
      ],
      { cwd: '/repo', timeoutMs: 1234 },
    );
    expect(output).toMatchObject({
      text: 'done: created 3 files',
      exitCode: 0,
    });
  });

  it('falls back to raw stdout when the JSON envelope drifts', async () => {
    // #given
    const exec = mockExec({ stdout: 'plain text output' });
    const tool = createClaudeCodeConnector({ exec });

    // #when
    const output = (await run(
      tool,
      { prompt: 'p' },
      GRANTED('agent-cli.claude-code'),
    )) as { text: string };

    // #then
    expect(output.text).toBe('plain text output');
  });
});

describe('createCodexConnector', () => {
  it('builds exec args with the model before the prompt, returns raw stdout', async () => {
    // #given
    const exec = mockExec({ stdout: 'codex says hi' });
    const tool = createCodexConnector({ exec });

    // #when
    const output = (await run(
      tool,
      { prompt: 'fix the bug', model: 'o5' },
      GRANTED('agent-cli.codex'),
    )) as { text: string; command: string };

    // #then
    expect(exec).toHaveBeenCalledWith(
      'codex',
      ['exec', '--sandbox=workspace-write', '--model=o5', '--', 'fix the bug'],
      { cwd: undefined, timeoutMs: 600_000 },
    );
    expect(output.text).toBe('codex says hi');
    expect(output.command).toBe(
      'codex exec --sandbox=<value:redacted> --model=<value:redacted> -- <prompt:redacted>',
    );
  });
});

describe('agent CLI connector enforcement', () => {
  it('denies without a grant and never spawns (approval-gated by default)', async () => {
    // #given
    const exec = mockExec();
    const tool = createClaudeCodeConnector({ exec });

    // #when
    const failure = await run(tool, { prompt: 'p' }, makeContext()).catch(
      (error: unknown) => error,
    );

    // #then
    expect(failure).toBeInstanceOf(ConnectorPolicyError);
    expect(exec).not.toHaveBeenCalled();
  });

  it('declares a write-class, dry-runnable manifest with provider egress', () => {
    // #given
    const tool = createClaudeCodeConnector({ exec: mockExec() });

    // #when / #then
    expect(connectorManifest(tool)).toMatchObject({
      sideEffect: 'write',
      requiresApproval: true,
      dryRun: true,
      egress: ['api.anthropic.com'],
    });
  });

  it('dry-run returns the command preview without spawning or needing a grant', async () => {
    // #given
    const exec = mockExec();
    const tool = createCodexConnector({ exec });

    // #when — no grant, dry-run requested
    const output = await run(
      tool,
      { prompt: 'refactor' },
      makeContext({ dryRun: true }),
    );

    // #then
    expect(output).toEqual({
      text: '',
      exitCode: 0,
      command: 'codex exec --sandbox=<value:redacted> -- <prompt:redacted>',
      simulated: true,
    });
    expect(exec).not.toHaveBeenCalled();
  });

  it('honors requiresApproval: false for sandboxed setups', async () => {
    // #given
    const exec = mockExec({ stdout: 'ok' });
    const tool = createCodexConnector({ exec, requiresApproval: false });

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
    };

    // #then
    expect(output.text).toBe('ok');
  });

  it('surfaces a non-zero exit as a safe structured AgentCliError', async () => {
    // #given
    const exec = mockExec({ exitCode: 2, stderr: 'not logged in' });
    const tool = createClaudeCodeConnector({ exec });

    // #when
    const failure = await run(
      tool,
      { prompt: 'p' },
      GRANTED('agent-cli.claude-code'),
    ).catch((error: unknown) => error);

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(failure).toMatchObject({
      code: 'nonzero-exit',
      connectorId: 'agent-cli.claude-code',
      exitCode: 2,
      stdoutCaptured: false,
      stderrCaptured: true,
      command:
        'claude -p --output-format=<value:redacted> --permission-mode=<value:redacted> -- <prompt:redacted>',
    });
    expect(String(failure)).not.toContain('not logged in');
  });

  it('supports binaryPath and id overrides for parallel configurations', async () => {
    // #given
    const exec = mockExec({ stdout: '{"result":"x"}' });
    const tool = createAgentCliConnector(
      {
        id: 'agent-cli.claude-code',
        description: 'custom',
        binary: 'claude',
        egress: ['api.anthropic.com'],
        buildFlags: () => ['-p'],
      },
      { exec, binaryPath: '/opt/bin/claude', id: 'agent-cli.claude-sandboxed' },
    );

    // #when
    const output = (await run(
      tool,
      { prompt: 'p' },
      GRANTED('agent-cli.claude-sandboxed'),
    )) as { command: string };

    // #then
    expect(exec).toHaveBeenCalledWith('/opt/bin/claude', ['-p', '--', 'p'], {
      cwd: undefined,
      timeoutMs: 600_000,
    });
    expect(output.command).toBe('/opt/bin/claude -p -- <prompt:redacted>');
  });

  it('neutralizes a flag-shaped prompt by placing it behind the -- separator', async () => {
    // #given — a prompt that is a dangerous claude flag verbatim
    const exec = mockExec({ stdout: '{"result":"ok"}' });
    const tool = createClaudeCodeConnector({ exec });
    const smuggled = '--allow-dangerously-skip-permissions';

    // #when
    await run(tool, { prompt: smuggled }, GRANTED('agent-cli.claude-code'));

    // #then — it appears only after '--', so the CLI parser sees it as the
    // positional prompt, never as an option
    const args = exec.mock.calls[0]?.[1] as string[];
    const separator = args.indexOf('--');
    expect(separator).toBeGreaterThanOrEqual(0);
    expect(args.indexOf(smuggled)).toBe(separator + 1);
    expect(args.slice(0, separator)).not.toContain(smuggled);
  });

  it('binds a flag-shaped model value with = instead of smuggling a flag', async () => {
    // #given — model value that is itself a codex flag
    const exec = mockExec({ stdout: 'ok' });
    const tool = createCodexConnector({ exec });

    // #when
    await run(
      tool,
      { prompt: 'p', model: '--dangerously-bypass-approvals-and-sandbox' },
      GRANTED('agent-cli.codex'),
    );

    // #then — the value is fused to --model= as one argv token, so no bare
    // dangerous flag is ever passed
    const args = exec.mock.calls[0]?.[1] as string[];
    expect(args).toContain(
      '--model=--dangerously-bypass-approvals-and-sandbox',
    );
    expect(args).not.toContain('--dangerously-bypass-approvals-and-sandbox');
  });

  it('rejects an empty prompt before spawning (schema z.string().min(1))', async () => {
    // #given
    const exec = mockExec();
    const tool = createClaudeCodeConnector({ exec });

    // #when — Mastra validates inputSchema before execute, so an empty prompt
    // never reaches the gate or the spawn
    const result = (await run(
      tool,
      { prompt: '' },
      GRANTED('agent-cli.claude-code'),
    )) as { error?: boolean };

    // #then
    expect(result.error).toBe(true);
    expect(exec).not.toHaveBeenCalled();
  });
});

describe('agent CLI private-data boundaries', () => {
  it('passes the real prompt only in argv and returns a redacted display command', async () => {
    const audit = new AuditLogger();
    const exec = mockExec({ stdout: 'safe result' });
    const tool = createCodexConnector({
      exec,
      requiresApproval: false,
      policies: { audit },
    });

    const output = (await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    )) as { text: string; command: string };

    expect(exec).toHaveBeenCalledWith(
      'codex',
      ['exec', '--sandbox=workspace-write', '--', PRIVATE_PROMPT],
      {
        cwd: undefined,
        timeoutMs: 600_000,
      },
    );
    expect(output).toEqual({
      text: 'safe result',
      exitCode: 0,
      command: 'codex exec --sandbox=<value:redacted> -- <prompt:redacted>',
    });
    expect(JSON.stringify(output.command)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
  });

  it('gives buildFlags a frozen input with a redacted prompt on real and dry-run paths', async () => {
    const audit = new AuditLogger();
    const observed: Array<{ prompt: string; frozen: boolean }> = [];
    const definition = privateDefinition({
      buildFlags: (input) => {
        observed.push({
          prompt: input.prompt,
          frozen: Object.isFrozen(input),
        });
        throw new Error(PRIVATE_PROMPT);
      },
    });
    const tool = createAgentCliConnector(definition, {
      exec: mockExec(),
      requiresApproval: false,
      policies: { audit },
    });

    const realFailure = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);
    const dryFailure = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext({ dryRun: true }),
    ).catch((error: unknown) => error);

    expect(observed).toEqual([
      { prompt: '<prompt:redacted>', frozen: true },
      { prompt: '<prompt:redacted>', frozen: true },
    ]);
    expect(realFailure).toMatchObject({ code: 'flags-failed' });
    expect(dryFailure).toMatchObject({ code: 'flags-failed' });
    expect(errorSurface(realFailure)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(dryFailure)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
  });

  it('contains injected executor failures and process output', async () => {
    const audit = new AuditLogger();
    const rejectingExec = vi
      .fn<AgentCliExec>()
      .mockRejectedValue(
        new Error(`${PRIVATE_PROMPT}:${PRIVATE_PROCESS_OUTPUT}`),
      );
    const rejectedTool = createAgentCliConnector(privateDefinition(), {
      exec: rejectingExec,
      requiresApproval: false,
      policies: { audit },
    });

    const rejected = await run(
      rejectedTool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    expect(rejected).toMatchObject({
      code: 'exec-failed',
      command: 'private-probe -- <prompt:redacted>',
    });
    expect(errorSurface(rejected)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(rejected)).not.toContain(PRIVATE_PROCESS_OUTPUT);

    const nonzeroTool = createAgentCliConnector(privateDefinition(), {
      exec: mockExec({
        stdout: PRIVATE_PROCESS_OUTPUT,
        stderr: PRIVATE_PROCESS_OUTPUT,
        exitCode: 9,
      }),
      requiresApproval: false,
      policies: { audit },
    });
    const nonzero = await run(
      nonzeroTool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    expect(nonzero).toMatchObject({
      code: 'nonzero-exit',
      exitCode: 9,
      stdoutCaptured: true,
      stderrCaptured: true,
    });
    expect(errorSurface(nonzero)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(nonzero)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_PROCESS_OUTPUT,
    );
  });

  it('contains parser exceptions and malformed parser values', async () => {
    const audit = new AuditLogger();
    const throwing = createAgentCliConnector(
      privateDefinition({
        parseOutput: () => {
          throw new Error(`${PRIVATE_PROMPT}:${PRIVATE_PROCESS_OUTPUT}`);
        },
      }),
      {
        exec: mockExec({ stdout: PRIVATE_PROCESS_OUTPUT }),
        requiresApproval: false,
        policies: { audit },
      },
    );
    const invalid = createAgentCliConnector(
      privateDefinition({
        parseOutput: (() => PRIVATE_PROCESS_OUTPUT.length) as unknown as (
          stdout: string,
        ) => string,
      }),
      {
        exec: mockExec({ stdout: PRIVATE_PROCESS_OUTPUT }),
        requiresApproval: false,
        policies: { audit },
      },
    );

    const thrown = await run(
      throwing,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);
    const malformed = await run(
      invalid,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    expect(thrown).toMatchObject({ code: 'parse-output-failed' });
    expect(malformed).toMatchObject({ code: 'parse-output-failed' });
    expect(errorSurface(thrown)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(thrown)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(errorSurface(malformed)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(malformed)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_PROCESS_OUTPUT,
    );
  });

  it('contains malformed executor results and schema validation messages', async () => {
    const audit = new AuditLogger();
    const malformedExec = vi.fn<AgentCliExec>().mockResolvedValue({
      stdout: PRIVATE_PROCESS_OUTPUT,
      stderr: PRIVATE_PROCESS_OUTPUT,
      exitCode: Number.NaN,
    });
    const tool = createAgentCliConnector(privateDefinition(), {
      exec: malformedExec,
      requiresApproval: false,
      policies: { audit },
    });

    const malformed = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);
    const validation = await run(
      tool,
      {
        prompt: PRIVATE_PROMPT,
        cwd: 42 as unknown as string,
      },
      makeContext(),
    );

    expect(malformed).toMatchObject({ code: 'invalid-exec-result' });
    expect(errorSurface(malformed)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(malformed)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(validation).toMatchObject({
      error: true,
      message: 'Agent CLI input or output validation failed.',
    });
    expect(JSON.stringify(validation)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(validation)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_PROCESS_OUTPUT,
    );
  });

  it('redacts legacy replay commands and sanitizes invalid cached output', async () => {
    const audit = new AuditLogger();
    const store = new InMemoryIdempotencyStore();
    store.put('agent-cli.codex:legacy-valid', {
      result: {
        text: 'cached result',
        exitCode: 0,
        command: `codex exec --model=${PRIVATE_PROCESS_OUTPUT} -- ${PRIVATE_PROMPT}`,
      },
    });
    store.put('agent-cli.codex:legacy-invalid', {
      result: {
        text: 7,
        exitCode: 0,
        command: `codex exec -- ${PRIVATE_PROMPT}`,
        raw: PRIVATE_PROCESS_OUTPUT,
      },
    });
    const exec = mockExec();
    const tool = createCodexConnector({
      exec,
      idempotencyKey: true,
      requiresApproval: false,
      policies: { audit, idempotencyStore: store },
    });

    const replay = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext({ idempotencyKey: 'legacy-valid' }),
    );
    const invalid = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext({ idempotencyKey: 'legacy-invalid' }),
    );

    expect(replay).toEqual({
      text: 'cached result',
      exitCode: 0,
      command: '<command:redacted>',
    });
    expect(invalid).toMatchObject({
      error: true,
      message: 'Agent CLI input or output validation failed.',
    });
    expect(JSON.stringify(replay)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(replay)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(invalid)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(invalid)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_PROCESS_OUTPUT,
    );
    expect(exec).not.toHaveBeenCalled();
  });

  it('contains evaluator exceptions at the connector boundary and in audit', async () => {
    const audit = new AuditLogger();
    const tool = createCodexConnector({
      exec: mockExec(),
      requiresApproval: false,
      policies: {
        audit,
        evaluators: [
          {
            name: 'private-evaluator',
            evaluate: () => {
              throw new Error(`${PRIVATE_PROMPT}:${PRIVATE_PROCESS_OUTPUT}`);
            },
          },
        ],
      },
    });

    const failure = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    expect(failure).toMatchObject({ code: 'connector-failed' });
    expect(errorSurface(failure)).not.toContain(PRIVATE_PROMPT);
    expect(errorSurface(failure)).not.toContain(PRIVATE_PROCESS_OUTPUT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(
      PRIVATE_PROCESS_OUTPUT,
    );
  });
});

// Structurally shaped like D1IdempotencyStore's public surface — agent-cli
// duck-types the pendingTtlMs field rather than importing the D1 class.
// Shared by the TTL-guard suite and the numeric-option suite below.
function fakeD1LikeStore(pendingTtlMs: number) {
  return {
    pendingTtlMs,
    get: () => undefined,
    put: () => {},
    reserve: () => ({ state: 'reserved' as const }),
    release: () => {},
  };
}

describe('idempotency TTL guard (audit D2)', () => {
  // Same shape as fakeD1LikeStore, but pendingTtlMs is deliberately NOT typed as a number — a
  // hand-built or misconfigured store duck-typing the field wrong (a string,
  // NaN, ...). Returned from a function (not an inline literal at the call
  // site) so TS's excess-property check on `policies.idempotencyStore`
  // (typed IdempotencyStore, which does not declare pendingTtlMs) does not
  // fire — the connector itself accesses the field structurally, same as it
  // does for fakeD1LikeStore's real-number case.
  function fakeStoreWithMalformedTtl(pendingTtlMs: unknown) {
    return { ...fakeD1LikeStore(0), pendingTtlMs };
  }

  it('rejects a store whose pendingTtlMs is <= the effective timeoutMs', () => {
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: 600_000,
        policies: { idempotencyStore: fakeD1LikeStore(600_000) },
      }),
    ).toThrow(/pendingTtlMs.*timeoutMs/);
  });

  it('accepts a store whose pendingTtlMs comfortably exceeds timeoutMs', () => {
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: 600_000,
        policies: { idempotencyStore: fakeD1LikeStore(900_000) },
      }),
    ).not.toThrow();
  });

  it('accepts a store with no pendingTtlMs field (nothing to validate)', () => {
    // #given — e.g. InMemoryIdempotencyStore
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        policies: { idempotencyStore: new InMemoryIdempotencyStore() },
      }),
    ).not.toThrow();
  });

  it('rejects a zero pendingTtlMs (falsy but present — not exempt)', () => {
    // #given — 0 is a present, finite, and unusably short TTL; the guard
    // must compare it, not skip it as absent via a falsy check
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: 600_000,
        policies: { idempotencyStore: fakeD1LikeStore(0) },
      }),
    ).toThrow(/pendingTtlMs.*timeoutMs/);
  });

  it('does not validate the store when idempotencyKey is not enabled', () => {
    // #given — a too-short pendingTtlMs, but idempotencyKey is off
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        timeoutMs: 600_000,
        policies: { idempotencyStore: fakeD1LikeStore(1) },
      }),
    ).not.toThrow();
  });

  it('rejects a PRESENT pendingTtlMs that is not a finite number — present-but-malformed is a misconfiguration, not an exemption', () => {
    // #given — a store shaped like D1IdempotencyStore but with a
    // string-typed pendingTtlMs (duck-typing gone wrong upstream)
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: 600_000,
        policies: { idempotencyStore: fakeStoreWithMalformedTtl('600000') },
      }),
    ).toThrow(/pendingTtlMs is present but not a finite number/);
  });

  it('rejects a PRESENT non-finite pendingTtlMs (NaN) the same way', () => {
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: 600_000,
        policies: {
          idempotencyStore: fakeStoreWithMalformedTtl(Number.NaN),
        },
      }),
    ).toThrow(/pendingTtlMs is present but not a finite number/);
  });
});

describe('numeric option validation (2026-07-11 review)', () => {
  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    0,
    -1,
    1.5,
    2 ** 31,
  ])('rejects timeoutMs %s at construction', (timeoutMs) => {
    // #given — setTimeout reinterprets NaN/negative delays as 0 and clamps
    // beyond-2^31-1 delays to 1ms: every one of these means instant SIGKILL
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({ exec: mockExec(), timeoutMs }),
    ).toThrow(/timeoutMs must be an integer/);
  });

  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    -1,
    10.5,
  ])('rejects maxOutputBytes %s at construction', (maxOutputBytes) => {
    // #given — Infinity never evicts (totalBytes <= Infinity holds
    // forever), NaN satisfies no comparison, negatives/fractions corrupt
    // the byte math: the runaway-output bound must fail loudly, not disarm
    // #when / #then
    expect(() => createClaudeCodeConnector({ maxOutputBytes })).toThrow(
      /maxOutputBytes must be an integer/,
    );
  });

  it('rejects a malformed maxOutputBytes even when an injected exec would ignore it', () => {
    // #given — present-but-malformed is a misconfiguration, not an exemption
    // (the pendingTtlMs posture); dropping the exec injection later must not
    // silently inherit a disarmed cap
    // #when / #then
    expect(() =>
      createClaudeCodeConnector({
        exec: mockExec(),
        maxOutputBytes: Number.NaN,
      }),
    ).toThrow(/maxOutputBytes/);
  });

  it('rejects a NaN timeoutMs BEFORE the D2 pendingTtlMs guard it would defeat', () => {
    // #given — `pendingTtlMs <= NaN` is false, so an accepted NaN timeout
    // would let ANY store (here an unusably short 1ms TTL) pass the takeover
    // guard vacuously
    // #when / #then — the thrown error is the timeoutMs one, not a pass
    expect(() =>
      createClaudeCodeConnector({
        idempotencyKey: true,
        timeoutMs: Number.NaN,
        policies: { idempotencyStore: fakeD1LikeStore(1) },
      }),
    ).toThrow(/timeoutMs must be an integer/);
  });

  it('accepts the boundary values (1 and 2^31-1 ms; 0 and MAX_SAFE_INTEGER bytes)', () => {
    // #when / #then — the guard rejects malformed values, not extreme ones
    expect(() =>
      createClaudeCodeConnector({ exec: mockExec(), timeoutMs: 1 }),
    ).not.toThrow();
    expect(() =>
      createClaudeCodeConnector({ exec: mockExec(), timeoutMs: 2 ** 31 - 1 }),
    ).not.toThrow();
    expect(() =>
      createClaudeCodeConnector({ exec: mockExec(), maxOutputBytes: 0 }),
    ).not.toThrow();
    expect(() =>
      createClaudeCodeConnector({
        exec: mockExec(),
        maxOutputBytes: Number.MAX_SAFE_INTEGER,
      }),
    ).not.toThrow();
  });
});

// Chunk-boundary geometry is not reachable deterministically through a real
// pipe (Node coalesces 'data' events), so these drive the accumulator
// directly — the exact QA repro class from the 2026-07-11 audit follow-up: a
// multi-byte codepoint SPLIT ACROSS stream chunks at the truncation cut.
describe('tailAccumulator (cross-chunk UTF-8 boundary)', () => {
  const codecs = {
    encoder: () => new TextEncoder(),
    decoder: () => new TextDecoder(),
  };

  it('drops a codepoint split 2+2 across chunks at the cut instead of decoding orphaned continuation bytes (QA repro)', () => {
    // #given — 😀 (F0 9F 98 80) split across two 'data' events, cap 3 bytes
    const tail = tailAccumulator(3, codecs);
    tail.push(new Uint8Array([0xf0, 0x9f]));
    tail.push(new Uint8Array([0x98, 0x80]));

    // #when
    const value = tail.value();

    // #then — the straddling codepoint is dropped whole; the old
    // stop-at-chunk-boundary skip decoded "��" here
    expect(value).toBe('…[truncated to the last 3 bytes]…');
  });

  it('retains a clean suffix when trailing data follows the split codepoint', () => {
    // #given — the split emoji straddles the cut, clean ASCII follows
    const tail = tailAccumulator(8, codecs);
    tail.push(new Uint8Array([0xf0, 0x9f]));
    tail.push(new Uint8Array([0x98, 0x80]));
    tail.push(new TextEncoder().encode('abcdefgh'));

    // #when
    const value = tail.value();

    // #then
    expect(value).toBe('…[truncated to the last 8 bytes]…abcdefgh');
  });

  it('never emits a replacement char under byte-at-a-time streaming of emoji', () => {
    // #given — every byte arrives as its own chunk (worst-case pipe split)
    const tail = tailAccumulator(64, codecs);
    const bytes = new TextEncoder().encode(`hello ${'😀'.repeat(50)}`);
    for (const b of bytes) tail.push(new Uint8Array([b]));

    // #when
    const value = tail.value();

    // #then — a clean whole-codepoint suffix within the byte budget
    expect(value).not.toContain('�');
    const retained = value.replace('…[truncated to the last 64 bytes]…', '');
    expect(new TextEncoder().encode(retained).length).toBeLessThanOrEqual(64);
    expect(retained).toMatch(/^(?:😀)+$/u);
  });
});

// The default runner is normally replaced by an injected `exec`; these tests
// exercise the real node:child_process path (spawn, timeout SIGKILL, ENOENT
// wrapping) using this Node process itself as a harmless stand-in binary.
describe('defaultExec (real node:child_process spawn)', () => {
  // A definition whose "binary" is the running Node and whose flags are an
  // inline script — no injected exec, so defaultExec actually spawns.
  function nodeScriptConnector(
    script: string,
    timeoutMs?: number,
    maxOutputBytes?: number,
    audit?: AuditLogger,
  ) {
    return createAgentCliConnector(
      {
        id: 'agent-cli.node-probe',
        description: 'probe',
        binary: process.execPath,
        egress: [],
        // `-e <script>` runs the script; the wrapper appends `-- <prompt>`,
        // which Node exposes as process.argv (ignored here).
        buildFlags: () => ['-e', script],
      },
      {
        timeoutMs,
        requiresApproval: false,
        maxOutputBytes,
        policies: audit ? { audit } : undefined,
      },
    );
  }

  it('spawns, captures stdout, and returns exitCode 0', async () => {
    // #given
    const tool = nodeScriptConnector('process.stdout.write("hello from node")');

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
      exitCode: number;
    };

    // #then
    expect(output.text).toBe('hello from node');
    expect(output.exitCode).toBe(0);
  });

  it('SIGKILLs and rejects when the child exceeds the timeout', async () => {
    // #given — a child that would sleep far past the budget
    const audit = new AuditLogger();
    const tool = nodeScriptConnector(
      'setTimeout(() => {}, 30000)',
      250,
      undefined,
      audit,
    );

    // #when
    const failure = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(failure).toMatchObject({
      code: 'timeout',
      timeoutMs: 250,
    });
    expect(errorSurface(failure)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
  });

  it('wraps a spawn failure (nonexistent binary) as AgentCliError', async () => {
    // #given
    const audit = new AuditLogger();
    const tool = createAgentCliConnector(
      {
        id: 'agent-cli.missing',
        description: 'missing',
        binary: '/nonexistent/agent-cli-binary-xyz',
        egress: [],
        buildFlags: () => [],
      },
      { requiresApproval: false, policies: { audit } },
    );

    // #when
    const failure = await run(
      tool,
      { prompt: PRIVATE_PROMPT },
      makeContext(),
    ).catch((error: unknown) => error);

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(failure).toMatchObject({
      code: 'spawn-failed',
      systemCode: 'ENOENT',
    });
    expect(errorSurface(failure)).not.toContain(PRIVATE_PROMPT);
    expect(JSON.stringify(audit.events())).not.toContain(PRIVATE_PROMPT);
  });

  it('surfaces a nonzero child exit without copying stderr into the error', async () => {
    // #given — writes to stderr and exits 3
    const tool = nodeScriptConnector(
      'process.stderr.write("boom"); process.exit(3)',
    );

    // #when
    const failure = await run(tool, { prompt: 'p' }, makeContext()).catch(
      (error: unknown) => error,
    );

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(failure).toMatchObject({
      code: 'nonzero-exit',
      exitCode: 3,
      stderrCaptured: true,
    });
    expect(String(failure)).not.toContain('boom');
  });

  it('caps retained stdout to the tail and marks truncation (audit D5)', async () => {
    // #given — a small cap so the test stays fast; the script writes a head
    // marker, filler, then a tail marker
    const tool = nodeScriptConnector(
      "process.stdout.write('HEAD' + 'x'.repeat(50) + 'TAIL')",
      undefined,
      20,
    );

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
    };

    // #then — the tail (including 'TAIL') survives; the head marker is
    // gone, and truncation is marked — the answer text arrives last
    expect(output.text.endsWith('TAIL')).toBe(true);
    expect(output.text).not.toContain('HEAD');
    expect(output.text).toContain('truncated');
  });

  it('maxOutputBytes: 0 retains nothing but still reports the exit and marks truncation', async () => {
    // #given — the legal floor of the validated range: exit-code-only usage
    const tool = nodeScriptConnector(
      "process.stdout.write('anything at all')",
      undefined,
      0,
    );

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
      exitCode: number;
    };

    // #then
    expect(output.exitCode).toBe(0);
    expect(output.text).toBe('…[truncated to the last 0 bytes]…');
  });

  it('leaves pure-ASCII truncation exactly as before (byte length == char length)', async () => {
    // #given — 30 ASCII bytes, cap 10: unambiguous, never enters the
    // UTF-8 continuation-byte-skip path
    const tool = nodeScriptConnector(
      "process.stdout.write('abcdefghijklmnopqrstuvwxyz1234')",
      undefined,
      10,
    );

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
    };

    // #then — exactly the last 10 ASCII bytes retained, byte-for-byte
    expect(output.text).toBe('…[truncated to the last 10 bytes]…uvwxyz1234');
  });

  it('caps retained output by real UTF-8 bytes, not UTF-16 code units (CJK, audit fix 2026-07-11)', async () => {
    // #given — a script writing 200 CJK characters (3 UTF-8 bytes, 1 UTF-16
    // code unit each) well past a 100-byte cap; a char-length cap (the
    // pre-fix bug) would retain ~3x maxOutputBytes
    const tool = nodeScriptConnector(
      "process.stdout.write('中'.repeat(200))",
      undefined,
      100,
    );

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
    };

    // #then — the retained text's OWN utf-8 byte length (excluding the
    // ASCII truncation marker) sits at/near the 100-byte cap, not ~300 bytes
    const marker = '…[truncated to the last 100 bytes]…';
    expect(output.text.startsWith(marker)).toBe(true);
    const retained = output.text.slice(marker.length);
    const retainedBytes = Buffer.byteLength(retained, 'utf8');
    expect(retainedBytes).toBeLessThanOrEqual(100);
    expect(retainedBytes).toBeGreaterThanOrEqual(90);
  });

  it('cuts at an odd byte boundary without leaving a lone surrogate or replacement char at the head (emoji)', async () => {
    // #given — 50 emoji (4 UTF-8 bytes each = 200 bytes); 101 is not a
    // multiple of 4, so the RAW byte cut lands mid-codepoint before the
    // continuation-byte skip kicks in
    const tool = nodeScriptConnector(
      "process.stdout.write('\u{1F600}'.repeat(50))",
      undefined,
      101,
    );

    // #when
    const output = (await run(tool, { prompt: 'p' }, makeContext())) as {
      text: string;
    };

    // #then — well-formed decode: no replacement character, and the head is
    // a complete codepoint (>= 0x10000), never a lone/unpaired surrogate half
    const marker = '…[truncated to the last 101 bytes]…';
    expect(output.text.startsWith(marker)).toBe(true);
    const retained = output.text.slice(marker.length);
    expect(retained).not.toContain('�');
    expect(retained.codePointAt(0) ?? 0).toBeGreaterThanOrEqual(0x1_0000);
  });
});
