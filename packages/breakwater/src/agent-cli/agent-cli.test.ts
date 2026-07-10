import { RequestContext } from '@mastra/core/request-context';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { describe, expect, it, type Mock, vi } from 'vitest';

import {
  APPROVED_CONNECTORS_CONTEXT_KEY,
  ConnectorPolicyError,
  connectorManifest,
  DRY_RUN_CONTEXT_KEY,
} from '../connector-sdk/index.js';
import {
  AgentCliError,
  type AgentCliExec,
  type AgentCliInput,
  createAgentCliConnector,
  createClaudeCodeConnector,
  createCodexConnector,
} from './index.js';

function makeContext(
  options: { approved?: readonly string[]; dryRun?: boolean } = {},
): ToolExecutionContext {
  const requestContext = new RequestContext();
  if (options.approved) {
    requestContext.set(APPROVED_CONNECTORS_CONTEXT_KEY, options.approved);
  }
  if (options.dryRun) {
    requestContext.set(DRY_RUN_CONTEXT_KEY, true);
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
      ['exec', '--model=o5', '--', 'fix the bug'],
      { cwd: undefined, timeoutMs: 600_000 },
    );
    expect(output.text).toBe('codex says hi');
    expect(output.command).toBe('codex exec --model=o5 -- fix the bug');
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
      command: 'codex exec -- refactor',
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

  it('surfaces a non-zero exit as AgentCliError carrying stderr', async () => {
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
    expect(String(failure)).toContain('exited 2');
    expect(String(failure)).toContain('not logged in');
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
    expect(output.command).toBe('/opt/bin/claude -p -- p');
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

// The default runner is normally replaced by an injected `exec`; these tests
// exercise the real node:child_process path (spawn, timeout SIGKILL, ENOENT
// wrapping) using this Node process itself as a harmless stand-in binary.
describe('defaultExec (real node:child_process spawn)', () => {
  // A definition whose "binary" is the running Node and whose flags are an
  // inline script — no injected exec, so defaultExec actually spawns.
  function nodeScriptConnector(script: string, timeoutMs?: number) {
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
      { timeoutMs, requiresApproval: false },
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
    const tool = nodeScriptConnector('setTimeout(() => {}, 30000)', 250);

    // #when
    const failure = await run(tool, { prompt: 'p' }, makeContext()).catch(
      (error: unknown) => error,
    );

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(String(failure)).toContain('timed out');
  });

  it('wraps a spawn failure (nonexistent binary) as AgentCliError', async () => {
    // #given
    const tool = createAgentCliConnector(
      {
        id: 'agent-cli.missing',
        description: 'missing',
        binary: '/nonexistent/agent-cli-binary-xyz',
        egress: [],
        buildFlags: () => [],
      },
      { requiresApproval: false },
    );

    // #when
    const failure = await run(tool, { prompt: 'p' }, makeContext()).catch(
      (error: unknown) => error,
    );

    // #then
    expect(failure).toBeInstanceOf(AgentCliError);
    expect(String(failure)).toContain('failed to spawn');
  });

  it('surfaces a nonzero child exit as AgentCliError with stderr', async () => {
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
    expect(String(failure)).toContain('exited 3');
    expect(String(failure)).toContain('boom');
  });
});
