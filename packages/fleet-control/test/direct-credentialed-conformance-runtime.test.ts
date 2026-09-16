// SPDX-License-Identifier: Apache-2.0

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { runInNewContext } from 'node:vm';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DirectBootstrapError } from '../scripts/direct-credentialed-bootstrap.mjs';
import * as directRuntime from '../scripts/direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_ADMISSION_VARIABLES,
  DIRECT_CONFORMANCE_USAGE,
  DIRECT_CREDENTIAL_VARIABLES,
  DIRECT_FIXED_OUTPUT,
  DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
  DIRECT_OUTPUT_PREFIX,
  DIRECT_USAGE_DIAGNOSTIC,
  type DirectConformanceMode,
  type DirectConformanceModules,
  directWritesStderr,
  parseDirectConformanceArgs,
  resolveDirectExitCode,
  runDirectConformance,
} from '../scripts/direct-credentialed-conformance-runtime.mjs';
import {
  DIRECT_EVIDENCE_KEYS,
  DirectEvidenceWriteError,
} from '../scripts/direct-credentialed-evidence.mjs';
import type { DirectInvocationClient } from '../scripts/direct-credentialed-invocation.mjs';
import { validateProviderAuth } from '../scripts/direct-credentialed-provider.mjs';
import {
  DIRECT_RUN_MAX_RESUME_COUNT,
  type DirectRunSnapshot,
  DirectRunStateError,
  type DirectTeardownFailure,
} from '../scripts/direct-credentialed-run-state.mjs';
import {
  cleanupDirectRunState,
  closed,
  completeScenario,
  fixture,
  maximalTeardown,
  opened,
  teardownState,
} from './fixtures/direct-run-state-builder.js';

const probes = vi.hoisted(() => ({ sdk: vi.fn() }));
vi.mock('cloudflare', async (importOriginal) => {
  const actual = await importOriginal<typeof import('cloudflare')>();
  return {
    ...actual,
    default: class extends actual.default {
      constructor(options: ConstructorParameters<typeof actual.default>[0]) {
        probes.sdk(options);
        super(options);
      }
    },
  };
});
const env = {
  PATH: process.env.PATH,
  CLOUDFLARE_ACCOUNT_ID: 'account',
  CLOUDFLARE_API_TOKEN: 'private-api-seed',
  FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'private-invoke-seed',
};
const now = () => Date.parse('2026-09-13T00:00:00.000Z');
/** The bytes the entry writes to each descriptor for an in-process result. */
function streamsOf(result: Awaited<ReturnType<typeof runDirectConformance>>) {
  return {
    stdout: result.stdoutLine ?? '',
    stderr: directWritesStderr(result) ? (result.stderrLine ?? '') : '',
  };
}
function expectCredentialSafeOutput(
  result:
    | Awaited<ReturnType<typeof runDirectConformance>>
    | { code: number | null; stdout: string; stderr: string },
  credentials: Readonly<Record<string, string | undefined>> = env,
) {
  if ('stdoutLine' in result) {
    // Total rendering: only a stderr-only refusal withholds the stdout summary,
    // so a result that would print nothing at all fails here.
    expect(result.stdoutLine === null).toBe(result.stderrOnly === true);
    if (result.stdoutLine !== null) {
      expect(result.stdoutLine.startsWith(DIRECT_OUTPUT_PREFIX)).toBe(true);
      expect(result.stdoutLine.endsWith('\n')).toBe(true);
      expect(Buffer.byteLength(result.stdoutLine)).toBeLessThanOrEqual(4096);
      expect(result.stdoutLine).toBe(
        `${DIRECT_OUTPUT_PREFIX}${result.stderrLine}`,
      );
      expect(
        JSON.parse(result.stdoutLine.slice(DIRECT_OUTPUT_PREFIX.length)),
      ).toEqual(result.summary);
    }
    if (result.stderrOnly) {
      expect(result.exitCode).toBe(2);
      if (result.stderrLine !== null)
        expect(DIRECT_FIXED_OUTPUT).toContain(result.stderrLine);
    }
  }
  // One discriminant for the whole helper, and one check per descriptor.
  const { stdout, stderr } =
    'stdoutLine' in result ? streamsOf(result) : result;
  for (const variable of DIRECT_CREDENTIAL_VARIABLES) {
    const credential = credentials[variable];
    if (typeof credential !== 'string' || credential.length === 0) continue;
    // Either descriptor order, because a reader interleaves two pipes.
    expect((stdout + stderr).includes(credential)).toBe(false);
    expect((stderr + stdout).includes(credential)).toBe(false);
  }
  return { stdout, stderr };
}
afterEach(async () => {
  expect(probes.sdk).not.toHaveBeenCalled();
  vi.clearAllMocks();
  await cleanupDirectRunState();
});
const NO_RETAINED_IDENTITIES = {
  fleetUuid: null,
  quotaUuid: null,
  exportBucket: null,
  scriptName: null,
  activeVersionId: null,
};
/**
 * The invalid-input diagnostic for `variable`, in one place rather than per
 * test. `freezes complete fixed output lines from the emitted constants` checks
 * it against the CLI's own list for every admission variable.
 */
function invalidInputLine(variable: string) {
  return `${JSON.stringify({ code: 'invalid-input', variable })}\n`;
}
/** Reads the credentials the real runtime reads, so the entry's guard is armed. */
function armGuard(input: { env: Record<string, string | undefined> }) {
  for (const variable of DIRECT_CREDENTIAL_VARIABLES) void input.env[variable];
}

async function world(fleetUuid?: string) {
  const f = await fixture(1000);
  const actual = await opened({ ...f.input, mode: 'run' });
  let snapshot: DirectRunSnapshot = actual.snapshot();
  if (fleetUuid !== undefined)
    snapshot = {
      ...snapshot,
      bootstrap: {
        context: {
          names: f.prepared.names,
          zoneId: 'zone',
          zoneName: 'example.test',
          accountWorkersDevSubdomain: 'attested-account',
          dispatch: { kind: 'empty', count: 0 },
        },
        fleet: { uuid: fleetUuid, name: f.prepared.names.fleetDatabase },
        quota: null,
        exports: null,
        upload: null,
        active: null,
        ingress: null,
        controlReadOrdinal: null,
        pending: null,
      },
    };
  const journal = {
    ...actual,
    snapshot: () => snapshot,
    recordResume: vi.fn(async () => {
      snapshot = {
        ...snapshot,
        resumeCount: Math.min(
          DIRECT_RUN_MAX_RESUME_COUNT,
          (snapshot.resumeCount ?? 0) + 1,
        ),
      };
    }),
    close: vi.fn(() => closed(actual)),
  };
  const modules = {
    preflight: vi.fn(async () => f.prepared),
    openRunState: vi.fn(async () => journal),
    inspectRunState: vi.fn(async () => ({
      directory: actual.directory,
      snapshot,
      close: journal.close,
    })),
    bootstrap: vi.fn<DirectConformanceModules['bootstrap']>(
      async () => ({}) as DirectInvocationClient,
    ),
    scenario: vi.fn<DirectConformanceModules['scenario']>(async () => ({
      status: 'restart-required',
    })),
    teardown: vi.fn<DirectConformanceModules['teardown']>(async () => ({
      status: 'cleaned',
      facts: {
        retainedIdentities: NO_RETAINED_IDENTITIES,
        receipts: maximalTeardown().receipts,
        residual: null,
        providerRequests: 7,
        failure: null,
      },
    })),
    distPresent: vi.fn(() => true),
  };
  const set = (fields: Partial<DirectRunSnapshot>) => {
    snapshot = { ...snapshot, ...fields };
  };
  const run = async (mode: DirectConformanceMode = 'resume') => {
    const result = await runDirectConformance({
      mode,
      configPath: f.configPath,
      env,
      modules,
      now,
      git: () => 'a'.repeat(40),
    });
    expectCredentialSafeOutput(result);
    return result;
  };
  return { f, journal, modules, set, run, snapshot: () => snapshot };
}

function retained(
  w: Awaited<ReturnType<typeof world>>,
  publish: boolean,
  reason: DirectTeardownFailure = 'invalid-state',
) {
  w.modules.teardown.mockImplementation(async () => {
    if (publish)
      w.set({
        teardown: {
          ...teardownState(),
          phase: 'refused',
          failure: 'scenario-incomplete',
        },
      });
    return {
      status: 'retained',
      reason,
      phase: 'refused',
      facts: {
        retainedIdentities: NO_RETAINED_IDENTITIES,
        receipts: teardownState().receipts,
        residual: null,
        providerRequests: 9,
        failure: 'invalid-state',
      },
    };
  });
}

const entry = fileURLToPath(
  new URL('../scripts/direct-credentialed-conformance.mjs', import.meta.url),
);
/**
 * One run directory holding one `--import` module, for the spawned entry tests.
 * A module that writes back into the directory receives it.
 */
async function preloaded(source: string | ((directory: string) => string)) {
  const f = await fixture();
  const preload = join(f.directory, 'preload.mjs');
  await writeFile(
    preload,
    typeof source === 'string' ? source : source(f.directory),
  );
  return { f, preload };
}
async function child(
  command: string,
  args: string[],
  cwd?: string,
  extraEnv: Record<string, string | undefined> = {},
  readCredentials = extraEnv,
) {
  const spawned = spawn(command, args, {
    cwd,
    env: { PATH: process.env.PATH, ...extraEnv },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  spawned.stdout.setEncoding('utf8');
  spawned.stderr.setEncoding('utf8');
  spawned.stdout.on('data', (chunk) => {
    stdout += chunk;
  });
  spawned.stderr.on('data', (chunk) => {
    stderr += chunk;
  });
  const code = await new Promise<number | null>((resolve, reject) => {
    spawned.once('error', reject);
    spawned.once('close', resolve);
  });
  const result = { code, stdout, stderr };
  expectCredentialSafeOutput(result, readCredentials);
  return result;
}

async function entryWithRuntime(
  run: typeof runDirectConformance,
  credentials: Record<string, string | undefined>,
  reenterStderr = false,
) {
  const result = { code: null as number | null, stdout: '', stderr: '' };
  const handlers = new Map<string, () => void>();
  const source = await readFile(entry, 'utf8');
  // The harness evaluates the entry with its imports supplied as context. A
  // strip that stopped matching would evaluate a different program, so the
  // removal is checked rather than assumed, and the names come from the real
  // module namespace so they cannot drift from the entry's import list.
  const stripped = source.replace(/^import \{[\s\S]*?\} from '[^']+';\n/m, '');
  expect(stripped).not.toBe(source);
  expect(stripped).not.toMatch(/^\s*import\b/mu);
  runInNewContext(stripped, {
    ...directRuntime,
    runDirectConformance: run,
    process: {
      argv: ['node', entry, '--run'],
      env: credentials,
      set exitCode(code: number) {
        result.code = code;
      },
      on: (event: string, handler: () => void) => handlers.set(event, handler),
      stdout: {
        write: (line: string) => {
          result.stdout += line;
        },
      },
      stderr: {
        write: (line: string) => {
          result.stderr += line;
          if (reenterStderr) {
            reenterStderr = false;
            handlers.get('unhandledRejection')?.();
          }
        },
      },
    },
  });
  await vi.waitFor(() => expect(result.code).not.toBeNull());
  expectCredentialSafeOutput(result, credentials);
  return result;
}

describe.sequential('direct CLI runtime', () => {
  it('freezes complete fixed output lines from the emitted constants', () => {
    expect(Object.isFrozen(DIRECT_FIXED_OUTPUT)).toBe(true);
    expect(DIRECT_FIXED_OUTPUT).toEqual([
      'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":false}\n',
      '{"code":"evidence-failed","evidenceWritten":false}\n',
      'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":true}\n',
      '{"code":"evidence-failed","evidenceWritten":true}\n',
      '{"code":"usage"}\n',
      '{"code":"internal-error"}\n',
      '{"code":"invalid-input","variable":"CLOUDFLARE_ACCOUNT_ID"}\n',
      '{"code":"invalid-input","variable":"CLOUDFLARE_API_TOKEN"}\n',
      '{"code":"invalid-input","variable":"FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET"}\n',
    ]);
    expect(DIRECT_FIXED_OUTPUT).toContain(DIRECT_USAGE_DIAGNOSTIC);
    expect(DIRECT_FIXED_OUTPUT).toContain(DIRECT_INTERNAL_ERROR_DIAGNOSTIC);
    // Membership derived from the emitted constants rather than copied: every
    // admission variable contributes its diagnostic, and nothing else does.
    for (const variable of DIRECT_ADMISSION_VARIABLES)
      expect(DIRECT_FIXED_OUTPUT).toContain(invalidInputLine(variable));
    expect(
      DIRECT_FIXED_OUTPUT.filter((line) => line.includes('invalid-input')),
    ).toEqual(DIRECT_ADMISSION_VARIABLES.map(invalidInputLine));
  });

  it.each([
    'DIRECT_CONFORMANCE',
    'code',
    'false',
    'enc',
    'evidence-failed',
    'evidenceWritten',
    'true',
    'usage',
    'internal-error',
    'E {"',
    'invalid-input',
    'variable',
  ])('refuses a fixed-output credential with a safe fixed diagnostic or silence (%s)', async (secret) => {
    const f = await fixture(680);
    // The refusal under test is the fixed-output collision, not the guard
    // beside it: every table value is a token this row's sibling admits.
    expect(() => validateProviderAuth(secret)).not.toThrow();
    expect(DIRECT_FIXED_OUTPUT.some((line) => line.includes(secret))).toBe(
      true,
    );
    for (const variable of DIRECT_CREDENTIAL_VARIABLES) {
      const credentials = { ...env, [variable]: secret };
      const distPresent = vi.fn(() => false);
      const openRunState = vi.fn();
      const diagnostic = invalidInputLine(variable);
      const stderrLine = diagnostic.includes(secret) ? null : diagnostic;
      const result = await runDirectConformance({
        mode: 'run',
        configPath: f.configPath,
        env: credentials,
        now,
        modules: { distPresent, openRunState },
      });
      expect(result).toEqual({
        exitCode: 2,
        summary: { code: 'invalid-input', variable },
        evidencePath: null,
        stdoutLine: null,
        stderrLine,
        stderrOnly: true,
      });
      expectCredentialSafeOutput(result, credentials);
      expect(distPresent).not.toHaveBeenCalled();
      expect(openRunState).not.toHaveBeenCalled();
      expect(probes.sdk).not.toHaveBeenCalled();
      expect(existsSync(f.base)).toBe(false);
      const spawned = await child(
        process.execPath,
        [entry, '--run'],
        undefined,
        {
          CLOUDFLARE_ACCOUNT_ID: credentials.CLOUDFLARE_ACCOUNT_ID,
          CLOUDFLARE_API_TOKEN: credentials.CLOUDFLARE_API_TOKEN,
          FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET:
            credentials.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
          FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
        },
      );
      expect(spawned.code).toBe(2);
      expect(spawned.stdout).toBe('');
      expect(spawned.stderr).toBe(stderrLine ?? '');
      expect(existsSync(f.base)).toBe(false);
    }
  });

  it('admits an account ID contained in the fixed output vocabulary', async () => {
    const f = await fixture(1000);
    const distPresent = vi.fn(() => false);
    const result = await runDirectConformance({
      mode: 'run',
      configPath: f.configPath,
      env: { ...env, CLOUDFLARE_ACCOUNT_ID: 'code' },
      now,
      modules: { distPresent },
    });
    expect(result.summary).toEqual({
      code: 'dist-missing',
      command: 'pnpm build',
    });
    expectCredentialSafeOutput(result);
    expect(distPresent).toHaveBeenCalledOnce();
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    'invalid-input',
    'variable',
    'code',
  ])('silently refuses a protocol-word token with another invalid environment value (%s)', async (secret) => {
    const f = await fixture(680);
    const credentials = {
      ...env,
      CLOUDFLARE_ACCOUNT_ID: secret === 'code' ? '' : env.CLOUDFLARE_ACCOUNT_ID,
      CLOUDFLARE_API_TOKEN: secret,
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: '',
    };
    const distPresent = vi.fn();
    const result = await runDirectConformance({
      mode: 'run',
      configPath: f.configPath,
      env: credentials,
      now,
      modules: { distPresent },
    });
    expectCredentialSafeOutput(result, credentials);
    expect(result).toMatchObject({
      exitCode: 2,
      stdoutLine: null,
      stderrLine: null,
      stderrOnly: true,
    });
    expect(distPresent).not.toHaveBeenCalled();
    const spawned = await child(process.execPath, [entry, '--run'], undefined, {
      CLOUDFLARE_ACCOUNT_ID: credentials.CLOUDFLARE_ACCOUNT_ID,
      CLOUDFLARE_API_TOKEN: secret,
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: '',
      FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
    });
    expect(spawned).toEqual({
      code: 2,
      stdout: '',
      stderr: '',
    });
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('prints fixed diagnostics for invalid argv despite colliding %s', async (variable) => {
    const usage = await child(
      process.execPath,
      [entry, '--bad'],
      undefined,
      { [variable]: 'usage' },
      {},
    );
    expect(usage).toEqual({
      code: 2,
      stdout: '',
      stderr: DIRECT_USAGE_DIAGNOSTIC,
    });
    const { preload } = await preloaded(
      `setTimeout(() => { throw new Error('secret-stack'); }, 0);\n`,
    );
    const failure = await child(
      process.execPath,
      ['--import', preload, entry, '--bad'],
      undefined,
      { [variable]: 'code' },
      {},
    );
    expect(failure).toEqual({
      code: 1,
      stdout: '',
      stderr: DIRECT_USAGE_DIAGNOSTIC + DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
    });
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ] as const)('drops a later rejection diagnostic when %s spans consecutive stderr lines', async (variable) => {
    const { f, preload } = await preloaded(
      `const write = process.stderr.write.bind(process.stderr);
process.stderr.write = (...args) => {
  const result = write(...args);
  if (args[0].includes('invalid-input'))
    setTimeout(() => { Promise.reject(new Error('secret-stack')); }, 0);
  return result;
};\n`,
    );
    const credentials = {
      ...env,
      [variable]: '}\n{',
      FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
    };
    const result = await child(
      process.execPath,
      ['--import', preload, entry, '--run'],
      undefined,
      credentials,
    );
    const refusal = invalidInputLine(variable);
    expect(refusal).not.toContain(credentials[variable]);
    expect(DIRECT_INTERNAL_ERROR_DIAGNOSTIC).not.toContain(
      credentials[variable],
    );
    expect(refusal + DIRECT_INTERNAL_ERROR_DIAGNOSTIC).toContain(
      credentials[variable],
    );
    expect(result).toEqual({
      code: 1,
      stdout: '',
      stderr: refusal,
    });
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('drops the stderr copy when %s spans stdout and stderr', async (variable) => {
    const summary = invalidInputLine('FLEET_DIRECT_CONFORMANCE_CONFIG');
    const stdout = `${DIRECT_OUTPUT_PREFIX}${summary}`;
    const secret = 'CONFIG"}\n{"code"';
    expect(stdout).not.toContain(secret);
    expect(summary).not.toContain(secret);
    expect(stdout + summary).toContain(secret);
    const result = await entryWithRuntime(
      async (input) => {
        armGuard(input);
        return {
          exitCode: 2,
          summary: JSON.parse(summary),
          evidencePath: null,
          stdoutLine: stdout,
          stderrLine: summary,
        };
      },
      { ...env, [variable]: secret },
    );
    expect(result).toEqual({ code: 2, stdout, stderr: '' });
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('exits 5 when the entry suppresses a dynamic summary containing %s', async (variable) => {
    const secret = 'fleet-dynamic-worker';
    expect(DIRECT_FIXED_OUTPUT.some((line) => line.includes(secret))).toBe(
      false,
    );
    const summary = { names: { worker: secret } };
    const stderrLine = `${JSON.stringify(summary)}\n`;
    let observedMode: unknown;
    const result = await entryWithRuntime(
      async (input) => {
        observedMode = input.mode;
        armGuard(input);
        return {
          exitCode: 0,
          summary,
          evidencePath: null,
          stdoutLine: `${DIRECT_OUTPUT_PREFIX}${stderrLine}`,
          stderrLine,
        };
      },
      { ...env, [variable]: secret },
    );
    expect(observedMode).toBe('run');
    expect(result).toEqual({ code: 5, stdout: '', stderr: '' });
  });

  it.each(
    DIRECT_CREDENTIAL_VARIABLES,
  )('drops the stderr copy when %s spans stderr before stdout', async (variable) => {
    const stderrLine = '{"a":"Y"}\n';
    const stdoutLine = `${DIRECT_OUTPUT_PREFIX}{"a":"X"}\n`;
    const secret = 'Y"}\nDIRECT';
    // The credential spans only the reverse boundary: a reader that takes
    // stderr before stdout sees it, the writer's own order never does.
    expect(stdoutLine + stderrLine).not.toContain(secret);
    expect(stderrLine + stdoutLine).toContain(secret);
    const result = await entryWithRuntime(
      async (input) => {
        armGuard(input);
        return {
          exitCode: 1,
          summary: JSON.parse(stderrLine),
          evidencePath: null,
          stdoutLine,
          stderrLine,
        };
      },
      { ...env, [variable]: secret },
    );
    expect(result).toEqual({ code: 1, stdout: stdoutLine, stderr: '' });
  });

  it.each([
    // A key name the projection writes, and the index an array element carries.
    'retainedIdentities',
    '1',
  ])('refuses the evidence artifact key %s as a credential before the run starts', async (secret) => {
    expect(DIRECT_FIXED_OUTPUT.some((line) => line.includes(secret))).toBe(
      false,
    );
    expect(() => validateProviderAuth(secret)).not.toThrow();
    for (const variable of DIRECT_CREDENTIAL_VARIABLES) {
      const f = await fixture(1000);
      const openRunState = vi.fn();
      const result = await runDirectConformance({
        mode: 'run',
        configPath: f.configPath,
        env: { ...env, [variable]: secret },
        now,
        modules: { openRunState, preflight: async () => f.prepared },
      });
      expect(result).toMatchObject({
        exitCode: 2,
        summary: { code: 'invalid-input', variable },
        stderrLine: invalidInputLine(variable),
        stderrOnly: true,
      });
      expectCredentialSafeOutput(result, { ...env, [variable]: secret });
      expect(openRunState).not.toHaveBeenCalled();
      expect(existsSync(f.base)).toBe(false);
    }
  });

  it('names every evidence artifact key in the admission vocabulary', () => {
    expect(DIRECT_EVIDENCE_KEYS).toContain('retainedIdentities');
    expect(DIRECT_EVIDENCE_KEYS.every((key) => /^[\w.-]+$/u.test(key))).toBe(
      true,
    );
  });

  it('reserves transcript bytes before a synchronous diagnostic re-enters the guard', async () => {
    const stderrLine = invalidInputLine('CLOUDFLARE_API_TOKEN');
    const result = await entryWithRuntime(
      async (input) => {
        armGuard(input);
        return {
          exitCode: 2,
          summary: JSON.parse(stderrLine),
          evidencePath: null,
          stdoutLine: null,
          stderrLine,
          stderrOnly: true,
        };
      },
      { ...env, CLOUDFLARE_API_TOKEN: '}\n{' },
      true,
    );
    expect(result).toEqual({
      code: 1,
      stdout: '',
      stderr: stderrLine,
    });
  });

  it('captures each live credential once after local preflight', async () => {
    const f = await fixture();
    const reads: string[] = [];
    const credentials = new Proxy(
      { ...env, FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath },
      {
        get(target, key) {
          if (DIRECT_CREDENTIAL_VARIABLES.includes(key as string))
            reads.push(key as string);
          return Reflect.get(target, key);
        },
      },
    );
    // The two observations the entry's own callbacks would swallow are
    // captured here and asserted after the call completes.
    let readsAtPreflight: string[] | undefined;
    let readsAfterRun: string[] | undefined;
    const result = await entryWithRuntime(async (input) => {
      const result = await runDirectConformance({
        ...input,
        modules: {
          preflight: async () => {
            readsAtPreflight = [...reads];
            return f.prepared;
          },
          distPresent: () => false,
        },
      });
      readsAfterRun = [...reads];
      return result;
    }, credentials);
    expect(readsAtPreflight).toEqual([]);
    expect(readsAfterRun).toEqual([...DIRECT_CREDENTIAL_VARIABLES]);
    expect(result.code).toBe(2);
    expect(result.stdout).toContain('dist-missing');
    expect(existsSync(f.base)).toBe(false);
  });

  it('arms the entry guard for a live refusal returned before the credential read', async () => {
    const credentials = {
      CLOUDFLARE_ACCOUNT_ID: 'account',
      CLOUDFLARE_API_TOKEN: 'FLEET_DIRECT_CONFORMANCE_CONFIG',
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'private-invoke-seed',
    };
    const withheld = await child(
      process.execPath,
      [entry, '--run'],
      undefined,
      credentials,
    );
    expect(withheld).toEqual({
      code: 5,
      stdout: '',
      stderr: '',
    });
    const line = invalidInputLine('FLEET_DIRECT_CONFORMANCE_CONFIG');
    const printed = await child(process.execPath, [entry, '--run'], undefined, {
      ...credentials,
      CLOUDFLARE_API_TOKEN: 'private-api-seed',
    });
    expect(printed.code).toBe(2);
    expect(printed.stdout).toBe(`${DIRECT_OUTPUT_PREFIX}${line}`);
    expect(printed.stderr).toBe(line);
  });

  it.each(
    DIRECT_ADMISSION_VARIABLES,
  )('emits the diagnostic of the loop that refused %s', async (variable) => {
    const f = await fixture();
    const result = await runDirectConformance({
      mode: 'run',
      configPath: f.configPath,
      env: { ...env, [variable]: '' },
      modules: { preflight: async () => f.prepared },
    });
    // The line travels with the refusal, so it names the variable the loop
    // refused whatever a decoded summary says.
    expect(result).toEqual({
      exitCode: 2,
      summary: { code: 'invalid-input', variable },
      evidencePath: null,
      stdoutLine: null,
      stderrLine: invalidInputLine(variable),
      stderrOnly: true,
    });
    expectCredentialSafeOutput(result, { ...env, [variable]: '' });
  });

  it('resolves terminal exit codes with evidence failure before internal error and stable ties', () => {
    const codes = [0, 1, 2, 3, 4, 5];
    const expected = [
      [0, 1, 0, 0, 0, 5],
      [1, 1, 1, 1, 1, 5],
      [2, 1, 2, 2, 2, 5],
      [3, 1, 3, 3, 3, 5],
      [4, 1, 4, 4, 4, 5],
      [5, 5, 5, 5, 5, 5],
    ];
    for (const next of codes) {
      expect(resolveDirectExitCode(null, next)).toBe(next);
      for (const current of codes)
        expect(resolveDirectExitCode(current, next)).toBe(
          expected[current]?.[next],
        );
    }
  });

  it.each([
    'Promise.reject(new Error("secret-stack"))',
    'throw new Error("secret-stack")',
  ])('prints help and a later timer error with noncolliding credentials (%s)', async (failure) => {
    const { preload } = await preloaded(
      `const write = process.stdout.write.bind(process.stdout);
process.stdout.write = (...args) => {
  const result = write(...args);
  setTimeout(() => { ${failure}; }, 0);
  return result;
};\n`,
    );
    const result = await child(
      process.execPath,
      ['--import', preload, entry, '--help'],
      undefined,
      env,
    );
    expect(result.code).toBe(1);
    expect(result.stdout).toBe(
      `${DIRECT_OUTPUT_PREFIX}${JSON.stringify({ usage: DIRECT_CONFORMANCE_USAGE })}\n`,
    );
    expect(result.stderr).toBe('{"code":"internal-error"}\n');
  });

  it('preserves an internal error when help completion subsequently sets its exit code', async () => {
    const { preload } = await preloaded(
      `const write = process.stdout.write.bind(process.stdout);
process.stdout.write = (...args) => {
  process.emit('unhandledRejection', new Error('secret-stack'));
  return write(...args);
};\n`,
    );
    const result = await child(process.execPath, [
      '--import',
      preload,
      entry,
      '--help',
    ]);
    expect(result.code).toBe(1);
    expect(result.stdout).toMatch(/^DIRECT_CONFORMANCE \{"usage":"Usage: /);
    expect(result.stderr).toBe('{"code":"internal-error"}\n');
  });

  it.each([
    ['--bad'],
    ['--run', '--resume'],
    ['--run', '--run'],
    ['value'],
    ['--run=true'],
    ['--help', '--run'],
    ['--', '--', '--run'],
    ['--preflight', '--'],
  ])('refuses invalid argv %j without effects', async (...argv) => {
    const f = await fixture();
    expect(parseDirectConformanceArgs(argv)).toBeNull();
    const result = await child(process.execPath, [entry, ...argv], undefined, {
      FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
    });
    expect(result.code).toBe(2);
    expect(result.stdout).toBe('');
    expect(result.stderr).toBe(DIRECT_USAGE_DIAGNOSTIC);
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    [],
    ['--preflight'],
    ['--', '--preflight'],
    ['--'],
  ])('accepts preflight argv %j', (...argv) => {
    expect(parseDirectConformanceArgs(argv)).toBe('preflight');
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
    undefined,
  ])('prints exact help without reading credentials or configuration (%s)', async (variable) => {
    const probed = [
      ...DIRECT_CREDENTIAL_VARIABLES,
      'FLEET_DIRECT_CONFORMANCE_CONFIG',
    ];
    const { f, preload } = await preloaded((directory) => {
      const path = JSON.stringify(join(directory, 'reads.json'));
      return `import { writeFileSync } from 'node:fs';
const reads = [];
process.env = new Proxy(process.env, {
  get(target, key) {
    if (${JSON.stringify(probed)}.includes(key)) reads.push(key);
    return Reflect.get(target, key);
  },
});
process.on('exit', () => writeFileSync(${path}, JSON.stringify(reads)));\n`;
    });
    const readsPath = join(f.directory, 'reads.json');
    const result = await child(
      process.execPath,
      ['--import', preload, entry, '--help'],
      undefined,
      {
        ...(variable ? { [variable]: 'DIRECT_CONFORMANCE' } : {}),
        FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
      },
      {},
    );
    const stdout = `${DIRECT_OUTPUT_PREFIX}${JSON.stringify({ usage: DIRECT_CONFORMANCE_USAGE })}\n`;
    expect(result).toEqual({ code: 0, stdout, stderr: '' });
    // Help is exempt from both reads in the same expression: the credentials
    // and the configuration path.
    expect(JSON.parse(await readFile(readsPath, 'utf8'))).toEqual([]);
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('prints local preflight without reading colliding %s', async (variable) => {
    const { f, preload } = await preloaded(
      `process.env = new Proxy(process.env, {
  get(target, key) {
    if (${JSON.stringify(DIRECT_CREDENTIAL_VARIABLES)}.includes(key)) throw new Error('credential-read');
    return Reflect.get(target, key);
  },
});\n`,
    );
    const result = await child(
      process.execPath,
      ['--import', preload, entry, '--preflight'],
      undefined,
      {
        [variable]: 'DIRECT_CONFORMANCE',
        FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath,
      },
      {},
    );
    expect(result.code).toBe(0);
    expect(result.stdout).toBe(
      `${DIRECT_OUTPUT_PREFIX}${JSON.stringify({
        configSha256: f.prepared.configSha256,
        referenceModuleSetSha256: f.prepared.referenceModuleSetSha256,
        referenceUploadBytes: f.prepared.referenceUploadBytes,
        names: f.prepared.names,
      })}\n`,
    );
    expect(result.stderr).toBe('');
    expect(existsSync(f.base)).toBe(false);
  });

  it('prints the exact help line without configuration or credentials', async () => {
    const result = await runDirectConformance({
      mode: 'help',
      configPath: undefined,
      env: {},
    });
    expect(result.exitCode).toBe(0);
    expect(result.summary).toHaveProperty('usage');
    expectCredentialSafeOutput(result, {});
    const expected = `${DIRECT_OUTPUT_PREFIX}${JSON.stringify({ usage: DIRECT_CONFORMANCE_USAGE })}\n`;
    expect(result.stdoutLine).toBe(expected);
  });

  it.each([
    'CLOUDFLARE_ACCOUNT_ID',
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('refuses invalid %s after preflight without locking', async (variable) => {
    const f = await fixture(1000);
    for (const value of [
      undefined,
      '',
      ' padded',
      'padded ',
      'in\nternal',
      'in\u007fternal',
      'in\u0000ternal',
    ]) {
      const openRunState = vi.fn();
      const preflight = vi.fn(async () => f.prepared);
      const result = await runDirectConformance({
        mode: 'run',
        configPath: f.configPath,
        env: { ...env, [variable]: value },
        modules: { preflight, openRunState },
        now,
      });
      expect(result).toMatchObject({
        exitCode: 2,
        summary: { code: 'invalid-input', variable },
      });
      expectCredentialSafeOutput(result, { ...env, [variable]: value });
      expect(preflight).toHaveBeenCalledOnce();
      expect(openRunState).not.toHaveBeenCalled();
      expect(existsSync(f.base)).toBe(false);
    }
  });

  it.each([
    undefined,
    '',
    ' padded',
    'bad\npath',
  ])('refuses invalid config environment %s', async (configPath) => {
    const preflight = vi.fn();
    const result = await runDirectConformance({
      mode: 'preflight',
      configPath,
      env: {},
      modules: { preflight },
    });
    expectCredentialSafeOutput(result, {});
    expect(result).toMatchObject({
      exitCode: 2,
      summary: { variable: 'FLEET_DIRECT_CONFORMANCE_CONFIG' },
    });
    expect(preflight).not.toHaveBeenCalled();
  });

  it('sanitizes local preflight failure and never creates state', async () => {
    const f = await fixture();
    await writeFile(f.configPath, 'not json private-api-seed');
    const result = await runDirectConformance({
      mode: 'run',
      configPath: f.configPath,
      env,
      now,
    });
    expect(result).toMatchObject({
      exitCode: 2,
      summary: { code: 'preflight-failed' },
    });
    expectCredentialSafeOutput(result);
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    '--preflight',
    '-- --preflight',
  ])('spawns a valid local %s entry with one summary', async (args) => {
    const f = await fixture();
    const result = await child(
      process.execPath,
      [entry, ...args.split(' ')],
      undefined,
      { FLEET_DIRECT_CONFORMANCE_CONFIG: f.configPath },
    );
    expect(result.code).toBe(0);
    const lines = result.stdout.trim().split('\n');
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatch(/^DIRECT_CONFORMANCE /);
    expect(
      JSON.parse(lines[0]?.slice('DIRECT_CONFORMANCE '.length) ?? ''),
    ).toMatchObject({
      configSha256: f.prepared.configSha256,
      referenceModuleSetSha256: f.prepared.referenceModuleSetSha256,
      referenceUploadBytes: f.prepared.referenceUploadBytes,
      names: f.prepared.names,
    });
    expect(result.stderr).toBe('');
    expect(existsSync(f.base)).toBe(false);
  });

  it('sanitizes a rejected promise in the spawned entry', async () => {
    const result = await child(process.execPath, [
      '--input-type=module',
      '-e',
      `await import(${JSON.stringify(entry)}); Promise.reject(new Error('secret-stack'));`,
    ]);
    expect(result.code).toBe(1);
    expect(result.stderr).toBe(
      invalidInputLine('FLEET_DIRECT_CONFORMANCE_CONFIG') +
        DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
    );
    expect(result.stderr).not.toContain('secret-stack');
    expect(result.stderr).not.toContain(' at ');
  });

  it('row 1 is evidence-only at the resume ceiling', async () => {
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
      resumeCount: DIRECT_RUN_MAX_RESUME_COUNT,
    });
    const before = JSON.stringify(w.snapshot());
    const result = await w.run();
    expect(result.exitCode).toBe(0);
    expect(w.modules.bootstrap).not.toHaveBeenCalled();
    expect(w.modules.scenario).not.toHaveBeenCalled();
    expect(w.modules.teardown).not.toHaveBeenCalled();
    expect(w.journal.recordResume).toHaveBeenCalledOnce();
    expect(JSON.stringify(w.snapshot())).toBe(before);
    expect(w.journal.close).toHaveBeenCalledOnce();
    expect(result.summary).toMatchObject({
      status: 'cleaned',
      teardownCall: null,
      retainedIdentities: NO_RETAINED_IDENTITIES,
    });
  });

  it.each([
    'refused',
    'ingress',
    'complete',
  ] as const)('row 2 consumes teardown phase %s before bootstrap or scenario', async (phase) => {
    const w = await world();
    w.set({
      teardown: { ...teardownState(), phase, failure: 'scenario-incomplete' },
    });
    retained(w, phase === 'refused', 'scenario-incomplete');
    for (let call = 0; call < 2; call++) {
      const result = await w.run();
      expect(result.exitCode).toBe(phase === 'refused' ? 1 : 4);
      expect(result.summary).toMatchObject({
        teardownCall: {
          status: 'retained',
          failure: { code: 'scenario-incomplete' },
          providerRequests: 9,
        },
      });
    }
    expect(w.modules.bootstrap).not.toHaveBeenCalled();
    expect(w.modules.scenario).not.toHaveBeenCalled();
    expect(w.modules.teardown).toHaveBeenCalledTimes(2);
  });

  it.each([
    false,
    true,
  ])('row 3 retains scenario failure with published refusal=%s', async (publish) => {
    const w = await world();
    w.set({
      scenario: {
        ...completeScenario(),
        failure: {
          code: 'budget-exhausted',
          ordinal: 1,
          detail: 'run-reserve',
        },
      },
    });
    retained(w, publish);
    const result = await w.run();
    expect(result.exitCode).toBe(publish ? 1 : 4);
    expect(result.summary).toMatchObject({
      scenario: {
        failure: {
          code: 'budget-exhausted',
          ordinal: 1,
          detail: 'run-reserve',
        },
      },
    });
    expect(w.modules.bootstrap).not.toHaveBeenCalled();
    expect(w.modules.scenario).not.toHaveBeenCalled();
  });

  it.each([
    false,
    true,
  ])('row 4 maps returned outcome with retained=%s and no published refusal', async (retain) => {
    const w = await world();
    w.set({ scenario: completeScenario() });
    if (retain) retained(w, false);
    const result = await w.run();
    expect(result.exitCode).toBe(retain ? 4 : 0);
    expect(w.modules.bootstrap).not.toHaveBeenCalled();
    expect(w.modules.scenario).not.toHaveBeenCalled();
    expect(w.modules.teardown).toHaveBeenCalledOnce();
  });

  it.each([
    'run',
    'resume',
  ] as const)('row 5 bootstraps a fresh %s and closes on restart without teardown', async (mode) => {
    const w = await world();
    const result = await w.run(mode);
    expect(result.exitCode).toBe(3);
    expect(w.modules.bootstrap).toHaveBeenCalledOnce();
    expect(w.modules.scenario).toHaveBeenCalledOnce();
    expect(w.modules.teardown).not.toHaveBeenCalled();
    expect(w.journal.close).toHaveBeenCalledOnce();
    expect(w.journal.recordResume).toHaveBeenCalledTimes(
      mode === 'resume' ? 1 : 0,
    );
    expect(result.summary).toMatchObject({
      command: 'pnpm fleet-control:credentialed:direct -- --resume',
    });
  });

  it.each([
    'complete',
    'failed',
  ] as const)('row 5 calls teardown after %s', async (status) => {
    const w = await world();
    w.modules.scenario.mockImplementation(async () =>
      status === 'failed'
        ? { status, reason: 'journal-failed', phase: null, invocationCount: 0 }
        : {
            status,
            facts: completeScenario().proofs,
            invocationCount: 0,
            attempts: { provider: 0, maintenance: 0, application: 0 },
            sdkRequests: 0,
          },
    );
    retained(w, status === 'failed');
    const result = await w.run();
    expect(result.exitCode).toBe(status === 'failed' ? 1 : 4);
    expect(w.modules.teardown).toHaveBeenCalledOnce();
  });

  it.each([
    'platform-page',
    'transport-failure',
    'non-contract-answer',
    'delivery-window-expired',
  ] as const)('prints bootstrap-time outcome-unknown detail %s', async (detail) => {
    const w = await world();
    w.modules.bootstrap.mockRejectedValue(
      new DirectBootstrapError('outcome-unknown', detail),
    );
    const result = await w.run('run');
    expect(result.exitCode).toBe(1);
    expect(result.summary).toMatchObject({
      code: 'outcome-unknown',
      detail,
      scenario: null,
    });
    expect(result.stdoutLine).toContain(JSON.stringify(detail));
    expectCredentialSafeOutput(result);
  });

  it.each([
    'platform-page',
    'transport-failure',
    'non-contract-answer',
    'delivery-window-expired',
  ] as const)('prints pending scenario failure detail %s without journal settlement', async (detail) => {
    const w = await world();
    const scenario = completeScenario();
    const pending = {
      ordinal: 1,
      action: { kind: 'control-read' as const },
      state: 'pending' as const,
      requestSha256: 'a'.repeat(64),
    };
    w.modules.scenario.mockImplementation(async () => {
      w.set({ invocationCount: 1, lastInvocation: pending, scenario });
      return {
        status: 'failed',
        reason: 'outcome-unknown',
        detail,
        phase: scenario.phase,
        invocationCount: 1,
      };
    });
    const result = await w.run('run');
    expect(result.summary).toMatchObject({
      scenario: { failure: { code: 'outcome-unknown', ordinal: 1, detail } },
    });
    expect(w.snapshot().lastInvocation).toEqual(pending);
    expect(w.snapshot().scenario?.failure).toBeNull();
    expectCredentialSafeOutput(result);
  });

  it('uses inspection on outcome-unknown without recording a resume', async () => {
    const w = await world();
    w.modules.openRunState.mockRejectedValue(
      new DirectRunStateError('outcome-unknown'),
    );
    const before = JSON.stringify(w.snapshot());
    const result = await w.run();
    expect(result.exitCode).toBe(1);
    expect(result.summary).toMatchObject({
      status: 'outcome-unknown',
      evidenceWritten: true,
    });
    expect(JSON.stringify(w.snapshot())).toBe(before);
    expect(w.journal.recordResume).not.toHaveBeenCalled();
    expect(w.modules.inspectRunState).toHaveBeenCalledOnce();
    expect(w.modules.teardown).not.toHaveBeenCalled();
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it.each([
    'run-exists',
    'run-missing',
    'lock-unavailable',
    'invalid-state',
    'unsupported-scenario-version',
  ] as const)('reports open refusal %s without evidence', async (code) => {
    const w = await world();
    w.modules.openRunState.mockRejectedValue(new DirectRunStateError(code));
    const result = await w.run();
    expect(result).toMatchObject({
      exitCode: 1,
      summary: { code },
      evidencePath: null,
    });
    expect(existsSync(join(w.f.runDirectory, 'evidence.json'))).toBe(false);
    expect(w.modules.inspectRunState).not.toHaveBeenCalled();
  });

  it.each([
    'run',
    'resume',
  ] as const)('refuses absent dist and sub-floor %s before acquiring a lock', async (mode) => {
    const f = await fixture(680);
    const openRunState = vi.fn();
    for (const distPresent of [false, true]) {
      const result = await runDirectConformance({
        mode,
        configPath: f.configPath,
        env,
        now,
        modules: { distPresent: () => distPresent, openRunState },
      });
      expect(result).toMatchObject({
        exitCode: 2,
        summary: {
          code: distPresent ? 'below-scenario-floor' : 'dist-missing',
        },
      });
      expectCredentialSafeOutput(result);
      if (!distPresent)
        expect(result.summary).toHaveProperty('command', 'pnpm build');
      expect(openRunState).not.toHaveBeenCalled();
      expect(existsSync(f.base)).toBe(false);
    }
  });

  it.each([
    'private-api-seed',
    'Bearer ',
    'seed"token',
    'seed\\token',
  ])('redacts a colliding identity and exits 5 (%s)', async (sentinel) => {
    const w = await world(sentinel);
    const credentials = {
      ...env,
      CLOUDFLARE_API_TOKEN:
        sentinel === 'Bearer ' ? env.CLOUDFLARE_API_TOKEN : sentinel,
    };
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env: credentials,
      now,
      modules: w.modules,
    });
    expect(result).toMatchObject({
      exitCode: 5,
      summary: {
        code: 'evidence-failed',
        sentinelClass: sentinel === 'Bearer ' ? 'literal' : 'env-secret',
        keyPath: 'retainedIdentities.fleetUuid',
        evidenceWritten: false,
      },
    });
    expectCredentialSafeOutput(result, credentials);
    expect(result.stdoutLine).not.toContain(sentinel);
    expect(existsSync(join(w.f.runDirectory, 'evidence.json'))).toBe(false);
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it('omits a cleaned status credential from row-1 refusal summaries and stdout bytes', async () => {
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env: { ...env, FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'cleaned' },
      now,
      modules: w.modules,
    });
    expect(result.exitCode).toBe(5);
    expect(result.summary).toEqual({
      code: 'evidence-failed',
      sentinelClass: 'env-secret',
      keyPath: 'status',
      evidenceWritten: false,
    });
    expect(result.summary).not.toHaveProperty('status');
    const { stdout } = expectCredentialSafeOutput(result, {
      ...env,
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'cleaned',
    });
    expect(Buffer.byteLength(stdout)).toBeLessThanOrEqual(4096);
    expect(existsSync(join(w.f.runDirectory, 'evidence.json'))).toBe(false);
  });

  it('refuses a replacement-code credential before an existing-run check', async () => {
    const f = await fixture(1000);
    const openRunState = vi
      .fn()
      .mockRejectedValue(new DirectRunStateError('run-exists'));
    const credentials = {
      ...env,
      CLOUDFLARE_API_TOKEN: 'run-exists',
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: 'evidence-failed',
    };
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: f.configPath,
      env: credentials,
      now,
      modules: { openRunState },
    });
    expect(result.exitCode).toBe(2);
    expect(result.summary).toEqual({
      code: 'invalid-input',
      variable: 'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
    });
    expectCredentialSafeOutput(result, credentials);
    expect(openRunState).not.toHaveBeenCalled();
    expect(probes.sdk).not.toHaveBeenCalled();
    expect(existsSync(f.base)).toBe(false);
  });

  it.each([
    `Bearer ${'x'.repeat(6993)}`,
    env.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
  ])('prints the one inspected preflight serialization when toJSON later yields %s', async (forbidden) => {
    const f = await fixture();
    const safeSerializations = 1;
    let serializations = 0;
    const result = await runDirectConformance({
      mode: 'preflight',
      configPath: f.configPath,
      env,
      modules: {
        preflight: async () => ({
          ...f.prepared,
          names: {
            ...f.prepared.names,
            toJSON: () =>
              ++serializations <= safeSerializations ? 'safe' : forbidden,
          },
        }),
      },
    });
    const serialized = result.stderrLine?.slice(0, -1);
    expect(result.exitCode).toBe(0);
    expect(serializations).toBe(1);
    expect(serialized).toBe(
      JSON.stringify({
        configSha256: f.prepared.configSha256,
        referenceModuleSetSha256: f.prepared.referenceModuleSetSha256,
        referenceUploadBytes: f.prepared.referenceUploadBytes,
        names: 'safe',
      }),
    );
    expect(JSON.stringify(result.summary)).toBe(serialized);
    expect(result.stdoutLine).toBe(
      `${DIRECT_OUTPUT_PREFIX}${JSON.stringify(result.summary)}\n`,
    );
    expectCredentialSafeOutput(result);
    expect(result.stdoutLine).not.toContain(forbidden);
    expect(result.stdoutLine).not.toContain('Bearer');
  });

  it('refuses a preflight summary whose first serialization yields a forbidden literal', async () => {
    const f = await fixture();
    let serializations = 0;
    const result = await runDirectConformance({
      mode: 'preflight',
      configPath: f.configPath,
      env,
      modules: {
        preflight: async () => ({
          ...f.prepared,
          names: {
            ...f.prepared.names,
            toJSON: () => {
              serializations += 1;
              return 'Bearer forbidden';
            },
          },
        }),
      },
    });
    expect(serializations).toBe(1);
    expect(result.exitCode).toBe(5);
    expect(result.summary).toMatchObject({
      code: 'evidence-failed',
      sentinelClass: 'literal',
      keyPath: 'names',
    });
    expectCredentialSafeOutput(result);
    expect(result.stdoutLine).not.toContain('Bearer');
  });

  it('prints inspected rawJSON resumeCount bytes without introducing a credential by reserialization', async () => {
    const rawJSON = (JSON as typeof JSON & { rawJSON(text: string): number })
      .rawJSON;
    const w = await world();
    w.set({
      resumeCount: rawJSON('1e3'),
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const credentials = { ...env, CLOUDFLARE_API_TOKEN: '"resumeCount":1000' };
    const result = await runDirectConformance({
      mode: 'run',
      configPath: w.f.configPath,
      env: credentials,
      modules: w.modules,
      now,
      git: () => null,
    });
    expectCredentialSafeOutput(result, credentials);
    expect(result.exitCode).toBe(0);
    expect(result.summary).toHaveProperty('resumeCount', 1000);
    expect(JSON.stringify(result.summary)).toContain(
      credentials.CLOUDFLARE_API_TOKEN,
    );
    const expected = `${DIRECT_OUTPUT_PREFIX}${JSON.stringify(result.summary).replace('"resumeCount":1000', '"resumeCount":1e3')}\n`;
    expect(result.stdoutLine).toBe(expected);
    expect(result.stderrLine).toBe(expected.slice(DIRECT_OUTPUT_PREFIX.length));
    expect(result.stdoutLine).not.toContain(credentials.CLOUDFLARE_API_TOKEN);
  });

  it.each([
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ])('scans a credential spanning the prefix and a dynamic summary field (%s)', async (variable) => {
    const secret = 'E {"status"';
    expect(DIRECT_FIXED_OUTPUT.some((line) => line.includes(secret))).toBe(
      false,
    );
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const credentials = { ...env, [variable]: secret };
    const result = await runDirectConformance({
      mode: 'run',
      configPath: w.f.configPath,
      env: credentials,
      modules: w.modules,
      now,
      git: () => null,
    });
    expectCredentialSafeOutput(result, credentials);
    expect(result.exitCode).toBe(5);
    expect(result.stdoutLine).toBe(
      'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":true}\n',
    );
    expect(result.stderrLine).toBe(
      '{"code":"evidence-failed","evidenceWritten":true}\n',
    );
    expect(DIRECT_FIXED_OUTPUT).toContain(result.stdoutLine);
    expect(DIRECT_FIXED_OUTPUT).toContain(result.stderrLine);
    expect(existsSync(result.evidencePath as string)).toBe(true);
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it.each([
    4096, 4097,
  ])('size-checks the assembled rawJSON preflight line at %s bytes before printing', async (bytes) => {
    const rawJSON = (JSON as typeof JSON & { rawJSON(text: string): object })
      .rawJSON;
    const f = await fixture();
    const summary = {
      configSha256: f.prepared.configSha256,
      referenceModuleSetSha256: f.prepared.referenceModuleSetSha256,
      referenceUploadBytes: f.prepared.referenceUploadBytes,
      names: { padding: rawJSON('1.0') },
    };
    const baseline = `${DIRECT_OUTPUT_PREFIX}${JSON.stringify(summary)}\n`;
    summary.names.padding = rawJSON(
      `1.${'0'.repeat(bytes - Buffer.byteLength(baseline) + 1)}`,
    );
    const expected = `${DIRECT_OUTPUT_PREFIX}${JSON.stringify(summary)}\n`;
    expect(Buffer.byteLength(expected)).toBe(bytes);
    expect(
      Buffer.byteLength(JSON.stringify(JSON.parse(JSON.stringify(summary)))),
    ).toBeLessThan(4096 - DIRECT_OUTPUT_PREFIX.length - 1);
    const result = await runDirectConformance({
      mode: 'preflight',
      configPath: f.configPath,
      env: {},
      modules: {
        preflight: async () => ({
          ...f.prepared,
          names: { ...f.prepared.names, toJSON: () => summary.names },
        }),
      },
    });
    expectCredentialSafeOutput(result, {});
    // The oversized line is replaced; the preflight keeps the code it resolved.
    expect(result.exitCode).toBe(0);
    expect(result.stdoutLine).toBe(
      bytes === 4096
        ? expected
        : 'DIRECT_CONFORMANCE {"code":"internal-error"}\n',
    );
    expect(Buffer.byteLength(result.stdoutLine as string)).toBeLessThanOrEqual(
      4096,
    );
  });

  it('keeps a failed run exit code when its oversized summary is replaced', async () => {
    const w = await world();
    w.set({
      scenario: completeScenario(),
      binding: { ...w.snapshot().binding, resourcePrefix: 'p'.repeat(4096) },
    });
    w.modules.teardown.mockRejectedValue(new DirectRunStateError());
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env,
      modules: w.modules,
      now,
      git: () => null,
    });
    expectCredentialSafeOutput(result);
    expect(result.stdoutLine).toBe(
      'DIRECT_CONFORMANCE {"code":"internal-error"}\n',
    );
    expect(result.exitCode).toBe(1);
  });

  it('bounds a sentinel replacement whose key path exceeds the stdout byte limit', async () => {
    const f = await fixture();
    const result = await runDirectConformance({
      mode: 'preflight',
      configPath: f.configPath,
      env: {},
      modules: {
        preflight: async () => ({
          ...f.prepared,
          names: { ...f.prepared.names, ['x'.repeat(4096)]: 'Bearer hidden' },
        }),
      },
    });
    expect(result.exitCode).toBe(5);
    expectCredentialSafeOutput(result, {});
    expect(result.summary).toEqual({
      code: 'evidence-failed',
      evidenceWritten: false,
    });
    expect(result.stdoutLine).toBe(
      'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":false}\n',
    );
  });

  it('omits status when evidence construction throws', async () => {
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env,
      now: () => Number.NaN,
      modules: w.modules,
    });
    expect(result.exitCode).toBe(5);
    expectCredentialSafeOutput(result);
    expect(result.summary).toEqual({
      code: 'evidence-failed',
      evidenceWritten: false,
    });
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it('uses a minimal refusal when a diagnostic key path collides with a credential', async () => {
    const secret = 'retainedIdentities.fleetUuid';
    const w = await world(secret);
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env: { ...env, FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: secret },
      now,
      modules: w.modules,
    });
    expect(result.exitCode).toBe(5);
    expect(result.summary).toEqual({
      code: 'evidence-failed',
      evidenceWritten: false,
    });
    expectCredentialSafeOutput(result, {
      ...env,
      FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET: secret,
    });
  });

  it.each([
    'https://example.invalid/signed?sig=opaque',
    'upstream returned an unexpected response',
  ])('refuses a malformed provider identity with exit 5 (%s)', async (fleetUuid) => {
    const w = await world(fleetUuid);
    const result = await w.run();
    expect(result.exitCode).toBe(5);
    expect(result.summary).toEqual({
      code: 'evidence-failed',
      refusalClass: 'identity-shape',
      keyPath: 'retainedIdentities.fleetUuid',
      evidenceWritten: false,
    });
    expect(result.stdoutLine).not.toContain(fleetUuid);
    expect(existsSync(join(w.f.runDirectory, 'evidence.json'))).toBe(false);
  });

  it('normalizes unexpected exceptions without disclosing their name or message', async () => {
    const w = await world();
    w.modules.bootstrap.mockRejectedValue(
      Object.assign(new Error('private-api-seed'), {
        name: 'untrusted-name',
        code: 'untrusted-code',
      }),
    );
    const result = await w.run();
    expect(result.exitCode).toBe(1);
    expect(result.summary).toEqual({ code: 'internal-error' });
    for (const forbidden of [
      'private-api-seed',
      'untrusted-name',
      'untrusted-code',
    ])
      expect(result.stdoutLine).not.toContain(forbidden);
  });

  it.each([
    null,
    '',
    'A'.repeat(40),
    'a'.repeat(39),
    'a'.repeat(41),
    'a'.repeat(40),
    'throw',
    'timeout',
  ])('validates the git seam result %s and keeps the summary under 4 KiB', async (value) => {
    const w = await world();
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env,
      now,
      modules: w.modules,
      git: () => {
        if (value === 'throw' || value === 'timeout') throw new Error(value);
        return value;
      },
    });
    const artifact = JSON.parse(
      await readFile(result.evidencePath as string, 'utf8'),
    );
    expect(artifact.commit).toBe(value === 'a'.repeat(40) ? value : null);
    expectCredentialSafeOutput(result);
  });

  it('reads no credentials before successful local preflight', async () => {
    const f = await fixture();
    const unreadable = new Proxy(
      {},
      {
        get() {
          throw new Error('credential read');
        },
      },
    );
    for (const mode of ['help', 'preflight'] as const) {
      const result = await runDirectConformance({
        mode,
        configPath: f.configPath,
        env: unreadable,
        now,
      });
      expect(result.exitCode).toBe(0);
      expectCredentialSafeOutput(result, {});
    }
    const result = await runDirectConformance({
      mode: 'run',
      configPath: f.configPath,
      env: unreadable,
      modules: {
        preflight: async () => {
          throw new Error('preflight');
        },
      },
    });
    expect(result.exitCode).toBe(2);
    expectCredentialSafeOutput(result, {});
  });

  it('reports the publication state when a byte hit collapses a summary without the flag', async () => {
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const credentials = { ...env, CLOUDFLARE_API_TOKEN: 'E {"status"' };
    const parse = JSON.parse;
    let stripped = false;
    const decode = vi
      .spyOn(JSON, 'parse')
      .mockImplementation((text, reviver) => {
        const decoded = parse(text, reviver);
        if (
          !stripped &&
          decoded !== null &&
          typeof decoded === 'object' &&
          'evidenceWritten' in decoded
        ) {
          stripped = true;
          const { evidenceWritten: _flag, ...rest } = decoded;
          return rest;
        }
        return decoded;
      });
    try {
      const result = await runDirectConformance({
        mode: 'run',
        configPath: w.f.configPath,
        env: credentials,
        modules: w.modules,
        now,
        git: () => null,
      });
      expect(stripped).toBe(true);
      expect(result.exitCode).toBe(5);
      expect(result.stdoutLine).toBe(
        'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":true}\n',
      );
      expect(existsSync(result.evidencePath as string)).toBe(true);
      expectCredentialSafeOutput(result, credentials);
    } finally {
      decode.mockRestore();
    }
  });

  it('reports a durability failure after replacement as exit 5 with evidence published', async () => {
    const w = await world();
    w.set({
      teardown: { ...maximalTeardown(), phase: 'complete', failure: null },
    });
    const result = await runDirectConformance({
      mode: 'resume',
      configPath: w.f.configPath,
      env,
      now,
      git: () => null,
      modules: {
        ...w.modules,
        writeEvidence: async () => {
          throw new DirectEvidenceWriteError();
        },
      },
    });
    expectCredentialSafeOutput(result);
    expect(result).toMatchObject({
      exitCode: 5,
      // The artifact replaced its predecessor before the fault, so the run
      // names the file it published.
      evidencePath: join(w.journal.directory, 'evidence.json'),
      summary: { code: 'evidence-failed', evidenceWritten: true },
    });
    expect(result.stdoutLine).toBe(
      'DIRECT_CONFORMANCE {"code":"evidence-failed","evidenceWritten":true}\n',
    );
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it('reports a failed evidence write as exit 5 after closing the journal', async () => {
    const w = await world();
    w.journal.directory = join(w.f.runDirectory, 'missing');
    const result = await w.run();
    expect(result).toMatchObject({
      exitCode: 5,
      summary: { code: 'evidence-failed', evidenceWritten: false },
    });
    expect(result.summary).not.toHaveProperty('status');
    expect(w.journal.close).toHaveBeenCalledOnce();
  });

  it('proves pnpm forwarding by the printed argv at both hops', async () => {
    const f = await fixture();
    const pkg = join(f.directory, 'probe');
    await mkdir(pkg);
    await writeFile(
      join(f.directory, 'pnpm-workspace.yaml'),
      'packages:\n  - probe\n',
    );
    await writeFile(
      join(f.directory, 'package.json'),
      JSON.stringify({
        private: true,
        packageManager: 'pnpm@10.34.4',
        scripts: { 'root:probe': 'pnpm --filter probe-pkg probe' },
      }),
    );
    await writeFile(
      join(pkg, 'package.json'),
      JSON.stringify({
        name: 'probe-pkg',
        private: true,
        scripts: { probe: 'node probe.mjs' },
      }),
    );
    await writeFile(
      join(pkg, 'probe.mjs'),
      'console.log(JSON.stringify(process.argv.slice(2)));\n',
    );
    for (const [cwd, args] of [
      [pkg, ['run', 'probe', '--', '--run']],
      [f.directory, ['root:probe', '--', '--run']],
    ] as const) {
      const result = await child('pnpm', [...args], cwd);
      const printed = result.stdout
        .split('\n')
        .filter((line) => line === '["--","--run"]');
      process.stdout.write(`PNPM_FORWARDING ${JSON.stringify(printed)}\n`);
      expect(printed).toEqual(['["--","--run"]']);
    }
  });
});
