// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  bootstrapDirectConformance,
  DirectBootstrapError,
} from './direct-credentialed-bootstrap.mjs';
import { preflightDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import {
  buildDirectEvidence,
  DIRECT_EVIDENCE_LITERALS,
  DirectEvidenceWriteError,
  inspectDirectEvidence,
  writeDirectEvidence,
} from './direct-credentialed-evidence.mjs';
import { validateProviderAuth } from './direct-credentialed-provider.mjs';
import {
  DirectRunStateError,
  inspectDirectRunState,
  openDirectRunState,
} from './direct-credentialed-run-state.mjs';
import { runDirectCredentialedScenario } from './direct-credentialed-scenario.mjs';
import { DIRECT_SCENARIO_MIN_INVOCATIONS } from './direct-credentialed-scenario-budget.mjs';
import { teardownDirectReference } from './direct-credentialed-teardown.mjs';

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export const DIRECT_CONFORMANCE_USAGE =
  'Usage: pnpm fleet-control:credentialed:direct -- [--preflight|--run|--resume|--help]';
export const DIRECT_OUTPUT_PREFIX = 'DIRECT_CONFORMANCE ';
const usageSummary = Object.freeze({ code: 'usage' });
const internalErrorSummary = Object.freeze({ code: 'internal-error' });
const evidenceFailureSummaries = Object.freeze({
  false: Object.freeze({ code: 'evidence-failed', evidenceWritten: false }),
  true: Object.freeze({ code: 'evidence-failed', evidenceWritten: true }),
});
export const DIRECT_USAGE_DIAGNOSTIC = `${JSON.stringify(usageSummary)}\n`;
export const DIRECT_INTERNAL_ERROR_DIAGNOSTIC = `${JSON.stringify(internalErrorSummary)}\n`;
const evidenceFailureLines = Object.freeze(
  Object.fromEntries(
    Object.entries(evidenceFailureSummaries).map(([written, summary]) => {
      const stderrLine = `${JSON.stringify(summary)}\n`;
      return [
        written,
        Object.freeze({
          stdoutLine: `${DIRECT_OUTPUT_PREFIX}${stderrLine}`,
          stderrLine,
        }),
      ];
    }),
  ),
);
const invalidInputLines = Object.freeze(
  Object.fromEntries(
    [
      'CLOUDFLARE_ACCOUNT_ID',
      'CLOUDFLARE_API_TOKEN',
      'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
    ].map((variable) => [
      variable,
      `${JSON.stringify({ code: 'invalid-input', variable })}\n`,
    ]),
  ),
);
/** A silent exit 2 means a credential collides with the CLI's fixed vocabulary. */
export const DIRECT_FIXED_OUTPUT = Object.freeze([
  ...Object.values(evidenceFailureLines).flatMap((lines) =>
    Object.values(lines),
  ),
  DIRECT_USAGE_DIAGNOSTIC,
  DIRECT_INTERNAL_ERROR_DIAGNOSTIC,
  ...Object.values(invalidInputLines),
]);

export function parseDirectConformanceArgs(argv) {
  const args = argv[0] === '--' ? argv.slice(1) : argv;
  if (args.length === 0) return 'preflight';
  if (
    args.length === 1 &&
    ['--preflight', '--run', '--resume', '--help'].includes(args[0])
  )
    return args[0].slice(2);
  return null;
}

export function resolveDirectExitCode(current, next) {
  const rank = (code) => (code === 5 ? 2 : code === 1 ? 1 : 0);
  return current === null || rank(next) > rank(current) ? next : current;
}

function gitCommit(env) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: packageDirectory,
    env: { PATH: env.PATH },
    timeout: 2000,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
  });
  return result.status === 0 && result.signal === null
    ? result.stdout.trim()
    : null;
}

function commitFrom(read) {
  try {
    const value = read();
    return typeof value === 'string' && /^[0-9a-f]{40}$/u.test(value)
      ? value
      : null;
  } catch {
    return null;
  }
}

function validEnvironment(value) {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value === value.trim() &&
    ![...value].some(
      (character) =>
        character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127,
    )
  );
}

function timestamp(now) {
  const value = now();
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error('internal-error');
  const time = new Date(value).toISOString();
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(time))
    throw new Error('internal-error');
  return time;
}

export async function runDirectConformance(input) {
  const modules = {
    preflight: preflightDirectConformance,
    openRunState: openDirectRunState,
    inspectRunState: inspectDirectRunState,
    bootstrap: bootstrapDirectConformance,
    scenario: runDirectCredentialedScenario,
    teardown: teardownDirectReference,
    distPresent: () => existsSync(join(packageDirectory, 'dist', 'index.js')),
    ...input.modules,
  };
  const now = input.now ?? Date.now;
  const sentinels = {
    secrets: [],
    literals: DIRECT_EVIDENCE_LITERALS,
  };
  const byteHit = (line) =>
    [...sentinels.secrets, ...sentinels.literals].some(
      (value) =>
        typeof value === 'string' && value.length > 0 && line.includes(value),
    );
  const result = (
    exitCode,
    summary,
    evidencePath = null,
    stderrOnly = false,
  ) => {
    const inspect = (value) => {
      const inspected = inspectDirectEvidence(value, sentinels);
      const stderrLine = inspected.serialized;
      return {
        ...inspected,
        stdoutLine: `${DIRECT_OUTPUT_PREFIX}${stderrLine}`,
        stderrLine,
      };
    };
    let lines = inspect(summary);
    summary = JSON.parse(lines.serialized);
    if (!stderrOnly && lines.hit) {
      exitCode = 5;
      summary = {
        code: 'evidence-failed',
        ...(summary.code === 'evidence-failed' ? {} : lines.hit),
        evidenceWritten: summary.evidenceWritten ?? false,
      };
      lines = inspect(summary);
    }
    if (!stderrOnly && Buffer.byteLength(lines.stdoutLine) > 4096) {
      lines = inspect(
        exitCode === 5
          ? evidenceFailureSummaries[summary.evidenceWritten === true]
          : internalErrorSummary,
      );
      if (exitCode !== 5) exitCode = 1;
    }
    if (stderrOnly) {
      const silent =
        lines.hit ||
        byteHit(lines.stderrLine) ||
        Buffer.byteLength(lines.stdoutLine) > 4096;
      lines = {
        stdoutLine: null,
        stderrLine: silent
          ? null
          : (invalidInputLines[summary.variable] ?? null),
      };
    } else if (lines.hit || byteHit(lines.stdoutLine)) {
      exitCode = 5;
      lines = evidenceFailureLines[summary.evidenceWritten === true];
    }
    return {
      exitCode,
      summary: lines.stderrLine ? JSON.parse(lines.stderrLine) : summary,
      evidencePath,
      stdoutLine: lines.stdoutLine,
      stderrLine: lines.stderrLine,
      ...(stderrOnly ? { stderrOnly: true } : {}),
    };
  };
  if (!['help', 'preflight', 'run', 'resume'].includes(input.mode))
    return result(2, usageSummary);
  if (input.mode === 'help')
    return result(0, { usage: DIRECT_CONFORMANCE_USAGE });
  if (!validEnvironment(input.configPath))
    return result(2, {
      code: 'invalid-input',
      variable: 'FLEET_DIRECT_CONFORMANCE_CONFIG',
    });
  let prepared;
  try {
    prepared = await modules.preflight({
      configPath: input.configPath,
      now: now(),
    });
  } catch {
    return result(2, { code: 'preflight-failed' });
  }
  if (input.mode === 'preflight')
    return result(0, {
      configSha256: prepared.configSha256,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      referenceUploadBytes: prepared.referenceUploadBytes,
      names: prepared.names,
    });
  sentinels.secrets = [
    input.env.CLOUDFLARE_API_TOKEN,
    input.env.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
  ];
  for (const variable of [
    'CLOUDFLARE_ACCOUNT_ID',
    'CLOUDFLARE_API_TOKEN',
    'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
  ]) {
    try {
      if (!validEnvironment(input.env[variable]))
        throw new Error('invalid-input');
      if (variable !== 'CLOUDFLARE_ACCOUNT_ID') {
        validateProviderAuth(input.env[variable]);
        if (
          DIRECT_FIXED_OUTPUT.some((output) =>
            output.includes(input.env[variable]),
          )
        )
          throw new Error('invalid-input');
      }
    } catch {
      return result(2, { code: 'invalid-input', variable }, null, true);
    }
  }
  let journal;
  let inspection;
  let outcome = { status: 'failed', exitCode: 1, teardownCall: null };
  let code;
  let detail;
  let invocationFailureDetail;
  let evidencePath = null;
  let summary;
  try {
    if (!modules.distPresent())
      return result(2, { code: 'dist-missing', command: 'pnpm build' });
    if (
      prepared.config.referenceWorker.maxInvocations <
      DIRECT_SCENARIO_MIN_INVOCATIONS
    )
      return result(2, { code: 'below-scenario-floor' });
    const stateInput = {
      configPath: input.configPath,
      prepared,
      accountId: input.env.CLOUDFLARE_ACCOUNT_ID,
    };
    try {
      journal = await modules.openRunState({
        ...stateInput,
        mode: input.mode,
        now: now(),
      });
    } catch (error) {
      if (
        input.mode !== 'resume' ||
        !(error instanceof DirectRunStateError) ||
        error.code !== 'outcome-unknown'
      )
        throw error;
      inspection = await modules.inspectRunState({
        ...stateInput,
        mode: 'inspect',
      });
      outcome = { status: 'outcome-unknown', exitCode: 1, teardownCall: null };
      code = 'outcome-unknown';
    }
    if (journal) {
      if (input.mode === 'resume') await journal.recordResume();
      const snapshot = journal.snapshot();
      if (
        snapshot.teardown?.phase === 'complete' &&
        snapshot.teardown.failure === null
      ) {
        outcome = { status: 'cleaned', exitCode: 0, teardownCall: null };
      } else {
        const networkInput = {
          prepared,
          journal,
          apiToken: input.env.CLOUDFLARE_API_TOKEN,
          ...(input.fetch ? { fetch: input.fetch } : {}),
        };
        let restart = false;
        if (
          snapshot.teardown === undefined &&
          (snapshot.scenario === undefined ||
            (snapshot.scenario.failure === null &&
              snapshot.scenario.phase !== 'complete'))
        ) {
          const invocation = await modules.bootstrap({
            ...networkInput,
            invokeSecret: input.env.FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET,
          });
          const scenario = await modules.scenario({
            ...networkInput,
            invocation,
          });
          if (
            scenario.status === 'failed' &&
            scenario.reason === 'outcome-unknown'
          )
            invocationFailureDetail = scenario.detail;
          restart = scenario.status === 'restart-required';
        }
        if (restart) {
          outcome = {
            status: 'restart-required',
            exitCode: 3,
            teardownCall: null,
          };
        } else {
          const teardown = await modules.teardown({
            ...networkInput,
            ...(input.delay ? { delay: input.delay } : {}),
          });
          const status =
            teardown.status === 'cleaned'
              ? 'cleaned'
              : journal.snapshot().teardown?.phase === 'refused'
                ? 'failed'
                : 'retained';
          outcome = {
            status,
            exitCode: status === 'cleaned' ? 0 : status === 'failed' ? 1 : 4,
            teardownCall: {
              status: teardown.status,
              failure:
                teardown.status === 'retained'
                  ? { code: teardown.reason }
                  : null,
              providerRequests: teardown.facts.providerRequests,
            },
          };
        }
      }
    }
  } catch (error) {
    code =
      error instanceof DirectRunStateError
        ? new DirectRunStateError(error.code).code
        : error instanceof DirectBootstrapError
          ? new DirectBootstrapError(error.code).code
          : 'internal-error';
    if (error instanceof DirectBootstrapError)
      detail = new DirectBootstrapError(error.code, error.detail).detail;
    outcome = { status: 'failed', exitCode: 1, teardownCall: null };
  } finally {
    const handle = journal ?? inspection;
    if (handle) {
      let evidenceWritten = false;
      try {
        const snapshot = journal ? journal.snapshot() : inspection.snapshot;
        const evidence = buildDirectEvidence({
          snapshot,
          invocationFailureDetail,
          prepared,
          mode: input.mode,
          outcome,
          times: { finishedAt: timestamp(now) },
          commit: commitFrom(input.git ?? (() => gitCommit(input.env))),
        });
        summary = {
          status: evidence.status,
          exitCode: evidence.exitCode,
          ...(code ? { code } : {}),
          ...(detail ? { detail } : {}),
          resourcePrefix: evidence.resourcePrefix,
          resumeCount: evidence.resumeCount,
          scenario: evidence.scenario
            ? {
                phase: evidence.scenario.phase,
                failure: evidence.scenario.failure,
                invocationCount: evidence.scenario.invocationCount,
              }
            : null,
          teardownCall: evidence.teardownCall,
          retainedIdentities: evidence.retainedIdentities,
          ...(outcome.status === 'restart-required'
            ? { command: 'pnpm fleet-control:credentialed:direct -- --resume' }
            : {}),
        };
        const directory =
          journal?.directory ??
          join(
            dirname(resolve(input.configPath)),
            '.direct-conformance',
            snapshot.binding.resourcePrefix,
          );
        const written = await writeDirectEvidence({
          directory,
          evidence,
          sentinels,
        });
        evidenceWritten = written.written;
        if (written.written) evidencePath = join(directory, 'evidence.json');
        if (!written.written) {
          outcome.exitCode = 5;
          summary = {
            code: 'evidence-failed',
            ...(written.sentinelClass
              ? {
                  sentinelClass: written.sentinelClass,
                  keyPath: written.keyPath,
                }
              : {}),
            evidenceWritten,
          };
        }
      } catch (error) {
        evidenceWritten =
          error instanceof DirectEvidenceWriteError && error.written;
        outcome.exitCode = 5;
        summary = { code: 'evidence-failed', evidenceWritten };
      } finally {
        summary = { ...summary, evidenceWritten };
        try {
          await handle.close();
        } catch {
          if (outcome.exitCode !== 5) {
            outcome.exitCode = 1;
            summary = { code: 'internal-error', evidenceWritten };
          }
        }
      }
    }
  }
  if (code === 'internal-error' && outcome.exitCode !== 5)
    return result(1, { code }, evidencePath);
  return result(
    outcome.exitCode,
    summary ?? {
      code,
      ...(code === 'run-exists'
        ? { command: '--resume' }
        : code === 'run-missing'
          ? { command: '--run' }
          : {}),
    },
    evidencePath,
  );
}
