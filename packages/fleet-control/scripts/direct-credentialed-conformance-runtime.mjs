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
  DIRECT_CONFORMANCE_COMMANDS,
  DIRECT_EVIDENCE_KEYS,
  DIRECT_EVIDENCE_LITERALS,
  DirectEvidenceWriteError,
  inspectDirectEvidence,
  writeDirectEvidence,
} from './direct-credentialed-evidence.mjs';
import { validateProviderAuth } from './direct-credentialed-provider.mjs';
import {
  DIRECT_RUN_TIMESTAMP,
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
/**
 * The summary codes this runtime mints. A run-state or bootstrap refusal
 * reaches the summary carrying its own code instead.
 */
export const DIRECT_CONFORMANCE_CODES = Object.freeze({
  belowScenarioFloor: 'below-scenario-floor',
  distMissing: 'dist-missing',
  evidenceFailed: 'evidence-failed',
  internalError: 'internal-error',
  invalidInput: 'invalid-input',
  preflightFailed: 'preflight-failed',
  usage: 'usage',
});
/**
 * The process exit codes the CLI resolves. `evidenceFailed` means the run's
 * evidence file or its summary line was not published, whichever came first.
 */
export const DIRECT_CONFORMANCE_EXIT_CODES = Object.freeze({
  success: 0,
  failed: 1,
  invalidInput: 2,
  restartRequired: 3,
  retained: 4,
  evidenceFailed: 5,
});
/** The modes that reach the provider and therefore carry credentials. */
export const DIRECT_LIVE_MODES = Object.freeze(['run', 'resume']);
export const DIRECT_CONFORMANCE_MODES = Object.freeze([
  'preflight',
  ...DIRECT_LIVE_MODES,
  'help',
]);
/** The environment variables that carry a credential. */
export const DIRECT_CREDENTIAL_VARIABLES = Object.freeze([
  'CLOUDFLARE_API_TOKEN',
  'FLEET_DIRECT_CONFORMANCE_INVOKE_SECRET',
]);
/**
 * The variables live-mode admission validates. `CLOUDFLARE_ACCOUNT_ID` is
 * admitted here and absent from `DIRECT_CREDENTIAL_VARIABLES`: it names an
 * account rather than authenticating to it.
 */
export const DIRECT_ADMISSION_VARIABLES = Object.freeze([
  'CLOUDFLARE_ACCOUNT_ID',
  ...DIRECT_CREDENTIAL_VARIABLES,
]);
const codes = DIRECT_CONFORMANCE_CODES;
const exits = DIRECT_CONFORMANCE_EXIT_CODES;
/** The one shape every line below carries: the summary's JSON text, newline-terminated. */
const fixedLineOf = (summary) => `${JSON.stringify(summary)}\n`;
const prefixed = (stderrLine) => `${DIRECT_OUTPUT_PREFIX}${stderrLine}`;
const evidenceFailureSummary = (evidenceWritten, hit) =>
  Object.freeze({ code: codes.evidenceFailed, ...hit, evidenceWritten });
const usageSummary = Object.freeze({ code: codes.usage });
const internalErrorSummary = Object.freeze({ code: codes.internalError });
export const DIRECT_USAGE_DIAGNOSTIC = fixedLineOf(usageSummary);
export const DIRECT_INTERNAL_ERROR_DIAGNOSTIC =
  fixedLineOf(internalErrorSummary);
const evidenceFailureLine = (evidenceWritten) => {
  const stderrLine = fixedLineOf(evidenceFailureSummary(evidenceWritten));
  return Object.freeze({ stdoutLine: prefixed(stderrLine), stderrLine });
};
const evidenceFailureLines = Object.freeze({
  false: evidenceFailureLine(false),
  true: evidenceFailureLine(true),
});
const invalidInputLines = Object.freeze(
  Object.fromEntries(
    DIRECT_ADMISSION_VARIABLES.map((variable) => [
      variable,
      fixedLineOf({ code: codes.invalidInput, variable }),
    ]),
  ),
);
/**
 * The lines the CLI can print without reading a run value: every member is
 * `fixedLineOf` over a summary this module's own constants build. A silent exit
 * 2 means a credential collides with one of them, so the refusal's own
 * diagnostic would carry it. The suppression is wider than this list: in a live
 * mode the entry scans every line it writes, its usage line and its
 * internal-error diagnostic included, and the runtime scans every line it
 * builds from a run value against the same credentials.
 */
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
    DIRECT_CONFORMANCE_MODES.map((mode) => `--${mode}`).includes(args[0])
  )
    return args[0].slice(2);
  return null;
}

export function isDirectLiveMode(mode) {
  return DIRECT_LIVE_MODES.includes(mode);
}

/** Whether a line the CLI would write carries one of `values`. */
export function directLineCarries(line, values) {
  return values.some(
    (value) =>
      typeof value === 'string' && value.length > 0 && line.includes(value),
  );
}

/**
 * The bytes a result renders on stdout: the summary line, or none where the
 * safe rendering is silence.
 */
export function directStdoutOf(result) {
  return result.stdoutLine ?? '';
}

/**
 * Whether the entry copies a result's stderr line to its own stderr. The
 * stderr-only refusal exits 2, so its own exit code already selects it.
 */
export function directWritesStderr(result) {
  return (
    result.stderrLine !== null &&
    result.exitCode !== exits.success &&
    result.exitCode !== exits.restartRequired
  );
}

export function resolveDirectExitCode(current, next) {
  const rank = (code) =>
    code === exits.evidenceFailed ? 2 : code === exits.failed ? 1 : 0;
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

// A clock the projection cannot use raises here, inside the evidence path, so
// the run reports `evidence-failed` for it.
function timestamp(now) {
  const value = now();
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(codes.internalError);
  const time = new Date(value).toISOString();
  if (!DIRECT_RUN_TIMESTAMP.test(time)) throw new Error(codes.internalError);
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
    writeEvidence: writeDirectEvidence,
    distPresent: () => existsSync(join(packageDirectory, 'dist', 'index.js')),
    ...input.modules,
  };
  const now = input.now ?? Date.now;
  // Secrets join the sentinel list once the run needs them: `--help` and
  // `--preflight` complete without reading a credential, so the summaries they
  // return are scanned against the literals alone. The entry arms its own guard
  // for those returns in live modes. The bag is replaced rather than mutated,
  // so every reader sees a complete, immutable list.
  let sentinels = Object.freeze({
    secrets: Object.freeze([]),
    literals: DIRECT_EVIDENCE_LITERALS,
  });
  const byteHit = (line) =>
    directLineCarries(line, [...sentinels.secrets, ...sentinels.literals]);
  let evidencePath = null;
  let evidenceWritten = false;
  const result = (
    exitCode,
    summary,
    evidencePathReported = null,
    // The fixed line an admission refusal prints, handed over by the branch
    // that refuses. A refusal supplies one and prints no stdout summary; the
    // line travels with the refusal, so no table lookup can miss here.
    fixedLine = null,
  ) => {
    const stderrOnly = fixedLine !== null;
    const inspect = (value) => {
      const inspected = inspectDirectEvidence(value, sentinels);
      return {
        hit: inspected.hit,
        stdoutLine: prefixed(inspected.serialized),
        stderrLine: inspected.serialized,
      };
    };
    let lines = inspect(summary);
    summary = JSON.parse(lines.stderrLine);
    if (!stderrOnly && lines.hit) {
      exitCode = exits.evidenceFailed;
      summary = evidenceFailureSummary(
        evidenceWritten,
        summary.code === codes.evidenceFailed ? undefined : lines.hit,
      );
      lines = inspect(summary);
    }
    if (!stderrOnly && Buffer.byteLength(lines.stdoutLine) > 4096) {
      // A summary too large to print is replaced by a fixed one; the run keeps
      // the exit code it resolved, so the line reports the size fault and the
      // code still reports the run.
      lines = inspect(
        exitCode === exits.evidenceFailed
          ? evidenceFailureSummary(evidenceWritten)
          : internalErrorSummary,
      );
    }
    if (stderrOnly) {
      lines = {
        stdoutLine: null,
        stderrLine:
          lines.hit || byteHit(fixedLine) || Buffer.byteLength(fixedLine) > 4096
            ? null
            : fixedLine,
      };
    } else if (lines.hit || byteHit(lines.stdoutLine)) {
      exitCode = exits.evidenceFailed;
      lines = evidenceFailureLines[evidenceWritten];
    }
    return {
      exitCode,
      summary: lines.stderrLine ? JSON.parse(lines.stderrLine) : summary,
      evidencePath: evidencePathReported,
      stdoutLine: lines.stdoutLine,
      stderrLine: lines.stderrLine,
      ...(stderrOnly ? { stderrOnly: true } : {}),
    };
  };
  if (!DIRECT_CONFORMANCE_MODES.includes(input.mode))
    return result(exits.invalidInput, usageSummary);
  if (input.mode === 'help')
    return result(exits.success, { usage: DIRECT_CONFORMANCE_USAGE });
  if (!validEnvironment(input.configPath))
    return result(exits.invalidInput, {
      code: codes.invalidInput,
      variable: 'FLEET_DIRECT_CONFORMANCE_CONFIG',
    });
  let prepared;
  try {
    prepared = await modules.preflight({
      configPath: input.configPath,
      now: now(),
    });
  } catch {
    return result(exits.invalidInput, { code: codes.preflightFailed });
  }
  if (input.mode === 'preflight')
    return result(exits.success, {
      configSha256: prepared.configSha256,
      referenceModuleSetSha256: prepared.referenceModuleSetSha256,
      referenceUploadBytes: prepared.referenceUploadBytes,
      names: prepared.names,
    });
  sentinels = Object.freeze({
    secrets: Object.freeze(
      DIRECT_CREDENTIAL_VARIABLES.map((variable) => input.env[variable]).filter(
        (value) => typeof value === 'string' && value.length > 0,
      ),
    ),
    literals: DIRECT_EVIDENCE_LITERALS,
  });
  // Each variable is admitted beside the diagnostic its refusal prints, so the
  // line is carried rather than looked up and the two can never disagree.
  for (const [variable, fixedLine] of Object.entries(invalidInputLines)) {
    try {
      if (!validEnvironment(input.env[variable]))
        throw new Error(codes.invalidInput);
      if (DIRECT_CREDENTIAL_VARIABLES.includes(variable)) {
        validateProviderAuth(input.env[variable]);
        // The third reason this refusal covers: the credential collides with a
        // line the CLI can print, or with a key the evidence artifact carries,
        // either of which the scan answers by withholding output at the end of
        // a live run. The scan compares array elements by their index, so a
        // credential of digits alone collides too. The credential changes
        // rather than the environment that supplies it.
        if (
          DIRECT_FIXED_OUTPUT.some((output) =>
            output.includes(input.env[variable]),
          ) ||
          DIRECT_EVIDENCE_KEYS.some((key) =>
            key.includes(input.env[variable]),
          ) ||
          /^\d+$/u.test(input.env[variable])
        )
          throw new Error(codes.invalidInput);
      }
    } catch {
      return result(
        exits.invalidInput,
        { code: codes.invalidInput, variable },
        null,
        fixedLine,
      );
    }
  }
  let journal;
  let inspection;
  let outcome = {
    status: 'failed',
    exitCode: exits.failed,
    teardownCall: null,
  };
  let code;
  let detail;
  let invocationFailureDetail;
  let summary;
  try {
    if (!modules.distPresent())
      return result(exits.invalidInput, {
        code: codes.distMissing,
        command: 'pnpm build',
      });
    if (
      prepared.config.referenceWorker.maxInvocations <
      DIRECT_SCENARIO_MIN_INVOCATIONS
    )
      return result(exits.invalidInput, { code: codes.belowScenarioFloor });
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
      outcome = {
        status: 'outcome-unknown',
        exitCode: exits.failed,
        teardownCall: null,
      };
      code = 'outcome-unknown';
    }
    if (journal) {
      if (input.mode === 'resume') await journal.recordResume();
      const snapshot = journal.snapshot();
      // A settled teardown leaves nothing to drive, so the run is
      // evidence-only.
      if (
        snapshot.teardown?.phase === 'complete' &&
        snapshot.teardown.failure === null
      ) {
        outcome = {
          status: 'cleaned',
          exitCode: exits.success,
          teardownCall: null,
        };
      } else {
        const networkInput = {
          prepared,
          journal,
          apiToken: input.env.CLOUDFLARE_API_TOKEN,
          ...(input.fetch ? { fetch: input.fetch } : {}),
        };
        let restart = false;
        // No recorded teardown and no settled scenario. This predicate's
        // complement — a recorded teardown, a failed scenario, a complete
        // scenario — skips straight to teardown on the journal's own record.
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
            exitCode: exits.restartRequired,
            teardownCall: null,
          };
        } else {
          // The other branches converge here.
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
            exitCode:
              status === 'cleaned'
                ? exits.success
                : status === 'failed'
                  ? exits.failed
                  : exits.retained,
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
    // Reminting through the class re-validates a `code` the thrower owns
    // against the class's own vocabulary, which an instance can carry past.
    code =
      error instanceof DirectRunStateError
        ? new DirectRunStateError(error.code).code
        : error instanceof DirectBootstrapError
          ? new DirectBootstrapError(error.code).code
          : codes.internalError;
    if (error instanceof DirectBootstrapError)
      detail = new DirectBootstrapError(error.code, error.detail).detail;
    outcome = {
      status: 'failed',
      exitCode: exits.failed,
      teardownCall: null,
    };
  } finally {
    const handle = journal ?? inspection;
    if (handle) {
      let projected = false;
      let evidence;
      let directory;
      try {
        const snapshot = journal ? journal.snapshot() : inspection.snapshot;
        evidence = buildDirectEvidence({
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
            ? { command: DIRECT_CONFORMANCE_COMMANDS.resume }
            : {}),
        };
        // The handle owns the layout; this module derives none of its own.
        directory = handle.directory;
        projected = true;
      } catch {
        outcome.exitCode = exits.evidenceFailed;
        summary = evidenceFailureSummary(evidenceWritten);
      }
      if (projected) {
        try {
          const written = await modules.writeEvidence({
            directory,
            evidence,
            sentinels,
          });
          evidenceWritten = written.written;
          if (written.written) evidencePath = join(directory, 'evidence.json');
          if (!written.written) {
            outcome.exitCode = exits.evidenceFailed;
            // A withheld artifact carries one class member: the credential
            // class the scan matched, or the refusal class of an identity
            // whose shape the boundary rejects. The summary forwards the one
            // that is present beside its path.
            const { sentinelClass, refusalClass, keyPath } = written;
            summary = evidenceFailureSummary(
              evidenceWritten,
              (sentinelClass ?? refusalClass)
                ? {
                    ...(sentinelClass === undefined
                      ? { refusalClass }
                      : { sentinelClass }),
                    keyPath,
                  }
                : undefined,
            );
          }
        } catch (error) {
          evidenceWritten = error instanceof DirectEvidenceWriteError;
          // The class is raised only after replacement, so the artifact the run
          // reports is the one on disk.
          if (evidenceWritten) evidencePath = join(directory, 'evidence.json');
          outcome.exitCode = exits.evidenceFailed;
          summary = evidenceFailureSummary(evidenceWritten);
        }
      }
      summary = { ...summary, evidenceWritten };
      try {
        await handle.close();
      } catch {
        outcome.exitCode = resolveDirectExitCode(
          outcome.exitCode,
          exits.failed,
        );
        if (outcome.exitCode !== exits.evidenceFailed)
          summary = { code: codes.internalError, evidenceWritten };
      }
    }
  }
  if (
    code === codes.internalError &&
    resolveDirectExitCode(outcome.exitCode, exits.failed) === exits.failed
  )
    return result(exits.failed, { code }, evidencePath);
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
