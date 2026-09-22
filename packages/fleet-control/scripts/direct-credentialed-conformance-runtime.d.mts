// SPDX-License-Identifier: Apache-2.0

import type { bootstrapDirectConformance } from './direct-credentialed-bootstrap.mjs';
import type { preflightDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  DirectLiveMode,
  writeDirectEvidence,
} from './direct-credentialed-evidence.mjs';
import type { reconcileDirectInvocation } from './direct-credentialed-invocation.mjs';
import type {
  inspectDirectRunState,
  openDirectRunState,
} from './direct-credentialed-run-state.mjs';
import type { runDirectCredentialedScenario } from './direct-credentialed-scenario.mjs';
import type { runDirectCredentialedSweep } from './direct-credentialed-sweep.mjs';
import type { teardownDirectReference } from './direct-credentialed-teardown.mjs';

export interface DirectConformanceModules {
  preflight: typeof preflightDirectConformance;
  openRunState: typeof openDirectRunState;
  inspectRunState: typeof inspectDirectRunState;
  reconcileInvocation: typeof reconcileDirectInvocation;
  bootstrap: typeof bootstrapDirectConformance;
  scenario: typeof runDirectCredentialedScenario;
  sweep: typeof runDirectCredentialedSweep;
  teardown: typeof teardownDirectReference;
  writeEvidence: typeof writeDirectEvidence;
  distPresent: () => boolean;
}
export type DirectConformanceMode = 'preflight' | 'run' | 'resume' | 'help';
export const DIRECT_CONFORMANCE_MODES: readonly DirectConformanceMode[];
export const DIRECT_LIVE_MODES: readonly DirectLiveMode[];
export function isDirectLiveMode(
  mode: DirectConformanceMode | null,
): mode is DirectLiveMode;
export const DIRECT_CREDENTIAL_VARIABLES: readonly string[];
export const DIRECT_ADMISSION_VARIABLES: readonly string[];
export const DIRECT_CONFORMANCE_CODES: Readonly<{
  belowScenarioFloor: 'below-scenario-floor';
  distMissing: 'dist-missing';
  evidenceFailed: 'evidence-failed';
  internalError: 'internal-error';
  invalidInput: 'invalid-input';
  preflightFailed: 'preflight-failed';
  usage: 'usage';
}>;
export type DirectConformanceCode =
  (typeof DIRECT_CONFORMANCE_CODES)[keyof typeof DIRECT_CONFORMANCE_CODES];
export const DIRECT_CONFORMANCE_EXIT_CODES: Readonly<{
  success: 0;
  failed: 1;
  invalidInput: 2;
  restartRequired: 3;
  retained: 4;
  evidenceFailed: 5;
}>;
export type DirectConformanceExitCode =
  (typeof DIRECT_CONFORMANCE_EXIT_CODES)[keyof typeof DIRECT_CONFORMANCE_EXIT_CODES];
export const DIRECT_CONFORMANCE_USAGE: string;
export const DIRECT_OUTPUT_PREFIX: string;
export const DIRECT_USAGE_DIAGNOSTIC: string;
export const DIRECT_INTERNAL_ERROR_DIAGNOSTIC: string;
export const DIRECT_FIXED_OUTPUT: readonly string[];
export function parseDirectConformanceArgs(
  argv: readonly string[],
): DirectConformanceMode | null;
export function directLineCarries(
  line: string,
  values: readonly (string | undefined)[],
): boolean;
/**
 * The bytes a result renders on stdout: the summary line, or the empty string
 * where the safe rendering is silence.
 */
export function directStdoutOf(result: DirectConformanceResult): string;
export function directWritesStderr(result: DirectConformanceResult): boolean;
/**
 * Resolves one process exit code from several. `evidenceFailed` ranks above
 * `failed`, which ranks above every other code, so a late failure replaces the
 * `restartRequired` or `retained` code a run had already reached.
 */
export function resolveDirectExitCode(
  current: number | null,
  next: number,
): number;
/**
 * The summary a run resolves. `code` is one of `DIRECT_CONFORMANCE_CODES` when
 * the runtime minted it, and the refusal's own code when a run-state or
 * bootstrap error carried one.
 */
export type DirectConformanceSummary = Readonly<{
  code?: string;
  variable?: string;
  evidenceWritten?: boolean;
}> &
  Readonly<Record<string, unknown>>;
/**
 * The two shapes a run resolves to.
 *
 * `stdoutLine` and `stderrLine` are the only members a caller writes: each is
 * scanned against the run's credentials and the forbidden literals, and is
 * `null` where the safe rendering is silence. `summary` is the same content
 * parsed back and carries bytes those lines withhold, so it belongs in an
 * assertion or a log the operator already trusts, never on a stream.
 *
 * The second member is live-mode admission, which refuses on stderr alone: it
 * exits 2, prints no summary, and its stderr line is one of
 * `DIRECT_FIXED_OUTPUT` or `null`.
 */
export type DirectConformanceResult =
  | Readonly<{
      exitCode: DirectConformanceExitCode;
      summary: DirectConformanceSummary;
      evidencePath: string | null;
      stdoutLine: string | null;
      stderrLine: string | null;
      stderrOnly?: undefined;
    }>
  | Readonly<{
      exitCode: 2;
      summary: DirectConformanceSummary;
      evidencePath: null;
      stdoutLine: null;
      stderrLine: string | null;
      stderrOnly: true;
    }>;
export function runDirectConformance(
  input: Readonly<{
    mode: DirectConformanceMode;
    configPath: string | undefined;
    env: Readonly<Record<string, string | undefined>>;
    fetch?: typeof fetch;
    delay?: (milliseconds: number) => Promise<void>;
    now?: () => number;
    git?: () => string | null;
    modules?: Partial<DirectConformanceModules>;
  }>,
): Promise<DirectConformanceResult>;
