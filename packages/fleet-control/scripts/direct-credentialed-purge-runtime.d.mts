// SPDX-License-Identifier: Apache-2.0

export type DirectPurgeMode = 'list' | 'delete' | 'help';
export interface DirectPurgeArguments {
  readonly mode: DirectPurgeMode;
  readonly confirmation: string | null;
}
export type DirectPurgeExitCode =
  (typeof DIRECT_PURGE_EXIT_CODES)[keyof typeof DIRECT_PURGE_EXIT_CODES];
export interface DirectPurgeResult {
  /** `internalError` is the entry's own code; the runtime never returns it. */
  readonly exitCode: Exclude<
    DirectPurgeExitCode,
    (typeof DIRECT_PURGE_EXIT_CODES)['internalError']
  >;
  readonly summary: Readonly<Record<string, unknown>>;
  readonly stdoutLine: string;
  readonly stderrLine: null;
}
export const DIRECT_PURGE_OUTPUT_PREFIX: 'DIRECT_PURGE ';
export const DIRECT_PURGE_USAGE: string;
export const DIRECT_PURGE_MAX_REQUESTS: 512;
export const DIRECT_PURGE_EXIT_CODES: Readonly<{
  success: 0;
  residual: 1;
  invalidInput: 2;
  providerFailed: 3;
  internalError: 4;
}>;
/**
 * Resolves one process exit code from several. `internalError` outranks every
 * other code, so a result that settles after a trapped fault cannot replace it.
 */
export function resolveDirectPurgeExitCode(
  current: DirectPurgeExitCode | null,
  next: DirectPurgeExitCode,
): DirectPurgeExitCode;
export function parseDirectPurgeArgs(
  argv: readonly string[],
): DirectPurgeArguments | null;
export function runDirectCredentialedPurge(
  input: Readonly<{
    argv?: readonly string[];
    parsed?: DirectPurgeArguments | null;
    configPath: string | undefined;
    env: Readonly<Record<string, string | undefined>>;
    fetch?: typeof fetch;
    delay?: (milliseconds: number) => Promise<void>;
  }>,
): Promise<DirectPurgeResult>;
