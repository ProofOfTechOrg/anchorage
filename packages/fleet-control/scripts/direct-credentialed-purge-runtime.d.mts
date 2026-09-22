// SPDX-License-Identifier: Apache-2.0

export type DirectPurgeMode = 'list' | 'delete' | 'help';
export interface DirectPurgeArguments {
  readonly mode: DirectPurgeMode;
  readonly confirmation: string | null;
}
export interface DirectPurgeResult {
  readonly exitCode: 0 | 1 | 2 | 3 | 4;
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
