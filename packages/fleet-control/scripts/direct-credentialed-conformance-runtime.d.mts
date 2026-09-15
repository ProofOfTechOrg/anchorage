// SPDX-License-Identifier: Apache-2.0

import type { bootstrapDirectConformance } from './direct-credentialed-bootstrap.mjs';
import type { preflightDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type {
  inspectDirectRunState,
  openDirectRunState,
} from './direct-credentialed-run-state.mjs';
import type { runDirectCredentialedScenario } from './direct-credentialed-scenario.mjs';
import type { teardownDirectReference } from './direct-credentialed-teardown.mjs';

export interface DirectConformanceModules {
  preflight: typeof preflightDirectConformance;
  openRunState: typeof openDirectRunState;
  inspectRunState: typeof inspectDirectRunState;
  bootstrap: typeof bootstrapDirectConformance;
  scenario: typeof runDirectCredentialedScenario;
  teardown: typeof teardownDirectReference;
  distPresent: () => boolean;
}
export type DirectConformanceMode = 'preflight' | 'run' | 'resume' | 'help';
export const DIRECT_CONFORMANCE_USAGE: string;
export const DIRECT_OUTPUT_PREFIX: string;
export const DIRECT_USAGE_DIAGNOSTIC: string;
export const DIRECT_INTERNAL_ERROR_DIAGNOSTIC: string;
export const DIRECT_FIXED_OUTPUT: readonly string[];
export function parseDirectConformanceArgs(
  argv: readonly string[],
): DirectConformanceMode | null;
export function resolveDirectExitCode(
  current: number | null,
  next: number,
): number;
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
): Promise<
  Readonly<{
    exitCode: number;
    summary: object;
    evidencePath: string | null;
    stdoutLine: string | null;
    stderrLine: string | null;
    stderrOnly?: true;
  }>
>;
