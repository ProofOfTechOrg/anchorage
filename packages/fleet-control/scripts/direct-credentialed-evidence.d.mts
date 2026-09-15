// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectInvocationFailureDetail } from './direct-credentialed-invocation.mjs';
import type {
  DirectRunSnapshot,
  DirectTeardownFailure,
} from './direct-credentialed-run-state.mjs';

export type DirectEvidenceStatus =
  | 'cleaned'
  | 'retained'
  | 'restart-required'
  | 'failed'
  | 'outcome-unknown';
export type DirectEvidenceTeardownCall = Readonly<{
  status: 'cleaned' | 'retained';
  failure: Readonly<{ code: DirectTeardownFailure }> | null;
  providerRequests: number;
}>;
export type DirectEvidenceSentinels = Readonly<{
  secrets: readonly string[];
  literals: readonly string[];
}>;
export type DirectEvidenceSentinelHit = Readonly<{
  sentinelClass: 'env-secret' | 'literal' | 'identity-shape';
  keyPath: string;
}>;
export const DIRECT_EVIDENCE_LITERALS: readonly string[];
export class DirectEvidenceWriteError extends Error {
  readonly written: true;
  constructor();
}
export function buildDirectEvidence(
  input: Readonly<{
    snapshot: DirectRunSnapshot;
    invocationFailureDetail?: DirectInvocationFailureDetail;
    prepared: PreparedDirectConformance;
    mode: 'run' | 'resume';
    outcome: Readonly<{
      status: DirectEvidenceStatus;
      exitCode: number;
      teardownCall: DirectEvidenceTeardownCall | null;
    }>;
    times: Readonly<{ finishedAt: string }>;
    commit: string | null;
  }>,
): Readonly<Record<string, unknown>>;
export function inspectDirectEvidence(
  evidence: object,
  sentinels: DirectEvidenceSentinels,
): Readonly<{ hit: DirectEvidenceSentinelHit | null; serialized: string }>;
export function scanDirectEvidence(
  evidence: object,
  sentinels: DirectEvidenceSentinels,
): DirectEvidenceSentinelHit | null;
export function writeDirectEvidence(
  input: Readonly<{
    directory: string;
    evidence: object;
    sentinels: DirectEvidenceSentinels;
    readBack?: (path: string) => Promise<Buffer>;
  }>,
): Promise<
  Readonly<{ written: boolean; sentinelClass?: string; keyPath?: string }>
>;
