// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectInvocationFailureDetail } from './direct-credentialed-invocation.mjs';
import type {
  DirectRunSnapshot,
  DirectTeardownFailure,
} from './direct-credentialed-run-state.mjs';
import type { DirectTeardownOutcome } from './direct-credentialed-teardown.mjs';

/** The modes that reach the provider, and therefore the modes evidence covers. */
export type DirectLiveMode = 'run' | 'resume';
export type DirectEvidenceStatus =
  | 'cleaned'
  | 'retained'
  | 'restart-required'
  | 'failed'
  | 'outcome-unknown';
export type DirectEvidenceTeardownCall = Readonly<{
  status: DirectTeardownOutcome['status'];
  failure: Readonly<{ code: DirectTeardownFailure }> | null;
  providerRequests: number;
}>;
export type DirectEvidenceSentinels = Readonly<{
  secrets: readonly string[];
  literals: readonly string[];
}>;
export type DirectEvidenceSentinelHit = Readonly<{
  /** The credential class the scan matched. A shape refusal carries none. */
  sentinelClass?: 'env-secret' | 'literal';
  /**
   * Set instead of `sentinelClass` when the value at `keyPath` is a provider
   * identity of the wrong shape: the artifact is withheld for what the journal
   * carries, not for a credential the scan found in it.
   */
  refusalClass?: 'identity-shape';
  /**
   * The dotted path the scan stopped at, empty when the sentinel is completed
   * by the serialization's own structure rather than by a member. It is
   * diagnostic provenance and can itself contain a credential, because an
   * object key that is one becomes part of the path: scan any line rendering
   * it before printing that line.
   */
  keyPath: string;
}>;
export const DIRECT_EVIDENCE_LITERALS: readonly string[];
/** The commands the artifact publishes and the runtime names in a restart summary. */
export const DIRECT_CONFORMANCE_COMMANDS: Readonly<{
  run: string;
  resume: string;
}>;
/**
 * Every key name the projection writes, including the scenario phases and the
 * residual surfaces. A credential one of these contains ends a run at
 * publication, which is why live-mode admission refuses it. Array elements are
 * keyed by index instead, which admission answers with its digits-only rule.
 */
export const DIRECT_EVIDENCE_KEYS: readonly string[];
/**
 * The dotted paths of the projected provider strings the journal decodes with
 * `identifier`, each guarded against the identity shape. A prefix-derived name
 * is absent because it is the run's own; a value the journal bounds to the
 * scenario charset is absent because that charset is strictly wider than the
 * identity shape, so a guard on it refuses values the journal admits.
 */
export const DIRECT_EVIDENCE_IDENTITY_PATHS: readonly string[];
/**
 * Raised only after the artifact has replaced its predecessor, so catching the
 * class is itself the proof that `evidence.json` exists.
 */
export class DirectEvidenceWriteError extends Error {
  constructor();
}
export type DirectEvidenceScenarioFailure = Readonly<{
  code: string;
  ordinal: number;
  detail: string | null;
}>;
export type DirectEvidenceScenario = Readonly<{
  phase: string;
  failure: DirectEvidenceScenarioFailure | null;
  invocationCount: number;
  sdkRequests: number | null;
  attempts: Readonly<Record<string, number>>;
  phaseCalls: Readonly<Record<string, number>>;
  restart: Readonly<Record<string, unknown>> | null;
  initial: Readonly<Record<string, unknown>>;
  candidate: Readonly<Record<string, unknown>>;
  final: Readonly<Record<string, unknown>>;
  fence: Readonly<Record<string, unknown>>;
  exports: Readonly<Record<string, unknown>>;
  inventories: Readonly<Record<string, unknown>>;
  terminalForce: Readonly<Record<string, unknown>>;
}>;
export type DirectEvidenceCost = Readonly<{
  basis: 'request-counters';
  referenceProvider: number | null;
  referenceMaintenance: number | null;
  referenceApplication: number | null;
  sdkRequests: number | null;
  referenceInvocations: number;
  teardownProvider: number | null;
  billed: null;
}>;
/**
 * The evidence artifact, in the member order `writeDirectEvidence` serializes
 * and `evidence.json` carries. The allowlist is the artifact's contract: a
 * member absent here is a member the projection does not write.
 */
export type DirectEvidenceArtifact = Readonly<{
  version: 1;
  contractVersion: number;
  packageVersion: string;
  commit: string | null;
  mode: DirectLiveMode;
  status: DirectEvidenceStatus;
  exitCode: number;
  startedAt: string | null;
  finishedAt: string;
  resumeCount: number;
  accountIdSha256Suffix: string;
  zoneIdSha256Suffix: string | null;
  resourcePrefix: string;
  maxInvocations: number;
  disposableAccount: boolean;
  configSha256: string;
  referenceModuleSetSha256: string;
  referenceUploadBytes: number;
  commands: readonly string[];
  bootstrap: Readonly<Record<string, unknown>> | null;
  scenario: DirectEvidenceScenario | null;
  teardown: Readonly<Record<string, unknown>> | null;
  teardownCall: DirectEvidenceTeardownCall | null;
  retainedIdentities: Readonly<Record<string, string | null>>;
  cost: DirectEvidenceCost;
}>;
export function buildDirectEvidence(
  input: Readonly<{
    snapshot: DirectRunSnapshot;
    invocationFailureDetail?: DirectInvocationFailureDetail;
    prepared: PreparedDirectConformance;
    mode: DirectLiveMode;
    outcome: Readonly<{
      status: DirectEvidenceStatus;
      exitCode: number;
      teardownCall: DirectEvidenceTeardownCall | null;
    }>;
    times: Readonly<{ finishedAt: string }>;
    commit: string | null;
  }>,
): DirectEvidenceArtifact;
/**
 * `serialized` is the exact byte sequence a caller publishes or prints: the
 * JSON text of `evidence` with the trailing newline already appended. A caller
 * that adds its own terminator writes two.
 */
export function inspectDirectEvidence(
  evidence: object,
  sentinels: DirectEvidenceSentinels,
): Readonly<{ hit: DirectEvidenceSentinelHit | null; serialized: string }>;
/**
 * Publishes the artifact, or withholds it and returns the hit that stopped it:
 * a refusal carries the same `keyPath` and the same class member — a
 * `sentinelClass` for a credential, a `refusalClass` for a shape — the scan
 * reports.
 */
export function writeDirectEvidence(
  input: Readonly<{
    directory: string;
    evidence: object;
    sentinels: DirectEvidenceSentinels;
    readBack?: (path: string) => Promise<Buffer>;
  }>,
): Promise<Readonly<{ written: boolean }> & Partial<DirectEvidenceSentinelHit>>;
