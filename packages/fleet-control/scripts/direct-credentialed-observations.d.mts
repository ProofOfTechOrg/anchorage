// SPDX-License-Identifier: Apache-2.0

import type { PreparedDirectConformance } from './direct-credentialed-conformance-preflight.mjs';
import type { DirectRunJournal } from './direct-credentialed-run-state.mjs';
import type { DirectFixtureRole } from './direct-credentialed-spec.js';
import type { DirectDecommissionExportMetadata } from './direct-reference-lifecycle.js';

export type DirectObservationErrorCode =
  | 'invalid-input'
  | 'outcome-unknown'
  | 'observation-mismatch'
  | 'provider-unavailable'
  | 'budget-exhausted';

export class DirectObservationError extends Error {
  readonly code: DirectObservationErrorCode;
  constructor(code?: DirectObservationErrorCode);
}

export interface DirectObservationContext {
  readonly prepared: PreparedDirectConformance;
  readonly journal: DirectRunJournal;
  readonly apiToken: string;
  readonly fetch?: typeof fetch;
}

export interface DirectExpectedWorkerVersion {
  readonly role: DirectFixtureRole;
  readonly versionId: string;
  readonly databaseId: string;
  readonly specDigest: string;
  readonly applicationRelease: '1' | '2';
}

export interface DirectWorkerVersionObservation
  extends DirectExpectedWorkerVersion {
  readonly accountId: string;
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly currentDeployment: Readonly<{
    deploymentId: string;
    activeVersionId: string;
    versions: readonly Readonly<{ versionId: string; percentage: number }>[];
  }>;
  readonly trafficPercentage: number;
  readonly cpuLimitMs: number;
  readonly subrequestLimit: number;
  readonly schemaVersion: number;
  readonly databaseId: string;
  readonly namespaces: readonly Readonly<{
    binding: 'MAINTENANCE' | 'RUNNER';
    className: 'Maintenance' | 'Runner';
    namespaceId: string;
  }>[];
  readonly bucket: Readonly<{
    name: string;
    jurisdiction: 'default';
    creationDate: string;
  }>;
}

export interface DirectSettlementEffect {
  readonly role: DirectFixtureRole;
  readonly tenantTag: string;
  readonly environment: string;
  readonly scriptName: string;
  readonly databaseId: string;
  readonly versionId: string;
  readonly specDigest: string;
  readonly schemaVersion: number;
  readonly settlementKey: string;
  readonly identitySha256: string;
  readonly provenanceSha256: string;
}

export interface DirectVerifiedExport {
  readonly verified: true;
  readonly role: DirectFixtureRole;
  readonly receipt: Readonly<{
    version: 1;
    authority: string;
    databaseId: string;
    operationId: string;
  }>;
  readonly location: string;
  readonly size: number;
  readonly sha256: string;
  readonly sourceInvocationOrdinal: number;
}

export function observeDirectWorkerVersion(
  input: DirectObservationContext & DirectExpectedWorkerVersion,
): Promise<DirectWorkerVersionObservation>;

export function readDirectSettlementEffects(
  input: DirectObservationContext &
    Readonly<{ expected: readonly DirectExpectedWorkerVersion[] }>,
): Promise<readonly DirectSettlementEffect[]>;

export function verifyDirectDecommissionExport(
  input: DirectObservationContext &
    Readonly<{
      role: DirectFixtureRole;
      sourceInvocationOrdinal: number;
      metadata: DirectDecommissionExportMetadata;
    }>,
): Promise<DirectVerifiedExport>;
