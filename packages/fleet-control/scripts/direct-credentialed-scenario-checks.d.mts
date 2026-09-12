// SPDX-License-Identifier: Apache-2.0

import type { CleanupTerminalReceipt } from '../src/types.js';
import type { DirectInvocationAttempts } from './direct-credentialed-invocation.mjs';
import type {
  DirectExpectedWorkerVersion,
  DirectWorkerVersionObservation,
} from './direct-credentialed-observations.mjs';
import type {
  DIRECT_SCENARIO_OPERATION_SLOTS,
  DirectRunActionSummary,
} from './direct-credentialed-run-state.mjs';
import type { DIRECT_SCENARIO_PHASES } from './direct-credentialed-scenario-budget.mjs';
import type { DirectFixtureRole } from './direct-credentialed-spec.js';

export type DirectScenarioNormalRole = Exclude<DirectFixtureRole, 'recovery'>;
export type DirectScenarioPhase = (typeof DIRECT_SCENARIO_PHASES)[number];
export type DirectScenarioOperationSlot =
  (typeof DIRECT_SCENARIO_OPERATION_SLOTS)[number];

export interface DirectScenarioRemoteOperation {
  readonly slot: DirectScenarioOperationSlot;
  readonly operationId: string | null;
  readonly inputJson: string;
  readonly tokenRevision: number | null;
}

export interface DirectScenarioOperationFacts {
  readonly slot: DirectScenarioOperationSlot;
  readonly operationId: string | null;
  readonly inputSha256: string;
  readonly tokenRevision: number | null;
}

export interface DirectScenarioRemoteRecord {
  readonly role: DirectFixtureRole;
  readonly present: boolean;
  readonly phase?: string | null;
  readonly desiredSpecDigest?: string | null;
  readonly pendingSpecDigest?: string | null;
  readonly artifactVersion?: string | null;
  readonly pendingArtifactVersion?: string | null;
  readonly databaseId?: string | null;
}

export interface DirectScenarioRecordFacts {
  readonly role: DirectFixtureRole;
  readonly present: boolean;
  readonly phase: string | null;
  readonly desiredSpecDigest: string | null;
  readonly pendingSpecDigest: string | null;
  readonly artifactVersion: string | null;
  readonly pendingArtifactVersion: string | null;
  readonly databaseId: string | null;
}

export interface DirectScenarioMigrationItem {
  readonly ordinal: number;
  readonly status: string;
  readonly planCursor: number;
}

export interface DirectScenarioInterruptedItem {
  readonly status: string;
  readonly planCursor: number;
  readonly entryRecordDigest: string;
  readonly targetSpecDigest: string;
}

export interface DirectScenarioInterruptionValue {
  readonly version: 1;
  readonly boundary: 'after-migration-admission';
  readonly slot: 'migration-next';
  readonly operationId: string;
  readonly claimJson: string;
  readonly returnedTokenJson: string;
  readonly item: Readonly<{
    ordinal: 0;
    beforeStatus: string;
    afterStatus: string;
    planCursor: number;
    tenantTag: string;
    environment: string;
    entryRecordDigest: string;
    targetSpecDigest: string;
  }>;
}

export interface DirectScenarioInterruption {
  readonly value: DirectScenarioInterruptionValue;
  readonly claim: unknown;
  readonly successor: unknown;
}

export interface DirectScenarioFootprint {
  readonly version: 1;
  readonly role: 'recovery';
  readonly beforeIdentitySha256: string;
  readonly fleetRecordPresent: false;
  readonly deploymentClaimsPresent: false;
  readonly database: Readonly<{
    id: string;
    expectedName: string;
    observedName: null;
  }>;
  readonly worker: Readonly<{
    scriptName: string;
    scriptPresent: boolean;
    workersDevEnabled: false | null;
    previewUrlsEnabled: false | null;
    customDomains: readonly never[];
    zoneRoutes: readonly never[];
    currentSecretNames: readonly never[];
    currentVersionIds: readonly string[] | null;
    currentNamespaceIds: readonly string[];
    survivingRecordedNamespaceIds: readonly string[];
  }>;
  readonly buckets: readonly Readonly<{
    bindingName: 'PROBE_BUCKET';
    bucketName: string;
    jurisdiction: 'default';
    expectedCreationDate: string;
    observedCreationDate: string | null;
  }>[];
  readonly priorCleanup: Readonly<{
    operationId: string;
    observedReceiptSha256: string;
    matchesBefore: true;
  }>;
}

export interface DirectScenarioFootprintContext {
  readonly resource: DirectWorkerVersionObservation;
  readonly databaseName: string;
  readonly cleanup: CleanupTerminalReceipt;
  readonly versionIdMaximum: number;
}

export interface DirectScenarioSettledCall {
  readonly ordinal: number;
  readonly action: DirectRunActionSummary;
  readonly outcome: string;
}

export interface DirectScenarioAllowedChanges {
  readonly slots: readonly string[];
  readonly roles: readonly DirectFixtureRole[];
}

export const NORMAL_ROLES: readonly DirectScenarioNormalRole[];
export const SCENARIO_ROLES: readonly DirectFixtureRole[];

export function requireFact(
  condition: unknown,
  code?: string,
  detail?: string,
): asserts condition;
export function equal(actual: unknown, expected: unknown): void;
export function parse(value: unknown): unknown;
export function hash(value: string): string;
export function jsonHash(value: unknown): string;
export function zeroAttempts(): DirectInvocationAttempts;
export function phaseInvocationReserve(phase: DirectScenarioPhase): number;
export function checkInvocationHeadroom(
  phase: DirectScenarioPhase,
  phaseCalls: Readonly<Record<DirectScenarioPhase, number>>,
  remaining: number,
): void;
export function changedBy(
  action: DirectRunActionSummary | null | undefined,
): DirectScenarioAllowedChanges;
export function recordFacts(
  record: DirectScenarioRemoteRecord,
): DirectScenarioRecordFacts;
export function operationFacts(
  operation: DirectScenarioRemoteOperation,
): DirectScenarioOperationFacts;
export function expectedVersion(
  record: DirectScenarioRemoteRecord | null | undefined,
  release: '1' | '2',
  candidate?: boolean,
): DirectExpectedWorkerVersion;
export function checkItemConvergence(
  items: readonly DirectScenarioMigrationItem[],
): void;
export function checkTrafficDistribution(
  candidate: DirectWorkerVersionObservation,
  previous: DirectWorkerVersionObservation,
): void;
export function migrationStartSettled(
  control: Readonly<{ operations: readonly DirectScenarioRemoteOperation[] }>,
  mutation: DirectScenarioSettledCall | null | undefined,
): boolean;
export function migrationInterruptSettled(
  control: Readonly<{ interruption: string | null }>,
  mutation: DirectScenarioSettledCall | null | undefined,
): boolean;
export function checkInterruptionWitness(
  interruption: unknown,
  expected: Readonly<{
    operationId: string | null;
    tenantTag: string;
    environment: string;
  }>,
): DirectScenarioInterruption;
export function checkInterruptedItems(
  witness: DirectScenarioInterruptionValue,
  items: readonly [
    DirectScenarioInterruptedItem,
    DirectScenarioInterruptedItem,
    ...DirectScenarioInterruptedItem[],
  ],
): void;
export function checkFootprint(
  observation: unknown,
  expected: DirectScenarioFootprintContext &
    (
      | Readonly<{ retained: true; force?: null }>
      | Readonly<{ retained: false; force: DirectScenarioFootprint }>
    ),
): DirectScenarioFootprint;
