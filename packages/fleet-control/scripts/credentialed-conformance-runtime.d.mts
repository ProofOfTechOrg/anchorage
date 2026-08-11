// SPDX-License-Identifier: Apache-2.0

import type {
  DeploymentSecrets,
  DeploymentSpec,
  ExternalPlatformProfile,
} from '../src/types.js';

export interface OperationalConformancePlan {
  readonly initialSpec: DeploymentSpec;
  readonly nextSpec: DeploymentSpec;
  readonly initialProfile: ExternalPlatformProfile;
  readonly nextProfile: ExternalPlatformProfile;
  readonly secrets: DeploymentSecrets;
}

export function validateOperationalConformance(options: {
  readonly plans: readonly OperationalConformancePlan[];
  readonly maintenanceCapabilityPublicKey: string;
  readonly validateDeploymentSpec: (spec: DeploymentSpec) => void;
  readonly validateDeploymentSecrets: (
    spec: DeploymentSpec,
    secrets: DeploymentSecrets,
  ) => void;
  readonly validateExternalPlatformProfile: (
    spec: DeploymentSpec,
    profile: ExternalPlatformProfile,
  ) => void;
  readonly canonicalMaintenanceCapabilityPublicKey: (value: string) => string;
}): void;

export function preflightMaintenanceCapabilityKeyPair(options: {
  readonly privateJwk: string;
  readonly publicJwk: string;
  readonly canonicalizePublicKey: (value: string) => string;
}): Readonly<Record<string, unknown>>;

export function loadCredentialedConformanceArtifacts<T>(options: {
  readonly privateJwk: string;
  readonly publicJwk: string;
  readonly canonicalizePublicKey: (value: string) => string;
  readonly workerBundle: string;
  readonly stateWorkerBundles: readonly string[];
  readonly readArtifact: (path: string) => T | Promise<T>;
}): Promise<
  Readonly<{
    maintenanceCapabilityPrivateKey: Readonly<Record<string, unknown>>;
    workerContent: T;
    stateWorkerContents: readonly T[];
  }>
>;

interface CredentialedCleanupSpec {
  readonly tenantTag: string;
  readonly environment: string;
}

interface CredentialedCleanupRecord {
  readonly phase: string;
  readonly desiredSpecDigest: string;
}

interface CredentialedCleanupDeployment<TSpec extends CredentialedCleanupSpec> {
  readonly initialSpec: TSpec;
  readonly nextSpec: TSpec;
  readonly currentSpec?: TSpec;
  readonly secrets: unknown;
  readonly store: {
    get(
      tenantTag: string,
      environment: string,
    ): Promise<CredentialedCleanupRecord | undefined>;
  };
}

export function selectCredentialedCleanupSpec<
  TSpec extends CredentialedCleanupSpec,
>(options: {
  readonly record: CredentialedCleanupRecord | undefined;
  readonly initialSpec: TSpec;
  readonly nextSpec: TSpec;
  readonly deploymentSpecDigest: (spec: TSpec) => string;
}): TSpec | undefined;

export function cleanupCredentialedDeployment<
  TSpec extends CredentialedCleanupSpec,
>(
  deployment: CredentialedCleanupDeployment<TSpec>,
  dependencies: {
    readonly backend: unknown;
    readonly deploymentSpecDigest: (spec: TSpec) => string;
    readonly beforeCleanup?: (
      spec: TSpec,
      record: CredentialedCleanupRecord,
    ) => unknown;
    readonly decommissionDeployment: (options: {
      readonly backend: unknown;
      readonly store: CredentialedCleanupDeployment<TSpec>['store'];
      readonly spec: TSpec;
      readonly secrets: unknown;
    }) => unknown;
    readonly cleanupDeploymentArtifacts: (options: {
      readonly backend: unknown;
      readonly store: CredentialedCleanupDeployment<TSpec>['store'];
      readonly spec: TSpec;
    }) => unknown;
  },
): Promise<void>;

export interface CredentialedConformanceDependencies<T> {
  readonly provisionV1: (deployment: T) => unknown;
  readonly probeV1: (deployment: T) => unknown;
  readonly assertTenantIsolation: (deployments: readonly T[]) => unknown;
  readonly activateV2: () => unknown;
  readonly migrateV2: (deployment: T) => unknown;
  readonly probeV2: (deployment: T) => unknown;
  readonly completeFlowSafe: (deployment: T) => unknown;
  readonly rollback: (deployment: T) => unknown;
  readonly proveNonemptyDecommission: (deployment: T) => unknown;
  readonly decommission: (deployment: T) => unknown;
  readonly assertZeroResiduals: () => unknown;
  readonly cleanup: (deployment: T) => unknown;
}

export function runCredentialedConformance<T>(
  config: { readonly deployments: readonly T[] },
  dependencies: CredentialedConformanceDependencies<T>,
): Promise<Readonly<Record<string, true>>>;
